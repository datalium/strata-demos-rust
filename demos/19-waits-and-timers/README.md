# 19 — waits-and-timers

Event waits, durable timers and ending a thread
(`design/29-Event-Waits-And-Timers.md`, `design/28-Ending-A-Thread.md`,
`design/42-Waking-Through-A-Resume.md`). An online shop's order process
spends most of its life waiting: for the payment provider, for a fraud
reviewer, for the carrier. While it waits, no process holds it. Each order is
a few rows in a SQLite file, and every step of the demo opens a fresh
`SqliteCheckpointStore` on that file, the way a webhook handler, a cron job
and a support tool would, each in its own process.

```
Place -> AwaitPayment      event "payment.received", at most 24 h, no timeout route
      -> FraudReview       orders of $1000 or more: a human boundary, at most 2 h, then Escalate
      -> Pack -> AwaitPickup (a 4 h timer) -> Ship
```

The .NET twin is
[`strata-demos-dotnet/demos/19-waits-and-timers`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/19-waits-and-timers):
same graph, same seven steps, same checks.

**The clock, where the engines differ.** .NET passes a `FakeTimeProvider` as
`RunOptions.TimeProvider`, and it decides both each wait's `wake_at` and what
"now" means when waking. Rust has no clock to inject at the freeze: a wait's
`wake_at` is stamped from `Utc::now()` (plus the delay, rounded up to a whole
second). Waking is the other way round: `wake_due(store, now)`,
`LevelSignals::due_at(now)` and `ThreadQuery::due_by(now)` all take the
caller's `now`. So the demo calls the moment it starts `T`, every freeze
happens within a second or so of `T`, and it wakes at `T+3h`, `T+4h30` and
`T+25h`: a day of waiting takes no time and no fake clock is needed. One
consequence: in .NET the payments arrive at 09:20 on the fake clock, so the
timers they start are due at 13:20; here the payments arrive at (real) `T`,
so the pickups are due at `T+4h00` and the fraud review at `T+2h00`. The story
and which thread is due when are the same. Times are printed relative to `T`
(the minutes after it, rounded down), with the stored timestamp, which is the
run's own wall clock, in brackets.

## What it demonstrates

- **A waiting thread is only rows.** After step 1 the store is dropped and
  the demo reads the file with plain `rusqlite` (no Strata code): five
  `interrupted` rows in the `checkpoint_threads` head table, each with a
  `wake_at`, and the `wait` column
  `{"kind":"event","event_type":"payment.received","wake_at":"..."}`
- **`add_wait_for_event_with(name, event_type, WaitOptions::new().with_timeout(..))`
  answered by `signal(thread_id, EventSignal, store)`.** The signal's
  overrides (`.with_override("paid", true)`, `.with_override("payment_ref", ..)`)
  replace top-level fields of the waiting state, so the payload is in the
  state for every later node. One of the three payments goes through
  `resume(thread_id, LevelSignals::event(signal), store)` instead, which
  answers the wait the same way (design/42); the .NET demo uses only
  `SignalAsync`
- **The edge out of a wait is chosen when it freezes**, on the state as it was
  then. The $2400 order's `next_node` is already `FraudReview` before anyone
  pays, so a signal's payload can't change where the thread goes next
- **The wrong answer is refused, and writes nothing** (5 rows before and
  after): a human `resume` of an event wait, a signal of another event type,
  and `LevelSignals::due_at(T+1h)` before the wait is due. Each comes back as
  `GraphExecutionResult::Failed(StrataError::ExecutionAborted(message))`
- **A human boundary with a timeout**:
  `add_interrupt_boundary_with("FraudReview", BoundaryOptions::new().with_reject("Refund").with_timeout(2 h).with_on_timeout("Escalate"))`.
  Nobody answers it, so at `T+3h` `wake_due` takes the `on_timeout` route
  (`WakeReason::Timeout`), and the thread records no decision
- **`add_timer(name, delay)` woken by `wake_due(store, now)`** once due, and
  not before: at `T+3h` only the fraud review is due, at `T+4h30` both pickups
  are
- **`ThreadQuery::due_by(now)`**: the application lists due threads through
  the query port (`CheckpointQuery::list_threads`), from the head table's
  `wake_at` column
- **`LevelSignals::due_at(now)` through your own `resume_streaming`**: the
  application wakes `order-1005` itself and gets the woken run's chunks
  (`wake_due` doesn't stream). The resume decides at its own load that the
  thread is still due. The driver that runs next finds only `order-1001`
- **A timeout with no route ends the thread**: nobody pays for `order-1002`,
  so at `T+25h` `wake_due` ends it, `Cancelled { reason: Ended, .. }`, with
  `strata.end_reason` = `timed_out` (`END_TIMED_OUT`), which a
  `ThreadQuery::with_metadata` filter finds
- **`end_with(thread_id, ThreadEnd::by("customer"), store, config)`** ends a
  waiting thread on request: `strata.end_reason` = `requested`,
  `strata.ended_by` = `customer`, status `Ended`, no `wait` left, and a late
  payment signal is refused
- **What the file holds at the end**: a completed run writes nothing (the
  graph uses the default `CheckpointPolicy::Boundaries`), so a finished
  order's head is its last resume stamp, `resumed`. The stamp keeps the
  `wake_at` of the wait it answered; only an interrupted, unresumed thread is
  due. 16 checkpoint rows in all, as in .NET

## What it does *not* cover

- **The hosted wake driver** (`datalium-strata-host`'s `Host`), which calls
  `wake_due` on a cadence; see [`28-hosting`](../28-hosting). Here the demo
  calls it by hand at chosen times
- **Graph identity**, which makes `wake_due` list only its own graph's
  threads. This file holds one graph's threads, so it doesn't matter here;
  see [`22-graph-identity`](../22-graph-identity)
- **Two drivers racing** for one due thread (the conditional save makes one
  win and the other skip it, reported in `WakeReport::skipped`), and waits
  inside a subgraph. For leases, see [`21-thread-leases`](../21-thread-leases)
- **`add_timer_until(name, |state| wake_at)`**, a timer due at a time computed
  from the state, `WakeOptions` (metadata filter, limit, stop token), and
  `LevelSignals::due()`, which decides at `Utc::now()` instead of a `now` you
  pass
- **A signal that arrives before its wait** is refused, not buffered. The
  engine doesn't build that

## Run it

```bash
cargo run -p waits-and-timers
```

Output of a real run (the bracketed timestamps are that run's wall clock, so
yours differ; everything else is the same on every run):

```
=== Step 1: T, five orders are placed and wait for payment ===
  order-1001 ($80): waits for event 'payment.received' until T+24h00 (2026-10-25T16:10:15Z)
  order-1002 ($45): waits for event 'payment.received' until T+24h00 (2026-10-25T16:10:15Z)
  order-1003 ($2400): waits for event 'payment.received' until T+24h00 (2026-10-25T16:10:15Z)
  order-1004 ($60): waits for event 'payment.received' until T+24h00 (2026-10-25T16:10:15Z)
  order-1005 ($35): waits for event 'payment.received' until T+24h00 (2026-10-25T16:10:15Z)
  order-1003's next node, fixed at the freeze: FraudReview
  The store is dropped. What is left of the five orders:
    checkpoint_threads (the head table the query port reads):
      order-1001  interrupted  wake_at T+24h00 (2026-10-25T16:10:15Z)
      order-1002  interrupted  wake_at T+24h00 (2026-10-25T16:10:15Z)
      order-1003  interrupted  wake_at T+24h00 (2026-10-25T16:10:15Z)
      order-1004  interrupted  wake_at T+24h00 (2026-10-25T16:10:15Z)
      order-1005  interrupted  wake_at T+24h00 (2026-10-25T16:10:15Z)
  order-1001's wait column: {"kind":"event","event_type":"payment.received","wake_at":"2026-10-25T16:10:15Z"}

=== Step 2: the wrong answers to a payment wait are refused ===
  A clerk approves it:      thread 'order-1001' is waiting for event 'payment.received' at [AwaitPayment]: only a signal of that event answers it
  A refund.issued event:    thread 'order-1001' is not waiting for event 'refund.issued': it is waiting for event 'payment.received' at [AwaitPayment]
  A wake at T+1h00:         thread 'order-1001' is not due until 2026-10-25T16:10:15Z: it is waiting for event 'payment.received' at [AwaitPayment]
  Checkpoint rows before and after: 5 and 5.

=== Step 3: T, payments arrive for order-1001, order-1003 and order-1005 ===
  order-1001: paid (PAY-5531, by signal), now waits for a timer until T+4h00 (2026-10-24T20:10:15Z)
  order-1003: paid (PAY-5532, by signal), now waits for a human until T+2h00 (2026-10-24T18:10:15Z)
  order-1005: paid (PAY-5533, by resume(LevelSignals::event)), now waits for a timer until T+4h00 (2026-10-24T20:10:15Z)

=== Step 4: T, the customer cancels order-1004 before paying ===
  Ended at AwaitPayment (Ended): status Ended, strata.end_reason=requested, strata.ended_by=customer
  A late payment for it:    thread 'order-1004' was ended: an ended thread can't be resumed

=== Step 5: T+3h00, the wake driver runs ===
  Due now (ThreadQuery::due_by): order-1003
  Woke order-1003 (Timeout): completed, placed for $2400 / held for a fraud review / not reviewed within 2 h: escalated to the risk team

=== Step 6: T+4h30, the carrier is here: both pickup timers are due ===
  Due now: order-1001, order-1005
  The application wakes order-1005 through its own resume_streaming(LevelSignals::due_at(now)):
    chunk NodeStart Ship
    chunk NodeEnd   Ship
    -> completed, placed for $35 / packed, payment PAY-5533 / handed to the carrier
  The wake driver woke order-1001 (Timer): completed, placed for $80 / packed, payment PAY-5531 / handed to the carrier

=== Step 7: T+25h00, order-1002 was never paid ===
  Woke order-1002 (Timeout): cancelled (Ended) at AwaitPayment
  Threads with strata.end_reason=timed_out: order-1002

=== Done: what the file holds now ===
    checkpoint_threads (the head table the query port reads):
      order-1001  resumed      wake_at T+4h00 (2026-10-24T20:10:15Z)
      order-1002  ended        wake_at -
      order-1003  resumed      wake_at T+2h00 (2026-10-24T18:10:15Z)
      order-1004  ended        wake_at -
      order-1005  resumed      wake_at T+4h00 (2026-10-24T20:10:15Z)
  16 checkpoint rows in all. A completed run writes nothing, so a finished order's head is its last resume stamp.
  The stamp keeps the wake_at of the wait it answered; only an interrupted, unresumed thread is due.
```

The demo checks each claim as it goes (each wait's kind, event type and
`wake_at`, to within a minute of `T` plus its delay; the refusals; that a
refusal writes nothing; which threads are due and why each woke; the end
reasons; the 16 rows and the final head table) and exits non-zero if one
fails. The database is `demos/19-waits-and-timers/orders.db` (gitignored),
deleted at the start of each run.

## Files

- `src/main.rs` — the whole demo
- `Cargo.toml` — `datalium-strata` and `datalium-strata-memory` (for
  `SqliteCheckpointStore`) from crates.io, `chrono`
  for the times, and `rusqlite` to read the file with no Strata code (the
  crate the SQLite store itself uses, so nothing new in the dependency tree)
