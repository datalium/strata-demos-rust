# 17 — parallel-joins

Parallel group controls and join policies
(`design/26-Parallel-Group-Controls-And-Joins.md`). Demo
[10](../10-parallel-fan-out) runs every branch of a group to the end and
merges them all. This one is about the options that change *when a group is
done* and *what its reducer sees*, on a shipping desk that asks three carriers
(swiftship, parcelpro, budgetpost) to quote for a parcel: a race for the first
quote, the first quote that passes a test, two quotes then the cheaper, a
failover chain, and a tracking board that keeps whatever answered in time. Who
won, and why the others stopped, is read from the run's events.

The .NET twin is
[`strata-demos-dotnet/demos/17-parallel-joins`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/17-parallel-joins):
same story, scenarios and checks. Where the two engines differ, it says so
below.

## What it demonstrates

- **`ParallelGroupOptions<S>`** on `.add_parallel_group_with(name, branches,
  reducer, options)` and **`.add_parallel_group_with_outcomes(...)`**, whose
  reducer gets a `BranchOutcomes<S>`: every branch's `BranchOutcome` (`name`,
  `status` = `Succeeded` / `Failed` / `TimedOut` / `Cancelled`, `state`,
  `error`) in declaration order, plus `join_met()`, `deciding()` and
  `succeeded()`. The options are a builder (`ParallelGroupOptions::new()
  .with_join(..).with_max_concurrency(..).with_branch_timeout(..)
  .keep_succeeded()`) where .NET sets init properties
- **`Join::first_success()`** (scenario 1): the first quote wins, the group
  reduces at once, and the branches still running are cancelled. Their
  `BranchCompleted` says `Cancelled` with **`reason: Some(LostRace)`**, and
  their tokens (`ctx.cancellation()`) really fire. A **`RunEventKind::JoinDecided
  { policy, met, deciding }`** event sits between the winner's
  `BranchCompleted` and the losers'
- **A failure doesn't end a race that can still be won** (scenario 2): a
  carrier's 503 is reported as `Failed`, and the race goes on
- **`with_max_concurrency`** (scenarios 3 and 7): branches beyond the limit
  start in declaration order as slots free. With `with_max_concurrency(1)` and
  `first_success`, a group is a **failover chain**: the backup carrier is only
  asked once the preferred one failed, and a branch that never started gets
  no events and is reported to the reducer as `Cancelled`
- **`Join::first_matching(predicate).described("...")`** (scenario 4): the
  first *matching* result wins, not the first result. budgetpost answers first
  and `Succeeded`, but only `JoinDecided` shows it wasn't the winner. With no
  match, **`.keep_succeeded()`** (`PartialResults::KeepSucceeded`) reduces over
  every answer with `join_met() == false` (the "first good answer, else the
  best of all" pattern), and the default `PartialResults::FailGroup` fails the
  group with `no branch matched the join (3 succeeded)`, transiently. The
  description is only exported (the definition's `condition`); the engine
  never reads it
- **`Join::quorum(n)`** (scenario 5): the plain reducer is handed the deciding
  states in **declaration order**, while `JoinDecided`'s `deciding` keeps
  **completion order**
- **Transient or fatal: can a retry still meet the join?** (scenario 6). A
  group that can't meet its join fails transiently only if `branches - fatal
  failures >= needed`. Two fatal refusals under `quorum(2)` of 3 make it fatal,
  so `RecoveryPolicy::retry(2)` never runs (`StrataError::NodeFailed {
  attempts: 1, .. }`, `error_kind` `fatal` on the `Failed` checkpoint). One
  fatal refusal plus one rate limit makes it transient (one `RetryScheduled`
  event), and the retry meets the quorum. The message names every failure in
  declaration order, and the retry re-runs **every** branch, the one that had
  succeeded included: there is no per-branch retry
- **`All` + `keep_succeeded` + `with_branch_timeout` + `with_max_concurrency`**
  (scenario 7), on **tokio's paused clock**: five tracking calls, two at a
  time, 45 s each. A hung carrier is `TimedOut` (a `NodeError::Transient`,
  `BranchCompleted` with `result: Failed, reason: Some(TimedOut)`), another
  fails fatally, and the reducer still gets the three that answered. A queued
  branch's timeout starts when *it* starts: budgetpost is asked at 30 s on the
  paused clock and answers at 60 s, and isn't timed out. An `All` group emits
  no `JoinDecided`
- **What `.compile()` refuses** (step 8): `Join::quorum(4)` on three branches
  is `StrataError::GraphInvalid` with `STRATA_ERR_INVALID_QUORUM`, and
  `with_max_concurrency(0)` is `StrataError::CompilationFailed`

Details worth knowing, all visible in the code:

- The peak in flight is replayed from `BranchStarted` / `BranchCompleted`
  events, not counted inside the branches: a group stops waiting for a branch
  the moment its token fires (a timeout or a lost race) and starts the next
  one. A map's items wind down instead (demo [18](../18-map-fan-out))
- Every race has a fixed order without timing: a carrier answers at once,
  never (it waits for its token), once the run's events show a named sibling
  settled in the same attempt (`RunEvent::attempt` against `ctx.attempt()`),
  or after a sleep on the paused clock. The output is identical on every run
- The run's events come from a small `RunObserver` of the demo's own
  (`Timeline`) that keeps every event and lets a branch wait for one. Events
  are delivered on a tokio task, off the executor, so the demo waits for
  `RunCompleted` after `run_with` returns

### Where the Rust engine differs

- **No fake clock.** The .NET demo passes a `FakeTimeProvider` in
  `RunOptions.TimeProvider` and advances it half a second at a time. Rust's
  branch timeouts run on tokio's clock, so the whole demo runs on a
  current-thread runtime started paused (`#[tokio::main(flavor =
  "current_thread", start_paused = true)]`, which needs tokio's
  **`test-util`** feature). Time jumps straight to the next timer when every
  task is waiting: 75 s of virtual time cost nothing, and the demo can check
  budgetpost's asked and answered times exactly
- **A loser is aborted, not unwound.** When a Rust group gives up on an async
  branch it cancels the branch's token and aborts its task, which drops the
  branch's future: no more of the branch's code runs. The demo sees this with
  a drop guard that notes the token had fired by the time the future was
  dropped. The .NET branch instead observes an `OperationCanceledException`
  and can still be unwinding when the group starts the next one. A sync
  branch (`.branch(...)`, on the blocking pool) can't be stopped at all: it
  runs on and its result is discarded
- **`with_max_concurrency(0)`** is refused by `.compile()`; .NET refuses it in
  the options setter (`ArgumentOutOfRangeException`)
- Names follow Rust: `join_met`, `first_success`, `PartialResults::FailGroup`.
  Money is in euro cents (`u32`) rather than a decimal type

## What it does *not* cover

- **The plain fan-out** (every branch, merged in declaration order, group-level
  retry and fallback): demo [10](../10-parallel-fan-out)
- **`with_cancel_siblings_on_failure(false)`** (under `FailGroup`, let every
  branch finish before the group fails, naming every failure), and the group's
  own `.set_timeout(...)`, which covers the whole group and discards every
  result
- **A map**, a fan-out over a list read from the state at run time: demo
  [18](../18-map-fan-out)
- **The exported `join` object** (`schema_version` `1.2.0`) and `strata graph
  validate`: see demo [14](../14-graph-ci-gate) for the CI gate
- **Streaming branches** (`.streaming_branch(...)`,
  `.async_streaming_branch(...)`)
- **No live carrier.** The carriers are canned answers, so the demo runs
  offline with no keys

## Run it

```bash
cargo run -p parallel-joins
```

Takes well under a second once built: scenario 7's 75 s pass on the paused
clock. Output (identical on every run):

```
=== Scenario 1: first_success -- three carriers race, the first quote wins ===
  Events:
    completed Quote/parcelpro Succeeded
    decided   first_success, met, deciding [parcelpro]
    completed Quote/swiftship Cancelled LostRace
    completed Quote/budgetpost Cancelled LostRace
  Reducer saw (declaration order): swiftship Cancelled, parcelpro Succeeded, budgetpost Cancelled; join_met: true
  Chosen: parcelpro EUR 14.20, 3 day(s) (parcelpro decided the join)
  swiftship and budgetpost both had their cancellation token fire before the group dropped their futures.

=== Scenario 2: swiftship errors first -- the race goes on ===
  Events:
    completed Quote/swiftship Failed -- 503 from the rating API
    completed Quote/parcelpro Succeeded
    decided   first_success, met, deciding [parcelpro]
    completed Quote/budgetpost Cancelled LostRace
  Chosen: parcelpro EUR 14.20, 3 day(s)

=== Scenario 3: max_concurrency(1) + first_success -- ask in order of preference, stop at the first answer ===
  Events:
    started   Quote/swiftship
    completed Quote/swiftship Failed -- 503 from the rating API
    started   Quote/parcelpro
    completed Quote/parcelpro Succeeded
    decided   first_success, met, deciding [parcelpro]
  Reducer saw (declaration order): swiftship Failed, parcelpro Succeeded, budgetpost Cancelled; join_met: true
  Carriers asked: swiftship, parcelpro (peak in flight: 1). budgetpost was never called and has no events.

=== Scenario 4a: first_matching("delivered within 2 days") -- the first quote isn't good enough ===
  Events:
    completed Quote/budgetpost Succeeded
    completed Quote/swiftship Succeeded
    decided   first_matching, met, deciding [swiftship]
    completed Quote/parcelpro Cancelled LostRace
  Chosen: swiftship EUR 24.50, 1 day(s). budgetpost answered first and Succeeded, but only JoinDecided says it didn't win.

=== Scenario 4b: no quote matches -- keep_succeeded reduces over every answer ===
  decided   first_matching, met: no, deciding []
  Reducer saw (declaration order): swiftship Succeeded, parcelpro Succeeded, budgetpost Succeeded; join_met: false
  Chosen: swiftship EUR 19.80, 3 day(s) (no quote met the join: the fastest that answered)

=== Scenario 4c: the same miss under the default PartialResults::FailGroup ===
  Failed: no branch matched the join (3 succeeded) -- transient (a retry asks again, and quotes change)

=== Scenario 5: quorum(2) -- wait for two quotes, take the cheaper ===
  Events:
    completed Quote/budgetpost Succeeded
    completed Quote/parcelpro Succeeded
    decided   quorum, met, deciding [budgetpost, parcelpro]
    completed Quote/swiftship Cancelled LostRace
  The plain reducer was handed: [parcelpro, budgetpost] -- declaration order, not completion order.
  Chosen: budgetpost EUR 6.90, 5 day(s)

=== Scenario 6a: quorum(2), two carriers refuse for good -- RecoveryPolicy::retry(2) can't help ===
  Failed after 1 attempt(s), fatal:
    quorum of 2 not reached: 1 of 3 branches succeeded; branch 'swiftship': parcels over 2 kg not accepted; branch 'budgetpost': no service to postcode 10115
  3 branches - 2 fatal failures = 1, fewer than the 2 needed, so no retry was made.

=== Scenario 6b: one refuses for good, one is rate limited once -- RecoveryPolicy::retry(2) meets the quorum ===
  Attempt 1 failed transiently (3 branches - 1 fatal = 2, enough for the quorum):
    quorum of 2 not reached: 1 of 3 branches succeeded; branch 'swiftship': parcels over 2 kg not accepted; branch 'budgetpost': rate limited
  Attempt 2 met the quorum, deciding [parcelpro, budgetpost].
  Calls -- swiftship: 2, parcelpro: 2, budgetpost: 2. The retry re-ran parcelpro too: there is no per-branch retry.

=== Scenario 7: tracking five parcels -- 2 at a time, 45 s each, keep what answered ===
  Started, in declaration order: swiftship, parcelpro, budgetpost, northline, coastal; peak in flight: 2.
  Tracked (order 4471):
    - parcelpro: out for delivery
    - budgetpost: in transit, Leipzig hub
    - coastal: delivered
  Gaps:
    - swiftship: TimedOut -- branch 'swiftship' timed out after 45000 ms
    - northline: Failed -- unknown tracking number NL-77Q
  join_met: false (an All join with failures), and no JoinDecided event: an All group never emits one.
  budgetpost was asked at 30 s and answered at 60 s on the paused clock: its 45 s timeout starts when it starts, so it wasn't charged for the wait.

=== Step 8: a quorum the group can't reach is refused at .compile() ===
  STRATA_ERR_INVALID_QUORUM at 'Quote': Parallel group 'Quote' has a quorum of 4, which isn't between 1 and its 3 branches.
  with_max_concurrency(0) is refused by .compile() too: parallel group 'Quote' has a max_concurrency of 0; it must be at least 1

=== Done: every join decided as it should, and the events said who won and why the rest stopped. ===
```

The demo checks each claim it prints (event order, statuses, `deciding`,
messages, error kinds, call counts, peak in flight, the paused-clock times)
and exits non-zero if one fails.

## Files

- `src/main.rs` — the whole demo: the scenarios in `main`, a `Desk` that
  builds each scenario's carriers (`Carriers::ask`) and runs it, and a
  `Timeline` run observer that records events and lets a branch wait for one
- `Cargo.toml` — package name is `parallel-joins` (Cargo package names can't
  start with a digit). Depends on `datalium-strata` and
  `datalium-strata-memory` from crates.io, `serde`,
  and `tokio` with `test-util` for the paused clock
