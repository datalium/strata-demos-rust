# 08 — error-recovery

Error recovery (`design/04-Error-Recovery.md` and
`design/25-Retry-Backoff.md` in [`datalium/strata`](https://github.com/datalium/strata)): an order pipeline
(`ValidateOrder` -> `ChargePayment` -> `SendConfirmation`) whose
`ChargePayment` node talks to a flaky simulated payment gateway. The node is
*fallible*: it returns `Result<S, NodeError>`, and a `RecoveryPolicy` tells
the engine what to do when it errs. Wait and retry a transient failure, fall
back to another node once retries run out, or persist a `Failed` checkpoint
when there is nowhere to fall back to. A `Failed` checkpoint is not the end:
once the cause is fixed, the thread is resumed and re-enters the node that
failed.

Every attempt of `ChargePayment` first **reserves a receipt number** on the
state it is building and only then calls the gateway. That reservation is the
partial work the demo watches: a failed attempt must not leak it into the
committed state, the fallback node, or the persisted checkpoint.

The .NET twin is
[`strata-demos-dotnet/demos/08-error-recovery`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/08-error-recovery);
same story, same scenarios, same checks.

## What it demonstrates

- `.add_fallible_node(name, |s: &S| -> Result<S, NodeError>)`: the handler
  *borrows* the committed state and returns a new one, so a failed attempt
  can't have mutated what the engine keeps. Rollback is structural, not
  something the engine has to undo
- `NodeError::Transient` vs `NodeError::Fatal`: a timeout or a 503 is worth
  retrying; a declined card isn't
- **Backoff.** `RecoveryPolicy::retry(3).with_backoff(200 ms, 2 s, Jitter::None)`
  waits `initial × 2^(n−1)`, capped at `max`, before the n-th retry.
  Scenario 1's two timeouts wait 200 ms, then 400 ms. The default jitter is
  `Jitter::Full` (a random wait in `[0, d]`); the demo turns it off so the
  numbers are stable
- **A retry hint.** `NodeError::transient_after("gateway unavailable (503)", 30 s)`
  (the `TransientAfter` variant) carries a `Retry-After`. With a backoff set,
  the hint replaces the computed delay, capped at `max` and not jittered:
  scenario 2 waits 2 s, twice
- **The fallback knows why.** Once retries run out, the engine routes to
  `QueueForManualReview`, which runs on the state from *before*
  `ChargePayment` (`receipt_id` is `None`, `SendConfirmation` never ran). It
  is an `add_async_node`, because only an async handler gets a `NodeContext`,
  and its `ctx.last_failure()` is a `NodeFailure` with the failed node, the
  error (kind, message, `retry_after()`) and the attempts, which it writes into
  `review_note`. Only a fallback's context carries it
- **A fatal error skips retries and waits.** Scenario 3 makes one gateway
  call, schedules no retry, persists a `CheckpointStatus::Failed` checkpoint
  and returns `GraphExecutionResult::Failed(StrataError::NodeFailed { .. })`
- **A durable `Failed` checkpoint.** `SqliteCheckpointStore` writes it to a
  real file, and a second, independent store instance reads it back: the last
  committed state (after `ValidateOrder`), the failed node as both
  `suspended_node` and `next_node`, and `error` / `error_kind` / `attempts` in
  `metadata`. It is the shape the .NET runtime writes too
  (`schema/fixtures/checkpoint-lifecycle/failed.json` in `datalium/strata`)
- **Resuming a `Failed` checkpoint** (`design/16-Nested-Interrupts.md`,
  "Resuming a failed thread"). The customer updates their card, and
  `resume("order-1003", HumanSignal::approve("support@shop.example").with_override("card_token", ...), store)`
  re-enters `ChargePayment` with the committed state plus the new card.
  `ValidateOrder` does not run again, `SendConfirmation` does. A checkpoint is
  resumed once: a second resume is refused with
  `StrataError::ExecutionAborted("... already resumed")`
- **Compile-time checks.** `QueueForManualReview` has no ordinary edge into
  it, yet `.compile()` doesn't flag it as an orphan (STRATA-SPEC-004): the
  fallback route counts as a guarded edge. A policy naming a fallback that
  doesn't exist (a typo, in step 6) is rejected by `.compile()` with
  `StrataError::CompilationFailed`
- **A paused clock.** `main` runs on
  `#[tokio::main(flavor = "current_thread", start_paused = true)]`, so the
  engine's backoff sleeps auto-advance tokio's virtual clock and 4 s of
  waiting take no real time. The delays it prints come from the engine's
  `RunEventKind::RetryScheduled` events (a `RunEventChannel` passed with
  `RunConfig::with_observer`), not from timing anything; scenario 1 also
  checks that the virtual clock moved exactly the sum of those delays. This is
  the Rust stand-in for the .NET twin's `FakeTimeProvider`, and the same
  technique as `rust/strata-core/tests/retry_backoff.rs` in `datalium/strata`. It needs tokio's
  `test-util` feature (a feature flag, not a new crate)

## What it does *not* cover

- **Node timeouts** (`set_timeout`, a slow attempt becoming a transient
  failure) and cancelling a run: see
  [`15-async-and-cancellation`](../15-async-and-cancellation)
- **A crash mid-run.** A wait between retries lives in memory; if the process
  dies during it, the wait is lost with the rest of the run. Recovering a run
  whose process died is [`20-durable-execution`](../20-durable-execution)
- **Retries of a whole group or child graph**: a parallel group's policy
  re-runs every branch ([`10-parallel-fan-out`](../10-parallel-fan-out)), a
  subgraph's re-runs the whole child
  ([`13-subgraph-composition`](../13-subgraph-composition))
- **A panic is not a `NodeError`.** It unwinds through the run as it does
  from any node; report expected failures with `Err`
- **`last_failure()` is not persisted or in a run event.** If the fallback
  itself is interrupted, a resume re-enters it without `last_failure()`
- **No live service.** The payment gateway is a deterministic stand-in, so the
  demo runs offline

## Run it

```bash
cargo run -p error-recovery
```

The demo deletes `recovery-checkpoint.sqlite` (in this directory,
`.gitignore`d) at both the start and the end of `main`, so repeated runs never
trip over a stale checkpoint. It checks every claim it prints and exits
non-zero if one fails.

Output of a real run:

```
=== Scenario 1: transient blip -- retry(3), backoff 200 ms doubling to 2 s, fallback 'QueueForManualReview' ===
    [gateway] call 1 for ord-1001 ($129.99, tok_visa_4242): transient error: gateway timed out
    [gateway] call 2 for ord-1001 ($129.99, tok_visa_4242): transient error: gateway timed out
    [gateway] call 3 for ord-1001 ($129.99, tok_visa_4242): approved
  Completed after 3 gateway call(s). Waits before each retry: 200 ms, 400 ms
  The paused clock moved 600 ms; no real time was spent waiting.
  State: OrderState { order_id: "ord-1001", amount: 129.99, card_token: "tok_visa_4242", payment_status: "charged", receipt_id: Some("RCPT-ord-1001-3"), confirmed: true, review_note: None }
  Only the successful attempt's reservation was committed: Some("RCPT-ord-1001-3")
  Checkpoint for 'order-1001': none (a recovered failure leaves nothing behind)

=== Scenario 2: rate limited (Retry-After: 30s) -- retry(2), same backoff, fallback 'QueueForManualReview' ===
    [gateway] call 1 for ord-1002 ($54.00, tok_visa_4242): transient error: gateway unavailable (503)
    [gateway] call 2 for ord-1002 ($54.00, tok_visa_4242): transient error: gateway unavailable (503)
    [gateway] call 3 for ord-1002 ($54.00, tok_visa_4242): transient error: gateway unavailable (503)
  Retries exhausted after 3 gateway call(s). Waits: 2000 ms, 2000 ms
  The 30 s hint replaced the computed delay, capped at the policy's 2 s max.
  State: OrderState { order_id: "ord-1002", amount: 54.0, card_token: "tok_visa_4242", payment_status: "manual_review", receipt_id: None, confirmed: false, review_note: Some("ChargePayment failed (Transient) after 3 attempt(s): gateway unavailable (503); gateway asked to wait 30s") }
  receipt_id is None and 'SendConfirmation' never ran (confirmed: false): the fallback ran on the state from before ChargePayment.

=== Scenario 3: card declined -- retry(3), same backoff, no fallback ===
    [gateway] call 1 for ord-1003 ($899.00, tok_expired_0119): fatal error: card declined: expired
  Failed: node 'ChargePayment' gave up after 1 attempt(s) -- 'card declined: expired'. Retries scheduled: 0.
  A fatal error is never retried, so it never waits either.

=== Step 4: reading the Failed checkpoint back (store instance #2) ===
  status:         Failed
  failed node:    Some("ChargePayment")
  resume from:    Some("ChargePayment")
  frozen state:   {"amount":899.0,"card_token":"tok_expired_0119","confirmed":false,"order_id":"ord-1003","payment_status":"validated","receipt_id":null,"review_note":null}
  metadata.attempts: 1
  metadata.error: card declined: expired
  metadata.error_kind: fatal
  The frozen state is the last committed state (after ValidateOrder). receipt_id is null: the declined attempt's reservation never reached it.

=== Step 5: the customer updates their card; the Failed checkpoint is resumed (store instance #2) ===
    [gateway] call 1 for ord-1003 ($899.00, tok_visa_4242): approved
  State: OrderState { order_id: "ord-1003", amount: 899.0, card_token: "tok_visa_4242", payment_status: "charged", receipt_id: Some("RCPT-ord-1003-1"), confirmed: true, review_note: None }
  ChargePayment ran again (1 gateway call), ValidateOrder did not (0 runs), and SendConfirmation completed the order.
  Resuming the same checkpoint again is refused: checkpoint for thread 'order-1003' was already resumed

=== Step 6: a recovery policy is checked at .compile(), not at runtime ===
  Rejected: recovery policy for 'ChargePayment' falls back to 'QueueForManualReveiw', which does not exist

=== Done: retries backed off, a rate limit's hint set the wait, the fallback knew why, and a fatal failure was resumed once its cause was fixed. ===
```

Step 5 uses a new gateway (its call counter starts at 1) and a new graph, as
a separate process would.

## Files

- `src/main.rs` — the whole demo, single file
- `Cargo.toml` — package name is `error-recovery` (folder keeps the `08-`
  prefix; Cargo package names can't start with a digit). Depends on tokio with
  `test-util` for the paused clock
- `recovery-checkpoint.sqlite` — written and removed at runtime,
  `.gitignore`d
