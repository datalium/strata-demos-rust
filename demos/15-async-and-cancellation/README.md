# 15 — async-and-cancellation

Async execution, node context and cancellation (`design/17-Async-Execution.md`):
a vendor-payment run (`LoadInvoice` -> `Screen` -> `ScoreFraud` ->
`PostToLedger` -> `PayVendor`) whose handlers are mostly async. Each async
handler is an `Fn(S, NodeContext) -> impl Future`, and the run's
cancellation token reaches it through the context (`ctx.cancellation()`). The
demo shows what the context tells a handler, how a per-node timeout becomes a
retry, that the engine doesn't wait for a handler it has given up on, and that
a cancelled run is a resumable outcome, not an error. A sixth, Rust-only
scenario shows inline sync nodes (`design/39-Inline-Sync-Handlers.md`).

The .NET twin is
[`strata-demos-dotnet/demos/15-async-and-cancellation`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/15-async-and-cancellation):
the same story, five scenarios and checks. Where the engines differ, it says
so below.

## What it demonstrates

- **Async handlers and `NodeContext`** (scenario 1). `add_async_node`,
  `add_async_fallible_node` and `ParallelBranches::async_branch` each take an
  async handler. Its context carries the thread id, the node's own name
  (`node()`) and its qualified path (`node_path()`, `Screen/Sanctions` for a
  branch), the attempt, the run id and the run metadata. A sync handler
  (`add_fallible_node`'s `Fn(&S) -> Result<S, NodeError>`, here
  `PostToLedger`) gets no context and no token
- **Run-level inputs in `RunConfig`**, passed to `run_with` / `resume_with`:
  `with_metadata` (`tenant`), `with_run_id`, `with_service` (an `FxRates`
  that `LoadInvoice` reads with `ctx.service::<FxRates>()`),
  `with_cancellation` and `with_deadline`. Rust has no `IServiceProvider`:
  a service is keyed by its concrete type
- **`set_timeout(node, Duration)`** (scenario 2). The fraud API hangs on
  attempt 1. When its 2 s timeout elapses, the attempt's token is cancelled,
  the engine drops the handler's future, and the attempt fails as
  **transient**. `RecoveryPolicy::retry(1)` runs attempt 2 with a fresh
  timeout, and it succeeds. Without the retry, the run ends
  `Failed(StrataError::NodeFailed { .. })` with `node 'ScoreFraud' timed out
  after 2000 ms` and a `Failed` checkpoint whose `error_kind` is `transient`.
  In Rust, an async handler needn't watch its token to stop: dropping its
  future stops it. The stand-ins keep the token they were handed and note the
  drop, and the demo checks both
- **The engine never waits for a handler it gave up on** (scenario 3). The
  ledger SDK is synchronous, so the engine runs `PostToLedger` on tokio's
  blocking pool, and its first call blocks on a gate the demo holds shut. The
  timeout fires, the retry posts, and the run **completes while call 1 is
  still blocked** on its pool thread. Then the demo opens the gate. The
  abandoned call's return value is discarded, but its side effect still
  happens: the invoice is posted twice. Sync work is abandoned, not stopped.
  A handler whose side effects must not overlap a retry should be async and
  honour its token
- **Cancellation by token** (scenario 4). An operator cancels during the bank
  call. `run_with` *returns* `GraphExecutionResult::Cancelled` (node
  `PayVendor`, reason `CancelReason::Requested`, `stopped_at ["PayVendor"]`),
  not a `Failed`. The bank call's token is cancelled and its future dropped.
  `PayVendor`'s `retry(2)` never runs, because a cancellation is not a
  failure. The `Cancelled` checkpoint (schema `1.2.0`) holds the state
  `PayVendor` started with, plus `cancel_reason = requested` next to the
  run's `tenant`
- **Resuming a cancelled thread**. `resume_with` re-enters `PayVendor` only,
  at attempt 1. The invoice isn't loaded, screened or posted again. The resume
  passes no metadata, and the handler still sees `tenant = acme-eu`, because
  a resume carries the stored run metadata forward. A second resume of the
  same checkpoint is refused
  (`Failed(StrataError::ExecutionAborted("... was already resumed"))`)
- **Cancellation by deadline** (scenario 5). `RunConfig::with_deadline`
  passes during the bank call: `Cancelled` with reason `Deadline` and
  `cancel_reason = deadline`. A resume with a fresh deadline completes the run
- **A paused clock**. Rust has no `TimeProvider`: node timeouts and the
  deadline run on tokio's clock, which the demo pauses
  (`tokio::time::pause()`, on a current-thread runtime). It waits until the
  slow handler has started and then moves the clock with
  `tokio::time::advance`. Every timeout fires exactly when the demo means it
  to, and no scenario waits for real time to pass. The deadline is a
  `std::time::Instant`, so the demo takes it from tokio's clock
  (`(tokio::time::Instant::now() + 30 s).into_std()`). Two tokio details
  shape this, both commented in `src/main.rs`: tokio doesn't auto-advance a
  paused clock while a blocking-pool task runs (scenario 3), and its timer
  wheel rounds a deadline up to the next millisecond, so the demo moves the
  clock one tick past each timeout
- **Inline sync nodes** (scenario 6, Rust only: the .NET engine has no
  counterpart). `add_inline_node` runs a sync handler on the executor instead
  of the blocking pool: `PickRail` runs on the thread polling the run
  (`main`'s, on this current-thread runtime), while `PostToLedger`
  (`add_fallible_node`) runs on a pool thread. Nothing can abandon an inline
  node, so `.compile()` refuses `set_timeout` on one
  (`StrataError::CompilationFailed`), and a cancel that lands while one runs
  takes effect when it returns: its result is committed and the run stops
  `Cancelled` at the *next* node (`PostToLedger`). Compare scenario 4, where
  the node the cancel landed on was abandoned and the resume re-entered it.
  An inline handler must not block: it holds the runtime while it runs

## What it does *not* cover

- **Retry backoff and `NodeError::TransientAfter`**
  (`design/25-Retry-Backoff.md`): [`08-error-recovery`](../08-error-recovery).
  The retries here are immediate
- **Cancellation inside a subgraph or a parallel group**. A cancelled child
  nests its `cancelled` checkpoint in the parent's, and a subgraph node's
  timeout fails it transiently with the child's checkpoint nested (see
  `rust/strata-core/tests/cancellation.rs` in `datalium/strata`).
  [`13-subgraph-composition`](../13-subgraph-composition) covers subgraphs.
  Per-branch timeouts are `design/26-Parallel-Group-Controls-And-Joins.md`
- **Streaming**. A cancelled stream ends with no `NodeEnd` for the stopped
  node. [`06-token-streaming`](../06-token-streaming) covers streaming
- **Run events and telemetry**: [`27-run-observability`](../27-run-observability).
  The run id and metadata appear here only as `NodeContext` sees them
- **`RunConfig::with_history`**. The context's history store is left unset
- **A real service**. The fraud API, ledger SDK and bank are deterministic
  stand-ins. Nothing goes over a network

## Run it

```bash
cargo run -p async-and-cancellation
```

It takes well under a second once built. Nothing in it waits on real time
except the moment scenario 3's unblocked ledger call takes to return, so the
output is identical on every run:

```
=== Scenario 1: a healthy payment run -- async handlers and their NodeContext ===
  RunConfig: with_metadata(tenant = acme-eu), with_run_id(payrun-2026-10-24), with_service(FxRates)
  What each async handler was told:
    node path            node         attempt  thread    run id             tenant
    LoadInvoice          LoadInvoice  1        inv-1001  payrun-2026-10-24  acme-eu
    Screen/Sanctions     Sanctions    1        inv-1001  payrun-2026-10-24  acme-eu
    Screen/Duplicates    Duplicates   1        inv-1001  payrun-2026-10-24  acme-eu
    ScoreFraud           ScoreFraud   1        inv-1001  payrun-2026-10-24  acme-eu
    PayVendor            PayVendor    1        inv-1001  payrun-2026-10-24  acme-eu
  PostToLedger is a sync handler (Fn(&Invoice) -> Result<Invoice, NodeError>): it gets no context and no token.
  Completed: Globex GmbH: USD 1250.00 = EUR 1150.00, fraud score 12, ledger LED-1001, payment PAY-7001

=== Scenario 2: the fraud API hangs -- set_timeout(2 s) + retry(1) ===
  Attempt 1 started, then the demo moved the paused clock 2 s: its token was cancelled, its future dropped, and the attempt failed as transient.
  ScoreFraud attempts seen in NodeContext::attempt(): [1, 2] -- each retry gets a fresh timeout.
  Completed: Initech: EUR 480.00, fraud score 12, ledger LED-1001, payment PAY-7001
  The same hang with no retry:
    NodeFailed: 'ScoreFraud' after 1 attempt(s): node 'ScoreFraud' timed out after 2000 ms
    Checkpoint: Failed, error_kind = transient

=== Scenario 3: the legacy ledger SDK blocks -- a sync handler is abandoned, set_timeout(5 s) + retry(1) ===
  The run completed while ledger call 1 was still blocked: true
  Ledger calls: 2; entries when the run ended: ["LED-1002 inv-1003"]
  Completed: Umbrella Ltd: EUR 99.90, fraud score 12, ledger LED-1002, payment PAY-7001
  Now the SDK's first call returns. Its result is discarded, but its side effect is not:
    ledger entries: ["LED-1002 inv-1003", "LED-1001 inv-1003"] -- one invoice, posted twice.
  A handler whose side effects must not overlap a retry should be async and honour its token.

=== Scenario 4: an operator cancels during the bank call -- Cancelled, then resumed ===
  Cancelled at 'PayVendor', reason Requested, stopped at ["PayVendor"] (run_with returned it; it isn't a Failed).
  The bank client's token was cancelled and its future dropped. Bank calls: 1 -- PayVendor's retry(2) never ran: a cancellation isn't a failure.
  Checkpoint: Cancelled, schema 1.2.0, next node PayVendor, metadata { cancel_reason = requested, tenant = acme-eu }
    frozen state: ledger_ref = LED-1001, payment_ref = null (the state PayVendor started with)
  Resumed: only ["PayVendor attempt 1, tenant acme-eu"] ran.
  Invoice loads: 1, ledger calls: 1, bank calls: 2.
  Completed: Hooli: USD 3200.00 = EUR 2944.00, fraud score 12, ledger LED-1001, payment PAY-7002
  A second resume of the same checkpoint: checkpoint for thread 'inv-1004' was already resumed

=== Scenario 5: the run's deadline passes during the bank call -- RunConfig::with_deadline ===
  Cancelled at 'PayVendor', reason Deadline; checkpoint Cancelled, cancel_reason = deadline
  Resumed with a fresh deadline: Stark Industries: USD 7400.00 = EUR 6808.00, fraud score 12, ledger LED-1001, payment PAY-7002

=== Scenario 6 (Rust only): an inline sync node -- add_inline_node, design/39 ===
  PickRail (add_inline_node) ran on the thread polling the run: true; rail SEPA
  PostToLedger (add_fallible_node) ran on a blocking-pool thread: true
  set_timeout("PickRail", 1 s): compile refuses it: node 'PickRail' runs inline and can't be timed out: add it with add_node or add_fallible_node to have its timeout honoured
  A cancel that lands while PickRail runs: Cancelled at 'PostToLedger', reason Requested; checkpoint next node PostToLedger, frozen rail SWIFT
  PickRail ran 1 time(s); ledger calls: 0 -- its result was committed, and the run stopped before PostToLedger.

=== Done: async handlers saw their context, timeouts became retries, a blocked handler was abandoned, cancelled runs resumed where they stopped, and an inline node ran to its end under a cancel. ===
```

Every claim above is checked at runtime. If one stops holding, the demo
prints `check failed: ...` and exits non-zero. A watchdog thread fails it the
same way if a scenario ever hangs for a minute.

## Files

- `src/main.rs` — the whole demo: the six scenarios, the graphs (`build` for
  the payment run, `rails` for scenario 6), and the stand-in services. Money
  is in integer cents (std has no decimal type), so the FX conversions are
  exact
- `Cargo.toml` — package name is `async-and-cancellation` (Cargo package
  names can't start with a digit). Depends on `datalium-strata`,
  `datalium-strata-memory` (for `InMemoryCheckpointStore`), `serde`, and
  `tokio` with its `test-util` feature for `tokio::time::pause()` /
  `advance()`
