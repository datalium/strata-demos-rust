# 20 — durable-execution

Durable execution (`design/33-Durable-Execution.md`, SG-10): a graph compiled
with `CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode)`
writes a `running` checkpoint before its first node and after every node that
commits, and `completed` when it finishes. If the process dies mid-run, the
thread is left `running`, and another process continues it from the last
checkpoint.

The crash here is real. The program starts a copy of itself
(`std::env::current_exe()`) in `worker` mode. The worker runs an order
(`Validate` -> `Charge` -> `Ship`) against a SQLite file, charges the card
inside `Charge`, prints a marker line and hangs. The parent kills the worker's
process when it reads the marker (`Child::kill`, which is `TerminateProcess` on
Windows), so `Charge` never commits. Then the parent opens the same file,
finds the dead thread and finishes it.

The .NET twin is
[`strata-demos-dotnet/demos/20-durable-execution`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/20-durable-execution).
Same story, same scenarios, same checks, and the same output apart from Rust
names (`is_recovery`, `recover_stale`, `Debug` formatting of the final state).

## What it demonstrates

- **`CheckpointPolicy::EveryNode`**: the dead worker left two `running`
  checkpoints, one before `Validate` and one after it (next node `Charge`). The
  default policy, `Boundaries`, writes nothing until a run stops at a boundary
  or ends, so the same crash would leave no trace of the thread at all
- **A real crash.** The process is killed inside a node: no `Drop`, no
  `failed` checkpoint, no lease release. The worker's lease (`worker-1`,
  token 1) is still in the file after it dies (`get_lease`), and the thread is
  listed under `ThreadState::Running` (`list_threads` with
  `ThreadQuery::default().with_states(..)`)
- **A `running` thread refuses an ordinary resume**:
  `resume` -> `Failed(ExecutionAborted("thread 'order-1042' is running: only a waiting, failed or cancelled thread can be resumed"))`,
  with nothing written. Only a recovery continues a running thread
- **`recover_stale(store, now, RecoverOptions)`, the recovery driver.** While
  the dead worker's lease is live, the driver skips the thread
  (`it is held by 'worker-1' until ...`): a lease is the only sign of life,
  and the worker might just be slow. Once the lease has expired the driver
  recovers the thread under its own lease (`worker-2`, token 2). With a lease
  on `RecoverOptions::with_config`'s `RunConfig`, `with_stale_after` defaults
  to zero
- **The claim.** The recovery first appends the crashed checkpoint again,
  conditional on the thread not having changed, with `strata.recoveries`
  (`RECOVERIES_KEY`) = 1. `recoveries_of(&checkpoint)` reads it: `0, 0, 1, 0, 0`
  over the whole log, because the next node to commit starts the count again
- **At least once.** `Charge`, the node in flight, runs again. `Validate`, which
  had committed, doesn't
- **`NodeContext::idempotency_key()`** is `order-1042/Charge#2` in both
  processes, because the hop count is in the checkpoint. The payment gateway (a
  file-backed stand-in that deduplicates by key, as real payment APIs do) sees
  two requests and makes one charge. `Ship` gets a different key
- **`NodeContext::is_recovery()`** is true only for the re-run of `Charge`, and
  false again for `Ship`
- **The recovery's run events** (through a `RunEventChannel` on the recovery's
  `RunConfig::with_observer`): `RunStarted` (mode `RunMode::Recover`),
  `LeaseAcquired`, the claim's `CheckpointSaved`, `Recovered` (at `[Charge]`,
  recoveries 1), then the nodes, a `CheckpointSaved` per node, and the
  `completed` one
- A completed thread refuses another recovery (`recover` ->
  `is not running: its latest checkpoint is completed`)

The demo checks each of these at runtime and exits non-zero if one fails.

**Where the engines differ.** There is no `FakeTimeProvider` in Rust. Each
process opens the store with `SqliteCheckpointStore::open(..).with_clock(..)`,
whose `Clock` is a closure over a time the demo moves by hand, both starting
at 2026-10-24 12:00:00 UTC; lease expiry is judged by that clock.
`recover_stale` takes the caller's `now` explicitly. A run's lease renewals run
on tokio's clock (every ttl / 3, here a real 10 s), and the worker is killed
long before its first one. The worker coordinates with the parent through a
marker line on its standard output, written after the gateway's and the
journal's appends have reached their files.

## What it does *not* cover

- **Leases in depth**: refusal of a second worker, renewal, and a stale worker
  fenced after a takeover are in [`21-thread-leases`](../21-thread-leases)
- **A crash inside a subgraph, a parallel group or a map.** A subgraph's child
  is re-entered at its node in flight. A group or a map is one checkpoint, so
  every branch or item runs again with `is_recovery()`. See
  `rust/strata-core/tests/durable_execution.rs` in `datalium/strata`
- **A crash right after an approval.** Under `EveryNode` a resume's claim is a
  `running` checkpoint that holds the decision, so a recovery continues with it
- **`RecoverOptions::with_max_recoveries`** for a node that kills its process
  every time, `with_stale_after` without leases, and `end` on a running thread
- **Hosting the driver on a timer**: `datalium-strata-host`'s `Host::recover`
  (`design/35-Hosting-Integration.md`); for the host, see [`28-hosting`](../28-hosting)
- **Exactly once.** The engine gives a stable key. Making an effect idempotent
  is the node's job (here, the gateway's)

## Run it

```bash
cargo run -p durable-execution
```

It takes about a second: the parent starts one child process (the same
binary). Lines starting with `|` are node output: the worker's comes through
the parent, which relays the child's standard output. The only
machine-specific line is the `Files left in` path (the system temp directory).
Output of a real run:

```
=== Step 1: a worker process runs the order and is killed inside Charge ===
  | worker-1: Validate (key order-1042/Validate#1, is_recovery false)
  | worker-1: Charge (key order-1042/Charge#2, is_recovery false)
  | worker-1: Charge: the gateway charged ch_001 ($129.00) under key order-1042/Charge#2
  | worker-1: Charge: hangs before it can return (the parent kills the process now)
  Killed the worker's process while Charge was still running: the node never committed.

=== Step 2: what the dead worker left in checkpoints.sqlite ===
    #1  running    after -         next Validate  recoveries 0
    #2  running    after Validate  next Charge    recoveries 0
  Threads in state Running: order-1042
  Lease: held by 'worker-1' until 2026-10-24T12:00:30Z (token 1): nobody released it.
  Payment gateway: 1 request, 1 charge.

=== Step 3: a running thread refuses an ordinary resume ===
  resume -> Failed: thread 'order-1042' is running: only a waiting, failed or cancelled thread can be resumed
  Nothing was written: the log still has 2 checkpoints. Only a recovery continues it.

=== Step 4: the recovery driver leaves a thread whose lease is live ===
  recover_stale at 2026-10-24T12:00:00Z: recovered 0, skipped:
    order-1042: it is held by 'worker-1' until 2026-10-24T12:00:30Z
  A live lease is the only sign of life: until it runs out, the worker might just be slow.

=== Step 5: the lease runs out, and the driver recovers the thread ===
  Clock advanced to 2026-10-24T12:00:31Z.
  | worker-2: Charge (key order-1042/Charge#2, is_recovery true)
  | worker-2: Charge: the gateway already had key order-1042/Charge#2: returned ch_001, charged nothing
  | worker-2: Ship (key order-1042/Ship#3, is_recovery false)
  recover_stale: recovered 'order-1042' at [Charge], recoveries 1, result Completed.
  The recovery run's events:
    RunStarted       mode Recover, at Charge
    LeaseAcquired    'worker-2', token 2, until 2026-10-24T12:01:01Z
    CheckpointSaved  running at [Validate]
    Recovered        at [Charge], recoveries 1
    NodeStarted      Charge
    NodeCompleted    Charge: Committed
    CheckpointSaved  running at [Charge]
    NodeStarted      Ship
    NodeCompleted    Ship: Committed
    CheckpointSaved  completed at [Ship]
    RunCompleted     Completed

=== Step 6: at least once, and charged once ===
  Every node run, from both processes (visits.log):
    worker-1  Validate  key order-1042/Validate#1  is_recovery false
    worker-1  Charge    key order-1042/Charge#2    is_recovery false
    worker-2  Charge    key order-1042/Charge#2    is_recovery true
    worker-2  Ship      key order-1042/Ship#3      is_recovery false
  Payment gateway: 2 requests, 1 charge. The re-run sent the same key and got ch_001 back.
  Final state: Order { order_id: "order-1042", amount_cents: 12900, validated: true, charge_id: Some("ch_001"), shipped: true }
  The whole checkpoint log, with recoveries_of(checkpoint):
    #1  running    after -         next Validate  recoveries 0
    #2  running    after Validate  next Charge    recoveries 0
    #3  running    after Validate  next Charge    recoveries 1
    #4  running    after Charge    next Ship      recoveries 0
    #5  completed  after Ship      next -         recoveries 0
  Only the claim counts the recovery: the next node to commit starts the count again.
  Lease: none (the recovery released it). Threads in state Completed: order-1042.
  recover again -> Failed: thread 'order-1042' is not running: its latest checkpoint is completed

Files left in %TEMP%\strata-demo-20-durable-execution
=== Done: the process died mid-node, and the thread finished without charging twice. ===
```

Checkpoint #3 is the claim: the crashed checkpoint again (after `Validate`,
next `Charge`), which is why its `CheckpointSaved` says `running at [Validate]`.
The folder is cleared at the start of every run and left behind afterwards,
so you can look at `checkpoints.sqlite`, `payments.log` and `visits.log`.

## Files

- `src/main.rs`: the whole demo. With no arguments it is the parent; with
  `worker <dir>` it is the child the parent starts and kills
- `Cargo.toml`: `datalium-strata`, `datalium-strata-memory` (for
  `SqliteCheckpointStore` and its `Clock`), `tokio`, `chrono`, `serde`
