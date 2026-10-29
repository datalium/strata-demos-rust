# 05 — hitl-interruption

Human-in-the-loop interruption (STRATA-SPEC-002) with typed human signals
(`design/21-Typed-Human-Signals.md` in [`datalium/strata`](https://github.com/datalium/strata)). A
`PrepareWireTransfer` node drafts an outbound wire transfer, the graph
freezes at an interrupt boundary before anything irreversible happens, and
the human's answer decides what runs next: `ExecuteWireTransfer` on an
approval, `CancelWireTransfer` on a rejection. The checkpoints go to a real
SQLite file. The store instance that froze the runs is dropped, and a new,
independent one at the same file answers them, standing in for the process
after a restart.

The Rust twin of
[`strata-demos-dotnet/demos/05-hitl-interruption`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/05-hitl-interruption):
same story, same two transfers, same checks.

## What it demonstrates

- **Freeze/Flush/Evict.** `graph.run(...)` runs `PrepareWireTransfer`, saves
  the frozen state through `SqliteCheckpointStore` (`strata-memory`'s SQLite
  adapter, opened on a file with `SqliteCheckpointStore::open(path)`, not
  `open_in_memory()`) and returns `GraphExecutionResult::Interrupted`.
  Nothing keeps running while the transfer waits
- **The checkpoint survives a restart.** Store instance #1 is dropped (its
  connection closes) before store instance #2 is opened. The only thing they
  share is the SQLite file, so every later step proves the checkpoints
  reached disk
- **Finding what is waiting after the restart.** Store #2 lists threads with
  `list_threads(&ThreadQuery::default().with_states([ThreadState::Interrupted]))`,
  the `CheckpointQuery` port every `strata-memory` store implements
  (`design/19`). Both transfers show up, waiting at `PrepareWireTransfer`
- **Typed overrides.** `HumanSignal::approve("jdoe@acme.io").with_override("amount", 4150.25)`
  writes a JSON number into the frozen state, so it fits the `f64` field.
  The 0.3-era string map (`field_overrides`, now also
  `.with_field_override(key, value)`) writes every value as a JSON string.
  Step 3 shows what that does to a number: the resume is refused with
  `StrataError::ExecutionAborted` before the checkpoint is stamped
  (`resumed_at` stays `None`), nothing runs, and the thread can still be
  answered
- **The recorded decision.** The resume that answers the boundary records the
  signal as a `HumanDecision` (boundary, approved or not, reviewer, every
  override typed, `decided_at`). `ExecuteWireTransfer` is an
  `add_async_fallible_node` that reads it from `ctx.decision()` and takes
  `approved_by` from `decision.reviewer`, so the reviewer doesn't have to type
  their own name into the state. It fails with `NodeError::Fatal` if there is
  no decision, so a transfer can't run unanswered
- **The reject route.** `.add_interrupt_boundary_with_reject("PrepareWireTransfer", "CancelWireTransfer")`.
  `HumanSignal::reject("mlee@acme.io")` continues at `CancelWireTransfer`
  instead of the next node fixed at freeze time, so `ExecuteWireTransfer`
  never sees a rejected transfer. The rejection is recorded as a decision
  too, and its override (a `note`) still applies
- **Each answer is used once.** After both resumes, nothing is listed as
  waiting: an answered thread's latest checkpoint is stamped, so it is
  `ThreadState::Resumed`, not `Interrupted`

## What it does *not* cover

- **No separate OS processes.** The restart is simulated by dropping one
  store and opening another at the same file. That is the part that matters
  (no shared in-memory state). For a real two-process hand-off, between Rust
  and .NET, see [`09-cross-language-interop`](../09-cross-language-interop)
- **A boundary with a timeout** (`add_interrupt_boundary_with(name, BoundaryOptions::new().with_timeout(..).with_on_timeout(..))`):
  see [`19-waits-and-timers`](../19-waits-and-timers)
- **A boundary inside a subgraph**, and the decision staying with the level
  that answered it: see
  [`13-subgraph-composition`](../13-subgraph-composition), scenario 6
- **The decision on a later checkpoint.** Every checkpoint a level writes
  after an approval carries the decision, so a transfer that fails after
  approval and is resumed later still knows who approved it. Here nothing
  fails after the boundary, so no such checkpoint is written. Resuming a
  `Failed` checkpoint is in [`08-error-recovery`](../08-error-recovery)
- **Who may approve.** Strata records the reviewer's identity and doesn't
  judge it; authorization is the caller's job
- **No staleness check on resume** (for example, re-checking hours later that
  the transfer is still valid). Put that in the node after the boundary
- **No real bank.** `Bank` is an in-memory list, so the demo can count what
  was sent and runs offline
- **No decimal type.** Rust's standard library has none and this demo adds no
  dependency, so `amount` is an `f64` (the .NET twin uses `decimal`). The
  refusal in step 3 is the same: serde won't read the JSON string
  `"4150.25"` as an `f64`

## Run it

```bash
cargo run -p hitl-interruption
```

The checkpoint file is `hitl-checkpoint.sqlite` in this directory
(`.gitignore`d). It is deleted at the start and at the end of each run. The
demo checks every claim it prints and exits with code 1 if one fails.

Output of a real run (the SQLite path depends on your checkout):

```
=== Step 1: two transfers run to the interrupt boundary (store instance #1) ===
  wire-acme-corp: suspended at PrepareWireTransfer, next ExecuteWireTransfer
    frozen: WireTransferState { beneficiary: "Acme Corp", amount: 4200.5, currency: "USD", invoice_generated: true, status: "awaiting approval", approved_by: None, note: None }
  wire-globex: suspended at PrepareWireTransfer, next ExecuteWireTransfer
    frozen: WireTransferState { beneficiary: "Globex Ltd", amount: 9800.0, currency: "USD", invoice_generated: true, status: "awaiting approval", approved_by: None, note: None }
  Store instance #1 dropped. The checkpoints now exist only in the SQLite file.

=== Step 2: after the restart, store instance #2 lists the transfers waiting for a human ===
  SQLite file: E:\GitHub\Datalium\strata-demos-rust\demos\05-hitl-interruption\hitl-checkpoint.sqlite
  wire-acme-corp: Interrupted, waiting at PrepareWireTransfer
  wire-globex: Interrupted, waiting at PrepareWireTransfer

=== Step 3: a string override for an f64 field is refused before the checkpoint is touched ===
  Refused: failed to rehydrate state for thread 'wire-acme-corp': invalid type: string "4150.25", expected f64
  Not stamped, nothing ran: 'wire-acme-corp' can still be answered.

=== Step 4: jdoe approves wire-acme-corp, correcting the amount to the invoice total ===
  completed: WireTransferState { beneficiary: "Acme Corp", amount: 4150.25, currency: "USD", invoice_generated: true, status: "sent (ref 0001)", approved_by: Some("jdoe@acme.io"), note: None }
  ExecuteWireTransfer saw: approved by jdoe@acme.io at PrepareWireTransfer, overrides amount=4150.25
  approved_by came from ctx.decision().reviewer. The signal set only amount.

=== Step 5: mlee rejects wire-globex, so the run takes the reject route ===
  completed: WireTransferState { beneficiary: "Globex Ltd", amount: 9800.0, currency: "USD", invoice_generated: true, status: "cancelled by mlee@acme.io", approved_by: None, note: Some("beneficiary not on the vendor list") }
  CancelWireTransfer saw: rejected by mlee@acme.io at PrepareWireTransfer, overrides note="beneficiary not on the vendor list"
  Transfers sent in total: 1. ExecuteWireTransfer never ran for Globex.

=== Step 6: the waiting list is empty ===
  Threads waiting for a human: 0
```

The decision's `decided_at` timestamp is checked but not printed, since it
changes every run.

## Files

- `src/main.rs` — the whole demo: `WireTransferState`, the in-memory `Bank`,
  the graph and the six steps
- `Cargo.toml` — package name is `hitl-interruption` (folder keeps the `05-`
  prefix; Cargo package names can't start with a digit); depends on
  `datalium-strata`, `datalium-strata-memory`, `serde` and `tokio`
- `hitl-checkpoint.sqlite` — written and removed at runtime, `.gitignore`d
