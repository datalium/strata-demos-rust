# 09 — cross-language-interop

Freeze a graph in one runtime, resume it in the other. The wire-transfer graph
from [`05-hitl-interruption`](../05-hitl-interruption), split across two
**processes** and two **runtimes** through a single SQLite file: the .NET
runtime freezes at the interrupt boundary and exits; the Rust runtime opens
the file in a fresh process and resumes — or the other way around.

Strata's wire formats carry a cross-language constraint ([`schema/README.md`](https://github.com/datalium/strata/blob/main/schema/README.md)
in `datalium/strata`): the Rust
`strata` CLI and runtime must be able to read checkpoints the C# runtime
produces, and vice versa. Until this demo that was only proven at *fixture*
level — both runtimes' unit tests agreed on a JSON shape — and no checkpoint
had ever crossed a real file between them. (It couldn't have: the two SQLite
stores wrote incompatible tables. See "What it took" below.)

This is the Rust half. The .NET twin is
[`strata-demos-dotnet/demos/09-cross-language-interop`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/09-cross-language-interop);
the two define the same graph, thread id, and state shape **independently**,
and the only thing they share is the database file.

> **Status with Strata 0.9.** Both halves run against Strata 0.9
> (`0.9.0-preview.1`). All four directions were run on 2026-10-24, against a
> pre-release build of `datalium/strata`: Rust →
> Rust, .NET → Rust, Rust → .NET, .NET → .NET, each followed by a second
> resume from *both* runtimes (refused every time), plus the `strata` CLI on
> a Rust-frozen, .NET-resumed file. Outputs below are from those runs. Rust →
> Rust and the CLI's `thread list` were re-run on 2026-10-29 against the
> published crates and the release CLI, with the same output.

## What it demonstrates

- A checkpoint written by `EfCoreCheckpointStore` (.NET) is read and resumed by
  `SqliteCheckpointStore` (Rust), and one written by Rust is resumed by .NET —
  through a real file, across process boundaries, no shared code
- Each node stamps the state with the runtime that ran it, so the completed
  state shows the hand-off: `frozen_by = "dotnet", executed_by = "rust"` (or
  the reverse)
- The checkpoint history is **shared**, too: the resume marker one runtime
  writes is what the other runtime sees, so resuming a thread a second time —
  from either runtime — is refused ("already resumed")
- The `strata` CLI reads the same file (see below): `thread list`,
  `thread inspect`, `checkpoint log`, `checkpoint replay` work on a database
  either runtime produced
- Two things the two runtimes must agree on **by hand**, since the state is
  user-defined JSON inside the checkpoint:
  - **Field names.** The state is `snake_case` on the wire; the .NET record pins
    that with `[JsonPropertyName]`. Without it, `System.Text.Json` writes
    `InvoiceGenerated` and `serde` can't find its fields
  - **Override keys.** A `HumanSignal` field override is merged into the frozen
    state's *JSON*, so it's keyed by the JSON name (`approved_by`) — not by the
    C# property name (`ApprovedBy`) that the .NET demo 05 uses when it stays
    within one runtime

Each mode checks what it claims and exits 1 if a check fails: `freeze` that
the run stopped at `PrepareWireTransfer` with `ExecuteWireTransfer` next, at a
plain human boundary (`waiting_at == ["PrepareWireTransfer"]`, no `wait`), with
the state stamped `frozen_by = "rust"`; `resume` that it completed, that the
`approved_by` override reached the state, that `executed_by` is `rust`, that
`frozen_by` carries *some* runtime's stamp, and that amount and currency
survived the trip.

## What it took (engine work in Strata)

Both SQLite stores were rewritten onto one shared layout. Before, they wrote
three mutually incompatible things:

| Writer | Table | Shape |
|---|---|---|
| Rust `SqliteCheckpointStore` | `checkpoints(thread_id, checkpoint_json)` | one row per thread, snake_case JSON |
| .NET `EfCoreCheckpointStore` | `Checkpoints(ThreadId, CheckpointJson)` | one row per thread, **PascalCase JSON, numeric `Status`** — not readable by `serde` |
| `strata` CLI | `checkpoints(thread_id, sequence, status, …)` | append-only log, one column per field |

SQLite table names are case-insensitive, so the Rust store and the CLI even
collided on the *same* table with different columns. All three now use the
CLI's append-only log (plus a `schema_version` column): `save` appends the next
`sequence`, `load` returns the latest. Each language's tests compare the created
table and a raw row against shared fixtures in `schema/fixtures/checkpoint-log/` in `datalium/strata`.

Since then the shared layout has grown, in both runtimes at once: columns for
nested, decision, wait and identity data, a `checkpoint_threads` head table kept
by a trigger (what `thread list` reads), `checkpoint_leases`, `checkpoint_blobs`,
and a `strata_schema_version` table (`checkpoints` is at version 2). A store
brings an older file up to date when it opens it, and refuses one a newer build
wrote (`design/34-Storage-Hardening.md`).

What the two 0.9 runtimes actually wrote, compared after these runs: the DDL
in `sqlite_master` (tables, indexes, trigger) is **identical** whichever runtime
created the file; both record `strata_schema_version` `checkpoints = 2`; both
write the frozen row as checkpoint `schema_version` `1.0.0` (an interrupt, no
nesting, no wait, no identity), `status` `interrupted`, with the same
snake_case state. The one difference is cosmetic: Rust writes the state's keys
in alphabetical order (`{"amount":…,"approved_by":…}`) and .NET in declaration
order (`{"invoice_generated":…,"amount":…}`). JSON object order carries no
meaning and neither side depends on it.

## What it does *not* cover

- **Not automated.** It's a manual, two-command demo, not a test — a live
  cross-runtime test would need both toolchains in one CI job. `datalium/strata`
  pins the layout with shared-fixture tests in each language instead
- **No lease.** One process at a time; two racing on one thread would collide
  on the primary key rather than corrupt the log. Thread leases
  (`RunConfig::with_lease`, `design/32-Thread-Leases.md`) fence a thread
  between processes and work on this SQLite layout; see
  [`21-thread-leases`](../21-thread-leases). Not used here, so the checkpoint
  stays the plain `1.0.0` row both twins have always exchanged
- **No graph definition sharing.** Each side builds the graph in code; only the
  *checkpoint* crosses. Nothing loads a runnable graph from a definition file.
  Graph identity (`CompileOptions::with_graph_id` / `with_graph_version`,
  `design/31-Graph-Identity.md`) can stamp checkpoints so a resume by a
  different graph is refused; see [`22-graph-identity`](../22-graph-identity).
  Not used here, for the same reason
- **No checkpoint per node.** The graph checkpoints only at its boundary
  (`CheckpointPolicy::Boundaries`, the default), so after the resume the
  thread's latest row is the `interrupted` one with `resumed_at` set: the CLI
  shows it as `resumed`, not `completed`. A graph compiled with
  `CheckpointPolicy::EveryNode` writes `running` and `completed` rows; see
  [`20-durable-execution`](../20-durable-execution)
- **Postgres and Redis.** Only the SQLite file is exchanged here. The same
  freeze / drop-the-connection / resume against SQLite, Postgres and Redis,
  within one runtime, is [`12-pluggable-storage`](../12-pluggable-storage)
- **No live service or LLM** — same wire-transfer nodes as demo 05

## Run it

Both runtimes default to the same file, `<temp dir>/strata-demo-09-interop/checkpoints.sqlite`
(Rust: `std::env::temp_dir()`; .NET: `Path.GetTempPath()`; both resolve to
`%TEMP%`), so no argument is needed.
`freeze` deletes and recreates it; both modes take an optional path as a second
argument. A bad or missing mode prints usage and exits 2.

**Rust freezes, Rust resumes**

```bash
# from strata-demos-rust
cargo run -p cross-language-interop -- freeze
cargo run -p cross-language-interop -- resume
cargo run -p cross-language-interop -- resume   # refused, exit 1
```

```
=== [rust] freeze: run to the interrupt boundary ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
  Interrupted at 'PrepareWireTransfer' (thread 'wire-transfer-xlang'); next node on resume: 'ExecuteWireTransfer'
  Frozen state: WireTransferState { invoice_generated: true, amount: 4200.5, currency: "USD", approved_by: None, is_completed: false, frozen_by: Some("rust"), executed_by: None }

  This process is about to exit; the checkpoint now exists only in the file.
  Next: run the `resume` step -- from the other runtime to see the hand-off.

=== [rust] resume: pick up a checkpoint another process froze ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
  Resumed and ran to completion.
  Completed state: WireTransferState { invoice_generated: true, amount: 4200.5, currency: "USD", approved_by: Some("compliance_officer_07"), is_completed: true, frozen_by: Some("rust"), executed_by: Some("rust") }

  frozen_by = "rust", executed_by = "rust"

=== [rust] resume: pick up a checkpoint another process froze ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
error: resume failed: checkpoint for thread 'wire-transfer-xlang' was already resumed (was this thread already resumed? run `freeze` again)
```

**.NET freezes, Rust resumes** — then a second resume from each runtime

```bash
# from strata-demos-dotnet
dotnet run --project demos/09-cross-language-interop/09-cross-language-interop.csproj -- freeze
# from strata-demos-rust
cargo run -p cross-language-interop -- resume
# both refused, exit 1:
dotnet run --project demos/09-cross-language-interop/09-cross-language-interop.csproj -- resume
cargo run -p cross-language-interop -- resume
```

```
=== [dotnet] freeze: run to the interrupt boundary ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
  Interrupted at 'PrepareWireTransfer' (thread 'wire-transfer-xlang'); next node on resume: 'ExecuteWireTransfer'
  Frozen state: WireTransferState { InvoiceGenerated = True, Amount = 4200.5, Currency = USD, ApprovedBy = , IsCompleted = False, FrozenBy = dotnet, ExecutedBy =  }

  This process is about to exit; the checkpoint now exists only in the file.
  Next: run the `resume` step -- from the other runtime to see the hand-off.

=== [rust] resume: pick up a checkpoint another process froze ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
  Resumed and ran to completion.
  Completed state: WireTransferState { invoice_generated: true, amount: 4200.5, currency: "USD", approved_by: Some("compliance_officer_07"), is_completed: true, frozen_by: Some("dotnet"), executed_by: Some("rust") }

  frozen_by = "dotnet", executed_by = "rust"

=== [dotnet] resume: pick up a checkpoint another process froze ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
error: resume failed: checkpoint for thread 'wire-transfer-xlang' was already resumed (was this thread already resumed? run `freeze` again)

=== [rust] resume: pick up a checkpoint another process froze ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
error: resume failed: checkpoint for thread 'wire-transfer-xlang' was already resumed (was this thread already resumed? run `freeze` again)
```

**Rust freezes, .NET resumes** — then a second resume from each runtime

```bash
# from strata-demos-rust
cargo run -p cross-language-interop -- freeze
# from strata-demos-dotnet
dotnet run --project demos/09-cross-language-interop/09-cross-language-interop.csproj -- resume
# both refused, exit 1:
dotnet run --project demos/09-cross-language-interop/09-cross-language-interop.csproj -- resume
cargo run -p cross-language-interop -- resume
```

```
=== [rust] freeze: run to the interrupt boundary ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
  Interrupted at 'PrepareWireTransfer' (thread 'wire-transfer-xlang'); next node on resume: 'ExecuteWireTransfer'
  Frozen state: WireTransferState { invoice_generated: true, amount: 4200.5, currency: "USD", approved_by: None, is_completed: false, frozen_by: Some("rust"), executed_by: None }

  This process is about to exit; the checkpoint now exists only in the file.
  Next: run the `resume` step -- from the other runtime to see the hand-off.

=== [dotnet] resume: pick up a checkpoint another process froze ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
  Resumed and ran to completion.
  Completed state: WireTransferState { InvoiceGenerated = True, Amount = 4200.5, Currency = USD, ApprovedBy = compliance_officer_07, IsCompleted = True, FrozenBy = rust, ExecutedBy = dotnet }

  frozen_by = "rust", executed_by = "dotnet"

=== [dotnet] resume: pick up a checkpoint another process froze ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
error: resume failed: checkpoint for thread 'wire-transfer-xlang' was already resumed (was this thread already resumed? run `freeze` again)

=== [rust] resume: pick up a checkpoint another process froze ===
  Checkpoint log: %TEMP%\strata-demo-09-interop\checkpoints.sqlite
error: resume failed: checkpoint for thread 'wire-transfer-xlang' was already resumed (was this thread already resumed? run `freeze` again)
```

(`ApprovedBy = ` is blank in a C# record's `ToString()` when it's null.) The
refusal message is the engine's own, and it's the same text in both runtimes.
.NET → .NET is the twin's own run; see its README.

**Inspect the file with the `strata` CLI** (the release binary; see the root [README](../../README.md#prerequisites))

```bash
strata thread list --storage <path-printed-by-freeze>
strata thread inspect wire-transfer-xlang --storage <path>
strata checkpoint log wire-transfer-xlang --storage <path>
strata checkpoint replay wire-transfer-xlang --sequence 0 --storage <path>
```

On the Rust-frozen file, before the resume (timestamps will differ):

```
+---------------------+---------------------+-------------+-------------+-------------+----------------------+
| Thread ID           | Current Node        | Sequence ID | State       | Waiting For | Updated At           |
+---------------------+---------------------+-------------+-------------+-------------+----------------------+
| wire-transfer-xlang | PrepareWireTransfer | 0           | interrupted | -           | 2026-10-24T15:59:13Z |
+---------------------+---------------------+-------------+-------------+-------------+----------------------+
```

And after .NET resumed it:

```
$ strata thread list --storage <path>
+---------------------+---------------------+-------------+---------+-------------+----------------------+
| Thread ID           | Current Node        | Sequence ID | State   | Waiting For | Updated At           |
+---------------------+---------------------+-------------+---------+-------------+----------------------+
| wire-transfer-xlang | PrepareWireTransfer | 1           | resumed | -           | 2026-10-24T15:59:15Z |
+---------------------+---------------------+-------------+---------+-------------+----------------------+

$ strata thread inspect wire-transfer-xlang --storage <path>
Thread: wire-transfer-xlang
  Status:          interrupted
  Thread state:    resumed
  Current node:    PrepareWireTransfer
  Next node:       ExecuteWireTransfer
  Checkpoint count: 2
  Created at:      2026-10-24T15:59:13Z
  Resumed at:      2026-10-24T15:59:15Z
  State:
{
  "amount": 4200.5,
  "approved_by": null,
  "currency": "USD",
  "executed_by": null,
  "frozen_by": "rust",
  "invoice_generated": true,
  "is_completed": false
}

$ strata checkpoint log wire-transfer-xlang --storage <path>
#0    [interrupted] node=PrepareWireTransfer -> ExecuteWireTransfer at=2026-10-24T15:59:13Z
#1    [interrupted] node=PrepareWireTransfer -> ExecuteWireTransfer at=2026-10-24T15:59:13Z

$ strata checkpoint replay wire-transfer-xlang --sequence 0 --storage <path>
Loading historical state from Checkpoint #0... Done.
Suspended at node: PrepareWireTransfer
Would resume at node: ExecuteWireTransfer
State at this checkpoint:
{
  "amount": 4200.5,
  "approved_by": null,
  "currency": "USD",
  "executed_by": null,
  "frozen_by": "rust",
  "invoice_generated": true,
  "is_completed": false
}
```

Row `#1` is the resume marker .NET wrote: the same checkpoint again with
`resumed_at` set. The state shown is the *frozen* one — with only boundary
checkpoints, the completed state lives in the resuming process, not the file.
The twin's README shows the same commands on a .NET-written file.

## Files

- `src/main.rs` — the whole demo, single file (`freeze` / `resume` modes)
- `Cargo.toml` — package name is `cross-language-interop` (folder keeps the
  `09-` prefix; Cargo package names can't start with a digit). Depends on
  `datalium-strata`, `datalium-strata-memory` (`SqliteCheckpointStore`),
  `serde` and `tokio` (for `#[tokio::main]`: `run` and `resume` are async in 0.9)
