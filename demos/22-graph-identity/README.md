# 22 — graph-identity

Graph identity in checkpoints (`design/31-Graph-Identity.md`): shipping a new
version of a graph while threads the old version wrote are still suspended. A
shop's returns process, `Inspect -> Approve (a manager's sign-off) -> Refund`,
waits at `Approve` for days, so when version 2 ships some version 1 threads
are still in the store. Version 2 renames a state field (`amount` ->
`refund_amount`), adds two (`refund_method`, `restocked`), renames a node
(`Refund` -> `IssueRefund`) and adds one after it (`Restock`).

Without an identity, nothing stops version 2 from reading a version 1 thread:
its frozen state has no `refund_amount`, so it wouldn't even deserialize as a
`ReturnV2`, and its next node, `Refund`, no longer exists. With one, version 2
refuses the thread before it writes anything, unless a migration adopts it.

The .NET twin is
[`strata-demos-dotnet/demos/22-graph-identity`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/22-graph-identity):
same story and steps, against one SQLite file.

## What it demonstrates

- **`CompileOptions::with_graph_id` / `with_graph_version`.** Every
  checkpoint the graph writes carries `graph` = `{id, version}` (a
  `GraphIdentity`, whose `Display` is `'returns' version 1`), at
  `schema_version` 1.6.0
- **A version 1 thread refused by version 2, with nothing written** (3 rows
  before and after):
  `Failed(StrataError::ExecutionAborted("thread 'R-1' was written by graph 'returns' version 1, not version 2, and this graph has no migration"))`.
  The thread stays resumable by version 1
- **A different graph refused**, even with the same builder, node names and
  state type (`'exchanges'`), and so is a graph with no identity. `end` checks
  identity the same way
- **`CompiledGraph::owns(&checkpoint)`**, the cheap check a worker makes
  before resuming: version 2 owns its own threads, not version 1's, until it
  has a migration
- **`ThreadQuery::with_graph` / `with_graph_version`** (with
  `with_states([ThreadState::Interrupted])`): listing one version's waiting
  threads through `CheckpointQuery::list_threads`, to drain them with the old
  deployment (picked by `checkpoint.graph.version` on `load_latest`) and to
  know when the old version can be retired
- **`CompileOptions::with_migration`** adopting a version 1 thread into
  version 2: it sees the level as stored, refuses anything but version 1
  (`None`), reshapes the frozen state JSON and renames the next node. It runs
  once, before anything is written, and not for version 2's own threads. The
  resume stamp is the migrated checkpoint: `R-2`'s log entry #1 has
  `graph_version` 2, `next_node` `IssueRefund` and the reshaped state

### Where the Rust demo differs from .NET

- **It reads the file through the store, not with raw SQL.** The .NET demo
  opens the database with `Microsoft.Data.Sqlite` and `SELECT`s the
  `checkpoint_threads` and `checkpoints` tables. This demo reads the same
  things through `SqliteCheckpointStore`'s query port instead, so it needs no
  `rusqlite` dependency of its own: `list_threads` returns each thread's
  `ThreadSummary` (state, `graph`, `schema_version`) from the head table, and
  `get_log` returns every row of a thread's log with its sequence. The Rust
  store's file has the same `graph_id` / `graph_version` columns on both
  tables (`strata-memory/src/checkpoint.rs`), and a `ThreadQuery` by graph is
  a `WHERE h.graph_id = ? AND h.graph_version = ?` on them
- **The frozen state printed** is the parsed JSON re-serialized by
  `serde_json`, so its keys come out sorted, not in the order the row holds
  them; field names are snake_case where .NET's are PascalCase
- **A refusal** is `Failed(StrataError::ExecutionAborted(message))`; .NET's is
  a `Failed` result with a `Message`
- The database is `demos/22-graph-identity/returns.db` (gitignored), deleted
  at the start of each run and left behind afterwards. Each step opens the
  file afresh, as the .NET demo does

## What it does *not* cover

- **Identity inside a subgraph.** Each level carries its own graph's
  identity and is checked, and migrated, by its own graph (`thread 't' at
  level [Work] was written by ...`)
- **`wake_due` listing only its own graph's threads** (and
  `WakeOptions::with_match_graph`). For waits and the wake driver, see
  [`19-waits-and-timers`](../19-waits-and-timers)
- **The exported definition's `identity`**, `RunStarted`'s graph, the run
  span's `strata.graph.id` / `strata.graph.version` tags, and the CLI's
  `thread list --graph`
- **Automatic migration.** A migration is the application's code; the engine
  has no version order or schema diff

## Run it

```bash
cargo run -p graph-identity
```

Expected output:

```
=== Step 1: version 1 is deployed; two returns wait for approval ===
  R-1 ($120) waits at Approve, written by 'returns' version 1
  R-2 ($75) waits at Approve, written by 'returns' version 1

=== Step 2: version 2 is deployed, with no migration ===
  R-3 ($40) is new: version 2 runs it, and it waits at Approve, written by 'returns' version 2
    list_threads (each thread's head, with the graph that wrote it):
      R-1  interrupted  graph_id returns  graph_version 1  schema_version 1.6.0
      R-2  interrupted  graph_id returns  graph_version 1  schema_version 1.6.0
      R-3  interrupted  graph_id returns  graph_version 2  schema_version 1.6.0
  Version 2 resumes R-1: thread 'R-1' was written by graph 'returns' version 1, not version 2, and this graph has no migration
  Checkpoint rows before and after: 3 and 3. R-1 still waits, resumable by version 1.
  v2.owns(R-1): false, v2.owns(R-3): true

=== Step 3: a different graph can't take a returns thread ===
  'exchanges' resumes R-1:      thread 'R-1' was written by graph 'returns' version 1, not 'exchanges' version 1
  An unnamed graph resumes R-1: thread 'R-1' was written by graph 'returns' version 1, and this graph has no identity
  'exchanges' ends R-1:         thread 'R-1' was written by graph 'returns' version 1, not 'exchanges' version 1

=== Step 4: the query port lists each version's waiting threads ===
  returns version 1, interrupted: R-1, R-2
  returns version 2, interrupted: R-3
  R-1 was written by version 1, so it goes to version 1:
    completed: inspected / sent for approval / refunded $120

=== Step 5: version 2 is redeployed with a migration for version 1 threads ===
  v2.owns(R-2) without the migration: false; with it: true
  Version 2 resumes R-2: completed: inspected / sent for approval / refunded $75 to original-payment / restocked
  R-2's log (CheckpointQuery::get_log): the resume stamp carries the migrated identity, state and next node:
    #0 graph_version 1  next_node Refund       frozen_state {"amount":75,"inspected":true,"log":["inspected","sent for approval"],"refunded":false,"return_id":"R-2"}
    #1 graph_version 2  next_node IssueRefund  frozen_state {"inspected":true,"log":["inspected","sent for approval"],"refund_amount":75,"refund_method":"original-payment","refunded":false,"restocked":false,"return_id":"R-2"}
  returns version 1, interrupted: none: version 1 can be retired

=== Done: what the file holds now ===
    list_threads (each thread's head, with the graph that wrote it):
      R-1  resumed      graph_id returns  graph_version 1  schema_version 1.6.0
      R-2  resumed      graph_id returns  graph_version 2  schema_version 1.6.0
      R-3  resumed      graph_id returns  graph_version 2  schema_version 1.6.0
```

The demo checks each claim (the refusal messages, that refusals write
nothing, `owns`, the listings by version, that the migration ran once and the
stamp is the migrated checkpoint) and exits with code 1 if one fails. A
completed run writes nothing (the default `CheckpointPolicy::Boundaries`), so
a finished return's head is its resume stamp, `resumed`.

## Files

- `src/main.rs` — the whole demo, single file
- `Cargo.toml` — package name is `graph-identity`; depends on
  `datalium-strata`, `datalium-strata-memory` (for `SqliteCheckpointStore`),
  `serde`, `serde_json` and `tokio`
