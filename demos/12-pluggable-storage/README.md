# 12 — pluggable-storage

One graph, one scenario, **four storage backends**: in-memory, SQLite, Postgres
and Redis. A wire-transfer graph (the one from
[`05-hitl-interruption`](../05-hitl-interruption)) freezes at its interrupt
boundary, every connection is dropped, and a brand-new one resumes it — while a
chat history is kept alongside in the same backend. The scenario is written
once and run against all four; nothing in it knows which backend it is talking
to.

`design/03-Architecture.md` promises that persistence is pluggable —
`CheckpointStore` and `HistoryStore` are traits, and "in-memory → SQLite →
Redis/PostgreSQL" are just implementations. Until this demo only SQLite
existed, so "pluggable" meant "two SQL-shaped things". This demo is the test of
the claim.

This is the Rust half, run against Strata 0.9 (`0.9.0-preview.1`). The .NET twin is
[`strata-demos-dotnet/demos/12-pluggable-storage`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/12-pluggable-storage)
(`../strata-demos-dotnet/demos/12-pluggable-storage` next to this
checkout); the two hand a frozen thread to each other through Postgres and
through Redis (below).

## What it demonstrates

- **The same code path against four backends.** `run_scenario` freezes a graph,
  drops the stores, resumes from fresh ones, reads the history back and checks a
  second resume is refused — and the only place that names a concrete store is
  `Backend::open`
- **Redis is not SQL-shaped, and the traits didn't change.** There are no tables
  and no `MAX(sequence) + 1`: a thread's checkpoint log and a session's history
  are each one Redis list (a checkpoint is appended by a Lua script that also
  keeps the query index, and read with `LINDEX`; a message is an `RPUSH`, read
  back with `LRANGE -n -1`). That the same traits admit it is the actual proof
  of "pluggable" — a Postgres adapter alone would only show SQL-to-SQL
- **Durability differs, visibly.** The in-memory store *loses* the checkpoint when
  it is "restarted" (the demo asserts that, as expected); the other three keep it
- **Concurrent writers (Postgres, Redis).** Four connections append to one session
  at once, one tokio task each on the multi-threaded runtime; nothing is lost and
  each writer's messages stay in order. Postgres does it with a per-session
  `pg_advisory_xact_lock` taken before choosing the next `sequence`; Redis needs
  nothing (`RPUSH` is atomic). SQLite's adapters assume a single writer, and the
  demo says so instead of trying
- **Opening a server store, the 0.9 way.** The stores are async (a
  `deadpool-postgres` pool over `tokio-postgres`; a `redis` `ConnectionManager`).
  `PostgresCheckpointStore::connect_with(url, PostgresStoreOptions::new())` and
  `PostgresHistoryStore::connect(url)` take one connection and create or migrate
  their tables (`strata_schema_version`) before they return, so an unreachable
  database, or one a newer build wrote, fails at startup rather than at the first
  save. Redis uses `RedisCheckpointStore::connect_with(url,
  RedisStoreOptions::new().with_prefix(...))`. Every call either store makes,
  that first connect included, retries a transient failure inside the store
  (`StoreRetry`: 4 attempts with backoff by default,
  `design/34-Storage-Hardening.md`). Dropping a store closes it
- **Delta checkpoints, written by one store and read by another** (`design/41-Checkpoint-Efficiency.md`).
  After the matrix, each durable backend runs a six-node graph compiled with
  `CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode)` (seven
  checkpoints: `running` before the first node and after each of the next five,
  `completed` after the last) on a store opened with `with_deltas(Deltas::new())`.
  Five of the seven are stored as deltas (only the keys that changed), counted
  from the stores' own `strata.store.checkpoint.size` telemetry. A fresh store
  opened **without** deltas then reads the thread's whole log back through
  `CheckpointQuery::get_log`: every row whole, in order, with the version the
  engine wrote. A reader needs no options to decode them
- **Cross-runtime, through a real server.** The Postgres tables and the Redis keys
  are shared with the .NET stores, so demo 09's hand-off works through them too
  (below): each runtime resumes the other's frozen thread and reads the other's
  history. All four directions (.NET → Rust and Rust → .NET, through Postgres and
  through Redis) were run with Strata 0.9; the output is below

### How the Rust half differs from the .NET twin

- **Telemetry.** .NET listens with a `MeterListener`. The Rust stores record
  through the [`metrics`](https://docs.rs/metrics) facade, so the demo installs a
  small global `metrics::Recorder` of its own (`CheckpointSizes`) that keeps only
  `strata.store.checkpoint.size`, by its `strata.store` and
  `strata.store.encoding` labels. It is global, not a thread-local recorder,
  because the SQLite store records on tokio's blocking pool
- **Byte counts** are what each store reports for the JSON it serialized; the
  Rust SQL stores' whole-row average comes out a byte under the .NET twin's
  (2162 vs 2163). The rows as stored in Postgres are the same length either way
  (`JSONB`), see "What's actually stored"
- **The approval** is `HumanSignal::approve("token-xyz").with_field_override("approved_by", ...)`:
  a string override, the counterpart of .NET's `FieldOverrides`

## What it took (work in Strata)

Two new adapters per store per language, plus one bug the demo found:

| | Rust (`strata-memory`) | .NET (`Datalium.Strata.Storage`) |
|---|---|---|
| Postgres | `postgres_store::{PostgresCheckpointStore, PostgresHistoryStore}` (`tokio-postgres`, `deadpool-postgres` since 0.4) | `Postgres.{PostgresCheckpointStore, PostgresHistoryStore}` (plain Npgsql) |
| Redis | `redis_store::{RedisCheckpointStore, RedisHistoryStore}` (async `redis`, `ConnectionManager`) | `Redis.{RedisCheckpointStore, RedisHistoryStore}` (StackExchange.Redis) |

- **No trait changes.** `CheckpointStore`/`HistoryStore` (and the C# interfaces)
  needed nothing to admit either backend
- **Layouts pinned across runtimes** by shared fixtures — `postgres-columns.json`
  for the two Postgres tables and the checkpoint wire JSON for Redis — asserted by
  both languages' tests (`schema/README.md`: "Postgres checkpoint log", "Redis
  layout")
- **A .NET SQLite bug, fixed** (found by the .NET twin). `EfCoreHistoryStore`
  created its table with `EnsureCreated()`, which silently does nothing when the
  file already has *any* table. Opened after `EfCoreCheckpointStore` on the same
  file, its first query hit "no such table" — the Rust stores could share a file,
  the .NET ones couldn't. It now runs `CREATE TABLE IF NOT EXISTS`
- Contract tests run the same assertions against all four backends, and the
  Postgres/Redis ones need live servers (`STRATA_TEST_POSTGRES_URL` /
  `STRATA_TEST_REDIS_URL`); both CI workflows start service containers for them

Since then (0.4–0.9) the stores have gained, in both runtimes: async stores, the
query port (`list_threads`, `get_log`) with a head table or index, retries inside
every call, migrations and a `strata_schema_version` table, retention (`purge`,
and an opt-in Redis TTL for finished threads), thread leases, group commit
(Postgres, SQLite), delta checkpoints and value offload. None of it changed the
traits this demo's scenario uses.

## What it does *not* cover

- **Retention.** Every built-in store implements `CheckpointRetention::purge`
  (deletes finished threads by age), and Redis can expire finished threads
  (`RedisStoreOptions::with_finished_thread_ttl`, off by default). The demo calls
  neither, so what it writes stays until you drop the containers
- **Group commit and value offload** (`with_group_commit`, `with_offload` on the
  same options). Only deltas are shown
- **Leases and recovery.** Thread leases (demo [`21-thread-leases`](../21-thread-leases))
  and crash recovery of a durable thread (demo [`20-durable-execution`](../20-durable-execution))
  work on these backends; the scenario here runs one writer per thread
- **Migrations as a deploy step.** The stores migrate when they connect (the
  default). `PostgresStoreOptions::with_auto_migrate(false)` makes them only check,
  and `migrate()` is then a deploy step's job; the demo doesn't show it
- **TLS.** `connect` uses `NoTls`; for TLS, a custom `search_path` or your own
  pool, build the store with `from_client`. No Redis Cluster or Sentinel
- **SQLite history isn't cross-runtime**: the EF Core store writes a
  `"HistoryRecords"` table with the role as `"User"`, Rust a `history_records`
  table with `"user"`. The Postgres and Redis history stores are shared. Demo 09
  is the SQLite hand-off, for checkpoints only
- **Not a benchmark.** Nothing here measures latency or throughput; the byte
  counts are what the stores report, not a measurement of the backend
- **Needs Docker** (or your own Postgres and Redis) — this is the first demo that
  needs external services. No LLM, no API key

## Run it

```bash
docker compose up -d --wait        # Postgres 16 + Redis 7, throwaway (see docker-compose.yml)
cargo run -p pluggable-storage     # the matrix: all four backends
docker compose down
```

To pick backends: `cargo run -p pluggable-storage -- matrix redis postgres`.
Connection URLs come from `STRATA_DEMO_POSTGRES_URL`
(default `postgres://strata:strata@localhost:5432/strata`) and
`STRATA_DEMO_REDIS_URL` (default `redis://localhost:6379`). If a server isn't
reachable the demo says so and how to start it, and exits 1, rather than
hanging. Each connect times out after 5 s, and a store tries a failed connect
four times with backoff before giving up, so that takes about 10 s for Redis
and 15 s for Postgres:

```
error: postgres: cannot use Postgres at postgres://strata:***@localhost:55499/strata: error connecting to server: No connection could be made because the target machine actively refused it. (os error 10061)
  start it with `docker compose up -d --wait` (or set STRATA_DEMO_POSTGRES_URL)
```

`datalium/strata` and `strata-demos-dotnet` ship the same
`docker-compose.yml` on the same ports — run one copy at a time. If those ports
are taken, run the two images on others and point the two variables at them, as
the run below did (`postgres://strata:strata@localhost:55432/strata`,
`redis://localhost:56379`).

**Cross-runtime hand-off** (Postgres or Redis; same thread id
`wire-transfer-pluggable` and Redis prefix `strata-demo-12` in both runtimes, so
no argument beyond the backend). With the same two variables set in both shells:

```bash
# .NET freezes, Rust resumes
# from strata-demos-dotnet
dotnet run --project demos/12-pluggable-storage/12-pluggable-storage.csproj -- freeze postgres
# from strata-demos-rust
cargo run -p pluggable-storage -- resume postgres

# Rust freezes, .NET resumes
# from strata-demos-rust
cargo run -p pluggable-storage -- freeze redis
# from strata-demos-dotnet
dotnet run --project demos/12-pluggable-storage/12-pluggable-storage.csproj -- resume redis
```

Output of a real run — the matrix:

```
=== one scenario, 4 storage backends ===
  freeze a wire-transfer graph -> drop every connection -> resume from a fresh one -> read history back

  in-memory  ok
  sqlite     ok
  postgres   ok
  redis      ok

  backend    survives restart       history  concurrent writers
  in-memory  no (lost, as expected) 3        in-process only
  sqlite     yes                    3        single writer assumed
  postgres   yes                    3        40 appends, none lost
  redis      yes                    3        40 appends, none lost

  Same graph, same assertions, same code path for all of them.

=== delta checkpoints: written with deltas on, read by a store without them ===
  a durable graph (6 nodes, CheckpointPolicy::EveryNode) runs once on a store with deltas; a fresh store
  opened without deltas reads the thread's log back

  backend    rows read  stored whole  as deltas  bytes (whole / delta, average)
  sqlite     7          2             5          2162 / 133
  postgres   7          2             5          2162 / 133
  redis      7          2             5          2374 / 346

  Every row came back whole, in order: the reader needed no options to decode the deltas.
```

`dotnet run … -- freeze postgres`, then `cargo run … -- resume postgres` (on fresh
containers, Strata 0.9):

```
=== [dotnet] freeze on postgres: run to the interrupt boundary ===
  Interrupted at 'PrepareWireTransfer' (thread 'wire-transfer-pluggable'); next node on resume: 'ExecuteWireTransfer'
  Appended 2 history messages.

  This process is about to exit; the state now lives only in the database.
  Next: run the `resume` step -- from the other runtime to see the hand-off.
```

```
=== [rust] resume on postgres: pick up a thread another process froze ===
  Resumed and ran to completion.
  Completed state: WireTransferState { invoice_generated: true, amount: 4200.5, currency: "USD", approved_by: Some("compliance_officer_07"), is_completed: true, frozen_by: Some("dotnet"), executed_by: Some("rust") }

  frozen_by = "dotnet", executed_by = "rust"

  History (as read from the store):
    user       [dotnet] Wire $4,200.50 to Acme Corp
    assistant  [dotnet] Prepared the transfer; waiting for approval.
    assistant  [rust] Transfer executed.
```

Then the other way on the same database: `cargo run … -- freeze postgres`, then
`dotnet run … -- resume postgres`. The session's history is shared and
append-only, so it now holds both hand-offs:

```
=== [rust] freeze on postgres: run to the interrupt boundary ===
  Interrupted at 'PrepareWireTransfer' (thread 'wire-transfer-pluggable'); next node on resume: 'ExecuteWireTransfer'
  Appended 2 history messages.

  This process is about to exit; the state now lives only in the database.
  Next: run the `resume` step -- from the other runtime to see the hand-off.
```

```
=== [dotnet] resume on postgres: pick up a thread another process froze ===
  Resumed and ran to completion.
  Completed state: WireTransferState { InvoiceGenerated = True, Amount = 4200.5, Currency = USD, ApprovedBy = compliance_officer_07, IsCompleted = True, FrozenBy = rust, ExecutedBy = dotnet }

  frozen_by = "rust", executed_by = "dotnet"

  History (as read from the store):
    user       [dotnet] Wire $4,200.50 to Acme Corp
    assistant  [dotnet] Prepared the transfer; waiting for approval.
    assistant  [rust] Transfer executed.
    user       [rust] Wire $4,200.50 to Acme Corp
    assistant  [rust] Prepared the transfer; waiting for approval.
    assistant  [dotnet] Transfer executed.
```

The same two hand-offs through Redis (`freeze redis` / `resume redis`) print the
same lines with `redis` in the headers: `frozen_by = "dotnet", executed_by =
"rust"` from the Rust resume, then `frozen_by = "rust", executed_by = "dotnet"`
from the .NET one, with the same history.

Running `resume` again prints `error: resume failed: … was already resumed …` and
exits 1, whichever runtime you use — the resume marker crosses the database too:

```
=== [rust] resume on redis: pick up a thread another process froze ===
error: resume failed: checkpoint for thread 'wire-transfer-pluggable' was already resumed (was this thread already resumed? run `freeze` again)
```

`freeze` after that starts a fresh checkpoint on the same thread (the log is
append-only, so the old one stays as history).

## What's actually stored

After the matrix and the two Postgres hand-offs above
(`docker compose exec postgres psql -U strata -d strata`):

```
select thread_id, sequence, status, resumed_at is not null as resumed, left(frozen_state::text, 44)
  from checkpoints where thread_id = 'wire-transfer-pluggable' order by sequence;

        thread_id        | sequence |   status    | resumed |                     left
-------------------------+----------+-------------+---------+----------------------------------------------
 wire-transfer-pluggable |        0 | interrupted | f       | {"amount": 4200.5, "currency": "USD", "froze    <- .NET froze
 wire-transfer-pluggable |        1 | interrupted | t       | {"amount": 4200.5, "currency": "USD", "froze    <- Rust resumed
 wire-transfer-pluggable |        2 | interrupted | f       | {"amount": 4200.5, "currency": "USD", "froze    <- Rust froze
 wire-transfer-pluggable |        3 | interrupted | t       | {"amount": 4200.5, "currency": "USD", "froze    <- .NET resumed
```

A resume adds the resume marker (a copy with `resumed_at` set) to the same log;
whichever runtime froze writes one row, whichever resumed the next.
`frozen_state` is `JSONB`, so key order isn't what was saved (`amount`,
`currency`, … not declaration order) — harmless, state is read by field name.

The deltas thread, as the Rust store wrote it (the run id differs per run):

```
select sequence, schema_version, status, length(frozen_state::text) as bytes
  from checkpoints where thread_id = 'ledger-<run id>-postgres' order by sequence;

 sequence | schema_version |  status   | bytes
----------+----------------+-----------+-------
        0 | 1.7.0          | running   |  2117
        1 | 1.8.0          | running   |   104
        2 | 1.8.0          | running   |   115
        3 | 1.8.0          | running   |   126
        4 | 1.8.0          | running   |   137
        5 | 1.8.0          | running   |   148
        6 | 1.0.0          | completed |  2181
```

The same as the .NET twin's table, row for row. `1.8.0` is a delta row: only
`entries` changed, so only `entries` is stored. Every store, in either runtime,
decodes it back to the `1.7.0` checkpoint the engine wrote.
`select * from strata_schema_version` shows `checkpoints` at 2 and `history` at 1.

In Redis the shared thread is a single list, one JSON checkpoint per element
(`docker compose exec redis redis-cli LRANGE strata-demo-12:checkpoints:wire-transfer-pluggable 0 -1`:
four elements after the two Redis hand-offs), and the history is another,
`strata-demo-12:history:wire-transfer-pluggable` (six). The matrix uses a
prefix of its own per run, `strata-demo-12-<run id>`.

## Files

- `src/main.rs` — the whole demo, single file (`matrix` / `freeze` / `resume` modes)
- `Cargo.toml` — package name is `pluggable-storage` (folder keeps the `12-`
  prefix; Cargo package names can't start with a digit). Besides the two Strata
  crates: `tokio` (the runtime), `metrics` (the size recorder; already in
  `strata-memory`'s dependency tree), `chrono` (only to build `ChatMessage`
  timestamps), `serde`
- `../../docker-compose.yml` — the throwaway Postgres + Redis
