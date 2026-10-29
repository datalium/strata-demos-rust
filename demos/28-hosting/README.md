# 28 — hosting

Strata in a long-running service (`design/35-Hosting-Integration.md`, crate
`datalium-strata-host`). A reminder service — `Schedule` -> `Wait` (a
five-minute timer) -> `Send` — is wired from `StrataSettings` read with serde
from `strata.json`: a SQLite checkpoint store (`settings.sqlite.open()`), the
run defaults (`settings.runs.config()`, the lease), and a `Host` running a
wake driver that resumes threads whose timer is due. `check_store` and
`Host::status()` are what a readiness endpoint would ask. The demo runs the
host on a task until a `CancellationToken` fires, so it exits on its own. The
host decides what is due on a clock the demo sets (`Host::with_clock`), and
the waits between rounds are tokio's, paused, so a five-minute timer is due as
soon as the demo moves the clock.

Every earlier demo called the engine directly. This one shows what an
application gets from the host instead: nobody calls `wake_due` in a loop, a
round that fails backs off instead of ending the service, and stopping the
process doesn't cancel a run.

## How it differs from the .NET twin

The .NET twin,
[`strata-demos-dotnet/demos/28-hosting`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/28-hosting),
is a .NET Generic Host demo: one `services.AddStrata()` chain registers the
store, the graph, a `StrataRunner<Reminder>`, the wake driver and two health
checks in the container. **Rust has no container, and this is not a copy of
that demo.** Same story, same five scenarios, but the wiring is the
application's own few lines of `main`:

| .NET (`Datalium.Strata.Hosting`) | Rust (`datalium-strata-host`) |
|---|---|
| `appsettings.json` bound to options (`Strata:Wake`, `Strata:Runs:Lease`) | `StrataSettings`, plain `serde` structs (`snake_case` keys), here from `strata.json` with `serde_json`. Durations are `"5s"` / `"30s"` or a number of seconds. Any format serde reads works; layering several sources (files, environment) is a config crate's job, so the demo sets `sqlite.path` in code |
| `.AddSqliteCheckpoints()` | `settings.sqlite.open()` returns a `SqliteCheckpointStore` |
| `.AddGraph(...)`, compiled at `StartAsync` | `compile_with(..)` returns a `Result`; a host is only ever handed a `CompiledGraph` |
| `StrataRunner<Reminder>` injected into `ReminderDesk` | `ReminderDesk` holds the graph, the store and a `RunConfig` itself |
| `NodeContext.Services` (the container) | `RunConfig::with_service(outbox)`, read with `NodeContext::service::<Outbox>()` |
| `.AddWakeDriver<Reminder>()`, a hosted service | `Host::new().wake(graph, store, settings.wake.with_config(config))`, run with `host.run(token)` on a task |
| `StopAsync` | cancelling the token; `host.run` returns once the run in flight has finished |
| Health checks `strata-checkpoints` / `strata-drivers` | `check_store(store, timeout)` and `Host::status()`; the demo composes the two into Healthy / Degraded / Unhealthy itself |
| `FakeTimeProvider` in the container | `Host::with_clock` for "now", and tokio's paused clock (`start_paused`, the `test-util` feature) for the waits between rounds |
| An `ILoggerProvider` keeps the driver's log lines | The host logs through `tracing`; the demo installs a minimal subscriber (written against `tracing` alone) that keeps them |

Two more differences show in the output:

- **The timer's due time comes from the system clock.** The engine stamps a
  timer `now + 5 min` with `Utc::now()` when the run freezes, so the demo's
  clock starts at the real time and the demo prints "due 5 min after it was
  set", not a fixed 09:05
- **The outage is simulated with a switch**, not by dropping the store's
  tables: the store is wrapped in `Switchable` (`src/support.rs`), which
  forwards every call to the SQLite store, leases included, and fails them
  all while it is down. The probe reports the store's error message, where
  .NET reports the exception's type; a driver's failure is reported by kind
  only (`ExecutionAborted`, the variant of the error `wake_due_with`
  returned), never its message, which could hold a connection string

## What it demonstrates

- **Settings**: `strata.json` sets `wake` (interval 5 s, batch 10) and
  `runs.lease` (owner `reminders-1`, ttl 30 s); the code sets `sqlite.path`, a
  temp file. Scenario 1 reads them back and checks them, and that
  `settings.runs.config()` has the lease
- **The run defaults** every run carries, the ones the desk starts and the
  ones the driver wakes alike: the lease from the settings, and the `Outbox`
  service the `Send` node gets from `NodeContext::service`
- **The wake driver** (scenario 2). `run_with` returns `Interrupted` at `Wait`
  with a timer due five minutes on. Four minutes on the host's clock, the
  driver's rounds find nothing. Past five, the driver resumes the thread, and
  the log reads `Running -> Running -> Interrupted -> Running -> Completed`.
  The graph is compiled with `CheckpointPolicy::EveryNode`, so the completed
  run leaves a checkpoint; the default policy writes nothing when a run
  completes
- **A clean stop** (scenario 3). Two threads are due. The driver wakes one
  (`remind-dave`, by the store's listing order) and its `Send` is held open.
  The token is cancelled, and `host.run` doesn't return for a minute of
  (paused) time while the run is in flight. Once the send is released, the
  run completes, `host.run` returns `Ok(())`, the other thread is still
  `Interrupted`, and no checkpoint is `Cancelled`: the token tells the round
  to start nothing more, and nothing cancels the run. The host's log shows the
  second "handled 1 thread(s)" landing after the token was cancelled. A
  second host on the same SQLite file then wakes the thread the first one left
- **Readiness** (scenarios 1 and 4). Both are healthy at start. Then the store
  goes down under the running host: `check_store` turns `Unhealthy`, the
  driver's next round fails, `Host::status()` shows it failing (the demo calls
  that Degraded), and the host keeps running. Cancelling the token then ends
  the driver's back-off wait (twice the interval) at once
- **Start-up refusals** (scenario 5). A graph with a node nobody wired in
  doesn't compile (`StrataError::GraphInvalid`, `STRATA_ERR_ORPHAN`), so there
  is no graph to hand the host. And settings the host can't run with
  (`wake.batch_size = 0`) make `Host::run` return a `HostError` before any
  driver starts

## What it does *not* cover

- **Timers and waits themselves.** Calling `wake_due` by hand, event waits and
  timeouts are in [`19-waits-and-timers`](../19-waits-and-timers); this demo
  only shows the driver that calls it
- **The other drivers.** `Host::recover` (runs whose process died; the engine
  side is in [`20-durable-execution`](../20-durable-execution)),
  `Host::retain` (purging old threads), `Host::retain_memory` and `Host::task`
  (a loop of the application's own) are added the same way and are not shown
- **The run scheduler** (`settings.scheduler`, `Host::close_on_shutdown`). See
  [`29-run-scheduler`](../29-run-scheduler), which uses the engine's
  `RunScheduler` directly
- **Exporting telemetry**, and serving readiness over HTTP. This is a console
  program with no web server; [`27-run-observability`](../27-run-observability)
  shows the spans and metrics the engine emits
- **Postgres and Redis stores** from settings (`settings.postgres.open()`,
  `settings.redis.open()`). The stores themselves are in
  [`12-pluggable-storage`](../12-pluggable-storage)
- **A real clock.** With the system clock the same host waits real minutes.
  The demo sets the host's clock and lets tokio's paused time pass while a
  background driver does its work (giving up after 2000 paused seconds); it
  never asserts how long anything took

## Run it

```bash
cargo run -p hosting
```

Expected output:

```
=== Scenario 1: the host starts, wired from configuration ===
  Checkpoint store: SqliteCheckpointStore on a temp file (sqlite.path, set in code over strata.json)
  Wake driver: every 5s, batches of 10 (wake, strata.json)
  Run lease: owner 'reminders-1', ttl 30s (runs.lease, strata.json)
  check_store: Healthy
  Host::status: Healthy (wake:Reminder, 0 failures in a row)

=== Scenario 2: a five-minute timer, woken by the driver ===
  run_with returned Interrupted at 'Wait': a Timer due 5 min after it was set
  4 minutes on the host's clock, 30 s of rounds: still waiting, nothing sent
  Past 5 minutes: the driver woke it. Checkpoint log: Running -> Running -> Interrupted -> Running -> Completed
  Sent: alice: stand-up at 09:05

=== Scenario 3: stopping the host with a woken run in flight ===
  Both are due; the driver woke one, and its Send is held open.
  Shutdown token cancelled. Host stopped within a minute? false -- it waits for the run.
  Released the send. Host::run returned Ok(()):
    remind-carol: Interrupted (still waiting on its timer)
    remind-dave: Completed
    sends started: 1, cancelled checkpoints: 0
  The host's log (datalium_strata_host's tracing events):
    [INFO] Strata driver started, every 5s (strata.driver=wake:Reminder)
    [INFO] Strata driver handled 1 thread(s) (strata.driver=wake:Reminder)
    -- shutdown token cancelled --
    [INFO] Strata driver handled 1 thread(s) (strata.driver=wake:Reminder)
    [INFO] Strata driver stopped (strata.driver=wake:Reminder)
  A second host on the same file:
    woke the one left behind and sent it: carol: deploy window

=== Scenario 4: the store breaks under the running host ===
  check_store: Unhealthy (the database is unreachable (the demo's simulated outage))
  Host::status: Degraded (wake:Reminder, 1 failure(s) in a row, last: ExecutionAborted)
  The host is still running: true
  Stopped from the driver's back-off wait, at once. The host's log:
    [WARN] Strata driver failed a round; 1 in a row (strata.driver=wake:Reminder, error.type=ExecutionAborted)
    [INFO] Strata driver stopped (strata.driver=wake:Reminder)

=== Scenario 5: start-up refusals ===
  compile_with refused the graph: [STRATA_ERR_ORPHAN] Node 'Escalate' is isolated and unreachable from the entry point 'Schedule'.
  No CompiledGraph, so nothing to hand Host::wake: the graph compiles at start-up, not at its first run.
  Host::run with wake.batch_size = 0 refused: the wake driver's batch_size must be between 1 and 1000
  Drivers started: 0. Host::run checks every driver before it starts any.

=== Done: settings, a store, a wake loop, a readiness probe and a clean stop, without a container. ===
```

The demo checks each claim (the settings, the probe and driver states, the
timer, the checkpoint statuses, the send counts, the host's log order, the
error code and the refusal) and exits non-zero if one fails. The SQLite file is
a temp file, deleted at the end.

## Files

- `src/main.rs` — the settings, the application's start-up (`App::start`),
  the graph, `ReminderDesk`, and the five scenarios
- `src/support.rs` — `Switchable` (the SQLite store behind an outage switch),
  `DemoClock` (the host's "now"), `Outbox`, and `DriverLog` (a minimal
  `tracing` subscriber that keeps the host's log lines)
- `strata.json` — the `wake` and `runs` settings
- `Cargo.toml` — `datalium-strata`, `datalium-strata-memory` and
  `datalium-strata-host` from crates.io, plus
  `serde_json` (to read `strata.json`), `tracing`, `chrono`, and tokio with
  `test-util` for the paused clock
