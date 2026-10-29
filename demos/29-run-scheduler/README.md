# 29 — run-scheduler

The run scheduler (`design/40-Run-Scheduler.md`): a `RunScheduler`, given to a
run through `RunConfig::with_scheduler`, admits runs to a fixed number of
slots. A run waits for a slot before it loads or writes anything, and gives it
back when `run_with` returns. Four scenarios, one rule each: strict priority, a
reserved slot, per-tenant limits, and a bounded queue that sheds.

Every run here has one async node that records that it started and then waits
on a gate (a `tokio::sync::Notify`) the demo opens. So which runs hold slots at
any moment is the demo's choice, not a matter of timing, and the order of
admission is read from the order the nodes started. The tokio clock is paused
and moved only by `tokio::time::advance`, so how long a run queued
(`RunStarted.queued_for`, `NotAdmitted.waited`) is an exact figure.

This is plain engine code: no host. The .NET twin is
[`strata-demos-dotnet/demos/29-run-scheduler`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/29-run-scheduler);
same four scenarios, same checks, same numbers.

## What it demonstrates

- **Strict priority, first come first served within one** (scenario 1). One
  slot (`SchedulerOptions::new(1)`), held by `blocker`. Five runs arrive with
  `RunConfig::with_priority`: low-1 (p0), high-1 (p5), low-2 (p0), high-2 (p5),
  mid-1 (p2). As each slot holder finishes, the next to start is high-1,
  high-2, mid-1, low-1, low-2. The clock moves a minute per hand-over, so
  `RunEventKind::RunStarted { queued_for, .. }` reads exactly 1 to 5 minutes
  (and `Some(0ns)` for the run admitted on arrival)
- **A reserved slot** (scenario 2). Three slots, `.with_reservation(5, 1)`.
  Two p0 runs start; a third waits beside the free slot, which is the reserve.
  A p5 run arrives and takes it at once. When the p5 run finishes, its slot
  goes back to the reserve and the p0 run still waits: a reservation is
  headroom that lends nothing back. The p0 run starts when an unreserved slot
  frees up
- **Tenant limits** (scenario 3). Four slots, `.with_tenant_limit(1)`,
  `.with_tenant_override("initech", 0)`. The tenant is the run's metadata
  `tenant` (`RunConfig::with_metadata("tenant", ..)`; the key is
  `SchedulerOptions::with_tenant_key`). acme-2 waits behind acme-1 with slots
  free, and globex-1, which arrived after it, starts at once: a tenant at its
  limit holds back only its own runs. initech is paused (limit 0).
  `snapshot().tenants` shows each tenant's `TenantUsage { active, queued,
  limit }`. `set_tenant_limit(tenant, Some(n))` changes a limit while runs
  wait, and the waiting run starts at once (`None` would return the tenant to
  the default limit)
- **A bounded queue that sheds** (scenario 4). One slot, `.with_max_queued(2)`,
  two p0 runs waiting. A third p0 run outranks nobody, so it is refused on
  arrival: the run returns
  `GraphExecutionResult::Failed(StrataError::NotAdmitted { reason: QueueFull, waited: 0ns })`.
  Thirty seconds later a p5 run arrives and sheds the lowest-ranked waiting run
  (lowest priority, latest arrival): report-b returns `NotAdmitted` with
  `Shed` after exactly 30 s. The demo prints `scheduler::NotAdmitted`'s
  `Display`, the same words as the .NET engine's. Both refused runs leave no
  checkpoint and no run event, while report-a, which ran, leaves both. The
  graph is compiled with
  `CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode)` so
  that contrast is real: under the default policy a completed run writes
  nothing either
- **`snapshot()`**: active and queued counts against `max_concurrent` and
  `max_queued`, printed after each step

### Where the engines differ

- **The result shape.** .NET returns a `GraphExecutionResult.NotAdmitted` case
  with `Reason`, `Waited` and `Message`; Rust returns `Failed` with
  `StrataError::NotAdmitted { reason, waited }`, and `NotAdmitted`'s `Display`
  is the message
- **When a run joins the queue.** .NET's `RunAsync` takes its place in the
  queue during the call. Rust's `run_with` is an `async fn`, so the run joins
  the queue when its future is first polled; the demo spawns each run on a
  task and waits until `snapshot().queued` grows before launching the next,
  which makes the arrival order the launch order. (`RunScheduler::admit`, for
  scheduling work of your own, does join on the call)
- **The clock.** .NET gives the runs a `FakeTimeProvider` through
  `RunOptions.TimeProvider`. The Rust scheduler measures waits on tokio's
  clock, so the demo runs on a paused, single-threaded runtime
  (`#[tokio::main(flavor = "current_thread", start_paused = true)]`, which
  needs tokio's `test-util` feature). It waits for the runs with
  `tokio::task::yield_now()`, never a sleep: on a paused clock a sleep would
  move time and spoil the exact figures
- **Durations** print as Rust's `Debug` (`60s`, `0ns`) where .NET prints
  `00:01:00`

## What it does *not* cover

- **The other ways a wait ends**: `with_max_queue_time` (`QueueTimeout`), the
  run's deadline (`Deadline`), its token (`Cancelled`; dropping the run's
  future too) and `close()` (`Closed`). They return the same
  `StrataError::NotAdmitted` with their own reason
- **`with_tenant_max_queued`**, a per-tenant cap on waiting runs
- **`RunScheduler::admit`** for work of your own beside the runs (it returns a
  `RunSlot` that gives the slot back when dropped)
- **Drivers under a scheduler.** The wake and recovery drivers wait for a slot
  per thread and skip a thread that isn't admitted
- **Hosting.** In `datalium-strata-host`, the wake and recovery drivers take a
  `RunConfig` (which can carry the scheduler) and a priority of their own, and
  `Host::close_on_shutdown(scheduler)` closes the scheduler when the host
  stops, so a queued run is refused instead of holding up the shutdown. The
  host itself is in [`28-hosting`](../28-hosting)
- **Telemetry**: the `strata.scheduler.*` metrics and the run span's
  scheduler wait (see [`27-run-observability`](../27-run-observability) for
  listening to the engine's metrics)
- **Preemption, aging, rate limits, and changing `max_concurrent` while
  running.** The scheduler has none of these (design/40, "Not built")

## Run it

```bash
cargo run -p run-scheduler
```

Takes well under a second. Output of a real run:

```
=== Scenario 1: one slot, five runs waiting at three priorities ===
  Arrived in this order: low-1 (p0), high-1 (p5), low-2 (p0), high-2 (p5), mid-1 (p2)
  active 1 of 1, queued 5 of 1024
  Started in this order: high-1, high-2, mid-1, low-1, low-2
    high-1 RunStarted.queued_for = Some(60s)
    high-2 RunStarted.queued_for = Some(120s)
    mid-1  RunStarted.queued_for = Some(180s)
    low-1  RunStarted.queued_for = Some(240s)
    low-2  RunStarted.queued_for = Some(300s)

=== Scenario 2: three slots, one kept for priority 5 and up ===
  batch-1..3 (p0): active 2 of 3, queued 1 of 1024
  batch-3 waits beside a free slot: that slot is the reserve.
  urgent (p5) arrives and starts at once: active 3 of 3, queued 1 of 1024
  urgent finishes; its slot goes back to the reserve: active 2 of 3, queued 1 of 1024
  batch-1 finishes; batch-3 starts: active 2 of 3, queued 0 of 1024

=== Scenario 3: four slots, one run per tenant, initech paused ===
  Arrived: acme-1, acme-2, globex-1, initech-1
  active 2 of 4, queued 2 of 1024
    acme     active 1, queued 1, limit 1
    globex   active 1, queued 0, limit 1
    initech  active 0, queued 1, limit 0
  set_tenant_limit("initech", Some(1)): initech-1 starts. active 3 of 4, queued 1 of 1024
  set_tenant_limit("acme", Some(2)): acme-2 starts. active 4 of 4, queued 0 of 1024

=== Scenario 4: one slot, room for two to wait ===
  blocker running, report-a and report-b (p0) waiting: active 1 of 1, queued 2 of 2
  report-c (p0) arrives: NotAdmitted(QueueFull) after 0ns -- "run not admitted: the scheduler's queue is full"
  page-oncall (p5) arrives 30 s later: report-b NotAdmitted(Shed) after 30s -- "run not admitted: shed from the scheduler's queue for a higher-priority run"
  active 1 of 1, queued 2 of 2
  Started after blocker: page-oncall, report-a
    report-a: 2 checkpoint(s), 6 run event(s)
    report-b: 0 checkpoint(s), 0 run event(s)
    report-c: 0 checkpoint(s), 0 run event(s)

=== Done: priority, a reserve, tenant limits and shedding, each decided before a run touched the store. ===
```

The times are paused-clock time, the same on every run. The demo checks each
claim (start order, exact queue times, snapshot counts, per-tenant usage,
refusal reasons and waits, what the refused runs left) and exits non-zero if
one fails. Run events are delivered by a task of their own, off the run's
path, so before it counts them the demo waits for the `RunCompleted` of every
run that was admitted.

## Files

- `src/main.rs` — the whole demo: the four scenarios, and `Lab`, which holds a
  scenario's scheduler, graph, store, observer and per-run gates
- `Cargo.toml` — package name is `run-scheduler`; depends on `datalium-strata`,
  `datalium-strata-memory` (the in-memory store and its checkpoint log),
  `serde`, and `tokio` with the `test-util` feature for the paused clock
