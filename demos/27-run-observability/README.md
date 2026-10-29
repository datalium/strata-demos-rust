# 27 — run-observability

Run metadata, run events and telemetry (`design/18-Run-Observability.md`):
three ways to see what a run did, all fed by the same engine hooks. Caller
metadata (`tenant`, `ticket`) rides on every checkpoint. An observer receives
typed `RunEvent`s for a run with a retry and an interrupt, then for its resume.
Two badly behaved observers show that an observer can't fail or stall a run.
And the engine's `tracing` spans and `metrics` facade, captured in-process,
give the span tree and the metrics.

Everything is deterministic: the "transient failure" is a counter, and the
demo prints counts, order and fields, never durations. The .NET twin is
[`strata-demos-dotnet/demos/27-run-observability`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/27-run-observability);
same story, same four scenarios, and the checkpoint log and event sequences
print the same.

## What it demonstrates

- **Run metadata on every checkpoint, at every level** (scenario 1).
  `RunConfig::with_metadata` is stamped on each checkpoint the run writes, and
  on the nested checkpoint of a subgraph (`Work`) inside it. The graph is
  compiled with `CompileOptions::with_checkpointing(CheckpointPolicy::EveryNode)`
  (from `design/33-Durable-Execution.md`) so the thread's log
  (`CheckpointQuery::get_log`) has a checkpoint per node to look at, including
  two-level ones
- **A resume carries the metadata forward and overrides it key by key.** The
  resume passes only `ticket=T-1002`; the checkpoints it writes, and
  `NodeContext::metadata()` in `Work/Send`, have `tenant=acme` (carried) and
  `ticket=T-1002` (overridden). The earlier checkpoints keep `T-1001`
- **Reserved keys are refused** before anything is written: the engine's own
  keys (`error`, `error_kind`, `attempts`, `cancel_reason`) and anything under
  `strata.`. In Rust the refusal is the run's result,
  `GraphExecutionResult::Failed(StrataError::ExecutionAborted(..))`, where .NET
  throws an `ArgumentException`. The log shows why the prefix is kept: the
  engine already writes `strata.hops` (the hop count a resume continues from,
  `design/24-Loop-Circuit-Breaker.md`) into the same map
- **Typed run events** (scenario 2), through `RunEventChannel::new()` (an
  observer for one run plus the `UnboundedReceiver` its events arrive on,
  which ends after `RunCompleted`): `RunStarted` (with the metadata),
  `NodeStarted`, `RetryScheduled` (the failed attempt, its error and the
  backoff delay, zero here), `NodeCompleted`, `CheckpointSaved`, `Interrupted`,
  `RunCompleted`, and for the resume the stamp, `Resumed` (who approved) and
  the rest. Each `RunEvent` has a `thread_id`, a `run_id`, a per-run
  `sequence` (1, 2, 3, ... with no gaps), a `node_path` and an `attempt`. The
  run id is a generated UUID v7 unless the caller supplies one
  (`RunConfig::with_run_id`, here `resume-order-2001`); a run and its resume
  are two runs, with two ids
- **An observer can't fail or stall the run** (scenario 3). A Rust observer
  can't throw, so the buggy one panics on its first event: the engine catches
  the panic, the run's result is untouched, the observer still gets every
  later event, and the `strata.observer.errors` metric counts it. (The demo's
  panic hook keeps that one panic's message off stderr.) The stuck one blocks
  inside `on_event` on a `Condvar`: it has received nothing when `run_with`
  returns, and released, it receives all nine events in order. Delivery runs
  off the executor, through an unbounded per-run queue and a pump task
- **The span tree** (scenario 4), from the engine's `tracing` spans, collected
  with a `tracing-subscriber` layer: `strata.run` -> `strata.node`
  (`Classify`) -> `strata.attempt` (`Classify #1`, `Classify #2`). What a
  trace viewer shows is the `otel.name` field (`Classify #1`); the span's own
  name is in brackets. The failed attempt has `error.type=transient` and
  `otel.status_code=ERROR`, the node span a `retry scheduled` event and
  `strata.retry_count`, the run span the metadata (`strata.metadata`, as
  JSON), the run id and the outcome. The resume is its own root span, shown
  as `strata.resume`
- **A handler's own spans and events.** The handler runs inside its attempt's
  span, so a span it opens (`classify-model-call`) nests under that attempt,
  and `NodeContext::span()` hands it the attempt's span to record an event on
  (`classified` with `order.risk=low`)
- **Metrics** through the `metrics` facade, under the names in
  `observability::metric_names`, collected with `metrics-util`'s
  `DebuggingRecorder`: `strata.graphs.started` by mode,
  `strata.graph.duration` by mode and outcome, `strata.node.retries` by node
  and error type, `strata.graph.transition`, `strata.node.duration` (one per
  node, its retries included) and `strata.checkpoint.latency` by operation and
  status. The demo prints which series were recorded, their labels and how
  many measurements each got, not their values in seconds

No exporter is needed for any of this: `tracing` and `metrics` are global
facades, so installing a subscriber and a recorder is all it takes for
Strata's spans and metrics to arrive (there is no `AddSource` /
`AddInstrumentation` step in Rust). An OpenTelemetry pipeline would install
`tracing-opentelemetry` and an OTLP or Prometheus metrics recorder instead.

### Where the Rust engine differs from the .NET twin

- **A handler can't tag its attempt's span.** A `tracing` span's fields are
  fixed when it is created, so where .NET's handler calls
  `NodeContext.Activity?.SetTag("order.risk", ..)`, the Rust handler records an
  event on `ctx.span()` carrying the field; an OpenTelemetry layer exports it
  as a span event with that attribute
- **No trace ids.** `tracing` has no trace id of its own; "a separate trace" is
  a separate root span (the resume's span has no parent), which an
  OpenTelemetry layer exports as a new trace
- **The metadata is one span field**, `strata.metadata={"tenant":"acme"}`,
  where .NET has a tag per key (`strata.metadata.tenant=acme`)
- **The run span has more events**: `run started` and `run completed` beside
  `checkpoint saved`
- **Metrics are read as series, not a stream.** A `DebuggingRecorder`
  snapshot groups measurements by name and labels and drains them, so the
  demo takes one snapshot as a clean slate and one after; node durations are
  listed in graph order, and the checkpoint latencies as a count per series
  (`save interrupted x2, load interrupted x1`) where .NET lists the three
  measurements in the order they happened
- **The stuck observer parks a tokio worker.** `on_event` is called on a
  tokio task and must not block; the demo blocks on purpose, so it runs on a
  multi-thread runtime with four workers. Real code hands slow work to a
  channel (`RunEventChannel`)

## What it does *not* cover

- **An exporter.** Nothing is sent to a collector; the demo prints what the
  subscriber and recorder received. [`28-hosting`](../28-hosting) leaves it
  out too
- **The other events**: parallel branches (`BranchStarted`,
  `BranchCompleted`, `JoinDecided`), maps (`MapExpanded`), leases, the hop
  limit, recovery, and tool and model calls. Demos that use those features
  show them: [`26-tool-registry-and-middleware`](../26-tool-registry-and-middleware)
  prints `ToolCallStarted` / `ToolCallCompleted`
- **Streaming runs.** `run_streaming_with` / `resume_streaming_with` emit the
  same sequence; this demo uses `run_with` / `resume_with`
- **Querying threads by metadata** (`ThreadQuery`'s metadata filter,
  `strata thread list --meta`)
- **Trace links across a freeze.** The resume's span isn't linked to the
  run's; the engine doesn't build that yet

## Run it

```bash
cargo run -p run-observability
```

Expected output. Only the generated run id of scenario 2 varies, so the demo
checks it is a UUID v7 and prints a placeholder:

```
=== Scenario 1: run metadata on every checkpoint ===
  Run with tenant=acme, ticket=T-1001: interrupted at Work/Approve.
  Resumed with ticket=T-1002 only: completed.
  Work/Send saw NodeContext::metadata() = tenant=acme, ticket=T-1002
  The thread's checkpoint log (7 checkpoints; '-> child' is the subgraph's nested checkpoint):
     0. Running     next=Prepare  strata.hops=0, tenant=acme, ticket=T-1001
     1. Running     next=Work     strata.hops=1, tenant=acme, ticket=T-1001
     2. Running     next=Work     strata.hops=1, tenant=acme, ticket=T-1001
          -> child Running     next=Approve  strata.hops=1, tenant=acme, ticket=T-1001
     3. Interrupted next=Work     strata.hops=1, tenant=acme, ticket=T-1001
          -> child Interrupted next=Send     strata.hops=2, tenant=acme, ticket=T-1001
     4. Running     next=Work     strata.hops=1, tenant=acme, ticket=T-1002
          -> child Running     next=Send     strata.hops=2, tenant=acme, ticket=T-1002
     5. Running     next=Finish   strata.hops=2, tenant=acme, ticket=T-1002
     6. Completed   next=-        strata.hops=3, tenant=acme, ticket=T-1002
  (strata.hops is the engine's own key -- the hop count it resumes with -- which is why the strata. prefix is reserved.)
  Metadata key 'error': refused -- Failed(ExecutionAborted("run metadata key 'error' is reserved"))
  Metadata key 'attempts': refused -- Failed(ExecutionAborted("run metadata key 'attempts' is reserved"))
  Metadata key 'strata.trace_id': refused -- Failed(ExecutionAborted("run metadata key 'strata.trace_id' is reserved"))
  Nothing was written for the refused runs.

=== Scenario 2: run events across a retry, an interrupt and a resume ===
  run_with -- thread order-2001, run id a generated UUID v7:
    #1 RunStarted Run at Classify  metadata: tenant=acme
    #2 NodeStarted Classify#1
    #3 RetryScheduled Classify#1  error: gateway timeout, delay: 0 ms
    #4 NodeCompleted Classify#2 Committed
    #5 NodeStarted Approve#1
    #6 NodeCompleted Approve#1 Committed
    #7 CheckpointSaved Interrupted
    #8 Interrupted at Approve, next Execute
    #9 RunCompleted Interrupted
  resume_with -- thread order-2001, run id resume-order-2001:
    #1 RunStarted Resume at Execute  metadata: tenant=acme
    #2 CheckpointSaved Interrupted (resume stamp)
    #3 Resumed by reviewer-7  from Interrupted at Approve, approved: true
    #4 NodeStarted Execute#1
    #5 NodeCompleted Execute#1 Committed
    #6 RunCompleted Completed

=== Scenario 3: an observer can't fail or stall the run ===
  Panicking observer: the run ended interrupted; the observer panicked on RunStarted,
    still got the other 8 events, and strata.observer.errors counted 1.
  Stuck observer: run_with returned (interrupted) while the observer had taken in 0 events.
    Once unblocked, it received all 9, in sequence order.

=== Scenario 4: telemetry -- the span tree and the metrics ===
  run_with's trace (display name [span name] fields, then its events):
    strata.run [strata.run]  strata.outcome=interrupted strata.metadata={"tenant":"acme"} strata.run_id=run-4001 event:"run started" event:"checkpoint saved" event:"run completed"
      Classify [strata.node]  strata.retry_count=1 event:"retry scheduled: gateway timeout"{error.type=transient}
        Classify #1 [strata.attempt]  error.type=transient otel.status_code=ERROR
          classify-model-call [classify-model-call]
        Classify #2 [strata.attempt]  event:"classified"{order.risk=low}
          classify-model-call [classify-model-call]
      Approve [strata.node]  strata.retry_count=0
        Approve #1 [strata.attempt]
  resume_with's trace (a separate root span: there is no link across the freeze):
    strata.resume [strata.run]  strata.outcome=completed strata.metadata={"tenant":"acme"} strata.run_id=resume-4001 event:"run started" event:"checkpoint saved" event:"run completed"
      Execute [strata.node]  strata.retry_count=0
        Execute #1 [strata.attempt]
  Metrics recorded during that run and resume (counts, never seconds):
    strata.graphs.started      run=1 resume=1
    strata.graph.duration      resume completed, run interrupted
    strata.node.retries        1 at Classify (error.type=transient)
    strata.graph.transition    Classify -> Approve
    strata.node.duration       3 measurements: Classify (committed), Approve (committed), Execute (committed)
    strata.checkpoint.latency  save interrupted x2, load interrupted x1

=== Done: metadata on every checkpoint, typed events per run, and spans and metrics from the same hooks. ===
```

The demo checks each claim above (the metadata on every level of every
checkpoint, the refused keys, the exact event sequences, gapless sequence
numbers, the run ids, both observers, the span parents, fields and events,
and the metrics) and exits non-zero if one fails.

## Files

- `src/main.rs` — the four scenarios and the two graphs
- `src/capture.rs` — the `tracing-subscriber` layer that keeps every span and
  event, the `DebuggingRecorder` the metrics go to, and the two bad observers
  (the capture pattern of `rust/strata-core/tests/telemetry.rs` in `datalium/strata`)
- `Cargo.toml` — `datalium-strata` and `datalium-strata-memory` (for
  `InMemoryCheckpointStore`) from crates.io, plus
  `tracing-subscriber` (`registry`, `std`) and `metrics-util` (`debugging`)
  for the capture side only
