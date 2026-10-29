//! Run observability (`design/18-Run-Observability.md`): run metadata, run
//! events and telemetry.
//!
//! Three ways to see what a run did, all fed by the same engine hooks:
//!
//!   1. Run metadata -- `RunConfig::with_metadata` (tenant, ticket) is stamped
//!      on every checkpoint the run writes, at every nesting level, and a
//!      resume carries it forward, overriding it key by key. The engine's own
//!      keys are reserved and refused.
//!   2. Run events   -- an observer (here `RunEventChannel`) gets typed
//!      `RunEvent`s across a run with a retry and an interrupt, then across
//!      the resume: each with run id, sequence, node path and attempt.
//!   3. Bad observers -- one that panics and one that is stuck: neither fails
//!      nor stalls the run.
//!   4. Telemetry    -- the engine's `tracing` spans and `metrics` facade,
//!      captured in-process: the `strata.run` -> `strata.node` ->
//!      `strata.attempt` span tree, a handler's own span and an event on its
//!      attempt's span (`NodeContext::span`), and the metrics under
//!      `observability::metric_names`.
//!
//! Everything is deterministic: the "transient failure" is a counter, and the
//! demo prints counts, order and fields, never durations.

mod capture;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use capture::{Measurement, OBSERVER_BUG, PanicsOnFirstEvent, StuckObserver, Trace, measurements};
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{
    CheckpointPolicy, CompileOptions, CompiledGraph, NodeError, RecoveryPolicy, StateGraph,
};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, metric_names};
use datalium_strata::schema::Checkpoint;
use datalium_strata::store::{CheckpointQuery, CheckpointStore};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;

/// A safety net so a broken claim fails instead of hanging. Nothing asserts
/// how long anything took.
const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    amount: i64,
    log: Vec<String>,
}

impl Order {
    fn new(amount: i64) -> Self {
        Self {
            amount,
            log: Vec::new(),
        }
    }

    fn then(mut self, step: &str) -> Self {
        self.log.push(step.to_string());
        self
    }
}

type Metadata = HashMap<String, String>;

// The stuck observer blocks the worker thread it is called on, so the run
// needs others: a multi-thread runtime with a known number of them.
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    capture::install();
    // The panicking observer's panic is caught by the engine; keep its message
    // off stderr so the output shows only what the demo prints.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if info.payload().downcast_ref::<&str>() != Some(&OBSERVER_BUG)
            && info.payload().downcast_ref::<String>().map(String::as_str) != Some(OBSERVER_BUG)
        {
            default_hook(info);
        }
    }));

    let run_events = scenario_1_and_2().await;
    scenario_3(run_events).await;
    scenario_4().await;

    println!();
    println!(
        "=== Done: metadata on every checkpoint, typed events per run, and spans and metrics from the same hooks. ==="
    );
}

async fn scenario_1_and_2() -> usize {
    // --- Scenario 1: run metadata on every checkpoint, at every level, through a resume. ---

    println!("=== Scenario 1: run metadata on every checkpoint ===");
    let seen_by_send: Arc<Mutex<Vec<Metadata>>> = Arc::default();
    let order_graph = order_graph(seen_by_send.clone());
    let store = Arc::new(InMemoryCheckpointStore::new());

    let first = order_graph
        .run_with(
            "order-1001",
            Order::new(5),
            store.clone(),
            RunConfig::new()
                .with_metadata("tenant", "acme")
                .with_metadata("ticket", "T-1001"),
        )
        .await;
    check(
        matches!(&first, GraphExecutionResult::Interrupted { waiting_at, .. } if waiting_at.join("/") == "Work/Approve"),
        "expected the run to stop at Work/Approve",
    );
    println!("  Run with tenant=acme, ticket=T-1001: interrupted at Work/Approve.");

    let resumed = order_graph
        .resume_with(
            "order-1001",
            HumanSignal::approve("reviewer-7"),
            store.clone(),
            RunConfig::new().with_metadata("ticket", "T-1002"),
        )
        .await;
    check(
        matches!(resumed, GraphExecutionResult::Completed(_)),
        "expected the resume to complete",
    );
    println!("  Resumed with ticket=T-1002 only: completed.");
    let seen = seen_by_send.lock().unwrap().clone();
    check(seen.len() == 1, "expected Work/Send to run once");
    println!(
        "  Work/Send saw NodeContext::metadata() = {}",
        show(Some(&seen[0]))
    );
    check(
        show(Some(&seen[0])) == "tenant=acme, ticket=T-1002",
        "expected the resume to carry tenant forward and override ticket",
    );

    // The graph is compiled with CheckpointPolicy::EveryNode, so the log holds a checkpoint per node.
    let log = store
        .get_log("order-1001", None, 100)
        .await
        .expect("the in-memory store answers")
        .entries;
    println!(
        "  The thread's checkpoint log ({} checkpoints; '-> child' is the subgraph's nested checkpoint):",
        log.len()
    );
    for entry in &log {
        let checkpoint = &entry.checkpoint;
        println!(
            "    {:>2}. {:<11} next={:<8} {}",
            entry.sequence,
            format!("{:?}", checkpoint.status),
            checkpoint.next_node.as_deref().unwrap_or("-"),
            show(checkpoint.metadata.as_ref())
        );
        if let Some(nested) = &checkpoint.nested {
            println!(
                "          -> child {:<11} next={:<8} {}",
                format!("{:?}", nested.status),
                nested.next_node.as_deref().unwrap_or("-"),
                show(nested.metadata.as_ref())
            );
        }
    }
    println!(
        "  (strata.hops is the engine's own key -- the hop count it resumes with -- which is why the strata. prefix is reserved.)"
    );
    let every_level: Vec<&Checkpoint> = log.iter().flat_map(|e| levels(&e.checkpoint)).collect();
    check(
        every_level.len() > log.len(),
        "expected some checkpoints to have a nested level",
    );
    check(
        every_level
            .iter()
            .all(|c| meta(c, "tenant") == Some("acme")),
        "expected tenant=acme on every checkpoint at every level",
    );
    let mut tickets: Vec<&str> = every_level
        .iter()
        .filter_map(|c| meta(c, "ticket"))
        .collect();
    tickets.dedup();
    check(
        tickets == ["T-1001", "T-1002"],
        "expected ticket T-1001 before the resume and T-1002 after it",
    );

    for reserved in ["error", "attempts", "strata.trace_id"] {
        let refused = order_graph
            .run_with(
                "order-1002",
                Order::new(1),
                store.clone(),
                RunConfig::new().with_metadata(reserved, "x"),
            )
            .await;
        match refused {
            GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
                println!(
                    "  Metadata key '{reserved}': refused -- Failed(ExecutionAborted(\"{message}\"))"
                );
                check(
                    message == format!("run metadata key '{reserved}' is reserved"),
                    "expected the refusal to name the key",
                );
            }
            _ => check(
                false,
                &format!("expected the reserved key '{reserved}' to be refused"),
            ),
        }
    }
    check(
        store
            .load("order-1002")
            .await
            .expect("the in-memory store answers")
            .is_none(),
        "expected a refused run to write nothing",
    );
    println!("  Nothing was written for the refused runs.");

    // --- Scenario 2: run events across a run (retry, interrupt) and its resume. ---

    println!();
    println!("=== Scenario 2: run events across a retry, an interrupt and a resume ===");
    let review_graph = review_graph();
    let store = Arc::new(InMemoryCheckpointStore::new());

    let (channel, events) = RunEventChannel::new();
    let run = review_graph
        .run_with(
            "order-2001",
            Order::new(40),
            store.clone(),
            RunConfig::new()
                .with_metadata("tenant", "acme")
                .with_observer(channel),
        )
        .await;
    let run_events = drain(events).await;

    // A caller can supply the run id (a job id, a request id); otherwise the engine generates a UUID v7.
    let (channel, events) = RunEventChannel::new();
    let resume = review_graph
        .resume_with(
            "order-2001",
            HumanSignal::approve("reviewer-7"),
            store.clone(),
            RunConfig::new()
                .with_run_id("resume-order-2001")
                .with_observer(channel),
        )
        .await;
    let resume_events = drain(events).await;
    check(
        matches!(run, GraphExecutionResult::Interrupted { .. })
            && matches!(resume, GraphExecutionResult::Completed(_)),
        "expected interrupt, then complete",
    );

    print_events("run_with", &run_events);
    print_events("resume_with", &resume_events);

    check(
        run_events.iter().map(describe).collect::<Vec<_>>()
            == [
                "RunStarted Run at Classify",
                "NodeStarted Classify#1",
                "RetryScheduled Classify#1",
                "NodeCompleted Classify#2 Committed",
                "NodeStarted Approve#1",
                "NodeCompleted Approve#1 Committed",
                "CheckpointSaved Interrupted",
                "Interrupted at Approve, next Execute",
                "RunCompleted Interrupted",
            ],
        "expected the documented event sequence for the run",
    );
    check(
        resume_events.iter().map(describe).collect::<Vec<_>>()
            == [
                "RunStarted Resume at Execute",
                "CheckpointSaved Interrupted (resume stamp)",
                "Resumed by reviewer-7",
                "NodeStarted Execute#1",
                "NodeCompleted Execute#1 Committed",
                "RunCompleted Completed",
            ],
        "expected the documented event sequence for the resume",
    );
    for events in [&run_events, &resume_events] {
        check(
            gapless(events),
            "expected gapless sequences 1, 2, 3, ... per run",
        );
        check(
            events
                .iter()
                .all(|e| e.run_id == events[0].run_id && &*e.thread_id == "order-2001"),
            "expected one run id per run",
        );
    }
    check(
        is_uuid_v7(&run_events[0].run_id),
        "expected a generated UUID v7 run id",
    );
    check(
        &*resume_events[0].run_id == "resume-order-2001",
        "expected the supplied run id on the resume's events",
    );
    let retries: Vec<&RunEvent> = run_events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::RetryScheduled { .. }))
        .collect();
    check(
        retries.len() == 1
            && retries[0].attempt == Some(1)
            && matches!(&retries[0].kind, RunEventKind::RetryScheduled { error, delay, .. }
                if error == "gateway timeout" && delay.is_zero()),
        "expected the retry to name the failed attempt and its error",
    );
    check(
        matches!(&resume_events[0].kind, RunEventKind::RunStarted { metadata, .. }
            if metadata.get("tenant").map(String::as_str) == Some("acme")),
        "expected the resume's RunStarted to carry the stored metadata",
    );
    run_events.len()
}

async fn scenario_3(run_event_count: usize) {
    // --- Scenario 3: a panicking observer and a stuck observer. ---

    println!();
    println!("=== Scenario 3: an observer can't fail or stall the run ===");
    measurements(); // a clean slate for the counter below
    let panicking = Arc::new(PanicsOnFirstEvent::default());
    let result = tokio::time::timeout(
        PATIENCE,
        review_graph().run_with(
            "order-3001",
            Order::new(1),
            Arc::new(InMemoryCheckpointStore::new()),
            RunConfig::new().with_observer(panicking.clone()),
        ),
    )
    .await
    .expect("the run doesn't hang");
    tokio::time::timeout(PATIENCE, panicking.done.notified())
        .await
        .expect("the observer gets RunCompleted");
    let errors: u64 = measurements()
        .iter()
        .filter(|m| m.name == metric_names::OBSERVER_ERRORS)
        .map(Measurement::count)
        .sum();
    let seen = panicking.seen.lock().unwrap().clone();
    println!(
        "  Panicking observer: the run ended {}; the observer panicked on RunStarted,",
        outcome(&result)
    );
    println!(
        "    still got the other {} events, and strata.observer.errors counted {errors}.",
        seen.len()
    );
    check(
        matches!(result, GraphExecutionResult::Interrupted { .. }),
        "expected the run to be unaffected by a panicking observer",
    );
    check(
        seen.len() == run_event_count - 1 && matches!(seen[0].kind, RunEventKind::NodeStarted),
        "expected every event after the one that panicked",
    );
    check(
        errors == 1,
        "expected the observer's panic to be counted once",
    );

    let stuck = Arc::new(StuckObserver::new(PATIENCE));
    let result = tokio::time::timeout(
        PATIENCE,
        review_graph().run_with(
            "order-3002",
            Order::new(1),
            Arc::new(InMemoryCheckpointStore::new()),
            RunConfig::new().with_observer(stuck.clone()),
        ),
    )
    .await
    .expect("the run doesn't wait for its observer");
    let seen_while_stuck = stuck.seen.lock().unwrap().len();
    println!(
        "  Stuck observer: run_with returned ({}) while the observer had taken in {seen_while_stuck} events.",
        outcome(&result)
    );
    stuck.open();
    tokio::time::timeout(PATIENCE, stuck.done.notified())
        .await
        .expect("the observer gets RunCompleted once released");
    let seen = stuck.seen.lock().unwrap().clone();
    println!(
        "    Once unblocked, it received all {}, in sequence order.",
        seen.len()
    );
    check(
        seen_while_stuck == 0,
        "expected the run to finish while the observer was still stuck on its first event",
    );
    check(
        seen.len() == run_event_count && gapless(&seen),
        "expected the stuck observer to get every event, in order, once released",
    );
}

async fn scenario_4() {
    // --- Scenario 4: telemetry from the same hooks. ---

    println!();
    println!("=== Scenario 4: telemetry -- the span tree and the metrics ===");
    measurements(); // a clean slate: only this run and resume from here
    let store = Arc::new(InMemoryCheckpointStore::new());
    let graph = review_graph();
    graph
        .run_with(
            "order-4001",
            Order::new(40),
            store.clone(),
            RunConfig::new()
                .with_metadata("tenant", "acme")
                .with_run_id("run-4001"),
        )
        .await;
    graph
        .resume_with(
            "order-4001",
            HumanSignal::approve("reviewer-7"),
            store.clone(),
            RunConfig::new().with_run_id("resume-4001"),
        )
        .await;
    let all = measurements();

    let run_trace = Trace::of("order-4001", "strata.run");
    let resume_trace = Trace::of("order-4001", "strata.resume");
    println!("  run_with's trace (display name [span name] fields, then its events):");
    print_tree(&run_trace, run_trace.root, 0);
    println!("  resume_with's trace (a separate root span: there is no link across the freeze):");
    print_tree(&resume_trace, resume_trace.root, 0);

    let root = &span_at(&run_trace, run_trace.root);
    let classify = run_trace.one("Classify");
    let attempt1 = run_trace.one("Classify #1");
    let attempt2 = run_trace.one("Classify #2");
    let own_spans = run_trace.all("classify-model-call");
    check(
        root.parent.is_none()
            && root.field("strata.metadata") == Some(r#"{"tenant":"acme"}"#)
            && root.field("strata.run_id") == Some("run-4001")
            && root.field("strata.outcome") == Some("interrupted"),
        "expected the metadata, run id and outcome on the run span",
    );
    let (Some(classify), Some(attempt1), Some(attempt2)) = (classify, attempt1, attempt2) else {
        fail("expected one Classify node span and two attempt spans");
    };
    check(
        classify.1.name == "strata.node" && classify.1.parent == Some(run_trace.root),
        "expected the node span under the run span",
    );
    check(
        attempt1.1.name == "strata.attempt"
            && attempt1.1.parent == Some(classify.0)
            && attempt2.1.parent == Some(classify.0),
        "expected both attempts under the node span",
    );
    check(
        attempt1.1.field("otel.status_code") == Some("ERROR")
            && attempt1.1.field("error.type") == Some("transient"),
        "expected the failed attempt marked",
    );
    check(
        classify.1.field("strata.retry_count") == Some("1")
            && run_trace
                .events_in(classify.0)
                .any(|e| e.message == "retry scheduled: gateway timeout"),
        "expected the retry on the node span",
    );
    check(
        run_trace.events_in(attempt2.0).any(|e| {
            e.message == "classified"
                && e.fields.get("order.risk").map(String::as_str) == Some("low")
        }),
        "expected the handler's event, with its field, on its own attempt span",
    );
    check(
        own_spans.len() == 2
            && own_spans[0].1.parent == Some(attempt1.0)
            && own_spans[1].1.parent == Some(attempt2.0),
        "expected the handler's own spans under their attempts",
    );
    let resume_root = span_at(&resume_trace, resume_trace.root);
    check(
        resume_root.parent.is_none()
            && resume_root.field("strata.run_id") == Some("resume-4001")
            && resume_root.field("strata.metadata") == Some(r#"{"tenant":"acme"}"#)
            && resume_trace.one("Execute #1").is_some()
            && run_trace.one("Execute #1").is_none(),
        "expected the resume in its own trace, with the stored metadata",
    );

    println!("  Metrics recorded during that run and resume (counts, never seconds):");
    let of = |name: &str| -> Vec<&Measurement> { all.iter().filter(|m| m.name == name).collect() };
    let started = |mode: &str| -> u64 {
        of(metric_names::GRAPHS_STARTED)
            .iter()
            .filter(|m| m.label("strata.run.mode") == mode)
            .map(|m| m.count())
            .sum()
    };
    println!(
        "    {:<26} run={} resume={}",
        metric_names::GRAPHS_STARTED,
        started("run"),
        started("resume")
    );
    let mut runs: Vec<String> = of(metric_names::GRAPH_DURATION)
        .iter()
        .map(|m| {
            format!(
                "{} {}",
                m.label("strata.run.mode"),
                m.label("strata.outcome")
            )
        })
        .collect();
    runs.sort();
    println!(
        "    {:<26} {}",
        metric_names::GRAPH_DURATION,
        runs.join(", ")
    );
    let retries = of(metric_names::NODE_RETRIES);
    for m in &retries {
        println!(
            "    {:<26} {} at {} (error.type={})",
            metric_names::NODE_RETRIES,
            m.count(),
            m.label("strata.node.path"),
            m.label("error.type")
        );
    }
    let transitions = of(metric_names::GRAPH_TRANSITION);
    println!(
        "    {:<26} {}",
        metric_names::GRAPH_TRANSITION,
        transitions
            .iter()
            .map(|m| format!(
                "{} -> {}",
                m.label("strata.node.path"),
                m.label("strata.next_node.path")
            ))
            .collect::<Vec<_>>()
            .join(", ")
    );
    // A snapshot groups measurements by series, not by time: list them in graph order.
    let node_order = ["Classify", "Approve", "Execute"];
    let mut durations = of(metric_names::NODE_DURATION);
    durations.sort_by_key(|m| {
        node_order
            .iter()
            .position(|n| *n == m.label("strata.node.path"))
    });
    println!(
        "    {:<26} {} measurements: {}",
        metric_names::NODE_DURATION,
        durations.iter().map(|m| m.count()).sum::<u64>(),
        durations
            .iter()
            .map(|m| format!(
                "{} ({})",
                m.label("strata.node.path"),
                m.label("strata.node.result")
            ))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut checkpoints: Vec<String> = of(metric_names::CHECKPOINT_LATENCY)
        .iter()
        .map(|m| {
            format!(
                "{} {} x{}",
                m.label("strata.checkpoint.operation"),
                m.label("strata.checkpoint.status"),
                m.count()
            )
        })
        .collect();
    checkpoints.sort_by(|a, b| b.cmp(a)); // saves first
    println!(
        "    {:<26} {}",
        metric_names::CHECKPOINT_LATENCY,
        checkpoints.join(", ")
    );

    check(
        started("run") == 1 && started("resume") == 1,
        "expected one run and one resume started",
    );
    check(
        runs == ["resume completed", "run interrupted"],
        "expected one run duration per run, by mode and outcome",
    );
    check(
        retries.len() == 1
            && retries[0].count() == 1
            && retries[0].label("strata.node.path") == "Classify"
            && retries[0].label("error.type") == "transient",
        "expected one retry, at Classify",
    );
    check(
        transitions.iter().any(|m| {
            m.label("strata.node.path") == "Classify"
                && m.label("strata.next_node.path") == "Approve"
        }),
        "expected the Classify -> Approve transition",
    );
    check(
        durations
            .iter()
            .map(|m| {
                (
                    m.label("strata.node.path"),
                    m.label("strata.node.result"),
                    m.count(),
                )
            })
            .collect::<Vec<_>>()
            == [
                ("Classify", "committed", 1),
                ("Approve", "committed", 1),
                ("Execute", "committed", 1),
            ],
        "expected one node duration per node, retries included in the node's",
    );
    check(
        checkpoints
            .iter()
            .any(|c| c.starts_with("save interrupted"))
            && checkpoints.iter().any(|c| c.starts_with("load ")),
        "expected the freeze's save and the resume's load measured",
    );
}

// Prepare -> Work (a subgraph: Draft -> Approve, a boundary -> Send) -> Finish,
// with a checkpoint after every node so the log shows the metadata on each.
fn order_graph(seen_by_send: Arc<Mutex<Vec<Metadata>>>) -> CompiledGraph<Order> {
    let child = StateGraph::<Order>::new()
        .add_node("Draft", |o: Order| o.then("draft"))
        .add_node("Approve", |o: Order| o.then("approve"))
        .add_interrupt_boundary("Approve")
        .add_async_node("Send", move |o: Order, ctx: NodeContext| {
            seen_by_send.lock().unwrap().push(ctx.metadata().clone());
            async move { o.then("send") }
        })
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Send")
        .compile()
        .expect("the child graph is valid");
    StateGraph::<Order>::new()
        .add_node("Prepare", |o: Order| o.then("prepare"))
        .add_subgraph(
            "Work",
            child,
            |o: &Order| o.clone(),
            |_: &Order, o: Order| o,
        )
        .add_node("Finish", |o: Order| o.then("finish"))
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .expect("the order graph is valid")
}

// Classify (fails transiently once, retry(1)) -> Approve (a boundary) -> Execute.
// Classify opens a span of its own (as a model client would) and records an
// event on its attempt's span through NodeContext::span.
fn review_graph() -> CompiledGraph<Order> {
    let calls = Arc::new(AtomicU32::new(0));
    StateGraph::<Order>::new()
        .add_async_fallible_node("Classify", move |o: Order, ctx: NodeContext| {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                {
                    // The handler runs inside its attempt's span, so this nests under it.
                    let _call = tracing::info_span!("classify-model-call").entered();
                    if first {
                        return Err(NodeError::Transient("gateway timeout".to_string()));
                    }
                }
                let risk = if o.amount > 100 { "high" } else { "low" };
                tracing::info!(parent: ctx.span(), order.risk = risk, "classified");
                Ok(o.then("classify"))
            }
        })
        .add_node("Approve", |o: Order| o.then("approve"))
        .add_interrupt_boundary("Approve")
        .add_node("Execute", |o: Order| o.then("execute"))
        .add_edge("Classify", "Approve")
        .add_edge("Approve", "Execute")
        .set_recovery_policy("Classify", RecoveryPolicy::retry(1))
        .compile()
        .expect("the review graph is valid")
}

fn levels(checkpoint: &Checkpoint) -> Vec<&Checkpoint> {
    let mut levels = vec![checkpoint];
    let mut level = checkpoint;
    while let Some(nested) = &level.nested {
        levels.push(nested);
        level = nested;
    }
    levels
}

fn meta<'a>(checkpoint: &'a Checkpoint, key: &str) -> Option<&'a str> {
    checkpoint.metadata.as_ref()?.get(key).map(String::as_str)
}

/// Every event the channel delivers; it ends after `RunCompleted`.
async fn drain(mut events: UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv())
        .await
        .expect("the channel ends after RunCompleted")
    {
        all.push(event);
    }
    all
}

fn gapless(events: &[RunEvent]) -> bool {
    events
        .iter()
        .map(|e| e.sequence)
        .eq(1..=events.len() as u64)
}

fn print_events(call: &str, events: &[RunEvent]) {
    let run_id = if is_uuid_v7(&events[0].run_id) {
        "a generated UUID v7"
    } else {
        &events[0].run_id
    };
    println!(
        "  {call} -- thread {}, run id {run_id}:",
        events[0].thread_id
    );
    for e in events {
        println!("    #{} {}{}", e.sequence, describe(e), detail(e));
    }
}

/// The kind, node path and attempt of an event: what the checks compare.
fn describe(e: &RunEvent) -> String {
    let path = e.node_path.as_deref().unwrap_or_default();
    let attempt = e.attempt.unwrap_or_default();
    match &e.kind {
        RunEventKind::RunStarted { mode, node, .. } => format!("RunStarted {mode:?} at {node}"),
        RunEventKind::Resumed { reviewer, .. } => format!("Resumed by {reviewer}"),
        RunEventKind::NodeStarted => format!("NodeStarted {path}#{attempt}"),
        RunEventKind::RetryScheduled { .. } => format!("RetryScheduled {path}#{attempt}"),
        RunEventKind::NodeCompleted { result, .. } => {
            format!("NodeCompleted {path}#{attempt} {result:?}")
        }
        RunEventKind::CheckpointSaved {
            status,
            resume_stamp,
            ..
        } => format!(
            "CheckpointSaved {status:?}{}",
            if *resume_stamp { " (resume stamp)" } else { "" }
        ),
        RunEventKind::Interrupted {
            waiting_at,
            next_node,
            ..
        } => format!("Interrupted at {}, next {next_node}", waiting_at.join("/")),
        RunEventKind::RunCompleted { outcome, .. } => format!("RunCompleted {outcome:?}"),
        // Non-exhaustive: later versions add kinds.
        other => format!("{other:?}"),
    }
}

/// What else an event carries, for printing only (durations are left out: they vary).
fn detail(e: &RunEvent) -> String {
    match &e.kind {
        RunEventKind::RunStarted { metadata, .. } if !metadata.is_empty() => {
            format!("  metadata: {}", show(Some(metadata)))
        }
        RunEventKind::RetryScheduled { error, delay, .. } => {
            format!("  error: {error}, delay: {} ms", delay.as_millis())
        }
        RunEventKind::Resumed {
            from,
            waiting_at,
            is_approved,
            ..
        } => format!(
            "  from {from:?} at {}, approved: {is_approved}",
            waiting_at.join("/")
        ),
        _ => String::new(),
    }
}

fn span_at(trace: &Trace, index: usize) -> capture::SpanRecord {
    trace
        .spans
        .iter()
        .find(|(i, _)| *i == index)
        .map(|(_, s)| s.clone())
        .expect("the root is in its own trace")
}

fn print_tree(trace: &Trace, index: usize, depth: usize) {
    let span = span_at(trace, index);
    println!(
        "    {}{} [{}]{}",
        "  ".repeat(depth),
        span.display(),
        span.name,
        fields(&span, trace, index)
    );
    let children: Vec<usize> = trace.children(index).map(|(i, _)| *i).collect();
    for child in children {
        print_tree(trace, child, depth + 1);
    }
}

/// The fields worth printing (ids and timings left out), then the span's events.
fn fields(span: &capture::SpanRecord, trace: &Trace, index: usize) -> String {
    let mut shown: Vec<String> = [
        "strata.outcome",
        "strata.metadata",
        "strata.run_id",
        "strata.retry_count",
        "error.type",
        "otel.status_code",
    ]
    .iter()
    // Every engine span carries the run id: show it on the run span only.
    .filter(|key| **key != "strata.run_id" || span.name == "strata.run")
    .filter_map(|key| span.field(key).map(|value| format!("{key}={value}")))
    .collect();
    for event in trace.events_in(index) {
        let mut extra: Vec<String> = event
            .fields
            .iter()
            .filter(|(key, _)| !key.starts_with("strata.") && *key != "node" && *key != "outcome")
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        extra.sort();
        if extra.is_empty() {
            shown.push(format!("event:\"{}\"", event.message));
        } else {
            shown.push(format!(
                "event:\"{}\"{{{}}}",
                event.message,
                extra.join(" ")
            ));
        }
    }
    if shown.is_empty() {
        String::new()
    } else {
        format!("  {}", shown.join(" "))
    }
}

fn show(metadata: Option<&Metadata>) -> String {
    match metadata {
        Some(metadata) if !metadata.is_empty() => {
            let mut pairs: Vec<String> = metadata.iter().map(|(k, v)| format!("{k}={v}")).collect();
            pairs.sort();
            pairs.join(", ")
        }
        _ => "(none)".to_string(),
    }
}

fn outcome(result: &GraphExecutionResult<Order>) -> &'static str {
    match result {
        GraphExecutionResult::Completed(_) => "completed",
        GraphExecutionResult::Interrupted { .. } => "interrupted",
        GraphExecutionResult::Failed(_) => "failed",
        GraphExecutionResult::Cancelled { .. } => "cancelled",
    }
}

/// A hyphenated UUID whose version digit is 7.
fn is_uuid_v7(id: &str) -> bool {
    let bytes = id.as_bytes();
    id.len() == 36
        && [8, 13, 18, 23].iter().all(|&i| bytes[i] == b'-')
        && bytes[14] == b'7'
        && id
            .chars()
            .enumerate()
            .all(|(i, c)| [8, 13, 18, 23].contains(&i) || c.is_ascii_hexdigit())
}

fn check(ok: bool, what: &str) {
    if !ok {
        fail(what);
    }
}

fn fail(what: &str) -> ! {
    eprintln!("check failed: {what}");
    std::process::exit(1);
}
