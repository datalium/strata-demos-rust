//! Model routes: failover and hedging over ordered candidates
//! (`design/46-Model-Routes.md`).
//!
//! A `ModelRoute` is a `ChatClient` over a list of candidate clients. It asks
//! them in order and moves on after a transient fault, a refusal (finish
//! reason `content_filter`) or an answer its `accept` rejects. A fatal fault
//! (a 4xx such as 401) stops it. With `with_hedge_after`, a candidate that is
//! slow to answer gets the next one started beside it, and the first accepted
//! answer wins. A shared `ModelHealth` skips a provider that is cooling down
//! after a `Retry-After` or whose breaker is open. When nothing answers, the
//! node fails transiently with the shortest wait as its retry hint.
//!
//! Every route here sits behind an ordinary model node (`add_model_node`,
//! which calls `complete_with`), so each attempt is a model call of the run,
//! and the route records one `ModelRouted` run event saying which candidate
//! answered and what happened to the others. The demo reads those events.
//!
//! No live model: every candidate is a `FakeModel`, a scripted `ChatClient`
//! written below that answers, fails with a status and a `Retry-After`,
//! refuses, or hangs. The whole demo runs on a paused tokio clock
//! (`start_paused`), which `ModelHealth`, the hedge timer and the retry
//! backoff all read: time only moves when every task is waiting on a timer,
//! so nothing is slept through and every figure is exact.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, Jitter, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind};
use datalium_strata::store::CheckpointStore;
use datalium_strata::{BoxFuture, StrataError};
use datalium_strata_ai::{
    ChatClient, ChatError, ChatRequest, ChatResponse, ChatStream, Message, ModelCandidate,
    ModelHealth, ModelHealthEntry, ModelHealthOptions, ModelHealthState, ModelNodeOptions,
    ModelRoute, ModelRouteOutcome, ModelUsage, StateGraphAiExt,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// How long (on the paused clock) the demo waits for a run's events before
/// calling it a failure.
const PATIENCE: Duration = Duration::from_secs(10);

const ROLL_BACK: &str = "Roll back deploy 412, because the error rate rose with it.";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Ticket {
    question: String,
    answer: String,
    summary: String,
}

fn ticket() -> Ticket {
    Ticket {
        question: "Why is checkout failing?".to_string(),
        ..Ticket::default()
    }
}

/// One `ModelRouted` event, as the demo reads it.
struct Routed {
    node_path: String,
    attempt: u32,
    route: String,
    answered: Option<String>,
    model: Option<String>,
    attempts: Vec<(String, ModelRouteOutcome, Option<String>, Option<String>)>,
    hedges: u32,
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    scenario_1_failover().await;
    scenario_2_fatal().await;
    scenario_3_hedging().await;
    scenario_4_eligible().await;
    scenario_5_shared_breaker().await;
    scenario_6_exhausted().await;

    println!();
    println!(
        "=== Done: the route failed over, stopped on a fatal fault, hedged, skipped by rule and by health, and handed its hint to the retry. ==="
    );
}

// --- Scenario 1: failover on a transient fault, a refusal and a rejected answer. ---

async fn scenario_1_failover() {
    println!("=== Scenario 1: failover past a 429, a refusal and a rejected answer ===");
    let primary = FakeModel::new(
        "northwind",
        "nw-large",
        vec![Turn::fail(429, Some(Duration::from_secs(30)), None)],
    );
    let careful = FakeModel::new(
        "contoso",
        "ct-guarded",
        vec![Turn::refuse("I can't help with production systems.")],
    );
    let terse = FakeModel::new("fabrikam", "fb-mini", vec![Turn::answer("Restart it.")]);
    let local = FakeModel::new("local", "llama-local", vec![Turn::answer(ROLL_BACK)]);
    let triage = ModelRoute::new("triage")
        .candidate(ModelCandidate::new("primary", primary))
        .candidate(ModelCandidate::new("careful", careful))
        .candidate(ModelCandidate::new("terse", terse))
        .candidate(ModelCandidate::new("local", local))
        // The application's rule for a usable answer: it must give a reason.
        .with_accept(|response| {
            if response.text().to_lowercase().contains("because") {
                Ok(())
            } else {
                Err("it gives no reason".to_string())
            }
        });
    let health = triage.health().clone();

    let (result, events) = run(&ask_graph(triage, None), "failover", ticket(), &[]).await;
    let answer = expect_completed(result).answer;
    println!("  Answer: {answer}");
    println!("  Model calls in the run (one per attempt, each naming the route):");
    for event in &events {
        if let Some(line) = describe(event) {
            println!("    {line}");
        }
    }

    let routed = print_routed(&events);
    print_health(&health);
    check(
        answer.starts_with("Roll back deploy 412"),
        "expected local's answer",
    );
    check(
        outcomes(&routed)
            == "primary:failed:429, careful:refused, terse:rejected:it gives no reason, local:answered",
        "expected primary to fail, careful to refuse, terse to be rejected and local to answer",
    );
    check(
        routed.answered.as_deref() == Some("local")
            && routed.model.as_deref() == Some("llama-local"),
        "expected local (llama-local) to answer",
    );
    let started: Vec<(String, Option<String>)> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::ModelCallStarted { call_id, route, .. } => {
                Some((call_id.clone(), route.clone()))
            }
            _ => None,
        })
        .collect();
    check(
        started.len() == 4
            && started
                .iter()
                .all(|(_, route)| route.as_deref() == Some("triage")),
        "expected four model calls, each in the route",
    );
    let attempt_ids: Vec<Option<String>> = routed.attempts.iter().map(|a| a.3.clone()).collect();
    let started_ids: Vec<Option<String>> = started.into_iter().map(|(id, _)| Some(id)).collect();
    check(
        attempt_ids == started_ids,
        "expected each attempt to name its model call",
    );
    // Only the transient fault counts against health: a refusal or a
    // rejection says nothing about availability.
    check(
        health_of(&health, "northwind").is_some_and(|e| {
            matches!(e.state, ModelHealthState::CoolingDown { .. }) && e.failures == 1
        }),
        "expected northwind to cool down after its Retry-After",
    );
    check(
        health_of(&health, "contoso")
            .is_some_and(|e| e.state == ModelHealthState::Available && e.failures == 0),
        "expected a refusal to leave contoso's health alone",
    );
    check(
        health_of(&health, "fabrikam")
            .is_some_and(|e| e.state == ModelHealthState::Available && e.failures == 0),
        "expected a rejection to leave fabrikam's health alone",
    );
}

// --- Scenario 2: a fatal fault stops the route. ---

async fn scenario_2_fatal() {
    println!();
    println!(
        "=== Scenario 2: a 401 is fatal -- the route stops, and retry(3) doesn't retry it ==="
    );
    let bad_key = FakeModel::new(
        "northwind",
        "nw-large",
        vec![Turn::fail(
            401,
            None,
            Some("401 Unauthorized: invalid API key"),
        )],
    );
    let backup = FakeModel::new("local", "llama-local", vec![Turn::answer("never asked")]);
    let fatal = ModelRoute::new("fatal")
        .candidate(ModelCandidate::new("primary", bad_key))
        .candidate(ModelCandidate::new("backup", backup.clone()));

    let (result, events) = run(
        &ask_graph(fatal, Some(RecoveryPolicy::retry(3))),
        "fatal",
        ticket(),
        &[],
    )
    .await;
    let (node, attempts, message) = expect_node_failed(result);
    println!("  Node '{node}' failed after {attempts} attempt(s): {message}");
    let routed = print_routed(&events);
    check(attempts == 1, "expected a fatal fault to skip the retries");
    check(
        message == "model call failed (401): 401 Unauthorized: invalid API key",
        "expected the candidate's own fault, unchanged",
    );
    check(
        outcomes(&routed) == "primary:failed:401, backup:not_tried",
        "expected backup to be not_tried",
    );
    check(backup.calls() == 0, "expected backup never to be asked");
}

// --- Scenario 3: hedging. A slow candidate gets the next one started beside it. ---

async fn scenario_3_hedging() {
    println!();
    println!(
        "=== Scenario 3a: hedge after 50 ms -- slow hangs, fast starts beside it and wins ==="
    );
    let slow = FakeModel::new("northwind", "nw-large", vec![Turn::hang()]);
    let fast = FakeModel::new("local", "llama-local", vec![Turn::answer(ROLL_BACK)]);
    let hedged = ModelRoute::new("hedged")
        .candidate(ModelCandidate::new("slow", slow.clone()))
        .candidate(ModelCandidate::new("fast", fast.clone()))
        .with_hedge_after(Duration::from_millis(50));

    let (result, events) = run(&ask_graph(hedged, None), "hedge-a", ticket(), &[]).await;
    println!("  Answer: {}", expect_completed(result).answer);
    let routed = print_routed(&events);
    check(
        outcomes(&routed) == "slow:lost_race, fast:answered",
        "expected fast to win and slow to lose the race",
    );
    check(routed.hedges == 1, "expected one hedge");
    check(
        slow.calls() == 1 && fast.calls() == 1,
        "expected both candidates to be asked once",
    );

    println!();
    println!(
        "=== Scenario 3b: the same route shape -- slow answers once the hedge has started, and still wins ==="
    );
    // slow answers only after the hedge has started; the hedge never answers.
    // Whatever the timing, both are running when slow's answer arrives, so
    // the first answer -- slow's -- wins.
    let started = Arc::new(Notify::new());
    let mut slow_turn = Turn::answer(ROLL_BACK);
    slow_turn.wait_for = Some(started.clone());
    let mut hedge_turn = Turn::hang();
    hedge_turn.on_asked = Some(started);
    let slow = FakeModel::new("northwind", "nw-large", vec![slow_turn]);
    let hedge = FakeModel::new("local", "llama-local", vec![hedge_turn]);
    let hedged = ModelRoute::new("hedged")
        .candidate(ModelCandidate::new("slow", slow))
        .candidate(ModelCandidate::new("hedge", hedge))
        .with_hedge_after(Duration::from_millis(50));

    let (result, events) = run(&ask_graph(hedged, None), "hedge-b", ticket(), &[]).await;
    println!("  Answer: {}", expect_completed(result).answer);
    let routed = print_routed(&events);
    check(
        outcomes(&routed) == "slow:answered, hedge:lost_race",
        "expected slow to win after the hedge had started",
    );
    check(
        routed.hedges == 1 && routed.model.as_deref() == Some("nw-large"),
        "expected one hedge and nw-large's answer",
    );
}

// --- Scenario 4: an eligibility rule skips a candidate for one request. ---

async fn scenario_4_eligible() {
    println!();
    println!("=== Scenario 4: with_eligible -- the free plan skips the frontier candidate ===");
    let frontier = FakeModel::new("northwind", "nw-large", vec![Turn::answer(ROLL_BACK)]);
    let cheap = FakeModel::new(
        "local",
        "llama-local",
        vec![Turn::answer(
            "Roll back deploy 412, because it is the newest change.",
        )],
    );
    let eligible = ModelRoute::new("plans")
        .candidate(
            ModelCandidate::new("frontier", frontier.clone())
                // The application's rule, asked per request: here the run's
                // metadata says the customer's plan.
                .with_eligible(|_, ctx| {
                    let plan = ctx.and_then(|ctx| ctx.metadata().get("plan"));
                    if plan.map(String::as_str) == Some("free") {
                        Err("the free plan doesn't use frontier models".to_string())
                    } else {
                        Ok(())
                    }
                }),
        )
        .candidate(ModelCandidate::new("local", cheap.clone()));
    let plans = ask_graph(eligible, None);

    let (result, events) = run(&plans, "plan-paid", ticket(), &[("plan", "paid")]).await;
    expect_completed(result);
    println!("  plan=paid:");
    let paid = print_routed(&events);
    let (result, events) = run(&plans, "plan-free", ticket(), &[("plan", "free")]).await;
    expect_completed(result);
    println!("  plan=free:");
    let free = print_routed(&events);
    check(
        outcomes(&paid) == "frontier:answered, local:not_tried",
        "expected frontier to answer the paid request",
    );
    check(
        outcomes(&free)
            == "frontier:ineligible:the free plan doesn't use frontier models, local:answered",
        "expected frontier to be skipped for the free request",
    );
    check(
        frontier.calls() == 1 && cheap.calls() == 1,
        "expected the ineligible candidate not to be asked",
    );
}

// --- Scenario 5: one ModelHealth shared by two routes; the breaker skips a failing provider. ---

async fn scenario_5_shared_breaker() {
    println!();
    println!(
        "=== Scenario 5: a shared ModelHealth with a breaker (2 failures, open 60 s) across two routes ==="
    );
    let shared =
        ModelHealth::new(ModelHealthOptions::new().with_breaker(2, Duration::from_secs(60)));
    // Two models of one provider: the health key is the provider's name, so
    // an outage counts against both.
    let large = FakeModel::new(
        "northwind",
        "nw-large",
        vec![Turn::fail(503, None, None), Turn::answer(ROLL_BACK)],
    );
    let small = FakeModel::new(
        "northwind",
        "nw-small",
        vec![
            Turn::fail(503, None, None),
            Turn::answer("Deploy 412 broke checkout."),
        ],
    );
    let steady = FakeModel::new(
        "local",
        "llama-local",
        vec![
            Turn::answer("Roll back deploy 412, because it is the newest change."),
            Turn::answer("Deploy 412 broke checkout."),
            Turn::answer("Roll back deploy 412, because it is the newest change."),
            Turn::answer("Deploy 412 broke checkout."),
        ],
    );
    let diagnose = ModelRoute::new("diagnose")
        .candidate(ModelCandidate::new("large", large.clone()))
        .candidate(ModelCandidate::new("steady", steady.clone()))
        .with_health(shared.clone());
    let summarise = ModelRoute::new("summarise")
        .candidate(ModelCandidate::new("small", small.clone()))
        .candidate(ModelCandidate::new("steady", steady))
        .with_health(shared.clone());
    let desk = StateGraph::<Ticket>::new()
        .add_model_node(
            "Diagnose",
            Arc::new(diagnose),
            |t: &Ticket, _| vec![Message::user(t.question.clone())],
            |t: Ticket, r: ChatResponse| Ticket {
                answer: r.text(),
                ..t
            },
            ModelNodeOptions::new(),
        )
        .add_model_node(
            "Summarise",
            Arc::new(summarise),
            |t: &Ticket, _| vec![Message::user(t.answer.clone())],
            |t: Ticket, r: ChatResponse| Ticket {
                summary: r.text(),
                ..t
            },
            ModelNodeOptions::new(),
        )
        .add_edge("Diagnose", "Summarise")
        .compile()
        .expect("the desk graph is valid");

    println!("  Run 1: northwind fails in each route (2 failures in a row: the breaker opens)");
    let (result, events) = run(&desk, "desk-1", ticket(), &[]).await;
    expect_completed(result);
    let run1 = print_all_routed(&events);
    print_health(&shared);
    check(
        run1.iter().map(outcomes).collect::<Vec<_>>()
            == [
                "large:failed:503, steady:answered",
                "small:failed:503, steady:answered",
            ],
        "expected northwind to fail in both routes",
    );
    check(
        health_of(&shared, "northwind")
            .is_some_and(|e| matches!(e.state, ModelHealthState::Open { .. }) && e.failures == 2),
        "expected northwind's breaker to be open",
    );

    println!("  Run 2: northwind's breaker is open, so neither route asks it");
    let (result, events) = run(&desk, "desk-2", ticket(), &[]).await;
    expect_completed(result);
    let run2 = print_all_routed(&events);
    check(
        run2.iter().map(outcomes).collect::<Vec<_>>()
            == [
                "large:breaker_open:60 s, steady:answered",
                "small:breaker_open:60 s, steady:answered",
            ],
        "expected northwind to be skipped",
    );
    check(
        large.calls() == 1 && small.calls() == 1,
        "expected northwind not to be asked while its breaker is open",
    );

    tokio::time::advance(Duration::from_secs(60)).await;
    println!("  (paused clock +60 s)");
    print_health(&shared);
    check(
        health_of(&shared, "northwind").is_some_and(|e| e.state == ModelHealthState::HalfOpen),
        "expected northwind to be half-open",
    );

    println!("  Run 3: one test call goes through, succeeds and closes the breaker");
    let (result, events) = run(&desk, "desk-3", ticket(), &[]).await;
    expect_completed(result);
    let run3 = print_all_routed(&events);
    print_health(&shared);
    check(
        run3.iter().map(outcomes).collect::<Vec<_>>()
            == [
                "large:answered, steady:not_tried",
                "small:answered, steady:not_tried",
            ],
        "expected northwind to answer again",
    );
    check(
        health_of(&shared, "northwind")
            .is_some_and(|e| e.state == ModelHealthState::Available && e.failures == 0),
        "expected the breaker to close",
    );
}

// --- Scenario 6: nothing answers. The node fails transiently, and its RecoveryPolicy waits for the hint. ---

async fn scenario_6_exhausted() {
    println!();
    println!(
        "=== Scenario 6: nothing answers -- a transient failure whose hint is the shortest wait ==="
    );
    let a = FakeModel::new(
        "northwind",
        "nw-large",
        vec![Turn::fail(429, Some(Duration::from_secs(8)), None)],
    );
    let b = FakeModel::new(
        "contoso",
        "ct-guarded",
        vec![
            Turn::fail(503, Some(Duration::from_secs(3)), None),
            Turn::answer(ROLL_BACK),
        ],
    );
    let exhausted = ModelRoute::new("exhausted")
        .candidate(ModelCandidate::new("a", a.clone()))
        .candidate(ModelCandidate::new("b", b.clone()));
    let retrying = ask_graph(
        exhausted,
        Some(RecoveryPolicy::retry(2).with_backoff(
            Duration::from_millis(200),
            Duration::from_secs(30),
            Jitter::None,
        )),
    );

    // On the paused clock the retry's 3 s wait passes the moment the run has
    // nothing else to do, and the health's cooldowns read the same clock.
    let (result, events) = run(&retrying, "exhausted", ticket(), &[]).await;
    let answer = expect_completed(result).answer;
    let retries: Vec<(String, Duration)> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::RetryScheduled { error, delay, .. } => Some((error.clone(), *delay)),
            _ => None,
        })
        .collect();
    check(retries.len() == 1, "expected exactly one retry");
    let (error, delay) = &retries[0];
    println!("  Attempt 1 failed: {error}");
    println!(
        "  RetryScheduled delay: {} s (b's Retry-After, the shorter of 8 s and 3 s; not the 200 ms backoff)",
        delay.as_secs_f64()
    );
    let attempts = print_all_routed(&events);
    println!("  Answer: {answer}");
    check(
        error
            == "model call failed (route_exhausted): no candidate of route 'exhausted' answered: a failed (429), b failed (503)",
        "expected the route's message to name each candidate",
    );
    check(
        *delay == Duration::from_secs(3),
        "expected the retry to wait for the shortest hint, 3 s",
    );
    check(attempts.len() == 2, "expected two calls through the route");
    check(
        outcomes(&attempts[0]) == "a:failed:429, b:failed:503",
        "expected both candidates to fail on the first attempt",
    );
    // The paused clock moved exactly 3 s, so a's 8 s cooldown has exactly 5 s left.
    check(
        outcomes(&attempts[1]) == "a:cooling_down:5 s, b:answered",
        "expected the retry to skip a (still cooling down, 5 s left) and get b's answer",
    );
    check(
        attempts[1].node_path == "Ask" && attempts[1].attempt == 2,
        "expected the second call to be the node's second attempt",
    );
    check(
        a.calls() == 1 && b.calls() == 2,
        "expected a to be asked once and b twice",
    );
}

// --- Helpers ---

fn ask_graph(route: ModelRoute, policy: Option<RecoveryPolicy>) -> CompiledGraph<Ticket> {
    let mut graph = StateGraph::<Ticket>::new().add_model_node(
        "Ask",
        Arc::new(route),
        |t: &Ticket, _| vec![Message::user(t.question.clone())],
        |t: Ticket, r: ChatResponse| Ticket {
            answer: r.text(),
            ..t
        },
        ModelNodeOptions::new(),
    );
    if let Some(policy) = policy {
        graph = graph.set_recovery_policy("Ask", policy);
    }
    graph.compile().expect("the ask graph is valid")
}

/// Runs `graph` on a fresh store with an event channel, and collects the
/// run's events up to and including `RunCompleted`.
async fn run(
    graph: &CompiledGraph<Ticket>,
    thread: &str,
    ticket: Ticket,
    metadata: &[(&str, &str)],
) -> (GraphExecutionResult<Ticket>, Vec<RunEvent>) {
    let (observer, mut receiver) = RunEventChannel::new();
    let mut config = RunConfig::new().with_observer(observer);
    for (key, value) in metadata {
        config = config.with_metadata(*key, *value);
    }
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let result = graph.run_with(thread, ticket, store, config).await;
    let mut events = Vec::new();
    loop {
        match tokio::time::timeout(PATIENCE, receiver.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) => break,
            Err(_) => fail("the run's events never ended with RunCompleted"),
        }
    }
    (result, events)
}

fn routed_of(events: &[RunEvent]) -> Vec<Routed> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::ModelRouted {
                route,
                answered,
                model,
                attempts,
                hedges,
                ..
            } => Some(Routed {
                node_path: event.node_path.as_deref().unwrap_or_default().to_string(),
                attempt: event.attempt.unwrap_or_default(),
                route: route.clone(),
                answered: answered.clone(),
                model: model.clone(),
                attempts: attempts
                    .iter()
                    .map(|a| {
                        (
                            a.candidate.clone(),
                            a.outcome,
                            a.reason.clone(),
                            a.call_id.clone(),
                        )
                    })
                    .collect(),
                hedges: *hedges,
            }),
            _ => None,
        })
        .collect()
}

fn print_routed(events: &[RunEvent]) -> Routed {
    let mut routed = print_all_routed(events);
    check(routed.len() == 1, "expected one call through the route");
    routed.remove(0)
}

fn print_all_routed(events: &[RunEvent]) -> Vec<Routed> {
    let routed = routed_of(events);
    for r in &routed {
        let who = match (&r.answered, &r.model) {
            (Some(answered), model) => format!(
                "answered by {answered} ({})",
                model.as_deref().unwrap_or("?")
            ),
            (None, _) => "nobody answered".to_string(),
        };
        println!(
            "  ModelRouted {}#{} route '{}': {who}, hedges {}",
            r.node_path, r.attempt, r.route, r.hedges
        );
        for (candidate, outcome, reason, _) in &r.attempts {
            let line = format!(
                "    {candidate:<9} {:<12} {}",
                outcome.as_str(),
                reason.as_deref().unwrap_or_default()
            );
            println!("{}", line.trim_end());
        }
    }
    routed
}

fn print_health(health: &ModelHealth) {
    let entries: Vec<String> = health
        .snapshot()
        .iter()
        .map(|e| format!("{}={}({})", e.key, e.state.as_str(), e.failures))
        .collect();
    println!("  Health: {}", entries.join(", "));
}

fn health_of(health: &ModelHealth, key: &str) -> Option<ModelHealthEntry> {
    health.snapshot().into_iter().find(|e| e.key == key)
}

/// `candidate:outcome[:reason]` for each candidate, in declaration order.
fn outcomes(routed: &Routed) -> String {
    routed
        .attempts
        .iter()
        .map(|(candidate, outcome, reason, _)| match reason {
            Some(reason) => format!("{candidate}:{}:{reason}", outcome.as_str()),
            None => format!("{candidate}:{}", outcome.as_str()),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn describe(event: &RunEvent) -> Option<String> {
    match &event.kind {
        RunEventKind::ModelCallStarted { model, route, .. } => Some(format!(
            "ModelCallStarted   {model:<12} route={}",
            route.as_deref().unwrap_or_default()
        )),
        RunEventKind::ModelCallCompleted {
            model,
            result,
            finish_reason,
            ..
        } => Some(format!(
            "ModelCallCompleted {model:<12} {result:?}{}",
            finish_reason
                .as_deref()
                .map(|f| format!(" ({f})"))
                .unwrap_or_default()
        )),
        _ => None,
    }
}

fn expect_completed(result: GraphExecutionResult<Ticket>) -> Ticket {
    match result {
        GraphExecutionResult::Completed(state) => state,
        other => fail(&format!(
            "expected the run to complete, got: {}",
            outcome(&other)
        )),
    }
}

fn expect_node_failed(result: GraphExecutionResult<Ticket>) -> (String, u32, String) {
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => (node, attempts, message),
        other => fail(&format!("expected NodeFailed, got: {}", outcome(&other))),
    }
}

/// A result the demo didn't expect, for its failure message.
fn outcome(result: &GraphExecutionResult<Ticket>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "completed".to_string(),
        GraphExecutionResult::Failed(error) => format!("failed: {error:?}"),
        GraphExecutionResult::Interrupted { waiting_at, .. } => {
            format!("interrupted at {waiting_at:?}")
        }
        GraphExecutionResult::Cancelled { .. } => "cancelled".to_string(),
    }
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

// --- The scripted model ---

/// One scripted turn of a [`FakeModel`].
#[derive(Default)]
struct Turn {
    text: String,
    /// Answer with finish reason `content_filter`.
    refused: bool,
    failure: Option<ChatError>,
    /// Wait until the call is dropped (a hedge's loser).
    hang: bool,
    /// Wait for this before answering.
    wait_for: Option<Arc<Notify>>,
    /// Notified as soon as the turn is asked.
    on_asked: Option<Arc<Notify>>,
}

impl Turn {
    fn answer(text: &str) -> Self {
        Self {
            text: text.to_string(),
            ..Self::default()
        }
    }

    fn refuse(text: &str) -> Self {
        Self {
            refused: true,
            ..Self::answer(text)
        }
    }

    fn hang() -> Self {
        Self {
            hang: true,
            ..Self::default()
        }
    }

    /// A provider's HTTP fault, as the engine's own clients return it:
    /// `ChatError::status` makes 408, 409, 429 and 5xx transient, every other
    /// status fatal, and its `error_type` the status.
    fn fail(status: u16, retry_after: Option<Duration>, message: Option<&str>) -> Self {
        let message = message.map_or_else(|| format!("{status} from the provider"), str::to_string);
        Self {
            failure: Some(ChatError::status(status, retry_after, message)),
            ..Self::default()
        }
    }
}

/// A scripted stand-in for a provider's chat client: it plays its turns in
/// order and names its provider (the route's default health key) and its
/// default model.
struct FakeModel {
    provider: &'static str,
    model: &'static str,
    script: Mutex<VecDeque<Turn>>,
    calls: AtomicUsize,
}

impl FakeModel {
    fn new(provider: &'static str, model: &'static str, script: Vec<Turn>) -> Arc<Self> {
        Arc::new(Self {
            provider,
            model,
            script: Mutex::new(script.into()),
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ChatClient for FakeModel {
    fn provider(&self) -> &str {
        self.provider
    }

    fn default_model(&self) -> Option<&str> {
        Some(self.model)
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let turn = self.script.lock().unwrap().pop_front();
        let model = request.model.unwrap_or_else(|| self.model.to_string());
        Box::pin(async move {
            let Some(turn) = turn else {
                return Err(ChatError::fatal(
                    "script_exhausted",
                    format!("{model} has no more scripted turns"),
                ));
            };
            if let Some(asked) = &turn.on_asked {
                // A permit is stored if nobody waits yet, so the order of
                // the two calls doesn't matter.
                asked.notify_one();
            }
            if turn.hang {
                std::future::pending::<()>().await;
            }
            if let Some(signal) = &turn.wait_for {
                signal.notified().await;
            }
            tokio::task::yield_now().await;
            if let Some(failure) = turn.failure {
                return Err(failure);
            }
            let finish = if turn.refused {
                "content_filter"
            } else {
                "stop"
            };
            Ok(ChatResponse {
                message: Message::assistant(turn.text),
                finish_reason: Some(finish.to_string()),
                model: Some(model),
                id: None,
                usage: ModelUsage::new(Some(20), Some(8)),
            })
        })
    }

    // The demo's runs don't stream, so the model node makes whole calls only.
    fn stream(&self, _: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        Box::pin(async {
            Err(ChatError::fatal(
                "not_supported",
                "the demo's runs don't stream",
            ))
        })
    }
}
