//! Parallel fan-out / fan-in (`design/14-Parallel-Execution.md`).
//!
//! A trip planner -- `ParseRequest` -> `GatherInfo` -> `BuildItinerary` --
//! where `GatherInfo` is a *parallel group* (`.add_parallel_group(...)`): it
//! asks three simulated services at once (flights, hotels, weather) and merges
//! what they return with a reducer. To the rest of the graph the group is one
//! ordinary node, with ordinary edges in and out.
//!
//! Every service takes a different amount of time (flights 300 ms, hotels
//! 200 ms, weather 100 ms), so the demo can watch four things:
//!
//! 1. **Real concurrency** -- the three calls overlap (peak in flight: 3), so
//!    the group takes exactly as long as its slowest branch, not the sum.
//! 2. **A deterministic merge** -- the services *finish* in the order weather,
//!    hotels, flights, but the reducer receives their results in the order the
//!    branches were *declared*, so the merged log is the same on every run.
//! 3. **Group-level failure** -- the first branch to fail fails the whole
//!    group at once and cancels the siblings still running. The group's
//!    `RecoveryPolicy` then retries *every* branch (a transient blip), or
//!    routes to a fallback node on the pre-group state (a fatal error).
//! 4. **Streaming branches** -- the same group under `run_streaming`, each
//!    branch pushing tokens that arrive attributed to `GatherInfo/<branch>`.
//!
//! The branches are async (`.async_branch(name, |state, ctx| async { .. })`),
//! so a cancelled sibling really stops: the group drops its future mid-`sleep`.
//! A sync `.branch(..)` runs on tokio's blocking pool and can't be stopped; it
//! would run on and have its result discarded.
//!
//! No live service and no LLM -- the services are deterministic stand-ins (a
//! `sleep` and a canned answer) so the demo runs offline. The demo runs on a
//! **paused tokio clock** (`start_paused = true`): no real time passes, the
//! clock jumps straight to the next timer whenever every task is waiting, so
//! the millisecond figures printed are exact and identical on every run.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::streaming::{StreamChunk, StreamChunkType};
use datalium_strata::{CancellationToken, StrataError};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TripPlan {
    destination: String,
    flight: Option<String>,
    hotel: Option<String>,
    forecast: Option<String>,
    log: Vec<String>,
    summary: Option<String>,
}

impl TripPlan {
    fn new(destination: &str) -> Self {
        Self {
            destination: destination.to_string(),
            flight: None,
            hotel: None,
            forecast: None,
            log: Vec::new(),
            summary: None,
        }
    }
}

/// What a scenario measures across all three services: how many branches were
/// running at once, and the order they finished in. Time is tokio's, so on the
/// paused clock `ms()` is exact.
struct Tracker {
    started: Instant,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    finished: Mutex<Vec<&'static str>>,
}

impl Tracker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            finished: Mutex::new(Vec::new()),
        })
    }

    fn ms(&self) -> u128 {
        self.started.elapsed().as_millis()
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn finished_order(&self) -> Vec<&'static str> {
        self.finished.lock().unwrap().clone()
    }

    fn enter(&self) {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
    }

    fn exit(&self, service: &'static str) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.finished.lock().unwrap().push(service);
    }
}

/// How a simulated service misbehaves.
#[derive(Clone, Copy)]
enum Fault {
    None,
    /// Times out for the first `n` calls, then works.
    TimeoutFirst(u32),
    /// Always rejects the request -- retrying can't help.
    Rejects(&'static str),
}

struct Service {
    name: &'static str,
    latency: Duration,
    fault: Fault,
    calls: AtomicU32,
    answered: AtomicU32,
    cancelled: AtomicU32,
    /// Cancelled calls whose token had fired: the group, not something else,
    /// stopped them.
    cancelled_by_token: AtomicU32,
    tracker: Arc<Tracker>,
}

impl Service {
    fn new(name: &'static str, latency_ms: u64, fault: Fault, tracker: &Arc<Tracker>) -> Arc<Self> {
        Arc::new(Self {
            name,
            latency: Duration::from_millis(latency_ms),
            fault,
            calls: AtomicU32::new(0),
            answered: AtomicU32::new(0),
            cancelled: AtomicU32::new(0),
            cancelled_by_token: AtomicU32::new(0),
            tracker: tracker.clone(),
        })
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn answered(&self) -> u32 {
        self.answered.load(Ordering::SeqCst)
    }

    fn cancelled(&self) -> u32 {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// One request to the service: takes `latency`, then answers or fails.
    ///
    /// When the group gives up on this branch it cancels `token` and drops this
    /// future wherever it is waiting -- no code after the `sleep` runs. The
    /// [`InFlight`] guard's `Drop` is what notices.
    async fn call(&self, token: &CancellationToken) -> Result<(), NodeError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.tracker.enter();
        println!(
            "    [{:>4} ms] {} call {call}: started",
            self.tracker.ms(),
            self.name
        );
        let mut guard = InFlight {
            service: self,
            call,
            token: token.clone(),
            settled: false,
        };

        tokio::time::sleep(self.latency).await;
        guard.settled = true;

        let result = match self.fault {
            Fault::TimeoutFirst(n) if call <= n => {
                Err(NodeError::Transient(format!("{} timed out", self.name)))
            }
            Fault::Rejects(reason) => Err(NodeError::Fatal(reason.to_string())),
            _ => Ok(()),
        };

        self.tracker.exit(self.name);
        let outcome = match &result {
            Ok(()) => "answered".to_string(),
            Err(error) if error.is_transient() => format!("transient error: {}", error.message()),
            Err(error) => format!("fatal error: {}", error.message()),
        };
        println!(
            "    [{:>4} ms] {} call {call}: {outcome}",
            self.tracker.ms(),
            self.name
        );
        if result.is_ok() {
            self.answered.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
}

/// Lives across a service call's wait. Dropped before the call settled means
/// the group dropped the branch's future: it was cancelled.
struct InFlight<'a> {
    service: &'a Service,
    call: u32,
    token: CancellationToken,
    settled: bool,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let service = self.service;
        service.cancelled.fetch_add(1, Ordering::SeqCst);
        if self.token.is_cancelled() {
            service.cancelled_by_token.fetch_add(1, Ordering::SeqCst);
        }
        service.tracker.exit(service.name);
        println!(
            "    [{:>4} ms] {} call {}: cancelled (the group gave up on it)",
            service.tracker.ms(),
            service.name,
            self.call
        );
    }
}

struct Services {
    flights: Arc<Service>,
    hotels: Arc<Service>,
    weather: Arc<Service>,
}

impl Services {
    fn all(&self) -> [&Arc<Service>; 3] {
        [&self.flights, &self.hotels, &self.weather]
    }

    fn total_calls(&self) -> u32 {
        self.all().iter().map(|s| s.calls()).sum()
    }

    /// Every cancelled call saw its token fire.
    fn cancellations_came_from_the_group(&self) -> bool {
        self.all()
            .iter()
            .all(|s| s.cancelled() == s.cancelled_by_token.load(Ordering::SeqCst))
    }
}

fn new_services(tracker: &Arc<Tracker>, flights: Fault, hotels: Fault, weather: Fault) -> Services {
    Services {
        flights: Service::new("flights", 300, flights, tracker),
        hotels: Service::new("hotels", 200, hotels, tracker),
        weather: Service::new("weather", 100, weather, tracker),
    }
}

/// Merges the three branch results back into one state.
///
/// Each branch fills in its own field, and adds a line to `log` after
/// whatever `entry.log` already held; the reducer keeps just that new suffix
/// from each output. `outputs` is in *declaration* order (flights, hotels,
/// weather) however the branches finished, which is what makes the merged log
/// identical on every run.
fn merge(entry: &TripPlan, outputs: Vec<TripPlan>) -> TripPlan {
    let mut merged = entry.clone();
    for output in outputs {
        merged.flight = merged.flight.or(output.flight);
        merged.hotel = merged.hotel.or(output.hotel);
        merged.forecast = merged.forecast.or(output.forecast);
        merged
            .log
            .extend(output.log.into_iter().skip(entry.log.len()));
    }
    merged
}

/// What one branch fills in: its own field of the plan.
type Answer = fn(&mut TripPlan);

/// Adds one async branch, named after `service`, that asks it. With
/// `streaming`, it is an `.async_streaming_branch(..)` and pushes a token
/// before and after the call.
fn ask(
    branches: ParallelBranches<TripPlan>,
    service: &Arc<Service>,
    answer: Answer,
    line: &'static str,
    streaming: bool,
) -> ParallelBranches<TripPlan> {
    let name = service.name;
    let service = service.clone();
    // Each branch gets its own clone of the plan as it was when the group
    // started, and returns it with one field filled in. Branches can't see
    // each other.
    let branch = move |mut plan: TripPlan, ctx: NodeContext| {
        let service = service.clone();
        async move {
            if let Some(stream) = ctx.stream() {
                stream.emit_token("asking...").await.map_err(stream_error)?;
            }
            service.call(ctx.cancellation()).await?;
            if let Some(stream) = ctx.stream() {
                stream.emit_token(line).await.map_err(stream_error)?;
            }
            answer(&mut plan);
            plan.log.push(line.to_string());
            Ok(plan)
        }
    };
    if streaming {
        branches.async_streaming_branch(name, branch)
    } else {
        branches.async_branch(name, branch)
    }
}

fn stream_error(error: StrataError) -> NodeError {
    NodeError::Fatal(format!("stream closed: {error:?}"))
}

/// `ParseRequest` -> `GatherInfo` (flights | hotels | weather) ->
/// `BuildItinerary`. When `policy` names a fallback node, `UseCachedPlan` is
/// added as that node -- it gets no ordinary edge, so the recovery policy is
/// the only way in.
fn trip_graph(
    services: &Services,
    policy: Option<RecoveryPolicy>,
    streaming: bool,
) -> CompiledGraph<TripPlan> {
    let branches = ParallelBranches::new();
    let branches = ask(
        branches,
        &services.flights,
        |plan| plan.flight = Some("LH 401, $612".to_string()),
        "flights: LH 401 for $612",
        streaming,
    );
    let branches = ask(
        branches,
        &services.hotels,
        |plan| plan.hotel = Some("Hotel Aurora, $148/night".to_string()),
        "hotels: Hotel Aurora at $148/night",
        streaming,
    );
    let branches = ask(
        branches,
        &services.weather,
        |plan| plan.forecast = Some("sunny, 24C".to_string()),
        "weather: sunny, 24C",
        streaming,
    );

    let mut graph = StateGraph::<TripPlan>::new()
        .add_node("ParseRequest", |mut plan: TripPlan| {
            plan.destination = plan.destination.trim().to_string();
            plan.log
                .push(format!("request: trip to {}", plan.destination));
            plan
        })
        .add_parallel_group("GatherInfo", branches, merge)
        .add_node("BuildItinerary", |mut plan: TripPlan| {
            plan.summary = Some(format!(
                "{}: {} | {} | {}",
                plan.destination,
                plan.flight.as_deref().unwrap_or("-"),
                plan.hotel.as_deref().unwrap_or("-"),
                plan.forecast.as_deref().unwrap_or("-"),
            ));
            plan
        })
        .add_edge("ParseRequest", "GatherInfo")
        .add_edge("GatherInfo", "BuildItinerary");

    if let Some(policy) = policy {
        if let Some(fallback) = policy.fallback.clone() {
            graph = graph.add_node(fallback, |mut plan: TripPlan| {
                plan.summary = Some(format!(
                    "{}: cached plan (live lookup unavailable)",
                    plan.destination
                ));
                plan
            });
        }
        graph = graph.set_recovery_policy("GatherInfo", policy);
    }

    graph.compile().expect("graph is structurally valid")
}

// A current-thread runtime with tokio's clock paused (tokio's `test-util`
// feature). The async branches still overlap: each is a task that holds no
// thread while it waits.
#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    // --- Scenario 1: everything healthy. Concurrency + deterministic merge. ---

    println!("=== Scenario 1: all three services healthy ===");
    let tracker = Tracker::new();
    let services = new_services(&tracker, Fault::None, Fault::None, Fault::None);
    let graph = trip_graph(&services, None, false);
    let began = Instant::now();
    let result = graph
        .run("trip-1", TripPlan::new("  Lisbon "), store.clone())
        .await;
    let elapsed = began.elapsed();
    let plan = expect_completed(result);
    let finished = tracker.finished_order();
    println!("  Completed in {} ms of tokio time.", elapsed.as_millis());
    println!("  Sequentially the three calls would take 600 ms (300 + 200 + 100).");
    println!(
        "  Peak branches in flight: {} of 3 -- they really overlapped.",
        tracker.peak()
    );
    println!("  Completion order:  {}", finished.join(", "));
    println!("  Merged log (declaration order, identical on every run):");
    for line in &plan.log {
        println!("    - {line}");
    }
    println!("  Summary: {}", plan.summary.as_deref().unwrap_or("(none)"));
    check(
        elapsed == Duration::from_millis(300),
        "expected the group to take exactly as long as its slowest branch (300 ms)",
    );
    check(
        tracker.peak() == 3,
        "expected all three branches to be in flight at once",
    );
    check(
        finished == ["weather", "hotels", "flights"],
        "expected completion order weather, hotels, flights",
    );
    check(
        plan.log[1..]
            == [
                "flights: LH 401 for $612",
                "hotels: Hotel Aurora at $148/night",
                "weather: sunny, 24C",
            ],
        "expected the merged log in declaration order",
    );

    // --- Scenario 2: a transient blip in one branch retries the whole group. ---

    println!();
    println!("=== Scenario 2: hotels times out once -- retry(2) on the group ===");
    let tracker = Tracker::new();
    let services = new_services(&tracker, Fault::None, Fault::TimeoutFirst(1), Fault::None);
    let graph = trip_graph(&services, Some(RecoveryPolicy::retry(2)), false);
    let began = Instant::now();
    let result = graph
        .run("trip-2", TripPlan::new("Porto"), store.clone())
        .await;
    let elapsed = began.elapsed();
    let plan = expect_completed(result);
    println!(
        "  Completed in {} ms. Service calls -- flights: {}, hotels: {}, weather: {}.",
        elapsed.as_millis(),
        services.flights.calls(),
        services.hotels.calls(),
        services.weather.calls()
    );
    println!(
        "  The failure cancelled flights mid-call ({} cancelled), and the retry re-ran *all three*: {} calls in total, not the 4 a hotels-only retry would make. Branches must be safe to repeat.",
        services.flights.cancelled(),
        services.total_calls()
    );
    println!("  Summary: {}", plan.summary.as_deref().unwrap_or("(none)"));
    check(
        services.total_calls() == 6,
        "expected the retry to re-run every branch (6 calls)",
    );
    check(
        services.flights.cancelled() == 1,
        "expected the hotels failure to cancel the flights call still in flight",
    );
    check(
        services.cancellations_came_from_the_group(),
        "expected every cancelled call to have seen its token fire",
    );
    check(
        elapsed == Duration::from_millis(500),
        "expected the retry to start when hotels failed (200 ms), not when flights would have finished",
    );
    check(
        plan.log.len() == 4,
        "expected the failed attempt's log lines to be discarded",
    );

    // --- Scenario 3: a fatal branch fails the group; fall back. ---

    println!();
    println!("=== Scenario 3: hotels has no rooms -- retry(3), fallback 'UseCachedPlan' ===");
    let tracker = Tracker::new();
    let services = new_services(
        &tracker,
        Fault::None,
        Fault::Rejects("no rooms left on these dates"),
        Fault::None,
    );
    let graph = trip_graph(
        &services,
        Some(RecoveryPolicy::retry(3).with_fallback("UseCachedPlan")),
        false,
    );
    let result = graph
        .run("trip-3", TripPlan::new("Madeira"), store.clone())
        .await;
    let_abandoned_branches_drop().await;
    let plan = expect_completed(result);
    println!(
        "  Fell back after {} service call(s) in total -- a fatal error skips all 3 retries.",
        services.total_calls()
    );
    println!(
        "  Weather had already answered; flights was cancelled mid-call ({} cancelled). What finished is discarded with the failed group:",
        services.flights.cancelled()
    );
    println!(
        "    flight: {:?}, hotel: {:?}, forecast: {:?}",
        plan.flight, plan.hotel, plan.forecast
    );
    println!("  Log: {:?}", plan.log);
    println!("  Summary: {}", plan.summary.as_deref().unwrap_or("(none)"));
    check(
        services.total_calls() == 3,
        "expected a fatal error to skip retries (3 calls)",
    );
    check(
        services.flights.cancelled() == 1 && services.weather.answered() == 1,
        "expected weather answered and flights cancelled",
    );
    check(
        services.cancellations_came_from_the_group(),
        "expected every cancelled call to have seen its token fire",
    );
    check(
        plan.flight.is_none() && plan.forecast.is_none(),
        "expected sibling results to be discarded",
    );
    check(
        plan.log.len() == 1,
        "expected the fallback to run on the pre-group state",
    );

    // --- Scenario 4: the same fatal error with no fallback names the branch. ---

    println!();
    println!("=== Scenario 4: same failure, no fallback -- the error names the branch ===");
    let tracker = Tracker::new();
    let services = new_services(
        &tracker,
        Fault::None,
        Fault::Rejects("no rooms left on these dates"),
        Fault::None,
    );
    let graph = trip_graph(&services, Some(RecoveryPolicy::retry(3)), false);
    let result = graph
        .run("trip-4", TripPlan::new("Madeira"), store.clone())
        .await;
    let_abandoned_branches_drop().await;
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => {
            println!("  Failed: group '{node}' gave up after {attempts} attempt(s) -- {message}");
            println!("  The cancelled flights call is not named: it didn't fail, it was stopped.");
            check(
                message == "branch 'hotels': no rooms left on these dates",
                "expected the failure message to name only the failing branch",
            );
            check(
                attempts == 1,
                "expected a fatal error to give up after one attempt",
            );
            check(
                services.flights.cancelled() == 1 && services.cancellations_came_from_the_group(),
                "expected the hotels failure to cancel the flights call still in flight",
            );
        }
        other => fail(&format!(
            "expected Failed(NodeFailed), got {}",
            describe(&other)
        )),
    }

    // --- Scenario 5: the same group, streaming. ---

    println!();
    println!("=== Scenario 5: streaming branches under run_streaming ===");
    let tracker = Tracker::new();
    let services = new_services(&tracker, Fault::None, Fault::None, Fault::None);
    let graph = Arc::new(trip_graph(&services, None, true));
    let (handle, mut receiver) =
        graph.run_streaming("trip-5", TripPlan::new("Faro"), store.clone());
    let mut chunks: Vec<StreamChunk> = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push(chunk);
    }
    let plan = expect_completed(handle.await.expect("the streaming run's task panicked"));

    // Tokens grouped by the node name they arrived with, sorted by name.
    let mut sources: Vec<(String, Vec<String>)> = Vec::new();
    for chunk in chunks
        .iter()
        .filter(|c| c.chunk_type == StreamChunkType::Token)
    {
        let name = chunk.node_name.to_string();
        match sources.iter_mut().find(|(source, _)| *source == name) {
            Some((_, tokens)) => tokens.push(chunk.content.to_string()),
            None => sources.push((name, vec![chunk.content.to_string()])),
        }
    }
    sources.sort_by(|a, b| a.0.cmp(&b.0));
    println!(
        "  Tokens, grouped by the node name they arrived with (each branch's own stay in order):"
    );
    for (source, tokens) in &sources {
        println!("    {source:<18} {}", tokens.join(" | "));
    }
    let lifecycle: Vec<String> = chunks
        .iter()
        .filter(|c| c.chunk_type == StreamChunkType::NodeStart)
        .map(|c| c.node_name.to_string())
        .collect();
    println!(
        "  NodeStart chunks: {} -- branches are not nodes and get none.",
        lifecycle.join(", ")
    );
    println!("  Summary: {}", plan.summary.as_deref().unwrap_or("(none)"));
    let names: Vec<&str> = sources.iter().map(|(source, _)| source.as_str()).collect();
    check(
        names
            == [
                "GatherInfo/flights",
                "GatherInfo/hotels",
                "GatherInfo/weather",
            ],
        "expected tokens attributed to GatherInfo/<branch>",
    );
    check(
        sources
            .iter()
            .all(|(_, tokens)| tokens.len() == 2 && tokens[0] == "asking..."),
        "expected each branch's tokens in order",
    );
    check(
        lifecycle == ["ParseRequest", "GatherInfo", "BuildItinerary"],
        "expected one NodeStart per node, none per branch",
    );

    // --- Compile-time guard: two branches with the same name. ---

    println!();
    println!("=== Step 6: a malformed group is rejected at .compile(), not at runtime ===");
    let duplicate = ParallelBranches::<TripPlan>::new()
        .branch("flights", |plan: &TripPlan| Ok(plan.clone()))
        .branch("flights", |plan: &TripPlan| Ok(plan.clone()));
    match StateGraph::<TripPlan>::new()
        .add_parallel_group("GatherInfo", duplicate, merge)
        .compile()
    {
        Err(StrataError::CompilationFailed(message)) => {
            println!("  Rejected: {message}");
            check(
                message.contains("more than one branch"),
                "expected the duplicate branch name to be the reason",
            );
        }
        Err(other) => fail(&format!("expected CompilationFailed, got {other:?}")),
        Ok(_) => fail("expected the duplicate branch name to be rejected"),
    }

    println!();
    println!(
        "=== Done: three services ran at once, merged in a fixed order, a failing branch was handled at the group level, and branches streamed. ==="
    );
}

/// A group doesn't wait for a branch it gives up on: it cancels the branch's
/// token, aborts its task and returns. On this current-thread runtime the
/// aborted task's future is dropped the next time the runtime gets a turn, so
/// a run that ends right after the group failed must yield once before the
/// cancelled call has been dropped (and its `InFlight` guard has run).
async fn let_abandoned_branches_drop() {
    tokio::task::yield_now().await;
}

fn expect_completed(result: GraphExecutionResult<TripPlan>) -> TripPlan {
    match result {
        GraphExecutionResult::Completed(plan) => plan,
        other => fail(&format!("expected Completed, got {}", describe(&other))),
    }
}

fn describe(result: &GraphExecutionResult<TripPlan>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Interrupted { .. } => "Interrupted".to_string(),
        GraphExecutionResult::Failed(err) => format!("Failed({err:?})"),
        GraphExecutionResult::Cancelled { node, .. } => format!("Cancelled at {node}"),
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
