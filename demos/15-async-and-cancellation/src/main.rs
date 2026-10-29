//! Async execution, node context and cancellation (design/17-Async-Execution.md).
//!
//! A vendor-payment run: LoadInvoice -> Screen (a parallel group: Sanctions, Duplicates) ->
//! ScoreFraud -> PostToLedger -> PayVendor. Most handlers are async and take a `NodeContext`;
//! PostToLedger is a sync call into a legacy ledger SDK that can't see a token.
//!
//!   1. A healthy run: what `NodeContext` tells each handler (thread, qualified path, attempt,
//!      run id, run metadata), with run-level inputs (metadata, run id, a service) in `RunConfig`.
//!   2. The fraud API hangs: `set_timeout` turns the stuck attempt into a transient failure, and
//!      `RecoveryPolicy::retry(1)` recovers on attempt 2. Without the retry, the timeout fails
//!      the run.
//!   3. The ledger SDK blocks: a sync handler on tokio's blocking pool is abandoned, not waited
//!      for. The run finishes while it is still blocked, and when it finally returns, its
//!      posting lands too.
//!   4. An operator cancels during the bank call: `Cancelled`, a resumable `cancelled`
//!      checkpoint, no retry, and a resume that re-enters PayVendor only.
//!   5. The run's deadline passes during the bank call: the same, with reason `Deadline`.
//!   6. (Rust only) Inline sync nodes (design/39): run on the thread polling the run, refuse a
//!      timeout at compile, and a cancellation that lands while one runs takes effect after it.
//!
//! The clock is tokio's, paused (`tokio::time::pause()`, tokio's `test-util` feature). Node
//! timeouts and the deadline run on it, so the demo fires them exactly when it means to (after the
//! handler has started) with `tokio::time::advance`, and never waits for real time. The services
//! are deterministic stand-ins; nothing goes over a network.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::ThreadId;
use std::time::Duration;

use datalium_strata::checkpoint::{CancelReason, GraphExecutionResult, HumanSignal};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::{CANCELLED_CHECKPOINT_SCHEMA_VERSION, Checkpoint, CheckpointStatus};
use datalium_strata::store::CheckpointStore;
use datalium_strata::{CancellationToken, StrataError};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::Instant;

const FRAUD_TIMEOUT: Duration = Duration::from_secs(2);
const LEDGER_TIMEOUT: Duration = Duration::from_secs(5);
const RUN_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Node timeouts and deadlines run on tokio's clock. Paused, it moves only when the demo
    // advances it (or, when every task is idle, to the next timer), so nothing waits on real
    // time. `pause` needs a current-thread runtime and tokio's `test-util` feature.
    tokio::time::pause();
    watchdog();
    let main_thread = std::thread::current().id();
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    // --- Scenario 1: a healthy run, and what each handler's NodeContext says. ---

    println!("=== Scenario 1: a healthy payment run -- async handlers and their NodeContext ===");
    let fakes = Fakes::new();
    let graph = build(&fakes, 1);
    let config = RunConfig::new()
        .with_metadata("tenant", "acme-eu")
        .with_run_id("payrun-2026-10-24")
        .with_service(Arc::new(FxRates));
    let paid = expect_completed(
        graph
            .run_with(
                "inv-1001",
                Invoice::new("inv-1001", "Globex GmbH", 125000, "USD"),
                store.clone(),
                config,
            )
            .await,
    );
    println!(
        "  RunConfig: with_metadata(tenant = acme-eu), with_run_id(payrun-2026-10-24), with_service(FxRates)"
    );
    println!("  What each async handler was told:");
    println!(
        "    {:<20} {:<12} attempt  {:<9} {:<18} tenant",
        "node path", "node", "thread", "run id"
    );
    let order = [
        "LoadInvoice",
        "Screen/Sanctions",
        "Screen/Duplicates",
        "ScoreFraud",
        "PayVendor",
    ];
    let contexts = fakes.contexts();
    for path in order {
        let Some(seen) = contexts.iter().find(|c| c.path == path) else {
            fail(&format!("no handler at '{path}' saw a context"));
        };
        println!(
            "    {:<20} {:<12} {:<8} {:<9} {:<18} {}",
            seen.path,
            seen.node,
            seen.attempt,
            seen.thread,
            seen.run_id,
            seen.tenant.as_deref().unwrap_or("-")
        );
    }
    println!(
        "  PostToLedger is a sync handler (Fn(&Invoice) -> Result<Invoice, NodeError>): it gets no context and no token."
    );
    println!("  Completed: {}", paid.summary());
    check(
        contexts.len() == order.len(),
        "expected exactly one context per async handler",
    );
    check(
        contexts.iter().all(|c| {
            c.attempt == 1
                && c.thread == "inv-1001"
                && c.run_id == "payrun-2026-10-24"
                && c.tenant.as_deref() == Some("acme-eu")
        }),
        "expected every handler to see attempt 1, the thread, the run id and the tenant",
    );
    check(
        contexts
            .iter()
            .any(|c| c.path == "Screen/Sanctions" && c.node == "Sanctions"),
        "expected a branch's node() to be its own name and its node_path() to be qualified by the group",
    );
    check(
        paid.checks == ["sanctions: clear", "duplicates: none"],
        "expected the checks in declaration order",
    );
    check(
        paid.amount_eur_cents == Some(115000),
        "expected the FX service from RunConfig::with_service to convert the amount",
    );

    // --- Scenario 2: the fraud API hangs. A timeout is a transient failure; a retry recovers. ---

    println!();
    println!(
        "=== Scenario 2: the fraud API hangs -- set_timeout({} s) + retry(1) ===",
        FRAUD_TIMEOUT.as_secs()
    );
    let fakes = Fakes::new();
    fakes.fraud.hang_on_attempt.store(1, Ordering::SeqCst);
    let graph = Arc::new(build(&fakes, 1));
    let run = spawn_run(
        &graph,
        &store,
        Invoice::new("inv-1002", "Initech", 48000, "EUR"),
        options(),
    );
    fakes.fraud.stall.started().await;
    move_clock(FRAUD_TIMEOUT).await;
    let paid = expect_completed(run.await.expect("the run task"));
    let fraud_attempts: Vec<u32> = fakes
        .contexts()
        .iter()
        .filter(|c| c.node == "ScoreFraud")
        .map(|c| c.attempt)
        .collect();
    println!(
        "  Attempt 1 started, then the demo moved the paused clock {} s: its token was cancelled, its future dropped, and the attempt failed as transient.",
        FRAUD_TIMEOUT.as_secs()
    );
    println!(
        "  ScoreFraud attempts seen in NodeContext::attempt(): {fraud_attempts:?} -- each retry gets a fresh timeout."
    );
    println!("  Completed: {}", paid.summary());
    check(
        fraud_attempts == [1, 2],
        "expected the timed-out attempt to be retried once",
    );
    check(
        fakes.fraud.stall.saw_cancel() && fakes.fraud.stall.was_dropped(),
        "expected the hung attempt's token to be cancelled and its future dropped",
    );

    println!("  The same hang with no retry:");
    let fakes = Fakes::new();
    fakes.fraud.hang_on_attempt.store(1, Ordering::SeqCst);
    let graph = Arc::new(build(&fakes, 0));
    let run = spawn_run(
        &graph,
        &store,
        Invoice::new("inv-1002b", "Initech", 48000, "EUR"),
        options(),
    );
    fakes.fraud.stall.started().await;
    move_clock(FRAUD_TIMEOUT).await;
    let GraphExecutionResult::Failed(StrataError::NodeFailed {
        node,
        attempts,
        message,
    }) = run.await.expect("the run task")
    else {
        fail("expected the exhausted timeout to fail the run with NodeFailed");
    };
    let failed_checkpoint = load(&store, "inv-1002b").await;
    let error_kind = meta(&failed_checkpoint, "error_kind");
    println!("    NodeFailed: '{node}' after {attempts} attempt(s): {message}");
    println!(
        "    Checkpoint: {:?}, error_kind = {error_kind}",
        failed_checkpoint.status
    );
    check(
        message == "node 'ScoreFraud' timed out after 2000 ms",
        "expected the engine's timeout message",
    );
    check(
        failed_checkpoint.status == CheckpointStatus::Failed && error_kind == "transient",
        "expected a timeout to be a transient failure with a failed checkpoint",
    );

    // --- Scenario 3: a sync handler that blocks is abandoned, not waited for. ---

    println!();
    println!(
        "=== Scenario 3: the legacy ledger SDK blocks -- a sync handler is abandoned, set_timeout({} s) + retry(1) ===",
        LEDGER_TIMEOUT.as_secs()
    );
    let fakes = Fakes::new();
    fakes.ledger.block_first_call.store(true, Ordering::SeqCst);
    let graph = Arc::new(build(&fakes, 1));
    let run = spawn_run(
        &graph,
        &store,
        Invoice::new("inv-1003", "Umbrella Ltd", 9990, "EUR"),
        options(),
    );
    // tokio doesn't auto-advance a paused clock while a blocking-pool task runs, so the timeout
    // fires only when the demo moves the clock.
    fakes.ledger.first_call_started.notified().await;
    move_clock(LEDGER_TIMEOUT).await;
    let paid = expect_completed(run.await.expect("the run task"));
    let blocked_when_run_ended = !fakes.ledger.first_call_finished.load(Ordering::SeqCst);
    let entries_when_run_ended = fakes.ledger.entries();
    println!("  The run completed while ledger call 1 was still blocked: {blocked_when_run_ended}");
    println!(
        "  Ledger calls: {}; entries when the run ended: {entries_when_run_ended:?}",
        fakes.ledger.calls()
    );
    println!("  Completed: {}", paid.summary());
    // Open the gate and wait (a moment of real time) for the abandoned call to return.
    fakes.ledger.gate.open();
    fakes.ledger.first_call_returned.notified().await;
    let entries = fakes.ledger.entries();
    println!(
        "  Now the SDK's first call returns. Its result is discarded, but its side effect is not:"
    );
    println!("    ledger entries: {entries:?} -- one invoice, posted twice.");
    println!(
        "  A handler whose side effects must not overlap a retry should be async and honour its token."
    );
    check(
        blocked_when_run_ended,
        "expected the engine not to wait for the abandoned sync handler",
    );
    check(
        entries_when_run_ended == ["LED-1002 inv-1003"],
        "expected only the retry's posting when the run ended",
    );
    check(
        paid.ledger_ref.as_deref() == Some("LED-1002"),
        "expected the state to hold the retry's ledger reference",
    );
    check(
        entries == ["LED-1002 inv-1003", "LED-1001 inv-1003"],
        "expected the abandoned call to post late",
    );

    // --- Scenario 4: an operator cancels the run mid-flight; resume it. ---

    println!();
    println!(
        "=== Scenario 4: an operator cancels during the bank call -- Cancelled, then resumed ==="
    );
    let fakes = Fakes::new();
    fakes.bank.hang.store(true, Ordering::SeqCst);
    let graph = Arc::new(build(&fakes, 1));
    let token = CancellationToken::new();
    let run = spawn_run(
        &graph,
        &store,
        Invoice::new("inv-1004", "Hooli", 320000, "USD"),
        options().with_cancellation(token.clone()),
    );
    fakes.bank.stall.started().await;
    token.cancel();
    let cancelled = expect_cancelled(run.await.expect("the run task"));
    println!(
        "  Cancelled at '{}', reason {:?}, stopped at {:?} (run_with returned it; it isn't a Failed).",
        cancelled.node, cancelled.reason, cancelled.stopped_at
    );
    println!(
        "  The bank client's token was cancelled and its future dropped. Bank calls: {} -- PayVendor's retry(2) never ran: a cancellation isn't a failure.",
        fakes.bank.calls()
    );
    let checkpoint = load(&store, "inv-1004").await;
    print_checkpoint(&checkpoint);
    check(
        cancelled.node == "PayVendor" && cancelled.reason == CancelReason::Requested,
        "expected a requested cancellation at PayVendor",
    );
    check(
        cancelled.stopped_at == ["PayVendor"],
        "expected the run to have stopped at PayVendor",
    );
    check(
        fakes.bank.stall.saw_cancel() && fakes.bank.stall.was_dropped(),
        "expected the bank call's token to be cancelled and its future dropped",
    );
    check(
        fakes.bank.calls() == 1,
        "expected no retry after a cancellation",
    );
    check(
        checkpoint.status == CheckpointStatus::Cancelled,
        "expected a cancelled checkpoint",
    );
    check(
        checkpoint.schema_version == CANCELLED_CHECKPOINT_SCHEMA_VERSION,
        "expected schema 1.2.0",
    );
    check(
        checkpoint.suspended_node.as_deref() == Some("PayVendor")
            && checkpoint.next_node.as_deref() == Some("PayVendor"),
        "expected the checkpoint at PayVendor",
    );
    check(
        meta(&checkpoint, "cancel_reason") == "requested"
            && meta(&checkpoint, "tenant") == "acme-eu",
        "expected cancel_reason requested next to the run's tenant",
    );
    check(
        checkpoint.frozen_state["payment_ref"].is_null()
            && checkpoint.frozen_state["ledger_ref"] == "LED-1001",
        "expected the state PayVendor started with",
    );

    fakes.bank.hang.store(false, Ordering::SeqCst);
    let contexts_before = fakes.contexts().len();
    // No metadata here: a resume carries the stored run metadata forward.
    let paid = expect_completed(
        graph
            .resume_with(
                "inv-1004",
                HumanSignal::approve("ops-oncall"),
                store.clone(),
                RunConfig::new().with_service(Arc::new(FxRates)),
            )
            .await,
    );
    let resumed: Vec<Seen> = fakes.contexts().split_off(contexts_before);
    let described: Vec<String> = resumed
        .iter()
        .map(|c| {
            format!(
                "{} attempt {}, tenant {}",
                c.path,
                c.attempt,
                c.tenant.as_deref().unwrap_or("-")
            )
        })
        .collect();
    println!("  Resumed: only {described:?} ran.");
    println!(
        "  Invoice loads: {}, ledger calls: {}, bank calls: {}.",
        fakes.loads(),
        fakes.ledger.calls(),
        fakes.bank.calls()
    );
    println!("  Completed: {}", paid.summary());
    let again = graph
        .resume(
            "inv-1004",
            HumanSignal::approve("ops-oncall"),
            store.clone(),
        )
        .await;
    let GraphExecutionResult::Failed(StrataError::ExecutionAborted(refusal)) = again else {
        fail("expected a second resume to be refused");
    };
    println!("  A second resume of the same checkpoint: {refusal}");
    check(
        matches!(resumed.as_slice(), [c] if c.path == "PayVendor" && c.attempt == 1 && c.tenant.as_deref() == Some("acme-eu")),
        "expected the resume to re-enter PayVendor only, at attempt 1, with the stored tenant",
    );
    check(
        (fakes.loads(), fakes.ledger.calls(), fakes.bank.calls()) == (1, 1, 2),
        "expected no earlier node to run again",
    );
    check(
        refusal.contains("already resumed"),
        "expected a second resume to be refused as already resumed",
    );

    // --- Scenario 5: the run's deadline passes mid-flight; resume it. ---

    println!();
    println!(
        "=== Scenario 5: the run's deadline passes during the bank call -- RunConfig::with_deadline ==="
    );
    let fakes = Fakes::new();
    fakes.bank.hang.store(true, Ordering::SeqCst);
    let graph = Arc::new(build(&fakes, 1));
    // The deadline is a std Instant; taking it from tokio's (paused) clock keeps it on that clock.
    let deadline = (Instant::now() + RUN_DEADLINE).into_std();
    let run = spawn_run(
        &graph,
        &store,
        Invoice::new("inv-1005", "Stark Industries", 740000, "USD"),
        options().with_deadline(deadline),
    );
    fakes.bank.stall.started().await;
    move_clock(RUN_DEADLINE).await;
    let late = expect_cancelled(run.await.expect("the run task"));
    let late_checkpoint = load(&store, "inv-1005").await;
    let late_reason = meta(&late_checkpoint, "cancel_reason");
    println!(
        "  Cancelled at '{}', reason {:?}; checkpoint {:?}, cancel_reason = {late_reason}",
        late.node, late.reason, late_checkpoint.status
    );
    fakes.bank.hang.store(false, Ordering::SeqCst);
    let fresh_deadline = (Instant::now() + RUN_DEADLINE).into_std();
    let paid = expect_completed(
        graph
            .resume_with(
                "inv-1005",
                HumanSignal::approve("ops-oncall"),
                store.clone(),
                options().with_deadline(fresh_deadline),
            )
            .await,
    );
    println!("  Resumed with a fresh deadline: {}", paid.summary());
    check(
        late.node == "PayVendor" && late.reason == CancelReason::Deadline,
        "expected a deadline cancellation at PayVendor",
    );
    check(late_reason == "deadline", "expected cancel_reason deadline");
    check(
        (fakes.loads(), fakes.bank.calls()) == (1, 2),
        "expected the resume to re-enter PayVendor only",
    );

    // --- Scenario 6 (Rust only): inline sync nodes. ---

    println!();
    println!("=== Scenario 6 (Rust only): an inline sync node -- add_inline_node, design/39 ===");
    let fakes = Fakes::new();
    let graph = rails(&fakes, None)
        .compile()
        .expect("the rails graph is valid");
    let paid = expect_completed(
        graph
            .run(
                "inv-1006",
                Invoice::new("inv-1006", "Wayne Enterprises", 64000, "EUR"),
                store.clone(),
            )
            .await,
    );
    let threads = fakes.threads();
    let pick_here = threads.get("PickRail") == Some(&main_thread);
    let post_elsewhere = threads
        .get("PostToLedger")
        .is_some_and(|t| *t != main_thread);
    println!(
        "  PickRail (add_inline_node) ran on the thread polling the run: {pick_here}; rail {}",
        paid.rail.as_deref().unwrap_or("-")
    );
    println!("  PostToLedger (add_fallible_node) ran on a blocking-pool thread: {post_elsewhere}");
    let refused = match rails(&fakes, None)
        .set_timeout("PickRail", Duration::from_secs(1))
        .compile()
    {
        Err(StrataError::CompilationFailed(message)) => message,
        Err(other) => fail(&format!(
            "expected CompilationFailed for a timeout on an inline node, got {other:?}"
        )),
        Ok(_) => fail("expected compile to refuse a timeout on an inline node"),
    };
    println!("  set_timeout(\"PickRail\", 1 s): compile refuses it: {refused}");
    check(
        pick_here && post_elsewhere,
        "expected the inline node on the polling thread and the pooled one off it",
    );
    check(
        paid.rail.as_deref() == Some("SEPA"),
        "expected a EUR invoice to go by SEPA",
    );
    check(
        refused.starts_with("node 'PickRail' runs inline and can't be timed out"),
        "expected the engine's refusal of a timeout on an inline node",
    );

    // An operator's cancel that lands while PickRail runs (the handler fires the token itself,
    // so the timing is exact). Nothing can abandon an inline node: its result is committed and
    // the run stops before the next node -- unlike scenario 4, where the node the cancel landed
    // on was abandoned and is re-entered by the resume.
    let fakes = Fakes::new();
    let token = CancellationToken::new();
    let graph = rails(&fakes, Some(token.clone()))
        .compile()
        .expect("the rails graph is valid");
    let stopped = expect_cancelled(
        graph
            .run_with(
                "inv-1007",
                Invoice::new("inv-1007", "Wonka Industries", 7500, "USD"),
                store.clone(),
                RunConfig::new().with_cancellation(token),
            )
            .await,
    );
    let rail_checkpoint = load(&store, "inv-1007").await;
    println!(
        "  A cancel that lands while PickRail runs: Cancelled at '{}', reason {:?}; checkpoint next node {}, frozen rail {}",
        stopped.node,
        stopped.reason,
        rail_checkpoint.next_node.as_deref().unwrap_or("-"),
        rail_checkpoint.frozen_state["rail"].as_str().unwrap_or("-")
    );
    println!(
        "  PickRail ran {} time(s); ledger calls: {} -- its result was committed, and the run stopped before PostToLedger.",
        fakes.picks(),
        fakes.ledger.calls()
    );
    check(
        stopped.node == "PostToLedger" && stopped.reason == CancelReason::Requested,
        "expected the cancellation to take effect at the node after the inline one",
    );
    check(
        stopped.state.rail.as_deref() == Some("SWIFT")
            && rail_checkpoint.frozen_state["rail"] == "SWIFT",
        "expected the inline node's result to be committed",
    );
    check(
        fakes.picks() == 1 && fakes.ledger.calls() == 0,
        "expected PickRail once and no ledger call",
    );

    println!();
    println!(
        "=== Done: async handlers saw their context, timeouts became retries, a blocked handler was abandoned, cancelled runs resumed where they stopped, and an inline node ran to its end under a cancel. ==="
    );
}

/// The run-level inputs every scenario's run shares: the tenant and the FX service.
fn options() -> RunConfig {
    RunConfig::new()
        .with_metadata("tenant", "acme-eu")
        .with_service(Arc::new(FxRates))
}

/// Moves the paused clock `by`, plus one more millisecond: tokio's timer wheel has millisecond
/// resolution and rounds a timer's deadline up to the next tick, so a timer set at a moment that
/// isn't on a tick fires up to 1 ms after its nominal time. Without the extra tick, a timeout set
/// while a blocking-pool task runs (which stops tokio auto-advancing the clock) would not fire.
async fn move_clock(by: Duration) {
    tokio::time::advance(by + Duration::from_millis(1)).await;
}

/// Runs `graph` on a task, so the scenario can act while it runs.
fn spawn_run(
    graph: &Arc<CompiledGraph<Invoice>>,
    store: &Arc<dyn CheckpointStore>,
    invoice: Invoice,
    config: RunConfig,
) -> tokio::task::JoinHandle<GraphExecutionResult<Invoice>> {
    let (graph, store) = (graph.clone(), store.clone());
    tokio::spawn(async move {
        let thread = invoice.id.clone();
        graph.run_with(thread, invoice, store, config).await
    })
}

/// A safety net: if a scenario ever hangs, fail after a minute of real time instead of hanging.
/// It doesn't touch tokio's clock, and a healthy run never gets near it.
fn watchdog() {
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(60));
        fail("the demo hung");
    });
}

async fn load(store: &Arc<dyn CheckpointStore>, thread_id: &str) -> Checkpoint {
    match store.load(thread_id).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => fail(&format!("expected a checkpoint for '{thread_id}'")),
        Err(error) => fail(&format!("loading '{thread_id}' failed: {error:?}")),
    }
}

fn meta(checkpoint: &Checkpoint, key: &str) -> String {
    checkpoint
        .metadata
        .as_ref()
        .and_then(|m| m.get(key))
        .cloned()
        .unwrap_or_default()
}

fn print_checkpoint(checkpoint: &Checkpoint) {
    let metadata: BTreeMap<_, _> = checkpoint
        .metadata
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let metadata: Vec<String> = metadata.iter().map(|(k, v)| format!("{k} = {v}")).collect();
    println!(
        "  Checkpoint: {:?}, schema {}, next node {}, metadata {{ {} }}",
        checkpoint.status,
        checkpoint.schema_version,
        checkpoint.next_node.as_deref().unwrap_or("-"),
        metadata.join(", ")
    );
    println!(
        "    frozen state: ledger_ref = {}, payment_ref = {} (the state PayVendor started with)",
        checkpoint.frozen_state["ledger_ref"]
            .as_str()
            .unwrap_or("null"),
        checkpoint.frozen_state["payment_ref"]
            .as_str()
            .unwrap_or("null")
    );
}

fn describe(result: &GraphExecutionResult<Invoice>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            format!("Interrupted at {suspended_node}")
        }
        GraphExecutionResult::Failed(error) => format!("Failed({error:?})"),
        GraphExecutionResult::Cancelled { node, reason, .. } => {
            format!("Cancelled at {node} ({reason:?})")
        }
    }
}

fn expect_completed(result: GraphExecutionResult<Invoice>) -> Invoice {
    match result {
        GraphExecutionResult::Completed(state) => state,
        other => fail(&format!("expected Completed, got {}", describe(&other))),
    }
}

struct Cancelled {
    node: String,
    reason: CancelReason,
    state: Invoice,
    stopped_at: Vec<String>,
}

fn expect_cancelled(result: GraphExecutionResult<Invoice>) -> Cancelled {
    match result {
        GraphExecutionResult::Cancelled {
            node,
            reason,
            state,
            stopped_at,
            ..
        } => Cancelled {
            node,
            reason,
            state,
            stopped_at,
        },
        other => fail(&format!("expected Cancelled, got {}", describe(&other))),
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

// --- The state and the graph. ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invoice {
    id: String,
    vendor: String,
    /// Money in cents: Rust has no decimal type in std, and integers keep the demo exact.
    amount_cents: i64,
    currency: String,
    amount_eur_cents: Option<i64>,
    checks: Vec<String>,
    fraud_score: Option<u32>,
    ledger_ref: Option<String>,
    payment_ref: Option<String>,
    /// Only scenario 6's graph sets it.
    rail: Option<String>,
}

impl Invoice {
    fn new(id: &str, vendor: &str, amount_cents: i64, currency: &str) -> Self {
        Self {
            id: id.to_string(),
            vendor: vendor.to_string(),
            amount_cents,
            currency: currency.to_string(),
            amount_eur_cents: None,
            checks: Vec::new(),
            fraud_score: None,
            ledger_ref: None,
            payment_ref: None,
            rail: None,
        }
    }

    fn summary(&self) -> String {
        let eur = match (self.currency.as_str(), self.amount_eur_cents) {
            ("EUR", _) | (_, None) => String::new(),
            (_, Some(cents)) => format!(" = EUR {}", money(cents)),
        };
        format!(
            "{}: {} {}{eur}, fraud score {}, ledger {}, payment {}",
            self.vendor,
            self.currency,
            money(self.amount_cents),
            self.fraud_score.map_or("-".to_string(), |s| s.to_string()),
            self.ledger_ref.as_deref().unwrap_or("-"),
            self.payment_ref.as_deref().unwrap_or("-"),
        )
    }
}

fn money(cents: i64) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

/// LoadInvoice -> Screen (Sanctions, Duplicates) -> ScoreFraud -> PostToLedger -> PayVendor.
fn build(f: &Arc<Fakes>, fraud_retries: u32) -> CompiledGraph<Invoice> {
    let screen = ParallelBranches::<Invoice>::new()
        .async_branch(
            "Sanctions",
            handler(f, |_, mut invoice, _ctx| async move {
                tokio::task::yield_now().await;
                invoice.checks = vec!["sanctions: clear".to_string()];
                Ok(invoice)
            }),
        )
        .async_branch(
            "Duplicates",
            handler(f, |_, mut invoice, _ctx| async move {
                tokio::task::yield_now().await;
                invoice.checks = vec!["duplicates: none".to_string()];
                Ok(invoice)
            }),
        );
    let ledger = f.clone();

    StateGraph::<Invoice>::new()
        .add_async_node(
            "LoadInvoice",
            handler(f, |f, mut invoice, ctx| async move {
                f.loads.fetch_add(1, Ordering::SeqCst);
                let rates = ctx
                    .service::<FxRates>()
                    .expect("RunConfig::with_service supplies the FX rates");
                let rate = rates.to_eur_basis_points(&invoice.currency).await;
                invoice.amount_eur_cents = Some(invoice.amount_cents * rate / 10_000);
                invoice
            }),
        )
        .add_parallel_group(
            "Screen",
            screen,
            |entry: &Invoice, outputs: Vec<Invoice>| Invoice {
                checks: outputs.into_iter().flat_map(|o| o.checks).collect(),
                ..entry.clone()
            },
        )
        .add_async_fallible_node(
            "ScoreFraud",
            handler(f, |f, mut invoice, ctx| async move {
                invoice.fraud_score = Some(f.fraud.score(ctx.attempt(), ctx.cancellation()).await);
                Ok(invoice)
            }),
        )
        .set_timeout("ScoreFraud", FRAUD_TIMEOUT)
        .set_recovery_policy(
            "ScoreFraud",
            if fraud_retries == 0 {
                RecoveryPolicy::default()
            } else {
                RecoveryPolicy::retry(fraud_retries)
            },
        )
        // A sync handler: the legacy SDK has no async API and no way to be told to stop. The
        // engine runs it on tokio's blocking pool.
        .add_fallible_node("PostToLedger", move |invoice: &Invoice| {
            Ok(Invoice {
                ledger_ref: Some(ledger.ledger.post(&invoice.id)),
                ..invoice.clone()
            })
        })
        .set_timeout("PostToLedger", LEDGER_TIMEOUT)
        .set_recovery_policy("PostToLedger", RecoveryPolicy::retry(1))
        .add_async_fallible_node(
            "PayVendor",
            handler(f, |f, mut invoice, ctx| async move {
                invoice.payment_ref = Some(f.bank.pay(ctx.cancellation()).await);
                Ok(invoice)
            }),
        )
        .set_recovery_policy("PayVendor", RecoveryPolicy::retry(2))
        .add_edge("LoadInvoice", "Screen")
        .add_edge("Screen", "ScoreFraud")
        .add_edge("ScoreFraud", "PostToLedger")
        .add_edge("PostToLedger", "PayVendor")
        .compile()
        .expect("the payment graph is valid")
}

/// Scenario 6: PickRail (inline) -> PostToLedger (on the blocking pool). With `cancel`, PickRail
/// fires that token while it runs, standing in for an operator's cancel landing just then.
fn rails(f: &Arc<Fakes>, cancel: Option<CancellationToken>) -> StateGraph<Invoice> {
    let (picker, poster) = (f.clone(), f.clone());
    StateGraph::<Invoice>::new()
        // Pure computation: no I/O, no lock held for long, so it may run on the executor.
        .add_inline_node("PickRail", move |invoice: Invoice| {
            picker.ran_on("PickRail");
            picker.picks.fetch_add(1, Ordering::SeqCst);
            if let Some(cancel) = &cancel {
                cancel.cancel();
            }
            let rail = if invoice.currency == "EUR" {
                "SEPA"
            } else {
                "SWIFT"
            };
            Invoice {
                rail: Some(rail.to_string()),
                ..invoice
            }
        })
        .add_fallible_node("PostToLedger", move |invoice: &Invoice| {
            poster.ran_on("PostToLedger");
            Ok(Invoice {
                ledger_ref: Some(poster.ledger.post(&invoice.id)),
                ..invoice.clone()
            })
        })
        .add_edge("PickRail", "PostToLedger")
}

/// Wraps an async handler body so it records what its `NodeContext` said before running.
fn handler<T, B, Fut>(
    f: &Arc<Fakes>,
    body: B,
) -> impl Fn(Invoice, NodeContext) -> Fut + Send + Sync + 'static
where
    B: Fn(Arc<Fakes>, Invoice, NodeContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    let f = f.clone();
    move |invoice, ctx| {
        f.saw(&ctx);
        body(f.clone(), invoice, ctx)
    }
}

// --- The stand-in services. ---

/// What one async handler was told by its `NodeContext`.
#[derive(Clone)]
struct Seen {
    node: String,
    path: String,
    attempt: u32,
    thread: String,
    run_id: String,
    tenant: Option<String>,
}

/// The stand-in services one scenario runs against, and what they recorded.
#[derive(Default)]
struct Fakes {
    loads: AtomicU32,
    picks: AtomicU32,
    contexts: Mutex<Vec<Seen>>,
    threads: Mutex<BTreeMap<&'static str, ThreadId>>,
    fraud: FraudApi,
    ledger: LedgerSdk,
    bank: BankClient,
}

impl Fakes {
    fn new() -> Arc<Self> {
        Arc::default()
    }

    fn saw(&self, ctx: &NodeContext) {
        self.contexts.lock().unwrap().push(Seen {
            node: ctx.node().to_string(),
            path: ctx.node_path().to_string(),
            attempt: ctx.attempt(),
            thread: ctx.thread_id().to_string(),
            run_id: ctx.run_id().to_string(),
            tenant: ctx.metadata().get("tenant").cloned(),
        });
    }

    fn contexts(&self) -> Vec<Seen> {
        self.contexts.lock().unwrap().clone()
    }

    fn ran_on(&self, node: &'static str) {
        self.threads
            .lock()
            .unwrap()
            .insert(node, std::thread::current().id());
    }

    fn threads(&self) -> BTreeMap<&'static str, ThreadId> {
        self.threads.lock().unwrap().clone()
    }

    fn loads(&self) -> u32 {
        self.loads.load(Ordering::SeqCst)
    }

    fn picks(&self) -> u32 {
        self.picks.load(Ordering::SeqCst)
    }
}

/// A call that hangs until the engine gives up on it. Says when it started, keeps the token it was
/// handed, and notes when its future is dropped: in Rust the engine stops an async handler by
/// dropping its future, so the handler needn't watch the token to stop.
#[derive(Default)]
struct Stall {
    started: Notify,
    token: Mutex<Option<CancellationToken>>,
    dropped: AtomicBool,
}

impl Stall {
    async fn hang(&self, token: &CancellationToken) {
        struct OnDrop<'a>(&'a AtomicBool);
        impl Drop for OnDrop<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        *self.token.lock().unwrap() = Some(token.clone());
        let _dropped = OnDrop(&self.dropped);
        self.started.notify_one();
        std::future::pending::<()>().await;
    }

    async fn started(&self) {
        self.started.notified().await;
    }

    fn saw_cancel(&self) -> bool {
        self.token
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
    }

    fn was_dropped(&self) -> bool {
        self.dropped.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct FraudApi {
    /// The attempt that hangs; 0 for none.
    hang_on_attempt: AtomicU32,
    stall: Stall,
}

impl FraudApi {
    async fn score(&self, attempt: u32, token: &CancellationToken) -> u32 {
        if attempt == self.hang_on_attempt.load(Ordering::SeqCst) {
            self.stall.hang(token).await;
        }
        tokio::task::yield_now().await;
        12
    }
}

#[derive(Default)]
struct BankClient {
    hang: AtomicBool,
    calls: AtomicU32,
    stall: Stall,
}

impl BankClient {
    async fn pay(&self, token: &CancellationToken) -> String {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.hang.load(Ordering::SeqCst) {
            self.stall.hang(token).await;
        }
        tokio::task::yield_now().await;
        format!("PAY-{}", 7000 + call)
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

/// A gate a blocking call waits on until the scenario opens it.
#[derive(Default)]
struct Gate(Mutex<bool>, Condvar);

impl Gate {
    fn wait(&self) {
        let open = self.0.lock().unwrap();
        let _open = self
            .1
            .wait_timeout_while(open, Duration::from_secs(30), |open| !*open)
            .unwrap();
    }

    fn open(&self) {
        *self.0.lock().unwrap() = true;
        self.1.notify_all();
    }
}

/// A blocking SDK: the first call can be made to block on a gate the scenario controls.
#[derive(Default)]
struct LedgerSdk {
    block_first_call: AtomicBool,
    calls: AtomicU32,
    gate: Gate,
    first_call_started: Notify,
    first_call_finished: AtomicBool,
    first_call_returned: Notify,
    entries: Mutex<Vec<String>>,
}

impl LedgerSdk {
    fn post(&self, invoice_id: &str) -> String {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let blocks = call == 1 && self.block_first_call.load(Ordering::SeqCst);
        if blocks {
            self.first_call_started.notify_one();
            self.gate.wait();
        }
        let reference = format!("LED-{}", 1000 + call);
        self.entries
            .lock()
            .unwrap()
            .push(format!("{reference} {invoice_id}"));
        if blocks {
            self.first_call_finished.store(true, Ordering::SeqCst);
            self.first_call_returned.notify_one();
        }
        reference
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn entries(&self) -> Vec<String> {
        self.entries.lock().unwrap().clone()
    }
}

/// An FX service the run hands every async handler (`RunConfig::with_service`).
struct FxRates;

impl FxRates {
    /// The rate to EUR in basis points (1/10 000).
    async fn to_eur_basis_points(&self, currency: &str) -> i64 {
        tokio::task::yield_now().await;
        if currency == "USD" { 9_200 } else { 10_000 }
    }
}
