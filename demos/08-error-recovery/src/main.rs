//! Error recovery (`design/04-Error-Recovery.md`, `design/25-Retry-Backoff.md`):
//! retry with backoff, a fallback that knows why, rollback on failure, and
//! resuming a failed thread.
//!
//! An order pipeline -- `ValidateOrder` -> `ChargePayment` ->
//! `SendConfirmation` -- where `ChargePayment` talks to a flaky simulated
//! payment gateway. `ChargePayment` is a *fallible* node
//! (`.add_fallible_node(...)`): it returns `Result<S, NodeError>`, and its
//! `RecoveryPolicy` (`.set_recovery_policy(...)`) says what the engine does
//! when it errs: wait, retry a transient failure, then route to a fallback
//! node, or persist a `Failed` checkpoint if there is none.
//!
//! Every attempt of `ChargePayment` first *reserves a receipt number* on the
//! state it is building, and only then calls the gateway. That reservation is
//! the partial work the demo watches: a failed attempt must not leak it into
//! the committed state, the fallback, or the checkpoint.
//!
//! The scenarios:
//!
//! 1. **Transient blip** -- the gateway times out twice, then works. The
//!    retries wait 200 ms, then 400 ms (exponential backoff, no jitter so the
//!    numbers are stable).
//! 2. **Rate limited** -- every call is a 503 with `Retry-After: 30s`
//!    (`NodeError::transient_after`). The hint replaces the computed delay,
//!    capped at the policy's 2 s max. Retries run out, and the fallback reads
//!    why from `NodeContext::last_failure()`.
//! 3. **Card declined** -- a `Fatal` error skips every retry and every wait.
//!    With no fallback, the engine persists a `Failed` checkpoint to a real
//!    SQLite file.
//! 4. A second, independent store instance reads that checkpoint back.
//! 5. The customer updates their card, and the `Failed` checkpoint is resumed
//!    with the new card as an override: `ChargePayment` runs again,
//!    `ValidateOrder` doesn't. A second resume is refused.
//! 6. A fallback that doesn't exist is caught by `.compile()`.
//!
//! The runtime starts with tokio's clock paused (`start_paused`, tokio's
//! `test-util` feature), so the engine's backoff sleeps auto-advance virtual
//! time and 4 seconds of waiting take none. The delays printed come from the
//! engine's `RetryScheduled` run events, not from timing anything.
//!
//! No live service and no LLM: the gateway is a deterministic stand-in, so the
//! demo runs offline.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult, HumanSignal};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, Jitter, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{RunEventChannel, RunEventKind};
use datalium_strata::schema::CheckpointStatus;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use serde::{Deserialize, Serialize};

const FALLBACK: &str = "QueueForManualReview";
const EXPIRED_CARD: &str = "tok_expired_0119";
const NEW_CARD: &str = "tok_visa_4242";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderState {
    order_id: String,
    amount: f64,
    card_token: String,
    payment_status: String,
    receipt_id: Option<String>,
    confirmed: bool,
    review_note: Option<String>,
}

impl OrderState {
    fn new(order_id: &str, amount: f64, card_token: &str) -> Self {
        Self {
            order_id: order_id.to_string(),
            amount,
            card_token: card_token.to_string(),
            payment_status: "new".to_string(),
            receipt_id: None,
            confirmed: false,
            review_note: None,
        }
    }
}

/// How the simulated gateway misbehaves. Whatever the mode, it declines an
/// expired card.
#[derive(Clone, Copy)]
enum Outage {
    None,
    /// Times out for the first `failures` calls, then works.
    Blip {
        failures: u32,
    },
    /// Every call is a 503 that asks the caller to wait 30 seconds.
    RateLimited,
}

struct PaymentGateway {
    outage: Outage,
    calls: AtomicU32,
}

impl PaymentGateway {
    fn new(outage: Outage) -> Arc<Self> {
        Arc::new(Self {
            outage,
            calls: AtomicU32::new(0),
        })
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn charge(&self, order_id: &str, amount: f64, card_token: &str) -> Result<(), NodeError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let result = match self.outage {
            Outage::Blip { failures } if call <= failures => {
                Err(NodeError::Transient("gateway timed out".to_string()))
            }
            // A 503 with Retry-After: the hint travels with the error.
            Outage::RateLimited => Err(NodeError::transient_after(
                "gateway unavailable (503)",
                Duration::from_secs(30),
            )),
            _ if card_token.starts_with("tok_expired") => {
                Err(NodeError::Fatal("card declined: expired".to_string()))
            }
            _ => Ok(()),
        };

        let outcome = match &result {
            Ok(()) => "approved".to_string(),
            Err(error) if error.is_transient() => format!("transient error: {}", error.message()),
            Err(error) => format!("fatal error: {}", error.message()),
        };
        println!(
            "    [gateway] call {call} for {order_id} (${amount:.2}, {card_token}): {outcome}"
        );
        result
    }
}

/// `ValidateOrder` -> `ChargePayment` -> `SendConfirmation`. When `policy`
/// names a fallback node, `QueueForManualReview` is added as that node. It
/// gets no ordinary edge, so the recovery policy is the only way in.
/// `validations` counts the runs of `ValidateOrder`.
fn order_graph(
    gateway: Arc<PaymentGateway>,
    validations: Arc<AtomicU32>,
    policy: RecoveryPolicy,
) -> CompiledGraph<OrderState> {
    let fallback = policy.fallback.clone();

    let mut graph = StateGraph::<OrderState>::new()
        .add_node("ValidateOrder", move |s: OrderState| {
            validations.fetch_add(1, Ordering::SeqCst);
            OrderState {
                payment_status: "validated".to_string(),
                ..s
            }
        })
        .add_fallible_node("ChargePayment", move |s: &OrderState| {
            // Reserve a receipt number on the state being built *before*
            // calling the gateway. If the call fails, the reservation is
            // discarded with the rest of the failed attempt.
            let mut next = s.clone();
            next.receipt_id = Some(format!("RCPT-{}-{}", s.order_id, gateway.calls() + 1));

            gateway.charge(&s.order_id, s.amount, &s.card_token)?;

            next.payment_status = "charged".to_string();
            Ok(next)
        })
        .add_node("SendConfirmation", |s: OrderState| OrderState {
            confirmed: true,
            ..s
        })
        .add_edge("ValidateOrder", "ChargePayment")
        .add_edge("ChargePayment", "SendConfirmation")
        .set_recovery_policy("ChargePayment", policy);

    if let Some(fallback) = fallback {
        // An async node, because only an async handler gets a NodeContext --
        // and only a fallback's context carries `last_failure()`: the final
        // failure that made the policy give up, with its kind, attempts and
        // retry hint.
        graph = graph.add_async_node(fallback, |s: OrderState, ctx: NodeContext| async move {
            let failure = ctx
                .last_failure()
                .expect("a fallback always has a last_failure");
            let kind = if failure.error.is_transient() {
                "Transient"
            } else {
                "Fatal"
            };
            let mut note = format!(
                "{} failed ({kind}) after {} attempt(s): {}",
                failure.node,
                failure.attempts,
                failure.error.message()
            );
            if let Some(wait) = failure.error.retry_after() {
                note += &format!("; gateway asked to wait {}s", wait.as_secs());
            }
            OrderState {
                payment_status: "manual_review".to_string(),
                review_note: Some(note),
                ..s
            }
        });
    }

    graph.compile().expect("graph is structurally valid")
}

/// Runs `thread_id` with a run-event observer and returns the result with the
/// delay of every retry the engine scheduled (its `RetryScheduled` events).
/// The clock is paused, so the backoff sleeps cost no real time.
async fn run_observed(
    graph: &CompiledGraph<OrderState>,
    thread_id: &str,
    initial: OrderState,
    store: Arc<dyn CheckpointStore>,
) -> (GraphExecutionResult<OrderState>, Vec<Duration>) {
    let (observer, mut events) = RunEventChannel::new();
    let result = graph
        .run_with(
            thread_id,
            initial,
            store,
            RunConfig::new().with_observer(observer),
        )
        .await;
    // The run has returned, so every event it produced is already queued.
    let mut delays = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let RunEventKind::RetryScheduled { delay, .. } = event.kind {
            delays.push(delay);
        }
    }
    (result, delays)
}

/// A file in this demo's own directory (`.gitignore`d, deleted at the start
/// and end of `main`) so the `Failed` checkpoint is a real row on disk.
fn checkpoint_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("recovery-checkpoint.sqlite")
}

fn open_store(path: &PathBuf) -> Arc<dyn CheckpointStore> {
    Arc::new(SqliteCheckpointStore::open(path).expect("failed to open checkpoint store"))
}

async fn checkpoint_status(store: &dyn CheckpointStore, thread_id: &str) -> String {
    match store
        .load(thread_id)
        .await
        .expect("checkpoint store readable")
    {
        Some(checkpoint) => format!("{:?}", checkpoint.status),
        None => "none".to_string(),
    }
}

fn ms(milliseconds: u64) -> Duration {
    Duration::from_millis(milliseconds)
}

fn format_delays(delays: &[Duration]) -> String {
    if delays.is_empty() {
        return "none".to_string();
    }
    delays
        .iter()
        .map(|delay| format!("{} ms", delay.as_millis()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn check(ok: bool, what: &str) {
    if !ok {
        eprintln!("check failed: {what}");
        std::process::exit(1);
    }
}

fn expect_completed(result: GraphExecutionResult<OrderState>) -> OrderState {
    match result {
        GraphExecutionResult::Completed(state) => state,
        other => {
            eprintln!("check failed: expected Completed, got {}", describe(&other));
            std::process::exit(1);
        }
    }
}

fn describe(result: &GraphExecutionResult<OrderState>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Interrupted { .. } => "Interrupted".to_string(),
        GraphExecutionResult::Failed(err) => format!("Failed({err:?})"),
        GraphExecutionResult::Cancelled { node, reason, .. } => {
            format!("Cancelled({node}, {reason:?})")
        }
    }
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    let path = checkpoint_path();
    // Start fresh so a stale checkpoint from a previous run can't leak in.
    let _ = std::fs::remove_file(&path);

    // Exponential backoff: the n-th retry waits 200 ms x 2^(n-1), capped at
    // 2 s. The default jitter is Full (a random wait in [0, d]); the demo
    // turns it off so the numbers are stable.
    let backoff = RecoveryPolicy::retry(3).with_backoff(ms(200), ms(2000), Jitter::None);

    // --- Scenarios 1-3, all against store instance #1. ---

    let store = open_store(&path);

    // --- Scenario 1: a transient blip, recovered by retries that back off. ---

    println!(
        "=== Scenario 1: transient blip -- retry(3), backoff 200 ms doubling to 2 s, fallback '{FALLBACK}' ==="
    );
    let gateway = PaymentGateway::new(Outage::Blip { failures: 2 });
    let graph = order_graph(
        gateway.clone(),
        Arc::default(),
        backoff.clone().with_fallback(FALLBACK),
    );
    let started = tokio::time::Instant::now();
    let (result, delays) = run_observed(
        &graph,
        "order-1001",
        OrderState::new("ord-1001", 129.99, NEW_CARD),
        store.clone(),
    )
    .await;
    let waited = started.elapsed();
    let state = expect_completed(result);
    println!(
        "  Completed after {} gateway call(s). Waits before each retry: {}",
        gateway.calls(),
        format_delays(&delays)
    );
    println!(
        "  The paused clock moved {} ms; no real time was spent waiting.",
        waited.as_millis()
    );
    println!("  State: {state:?}");
    println!(
        "  Only the successful attempt's reservation was committed: {:?}",
        state.receipt_id
    );
    let status = checkpoint_status(store.as_ref(), "order-1001").await;
    println!("  Checkpoint for 'order-1001': {status} (a recovered failure leaves nothing behind)");
    check(
        gateway.calls() == 3 && state.receipt_id.as_deref() == Some("RCPT-ord-1001-3"),
        "expected the third attempt to succeed with its own reservation",
    );
    check(delays == [ms(200), ms(400)], "expected 200 ms, then 400 ms");
    // Virtual time only moves when the runtime has nothing to do but wait for
    // a timer, so it moved exactly as far as the engine's backoff slept.
    check(
        waited == delays.iter().sum::<Duration>(),
        "expected the paused clock to move exactly the scheduled delays",
    );
    check(
        status == "none",
        "expected no checkpoint for a recovered run",
    );

    // --- Scenario 2: rate limited until retries run out; the fallback learns why. ---

    println!();
    println!(
        "=== Scenario 2: rate limited (Retry-After: 30s) -- retry(2), same backoff, fallback '{FALLBACK}' ==="
    );
    let gateway = PaymentGateway::new(Outage::RateLimited);
    let policy = RecoveryPolicy {
        max_retries: 2,
        ..backoff.clone()
    }
    .with_fallback(FALLBACK);
    let graph = order_graph(gateway.clone(), Arc::default(), policy);
    let (result, delays) = run_observed(
        &graph,
        "order-1002",
        OrderState::new("ord-1002", 54.00, NEW_CARD),
        store.clone(),
    )
    .await;
    let state = expect_completed(result);
    println!(
        "  Retries exhausted after {} gateway call(s). Waits: {}",
        gateway.calls(),
        format_delays(&delays)
    );
    println!("  The 30 s hint replaced the computed delay, capped at the policy's 2 s max.");
    println!("  State: {state:?}");
    println!(
        "  receipt_id is None and 'SendConfirmation' never ran (confirmed: {}): the fallback ran on the state from before ChargePayment.",
        state.confirmed
    );
    check(
        delays == [ms(2000), ms(2000)],
        "expected the hint, capped at 2 s, twice",
    );
    check(
        state.payment_status == "manual_review" && state.receipt_id.is_none() && !state.confirmed,
        "expected the manual-review fallback with the failed attempts' reservations rolled back",
    );
    check(
        state.review_note.as_deref()
            == Some(
                "ChargePayment failed (Transient) after 3 attempt(s): gateway unavailable (503); gateway asked to wait 30s",
            ),
        "expected the fallback to record the failure it was given",
    );

    // --- Scenario 3: a fatal error with no fallback. ---

    println!();
    println!("=== Scenario 3: card declined -- retry(3), same backoff, no fallback ===");
    let gateway = PaymentGateway::new(Outage::None);
    let graph = order_graph(gateway.clone(), Arc::default(), backoff.clone());
    let (result, delays) = run_observed(
        &graph,
        "order-1003",
        OrderState::new("ord-1003", 899.00, EXPIRED_CARD),
        store.clone(),
    )
    .await;
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => {
            println!(
                "  Failed: node '{node}' gave up after {attempts} attempt(s) -- '{message}'. Retries scheduled: {}.",
                delays.len()
            );
            check(attempts == 1, "expected a single attempt for a fatal error");
        }
        other => {
            eprintln!(
                "check failed: expected Failed(NodeFailed), got {}",
                describe(&other)
            );
            std::process::exit(1);
        }
    }
    println!("  A fatal error is never retried, so it never waits either.");
    check(delays.is_empty(), "expected no retry for a fatal error");
    check(gateway.calls() == 1, "expected exactly one gateway call");

    // --- Step 4: drop store instance #1. A brand-new one reads the Failed checkpoint from disk. ---

    drop(store);
    println!();
    println!("=== Step 4: reading the Failed checkpoint back (store instance #2) ===");
    let store_2 = open_store(&path);
    let checkpoint = store_2
        .load("order-1003")
        .await
        .expect("checkpoint store readable")
        .expect("a Failed checkpoint must have been persisted");
    println!("  status:         {:?}", checkpoint.status);
    println!("  failed node:    {:?}", checkpoint.suspended_node);
    println!("  resume from:    {:?}", checkpoint.next_node);
    println!("  frozen state:   {}", checkpoint.frozen_state);
    let metadata = checkpoint.metadata.clone().unwrap_or_default();
    let mut keys: Vec<_> = metadata.keys().collect();
    keys.sort();
    for key in keys {
        println!("  metadata.{key}: {}", metadata[key]);
    }
    println!(
        "  The frozen state is the last committed state (after ValidateOrder). receipt_id is null: the declined attempt's reservation never reached it."
    );
    check(
        checkpoint.status == CheckpointStatus::Failed,
        "expected a Failed checkpoint",
    );
    check(
        checkpoint.suspended_node.as_deref() == Some("ChargePayment")
            && checkpoint.next_node.as_deref() == Some("ChargePayment"),
        "expected the failed node as both suspended_node and next_node",
    );
    check(
        checkpoint.frozen_state["receipt_id"].is_null()
            && checkpoint.frozen_state["payment_status"] == "validated",
        "expected the committed state from after ValidateOrder, with no receipt",
    );
    check(
        metadata.get("error").map(String::as_str) == Some("card declined: expired")
            && metadata.get("error_kind").map(String::as_str) == Some("fatal")
            && metadata.get("attempts").map(String::as_str) == Some("1"),
        "expected error, error_kind and attempts in the metadata",
    );

    // --- Step 5: fix the cause, then resume the Failed checkpoint. ---

    println!();
    println!(
        "=== Step 5: the customer updates their card; the Failed checkpoint is resumed (store instance #2) ==="
    );
    // A new gateway and a new graph, as a separate process would have.
    let resume_gateway = PaymentGateway::new(Outage::None);
    let validations = Arc::new(AtomicU32::new(0));
    let resume_graph = order_graph(resume_gateway.clone(), validations.clone(), backoff);

    // Resuming a failed thread re-enters the node that failed, with the
    // committed state plus the signal's overrides. It isn't answering an
    // interrupt boundary, so no decision is recorded; the signal is just the
    // carrier for the fix and who made it.
    let fix = HumanSignal::approve("support@shop.example").with_override("card_token", NEW_CARD);
    let resumed = expect_completed(
        resume_graph
            .resume("order-1003", fix.clone(), store_2.clone())
            .await,
    );
    println!("  State: {resumed:?}");
    println!(
        "  ChargePayment ran again ({} gateway call), ValidateOrder did not ({} runs), and SendConfirmation completed the order.",
        resume_gateway.calls(),
        validations.load(Ordering::SeqCst)
    );
    check(
        resume_gateway.calls() == 1 && validations.load(Ordering::SeqCst) == 0,
        "expected the resume to re-enter ChargePayment only",
    );
    check(
        resumed.payment_status == "charged" && resumed.confirmed && resumed.card_token == NEW_CARD,
        "expected the order to complete on the new card",
    );

    match resume_graph
        .resume("order-1003", fix, store_2.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            println!("  Resuming the same checkpoint again is refused: {message}");
            check(
                message.contains("already resumed"),
                "expected the refusal to say the checkpoint was already resumed",
            );
        }
        other => {
            eprintln!(
                "check failed: expected the second resume to be refused, got {}",
                describe(&other)
            );
            std::process::exit(1);
        }
    }
    check(
        resume_gateway.calls() == 1,
        "a refused resume must not charge the card again",
    );

    // --- Step 6: a fallback that doesn't exist. ---

    println!();
    println!("=== Step 6: a recovery policy is checked at .compile(), not at runtime ===");
    let misconfigured = StateGraph::<OrderState>::new()
        .add_fallible_node("ChargePayment", |s: &OrderState| Ok(s.clone()))
        .set_recovery_policy(
            "ChargePayment",
            RecoveryPolicy::retry(1).with_fallback("QueueForManualReveiw"),
        )
        .compile();
    match misconfigured {
        Err(StrataError::CompilationFailed(message)) => {
            println!("  Rejected: {message}");
            check(
                message.contains("does not exist"),
                "expected the misspelled fallback to be named as missing",
            );
        }
        Err(other) => {
            eprintln!("check failed: expected CompilationFailed, got {other:?}");
            std::process::exit(1);
        }
        Ok(_) => {
            eprintln!("check failed: expected the misspelled fallback to be rejected");
            std::process::exit(1);
        }
    }

    // Leave no state behind for the next `cargo run`.
    drop(store_2);
    let _ = std::fs::remove_file(&path);

    println!();
    println!(
        "=== Done: retries backed off, a rate limit's hint set the wait, the fallback knew why, and a fatal failure was resumed once its cause was fixed. ==="
    );
}
