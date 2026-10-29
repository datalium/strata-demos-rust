//! Thread leases with fencing (design/32-Thread-Leases.md, SG-12): two
//! workers, one thread.
//!
//! An invoice graph: `Draft` (an interrupt boundary, so the run freezes for
//! approval after it) -> `Send`. Two workers, worker-a and worker-b, get the
//! same invoice, as happens when a queue redelivers a message whose first
//! consumer went quiet. Each runs with
//! `RunConfig::with_lease(LeaseOptions::new(owner, ttl))`:
//!
//!   1. While worker-a holds the thread, worker-b is refused (`Failed` "thread
//!      ... is held by 'worker-a' until ...") and runs nothing. worker-a renews
//!      every ttl / 3, so a slow node keeps its thread.
//!   2. Without leases, the problem they solve: worker-a stalls, worker-b
//!      drafts and freezes the invoice, then worker-a wakes up and saves its
//!      stale draft on top. The thread now waits on the wrong draft.
//!   3. With leases, the same stall: worker-a's lease runs out, worker-b takes
//!      the thread (token 2), and worker-a's save under token 1 is refused.
//!      worker-a ends `Cancelled` with `CancelReason::LeaseLost` and writes
//!      nothing, not even a `cancelled` checkpoint.
//!   4. The usual way a worker finds out: its next renewal is refused, its run
//!      is cancelled at once, `Draft`'s future is dropped and its
//!      `CancellationToken` is cancelled, so it stops working on a thread that
//!      is no longer its.
//!
//! Time is fake. The program runs on a paused tokio clock (tokio's
//! `test-util` feature), which drives every run's renewals. The store has its
//! own clock (`InMemoryCheckpointStore::with_clock`), the one lease expiry is
//! judged by (Postgres uses its server clock for this): tokio's clock plus
//! whatever the demo adds to the store alone. A stalled worker is one that
//! sees no time pass while the store's clock moves on: a GC pause or a frozen
//! VM fires no timers.
//!
//!   cargo run -p thread-leases

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use datalium_strata::checkpoint::{
    CancelReason, CheckpointQuery, CheckpointStore, GraphExecutionResult, LeaseOptions, ThreadLease,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, RunOutcome};
use datalium_strata::schema::CheckpointStatus;
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use datalium_strata_memory::Clock;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

const TTL: Duration = Duration::from_secs(30);

fn noon() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 24, 12, 0, 0).unwrap()
}

// A current-thread runtime with its clock paused from the start: a timer
// fires when the demo lets time pass, or when nothing else can run.
#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    held().await;
    without_leases().await;
    fenced_save().await;
    lost_on_renewal().await;

    println!();
    println!("=== Done: one thread, one owner at a time, and a stale owner writes nothing. ===");
}

// --- 1. A held thread refuses a second worker. -------------------------------

async fn held() {
    println!("=== Scenario 1: worker-a holds the thread, worker-b is refused ===");
    const THREAD: &str = "invoice-1";

    // The store's clock follows tokio's, so worker-a's renewals move its
    // lease's expiry as time passes.
    let clock = StoreClock::new();
    let store = Arc::new(InMemoryCheckpointStore::new().with_clock(clock.clock()));
    let gate = Gate::new();
    let a = Worker::new("worker-a", Some(gate.stall()));
    let b = Worker::new("worker-b", None);

    let (a_observer, a_events) = RunEventChannel::new();
    let a_run = a.spawn(
        THREAD,
        &store,
        Some(leased("worker-a").with_observer(a_observer)),
    );
    gate.started().await;

    let lease = lease_of(&store, THREAD).await;
    println!(
        "  worker-a is in Draft. Lease: '{}', token {}, until {}.",
        lease.as_ref().map_or("-", |l| l.owner.as_str()),
        lease.as_ref().map_or(0, |l| l.token),
        text(lease.as_ref().map(|l| l.expires_at))
    );
    let (b_observer, b_events) = RunEventChannel::new();
    let refused = b
        .run(
            THREAD,
            &store,
            Some(leased("worker-b").with_observer(b_observer)),
        )
        .await;
    let refusal = refusal_of(&refused);
    println!(
        "  worker-b -> Failed: {}",
        refusal.as_deref().unwrap_or("-")
    );
    check(
        refusal.as_deref()
            == Some("thread 'invoice-1' is held by 'worker-a' until 2026-10-24T12:00:30Z"),
        "expected worker-b to be refused",
    );
    check(
        b.drafts() == 0 && log_length(&store, THREAD).await == 0,
        "expected the refused run to run and write nothing",
    );
    let b_seen = events_of(b_events).await;
    println!(
        "  worker-b's events: {}. It drafted nothing and wrote nothing.",
        describe(&b_seen)
    );
    check(
        !b_seen
            .iter()
            .any(|e| matches!(e.kind, RunEventKind::LeaseAcquired { .. })),
        "expected no LeaseAcquired for a refused run",
    );

    // Draft takes longer than the ttl. Every third of a ttl worker-a renews,
    // on tokio's clock, so its lease never lapses.
    for third in 1..=4 {
        let before = lease_of(&store, THREAD).await.map(|l| l.expires_at);
        tokio::time::sleep(TTL / 3).await;
        let mut renewed = false;
        for _ in 0..1000 {
            if lease_of(&store, THREAD).await.map(|l| l.expires_at) > before {
                renewed = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        check(
            renewed,
            &format!("expected renewal {third} to move the lease's expiry"),
        );
    }

    let renewed = lease_of(&store, THREAD).await;
    println!(
        "  40 s later, still in Draft: worker-a renewed every 10 s. Lease: token {}, until {}.",
        renewed.as_ref().map_or(0, |l| l.token),
        text(renewed.as_ref().map(|l| l.expires_at))
    );
    let again = b.run(THREAD, &store, Some(leased("worker-b"))).await;
    let refusal = refusal_of(&again);
    println!(
        "  worker-b again -> Failed: {}",
        refusal.as_deref().unwrap_or("-")
    );
    check(
        renewed
            .as_ref()
            .is_some_and(|l| l.owner == "worker-a" && l.token == 1)
            && refusal.as_deref()
                == Some("thread 'invoice-1' is held by 'worker-a' until 2026-10-24T12:01:10Z"),
        "expected the renewed lease to keep worker-b out past the first ttl",
    );

    gate.release();
    let a_result = a_run.await.expect("worker-a's run");
    check(
        matches!(a_result, GraphExecutionResult::Interrupted { .. }),
        "expected worker-a to freeze at Draft",
    );
    println!(
        "  worker-a froze at Draft for approval. Latest checkpoint: {}.",
        latest(&store, THREAD).await
    );
    let a_seen = events_of(a_events).await;
    println!("  worker-a's events: {}.", describe(&a_seen));
    check(
        acquired(&a_seen) == [("worker-a".to_string(), 1)],
        "expected worker-a to acquire token 1",
    );
    check(
        lease_of(&store, THREAD).await.is_none(),
        "expected worker-a to release its lease when its run returned",
    );
    println!(
        "  Lease after the run: none. A lease lasts as long as one run, not as long as the thread."
    );
}

// --- 2. Without leases, the stale worker wins. -------------------------------

async fn without_leases() {
    println!();
    println!("=== Scenario 2: no leases -- the stale worker overwrites the new one ===");
    const THREAD: &str = "invoice-2";
    let store = Arc::new(InMemoryCheckpointStore::new());
    let gate = Gate::new();
    let a = Worker::new("worker-a", Some(gate.stall()));
    let b = Worker::new("worker-b", None);

    let a_run = a.spawn(THREAD, &store, None);
    gate.started().await;
    println!("  worker-a stalls in Draft. The queue gives the invoice to worker-b.");
    let b_result = b.run(THREAD, &store, None).await;
    check(
        matches!(b_result, GraphExecutionResult::Interrupted { .. }),
        "expected worker-b to freeze at Draft",
    );
    println!(
        "  worker-b drafts and freezes. Latest checkpoint: {}.",
        latest(&store, THREAD).await
    );
    gate.release();
    let a_result = a_run.await.expect("worker-a's run");
    check(
        matches!(a_result, GraphExecutionResult::Interrupted { .. }),
        "expected worker-a to freeze too",
    );
    println!(
        "  worker-a wakes up and freezes too. Latest checkpoint: {}.",
        latest(&store, THREAD).await
    );
    check(
        log_length(&store, THREAD).await == 2
            && drafted_by(&store, THREAD).await.as_deref() == Some("worker-a"),
        "expected worker-a's stale draft to be the thread's latest checkpoint",
    );
    println!(
        "  Two checkpoints, and the latest is worker-a's: an approval now sends the stale draft."
    );
}

// --- 3. With leases, the stale worker's save is fenced. ----------------------

async fn fenced_save() {
    println!();
    println!(
        "=== Scenario 3: leases -- worker-a's lease is taken over, and its save is refused ==="
    );
    const THREAD: &str = "invoice-3";
    let clock = StoreClock::new();
    let store = Arc::new(InMemoryCheckpointStore::new().with_clock(clock.clock()));
    let gate = Gate::new();
    let a = Worker::new("worker-a", Some(gate.stall()));
    let b = Worker::new("worker-b", None);

    let (a_observer, a_events) = RunEventChannel::new();
    let a_run = a.spawn(
        THREAD,
        &store,
        Some(leased("worker-a").with_observer(a_observer)),
    );
    gate.started().await;
    check(
        lease_of(&store, THREAD).await.is_some_and(|l| l.token == 1),
        "expected worker-a to hold token 1",
    );
    println!("  worker-a takes the lease (token 1, until 12:00:30) and stalls in Draft.");

    // Time passes for the store alone. Tokio's clock doesn't move, so
    // worker-a's renewal timer (due at +10 s) never fires: it is stalled.
    clock.advance_store_alone(TTL + Duration::from_secs(1));
    let tokio_now = tokio::time::Instant::now();
    let (b_observer, b_events) = RunEventChannel::new();
    let b_result = b
        .run(
            THREAD,
            &store,
            Some(leased("worker-b").with_observer(b_observer)),
        )
        .await;
    check(
        matches!(b_result, GraphExecutionResult::Interrupted { .. }),
        "expected worker-b to freeze at Draft",
    );
    check(
        tokio::time::Instant::now() == tokio_now,
        "expected no time to pass on tokio's clock while worker-b ran, so worker-a never renewed",
    );
    println!(
        "  At 12:00:31 the lease has expired. worker-b takes the thread and freezes it. Latest checkpoint: {}.",
        latest(&store, THREAD).await
    );
    let b_seen = events_of(b_events).await;
    println!("  worker-b's events: {}.", describe(&b_seen));
    check(
        acquired(&b_seen) == [("worker-b".to_string(), 2)],
        "expected worker-b to take the thread's second lease",
    );
    check(
        lease_of(&store, THREAD).await.is_none(),
        "expected worker-b to have released the thread",
    );

    gate.release();
    let a_result = a_run.await.expect("worker-a's run");
    let lost = lost_at(&a_result);
    println!(
        "  worker-a wakes up and finishes Draft -> {} at '{}'.",
        outcome(&a_result),
        lost.as_deref().unwrap_or("-")
    );
    check(
        lost.as_deref() == Some("Draft"),
        "expected worker-a's run to end LeaseLost at Draft",
    );
    let a_seen = events_of(a_events).await;
    println!("  worker-a's events: {}.", describe(&a_seen));
    check_lost(&a_seen);
    check(
        log_length(&store, THREAD).await == 1
            && drafted_by(&store, THREAD).await.as_deref() == Some("worker-b"),
        "expected worker-b's checkpoint to stay the only one",
    );
    println!(
        "  The log still holds one checkpoint, worker-b's: {}.",
        latest(&store, THREAD).await
    );
    println!(
        "  worker-b had already released its lease, so nobody held the thread: the save was refused"
    );
    println!(
        "  because token 1 is no longer the thread's latest. Expiry only lets someone else in; the token decides."
    );
}

// --- 4. A refused renewal cancels the run, and Draft is dropped. -------------

async fn lost_on_renewal() {
    println!();
    println!("=== Scenario 4: leases -- worker-a finds out at its next renewal ===");
    const THREAD: &str = "invoice-4";
    let clock = StoreClock::new();
    let store = Arc::new(InMemoryCheckpointStore::new().with_clock(clock.clock()));

    // Draft waits for nothing but its token. Rust cancels a future by
    // dropping it: the guard records that, and the token is kept to look at.
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let token: Arc<Mutex<Option<CancellationToken>>> = Arc::default();
    let stall: Stall = {
        let (started, dropped, token) = (started.clone(), dropped.clone(), token.clone());
        Arc::new(move |context: NodeContext| {
            let (started, dropped) = (started.clone(), dropped.clone());
            *token.lock().unwrap() = Some(context.cancellation().clone());
            Box::pin(async move {
                let _guard = DropGuard(dropped);
                started.notify_one();
                std::future::pending::<()>().await;
            })
        })
    };
    let a = Worker::new("worker-a", Some(stall));
    let b = Worker::new("worker-b", None);

    let (a_observer, a_events) = RunEventChannel::new();
    let a_run = a.spawn(
        THREAD,
        &store,
        Some(leased("worker-a").with_observer(a_observer)),
    );
    started.notified().await;
    clock.advance_store_alone(TTL + Duration::from_secs(1));
    let b_result = b.run(THREAD, &store, Some(leased("worker-b"))).await;
    check(
        matches!(b_result, GraphExecutionResult::Interrupted { .. }),
        "expected worker-b to freeze at Draft",
    );
    println!(
        "  worker-a's lease expired while it waited in Draft; worker-b took the thread. Latest checkpoint: {}.",
        latest(&store, THREAD).await
    );

    // Tokio's clock reaches worker-a's next renewal (a third of the ttl),
    // which the store refuses.
    tokio::time::sleep(TTL / 3).await;
    let a_result = a_run.await.expect("worker-a's run");
    let lost = lost_at(&a_result);
    println!(
        "  worker-a's renewal at +10 s is refused -> {} at '{}', Draft unfinished.",
        outcome(&a_result),
        lost.as_deref().unwrap_or("-")
    );
    check(
        lost.as_deref() == Some("Draft"),
        "expected worker-a's run to end LeaseLost at Draft",
    );
    check(
        a.drafted() == 0,
        "expected Draft never to have returned a draft",
    );

    let cancelled = token
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled);
    check(
        cancelled && dropped.load(Ordering::SeqCst),
        "expected Draft's token cancelled and its future dropped",
    );
    println!(
        "  Draft's future was dropped and its CancellationToken cancelled, so worker-a stopped working on a thread that isn't its."
    );
    let a_seen = events_of(a_events).await;
    println!("  worker-a's events: {}.", describe(&a_seen));
    check_lost(&a_seen);
    check(
        log_length(&store, THREAD).await == 1
            && drafted_by(&store, THREAD).await.as_deref() == Some("worker-b"),
        "expected worker-a to write nothing",
    );
    println!(
        "  Still one checkpoint, worker-b's: {}.",
        latest(&store, THREAD).await
    );
}

// --- Workers -----------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invoice {
    invoice_id: String,
    drafted_by: Option<String>,
    sent: bool,
}

impl Invoice {
    fn new(id: &str) -> Self {
        Self {
            invoice_id: id.to_string(),
            drafted_by: None,
            sent: false,
        }
    }
}

/// Runs inside Draft: how a scenario makes a worker slow.
type Stall = Arc<dyn Fn(NodeContext) -> BoxFuture<'static, ()> + Send + Sync>;

/// One worker process: its own compiled graph, which stamps the draft with
/// the worker's name.
struct Worker {
    graph: CompiledGraph<Invoice>,
    /// Draft started.
    drafts: Arc<AtomicU32>,
    /// Draft returned a draft.
    drafted: Arc<AtomicU32>,
}

impl Worker {
    fn new(name: &str, stall: Option<Stall>) -> Arc<Self> {
        let drafts = Arc::new(AtomicU32::new(0));
        let drafted = Arc::new(AtomicU32::new(0));
        let name = name.to_string();
        let graph = StateGraph::<Invoice>::new()
            .add_async_node("Draft", {
                let (drafts, drafted) = (drafts.clone(), drafted.clone());
                move |invoice: Invoice, context: NodeContext| {
                    drafts.fetch_add(1, Ordering::SeqCst);
                    let stall = stall.as_ref().map(|stall| stall(context));
                    let (drafted, name) = (drafted.clone(), name.clone());
                    async move {
                        if let Some(stall) = stall {
                            stall.await;
                        }
                        drafted.fetch_add(1, Ordering::SeqCst);
                        Invoice {
                            drafted_by: Some(name),
                            ..invoice
                        }
                    }
                }
            })
            .add_interrupt_boundary("Draft")
            .add_async_node("Send", |invoice: Invoice, _: NodeContext| async move {
                Invoice {
                    sent: true,
                    ..invoice
                }
            })
            .add_edge("Draft", "Send")
            .compile()
            .expect("the graph is valid");
        Arc::new(Self {
            graph,
            drafts,
            drafted,
        })
    }

    fn drafts(&self) -> u32 {
        self.drafts.load(Ordering::SeqCst)
    }

    fn drafted(&self) -> u32 {
        self.drafted.load(Ordering::SeqCst)
    }

    async fn run(
        &self,
        thread: &str,
        store: &Arc<InMemoryCheckpointStore>,
        config: Option<RunConfig>,
    ) -> GraphExecutionResult<Invoice> {
        let store: Arc<dyn CheckpointStore> = store.clone();
        self.graph
            .run_with(
                thread,
                Invoice::new(thread),
                store,
                config.unwrap_or_default(),
            )
            .await
    }

    fn spawn(
        self: &Arc<Self>,
        thread: &'static str,
        store: &Arc<InMemoryCheckpointStore>,
        config: Option<RunConfig>,
    ) -> JoinHandle<GraphExecutionResult<Invoice>> {
        let (worker, store) = (self.clone(), store.clone());
        tokio::spawn(async move { worker.run(thread, &store, config).await })
    }
}

/// A stall that ignores its token, like a paused process: it ends when the
/// demo says so.
#[derive(Clone)]
struct Gate {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Gate {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }

    fn stall(&self) -> Stall {
        let gate = self.clone();
        Arc::new(move |_| {
            let gate = gate.clone();
            Box::pin(async move {
                gate.started.notify_one();
                gate.release.notified().await;
            })
        })
    }

    async fn started(&self) {
        self.started.notified().await;
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

struct DropGuard(Arc<AtomicBool>);

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// The store's clock: noon when it was created, moving with tokio's clock
/// plus whatever [`Self::advance_store_alone`] added.
#[derive(Clone)]
struct StoreClock {
    started: tokio::time::Instant,
    ahead: Arc<Mutex<Duration>>,
}

impl StoreClock {
    /// Inside the runtime whose clock it follows.
    fn new() -> Self {
        Self {
            started: tokio::time::Instant::now(),
            ahead: Arc::default(),
        }
    }

    fn clock(&self) -> Clock {
        let clock = self.clone();
        Arc::new(move || {
            let elapsed = clock.started.elapsed() + *clock.ahead.lock().unwrap();
            noon() + chrono::Duration::from_std(elapsed).unwrap()
        })
    }

    /// Lets `by` pass in the store alone: the world goes on while a worker
    /// is stalled.
    fn advance_store_alone(&self, by: Duration) {
        *self.ahead.lock().unwrap() += by;
    }
}

// --- Helpers -----------------------------------------------------------------

fn leased(owner: &str) -> RunConfig {
    RunConfig::new().with_lease(LeaseOptions::new(owner, TTL))
}

async fn lease_of(store: &InMemoryCheckpointStore, thread: &str) -> Option<ThreadLease> {
    store.get_lease(thread).await.expect("read the lease")
}

async fn log_length(store: &InMemoryCheckpointStore, thread: &str) -> usize {
    store
        .get_log(thread, None, 100)
        .await
        .expect("read the log")
        .entries
        .len()
}

async fn drafted_by(store: &InMemoryCheckpointStore, thread: &str) -> Option<String> {
    let latest = store.load(thread).await.expect("load")?;
    latest.frozen_state["drafted_by"]
        .as_str()
        .map(str::to_string)
}

async fn latest(store: &InMemoryCheckpointStore, thread: &str) -> String {
    let status = match store.load(thread).await.expect("load") {
        Some(c) => format!(
            "{} at {}",
            status(c.status),
            c.suspended_node.as_deref().unwrap_or("-")
        ),
        None => "none".to_string(),
    };
    format!(
        "[{status}, drafted by {}]",
        drafted_by(store, thread).await.as_deref().unwrap_or("-")
    )
}

fn status(status: CheckpointStatus) -> &'static str {
    match status {
        CheckpointStatus::Interrupted => "interrupted",
        CheckpointStatus::Completed => "completed",
        CheckpointStatus::Failed => "failed",
        CheckpointStatus::Cancelled => "cancelled",
        CheckpointStatus::Ended => "ended",
        CheckpointStatus::Running => "running",
    }
}

fn refusal_of<S>(result: &GraphExecutionResult<S>) -> Option<String> {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            Some(message.clone())
        }
        _ => None,
    }
}

/// The node a run that lost its lease stopped at; `None` for anything else.
fn lost_at<S>(result: &GraphExecutionResult<S>) -> Option<String> {
    match result {
        GraphExecutionResult::Cancelled {
            reason: CancelReason::LeaseLost,
            node,
            ..
        } => Some(node.clone()),
        _ => None,
    }
}

fn outcome<S>(result: &GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Interrupted { .. } => "Interrupted".to_string(),
        GraphExecutionResult::Failed(error) => format!("Failed({error:?})"),
        GraphExecutionResult::Cancelled { reason, .. } => format!("Cancelled ({reason:?})"),
    }
}

/// The channel ends after `RunCompleted`, so this returns once the run's last
/// event is in.
async fn events_of(mut events: UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = events.recv().await {
        all.push(event);
    }
    all
}

fn acquired(events: &[RunEvent]) -> Vec<(String, i64)> {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::LeaseAcquired { owner, token, .. } => Some((owner.clone(), *token)),
            _ => None,
        })
        .collect()
}

fn describe(events: &[RunEvent]) -> String {
    events
        .iter()
        .map(|e| match &e.kind {
            RunEventKind::RunStarted { .. } => "RunStarted".to_string(),
            RunEventKind::LeaseAcquired {
                owner,
                token,
                expires_at,
            } => format!(
                "LeaseAcquired('{owner}', token {token}, until {})",
                text(Some(*expires_at))
            ),
            RunEventKind::LeaseLost { owner, token } => {
                format!("LeaseLost('{owner}', token {token})")
            }
            RunEventKind::NodeStarted => "NodeStarted".to_string(),
            RunEventKind::NodeCompleted { result, .. } => format!("NodeCompleted({result:?})"),
            RunEventKind::CheckpointSaved { .. } => "CheckpointSaved".to_string(),
            RunEventKind::Interrupted { .. } => "Interrupted".to_string(),
            RunEventKind::RunCompleted {
                outcome,
                cancel_reason: Some(reason),
                ..
            } => format!("RunCompleted({outcome:?}, {reason:?})"),
            RunEventKind::RunCompleted { outcome, .. } => format!("RunCompleted({outcome:?})"),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A lost lease: `LeaseLost` for worker-a's token, a run that ends
/// `Cancelled(LeaseLost)`, and no save at all.
fn check_lost(events: &[RunEvent]) {
    let lost: Vec<(String, i64)> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::LeaseLost { owner, token } => Some((owner.clone(), *token)),
            _ => None,
        })
        .collect();
    check(
        lost == [("worker-a".to_string(), 1)],
        "expected LeaseLost for worker-a's token 1",
    );
    check(
        events.last().is_some_and(|e| {
            matches!(
                e.kind,
                RunEventKind::RunCompleted {
                    outcome: RunOutcome::Cancelled,
                    cancel_reason: Some(CancelReason::LeaseLost),
                    ..
                }
            )
        }),
        "expected the run to complete Cancelled(LeaseLost)",
    );
    check(
        !events
            .iter()
            .any(|e| matches!(e.kind, RunEventKind::CheckpointSaved { .. })),
        "expected no CheckpointSaved from the stale worker",
    );
}

fn text(time: Option<DateTime<Utc>>) -> String {
    time.map_or("-".to_string(), |t| {
        t.to_rfc3339_opts(SecondsFormat::Secs, true)
    })
}

fn check(ok: bool, what: &str) {
    if !ok {
        eprintln!("check failed: {what}");
        std::process::exit(1);
    }
}
