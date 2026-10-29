//! Event waits, durable timers and ending a thread
//! (`design/29-Event-Waits-And-Timers.md`, `design/28-Ending-A-Thread.md`,
//! `design/42-Waking-Through-A-Resume.md`).
//!
//! An online shop's order process, which spends almost all of its life waiting:
//!
//! ```text
//! Place -> AwaitPayment (an event wait: "payment.received", at most 24 h, no timeout route)
//!       -> FraudReview, for orders of $1000 or more (a human boundary: at most 2 h, then Escalate)
//!       -> Pack -> AwaitPickup (a 4 h timer) -> Ship
//! ```
//!
//! While an order waits, no process holds it. Its thread is a few rows in a
//! SQLite file, and every step below opens a fresh `SqliteCheckpointStore` on
//! that file, the way a webhook handler, a cron job and a support tool would,
//! each in its own process. Nothing waits in memory between steps.
//!
//! The clock: a freeze stamps its `wake_at` from `Utc::now()`, so every wait
//! below is due a fixed delay after the moment the demo starts, `T`. Waking
//! is the other way round: `wake_due(store, now)`, `LevelSignals::due_at(now)`
//! and `ThreadQuery::due_by(now)` all take the caller's `now`, so the demo
//! passes `T + 3 h`, `T + 4 h 30`, `T + 25 h` and a day of waiting takes no
//! time. Times are printed relative to `T`; the absolute ones are this run's.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SubsecRound, Utc};
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CancelReason, CheckpointQuery, CheckpointStore, END_REASON_KEY, END_REQUESTED, END_TIMED_OUT,
    ENDED_BY_KEY, EventSignal, GraphExecutionResult, HumanSignal, LevelSignals, ThreadEnd,
    ThreadQuery, WakeReason,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::schema::{CheckpointStatus, CheckpointWait, WaitKind};
use datalium_strata::streaming::StreamChunkType;
use datalium_strata::wait::{BoundaryOptions, WaitOptions, WokenThread};
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use serde::{Deserialize, Serialize};

const HOUR: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    total: u32,
    paid: bool,
    payment_ref: Option<String>,
    review: Option<String>,
    shipped: bool,
    log: Vec<String>,
}

impl Order {
    fn new(order_id: &str, total: u32) -> Self {
        Self {
            order_id: order_id.to_string(),
            total,
            paid: false,
            payment_ref: None,
            review: None,
            shipped: false,
            log: Vec::new(),
        }
    }

    fn note(mut self, line: impl Into<String>) -> Self {
        self.log.push(line.into());
        self
    }
}

/// The demo's clock: `T`, the moment it starts (whole seconds), and the file.
struct Shop {
    t0: DateTime<Utc>,
    db: PathBuf,
}

#[tokio::main]
async fn main() {
    // The database lives next to this demo's Cargo.toml (`*.db` is
    // gitignored), deleted first so every run starts from an empty file.
    let db = Path::new(env!("CARGO_MANIFEST_DIR")).join("orders.db");
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let shop = Shop {
        t0: Utc::now().trunc_subsecs(0),
        db,
    };
    let orders = Arc::new(build_order_graph());

    // --- Step 1: five orders are placed, and each waits for its payment. ---

    step(1, "Step 1: T, five orders are placed and wait for payment");
    let placed = [
        ("order-1001", 80),
        ("order-1002", 45),
        ("order-1003", 2400),
        ("order-1004", 60),
        ("order-1005", 35),
    ];
    {
        let store = shop.open();
        for (id, total) in placed {
            let result = orders.run(id, Order::new(id, total), store.clone()).await;
            let GraphExecutionResult::Interrupted { wait, .. } = result else {
                fail(&format!(
                    "{id}: expected Interrupted, got {}",
                    outcome(&result)
                ));
            };
            println!("  {id} (${total}): {}", shop.describe(wait.as_ref()));
            check(
                shop.is_wait(
                    wait.as_ref(),
                    WaitKind::Event,
                    Some("payment.received"),
                    24 * HOUR,
                ),
                &format!("{id} should wait for payment.received for 24 h"),
            );
        }

        // The edge out of a wait is chosen when it freezes, on the state as it
        // was then, so the high-value order is already headed for FraudReview
        // before anyone has paid.
        let high_value = store.load("order-1003").await.unwrap().unwrap();
        let next = high_value.next_node.unwrap_or_default();
        println!("  order-1003's next node, fixed at the freeze: {next}");
        check(
            next == "FraudReview",
            "order-1003 should be routed to FraudReview",
        );
    }

    println!("  The store is dropped. What is left of the five orders:");
    shop.print_heads();
    println!(
        "  order-1001's wait column: {}",
        shop.scalar("SELECT wait FROM checkpoints WHERE thread_id = 'order-1001'")
    );
    check(shop.rows() == 5, "expected one checkpoint row per order");

    // --- Step 2: the wrong answer to a wait is refused, and writes nothing. ---

    step(2, "Step 2: the wrong answers to a payment wait are refused");
    {
        let store = shop.open();
        let before = shop.rows();

        let approved = refused(
            orders
                .resume("order-1001", HumanSignal::approve("clerk"), store.clone())
                .await,
        );
        println!("  A clerk approves it:      {approved}");
        check(
            approved.contains("only a signal of that event answers it"),
            "a human resume of an event wait should be refused",
        );

        let refund = refused(
            orders
                .signal(
                    "order-1001",
                    EventSignal::new("refund.issued"),
                    store.clone(),
                )
                .await,
        );
        println!("  A refund.issued event:    {refund}");
        check(
            refund.contains("is not waiting for event 'refund.issued'"),
            "a signal of another event type should be refused",
        );

        // Through `resume`, decided at the caller's `now`: T + 1 h.
        let early = refused(
            orders
                .resume(
                    "order-1001",
                    LevelSignals::due_at(shop.at(HOUR)),
                    store.clone(),
                )
                .await,
        );
        println!("  A wake at T+1h00:         {early}");
        check(
            early.contains("is not due until"),
            "a wake before the timeout should be refused",
        );

        let after = shop.rows();
        println!("  Checkpoint rows before and after: {before} and {after}.");
        check(after == before, "a refused answer should write nothing");
    }

    // --- Step 3: the payment provider's webhook signals three orders. ---

    step(
        3,
        "Step 3: T, payments arrive for order-1001, order-1003 and order-1005",
    );
    {
        let store = shop.open();
        for (id, reference) in [
            ("order-1001", "PAY-5531"),
            ("order-1003", "PAY-5532"),
            ("order-1005", "PAY-5533"),
        ] {
            // The signal's overrides replace top-level fields of the waiting
            // state: the payload is in the state from here on, in every later
            // node and checkpoint.
            let signal = EventSignal::new("payment.received")
                .with_override("paid", true)
                .with_override("payment_ref", reference);
            // `signal` is the usual door; `resume` with `LevelSignals::event`
            // answers the same wait the same way (design/42), as an
            // application that resumes every thread through one call would.
            let result = if id == "order-1005" {
                orders
                    .resume(id, LevelSignals::event(signal), store.clone())
                    .await
            } else {
                orders.signal(id, signal, store.clone()).await
            };
            let GraphExecutionResult::Interrupted {
                wait, frozen_state, ..
            } = result
            else {
                fail(&format!(
                    "{id}: expected Interrupted, got {}",
                    outcome(&result)
                ));
            };
            let door = if id == "order-1005" {
                "resume(LevelSignals::event)"
            } else {
                "signal"
            };
            println!(
                "  {id}: paid ({}, by {door}), now {}",
                frozen_state.payment_ref.as_deref().unwrap_or("-"),
                shop.describe(wait.as_ref())
            );
            check(
                frozen_state.paid && frozen_state.payment_ref.as_deref() == Some(reference),
                &format!("{id} should carry its payment"),
            );
        }

        let pickup = store.load("order-1001").await.unwrap().unwrap().wait;
        check(
            shop.is_wait(pickup.as_ref(), WaitKind::Timer, None, 4 * HOUR),
            "order-1001 should wait for the carrier for 4 h",
        );
        let review = store.load("order-1003").await.unwrap().unwrap().wait;
        check(
            shop.is_wait(review.as_ref(), WaitKind::Human, None, 2 * HOUR),
            "order-1003 should wait for a reviewer for 2 h",
        );
    }

    // --- Step 4: a customer cancels an unpaid order. ---

    step(
        4,
        "Step 4: T, the customer cancels order-1004 before paying",
    );
    {
        let store = shop.open();
        let result = orders
            .end_with(
                "order-1004",
                ThreadEnd::by("customer"),
                store.clone(),
                RunConfig::default(),
            )
            .await;
        let GraphExecutionResult::Cancelled { node, reason, .. } = result else {
            fail(&format!(
                "order-1004: expected Cancelled, got {}",
                outcome(&result)
            ));
        };
        let checkpoint = store.load("order-1004").await.unwrap().unwrap();
        let metadata = checkpoint.metadata.clone().unwrap_or_default();
        let get = |key: &str| metadata.get(key).cloned().unwrap_or_default();
        println!(
            "  Ended at {node} ({reason:?}): status {:?}, {END_REASON_KEY}={}, {ENDED_BY_KEY}={}",
            checkpoint.status,
            get(END_REASON_KEY),
            get(ENDED_BY_KEY)
        );
        check(
            reason == CancelReason::Ended && node == "AwaitPayment",
            "order-1004 should end where it waited",
        );
        check(
            checkpoint.status == CheckpointStatus::Ended,
            "order-1004's checkpoint should be ended",
        );
        check(
            get(END_REASON_KEY) == END_REQUESTED && get(ENDED_BY_KEY) == "customer",
            "a caller's end should record 'requested' and who asked",
        );
        check(
            checkpoint.wait.is_none(),
            "an ended thread waits for nothing",
        );

        let late = refused(
            orders
                .signal(
                    "order-1004",
                    EventSignal::new("payment.received"),
                    store.clone(),
                )
                .await,
        );
        println!("  A late payment for it:    {late}");
        check(
            late.contains("was ended"),
            "a signal to an ended thread should be refused",
        );
    }

    // --- Step 5: T + 3 h, the wake driver runs. Only the fraud review is due. ---

    let now = shop.at(3 * HOUR);
    step(5, "Step 5: T+3h00, the wake driver runs");
    {
        let store = shop.open();
        let due = due_now(&store, now).await;
        println!("  Due now (ThreadQuery::due_by): {}", due.join(", "));
        check(
            due == ["order-1003"],
            "only the unanswered fraud review should be due",
        );

        let report = orders.wake_due(store.clone(), now).await.unwrap();
        for woken in &report.woken {
            println!(
                "  Woke {} ({:?}): {}",
                woken.thread_id,
                woken.reason,
                outcome(&woken.result)
            );
        }
        let escalated = single(&report.woken);
        check(
            escalated.thread_id == "order-1003" && escalated.reason == WakeReason::Timeout,
            "order-1003's review should time out",
        );
        let GraphExecutionResult::Completed(state) = &escalated.result else {
            fail("order-1003 should complete at its timeout route");
        };
        check(
            state.review.as_deref() == Some("escalated") && !state.shipped,
            "a timed-out review should take its route to Escalate",
        );
        let latest = store.load("order-1003").await.unwrap().unwrap();
        check(
            latest.decision.is_none(),
            "a timeout is not a human's answer: no decision",
        );
    }

    // --- Step 6: T + 4 h 30, the pickup timers are due. The application wakes one itself, streaming. ---

    let now = shop.at(4 * HOUR + HOUR / 2);
    step(
        6,
        "Step 6: T+4h30, the carrier is here: both pickup timers are due",
    );
    {
        let store = shop.open();
        let due = due_now(&store, now).await;
        println!("  Due now: {}", due.join(", "));
        check(
            due == ["order-1001", "order-1005"],
            "both pickup timers should be due",
        );

        // `wake_due` runs each thread as a plain resume and doesn't stream. An
        // application that wants a woken run's chunks (or its own config per
        // thread) resumes it with `LevelSignals::due_at(now)`, which decides
        // at its own load that the thread is still due.
        println!(
            "  The application wakes order-1005 through its own resume_streaming(LevelSignals::due_at(now)):"
        );
        let (handle, mut chunks) =
            orders
                .clone()
                .resume_streaming("order-1005", LevelSignals::due_at(now), store.clone());
        let mut seen = Vec::new();
        while let Some(chunk) = chunks.recv().await {
            let kind = format!("{:?}", chunk.chunk_type);
            println!("    chunk {kind:<9} {}", chunk.node_name);
            if chunk.chunk_type != StreamChunkType::Token {
                seen.push(format!("{kind}:{}", chunk.node_name));
            }
        }
        let result = handle.await.expect("the woken run");
        println!("    -> {}", outcome(&result));
        check(
            seen == ["NodeStart:Ship", "NodeEnd:Ship"],
            "the woken run should stream from the node after the timer",
        );
        check(
            matches!(&result, GraphExecutionResult::Completed(order) if order.shipped),
            "order-1005 should ship",
        );

        // The driver then finds only the other one: order-1005 is no longer due.
        let report = orders.wake_due(store.clone(), now).await.unwrap();
        for woken in &report.woken {
            println!(
                "  The wake driver woke {} ({:?}): {}",
                woken.thread_id,
                woken.reason,
                outcome(&woken.result)
            );
        }
        let woken = single(&report.woken);
        check(
            woken.thread_id == "order-1001" && woken.reason == WakeReason::Timer,
            "the driver should wake order-1001 only",
        );
    }

    // --- Step 7: the next morning, order-1002's payment window has closed. ---

    let now = shop.at(25 * HOUR);
    step(7, "Step 7: T+25h00, order-1002 was never paid");
    {
        let store = shop.open();
        let report = orders.wake_due(store.clone(), now).await.unwrap();
        let expired = single(&report.woken);
        println!(
            "  Woke {} ({:?}): {}",
            expired.thread_id,
            expired.reason,
            outcome(&expired.result)
        );
        check(
            expired.thread_id == "order-1002" && expired.reason == WakeReason::Timeout,
            "order-1002's payment wait should time out",
        );
        check(
            matches!(
                &expired.result,
                GraphExecutionResult::Cancelled { reason: CancelReason::Ended, node, .. }
                    if node == "AwaitPayment"
            ),
            "a timeout with no route should end the thread",
        );

        // The engine's end records why, where the query port can filter on it.
        let timed_out = store
            .list_threads(&ThreadQuery::default().with_metadata(END_REASON_KEY, END_TIMED_OUT))
            .await
            .unwrap();
        let ids: Vec<String> = timed_out
            .threads
            .iter()
            .map(|thread| thread.thread_id.clone())
            .collect();
        println!(
            "  Threads with {END_REASON_KEY}={END_TIMED_OUT}: {}",
            ids.join(", ")
        );
        check(
            ids == ["order-1002"],
            "only order-1002 should have timed out",
        );
        check(
            due_now(&store, now).await.is_empty(),
            "nothing should be due any more",
        );
    }

    step(8, "Done: what the file holds now");
    shop.print_heads();
    let rows = shop.rows();
    println!(
        "  {rows} checkpoint rows in all. A completed run writes nothing, so a finished order's head is its last resume stamp."
    );
    println!(
        "  The stamp keeps the wake_at of the wait it answered; only an interrupted, unresumed thread is due."
    );
    check(rows == 16, "expected 16 checkpoint rows");
    let heads: Vec<(String, String, Option<String>)> = shop
        .heads()
        .into_iter()
        .map(|(thread, state, wake_at)| (thread, state, wake_at.map(|at| shop.label(&at))))
        .collect();
    let expected = [
        ("order-1001", "resumed", Some("T+4h00")),
        ("order-1002", "ended", None),
        ("order-1003", "resumed", Some("T+2h00")),
        ("order-1004", "ended", None),
        ("order-1005", "resumed", Some("T+4h00")),
    ];
    check(
        heads.len() == expected.len()
            && heads.iter().zip(expected).all(|(head, want)| {
                head.0 == want.0 && head.1 == want.1 && head.2.as_deref() == want.2
            }),
        "unexpected head table",
    );
}

// --- The graph ---

/// `AwaitPayment` has no timeout route: an order nobody pays for is ended,
/// `timed_out`, where it waits.
fn build_order_graph() -> CompiledGraph<Order> {
    StateGraph::<Order>::new()
        .add_node("Place", |o: Order| {
            let line = format!("placed for ${}", o.total);
            o.note(line)
        })
        .add_wait_for_event_with(
            "AwaitPayment",
            "payment.received",
            WaitOptions::new().with_timeout(24 * HOUR),
        )
        .add_node("FraudReview", |o: Order| o.note("held for a fraud review"))
        .add_interrupt_boundary_with(
            "FraudReview",
            BoundaryOptions::new()
                .with_reject("Refund")
                .with_timeout(2 * HOUR)
                .with_on_timeout("Escalate"),
        )
        .add_node("Pack", |o: Order| {
            let line = format!(
                "packed, payment {}",
                o.payment_ref.as_deref().unwrap_or("-")
            );
            o.note(line)
        })
        .add_timer("AwaitPickup", 4 * HOUR)
        .add_node("Ship", |o: Order| {
            Order { shipped: true, ..o }.note("handed to the carrier")
        })
        .add_node("Escalate", |o: Order| {
            Order {
                review: Some("escalated".to_string()),
                ..o
            }
            .note("not reviewed within 2 h: escalated to the risk team")
        })
        .add_node("Refund", |o: Order| {
            Order {
                review: Some("rejected".to_string()),
                ..o
            }
            .note("refunded")
        })
        .add_edge("Place", "AwaitPayment")
        .add_conditional_edge("AwaitPayment", "FraudReview", |o: &Order| o.total >= 1000)
        .add_edge("AwaitPayment", "Pack")
        .add_edge("FraudReview", "Pack")
        .add_edge("Pack", "AwaitPickup")
        .add_edge("AwaitPickup", "Ship")
        .compile()
        .expect("the order graph is valid")
}

// --- Helpers ---

impl Shop {
    /// A fresh store on the file, as each step's own process would open it.
    fn open(&self) -> Arc<SqliteCheckpointStore> {
        Arc::new(SqliteCheckpointStore::open(&self.db).expect("open the SQLite store"))
    }

    /// `T + offset`.
    fn at(&self, offset: Duration) -> DateTime<Utc> {
        self.t0 + chrono::Duration::from_std(offset).unwrap()
    }

    /// How long after `T` a stored time is, to the minute below: `T+24h00`.
    /// The freezes happen within seconds of `T`, so this is the wait's delay.
    fn label(&self, text: &str) -> String {
        let minutes = (parse(text) - self.t0).num_minutes();
        format!("T+{}h{:02}", minutes / 60, minutes % 60)
    }

    /// Whether `wait` is of `kind` (and `event`), due `delay` after the
    /// freeze: at least `delay` after `T`, and less than a minute more.
    fn is_wait(
        &self,
        wait: Option<&CheckpointWait>,
        kind: WaitKind,
        event: Option<&str>,
        delay: Duration,
    ) -> bool {
        let Some(wait) = wait else { return false };
        let Some(wake_at) = wait.wake_at.as_deref().map(parse) else {
            return false;
        };
        let after = wake_at - self.at(delay);
        wait.kind == kind
            && wait.event_type.as_deref() == event
            && after >= chrono::Duration::zero()
            && after < chrono::Duration::minutes(1)
    }

    fn describe(&self, wait: Option<&CheckpointWait>) -> String {
        let Some(wait) = wait else {
            return "waits for nothing".to_string();
        };
        let until = wait.wake_at.as_deref().map_or_else(
            || "with no timeout".to_string(),
            |at| format!("until {} ({at})", self.label(at)),
        );
        match wait.kind {
            WaitKind::Event => format!(
                "waits for event '{}' {until}",
                wait.event_type.as_deref().unwrap_or_default()
            ),
            WaitKind::Timer => format!("waits for a timer {until}"),
            _ => format!("waits for a human {until}"),
        }
    }

    // Reads the file directly, with rusqlite and no Strata code, to show a
    // waiting order is only rows.
    fn raw(&self) -> rusqlite::Connection {
        rusqlite::Connection::open_with_flags(&self.db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open the file read-only")
    }

    fn rows(&self) -> i64 {
        self.raw()
            .query_row("SELECT COUNT(*) FROM checkpoints", [], |row| row.get(0))
            .unwrap()
    }

    fn scalar(&self, sql: &str) -> String {
        self.raw()
            .query_row(sql, [], |row| row.get::<_, Option<String>>(0))
            .unwrap()
            .unwrap_or_default()
    }

    fn heads(&self) -> Vec<(String, String, Option<String>)> {
        let connection = self.raw();
        let mut statement = connection
            .prepare("SELECT thread_id, state, wake_at FROM checkpoint_threads ORDER BY thread_id")
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn print_heads(&self) {
        println!("    checkpoint_threads (the head table the query port reads):");
        for (thread, state, wake_at) in self.heads() {
            let wake_at = wake_at.map_or_else(
                || "-".to_string(),
                |at| format!("{} ({at})", self.label(&at)),
            );
            println!("      {thread}  {state:<11}  wake_at {wake_at}");
        }
    }
}

fn parse(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("a stored RFC 3339 time")
        .with_timezone(&Utc)
}

async fn due_now(store: &Arc<SqliteCheckpointStore>, now: DateTime<Utc>) -> Vec<String> {
    let page = store
        .list_threads(&ThreadQuery::default().due_by(now))
        .await
        .unwrap();
    // The listing is newest first, by wall-clock time; sort for stable output.
    let mut ids: Vec<String> = page.threads.into_iter().map(|t| t.thread_id).collect();
    ids.sort();
    ids
}

fn outcome(result: &GraphExecutionResult<Order>) -> String {
    match result {
        GraphExecutionResult::Completed(order) => format!("completed, {}", order.log.join(" / ")),
        GraphExecutionResult::Cancelled { reason, node, .. } => {
            format!("cancelled ({reason:?}) at {node}")
        }
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            format!("interrupted at {suspended_node}")
        }
        GraphExecutionResult::Failed(error) => format!("failed: {error:?}"),
    }
}

/// The message of a refusal: the engine refuses with `ExecutionAborted`.
fn refused(result: GraphExecutionResult<Order>) -> String {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => message,
        other => fail(&format!("expected a refusal, got {}", outcome(&other))),
    }
}

fn single(woken: &[WokenThread<Order>]) -> &WokenThread<Order> {
    match woken {
        [one] => one,
        _ => fail(&format!("expected one woken thread, got {}", woken.len())),
    }
}

fn step(number: u32, title: &str) {
    if number > 1 {
        println!();
    }
    println!("=== {title} ===");
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
