//! Human-in-the-loop interruption (STRATA-SPEC-002) with typed human signals
//! (`design/21-Typed-Human-Signals.md` in `datalium/strata`).
//!
//! `PrepareWireTransfer` -> [interrupt boundary] -> `ExecuteWireTransfer`,
//! rejecting to `CancelWireTransfer`. A node drafts something high-risk (an
//! outbound wire transfer), the graph freezes for a human before anything
//! irreversible happens, and the human's answer decides where it goes next.
//!
//! The core proof is still "Flush": the checkpoints go to a real SQLite file
//! (`SqliteCheckpointStore::open(path)`, not `open_in_memory()`). Store
//! instance #1 runs two transfers to their boundary and is dropped. Store
//! instance #2, which shares nothing with #1 but the file, stands in for the
//! process after a restart: it finds the waiting transfers with the query port
//! (`CheckpointQuery::list_threads`) and answers them.
//!
//! What the answer carries, in 0.9:
//! - Typed overrides. `HumanSignal::approve(reviewer).with_override("amount", 4150.25)`
//!   writes a JSON number, so it fits an `f64` field. The string-only
//!   `with_field_override` (the 0.3-era `field_overrides` map) writes
//!   `"4150.25"`, which doesn't fit, and the engine refuses it before touching
//!   the checkpoint.
//! - A recorded decision. The resume records who answered as a
//!   `HumanDecision`, and the nodes after the boundary read it from
//!   `NodeContext::decision()`. The audit trail comes from the engine, not
//!   from a field the reviewer has to fill in.
//! - A reject route. `.add_interrupt_boundary_with_reject(name, on_reject)`
//!   sends a rejection to its own node, so the transfer node never sees one.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::{CompiledGraph, NodeError, StateGraph};
use datalium_strata::schema::HumanDecision;
use datalium_strata::store::{
    CheckpointQuery, CheckpointStore, ThreadQuery, ThreadState, ThreadSummary,
};
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use serde::{Deserialize, Serialize};

const ACME_THREAD: &str = "wire-acme-corp";
const GLOBEX_THREAD: &str = "wire-globex";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireTransferState {
    beneficiary: String,
    amount: f64,
    currency: String,
    invoice_generated: bool,
    status: String,
    approved_by: Option<String>,
    note: Option<String>,
}

impl WireTransferState {
    fn new(beneficiary: &str, amount: f64) -> Self {
        Self {
            beneficiary: beneficiary.to_string(),
            amount,
            currency: "USD".to_string(),
            invoice_generated: false,
            status: "new".to_string(),
            approved_by: None,
            note: None,
        }
    }
}

/// The irreversible side effect, kept in memory so the demo can count what was sent.
#[derive(Clone, Default)]
struct Bank(Arc<Mutex<Vec<String>>>);

impl Bank {
    fn send(&self, beneficiary: &str, amount: f64, currency: &str) -> String {
        let mut sent = self.0.lock().unwrap();
        sent.push(format!("{beneficiary} {amount:.2} {currency}"));
        format!("ref {:04}", sent.len())
    }

    fn sent(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

/// Every decision a node after the boundary saw, in the order they ran.
#[derive(Clone, Default)]
struct Decisions(Arc<Mutex<Vec<HumanDecision>>>);

impl Decisions {
    fn add(&self, decision: &HumanDecision) {
        self.0.lock().unwrap().push(decision.clone());
    }

    fn all(&self) -> Vec<HumanDecision> {
        self.0.lock().unwrap().clone()
    }
}

fn wire_transfer_graph(bank: &Bank, decisions: &Decisions) -> CompiledGraph<WireTransferState> {
    let (execute_bank, execute_seen) = (bank.clone(), decisions.clone());
    let cancel_seen = decisions.clone();
    StateGraph::<WireTransferState>::new()
        .add_node("PrepareWireTransfer", |s: WireTransferState| {
            WireTransferState {
                invoice_generated: true,
                status: "awaiting approval".to_string(),
                ..s
            }
        })
        .add_interrupt_boundary_with_reject("PrepareWireTransfer", "CancelWireTransfer")
        .add_async_fallible_node(
            "ExecuteWireTransfer",
            move |s: WireTransferState, ctx: NodeContext| {
                // The run resumed at the boundary, so a decision is in effect.
                // The reviewer comes from it, and `s.amount` already holds any
                // amount the reviewer corrected.
                let result = match ctx.decision() {
                    None => Err(NodeError::Fatal(
                        "a transfer must not run without a decision".to_string(),
                    )),
                    Some(decision) => {
                        execute_seen.add(decision);
                        let reference = execute_bank.send(&s.beneficiary, s.amount, &s.currency);
                        Ok(WireTransferState {
                            status: format!("sent ({reference})"),
                            approved_by: Some(decision.reviewer.clone()),
                            ..s
                        })
                    }
                };
                std::future::ready(result)
            },
        )
        .add_async_node(
            "CancelWireTransfer",
            move |s: WireTransferState, ctx: NodeContext| {
                // A rejection is recorded as a decision too, so the reject
                // route knows who said no.
                let decision = ctx.decision().expect("a rejection is a decision");
                cancel_seen.add(decision);
                std::future::ready(WireTransferState {
                    status: format!("cancelled by {}", decision.reviewer),
                    ..s
                })
            },
        )
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .expect("graph is structurally valid")
}

#[tokio::main]
async fn main() {
    // A file in this demo's own directory, `.gitignore`d. Deleted up front so
    // a run never resumes a checkpoint left over from the previous one, and
    // again at the end.
    let db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("hitl-checkpoint.sqlite");
    remove_database(&db_path);

    let bank = Bank::default();
    let decisions = Decisions::default();
    let graph = wire_transfer_graph(&bank, &decisions);

    // --- Step 1: run two transfers to the boundary, using store instance #1.

    println!("=== Step 1: two transfers run to the interrupt boundary (store instance #1) ===");
    {
        let store = open_file_backed_store(&db_path);
        for (thread_id, transfer) in [
            (ACME_THREAD, WireTransferState::new("Acme Corp", 4200.50)),
            (GLOBEX_THREAD, WireTransferState::new("Globex Ltd", 9800.00)),
        ] {
            match graph.run(thread_id, transfer, store.clone()).await {
                GraphExecutionResult::Interrupted {
                    thread_id,
                    suspended_node,
                    next_node,
                    frozen_state,
                    ..
                } => {
                    println!("  {thread_id}: suspended at {suspended_node}, next {next_node}");
                    println!("    frozen: {frozen_state:?}");
                }
                other => fail(&format!(
                    "expected '{thread_id}' to interrupt at the boundary, got: {}",
                    describe_result(&other)
                )),
            }
        }
        // `store` is dropped here: its connection to the file closes.
    }

    println!("  Store instance #1 dropped. The checkpoints now exist only in the SQLite file.");
    check(
        bank.sent().is_empty(),
        "nothing may be sent before a human answers",
    );

    // --- Step 2: "restart". A new store instance finds what is waiting.

    println!();
    println!(
        "=== Step 2: after the restart, store instance #2 lists the transfers waiting for a human ==="
    );
    println!("  SQLite file: {}", db_path.display());
    let restarted = open_file_backed_store(&db_path);
    let waiting = list_waiting(&restarted).await;
    for thread in &waiting {
        println!(
            "  {}: {:?}, waiting at {}",
            thread.thread_id,
            thread.state,
            thread.waiting_at.join("/")
        );
    }
    check(
        waiting
            .iter()
            .map(|t| t.thread_id.as_str())
            .eq([ACME_THREAD, GLOBEX_THREAD]),
        "expected both transfers to be waiting",
    );

    // --- Step 3: a string override that doesn't fit is refused, and the thread keeps waiting.

    println!();
    println!(
        "=== Step 3: a string override for an f64 field is refused before the checkpoint is touched ==="
    );
    let stringly = HumanSignal::approve("jdoe@acme.io").with_field_override("amount", "4150.25");
    match graph.resume(ACME_THREAD, stringly, restarted.clone()).await {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            println!("  Refused: {message}");
        }
        other => fail(&format!(
            "expected the string override to be refused, got: {}",
            describe_result(&other)
        )),
    }
    let checkpoint = restarted
        .load(ACME_THREAD)
        .await
        .expect("the store can be read")
        .expect("the thread still has its checkpoint");
    check(
        checkpoint.resumed_at.is_none(),
        "a refused resume must not stamp the checkpoint",
    );
    check(
        bank.sent().is_empty(),
        "a refused resume must not run anything",
    );
    println!("  Not stamped, nothing ran: '{ACME_THREAD}' can still be answered.");

    // --- Step 4: approve with a typed override. The transfer node reads the decision.

    println!();
    println!(
        "=== Step 4: jdoe approves wire-acme-corp, correcting the amount to the invoice total ==="
    );
    let approval = HumanSignal::approve("jdoe@acme.io").with_override("amount", 4150.25);
    let approved = completed(graph.resume(ACME_THREAD, approval, restarted.clone()).await);
    println!("  completed: {approved:?}");
    let seen = decisions.all();
    check(seen.len() == 1, "expected exactly one decision so far");
    let acme = &seen[0];
    println!("  ExecuteWireTransfer saw: {}", describe(acme));
    check(
        acme.is_approved
            && acme.boundary == "PrepareWireTransfer"
            && acme.reviewer == "jdoe@acme.io"
            && acme.overrides.get("amount").and_then(|v| v.as_f64()) == Some(4150.25)
            && !acme.decided_at.is_empty(),
        "expected the decision to record the approval and its typed override",
    );
    check(
        approved.amount == 4150.25,
        "expected the typed override to set the amount",
    );
    check(
        approved.approved_by.as_deref() == Some("jdoe@acme.io"),
        "expected the reviewer from the recorded decision",
    );
    check(
        bank.sent() == ["Acme Corp 4150.25 USD"],
        "expected exactly the corrected transfer to be sent",
    );
    println!("  approved_by came from ctx.decision().reviewer. The signal set only amount.");

    // --- Step 5: reject. The reject route runs; the transfer node never does.

    println!();
    println!("=== Step 5: mlee rejects wire-globex, so the run takes the reject route ===");
    let rejection = HumanSignal::reject("mlee@acme.io")
        .with_override("note", "beneficiary not on the vendor list");
    let rejected = completed(
        graph
            .resume(GLOBEX_THREAD, rejection, restarted.clone())
            .await,
    );
    println!("  completed: {rejected:?}");
    let seen = decisions.all();
    let last = seen.last().expect("a decision was recorded");
    println!("  CancelWireTransfer saw: {}", describe(last));
    check(
        seen.len() == 2 && !last.is_approved && last.reviewer == "mlee@acme.io",
        "expected the rejection to be recorded",
    );
    check(
        rejected.status == "cancelled by mlee@acme.io",
        "expected CancelWireTransfer to run",
    );
    check(
        rejected.note.as_deref() == Some("beneficiary not on the vendor list"),
        "expected the rejection's override to apply too",
    );
    check(
        bank.sent().len() == 1,
        "a rejected transfer must never reach the bank",
    );
    println!(
        "  Transfers sent in total: {}. ExecuteWireTransfer never ran for Globex.",
        bank.sent().len()
    );

    // --- Step 6: nothing is left waiting.

    println!();
    println!("=== Step 6: the waiting list is empty ===");
    let still_waiting = list_waiting(&restarted).await;
    println!("  Threads waiting for a human: {}", still_waiting.len());
    check(
        still_waiting.is_empty(),
        "expected both answers to have used up their checkpoints",
    );

    drop(restarted);
    remove_database(&db_path);
}

fn open_file_backed_store(path: &Path) -> Arc<SqliteCheckpointStore> {
    Arc::new(SqliteCheckpointStore::open(path).expect("the checkpoint file opens"))
}

/// The threads waiting for a human, by thread id.
async fn list_waiting(store: &SqliteCheckpointStore) -> Vec<ThreadSummary> {
    let query = ThreadQuery::default().with_states([ThreadState::Interrupted]);
    let page = store
        .list_threads(&query)
        .await
        .expect("the store can be queried");
    // Newest first is the store's order; both were saved in the same second,
    // so sort for stable output.
    let mut threads = page.threads;
    threads.sort_by(|a, b| a.thread_id.cmp(&b.thread_id));
    threads
}

/// `decided_at` (when the resume began) is left out: it changes every run.
fn describe(decision: &HumanDecision) -> String {
    let overrides: Vec<String> = decision
        .overrides
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    format!(
        "{} by {} at {}, overrides {}",
        if decision.is_approved {
            "approved"
        } else {
            "rejected"
        },
        decision.reviewer,
        decision.boundary,
        overrides.join(", ")
    )
}

fn describe_result<S>(result: &GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            format!("Interrupted at {suspended_node}")
        }
        GraphExecutionResult::Failed(err) => format!("Failed({err:?})"),
        GraphExecutionResult::Cancelled { node, .. } => format!("Cancelled at {node}"),
    }
}

fn completed<S>(result: GraphExecutionResult<S>) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        other => fail(&format!(
            "expected Completed, got: {}",
            describe_result(&other)
        )),
    }
}

/// The SQLite file and any sidecar files SQLite may leave next to it.
fn remove_database(path: &Path) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        let _ = std::fs::remove_file(file);
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
