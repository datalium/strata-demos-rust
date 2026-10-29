//! Graph identity (`design/31-Graph-Identity.md`): deploying a new version of
//! a graph while threads the old one wrote are still waiting.
//!
//! A shop's returns process: `Inspect` -> `Approve` (a manager's sign-off) ->
//! `Refund`. Returns wait at `Approve` for days, so when version 2 ships, some
//! threads version 1 wrote are still suspended in the store. Version 2 renames
//! a state field (`amount` -> `refund_amount`), adds two (`refund_method`,
//! `restocked`), renames a node (`Refund` -> `IssueRefund`) and adds a node
//! after it (`Restock`).
//!
//! A graph compiled with `CompileOptions::with_graph_id` /
//! `with_graph_version` writes that identity on every checkpoint, and refuses
//! a thread another graph or another version wrote, before writing anything,
//! unless its `with_migration` adopts it. Everything runs against one SQLite
//! file, read back through the store's query API (`CheckpointQuery`), whose
//! `list_threads` filters on the file's `graph_id` / `graph_version` columns.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointQuery, CheckpointStore, GraphExecutionResult, HumanSignal, MAX_PAGE_SIZE,
    ThreadQuery, ThreadState,
};
use datalium_strata::graph::{CompileOptions, StateGraph};
use datalium_strata::schema::Checkpoint;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use serde::{Deserialize, Serialize};

/// Version 1's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReturnV1 {
    return_id: String,
    amount: u32,
    inspected: bool,
    refunded: bool,
    log: Vec<String>,
}

impl ReturnV1 {
    fn new(return_id: &str, amount: u32) -> Self {
        Self {
            return_id: return_id.to_string(),
            amount,
            inspected: false,
            refunded: false,
            log: Vec::new(),
        }
    }
}

/// Version 2's state: `amount` is now `refund_amount`, and `refund_method`
/// and `restocked` are new.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReturnV2 {
    return_id: String,
    refund_amount: u32,
    refund_method: String,
    inspected: bool,
    refunded: bool,
    restocked: bool,
    log: Vec<String>,
}

impl ReturnV2 {
    fn new(return_id: &str, amount: u32) -> Self {
        Self {
            return_id: return_id.to_string(),
            refund_amount: amount,
            refund_method: "original-payment".to_string(),
            inspected: false,
            refunded: false,
            restocked: false,
            log: Vec::new(),
        }
    }
}

fn returns_v1() -> StateGraph<ReturnV1> {
    StateGraph::<ReturnV1>::new()
        .add_node("Inspect", |mut r: ReturnV1| {
            r.inspected = true;
            r.log.push("inspected".to_string());
            r
        })
        .add_node("Approve", |mut r: ReturnV1| {
            r.log.push("sent for approval".to_string());
            r
        })
        .add_interrupt_boundary("Approve")
        .add_node("Refund", |mut r: ReturnV1| {
            r.refunded = true;
            r.log.push(format!("refunded ${}", r.amount));
            r
        })
        .add_edge("Inspect", "Approve")
        .add_edge("Approve", "Refund")
}

fn returns_v2() -> StateGraph<ReturnV2> {
    StateGraph::<ReturnV2>::new()
        .add_node("Inspect", |mut r: ReturnV2| {
            r.inspected = true;
            r.log.push("inspected".to_string());
            r
        })
        .add_node("Approve", |mut r: ReturnV2| {
            r.log.push("sent for approval".to_string());
            r
        })
        .add_interrupt_boundary("Approve")
        .add_node("IssueRefund", |mut r: ReturnV2| {
            r.refunded = true;
            r.log.push(format!(
                "refunded ${} to {}",
                r.refund_amount, r.refund_method
            ));
            r
        })
        .add_node("Restock", |mut r: ReturnV2| {
            r.restocked = true;
            r.log.push("restocked".to_string());
            r
        })
        .add_edge("Inspect", "Approve")
        .add_edge("Approve", "IssueRefund")
        .add_edge("IssueRefund", "Restock")
}

fn named(id: &str, version: &str) -> CompileOptions {
    CompileOptions::new()
        .with_graph_id(id)
        .with_graph_version(version)
}

#[tokio::main]
async fn main() {
    // The file lives in the demo's folder (gitignored), deleted first so every
    // run starts from an empty file, and left behind for inspection.
    let db = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("returns.db");
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }

    let approve = || HumanSignal::approve("manager-ines");
    let v1 = returns_v1()
        .compile_with(named("returns", "1"))
        .expect("version 1 compiles");
    let v2 = returns_v2()
        .compile_with(named("returns", "2"))
        .expect("version 2 compiles");

    // --- Step 1: version 1 is live. Two returns wait for a manager. ---

    step(
        "Step 1: version 1 is deployed; two returns wait for approval",
        true,
    );
    {
        let store = open(&db);
        for (id, amount) in [("R-1", 120), ("R-2", 75)] {
            let result = v1.run(id, ReturnV1::new(id, amount), store.clone()).await;
            check(
                matches!(&result, GraphExecutionResult::Interrupted { suspended_node, .. } if suspended_node == "Approve"),
                &format!("{id} should wait at Approve"),
            );
            println!(
                "  {id} (${amount}) waits at Approve, written by {}",
                written_by(&load(&store, id).await)
            );
        }
    }

    // --- Step 2: version 2 ships, with no migration. It runs new returns and refuses old ones. ---

    step("Step 2: version 2 is deployed, with no migration", false);
    {
        let store = open(&db);
        let fresh = v2.run("R-3", ReturnV2::new("R-3", 40), store.clone()).await;
        check(
            matches!(fresh, GraphExecutionResult::Interrupted { .. }),
            "R-3 should wait at Approve",
        );
        println!(
            "  R-3 ($40) is new: version 2 runs it, and it waits at Approve, written by {}",
            written_by(&load(&store, "R-3").await)
        );

        print_threads(&store).await;

        let before = rows(&store).await;
        let refused = refusal(v2.resume("R-1", approve(), store.clone()).await);
        println!("  Version 2 resumes R-1: {refused}");
        check(
            refused
                == "thread 'R-1' was written by graph 'returns' version 1, not version 2, and this graph has no migration",
            "version 2 should refuse a version 1 thread",
        );
        let after = rows(&store).await;
        println!(
            "  Checkpoint rows before and after: {before} and {after}. R-1 still waits, resumable by version 1."
        );
        check(after == before, "a refused resume should write nothing");

        // `owns` is the cheap pre-check a worker can make before resuming anything.
        let (r1, r3) = (load(&store, "R-1").await, load(&store, "R-3").await);
        println!(
            "  v2.owns(R-1): {}, v2.owns(R-3): {}",
            v2.owns(&r1),
            v2.owns(&r3)
        );
        check(
            !v2.owns(&r1) && v2.owns(&r3),
            "v2 should own its own version's threads only",
        );
    }

    // --- Step 3: another graph, even with the same node names and state, is refused. ---

    step(
        "Step 3: a different graph can't take a returns thread",
        false,
    );
    {
        let store = open(&db);
        let before = rows(&store).await;

        // Same builder, same nodes, same state type: only the identity differs.
        let exchanges = returns_v1()
            .compile_with(named("exchanges", "1"))
            .expect("compiles");
        let other = refusal(exchanges.resume("R-1", approve(), store.clone()).await);
        println!("  'exchanges' resumes R-1:      {other}");
        check(
            other
                == "thread 'R-1' was written by graph 'returns' version 1, not 'exchanges' version 1",
            "another id should be refused",
        );

        let unnamed = returns_v1().compile().expect("compiles");
        let anonymous = refusal(unnamed.resume("R-1", approve(), store.clone()).await);
        println!("  An unnamed graph resumes R-1: {anonymous}");
        check(
            anonymous.ends_with("and this graph has no identity"),
            "a graph without an identity should be refused",
        );

        let end = refusal(exchanges.end("R-1", store.clone()).await);
        println!("  'exchanges' ends R-1:         {end}");
        check(end == other, "an end should be refused like a resume");
        check(
            rows(&store).await == before,
            "refusals should write nothing",
        );
    }

    // --- Step 4: which threads still need version 1? Drain them with version 1. ---

    step(
        "Step 4: the query port lists each version's waiting threads",
        false,
    );
    {
        let store = open(&db);
        let v1_waiting = waiting(&store, "1").await;
        let v2_waiting = waiting(&store, "2").await;
        println!(
            "  returns version 1, interrupted: {}",
            v1_waiting.join(", ")
        );
        println!(
            "  returns version 2, interrupted: {}",
            v2_waiting.join(", ")
        );
        check(
            v1_waiting == ["R-1", "R-2"] && v2_waiting == ["R-3"],
            "unexpected listing by version",
        );

        // Version 1 stays deployed beside version 2 for a while. A dispatcher
        // picks the runner by the identity stored on the thread's latest
        // checkpoint.
        let latest = store
            .load_latest("R-1")
            .await
            .expect("store works")
            .expect("R-1 exists");
        let version = latest
            .checkpoint
            .graph
            .as_ref()
            .and_then(|g| g.version.clone())
            .unwrap_or_default();
        println!("  R-1 was written by version {version}, so it goes to version 1:");
        check(version == "1", "R-1 should be a version 1 thread");
        let refunded = completed(v1.resume("R-1", approve(), store.clone()).await, "R-1");
        println!("    completed: {}", refunded.log.join(" / "));
        check(refunded.refunded, "version 1 should refund R-1");
    }

    // --- Step 5: version 2 gets a migration, and adopts the last version 1 thread. ---

    step(
        "Step 5: version 2 is redeployed with a migration for version 1 threads",
        false,
    );
    let migrations = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen = migrations.clone();
    let v2_migrating = returns_v2()
        .compile_with(
            named("returns", "2").with_migration(move |mut checkpoint: Checkpoint| {
                // It sees the level as stored. Anything but version 1 is refused (None).
                if checkpoint.graph.as_ref()?.version.as_deref() != Some("1") {
                    return None;
                }
                seen.lock().unwrap().push(checkpoint.thread_id.clone());
                let state = checkpoint.frozen_state.as_object_mut()?;
                let amount = state.remove("amount")?;
                state.insert("refund_amount".to_string(), amount);
                state.insert("refund_method".to_string(), "original-payment".into());
                state.insert("restocked".to_string(), false.into());
                if checkpoint.next_node.as_deref() == Some("Refund") {
                    checkpoint.next_node = Some("IssueRefund".to_string());
                }
                Some(checkpoint)
            }),
        )
        .expect("version 2 with a migration compiles");
    {
        let store = open(&db);
        let r2 = load(&store, "R-2").await;
        println!(
            "  v2.owns(R-2) without the migration: {}; with it: {}",
            v2.owns(&r2),
            v2_migrating.owns(&r2)
        );
        check(
            !v2.owns(&r2) && v2_migrating.owns(&r2),
            "a graph with a migration should own other versions of its id",
        );

        let state = completed(
            v2_migrating.resume("R-2", approve(), store.clone()).await,
            "R-2",
        );
        println!(
            "  Version 2 resumes R-2: completed: {}",
            state.log.join(" / ")
        );
        check(
            *migrations.lock().unwrap() == ["R-2"],
            "the migration should run once, for R-2",
        );
        check(
            state.refund_amount == 75
                && state.refund_method == "original-payment"
                && state.refunded
                && state.restocked,
            "R-2 should be refunded and restocked by version 2, with its amount carried over",
        );

        // Version 2's own threads need no migration.
        let own = completed(
            v2_migrating.resume("R-3", approve(), store.clone()).await,
            "R-3",
        );
        check(own.restocked, "R-3 should complete");
        check(
            migrations.lock().unwrap().len() == 1,
            "a version 2 thread should not be migrated",
        );

        println!(
            "  R-2's log (CheckpointQuery::get_log): the resume stamp carries the migrated identity, state and next node:"
        );
        let log = log_of(&store, "R-2").await;
        for (sequence, checkpoint) in &log {
            println!(
                "    #{sequence} graph_version {}  next_node {:<11}  frozen_state {}",
                version_of(checkpoint),
                checkpoint.next_node.as_deref().unwrap_or("-"),
                checkpoint.frozen_state
            );
        }
        check(
            log.len() == 2 && version_of(&log[0].1) == "1" && version_of(&log[1].1) == "2",
            "the stamp should record version 2",
        );
        check(
            log[1].1.next_node.as_deref() == Some("IssueRefund")
                && log[1].1.frozen_state["refund_amount"] == 75
                && log[1].1.frozen_state.get("amount").is_none(),
            "the stamp should be the migrated checkpoint",
        );

        let v1_waiting = waiting(&store, "1").await;
        println!(
            "  returns version 1, interrupted: {}",
            if v1_waiting.is_empty() {
                "none: version 1 can be retired".to_string()
            } else {
                v1_waiting.join(", ")
            }
        );
        check(
            v1_waiting.is_empty(),
            "no version 1 thread should be left waiting",
        );
    }

    step("Done: what the file holds now", false);
    print_threads(&open(&db)).await;
}

// --- Helpers ---

fn open(db: &Path) -> Arc<SqliteCheckpointStore> {
    Arc::new(SqliteCheckpointStore::open(db).expect("the SQLite file opens"))
}

async fn load(store: &Arc<SqliteCheckpointStore>, thread: &str) -> Checkpoint {
    store
        .load(thread)
        .await
        .expect("store works")
        .unwrap_or_else(|| fail(&format!("expected a checkpoint for {thread}")))
}

fn written_by(checkpoint: &Checkpoint) -> String {
    checkpoint
        .graph
        .as_ref()
        .map_or_else(|| "no graph".to_string(), ToString::to_string)
}

fn version_of(checkpoint: &Checkpoint) -> &str {
    checkpoint
        .graph
        .as_ref()
        .and_then(|g| g.version.as_deref())
        .unwrap_or("-")
}

/// The interrupted threads `returns` version `version` wrote, by the
/// `graph_id` / `graph_version` columns of the file's head table.
async fn waiting(store: &Arc<SqliteCheckpointStore>, version: &str) -> Vec<String> {
    let query = ThreadQuery::default()
        .with_graph("returns")
        .with_graph_version(version)
        .with_states([ThreadState::Interrupted]);
    let page = store.list_threads(&query).await.expect("query works");
    // The listing is newest first, by wall-clock time; sort for stable output.
    let mut ids: Vec<String> = page.threads.into_iter().map(|t| t.thread_id).collect();
    ids.sort();
    ids
}

/// Every thread's log, oldest first, as `(sequence, checkpoint)`.
async fn log_of(store: &Arc<SqliteCheckpointStore>, thread: &str) -> Vec<(i64, Checkpoint)> {
    let page = store
        .get_log(thread, None, MAX_PAGE_SIZE)
        .await
        .expect("query works");
    page.entries
        .into_iter()
        .map(|e| (e.sequence, e.checkpoint))
        .collect()
}

/// How many checkpoint rows the file holds, across every thread.
async fn rows(store: &Arc<SqliteCheckpointStore>) -> usize {
    let page = store
        .list_threads(&ThreadQuery::default())
        .await
        .expect("query works");
    let mut total = 0;
    for thread in page.threads {
        total += log_of(store, &thread.thread_id).await.len();
    }
    total
}

async fn print_threads(store: &Arc<SqliteCheckpointStore>) {
    let mut threads = store
        .list_threads(&ThreadQuery::default())
        .await
        .expect("query works")
        .threads;
    threads.sort_by(|a, b| a.thread_id.cmp(&b.thread_id));
    println!("    list_threads (each thread's head, with the graph that wrote it):");
    for t in threads {
        let (id, version) = t.graph.map_or_else(
            || ("-".to_string(), "-".to_string()),
            |g| (g.id, g.version.unwrap_or_else(|| "-".to_string())),
        );
        println!(
            "      {}  {:<11}  graph_id {id}  graph_version {version}  schema_version {}",
            t.thread_id,
            format!("{:?}", t.state).to_lowercase(),
            t.schema_version
        );
    }
}

fn step(title: &str, first: bool) {
    if !first {
        println!();
    }
    println!("=== {title} ===");
}

fn completed<S>(result: GraphExecutionResult<S>, thread: &str) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => {
            fail(&format!("{thread}: expected Completed, got {err:?}"))
        }
        _ => fail(&format!("{thread}: expected Completed")),
    }
}

/// A refusal is `Failed(ExecutionAborted(why))`, returned before anything is written.
fn refusal<S>(result: GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => message,
        GraphExecutionResult::Failed(other) => fail(&format!("expected a refusal, got {other:?}")),
        _ => fail("expected a refusal"),
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
