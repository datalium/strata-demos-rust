//! Durable execution (design/33-Durable-Execution.md, SG-10): a checkpoint per
//! node, and a real crash recovered.
//!
//! An order runs `Validate` -> `Charge` -> `Ship`, compiled with
//! `CheckpointPolicy::EveryNode`, so the run writes a `running` checkpoint
//! before its first node and after every node that commits, and `completed` at
//! the end. With the default policy (`Boundaries`) a graph with no interrupt
//! boundary writes nothing until it stops, and a process that dies mid-run
//! leaves no trace of the thread at all.
//!
//! The crash is real. The program starts a copy of itself
//! (`std::env::current_exe()`) in `worker` mode. The worker runs the order
//! under a lease, charges the card inside `Charge`, prints a marker line and
//! hangs before the node can return. The parent kills the worker's process
//! when it reads the marker (`Child::kill`, `TerminateProcess` on Windows), so
//! `Charge` never commits, nothing releases the lease, and the thread is left
//! `running` in a SQLite file. The parent then opens the same file and:
//!
//!   - shows that a `running` thread refuses an ordinary `resume`;
//!   - runs the recovery driver (`recover_stale`), which skips the thread
//!     while the dead worker's lease is live, and recovers it once the lease
//!     has expired;
//!   - shows the contract: at least once. `Charge` runs again, with the same
//!     `NodeContext::idempotency_key` and `NodeContext::is_recovery` set, and
//!     the payment gateway, which deduplicates by that key, charges once.
//!
//! Both processes open the store with a hand-moved clock
//! (`SqliteCheckpointStore::with_clock`) at the same instant, so the lease
//! expires when the parent moves its clock on, not after a real 30-second
//! wait. Everything is deterministic and offline.
//!
//!   cargo run -p durable-execution
//!
//! The `worker <dir>` mode is the child's; you don't run it by hand.

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointQuery, CheckpointStore, GraphExecutionResult, HumanSignal, LeaseOptions, ThreadQuery,
    ThreadState,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, RunMode};
use datalium_strata::recovery::{RecoverOptions, recoveries_of};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::store::timestamp_text;
use datalium_strata_memory::Clock;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use serde::{Deserialize, Serialize};

const THREAD: &str = "order-1042";
const CRASH_MARKER: &str = "@@crash-here";
const TTL: Duration = Duration::from_secs(30);

fn noon() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 24, 12, 0, 0).unwrap()
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            run_parent(&std::env::temp_dir().join("strata-demo-20-durable-execution")).await;
            ExitCode::SUCCESS
        }
        [mode, dir] if mode == "worker" => run_worker(Path::new(dir)).await,
        _ => {
            eprintln!(
                "usage: durable-execution (no arguments; `worker <dir>` is the child process's mode)"
            );
            ExitCode::from(2)
        }
    }
}

// --- The child: run the order until Charge, where the parent kills it. -------

async fn run_worker(dir: &Path) -> ExitCode {
    let files = DemoFiles::new(dir);
    // The store's clock never moves here: the lease simply runs out at
    // 12:00:30. Renewals run on tokio's (real) clock every ttl / 3, and the
    // worker is killed long before the first one.
    let clock = HandClock::new(noon());
    let store: Arc<dyn CheckpointStore> = Arc::new(open_store(&files.checkpoints, &clock));
    let graph = build_graph(
        Journal::new(&files.visits, "worker-1", ""),
        Gateway::new(&files.payments),
        true,
    );

    let config = RunConfig::new().with_lease(LeaseOptions::new("worker-1", TTL));
    let result = graph
        .run_with(THREAD, Order::new(THREAD, 12900), store, config)
        .await;

    // Only reached if Charge didn't hang: the parent treats a worker that
    // exits on its own as a failure.
    eprintln!(
        "worker: the run returned instead of hanging in Charge: {}",
        outcome(&result)
    );
    ExitCode::FAILURE
}

// --- The parent: crash a worker, then recover what it left. ------------------

async fn run_parent(dir: &Path) {
    if dir.exists() {
        std::fs::remove_dir_all(dir).expect("clear the demo folder");
    }
    std::fs::create_dir_all(dir).expect("create the demo folder");
    let files = DemoFiles::new(dir);
    let gateway = Gateway::new(&files.payments);

    println!("=== Step 1: a worker process runs the order and is killed inside Charge ===");
    crash_worker(dir);
    println!(
        "  Killed the worker's process while Charge was still running: the node never committed."
    );

    println!();
    println!("=== Step 2: what the dead worker left in checkpoints.sqlite ===");
    let clock = HandClock::new(noon());
    let store = Arc::new(open_store(&files.checkpoints, &clock));
    let dyn_store: Arc<dyn CheckpointStore> = store.clone();
    let crashed = log_of(&store).await;
    print_log(&crashed);
    check(
        shapes(&crashed) == ["running:->Validate", "running:Validate>Charge"],
        "expected a running checkpoint before Validate and one after it, and nothing after Charge started",
    );
    let running = threads_in(&store, ThreadState::Running).await;
    println!("  Threads in state Running: {}", running.join(", "));
    check(
        running == [THREAD],
        "expected the thread to be listed as running",
    );
    let dead_lease = store.get_lease(THREAD).await.expect("read the lease");
    match &dead_lease {
        Some(lease) => println!(
            "  Lease: held by '{}' until {} (token {}): nobody released it.",
            lease.owner,
            timestamp_text(lease.expires_at),
            lease.token
        ),
        None => println!("  Lease: none."),
    }
    check(
        dead_lease
            .as_ref()
            .is_some_and(|lease| lease.owner == "worker-1" && lease.token == 1),
        "expected the dead worker's lease to be still in place",
    );
    println!(
        "  Payment gateway: {} request, {} charge.",
        gateway.requests(),
        gateway.charges()
    );
    check(
        (gateway.requests(), gateway.charges()) == (1, 1),
        "expected the worker to have charged the card before it died",
    );

    println!();
    println!("=== Step 3: a running thread refuses an ordinary resume ===");
    let recoverer = build_graph(
        Journal::new(&files.visits, "worker-2", "  | "),
        gateway.clone(),
        false,
    );
    let resumed = recoverer
        .resume(
            THREAD,
            HumanSignal::approve("ops-oncall"),
            dyn_store.clone(),
        )
        .await;
    let refusal = refusal_of(&resumed);
    println!("  resume -> Failed: {}", refusal.as_deref().unwrap_or("-"));
    check(
        refusal.as_deref()
            == Some(&*format!(
                "thread '{THREAD}' is running: only a waiting, failed or cancelled thread can be resumed"
            )),
        "expected a running thread to refuse a resume",
    );
    check(
        log_of(&store).await.len() == crashed.len(),
        "expected the refused resume to write nothing",
    );
    println!(
        "  Nothing was written: the log still has {} checkpoints. Only a recovery continues it.",
        crashed.len()
    );

    println!();
    println!("=== Step 4: the recovery driver leaves a thread whose lease is live ===");
    let early = recoverer
        .recover_stale(
            store.clone(),
            clock.now(),
            RecoverOptions::new().with_config(leased("worker-2")),
        )
        .await
        .expect("the driver runs");
    println!(
        "  recover_stale at {}: recovered {}, skipped:",
        timestamp_text(clock.now()),
        early.recovered.len()
    );
    for skipped in &early.skipped {
        println!("    {}: {}", skipped.thread_id, skipped.reason);
    }
    check(
        early.recovered.is_empty()
            && early.skipped.len() == 1
            && early.skipped[0].thread_id == THREAD
            && early.skipped[0].reason == "it is held by 'worker-1' until 2026-10-24T12:00:30Z",
        "expected the live lease to protect the thread",
    );
    println!(
        "  A live lease is the only sign of life: until it runs out, the worker might just be slow."
    );

    println!();
    println!("=== Step 5: the lease runs out, and the driver recovers the thread ===");
    clock.advance(TTL + Duration::from_secs(1));
    println!("  Clock advanced to {}.", timestamp_text(clock.now()));
    let (observer, mut events) = RunEventChannel::new();
    // With a lease on the recovery runs, stale_after defaults to zero: a
    // running thread nobody holds is dead.
    let mut report = recoverer
        .recover_stale(
            store.clone(),
            clock.now(),
            RecoverOptions::new().with_config(leased("worker-2").with_observer(observer)),
        )
        .await
        .expect("the driver runs");
    check(
        report.recovered.len() == 1 && report.skipped.is_empty(),
        "expected exactly one thread recovered and none skipped",
    );
    let recovered = report.recovered.remove(0);
    println!(
        "  recover_stale: recovered '{}' at [{}], recoveries {}, result {}.",
        recovered.thread_id,
        recovered.at.join(", "),
        recovered.recoveries,
        outcome(&recovered.result)
    );
    check(
        recovered.at == ["Charge"] && recovered.recoveries == 1,
        "expected the thread to continue at Charge, recovered once",
    );
    let GraphExecutionResult::Completed(last) = recovered.result else {
        fail("expected the recovery to complete")
    };
    println!("  The recovery run's events:");
    let mut seen = Vec::new();
    while let Some(event) = events.recv().await {
        println!("    {}", describe(&event));
        seen.push(event);
    }
    let names: Vec<&str> = seen.iter().map(name).collect();
    check(
        names.starts_with(&[
            "RunStarted",
            "LeaseAcquired",
            "CheckpointSaved",
            "Recovered",
        ]),
        "expected RunStarted, LeaseAcquired, the claim's CheckpointSaved and Recovered before the first node",
    );
    check(
        matches!(
            seen[0].kind,
            RunEventKind::RunStarted {
                mode: RunMode::Recover,
                ..
            }
        ),
        "expected the recovery's run to start in mode Recover",
    );
    check(
        names[4..]
            == [
                "NodeStarted",
                "NodeCompleted",
                "CheckpointSaved",
                "NodeStarted",
                "NodeCompleted",
                "CheckpointSaved",
                "RunCompleted",
            ],
        "expected Charge and Ship to run, each followed by its checkpoint, and then RunCompleted",
    );

    println!();
    println!("=== Step 6: at least once, and charged once ===");
    println!("  Every node run, from both processes (visits.log):");
    let visits = Journal::read(&files.visits);
    for visit in &visits {
        println!(
            "    {}  {:<9} key {:<22} is_recovery {}",
            visit.worker, visit.node, visit.key, visit.is_recovery
        );
    }
    check(
        visits
            == [
                Visit::new("worker-1", "Validate", "order-1042/Validate#1", false),
                Visit::new("worker-1", "Charge", "order-1042/Charge#2", false),
                Visit::new("worker-2", "Charge", "order-1042/Charge#2", true),
                Visit::new("worker-2", "Ship", "order-1042/Ship#3", false),
            ],
        "expected Charge to run twice with one key, is_recovery only on the re-run, and Validate not to run again",
    );
    println!(
        "  Payment gateway: {} requests, {} charge. The re-run sent the same key and got {} back.",
        gateway.requests(),
        gateway.charges(),
        last.charge_id.as_deref().unwrap_or("-")
    );
    check(
        (gateway.requests(), gateway.charges()) == (2, 1),
        "expected two requests and one charge",
    );
    println!("  Final state: {last:?}");
    check(
        last.validated && last.charge_id.as_deref() == Some("ch_001") && last.shipped,
        "expected the order validated, charged once and shipped",
    );

    println!("  The whole checkpoint log, with recoveries_of(checkpoint):");
    let log = log_of(&store).await;
    print_log(&log);
    check(
        shapes(&log)
            == [
                "running:->Validate",
                "running:Validate>Charge",
                "running:Validate>Charge",
                "running:Charge>Ship",
                "completed:Ship>-",
            ],
        "expected the claim (the crashed checkpoint again), a checkpoint after Charge, and completed",
    );
    check(
        log.iter().map(recoveries_of).collect::<Vec<_>>() == [0, 0, 1, 0, 0],
        "expected only the claim to count a recovery",
    );
    println!(
        "  Only the claim counts the recovery: the next node to commit starts the count again."
    );

    let lease = store.get_lease(THREAD).await.expect("read the lease");
    let completed = threads_in(&store, ThreadState::Completed).await;
    println!(
        "  Lease: {}. Threads in state Completed: {}.",
        match &lease {
            None => "none (the recovery released it)".to_string(),
            Some(lease) => format!("held by '{}'", lease.owner),
        },
        completed.join(", ")
    );
    check(
        lease.is_none() && completed == [THREAD],
        "expected the thread completed and its lease released",
    );
    let again = recoverer.recover(THREAD, dyn_store.clone()).await;
    let refusal = refusal_of(&again);
    println!(
        "  recover again -> Failed: {}",
        refusal.as_deref().unwrap_or("-")
    );
    check(
        refusal.as_deref()
            == Some(&*format!(
                "thread '{THREAD}' is not running: its latest checkpoint is completed"
            )),
        "expected a completed thread to refuse a recovery",
    );

    println!();
    println!("Files left in {}", dir.display());
    println!(
        "=== Done: the process died mid-node, and the thread finished without charging twice. ==="
    );
}

/// Starts this program in `worker` mode, relays its output, and kills it when
/// it says it is inside Charge.
fn crash_worker(dir: &Path) {
    let me = std::env::current_exe().expect("can't tell where this program is");
    let mut worker = Command::new(me)
        .arg("worker")
        .arg(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("couldn't start the worker");

    let stdout = worker.stdout.take().expect("piped stdout");
    let mut stderr = worker.stderr.take().expect("piped stderr");
    let (lines, incoming) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let errors = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stderr.read_to_string(&mut text);
        text
    });

    // A worker that never reaches Charge is a failure, not a reason to wait
    // forever. A real-time limit only for that; nothing is asserted on it.
    let patience = Instant::now() + Duration::from_secs(60);
    let mut killed = false;
    loop {
        let left = patience.saturating_duration_since(Instant::now());
        match incoming.recv_timeout(left) {
            Ok(Ok(line)) if line == CRASH_MARKER => {
                worker.kill().expect("kill the worker");
                killed = true;
                break;
            }
            Ok(Ok(line)) => println!("  | {line}"),
            Ok(Err(_)) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                let _ = worker.kill();
                break;
            }
        }
    }
    let _ = worker.wait();
    if !killed {
        fail(&format!(
            "the worker didn't reach Charge: {}",
            errors.join().unwrap_or_default()
        ));
    }
}

fn build_graph(
    journal: Arc<Journal>,
    gateway: Arc<Gateway>,
    hang_in_charge: bool,
) -> CompiledGraph<Order> {
    let validate = journal.clone();
    let ship = journal.clone();
    StateGraph::<Order>::new()
        .add_async_node("Validate", move |order: Order, context: NodeContext| {
            validate.visit(&context);
            async move {
                Order {
                    validated: true,
                    ..order
                }
            }
        })
        .add_async_node("Charge", move |order: Order, context: NodeContext| {
            let (journal, gateway) = (journal.clone(), gateway.clone());
            async move {
                journal.visit(&context);
                let key = context.idempotency_key();

                // The effect outside the state: the gateway deduplicates by
                // the key, as real payment APIs do.
                let (id, repeated) = gateway.charge(key, order.amount_cents);
                journal.say(&if repeated {
                    format!(
                        "{}: Charge: the gateway already had key {key}: returned {id}, charged nothing",
                        journal.worker
                    )
                } else {
                    format!(
                        "{}: Charge: the gateway charged {id} (${}.{:02}) under key {key}",
                        journal.worker,
                        order.amount_cents / 100,
                        order.amount_cents % 100
                    )
                });
                if hang_in_charge {
                    // The crash point: the card is charged, the node hasn't
                    // returned, so nothing records it.
                    journal.say(&format!(
                        "{}: Charge: hangs before it can return (the parent kills the process now)",
                        journal.worker
                    ));
                    println!("{CRASH_MARKER}");
                    std::future::pending::<()>().await;
                }
                Order {
                    charge_id: Some(id),
                    ..order
                }
            }
        })
        .add_async_node("Ship", move |order: Order, context: NodeContext| {
            ship.visit(&context);
            async move {
                Order {
                    shipped: true,
                    ..order
                }
            }
        })
        .add_edge("Validate", "Charge")
        .add_edge("Charge", "Ship")
        .compile_with(
            CompileOptions::new()
                .with_graph_id("order-fulfilment")
                .with_graph_version("1")
                .with_checkpointing(CheckpointPolicy::EveryNode),
        )
        .expect("the graph is valid")
}

/// The store's clock is the one lease expiry is judged by, in every process
/// that opens the file.
fn open_store(path: &Path, clock: &HandClock) -> SqliteCheckpointStore {
    SqliteCheckpointStore::open(path)
        .expect("open checkpoints.sqlite")
        .with_clock(clock.clock())
}

fn leased(owner: &str) -> RunConfig {
    RunConfig::new().with_lease(LeaseOptions::new(owner, TTL))
}

async fn log_of(store: &SqliteCheckpointStore) -> Vec<Checkpoint> {
    store
        .get_log(THREAD, None, 100)
        .await
        .expect("read the log")
        .entries
        .into_iter()
        .map(|entry| entry.checkpoint)
        .collect()
}

async fn threads_in(store: &SqliteCheckpointStore, state: ThreadState) -> Vec<String> {
    store
        .list_threads(&ThreadQuery::default().with_states([state]))
        .await
        .expect("list threads")
        .threads
        .into_iter()
        .map(|thread| thread.thread_id)
        .collect()
}

fn print_log(log: &[Checkpoint]) {
    for (i, c) in log.iter().enumerate() {
        println!(
            "    #{}  {:<9}  after {:<9} next {:<8}  recoveries {}",
            i + 1,
            status(c.status),
            c.suspended_node.as_deref().unwrap_or("-"),
            c.next_node.as_deref().unwrap_or("-"),
            recoveries_of(c)
        );
    }
}

/// `running:Validate>Charge`: the status, the node that last committed, the
/// node to run next.
fn shapes(log: &[Checkpoint]) -> Vec<String> {
    log.iter()
        .map(|c| {
            format!(
                "{}:{}>{}",
                status(c.status),
                c.suspended_node.as_deref().unwrap_or("-"),
                c.next_node.as_deref().unwrap_or("-")
            )
        })
        .collect()
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

fn outcome<S>(result: &GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Interrupted { .. } => "Interrupted".to_string(),
        GraphExecutionResult::Failed(error) => format!("Failed({error:?})"),
        GraphExecutionResult::Cancelled { reason, .. } => format!("Cancelled({reason:?})"),
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

fn name(event: &RunEvent) -> &'static str {
    match event.kind {
        RunEventKind::RunStarted { .. } => "RunStarted",
        RunEventKind::LeaseAcquired { .. } => "LeaseAcquired",
        RunEventKind::CheckpointSaved { .. } => "CheckpointSaved",
        RunEventKind::Recovered { .. } => "Recovered",
        RunEventKind::NodeStarted => "NodeStarted",
        RunEventKind::NodeCompleted { .. } => "NodeCompleted",
        RunEventKind::RunCompleted { .. } => "RunCompleted",
        _ => "other",
    }
}

fn describe(event: &RunEvent) -> String {
    let path = event.node_path.as_deref().unwrap_or("-");
    match &event.kind {
        RunEventKind::RunStarted { mode, node, .. } => {
            format!("RunStarted       mode {mode:?}, at {node}")
        }
        RunEventKind::LeaseAcquired {
            owner,
            token,
            expires_at,
        } => format!(
            "LeaseAcquired    '{owner}', token {token}, until {}",
            timestamp_text(*expires_at)
        ),
        RunEventKind::CheckpointSaved {
            status: saved,
            waiting_at,
            ..
        } => format!(
            "CheckpointSaved  {} at [{}]",
            status(*saved),
            waiting_at.join(", ")
        ),
        RunEventKind::Recovered { at, recoveries } => {
            format!(
                "Recovered        at [{}], recoveries {recoveries}",
                at.join(", ")
            )
        }
        RunEventKind::NodeStarted => format!("NodeStarted      {path}"),
        RunEventKind::NodeCompleted { result, .. } => {
            format!("NodeCompleted    {path}: {result:?}")
        }
        RunEventKind::RunCompleted { outcome, .. } => format!("RunCompleted     {outcome:?}"),
        other => format!("{other:?}"),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount_cents: i64,
    validated: bool,
    charge_id: Option<String>,
    shipped: bool,
}

impl Order {
    fn new(order_id: &str, amount_cents: i64) -> Self {
        Self {
            order_id: order_id.to_string(),
            amount_cents,
            validated: false,
            charge_id: None,
            shipped: false,
        }
    }
}

/// A clock the demo moves by hand: what each process gives its store.
#[derive(Clone)]
struct HandClock(Arc<Mutex<DateTime<Utc>>>);

impl HandClock {
    fn new(at: DateTime<Utc>) -> Self {
        Self(Arc::new(Mutex::new(at)))
    }

    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }

    fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += chrono::Duration::from_std(by).unwrap();
    }

    fn clock(&self) -> Clock {
        let now = self.0.clone();
        Arc::new(move || *now.lock().unwrap())
    }
}

/// Where the two processes meet: the checkpoint store and two plain logs, all
/// in one folder.
struct DemoFiles {
    checkpoints: PathBuf,
    payments: PathBuf,
    visits: PathBuf,
}

impl DemoFiles {
    fn new(dir: &Path) -> Self {
        Self {
            checkpoints: dir.join("checkpoints.sqlite"),
            payments: dir.join("payments.log"),
            visits: dir.join("visits.log"),
        }
    }
}

fn append_line(path: &Path, line: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open a log file");
    writeln!(file, "{line}").expect("append to a log file");
}

fn read_lines(path: &Path) -> Vec<Vec<String>> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| line.split('\t').map(str::to_string).collect())
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct Visit {
    worker: String,
    node: String,
    key: String,
    is_recovery: bool,
}

impl Visit {
    fn new(worker: &str, node: &str, key: &str, is_recovery: bool) -> Self {
        Self {
            worker: worker.to_string(),
            node: node.to_string(),
            key: key.to_string(),
            is_recovery,
        }
    }
}

/// A record of every node run, appended to a file so it survives the worker's
/// death. Each append is closed before the node goes on, so a kill can't lose
/// it.
struct Journal {
    path: PathBuf,
    worker: String,
    prefix: String,
}

impl Journal {
    fn new(path: &Path, worker: &str, prefix: &str) -> Arc<Self> {
        Arc::new(Self {
            path: path.to_path_buf(),
            worker: worker.to_string(),
            prefix: prefix.to_string(),
        })
    }

    /// A node's output: the worker's goes through the parent, which adds the
    /// same `  | ` prefix.
    fn say(&self, line: &str) {
        println!("{}{line}", self.prefix);
    }

    fn visit(&self, context: &NodeContext) {
        append_line(
            &self.path,
            &format!(
                "{}\t{}\t{}\t{}",
                self.worker,
                context.node_path(),
                context.idempotency_key(),
                context.is_recovery()
            ),
        );
        self.say(&format!(
            "{}: {} (key {}, is_recovery {})",
            self.worker,
            context.node_path(),
            context.idempotency_key(),
            context.is_recovery()
        ));
    }

    fn read(path: &Path) -> Vec<Visit> {
        read_lines(path)
            .into_iter()
            .map(|f| Visit::new(&f[0], &f[1], &f[2], f[3] == "true"))
            .collect()
    }
}

/// A stand-in for a payment provider that takes an idempotency key: a request
/// with a key it has seen returns the first charge instead of making another.
/// Backed by a file, so it outlives either process.
struct Gateway {
    path: PathBuf,
}

impl Gateway {
    fn new(path: &Path) -> Arc<Self> {
        Arc::new(Self {
            path: path.to_path_buf(),
        })
    }

    fn requests(&self) -> usize {
        read_lines(&self.path)
            .iter()
            .filter(|l| l[0] == "request")
            .count()
    }

    fn charges(&self) -> usize {
        read_lines(&self.path)
            .iter()
            .filter(|l| l[0] == "charge")
            .count()
    }

    /// The charge's id, and whether the key had been seen before.
    fn charge(&self, idempotency_key: &str, amount_cents: i64) -> (String, bool) {
        append_line(&self.path, &format!("request\t{idempotency_key}"));
        if let Some(earlier) = read_lines(&self.path)
            .into_iter()
            .find(|l| l[0] == "charge" && l[1] == idempotency_key)
        {
            return (earlier[2].clone(), true);
        }
        let id = format!("ch_{:03}", self.charges() + 1);
        append_line(
            &self.path,
            &format!("charge\t{idempotency_key}\t{id}\t{amount_cents}"),
        );
        (id, false)
    }
}
