//! Hosting a Strata graph in a service (`design/35-Hosting-Integration.md`),
//! with `datalium-strata-host`.
//!
//! A reminder service: Schedule -> Wait (a five-minute timer) -> Send. Rust has
//! no DI container, so the application wires the pieces itself, in a few lines
//! of `main`:
//!
//!   `StrataSettings`      read with serde from `strata.json` (the wake driver's
//!                         interval, the run lease), with the SQLite file set in
//!                         code: `settings.sqlite.open()` opens the store
//!   `RunConfig`           `settings.runs.config()` (the lease) plus the
//!                         `Outbox` service the `Send` node reads
//!   `Host`                `Host::new().with_clock(..).wake(graph, store, ..)`:
//!                         a driver task that wakes threads whose timer is due,
//!                         run until a `CancellationToken` fires
//!   `check_store`         plus `Host::status()`: what a readiness endpoint asks
//!
//! The host decides what is due on a clock the demo sets, and the waits between
//! its rounds are tokio's, paused: a five-minute timer is due as soon as the
//! demo moves the clock, and the demo asserts what happened, never how long it
//! took.
//!
//! Five scenarios:
//!   1. The host starts: the store and the settings came from configuration,
//!      the store answers and the driver is healthy.
//!   2. A thread waits on its timer; once it is due, the wake driver resumes it.
//!      Nobody calls `wake_due` by hand.
//!   3. A clean stop: the host is stopped while a woken run is in flight. The
//!      run finishes, nothing is cancelled, and the other due thread is left
//!      for the next host, which wakes it.
//!   4. An outage under a running host: the store probe goes unhealthy, the
//!      driver's rounds fail and back off, and the host keeps running.
//!   5. Start-up refusals: a graph with a structural error never compiles, and
//!      a driver setting the host can't run with stops `Host::run` before any
//!      driver starts.
//!
//! No LLM and no network: the "send" is a list in memory.

mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::schema::{Checkpoint, CheckpointStatus, WaitKind};
use datalium_strata::store::{CheckpointQuery, CheckpointStore};
use datalium_strata::{CancellationToken, StrataError};
use datalium_strata_host::{
    DriverStatus, Host, HostError, StoreHealth, StrataSettings, check_store,
};
use serde::{Deserialize, Serialize};
use support::{DemoClock, DriverLog, Outbox, Switchable};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reminder {
    to: String,
    text: String,
    sent: bool,
}

/// How long the store probe may take before it counts as unhealthy.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

// `start_paused` (tokio's `test-util` feature): the drivers' waits between
// rounds pass as soon as nothing else is ready to run.
#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    DriverLog::install();
    let db_path =
        std::env::temp_dir().join(format!("strata-demo-28-{}.sqlite", std::process::id()));
    run(&db_path).await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db_path.display()));
    }
}

async fn run(db_path: &Path) {
    // Real "now": a timer's due time is taken from the system clock when it is
    // set. The host's clock then starts here and only moves when the demo says.
    let start = Utc::now();
    let clock = DemoClock::at(start);

    // --- Scenario 1: a host from configuration ---

    println!("=== Scenario 1: the host starts, wired from configuration ===");
    let settings = settings(db_path);
    let app = App::start(
        &settings,
        &clock,
        reminder_graph(false).expect("the graph is valid"),
    );
    println!(
        "  Checkpoint store: SqliteCheckpointStore on a temp file (sqlite.path, set in code over strata.json)"
    );
    println!(
        "  Wake driver: every {:?}, batches of {} (wake, strata.json)",
        settings.wake.interval, settings.wake.batch_size
    );
    println!(
        "  Run lease: owner '{}', ttl {:?} (runs.lease, strata.json)",
        settings.runs.lease.owner,
        settings.runs.lease.ttl.unwrap_or_default()
    );
    check(
        db_path.exists(),
        "expected the SQLite file created when the store opened",
    );
    check(
        settings.wake.interval == Duration::from_secs(5) && settings.wake.batch_size == 10,
        "expected the wake settings from strata.json",
    );
    check(
        settings.runs.lease.owner == "reminders-1"
            && settings.runs.lease.ttl == Some(Duration::from_secs(30))
            && settings.runs.config().has_lease(),
        "expected the lease from strata.json",
    );

    until("the wake driver to start", || async {
        !app.status.drivers().is_empty()
    })
    .await;
    let (store_health, drivers) = readiness(&app).await;
    println!("  check_store: {store_health}");
    println!("  Host::status: {drivers}");
    check(
        store_health == "Healthy" && drivers == "Healthy (wake:Reminder, 0 failures in a row)",
        "expected the store to answer and one healthy driver",
    );

    // --- Scenario 2: the wake driver resumes a due timer ---

    println!();
    println!("=== Scenario 2: a five-minute timer, woken by the driver ===");
    let first = app
        .desk
        .schedule("remind-alice", "alice", "  stand-up at 09:05 ")
        .await;
    let GraphExecutionResult::Interrupted {
        suspended_node,
        wait: Some(wait),
        ..
    } = &first
    else {
        fail("expected the run to wait on its timer");
    };
    let waiting = latest(&app.store, "remind-alice").await;
    let due_in = wait
        .wake_at
        .as_deref()
        .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
        .zip(DateTime::parse_from_rfc3339(&waiting.created_at).ok())
        .map(|(due, set)| (due - set).num_seconds());
    println!(
        "  run_with returned Interrupted at '{suspended_node}': a {:?} due {} min after it was set",
        wait.kind,
        due_in.unwrap_or_default() / 60
    );
    check(
        wait.kind == WaitKind::Timer && due_in.is_some_and(|s| (299..=301).contains(&s)),
        "expected a timer due five minutes after it was set",
    );

    // The driver's rounds come and go; nothing is due yet.
    clock.set(start + chrono::Duration::minutes(4));
    pass(30).await;
    check(
        app.outbox.sent().is_empty()
            && latest(&app.store, "remind-alice").await.status == CheckpointStatus::Interrupted,
        "expected nothing sent before the timer is due",
    );
    println!("  4 minutes on the host's clock, 30 s of rounds: still waiting, nothing sent");

    clock.set(start + chrono::Duration::minutes(6));
    until("the wake driver to resume remind-alice", || async {
        latest(&app.store, "remind-alice").await.status == CheckpointStatus::Completed
    })
    .await;
    let log = statuses(&app.store, "remind-alice").await;
    println!(
        "  Past 5 minutes: the driver woke it. Checkpoint log: {}",
        log.iter()
            .map(|s| format!("{s:?}"))
            .collect::<Vec<_>>()
            .join(" -> ")
    );
    println!("  Sent: {}", app.outbox.sent().join(", "));
    check(
        app.outbox.sent() == ["alice: stand-up at 09:05"],
        "expected one reminder sent",
    );
    // Running before Schedule and after it, Interrupted at the timer, then the
    // driver's resume claims the thread (Running) and the run completes.
    check(
        log == [
            CheckpointStatus::Running,
            CheckpointStatus::Running,
            CheckpointStatus::Interrupted,
            CheckpointStatus::Running,
            CheckpointStatus::Completed,
        ],
        "expected the log Running -> Running -> Interrupted -> Running -> Completed",
    );

    // --- Scenario 3: stopping lets the run in flight finish ---

    println!();
    println!("=== Scenario 3: stopping the host with a woken run in flight ===");
    app.desk
        .schedule("remind-carol", "carol", "deploy window")
        .await;
    app.desk
        .schedule("remind-dave", "dave", "deploy window")
        .await;
    app.outbox.hold();
    clock.set(start + chrono::Duration::minutes(12));
    until("a woken run to reach Send", || async {
        app.outbox.started() == 1
    })
    .await;
    println!("  Both are due; the driver woke one, and its Send is held open.");

    DriverLog::note("-- shutdown token cancelled --");
    let App {
        shutdown,
        mut running,
        store,
        outbox,
        ..
    } = app;
    shutdown.cancel();
    // The run is parked on the Outbox's gate, so nothing but our release can let the host finish.
    let stopped_early = tokio::time::timeout(Duration::from_secs(60), &mut running)
        .await
        .is_ok();
    println!(
        "  Shutdown token cancelled. Host stopped within a minute? {stopped_early} -- it waits for the run."
    );
    check(
        !stopped_early,
        "expected the host to wait for the run in flight",
    );

    outbox.release();
    let stopped = tokio::time::timeout(Duration::from_secs(600), running)
        .await
        .expect("the host stops once the run in flight has finished")
        .expect("the host task doesn't panic");
    check(stopped == Ok(()), "expected Host::run to return Ok");
    println!("  Released the send. Host::run returned Ok(()):");
    let mut all: Vec<Checkpoint> = Vec::new();
    let mut finished = 0;
    let mut waiting = 0;
    for id in ["remind-carol", "remind-dave"] {
        let latest = latest(&store, id).await;
        let note = if latest.status == CheckpointStatus::Interrupted {
            waiting += 1;
            " (still waiting on its timer)"
        } else {
            finished += usize::from(latest.status == CheckpointStatus::Completed);
            ""
        };
        println!("    {id}: {:?}{note}", latest.status);
        all.extend(log_of(&store, id).await);
    }
    let cancelled = all
        .iter()
        .filter(|c| c.status == CheckpointStatus::Cancelled)
        .count();
    println!(
        "    sends started: {}, cancelled checkpoints: {cancelled}",
        outbox.started()
    );
    check(
        outbox.started() == 1 && outbox.sent().len() == 2,
        "expected exactly one woken run, and it finished",
    );
    check(
        finished == 1 && waiting == 1,
        "expected one thread finished and the other still waiting",
    );
    check(cancelled == 0, "expected nothing cancelled");

    println!("  The host's log (datalium_strata_host's tracing events):");
    let lines = DriverLog::take();
    for line in &lines {
        println!("    {line}");
    }
    let marker = lines
        .iter()
        .position(|l| l.starts_with("-- "))
        .unwrap_or_default();
    check(
        lines[marker + 1..]
            .iter()
            .any(|l| l.contains("handled 1 thread(s)")),
        "expected the in-flight run's round to finish after the token fired",
    );
    drop((store, outbox));

    // The thread left behind is on disk. A new host on the same file wakes it in its first round.
    println!("  A second host on the same file:");
    let app = App::start(
        &settings,
        &clock,
        reminder_graph(false).expect("the graph is valid"),
    );
    until("the second host to wake the left-over thread", || async {
        latest(&app.store, "remind-carol").await.status == CheckpointStatus::Completed
            && latest(&app.store, "remind-dave").await.status == CheckpointStatus::Completed
    })
    .await;
    println!(
        "    woke the one left behind and sent it: {}",
        app.outbox.sent().join(", ")
    );
    check(
        app.outbox.sent().len() == 1,
        "expected the second host to send only the one left behind",
    );
    DriverLog::take();

    // --- Scenario 4: the store breaks under the running host ---

    println!();
    println!("=== Scenario 4: the store breaks under the running host ===");
    app.store.set_down(true);
    let (store_health, _) = readiness(&app).await;
    println!("  check_store: {store_health}");
    check(
        store_health.starts_with("Unhealthy"),
        "expected the store probe to fail",
    );

    until("a driver round to fail", || async {
        !app.status.failing().is_empty()
    })
    .await;
    let (_, drivers) = readiness(&app).await;
    println!("  Host::status: {drivers}");
    println!(
        "  The host is still running: {}",
        !app.running.is_finished()
    );
    check(
        drivers.starts_with("Degraded") && !app.running.is_finished(),
        "expected a failing driver, and the host still running",
    );

    // The driver now sits in its back-off wait (twice the interval). Stopping
    // ends a wait at once: there is no run to let finish.
    let App {
        shutdown, running, ..
    } = app;
    shutdown.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(1), running).await;
    check(
        matches!(stopped, Ok(Ok(Ok(())))),
        "expected the host to stop at once from its back-off wait",
    );
    println!("  Stopped from the driver's back-off wait, at once. The host's log:");
    for line in DriverLog::take() {
        println!("    {line}");
    }

    // --- Scenario 5: start-up refusals ---

    println!();
    println!("=== Scenario 5: start-up refusals ===");
    match reminder_graph(true) {
        Err(StrataError::GraphInvalid(result)) => {
            for error in &result.errors {
                println!(
                    "  compile_with refused the graph: [{}] {}",
                    error.code, error.message
                );
            }
            check(
                result.errors.iter().any(|e| e.code == "STRATA_ERR_ORPHAN"),
                "expected an orphan node error",
            );
        }
        Ok(_) => fail("expected the graph with an orphan node to be refused"),
        Err(other) => fail(&format!("expected GraphInvalid, got {other:?}")),
    }
    println!(
        "  No CompiledGraph, so nothing to hand Host::wake: the graph compiles at start-up, not at its first run."
    );

    let mut broken: StrataSettings =
        serde_json::from_str(r#"{ "wake": { "batch_size": 0 } }"#).expect("the settings parse");
    broken.sqlite.path = Some(db_path.display().to_string());
    let store = Arc::new(Switchable::new(
        broken.sqlite.open().expect("the file opens"),
    ));
    let host = Host::new().wake(
        Arc::new(reminder_graph(false).expect("the graph is valid")),
        store,
        broken.wake.clone(),
    );
    let status = host.status();
    match host.run(CancellationToken::new()).await {
        Err(HostError(refusal)) => {
            println!("  Host::run with wake.batch_size = 0 refused: {refusal}");
            check(
                refusal == "the wake driver's batch_size must be between 1 and 1000",
                "expected the batch size refused",
            );
        }
        Ok(()) => fail("expected the host to refuse a batch size of 0"),
    }
    println!(
        "  Drivers started: {}. Host::run checks every driver before it starts any.",
        status.drivers().len()
    );
    check(status.drivers().is_empty(), "expected no driver to start");

    println!();
    println!(
        "=== Done: settings, a store, a wake loop, a readiness probe and a clean stop, without a container. ==="
    );
}

/// What the application builds at start-up: the store, the outbox, the runner
/// (`ReminderDesk`) and the host, running on a task until `shutdown` fires.
struct App {
    store: Arc<Switchable>,
    outbox: Arc<Outbox>,
    desk: ReminderDesk,
    status: DriverStatus,
    shutdown: CancellationToken,
    running: JoinHandle<Result<(), HostError>>,
}

impl App {
    fn start(settings: &StrataSettings, clock: &DemoClock, graph: CompiledGraph<Reminder>) -> App {
        let store = Arc::new(Switchable::new(
            settings.sqlite.open().expect("the SQLite file opens"),
        ));
        let outbox = Arc::new(Outbox::default());
        // Every run's defaults: the lease from the settings, and the services
        // the nodes read (`NodeContext::service`).
        let config = settings.runs.config().with_service(outbox.clone());
        let graph = Arc::new(graph);
        let host = Host::new().with_clock(clock.clock()).wake(
            graph.clone(),
            store.clone(),
            settings.wake.clone().with_config(config.clone()),
        );
        let status = host.status();
        let shutdown = CancellationToken::new();
        let running = tokio::spawn(host.run(shutdown.clone()));
        App {
            desk: ReminderDesk {
                graph,
                store: store.clone(),
                config,
            },
            store,
            outbox,
            status,
            shutdown,
            running,
        }
    }
}

/// The application's own service: it holds the graph, the store and the run
/// defaults, as any worker or endpoint would. (.NET injects a
/// `StrataRunner<Reminder>` for this.)
struct ReminderDesk {
    graph: Arc<CompiledGraph<Reminder>>,
    store: Arc<Switchable>,
    config: RunConfig,
}

impl ReminderDesk {
    async fn schedule(
        &self,
        thread_id: &str,
        to: &str,
        text: &str,
    ) -> GraphExecutionResult<Reminder> {
        let reminder = Reminder {
            to: to.to_string(),
            text: text.to_string(),
            sent: false,
        };
        self.graph
            .run_with(thread_id, reminder, self.store.clone(), self.config.clone())
            .await
    }
}

/// `strata.json`, read with serde, with the SQLite file set in code (a temp
/// path): the layering a config crate would do from several sources.
fn settings(db_path: &Path) -> StrataSettings {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("strata.json");
    let text = std::fs::read_to_string(&path).expect("strata.json is next to Cargo.toml");
    let mut settings: StrataSettings = serde_json::from_str(&text).expect("strata.json parses");
    settings.sqlite.path = Some(db_path.display().to_string());
    settings
}

// Schedule -> Wait (five minutes) -> Send. Send reaches the Outbox through the
// run config's services (`NodeContext::service`).
fn reminder_graph(with_orphan: bool) -> Result<CompiledGraph<Reminder>, StrataError> {
    let mut graph = StateGraph::<Reminder>::new()
        .add_node("Schedule", |r: Reminder| Reminder {
            text: r.text.trim().to_string(),
            ..r
        })
        .add_timer("Wait", Duration::from_secs(5 * 60))
        .add_async_node("Send", |r: Reminder, ctx: NodeContext| async move {
            let outbox = ctx
                .service::<Outbox>()
                .expect("the run config carries the outbox");
            outbox.send(&r).await;
            Reminder { sent: true, ..r }
        })
        .add_edge("Schedule", "Wait")
        .add_edge("Wait", "Send");

    // A node someone added and forgot to wire in.
    if with_orphan {
        graph = graph.add_node("Escalate", |r: Reminder| r);
    }

    // EveryNode: a checkpoint after each node and a completed one at the end,
    // so the store shows what the driver did. The default (Boundaries) writes
    // nothing when a run completes.
    graph.compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
}

/// What a readiness endpoint would answer: whether the store answers
/// (`check_store`), and how the drivers stand (`Host::status`): degraded, not
/// down, while a driver's rounds fail, since the host goes on.
async fn readiness(app: &App) -> (String, String) {
    let store = match check_store(&*app.store, PROBE_TIMEOUT).await {
        StoreHealth::Healthy { .. } => "Healthy".to_string(),
        StoreHealth::Unhealthy { error } => format!("Unhealthy ({error})"),
    };
    let drivers = app.status.drivers();
    let failing = app.status.failing();
    let describe = |d: &datalium_strata_host::DriverState| match &d.last_failure {
        Some(kind) => format!(
            "{}, {} failure(s) in a row, last: {kind}",
            d.name, d.failures_in_a_row
        ),
        None => format!("{}, {} failures in a row", d.name, d.failures_in_a_row),
    };
    let drivers = if failing.is_empty() {
        format!(
            "Healthy ({})",
            drivers.iter().map(describe).collect::<Vec<_>>().join("; ")
        )
    } else {
        format!(
            "Degraded ({})",
            failing.iter().map(describe).collect::<Vec<_>>().join("; ")
        )
    };
    (store, drivers)
}

async fn latest(store: &Switchable, thread_id: &str) -> Checkpoint {
    store
        .load(thread_id)
        .await
        .expect("the store answers")
        .unwrap_or_else(|| fail(&format!("no checkpoint for {thread_id}")))
}

async fn log_of(store: &Switchable, thread_id: &str) -> Vec<Checkpoint> {
    store
        .get_log(thread_id, None, 100)
        .await
        .expect("the store answers")
        .entries
        .into_iter()
        .map(|entry| entry.checkpoint)
        .collect()
}

async fn statuses(store: &Switchable, thread_id: &str) -> Vec<CheckpointStatus> {
    log_of(store, thread_id)
        .await
        .iter()
        .map(|c| c.status)
        .collect()
}

/// Lets `seconds` of tokio's paused time pass, a second at a time.
async fn pass(seconds: u64) {
    for _ in 0..seconds {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Waits, in tokio's paused time, for something a background driver does. The
/// cap only stops a broken demo from hanging; nothing asserts how long it took.
async fn until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..2000 {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    fail(&format!("gave up waiting for {what}"));
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
