//! The run scheduler (design/40-Run-Scheduler.md): admitting runs to a fixed
//! number of slots.
//!
//! A `RunScheduler` is given to a run through `RunConfig::with_scheduler`.
//! Before a run loads or writes anything it waits for a slot; it gives the slot
//! back when `run_with` returns. Four scenarios, one rule each:
//!
//!   1. Strict priority. Higher `RunConfig::with_priority` first; first come,
//!      first served within one.
//!   2. A reserved slot. A slot kept for priority 5 and up: a low-priority run
//!      waits beside a free slot, and an urgent run takes it at once.
//!   3. Tenants. At most one run per tenant (the run's metadata `tenant`). A
//!      tenant at its limit holds back only its own runs, a limit of 0 pauses a
//!      tenant, and `set_tenant_limit` changes a limit while runs wait.
//!   4. A bounded queue. When it is full, an equal-priority newcomer is refused
//!      and a higher-priority one sheds the lowest-ranked waiting run. Both get
//!      `StrataError::NotAdmitted`, and neither leaves a checkpoint or a run
//!      event behind.
//!
//! Every run's one node waits on a gate (a `tokio::sync::Notify`) the demo
//! opens, so which runs hold slots at any moment is decided by the demo, not by
//! timing. The order of admission is read from the order the nodes started.
//! The tokio clock is paused (`start_paused`, tokio's `test-util` feature) and
//! moved only by `tokio::time::advance`, so how long a run queued
//! (`RunStarted.queued_for`, `NotAdmitted.waited`) is an exact figure. No host,
//! no LLM, no network.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventKind, RunObserver};
use datalium_strata::scheduler::{
    NotAdmitted, NotAdmittedReason, RunScheduler, SchedulerOptions, TenantUsage,
};
use datalium_strata::store::{CheckpointQuery, CheckpointStore};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const MINUTE: Duration = Duration::from_secs(60);

// A paused clock on one thread: nothing runs between the demo's steps but the
// tasks it spawned, and time moves only when the demo advances it.
#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    // --- Scenario 1: strict priority, FIFO within a priority ---

    println!("=== Scenario 1: one slot, five runs waiting at three priorities ===");
    let mut lab = Lab::new(SchedulerOptions::new(1).with_name("priority"));
    lab.start_and_wait("blocker", 0, None).await;
    for (id, priority) in [
        ("low-1", 0),
        ("high-1", 5),
        ("low-2", 0),
        ("high-2", 5),
        ("mid-1", 2),
    ] {
        lab.queue(id, priority, None).await;
    }
    println!(
        "  Arrived in this order: low-1 (p0), high-1 (p5), low-2 (p0), high-2 (p5), mid-1 (p2)"
    );
    println!("  {}", lab.describe());
    check(
        counts(&lab.scheduler) == (1, 5),
        "expected one run active and five queued",
    );

    // Each minute of the paused clock, the run holding the slot finishes and the next one starts.
    let mut holder = "blocker".to_string();
    for _ in 1..=5 {
        tokio::time::advance(MINUTE).await;
        holder = lab.finish_and_next(&holder).await;
    }
    lab.finish(&holder).await;
    let order: Vec<String> = lab.start_order().into_iter().skip(1).collect();
    println!("  Started in this order: {}", order.join(", "));
    let mut queued_for = Vec::new();
    for id in &order {
        let waited = lab.queued_for(id).await;
        println!("    {id:<6} RunStarted.queued_for = {waited:?}");
        queued_for.push(waited);
    }

    check(
        order == ["high-1", "high-2", "mid-1", "low-1", "low-2"],
        "expected priority order, FIFO within a priority",
    );
    check(
        lab.queued_for("blocker").await == Some(Duration::ZERO),
        "expected the first run admitted on arrival",
    );
    check(
        queued_for == (1..=5).map(|m| Some(MINUTE * m)).collect::<Vec<_>>(),
        "expected each run to have queued a whole number of minutes, low-2 five",
    );

    // --- Scenario 2: a reserved slot ---

    println!();
    println!("=== Scenario 2: three slots, one kept for priority 5 and up ===");
    let mut lab = Lab::new(
        SchedulerOptions::new(3)
            .with_name("reserved")
            .with_reservation(5, 1),
    );
    lab.start_and_wait("batch-1", 0, None).await;
    lab.start_and_wait("batch-2", 0, None).await;
    lab.queue("batch-3", 0, None).await;
    println!("  batch-1..3 (p0): {}", lab.describe());
    println!("  batch-3 waits beside a free slot: that slot is the reserve.");
    check(
        counts(&lab.scheduler) == (2, 1),
        "expected batch-3 to wait while a slot is free",
    );

    lab.start_and_wait("urgent", 5, None).await;
    println!(
        "  urgent (p5) arrives and starts at once: {}",
        lab.describe()
    );
    check(
        counts(&lab.scheduler) == (3, 1),
        "expected urgent to take the reserved slot",
    );

    lab.finish("urgent").await;
    println!(
        "  urgent finishes; its slot goes back to the reserve: {}",
        lab.describe()
    );
    check(
        counts(&lab.scheduler) == (2, 1),
        "expected batch-3 still to wait: a reservation lends nothing back",
    );

    let next = lab.finish_and_next("batch-1").await;
    println!("  batch-1 finishes; {next} starts: {}", lab.describe());
    check(
        next == "batch-3",
        "expected batch-3 to start once an unreserved slot was free",
    );
    lab.finish("batch-2").await;
    lab.finish("batch-3").await;

    // --- Scenario 3: tenant limits ---

    println!();
    println!("=== Scenario 3: four slots, one run per tenant, initech paused ===");
    let mut lab = Lab::new(
        SchedulerOptions::new(4)
            .with_name("tenants")
            .with_tenant_limit(1)
            .with_tenant_override("initech", 0),
    );
    lab.start_and_wait("acme-1", 0, Some("acme")).await;
    lab.queue("acme-2", 0, Some("acme")).await;
    lab.start_and_wait("globex-1", 0, Some("globex")).await;
    lab.queue("initech-1", 0, Some("initech")).await;
    println!("  Arrived: acme-1, acme-2, globex-1, initech-1");
    println!("  {}", lab.describe());
    let tenants = lab.scheduler.snapshot().tenants;
    let mut names: Vec<&String> = tenants.keys().collect();
    names.sort();
    for tenant in names {
        let usage = tenants[tenant];
        println!(
            "    {tenant:<8} active {}, queued {}, limit {}",
            usage.active,
            usage.queued,
            usage.limit.map_or("none".to_string(), |l| l.to_string())
        );
    }

    check(
        lab.start_order() == ["acme-1", "globex-1"],
        "expected globex-1 to start past acme-2, which waits on its tenant",
    );
    check(
        tenants.get("acme") == Some(&usage(1, 1, 1))
            && tenants.get("globex") == Some(&usage(1, 0, 1))
            && tenants.get("initech") == Some(&usage(0, 1, 0)),
        "expected the per-tenant counts and limits",
    );

    lab.scheduler.set_tenant_limit("initech", Some(1));
    lab.wait_started("initech-1").await;
    println!(
        "  set_tenant_limit(\"initech\", Some(1)): initech-1 starts. {}",
        lab.describe()
    );
    lab.scheduler.set_tenant_limit("acme", Some(2));
    lab.wait_started("acme-2").await;
    println!(
        "  set_tenant_limit(\"acme\", Some(2)): acme-2 starts. {}",
        lab.describe()
    );
    check(
        counts(&lab.scheduler) == (4, 0),
        "expected every run active once the limits were raised",
    );
    for id in ["acme-1", "acme-2", "globex-1", "initech-1"] {
        lab.finish(id).await;
    }

    // --- Scenario 4: a bounded queue that sheds ---

    println!();
    println!("=== Scenario 4: one slot, room for two to wait ===");
    let mut lab = Lab::new(
        SchedulerOptions::new(1)
            .with_name("shedding")
            .with_max_queued(2),
    );
    lab.start_and_wait("blocker", 0, None).await;
    lab.queue("report-a", 0, None).await;
    lab.queue("report-b", 0, None).await;
    println!(
        "  blocker running, report-a and report-b (p0) waiting: {}",
        lab.describe()
    );

    // An equal-priority newcomer outranks nobody: it is refused at once.
    lab.launch("report-c", 0, None);
    let report_c = refusal(lab.result("report-c").await);
    println!("  report-c (p0) arrives: {}", shown(report_c));
    check(
        report_c
            == Some(NotAdmitted {
                reason: NotAdmittedReason::QueueFull,
                waited: Duration::ZERO,
            }),
        "expected report-c refused at once with QueueFull",
    );

    // A higher-priority newcomer sheds the lowest-ranked waiting run: lowest priority, latest arrival.
    tokio::time::advance(Duration::from_secs(30)).await;
    lab.launch("page-oncall", 5, None);
    let report_b = refusal(lab.result("report-b").await);
    println!(
        "  page-oncall (p5) arrives 30 s later: report-b {}",
        shown(report_b)
    );
    println!("  {}", lab.describe());
    check(
        report_b
            == Some(NotAdmitted {
                reason: NotAdmittedReason::Shed,
                waited: Duration::from_secs(30),
            }),
        "expected report-b shed after 30 s in the queue",
    );
    check(
        counts(&lab.scheduler) == (1, 2),
        "expected page-oncall and report-a waiting",
    );

    let next = lab.finish_and_next("blocker").await;
    check(next == "page-oncall", "expected page-oncall to start first");
    let next = lab.finish_and_next("page-oncall").await;
    check(next == "report-a", "expected report-a to start next");
    lab.finish("report-a").await;
    println!(
        "  Started after blocker: {}",
        lab.start_order()
            .into_iter()
            .skip(1)
            .collect::<Vec<_>>()
            .join(", ")
    );

    // What the refused runs left behind: nothing. The ones that ran left checkpoints and events.
    lab.events_delivered(&["blocker", "page-oncall", "report-a"])
        .await;
    let mut left = HashMap::new();
    for id in ["report-a", "report-b", "report-c"] {
        let (checkpoints, events) = (lab.checkpoint_count(id).await, lab.event_count(id));
        println!("    {id}: {checkpoints} checkpoint(s), {events} run event(s)");
        left.insert(id, (checkpoints, events));
    }
    check(
        left["report-a"].0 > 0 && left["report-a"].1 > 0,
        "expected report-a to leave checkpoints and events",
    );
    check(
        left["report-b"] == (0, 0) && left["report-c"] == (0, 0),
        "expected the refused runs to leave nothing behind",
    );

    println!();
    println!(
        "=== Done: priority, a reserve, tenant limits and shedding, each decided before a run touched the store. ==="
    );
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    id: String,
    done: bool,
}

fn counts(scheduler: &RunScheduler) -> (u32, u32) {
    let snapshot = scheduler.snapshot();
    (snapshot.active, snapshot.queued)
}

fn usage(active: u32, queued: u32, limit: u32) -> TenantUsage {
    TenantUsage {
        active,
        queued,
        limit: Some(limit),
    }
}

/// A refused run's `NotAdmitted`: the run returns `Failed(StrataError::NotAdmitted { .. })`.
fn refusal(result: GraphExecutionResult<Job>) -> Option<NotAdmitted> {
    match result {
        GraphExecutionResult::Failed(StrataError::NotAdmitted { reason, waited }) => {
            Some(NotAdmitted { reason, waited })
        }
        _ => None,
    }
}

fn shown(refused: Option<NotAdmitted>) -> String {
    match refused {
        Some(refused) => format!(
            "NotAdmitted({:?}) after {:?} -- \"{refused}\"",
            refused.reason, refused.waited
        ),
        None => "unexpectedly admitted".to_string(),
    }
}

/// Every event of every run the lab starts.
#[derive(Default)]
struct Events(Mutex<Vec<RunEvent>>);

impl RunObserver for Events {
    fn on_event(&self, event: &RunEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

/// One scheduler, one graph and one store for a scenario, plus a gate per run:
/// the run's only node records that it started and then waits until the demo
/// opens its gate.
struct Lab {
    scheduler: RunScheduler,
    graph: Arc<CompiledGraph<Job>>,
    store: Arc<InMemoryCheckpointStore>,
    gates: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    runs: HashMap<String, JoinHandle<GraphExecutionResult<Job>>>,
    started: Arc<Mutex<Vec<String>>>,
    events: Arc<Events>,
}

impl Lab {
    fn new(options: SchedulerOptions) -> Self {
        let scheduler = RunScheduler::new(options).expect("valid scheduler options");
        let gates: Arc<Mutex<HashMap<String, Arc<Notify>>>> = Arc::default();
        let started: Arc<Mutex<Vec<String>>> = Arc::default();
        // EveryNode: a run that gets a slot writes a checkpoint before its node
        // and one when it completes, so "nothing written" for a refused run is a
        // real contrast.
        let graph = {
            let (gates, started) = (gates.clone(), started.clone());
            StateGraph::<Job>::new()
                .add_async_node("Work", move |job: Job, _ctx: NodeContext| {
                    let (gates, started) = (gates.clone(), started.clone());
                    async move {
                        started.lock().unwrap().push(job.id.clone());
                        let gate = gates.lock().unwrap()[&job.id].clone();
                        gate.notified().await;
                        Job { done: true, ..job }
                    }
                })
                .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
                .expect("the graph is valid")
        };
        Self {
            scheduler,
            graph: Arc::new(graph),
            store: Arc::new(InMemoryCheckpointStore::new()),
            gates,
            runs: HashMap::new(),
            started,
            events: Arc::default(),
        }
    }

    /// Spawns the run. It takes its place in the queue when the task first runs.
    fn launch(&mut self, id: &str, priority: i32, tenant: Option<&str>) {
        self.gates
            .lock()
            .unwrap()
            .insert(id.to_string(), Arc::new(Notify::new()));
        let mut config = RunConfig::new()
            .with_scheduler(self.scheduler.clone())
            .with_priority(priority)
            .with_observer(self.events.clone());
        if let Some(tenant) = tenant {
            config = config.with_metadata("tenant", tenant);
        }
        let (graph, thread) = (self.graph.clone(), id.to_string());
        let store: Arc<dyn CheckpointStore> = self.store.clone();
        let job = Job {
            id: id.to_string(),
            done: false,
        };
        self.runs.insert(
            id.to_string(),
            tokio::spawn(async move { graph.run_with(&thread, job, store, config).await }),
        );
    }

    /// Launches a run that should get a slot at once, and waits for its node to start.
    async fn start_and_wait(&mut self, id: &str, priority: i32, tenant: Option<&str>) {
        self.launch(id, priority, tenant);
        self.wait_started(id).await;
    }

    /// Launches a run that should wait, and checks that it does.
    async fn queue(&mut self, id: &str, priority: i32, tenant: Option<&str>) {
        // Waiting for the queue to grow before the next launch makes the arrival order the call order.
        let before = self.scheduler.snapshot().queued;
        self.launch(id, priority, tenant);
        until(&format!("{id} to join the queue"), || {
            self.scheduler.snapshot().queued == before + 1
        })
        .await;
        check(
            !self.start_order().iter().any(|s| s == id),
            &format!("expected {id} to wait for a slot"),
        );
    }

    async fn wait_started(&self, id: &str) {
        until(&format!("{id} to start"), || {
            self.start_order().iter().any(|s| s == id)
        })
        .await;
    }

    /// Opens a running run's gate and waits for it to return `Completed`, slot given back.
    async fn finish(&mut self, id: &str) {
        self.gates.lock().unwrap()[id].notify_one();
        let result = self.result(id).await;
        check(
            matches!(
                result,
                GraphExecutionResult::Completed(Job { done: true, .. })
            ),
            &format!("expected {id} to complete"),
        );
    }

    /// Finishes `id` and returns the run that started in its slot.
    async fn finish_and_next(&mut self, id: &str) -> String {
        let count = self.start_order().len();
        self.finish(id).await;
        until(&format!("a run to start after {id}"), || {
            self.start_order().len() > count
        })
        .await;
        self.start_order()[count].clone()
    }

    async fn result(&mut self, id: &str) -> GraphExecutionResult<Job> {
        let run = self.runs.remove(id).expect("a run launched under that id");
        until(&format!("{id} to return"), || run.is_finished()).await;
        run.await.expect("the run's task didn't panic")
    }

    fn start_order(&self) -> Vec<String> {
        self.started.lock().unwrap().clone()
    }

    /// The run's `RunStarted.queued_for`. Events are delivered by a task of
    /// their own, so this waits for it.
    async fn queued_for(&self, id: &str) -> Option<Duration> {
        let find = || {
            self.events
                .0
                .lock()
                .unwrap()
                .iter()
                .find_map(|e| match &e.kind {
                    RunEventKind::RunStarted { queued_for, .. } if &*e.thread_id == id => {
                        Some(*queued_for)
                    }
                    _ => None,
                })
        };
        until(&format!("{id}'s RunStarted"), || find().is_some()).await;
        find().flatten()
    }

    async fn events_delivered(&self, ids: &[&str]) {
        until("every run's RunCompleted", || {
            let events = self.events.0.lock().unwrap();
            ids.iter().all(|id| {
                events.iter().any(|e| {
                    &*e.thread_id == *id && matches!(e.kind, RunEventKind::RunCompleted { .. })
                })
            })
        })
        .await;
    }

    fn event_count(&self, id: &str) -> usize {
        self.events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| &*e.thread_id == id)
            .count()
    }

    async fn checkpoint_count(&self, id: &str) -> usize {
        self.store
            .get_log(id, None, 100)
            .await
            .expect("the in-memory store reads its log")
            .entries
            .len()
    }

    fn describe(&self) -> String {
        let snapshot = self.scheduler.snapshot();
        format!(
            "active {} of {}, queued {} of {}",
            snapshot.active, snapshot.max_concurrent, snapshot.queued, snapshot.max_queued
        )
    }
}

/// Yields to the runs until `condition` holds. A yield doesn't move the paused
/// clock (a sleep would), and the cap only stops a broken demo from hanging.
async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..100_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    check(false, &format!("gave up waiting for {what}"));
}

fn check(ok: bool, what: &str) {
    if !ok {
        eprintln!("check failed: {what}");
        std::process::exit(1);
    }
}
