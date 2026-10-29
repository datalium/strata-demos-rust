//! Pluggable storage: one graph, one scenario, four storage backends.
//!
//! `design/03-Architecture.md` promises the persistence layer is pluggable:
//! `CheckpointStore` and `HistoryStore` are traits, and "in-memory → SQLite →
//! Redis/PostgreSQL" are just implementations. Until this demo only SQLite
//! existed, so "pluggable" meant "two SQL-ish things". This demo runs the
//! *same* human-in-the-loop scenario — freeze a wire-transfer graph, drop every
//! connection, resume it from a brand-new one, keep a chat history alongside —
//! against **in-memory, SQLite, Postgres and Redis**, through the same code:
//! nothing in `run_scenario` knows which backend it is talking to.
//!
//! Redis is the interesting one. It has no tables and no `MAX(sequence) + 1`:
//! a thread's checkpoint log and a session's history are each one Redis list.
//! That the traits admit it unchanged is the actual test of "pluggable".
//!
//! After the matrix, one more in-process check on the three durable backends:
//! a durable graph's thread written by a store with delta checkpoints on
//! (`design/41-Checkpoint-Efficiency.md`) and read back, whole, by a store
//! opened without them.
//!
//! ```text
//! docker compose up -d --wait        # Postgres + Redis (this repo's docker-compose.yml)
//! cargo run -p pluggable-storage     # the matrix: all four backends
//! ```
//!
//! **Cross-runtime.** The Postgres and Redis layouts are shared with the .NET
//! stores, so the freeze/resume hand-off of demo 09 works through them too:
//!
//! ```text
//! cargo run -p pluggable-storage -- freeze postgres    # Rust freezes ...
//! dotnet run --project demos/12-pluggable-storage/12-pluggable-storage.csproj -- resume postgres   # (in ../strata-demos-dotnet) ... .NET resumes
//! ```
//!
//! Modes: `[matrix [backend...]]`, `freeze <postgres|redis>`,
//! `resume <postgres|redis>`. Connection URLs come from
//! `STRATA_DEMO_POSTGRES_URL` / `STRATA_DEMO_REDIS_URL` (defaults match the
//! compose file). The .NET twin is `demos/12-pluggable-storage/Program.cs` in
//! `strata-demos-dotnet`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Utc;
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointQuery, CheckpointStore, GraphExecutionResult, HumanSignal,
};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
use datalium_strata::schema::CheckpointStatus;
use datalium_strata_memory::checkpoint::{InMemoryCheckpointStore, SqliteCheckpointStore};
use datalium_strata_memory::group_commit::{CHECKPOINT_SIZE_METRIC, ENCODING_LABEL};
use datalium_strata_memory::history::{InMemoryHistoryStore, SqliteHistoryStore};
use datalium_strata_memory::postgres_store::{PostgresCheckpointStore, PostgresHistoryStore};
use datalium_strata_memory::redis_store::{RedisCheckpointStore, RedisHistoryStore};
use datalium_strata_memory::retry::STORE_LABEL;
use datalium_strata_memory::{Deltas, PostgresStoreOptions, RedisStoreOptions, SqliteStoreOptions};
use serde::{Deserialize, Serialize};

const RUNTIME: &str = "rust";
/// The thread/session the cross-runtime `freeze`/`resume` modes share with the .NET twin.
const SHARED_ID: &str = "wire-transfer-pluggable";
/// The Redis key prefix the cross-runtime modes share with the .NET twin.
const SHARED_REDIS_PREFIX: &str = "strata-demo-12";

/// The in-process deltas check's graph: this many nodes, one checkpoint after each.
const LEDGER_STEPS: usize = 6;

/// Field names are snake_case on the wire, and the .NET twin pins the same
/// names with `[JsonPropertyName]` (see demo 09 for why that matters: the
/// state is user-defined JSON inside the checkpoint).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireTransferState {
    invoice_generated: bool,
    amount: f64,
    currency: String,
    approved_by: Option<String>,
    is_completed: bool,
    frozen_by: Option<String>,
    executed_by: Option<String>,
}

fn initial_state() -> WireTransferState {
    WireTransferState {
        invoice_generated: false,
        amount: 4_200.50,
        currency: "USD".to_string(),
        approved_by: None,
        is_completed: false,
        frozen_by: None,
        executed_by: None,
    }
}

fn wire_transfer_graph() -> CompiledGraph<WireTransferState> {
    StateGraph::<WireTransferState>::new()
        .add_node("PrepareWireTransfer", |s: WireTransferState| {
            WireTransferState {
                invoice_generated: true,
                frozen_by: Some(RUNTIME.to_string()),
                ..s
            }
        })
        .add_interrupt_boundary("PrepareWireTransfer")
        .add_node("ExecuteWireTransfer", |s: WireTransferState| {
            WireTransferState {
                is_completed: true,
                executed_by: Some(RUNTIME.to_string()),
                ..s
            }
        })
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .expect("graph is structurally valid")
}

/// `GraphExecutionResult` has no `Debug`; say which variant came back.
fn describe<S>(result: &GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Interrupted { .. } => "Interrupted".to_string(),
        GraphExecutionResult::Failed(err) => format!("Failed({})", error_text(err)),
        GraphExecutionResult::Cancelled { reason, .. } => format!("Cancelled({reason:?})"),
    }
}

/// `StrataError` has no `Display`. A refusal (`ExecutionAborted`) carries the
/// engine's own words, the same text the .NET twin prints; anything else is
/// shown as the variant. Demo 09 does the same.
fn error_text(err: &StrataError) -> String {
    match err {
        StrataError::ExecutionAborted(message) => message.clone(),
        other => format!("{other:?}"),
    }
}

/// The override is keyed by the *JSON* name (`approved_by`): it is merged into
/// the frozen state's JSON before that is rehydrated (see demo 09). A
/// `field_override` is written as a JSON string, as the .NET twin's
/// `FieldOverrides` are.
fn approval() -> HumanSignal {
    HumanSignal::approve("token-xyz").with_field_override("approved_by", "compliance_officer_07")
}

fn message(role: MessageRole, text: &str) -> ChatMessage {
    ChatMessage {
        role,
        content: Arc::from(text),
        timestamp: Utc::now(),
    }
}

fn check(condition: bool, what: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(format!("check failed: {what}"))
    }
}

// --- The backends -----------------------------------------------------------

/// A checkpoint store and a history store on the same backend. `query` is the
/// checkpoint store again, through its query port (`get_log`).
struct Stores {
    checkpoints: Arc<dyn CheckpointStore>,
    query: Arc<dyn CheckpointQuery>,
    history: Arc<dyn HistoryStore>,
}

impl Stores {
    fn new<C>(checkpoints: C, history: impl HistoryStore + 'static) -> Self
    where
        C: CheckpointStore + CheckpointQuery + 'static,
    {
        let checkpoints = Arc::new(checkpoints);
        Self {
            query: checkpoints.clone(),
            checkpoints,
            history: Arc::new(history),
        }
    }
}

/// Where a backend keeps its data. Everything below this line is generic over
/// it; this is the *only* place that names a concrete store.
#[derive(Clone)]
enum Backend {
    InMemory,
    Sqlite(PathBuf),
    Postgres(String),
    Redis { url: String, prefix: String },
}

impl Backend {
    fn name(&self) -> &'static str {
        match self {
            Backend::InMemory => "in-memory",
            Backend::Sqlite(_) => "sqlite",
            Backend::Postgres(_) => "postgres",
            Backend::Redis { .. } => "redis",
        }
    }

    /// Does what one store instance saved outlive it? (`open` again and see.)
    fn durable(&self) -> bool {
        !matches!(self, Backend::InMemory)
    }

    /// Is more than one writer to the same thread/session supported? SQLite's
    /// adapters assume a single writer; Postgres serializes writers with an
    /// advisory lock; Redis appends with an atomic `RPUSH`.
    fn multi_writer(&self) -> bool {
        matches!(self, Backend::Postgres(_) | Backend::Redis { .. })
    }

    /// A fresh pair of stores with their own connections — what a newly
    /// started process would have. `deltas` turns on delta checkpoints for
    /// the checkpoint store (not the in-memory one, which has none).
    ///
    /// `connect_with` on a Postgres store takes one connection from its pool
    /// and creates or migrates its tables (`strata_schema_version`) before it
    /// returns, so an unreachable or too-new database fails here, at startup,
    /// not at the first save. A Redis store connects and indexes its prefix.
    /// Both retry a transient failure inside every call, this first connect
    /// included (`StoreRetry`'s default: 4 attempts with backoff,
    /// `design/34-Storage-Hardening.md`). Dropping a store closes it.
    async fn open(&self, deltas: Option<Deltas>) -> Result<Stores, String> {
        match self {
            Backend::InMemory => Ok(Stores::new(
                InMemoryCheckpointStore::new(),
                InMemoryHistoryStore::new(),
            )),
            Backend::Sqlite(path) => {
                let mut options = SqliteStoreOptions::new();
                if let Some(deltas) = deltas {
                    options = options.with_deltas(deltas);
                }
                Ok(Stores::new(
                    SqliteCheckpointStore::open_with(path, options).map_err(|e| e.0)?,
                    SqliteHistoryStore::open(path).map_err(|e| e.0)?,
                ))
            }
            Backend::Postgres(url) => {
                let unreachable = |e: String| {
                    format!(
                        "cannot use Postgres at {}: {e}\n  start it with `docker compose up -d --wait` (or set STRATA_DEMO_POSTGRES_URL)",
                        redact(url)
                    )
                };
                let mut options = PostgresStoreOptions::new();
                if let Some(deltas) = deltas {
                    options = options.with_deltas(deltas);
                }
                let checkpoints = PostgresCheckpointStore::connect_with(url, options)
                    .await
                    .map_err(|e| unreachable(e.0))?;
                let history = PostgresHistoryStore::connect(url)
                    .await
                    .map_err(|e| unreachable(e.0))?;
                Ok(Stores::new(checkpoints, history))
            }
            Backend::Redis { url, prefix } => {
                let unreachable = |e: String| {
                    format!(
                        "cannot use Redis at {}: {e}\n  start it with `docker compose up -d --wait` (or set STRATA_DEMO_REDIS_URL)",
                        redact(url)
                    )
                };
                let mut options = RedisStoreOptions::new().with_prefix(prefix);
                if let Some(deltas) = deltas {
                    options = options.with_deltas(deltas);
                }
                let checkpoints = RedisCheckpointStore::connect_with(url, options)
                    .await
                    .map_err(|e| unreachable(e.0))?;
                let history = RedisHistoryStore::connect_with(
                    url,
                    RedisStoreOptions::new().with_prefix(prefix),
                )
                .await
                .map_err(|e| unreachable(e.0))?;
                Ok(Stores::new(checkpoints, history))
            }
        }
    }
}

fn postgres_url() -> String {
    std::env::var("STRATA_DEMO_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://strata:strata@localhost:5432/strata".to_string())
}

fn redis_url() -> String {
    std::env::var("STRATA_DEMO_REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

/// `scheme://user:password@host/...` with the password masked, for error messages.
fn redact(url: &str) -> String {
    match (url.find("://"), url.rfind('@')) {
        (Some(scheme_end), Some(at)) if at > scheme_end => {
            let userinfo = &url[scheme_end + 3..at];
            match userinfo.split_once(':') {
                Some((user, _)) => {
                    format!("{}://{user}:***@{}", &url[..scheme_end], &url[at + 1..])
                }
                None => url.to_string(),
            }
        }
        _ => url.to_string(),
    }
}

// --- The scenario: identical for every backend --------------------------------

struct Row {
    backend: &'static str,
    durable: bool,
    history: usize,
    concurrent: String,
}

/// Freeze → "process exit" → resume from a fresh connection → read back. Knows
/// nothing about which backend `backend` is beyond the questions it asks
/// through [`Backend`]'s methods.
async fn run_scenario(backend: &Backend, run_id: &str) -> Result<Row, String> {
    let id = format!("wire-{run_id}-{}", backend.name());

    // Process 1: run to the interrupt boundary and chat a little.
    let first = backend.open(None).await?;
    match wire_transfer_graph()
        .run(&id, initial_state(), first.checkpoints.clone())
        .await
    {
        GraphExecutionResult::Interrupted { .. } => {}
        other => return Err(format!("expected Interrupted, got {}", describe(&other))),
    }
    first
        .history
        .append(
            &id,
            message(MessageRole::User, "Wire $4,200.50 to Acme Corp"),
        )
        .await
        .map_err(|e| e.0)?;
    first
        .history
        .append(
            &id,
            message(
                MessageRole::Assistant,
                "Prepared the transfer; waiting for approval.",
            ),
        )
        .await
        .map_err(|e| e.0)?;

    // "The process exits." A durable backend hands us nothing but its address;
    // the in-memory one takes its data with it.
    let second = if backend.durable() {
        drop(first);
        backend.open(None).await?
    } else {
        let restarted = backend.open(None).await?;
        check(
            restarted
                .checkpoints
                .load(&id)
                .await
                .map_err(|e| e.0)?
                .is_none(),
            "a restarted in-memory store should have lost the checkpoint",
        )?;
        first
    };

    // Process 2: resume from the store alone, using a graph built from scratch.
    let state = match wire_transfer_graph()
        .resume(&id, approval(), second.checkpoints.clone())
        .await
    {
        GraphExecutionResult::Completed(state) => state,
        other => {
            return Err(format!(
                "expected Completed on resume, got {}",
                describe(&other)
            ));
        }
    };
    check(
        state.is_completed && state.approved_by.as_deref() == Some("compliance_officer_07"),
        "the resumed state carries the human's approval",
    )?;
    second
        .history
        .append(&id, message(MessageRole::Assistant, "Transfer executed."))
        .await
        .map_err(|e| e.0)?;

    // Process 3: read everything back through yet another connection.
    let third = if backend.durable() {
        drop(second);
        backend.open(None).await?
    } else {
        second
    };
    let latest = third
        .checkpoints
        .load(&id)
        .await
        .map_err(|e| e.0)?
        .ok_or("no checkpoint after resume")?;
    check(
        latest.resumed_at.is_some() && latest.status == CheckpointStatus::Interrupted,
        "the latest checkpoint is the resumed one",
    )?;
    let history = third.history.get_history(&id, 50).await.map_err(|e| e.0)?;
    let roles: Vec<MessageRole> = history.iter().map(|m| m.role).collect();
    check(
        roles
            == [
                MessageRole::User,
                MessageRole::Assistant,
                MessageRole::Assistant,
            ]
            && history[2].content.as_ref() == "Transfer executed.",
        "history comes back complete and in order",
    )?;
    // A resumed thread cannot be resumed twice, whichever backend holds it.
    check(
        !matches!(
            wire_transfer_graph()
                .resume(&id, approval(), third.checkpoints.clone())
                .await,
            GraphExecutionResult::Completed(_)
        ),
        "a second resume is refused",
    )?;

    let concurrent = if backend.multi_writer() {
        let appended = concurrent_appends(backend, &format!("{id}-concurrent")).await?;
        format!("{appended} appends, none lost")
    } else if backend.durable() {
        "single writer assumed".to_string()
    } else {
        "in-process only".to_string()
    };

    Ok(Row {
        backend: backend.name(),
        durable: backend.durable(),
        history: history.len(),
        concurrent,
    })
}

/// Four connections append to the same session at once. Postgres serializes
/// them with a per-session advisory lock and Redis with an atomic `RPUSH`;
/// either way nothing is lost and each writer's messages stay in its own order.
async fn concurrent_appends(backend: &Backend, session: &str) -> Result<usize, String> {
    const WRITERS: usize = 4;
    const EACH: usize = 10;
    let mut stores = Vec::with_capacity(WRITERS);
    for _ in 0..WRITERS {
        stores.push(backend.open(None).await?);
    }

    // One tokio task per writer, on the multi-threaded runtime: they really
    // do run at once.
    let writers: Vec<_> = stores
        .iter()
        .enumerate()
        .map(|(writer, stores)| {
            let history = stores.history.clone();
            let session = session.to_string();
            tokio::spawn(async move {
                for n in 0..EACH {
                    history
                        .append(
                            &session,
                            message(MessageRole::User, &format!("{writer}:{n}")),
                        )
                        .await
                        .map_err(|e| e.0)?;
                }
                Ok::<(), String>(())
            })
        })
        .collect();
    for writer in writers {
        writer
            .await
            .map_err(|_| "a writer panicked".to_string())??;
    }

    let history = stores[0]
        .history
        .get_history(session, WRITERS * EACH + 10)
        .await
        .map_err(|e| e.0)?;
    check(
        history.len() == WRITERS * EACH,
        "no concurrent append was lost",
    )?;
    for writer in 0..WRITERS {
        let mine: Vec<usize> = history
            .iter()
            .filter_map(|m| m.content.strip_prefix(&format!("{writer}:")))
            .map(|n| n.parse().unwrap_or(usize::MAX))
            .collect();
        check(
            mine == (0..EACH).collect::<Vec<_>>(),
            "each writer's messages stay in order",
        )?;
    }
    Ok(history.len())
}

// --- Deltas: one durable thread, written with deltas, read back without ------

/// About 2 KB that no node changes: what a delta doesn't have to store again.
fn default_terms() -> String {
    vec!["Payment is due within thirty days of the invoice date."; 38].join(" ")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerState {
    terms: String,
    entries: Vec<String>,
}

/// Six nodes, each adding one entry to a state that also carries 2 KB of
/// terms that never change. Compiled with `CheckpointPolicy::EveryNode`
/// (`design/33-Durable-Execution.md`), so the run writes a `running`
/// checkpoint before its first node and after every node that leads on, and
/// a `completed` one after the last: seven rows. The first is whole (the
/// store has nothing to diff against), the next five are deltas, and
/// `completed` is whole (only `running` checkpoints are stored as deltas).
fn ledger_graph() -> CompiledGraph<LedgerState> {
    let mut graph = StateGraph::<LedgerState>::new();
    for step in 1..=LEDGER_STEPS {
        let entry = format!("entry {step}");
        graph = graph.add_node(format!("Step{step}"), move |mut s: LedgerState| {
            s.entries.push(entry.clone());
            s
        });
        if step > 1 {
            graph = graph.add_edge(format!("Step{}", step - 1), format!("Step{step}"));
        }
    }
    graph
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .expect("graph is structurally valid")
}

struct DeltaRow {
    backend: &'static str,
    rows: usize,
    whole: usize,
    deltas: usize,
    whole_bytes: f64,
    delta_bytes: f64,
}

/// A store with deltas on keeps a `running` checkpoint as the keys that
/// changed since the thread's previous row (checkpoint `1.8.0` in the table or
/// list). Every store decodes such rows whatever its own options, so a reader
/// opened without deltas gets each checkpoint back whole. The writer's
/// encodings are counted from the stores' own telemetry
/// (`strata.store.checkpoint.size`), which names no backend's internals.
async fn run_deltas(backend: &Backend, run_id: &str) -> Result<DeltaRow, String> {
    let id = format!("ledger-{run_id}-{}", backend.name());
    let terms = default_terms();
    let since = SIZES.mark();

    {
        let writer = backend.open(Some(Deltas::new())).await?;
        let result = ledger_graph()
            .run(
                &id,
                LedgerState {
                    terms: terms.clone(),
                    entries: Vec::new(),
                },
                writer.checkpoints.clone(),
            )
            .await;
        check(
            matches!(result, GraphExecutionResult::Completed(_)),
            &format!("the ledger run completes (got {})", describe(&result)),
        )?;
    }

    let reader = backend.open(None).await?;
    let log = reader
        .query
        .get_log(&id, None, 50)
        .await
        .map_err(|e| e.0)?
        .entries;
    let latest = reader
        .checkpoints
        .load(&id)
        .await
        .map_err(|e| e.0)?
        .ok_or("no checkpoint for the ledger")?;

    // Each row comes back whole: the unchanged terms, the entries written so
    // far, and the version the engine wrote (1.7.0 for `running`; 1.0.0 for a
    // plain `completed`) rather than the 1.8.0 the store kept.
    let rows_whole = log.iter().enumerate().all(|(i, entry)| {
        let checkpoint = &entry.checkpoint;
        let expected_version = if checkpoint.status == CheckpointStatus::Running {
            "1.7.0"
        } else {
            "1.0.0"
        };
        checkpoint.schema_version == expected_version
            && checkpoint.frozen_state["terms"].as_str() == Some(terms.as_str())
            && checkpoint.frozen_state["entries"].as_array().map(Vec::len)
                == Some(i.min(LEDGER_STEPS))
    });
    check(
        log.len() == LEDGER_STEPS + 1,
        &format!(
            "expected {} checkpoints, got {}",
            LEDGER_STEPS + 1,
            log.len()
        ),
    )?;
    check(
        rows_whole,
        "a store without deltas reads every row back whole",
    )?;
    check(
        latest.status == CheckpointStatus::Completed,
        "the latest checkpoint is the completed one",
    )?;
    let (deltas, delta_bytes) = SIZES.since(since, backend.name(), "delta");
    let (whole, whole_bytes) = SIZES.since(since, backend.name(), "plain");
    check(
        deltas == LEDGER_STEPS - 1 && whole == 2,
        &format!(
            "expected {} deltas and 2 whole rows, saw {deltas} and {whole}",
            LEDGER_STEPS - 1
        ),
    )?;
    check(
        delta_bytes * 4.0 < whole_bytes,
        "a delta is much smaller than a whole row",
    )?;

    Ok(DeltaRow {
        backend: backend.name(),
        rows: log.len(),
        whole,
        deltas,
        whole_bytes,
        delta_bytes,
    })
}

/// What the checkpoint stores report through `strata.store.checkpoint.size`:
/// the bytes of each row written, by store and encoding (`plain`, `delta`,
/// ...). A minimal `metrics` recorder, installed globally so it also sees the
/// SQLite store, which records on tokio's blocking pool. Every other metric is
/// dropped.
struct CheckpointSizes {
    rows: Mutex<Vec<(String, String, f64)>>,
}

static SIZES: CheckpointSizes = CheckpointSizes {
    rows: Mutex::new(Vec::new()),
};

impl CheckpointSizes {
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(String, String, f64)>> {
        self.rows.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Where the rows recorded from now on start.
    fn mark(&self) -> usize {
        self.lock().len()
    }

    /// How many rows `store` wrote as `encoding` since `mark`, and their
    /// average size in bytes.
    fn since(&self, mark: usize, store: &str, encoding: &str) -> (usize, f64) {
        let rows = self.lock();
        let sizes: Vec<f64> = rows[mark..]
            .iter()
            .filter(|(s, e, _)| s == store && e == encoding)
            .map(|(_, _, bytes)| *bytes)
            .collect();
        let average = if sizes.is_empty() {
            0.0
        } else {
            sizes.iter().sum::<f64>() / sizes.len() as f64
        };
        (sizes.len(), average)
    }
}

struct SizeHistogram {
    store: String,
    encoding: String,
}

impl metrics::HistogramFn for SizeHistogram {
    fn record(&self, bytes: f64) {
        SIZES
            .lock()
            .push((self.store.clone(), self.encoding.clone(), bytes));
    }
}

impl metrics::Recorder for CheckpointSizes {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        key: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        if key.name() != CHECKPOINT_SIZE_METRIC {
            return metrics::Histogram::noop();
        }
        let label = |name: &str| {
            key.labels()
                .find(|label| label.key() == name)
                .map(|label| label.value().to_string())
        };
        match (label(STORE_LABEL), label(ENCODING_LABEL)) {
            (Some(store), Some(encoding)) => {
                metrics::Histogram::from_arc(Arc::new(SizeHistogram { store, encoding }))
            }
            _ => metrics::Histogram::noop(),
        }
    }
}

// --- Modes -----------------------------------------------------------------

fn parse_backend(name: &str, run_id: &str) -> Result<Backend, String> {
    match name {
        "memory" | "in-memory" => Ok(Backend::InMemory),
        "sqlite" => Ok(Backend::Sqlite(
            std::env::temp_dir().join(format!("strata-demo-12-{run_id}.sqlite")),
        )),
        "postgres" => Ok(Backend::Postgres(postgres_url())),
        "redis" => Ok(Backend::Redis {
            url: redis_url(),
            prefix: format!("strata-demo-12-{run_id}"),
        }),
        other => Err(format!(
            "unknown backend '{other}' (memory, sqlite, postgres, redis)"
        )),
    }
}

/// Deletes a SQLite backend's file and its WAL side files, if any.
fn remove_sqlite(backend: &Backend) {
    if let Backend::Sqlite(path) = backend {
        for suffix in ["", "-wal", "-shm"] {
            let mut file = path.clone().into_os_string();
            file.push(suffix);
            let _ = std::fs::remove_file(file);
        }
    }
}

async fn matrix(names: &[String]) -> Result<(), String> {
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_millis()
        .to_string();
    let names: Vec<String> = if names.is_empty() {
        ["memory", "sqlite", "postgres", "redis"]
            .map(String::from)
            .to_vec()
    } else {
        names.to_vec()
    };
    let backends = names
        .iter()
        .map(|name| parse_backend(name, &run_id))
        .collect::<Result<Vec<_>, _>>()?;

    println!("=== one scenario, {} storage backends ===", backends.len());
    println!(
        "  freeze a wire-transfer graph -> drop every connection -> resume from a fresh one -> read history back\n"
    );

    let mut rows = Vec::new();
    for backend in &backends {
        print!("  {:<10} ", backend.name());
        let outcome = run_scenario(backend, &run_id).await;
        remove_sqlite(backend);
        match outcome {
            Ok(row) => {
                println!("ok");
                rows.push(row);
            }
            Err(e) => {
                println!("FAILED");
                return Err(format!("{}: {e}", backend.name()));
            }
        }
    }

    println!();
    println!(
        "  {:<10} {:<22} {:<8} concurrent writers",
        "backend", "survives restart", "history"
    );
    for row in &rows {
        println!(
            "  {:<10} {:<22} {:<8} {}",
            row.backend,
            if row.durable {
                "yes"
            } else {
                "no (lost, as expected)"
            },
            row.history,
            row.concurrent
        );
    }
    println!();
    println!("  Same graph, same assertions, same code path for all of them.");

    let durable: Vec<&Backend> = backends.iter().filter(|b| b.durable()).collect();
    if durable.is_empty() {
        return Ok(());
    }

    println!();
    println!("=== delta checkpoints: written with deltas on, read by a store without them ===");
    println!(
        "  a durable graph ({LEDGER_STEPS} nodes, CheckpointPolicy::EveryNode) runs once on a store with deltas; a fresh store\n  opened without deltas reads the thread's log back\n"
    );
    let mut delta_rows = Vec::new();
    for backend in durable {
        let outcome = run_deltas(backend, &run_id).await;
        remove_sqlite(backend);
        delta_rows.push(outcome.map_err(|e| format!("{} (deltas): {e}", backend.name()))?);
    }

    println!(
        "  {:<10} {:<10} {:<13} {:<10} bytes (whole / delta, average)",
        "backend", "rows read", "stored whole", "as deltas"
    );
    for row in &delta_rows {
        println!(
            "  {:<10} {:<10} {:<13} {:<10} {:.0} / {:.0}",
            row.backend, row.rows, row.whole, row.deltas, row.whole_bytes, row.delta_bytes
        );
    }
    println!();
    println!(
        "  Every row came back whole, in order: the reader needed no options to decode the deltas."
    );
    Ok(())
}

fn shared_backend(name: Option<&str>) -> Result<Backend, String> {
    match name {
        Some("postgres") => Ok(Backend::Postgres(postgres_url())),
        Some("redis") => Ok(Backend::Redis {
            url: redis_url(),
            prefix: SHARED_REDIS_PREFIX.to_string(),
        }),
        Some("sqlite") => Err(
            "the SQLite hand-off is demo 09; this demo shares Postgres and Redis (postgres|redis)"
                .into(),
        ),
        _ => Err("usage: pluggable-storage <freeze|resume> <postgres|redis>".into()),
    }
}

fn print_history(history: &[ChatMessage]) {
    println!("  History (as read from the store):");
    for m in history {
        println!(
            "    {:<10} {}",
            format!("{:?}", m.role).to_lowercase(),
            m.content
        );
    }
}

async fn freeze(backend: &Backend) -> Result<(), String> {
    println!(
        "=== [{RUNTIME}] freeze on {}: run to the interrupt boundary ===",
        backend.name()
    );
    let stores = backend.open(None).await?;
    match wire_transfer_graph()
        .run(SHARED_ID, initial_state(), stores.checkpoints.clone())
        .await
    {
        GraphExecutionResult::Interrupted {
            thread_id,
            suspended_node,
            next_node,
            ..
        } => println!(
            "  Interrupted at '{suspended_node}' (thread '{thread_id}'); next node on resume: '{next_node}'"
        ),
        other => return Err(format!("expected Interrupted, got {}", describe(&other))),
    }
    for m in [
        message(MessageRole::User, "[rust] Wire $4,200.50 to Acme Corp"),
        message(
            MessageRole::Assistant,
            "[rust] Prepared the transfer; waiting for approval.",
        ),
    ] {
        stores.history.append(SHARED_ID, m).await.map_err(|e| e.0)?;
    }
    println!("  Appended 2 history messages.");
    println!();
    println!("  This process is about to exit; the state now lives only in the database.");
    println!("  Next: run the `resume` step -- from the other runtime to see the hand-off.");
    Ok(())
}

async fn resume(backend: &Backend) -> Result<(), String> {
    println!(
        "=== [{RUNTIME}] resume on {}: pick up a thread another process froze ===",
        backend.name()
    );
    let stores = backend.open(None).await?;
    match wire_transfer_graph()
        .resume(SHARED_ID, approval(), stores.checkpoints.clone())
        .await
    {
        GraphExecutionResult::Completed(state) => {
            println!("  Resumed and ran to completion.");
            println!("  Completed state: {state:?}");
            println!();
            println!(
                "  frozen_by = {:?}, executed_by = {:?}",
                state.frozen_by.as_deref().unwrap_or("?"),
                state.executed_by.as_deref().unwrap_or("?")
            );
        }
        GraphExecutionResult::Failed(err) => {
            return Err(format!(
                "resume failed: {} (was this thread already resumed? run `freeze` again)",
                error_text(&err)
            ));
        }
        other => return Err(format!("expected Completed, got {}", describe(&other))),
    }
    stores
        .history
        .append(
            SHARED_ID,
            message(MessageRole::Assistant, "[rust] Transfer executed."),
        )
        .await
        .map_err(|e| e.0)?;
    println!();
    print_history(
        &stores
            .history
            .get_history(SHARED_ID, 50)
            .await
            .map_err(|e| e.0)?,
    );
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    // Before any store exists, so every size it records is seen.
    if metrics::set_global_recorder(&SIZES).is_err() {
        eprintln!("error: a metrics recorder is already installed");
        return ExitCode::FAILURE;
    }

    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        None | Some("matrix") => matrix(args.get(1..).unwrap_or(&[])).await,
        Some("freeze") => match shared_backend(args.get(1).map(String::as_str)) {
            Ok(backend) => freeze(&backend).await,
            Err(e) => Err(e),
        },
        Some("resume") => match shared_backend(args.get(1).map(String::as_str)) {
            Ok(backend) => resume(&backend).await,
            Err(e) => Err(e),
        },
        Some(_) => {
            eprintln!(
                "usage: pluggable-storage [matrix [memory|sqlite|postgres|redis ...]]\n       pluggable-storage <freeze|resume> <postgres|redis>"
            );
            return ExitCode::from(2);
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}
