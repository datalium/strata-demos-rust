//! The demo's plumbing: a store with an outage switch, a clock the demo sets,
//! the outbox the `Send` node writes to, and a minimal `tracing` subscriber
//! that keeps the host's own log lines.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use datalium_strata::BoxFuture;
use datalium_strata::schema::Checkpoint;
use datalium_strata::store::{
    CheckpointLogPage, CheckpointQuery, CheckpointStore, CheckpointStoreError, LatestCheckpoint,
    LeaseAttempt, LeasedSave, ThreadLease, ThreadPage, ThreadQuery,
};
use datalium_strata_memory::Clock;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use tokio::sync::Notify;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

use crate::Reminder;

// --- A store with an outage switch -------------------------------------------

/// The SQLite store behind a switch: while it is down every call fails, as a
/// database that went away would. Every call is forwarded otherwise, the
/// lease calls included, so leased runs work through it.
pub struct Switchable {
    inner: SqliteCheckpointStore,
    down: AtomicBool,
}

impl Switchable {
    pub fn new(inner: SqliteCheckpointStore) -> Self {
        Self {
            inner,
            down: AtomicBool::new(false),
        }
    }

    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
    }

    fn up(&self) -> Result<(), CheckpointStoreError> {
        if self.down.load(Ordering::SeqCst) {
            Err(CheckpointStoreError(
                "the database is unreachable (the demo's simulated outage)".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

impl CheckpointStore for Switchable {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner.save(checkpoint).await
        })
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner.load(thread_id).await
        })
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner.load_latest(thread_id).await
        })
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner
                .save_if_latest(checkpoint, expected_sequence)
                .await
        })
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner.try_acquire_lease(thread_id, owner, ttl).await
        })
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner.renew_lease(lease, ttl).await
        })
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner.release_lease(lease).await
        })
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner.get_lease(thread_id).await
        })
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner
                .save_under_lease(checkpoint, lease, expected_sequence)
                .await
        })
    }
}

impl CheckpointQuery for Switchable {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner.list_threads(query).await
        })
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        Box::pin(async move {
            self.up()?;
            self.inner.get_log(thread_id, after_sequence, limit).await
        })
    }
}

// --- A clock the demo sets ---------------------------------------------------

/// What the host calls "now" (`Host::with_clock`): what is due is decided on
/// it. The waits between a driver's rounds are tokio's, paused in this demo.
#[derive(Clone)]
pub struct DemoClock(Arc<Mutex<DateTime<Utc>>>);

impl DemoClock {
    pub fn at(now: DateTime<Utc>) -> Self {
        Self(Arc::new(Mutex::new(now)))
    }

    pub fn set(&self, now: DateTime<Utc>) {
        *self.0.lock().unwrap() = now;
    }

    pub fn clock(&self) -> Clock {
        let now = self.0.clone();
        Arc::new(move || *now.lock().unwrap())
    }
}

// --- Where reminders go ------------------------------------------------------

/// Where reminders go. [`Outbox::hold`] makes the next send wait until
/// [`Outbox::release`].
#[derive(Default)]
pub struct Outbox {
    sent: Mutex<Vec<String>>,
    started: AtomicUsize,
    gate: Mutex<Option<Arc<Notify>>>,
}

impl Outbox {
    pub fn sent(&self) -> Vec<String> {
        self.sent.lock().unwrap().clone()
    }

    pub fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    pub fn hold(&self) {
        *self.gate.lock().unwrap() = Some(Arc::new(Notify::new()));
        self.started.store(0, Ordering::SeqCst);
    }

    pub fn release(&self) {
        if let Some(gate) = self.gate.lock().unwrap().as_ref() {
            gate.notify_one();
        }
    }

    pub async fn send(&self, reminder: &Reminder) {
        self.started.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        self.sent
            .lock()
            .unwrap()
            .push(format!("{}: {}", reminder.to, reminder.text));
    }
}

// --- The host's log lines ----------------------------------------------------

static LINES: LazyLock<Mutex<Vec<String>>> = LazyLock::new(Mutex::default);

/// A minimal `tracing` subscriber, written against the `tracing` crate alone:
/// it keeps the events of `datalium-strata-host` (its drivers' log lines) and
/// ignores everything else. An application would install `tracing-subscriber`'s
/// `fmt` layer or an OpenTelemetry one instead.
pub struct DriverLog {
    next_span: AtomicU64,
}

impl DriverLog {
    pub fn install() {
        tracing::subscriber::set_global_default(DriverLog {
            next_span: AtomicU64::new(1),
        })
        .expect("no other subscriber is installed");
    }

    /// A marker of the demo's own, so the log shows where the main flow was.
    pub fn note(line: &str) {
        LINES.lock().unwrap().push(line.to_string());
    }

    /// The lines kept since the last call.
    pub fn take() -> Vec<String> {
        std::mem::take(&mut *LINES.lock().unwrap())
    }
}

/// An event's message and its string fields (`strata.driver`, `error.type`).
#[derive(Default)]
struct Line {
    message: String,
    fields: Vec<String>,
}

impl Visit for Line {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.push(format!("{}={value}", field.name()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

impl Subscriber for DriverLog {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("datalium_strata_host")
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut line = Line::default();
        event.record(&mut line);
        LINES.lock().unwrap().push(format!(
            "[{}] {} ({})",
            event.metadata().level(),
            line.message,
            line.fields.join(", ")
        ));
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}
