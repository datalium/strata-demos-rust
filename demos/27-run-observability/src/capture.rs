//! What an exporter would receive, captured in-process: a `tracing` layer that
//! keeps every span and event (what an OpenTelemetry layer would turn into
//! spans), a `metrics` recorder that keeps every measurement (what a
//! Prometheus or OTLP recorder would export), and the two badly behaved
//! observers of scenario 3. The capture pattern is the one Strata's own
//! `rust/strata-core/tests/telemetry.rs` (in `datalium/strata`) uses.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Condvar, LazyLock, Mutex};
use std::time::Duration;

use datalium_strata::observability::{RunEvent, RunEventKind, RunObserver};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use tokio::sync::Notify;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

// --- Spans -------------------------------------------------------------------

/// One span, as a trace viewer would show it.
#[derive(Debug, Clone)]
pub struct SpanRecord {
    /// The span's own name: `strata.run`, `strata.node`, `strata.attempt`, or a
    /// handler's.
    pub name: &'static str,
    pub fields: HashMap<String, String>,
    /// The index of its parent in [`Captured::spans`].
    pub parent: Option<usize>,
}

impl SpanRecord {
    /// What a trace viewer shows: `otel.name`, or the span's own name.
    pub fn display(&self) -> &str {
        self.fields
            .get("otel.name")
            .map(String::as_str)
            .unwrap_or(self.name)
    }

    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

/// An event recorded inside a span (a span event, to OpenTelemetry).
#[derive(Debug, Clone)]
pub struct EventRecord {
    pub span: Option<usize>,
    pub message: String,
    pub fields: HashMap<String, String>,
}

#[derive(Default)]
pub struct Captured {
    /// Every span, in the order it was created.
    pub spans: Vec<SpanRecord>,
    pub events: Vec<EventRecord>,
    // Span ids are reused once a span closes, so they map to a record only while open.
    open: HashMap<u64, usize>,
}

pub static CAPTURED: LazyLock<Mutex<Captured>> = LazyLock::new(Mutex::default);

struct Fields<'a>(&'a mut HashMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

/// A `tracing` layer that keeps every span with its fields and parent, and
/// every event with the span it was recorded in.
struct Capture;

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        attrs.record(&mut Fields(&mut fields));
        let parent = ctx
            .span(id)
            .and_then(|span| span.parent())
            .map(|parent| parent.id().into_u64());
        let mut captured = CAPTURED.lock().unwrap();
        let parent = parent.and_then(|parent| captured.open.get(&parent).copied());
        let index = captured.spans.len();
        captured.spans.push(SpanRecord {
            name: attrs.metadata().name(),
            fields,
            parent,
        });
        captured.open.insert(id.into_u64(), index);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let mut captured = CAPTURED.lock().unwrap();
        if let Some(&index) = captured.open.get(&id.into_u64()) {
            values.record(&mut Fields(&mut captured.spans[index].fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        event.record(&mut Fields(&mut fields));
        let span = ctx.event_span(event).map(|span| span.id().into_u64());
        let mut captured = CAPTURED.lock().unwrap();
        let span = span.and_then(|span| captured.open.get(&span).copied());
        let message = fields.remove("message").unwrap_or_default();
        captured.events.push(EventRecord {
            span,
            message,
            fields,
        });
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        CAPTURED.lock().unwrap().open.remove(&id.into_u64());
    }
}

static SNAPSHOTTER: LazyLock<Snapshotter> = LazyLock::new(|| {
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture))
        .expect("no other subscriber is installed");
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("no other recorder is installed");
    snapshotter
});

/// Installs the subscriber and the recorder, once for the process: both
/// facades are global, so Strata's spans and metrics arrive with no call to
/// the engine.
pub fn install() {
    LazyLock::force(&SNAPSHOTTER);
}

/// The spans of the trace whose root is the `strata.run` span shown as `root`
/// (`strata.run`, `strata.resume`) for `thread_id`: its index and every span
/// under it, in creation order, with the events recorded in them.
pub struct Trace {
    pub root: usize,
    pub spans: Vec<(usize, SpanRecord)>,
    pub events: Vec<EventRecord>,
}

impl Trace {
    pub fn of(thread_id: &str, root: &str) -> Trace {
        let captured = CAPTURED.lock().unwrap();
        let root = captured
            .spans
            .iter()
            .position(|s| {
                s.name == "strata.run"
                    && s.display() == root
                    && s.field("strata.thread_id") == Some(thread_id)
            })
            .unwrap_or_else(|| panic!("no {root} span for {thread_id}"));
        let under_root = |mut index: usize| loop {
            if index == root {
                return true;
            }
            match captured.spans[index].parent {
                Some(parent) => index = parent,
                None => return false,
            }
        };
        let spans = captured
            .spans
            .iter()
            .enumerate()
            .filter(|(index, _)| under_root(*index))
            .map(|(index, span)| (index, span.clone()))
            .collect();
        let events = captured
            .events
            .iter()
            .filter(|e| e.span.is_some_and(under_root))
            .cloned()
            .collect();
        Trace {
            root,
            spans,
            events,
        }
    }

    /// The spans shown as `display`.
    pub fn all(&self, display: &str) -> Vec<(usize, &SpanRecord)> {
        self.spans
            .iter()
            .filter(|(_, s)| s.display() == display)
            .map(|(index, s)| (*index, s))
            .collect()
    }

    /// The one span shown as `display`.
    pub fn one(&self, display: &str) -> Option<(usize, &SpanRecord)> {
        match self.all(display).as_slice() {
            [one] => Some(*one),
            _ => None,
        }
    }

    pub fn children(&self, parent: usize) -> impl Iterator<Item = &(usize, SpanRecord)> {
        self.spans
            .iter()
            .filter(move |(_, s)| s.parent == Some(parent))
    }

    pub fn events_in(&self, span: usize) -> impl Iterator<Item = &EventRecord> {
        self.events.iter().filter(move |e| e.span == Some(span))
    }
}

// --- Metrics -----------------------------------------------------------------

/// One series the recorder saw since the last snapshot: its name, labels, and
/// what was recorded (a counter's count, a histogram's values).
pub struct Measurement {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub value: DebugValue,
}

impl Measurement {
    pub fn label(&self, key: &str) -> &str {
        self.labels.get(key).map(String::as_str).unwrap_or("")
    }

    pub fn count(&self) -> u64 {
        match &self.value {
            DebugValue::Counter(n) => *n,
            DebugValue::Histogram(values) => values.len() as u64,
            DebugValue::Gauge(_) => 0,
        }
    }
}

/// Everything recorded since the last call: a snapshot drains the recorder, so
/// calling this first starts a clean slate. Series with nothing new are left out.
pub fn measurements() -> Vec<Measurement> {
    SNAPSHOTTER
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(key, _, _, value)| Measurement {
            name: key.key().name().to_string(),
            labels: key
                .key()
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect(),
            value,
        })
        .filter(|m| m.count() > 0)
        .collect()
}

// --- Two badly behaved observers -------------------------------------------

/// The panic message of [`PanicsOnFirstEvent`], which `main`'s panic hook keeps
/// off stderr.
pub const OBSERVER_BUG: &str = "observer bug";

/// An observer with a bug: it panics on the first event it is given, then
/// behaves. (A Rust observer can't throw; a panic is its failure.)
#[derive(Default)]
pub struct PanicsOnFirstEvent {
    calls: AtomicU32,
    pub seen: Mutex<Vec<RunEvent>>,
    pub done: Notify,
}

impl RunObserver for PanicsOnFirstEvent {
    fn on_event(&self, event: &RunEvent) {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("{OBSERVER_BUG}");
        }
        self.seen.lock().unwrap().push(event.clone());
        if matches!(event.kind, RunEventKind::RunCompleted { .. }) {
            self.done.notify_one();
        }
    }
}

/// A stuck observer: every event waits for the gate to open, like a sink whose
/// network is down. It blocks the thread it is called on, which an observer
/// must never do (it is called on a tokio task): the demo does it on purpose,
/// on a multi-thread runtime, to show the run doesn't wait for it.
pub struct StuckObserver {
    gate: (Mutex<bool>, Condvar),
    patience: Duration,
    pub seen: Mutex<Vec<RunEvent>>,
    pub done: Notify,
}

impl StuckObserver {
    /// `patience` only stops a broken demo from hanging forever.
    pub fn new(patience: Duration) -> Self {
        Self {
            gate: (Mutex::new(false), Condvar::new()),
            patience,
            seen: Mutex::default(),
            done: Notify::new(),
        }
    }

    pub fn open(&self) {
        *self.gate.0.lock().unwrap() = true;
        self.gate.1.notify_all();
    }
}

impl RunObserver for StuckObserver {
    fn on_event(&self, event: &RunEvent) {
        let (open, wake) = &self.gate;
        let guard = open.lock().unwrap();
        let _open = wake
            .wait_timeout_while(guard, self.patience, |open| !*open)
            .unwrap();
        self.seen.lock().unwrap().push(event.clone());
        if matches!(event.kind, RunEventKind::RunCompleted { .. }) {
            self.done.notify_one();
        }
    }
}
