//! Backpressure (STRATA-SPEC-001): what happens when a node produces output
//! faster than the consumer reads it. The spec's claim is that the stream
//! channel is bounded at 1024 chunks and that a full channel *pauses the node*
//! rather than dropping tokens or growing without limit. `streaming.rs` proves
//! that in isolation; this demo proves it through the real engine —
//! `add_async_streaming_node` + `run_streaming` — by running a flood node
//! against three deliberately bad consumers:
//!
//! 1. **Stalled** — the consumer doesn't read at all. The node must freeze at
//!    exactly the channel's capacity (1023 tokens + the engine's `NodeStart`
//!    chunk = 1024 queued chunks), still be running, and lose nothing once the
//!    consumer finally drains.
//! 2. **Slow** — the consumer reads in bursts with pauses between. The node's
//!    lead over the consumer must never exceed the capacity, for the whole run.
//! 3. **Abandoned** — the consumer stops reading while the node is frozen:
//!    - it drops the receiver: the pending emit fails with `ChannelClosed`,
//!      the node stops, and the rest of the graph runs;
//!    - it keeps the receiver but never reads again (a stuck UI task, say):
//!      nothing frees a slot, so the caller cancels the run's token
//!      (`design/17-Async-Execution.md`). The run ends `Cancelled` at `Flood`
//!      with a resumable `cancelled` checkpoint; the engine drops the
//!      handler's future, pending emit and all;
//!    - the same with a sync handler blocked in `emit_token_blocking`: the
//!      run still ends `Cancelled`, but nothing can stop a blocked thread,
//!      and the send it is blocked in holds the channel open. It is released
//!      only when the receiver is dropped.
//!
//! Everything is deterministic and offline: the "flood" is a loop counting up
//! to 20 000, and every token carries its own sequence number so the consumer
//! can prove none went missing or arrived out of order. The demo asserts on
//! counts, not on wall-clock time.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datalium_strata::CancellationToken;
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CancelReason, CheckpointStore, GraphExecutionResult};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::schema::CheckpointStatus;
use datalium_strata::streaming::{
    DEFAULT_CHANNEL_CAPACITY, StreamChunk, StreamChunkType, StreamContext,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

/// Enough tokens to overflow the channel ~19 times over.
const TOTAL_TOKENS: usize = 20_000;

/// The engine emits a `NodeStart` chunk before the node body runs, and that
/// chunk occupies one of the 1024 slots — so a stalled node can only get this
/// many tokens out before its next send blocks.
const FROZEN_AT: usize = DEFAULT_CHANNEL_CAPACITY - 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FloodState {
    total: usize,
    sent: usize,
    summary: String,
}

impl FloodState {
    fn new(total: usize) -> Self {
        Self {
            total,
            sent: 0,
            summary: String::new(),
        }
    }
}

/// Watches the producer from outside while it runs (node state is only
/// visible once the node finishes): the number of emits that have *returned*
/// (a blocked emit hasn't), and how the handler's body ended, once it has.
#[derive(Default)]
struct Progress {
    emitted: AtomicUsize,
    exited: Mutex<Option<&'static str>>,
}

impl Progress {
    fn emitted(&self) -> usize {
        self.emitted.load(Ordering::SeqCst)
    }

    fn exited(&self) -> Option<&'static str> {
        *self.exited.lock().unwrap()
    }

    /// Records how the body ended; only the first call counts.
    fn exit(&self, how: &'static str) {
        self.exited.lock().unwrap().get_or_insert(how);
    }
}

/// Lives inside the async handler's future. If the engine drops the future
/// before the body ends (a cancelled run), this is what notices.
struct DroppedGuard(Arc<Progress>);

impl Drop for DroppedGuard {
    fn drop(&mut self) {
        self.0.exit("dropped mid-emit");
    }
}

fn how_emit_failed(error: &StrataError) -> &'static str {
    match error {
        StrataError::ChannelClosed => "ChannelClosed",
        _ => "another error",
    }
}

/// Builds `Flood` (a streaming node that emits `total` tokens as fast as it
/// can, token *i* being the text `i`) followed by a plain `Summarize` node.
/// `Flood` is async by default; `sync` swaps in a sync handler for contrast.
fn build_graph(progress: Arc<Progress>, sync: bool) -> Arc<CompiledGraph<FloodState>> {
    let graph = StateGraph::<FloodState>::new();
    let graph = if sync {
        // A sync streaming handler: the engine runs it on tokio's blocking
        // pool and hands it the stream directly. `emit_token_blocking` blocks
        // that thread while the channel is full.
        graph.add_streaming_node("Flood", move |state: FloodState, stream: &StreamContext| {
            for i in 0..state.total {
                if let Err(error) = stream.emit_token_blocking(&i.to_string()) {
                    progress.exit(how_emit_failed(&error));
                    break;
                }
                progress.emitted.fetch_add(1, Ordering::SeqCst);
            }
            progress.exit("returned");
            FloodState {
                sent: progress.emitted(),
                ..state
            }
        })
    } else {
        // An async streaming handler: the stream is on `NodeContext::stream`,
        // and `emit_token(..).await` waits for a free slot without holding a
        // thread. A cancelled run drops this future where it waits.
        graph.add_async_streaming_node("Flood", move |state: FloodState, ctx: NodeContext| {
            let progress = progress.clone();
            async move {
                let _guard = DroppedGuard(progress.clone());
                let stream = ctx.stream().expect("a streaming run always has a stream");
                for i in 0..state.total {
                    // Err means the consumer dropped the receiver: stop.
                    if let Err(error) = stream.emit_token(&i.to_string()).await {
                        progress.exit(how_emit_failed(&error));
                        break;
                    }
                    progress.emitted.fetch_add(1, Ordering::SeqCst);
                }
                progress.exit("returned");
                FloodState {
                    sent: progress.emitted(),
                    ..state
                }
            }
        })
    };
    Arc::new(
        graph
            .add_node("Summarize", |state: FloodState| FloodState {
                summary: format!("Flood emitted {} of {} tokens", state.sent, state.total),
                ..state
            })
            .add_edge("Flood", "Summarize")
            .compile()
            .expect("graph is structurally valid"),
    )
}

fn store() -> Arc<dyn CheckpointStore> {
    Arc::new(InMemoryCheckpointStore::new())
}

/// What a consumer saw while draining a whole run.
struct Drained {
    tokens: usize,
    in_order: bool,
    node_starts: usize,
    node_ends: usize,
    /// The largest number of tokens the producer had emitted beyond what the
    /// consumer had read, sampled every time the consumer read one.
    max_lead: usize,
    elapsed: Duration,
}

/// Reads every chunk until the channel closes. If `pace` is
/// `Some((every, pause))`, sleeps `pause` after every `every` tokens.
async fn drain(
    receiver: &mut Receiver<StreamChunk>,
    progress: &Progress,
    pace: Option<(usize, Duration)>,
) -> Drained {
    let started = Instant::now();
    let mut drained = Drained {
        tokens: 0,
        in_order: true,
        node_starts: 0,
        node_ends: 0,
        max_lead: 0,
        elapsed: Duration::ZERO,
    };

    while let Some(chunk) = receiver.recv().await {
        match chunk.chunk_type {
            StreamChunkType::NodeStart => drained.node_starts += 1,
            StreamChunkType::NodeEnd => drained.node_ends += 1,
            StreamChunkType::Token => {
                if chunk.content.parse::<usize>().ok() != Some(drained.tokens) {
                    drained.in_order = false;
                }
                drained.tokens += 1;
                let lead = progress.emitted().saturating_sub(drained.tokens);
                drained.max_lead = drained.max_lead.max(lead);
                if let Some((every, pause)) = pace
                    && drained.tokens.is_multiple_of(every)
                {
                    tokio::time::sleep(pause).await;
                }
            }
            // `Flood` isn't fallible, so nothing here retries or fails.
            StreamChunkType::Error | StreamChunkType::Retry => fail(&format!(
                "unexpected {:?} chunk: {}",
                chunk.chunk_type, chunk.content
            )),
        }
    }

    drained.elapsed = started.elapsed();
    drained
}

fn fail(what: &str) -> ! {
    eprintln!("check failed: {what}");
    std::process::exit(1);
}

fn check(ok: bool, what: &str) {
    if !ok {
        fail(what);
    }
}

/// Polls `condition` until it holds. A slow machine only delays this; it never
/// makes the demo's checks flaky.
async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        if Instant::now() >= deadline {
            fail(&format!("timed out waiting for {what}"));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Awaits the graph task, but fails loudly instead of hanging if it never ends.
async fn end_within(
    handle: JoinHandle<GraphExecutionResult<FloodState>>,
    what: &str,
) -> GraphExecutionResult<FloodState> {
    wait_until(what, || handle.is_finished()).await;
    handle.await.expect("streaming task panicked")
}

async fn complete_within(
    handle: JoinHandle<GraphExecutionResult<FloodState>>,
    what: &str,
) -> FloodState {
    match end_within(handle, what).await {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => fail(&format!("graph execution failed: {err:?}")),
        _ => fail("expected Completed"),
    }
}

async fn scenario_stalled() {
    println!("=== Scenario 1: the consumer stalls (reads nothing) ===");
    let progress = Arc::new(Progress::default());
    let graph = build_graph(progress.clone(), false);
    let (handle, mut receiver) =
        graph.run_streaming("stalled", FloodState::new(TOTAL_TOKENS), store());

    // Deliberately do not read. Wait for the producer to run out of room, then
    // give it a generous window in which to (wrongly) make more progress.
    wait_until("the flood node to fill the channel", || {
        progress.emitted() >= FROZEN_AT
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let frozen = progress.emitted();
    println!(
        "  Consumer has read nothing. The flood node wants to emit {TOTAL_TOKENS} tokens; \
         after 300 ms it has emitted {frozen}."
    );
    println!(
        "  Queued in the channel: {} chunk(s) (capacity {DEFAULT_CHANNEL_CAPACITY}) = the \
         engine's NodeStart + {frozen} tokens.",
        receiver.len()
    );
    check(
        frozen == FROZEN_AT,
        "a stalled consumer must freeze the producer at exactly capacity - 1 tokens",
    );
    check(
        receiver.len() == DEFAULT_CHANNEL_CAPACITY,
        "the channel should be exactly full",
    );
    check(
        !handle.is_finished(),
        "the node must be paused, not finished or dead",
    );
    println!("  The node is still running -- paused awaiting emit_token, not dropping.");

    println!("  Now the consumer starts reading...");
    let drained = drain(&mut receiver, &progress, None).await;
    let state = complete_within(handle, "the graph to finish after draining").await;

    println!(
        "  Received {} tokens in order: {}. Lifecycle chunks: {} NodeStart, {} NodeEnd \
         (one pair per node).",
        drained.tokens, drained.in_order, drained.node_starts, drained.node_ends
    );
    println!("  {}", state.summary);
    check(drained.tokens == TOTAL_TOKENS, "tokens were dropped");
    check(drained.in_order, "tokens arrived out of order");
    check(
        (drained.node_starts, drained.node_ends) == (2, 2),
        "expected one NodeStart/NodeEnd pair per node",
    );
    check(
        state.sent == TOTAL_TOKENS,
        "the node did not finish emitting",
    );
    println!(
        "  Nothing lost: pausing the node cost time, not data ({} tokens is {}x the channel).\n",
        TOTAL_TOKENS,
        TOTAL_TOKENS / DEFAULT_CHANNEL_CAPACITY
    );
}

async fn scenario_slow() {
    println!("=== Scenario 2: the consumer is slow (reads 250 tokens, then naps 1 ms) ===");
    let progress = Arc::new(Progress::default());
    let graph = build_graph(progress.clone(), false);
    let (handle, mut receiver) =
        graph.run_streaming("slow", FloodState::new(TOTAL_TOKENS), store());

    let drained = drain(
        &mut receiver,
        &progress,
        Some((250, Duration::from_millis(1))),
    )
    .await;
    let state = complete_within(handle, "the graph to finish").await;

    println!(
        "  Received {} tokens in order: {}, in {} ms.",
        drained.tokens,
        drained.in_order,
        drained.elapsed.as_millis()
    );
    println!(
        "  Largest lead the producer ever had over the consumer: {} tokens (capacity \
         {DEFAULT_CHANNEL_CAPACITY}).",
        drained.max_lead
    );
    println!("  {}", state.summary);
    check(drained.tokens == TOTAL_TOKENS, "tokens were dropped");
    check(drained.in_order, "tokens arrived out of order");
    check(
        drained.max_lead <= DEFAULT_CHANNEL_CAPACITY,
        "the producer got further ahead of the consumer than the channel holds",
    );
    check(
        drained.max_lead >= DEFAULT_CHANNEL_CAPACITY / 2,
        "the producer never got far ahead, so this scenario proved nothing",
    );
    println!(
        "  The producer ran flat out and hit the wall over and over; it never got more than \
         {DEFAULT_CHANNEL_CAPACITY} tokens ahead.\n"
    );
}

async fn scenario_abandoned() {
    println!("=== Scenario 3: the consumer walks away while the node is frozen ===");

    // --- 3a: the consumer drops the receiver. ---
    println!("  a) It drops the receiver:");
    let progress = Arc::new(Progress::default());
    let graph = build_graph(progress.clone(), false);
    let (handle, receiver) = graph.run_streaming("dropped", FloodState::new(TOTAL_TOKENS), store());

    wait_until("the flood node to fill the channel", || {
        progress.emitted() >= FROZEN_AT
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    check(
        !handle.is_finished(),
        "the frozen node must still be waiting",
    );
    println!("  The node is frozen at {FROZEN_AT} tokens. Dropping the receiver...");
    drop(receiver);

    let state = complete_within(handle, "the frozen node to notice the receiver is gone").await;
    println!(
        "  The pending emit_token failed with {}; the node stopped, and the graph ran to the end: {}.",
        progress.exited().unwrap_or("nothing"),
        state.summary
    );
    check(
        progress.exited() == Some("ChannelClosed"),
        "expected the pending emit to fail with ChannelClosed",
    );
    check(
        state.sent == FROZEN_AT,
        "the failed emit must not sneak one more token through",
    );

    // --- 3b: the consumer keeps the receiver but never reads again. ---
    println!();
    println!("  b) It keeps the receiver but never reads again:");
    let progress = Arc::new(Progress::default());
    let graph = build_graph(progress.clone(), false);
    let store = store();
    let token = CancellationToken::new();
    let (handle, mut receiver) = graph.run_streaming_with(
        "abandoned",
        FloodState::new(TOTAL_TOKENS),
        store.clone(),
        RunConfig::new().with_cancellation(token.clone()),
    );

    wait_until("the flood node to fill the channel", || {
        progress.emitted() >= FROZEN_AT
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    check(
        progress.emitted() == FROZEN_AT,
        "the frozen node must not make progress",
    );
    check(
        !handle.is_finished(),
        "the frozen node must still be waiting",
    );
    println!(
        "  The node is frozen at {FROZEN_AT} tokens, and 300 ms later still waiting in emit_token. \
         Nothing reads, so nothing will free a slot."
    );

    println!("  The caller gives up and cancels the run's token.");
    token.cancel();
    let (node, reason) = match end_within(handle, "the cancelled run to end").await {
        GraphExecutionResult::Cancelled { node, reason, .. } => (node, reason),
        _ => fail("expected the cancelled run to end Cancelled"),
    };
    let checkpoint = store
        .load("abandoned")
        .await
        .expect("checkpoint store readable")
        .unwrap_or_else(|| fail("expected a cancelled checkpoint"));
    println!(
        "  The run ended Cancelled ({reason:?}) at '{node}', without anyone reading. The engine \
         dropped the handler's future ({}), so it emitted nothing more ({} tokens).",
        progress.exited().unwrap_or("still running"),
        progress.emitted()
    );
    println!(
        "  The store holds a '{:?}' checkpoint at '{}': a resume would re-enter Flood with its \
         starting state.",
        checkpoint.status,
        checkpoint.next_node.as_deref().unwrap_or("nowhere")
    );
    check(
        reason == CancelReason::Requested && node == "Flood",
        "expected the run to stop at Flood because it was asked to",
    );
    check(
        progress.exited() == Some("dropped mid-emit"),
        "expected the engine to drop the handler's future at its pending emit",
    );
    check(
        progress.emitted() == FROZEN_AT,
        "the cancelled node must not emit more tokens",
    );
    check(
        checkpoint.status == CheckpointStatus::Cancelled
            && checkpoint.next_node.as_deref() == Some("Flood"),
        "expected a cancelled checkpoint at Flood",
    );

    // What was already queued is still readable: the stream ended, it wasn't torn down.
    let leftover = drain(&mut receiver, &progress, None).await;
    println!(
        "  The {} tokens already queued are still readable; the stream ends with {} NodeStart \
         and {} NodeEnd (a cancelled node gets none).",
        leftover.tokens, leftover.node_starts, leftover.node_ends
    );
    check(
        leftover.tokens == FROZEN_AT
            && leftover.in_order
            && (leftover.node_starts, leftover.node_ends) == (1, 0),
        "expected the queued tokens and no NodeEnd",
    );

    // --- 3c: the same with a sync handler blocked in emit_token_blocking. ---
    println!();
    println!("  c) The same, with a sync handler blocked in emit_token_blocking:");
    let progress = Arc::new(Progress::default());
    let graph = build_graph(progress.clone(), true);
    let token = CancellationToken::new();
    let (handle, receiver) = graph.run_streaming_with(
        "abandoned-sync",
        FloodState::new(TOTAL_TOKENS),
        store.clone(),
        RunConfig::new().with_cancellation(token.clone()),
    );
    wait_until("the sync flood node to fill the channel", || {
        progress.emitted() >= FROZEN_AT
    })
    .await;
    token.cancel();
    let (node, reason) = match end_within(handle, "the cancelled sync run to end").await {
        GraphExecutionResult::Cancelled { node, reason, .. } => (node, reason),
        _ => fail("expected the cancelled sync run to end Cancelled"),
    };
    println!(
        "  The run ended Cancelled ({reason:?}) at '{node}' too: the engine stopped waiting for \
         the handler and abandoned it."
    );
    check(
        reason == CancelReason::Requested && node == "Flood",
        "expected the sync run to end Cancelled at Flood",
    );

    // Ending the run closes the stream for later emits, but the blocked
    // `blocking_send` already holds its own sender, so the channel stays open
    // under it: the thread waits for a free slot or for the receiver to go.
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!(
        "  300 ms after the run ended, the handler is {} at {} tokens: a blocked thread can't be \
         stopped, and the receiver it is waiting on is still alive.",
        progress.exited().unwrap_or("still blocked"),
        progress.emitted()
    );
    check(
        progress.exited().is_none() && progress.emitted() == FROZEN_AT,
        "expected the abandoned sync handler to still be blocked in emit_token_blocking",
    );
    drop(receiver);
    wait_until("the sync flood handler to exit", || {
        progress.exited().is_some()
    })
    .await;
    println!(
        "  Dropping the receiver releases it: emit_token_blocking failed with {} and it emitted \
         nothing more ({} tokens).",
        progress.exited().unwrap_or("nothing"),
        progress.emitted()
    );
    check(
        progress.exited() == Some("ChannelClosed") && progress.emitted() == FROZEN_AT,
        "expected the blocked emit to fail with ChannelClosed once the receiver was dropped",
    );
    println!(
        "  So cancelling frees an async producer at once; a sync one is freed by the consumer \
         letting go of the stream.\n"
    );
}

#[tokio::main]
async fn main() {
    scenario_stalled().await;
    scenario_slow().await;
    scenario_abandoned().await;
    println!(
        "=== Done: the 1024-chunk channel paused the producer instead of dropping tokens, never \
         let it run further ahead than its capacity, and an abandoned producer was released. ==="
    );
}
