# 11 — backpressure

Backpressure (STRATA-SPEC-001): what happens when a node produces output
faster than anyone reads it. The spec's claim is that the stream channel is
bounded at 1024 chunks and that a full channel **pauses the node** instead of
dropping tokens or buffering without limit.

`strata-core`'s own tests prove that against the bare `StreamContext`. This demo
proves it **through the real engine** — a streaming node added with
`.add_async_streaming_node(...)` and run with `CompiledGraph::run_streaming(...)`,
the same path [`06-token-streaming`](../06-token-streaming) uses — with a flood
node that emits 20 000 tokens as fast as it can (19 times the channel), read by
three deliberately bad consumers. No network and no LLM: every token is just its
own sequence number, so the consumer can prove none went missing or arrived out
of order.

The .NET twin is
[`strata-demos-dotnet/demos/11-backpressure`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/11-backpressure).
Scenarios 1 and 2 are the same; scenario 3 is adapted, because a Rust
consumer can walk away in a way a .NET one can't (see below).

## What it demonstrates

- **An async streaming node, the 0.9 idiom.** `Flood` is added with
  `.add_async_streaming_node(name, |state, ctx: NodeContext| async move { .. })`,
  finds the stream on `ctx.stream()`, and emits with `emit_token(..).await`,
  which waits for a free slot without holding a thread
  (`design/17-Async-Execution.md`)
- **Scenario 1, a stalled consumer.** Nothing reads. The node freezes at
  **exactly 1023 tokens**: the channel holds 1024 chunks, and one slot is taken
  by the `NodeStart` chunk the engine emits before the node body runs. The demo
  checks the count, that the queue holds exactly 1024 chunks, and that the node
  is still running (paused, not dead) 300 ms later. Then the consumer starts
  reading and gets **all 20 000 tokens, in order**: pausing cost time, not data
- **Scenario 2, a slow consumer.** The consumer reads 250 tokens, naps 1 ms,
  repeats. Every time it reads a token it samples how far ahead the producer is;
  the largest lead over the whole run is **≤ 1024** (and ≥ 512, so the scenario
  can't pass vacuously with a producer that never got ahead). The producer runs
  flat out and hits the wall over and over, yet nothing is dropped
- **Scenario 3, an abandoned consumer**, three ways:
  - **a) It drops the receiver.** A Rust consumer owns its
    `tokio::sync::mpsc::Receiver`, so walking away can mean dropping it. The
    pending `emit_token` **returns `Err(StrataError::ChannelClosed)`** rather
    than hanging, the node stops, the rest of the graph runs, and the run
    completes. (A .NET `ChannelReader` has nothing to drop, so the twin has no
    equivalent)
  - **b) It keeps the receiver but never reads again** (a stuck UI task, say),
    so nothing will free a slot. The caller cancels the token it passed with
    `run_streaming_with(.., RunConfig::new().with_cancellation(token))`. The run
    ends **`Cancelled { reason: Requested, node: "Flood" }`** at once, with
    nobody reading. The engine drops the handler's future where it waits (a
    drop guard inside it records that), so it emits nothing more; the store
    holds a resumable **`Cancelled` checkpoint** at `Flood`; and the 1023
    tokens already queued can still be read (the stream ends with no `NodeEnd`
    for the stopped node). This is the .NET twin's scenario 3
  - **c) The same with a sync handler** (`add_streaming_node`) blocked in
    `emit_token_blocking`. The run still ends `Cancelled` at `Flood`: the
    engine stops waiting for a handler on the blocking pool and abandons it.
    But the thread stays blocked after the run has ended — the demo checks it
    is still blocked 300 ms later — and is released only when the receiver is
    dropped, with `ChannelClosed`. See "engine difference" below
- The whole time the run is watched from outside with an atomic counter of
  emits that have *returned* (a blocked emit hasn't), since node state is only
  visible once the node finishes

**Engine difference (3c).** When a streaming run ends, the engine closes the
stream (`StreamContext::close`, `strata-core/src/streaming.rs:188`), which
makes *later* emits fail. A `blocking_send` already in progress
(`emit_blocking`, `streaming.rs:160-171`) holds its own clone of the sender,
so the channel stays open under it, and it waits for a free slot or for the
receiver to be dropped. In .NET, completing the channel fails a blocked
`EmitToken` with `ChannelClosedException`, so a sync producer there is
released as soon as its run ends. The demo checks the Rust behaviour as it is
today; if the engine starts releasing the thread on close, that check will
fail and the scenario should be updated.

## What it does *not* cover

- **Only one producer.** A parallel group's streaming branches write to the
  same channel concurrently, attributed to `<group>/<branch>`
  ([`10-parallel-fan-out`](../10-parallel-fan-out)); this demo floods from one
  node
- **Only the default capacity.** `run_streaming` always creates the channel at
  1024 (`DEFAULT_CHANNEL_CAPACITY`); there's no way to ask for another bound
  through the engine, so the demo can only test that one number. Smaller
  capacities are exercised by `streaming.rs`'s own tests against the bare
  primitive
- **Resuming the cancelled thread.** The demo checks the checkpoint is there;
  resuming a `Cancelled` checkpoint is shown in
  [`15-async-and-cancellation`](../15-async-and-cancellation)
- **A deadline or a node timeout.** `RunConfig::with_deadline` and
  `set_timeout` stop a run or an attempt the same way; see demo 15
- **No throughput numbers.** The demo checks counts and orderings, never speed
  — the elapsed time it prints depends on the consumer's naps and the OS timer
  (`tokio::time::sleep(1 ms)` takes about 15 ms on Windows)
- **No lossy mode.** There is no drop-oldest / drop-newest policy; the channel
  either has room or the node waits

## Run it

```bash
cargo run -p backpressure
```

Takes about two seconds. The demo checks every claim it prints and exits
non-zero if one fails. Output of a real run (the `ms` figure in scenario 2
will differ, and the largest lead is 1023 or 1024 depending on timing):

```
=== Scenario 1: the consumer stalls (reads nothing) ===
  Consumer has read nothing. The flood node wants to emit 20000 tokens; after 300 ms it has emitted 1023.
  Queued in the channel: 1024 chunk(s) (capacity 1024) = the engine's NodeStart + 1023 tokens.
  The node is still running -- paused awaiting emit_token, not dropping.
  Now the consumer starts reading...
  Received 20000 tokens in order: true. Lifecycle chunks: 2 NodeStart, 2 NodeEnd (one pair per node).
  Flood emitted 20000 of 20000 tokens
  Nothing lost: pausing the node cost time, not data (20000 tokens is 19x the channel).

=== Scenario 2: the consumer is slow (reads 250 tokens, then naps 1 ms) ===
  Received 20000 tokens in order: true, in 1234 ms.
  Largest lead the producer ever had over the consumer: 1023 tokens (capacity 1024).
  Flood emitted 20000 of 20000 tokens
  The producer ran flat out and hit the wall over and over; it never got more than 1024 tokens ahead.

=== Scenario 3: the consumer walks away while the node is frozen ===
  a) It drops the receiver:
  The node is frozen at 1023 tokens. Dropping the receiver...
  The pending emit_token failed with ChannelClosed; the node stopped, and the graph ran to the end: Flood emitted 1023 of 20000 tokens.

  b) It keeps the receiver but never reads again:
  The node is frozen at 1023 tokens, and 300 ms later still waiting in emit_token. Nothing reads, so nothing will free a slot.
  The caller gives up and cancels the run's token.
  The run ended Cancelled (Requested) at 'Flood', without anyone reading. The engine dropped the handler's future (dropped mid-emit), so it emitted nothing more (1023 tokens).
  The store holds a 'Cancelled' checkpoint at 'Flood': a resume would re-enter Flood with its starting state.
  The 1023 tokens already queued are still readable; the stream ends with 1 NodeStart and 0 NodeEnd (a cancelled node gets none).

  c) The same, with a sync handler blocked in emit_token_blocking:
  The run ended Cancelled (Requested) at 'Flood' too: the engine stopped waiting for the handler and abandoned it.
  300 ms after the run ended, the handler is still blocked at 1023 tokens: a blocked thread can't be stopped, and the receiver it is waiting on is still alive.
  Dropping the receiver releases it: emit_token_blocking failed with ChannelClosed and it emitted nothing more (1023 tokens).
  So cancelling frees an async producer at once; a sync one is freed by the consumer letting go of the stream.

=== Done: the 1024-chunk channel paused the producer instead of dropping tokens, never let it run further ahead than its capacity, and an abandoned producer was released. ===
```

When it was first written (against 0.3, with a sync `Flood`) the demo was
also run against a `strata-core` deliberately broken to drop chunks when the
channel is full (`try_send`, result ignored): scenario 1 failed immediately,
with the node having emitted all 20 000 tokens instead of freezing at 1023.

## Files

- `src/main.rs` — the whole demo, single file
- `Cargo.toml` — package name is `backpressure` (folder keeps the `11-` prefix;
  Cargo package names can't start with a digit)
