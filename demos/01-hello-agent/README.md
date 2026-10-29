# 01 — hello-agent

The smallest possible Strata program: a one-node graph that reads `name`
from its state and writes a `greeting`. No tools, no streaming, no
human-in-the-loop — just enough to prove the engine and the wiring to
the published Strata crates both work.

The .NET twin is
[`strata-demos-dotnet/demos/01-hello-agent`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/01-hello-agent).

## What it demonstrates

- `StateGraph::<S>::new()` + `.add_node(...)` — the fluent graph builder
- `.compile()` running STRATA-SPEC-004's structural validation (entry-point,
  dangling-edge, orphan and unguarded-cycle checks) before anything
  executes. It returns `Result<CompiledGraph<S>, StrataError>`
  (`StrataError::GraphInvalid(...)` on a bad graph) where .NET's `.Compile()`
  throws
- `.run(thread_id, initial_state, store).await` executing the graph and
  returning a `GraphExecutionResult<S>`. Besides `Completed` and `Failed`
  there are `Interrupted` and `Cancelled`, so the `match` has a `_ =>` arm
  for the outcomes this graph can't reach
- Why `run` always needs a `CheckpointStore` argument (an
  `Arc<dyn CheckpointStore>`) even though this graph never stops early —
  see the comment in `src/main.rs`
- The simplest handler kind, a synchronous `Fn(S) -> S`. The engine runs it
  on tokio's blocking pool, so `main` is `#[tokio::main] async fn`. Every
  handler kind also has an async form (`.add_async_node(...)`) that gets a
  `NodeContext`, which carries the run's cancellation token; that comes in
  [`02-tool-calling`](../02-tool-calling). Rust also has
  `.add_inline_node(...)` (no .NET counterpart) for a handler that only
  computes for a few microseconds: it runs on the executor without the
  blocking-pool hop, and can't be timed out or abandoned mid-run

## What it does *not* cover

- Tools and async nodes: [`02-tool-calling`](../02-tool-calling)
- A real LLM call: [`03-llm-tool-calling`](../03-llm-tool-calling)
- Edges, branching and `.set_entry_point(...)`:
  [`04-multi-node-graph`](../04-multi-node-graph)
- Pausing for a human (`.add_interrupt_boundary(...)`):
  [`05-hitl-interruption`](../05-hitl-interruption)
- Streaming: [`06-token-streaming`](../06-token-streaming)
- `.compile()` rejecting a graph: [`07-graph-validation`](../07-graph-validation)
- Cancellation, deadlines and timeouts (`run_with` and `RunConfig`):
  [`15-async-and-cancellation`](../15-async-and-cancellation)
- `.compile_with(CompileOptions::new()...)`: a hop limit
  ([`16-loop-circuit-breaker`](../16-loop-circuit-breaker)), a graph identity
  ([`22-graph-identity`](../22-graph-identity)), and checkpointing after every
  node ([`20-durable-execution`](../20-durable-execution))

## Run it

```bash
cargo run -p hello-agent
```

Expected output:

```
Hello, World!
```

## Files

- `src/main.rs` — the whole demo, single file
- `Cargo.toml` — package name is `hello-agent` (Cargo package names can't
  start with a digit, hence the folder/package name mismatch)
