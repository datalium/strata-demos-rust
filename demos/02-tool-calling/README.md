# 02 — tool-calling

A node dispatches an `add(a, b)` tool by name through a `ToolRegistry`,
instead of computing the sum inline. This is STRATA-SPEC-003's tool-calling
metaprogramming: no runtime reflection, no schema drift between what the
tool declares and what actually gets called. The node is async, so it awaits
the tool and passes it a `ToolCallContext` built from its own `NodeContext`
(`design/20-Tool-Dispatch-And-Registry.md`), which tells the tool which
thread and node called it.

The .NET twin is
[`strata-demos-dotnet/demos/02-tool-calling`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/02-tool-calling).

## What it demonstrates

- `#[derive(StrataSchema)]` on an argument struct (`AddArgs`) generates its
  JSON-Schema `parameters` entry at compile time. Unlike .NET, where the
  source generator turns method parameters into schema fields, Rust declares
  the arguments as a struct
- `#[strata_tool(name = "...", description = "...")]` on an `async fn`
  generates a zero-sized dispatch handle (`AddTool`) implementing
  `StrataTool` — the macro is the only place the tool's name and function
  are wired together
- An **async node** (`.add_async_node(name, |state, ctx: NodeContext| async
  move { ... })`) that awaits the tool. The sync `Fn(S) -> S` form from
  [`01-hello-agent`](../01-hello-agent) runs on tokio's blocking pool and
  would have to `block_on` the tool's future
- `ToolRegistry::dispatch_with(name, raw_json_args, ToolCallContext::from(&ctx))`:
  the context carries the thread id, run id, node path, attempt, run
  metadata, span, services and the run's cancellation token. The registry
  adds a call id (a UUIDv7) when the context has none
- A `ToolCallContext` parameter on the tool itself (`async fn add(args:
  AddArgs, ctx: ToolCallContext)`). The macro fills it from the dispatch call
  and leaves it out of the manifest; the demo checks that
  `ToolRegistry::manifest()` still lists only `a` and `b`. The tool uses the
  context for an audit line and refuses a call with no caller
- The refusal itself: the context-free `ToolRegistry::dispatch(name, args)`
  passes `ToolCallContext::default()` (every field `None`), and the demo
  checks that such a call comes back as `Err(ToolExecutionFailure("add"))`.
  The generated wrapper replaces whatever error the tool returned with the
  tool's name, so the tool's own message doesn't reach the caller
- The result is the tool's `String` as returned (`"8"`), where .NET wraps it
  as `{"result": ...}`

## What it does *not* cover

- **No LLM.** The tool's arguments come from the graph's own state.
  `ToolRegistry::manifest()` is what you'd hand a model's tool-calling API;
  see [`03-llm-tool-calling`](../03-llm-tool-calling) for a live call, and
  [`23-agent-tool-loop`](../23-agent-tool-loop) for the agent loop that
  `datalium-strata-ai` builds around a model and its tools.
- **No subsets or middleware.** One registry with one tool. A per-agent
  `subset(...)`, tool middleware (`use_middleware`), and the `strata.tool`
  span and `ToolCallStarted` / `ToolCallCompleted` run events a registry
  dispatch adds are in
  [`26-tool-registry-and-middleware`](../26-tool-registry-and-middleware).
- **No cancellation scenario.** The token reaches the tool through the
  context, but nothing cancels it; see
  [`15-async-and-cancellation`](../15-async-and-cancellation).
- **No error handling in the node.** A failed dispatch panics the node
  (`.expect(...)`); a node that returns errors and a recovery policy are in
  [`08-error-recovery`](../08-error-recovery).

## Run it

```bash
cargo run -p tool-calling
```

Expected output:

```
[add] called by node 'Add' on thread 'calc-thread', attempt 1
3 + 5 = 8
manifest parameters for 'add': a, b
dispatch without a context: Err(ToolExecutionFailure("add"))
```

The first line is printed by the tool, from the context it was given. The
program exits with code 1 if the sum, the manifest check or the refusal check
is wrong.

## Files

- `src/main.rs` — tool definition, graph, execution and the checks, single
  file
- `Cargo.toml` — package name is `tool-calling` (see
  [`01-hello-agent`](../01-hello-agent)'s README for why the digit prefix is
  dropped from the package name)
