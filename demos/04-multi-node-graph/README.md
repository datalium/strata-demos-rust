# 04 — multi-node-graph

A "diamond" shaped graph computing an order receipt: a linear chain
(`CalculateSubtotal` -> `ApplyDiscount`) feeds a conditional branch
(`TagBulkShipping` or `TagStandardShipping`, chosen by order size) that
converges back into a shared `FormatReceipt` node. `01`-`03` are all
single-node graphs; this is the first demo to use `StateGraph`'s edge
wiring at all, and the first to branch on runtime state.

The .NET twin is
[`strata-demos-dotnet/demos/04-multi-node-graph`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/04-multi-node-graph).

## What it demonstrates

- `.add_edge(from, to)` chaining more than one node into an execution path —
  the engine follows edges, not the order nodes were added in
- `.add_conditional_edge(from, to, condition)` — a runtime predicate
  (`Fn(&OrderState) -> bool`), evaluated against the state the `from` node
  just produced. When a node has more than one outgoing edge, they're tried
  in the order they were added; the first whose condition matches wins. This
  demo adds the conditional edge (the "if": 10+ units ships freight) before
  a plain `.add_edge(...)` to the other branch (the "else": everything else)
  — a plain edge always matches, so putting one last is how you write a
  default branch. Without one, a state that no condition matches ends the
  run with `GraphExecutionResult::Failed(StrataError::NoMatchingEdge(node))`,
  a gap `.compile()` can't see because it depends on runtime state
- The graph runs twice, once per sample order, and checks that each took
  the branch it should (`bulk-order-thread` takes `TagBulkShipping`,
  `standard-order-thread` takes `TagStandardShipping`); it exits with code 1
  if not
- Both branches reconverging on a single `FormatReceipt` node — a graph
  doesn't have to stay a single chain or tree, edges can rejoin
- `.add_node(...)` order is purely bookkeeping, not execution order: this
  demo adds `FormatReceipt` *first* to prove it, then uses
  `.set_entry_point("CalculateSubtotal")` to override the "first node added
  becomes the entry point" default that `01-hello-agent` relies on implicitly

## What it does *not* cover

- Tools and async nodes: [`02-tool-calling`](../02-tool-calling)
- Pausing for a human: [`05-hitl-interruption`](../05-hitl-interruption)
- Streaming: [`06-token-streaming`](../06-token-streaming)
- Loops, and what `.compile()` says about them:
  [`07-graph-validation`](../07-graph-validation); capping how many nodes a
  looping thread may run is
  [`16-loop-circuit-breaker`](../16-loop-circuit-breaker)
- Branches that run *concurrently* rather than one-or-the-other:
  [`10-parallel-fan-out`](../10-parallel-fan-out),
  [`17-parallel-joins`](../17-parallel-joins) (join policies) and
  [`18-map-fan-out`](../18-map-fan-out) (a map over a list of items)
- A supervisor routing to whole sub-agents with the same conditional edges:
  [`13-subgraph-composition`](../13-subgraph-composition)
- Seeing which nodes ran from outside the graph, as run events:
  [`27-run-observability`](../27-run-observability). Here the branch taken
  is read from the final state

## Run it

```bash
cargo run -p multi-node-graph
```

Expected output:

```
12x widget @ $9.50 = $114.00 (- $11.40 discount) => $102.60 [bulk order, ships via freight]
3x widget @ $9.50 = $28.50 (- $0.00 discount) => $28.50 [standard shipping]
```

## Files

- `src/main.rs` — the whole demo, single file
- `Cargo.toml` — package name is `multi-node-graph` (folder keeps the `04-`
  prefix; Cargo package names can't start with a digit)
