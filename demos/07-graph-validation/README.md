# 07 — graph-validation

Builds the same four-node document review pipeline (`Draft` ->
`LegalReview` (first pass only) -> `Review` -> `Archive`) twice, from the
same nodes; only the edges differ. The **broken** version routes `Review`
back to `Draft` on a plain `.add_edge(...)` with no condition, and its exit
edge names `"Archve"`, a typo. `.compile()` returns
`Err(StrataError::GraphInvalid(ValidationResult))` carrying three
diagnostics (dangling edge, orphan node, unguarded cycle) before a single
node runs. The **fixed** version guards the retry edge with a real predicate
and fixes the typo. It compiles clean, even though the loop contains a branch
that rejoins on plain edges, and it runs the loop to completion.

This is the first demo to show STRATA-SPEC-004's *reject* path
(`design/Arch-Specs/STRATA-SPEC-004.md` in [`datalium/strata`](https://github.com/datalium/strata)). Every earlier
demo's `.compile()` call ran the same validator and passed. The Rust twin of
[`strata-demos-dotnet/demos/07-graph-validation`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/07-graph-validation),
with the same graphs, output and checks.

## What it demonstrates

- `.compile()` runs compile-time validation and returns
  `Err(StrataError::GraphInvalid(validation))` (its `errors` carries every
  `ValidationError`) — structural errors abort at startup, not at runtime.
  The demo matches on it, prints every error's `code` and `message`, and
  checks that exactly the three expected errors are present
- **Dangling edge** (`STRATA_ERR_DANGLING_EDGE`): `.add_edge("Review", "Archve")`
  names a node that doesn't exist. `add_edge` doesn't check its arguments;
  `.compile()` reports one error per such edge, with `node` = the edge's
  `from` and `related_node` = its `to`
- **Orphan node** (`STRATA_ERR_ORPHAN`): the same typo leaves `Archive`
  unreachable from the entry point. A dangling edge is left out of the
  reachability walk, so one typo produces both errors, and the dangling-edge
  error says where the mistake is
- **Unguarded cycle** (`STRATA_ERR_UNGUARDED_CYCLE`): a plain `.add_edge(...)`
  closing a loop (`Review` -> `Draft`) with nothing bounding how many times
  it can fire
- The rule for loops: a graph is rejected only if its **plain** edges alone
  form a cycle. The fixed graph has two plain paths from `Draft` to `Review`
  (through `LegalReview`, and directly), so it holds two cycles back to
  `Draft`, and both go through the one guarded edge
  `.add_conditional_edge("Review", "Draft", |s| s.revision < 3)`. The
  runtime predicate, not the static shape, is what bounds the loop
- Fixing everything without changing the nodes: guarding the retry edge, and
  spelling `Archive` right so the plain `.add_edge("Review", "Archive")` after
  it is the loop's "else" exit (same declaration-order rule as
  [`04-multi-node-graph`](../04-multi-node-graph))
- That the fixed graph doesn't just satisfy the validator: it runs, the
  `Draft`/`Review` loop fires three times before routing to `Archive`, and
  the recorded path shows both branches into `Review` were taken
  (`LegalReview` on the first pass, the direct edge on the next two). The
  program exits with code 1 if any of this differs

The 0.3 version of this demo had no `LegalReview` branch and no typo, and
showed only the orphan and the cycle (`Archive` was simply never wired in).
The dangling-edge check and the plain-cycles-only rule for loops are what
the 0.9 validator adds to that story.

## What it does *not* cover

- The `strata graph validate` CLI command (STRATA-SPEC-004's other entry
  point): see [`14-graph-ci-gate`](../14-graph-ci-gate), which runs it over
  a folder of exported graphs
- A loop whose guard never turns false: the validator accepts it, because
  it can't evaluate the predicate.
  `.compile_with(CompileOptions::default().with_max_node_hops(..))` stops
  such a thread at run time; see
  [`16-loop-circuit-breaker`](../16-loop-circuit-breaker)
- The validator's checks for parallel groups and maps (an empty group, a
  duplicate branch, an unreachable quorum, a wait or boundary inside a map):
  see [`17-parallel-joins`](../17-parallel-joins) and [`18-map-fan-out`](../18-map-fan-out)
- The spec's "missing terminal path" check is not built: a graph with no
  exit passes if it has no orphan and no unguarded cycle
- Tool-calling, streaming, interruption: see
  [`02-tool-calling`](../02-tool-calling) and the demos after this one

## Run it

```bash
cargo run -p graph-validation
```

Output of a real run:

```
Compilation rejected, as STRATA-SPEC-004 requires:
  [STRATA_ERR_DANGLING_EDGE] Edge 'Review' -> 'Archve' points at 'Archve', which is not a node in the graph.
  [STRATA_ERR_ORPHAN] Node 'Archive' is isolated and unreachable from the entry point 'Draft'.
  [STRATA_ERR_UNGUARDED_CYCLE] Unguarded cycle detected between 'Review' and 'Draft'. Add a guarded/conditional edge to protect this loop.

Same nodes, fixed: guarded the retry edge and fixed the typo -- compiles clean.
path: Draft -> LegalReview -> Review -> Draft -> Review -> Draft -> Review -> Archive
'Q3 roadmap' approved after 3 revision(s), archived: true
```

(The order of the errors is fixed by the validator, not by declaration
order: for each graph level `validate()` reports the entry point, then
dangling edges, then orphans, then unguarded cycles.)

## Files

- `src/main.rs` — the whole demo, single file
- `Cargo.toml` — package name is `graph-validation` (folder keeps the `07-`
  prefix; Cargo package names can't start with a digit); depends on
  `datalium-strata`, `datalium-strata-memory` (for `InMemoryCheckpointStore`),
  `serde` and `tokio`
