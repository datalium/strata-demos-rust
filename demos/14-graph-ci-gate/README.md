# 14 — graph-ci-gate

`strata graph validate` as a **CI gate** (STRATA-SPEC-004's other entry point).
`.compile()` already rejects a structurally broken graph — but only when the
program *runs*. A pipeline wants the same verdict from a file, without running
anything: `strata graph validate --path graphs/`. This demo builds seven graphs
in code, exports them to JSON, and checks the folder with the real `strata`
binary.

Four of the graphs are healthy — `order-pipeline` (its `ManualReview` node is
reachable only through a recovery policy's fallback), `document-review` (a
guarded retry loop), `support-desk` (a branch that rejoins) and
`payout-approval` (an interrupt boundary whose reject route leads to a node
nothing else enters). Three each break a different rule: `refund-flow`
(unguarded cycle), `report-builder` (orphan node), `import-job` (entry point
names a node that doesn't exist).

Nothing here ever *runs* a graph; only their shape matters, so every graph uses
one trivial state type. The .NET twin is
[`strata-demos-dotnet/demos/14-graph-ci-gate`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/14-graph-ci-gate):
the same seven graphs, scenarios and checks, and byte-identical exports.

## What it demonstrates

- **`StateGraph::to_definition()` — export without validating.** The way from
  code to the CLI's file. `.compile()` can't be the export step: it returns
  `Err(StrataError::GraphInvalid(..))` for a broken graph, and a broken graph is
  what a gate exists to catch. The demo writes all seven to `graphs/*.json`
  *before* compiling any of them
- **Scenario 1 — a broken pull request.** One call, `strata graph validate
  --path graphs`, checks every `*.json` under the folder, reports each file
  (`OK` / `FAILED`) with its errors, and exits **1**. Then a table checking that
  for every graph the CLI (`--json` report) and `.compile()` return the *same*
  diagnostics
- **Scenario 2 — a trap the export avoids.** A compiled graph's public
  `definition` field holds only the edges the *executor* routes over. It has no
  edge into `ManualReview`, so the CLI would call it an orphan of a graph
  `.compile()` accepted. `to_definition()` (on the builder or the compiled
  graph) adds the fallback as a guarded `on_failure` edge, so both agree
- **Scenario 3 — an interrupt boundary in the file**
  (`design/48-Boundaries-In-Definitions.md`). `payout-approval` marks `Approve`
  with `.add_interrupt_boundary_with_reject("Approve", "Refuse")`. The export
  writes `"interrupt": { "timeout_ms": null }` on that node
  (`GraphNodeDef::interrupt`), the reject route as a guarded `on_reject` edge,
  and the level as `schema_version` `1.6.0` (the other six stay `1.0.0`).
  `strata graph view` prints the boundary and where a rejection goes
- **Scenario 4 — the fix.** Three small changes (guard the retry edge, wire
  `Archive` in, fix the entry point), same seven files: exit **0**, and all
  seven really do compile
- **Scenario 5 — when the gate can't check.** A `.json` file that isn't a graph
  definition, a hand edit that drops `Approve`'s `interrupt` but keeps its
  `on_reject` edge (a shape no engine writes; `GraphDefinition::check_shape()`,
  the check the CLI runs before validating, refuses it with the same message),
  an empty folder, a mistyped path: all exit **2**. A gate that couldn't look at
  something must not read as a pass
- **Exit codes:** `0` every graph valid · `1` structural errors · `2` could not
  check
- **Cross-language.** `--out <dir>` keeps the final export. The .NET twin
  writes **byte-identical** files for all seven graphs: checked with Strata 0.9
  by running both demos with `--out` and `diff -r` of the two folders (clean)

## What it does *not* cover

- **Composite nodes, joins, maps, waits, identity.** These graphs are flat. A
  definition can carry much more: parallel groups with their branch names and
  subgraphs with the child's whole definition (`1.1.0`, see
  [`13-subgraph-composition`](../13-subgraph-composition)'s step 7), joins
  (`1.2.0`), maps, waits, graph identity, and boundaries on any of them. The
  validator recurses into them and names a child's errors by path
  (`Remediate/Abort`)
- **A boundary's timeout and timeout route.** `payout-approval` has no timeout
  (`add_interrupt_boundary_with` and `BoundaryOptions`), so `timeout_ms` is
  `null` and there is no `on_timeout` edge; waits and timers are demo
  [`19-waits-and-timers`](../19-waits-and-timers)
- **A conditional edge's predicate.** The export records that an edge is
  *guarded* — which is all the validator needs — but not what the condition is
  (the engine keeps no text for it)
- **Dangling edges** (`STRATA_ERR_DANGLING_EDGE`, an edge to a name that isn't
  a node; see [`07-graph-validation`](../07-graph-validation)) and the
  composite-node rules (`STRATA_ERR_EMPTY_GROUP`, `STRATA_ERR_DUPLICATE_BRANCH`,
  `STRATA_ERR_INVALID_QUORUM`, `STRATA_ERR_BOUNDARY_IN_MAP`): the gate reports
  them the same way, but none of these graphs breaks them
- **What isn't exported**: a hop limit (`CompileOptions::with_max_node_hops`),
  node and branch timeouts, a group's concurrency limit. The gate can't see them
- **Running the graphs**, a real CI runner, or a real LLM. Everything is
  offline and deterministic

## Wiring it into CI

Your repo needs one small program (a test, an example, a `build.rs`, a tool)
that builds its graphs and writes `graph.to_definition()` as JSON — this demo's
`export_all` is that program. Then, with the `strata` binary on the runner
(the binary for its platform from the [`datalium/strata` release](https://github.com/datalium/strata/releases/tag/v0.9.0-preview.1),
checked against `SHA256SUMS.txt`):

```yaml
- run: cargo run -p export-graphs -- graphs/   # writes graphs/*.json via to_definition()
- run: strata graph validate --path graphs     # non-zero exit fails the job
```

## Run it

```bash
cargo run -p graph-ci-gate
cargo run -p graph-ci-gate -- --out ./graphs-rust     # also keep the final export
```

The demo runs the `strata` binary on your `PATH`, or the one
`STRATA_BIN=/path/to/strata` names. The CLI is not on crates.io: download
`strata-linux-x64`, `strata-macos-x64` or `strata-windows-x64.exe` from the
[`v0.9.0-preview.1` release](https://github.com/datalium/strata/releases/tag/v0.9.0-preview.1), rename it to `strata`
(`strata.exe` on Windows; `chmod +x` it elsewhere) and put it on the `PATH`.
Without one, the demo stops and says where to get it.

To repeat the cross-language check (the .NET twin also honours `STRATA_BIN`):

```bash
cargo run -p graph-ci-gate -- --out /tmp/graphs-rust
(cd ../strata-demos-dotnet && dotnet run --project demos/14-graph-ci-gate/14-graph-ci-gate.csproj -- --out /tmp/graphs-dotnet)
diff -r /tmp/graphs-rust /tmp/graphs-dotnet   # no output: all seven files are byte-identical
```

Output of a real run, on Windows (paths print with `/` on Linux and macOS; the
last line of scenario 5 quotes the operating system's own error text; blank
lines inside the indented CLI output are dropped, as in the .NET twin):

```
Using the strata CLI: strata 0.9.0-preview.1

== 1. A broken pull request: seven graphs exported, one CLI call ==

$ strata graph validate --path graphs
  OK      graphs\document-review.json
  FAILED  graphs\import-job.json  (1 structural error(s))
    [STRATA_ERR_MISSING_ENTRY] Load: Entry point node 'Load' does not exist in the graph definition.
  OK      graphs\order-pipeline.json
  OK      graphs\payout-approval.json
  FAILED  graphs\refund-flow.json  (1 structural error(s))
    [STRATA_ERR_UNGUARDED_CYCLE] Approve -> Request: Unguarded cycle detected between 'Approve' and 'Request'. Add a guarded/conditional edge to protect this loop.
  FAILED  graphs\report-builder.json  (1 structural error(s))
    [STRATA_ERR_ORPHAN] Archive: Node 'Archive' is isolated and unreachable from the entry point 'Collect'.
  OK      graphs\support-desk.json
  7 graph file(s) checked: 4 valid, 3 invalid, 0 unreadable.
  (exit code 1)

The CLI runs the same check as .compile() -- same diagnostics, every graph:

  graph             .compile()                    strata graph validate
  order-pipeline    ok                            ok
  document-review   ok                            ok
  support-desk      ok                            ok
  refund-flow       STRATA_ERR_UNGUARDED_CYCLE    STRATA_ERR_UNGUARDED_CYCLE
  report-builder    STRATA_ERR_ORPHAN             STRATA_ERR_ORPHAN
  import-job        STRATA_ERR_MISSING_ENTRY      STRATA_ERR_MISSING_ENTRY
  payout-approval   ok                            ok

== 2. Why export with to_definition(), not the `definition` field ==

order-pipeline's ManualReview node is reachable only through Charge's
fallback. The compiled graph's `definition` field holds just the edges the
executor routes over, so it has no edge into ManualReview:

  FAILED  trap\routing-edges-only.json  (1 structural error(s))
    [STRATA_ERR_ORPHAN] ManualReview: Node 'ManualReview' is isolated and unreachable from the entry point 'Validate'.
  OK      trap\to-definition.json
  2 graph file(s) checked: 1 valid, 1 invalid, 0 unreadable.

  a false alarm on a graph .compile() accepted; to_definition() adds the
  fallback as a guarded `on_failure` edge, so both agree.

== 3. An interrupt boundary is in the file, with its reject route ==

payout-approval.json is schema_version 1.6.0: a level with a boundary.
The Approve node, and the route a rejection takes, as exported:

  {"name":"Approve","metadata":null,"interrupt":{"timeout_ms":null}}
  {"from":"Approve","to":"Refuse","guarded":true,"condition":"on_reject"}

$ strata graph view --path graphs/payout-approval.json
  ┌─────────┐
  │ Prepare │
  └─────────┘
       │     
  ┌─────────┐
  │ Approve │
  └─────────┘
       │     
  ┌─────────┐
  │   Pay   │
  └─────────┘
  Additional edges:
    Approve -> Refuse  (guarded: on_reject)
  Composite nodes:
    Approve: interrupt boundary, rejects to Refuse

  Refuse has no ordinary edge in. Like ManualReview, it is reachable only through
  a route, and the exported on_reject edge is what keeps it from looking orphaned.

== 4. The fix: three small changes, same seven files ==

$ strata graph validate --path graphs
  OK      graphs\document-review.json
  OK      graphs\import-job.json
  OK      graphs\order-pipeline.json
  OK      graphs\payout-approval.json
  OK      graphs\refund-flow.json
  OK      graphs\report-builder.json
  OK      graphs\support-desk.json
  7 graph file(s) checked: 7 valid, 0 invalid, 0 unreadable.
  (exit code 0)

All seven also compile -- the gate said yes to graphs the runtime accepts.

== 5. When the gate can't check something, it doesn't pass ==

a file in graphs/ that isn't a graph definition:
  OK      graphs\document-review.json
  OK      graphs\import-job.json
  ERROR   failed to parse 'graphs\notes.json' as a graph definition: missing field `schema_version` at line 1 column 29
  OK      graphs\order-pipeline.json
  OK      graphs\payout-approval.json
  OK      graphs\refund-flow.json
  OK      graphs\report-builder.json
  OK      graphs\support-desk.json
  8 graph file(s) checked: 7 valid, 0 invalid, 1 unreadable.
  (exit code 2)

a hand edit that drops Approve's `interrupt` but keeps its on_reject edge:
  ERROR   'edited\payout-approval.json' is not a graph definition: node 'Approve' has an on_reject edge but is not an interrupt boundary
  1 graph file(s) checked: 0 valid, 0 invalid, 1 unreadable.
  (exit code 2)
  Rust's GraphDefinition::check_shape() agrees: node 'Approve' has an on_reject edge but is not an interrupt boundary

an empty folder:
  strata: no graph definition files (*.json) found under 'empty'  (exit code 2)

a mistyped path:
  strata: failed to read 'graphz': The system cannot find the file specified. (os error 2)  (exit code 2)

Exit codes: 0 every graph valid | 1 structural errors | 2 could not check.
```

The demo checks every exit code, the exact set of failing files and error
codes, the CLI-versus-`.compile()` agreement, the trap, the boundary in the
export and in `graph view`, and the refused hand edit (that the CLI's message is
`check_shape()`'s), and exits non-zero if any of them changes.

## Files

- `src/main.rs` — the whole demo, single file
- `Cargo.toml` — package name is `graph-ci-gate` (folder keeps the `14-`
  prefix; Cargo package names can't start with a digit). `serde_json` is used
  to write the exports and read the CLI's `--json` report. No tokio: nothing
  runs a graph
