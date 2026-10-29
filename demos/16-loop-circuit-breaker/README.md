# 16 — loop-circuit-breaker

The loop circuit breaker, STRATA-SPEC-004's max node hops
(`design/24-Loop-Circuit-Breaker.md`). A meeting-scheduling agent loops
`ProposeSlot` -> `CheckCalendars` -> back to `ProposeSlot` while someone is
busy, and goes on to `SendInvite` once a slot is free. The back edge is
guarded, so `.compile()` accepts the graph. Validation is structural and
can't tell whether the guard ever turns false
([`07-graph-validation`](../07-graph-validation) shows what it does catch).
When an attendee is on leave all week, the guard never turns false and the
agent re-plans forever. In a real agent, every pass is an LLM call.
`.compile_with(CompileOptions::new().with_max_node_hops(12))` stops it.

The .NET twin is
[`strata-demos-dotnet/demos/16-loop-circuit-breaker`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/16-loop-circuit-breaker):
same story, same scenarios, same numbers.

## What it demonstrates

- **A loop that converges never notices the limit** (scenario 1). Alice +
  Ravi is booked in 7 hops (three proposals, three checks, one invite).
  `graph.max_node_hops()` is `Some(12)`
- **A loop that doesn't converge is refused before hop 13** (scenario 2).
  Alice + Bob + Dana ends with
  `Failed(StrataError::HopLimitExceeded { node: "ProposeSlot", hops: 12, limit: 12 })`.
  The refused node never runs. It is a `StrataError` inside `Failed`, so a
  `match` arm that already handles `Failed(_)` catches it. The `Failed`
  checkpoint sits at the refused node (`suspended_node` and `next_node` both
  `ProposeSlot`), with `error_kind = hop_limit`, the message in `error`,
  `strata.hops = 12`, no `attempts` (nothing was attempted), and the last
  committed state (six proposals)
- **A retry isn't a hop** (scenario 3). `CheckCalendars` is an
  `add_async_fallible_node` with `RecoveryPolicy::retry(1)` whose first attempt
  (`ctx.attempt() == 1`) fails with `NodeError::Transient`. That makes 10
  handler calls, but only 7 nodes start. A limit of exactly 7 completes. A
  limit of 6 trips at `SendInvite`. With a limit of N, the N-th node runs, and
  only the node that would be N+1 is refused
- **The count survives interrupts and resumes** (scenario 4). With an
  interrupt boundary on `ProposeSlot` (the organizer reviews each proposal),
  the thread freezes six times. Each checkpoint carries `strata.hops` (1, 3,
  5 … 11), and each `resume` is made by a **freshly built graph**, as another
  process would make it. The thread trips at the same node, after the same 12
  node starts, as the run without interrupts
- **`RunConfig::with_max_node_hops` replaces the limit for one run**
  (scenario 5):
  - resuming the tripped thread as it is trips again at once, and runs nothing
  - an operator drops Dana with a typed override
    (`HumanSignal::approve(..).with_override("attendees", json!([..]))`) and
    calls `resume_with(.., RunConfig::new().with_max_node_hops(20))`. The count
    goes on from the stored 12 (hops 13-15), and the meeting is booked
  - a test can `run_with` the production graph and a *lower* limit (5)
  - a graph compiled with no limit (`max_node_hops()` is `None`) gets one for
    a single run (4)
- **A limit below 1 is refused** (step 6): `compile_with` returns
  `Err(StrataError::CompilationFailed("max_node_hops must be at least 1"))`,
  and `run_with` a per-run 0 returns `Failed(ExecutionAborted(..))` without
  writing anything

### Where the Rust engine differs from .NET

- **The trip is an error variant, not a result subclass.** .NET returns
  `GraphExecutionResult.HopLimitExceeded`, which derives from `Failed`; Rust
  returns `GraphExecutionResult::Failed(StrataError::HopLimitExceeded { .. })`.
  Either way, code that handles failure already catches it
- **`StrataError` has no `Display`**, so the human-readable message the demo
  prints is the `error` the engine wrote on the `Failed` checkpoint. The demo
  checks that it matches the error's `node`, `hops` and `limit` every time
- **A zero limit**: .NET's `Compile` throws `ArgumentOutOfRangeException`;
  Rust's `compile_with` returns `Err(CompilationFailed)`. The demo also shows
  the per-run refusal, which the .NET demo doesn't
- **The attempt number** comes from the async handler's `NodeContext`
  (`ctx.attempt()`): a sync `add_fallible_node` gets no context in Rust

## What it does *not* cover

- **Limits inside subgraphs.** Each compiled graph counts its own hops. A
  subgraph node is one hop to its parent, and a child that trips its own limit
  fails the subgraph node fatally (`NodeFailed`, `error_kind = fatal`), with
  its `hop_limit` checkpoint nested. The per-run override reaches the
  outermost graph only. See `rust/strata-core/tests/hop_limit.rs` in
  `datalium/strata`. [`13-subgraph-composition`](../13-subgraph-composition) covers
  subgraphs
- **Fallbacks and parallel groups.** A fallback node is a hop of its own, and
  a parallel group is one hop however many branches it runs
- **Telemetry.** The trip is also a `RunEventKind::HopLimitExceeded` run
  event, a `strata.graph.hop_limit_exceeded` counter and
  `error.type = hop_limit` on the run span
  ([`27-run-observability`](../27-run-observability) shows events and
  telemetry). `strata thread inspect` prints `Hops:`
- **Per-node visit limits, and token or cost budgets.** The breaker counts all
  node starts at a level. To bound time, use `RunConfig::with_deadline` (see
  [`15-async-and-cancellation`](../15-async-and-cancellation))
- **A live LLM.** The "agent" proposes slots in a fixed order, and the
  calendars are a `match`

## Run it

```bash
cargo run -p loop-circuit-breaker
```

It finishes in well under a second, and the output is identical on every run:

```
Calendars: Ravi is busy Mon 09:00 and Mon 14:00; Dana is on leave all week.
The agent proposes, in order: Mon 09:00, Mon 14:00, Tue 10:00, Tue 15:00, Wed 11:00, then starts over.

=== Scenario 1: Alice + Ravi -- the loop converges, the limit is never reached ===
  Compiled with max_node_hops = Some(12).
  Tried: Mon 09:00 Ravi busy | Mon 14:00 Ravi busy | Tue 10:00 free
  Nodes started: 7 (ProposeSlot 3, CheckCalendars 3, SendInvite 1). Invite: 'Q4 planning' Tue 10:00 for Alice, Ravi

=== Scenario 2: Alice + Bob + Dana -- the agent re-plans until the breaker trips ===
  HopLimitExceeded: node 'ProposeSlot', hops 12, limit 12
  Message: hop limit of 12 reached: node 'ProposeSlot' would be hop 13
  It is a StrataError inside Failed, so an existing `Failed(_)` arm catches it: true
  Nodes started: 12 (ProposeSlot 6, CheckCalendars 6, SendInvite 0) -- the 13th never ran.
  Checkpoint: Failed at ProposeSlot, 6 proposals in the frozen state
    metadata { error = hop limit of 12 reached: node 'ProposeSlot' would be hop 13, error_kind = hop_limit, strata.hops = 12 }

=== Scenario 3: the calendar API fails once per check -- retry(1) on CheckCalendars ===
  max_node_hops = 7: completed. Handler calls: 10 (ProposeSlot 3, CheckCalendars 6, SendInvite 1); nodes started: 7.
  max_node_hops = 6: hop limit of 6 reached: node 'SendInvite' would be hop 7
  With a limit of N the N-th node runs; only the node that would be N+1 is refused.

=== Scenario 4: the organizer reviews every proposal -- an interrupt boundary inside the loop ===
  run       -> Interrupted after ProposeSlot (proposal 1: Mon 09:00), strata.hops = 1
  resume 1  -> Interrupted after ProposeSlot (proposal 2: Mon 14:00), strata.hops = 3
  resume 2  -> Interrupted after ProposeSlot (proposal 3: Tue 10:00), strata.hops = 5
  resume 3  -> Interrupted after ProposeSlot (proposal 4: Tue 15:00), strata.hops = 7
  resume 4  -> Interrupted after ProposeSlot (proposal 5: Wed 11:00), strata.hops = 9
  resume 5  -> Interrupted after ProposeSlot (proposal 6: Mon 09:00), strata.hops = 11
  resume 6  -> hop limit of 12 reached: node 'ProposeSlot' would be hop 13
  Nodes started across 7 runs: 12 (ProposeSlot 6, CheckCalendars 6, SendInvite 0) -- the same trip as scenario 2.

=== Scenario 5: RunConfig::with_max_node_hops -- one run's limit, higher or lower ===
  Resuming scenario 2's thread as it is: hop limit of 12 reached: node 'ProposeSlot' would be hop 13 (nodes run: 0)
  Resumed with attendees = [Alice, Bob] and RunConfig::new().with_max_node_hops(20):
    nodes run: 3 (hops 13-15, counted on from the stored 12). Invite: 'Offsite' Mon 14:00 for Alice, Bob
  A test runs the bookable meeting with RunConfig::new().with_max_node_hops(5): hop limit of 5 reached: node 'CheckCalendars' would be hop 6
  A graph compiled with no limit (max_node_hops = None) gets one for a run: hop limit of 4 reached: node 'ProposeSlot' would be hop 5

=== Step 6: a limit below 1 is refused, by compile_with and by run_with ===
  compile_with: CompilationFailed("max_node_hops must be at least 1")
  run_with:     Failed(ExecutionAborted("a run's max_node_hops must be at least 1")), nothing written

=== Done: the breaker stopped a loop that never converged, counted through retries and interrupts, and let one run go further. ===
```

Every claim above is checked at runtime. If one stops holding, the demo
prints `check failed: ...` and exits with code 1.

## Files

- `src/main.rs` — the whole demo: the scenarios, the scheduling graph
  (`build`), and a per-node call counter that tells handler calls (retries
  included) from node starts (hops)
- `Cargo.toml` — package name is `loop-circuit-breaker`; depends on
  `datalium-strata`, `datalium-strata-memory` (for `InMemoryCheckpointStore`),
  `serde`, `serde_json` and `tokio`
