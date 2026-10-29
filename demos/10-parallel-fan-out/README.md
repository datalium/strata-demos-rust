# 10 — parallel-fan-out

Parallel fan-out / fan-in (`design/14-Parallel-Execution.md`): a trip planner
(`ParseRequest` -> `GatherInfo` -> `BuildItinerary`) whose `GatherInfo` node is
a **parallel group**. It asks three simulated services at once (flights,
hotels, weather) and a reducer merges what they return. To the rest of the
graph the group is one ordinary node, with ordinary edges in and out.

The services take different amounts of time (flights 300 ms, hotels 200 ms,
weather 100 ms) so the demo can watch what a parallel group actually
promises: real overlap, a merge order that doesn't depend on timing, failures
handled at the level of the group, and branches that stream.

`01`-`09` were strictly sequential: the executor followed one edge at a time.
This is the first demo where two things run at once. It covers the basic
group, with no options; [`17-parallel-joins`](../17-parallel-joins) covers
what `ParallelGroupOptions` adds (`design/26-Parallel-Group-Controls-And-Joins.md`).
The .NET twin is
[`strata-demos-dotnet/demos/10-parallel-fan-out`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/10-parallel-fan-out).

## What it demonstrates

- `.add_parallel_group(name, branches, reducer)` with a `ParallelBranches`
  builder. Each branch here is **async**,
  `.async_branch(name, |state: S, ctx: NodeContext| async { .. })`
  (`design/17-Async-Execution.md`): it gets its own clone of the state as it
  was when the group started and returns its own result state, so branches
  can't see (or race with) each other
- **Real concurrency, shown without a wall clock.** The demo runs on a
  **paused tokio clock** (`#[tokio::main(flavor = "current_thread", start_paused = true)]`):
  no real time passes, and the clock jumps to the next timer whenever every
  task is waiting, so elapsed time is exact. Scenario 1 checks that the
  group took **exactly 300 ms** (the slowest branch, not the 600 ms sum) and
  that the peak number of branches in flight was 3 of 3. All three ran on one
  thread: an async branch is a task and holds no thread while it waits (a sync
  `.branch(name, |s: &S| ..)` gets a thread from tokio's blocking pool instead)
- **A deterministic merge**: the services *finish* in the order weather,
  hotels, flights, but the reducer receives their results in **declaration
  order** (flights, hotels, weather), so the merged log is identical on every
  run. The reducer also gets the entry state, so each branch's contribution can
  be a delta (here: the log lines added after the entry log)
- **Failure is handled at the group level.** A branch fails by returning
  `Err(NodeError::Transient(..))` / `Err(NodeError::Fatal(..))`. **The first
  failure fails the group at once and cancels the siblings still running**:
  their `ctx.cancellation()` token fires and the group drops their futures
  mid-`sleep`. A drop guard in the service call notices this, and checks that
  the token had fired. The group is a fallible node, so it reuses
  `RecoveryPolicy` from [`08-error-recovery`](../08-error-recovery):
  - **Scenario 2, a transient blip**: hotels times out once at 200 ms, which
    cancels the flights call in flight. `RecoveryPolicy::retry(2)` re-runs
    **all three** branches (6 service calls, not 4), so branches must be safe
    to repeat. The retry starts at 200 ms, without waiting for flights, and
    the run ends at exactly 500 ms. The failed attempt's log lines are
    discarded
  - **Scenario 3, a fatal error + fallback**: hotels has no rooms. Weather had
    already answered and flights is cancelled; what finished is thrown away
    with the failed group, and `UseCachedPlan` runs on the state as it was
    *before* the group. A fatal error skips all 3 retries
  - **Scenario 4, a fatal error, no fallback**: the `StrataError::NodeFailed`
    message names the failing branch (`branch 'hotels': no rooms left on these
    dates`), and not the cancelled one
- **Scenario 5, streaming branches**: the same branches added with
  `.async_streaming_branch(..)` and run under `run_streaming` push tokens
  through `ctx.stream()`, concurrently on the run's one bounded channel. Each
  token's `node_name` is `GatherInfo/<branch>`, one branch's tokens stay in
  order, and only the group gets `NodeStart`/`NodeEnd` chunks: branches aren't
  nodes
- **Compile-time check**: a group with two branches of the same name (or none)
  is rejected by `.compile()` with `StrataError::CompilationFailed`

## What it does *not* cover

- **Group options**: a concurrency limit (`with_max_concurrency`), a timeout
  per branch (`with_branch_timeout`), letting siblings finish after a failure
  (`with_cancel_siblings_on_failure(false)`), keeping partial results
  (`keep_succeeded()`), and joins other than "every branch"
  (`Join::first_success`, `first_matching`, `quorum`). See
  [`17-parallel-joins`](../17-parallel-joins)
- **Fan-out over a list decided at runtime**: a group's branch set is fixed
  when the graph is built. `add_map` runs a child graph per item; see
  [`18-map-fan-out`](../18-map-fan-out)
- **Branches are closures, not graph nodes.** They have no edges of their
  own, no per-branch recovery policy, and no interrupt boundaries. The graph
  definition lists them by name (see [`14-graph-ci-gate`](../14-graph-ci-gate)),
  but fan-out written as several outgoing `.add_edge(..)`s from one node isn't
  supported. A branch can't hold a group
- **A sync branch being cancelled.** A `.branch(..)` runs on tokio's blocking
  pool and can't see its token: when the group gives up on it, it runs on
  detached and its result is discarded. The demo uses async branches so the
  cancellation is visible
- **A group waiting for the branches it gave up on.** It doesn't: it cancels
  their tokens, aborts their tasks and returns. On this current-thread runtime
  an aborted task is dropped the next time the runtime gets a turn, so when a
  run ends right after the group failed (scenarios 3 and 4) the demo
  yields once (`tokio::task::yield_now()`) before it counts the cancelled call
- **No live service.** The services are a `tokio::time::sleep` and a canned
  answer, so the demo runs offline

## Differences from the .NET twin

- **Time.** The .NET twin waits on the real clock (`Task.Delay`), so its
  millisecond figures vary from run to run, and it checks peak in flight but
  not the elapsed time. This demo runs on tokio's paused clock, so every
  figure below is exact and the same on every run, and scenarios 1 and 2
  check the elapsed time exactly. That needs tokio's `test-util` feature
  (in `Cargo.toml`)
- **Cancellation is a dropped future, not an exception.** In .NET the
  cancelled `Task.Delay(latency, ct)` throws, and the service catches it.
  Here the group drops the branch's future, so no code after the `.await`
  runs. A drop guard (`InFlight`) records the cancellation instead

## Run it

```bash
cargo run -p parallel-fan-out
```

Takes well under a second: no real time passes on the paused clock. The output
is identical on every run:

```
=== Scenario 1: all three services healthy ===
    [   0 ms] flights call 1: started
    [   0 ms] hotels call 1: started
    [   0 ms] weather call 1: started
    [ 100 ms] weather call 1: answered
    [ 200 ms] hotels call 1: answered
    [ 300 ms] flights call 1: answered
  Completed in 300 ms of tokio time.
  Sequentially the three calls would take 600 ms (300 + 200 + 100).
  Peak branches in flight: 3 of 3 -- they really overlapped.
  Completion order:  weather, hotels, flights
  Merged log (declaration order, identical on every run):
    - request: trip to Lisbon
    - flights: LH 401 for $612
    - hotels: Hotel Aurora at $148/night
    - weather: sunny, 24C
  Summary: Lisbon: LH 401, $612 | Hotel Aurora, $148/night | sunny, 24C

=== Scenario 2: hotels times out once -- retry(2) on the group ===
    [   0 ms] flights call 1: started
    [   0 ms] hotels call 1: started
    [   0 ms] weather call 1: started
    [ 100 ms] weather call 1: answered
    [ 200 ms] hotels call 1: transient error: hotels timed out
    [ 200 ms] flights call 1: cancelled (the group gave up on it)
    [ 200 ms] flights call 2: started
    [ 200 ms] hotels call 2: started
    [ 200 ms] weather call 2: started
    [ 300 ms] weather call 2: answered
    [ 400 ms] hotels call 2: answered
    [ 500 ms] flights call 2: answered
  Completed in 500 ms. Service calls -- flights: 2, hotels: 2, weather: 2.
  The failure cancelled flights mid-call (1 cancelled), and the retry re-ran *all three*: 6 calls in total, not the 4 a hotels-only retry would make. Branches must be safe to repeat.
  Summary: Porto: LH 401, $612 | Hotel Aurora, $148/night | sunny, 24C

=== Scenario 3: hotels has no rooms -- retry(3), fallback 'UseCachedPlan' ===
    [   0 ms] flights call 1: started
    [   0 ms] hotels call 1: started
    [   0 ms] weather call 1: started
    [ 100 ms] weather call 1: answered
    [ 200 ms] hotels call 1: fatal error: no rooms left on these dates
    [ 200 ms] flights call 1: cancelled (the group gave up on it)
  Fell back after 3 service call(s) in total -- a fatal error skips all 3 retries.
  Weather had already answered; flights was cancelled mid-call (1 cancelled). What finished is discarded with the failed group:
    flight: None, hotel: None, forecast: None
  Log: ["request: trip to Madeira"]
  Summary: Madeira: cached plan (live lookup unavailable)

=== Scenario 4: same failure, no fallback -- the error names the branch ===
    [   0 ms] flights call 1: started
    [   0 ms] hotels call 1: started
    [   0 ms] weather call 1: started
    [ 100 ms] weather call 1: answered
    [ 200 ms] hotels call 1: fatal error: no rooms left on these dates
    [ 200 ms] flights call 1: cancelled (the group gave up on it)
  Failed: group 'GatherInfo' gave up after 1 attempt(s) -- branch 'hotels': no rooms left on these dates
  The cancelled flights call is not named: it didn't fail, it was stopped.

=== Scenario 5: streaming branches under run_streaming ===
    [   0 ms] flights call 1: started
    [   0 ms] hotels call 1: started
    [   0 ms] weather call 1: started
    [ 100 ms] weather call 1: answered
    [ 200 ms] hotels call 1: answered
    [ 300 ms] flights call 1: answered
  Tokens, grouped by the node name they arrived with (each branch's own stay in order):
    GatherInfo/flights asking... | flights: LH 401 for $612
    GatherInfo/hotels  asking... | hotels: Hotel Aurora at $148/night
    GatherInfo/weather asking... | weather: sunny, 24C
  NodeStart chunks: ParseRequest, GatherInfo, BuildItinerary -- branches are not nodes and get none.
  Summary: Faro: LH 401, $612 | Hotel Aurora, $148/night | sunny, 24C

=== Step 6: a malformed group is rejected at .compile(), not at runtime ===
  Rejected: parallel group 'GatherInfo' has more than one branch named 'flights'

=== Done: three services ran at once, merged in a fixed order, a failing branch was handled at the group level, and branches streamed. ===
```

The demo checks the parts that must hold: the exact elapsed time, peak in
flight, completion order, merged log, call and cancellation counts, that every
cancelled call saw its token fire, the failure message, the streamed tokens'
attribution and the lifecycle chunks. If a check fails, it prints
`check failed: ...` and exits with code 1.

## Files

- `src/main.rs`: the whole demo, single file
- `Cargo.toml`: package name is `parallel-fan-out` (the folder keeps the `10-`
  prefix; Cargo package names can't start with a digit). Depends on
  `datalium-strata`, `datalium-strata-memory` (for `InMemoryCheckpointStore`),
  `serde`, and `tokio` with `test-util` for the paused clock
