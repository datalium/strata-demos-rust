# 18 — map-fan-out

Dynamic fan-out with a map (`design/27-Map-Fan-Out.md`). An accounts-payable
check: an invoice arrives with however many lines it has, and `AuditLines`
runs the same compiled child graph (`Classify` -> `CheckPrice` -> `Score`)
once per line before `Decide` approves the invoice or holds it for review.

```
AuditLines (map: one LineAudit child per invoice line, keyed by line id) -> Decide
```

How it differs from its neighbours: demo [10](../10-parallel-fan-out)'s
parallel group has branches fixed when the graph is built, and demo
[13](../13-subgraph-composition)'s subgraph runs one child graph inside one
node. A map reads a list from the state when the node starts, so how many
children run is only known at run time. To the parent it is still one node
and one hop.

The .NET twin is
[`strata-demos-dotnet/demos/18-map-fan-out`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/18-map-fan-out):
same invoices, scenarios and checks, and the same output apart from Rust
names (see "Where the engines differ" below).

## What it demonstrates

- **`.add_map(name, items, key, child, input, reducer)`** and
  **`.add_map_with(..., options)`**: `items` reads the lines from the state
  (`Fn(&S) -> Vec<I>`), `key` names each one (`Fn(&I) -> String`), `child` is
  an `Arc<CompiledGraph<LineAudit>>`, `input` builds each child's start state
  (`Fn(&S, I) -> T`), and the plain reducer gets the children's final states
  as a `Vec<T>` **in list order**
- **The fan-out** (scenario 1): 12 lines, `RunEventKind::MapExpanded` with
  every key in list order before any item starts, items started
  (`BranchStarted`) in list order, each item's nodes at
  **`AuditLines/<line id>/<node>`**, and 36 child nodes run (12 x 3)
- **`max_concurrency` is 8 by default and never unbounded** (scenarios 1 and
  1b): the price service sees exactly 8 lookups in flight with the default
  (`MapOptions::DEFAULT_MAX_CONCURRENCY`), and exactly 3 with
  `.with_max_concurrency(3)`. It is a `usize`, not an `Option<usize>` as on
  `ParallelGroupOptions`, and `.compile()` refuses `0` (step 5)
- **The required key, and what fails the map before anything starts**
  (scenario 2): a duplicate key, a key containing `/`, an empty key, or more
  items than `.with_max_items(50)` fail the map **fatally**, with no
  `MapExpanded`, no item started and no lookup made. The list comes from the
  state, so a retry would see the same list: `RecoveryPolicy::retry(3)` on the
  map doesn't run
- **A failing item, and how its kind propagates** (scenario 3). A child
  failure fails the item with the child's kind, and the map with the group
  runner's rule:
  - an unknown SKU is a `NodeError::Fatal` in the child's `CheckPrice`;
    neither the child's `retry(1)` nor the map's `retry(2)` runs, and the
    message names the item and the child node: `item 'L03': failed at
    'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'`
  - a busy price service is `NodeError::transient_after(.., 2 s)`. The
    child's own `retry(1)` runs out first, then the item and the map fail
    transiently, and the map's `Failed` checkpoint carries
    `error_kind = transient` and `strata.retry_after_ms = 2000`
  - with `retry(1)` on the map, the retry emits a second `MapExpanded`
    (attempt 2) and re-runs **every** line, the one that had succeeded
    included: there is no per-item retry
- **`.add_map_with_outcomes(...)` with `MapOptions::new().keep_succeeded()`**
  (scenario 4): the reducer sees every line's `BranchOutcome` (`name` is the
  key) in list order, with `join_met() == false` and an empty `deciding()`.
  The line that failed is flagged for review with its own error and kind, and
  no other line is cancelled because of it (no `BranchCompleted` with
  `BranchResult::Cancelled`)
- **`.compile()` refuses a child with an interrupt boundary** (step 5): a map
  item can't freeze for a human, so `StrataError::CompilationFailed("map
  'AuditLines' runs a graph with an interrupt boundary at 'Score'; a map item
  can't freeze")`. The check looks into subgraphs inside the child too
  (`'Review/Score'`)

The concurrency peaks are exact, not a matter of timing: the price list holds
each lookup (on a `tokio::sync::watch` gate) until the expected number are in
flight together, then lets everything through.

### Where the engines differ

- **`max_concurrency = 0`**: .NET's `MapOptions` setter throws
  `ArgumentOutOfRangeException` as you build the options. Rust's
  `.with_max_concurrency(0)` is accepted by the builder and refused by
  `.compile()` with `CompilationFailed("map 'AuditLines' has a
  max_concurrency of 0; it must be at least 1")`, which the demo checks
- **Results and errors are values**: `.compile()` returns a `Result`, a run
  returns `GraphExecutionResult::Failed(StrataError::NodeFailed { node,
  attempts, message })`, and a failure's kind is the `NodeError` variant
  (`Fatal`, `Transient`, `TransientAfter`) rather than a `NodeException`
  factory. The demo prints kinds lowercase (`fatal`), as the checkpoint's
  `error_kind` has them
- **Events** are collected through a `RunEventChannel` observer, whose
  receiver ends after `RunCompleted`; the .NET twin writes an `IRunObserver`
  and waits for `RunCompleted` itself
- **Inline nodes**: `Classify`, `Score` and `Decide` only compute, so they
  are `.add_inline_node(...)` (Rust only, `design/39-Inline-Sync-Handlers.md`)
  and run on the executor instead of hopping to tokio's blocking pool. Only
  `CheckPrice`, the node that waits, is async (`.add_async_fallible_node(...)`)
- **Money** is integer cents (`i64`): there's no decimal type in the engine's
  dependency tree, and the demo adds no crate for one

## What it does *not* cover

- **Joins on a map** (`.with_join(Join::first_success())`,
  `Join::first_matching(..)`, `Join::quorum(n)`): a map takes the same joins
  as a group, and demo [17](../17-parallel-joins) shows them on a group
- **`.with_item_timeout(..)`, `.with_cancel_siblings_on_failure(false)`, and
  items winding down.** An item the map gives up on stops at its current
  child node and is waited for, so its child's `NodeCompleted` comes before
  the item's `BranchCompleted`; `rust/strata-core/tests/map.rs` in
  `datalium/strata` covers it
- **Streaming from items** (chunks named `<map>/<key>/<node>`), a map inside a
  subgraph, the cancelled-run checkpoint, and the hop limit per item
- **The exported definition** (`kind: "map"` with the child as a template,
  `schema_version` `1.3.0`)
- **No live service.** The price list is an in-memory `match`

## Run it

```bash
cargo run -p map-fan-out
```

Takes well under a second, on tokio's real clock: no retry here has a
backoff, so the 2 s retry hint is recorded but never slept on. Output
(identical on every run):

```
=== Scenario 1: a 12-line invoice -- one child graph per line, default options ===
  MapExpanded: [L01, L02, L03, L04, L05, L06, L07, L08, L09, L10, L11, L12]
  Items started in list order: L01, L02, L03, L04, L05, L06, L07, L08, L09, L10, L11, L12
  Peak price lookups in flight: 8 (MapOptions::DEFAULT_MAX_CONCURRENCY = 8); the other 4 lines waited for a slot.
  One item's child nodes: AuditLines/L07/Classify -> AuditLines/L07/CheckPrice -> AuditLines/L07/Score
  Child nodes run: 36 (12 lines x 3 nodes)
  Reducer got the audits in list order:
    L01 PAP-A4   paper      score 100
    L02 PAP-A3   paper      score 100
    L03 INK-K    toner      score 100
    L04 INK-C    toner      score 100
    L05 PEN-BLU  stationery score 100
    L06 PEN-RED  stationery score 100
    L07 PAD-A5   paper      score 100
    L08 FLD-GRN  stationery score 100
    L09 INK-M    toner      score  52  billed 79.50, list 71.00 (+12%)
    L10 SVC-INST service    score 100
    L11 PAP-A4   paper      score 100
    L12 SVC-DLV  service    score 100
  Decision: hold: L09 billed 79.50, list 71.00 (+12%)

=== Scenario 1b: the same invoice with max_concurrency = 3 (the price API's own limit) ===
  Peak price lookups in flight: 3; items still started in list order: true.

=== Scenario 2: bad lists fail before any line is looked at -- even with retry(3) on the map ===
  an exported row repeated   -> fatal, attempts 1, lookups 0: map 'AuditLines': items 1 and 2 have the same key 'L02'
  a split line               -> fatal, attempts 1, lookups 0: map 'AuditLines': item 1 has the key 'L02/1', which contains '/'
  a line with no id          -> fatal, attempts 1, lookups 0: map 'AuditLines': item 1 has an empty key
  60 lines, limit 50         -> fatal, attempts 1, lookups 0: map 'AuditLines' has 60 items, more than its limit of 50

=== Scenario 3a: an unknown SKU (fatal) -- retry(2) on the map, retry(1) on CheckPrice inside the child ===
  Failed at 'AuditLines' after 1 attempt(s), fatal:
    item 'L03': failed at 'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'
  Fatal in the child, fatal for the item, fatal for the map: neither retry policy ran.

=== Scenario 3b: the price API is busy (transient, retry in 2 s) -- one request at a time ===
  No retry on the map: failed, transient, strata.retry_after_ms = 2000:
    item 'L02': failed at 'CheckPrice' after 2 attempt(s): price service busy (429)
  Lookups: L01, L02, L02 -- L03 never started.
  With retry(1) on the map: approve. MapExpanded on attempts 1 and 2.
  Lookups: L01, L02, L02, L01, L02, L03 -- the retry re-ran L01, which had succeeded.

=== Scenario 4: PartialResults::KeepSucceeded -- audit what can be audited, flag the rest ===
  Reducer saw (list order): L01 Succeeded, L02 Succeeded, L03 Failed, L04 Succeeded, L05 Succeeded; join_met: false
  L03's error: fatal, "failed at 'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'"
  Decision: hold: 2 line(s) need review
    - L03 not audited: failed at 'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'
    - L05 billed 52.00, list 45.00 (+16%)

=== Step 5: a child graph that could pause for a human is refused at .compile() ===
  Refused: map 'AuditLines' runs a graph with an interrupt boundary at 'Score'; a map item can't freeze
  Refused: map 'AuditLines' runs a graph with an interrupt boundary at 'Review/Score'; a map item can't freeze
  Refused: map 'AuditLines' has a max_concurrency of 0; it must be at least 1
  (max_concurrency is a usize, not an Option: a map is never unbounded.)

=== Done: one child graph per line, bounded, keyed, and failing the way its children failed. ===
```

The demo checks each claim it prints (keys, start order, peaks, paths,
messages, error kinds, retry hints, lookup sequences, statuses) and exits
non-zero if one fails.

## Files

- `src/main.rs` — the whole demo: one `async fn` per scenario, the parent and
  child graphs, a `PriceList` that records lookups and their peak in flight,
  and helpers that run a graph with a `RunEventChannel` and read its events
- `Cargo.toml` — depends on `datalium-strata` and `datalium-strata-memory`
  (for `InMemoryCheckpointStore`) from crates.io,
  plus `serde` and `tokio`
