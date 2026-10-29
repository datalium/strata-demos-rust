# 24 — model-routes

Model routes (`design/46-Model-Routes.md`): `ModelRoute` in
`datalium-strata-ai`, a `ChatClient` over ordered candidate clients. It asks
them in order and moves on when one fails transiently, refuses, or gives an
answer the route's `with_accept` rejects. A fatal fault stops it. With
`with_hedge_after`, a slow candidate gets the next one started beside it, and
the first accepted answer wins. A `ModelHealth` shared between routes skips a
provider that is cooling down or whose breaker is open. When nothing answers,
the node fails transiently with the shortest wait as its retry hint, and the
node's `RecoveryPolicy` waits that long.

Every route sits behind an ordinary model node (`add_model_node`, which calls
`complete_with`), so each attempt is a model call of the run, and the route
records one `ModelRouted` run event per call. The demo reads that event to
show which candidate answered and what happened to each of the others.

**The models are stand-ins.** `FakeModel` is a small scripted `ChatClient`:
each turn answers, fails with an HTTP status and an optional `Retry-After`
(`ChatError::status`), refuses (finish reason `content_filter`), or hangs until
its call is dropped. It names a provider (`ChatClient::provider`), which is
where the route's default health key comes from, and a default model. No
network, no API key, and every run prints the same thing.

The .NET twin is
[`strata-demos-dotnet/demos/24-model-routes`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/24-model-routes).
It has the same six scenarios and the same checks. Where the engines differ:

- **One paused clock instead of a real one plus a fake one.** .NET hedges on
  the real clock (50 ms) and runs the breaker and the retry on a
  `FakeTimeProvider`, which it moves in 500 ms steps until the retry fires.
  Rust's `ModelHealth`, the hedge timer and the retry backoff all read tokio's
  clock, so the whole demo runs on a paused one
  (`#[tokio::main(flavor = "current_thread", start_paused = true)]`, which
  needs tokio's `test-util` feature). Time moves only when every task waits
  on a timer: the hedge starts at exactly 50 ms, the breaker's 60 s pass with
  `tokio::time::advance`, and the retry's 3 s wait passes by itself. So
  scenario 6 checks `a`'s remaining cooldown exactly (`5 s`), which the .NET
  twin can't
- **Labels.** Outcomes and health states print as the engine's own labels
  (`ModelRouteOutcome::as_str`, `ModelHealthState::as_str`: `lost_race`,
  `cooling_down`, `half_open`), where .NET prints its enum names
  (`LostRace`, `CoolingDown`, `HalfOpen`)
- **Method shapes.** A route is built with `ModelRoute::new(name)` and
  `.candidate(...)`. An eligibility rule is `ModelCandidate::with_eligible`
  (it gets the request and `Option<&NodeContext>`). The breaker is
  `ModelHealthOptions::new().with_breaker(2, 60 s)`, and the backoff takes a
  `Jitter` (`Jitter::None` here). The demo reads `ModelRouted` from a
  `RunEventChannel` rather than from an observer of its own

## What it demonstrates

- **Failover** (scenario 1). Four candidates: a 429 with `Retry-After: 30 s`
  (`failed 429`), a refusal (`refused`), an answer `with_accept` rejects
  because it gives no reason (`rejected`), and one that answers (`answered`).
  The run has four `ModelCallStarted` / `ModelCallCompleted` pairs, each
  naming the route, and each `ModelRouteAttempt::call_id` pairs with one. Only
  the transient fault counts against health: northwind is `cooling_down` with
  1 failure, while the refusing and rejected providers stay `available` with 0
- **A fatal fault stops the route** (scenario 2). A 401 is the request's
  fault, not the model's, so the route doesn't try the backup (`not_tried`,
  never called). The node fails with the candidate's own fault,
  `model call failed (401): ...`, after 1 attempt, although its policy is
  `RecoveryPolicy::retry(3)`
- **Hedging** (scenario 3). With `with_hedge_after(50 ms)`, a candidate that
  hasn't answered gets the next one started beside it (`hedges 1`). The first
  accepted answer wins and the other is dropped (`lost_race`). 3a: the hedge
  wins. 3b: the first candidate answers after the hedge has started, and wins.
  Neither depends on timing: in 3a the slow candidate hangs until it is
  dropped, and in 3b it waits for a `tokio::sync::Notify` the hedge signals
  when it is asked, while the hedge hangs. The 50 ms only decides when the
  hedge starts
- **`with_eligible`** (scenario 4) skips a candidate for one request. Here the
  rule reads the run's metadata (`plan`, set with `RunConfig::with_metadata`)
  from the node's context: a paid request is answered by `frontier`, a free
  one sees `frontier` as `ineligible` with the rule's reason and is answered
  by `local`. The ineligible candidate isn't called
- **A shared `ModelHealth` breaker** (scenario 5). Two routes in one graph,
  `diagnose` and `summarise`, share a `ModelHealth` (`with_health`) whose
  breaker opens after 2 failures, for 60 s. Their first candidates are two
  models of one provider (`nw-large`, `nw-small`). The default health key is
  the provider's name, so a 503 in each route makes 2 failures in a row on one
  key and opens its breaker. The next run skips northwind in both routes
  (`breaker_open 60 s`) without calling it. After the paused clock moves 60 s
  the key is `half_open`: one test call goes through, succeeds, and closes the
  breaker
- **Nothing answers** (scenario 6). Both candidates fail, with `Retry-After`
  8 s and 3 s. The route fails with `route_exhausted`, transient, and its hint
  is the **shortest** wait (3 s), when some candidate can be asked again. The
  node fails transiently with that hint (`NodeError::TransientAfter`). Its
  policy, `retry(2).with_backoff(200 ms, 30 s, Jitter::None)`, waits the hint
  instead of its own backoff (`RetryScheduled`'s `delay` is 3 s). Health
  persists across the retry: attempt 2 (`Ask#2`) skips `a`, which still has
  exactly 5 s of its cooldown to run, and `b` answers

Each claim is checked when the demo runs, and the demo exits non-zero if one
fails.

## What it does *not* cover

- **Streams.** A streaming run (`run_streaming_with`) routes a stream: it
  fails over only until its first update, only the winner's tokens reach the
  run's stream, and a hedged stream is won by the first candidate to send an
  update. This demo's runs don't stream (`FakeModel::stream` refuses). See
  [`06-token-streaming`](../06-token-streaming) for a streaming model node.
- **An agent over a route.** `Agent` takes a route like any `ChatClient`, and
  a failover can happen on any turn of its tool loop.
  [`23-agent-tool-loop`](../23-agent-tool-loop) covers the agent itself.
- **Cancellation** of a route call (every attempt in flight `cancelled`, the
  rest `not_tried`, health untouched), `on_routed` for a route called outside
  a run, a custom `with_fault_classifier`, `with_health_key`,
  `ModelCandidate::with_model`, and the `route <name>` span and
  `strata.route.*` metrics. The engine's `strata-ai/tests/routes.rs` and
  `route_cases.rs` cover these.
- **Retries on the same candidate.** A route never asks a candidate twice in
  one call. The node's `RecoveryPolicy` retries the whole route, as scenario 6
  shows.

## Run it

```bash
cargo run -p model-routes
```

Output of a real run (identical on every run):

```
=== Scenario 1: failover past a 429, a refusal and a rejected answer ===
  Answer: Roll back deploy 412, because the error rate rose with it.
  Model calls in the run (one per attempt, each naming the route):
    ModelCallStarted   nw-large     route=triage
    ModelCallCompleted nw-large     Failed
    ModelCallStarted   ct-guarded   route=triage
    ModelCallCompleted ct-guarded   Succeeded (content_filter)
    ModelCallStarted   fb-mini      route=triage
    ModelCallCompleted fb-mini      Succeeded (stop)
    ModelCallStarted   llama-local  route=triage
    ModelCallCompleted llama-local  Succeeded (stop)
  ModelRouted Ask#1 route 'triage': answered by local (llama-local), hedges 0
    primary   failed       429
    careful   refused
    terse     rejected     it gives no reason
    local     answered
  Health: contoso=available(0), fabrikam=available(0), local=available(0), northwind=cooling_down(1)

=== Scenario 2: a 401 is fatal -- the route stops, and retry(3) doesn't retry it ===
  Node 'Ask' failed after 1 attempt(s): model call failed (401): 401 Unauthorized: invalid API key
  ModelRouted Ask#1 route 'fatal': nobody answered, hedges 0
    primary   failed       401
    backup    not_tried

=== Scenario 3a: hedge after 50 ms -- slow hangs, fast starts beside it and wins ===
  Answer: Roll back deploy 412, because the error rate rose with it.
  ModelRouted Ask#1 route 'hedged': answered by fast (llama-local), hedges 1
    slow      lost_race
    fast      answered

=== Scenario 3b: the same route shape -- slow answers once the hedge has started, and still wins ===
  Answer: Roll back deploy 412, because the error rate rose with it.
  ModelRouted Ask#1 route 'hedged': answered by slow (nw-large), hedges 1
    slow      answered
    hedge     lost_race

=== Scenario 4: with_eligible -- the free plan skips the frontier candidate ===
  plan=paid:
  ModelRouted Ask#1 route 'plans': answered by frontier (nw-large), hedges 0
    frontier  answered
    local     not_tried
  plan=free:
  ModelRouted Ask#1 route 'plans': answered by local (llama-local), hedges 0
    frontier  ineligible   the free plan doesn't use frontier models
    local     answered

=== Scenario 5: a shared ModelHealth with a breaker (2 failures, open 60 s) across two routes ===
  Run 1: northwind fails in each route (2 failures in a row: the breaker opens)
  ModelRouted Diagnose#1 route 'diagnose': answered by steady (llama-local), hedges 0
    large     failed       503
    steady    answered
  ModelRouted Summarise#1 route 'summarise': answered by steady (llama-local), hedges 0
    small     failed       503
    steady    answered
  Health: local=available(0), northwind=open(2)
  Run 2: northwind's breaker is open, so neither route asks it
  ModelRouted Diagnose#1 route 'diagnose': answered by steady (llama-local), hedges 0
    large     breaker_open 60 s
    steady    answered
  ModelRouted Summarise#1 route 'summarise': answered by steady (llama-local), hedges 0
    small     breaker_open 60 s
    steady    answered
  (paused clock +60 s)
  Health: local=available(0), northwind=half_open(2)
  Run 3: one test call goes through, succeeds and closes the breaker
  ModelRouted Diagnose#1 route 'diagnose': answered by large (nw-large), hedges 0
    large     answered
    steady    not_tried
  ModelRouted Summarise#1 route 'summarise': answered by small (nw-small), hedges 0
    small     answered
    steady    not_tried
  Health: local=available(0), northwind=available(0)

=== Scenario 6: nothing answers -- a transient failure whose hint is the shortest wait ===
  Attempt 1 failed: model call failed (route_exhausted): no candidate of route 'exhausted' answered: a failed (429), b failed (503)
  RetryScheduled delay: 3 s (b's Retry-After, the shorter of 8 s and 3 s; not the 200 ms backoff)
  ModelRouted Ask#1 route 'exhausted': nobody answered, hedges 0
    a         failed       429
    b         failed       503
  ModelRouted Ask#2 route 'exhausted': answered by b (ct-guarded), hedges 0
    a         cooling_down 5 s
    b         answered
  Answer: Roll back deploy 412, because the error rate rose with it.

=== Done: the route failed over, stopped on a fatal fault, hedged, skipped by rule and by health, and handed its hint to the retry. ===
```

`Health: key=state(failures)` is `ModelHealth::snapshot()`. `Ask#2` is the
node's second attempt, the retry. Its `5 s` is how long `a`'s 8 s cooldown
still has to run (whole seconds, rounded up): exactly 8 s minus the 3 s the
paused clock moved for the retry.

## Files

- `src/main.rs` — the six scenarios, plus `FakeModel` (the scripted client),
  `Turn` (one scripted answer or fault) and the helpers that run a graph with
  a `RunEventChannel` and read its `ModelRouted` events
- `Cargo.toml` — depends on `datalium-strata`, `datalium-strata-ai` and
  `datalium-strata-memory` (for `InMemoryCheckpointStore`), plus `serde`, and
  `tokio` with `test-util` for the paused clock
