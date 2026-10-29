# 26 — tool-registry-and-middleware

The tool registry, the tool call context and tool middleware
(`design/20-Tool-Dispatch-And-Registry.md`, `design/23-Tool-Middleware.md`).
[`02-tool-calling`](../02-tool-calling) dispatches one generated tool
directly. A real agent platform has several providers, and each agent should
see only some of their tools. This demo puts three providers' `#[strata_tool]`s
(inventory, billing, diagnostics) into one `ToolRegistry`, gives each agent a
subset, lets a tool see who called it, and wraps every call in middleware: an
audit log, a refund policy, an argument rewrite and a cache.

No LLM: the "agent" is a graph node that makes a fixed list of tool calls, the
way a model's tool-call loop would.

The .NET twin is
[`strata-demos-dotnet/demos/26-tool-registry-and-middleware`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/26-tool-registry-and-middleware).
Same four steps and the same checks; where the two engines differ in shape,
this README says so.

## What it demonstrates

- **One registry, many providers.** `ToolRegistry::new()` plus
  `register(Box::new(...Tool))` merges the tools' manifest entries in
  registration order (`registry.manifest()`, `registry.names()`). `register`
  panics on a duplicate name (a programming error found at startup);
  `try_register` returns `StrataError::DuplicateTool(name)` and registers
  nothing.
  - *Engines differ:* a Rust tool is a free `async fn`, and the registry takes
    one tool at a time, not a provider object. So `try_register` refuses only
    the clashing tool: registering the legacy provider tool by tool lets its
    unique `inventory.audit` in before `inventory.lookup` clashes (the demo
    shows this on a copy). To get .NET's all-or-nothing `Register(provider)`,
    the demo's `register_provider` registers into a `clone()` of the registry
    and keeps the copy only if every tool fit.
  - `#[strata_tool]` has no compile-time duplicate check like the .NET
    generator's `STRATA003`; in Rust only the registry sees a clash
- **Per-agent subsets.** `registry.subset([...])` keeps the registry's order,
  not the order asked for. A tool the subset left out is refused with
  `StrataError::ToolNotPermitted`, which is distinct from a name nobody
  registered (`StrataError::ToolNotFound`). In .NET the first exception
  derives from the second; in Rust they are sibling variants, so one match arm
  `ToolNotFound(_) | ToolNotPermitted(_)` covers both. A subset can only
  narrow: asking it for a tool it left out is `ToolNotPermitted` too
- **The tool sees its caller.** `ToolCallContext::from(&ctx).with_caller_id(..)`
  copies the thread id, run id, qualified node path (`Triage/Investigate`,
  inside a subgraph), attempt, run metadata (`tenant`, `ticket`), the run's
  services and the node's token. When the caller sets no call id, the registry
  generates a UUID v7. A tool receives the context as a second
  `ctx: ToolCallContext` parameter, after its arguments struct, and the token
  as `ctx.cancellation()`; neither is in the manifest (`inventory.reserve`'s
  `Reserve { sku, count }` is listed as `(sku, count)`). .NET takes the context
  and a `CancellationToken` as parameters in any position
- **Services through the context.** The tools reach their `Backend` with
  `ctx.service::<Backend>()`: from `RunConfig::with_service` inside a run, or
  `ToolCallContext::new().with_service(..)` outside one (step 2). In .NET the
  providers are objects with a constructor dependency
- **A cancelled token stops a call before it starts.** The tool never runs
  (nor would any middleware), the dispatch returns `StrataError::ToolCancelled`
  and the call is reported `Cancelled`
- **`ToolCallStarted` / `ToolCallCompleted` run events** (`RunEventKind`) for
  every call whose context came from a node, at the calling node's path and
  attempt, with the tool, call id, caller and result. They don't carry the
  arguments or the result, which can hold secrets
- **Middleware with `use_middleware(impl ToolMiddleware)`**, run in
  registration order, outermost first. Each implements
  `invoke(call, next) -> BoxFuture<Result<String, StrataError>>`. The demo
  prints the chain each call took:
  - `AuditLog` (outermost, passed as an `Arc` so the demo keeps a handle)
    records every call's outcome, including the ones a middleware inside it
    denied or answered from the cache
  - `RefundPolicy` reads the tenant from the call's metadata and returns
    `StrataError::ToolCallDenied { tool, reason }` above the tenant's limit.
    The call is reported `Refused`, with the reason, and the tool never runs
  - `NormalizeSku` rewrites the arguments (`call.with_arguments(..)`):
    `" ab-12 "` becomes `"AB-12"`
  - `ReadCache` answers a repeated lookup without running `next` (a
    short-circuit, reported `Succeeded`). Because it is registered after
    `NormalizeSku`, `" ab-12 "` and `"AB-12"` share one cache entry: order
    matters. It also keys by tenant, so one tenant's answer is never served
    to another
- **Refused calls skip the chain.** A call the registry refuses (not
  permitted, not found) runs no `invoke`; each middleware's `on_refused` hook
  sees it instead, which is how the audit log records those two
- **A subset made after `use_middleware(..)` shares the parent's chain**, so
  the billing agent's calls go through the shared audit and policy

## What it does *not* cover

- **The model ↔ tool loop.** Here a node calls tools from a fixed script. See
  [`23-agent-tool-loop`](../23-agent-tool-loop) for `datalium-strata-ai`'s
  `Agent` driving a registry from a (scripted) model's tool calls
- **The `strata.tool` span and the `strata.tool.*` metrics.** Every registry
  dispatch has them (a cache hit is tagged `strata.tool.short_circuited`), but
  this demo reads only the run events. See
  [`27-run-observability`](../27-run-observability) for listening to the
  engine's spans and metrics
- **A tool's own error.** None of these tools fails. If one did, the
  `#[strata_tool]`-generated `execute_with` maps its `Err` to
  `StrataError::ToolExecutionFailure(tool name)` and drops the tool's own
  error (`rust/strata-macros/src/lib.rs` in `datalium/strata`, the `map_err(|_| ...)`
  on the tool's result, line 225 today), so a middleware or a model loop can't see why it failed
- **A middleware that runs `next` more than once, a timeout middleware, or a
  failing middleware** (`StrataError::ToolCallFailed`, reported `Failed`, not
  `Refused`). The engine's `strata-core/tests/tool_middleware.rs` covers them
- **Real policy.** The refund limit is a map. Authorization rules, rate
  limits and audit storage belong to whatever sits behind the middleware

## Run it

```bash
cargo run -p tool-registry-and-middleware
```

Output of a real run. Run ids and generated call ids are UUID v7s, different on
every run, so the demo checks their version and prints a placeholder. Errors
print as their `Debug` form (`StrataError` has no `Display`); the
`ToolCallCompleted` error texts are the engine's own words:

```
=== Step 1: one registry, three providers' #[strata_tool]s ===
  Registered, in registration order (parameters from the merged manifest):
    inventory.lookup(sku)
    inventory.reserve(sku, count)
    billing.refund(orderId, amount)
    diag.whoami()
  inventory_reserve is `async fn(Reserve { sku, count }, ToolCallContext)`; its manifest lists only (sku, count).
  legacy tools one by one with try_register: [DuplicateTool("inventory.lookup")], and inventory.audit got in (5 tools).
  The legacy provider, all or nothing: refused -- DuplicateTool("inventory.lookup")
  Its other tool, inventory.audit, was not registered either: 4 tools, none of them inventory.audit.

=== Step 2: a per-agent subset(..) ===
  support-agent's subset, asked for in another order: inventory.lookup, inventory.reserve, diag.whoami
  inventory.lookup AB-12 -> 7
  billing.refund    -> ToolNotPermitted("billing.refund")
  inventory.restock -> ToolNotFound("inventory.restock")
  support.subset(["billing.refund"]) -> ToolNotPermitted("billing.refund"): a subset can only narrow.
  ToolNotPermitted and ToolNotFound are sibling StrataError variants: one match arm `ToolNotFound(_) | ToolNotPermitted(_)` tells a model "no such tool" for both.

=== Step 3: ToolCallContext::from(&ctx) -- the tool sees its caller ===
  What inventory.reserve was handed:
    thread_id:  ticket-T-1001
    run_id:     (a UUID v7, generated per run)
    node_path:  Triage/Investigate
    attempt:    1
    metadata:   tenant=acme, ticket=T-1001
    caller_id:  support-agent
    call_id:    (a UUID v7, generated by the registry)
    service:    Backend (from RunConfig::with_service)
  Results:
    support-agent at Triage/Investigate (attempt 1)
    reserved 2 x AB-12 for acme, requested by support-agent
    cancelled before the tool ran
  Tool-call run events (the arguments and results are not in them -- they can hold secrets):
    #4  ToolCallStarted   Triage/Investigate#1 diag.whoami by support-agent
    #5  ToolCallCompleted Triage/Investigate#1 diag.whoami Succeeded
    #6  ToolCallStarted   Triage/Investigate#1 inventory.reserve by support-agent
    #7  ToolCallCompleted Triage/Investigate#1 inventory.reserve Succeeded
    #8  ToolCallStarted   Triage/Investigate#1 inventory.reserve by support-agent
    #9  ToolCallCompleted Triage/Investigate#1 inventory.reserve Cancelled

=== Step 4: middleware -- audit, policy, rewrite, cache ===
  What the agent was told for each call (what a model would see):
    call_1 inventory.lookup {"sku":" ab-12 "} -> 7
    call_2 inventory.lookup {"sku":"AB-12"} -> 7
    call_3 billing.refund {"orderId":"A-1001","amount":40} -> refunded 40.00 on A-1001
    call_4 billing.refund {"orderId":"A-1002","amount":250} -> denied: tenant acme may refund at most 100.00 without a human
    call_5 inventory.reserve {"sku":"AB-12","count":1} -> not permitted for this agent
    call_6 inventory.restock {"sku":"AB-12"} -> no such tool
  The chain each call took (registration order, outermost first; refused calls skip it):
    call_1: audit -> policy -> normalize -> cache (miss) -> inventory.lookup ran
    call_2: audit -> policy -> normalize -> cache (hit)
    call_3: audit -> policy -> normalize -> cache (skip) -> billing.refund ran
    call_4: audit -> policy
    call_5: (none)
    call_6: (none)
  Audit log (outermost, so it sees denials, cache hits and refusals too):
    billing-agent call_1 inventory.lookup: ok
    billing-agent call_2 inventory.lookup: ok
    billing-agent call_3 billing.refund: ok
    billing-agent call_4 billing.refund: denied (tenant acme may refund at most 100.00 without a human)
    billing-agent call_5 inventory.reserve: refused (not permitted)
    billing-agent call_6 inventory.restock: refused (no such tool)
  Calls that reached a tool: inventory.lookup AB-12; billing.refund A-1001 40.00
  ToolCallCompleted events:
    call_1 inventory.lookup: Succeeded
    call_2 inventory.lookup: Succeeded
    call_3 billing.refund: Succeeded
    call_4 billing.refund: Refused -- tool 'billing.refund' was denied: tenant acme may refund at most 100.00 without a human
    call_5 inventory.reserve: Refused -- tool 'inventory.reserve' is not permitted here
    call_6 inventory.restock: Refused -- tool 'inventory.restock' not found

=== Done: one registry, a subset per agent, a context per call, and middleware around every call. ===
```

The demo checks every claim above (registration order, the refused provider
and what `try_register` let in, the subset's order and refusals, the context
the tool saw, the event results and where they were reported, the chain each
call took, the audit log and which calls reached a tool) and exits non-zero if
one fails.

## Files

- `src/main.rs` — the four steps, plus `register_provider` (all-or-nothing
  registration) and the `tell` helper (what a tool-call loop tells a model)
- `src/tools.rs` — the `#[strata_tool]`s (inventory, billing, diagnostics, and
  the clashing `legacy` module), each provider's list of generated handles,
  and the `Backend` service that records which calls reached a tool
- `src/middleware.rs` — `AuditLog`, `RefundPolicy`, `NormalizeSku`,
  `ReadCache`, and the `ChainTrace` they write the chain into
- `Cargo.toml` — package name is `tool-registry-and-middleware`; depends on
  `datalium-strata` (whose prelude re-exports `#[strata_tool]` and
  `#[derive(StrataSchema)]`), `datalium-strata-memory` for the in-memory
  store, `serde`, `serde_json` and `tokio`
