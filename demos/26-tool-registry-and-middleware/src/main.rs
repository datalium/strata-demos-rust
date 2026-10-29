//! Tool registry, tool context and tool middleware
//! (design/20-Tool-Dispatch-And-Registry.md, design/23-Tool-Middleware.md).
//!
//! Demo 02 dispatches one generated tool directly. Real agents have several
//! providers, and each agent should see only some of their tools. This demo
//! puts three providers' `#[strata_tool]`s into one `ToolRegistry` and walks
//! through what the registry adds:
//!
//!   1. One registry, many providers -- merged manifest in registration order;
//!      a duplicate tool name is refused at registration (`DuplicateTool`).
//!   2. Per-agent subsets -- `subset(..)` refuses a left-out tool as *not
//!      permitted*, which is distinct from *no such tool*.
//!   3. The tool sees its caller -- `ToolCallContext::from(&ctx)` carries
//!      thread, run, node path, attempt, run metadata and services; the token
//!      is on the context. `ToolCallStarted` / `ToolCallCompleted` run events
//!      report each call.
//!   4. Middleware -- `use_middleware(..)` adds an audit log (outermost), a
//!      refund policy that denies, an argument rewrite and a cache that
//!      short-circuits. Refused calls skip the chain and reach `on_refused`.
//!
//! No LLM: the "agent" is a node that makes a fixed list of calls, as a
//! model's tool-call loop would.

mod middleware;
mod tools;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::CancellationToken;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::StateGraph;
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, ToolCallResult};
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;

use middleware::{AuditLog, ChainTrace, NormalizeSku, ReadCache, RefundPolicy};
use tools::Backend;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ticket {
    id: String,
}

fn ticket(id: &str) -> Ticket {
    Ticket { id: id.to_string() }
}

#[tokio::main]
async fn main() {
    // --- Step 1: one registry, three providers. ---

    println!("=== Step 1: one registry, three providers' #[strata_tool]s ===");
    let backend = Backend::new();
    let mut registry = new_registry();
    println!("  Registered, in registration order (parameters from the merged manifest):");
    for (name, parameters) in manifest(&registry) {
        println!("    {name}({})", parameters.join(", "));
    }

    let reserve_parameters = manifest(&registry)
        .into_iter()
        .find(|(name, _)| name == "inventory.reserve")
        .map(|(_, parameters)| parameters)
        .unwrap_or_default();
    println!(
        "  inventory_reserve is `async fn(Reserve {{ sku, count }}, ToolCallContext)`; its manifest lists only ({}).",
        reserve_parameters.join(", ")
    );
    check(
        registry.names().collect::<Vec<_>>()
            == [
                "inventory.lookup",
                "inventory.reserve",
                "billing.refund",
                "diag.whoami",
            ],
        "expected the tools in registration order",
    );
    check(
        reserve_parameters == ["sku", "count"],
        "expected the context left out of the manifest",
    );

    // `try_register` refuses one tool, not a provider: registering the legacy
    // provider tool by tool lets its unique `inventory.audit` in before
    // `inventory.lookup` clashes.
    let mut tool_by_tool = registry.clone();
    let refusals: Vec<StrataError> = tools::legacy::tools()
        .into_iter()
        .filter_map(|tool| tool_by_tool.try_register(tool).err())
        .collect();
    println!(
        "  legacy tools one by one with try_register: {refusals:?}, and inventory.audit got in ({} tools).",
        tool_by_tool.names().count()
    );
    check(
        matches!(refusals.as_slice(), [StrataError::DuplicateTool(name)] if name == "inventory.lookup"),
        "expected only the clashing tool refused",
    );
    check(
        tool_by_tool.names().any(|n| n == "inventory.audit"),
        "expected try_register to have registered the tool before the clash",
    );

    // All or nothing: register a provider into a copy, and keep the copy only
    // if every tool fit.
    match register_provider(&mut registry, tools::legacy::tools()) {
        Ok(()) => check(false, "expected a duplicate tool name to be refused"),
        Err(refusal) => println!("  The legacy provider, all or nothing: refused -- {refusal:?}"),
    }
    println!(
        "  Its other tool, inventory.audit, was not registered either: {} tools, none of them inventory.audit.",
        registry.names().count()
    );
    check(
        registry.names().count() == 4 && registry.names().all(|n| n != "inventory.audit"),
        "expected nothing from a refused provider to be registered",
    );

    // --- Step 2: per-agent subsets. ---

    println!();
    println!("=== Step 2: a per-agent subset(..) ===");
    let support = registry
        .subset(["diag.whoami", "inventory.reserve", "inventory.lookup"])
        .expect("every name is registered");
    println!(
        "  support-agent's subset, asked for in another order: {}",
        support.names().collect::<Vec<_>>().join(", ")
    );
    check(
        support.names().collect::<Vec<_>>()
            == ["inventory.lookup", "inventory.reserve", "diag.whoami"],
        "expected a subset to keep the registry's order",
    );

    // Outside a run, the caller builds the context itself: here only the service.
    let outside = ToolCallContext::new().with_service(backend.clone());
    let stock = support
        .dispatch_with("inventory.lookup", r#"{"sku":"AB-12"}"#, outside.clone())
        .await
        .expect("inventory.lookup is in the subset");
    println!("  inventory.lookup AB-12 -> {stock}");
    check(stock == "7", "expected 7 units of AB-12");

    let refused = support
        .dispatch_with(
            "billing.refund",
            r#"{"orderId":"A-1","amount":5}"#,
            outside.clone(),
        )
        .await
        .expect_err("billing.refund is left out");
    println!("  billing.refund    -> {refused:?}");
    check(
        matches!(refused, StrataError::ToolNotPermitted(_)),
        "expected a left-out tool to be not permitted",
    );

    let missing = support
        .dispatch_with("inventory.restock", "{}", outside.clone())
        .await
        .expect_err("nobody registered inventory.restock");
    println!("  inventory.restock -> {missing:?}");
    check(
        matches!(missing, StrataError::ToolNotFound(_)),
        "expected an unknown tool to be not found, not 'not permitted'",
    );

    match support.subset(["billing.refund"]) {
        Ok(_) => check(false, "expected a subset to be unable to widen"),
        Err(widen) => println!(
            "  support.subset([\"billing.refund\"]) -> {widen:?}: a subset can only narrow."
        ),
    }
    println!(
        "  ToolNotPermitted and ToolNotFound are sibling StrataError variants: one match arm `ToolNotFound(_) | ToolNotPermitted(_)` tells a model \"no such tool\" for both."
    );
    check(
        backend.calls_to("billing.refund") == 0,
        "expected billing.refund never to run",
    );

    // --- Step 3: the tool sees its caller. ---

    println!();
    println!("=== Step 3: ToolCallContext::from(&ctx) -- the tool sees its caller ===");
    let support = Arc::new(support);
    let outcomes = Arc::new(Mutex::new(Vec::<String>::new()));
    let investigate = {
        let (support, outcomes) = (support.clone(), outcomes.clone());
        StateGraph::<Ticket>::new()
            .add_async_node("Investigate", move |ticket: Ticket, ctx: NodeContext| {
                let (support, outcomes) = (support.clone(), outcomes.clone());
                async move {
                    // Copy what the node knows, then add who is calling. No call
                    // id: the registry generates one, as it would when a model's
                    // tool call carries none.
                    let call = ToolCallContext::from(&ctx).with_caller_id("support-agent");
                    let mut said = Vec::new();
                    for (tool, arguments) in [
                        ("diag.whoami", "{}"),
                        ("inventory.reserve", r#"{"sku":"AB-12","count":2}"#),
                    ] {
                        said.push(
                            match support.dispatch_with(tool, arguments, call.clone()).await {
                                Ok(result) => result,
                                Err(error) => format!("{error:?}"),
                            },
                        );
                    }

                    // A token that has already fired stops the call before the
                    // tool (or any middleware) runs.
                    let cancelled = CancellationToken::new();
                    cancelled.cancel();
                    match support
                        .dispatch_with(
                            "inventory.reserve",
                            r#"{"sku":"AB-12","count":1}"#,
                            call.with_cancellation(cancelled),
                        )
                        .await
                    {
                        Err(StrataError::ToolCancelled(_)) => {
                            said.push("cancelled before the tool ran".to_string())
                        }
                        other => said.push(format!("unexpectedly {other:?}")),
                    }
                    outcomes.lock().unwrap().extend(said);
                    ticket
                }
            })
            .compile()
            .expect("the child graph is valid")
    };
    let graph = StateGraph::<Ticket>::new()
        .add_subgraph(
            "Triage",
            investigate,
            |t: &Ticket| t.clone(),
            |_: &Ticket, t: Ticket| t,
        )
        .compile()
        .expect("the graph is valid");
    let (observer, received) = RunEventChannel::new();
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let config = RunConfig::new()
        .with_metadata("tenant", "acme")
        .with_metadata("ticket", "T-1001")
        .with_observer(observer)
        .with_service(backend.clone());
    let run = graph
        .run_with("ticket-T-1001", ticket("T-1001"), store, config)
        .await;
    let events = drain(received).await;
    check(
        matches!(run, GraphExecutionResult::Completed(_)),
        "expected the run to complete",
    );

    let seen = backend
        .last_context()
        .expect("inventory.reserve recorded its context");
    let mut metadata: Vec<String> = seen
        .metadata()
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    metadata.sort();
    println!("  What inventory.reserve was handed:");
    println!("    thread_id:  {}", seen.thread_id().unwrap_or("-"));
    println!(
        "    run_id:     {}",
        placeholder(seen.run_id(), "(a UUID v7, generated per run)")
    );
    println!("    node_path:  {}", seen.node_path().unwrap_or("-"));
    println!("    attempt:    {}", seen.attempt().unwrap_or(0));
    println!("    metadata:   {}", metadata.join(", "));
    println!("    caller_id:  {}", seen.caller_id().unwrap_or("-"));
    println!(
        "    call_id:    {}",
        placeholder(seen.call_id(), "(a UUID v7, generated by the registry)")
    );
    println!(
        "    service:    Backend {}",
        if seen.service::<Backend>().is_some() {
            "(from RunConfig::with_service)"
        } else {
            "missing"
        }
    );
    println!("  Results:");
    for outcome in outcomes.lock().unwrap().iter() {
        println!("    {outcome}");
    }
    println!(
        "  Tool-call run events (the arguments and results are not in them -- they can hold secrets):"
    );
    for event in events.iter().filter(|e| is_tool_event(e)) {
        println!("    #{:<2} {}", event.sequence, describe(event));
    }

    check(
        seen.thread_id() == Some("ticket-T-1001")
            && seen.node_path() == Some("Triage/Investigate")
            && seen.attempt() == Some(1)
            && seen.caller_id() == Some("support-agent"),
        "expected the tool to see the thread, the qualified node path, the attempt and the caller",
    );
    check(
        seen.metadata().get("tenant").map(String::as_str) == Some("acme"),
        "expected the run metadata on the tool's context",
    );
    check(
        is_uuid_v7(seen.call_id()),
        "expected the registry to generate a UUID v7 call id",
    );
    check(
        is_uuid_v7(seen.run_id()) && seen.run_id() == events.first().map(|e| &*e.run_id),
        "expected the tool's run id to be the run's",
    );
    check(
        backend.calls_to("inventory.reserve") == 1,
        "expected the already-cancelled call never to reach the tool",
    );
    check(
        completed(&events)
            .into_iter()
            .map(|(_, tool, result, _)| (tool, result))
            .collect::<Vec<_>>()
            == [
                ("diag.whoami".to_string(), ToolCallResult::Succeeded),
                ("inventory.reserve".to_string(), ToolCallResult::Succeeded),
                ("inventory.reserve".to_string(), ToolCallResult::Cancelled),
            ],
        "expected one ToolCallCompleted per call, the last one cancelled",
    );
    check(
        events.iter().filter(|e| is_tool_event(e)).count() == 6
            && events.iter().filter(|e| is_tool_event(e)).all(|e| {
                e.node_path.as_deref() == Some("Triage/Investigate") && e.attempt == Some(1)
            }),
        "expected the tool-call events at the calling node's path and attempt",
    );

    // --- Step 4: middleware. ---

    println!();
    println!("=== Step 4: middleware -- audit, policy, rewrite, cache ===");
    let trace = ChainTrace::default();
    let backend = Backend::traced(trace.clone());
    let audit = AuditLog::new(trace.clone());
    let mut registry = new_registry();
    registry
        // An `Arc`, so the demo keeps a handle to read the log.
        .use_middleware(audit.clone())
        .use_middleware(RefundPolicy::new(trace.clone(), [("acme", 100.0)]))
        .use_middleware(NormalizeSku::new(trace.clone()))
        .use_middleware(ReadCache::new(trace.clone(), &["inventory.lookup"]));
    // Made after use_middleware(..), so the subset shares the chain.
    let billing_agent = Arc::new(
        registry
            .subset(["inventory.lookup", "billing.refund"])
            .expect("both are registered"),
    );

    const SCRIPT: [(&str, &str); 6] = [
        ("inventory.lookup", r#"{"sku":" ab-12 "}"#),
        ("inventory.lookup", r#"{"sku":"AB-12"}"#),
        ("billing.refund", r#"{"orderId":"A-1001","amount":40}"#),
        ("billing.refund", r#"{"orderId":"A-1002","amount":250}"#),
        ("inventory.reserve", r#"{"sku":"AB-12","count":1}"#),
        ("inventory.restock", r#"{"sku":"AB-12"}"#),
    ];
    let told = Arc::new(Mutex::new(Vec::<String>::new()));
    let chains = Arc::new(Mutex::new(Vec::<String>::new()));
    let agent = {
        let (billing_agent, told, chains, trace) = (
            billing_agent.clone(),
            told.clone(),
            chains.clone(),
            trace.clone(),
        );
        StateGraph::<Ticket>::new()
            .add_async_node("BillingAgent", move |ticket: Ticket, ctx: NodeContext| {
                let (billing_agent, told, chains, trace) = (
                    billing_agent.clone(),
                    told.clone(),
                    chains.clone(),
                    trace.clone(),
                );
                async move {
                    for (i, (tool, arguments)) in SCRIPT.iter().enumerate() {
                        let call_id = format!("call_{}", i + 1);
                        let call = ToolCallContext::from(&ctx)
                            .with_caller_id("billing-agent")
                            .with_call_id(call_id.clone());
                        let answer = tell(&billing_agent, tool, arguments, call).await;
                        told.lock()
                            .unwrap()
                            .push(format!("{call_id} {tool} {arguments} -> {answer}"));
                        chains.lock().unwrap().push(trace.drain());
                    }
                    ticket
                }
            })
            .compile()
            .expect("the graph is valid")
    };
    let (observer, received) = RunEventChannel::new();
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let config = RunConfig::new()
        .with_metadata("tenant", "acme")
        .with_observer(observer)
        .with_service(backend.clone());
    let run = agent
        .run_with("ticket-T-1002", ticket("T-1002"), store, config)
        .await;
    let events = drain(received).await;
    check(
        matches!(run, GraphExecutionResult::Completed(_)),
        "expected the run to complete",
    );

    println!("  What the agent was told for each call (what a model would see):");
    for line in told.lock().unwrap().iter() {
        println!("    {line}");
    }
    let chains = chains.lock().unwrap().clone();
    println!(
        "  The chain each call took (registration order, outermost first; refused calls skip it):"
    );
    for (i, chain) in chains.iter().enumerate() {
        let shown = if chain.is_empty() { "(none)" } else { chain };
        println!("    call_{}: {shown}", i + 1);
    }
    println!("  Audit log (outermost, so it sees denials, cache hits and refusals too):");
    for line in audit.lines() {
        println!("    {line}");
    }
    println!(
        "  Calls that reached a tool: {}",
        backend.calls().join("; ")
    );
    println!("  ToolCallCompleted events:");
    for (call_id, tool, result, error) in completed(&events) {
        let error = error.map(|e| format!(" -- {e}")).unwrap_or_default();
        println!("    {call_id} {tool}: {result:?}{error}");
    }

    check(
        backend.calls() == ["inventory.lookup AB-12", "billing.refund A-1001 40.00"],
        "expected only the first lookup (normalized) and the small refund to reach a tool",
    );
    check(
        chains
            == [
                "audit -> policy -> normalize -> cache (miss) -> inventory.lookup ran",
                "audit -> policy -> normalize -> cache (hit)",
                "audit -> policy -> normalize -> cache (skip) -> billing.refund ran",
                "audit -> policy",
                "",
                "",
            ],
        "expected the middleware in registration order, a cache hit short-circuiting, a denial stopping the chain",
    );
    check(
        audit.lines()
            == [
                "billing-agent call_1 inventory.lookup: ok",
                "billing-agent call_2 inventory.lookup: ok",
                "billing-agent call_3 billing.refund: ok",
                "billing-agent call_4 billing.refund: denied (tenant acme may refund at most 100.00 without a human)",
                "billing-agent call_5 inventory.reserve: refused (not permitted)",
                "billing-agent call_6 inventory.restock: refused (no such tool)",
            ],
        "expected the audit log to see every call",
    );
    check(
        completed(&events)
            .into_iter()
            .map(|(_, _, result, _)| result)
            .collect::<Vec<_>>()
            == [
                ToolCallResult::Succeeded,
                ToolCallResult::Succeeded,
                ToolCallResult::Succeeded,
                ToolCallResult::Refused,
                ToolCallResult::Refused,
                ToolCallResult::Refused,
            ],
        "expected a short-circuit to be a success and a denial to be a refusal",
    );

    println!();
    println!(
        "=== Done: one registry, a subset per agent, a context per call, and middleware around every call. ==="
    );
}

/// The three providers, each tool added with `register` (which panics on a
/// duplicate: a programming error found at startup).
fn new_registry() -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    for tool in tools::inventory_tools()
        .into_iter()
        .chain(tools::billing_tools())
        .chain(tools::diagnostics_tools())
    {
        registry.register(tool);
    }
    registry
}

/// Registers every tool of a provider, or none: `try_register` into a copy,
/// kept only when every name fit.
fn register_provider(
    registry: &mut ToolRegistry,
    provider: Vec<Box<dyn StrataTool>>,
) -> Result<(), StrataError> {
    let mut candidate = registry.clone();
    for tool in provider {
        candidate.try_register(tool)?;
    }
    *registry = candidate;
    Ok(())
}

/// Each manifest entry's name and parameter names. `properties` is a JSON
/// object (serde_json keeps it sorted by name); `required` keeps the order the
/// arguments struct declares its fields in, and every field here is required.
fn manifest(registry: &ToolRegistry) -> Vec<(String, Vec<String>)> {
    registry
        .manifest()
        .as_array()
        .expect("the manifest is an array")
        .iter()
        .map(|entry| {
            let parameters = entry["parameters"]["required"]
                .as_array()
                .map(|names| {
                    names
                        .iter()
                        .filter_map(|n| n.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            (
                entry["name"].as_str().unwrap_or("?").to_string(),
                parameters,
            )
        })
        .collect()
}

/// What a tool-call loop tells the model, for each way a call can end.
async fn tell(tools: &ToolRegistry, tool: &str, arguments: &str, call: ToolCallContext) -> String {
    match tools.dispatch_with(tool, arguments, call).await {
        Ok(result) => result,
        Err(StrataError::ToolCallDenied { reason, .. }) => format!("denied: {reason}"),
        Err(StrataError::ToolNotPermitted(_)) => "not permitted for this agent".to_string(),
        Err(StrataError::ToolNotFound(_)) => "no such tool".to_string(),
        Err(other) => format!("failed: {other:?}"),
    }
}

/// Every event of the run. The channel closes after the run's last event.
async fn drain(mut received: UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut events = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), received.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) => return events,
            Err(_) => {
                eprintln!("check failed: gave up waiting for the run's events");
                std::process::exit(1);
            }
        }
    }
}

fn is_tool_event(event: &RunEvent) -> bool {
    matches!(
        event.kind,
        RunEventKind::ToolCallStarted { .. } | RunEventKind::ToolCallCompleted { .. }
    )
}

/// (call id, tool, result, error) of every `ToolCallCompleted`.
type Completed = (String, String, ToolCallResult, Option<String>);

fn completed(events: &[RunEvent]) -> Vec<Completed> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::ToolCallCompleted {
                tool,
                call_id,
                result,
                error,
                ..
            } => Some((call_id.clone(), tool.clone(), *result, error.clone())),
            _ => None,
        })
        .collect()
}

fn describe(event: &RunEvent) -> String {
    let at = format!(
        "{}#{}",
        event.node_path.as_deref().unwrap_or("-"),
        event.attempt.unwrap_or(0)
    );
    match &event.kind {
        RunEventKind::ToolCallStarted {
            tool, caller_id, ..
        } => format!(
            "ToolCallStarted   {at} {tool} by {}",
            caller_id.as_deref().unwrap_or("-")
        ),
        RunEventKind::ToolCallCompleted { tool, result, .. } => {
            format!("ToolCallCompleted {at} {tool} {result:?}")
        }
        other => format!("{other:?}"),
    }
}

/// The version digit of a hyphenated UUID is the 15th character.
fn is_uuid_v7(id: Option<&str>) -> bool {
    id.is_some_and(|id| id.len() == 36 && id.chars().nth(14) == Some('7'))
}

fn placeholder<'a>(id: Option<&'a str>, uuid_v7: &'a str) -> &'a str {
    if is_uuid_v7(id) {
        uuid_v7
    } else {
        id.unwrap_or("-")
    }
}

fn check(ok: bool, what: &str) {
    if !ok {
        eprintln!("check failed: {what}");
        std::process::exit(1);
    }
}
