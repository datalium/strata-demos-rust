//! Tool-calling (STRATA-SPEC-003): a node dispatches a `#[strata_tool]`-generated
//! tool by name through a `ToolRegistry`, instead of computing the result inline.
//! The node is async, so it awaits the tool and hands it a `ToolCallContext`
//! built from its own `NodeContext` (`design/20-Tool-Dispatch-And-Registry.md`).
//!
//! `#[derive(StrataSchema)]` on `AddArgs` generates the JSON-Schema manifest
//! entry for the tool's parameters at compile time; `#[strata_tool]` generates
//! the zero-sized `AddTool` dispatch handle. Neither uses runtime reflection.

use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::NodeContext;
use datalium_strata::graph::StateGraph;
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CalculatorState {
    a: f64,
    b: f64,
    sum: Option<f64>,
}

/// The tool's JSON arguments: these fields, and only these, are the manifest's
/// `parameters`.
#[derive(Deserialize, StrataSchema)]
struct AddArgs {
    a: f64,
    b: f64,
}

/// `ctx` is not an argument: `#[strata_tool]` recognises a trailing
/// `ToolCallContext` parameter, fills it from the dispatch call and leaves it
/// out of the manifest.
#[strata_tool(name = "add", description = "Adds two numbers and returns their sum.")]
async fn add(args: AddArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    // An audit line needs to know who called. `ToolRegistry::dispatch` (no
    // context) passes `ToolCallContext::default()`, where every field is
    // `None`, so a call made that way is refused rather than logged as
    // anonymous.
    let (Some(thread), Some(node), Some(attempt)) =
        (ctx.thread_id(), ctx.node_path(), ctx.attempt())
    else {
        return Err(StrataError::ToolExecutionFailure(
            "add needs a caller: dispatch it with a ToolCallContext".to_string(),
        ));
    };

    println!("[add] called by node '{node}' on thread '{thread}', attempt {attempt}");
    Ok((args.a + args.b).to_string())
}

#[tokio::main]
async fn main() {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(AddTool));
    let tools = Arc::new(registry);

    // An async node gets a `NodeContext` (thread id, run id, node path,
    // attempt, metadata, span, services, the run's cancellation token).
    // `ToolCallContext::from(&ctx)` carries all of it to the tool, the token
    // included, so cancelling the run would reach the tool too.
    let node_tools = tools.clone();
    let graph = StateGraph::<CalculatorState>::new()
        .add_async_node("Add", move |state: CalculatorState, ctx: NodeContext| {
            let tools = node_tools.clone();
            async move {
                let raw_args = serde_json::json!({ "a": state.a, "b": state.b }).to_string();
                let result = tools
                    .dispatch_with("add", &raw_args, ToolCallContext::from(&ctx))
                    .await
                    .expect("add tool call failed");

                CalculatorState {
                    sum: Some(result.parse().expect("add tool returns a numeric string")),
                    ..state
                }
            }
        })
        .compile()
        .expect("graph is structurally valid");

    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let initial = CalculatorState {
        a: 3.0,
        b: 5.0,
        sum: None,
    };

    match graph.run("calc-thread", initial, store).await {
        GraphExecutionResult::Completed(state) => {
            let sum = state.sum.expect("Add node always sets this");
            println!("{} + {} = {}", state.a, state.b, sum);
            check(sum == 8.0, "the tool's sum came back into the state");
        }
        GraphExecutionResult::Failed(err) => panic!("graph execution failed: {err:?}"),
        _ => unreachable!("no interrupt boundary, wait or cancellation in this graph"),
    }

    // The manifest is what an LLM would be shown. The context parameter is
    // plumbing, not an argument the model fills in, so it must not appear there.
    let manifest = tools.manifest();
    let parameters = &manifest[0]["parameters"];
    let names: Vec<&str> = parameters["properties"]
        .as_object()
        .expect("the parameters schema has properties")
        .keys()
        .map(String::as_str)
        .collect();
    println!("manifest parameters for 'add': {}", names.join(", "));
    check(
        manifest[0]["name"] == "add" && names == ["a", "b"],
        "the ToolCallContext parameter is left out of the manifest",
    );

    // Outside a node, the context-free `dispatch` gives the tool nothing to
    // audit, and it refuses. The generated wrapper reports any error the tool
    // returns as `ToolExecutionFailure(<tool name>)`.
    let refused = tools.dispatch("add", r#"{"a": 1, "b": 2}"#).await;
    println!("dispatch without a context: {refused:?}");
    check(
        matches!(&refused, Err(StrataError::ToolExecutionFailure(name)) if name == "add"),
        "a call with no caller is refused",
    );
}

/// Exits with code 1 when a claim the demo makes turns out false.
fn check(ok: bool, claim: &str) {
    if !ok {
        eprintln!("CHECK FAILED: {claim}");
        std::process::exit(1);
    }
}
