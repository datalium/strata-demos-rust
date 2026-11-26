//! An MCP server's tools as Strata tools (`datalium-strata-mcp`,
//! design/50-MCP-Client.md).
//!
//! One program, two roles. With `--mcp-server` it is a small MCP server,
//! "warehouse" (`server.rs`). Without it, it is the client: it starts itself
//! again as that server, over stdio, and walks through what Strata's MCP
//! client does:
//!
//!   1. Connect -- `McpClient::connect_with` starts the server, finds the
//!      protocol version (`server/discover`, 2026-07-28) and lists its tools.
//!   2. One registry -- `tool_set` picks two of the three tools and prefixes
//!      them; `register_mcp` puts them next to a local `#[strata_tool]`.
//!   3. Middleware -- one `ToolMiddleware` wraps every call, MCP or local.
//!   4. A graph -- a node calls the tools through the registry, as a model's
//!      tool loop would. The server's own error reaches it as
//!      `{"error": "..."}`, and the node goes on.
//!   5. Close -- `close().await` closes the server's stdin and waits for it.
//!
//! No LLM, no network, no Node: the server is this program.

mod server;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::StateGraph;
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata::tools::{Next, ToolCall, ToolMiddleware};
use datalium_strata_mcp::{
    McpClient, McpClientOptions, McpLogEntry, McpRegistryExt, McpServer, McpToolOptions,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

fn main() {
    if std::env::args().nth(1).as_deref() == Some("--mcp-server") {
        server::serve();
        return;
    }
    tokio::runtime::Runtime::new()
        .expect("a tokio runtime")
        .block_on(client());
}

async fn client() {
    // --- Step 1: connect. ---

    println!("=== Step 1: connect to an MCP server over stdio ===");
    // The server's stderr lines reach the client's log hook, at Debug.
    let stderr = Arc::new(Mutex::new(Vec::<String>::new()));
    let options = McpClientOptions::new().with_log({
        let stderr = stderr.clone();
        move |entry: &McpLogEntry| {
            if let Some(line) = entry.message.strip_prefix("stderr: ") {
                stderr.lock().unwrap().push(line.to_string());
            }
        }
    });
    let me = std::env::current_exe().expect("the program's own path");
    let client = McpClient::connect_with(McpServer::stdio(me).args(["--mcp-server"]), options)
        .await
        .unwrap_or_else(|error| fail(&format!("couldn't connect: {error}")));
    println!("  Started this program again as an MCP server (--mcp-server), over stdio.");
    println!("  Protocol version: {}", client.protocol_version());
    let info = client.server_info().expect("the server says who it is");
    println!("  Server: {} {}", info.name, info.version);
    println!("  Tools on the server:");
    for tool in client.tools() {
        println!(
            "    {:<13} {}",
            tool.name,
            tool.description.as_deref().unwrap_or("")
        );
    }
    check(
        client.protocol_version() == "2026-07-28",
        "expected the stateless 2026-07-28 revision",
    );
    check(
        info.name == "warehouse" && info.version == "1.0.0",
        "expected the warehouse server",
    );
    check(
        client
            .tools()
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>()
            == ["lookup_stock", "reserve", "delete_sku"],
        "expected the server's three tools",
    );

    // --- Step 2: one registry. ---

    println!();
    println!("=== Step 2: MCP tools and a local #[strata_tool] in one registry ===");
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(QuoteTool));
    let warehouse = client
        .tool_set(
            McpToolOptions::new()
                .include(["lookup_stock", "reserve"])
                .prefix("warehouse."),
        )
        .expect("the server lists both");
    registry
        .register_mcp(warehouse)
        .expect("the prefixed names are free");
    let names: Vec<&str> = registry.names().collect();
    println!("  Registered: {}", names.join(", "));
    println!("  delete_sku isn't there: include(..) left it out, so no model can call it.");
    let manifest = registry.manifest();
    let reserve = manifest
        .as_array()
        .and_then(|tools| tools.iter().find(|t| t["name"] == "warehouse.reserve"))
        .expect("warehouse.reserve is in the manifest");
    let parameters: Vec<&str> = reserve["parameters"]["required"]
        .as_array()
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    println!(
        "  warehouse.reserve takes ({}), from the server's inputSchema.",
        parameters.join(", ")
    );
    check(
        names == ["quote", "warehouse.lookup_stock", "warehouse.reserve"],
        "expected the local tool, then the two MCP tools",
    );

    // --- Step 3: middleware. ---

    println!();
    println!("=== Step 3: one middleware around every call, MCP or local ===");
    let audit = Audit::default();
    registry.use_middleware(audit.clone());
    for (tool, arguments) in [
        ("warehouse.lookup_stock", r#"{"sku":"B-200"}"#),
        ("quote", r#"{"sku":"A-100","quantity":1}"#),
    ] {
        registry
            .dispatch(tool, arguments)
            .await
            .unwrap_or_else(|error| fail(&format!("{tool} failed: {error:?}")));
    }
    check(
        audit.calls() == ["warehouse.lookup_stock", "quote"],
        "expected the audit to see both kinds of tool",
    );

    // --- Step 4: a graph. ---

    println!();
    println!("=== Step 4: a node calls the tools, as a model's tool loop would ===");
    let registry = Arc::new(registry);
    let graph = {
        let registry = registry.clone();
        StateGraph::<Order>::new()
            .add_async_node("PlaceOrder", move |order: Order, ctx: NodeContext| {
                let registry = registry.clone();
                async move { place(order, &registry, &ctx).await }
            })
            .compile()
            .expect("the graph is valid")
    };
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let order = match graph
        .run_with("order-1", Order::default(), store, RunConfig::new())
        .await
    {
        GraphExecutionResult::Completed(order) => order,
        GraphExecutionResult::Failed(error) => fail(&format!("the run failed: {error:?}")),
        _ => fail("expected the run to complete"),
    };
    println!(
        "  Final state: {}",
        serde_json::to_string(&order).expect("the state serializes")
    );
    check(
        order.reserved == [Line::new("A-100", 3)]
            && order.backordered == ["B-200"]
            && order.total == 7.5,
        "expected A-100 reserved and B-200 backordered",
    );

    // --- Step 5: close. ---

    println!();
    println!("=== Step 5: close the connection ===");
    client.close().await;
    // The stderr reader may hand on the last line just after the process ends.
    for _ in 0..200 {
        if stderr
            .lock()
            .unwrap()
            .iter()
            .any(|l| l == "warehouse: stopped")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!(
        "  close().await closed the server's stdin and waited for it to exit. Its stderr, through with_log:"
    );
    for line in stderr.lock().unwrap().iter() {
        println!("    {line}");
    }
    check(
        stderr.lock().unwrap().last().map(String::as_str) == Some("warehouse: stopped"),
        "expected the server to have stopped",
    );

    println!();
    println!(
        "=== Done: an MCP server's tools, chosen, prefixed and audited like any Strata tool. ==="
    );
}

// --- The order the graph places. ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Line {
    sku: String,
    quantity: u64,
}

impl Line {
    fn new(sku: &str, quantity: u64) -> Self {
        Self {
            sku: sku.to_string(),
            quantity,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Order {
    reserved: Vec<Line>,
    backordered: Vec<String>,
    total: f64,
}

/// A fixed list of tool calls, the way a model's tool loop would make them:
/// 3 x A-100 (look up, quote, reserve), then 1 x B-200, which is out of stock.
async fn place(mut order: Order, registry: &ToolRegistry, ctx: &NodeContext) -> Order {
    let call = |tool: &'static str, arguments: &'static str| async move {
        let result = registry
            .dispatch_with(tool, arguments, ToolCallContext::from(ctx))
            .await
            .unwrap_or_else(|error| fail(&format!("{tool} failed: {error:?}")));
        serde_json::from_str::<Value>(&result).expect("each tool answers JSON")
    };

    call("warehouse.lookup_stock", r#"{"sku":"A-100"}"#).await;
    let quote = call("quote", r#"{"sku":"A-100","quantity":3}"#).await;
    let reserved = call("warehouse.reserve", r#"{"sku":"A-100","quantity":3}"#).await;
    if reserved.get("error").is_none() {
        order.reserved.push(Line::new("A-100", 3));
        order.total += quote["total"].as_f64().unwrap_or_default();
    }

    let reserved = call("warehouse.reserve", r#"{"sku":"B-200","quantity":1}"#).await;
    if let Some(error) = reserved.get("error") {
        // The server's own error (isError) is a result, not a failure: the
        // node reads it and goes on.
        println!("  B-200: the server said {error}, so it is backordered.");
        order.backordered.push("B-200".to_string());
    }
    order
}

// --- A local tool, next to the MCP ones. ---

#[derive(Deserialize, StrataSchema)]
struct QuoteArgs {
    sku: String,
    quantity: u64,
}

#[strata_tool(name = "quote", description = "Prices a quantity of a SKU.")]
async fn quote(args: QuoteArgs) -> Result<String, StrataError> {
    // An f64, as .NET's twin uses a double: 3 x 2.50 prints as 7.5 in both.
    let price = match args.sku.as_str() {
        "A-100" => 2.50,
        "B-200" => 4.00,
        _ => return Err(StrataError::ToolExecutionFailure("quote".to_string())),
    };
    Ok(json!({
        "quantity": args.quantity,
        "sku": args.sku,
        "total": price * args.quantity as f64,
    })
    .to_string())
}

// --- The audit middleware. ---

/// Prints every call and its result. The registry runs it for the MCP tools
/// and the local one alike: an MCP tool is a Strata tool.
#[derive(Clone, Default)]
struct Audit(Arc<Mutex<Vec<String>>>);

impl Audit {
    fn calls(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

impl ToolMiddleware for Audit {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            println!("  audit → {} {}", call.name(), call.arguments());
            self.0.lock().unwrap().push(call.name().to_string());
            let result = next.run(call).await;
            match &result {
                Ok(text) => println!("  audit ← {text}"),
                Err(error) => println!("  audit ✗ {error:?}"),
            }
            result
        })
    }
}

fn check(ok: bool, what: &str) {
    if !ok {
        fail(what);
    }
}

fn fail(what: &str) -> ! {
    eprintln!("check failed: {what}");
    std::process::exit(1);
}
