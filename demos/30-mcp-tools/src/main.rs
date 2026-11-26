//! MCP tools as Strata tools (design/50-MCP-Client.md), with
//! `datalium-strata-mcp`.
//!
//! An MCP server offers tools to any client that speaks the Model Context
//! Protocol. Strata's own MCP client connects to one and makes its tools
//! ordinary Strata tools: they sit in a `ToolRegistry` next to
//! `#[strata_tool]`s, go through the same middleware, and a graph node calls
//! them the way it calls any other tool.
//!
//! The program has two roles. Started with `--mcp-server` it is a small MCP
//! server (`server.rs`). Otherwise it is the client, and starts itself as that
//! server over stdio, so the demo needs no Node, no network and no MCP crate
//! but Strata's:
//!
//! 1. Connect: `McpClient::connect_with(McpServer::stdio(..), ..)`. The
//!    server is probed, speaks 2026-07-28, and lists three tools.
//! 2. One registry: `client.tool_set(..)` with `include` (leave the
//!    destructive tool out) and `prefix`, registered with `register_mcp` next
//!    to a local `#[strata_tool]`.
//! 3. Middleware: one audit log wraps every call, local or MCP.
//! 4. A graph: a node makes a fixed list of tool calls, as a model's tool
//!    loop would. A tool's own error (`isError`) arrives as
//!    `{"error": "..."}` and the node goes on.
//! 5. Close: `close().await` closes the server's stdin, and the server
//!    stops.
//!
//! No LLM: the "agent" is a node with a script of calls, as in demo 26.

mod server;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::NodeContext;
use datalium_strata::graph::StateGraph;
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata::tools::{Next, ToolCall, ToolMiddleware};
use datalium_strata_mcp::{
    McpClient, McpClientOptions, McpLogEntry, McpRegistryExt, McpServer, McpTool, McpToolOptions,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    // --- 1. Connect. ---

    println!("=== 1. Connect: the demo starts itself as an MCP server, over stdio ===");
    // The client reports the server's stderr to its log, at Debug, as "stderr: <line>".
    let log = Arc::new(Mutex::new(Vec::<McpLogEntry>::new()));
    let options = McpClientOptions::new().with_log({
        let log = log.clone();
        move |entry: &McpLogEntry| log.lock().unwrap().push(entry.clone())
    });
    let me = std::env::current_exe().expect("the program's own path");
    let client = McpClient::connect_with(McpServer::stdio(me).args(["--mcp-server"]), options)
        .await
        .unwrap_or_else(|error| fail(&format!("couldn't connect: {error}")));
    let info = client.server_info().expect("the server says who it is");
    println!("  Protocol version: {}", client.protocol_version());
    println!("  Server:           {} {}", info.name, info.version);
    println!("  The server's tools:");
    for tool in client.tools() {
        println!(
            "    {:<13} {}{}",
            tool.name,
            tool.description.as_deref().unwrap_or(""),
            hints(tool)
        );
    }
    check(
        client.protocol_version() == "2026-07-28",
        "expected the stateless 2026-07-28 revision",
    );
    check(
        client
            .tools()
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>()
            == ["lookup_stock", "reserve", "delete_sku"],
        "expected three tools",
    );

    // --- 2. One registry. ---

    println!();
    println!(
        "=== 2. One registry: a local #[strata_tool] and the server's tools, side by side ==="
    );
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
    let manifest = registry.manifest();
    let reserve = manifest
        .as_array()
        .and_then(|tools| tools.iter().find(|t| t["name"] == "warehouse.reserve"))
        .expect("warehouse.reserve is in the manifest");
    // serde_json sorts an object's keys, so the order comes from `required`.
    let parameters: Vec<&str> = reserve["parameters"]["required"]
        .as_array()
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    println!(
        "  warehouse.reserve takes ({}), from the server's input schema.",
        parameters.join(", ")
    );
    match registry
        .dispatch("warehouse.delete_sku", r#"{"sku":"A-100"}"#)
        .await
    {
        Err(StrataError::ToolNotFound(_)) => println!(
            "  warehouse.delete_sku: no such tool. Include left it out, so nothing that uses this registry can call it."
        ),
        other => fail(&format!(
            "expected warehouse.delete_sku to be missing, got {other:?}"
        )),
    }
    check(
        names == ["quote", "warehouse.lookup_stock", "warehouse.reserve"],
        "expected the local tool, then the two MCP tools with the prefix",
    );

    // --- 3. Middleware. ---

    println!();
    println!("=== 3. Middleware: one audit log around every call, local or MCP ===");
    let audit = AuditLog::default();
    registry.use_middleware(audit.clone());
    for (tool, arguments) in [
        ("quote", r#"{"sku":"A-100","quantity":1}"#),
        ("warehouse.lookup_stock", r#"{"sku":"A-100"}"#),
    ] {
        registry
            .dispatch(tool, arguments)
            .await
            .unwrap_or_else(|error| fail(&format!("{tool} failed: {error:?}")));
    }
    println!(
        "  A #[strata_tool] returns the String it builds. An MCP tool returns its structured content, as compact JSON."
    );
    check(
        audit.lines().len() == 4,
        "expected the audit log to see both calls",
    );

    // --- 4. A graph whose node calls the tools. ---

    println!();
    println!("=== 4. A graph: a node calls the tools, as a model's tool loop would ===");
    let registry = Arc::new(registry);
    let graph = {
        let registry = registry.clone();
        StateGraph::<Order>::new()
            .add_async_node("Fulfil", move |order: Order, ctx: NodeContext| {
                let registry = registry.clone();
                async move { fulfil(order, &registry, &ctx).await }
            })
            .compile()
            .expect("the graph is valid")
    };
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let order = match graph
        .run("order-1001", Order::new("order-1001"), store)
        .await
    {
        GraphExecutionResult::Completed(order) => order,
        GraphExecutionResult::Failed(error) => fail(&format!("the run failed: {error:?}")),
        _ => fail("expected the run to complete"),
    };
    println!("  Final state of {}:", order.id);
    println!("    reserved:    {}", order.reserved.join("; "));
    println!("    backordered: {}", order.backordered.join("; "));
    check(
        order.reserved == ["3 x A-100 (9 left)"],
        "expected the A-100 line reserved",
    );
    check(
        order.backordered == ["1 x B-200 (only 0 of B-200 on hand)"],
        "expected the B-200 line backordered",
    );

    // --- 5. Close. ---

    println!();
    println!("=== 5. Close: the client stops the server ===");
    client.close().await;
    let stopped = "warehouse: stopped after 4 tool calls";
    // close() waits for the server to exit; the line it wrote last on its
    // stderr may reach the log a moment later.
    let mut stderr = Vec::new();
    for _ in 0..200 {
        stderr = stderr_lines(&log);
        if stderr.iter().any(|line| line == stopped) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!("  close().await closed the server's stdin. The server's last words, on its stderr,");
    println!("  which the client reads and reports to its log:");
    for line in &stderr {
        println!("    {line}");
    }
    check(
        stderr == [stopped],
        "expected the server to stop after four calls",
    );
    println!("  Four calls reached the server: warehouse.delete_sku never did.");

    println!();
    println!(
        "=== Done: an MCP server's tools, registered, filtered, wrapped in middleware and called from a graph. ==="
    );
}

/// The lines of the server's stderr the client reported to its log.
fn stderr_lines(log: &Mutex<Vec<McpLogEntry>>) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .filter_map(|entry| entry.message.strip_prefix("stderr: ").map(str::to_string))
        .collect()
}

/// A tool's annotations are the server's hints, not guarantees: the client
/// decides what to expose.
fn hints(tool: &McpTool) -> String {
    let Some(Value::Object(annotations)) = &tool.annotations else {
        return String::new();
    };
    let set: Vec<&str> = annotations
        .iter()
        .filter(|(_, value)| **value == Value::Bool(true))
        .map(|(name, _)| name.as_str())
        .collect();
    format!(" ({})", set.join(", "))
}

// --- The order the graph fulfils: what it could reserve, and what it couldn't. ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: String,
    reserved: Vec<String>,
    backordered: Vec<String>,
}

impl Order {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            reserved: Vec::new(),
            backordered: Vec::new(),
        }
    }
}

/// A fixed list of tool calls, the way a model's tool loop would make them.
const SCRIPT: [(&str, &str); 4] = [
    ("warehouse.lookup_stock", r#"{"sku":"A-100"}"#),
    ("quote", r#"{"sku":"A-100","quantity":3}"#),
    ("warehouse.reserve", r#"{"sku":"A-100","quantity":3}"#),
    ("warehouse.reserve", r#"{"sku":"B-200","quantity":1}"#),
];

async fn fulfil(mut order: Order, registry: &ToolRegistry, ctx: &NodeContext) -> Order {
    let call = ToolCallContext::from(ctx).with_caller_id("order-agent");
    for (tool, arguments) in SCRIPT {
        let result = registry
            .dispatch_with(tool, arguments, call.clone())
            .await
            .unwrap_or_else(|error| fail(&format!("{tool} failed: {error:?}")));
        if tool != "warehouse.reserve" {
            continue;
        }
        let result: Value = serde_json::from_str(&result).expect("each tool answers JSON");
        let line: Value = serde_json::from_str(arguments).expect("the script is JSON");
        let what = format!(
            "{} x {}",
            line["quantity"],
            line["sku"].as_str().unwrap_or_default()
        );
        // The tool's own error is a result, not an Err: the node reads it and goes on.
        match result.get("error").and_then(Value::as_str) {
            Some(error) => order.backordered.push(format!("{what} ({error})")),
            None => order
                .reserved
                .push(format!("{what} ({} left)", result["left"])),
        }
    }
    order
}

// --- A local tool, generated at compile time, next to the MCP server's tools. ---

#[derive(Deserialize, StrataSchema)]
struct QuoteArgs {
    sku: String,
    quantity: u64,
}

/// A price quote, with the field names the MCP tools use too. A struct, so
/// serde writes the fields in this order.
#[derive(Serialize)]
struct Quote {
    sku: String,
    quantity: u64,
    total: f64,
}

#[strata_tool(name = "quote", description = "Prices a quantity of a SKU.")]
async fn quote(args: QuoteArgs) -> Result<String, StrataError> {
    // An f64, as .NET's twin uses a double: 3 x 2.50 prints as 7.5 in both.
    let price = match args.sku.as_str() {
        "A-100" => 2.50,
        "B-200" => 4.00,
        _ => 0.0,
    };
    let total = price * args.quantity as f64;
    serde_json::to_string(&Quote {
        sku: args.sku,
        quantity: args.quantity,
        total,
    })
    .map_err(|_| StrataError::ToolExecutionFailure("quote".to_string()))
}

// --- The audit middleware. ---

/// One middleware around every call in the registry. It can't tell, and
/// doesn't need to know, which tools are local and which an MCP server runs:
/// both are just tools to it.
#[derive(Clone, Default)]
struct AuditLog(Arc<Mutex<Vec<String>>>);

impl AuditLog {
    fn lines(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }

    fn write(&self, line: String) {
        println!("    {line}");
        self.0.lock().unwrap().push(line);
    }
}

impl ToolMiddleware for AuditLog {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            self.write(format!("→ {} {}", call.name(), call.arguments()));
            let result = next.run(call).await;
            match &result {
                Ok(text) => self.write(format!("← {text}")),
                Err(error) => self.write(format!("✗ {error:?}")),
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
