//! The demo's own MCP server, run when the program is started with
//! `--mcp-server`. It is written by hand, with no MCP crate, to show how
//! little a client needs from the other side: MCP's stdio transport is one
//! JSON-RPC 2.0 message per line, on stdin and stdout, and nothing else on
//! stdout. It speaks the stateless `2026-07-28` revision: `server/discover`,
//! `tools/list` and `tools/call`. Any other request gets "method not found",
//! and notifications get no answer. Diagnostics go to stderr, which the
//! client reads and reports to its log.

use std::collections::HashMap;
use std::io::{BufRead, Write};

use serde_json::{Value, json};

const VERSION: &str = "2026-07-28";

/// Serves until the client closes stdin (MCP's stdio shutdown), then says
/// how many tool calls it served, on stderr.
pub fn serve() {
    // What the warehouse holds: name and units on hand, per SKU.
    let mut stock: HashMap<String, (&str, u64)> = HashMap::from([
        ("A-100".to_string(), ("Widget", 12)),
        ("B-200".to_string(), ("Gadget", 0)),
    ]);
    let mut calls = 0;
    let mut stdout = std::io::stdout().lock();
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            eprintln!("warehouse: not JSON: {line}");
            continue;
        };
        // A notification: no answer.
        let Some(id) = request.get("id").cloned() else {
            continue;
        };
        let method = request["method"].as_str().unwrap_or_default();
        let tool = request["params"]["name"].as_str().unwrap_or_default();
        let outcome = match method {
            "server/discover" => Ok(json!({
                "resultType": "complete",
                "supportedVersions": [VERSION],
                "capabilities": {"tools": {}},
                "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "warehouse", "version": "1.0.0"}},
            })),
            "tools/list" => Ok(json!({"resultType": "complete", "tools": tools()})),
            "tools/call" if matches!(tool, "lookup_stock" | "reserve" | "delete_sku") => {
                calls += 1;
                Ok(call(tool, &request["params"]["arguments"], &mut stock))
            }
            // MCP: an unknown tool is a protocol error, not a tool error.
            "tools/call" => Err((-32602, "unknown tool".to_string())),
            other => Err((-32601, format!("method not found: {other}"))),
        };
        let response = match outcome {
            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
            Err((code, message)) => {
                json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
            }
        };
        if writeln!(stdout, "{response}")
            .and_then(|()| stdout.flush())
            .is_err()
        {
            break;
        }
    }
    eprintln!("warehouse: stopped after {calls} tool calls");
}

fn tools() -> Value {
    json!([
        {
            "name": "lookup_stock",
            "description": "Units on hand for a SKU.",
            "inputSchema": {
                "type": "object",
                "properties": {"sku": {"type": "string", "description": "The SKU, such as A-100."}},
                "required": ["sku"],
            },
            "annotations": {"readOnlyHint": true},
        },
        {
            "name": "reserve",
            "description": "Reserves units of a SKU for an order.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string", "description": "The SKU, such as A-100."},
                    "quantity": {"type": "integer", "minimum": 1, "description": "How many units."},
                },
                "required": ["sku", "quantity"],
            },
        },
        {
            "name": "delete_sku",
            "description": "Deletes a SKU and its stock.",
            "inputSchema": {
                "type": "object",
                "properties": {"sku": {"type": "string"}},
                "required": ["sku"],
            },
            "annotations": {"destructiveHint": true},
        },
    ])
}

fn call(tool: &str, arguments: &Value, stock: &mut HashMap<String, (&str, u64)>) -> Value {
    let sku = arguments["sku"].as_str().unwrap_or_default().to_string();
    let Some(&(name, on_hand)) = stock.get(&sku) else {
        return error(format!("no such SKU: {sku}"));
    };
    match tool {
        "lookup_stock" => success(
            format!("{sku} {name}: {on_hand} on hand"),
            json!({"sku": sku, "name": name, "on_hand": on_hand}),
        ),
        "reserve" => {
            let quantity = arguments["quantity"].as_u64().unwrap_or(0);
            if quantity < 1 {
                return error("quantity must be at least 1".to_string());
            }
            if quantity > on_hand {
                return error(format!("only {on_hand} of {sku} on hand"));
            }
            stock.insert(sku.clone(), (name, on_hand - quantity));
            success(
                format!("reserved {quantity} of {sku}"),
                json!({"sku": sku, "reserved": quantity, "left": on_hand - quantity}),
            )
        }
        // delete_sku
        _ => {
            stock.remove(&sku);
            success(
                format!("deleted {sku}"),
                json!({"sku": sku, "deleted": true}),
            )
        }
    }
}

/// A tool result: a text block for any client, and the same facts as
/// structured content.
fn success(text: String, structured: Value) -> Value {
    json!({
        "resultType": "complete",
        "content": [{"type": "text", "text": text}],
        "structuredContent": structured,
    })
}

/// The tool's own error: a result with `isError`, not a JSON-RPC error, so the
/// model can read it and try again.
fn error(message: String) -> Value {
    json!({
        "resultType": "complete",
        "content": [{"type": "text", "text": message}],
        "isError": true,
    })
}
