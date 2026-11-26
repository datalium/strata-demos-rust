//! The other role of this program: a small MCP server, "warehouse", over
//! stdio. `main` runs it when started with `--mcp-server`, which is how the
//! client half starts it as a child process.
//!
//! It speaks the stateless `2026-07-28` revision: `server/discover` says who
//! it is and which version it speaks, and every request carries the version
//! in its `_meta`. One JSON-RPC 2.0 message per line on stdout, and nothing
//! else there: diagnostics go to stderr. It is written by hand for the demo,
//! so it shows only what a client needs. It is not part of Strata, which is
//! an MCP client only.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};

use serde_json::{Value, json};

const VERSION: &str = "2026-07-28";

/// Serves until the client closes stdin, then says so on stderr and returns.
pub fn serve() {
    eprintln!("warehouse: serving on stdio");
    let mut stock = BTreeMap::from([
        ("A-100".to_string(), ("Widget", 12_u64)),
        ("B-200".to_string(), ("Gadget", 0_u64)),
    ]);
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            eprintln!("warehouse: not JSON: {line}");
            continue;
        };
        // A notification (no id) gets no reply.
        let Some(id) = message.get("id").cloned() else {
            continue;
        };
        let method = message["method"].as_str().unwrap_or_default();
        let reply = match answer(method, &message["params"], &mut stock) {
            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
            Err((code, text)) => {
                json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": text}})
            }
        };
        if writeln!(stdout, "{reply}")
            .and_then(|()| stdout.flush())
            .is_err()
        {
            break;
        }
    }
    eprintln!("warehouse: stopped");
}

type Stock = BTreeMap<String, (&'static str, u64)>;

fn answer(method: &str, params: &Value, stock: &mut Stock) -> Result<Value, (i64, String)> {
    if method == "server/discover" {
        return Ok(json!({
            "resultType": "complete",
            "supportedVersions": [VERSION],
            "capabilities": {"tools": {}},
            "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "warehouse", "version": "1.0.0"}},
        }));
    }
    // Stateless: there's no handshake, so every request says its version.
    if params["_meta"]["io.modelcontextprotocol/protocolVersion"] != VERSION {
        return Err((-32602, format!("this server speaks {VERSION} only")));
    }
    match method {
        "tools/list" => Ok(json!({"tools": tools()})),
        "tools/call" => call(params, stock),
        other => Err((-32601, format!("Method not found: {other}"))),
    }
}

fn tools() -> Value {
    let sku = json!({"type": "string", "description": "The SKU, such as A-100"});
    json!([
        {
            "name": "lookup_stock",
            "description": "Stock on hand for a SKU.",
            "inputSchema": {"type": "object", "properties": {"sku": sku}, "required": ["sku"]},
        },
        {
            "name": "reserve",
            "description": "Reserves units of a SKU, taking them out of stock.",
            "inputSchema": {
                "type": "object",
                "properties": {"sku": sku, "quantity": {"type": "integer", "minimum": 1}},
                "required": ["sku", "quantity"],
            },
        },
        {
            "name": "delete_sku",
            "description": "Deletes a SKU and its stock. Destructive.",
            "inputSchema": {"type": "object", "properties": {"sku": sku}, "required": ["sku"]},
            "annotations": {"destructiveHint": true},
        },
    ])
}

/// A tool's result. The structured content's keys are in alphabetical order,
/// so the text Strata hands the model is the same in both engines (Rust's
/// `serde_json` sorts an object's keys; .NET keeps the server's order).
fn call(params: &Value, stock: &mut Stock) -> Result<Value, (i64, String)> {
    let arguments = &params["arguments"];
    let sku = arguments["sku"].as_str().unwrap_or_default().to_string();
    match params["name"].as_str().unwrap_or_default() {
        "lookup_stock" => Ok(match stock.get(&sku) {
            Some((name, on_hand)) => structured(
                format!("{name} ({sku}): {on_hand} on hand"),
                json!({"name": name, "on_hand": on_hand, "sku": sku}),
            ),
            None => tool_error(format!("no such SKU: {sku}")),
        }),
        "reserve" => {
            let quantity = arguments["quantity"].as_u64().unwrap_or(0);
            Ok(match stock.get_mut(&sku) {
                None => tool_error(format!("no such SKU: {sku}")),
                Some(_) if quantity < 1 => tool_error("quantity must be at least 1".to_string()),
                Some((_, on_hand)) if *on_hand < quantity => {
                    tool_error(format!("only {on_hand} of {sku} on hand"))
                }
                Some((_, on_hand)) => {
                    *on_hand -= quantity;
                    structured(
                        format!("reserved {quantity} of {sku}, {on_hand} left"),
                        json!({"left": *on_hand, "reserved": quantity, "sku": sku}),
                    )
                }
            })
        }
        "delete_sku" => Ok(match stock.remove(&sku) {
            Some(_) => structured(format!("deleted {sku}"), json!({"deleted": sku})),
            None => tool_error(format!("no such SKU: {sku}")),
        }),
        other => Err((-32602, format!("Unknown tool: {other}"))),
    }
}

fn structured(text: String, value: Value) -> Value {
    json!({"content": [{"type": "text", "text": text}], "structuredContent": value})
}

/// The tool's own error: a result with `isError`, not a JSON-RPC error. The
/// client hands it to the model, which can do something about it.
fn tool_error(text: String) -> Value {
    json!({"content": [{"type": "text", "text": text}], "isError": true})
}
