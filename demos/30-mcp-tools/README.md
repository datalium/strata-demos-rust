# 30 — mcp-tools

Strata's own MCP client, `datalium-strata-mcp`
([`design/50-MCP-Client.md`](https://github.com/datalium/strata/blob/main/design/50-MCP-Client.md)):
an MCP server's tools become Strata tools. [`26-tool-registry-and-middleware`](../26-tool-registry-and-middleware)
puts `#[strata_tool]`s in a `ToolRegistry`. This demo puts a server's tools
there too, next to a local one, and shows that the registry, middleware and a
graph treat both the same.

**Needs Strata `1.0.0-rc.1`.** `datalium-strata-mcp` is new in that release.
Until it is on crates.io, this demo builds only against a local clone of
`datalium/strata` (see [Run it](#run-it)).

One program, two roles. Started with `--mcp-server`, it is a small MCP server
called "warehouse" ([`src/server.rs`](src/server.rs)), written by hand for the
demo. Otherwise it is the client, and it starts *itself* as that server, over
stdio. No Node, no network, no LLM: the "agent" is a graph node that makes a
fixed list of tool calls, the way a model's tool-call loop would.

The .NET twin is
[`strata-demos-dotnet/demos/30-mcp-tools`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/30-mcp-tools).
It has the same five sections, the same server, the same data and the same
checks. Where the two engines differ, this README says so.

## What it demonstrates

- **Connecting over stdio.** `McpClient::connect_with(McpServer::stdio(exe).args(["--mcp-server"]), options)`
  starts the server as a child process, sends `server/discover`, finds the
  stateless `2026-07-28` revision (`protocol_version()`), and lists the tools
  (`tools()`). The server says who it is (`server_info()`). A legacy server,
  one that wants the `initialize` handshake, would be found by the same call,
  with no change to the code
- **Annotations are hints.** The server marks `lookup_stock` `readOnlyHint`
  and `delete_sku` `destructiveHint` (`McpTool::annotations`). The demo
  prints them, and the client decides what to expose: a server says what it
  likes about its tools
- **Choosing and naming a server's tools.** `client.tool_set(McpToolOptions::new().include(["lookup_stock", "reserve"]).prefix("warehouse."))`
  takes two of the server's three tools, and `registry.register_mcp(..)`
  registers them next to the local `quote` tool, all or nothing. The
  destructive `delete_sku` is left out, so a dispatch of
  `warehouse.delete_sku` is `StrataError::ToolNotFound`: nothing that uses the
  registry can call it. The prefix keeps a server's names apart from everyone
  else's, and the call still sends the server's own name (`reserve`)
- **The server's schema is the manifest.** `warehouse.reserve`'s parameters in
  `registry.manifest()` are the server's `inputSchema`, as a model would see
  them
- **Middleware sees every call.** One `ToolMiddleware`, added with
  `use_middleware`, prints each call and its result, whether the tool is local
  or on the server. An MCP tool is a Strata tool: subsets, the `strata.tool`
  span and the run events apply to it in the same way
- **What a tool returns.** When an MCP tool returns `structuredContent`, that
  is what the registry returns, as compact JSON, and otherwise the content
  blocks' text. The local `#[strata_tool]` returns the `String` it builds
- **A tool's own error is a result.** The server answers the second
  reservation with `isError` ("only 0 of B-200 on hand"), text only. It
  reaches the node as `{"error":"only 0 of B-200 on hand"}`, the shape a
  model is given for a refusal, and the node goes on and marks the line
  backordered. A protocol or transport failure would instead fail the call,
  as `StrataError::ToolCallFailed`
- **Closing.** `client.close().await` closes the server's stdin and waits for
  it to exit. The server writes how many tool calls it served to its stderr.
  The client reports each stderr line to its log hook
  (`McpClientOptions::with_log`) at `McpLogLevel::Debug`, as
  `stderr: <line>`. That line can reach the log just after `close()` returns,
  so the demo waits for it (up to 2 s). Four calls reached the server;
  `warehouse.delete_sku` never did. Dropping the last handle without closing
  would stop the server's process tree at once

*Engines differ:*

- **A local tool's result.** A .NET `[StrataTool]` that returns an object
  gives `{"result": {...}}`. A Rust `#[strata_tool]` returns a
  `Result<String, StrataError>`, and the `String` is the result as it is. Here
  it is the `Quote` struct serialized by serde: `{"sku":"A-100","quantity":1,"total":2.5}`
- **JSON key order.** The server writes `{"sku", "name", "on_hand"}`. The
  Rust client reads `structuredContent` into a `serde_json::Value`, which keeps
  an object's keys sorted, so the registry returns
  `{"name":"Widget","on_hand":12,"sku":"A-100"}`. .NET keeps the server's
  order. The values are the same
- **Tool names are interned in Rust.** `StrataTool::name()` is a
  `&'static str`, so `datalium-strata-mcp` keeps each distinct name it learns
  from a server (`warehouse.reserve`) for the life of the process. It does
  this once per name, so reconnecting doesn't leak more. .NET needs nothing
  like it
- **The client's API.** Rust registers a server's tools with
  `tool_set(..)` + `register_mcp(..)` (`McpRegistryExt`), and closes with
  `close().await`. .NET registers `client.ToolProvider(..)` with
  `ToolRegistry.Register` and closes by disposing the client

## What it does *not* cover

- **Streamable HTTP.** `McpServer::http(url)` (with `HttpServer::header` and
  an access token hook, `access_token`) reaches a remote server. Its calls
  look the same to the registry. This demo stays offline
- **Authorization.** A stdio server takes its credentials from its
  environment (`StdioServer::env`). Full OAuth 2.1 is not built
- **A model.** See [`23-agent-tool-loop`](../23-agent-tool-loop) for
  `datalium-strata-ai`'s `Agent` driving a registry from a (scripted) model.
  MCP tools in that registry would be offered to the model like the others.
  For an agent, `mcp_fault_classifier` makes the transient protocol failures
  transient
- **Resources, prompts, sampling, roots, elicitation and subscriptions.**
  Strata's client is for tools only
- **Strata as an MCP server.** Strata is a client. The server here is the
  demo's own, about 150 lines, so the demo runs anywhere. It is not a
  reference implementation
- **Restarts.** A stdio server that exits is started again on the next call
  (`with_restart_on_exit`, on by default). None exits here

## Run it

Once `1.0.0-rc.1` is on crates.io:

```bash
cargo run -p mcp-tools
```

Until then, build against a local clone of `datalium/strata` next to this
repo. Cargo's `--config` adds a `[patch.crates-io]` for each Strata crate
without editing `Cargo.toml`. Put the flags after the subcommand, so
`cargo clippy` gets them too:

```bash
S=../strata/rust
cargo run -p mcp-tools \
  --config "patch.crates-io.datalium-strata.path='$S/strata-core'" \
  --config "patch.crates-io.datalium-strata-macros.path='$S/strata-macros'" \
  --config "patch.crates-io.datalium-strata-memory.path='$S/strata-memory'" \
  --config "patch.crates-io.datalium-strata-ai.path='$S/strata-ai'" \
  --config "patch.crates-io.datalium-strata-host.path='$S/strata-host'" \
  --config "patch.crates-io.datalium-strata-mcp.path='$S/strata-mcp'"
```

That changes `Cargo.lock`. Don't commit it.

The demo checks every claim (the protocol version, the tools, the registry's
names, the left-out tool, the audit log, the final state and the server's
last line) and exits non-zero if one fails. Output of a real run, against
`datalium/strata` `main` @ `253c104` (still version `0.9.0`, with the MCP
client of #66):

```
=== 1. Connect: the demo starts itself as an MCP server, over stdio ===
  Protocol version: 2026-07-28
  Server:           warehouse 1.0.0
  The server's tools:
    lookup_stock  Units on hand for a SKU. (readOnlyHint)
    reserve       Reserves units of a SKU for an order.
    delete_sku    Deletes a SKU and its stock. (destructiveHint)

=== 2. One registry: a local #[strata_tool] and the server's tools, side by side ===
  Registered: quote, warehouse.lookup_stock, warehouse.reserve
  warehouse.reserve takes (sku, quantity), from the server's input schema.
  warehouse.delete_sku: no such tool. Include left it out, so nothing that uses this registry can call it.

=== 3. Middleware: one audit log around every call, local or MCP ===
    → quote {"sku":"A-100","quantity":1}
    ← {"sku":"A-100","quantity":1,"total":2.5}
    → warehouse.lookup_stock {"sku":"A-100"}
    ← {"name":"Widget","on_hand":12,"sku":"A-100"}
  A #[strata_tool] returns the String it builds. An MCP tool returns its structured content, as compact JSON.

=== 4. A graph: a node calls the tools, as a model's tool loop would ===
    → warehouse.lookup_stock {"sku":"A-100"}
    ← {"name":"Widget","on_hand":12,"sku":"A-100"}
    → quote {"sku":"A-100","quantity":3}
    ← {"sku":"A-100","quantity":3,"total":7.5}
    → warehouse.reserve {"sku":"A-100","quantity":3}
    ← {"left":9,"reserved":3,"sku":"A-100"}
    → warehouse.reserve {"sku":"B-200","quantity":1}
    ← {"error":"only 0 of B-200 on hand"}
  Final state of order-1001:
    reserved:    3 x A-100 (9 left)
    backordered: 1 x B-200 (only 0 of B-200 on hand)

=== 5. Close: the client stops the server ===
  close().await closed the server's stdin. The server's last words, on its stderr,
  which the client reads and reports to its log:
    warehouse: stopped after 4 tool calls
  Four calls reached the server: warehouse.delete_sku never did.

=== Done: an MCP server's tools, registered, filtered, wrapped in middleware and called from a graph. ===
```
