# 23 — agent-tool-loop

The agent from `design/44-LLM-Integration.md` ("The agent's loop", "Tool
names", "Structured output"): `Agent<O>` and `add_agent_node` in
`datalium-strata-ai`. A support desk's refund agent looks an order up, issues
a refund through a `ToolRegistry`, and answers with a typed `RefundDecision`
(`#[derive(Deserialize, StrataSchema)]`) that is parsed, validated and
repaired. The demo then breaks the loop on purpose: bad tool calls, a failing
tool, a bad answer, and a model that never stops calling tools.

**The model is a stand-in.** `ScriptedModel` is a small `ChatClient` that
plays a script, one turn per request, and records what it was sent. A turn is
a function of the conversation, so it can read the tool results (an order's
total, a refund id) the way a model would, but there is no intelligence in it:
the script decides everything. That keeps the demo offline, with no API key,
and makes every run identical. The agent only sees `Arc<dyn ChatClient>`, so it
can't tell the difference. [`03-llm-tool-calling`](../03-llm-tool-calling)
runs the same kind of loop against a real OpenAI model.

The .NET twin is
[`strata-demos-dotnet/demos/23-agent-tool-loop`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/23-agent-tool-loop):
same story, same scenarios, same checks. Where the engines differ, it says so
below.

## What it demonstrates

- **The loop** (scenario 1). Each turn is one model call with the registry's
  tools declared. The agent dispatches the answer's tool calls through the
  registry, sends the results back in **one tool message, in the model's
  order** (turn 1 has two calls, so its tool message has two results), and
  goes round again. An answer with no tool calls ends the loop.
- **`AgentResult<O>`**: `output`, `turns`, every tool call as
  `AgentToolCall { name, call_id, result }`, the whole conversation in
  `messages`, and `usage` summed over the turns (checked against what the
  stand-in reported).
- **Provider-safe tool names.** `orders.lookup` is declared as
  `orders_lookup` (`ToolDeclarations::for_registry(&registry)`, which the
  agent builds once). The model calls `orders_lookup`; the agent maps it
  back, so the registry, its `ToolCallStarted` events and
  `AgentToolCall::name` all see `orders.lookup`, with the agent's id as
  `caller_id` and the model's id as `call_id`.
- **A registry `subset`.** The desk has four tools; the agent gets three.
  `accounts.close` is not declared to the model, and a call to it is refused.
- **A typed answer** (scenario 2). `Agent<RefundDecision>` asks every turn for
  the struct's JSON schema as the response format
  (`ResponseFormat::JsonSchema`, strict: every property required, the
  `Option` one as itself or `null`, `additionalProperties: false`). The
  answer is parsed (a Markdown fence around it is stripped), then the
  `with_validate` function checks what a schema can't say. A bad answer goes
  back with the problems listed: first a missing `reply` (it doesn't parse),
  then an `outcome` of `denied` (it doesn't validate). With
  `with_max_repairs(2)` the third answer is the output, and `turns` counts
  the repair turns. With the default of 1 (scenario 2b) the node fails:
  `agent 'support-agent' gave no valid answer: …`.
- **Refusals go back to the model** (scenario 3), each as `{"error": …}` in
  the agent's words, and the loop goes on: a tool that doesn't exist
  (`was not found`), a tool outside the subset (`is not permitted here`),
  arguments the tool can't read (`invalid arguments for tool …`), and
  arguments that aren't a JSON object. None of those tools ran.
- **Any other tool failure fails the node** (scenario 3b): the refund gateway
  fails, and the node fails with `tool 'refunds.issue' failed: …`. With no
  fault classifier (`AgentOptions::with_fault_classifier`) a tool's failure
  is fatal, so `RecoveryPolicy::retry(2)` doesn't retry it. With
  `ToolFailures::ReturnToModel` (scenario 3c) the same failure is sent to the
  model instead, recorded as `Failed`, and the model escalates.
- **Max turns** (scenario 4, a text agent, `Agent<String>`). The model keeps
  polling `shipping.track`. One tool turn more than `with_max_turns(3)`
  fails the node **transiently** before that turn's call runs, so the node's
  `RecoveryPolicy::retry(1)` runs it again. The retry **starts the
  conversation over**: its first request holds only the two prompt messages.
  The conversation lives for one attempt, and the result describes only the
  attempt that answered.
- **The agent node is an ordinary node**: `add_agent_node(name, agent,
  prompt, apply)` builds the messages from the state and folds the result
  back into it. Here it is followed by a plain `Notify` node, and takes a
  recovery policy like any fallible node.
- **Tools with state, the Rust way.** A `#[strata_tool]` is a free function,
  not a method, so the desk is a run service
  (`RunConfig::with_service(desk)`): each tool takes the call's
  `ToolCallContext` and finds the desk with `ctx.service::<SupportDesk>()`.
  The agent builds that context from the node's (`ToolCallContext::from`), so
  services reach the tools. Each scenario gets a fresh desk and counts what
  ran.

Some things the demo shows that are easy to miss:

- The agent and the registry read the same call differently. Arguments the
  tool can't read are `Refused` in `AgentToolCall` but `Failed` in the
  registry's `ToolCallCompleted`. Arguments that aren't a JSON object are
  refused before dispatch, so the registry never sees the call and records no
  event for it.
- "Not permitted" needs the model to use the excluded tool's Strata name
  (`accounts.close`). An excluded tool is never declared, so its provider name
  (`accounts_close`) maps to nothing and comes back as "not found".
- **A `#[strata_tool]` function's own error is dropped.** The generated code
  turns any `Err` into `StrataError::ToolExecutionFailure`, so the agent can
  only say `tool 'x' failed: the tool returned an error`. That's why
  `refunds.issue` is a hand-written `StrataTool` (reusing its arguments'
  derived schema): it returns `StrataError::ToolCallFailed { tool, message }`,
  which the agent passes on as `card ending 4242 is closed`. The agent
  refuses back to the model only `ToolArgumentParseFailure` (and not found,
  not permitted, a middleware's denial); every other error is the tool's
  failure. An unknown order is a `null` answer, not an error.

### Where this differs from the .NET twin

- **Tool results go back as the tool's text.** A Rust tool returns a
  `String`, sent to the model as it is: `delivered`, `RF-0001`, or the JSON
  the lookup serialized. .NET wraps each result as `{"result": …}`.
- **Amounts are `f64`**, so `39.90` is `39.9` in JSON, and the stand-in's
  made-up output count is 115, not .NET's 116 (one character less to count).
- **JSON objects print with their keys sorted** (`serde_json`'s map), so the
  refund call's arguments and the schema's `required` list
  (`[orderId, outcome, refundId, refunded, reply]`) read in a different order
  from .NET's. The check is on the set: every property required.
- **The parse failure is serde's** (``missing field `reply` at line 1
  column 70``), not System.Text.Json's.
- **The bad-arguments refusal is worded the same as in .NET.**
  `#[strata_tool]` reports what is wrong through `tools::argument_problems`
  (`argument 'orderId' must be a string`), the rule both engines share, and
  the check asserts that exact text (`BAD_ARGUMENTS_REFUSAL` in
  `src/main.rs`). Before Strata's `argument_problems` change, Rust passed
  on serde's message instead.
- **State for the tools** comes from a run service (above), where .NET's
  `[StrataTool]` methods are instance methods on the desk.
- **The model's token counts** are reported as `ModelUsage` with optional
  counts; `AgentResult::usage` sums them.

## What it does *not* cover

- **A real model**: [`03-llm-tool-calling`](../03-llm-tool-calling) (OpenAI,
  needs `OPENAI_API_KEY`).
- **Streaming the turns' text** under `run_streaming`:
  [`06-token-streaming`](../06-token-streaming). This demo never streams
  (`ScriptedModel` answers a stream request too, as a client should).
- **Model faults** (a 429 or 5xx, `Retry-After`) and failing over between
  models: [`24-model-routes`](../24-model-routes). Backoff between retries:
  [`08-error-recovery`](../08-error-recovery).
- **Tool middleware**, including a middleware's denial, which the agent sends
  back as `{"error": "denied: …"}`:
  [`26-tool-registry-and-middleware`](../26-tool-registry-and-middleware).
- **A fault classifier** (`with_fault_classifier`), which makes a tool's
  failure transient so a retry can run it again.
- **Changing a turn's request** (`with_configure_turn`; demo
  [03](../03-llm-tool-calling) uses it): every turn here sends the same template.
- **`with_tool_concurrency`**: a turn's calls run one after another here, the
  default.
- **Name collisions.** `ToolDeclarations` makes clashing names unique with
  `_2`, `_3` and cuts names to 64 characters; these names don't clash.
- **Run events and spans** beyond the tool and retry events checked here:
  [`27-run-observability`](../27-run-observability).
- **Keeping a conversation** past one attempt. Strata doesn't persist it;
  [`25-long-term-memory`](../25-long-term-memory) covers memory.

## Run it

```bash
cargo run -p agent-tool-loop
```

Takes well under a second once built. The output is the same on every run
(the token counts are the stand-in's made-up numbers):

```
=== Scenario 1: look the order up, refund it, answer ===
  The tools as the model sees them (ToolDeclarations::for_registry(subset)):
    orders.lookup   -> orders_lookup
    shipping.track  -> shipping_track
    refunds.issue   -> refunds_issue
  The conversation after the prompt:
    model calls orders_lookup {"orderId":"A-1001"}  (call_1)
    model calls shipping_track {"orderId":"A-1001"}  (call_2)
    tool message, 2 result(s):
      call_1: {"orderId":"A-1001","customerId":"C-77","total":39.9,"status":"delivered, box crushed in transit","daysSinceDelivery":3}
      call_2: delivered
    model calls refunds_issue {"amount":39.9,"orderId":"A-1001","reason":"damaged in transit"}  (call_3)
    tool message, 1 result(s):
      call_3: RF-0001
    model answers: {"orderId":"A-1001","outcome":"refunded","refunded":39.9,"refundId":"RF-0001","reply":"Sorry about the crushed box. I have refunded 39.90 to your card (RF-0001)."}
  Turns: 3. Tool calls (Strata names, the model's ids):
    orders.lookup   call_1  Succeeded
    shipping.track  call_2  Succeeded
    refunds.issue   call_3  Succeeded
  ToolCallStarted events: orders.lookup call_1 caller=support-agent, shipping.track call_2 caller=support-agent, refunds.issue call_3 caller=support-agent
  Usage summed over the turns: 480 in, 115 out (the model reported 480 and 115).
  Output: RefundDecision { order_id: "A-1001", outcome: "refunded", refunded: 39.9, refund_id: Some("RF-0001"), reply: "Sorry about the crushed box. I have refunded 39.90 to your card (RF-0001)." }
  Refunds issued: RF-0001 A-1001 39.9
  Notify sent: to C-77: refunded: Sorry about the crushed box. I have refunded 39.90 to your card (RF-0001).

=== Scenario 2: an answer that doesn't parse, then one that doesn't validate -- max repairs 2 ===
  Every turn asks for JSON schema 'RefundDecision': required [orderId, outcome, refundId, refunded, reply], additionalProperties false, strict true.
  The conversation after the prompt:
    model calls orders_lookup {"orderId":"A-1003"}  (call_1)
    tool message, 1 result(s):
      call_1: {"orderId":"A-1003","customerId":"C-12","total":64.5,"status":"delivered","daysSinceDelivery":70}
    model answers: ```json {"orderId":"A-1003","outcome":"declined","refunded":0,"refundId":null} ```
    repair request: Your answer was not valid: it is not JSON that matches the schema (missing field `reply` at line 1 column 70). Answer again with one JSON object that matches the schema.
    model answers: {"orderId":"A-1003","outcome":"denied","refunded":0,"refundId":null,"reply":"A-1003 was delivered 70 days ago, past our 30-day window."}
    repair request: Your answer was not valid: outcome 'denied' is not one of refunded, declined, escalated. Answer again with one JSON object that matches the schema.
    model answers: {"orderId":"A-1003","outcome":"declined","refunded":0,"refundId":null,"reply":"A-1003 was delivered 70 days ago, past our 30-day window, so I can't refund it."}
  Turns: 4, repair turns included. Output: RefundDecision { order_id: "A-1003", outcome: "declined", refunded: 0.0, refund_id: None, reply: "A-1003 was delivered 70 days ago, past our 30-day window, so I can't refund it." }

=== Scenario 2b: the same answers with max repairs 1 -- the node fails ===
  NodeFailed at 'Resolve' after 1 attempt(s): agent 'support-agent' gave no valid answer: outcome 'denied' is not one of refunded, declined, escalated

=== Scenario 3: four bad calls in one turn -- each refused back to the model ===
  The conversation after the prompt:
    model calls orders_cancel {"orderId":"A-1001"}  (call_1)
    model calls accounts.close {"customerId":"C-77"}  (call_2)
    model calls orders_lookup {"orderId":1001}  (call_3)
    model calls refunds_issue <not a JSON object>  (call_4)
    tool message, 4 result(s):
      call_1: {"error":"tool 'orders_cancel' was not found"}
      call_2: {"error":"tool 'accounts.close' is not permitted here"}
      call_3: {"error":"invalid arguments for tool 'orders_lookup': argument 'orderId' must be a string"}
      call_4: {"error":"the arguments for tool 'refunds_issue' aren't a JSON object"}
    model calls orders_lookup {"orderId":"A-1001"}  (call_5)
    tool message, 1 result(s):
      call_5: {"orderId":"A-1001","customerId":"C-77","total":39.9,"status":"delivered, box crushed in transit","daysSinceDelivery":3}
    model answers: {"orderId":"A-1001","outcome":"escalated","refunded":0,"refundId":null,"reply":"I can't close accounts. A colleague will contact you about A-1001 and your account."}
  Agent's record:  orders_cancel Refused, accounts.close Refused, orders.lookup Refused, refunds.issue Refused, orders.lookup Succeeded
  Registry events: orders_cancel Refused, accounts.close Refused, orders.lookup Failed, orders.lookup Succeeded
  Tools that actually ran: orders.lookup(A-1001)

=== Scenario 3b: the refund gateway fails -- ToolFailures::FailNode (the default), retry(2) ===
  NodeFailed at 'Resolve' after 1 attempt(s): tool 'refunds.issue' failed: card ending 4242 is closed

=== Scenario 3c: the same failure with ToolFailures::ReturnToModel ===
  The conversation after the prompt:
    model calls orders_lookup {"orderId":"A-1001"}  (call_1)
    tool message, 1 result(s):
      call_1: {"orderId":"A-1001","customerId":"C-77","total":39.9,"status":"delivered, box crushed in transit","daysSinceDelivery":3}
    model calls refunds_issue {"amount":39.9,"orderId":"A-1001","reason":"damaged in transit"}  (call_2)
    tool message, 1 result(s):
      call_2: {"error":"tool 'refunds_issue' failed: card ending 4242 is closed"}
    model answers: {"orderId":"A-1001","outcome":"escalated","refunded":0,"refundId":null,"reply":"Your card is closed, so a colleague will arrange the refund another way."}

=== Scenario 4: the model keeps polling -- max turns 3, retry(1) on the node ===
  Attempt 1 failed transiently: agent 'support-agent' made 3 tool turns without answering
  Tool calls run, by attempt: attempt 1: 3, attempt 2: 1 (the 4th turn's call never ran).
  Attempt 2's first request held 2 messages: the prompt, nothing from attempt 1.
  Turns in the result: 2 (attempt 2 only). Answer: Your parcel A-1002 has left the Rotterdam hub and is on its way. There is no delivery date yet.

=== Done: tool calls dispatched and answered, a typed answer repaired, refusals told to the model, and a runaway loop retried from scratch. ===
```

The demo checks each claim as it runs (the declarations, the model and tools
every turn sends, the tool message order, the Strata names in events, the
summed usage, the schema, the repair requests, every refusal's wording, which
tools actually ran, the attempts and the fresh conversation on retry) and
exits non-zero if one fails.

## Files

- `src/main.rs` — the scenarios and their checks
- `src/scripted_model.rs` — the stand-in model: a scripted `ChatClient` that
  records every request and assembles its answers with `ChatUpdates`, as a
  real client does
- `src/support_desk.rs` — the four tools over an in-memory order book: three
  `#[strata_tool]` functions and the hand-written `refunds.issue`
- `Cargo.toml` — `datalium-strata`, `datalium-strata-ai`,
  `datalium-strata-memory` (the in-memory checkpoint store), `serde`,
  `serde_json`, `tokio`, and `futures-util` (the stand-in's stream)
