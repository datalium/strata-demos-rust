//! The agent (design/44-LLM-Integration.md, "The agent's loop", "Tool names",
//! "Structured output"): `Agent<O>` and `add_agent_node` from
//! `datalium-strata-ai`.
//!
//! A support desk's refund agent. The agent node asks the model, dispatches the
//! tool calls it makes through a `ToolRegistry` (support_desk.rs), sends the
//! results back, and stops when the model answers without calling a tool. The
//! answer is a typed `RefundDecision`, parsed and validated, and sent back to
//! be repaired when it is bad.
//!
//! The model is a stand-in (scripted_model.rs): a `ChatClient` that plays a
//! script and records what it was sent, so the demo runs offline with no API
//! key. The agent can't tell the difference. Demo 03 runs the same kind of loop
//! against a real OpenAI model.
//!
//!   1. The loop: two tool turns (one with two calls), then a typed answer.
//!      Provider-safe names mapped back.
//!   2. A typed answer repaired: a reply that doesn't parse, then one that
//!      doesn't validate (max repairs 2); then the same with max repairs 1,
//!      which fails the node.
//!   3. Refusals go back to the model as {"error": ...}; a tool's own failure
//!      fails the node, unless `ToolFailures::ReturnToModel` says to tell the
//!      model instead.
//!   4. Max turns: too many tool turns fail the node transiently, and a retry
//!      starts the conversation over.

mod scripted_model;
mod support_desk;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, ToolCallResult};
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata_ai::{
    Agent, AgentOptions, AgentResult, Content, Message, ResponseFormat, Role, StateGraphAiExt,
    ToolDeclarations, ToolFailures,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use scripted_model::{MODEL, Reply, ScriptedModel, Turn, tool_result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use support_desk::SupportDesk;

const AGENT_ID: &str = "support-agent";

/// The agent's typed answer. Its JSON schema is the response format every turn
/// asks for.
#[derive(Debug, Clone, PartialEq, Deserialize, StrataSchema)]
#[serde(rename_all = "camelCase")]
struct RefundDecision {
    order_id: String,
    outcome: String,
    refunded: f64,
    refund_id: Option<String>,
    reply: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ticket {
    id: String,
    customer: String,
    message: String,
    resolution: Option<String>,
    turns: u32,
    sent: Option<String>,
}

impl Ticket {
    fn new(id: &str, customer: &str, message: &str) -> Self {
        Self {
            id: id.into(),
            customer: customer.into(),
            message: message.into(),
            resolution: None,
            turns: 0,
            sent: None,
        }
    }
}

/// Where a graph's `apply` leaves the agent's result for the demo to read.
type Seen<O> = Arc<Mutex<Option<AgentResult<O>>>>;

#[tokio::main]
async fn main() {
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    // --- Scenario 1: the model <-> tool loop. ---

    println!("=== Scenario 1: look the order up, refund it, answer ===");
    let desk = SupportDesk::new();

    // Turn 1 makes two calls at once; the model knows the tools only by their
    // provider-safe names. Turn 2 refunds the total the lookup returned. Turn 3
    // has no tool calls, so it is the answer: JSON matching the RefundDecision
    // schema.
    let model = ScriptedModel::new(vec![
        turn(|_| {
            Reply::call(&[
                ("call_1", "orders_lookup", r#"{"orderId":"A-1001"}"#),
                ("call_2", "shipping_track", r#"{"orderId":"A-1001"}"#),
            ])
        }),
        turn(|c| refund_call(c, "call_3", "call_1")),
        turn(|c| {
            let total = total(c, "call_1");
            let refund_id = tool_result(c, "call_3");
            Reply::say(format!(
                r#"{{"orderId":"A-1001","outcome":"refunded","refunded":{total},"refundId":"{refund_id}","reply":"Sorry about the crushed box. I have refunded 39.90 to your card ({refund_id})."}}"#
            ))
        }),
    ]);
    let agent = refund_agent(model.clone(), 1, ToolFailures::FailNode);
    let seen: Seen<RefundDecision> = Seen::default();
    let (result, events) = run(
        &refund_graph(agent.clone(), seen.clone(), None),
        "ticket-1",
        Ticket::new(
            "T-1",
            "C-77",
            "My order A-1001 arrived with the box crushed and the lamp broken.",
        ),
        &desk,
        &store,
    )
    .await;
    let ticket = expect_completed(result);
    let seen = taken(&seen);
    let requests = model.requests();

    println!("  The tools as the model sees them (ToolDeclarations::for_registry(subset)):");
    for declaration in agent.declarations().declarations() {
        println!(
            "    {:<15} -> {}",
            agent.declarations().strata_name(&declaration.name),
            declaration.name
        );
    }
    println!("  The conversation after the prompt:");
    print_conversation(&seen.messages);
    println!(
        "  Turns: {}. Tool calls (Strata names, the model's ids):",
        seen.turns
    );
    for call in &seen.tool_calls {
        println!("    {:<15} {}  {:?}", call.name, call.call_id, call.result);
    }
    println!(
        "  ToolCallStarted events: {}",
        tool_started(&events).join(", ")
    );
    let input: u64 = requests.iter().map(|r| r.input_tokens).sum();
    let output: u64 = requests.iter().map(|r| r.output_tokens).sum();
    println!(
        "  Usage summed over the turns: {} in, {} out (the model reported {input} and {output}).",
        seen.usage.input_tokens.unwrap_or_default(),
        seen.usage.output_tokens.unwrap_or_default()
    );
    println!("  Output: {:?}", seen.output);
    let refunds = desk.refunds();
    println!(
        "  Refunds issued: {}",
        refunds
            .iter()
            .map(|(id, order, amount)| format!("{id} {order} {amount}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  Notify sent: {}",
        ticket.sent.as_deref().unwrap_or("<nothing>")
    );

    let declared = ["orders_lookup", "shipping_track", "refunds_issue"];
    check(
        declaration_names(agent.declarations()) == declared,
        "expected the subset's three tools under provider-safe names, in registry order",
    );
    check(
        declaration_names(&ToolDeclarations::for_registry(&agent_tools()))
            == declaration_names(agent.declarations()),
        "expected the agent's declarations to be ToolDeclarations::for_registry(its registry)",
    );
    check(
        agent
            .declarations()
            .provider_name_of("accounts.close")
            .is_none(),
        "expected accounts.close, outside the subset, not to be declared",
    );
    check(
        requests.iter().all(|r| {
            r.request.tools.iter().map(|t| t.name.as_str()).eq(declared)
                && r.request.model.as_deref() == Some(MODEL)
        }),
        "expected every turn to send the same declarations and the agent's model",
    );
    check(
        seen.turns == 3 && requests.len() == 3,
        "expected three turns",
    );
    check(
        seen.tool_calls
            .iter()
            .map(|c| format!("{} {} {:?}", c.name, c.call_id, c.result))
            .eq([
                "orders.lookup call_1 Succeeded",
                "shipping.track call_2 Succeeded",
                "refunds.issue call_3 Succeeded",
            ]),
        "expected every call recorded under its Strata name with the model's id",
    );
    let tool_messages: Vec<&Message> = seen
        .messages
        .iter()
        .filter(|m| m.role == Role::Tool)
        .collect();
    check(
        tool_messages.len() == 2 && result_ids(tool_messages[0]) == ["call_1", "call_2"],
        "expected turn 1's two results in one tool message, in the model's order",
    );
    check(
        tool_started(&events)
            == [
                "orders.lookup call_1 caller=support-agent",
                "shipping.track call_2 caller=support-agent",
                "refunds.issue call_3 caller=support-agent",
            ],
        "expected the registry's events to see the Strata names, the agent as caller and the model's ids",
    );
    check(
        seen.usage.input_tokens == Some(input) && seen.usage.output_tokens == Some(output),
        "expected the usage summed over the turns",
    );
    check(
        seen.output
            == RefundDecision {
                order_id: "A-1001".into(),
                outcome: "refunded".into(),
                refunded: 39.9,
                refund_id: Some("RF-0001".into()),
                reply: seen.output.reply.clone(),
            },
        "expected the parsed RefundDecision",
    );
    check(
        refunds == [("RF-0001".to_string(), "A-1001".to_string(), 39.9)],
        "expected one refund of the order's total",
    );
    check(
        ticket.turns == 3 && ticket.sent.is_some(),
        "expected the agent node's result folded into the state, and Notify to run after it",
    );

    // --- Scenario 2: a typed answer, parsed, validated and repaired. ---

    println!();
    println!(
        "=== Scenario 2: an answer that doesn't parse, then one that doesn't validate -- max repairs 2 ==="
    );
    let model = ScriptedModel::new(decline_script(true));
    let agent = refund_agent(model.clone(), 2, ToolFailures::FailNode);
    let seen: Seen<RefundDecision> = Seen::default();
    let (result, _) = run(
        &refund_graph(agent.clone(), seen.clone(), None),
        "ticket-2",
        Ticket::new("T-2", "C-12", "Refund A-1003 please, I just don't like it."),
        &SupportDesk::new(),
        &store,
    )
    .await;
    expect_completed(result);
    let seen = taken(&seen);
    let requests = model.requests();
    let Some(ResponseFormat::JsonSchema {
        name,
        schema,
        strict,
    }) = agent.response_format()
    else {
        fail("expected a typed agent to ask for a JSON schema");
    };
    let required: Vec<&str> = schema["required"]
        .as_array()
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    println!(
        "  Every turn asks for JSON schema '{name}': required [{}], additionalProperties {}, strict {strict}.",
        required.join(", "),
        schema["additionalProperties"]
    );
    println!("  The conversation after the prompt:");
    print_conversation(&seen.messages);
    println!(
        "  Turns: {}, repair turns included. Output: {:?}",
        seen.turns, seen.output
    );
    let repairs: Vec<String> = seen.messages[2..]
        .iter()
        .filter(|m| m.role == Role::User)
        .map(Message::text)
        .collect();
    let mut properties: Vec<&str> = schema["properties"]
        .as_object()
        .map(|p| p.keys().map(String::as_str).collect())
        .unwrap_or_default();
    properties.sort_unstable();
    let mut sorted_required = required.clone();
    sorted_required.sort_unstable();
    check(
        name == "RefundDecision" && schema["additionalProperties"] == Value::Bool(false) && *strict,
        "expected the strict RefundDecision schema, closed to other properties",
    );
    check(
        properties == ["orderId", "outcome", "refundId", "refunded", "reply"]
            && sorted_required == properties,
        "expected every property required, the optional refundId included",
    );
    check(
        requests
            .iter()
            .all(|r| r.request.response_format.as_ref() == agent.response_format()),
        "expected every turn to ask for the same format",
    );
    check(
        repairs.len() == 2
            && repairs[0]
                .starts_with("Your answer was not valid: it is not JSON that matches the schema")
            && repairs[1]
                == "Your answer was not valid: outcome 'denied' is not one of refunded, declined, escalated. Answer again with one JSON object that matches the schema.",
        "expected a repair request for the parse failure, then one for the validation failure",
    );
    check(
        seen.turns == 4 && seen.tool_calls.len() == 1 && seen.output.outcome == "declined",
        "expected the third answer to be the output",
    );

    println!();
    println!("=== Scenario 2b: the same answers with max repairs 1 -- the node fails ===");
    let model = ScriptedModel::new(decline_script(false));
    let agent = refund_agent(model.clone(), 1, ToolFailures::FailNode);
    let (result, _) = run(
        &refund_graph(agent, Seen::default(), None),
        "ticket-2b",
        Ticket::new("T-2", "C-12", "Refund A-1003 please, I just don't like it."),
        &SupportDesk::new(),
        &store,
    )
    .await;
    let (node, attempts, message) = expect_node_failed(result);
    println!("  NodeFailed at '{node}' after {attempts} attempt(s): {message}");
    check(
        node == "Resolve"
            && attempts == 1
            && message
                == "agent 'support-agent' gave no valid answer: outcome 'denied' is not one of refunded, declined, escalated",
        "expected the node to fail once the one repair was used",
    );
    check(
        model.requests().len() == 3,
        "expected no turn after the repair was used up",
    );

    // --- Scenario 3: refusals go back to the model; another tool failure fails the node. ---

    println!();
    println!("=== Scenario 3: four bad calls in one turn -- each refused back to the model ===");
    let desk = SupportDesk::new();
    let model = ScriptedModel::new(vec![
        turn(|_| {
            Reply::call(&[
                ("call_1", "orders_cancel", r#"{"orderId":"A-1001"}"#), // no such tool
                ("call_2", "accounts.close", r#"{"customerId":"C-77"}"#), // a real tool, outside the agent's subset
                ("call_3", "orders_lookup", r#"{"orderId":1001}"#), // a number where the tool takes a string
                ("call_4", "refunds_issue", r#"{"orderId":"A-1001","amou"#), // cut off: not a JSON object
            ])
        }),
        turn(|_| Reply::call(&[("call_5", "orders_lookup", r#"{"orderId":"A-1001"}"#)])),
        turn(|_| {
            Reply::say(
                r#"{"orderId":"A-1001","outcome":"escalated","refunded":0,"refundId":null,"reply":"I can't close accounts. A colleague will contact you about A-1001 and your account."}"#,
            )
        }),
    ]);
    let agent = refund_agent(model, 1, ToolFailures::FailNode);
    let seen: Seen<RefundDecision> = Seen::default();
    let (result, events) = run(
        &refund_graph(agent.clone(), seen.clone(), None),
        "ticket-3",
        Ticket::new("T-3", "C-77", "Refund A-1001 and close my account."),
        &desk,
        &store,
    )
    .await;
    expect_completed(result);
    let seen = taken(&seen);
    println!("  The conversation after the prompt:");
    print_conversation(&seen.messages);
    println!(
        "  Agent's record:  {}",
        seen.tool_calls
            .iter()
            .map(|c| format!("{} {:?}", c.name, c.result))
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("  Registry events: {}", tool_completed(&events).join(", "));
    println!("  Tools that actually ran: {}", desk.calls().join(", "));
    let first_tool_message = seen
        .messages
        .iter()
        .find(|m| m.role == Role::Tool)
        .expect("a tool message");
    let refused = result_contents(first_tool_message);
    check(
        refused.len() == 4
            && refused[0] == r#"{"error":"tool 'orders_cancel' was not found"}"#
            && refused[1] == r#"{"error":"tool 'accounts.close' is not permitted here"}"#
            && refused[2] == BAD_ARGUMENTS_REFUSAL
            && refused[3]
                == r#"{"error":"the arguments for tool 'refunds_issue' aren't a JSON object"}"#,
        "expected the four refusals in one tool message, in the agent's words",
    );
    check(
        seen.tool_calls.iter().map(|c| c.result).eq([
            ToolCallResult::Refused,
            ToolCallResult::Refused,
            ToolCallResult::Refused,
            ToolCallResult::Refused,
            ToolCallResult::Succeeded,
        ]),
        "expected four refusals and one success",
    );
    check(
        tool_completed(&events)
            == [
                "orders_cancel Refused",
                "accounts.close Refused",
                "orders.lookup Failed",
                "orders.lookup Succeeded",
            ],
        "expected the registry to record the bad arguments as Failed, and never to see the call that wasn't a JSON object",
    );
    check(
        desk.calls() == ["orders.lookup(A-1001)"],
        "expected only the good lookup to run: no account closed, no refund",
    );
    // An excluded tool is never declared, so its provider-safe name maps to
    // nothing: "accounts_close" would be not found, not "not permitted".
    check(
        agent.declarations().strata_name("accounts_close") == "accounts_close",
        "expected no mapping for an undeclared tool",
    );

    println!();
    println!(
        "=== Scenario 3b: the refund gateway fails -- ToolFailures::FailNode (the default), retry(2) ==="
    );
    let model = ScriptedModel::new(gateway_script());
    let agent = refund_agent(model, 1, ToolFailures::FailNode);
    let (result, events) = run(
        &refund_graph(agent, Seen::default(), Some(RecoveryPolicy::retry(2))),
        "ticket-3b",
        Ticket::new("T-4", "C-77", "My order A-1001 arrived broken."),
        &SupportDesk::with_gateway_failure("card ending 4242 is closed"),
        &store,
    )
    .await;
    let (node, attempts, message) = expect_node_failed(result);
    println!("  NodeFailed at '{node}' after {attempts} attempt(s): {message}");
    check(
        node == "Resolve"
            && attempts == 1
            && message == "tool 'refunds.issue' failed: card ending 4242 is closed",
        "expected the tool's failure, under its Strata name, to fail the node",
    );
    check(
        !events
            .iter()
            .any(|e| matches!(e.kind, RunEventKind::RetryScheduled { .. })),
        "expected no retry: a tool failure with no fault classifier is fatal",
    );

    println!();
    println!("=== Scenario 3c: the same failure with ToolFailures::ReturnToModel ===");
    let mut script = gateway_script();
    script.push(turn(|_| {
        Reply::say(
            r#"{"orderId":"A-1001","outcome":"escalated","refunded":0,"refundId":null,"reply":"Your card is closed, so a colleague will arrange the refund another way."}"#,
        )
    }));
    let model = ScriptedModel::new(script);
    let agent = refund_agent(model, 1, ToolFailures::ReturnToModel);
    let seen: Seen<RefundDecision> = Seen::default();
    let (result, _) = run(
        &refund_graph(agent, seen.clone(), None),
        "ticket-3c",
        Ticket::new("T-4", "C-77", "My order A-1001 arrived broken."),
        &SupportDesk::with_gateway_failure("card ending 4242 is closed"),
        &store,
    )
    .await;
    expect_completed(result);
    let seen = taken(&seen);
    println!("  The conversation after the prompt:");
    print_conversation(&seen.messages);
    check(
        seen.tool_calls
            .iter()
            .map(|c| format!("{} {:?}", c.name, c.result))
            .eq(["orders.lookup Succeeded", "refunds.issue Failed"]),
        "expected the failure recorded as Failed",
    );
    let last_tool_message = seen
        .messages
        .iter()
        .rfind(|m| m.role == Role::Tool)
        .expect("a tool message");
    check(
        result_contents(last_tool_message)
            == [r#"{"error":"tool 'refunds_issue' failed: card ending 4242 is closed"}"#],
        "expected the model to be told, under the name it called",
    );
    check(
        seen.output.outcome == "escalated",
        "expected the model to escalate",
    );

    // --- Scenario 4: max turns, and a retry that starts the conversation over. ---

    println!();
    println!("=== Scenario 4: the model keeps polling -- max turns 3, retry(1) on the node ===");
    let desk = SupportDesk::new();

    // Four tool turns in a row: the fourth is one too many. Then the retry, a
    // fresh conversation, where the script polls once and answers, as a real
    // model might on a second try.
    let mut script: Vec<Turn> = ["poll_1", "poll_2", "poll_3", "poll_4", "track_1"]
        .into_iter()
        .map(|id| turn(move |_| Reply::call(&[(id, "shipping_track", r#"{"orderId":"A-1002"}"#)])))
        .collect();
    script.push(turn(|_| {
        Reply::say(
            "Your parcel A-1002 has left the Rotterdam hub and is on its way. There is no delivery date yet.",
        )
    }));
    let model = ScriptedModel::new(script);
    let text_agent = Arc::new(Agent::<String>::new(
        model.clone(),
        Arc::new(agent_tools()),
        AgentOptions::new(AGENT_ID)
            .with_model(MODEL)
            .with_max_turns(3),
    ));
    let seen_text: Seen<String> = Seen::default();
    let graph = StateGraph::<Ticket>::new()
        .add_agent_node("Resolve", text_agent, prompt, {
            let seen_text = seen_text.clone();
            move |ticket: Ticket, result: AgentResult<String>| {
                let ticket = Ticket {
                    resolution: Some(result.output.clone()),
                    turns: result.turns,
                    ..ticket
                };
                *seen_text.lock().unwrap() = Some(result);
                ticket
            }
        })
        .set_recovery_policy("Resolve", RecoveryPolicy::retry(1))
        .compile()
        .expect("the graph is valid");
    let (result, events) = run(
        &graph,
        "ticket-4",
        Ticket::new("T-5", "C-77", "Where is my parcel A-1002?"),
        &desk,
        &store,
    )
    .await;
    let ticket = expect_completed(result);
    let seen_text = taken(&seen_text);
    let requests = model.requests();
    let retries: Vec<&str> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::RetryScheduled { error, .. } => Some(error.as_str()),
            _ => None,
        })
        .collect();
    let mut by_attempt: BTreeMap<u32, usize> = BTreeMap::new();
    for event in &events {
        if matches!(event.kind, RunEventKind::ToolCallStarted { .. }) {
            *by_attempt
                .entry(event.attempt.unwrap_or_default())
                .or_default() += 1;
        }
    }
    println!(
        "  Attempt 1 failed transiently: {}",
        retries.first().copied().unwrap_or("<no retry>")
    );
    println!(
        "  Tool calls run, by attempt: {} (the 4th turn's call never ran).",
        by_attempt
            .iter()
            .map(|(attempt, count)| format!("attempt {attempt}: {count}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let retry_first = &requests[4].request.messages;
    println!(
        "  Attempt 2's first request held {} messages: the prompt, nothing from attempt 1.",
        retry_first.len()
    );
    println!(
        "  Turns in the result: {} (attempt 2 only). Answer: {}",
        seen_text.turns,
        ticket.resolution.as_deref().unwrap_or_default()
    );
    check(
        retries == ["agent 'support-agent' made 3 tool turns without answering"],
        "expected the max turns failure, transient, to be retried once",
    );
    check(
        requests.len() == 6,
        "expected four turns in attempt 1 and two in attempt 2",
    );
    check(
        by_attempt == BTreeMap::from([(1, 3), (2, 1)]),
        "expected three calls in attempt 1 and one in attempt 2",
    );
    check(
        retry_first.len() == 2 && retry_first.iter().all(|m| m.role != Role::Tool),
        "expected the retry to start the conversation over",
    );
    check(
        seen_text.turns == 2
            && seen_text.tool_calls.len() == 1
            && seen_text.tool_calls[0].call_id == "track_1",
        "expected the result to hold only the attempt that answered",
    );
    check(
        requests[0].request.response_format.is_none(),
        "expected a text agent to ask for no response format",
    );

    println!();
    println!(
        "=== Done: tool calls dispatched and answered, a typed answer repaired, refusals told to the model, and a runaway loop retried from scratch. ==="
    );
}

/// What the agent tells the model about `{"orderId":1001}` for a
/// `#[strata_tool]` whose argument is a `String`: the registry's
/// `ToolArgumentParseFailure`, in the agent's words. The detail is
/// `tools::argument_problems`'s, the same words the .NET engine uses.
const BAD_ARGUMENTS_REFUSAL: &str = r#"{"error":"invalid arguments for tool 'orders_lookup': argument 'orderId' must be a string"}"#;

/// The agent's tools: the desk's registry, narrowed to what a support agent may
/// call. accounts.close stays out.
fn agent_tools() -> ToolRegistry {
    support_desk::registry()
        .subset(["orders.lookup", "shipping.track", "refunds.issue"])
        .expect("the desk has these tools")
}

fn refund_agent(
    model: Arc<ScriptedModel>,
    max_repairs: u32,
    failures: ToolFailures,
) -> Arc<Agent<RefundDecision>> {
    Arc::new(Agent::new(
        model,
        Arc::new(agent_tools()),
        AgentOptions::new(AGENT_ID)
            .with_model(MODEL)
            .with_temperature(0.0)
            .with_max_repairs(max_repairs)
            .with_tool_failures(failures)
            .with_validate(validate),
    ))
}

/// What the schema can't say: an outcome from a fixed list, and a refund id for
/// a refund.
fn validate(decision: &RefundDecision) -> Vec<String> {
    let mut problems = Vec::new();
    if !["refunded", "declined", "escalated"].contains(&decision.outcome.as_str()) {
        problems.push(format!(
            "outcome '{}' is not one of refunded, declined, escalated",
            decision.outcome
        ));
    }
    if decision.outcome == "refunded" && (decision.refund_id.is_none() || decision.refunded <= 0.0)
    {
        problems.push("a refund needs a refund id and an amount".to_string());
    }
    problems
}

fn prompt(ticket: &Ticket, _: &NodeContext) -> Vec<Message> {
    vec![
        Message::system(
            "You are Northwind Gear's support agent. Look an order up before deciding anything. \
             Refund an order that arrived damaged within 30 days of delivery; decline other \
             refunds; escalate what you can't do.",
        ),
        Message::user(format!(
            "Ticket {} from customer {}: {}",
            ticket.id, ticket.customer, ticket.message
        )),
    ]
}

/// Resolve (the agent node) -> Notify. The agent node is an ordinary fallible
/// node: edges, a recovery policy.
fn refund_graph(
    agent: Arc<Agent<RefundDecision>>,
    seen: Seen<RefundDecision>,
    policy: Option<RecoveryPolicy>,
) -> CompiledGraph<Ticket> {
    let mut graph = StateGraph::<Ticket>::new()
        .add_agent_node(
            "Resolve",
            agent,
            prompt,
            move |ticket: Ticket, result: AgentResult<RefundDecision>| {
                let ticket = Ticket {
                    resolution: Some(format!(
                        "{}: {}",
                        result.output.outcome, result.output.reply
                    )),
                    turns: result.turns,
                    ..ticket
                };
                *seen.lock().unwrap() = Some(result);
                ticket
            },
        )
        .add_node("Notify", |ticket: Ticket| Ticket {
            sent: Some(format!(
                "to {}: {}",
                ticket.customer,
                ticket.resolution.as_deref().unwrap_or_default()
            )),
            ..ticket
        })
        .add_edge("Resolve", "Notify");
    if let Some(policy) = policy {
        graph = graph.set_recovery_policy("Resolve", policy);
    }
    graph.compile().expect("the graph is valid")
}

fn turn(f: impl Fn(&[Message]) -> Reply + Send + Sync + 'static) -> Turn {
    Box::new(f)
}

/// A refund of the total the lookup `lookup_id` returned, as call `id`.
fn refund_call(conversation: &[Message], id: &str, lookup_id: &str) -> Reply {
    let arguments = format!(
        r#"{{"orderId":"A-1001","amount":{},"reason":"damaged in transit"}}"#,
        total(conversation, lookup_id)
    );
    Reply::call(&[(id, "refunds_issue", &arguments)])
}

/// A lookup of A-1003 (delivered 70 days ago), then answers: one missing
/// "reply" (inside a Markdown fence, which the agent strips), one with an
/// outcome the validator refuses, and, if asked for, a good one.
fn decline_script(good_answer_last: bool) -> Vec<Turn> {
    let mut script = vec![
        turn(|_| Reply::call(&[("call_1", "orders_lookup", r#"{"orderId":"A-1003"}"#)])),
        turn(|_| {
            Reply::say(
                "```json\n{\"orderId\":\"A-1003\",\"outcome\":\"declined\",\"refunded\":0,\"refundId\":null}\n```",
            )
        }),
        turn(|_| {
            Reply::say(
                r#"{"orderId":"A-1003","outcome":"denied","refunded":0,"refundId":null,"reply":"A-1003 was delivered 70 days ago, past our 30-day window."}"#,
            )
        }),
    ];
    if good_answer_last {
        script.push(turn(|_| {
            Reply::say(
                r#"{"orderId":"A-1003","outcome":"declined","refunded":0,"refundId":null,"reply":"A-1003 was delivered 70 days ago, past our 30-day window, so I can't refund it."}"#,
            )
        }));
    }
    script
}

fn gateway_script() -> Vec<Turn> {
    vec![
        turn(|_| Reply::call(&[("call_1", "orders_lookup", r#"{"orderId":"A-1001"}"#)])),
        turn(|c| refund_call(c, "call_2", "call_1")),
    ]
}

/// The order total from a lookup's result, as the JSON number it was.
fn total(conversation: &[Message], call_id: &str) -> String {
    let order: Value =
        serde_json::from_str(tool_result(conversation, call_id)).expect("a lookup returns JSON");
    order["total"].to_string()
}

async fn run(
    graph: &CompiledGraph<Ticket>,
    thread_id: &str,
    ticket: Ticket,
    desk: &Arc<SupportDesk>,
    store: &Arc<dyn CheckpointStore>,
) -> (GraphExecutionResult<Ticket>, Vec<RunEvent>) {
    let (observer, mut receiver) = RunEventChannel::new();
    let config = RunConfig::new()
        .with_observer(observer)
        .with_service(desk.clone());
    let result = graph
        .run_with(thread_id, ticket, store.clone(), config)
        .await;
    // The channel closes after the run's last event.
    let mut events = Vec::new();
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_secs(10), receiver.recv()).await
    {
        events.push(event);
    }
    (result, events)
}

/// Everything after the two prompt messages: each model turn, each tool
/// message, each repair request.
fn print_conversation(messages: &[Message]) {
    for message in &messages[2..] {
        match message.role {
            Role::Tool => {
                let results: Vec<(&str, &str)> = message
                    .content
                    .iter()
                    .filter_map(|content| match content {
                        Content::ToolResult { call_id, content } => {
                            Some((call_id.as_str(), content.as_str()))
                        }
                        _ => None,
                    })
                    .collect();
                println!("    tool message, {} result(s):", results.len());
                for (call_id, content) in results {
                    println!("      {call_id}: {content}");
                }
            }
            Role::User => println!("    repair request: {}", message.text()),
            _ => {
                for content in &message.content {
                    match content {
                        Content::ToolCall {
                            id,
                            name,
                            arguments,
                        } => {
                            // Arguments that aren't a JSON object are kept as
                            // the model's text, a `Value::String`.
                            let arguments = match arguments {
                                Value::String(_) => "<not a JSON object>".to_string(),
                                other => other.to_string(),
                            };
                            println!("    model calls {name} {arguments}  ({id})");
                        }
                        Content::Text(text) => {
                            println!("    model answers: {}", text.replace('\n', " "))
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

fn result_ids(message: &Message) -> Vec<&str> {
    message
        .content
        .iter()
        .filter_map(|content| match content {
            Content::ToolResult { call_id, .. } => Some(call_id.as_str()),
            _ => None,
        })
        .collect()
}

fn result_contents(message: &Message) -> Vec<&str> {
    message
        .content
        .iter()
        .filter_map(|content| match content {
            Content::ToolResult { content, .. } => Some(content.as_str()),
            _ => None,
        })
        .collect()
}

fn declaration_names(declarations: &ToolDeclarations) -> Vec<&str> {
    declarations
        .declarations()
        .iter()
        .map(|d| d.name.as_str())
        .collect()
}

fn tool_started(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::ToolCallStarted {
                tool,
                call_id,
                caller_id,
                ..
            } => Some(format!(
                "{tool} {call_id} caller={}",
                caller_id.as_deref().unwrap_or_default()
            )),
            _ => None,
        })
        .collect()
}

fn tool_completed(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::ToolCallCompleted { tool, result, .. } => {
                Some(format!("{tool} {result:?}"))
            }
            _ => None,
        })
        .collect()
}

fn taken<O>(seen: &Seen<O>) -> AgentResult<O> {
    seen.lock()
        .unwrap()
        .take()
        .unwrap_or_else(|| fail("expected the agent node to have run"))
}

fn expect_completed(result: GraphExecutionResult<Ticket>) -> Ticket {
    match result {
        GraphExecutionResult::Completed(ticket) => ticket,
        other => fail(&format!("expected Completed, got: {}", describe(&other))),
    }
}

fn expect_node_failed(result: GraphExecutionResult<Ticket>) -> (String, u32, String) {
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => (node, attempts, message),
        other => fail(&format!("expected NodeFailed, got: {}", describe(&other))),
    }
}

fn describe(result: &GraphExecutionResult<Ticket>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Failed(error) => format!("Failed({error:?})"),
        GraphExecutionResult::Interrupted { waiting_at, .. } => {
            format!("Interrupted at {waiting_at:?}")
        }
        GraphExecutionResult::Cancelled { node, .. } => format!("Cancelled at '{node}'"),
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
