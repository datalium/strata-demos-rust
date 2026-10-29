//! A live LLM (OpenAI's Chat Completions API, through `datalium-strata-ai`'s
//! `OpenAiChatClient`) decides to call the `add` tool from `02-tool-calling`.
//! Strata's own agent loop (`Agent`, design/44-LLM-Integration.md) does the
//! rest: it declares the registry's tools to the model, dispatches the call
//! the model asks for through the `ToolRegistry`, sends the result back, and
//! returns the model's answer. The demo has no OpenAI request or response
//! code of its own.
//!
//! Requires `OPENAI_API_KEY` in the environment. Costs a small amount of
//! real OpenAI usage each run.
//! Or run it free against a local model: set `OPENAI_BASE_URL` to any
//! OpenAI-compatible server (Ollama, vLLM, LM Studio) and `OPENAI_MODEL` to a
//! model it serves that supports tool calls.

use std::env;
use std::process::ExitCode;
use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::StateGraph;
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata_ai::{
    Agent, AgentOptions, AgentResult, AgentToolCall, Content, Message, OpenAiChatClient,
    OpenAiOptions, StateGraphAiExt, ToolCallResult, ToolChoice,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const DEFAULT_MODEL: &str = "gpt-4o-mini";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LlmState {
    question: String,
    answer: Option<String>,
    tool_result: Option<f64>,
    turns: u32,
    tool_calls: Vec<String>,
}

#[derive(Deserialize, StrataSchema)]
struct AddArgs {
    a: f64,
    b: f64,
}

/// The same `add` tool as demo 02, answering `{"result": <sum>}` and logging
/// who called it. `a` and `b` are what the model sends; `ctx` is filled by
/// the agent that dispatches the call: its `caller_id` is the agent's id and
/// its `call_id` the model's id for the call (design/44-LLM-Integration.md).
#[strata_tool(name = "add", description = "Adds two numbers and returns their sum.")]
async fn add(args: AddArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    println!(
        "[add] {} + {}, called by agent '{}' (model call id {}) in node '{}'",
        args.a,
        args.b,
        ctx.caller_id().unwrap_or_default(),
        ctx.call_id().unwrap_or_default(),
        ctx.node_path().unwrap_or_default(),
    );
    Ok(format!(r#"{{"result":{}}}"#, args.a + args.b))
}

#[tokio::main]
async fn main() -> ExitCode {
    let options = match provider_options() {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    // The client does no retrying of its own (design/44): a transient fault
    // carries the provider's retry hint, and a RecoveryPolicy on the node
    // would do the retrying.
    let chat = Arc::new(OpenAiChatClient::new(options));

    // The registry is what the agent offers the model: every tool registered
    // here, nothing else.
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(AddTool));
    let agent = Arc::new(Agent::<String>::new(
        chat,
        Arc::new(tools),
        // One addition needs one tool turn. A model that keeps calling tools
        // fails the node instead of running up the bill (demo 23 shows the
        // limit firing).
        AgentOptions::new("calculator")
            .with_max_turns(2)
            // Require a tool call on the first turn only, so the model must
            // use `add`, then may answer. A `tool_choice` on the agent's
            // request template would apply to every turn, and the model
            // could never answer.
            .with_configure_turn(|turn, request| {
                if turn.number == 1 {
                    request.tool_choice = Some(ToolChoice::Required);
                }
            }),
    ));

    let graph = StateGraph::<LlmState>::new()
        .add_agent_node(
            "AskLlm",
            agent,
            |state: &LlmState, _| {
                vec![
                    Message::system(
                        "You are a calculator assistant. Always use the `add` tool for arithmetic instead of computing it yourself.",
                    ),
                    Message::user(state.question.clone()),
                ]
            },
            |state: LlmState, result: AgentResult<String>| LlmState {
                tool_calls: result
                    .tool_calls
                    .iter()
                    .map(|call| describe(call, &result.messages))
                    .collect(),
                tool_result: last_add_result(&result),
                turns: result.turns,
                answer: Some(result.output),
                ..state
            },
        )
        .compile()
        .expect("graph is structurally valid");

    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let initial = LlmState {
        question: "What is 47 plus 89?".to_string(),
        answer: None,
        tool_result: None,
        turns: 0,
        tool_calls: Vec::new(),
    };

    let completed = match graph.run("llm-thread", initial, store).await {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => {
            eprintln!("graph execution failed: {err:?}");
            return ExitCode::FAILURE;
        }
        _ => unreachable!("no interrupt boundary, wait or cancellation in this graph"),
    };

    for call in &completed.tool_calls {
        println!("tool call: {call}");
    }
    println!(
        "tool result: {}",
        completed
            .tool_result
            .map_or_else(|| "none".to_string(), |sum| sum.to_string())
    );
    println!("model turns: {}", completed.turns);
    println!("model: {}", completed.answer.as_deref().unwrap_or_default());

    // The model's wording varies; these don't. If the model answered without
    // the tool, say so rather than print a number nobody computed.
    check(
        !completed.tool_calls.is_empty(),
        "the model answered without calling the add tool",
    );
    check(
        completed.tool_result == Some(136.0),
        &format!(
            "expected the add tool to return 136, got {:?}",
            completed.tool_result
        ),
    );
    check(
        completed.turns >= 2,
        "expected one turn for the tool call and one for the answer",
    );
    ExitCode::SUCCESS
}

/// `add {"a":47,"b":89} -> {"result":136} (Succeeded)`: the call as the model
/// made it and as Strata answered it, read back from the agent's conversation
/// by the model's call id.
fn describe(call: &AgentToolCall, messages: &[Message]) -> String {
    let contents = || messages.iter().flat_map(|message| &message.content);
    let asked = contents()
        .find_map(|content| match content {
            Content::ToolCall { id, arguments, .. } if *id == call.call_id => Some(arguments),
            _ => None,
        })
        .map_or_else(String::new, Value::to_string);
    let answered = tool_result_of(messages, &call.call_id).unwrap_or_default();
    format!("{} {asked} -> {answered} ({:?})", call.name, call.result)
}

/// The number the last successful `add` call returned (`{"result": <sum>}`).
fn last_add_result(result: &AgentResult<String>) -> Option<f64> {
    let last = result
        .tool_calls
        .iter()
        .rev()
        .find(|call| call.name == "add" && call.result == ToolCallResult::Succeeded)?;
    let json: Value =
        serde_json::from_str(tool_result_of(&result.messages, &last.call_id)?).ok()?;
    json["result"].as_f64()
}

/// What Strata sent back to the model for the call with `call_id`.
fn tool_result_of<'a>(messages: &'a [Message], call_id: &str) -> Option<&'a str> {
    messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|content| match content {
            Content::ToolResult {
                call_id: id,
                content,
            } if id == call_id => Some(content.as_str()),
            _ => None,
        })
}

fn check(ok: bool, what: &str) {
    if !ok {
        eprintln!("check failed: {what}");
        std::process::exit(1);
    }
}

/// OpenAI by default. `OPENAI_BASE_URL` points the same client at any
/// OpenAI-compatible server instead (e.g. Ollama at
/// `http://localhost:11434/v1`), where the key is optional.
fn provider_options() -> Result<OpenAiOptions, String> {
    let var = |name: &str| env::var(name).ok().filter(|value| !value.is_empty());
    let model = var("OPENAI_MODEL");
    match (var("OPENAI_BASE_URL"), var("OPENAI_API_KEY")) {
        (Some(base_url), api_key) => {
            let model = model.ok_or(
                "OPENAI_BASE_URL is set, so set OPENAI_MODEL too: a model your server has, e.g. qwen2.5:3b.",
            )?;
            let options = OpenAiOptions::compatible(base_url).with_model(model);
            Ok(match api_key {
                Some(api_key) => options.with_api_key(api_key),
                None => options,
            })
        }
        (None, Some(api_key)) => Ok(OpenAiOptions::new(api_key)
            .with_model(model.unwrap_or_else(|| DEFAULT_MODEL.to_string()))),
        (None, None) => Err(
            "OPENAI_API_KEY is not set. Get a key at https://platform.openai.com/api-keys \
             and export it, e.g.:\n\n    export OPENAI_API_KEY=sk-...\n\n\
             Or use a local model, e.g. with Ollama:\n\n    \
             export OPENAI_BASE_URL=http://localhost:11434/v1 OPENAI_MODEL=qwen2.5:3b\n"
                .to_string(),
        ),
    }
}
