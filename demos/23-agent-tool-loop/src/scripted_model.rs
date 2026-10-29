//! A stand-in for a real model, so the demo runs offline: a [`ChatClient`] that
//! plays one scripted turn per request, in order, and records every request. A
//! turn is a function of the conversation so far, so it can read the tool
//! results the agent sent back (an order's total, a refund id) the way a model
//! would.
//!
//! Nothing here is intelligent: the script is the model. The agent can't tell,
//! which is the point: it only sees `dyn ChatClient`. Demo 03 runs the same kind
//! of loop against a real OpenAI model.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;
use datalium_strata_ai::{
    ChatClient, ChatError, ChatRequest, ChatResponse, ChatStream, ChatUpdate, ChatUpdates, Content,
    Message, ModelUsage, Role,
};

pub const MODEL: &str = "support-model-stand-in";

/// What the stand-in says in one turn: text, tool calls, or both. A call's
/// arguments are JSON text, `(id, name, arguments)`, as a provider sends them.
pub struct Reply {
    pub text: String,
    pub calls: Vec<(String, String, String)>,
}

impl Reply {
    pub fn say(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            calls: Vec::new(),
        }
    }

    pub fn call(calls: &[(&str, &str, &str)]) -> Self {
        Self {
            text: String::new(),
            calls: calls
                .iter()
                .map(|(id, name, arguments)| {
                    (id.to_string(), name.to_string(), arguments.to_string())
                })
                .collect(),
        }
    }
}

/// One scripted turn: the conversation so far in, the reply out.
pub type Turn = Box<dyn Fn(&[Message]) -> Reply + Send + Sync>;

/// One request the agent sent, and the usage the stand-in reported for it.
#[derive(Clone)]
pub struct Request {
    pub request: ChatRequest,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

pub struct ScriptedModel {
    turns: Mutex<VecDeque<Turn>>,
    requests: Mutex<Vec<Request>>,
}

impl ScriptedModel {
    pub fn new(turns: Vec<Turn>) -> Arc<Self> {
        Arc::new(Self {
            turns: Mutex::new(turns.into()),
            requests: Mutex::new(Vec::new()),
        })
    }

    /// Every request so far, in order.
    pub fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }

    /// Plays the next turn, records the request, and returns the answer as the
    /// updates a provider would stream.
    fn answer(&self, request: ChatRequest) -> Result<Vec<ChatUpdate>, ChatError> {
        let turn = self
            .turns
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| ChatError::fatal("script", "the script has no more turns"))?;
        let reply = turn(&request.messages);

        // Made-up but deterministic token counts: 40 per message read, 10 per
        // answer plus 15 per tool call and one per four characters of text.
        let input = 40 * request.messages.len() as u64;
        let output = 10 + 15 * reply.calls.len() as u64 + reply.text.len() as u64 / 4;
        let model = request.model.clone().unwrap_or_else(|| MODEL.to_string());
        self.requests.lock().unwrap().push(Request {
            request,
            input_tokens: input,
            output_tokens: output,
        });

        let mut updates = Vec::new();
        if !reply.text.is_empty() {
            updates.push(ChatUpdate::Text(reply.text));
        }
        let finish = if reply.calls.is_empty() {
            "stop"
        } else {
            "tool_calls"
        };
        for (index, (id, name, arguments)) in reply.calls.into_iter().enumerate() {
            updates.push(ChatUpdate::ToolCall {
                index,
                id: Some(id),
                name: Some(name),
                arguments,
            });
        }
        updates.push(ChatUpdate::Usage(ModelUsage::new(
            Some(input),
            Some(output),
        )));
        updates.push(ChatUpdate::Finish(finish.to_string()));
        updates.push(ChatUpdate::Meta {
            model: Some(model),
            id: None,
        });
        Ok(updates)
    }
}

impl ChatClient for ScriptedModel {
    fn provider(&self) -> &str {
        "scripted"
    }

    fn default_model(&self) -> Option<&str> {
        Some(MODEL)
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        let answer = self.answer(request);
        Box::pin(async move {
            // `ChatUpdates` assembles the answer as a real client's does: it
            // parses each call's arguments, and keeps arguments that aren't a
            // JSON object as their text, for the agent to refuse.
            let mut assembled = ChatUpdates::new();
            for update in answer? {
                assembled.push(update);
            }
            assembled.finish()
        })
    }

    // `complete_with` streams only when the run does (`run_streaming`); this
    // demo never does, but a client should answer both.
    fn stream(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        let answer = self.answer(request);
        Box::pin(async move {
            let stream: ChatStream =
                Box::pin(futures_util::stream::iter(answer?.into_iter().map(Ok)));
            Ok(stream)
        })
    }
}

/// The text the agent sent back for the tool call `call_id`.
pub fn tool_result<'a>(conversation: &'a [Message], call_id: &str) -> &'a str {
    conversation
        .iter()
        .filter(|m| m.role == Role::Tool)
        .flat_map(|m| &m.content)
        .find_map(|content| match content {
            Content::ToolResult {
                call_id: id,
                content,
            } if id == call_id => Some(content.as_str()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no result for {call_id}"))
}
