//! Token streaming (STRATA-SPEC-001) of a live model answer through
//! `datalium-strata-ai` (design/44-LLM-Integration.md). `AnswerQuestion` is a
//! model node (`add_model_node`): one call through `complete_with`, which
//! streams because the run streams (`run_streaming`) and sends each text
//! update to the run's stream as a `Token` chunk. The node still gets the
//! whole answer back as one `ChatResponse`. A plain node after it
//! (`SummarizeAnswer`) gets `NodeStart`/`NodeEnd` chunks like any node.
//!
//! A second run asks OpenAI for a model that doesn't exist. The provider's
//! 404 becomes a fatal node failure, and the node's `RecoveryPolicy`, which
//! would retry a transient one, doesn't retry it.
//!
//! Requires `OPENAI_API_KEY` in the environment. Costs a small amount of
//! real OpenAI usage each run (the second run is refused before any tokens
//! are generated).
//! Or run it free against a local model: set `OPENAI_BASE_URL` to any
//! OpenAI-compatible server (Ollama, vLLM, LM Studio) and `OPENAI_MODEL` to a
//! model it serves.

use std::env;
use std::io::Write;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::{CompiledGraph, Jitter, RecoveryPolicy, StateGraph};
use datalium_strata::store::CheckpointStore;
use datalium_strata::streaming::StreamChunkType;
use datalium_strata_ai::{
    ChatClient, Message, ModelNodeOptions, OpenAiChatClient, OpenAiOptions, StateGraphAiExt,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

const DEFAULT_MODEL: &str = "gpt-4o-mini";
const MISSING_MODEL: &str = "strata-demo-no-such-model";
const QUESTION: &str = "What makes Rust's borrow checker unusual compared to garbage collection?";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LlmStreamState {
    question: String,
    answer: String,
    answer_char_count: usize,
}

impl LlmStreamState {
    fn new() -> Self {
        Self {
            question: QUESTION.to_string(),
            answer: String::new(),
            answer_char_count: 0,
        }
    }
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

    let chat = openai_client(options.clone());
    let missing = openai_client(options.with_model(MISSING_MODEL));

    // --- 1. A live answer, streamed -----------------------------------------

    println!("=== Streaming a live answer, one token at a time ===\n");
    let (handle, mut chunks) =
        build_graph(chat).run_streaming("stream-thread", LlmStreamState::new(), store());

    let mut tokens: Vec<String> = Vec::new();
    let mut mid_line = false; // the tokens written so far don't end in a newline
    while let Some(chunk) = chunks.recv().await {
        if chunk.chunk_type != StreamChunkType::Token && mid_line {
            println!();
            mid_line = false;
        }
        match chunk.chunk_type {
            StreamChunkType::NodeStart => println!("[node start] {}", chunk.node_name),
            StreamChunkType::NodeEnd => println!("[node end]   {}", chunk.node_name),
            StreamChunkType::Token => {
                tokens.push(chunk.content.to_string());
                print!("{}", chunk.content);
                std::io::stdout().flush().expect("stdout flushes");
                mid_line = true;
            }
            // A transient fault (a 429, a dropped stream) retries the node,
            // which streams its answer again from the start. The consumer
            // drops what the failed attempt sent.
            StreamChunkType::Retry => {
                println!("[retry]      {}", chunk.node_name);
                tokens.clear();
            }
            StreamChunkType::Error => {
                println!("[error]      {}: {}", chunk.node_name, chunk.content);
            }
        }
    }

    // Draining the receiver shows chunks arrived during the run. Awaiting the
    // handle is what shows the run finished, and hands back the same final
    // state `run` would have returned.
    let answered = match handle.await.expect("the run's task doesn't panic") {
        GraphExecutionResult::Completed(state) => state,
        other => {
            eprintln!("expected Completed, got: {}", outcome(&other));
            return ExitCode::FAILURE;
        }
    };

    println!();
    println!("=== Done ===");
    println!("token_chunks: {}", tokens.len());
    println!("answer_char_count: {}", answered.answer_char_count);
    check(
        tokens.len() > 1,
        "expected the answer to arrive as more than one Token chunk",
    );
    check(
        tokens.concat() == answered.answer,
        "expected the node's whole answer to be exactly the tokens the consumer saw, in order",
    );
    check(
        answered.answer_char_count == answered.answer.chars().count(),
        "expected SummarizeAnswer to run after AnswerQuestion, on its answer",
    );

    // --- 2. A provider fault ------------------------------------------------

    println!("\n=== A provider fault: asking for model '{MISSING_MODEL}' ===\n");
    let (handle, mut chunks) =
        build_graph(missing).run_streaming("fault-thread", LlmStreamState::new(), store());

    let mut fault_chunks = Vec::new();
    while let Some(chunk) = chunks.recv().await {
        fault_chunks.push(chunk.chunk_type);
    }

    let (node, attempts, message) = match handle.await.expect("the run's task doesn't panic") {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => (node, attempts, message),
        other => {
            eprintln!(
                "expected the run to end Failed(NodeFailed), got: {}",
                outcome(&other)
            );
            return ExitCode::FAILURE;
        }
    };

    let seen: Vec<String> = fault_chunks.iter().map(|t| format!("{t:?}")).collect();
    println!("node failed: {node}, after {attempts} attempt(s)");
    println!(
        "message: {}",
        message.lines().next().unwrap_or_default().trim()
    );
    println!("chunks the consumer saw: {}", seen.join(", "));

    // The client reads the provider's status: a 4xx other than 408/409/429 is
    // the request's fault, so it fails the node fatally and the policy's two
    // retries are never used.
    check(
        node == "AnswerQuestion",
        "expected the model node to be the one that failed",
    );
    check(
        message.starts_with("model call failed (4"),
        "expected a 4xx from the provider, as the node's failure message",
    );
    check(attempts == 1, "expected a fatal fault not to be retried");
    check(
        !fault_chunks.contains(&StreamChunkType::Retry)
            && !fault_chunks.contains(&StreamChunkType::Token),
        "expected no retry and no tokens",
    );
    ExitCode::SUCCESS
}

/// The model node, a recovery policy for it, and a plain node after it. The
/// policy retries a transient fault (429, 5xx, a broken stream) twice, after
/// the provider's retry hint or else a jittered backoff from 1s; a fatal one
/// isn't retried at all.
fn build_graph(client: Arc<dyn ChatClient>) -> Arc<CompiledGraph<LlmStreamState>> {
    let graph = StateGraph::<LlmStreamState>::new()
        .add_model_node(
            "AnswerQuestion",
            client,
            |state: &LlmStreamState, _| {
                vec![
                    Message::system(
                        "You are a helpful assistant. Answer in two or three sentences.",
                    ),
                    Message::user(state.question.clone()),
                ]
            },
            |state: LlmStreamState, response| LlmStreamState {
                answer: response.text(),
                ..state
            },
            ModelNodeOptions::new().with_max_tokens(200),
        )
        .set_recovery_policy(
            "AnswerQuestion",
            RecoveryPolicy::retry(2).with_backoff(
                Duration::from_secs(1),
                Duration::from_secs(10),
                Jitter::Full,
            ),
        )
        .add_node("SummarizeAnswer", |state: LlmStreamState| LlmStreamState {
            answer_char_count: state.answer.chars().count(),
            ..state
        })
        .add_edge("AnswerQuestion", "SummarizeAnswer")
        .compile()
        .expect("graph is structurally valid");
    Arc::new(graph)
}

/// The client does no retrying of its own (design/44): the node's
/// RecoveryPolicy does, and it sees the provider's retry hint.
fn openai_client(options: OpenAiOptions) -> Arc<dyn ChatClient> {
    Arc::new(OpenAiChatClient::new(options))
}

fn store() -> Arc<dyn CheckpointStore> {
    Arc::new(InMemoryCheckpointStore::new())
}

/// How a run ended, for a message (`GraphExecutionResult` isn't `Debug`).
fn outcome<S>(result: &GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Failed(err) => format!("Failed({err:?})"),
        GraphExecutionResult::Interrupted { next_node, .. } => {
            format!("Interrupted before {next_node}")
        }
        GraphExecutionResult::Cancelled { node, reason, .. } => {
            format!("Cancelled at {node} ({reason:?})")
        }
    }
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
