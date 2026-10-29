# 06 — token-streaming

Token streaming (STRATA-SPEC-001) of a live model answer through
`datalium-strata-ai` (`design/44-LLM-Integration.md`). The `AnswerQuestion`
node is a model node (`.add_model_node(...)`): it makes one call to OpenAI
through `complete_with`, which streams because the run streams
(`run_streaming`) and sends each piece of text to the run's stream as a
`Token` chunk. The consumer prints the answer as it arrives, and the node
still gets the whole answer back as one `ChatResponse`. A second run asks
for a model that doesn't exist, to show a provider fault failing the node.
Pairs with [`03-llm-tool-calling`](../03-llm-tool-calling): the same OpenAI
client, a model node instead of an agent.

This demo makes real, billed calls to the OpenAI API. It and demo 03 are
the only demos that do. Its .NET twin is
[`strata-demos-dotnet/demos/06-token-streaming`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/06-token-streaming).

## What it demonstrates

- `.add_model_node(name, client, prompt, apply, options)` from
  `StateGraphAiExt`: a model call as a node, with no streaming or HTTP code
  in it. The node's `prompt` builds the messages from the state; its `apply`
  folds the `ChatResponse` into the next state (`response.text()`).
  `ModelNodeOptions::new().with_max_tokens(200)` limits the answer (sent as
  OpenAI's `max_completion_tokens`)
- `Arc<CompiledGraph>::run_streaming(...)` returning
  `(JoinHandle<GraphExecutionResult<S>>, mpsc::Receiver<StreamChunk>)`.
  Because the run streams, the model node's `NodeContext` has a stream, and
  `complete_with` calls the client's `stream` instead of `complete`. Under
  `run` the same node makes one plain request
- The two views of one answer: the consumer sees many `Token` chunks while
  the node runs; the node sees the whole text once the stream ends. The demo
  checks that the tokens, joined in order, are exactly the node's answer
- `NodeStart`/`NodeEnd` chunks for every node, streaming or not:
  `SummarizeAnswer` is a plain `add_node` after the model node, and it reads
  the whole answer from the state
- A `RecoveryPolicy` on the model node
  (`RecoveryPolicy::retry(2).with_backoff(1s, 10s, Jitter::Full)`). A
  transient fault (429, 5xx, a stream that breaks part-way) would retry the
  node after the provider's retry hint, and the stream would carry a `Retry`
  chunk before the new attempt's tokens. The consumer clears what it
  collected when it sees one
- A provider fault as a node failure: asking OpenAI for
  `strata-demo-no-such-model` gets a 4xx back. `OpenAiChatClient` reads the
  status as a **fatal** `ChatError` (any 4xx except 408, 409 and 429 is the
  request's fault, `is_transient_status`), which becomes a
  `NodeError::Fatal`, so the run ends
  `Failed(StrataError::NodeFailed { attempts: 1, .. })`, the policy's two
  retries unused, and the stream has no `Retry` and no `Token` chunks. The
  fault is real, from OpenAI, not a fake
- The client does no retrying of its own, so the node's policy is the only
  one retrying, as design/44 advises

Where it differs from the .NET twin: the run ends
`GraphExecutionResult::Failed(StrataError::NodeFailed { .. })` rather than a
`NodeFailed` result of its own; the failure message is the Rust client's
(`model call failed (404): 404 Not Found: <OpenAI's message>`), and the demo
checks only its `model call failed (4` prefix, as the .NET twin does;
`answer_char_count` counts Unicode scalar values (`chars()`), where .NET
counts UTF-16 code units. `with_backoff` takes its `Jitter` explicitly
(`Full`, the .NET default).

## What it does *not* cover

- A transient fault actually being retried: OpenAI can't be made to return
  a 429 or break a stream on demand. Demos
  [`08-error-recovery`](../08-error-recovery) (retries and backoff) and
  [`24-model-routes`](../24-model-routes) (failing over between models, with
  a scripted model) cover retrying
- Backpressure under a full channel: [`11-backpressure`](../11-backpressure)
- Tool calls: [`03-llm-tool-calling`](../03-llm-tool-calling) (live) and
  [`23-agent-tool-loop`](../23-agent-tool-loop) (scripted). An agent's turns
  stream the same way when its run streams
- The run events and telemetry a model call records (`ModelCallStarted`,
  `ModelCallCompleted` with token usage, a `chat <model>` span):
  [`24-model-routes`](../24-model-routes) reads the model-call events, and
  [`27-run-observability`](../27-run-observability) covers run events in
  general
- Resuming a streaming run after an interrupt: `resume_streaming(...)`
  exists, but this graph has no interrupt boundary

## Setup

Same as demo 03. Requires an OpenAI API key with access to the Chat
Completions API:

```bash
export OPENAI_API_KEY=sk-...
# optional, defaults to gpt-4o-mini
export OPENAI_MODEL=gpt-4o-mini
```

On Windows PowerShell: `$env:OPENAI_API_KEY = "sk-..."`. See
[demo 03's README](../03-llm-tool-calling/README.md#setup) for making it
persist.

## Run it

```bash
cargo run -p token-streaming
```

Without `OPENAI_API_KEY` set, the demo prints a short setup message to
stderr and exits with code 1, before making any network call. This is a
real run, without a key:

```
OPENAI_API_KEY is not set. Get a key at https://platform.openai.com/api-keys and export it, e.g.:

    export OPENAI_API_KEY=sk-...

```

With a key, a live run against OpenAI (`gpt-4o-mini`, 2026-10-24). The
answer's text, its length and the number of token chunks vary from run to
run, since it's a live model:

```
=== Streaming a live answer, one token at a time ===

[node start] AnswerQuestion
Rust's borrow checker enforces memory safety at compile time through ownership rules, ensuring that references do not outlive their data and preventing data races. Unlike garbage collection, which automatically reclaims memory at runtime, the borrow checker requires developers to explicitly manage lifetimes, making errors more detectable during compilation rather than at runtime. This approach can lead to more predictable performance, as there is no overhead of garbage collection during execution.
[node end]   AnswerQuestion
[node start] SummarizeAnswer
[node end]   SummarizeAnswer

=== Done ===
token_chunks: 83
answer_char_count: 502

=== A provider fault: asking for model 'strata-demo-no-such-model' ===

node failed: AnswerQuestion, after 1 attempt(s)
message: model call failed (404): 404 Not Found: The model `strata-demo-no-such-model` does not exist or you do not have access to it.
chunks the consumer saw: NodeStart, Error
```

OpenAI answers the missing model with a 404, which
fails the node fatally after one attempt; the demo only checks for a 4xx
and a single attempt. The retry path wasn't seen live: against a local
stand-in for the Chat Completions API, a 429 with `retry-after-ms` on the
first request, and a stream cut off half-way, each printed
`[retry]      AnswerQuestion` and then the whole answer, and the checks
passed.

## Files

- `src/main.rs` — the whole demo: the OpenAI client, the graph (model node,
  recovery policy, plain node), the two runs and their checks
- `Cargo.toml` — package name is `token-streaming` (folder keeps the `06-`
  prefix; Cargo package names can't start with a digit); depends on
  `datalium-strata-ai` for the client and the model node
