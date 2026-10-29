# 03 — llm-tool-calling

A live LLM (OpenAI's Chat Completions API, through `datalium-strata-ai`'s
`OpenAiChatClient`) decides to call the `add` tool from
[`02-tool-calling`](../02-tool-calling), instead of the program handing the
tool its arguments. Strata's own agent loop, `Agent<O>` from
`datalium-strata-ai` (`design/44-LLM-Integration.md`), does the work in
between: it declares the `ToolRegistry`'s tools to the model, dispatches the
call the model asks for, sends the result back, and returns the model's
answer. The demo has no OpenAI request or response code of its own, and no
HTTP client of its own either: `reqwest` reaches it only through
`datalium-strata-ai`.

By default this demo makes real, billed calls to the OpenAI API. [With a local model](#with-a-local-model) it costs nothing. It and
[`06-token-streaming`](../06-token-streaming) are the only demos that do (see
the "Note on LLMs" in the root [README](../../README.md)). Everything else
stays offline. Its .NET twin is
[`strata-demos-dotnet/demos/03-llm-tool-calling`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/03-llm-tool-calling).

## What it demonstrates

- `OpenAiChatClient::new(OpenAiOptions::new(key).with_model(model))`:
  Strata's own `ChatClient` trait (the Rust counterpart of .NET's
  `IChatClient`) and its built-in Chat Completions client, held as an
  `Arc<dyn ChatClient>`. The client doesn't retry: a transient fault carries
  the provider's retry hint, and a `RecoveryPolicy` on the node would do the
  retrying (design/44). So there is no SDK retry policy to switch off, as
  the .NET twin has to
- A `ToolRegistry` with the `#[strata_tool]` `add` tool registered: the
  registry is the whole set of tools the agent offers the model. The
  macro-generated manifest from demo 02 becomes the model's tool
  declarations (`ToolDeclarations`), with no hand-written JSON
- `Agent::<String>::new(client, registry, options)` (a text agent) run as a
  graph node with `.add_agent_node(name, agent, prompt, apply)` from
  `StateGraphAiExt`. The model asks for `add {"a":47,"b":89}`, the agent
  dispatches it through the registry, sends `{"result":136}` back, and the
  model's next turn is the answer
- The context the agent dispatches with: the tool function takes a
  `ctx: ToolCallContext` after its argument struct (kept out of the
  manifest), and its `caller_id()` is the agent's id (`calculator`), its
  `call_id()` the model's id for the call, its `node_path()` the node
- `AgentResult<String>`: the answer (`output`), the number of model calls
  (`turns`), every tool call with how it ended (`tool_calls`, each an
  `AgentToolCall` with a `ToolCallResult`), and the whole conversation
  (`messages`), which the demo reads (`Content::ToolCall` /
  `Content::ToolResult`, matched by the model's call id) to print the
  arguments the model chose and the result Strata gave back
- `AgentOptions::new("calculator").with_max_turns(2)` as a cost guard: a
  model that keeps asking for tools fails the node instead of looping
- The demo checks what doesn't vary (the tool was called, it returned 136,
  there were at least two turns) and exits non-zero if not. Only the
  model's wording varies

The first turn requires a tool call: `AgentOptions::with_configure_turn`
sets `request.tool_choice = Some(ToolChoice::Required)` on turn 1 only,
which OpenAI receives as `tool_choice: "required"`; later turns send no
`tool_choice`, so the model may answer. Setting `tool_choice` on the
agent's request template (`AgentOptions::with_request`) instead would apply
to every turn, and the model could never answer. The demo still fails with
`the model answered without calling the add tool` if no call was made.

Where it differs from the .NET twin: a Rust tool returns its own text, so
`add` writes `{"result":136}` itself (the .NET source generator serializes
the returned `double` into that shape); and the client is Strata's own
`OpenAiChatClient`, not the OpenAI SDK behind `Microsoft.Extensions.AI`.

## What it does *not* cover

- Streaming the answer token by token:
  [`06-token-streaming`](../06-token-streaming)
- Typed (`#[derive(Deserialize, StrataSchema)]`) answers, repairs, refused
  tool calls and the `max_turns` limit firing, all with a scripted model and
  no key: [`23-agent-tool-loop`](../23-agent-tool-loop)
- Failing over between models: [`24-model-routes`](../24-model-routes)
- Retries of a provider fault: there's no `RecoveryPolicy` here. Demo 06
  shows a provider fault failing the node, and
  [`08-error-recovery`](../08-error-recovery) covers recovery policies
- Middleware, subsets and permissions on the registry:
  [`26-tool-registry-and-middleware`](../26-tool-registry-and-middleware)
- A conversation that lasts longer than one run of the node: the agent's
  conversation lives for one attempt

## Setup

Requires an OpenAI API key with access to the Chat Completions API.

macOS/Linux (bash/zsh):

```bash
export OPENAI_API_KEY=sk-...
# optional, defaults to gpt-4o-mini
export OPENAI_MODEL=gpt-4o-mini
```

Windows (PowerShell):

```powershell
$env:OPENAI_API_KEY = "sk-..."
# optional, defaults to gpt-4o-mini
$env:OPENAI_MODEL = "gpt-4o-mini"
```

Either way, this only sets the variable for the current shell session. For
it to persist across sessions, add the `export`/`$env:` line to your shell
profile, or set it as a permanent user/system environment variable
(`setx OPENAI_API_KEY "sk-..."` on Windows, which takes effect in new
terminals only).

### With a local model

Any OpenAI-compatible server works instead of OpenAI: Ollama, vLLM, LM Studio and others. A run then costs nothing. Set `OPENAI_BASE_URL` to the server's address, up to `/v1`, and `OPENAI_MODEL` to a model it serves. A key is optional: `OPENAI_API_KEY` is sent if it's set. With [Ollama](https://ollama.com):

```bash
ollama pull qwen2.5:3b
export OPENAI_BASE_URL=http://localhost:11434/v1
export OPENAI_MODEL=qwen2.5:3b
```

The model must support tool calls. A model that answers without calling `add` fails the demo's check, which says so. `qwen2.5:3b` does, as the run below shows.

The repo's **Live-model demos** workflow runs this demo the same way, against Ollama on the GitHub runner. This is its run on 2026-10-29 (`qwen2.5:3b`, Ollama 0.40.4). The wording varies from run to run:

```
[add] 47 + 89, called by agent 'calculator' (model call id call_93r44eic) in node 'AskLlm'
tool call: add {"a":47,"b":89} -> {"result":136} (Succeeded)
tool result: 136
model turns: 2
model: The sum of 47 and 89 is 136.
```

## Run it

```bash
cargo run -p llm-tool-calling
```

Without `OPENAI_API_KEY` or `OPENAI_BASE_URL` set, the demo prints a short setup message to
stderr and exits with code 1, before making any network call. This is a
real run, without a key:

```
OPENAI_API_KEY is not set. Get a key at https://platform.openai.com/api-keys and export it, e.g.:

    export OPENAI_API_KEY=sk-...

Or use a local model, e.g. with Ollama:

    export OPENAI_BASE_URL=http://localhost:11434/v1 OPENAI_MODEL=qwen2.5:3b

```

With a key, a live run against OpenAI (`gpt-4o-mini`, 2026-10-24). The
model's call id and its wording vary from run to run; the arguments, the
tool result and the turn count shouldn't:

```
[add] 47 + 89, called by agent 'calculator' (model call id call_IKMiHnxk4LAp4KZCaKDgWyrU) in node 'AskLlm'
tool call: add {"a":47,"b":89} -> {"result":136} (Succeeded)
tool result: 136
model turns: 2
model: 47 plus 89 equals 136.
```

The agent requires a tool call on the first turn only (`tool_choice:
"required"`), so the model called `add` there and answered on the second.
The output has the same lines as the .NET twin's.

## Files

- `src/main.rs` — the `#[strata_tool]` `add` tool (logging the agent and
  the model's call id from its `ToolCallContext`), the OpenAI client, the
  registry, the agent, a one-node graph and the checks
- `Cargo.toml` — package name is `llm-tool-calling` (see
  [`01-hello-agent`](../01-hello-agent)'s README for why the digit prefix is
  dropped from the package name); depends on `datalium-strata-ai` for the
  client and the agent
