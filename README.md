# Datalium Strata Demos - Rust

Rust demo suite for Datalium Strata — runnable agentic AI examples from minimal to real-world, built against Strata **0.9** (`0.9.0-preview.1`). It is the twin of [`strata-demos-dotnet`](https://github.com/datalium/strata-demos-dotnet): one demo each in both languages, same number, same story.

## Prerequisites

- Rust (edition 2024 — a recent stable toolchain via [rustup](https://rustup.rs))
- Nothing else from Strata to build: Cargo fetches the `datalium-strata*` crates (`0.9.0-preview.1`) from [crates.io](https://crates.io/crates/datalium-strata)
- The `strata` CLI (optional) — only for [demo 14](demos/14-graph-ci-gate) and for inspecting demo 09's checkpoint file. It is not on crates.io: download `strata-linux-x64`, `strata-macos-x64` or `strata-windows-x64.exe` from the [`v0.9.0-preview.1` release](https://github.com/datalium/strata/releases/tag/v0.9.0-preview.1) (check it against `SHA256SUMS.txt`), rename it to `strata` (`strata.exe` on Windows), and put it on your `PATH`, or set `STRATA_BIN` to its path
- [Docker](https://docs.docker.com/get-docker/) (optional) — only [demo 12](demos/12-pluggable-storage) needs it, for a throwaway Postgres and Redis (`docker compose up -d --wait`, see [`docker-compose.yml`](docker-compose.yml)); every other demo runs with no external services
- The .NET SDK (optional) — only to run the cross-runtime hand-offs of [demo 09](demos/09-cross-language-interop) and [demo 12](demos/12-pluggable-storage), and the export comparison of [demo 14](demos/14-graph-ci-gate), against the .NET twin

Clone this repo and build; there is no framework checkout to set up. Only the cross-runtime demos need the .NET twin, cloned next to this repo:

```
GitHub/Datalium/
├── strata-demos-rust/
└── strata-demos-dotnet/   (optional, for the cross-runtime demos)
```

```bash
git clone https://github.com/datalium/strata-demos-rust.git
```

## Structure

```
strata-demos-rust/
├── Cargo.toml       # workspace root
└── demos/
    ├── 01-hello-agent/
    ├── 02-tool-calling/
    └── ...
```

Demos are numbered in rough order of complexity (`01-` simplest, higher numbers more advanced / closer to real-world). See [demos/README.md](demos/README.md) for the current list. Every demo checks its own claims at runtime and exits non-zero when a check fails, and its README holds the output of a real run.

**Note on LLMs:** Strata is orchestration infrastructure — the graph engine, tool-calling dispatch, streaming, checkpoints, history. The "intelligence" is bring-your-own. Every demo's tools and node logic are deterministic (string matching, arithmetic, scripted stand-in models), so they run offline with no API keys, with two exceptions: [`03-llm-tool-calling`](demos/03-llm-tool-calling) and [`06-token-streaming`](demos/06-token-streaming) call OpenAI through `datalium-strata-ai`'s `OpenAiChatClient` and need `OPENAI_API_KEY` set, or `OPENAI_BASE_URL` pointing at a local OpenAI-compatible server such as Ollama. Demos 23 and 24 use the same `ChatClient` trait with scripted models written in the demo.

**Note on time:** no demo asserts on wall-clock time. Retries, timeouts, leases and hedges run on tokio's paused clock (which needs tokio's `test-util` feature, a feature flag, not an extra crate), and waits and recovery take an explicit `now`.

## Dependencies on Strata

The workspace centralizes references to the Strata crates in `[workspace.dependencies]` (`Cargo.toml`), pinned to the published crates.io release:

```toml
[workspace.dependencies]
datalium-strata = "=0.9.0-preview.1"
datalium-strata-macros = "=0.9.0-preview.1"
datalium-strata-memory = "=0.9.0-preview.1"
datalium-strata-ai = "=0.9.0-preview.1"
datalium-strata-host = "=0.9.0-preview.1"
```

| Crate | Source in [`datalium/strata`](https://github.com/datalium/strata) | What the demos use it for |
|---|---|---|
| `datalium-strata` | `rust/strata-core` | The engine: graphs, runs, interrupts, waits, parallel groups, maps, subgraphs, tools, observability, the scheduler and the in-memory memory store |
| `datalium-strata-macros` | `rust/strata-macros` | `#[strata_tool]` and `#[derive(StrataSchema)]` |
| `datalium-strata-memory` | `rust/strata-memory` | Checkpoint and history stores: in-memory, SQLite, Postgres, Redis |
| `datalium-strata-ai` | `rust/strata-ai` | Model calls, model routes, agents and embeddings (demos 03, 06, 23, 24, 25) |
| `datalium-strata-host` | `rust/strata-host` | Settings, drivers and a store health probe (demo 28) |

Individual demo crates reference these with `workspace = true` rather than repeating the relative path:

```toml
[dependencies]
datalium-strata = { workspace = true }
```

Note: the crate names are `datalium-strata-*`, not `strata-*`, despite the directory names in `datalium/strata`'s `rust/`. The versions are pinned exactly (`=`): a preview can change its API from one release to the next, so moving to a new one is a deliberate edit here, with every demo re-run. To try the demos against an unreleased change in a local clone of `datalium/strata`, add a `[patch.crates-io]` section to the root `Cargo.toml` and don't commit it.

## Building and running

```bash
cargo build --workspace
cargo run -p hello-agent
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Adding a new demo

1. `cargo new demos/NN-demo-name`, and set the package `name` without the `NN-` prefix (Cargo package names can't start with a digit)
2. Add the path to `members` in the root `Cargo.toml`
3. Reference the Strata crates with `{ workspace = true }`
4. Add an entry to [demos/README.md](demos/README.md)

## Related

- [datalium/strata](https://github.com/datalium/strata) — the framework (design docs, the Rust crates, the .NET packages, the `strata` CLI); documentation at [stratamesh.dev](https://stratamesh.dev)
- [strata-demos-dotnet](https://github.com/datalium/strata-demos-dotnet) — the .NET twin of this repo
