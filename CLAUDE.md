# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this repo is

Rust demo suite for [Datalium Strata](https://github.com/datalium/strata) — runnable examples of
the framework, numbered in rough order of complexity (`demos/01-...` simplest,
higher numbers closer to real-world). See the root [README.md](README.md) for
prerequisites and wiring, and [demos/README.md](demos/README.md) for the
current list.

This repo builds against the published Strata crates on crates.io, pinned to
`=0.9.0-preview.1` in `[workspace.dependencies]` (root `Cargo.toml`). No
checkout of the framework is needed. The `strata` CLI (demo 14) is not on
crates.io: it is a binary on the `datalium/strata` GitHub Release.

## Conventions to follow

- **Verify the actual API before writing demo code.** The design docs in
  [`datalium/strata`](https://github.com/datalium/strata) (`design/`,
  `CLAUDE.md`) describe intent; the real method names, signatures, and
  error-handling shape can differ or have moved on. Read the source of the
  crate version this repo pins directly (Cargo's registry copy under
  `~/.cargo/registry/src/`, or `rust/strata-core/src`, `strata-memory`,
  `strata-macros` in `datalium/strata` at tag `v0.9.0-preview.1`), and prefer
  an existing working example (`examples/rust-agent` in `datalium/strata`)
  over inferring from prose.
- **Every demo must actually build and run before it's considered done.**
  `cargo run -p <package>` and confirm the real output, plus
  `cargo build --workspace` and `cargo clippy --workspace --all-targets -- -D
  warnings` for the whole workspace — not just the new demo in isolation.
- **A demo needs a purpose beyond "it compiles."** Each one should
  demonstrate something specific about the framework (a spec number, an API
  surface, a gotcha) — not just restate the previous demo with different
  names. If a demo would be trivial or redundant, say so rather than padding
  the list.
- **No live LLM calls, except by explicit request.** Strata's own reference
  example (`examples/rust-agent`) calls no real LLM provider — Strata is
  orchestration infrastructure (graph, tool dispatch, streaming,
  checkpoints, history), and the "intelligence" is bring-your-own. Keep demo
  tool/node logic deterministic (string matching, arithmetic, etc.) so demos
  run offline with no API keys, unless the user explicitly asks to wire in a
  real provider — see [`03-llm-tool-calling`](demos/03-llm-tool-calling) and
  [`06-token-streaming`](demos/06-token-streaming), the two demos that do
  (`datalium-strata-ai`'s `OpenAiChatClient`, gated on `OPENAI_API_KEY`). Keep that pattern contained to demos that exist
  specifically to show it off; don't let it creep into otherwise-offline
  demos.
- **Each demo gets its own `README.md`** (see `demos/01-hello-agent/README.md`
  for the shape): what it demonstrates, what it deliberately doesn't cover,
  how to run it, expected output. Also add a row to `demos/README.md`'s table
  and wire the new package into the root `Cargo.toml`'s `members`.
- **Cargo package names can't start with a digit.** Demo folders keep the
  `NN-` prefix; the `[package] name` in that demo's `Cargo.toml` drops it
  (e.g. folder `02-tool-calling`, package `tool-calling`).
- **Ask before scope decisions that aren't purely mechanical** — e.g. whether
  a new demo should introduce a new external dependency, call a live service,
  or otherwise expand beyond what a "runnable, offline, self-contained demo"
  implies. Don't assume.

## Commands

```bash
cargo build --workspace
cargo run -p <package-name>
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
