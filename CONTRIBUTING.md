# Contributing to the Rust demos

Thanks for helping. These are runnable examples for [Datalium Strata](https://github.com/datalium/strata), built from the published packages. Bugs and feature requests for Strata itself go to [datalium/strata](https://github.com/datalium/strata/issues). For a new demo, open an issue first so we can agree on what it shows.

## What you need

- **Rust** 1.85 or later, with `rustfmt` and `clippy` (`rustup component add rustfmt clippy`).
- **Docker**, only for demo 12 (`docker compose up -d --wait` starts Postgres and Redis).
- The **`strata` CLI**, only for demo 14 (see the [README](README.md#prerequisites)).
- An **`OPENAI_API_KEY`**, only for demos 03 and 06, which make real, billed calls. Or a free local model: Ollama with `OPENAI_BASE_URL` and `OPENAI_MODEL` (see their READMEs).

## Build and run

These are the commands CI runs:

```bash
cargo fmt --all                                         # format
cargo clippy --workspace --all-targets -- -D warnings   # lint
cargo build --workspace
cargo test --workspace
cargo run -p hello-agent
```

## Rules every demo follows

- **It shows one thing.** Each demo demonstrates something specific about Strata, not a rename of an earlier demo.
- **It checks its own claims.** A demo exits non-zero when a check fails, and its README shows the output of a real run.
- **It runs offline.** Only 03 and 06 call a live model. Everything else is deterministic, with no API keys and no network.
- **It has a twin.** Each demo has the same number and story in [`strata-demos-dotnet`](https://github.com/datalium/strata-demos-dotnet). A change to what a demo shows usually needs the same change there. Demos 09 and 12 hand threads between the two, so don't change what crosses the wire (thread ids, state field names, default paths, mode names) on one side only.
- **A new demo** needs its own README, a row in [demos/README.md](demos/README.md), and an entry in the root `Cargo.toml`'s `members`, and a package `name` without the `NN-` prefix.

## Pull requests

- One change per pull request, with a clear title. PRs are squash-merged, so the title becomes the commit message.
- The `Rust demos` check must pass.

### Sign your commits (DCO)

Every commit needs a `Signed-off-by` line. It certifies that you wrote the change, or otherwise have the right to submit it under the project's license, as the [Developer Certificate of Origin](https://developercertificate.org/) describes. Add it with `git commit -s`.

## License

By contributing, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE).
