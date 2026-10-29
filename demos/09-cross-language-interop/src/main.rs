//! Cross-language interop: freeze a graph in one runtime, resume it in the other.
//!
//! Strata's wire-format constraint (`schema/README.md` in `datalium/strata`)
//! is that the Rust `strata` CLI and
//! runtime can read checkpoints the C# runtime produced, and vice versa.
//! Until this demo that was only proven at fixture level (both runtimes'
//! tests agree on a JSON shape); no checkpoint had ever crossed a real file
//! between the two. This demo is the same wire-transfer graph as
//! `05-hitl-interruption`, split across two *processes* and two *runtimes*
//! through one SQLite file:
//!
//! ```text
//! cargo run -p cross-language-interop -- freeze   # Rust freezes ...
//! # ... .NET resumes (in ../strata-demos-dotnet):
//! dotnet run --project demos/09-cross-language-interop/09-cross-language-interop.csproj -- resume
//! ```
//!
//! and the other way around (`.NET freeze`, then `cargo run ... -- resume`).
//! The .NET twin of this file is `Program.cs` in the sibling repo; the two
//! define the same graph, thread id, and state shape independently, and the
//! only thing they share is the database file.
//!
//! Each node stamps the state with the runtime that ran it (`frozen_by`,
//! `executed_by`), so the completed state shows the hand-off: `frozen_by:
//! dotnet, executed_by: rust`, or the reverse.
//!
//! Modes: `freeze [path]` and `resume [path]`. `path` defaults to
//! `strata-demo-09-interop/checkpoints.sqlite` under the system temp dir --
//! the .NET twin computes the same default, so no argument is needed for the
//! two to find each other.
//!
//! Everything that crosses the wire -- thread id, state field names, default
//! path, mode names -- must stay in step with the twin.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::store::CheckpointStore;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use serde::{Deserialize, Serialize};

const RUNTIME: &str = "rust";
const THREAD_ID: &str = "wire-transfer-xlang";
const APPROVER: &str = "compliance_officer_07";

/// Field names are snake_case on the wire, and the .NET twin pins the same
/// names with `[JsonPropertyName]`. That matters beyond style: the state is
/// user-defined JSON inside the checkpoint, so nothing but this agreement
/// keeps the two runtimes' `WireTransferState`s compatible -- and a
/// `HumanSignal` field override is keyed by the *JSON* name (`approved_by`),
/// not by either language's property name.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireTransferState {
    invoice_generated: bool,
    amount: f64,
    currency: String,
    approved_by: Option<String>,
    is_completed: bool,
    frozen_by: Option<String>,
    executed_by: Option<String>,
}

fn wire_transfer_graph() -> CompiledGraph<WireTransferState> {
    StateGraph::<WireTransferState>::new()
        .add_node("PrepareWireTransfer", |s: WireTransferState| {
            WireTransferState {
                invoice_generated: true,
                frozen_by: Some(RUNTIME.to_string()),
                ..s
            }
        })
        .add_interrupt_boundary("PrepareWireTransfer")
        .add_node("ExecuteWireTransfer", |s: WireTransferState| {
            WireTransferState {
                is_completed: true,
                executed_by: Some(RUNTIME.to_string()),
                ..s
            }
        })
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .expect("graph is structurally valid")
}

fn default_path() -> PathBuf {
    std::env::temp_dir()
        .join("strata-demo-09-interop")
        .join("checkpoints.sqlite")
}

fn open_store(path: &Path) -> Result<Arc<dyn CheckpointStore>, String> {
    let store = SqliteCheckpointStore::open(path)
        .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    Ok(Arc::new(store))
}

/// A failed check ends the run with exit code 1, like the .NET twin's errors.
fn check(ok: bool, what: &str) -> Result<(), String> {
    if ok {
        Ok(())
    } else {
        Err(format!("check failed: {what}"))
    }
}

/// `StrataError` has no `Display`; the engine's own message is what both
/// runtimes print for a refused resume.
fn describe(err: &StrataError) -> String {
    match err {
        StrataError::ExecutionAborted(message) => message.clone(),
        other => format!("{other:?}"),
    }
}

async fn freeze(path: &Path) -> Result<(), String> {
    // A fresh log each time: `freeze` always starts a new thread history.
    let _ = std::fs::remove_file(path);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
    }

    println!("=== [{RUNTIME}] freeze: run to the interrupt boundary ===");
    println!("  Checkpoint log: {}", path.display());
    let store = open_store(path)?;

    let initial = WireTransferState {
        invoice_generated: false,
        amount: 4_200.50,
        currency: "USD".to_string(),
        approved_by: None,
        is_completed: false,
        frozen_by: None,
        executed_by: None,
    };

    match wire_transfer_graph().run(THREAD_ID, initial, store).await {
        GraphExecutionResult::Interrupted {
            thread_id,
            suspended_node,
            next_node,
            frozen_state,
            waiting_at,
            wait,
        } => {
            println!(
                "  Interrupted at '{suspended_node}' (thread '{thread_id}'); next node on resume: '{next_node}'"
            );
            println!("  Frozen state: {frozen_state:?}");
            check(thread_id == THREAD_ID, "the thread id is the shared one")?;
            check(
                suspended_node == "PrepareWireTransfer" && next_node == "ExecuteWireTransfer",
                "frozen after PrepareWireTransfer, ExecuteWireTransfer next",
            )?;
            check(
                waiting_at == ["PrepareWireTransfer"] && wait.is_none(),
                "a plain human boundary in the outermost graph (no event, timer or nesting)",
            )?;
            check(
                frozen_state.invoice_generated
                    && !frozen_state.is_completed
                    && frozen_state.frozen_by.as_deref() == Some(RUNTIME)
                    && frozen_state.executed_by.is_none(),
                "the frozen state is stamped by this runtime and not yet executed",
            )?;
        }
        GraphExecutionResult::Completed(_) => {
            return Err("expected Interrupted, got Completed".into());
        }
        GraphExecutionResult::Failed(err) => {
            return Err(format!("graph execution failed: {}", describe(&err)));
        }
        GraphExecutionResult::Cancelled { .. } => {
            return Err("expected Interrupted, got Cancelled".into());
        }
    }

    println!();
    println!("  This process is about to exit; the checkpoint now exists only in the file.");
    println!("  Next: run the `resume` step -- from the other runtime to see the hand-off.");
    Ok(())
}

async fn resume(path: &Path) -> Result<(), String> {
    println!("=== [{RUNTIME}] resume: pick up a checkpoint another process froze ===");
    println!("  Checkpoint log: {}", path.display());
    if !path.exists() {
        return Err("no checkpoint log there -- run the `freeze` step first".into());
    }
    let store = open_store(path)?;

    // The override is keyed by the *JSON* name (`approved_by`): it is merged
    // into the frozen state's JSON before that is deserialized, whichever
    // runtime wrote it.
    let signal = HumanSignal {
        is_approved: true,
        reviewer_token: "token-xyz".to_string(),
        field_overrides: HashMap::from([("approved_by".to_string(), APPROVER.to_string())]),
        ..HumanSignal::default()
    };

    match wire_transfer_graph().resume(THREAD_ID, signal, store).await {
        GraphExecutionResult::Completed(state) => {
            println!("  Resumed and ran to completion.");
            println!("  Completed state: {state:?}");
            println!();
            println!(
                "  frozen_by = {:?}, executed_by = {:?}",
                state.frozen_by.as_deref().unwrap_or("?"),
                state.executed_by.as_deref().unwrap_or("?")
            );
            check(
                matches!(state.frozen_by.as_deref(), Some("rust" | "dotnet")),
                "the frozen state carries the freezing runtime's stamp",
            )?;
            check(
                state.executed_by.as_deref() == Some(RUNTIME) && state.is_completed,
                "this runtime ran ExecuteWireTransfer",
            )?;
            check(
                state.approved_by.as_deref() == Some(APPROVER),
                "the `approved_by` override reached the state",
            )?;
            check(
                state.invoice_generated && state.amount == 4_200.50 && state.currency == "USD",
                "the rest of the frozen state survived the hand-off",
            )?;
            Ok(())
        }
        GraphExecutionResult::Failed(err) => Err(format!(
            "resume failed: {} (was this thread already resumed? run `freeze` again)",
            describe(&err)
        )),
        GraphExecutionResult::Interrupted { .. } => {
            Err("expected Completed, got Interrupted".into())
        }
        GraphExecutionResult::Cancelled { .. } => Err("expected Completed, got Cancelled".into()),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mode = args.next();
    let path = args.next().map(PathBuf::from).unwrap_or_else(default_path);

    let result = match mode.as_deref() {
        Some("freeze") => freeze(&path).await,
        Some("resume") => resume(&path).await,
        _ => {
            eprintln!("usage: cross-language-interop <freeze|resume> [checkpoint-log-path]");
            return ExitCode::from(2);
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}
