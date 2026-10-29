//! Compile-time graph validation (STRATA-SPEC-004): every earlier demo's
//! `.compile()` call ran the validator and passed. This one shows it
//! rejecting a graph, and what it takes to pass.
//!
//! A document review pipeline is built twice from the same four nodes. Only
//! the edges differ.
//!
//! - **Broken**: `Review` routes back to `Draft` on a plain `.add_edge(...)`,
//!   and the exit edge to `Archive` has a typo (`"Archve"`). `.compile()`
//!   returns `Err(StrataError::GraphInvalid(..))` before any node runs, with
//!   three errors: `STRATA_ERR_DANGLING_EDGE` (the edge names a node that
//!   doesn't exist), `STRATA_ERR_ORPHAN` (so `Archive` is unreachable) and
//!   `STRATA_ERR_UNGUARDED_CYCLE` (nothing bounds the `Draft`/`Review` loop).
//! - **Fixed**: the retry edge becomes an `.add_conditional_edge(...)` guarded
//!   by a revision count, and the typo is fixed. Inside the loop, `Draft`
//!   reaches `Review` two ways (through `LegalReview` on the first pass,
//!   directly after that), both on plain edges. That branch-and-rejoin is
//!   fine: the validator rejects a cycle only if it is made of plain edges,
//!   and every cycle here goes through the guarded `Review -> Draft` edge. The
//!   graph compiles and runs both paths.

use std::sync::Arc;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::StateGraph;
use datalium_strata::store::CheckpointStore;
use datalium_strata::validate::ValidationErrorKind;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DocumentState {
    title: String,
    revision: u32,
    legal_cleared: bool,
    approved: bool,
    archived: bool,
    /// Each node appends its name, so the run shows its path.
    trail: Vec<String>,
}

/// The four nodes both versions share.
fn nodes() -> StateGraph<DocumentState> {
    StateGraph::<DocumentState>::new()
        .add_node("Draft", |mut s: DocumentState| {
            s.revision += 1;
            s.trail.push("Draft".to_string());
            s
        })
        .add_node("LegalReview", |mut s: DocumentState| {
            s.legal_cleared = true;
            s.trail.push("LegalReview".to_string());
            s
        })
        .add_node("Review", |mut s: DocumentState| {
            // Approved once it's survived three drafts.
            s.approved = s.revision >= 3;
            s.trail.push("Review".to_string());
            s
        })
        .add_node("Archive", |mut s: DocumentState| {
            s.archived = true;
            s.trail.push("Archive".to_string());
            s
        })
        .set_entry_point("Draft")
}

#[tokio::main]
async fn main() {
    // --- Broken: an unguarded retry loop, and a typo in the exit edge.
    let broken = nodes()
        .add_conditional_edge("Draft", "LegalReview", |s: &DocumentState| !s.legal_cleared)
        .add_edge("Draft", "Review")
        .add_edge("LegalReview", "Review")
        // Every review sends the document straight back to Draft. Nothing
        // bounds how many times this can fire, so the Draft/Review loop is an
        // unguarded cycle.
        .add_edge("Review", "Draft")
        .add_edge("Review", "Archve")
        .compile();

    let validation = match broken {
        Ok(_) => fail("expected the broken graph to fail compilation"),
        Err(StrataError::GraphInvalid(validation)) => validation,
        Err(other) => fail(&format!("expected GraphInvalid, got {other:?}")),
    };

    println!("Compilation rejected, as STRATA-SPEC-004 requires:");
    for error in &validation.errors {
        println!("  [{}] {}", error.code, error.message);
    }

    let errors = &validation.errors;
    check(errors.len() == 3, "exactly three errors are reported");
    check(
        errors.iter().any(|e| {
            e.kind == ValidationErrorKind::DanglingEdge
                && e.node == "Review"
                && e.related_node.as_deref() == Some("Archve")
        }),
        "the Review -> Archve edge is flagged as dangling",
    );
    check(
        errors
            .iter()
            .any(|e| e.kind == ValidationErrorKind::OrphanNode && e.node == "Archive"),
        "Archive is flagged as an orphan node",
    );
    check(
        errors.iter().any(|e| {
            e.kind == ValidationErrorKind::UnguardedCycle
                && e.node == "Review"
                && e.related_node.as_deref() == Some("Draft")
        }),
        "the Draft/Review loop is flagged as an unguarded cycle",
    );

    // --- Fixed: same nodes, the retry edge guarded, the typo fixed.
    let fixed = nodes()
        // Legal signs off once: the first draft goes through LegalReview,
        // later ones skip it. Both paths rejoin at Review on plain edges.
        .add_conditional_edge("Draft", "LegalReview", |s: &DocumentState| !s.legal_cleared)
        .add_edge("Draft", "Review")
        .add_edge("LegalReview", "Review")
        // The conditional edge is the "if" (still under three revisions, send
        // it back); the plain edge added right after it is the "else"
        // (approved, file it away). Same declaration-order-wins rule as
        // `04-multi-node-graph`'s shipping branch.
        .add_conditional_edge("Review", "Draft", |s: &DocumentState| s.revision < 3)
        .add_edge("Review", "Archive")
        .compile()
        .unwrap_or_else(|err| fail(&format!("the fixed graph should compile: {err:?}")));

    println!();
    println!("Same nodes, fixed: guarded the retry edge and fixed the typo -- compiles clean.");

    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let initial = DocumentState {
        title: "Q3 roadmap".to_string(),
        ..DocumentState::default()
    };

    match fixed.run("roadmap-review-thread", initial, store).await {
        GraphExecutionResult::Completed(state) => {
            println!("path: {}", state.trail.join(" -> "));
            println!(
                "'{}' approved after {} revision(s), archived: {}",
                state.title, state.revision, state.archived
            );
            check(
                state.revision == 3 && state.approved && state.archived,
                "the guarded loop ran three times and exited",
            );
            check(
                state.trail
                    == [
                        "Draft",
                        "LegalReview",
                        "Review",
                        "Draft",
                        "Review",
                        "Draft",
                        "Review",
                        "Archive",
                    ],
                "both branches into Review were taken: through LegalReview once, directly twice",
            );
        }
        GraphExecutionResult::Failed(err) => fail(&format!("graph execution failed: {err:?}")),
        _ => fail("no interrupt boundary, wait or cancellation in this graph"),
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
