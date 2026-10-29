//! Multi-node graphs: a "diamond" shaped graph combining a linear chain
//! (`.add_edge(...)`) with a conditional branch (`.add_conditional_edge(...)`)
//! that converges back to a shared node: the first graph in this suite with
//! more than one node.
//!
//! `CalculateSubtotal` -> `ApplyDiscount` -> (branch on order size) ->
//! `TagBulkShipping` or `TagStandardShipping` -> `FormatReceipt`.
//!
//! Also demonstrates that execution order comes from edges and the entry
//! point, not from the order nodes are passed to `.add_node(...)`: nodes are
//! deliberately added out of run order below, and `.set_entry_point(...)`
//! overrides the "first node added" default that `01-hello-agent` relies on.
//!
//! The graph runs once per sample order and checks that each took the branch
//! it should, exiting with code 1 if not.

use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::StateGraph;
use datalium_strata::store::CheckpointStore;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderState {
    item: String,
    quantity: u32,
    unit_price: f64,
    subtotal: Option<f64>,
    discount: Option<f64>,
    total: Option<f64>,
    shipping_note: Option<String>,
    receipt: Option<String>,
}

#[tokio::main]
async fn main() {
    // `FormatReceipt` is added first — without `.set_entry_point(...)` below
    // it would silently become the entry point (per `add_node`'s "first
    // node added" rule) and the graph would run receipt-formatting on
    // half-populated state before anything computed a subtotal.
    let graph = StateGraph::<OrderState>::new()
        .add_node("FormatReceipt", |state: OrderState| {
            let receipt = format!(
                "{}x {} @ ${:.2} = ${:.2} (- ${:.2} discount) => ${:.2} [{}]",
                state.quantity,
                state.item,
                state.unit_price,
                state
                    .subtotal
                    .expect("CalculateSubtotal runs before this node"),
                state.discount.expect("ApplyDiscount runs before this node"),
                state.total.expect("ApplyDiscount runs before this node"),
                state
                    .shipping_note
                    .as_deref()
                    .expect("TagBulkShipping/TagStandardShipping runs before this node"),
            );
            OrderState {
                receipt: Some(receipt),
                ..state
            }
        })
        .add_node("CalculateSubtotal", |state: OrderState| OrderState {
            subtotal: Some(state.quantity as f64 * state.unit_price),
            ..state
        })
        .add_node("ApplyDiscount", |state: OrderState| {
            let subtotal = state
                .subtotal
                .expect("CalculateSubtotal runs before this node");
            // Bulk orders of $100+ get a flat 10% off.
            let discount = if subtotal >= 100.0 {
                subtotal * 0.10
            } else {
                0.0
            };
            OrderState {
                discount: Some(discount),
                total: Some(subtotal - discount),
                ..state
            }
        })
        .add_node("TagBulkShipping", |state: OrderState| OrderState {
            shipping_note: Some("bulk order, ships via freight".to_string()),
            ..state
        })
        .add_node("TagStandardShipping", |state: OrderState| OrderState {
            shipping_note: Some("standard shipping".to_string()),
            ..state
        })
        .set_entry_point("CalculateSubtotal")
        .add_edge("CalculateSubtotal", "ApplyDiscount")
        // The conditional edge is the "if" (10+ units ships freight); the
        // plain edge added right after it is the "else" — `run` tries a
        // node's outgoing edges in the order they were added and takes the
        // first whose condition matches, so an unconditional edge added
        // after one or more conditional edges is the default branch. This
        // condition (quantity) is deliberately independent of
        // `ApplyDiscount`'s dollar-amount threshold above, so the two
        // sample orders below each exercise a different combination.
        .add_conditional_edge("ApplyDiscount", "TagBulkShipping", |s: &OrderState| {
            s.quantity >= 10
        })
        .add_edge("ApplyDiscount", "TagStandardShipping")
        .add_edge("TagBulkShipping", "FormatReceipt")
        .add_edge("TagStandardShipping", "FormatReceipt")
        .compile()
        .expect("graph is structurally valid");

    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    // Each order names the branch it must take, so the run proves both fire.
    let orders = [
        (
            "bulk-order-thread",
            order(12),
            "bulk order, ships via freight",
        ),
        ("standard-order-thread", order(3), "standard shipping"),
    ];

    for (thread_id, initial, expected_shipping) in orders {
        match graph.run(thread_id, initial, store.clone()).await {
            GraphExecutionResult::Completed(state) => {
                println!(
                    "{}",
                    state
                        .receipt
                        .as_deref()
                        .expect("FormatReceipt node always sets this")
                );
                check(
                    state.shipping_note.as_deref() == Some(expected_shipping),
                    &format!(
                        "{thread_id} took the wrong branch ({:?})",
                        state.shipping_note
                    ),
                );
            }
            GraphExecutionResult::Failed(err) => panic!("graph execution failed: {err:?}"),
            _ => unreachable!("no interrupt boundary, wait or cancellation in this graph"),
        }
    }
}

/// A widget order at $9.50 a unit, before any node has run.
fn order(quantity: u32) -> OrderState {
    OrderState {
        item: "widget".to_string(),
        quantity,
        unit_price: 9.50,
        subtotal: None,
        discount: None,
        total: None,
        shipping_note: None,
        receipt: None,
    }
}

/// Exits with code 1 when a claim the demo makes turns out false.
fn check(ok: bool, claim: &str) {
    if !ok {
        eprintln!("CHECK FAILED: {claim}");
        std::process::exit(1);
    }
}
