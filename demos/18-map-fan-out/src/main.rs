//! Dynamic fan-out: the map (`design/27-Map-Fan-Out.md`).
//!
//! An accounts-payable check. An invoice arrives with however many lines it
//! has, and every line runs the same small audit graph -- `Classify` ->
//! `CheckPrice` -> `Score` -- before `Decide` approves the invoice or holds it
//! for review:
//!
//! ```text
//! AuditLines (map: one LineAudit child per invoice line) -> Decide
//! ```
//!
//! Demo 10's parallel group has branches fixed when the graph is built. Demo
//! 13's subgraph runs one child graph inside one node. A map runs a compiled
//! child graph once per item of a list it reads from the state when the node
//! starts, so how many children run is only known at run time. This demo
//! shows:
//!
//! 1. The fan-out    -- 12 lines, at most 8 at a time by default (the limit is
//!    never unbounded), started and reduced in list order, each item's nodes at
//!    `AuditLines/<line id>/<node>`.
//! 2. Bad lists      -- a duplicate key, a key with '/', an empty key, or more
//!    lines than `max_items` fail the map fatally before any line is looked at.
//! 3. A failing line -- the child's failure kind becomes the item's and then the
//!    map's: a fatal one is never retried, a transient one carries its retry
//!    hint, and a retry of the map re-runs every line.
//! 4. KeepSucceeded  -- an outcomes reducer that sees every line, and flags the
//!    one that failed.
//! 5. `.compile()`   -- refuses a child graph with an interrupt boundary, and a
//!    `max_concurrency` of 0.
//!
//! The price list is an in-memory map, so the demo runs offline. Where
//! concurrency matters, a gate holds price lookups until the expected number
//! are in flight at once, so the peak is exact rather than dependent on timing.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::map::MapOptions;
use datalium_strata::observability::{BranchResult, RunEvent, RunEventChannel, RunEventKind};
use datalium_strata::parallel::{BranchOutcomes, BranchStatus};
use datalium_strata::store::CheckpointStore;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[tokio::main]
async fn main() {
    scenario_1_fan_out().await;
    scenario_2_bad_lists().await;
    scenario_3_failing_lines().await;
    scenario_4_keep_succeeded().await;
    step_5_compile_refusals();

    println!();
    println!(
        "=== Done: one child graph per line, bounded, keyed, and failing the way its children failed. ==="
    );
}

// --- Scenario 1: the fan-out. ---

async fn scenario_1_fan_out() {
    println!("=== Scenario 1: a 12-line invoice -- one child graph per line, default options ===");
    let invoice = Invoice::of(
        "INV-2207",
        vec![
            line("L01", "PAP-A4", 40, 420),
            line("L02", "PAP-A3", 10, 810),
            line("L03", "INK-K", 4, 6200),
            line("L04", "INK-C", 2, 7100),
            line("L05", "PEN-BLU", 200, 90),
            line("L06", "PEN-RED", 50, 90),
            line("L07", "PAD-A5", 60, 240),
            line("L08", "FLD-GRN", 100, 35),
            line("L09", "INK-M", 2, 7950),
            line("L10", "SVC-INST", 1, 4500),
            line("L11", "PAP-A4", 20, 420),
            line("L12", "SVC-DLV", 1, 1200),
        ],
    );
    let ids = invoice.ids();
    let prices = PriceList::holding_until(8);
    let graph = invoice_graph(&prices, MapOptions::new())
        .compile()
        .expect("the invoice graph is valid");
    let (result, events) = run_recorded(&graph, "inv-1", invoice.clone(), memory_store()).await;
    let audited = expect_completed(result);

    let expansions = map_expansions(&events);
    let started = items_started(&events);
    let l07: Vec<String> = completed_paths(&events)
        .into_iter()
        .filter(|path| path.starts_with("AuditLines/L07/"))
        .collect();
    let child_nodes = completed_paths(&events)
        .iter()
        .filter(|path| path.matches('/').count() == 2)
        .count();
    let peak = prices.peak();
    let keys = expansions
        .first()
        .map(|(_, keys)| keys.clone())
        .unwrap_or_default();
    println!("  MapExpanded: [{}]", keys.join(", "));
    println!("  Items started in list order: {}", started.join(", "));
    println!(
        "  Peak price lookups in flight: {peak} (MapOptions::DEFAULT_MAX_CONCURRENCY = {}); the other {} lines waited for a slot.",
        MapOptions::<LineAudit>::DEFAULT_MAX_CONCURRENCY,
        ids.len() - peak
    );
    println!("  One item's child nodes: {}", l07.join(" -> "));
    println!("  Child nodes run: {child_nodes} (12 lines x 3 nodes)");
    println!("  Reducer got the audits in list order:");
    for audit in &audited.audits {
        println!("    {audit}");
    }
    println!("  Decision: {}", audited.decision_text());

    check(
        expansions.len() == 1 && keys == ids,
        "expected one MapExpanded listing every line id in list order",
    );
    let first = |wanted: fn(&RunEventKind) -> bool| events.iter().position(|e| wanted(&e.kind));
    check(
        first(|k| matches!(k, RunEventKind::MapExpanded { .. }))
            < first(|k| matches!(k, RunEventKind::BranchStarted)),
        "expected MapExpanded before the first item starts",
    );
    check(started == ids, "expected items to start in list order");
    check(
        peak == 8 && MapOptions::<LineAudit>::new().max_concurrency() == 8,
        "expected the default limit of 8",
    );
    check(
        audited
            .audits
            .iter()
            .map(|a| a.line.id.clone())
            .collect::<Vec<_>>()
            == ids,
        "expected the reducer to get list order",
    );
    check(
        l07 == [
            "AuditLines/L07/Classify",
            "AuditLines/L07/CheckPrice",
            "AuditLines/L07/Score",
        ],
        "expected <map>/<key>/<node> paths",
    );
    check(child_nodes == 36, "expected 36 child nodes (12 x 3)");
    check(
        audited.decision.as_deref() == Some("hold: L09 billed 79.50, list 71.00 (+12%)"),
        "expected L09's overcharge to hold the invoice",
    );

    println!();
    println!(
        "=== Scenario 1b: the same invoice with max_concurrency = 3 (the price API's own limit) ==="
    );
    let prices = PriceList::holding_until(3);
    let graph = invoice_graph(&prices, MapOptions::new().with_max_concurrency(3))
        .compile()
        .expect("the invoice graph is valid");
    let (result, events) = run_recorded(&graph, "inv-1b", invoice, memory_store()).await;
    expect_completed(result);
    let in_order = items_started(&events) == ids;
    println!(
        "  Peak price lookups in flight: {}; items still started in list order: {in_order}.",
        prices.peak()
    );
    check(prices.peak() == 3, "expected at most 3 items in flight");
    check(in_order, "expected items to start in list order");
}

// --- Scenario 2: bad lists fail the map before anything starts. ---

async fn scenario_2_bad_lists() {
    println!();
    println!(
        "=== Scenario 2: bad lists fail before any line is looked at -- even with retry(3) on the map ==="
    );
    let bad_lists = [
        (
            "an exported row repeated",
            Invoice::of(
                "INV-2208",
                vec![
                    line("L01", "PAP-A4", 1, 420),
                    line("L02", "INK-K", 1, 6200),
                    line("L02", "INK-K", 1, 6200),
                ],
            ),
        ),
        (
            "a split line",
            Invoice::of(
                "INV-2209",
                vec![
                    line("L01", "PAP-A4", 1, 420),
                    line("L02/1", "INK-K", 1, 6200),
                ],
            ),
        ),
        (
            "a line with no id",
            Invoice::of(
                "INV-2210",
                vec![line("L01", "PAP-A4", 1, 420), line("", "INK-K", 1, 6200)],
            ),
        ),
        (
            "60 lines, limit 50",
            Invoice::of(
                "INV-2211",
                (1..=60)
                    .map(|i| line(&format!("L{i:02}"), "PEN-BLU", 1, 90))
                    .collect(),
            ),
        ),
    ];
    for (problem, bad) in bad_lists {
        let prices = PriceList::new();
        let store = memory_store();
        let graph = invoice_graph(&prices, MapOptions::new().with_max_items(50))
            .set_recovery_policy("AuditLines", RecoveryPolicy::retry(3))
            .compile()
            .expect("the invoice graph is valid");
        let number = bad.number.clone();
        let (result, events) = run_recorded(&graph, &number, bad, store.clone()).await;
        let (_, attempts, message) = expect_node_failed(result);
        let metadata = failed_metadata(&store, &number).await;
        let kind = &metadata["error_kind"];
        let lookups = prices.lookups().len();
        println!("  {problem:<26} -> {kind}, attempts {attempts}, lookups {lookups}: {message}");
        check(
            attempts == 1 && kind == "fatal",
            "expected a fatal failure, not retried",
        );
        check(lookups == 0, "expected no line to be looked at");
        check(
            !events.iter().any(|e| {
                matches!(
                    e.kind,
                    RunEventKind::MapExpanded { .. } | RunEventKind::BranchStarted
                )
            }),
            "expected no MapExpanded and no item started",
        );
    }
}

// --- Scenario 3: a failing line -- the child's kind becomes the map's. ---

async fn scenario_3_failing_lines() {
    println!();
    println!(
        "=== Scenario 3a: an unknown SKU (fatal) -- retry(2) on the map, retry(1) on CheckPrice inside the child ==="
    );
    let with_unknown_sku = Invoice::of(
        "INV-2212",
        vec![
            line("L01", "PAP-A4", 10, 420),
            line("L02", "INK-K", 1, 6200),
            line("L03", "INK-X9", 1, 5800),
            line("L04", "PEN-BLU", 20, 90),
        ],
    );
    {
        let store = memory_store();
        let prices = PriceList::new();
        let graph = invoice_graph(&prices, MapOptions::new())
            .set_recovery_policy("AuditLines", RecoveryPolicy::retry(2))
            .compile()
            .expect("the invoice graph is valid");
        let result = graph.run("inv-3a", with_unknown_sku, store.clone()).await;
        let (node, attempts, message) = expect_node_failed(result);
        let metadata = failed_metadata(&store, "inv-3a").await;
        let kind = &metadata["error_kind"];
        println!("  Failed at '{node}' after {attempts} attempt(s), {kind}:");
        println!("    {message}");
        println!(
            "  Fatal in the child, fatal for the item, fatal for the map: neither retry policy ran."
        );
        check(
            message
                == "item 'L03': failed at 'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'",
            "expected the item's message",
        );
        check(
            node == "AuditLines" && attempts == 1 && kind == "fatal",
            "expected a fatal map failure with no retry",
        );
        check(
            prices.lookups().iter().filter(|id| *id == "L03").count() == 1,
            "expected L03 to be looked up once",
        );
    }

    println!();
    println!(
        "=== Scenario 3b: the price API is busy (transient, retry in 2 s) -- one request at a time ==="
    );
    let small = Invoice::of(
        "INV-2213",
        vec![
            line("L01", "PAP-A4", 10, 420),
            line("L02", "INK-K", 1, 6200),
            line("L03", "PEN-BLU", 20, 90),
        ],
    );
    let one_at_a_time = || MapOptions::new().with_max_concurrency(1);
    {
        let store = memory_store();
        let prices = PriceList::busy_for("INK-K", 2);
        let graph = invoice_graph(&prices, one_at_a_time())
            .compile()
            .expect("the invoice graph is valid");
        let result = graph.run("inv-3b", small.clone(), store.clone()).await;
        let (_, _, message) = expect_node_failed(result);
        let metadata = failed_metadata(&store, "inv-3b").await;
        let kind = metadata["error_kind"].as_str();
        let hint = metadata
            .get("strata.retry_after_ms")
            .map(String::as_str)
            .unwrap_or("(none)");
        let lookups = prices.lookups();
        println!("  No retry on the map: failed, {kind}, strata.retry_after_ms = {hint}:");
        println!("    {message}");
        println!("  Lookups: {} -- L03 never started.", lookups.join(", "));
        check(
            message
                == "item 'L02': failed at 'CheckPrice' after 2 attempt(s): price service busy (429)",
            "expected the child's retries to have run out",
        );
        check(
            kind == "transient" && hint == "2000",
            "expected the item's kind and hint on the map",
        );
        check(
            lookups == ["L01", "L02", "L02"],
            "expected L03 never to start",
        );
    }
    {
        let prices = PriceList::busy_for("INK-K", 2);
        let graph = invoice_graph(&prices, one_at_a_time())
            .set_recovery_policy("AuditLines", RecoveryPolicy::retry(1))
            .compile()
            .expect("the invoice graph is valid");
        let (result, events) = run_recorded(&graph, "inv-3c", small, memory_store()).await;
        let done = expect_completed(result);
        let attempts: Vec<String> = map_expansions(&events)
            .iter()
            .map(|(attempt, _)| attempt.to_string())
            .collect();
        let lookups = prices.lookups();
        println!(
            "  With retry(1) on the map: {}. MapExpanded on attempts {}.",
            done.decision_text(),
            attempts.join(" and ")
        );
        println!(
            "  Lookups: {} -- the retry re-ran L01, which had succeeded.",
            lookups.join(", ")
        );
        check(
            attempts == ["1", "2"],
            "expected one MapExpanded per attempt",
        );
        check(
            lookups == ["L01", "L02", "L02", "L01", "L02", "L03"],
            "expected the retry to re-run every line",
        );
        check(
            done.decision.as_deref() == Some("approve"),
            "expected the retried invoice to be approved",
        );
    }
}

// --- Scenario 4: KeepSucceeded with an outcomes reducer. ---

async fn scenario_4_keep_succeeded() {
    println!();
    println!(
        "=== Scenario 4: PartialResults::KeepSucceeded -- audit what can be audited, flag the rest ==="
    );
    let mixed = Invoice::of(
        "INV-2214",
        vec![
            line("L01", "PAP-A4", 10, 420),
            line("L02", "INK-K", 1, 6200),
            line("L03", "INK-X9", 1, 5800),
            line("L04", "PEN-BLU", 20, 90),
            line("L05", "SVC-INST", 1, 5200),
        ],
    );
    let reduced: Arc<Mutex<Option<BranchOutcomes<LineAudit>>>> = Arc::default();
    let keep = reduced.clone();
    let graph = StateGraph::<Invoice>::new()
        .add_map_with_outcomes(
            "AuditLines",
            |inv: &Invoice| inv.lines.clone(),
            |line: &InvoiceLine| line.id.clone(),
            line_audit_graph(&PriceList::new()),
            |_: &Invoice, line: InvoiceLine| LineAudit::for_line(line),
            move |inv: &Invoice, results: BranchOutcomes<LineAudit>| {
                let audits = results.succeeded().cloned().collect();
                let review = results
                    .outcomes()
                    .iter()
                    .filter(|o| o.status != BranchStatus::Succeeded)
                    .map(|o| {
                        let why = o.error.as_ref().map(NodeError::message).unwrap_or("?");
                        format!("{} not audited: {why}", o.name)
                    })
                    .collect();
                *keep.lock().unwrap() = Some(results);
                Invoice {
                    audits,
                    review,
                    ..inv.clone()
                }
            },
            MapOptions::new().keep_succeeded(),
        )
        .add_inline_node("Decide", decide)
        .add_edge("AuditLines", "Decide")
        .compile()
        .expect("the invoice graph is valid");
    let (result, events) = run_recorded(&graph, "inv-4", mixed, memory_store()).await;
    let kept = expect_completed(result);
    let Some(seen) = reduced.lock().unwrap().take() else {
        fail("expected the reducer to run");
    };

    let statuses: Vec<String> = seen
        .outcomes()
        .iter()
        .map(|o| format!("{} {:?}", o.name, o.status))
        .collect();
    println!(
        "  Reducer saw (list order): {}; join_met: {}",
        statuses.join(", "),
        seen.join_met()
    );
    let l03 = seen.get("L03").and_then(|o| o.error.clone());
    if let Some(error) = &l03 {
        println!("  L03's error: {}, \"{}\"", kind_of(error), error.message());
    }
    println!("  Decision: {}", kept.decision_text());
    for line in &kept.review {
        println!("    - {line}");
    }

    check(
        seen.outcomes().iter().map(|o| o.status).collect::<Vec<_>>()
            == [
                BranchStatus::Succeeded,
                BranchStatus::Succeeded,
                BranchStatus::Failed,
                BranchStatus::Succeeded,
                BranchStatus::Succeeded,
            ],
        "expected every line's outcome, in list order",
    );
    check(
        seen.outcomes()
            .iter()
            .map(|o| o.name.as_str())
            .collect::<Vec<_>>()
            == ["L01", "L02", "L03", "L04", "L05"],
        "expected each outcome to be named by its key",
    );
    check(
        l03.as_ref().is_some_and(|e| {
            !e.is_transient()
                && e.message() == "failed at 'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'"
        }),
        "expected L03's own error",
    );
    check(
        !seen.join_met() && seen.deciding().is_empty(),
        "expected an unmet All join",
    );
    check(
        !events.iter().any(|e| {
            matches!(
                e.kind,
                RunEventKind::BranchCompleted {
                    result: BranchResult::Cancelled,
                    ..
                }
            )
        }),
        "expected no line to be cancelled for L03's failure",
    );
    check(
        kept.review.len() == 2 && kept.decision.as_deref() == Some("hold: 2 line(s) need review"),
        "expected L03 and L05 to need review",
    );
}

// --- Step 5: what .compile() refuses. ---

fn step_5_compile_refusals() {
    println!();
    println!("=== Step 5: a child graph that could pause for a human is refused at .compile() ===");
    let sign_off = Arc::new(
        StateGraph::<LineAudit>::new()
            .add_inline_node("Classify", |a: LineAudit| a)
            .add_inline_node("Score", |a: LineAudit| a)
            .add_edge("Classify", "Score")
            .add_interrupt_boundary("Score")
            .compile()
            .expect("a child with a boundary compiles on its own"),
    );
    // The same boundary one level down, inside a subgraph of the child, is found too.
    let with_review = Arc::new(
        StateGraph::<LineAudit>::new()
            .add_inline_node("CheckPrice", |a: LineAudit| a)
            .add_subgraph(
                "Review",
                sign_off.clone(),
                |a: &LineAudit| a.clone(),
                |_: &LineAudit, reviewed: LineAudit| reviewed,
            )
            .add_edge("CheckPrice", "Review")
            .compile()
            .expect("a child with a nested boundary compiles on its own"),
    );
    for (child, at) in [(sign_off, "Score"), (with_review, "Review/Score")] {
        let message = compilation_failure(plain_map(child, MapOptions::new()));
        println!("  Refused: {message}");
        check(
            message
                == format!(
                    "map 'AuditLines' runs a graph with an interrupt boundary at '{at}'; a map item can't freeze"
                ),
            "expected the boundary to be named by its path",
        );
    }

    let child = line_audit_graph(&PriceList::new());
    let message = compilation_failure(plain_map(child, MapOptions::new().with_max_concurrency(0)));
    println!("  Refused: {message}");
    println!("  (max_concurrency is a usize, not an Option: a map is never unbounded.)");
    check(
        message == "map 'AuditLines' has a max_concurrency of 0; it must be at least 1",
        "expected max_concurrency = 0 to be refused",
    );
}

/// A one-node parent over `child`, for the compile-time refusals.
fn plain_map(
    child: Arc<CompiledGraph<LineAudit>>,
    options: MapOptions<LineAudit>,
) -> StateGraph<Invoice> {
    StateGraph::<Invoice>::new().add_map_with(
        "AuditLines",
        |inv: &Invoice| inv.lines.clone(),
        |line: &InvoiceLine| line.id.clone(),
        child,
        |_: &Invoice, line: InvoiceLine| LineAudit::for_line(line),
        |inv: &Invoice, _: Vec<LineAudit>| inv.clone(),
        options,
    )
}

fn compilation_failure(graph: StateGraph<Invoice>) -> String {
    match graph.compile() {
        Err(StrataError::CompilationFailed(message)) => message,
        Err(other) => fail(&format!("expected CompilationFailed, got {other:?}")),
        Ok(_) => fail("expected CompilationFailed, got a compiled graph"),
    }
}

// --- The graphs. ---

/// The parent: `AuditLines` (a map over the invoice's lines, keyed by line
/// id) -> `Decide`. Returned uncompiled so a scenario can add a recovery
/// policy first.
fn invoice_graph(prices: &Arc<PriceList>, options: MapOptions<LineAudit>) -> StateGraph<Invoice> {
    StateGraph::<Invoice>::new()
        .add_map_with(
            "AuditLines",
            |inv: &Invoice| inv.lines.clone(),
            |line: &InvoiceLine| line.id.clone(),
            line_audit_graph(prices),
            |_: &Invoice, line: InvoiceLine| LineAudit::for_line(line),
            |inv: &Invoice, audits: Vec<LineAudit>| Invoice {
                audits,
                ..inv.clone()
            },
            options,
        )
        .add_inline_node("Decide", decide)
        .add_edge("AuditLines", "Decide")
}

/// The child, run once per line: `Classify` -> `CheckPrice` -> `Score`.
/// `CheckPrice` retries a transient failure once on its own, inside the item,
/// before the item fails. `Classify` and `Score` only compute, so they are
/// inline nodes (Rust-only, `design/39`): no hop to the blocking pool.
fn line_audit_graph(prices: &Arc<PriceList>) -> Arc<CompiledGraph<LineAudit>> {
    let prices = prices.clone();
    Arc::new(
        StateGraph::<LineAudit>::new()
            .add_inline_node("Classify", |audit: LineAudit| LineAudit {
                category: Some(category(&audit.line.sku).to_string()),
                ..audit
            })
            .add_async_fallible_node("CheckPrice", move |audit: LineAudit, _: NodeContext| {
                let prices = prices.clone();
                async move {
                    let list = prices.look_up(&audit.line).await?;
                    Ok(LineAudit {
                        list_price: Some(list),
                        ..audit
                    })
                }
            })
            .add_inline_node("Score", score)
            .add_edge("Classify", "CheckPrice")
            .add_edge("CheckPrice", "Score")
            .set_recovery_policy("CheckPrice", RecoveryPolicy::retry(1))
            .compile()
            .expect("the line audit graph is valid"),
    )
}

fn category(sku: &str) -> &'static str {
    match sku.split('-').next().unwrap_or_default() {
        "PAP" | "PAD" => "paper",
        "INK" => "toner",
        "PEN" | "FLD" => "stationery",
        "SVC" => "service",
        _ => "other",
    }
}

/// 100 at or below list price, 4 points off per percent over it.
fn score(audit: LineAudit) -> LineAudit {
    let list = audit.list_price.expect("CheckPrice sets the list price");
    let billed = audit.line.unit_price;
    let over = ((billed - list) as f64 / list as f64 * 100.0).round() as i64;
    if over <= 0 {
        return LineAudit {
            score: Some(100),
            ..audit
        };
    }
    LineAudit {
        score: Some((100 - 4 * over).max(0)),
        note: Some(format!(
            "billed {}, list {} (+{over}%)",
            money(billed),
            money(list)
        )),
        ..audit
    }
}

/// Any line scoring under 80 (or not audited at all) holds the invoice.
fn decide(invoice: Invoice) -> Invoice {
    let mut review = invoice.review.clone();
    review.extend(
        invoice
            .audits
            .iter()
            .filter(|a| a.score.unwrap_or(0) < 80)
            .map(|a| format!("{} {}", a.line.id, a.note.as_deref().unwrap_or_default())),
    );
    let decision = match review.len() {
        0 => "approve".to_string(),
        1 if invoice.review.is_empty() => format!("hold: {}", review[0]),
        n => format!("hold: {n} line(s) need review"),
    };
    Invoice {
        review,
        decision: Some(decision),
        ..invoice
    }
}

// --- State. Amounts are integer cents: no decimal crate. ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct InvoiceLine {
    id: String,
    sku: String,
    quantity: u32,
    /// Cents.
    unit_price: i64,
}

fn line(id: &str, sku: &str, quantity: u32, unit_price: i64) -> InvoiceLine {
    InvoiceLine {
        id: id.to_string(),
        sku: sku.to_string(),
        quantity,
        unit_price,
    }
}

/// The parent's state: the lines to audit, the audits that came back, and the decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invoice {
    number: String,
    lines: Vec<InvoiceLine>,
    audits: Vec<LineAudit>,
    review: Vec<String>,
    decision: Option<String>,
}

impl Invoice {
    fn of(number: &str, lines: Vec<InvoiceLine>) -> Self {
        Self {
            number: number.to_string(),
            lines,
            audits: Vec::new(),
            review: Vec::new(),
            decision: None,
        }
    }

    fn ids(&self) -> Vec<String> {
        self.lines.iter().map(|l| l.id.clone()).collect()
    }

    fn decision_text(&self) -> &str {
        self.decision.as_deref().unwrap_or("(none)")
    }
}

/// One item's child state: the line, and what Classify, CheckPrice and Score found.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LineAudit {
    line: InvoiceLine,
    category: Option<String>,
    /// Cents.
    list_price: Option<i64>,
    score: Option<i64>,
    note: Option<String>,
}

impl LineAudit {
    fn for_line(line: InvoiceLine) -> Self {
        Self {
            line,
            category: None,
            list_price: None,
            score: None,
            note: None,
        }
    }
}

impl fmt::Display for LineAudit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {:<8} {:<10} score {:>3}",
            self.line.id,
            self.line.sku,
            self.category.as_deref().unwrap_or_default(),
            self.score.map(|s| s.to_string()).unwrap_or_default()
        )?;
        if let Some(note) = &self.note {
            write!(f, "  {note}")?;
        }
        Ok(())
    }
}

fn money(cents: i64) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

// --- The price list. ---

/// The supplier's price list. It records every lookup and the peak number in
/// flight; it can hold lookups until a number are in flight at once (so a peak
/// is exact, not a matter of timing), and answer "busy" for a SKU's first few
/// lookups.
struct PriceList {
    /// Hold each lookup until this many are in flight at once (0: don't hold).
    hold_until_in_flight: usize,
    /// SKU -> how many of its first lookups answer "busy".
    busy_for: HashMap<String, u32>,
    lookups: Mutex<Vec<String>>,
    calls: Mutex<HashMap<String, u32>>,
    gate: watch::Sender<bool>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

impl PriceList {
    fn with(hold_until_in_flight: usize, busy_for: HashMap<String, u32>) -> Arc<Self> {
        Arc::new(Self {
            hold_until_in_flight,
            busy_for,
            lookups: Mutex::default(),
            calls: Mutex::default(),
            gate: watch::Sender::new(false),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }

    fn new() -> Arc<Self> {
        Self::with(0, HashMap::new())
    }

    fn holding_until(in_flight: usize) -> Arc<Self> {
        Self::with(in_flight, HashMap::new())
    }

    fn busy_for(sku: &str, lookups: u32) -> Arc<Self> {
        Self::with(0, HashMap::from([(sku.to_string(), lookups)]))
    }

    /// The line id of every lookup, in the order they arrived.
    fn lookups(&self) -> Vec<String> {
        self.lookups.lock().unwrap().clone()
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    async fn look_up(&self, line: &InvoiceLine) -> Result<i64, NodeError> {
        self.lookups.lock().unwrap().push(line.id.clone());
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        // Counts this lookup out again however it ends, a dropped future included.
        let _in_flight = InFlight(&self.in_flight);
        self.peak.fetch_max(now, Ordering::SeqCst);

        // Once the first wave is in flight together, the gate stays open for
        // every later lookup.
        if now >= self.hold_until_in_flight {
            self.gate.send_replace(true);
        }
        let mut gate = self.gate.subscribe();
        // A guard against a hang if the expected wave never forms; it never
        // fires in this demo, and the demo asserts on counts, not time.
        match tokio::time::timeout(Duration::from_secs(10), gate.wait_for(|open| *open)).await {
            Ok(Ok(_)) => {}
            _ => {
                return Err(NodeError::Fatal(format!(
                    "the gate never opened: fewer than {} lookups in flight",
                    self.hold_until_in_flight
                )));
            }
        }

        let call = {
            let mut calls = self.calls.lock().unwrap();
            let count = calls.entry(line.sku.clone()).or_insert(0);
            *count += 1;
            *count
        };
        if call <= self.busy_for.get(&line.sku).copied().unwrap_or(0) {
            return Err(NodeError::transient_after(
                "price service busy (429)",
                Duration::from_secs(2),
            ));
        }
        match list_price(&line.sku) {
            Some(price) => Ok(price),
            None => Err(NodeError::Fatal(format!("unknown SKU '{}'", line.sku))),
        }
    }
}

struct InFlight<'a>(&'a AtomicUsize);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// List prices in cents.
fn list_price(sku: &str) -> Option<i64> {
    Some(match sku {
        "PAP-A4" => 420,
        "PAP-A3" => 810,
        "PAD-A5" => 240,
        "INK-K" => 6200,
        "INK-C" => 7100,
        "INK-M" => 7100,
        "PEN-BLU" => 90,
        "PEN-RED" => 90,
        "FLD-GRN" => 35,
        "SVC-INST" => 4500,
        "SVC-DLV" => 1200,
        _ => return None,
    })
}

// --- Running and reading results. ---

fn memory_store() -> Arc<dyn CheckpointStore> {
    Arc::new(InMemoryCheckpointStore::new())
}

/// Runs with a `RunEventChannel` observer and collects every event. The
/// channel ends after `RunCompleted`, which may arrive after `run_with` returns.
async fn run_recorded(
    graph: &CompiledGraph<Invoice>,
    thread: &str,
    start: Invoice,
    store: Arc<dyn CheckpointStore>,
) -> (GraphExecutionResult<Invoice>, Vec<RunEvent>) {
    let (observer, mut receiver) = RunEventChannel::new();
    let result = graph
        .run_with(
            thread,
            start,
            store,
            RunConfig::new().with_observer(observer),
        )
        .await;
    let mut events = Vec::new();
    while let Some(event) = receiver.recv().await {
        events.push(event);
    }
    (result, events)
}

/// Each `MapExpanded`'s attempt and keys, in order.
fn map_expansions(events: &[RunEvent]) -> Vec<(u32, Vec<String>)> {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::MapExpanded { keys } => Some((e.attempt.unwrap_or(0), keys.clone())),
            _ => None,
        })
        .collect()
}

/// The keys of the items the map started, in the order it started them.
fn items_started(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::BranchStarted))
        .filter_map(|e| e.node_path.as_deref())
        .map(|path| path.trim_start_matches("AuditLines/").to_string())
        .collect()
}

/// The path of every `NodeCompleted`, in order.
fn completed_paths(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::NodeCompleted { .. }))
        .filter_map(|e| e.node_path.as_deref().map(str::to_string))
        .collect()
}

/// The metadata of the thread's `Failed` checkpoint.
async fn failed_metadata(
    store: &Arc<dyn CheckpointStore>,
    thread: &str,
) -> HashMap<String, String> {
    store
        .load(thread)
        .await
        .ok()
        .flatten()
        .and_then(|checkpoint| checkpoint.metadata)
        .unwrap_or_else(|| fail(&format!("expected a failed checkpoint for '{thread}'")))
}

fn kind_of(error: &NodeError) -> &'static str {
    if error.is_transient() {
        "transient"
    } else {
        "fatal"
    }
}

fn expect_completed(result: GraphExecutionResult<Invoice>) -> Invoice {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(error) => fail(&format!("expected Completed, got {error:?}")),
        _ => fail("expected Completed"),
    }
}

/// `(node, attempts, message)` of a node failure.
fn expect_node_failed(result: GraphExecutionResult<Invoice>) -> (String, u32, String) {
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => (node, attempts, message),
        GraphExecutionResult::Failed(error) => fail(&format!("expected NodeFailed, got {error:?}")),
        _ => fail("expected NodeFailed"),
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
