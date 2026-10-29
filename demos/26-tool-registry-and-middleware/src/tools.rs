//! The `#[strata_tool]` "providers" (inventory, billing, diagnostics, and a
//! legacy inventory that clashes) and the `Backend` they talk to.
//!
//! A Rust tool is a free `async fn(args, ToolCallContext)`, not a method on a
//! provider object, so a provider here is a module plus a function that lists
//! its generated handles. A tool reaches its dependencies through the call's
//! services (`ctx.service::<Backend>()`), which the caller puts on the context
//! (`ToolCallContext::with_service`) or on the run (`RunConfig::with_service`,
//! copied by `ToolCallContext::from(&node_ctx)`).

use std::sync::{Arc, Mutex};

use datalium_strata::prelude::*;
use serde::Deserialize;

use crate::middleware::ChainTrace;

/// Stands in for the systems the tools talk to. It records every call that
/// actually reached a tool, so the demo can tell a call the tool ran from one
/// a middleware or the registry stopped.
#[derive(Default)]
pub struct Backend {
    calls: Mutex<Vec<String>>,
    last_context: Mutex<Option<ToolCallContext>>,
    /// Where a call that reached a tool is marked, after the middleware it went through.
    trace: Option<ChainTrace>,
}

impl Backend {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn traced(trace: ChainTrace) -> Arc<Self> {
        Arc::new(Self {
            trace: Some(trace),
            ..Self::default()
        })
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    pub fn calls_to(&self, tool: &str) -> usize {
        let prefix = format!("{tool} ");
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.starts_with(&prefix))
            .count()
    }

    /// The last context a tool was handed, as the tool saw it.
    pub fn last_context(&self) -> Option<ToolCallContext> {
        self.last_context.lock().unwrap().clone()
    }

    fn record(&self, tool: &str, detail: &str, ctx: Option<&ToolCallContext>) {
        self.calls.lock().unwrap().push(format!("{tool} {detail}"));
        if let Some(trace) = &self.trace {
            trace.enter(&format!("{tool} ran"));
        }
        if let Some(ctx) = ctx {
            *self.last_context.lock().unwrap() = Some(ctx.clone());
        }
    }
}

/// The backend the caller supplied on the call's context.
fn backend(ctx: &ToolCallContext) -> Arc<Backend> {
    ctx.service::<Backend>()
        .expect("the caller supplies a Backend service")
}

fn stock(sku: &str) -> u32 {
    match sku {
        "AB-12" => 7,
        _ => 0,
    }
}

#[derive(Deserialize, StrataSchema)]
pub struct Sku {
    pub sku: String,
}

#[derive(Deserialize, StrataSchema)]
pub struct Reserve {
    pub sku: String,
    pub count: u32,
}

#[derive(Deserialize, StrataSchema)]
#[serde(rename_all = "camelCase")]
pub struct Refund {
    pub order_id: String,
    pub amount: f64,
}

#[derive(Deserialize, StrataSchema)]
pub struct NoArgs {}

// --- Inventory: stock lookups and reservations. ---

#[strata_tool(name = "inventory.lookup", description = "Units in stock for a SKU.")]
pub async fn inventory_lookup(args: Sku, ctx: ToolCallContext) -> Result<String, StrataError> {
    backend(&ctx).record("inventory.lookup", &args.sku, None);
    Ok(stock(&args.sku).to_string())
}

// The context is the second parameter and carries the call's token
// (`ctx.cancellation()`). Neither is in the manifest: a model only ever sees
// `sku` and `count`.
#[strata_tool(
    name = "inventory.reserve",
    description = "Reserves units of a SKU for the calling tenant."
)]
pub async fn inventory_reserve(args: Reserve, ctx: ToolCallContext) -> Result<String, StrataError> {
    backend(&ctx).record(
        "inventory.reserve",
        &format!("{} x {}", args.count, args.sku),
        Some(&ctx),
    );
    tokio::task::yield_now().await;
    if ctx.cancellation().is_cancelled() {
        return Err(StrataError::ToolCancelled("inventory.reserve".to_string()));
    }
    let tenant = ctx
        .metadata()
        .get("tenant")
        .map_or("(no tenant)", String::as_str);
    Ok(format!(
        "reserved {} x {} for {tenant}, requested by {}",
        args.count,
        args.sku,
        ctx.caller_id().unwrap_or("(nobody)")
    ))
}

pub fn inventory_tools() -> Vec<Box<dyn StrataTool>> {
    vec![
        Box::new(InventoryLookupTool),
        Box::new(InventoryReserveTool),
    ]
}

// --- Billing: refunds. ---

#[strata_tool(
    name = "billing.refund",
    description = "Refunds part or all of an order."
)]
pub async fn billing_refund(args: Refund, ctx: ToolCallContext) -> Result<String, StrataError> {
    backend(&ctx).record(
        "billing.refund",
        &format!("{} {:.2}", args.order_id, args.amount),
        None,
    );
    Ok(format!("refunded {:.2} on {}", args.amount, args.order_id))
}

pub fn billing_tools() -> Vec<Box<dyn StrataTool>> {
    vec![Box::new(BillingRefundTool)]
}

// --- Diagnostics: a tool that only reports what it was told about its caller. ---

#[strata_tool(
    name = "diag.whoami",
    description = "Reports what the tool knows about its caller."
)]
pub async fn diag_whoami(_args: NoArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    backend(&ctx).record("diag.whoami", "-", Some(&ctx));
    Ok(format!(
        "{} at {} (attempt {})",
        ctx.caller_id().unwrap_or("(nobody)"),
        ctx.node_path().unwrap_or("(no node)"),
        ctx.attempt().unwrap_or(0)
    ))
}

pub fn diagnostics_tools() -> Vec<Box<dyn StrataTool>> {
    vec![Box::new(DiagWhoamiTool)]
}

/// An older inventory provider that also declares `inventory.lookup`. The
/// registry refuses it next to [`inventory_tools`]. `#[strata_tool]` has no
/// compile-time check for two tools of one name (the .NET generator's
/// `STRATA003`): the handles are named after the functions, so only the
/// registry sees the clash, in one module or across several.
pub mod legacy {
    use datalium_strata::prelude::*;

    use super::{NoArgs, Sku};

    #[strata_tool(name = "inventory.audit", description = "Lists stock discrepancies.")]
    pub async fn legacy_audit(_args: NoArgs) -> Result<String, StrataError> {
        Ok("no discrepancies".to_string())
    }

    #[strata_tool(
        name = "inventory.lookup",
        description = "Units in stock (legacy warehouse)."
    )]
    pub async fn legacy_lookup(_args: Sku) -> Result<String, StrataError> {
        Ok("0".to_string())
    }

    pub fn tools() -> Vec<Box<dyn StrataTool>> {
        vec![Box::new(LegacyAuditTool), Box::new(LegacyLookupTool)]
    }
}
