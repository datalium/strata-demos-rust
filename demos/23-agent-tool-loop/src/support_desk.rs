//! A small support desk: an order book, a shipping tracker, a refund gateway
//! and an account service, as four tools. Three are `#[strata_tool]` functions;
//! the refund gateway is a hand-written `StrataTool`, because it has to say why
//! it failed (see [`RefundsIssueTool`]).
//!
//! The desk is a run service (`RunConfig::with_service`): each tool takes the
//! call's `ToolCallContext` and finds the desk in it, so each scenario gets a
//! fresh desk and can count what ran. That is the Rust way to give tools state;
//! a `#[strata_tool]` is a free function, not a method.
//!
//! The names are dotted, as Strata names usually are. Providers don't accept a
//! dot in a tool name, which is what `ToolDeclarations` is for.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;
use datalium_strata::prelude::*;
use serde::{Deserialize, Serialize};

/// An order as `orders.lookup` returns it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderInfo {
    pub order_id: String,
    pub customer_id: String,
    pub total: f64,
    pub status: String,
    pub days_since_delivery: u32,
}

#[derive(Default)]
struct Ledger {
    /// Every tool that ran, as "name(arguments)", in order.
    calls: Vec<String>,
    /// Refunds actually issued: (refund id, order, amount).
    refunds: Vec<(String, String, f64)>,
}

pub struct SupportDesk {
    orders: BTreeMap<&'static str, OrderInfo>,
    /// When set, the refund gateway rejects every refund with this message.
    gateway_failure: Option<String>,
    ledger: Mutex<Ledger>,
}

impl SupportDesk {
    pub fn new() -> Arc<Self> {
        Self::build(None)
    }

    pub fn with_gateway_failure(message: &str) -> Arc<Self> {
        Self::build(Some(message.to_string()))
    }

    fn build(gateway_failure: Option<String>) -> Arc<Self> {
        let order = |id: &str, customer: &str, total, status: &str, days| OrderInfo {
            order_id: id.into(),
            customer_id: customer.into(),
            total,
            status: status.into(),
            days_since_delivery: days,
        };
        Arc::new(Self {
            orders: BTreeMap::from([
                (
                    "A-1001",
                    order(
                        "A-1001",
                        "C-77",
                        39.90,
                        "delivered, box crushed in transit",
                        3,
                    ),
                ),
                ("A-1002", order("A-1002", "C-77", 129.00, "in transit", 0)),
                ("A-1003", order("A-1003", "C-12", 64.50, "delivered", 70)),
            ]),
            gateway_failure,
            ledger: Mutex::default(),
        })
    }

    pub fn calls(&self) -> Vec<String> {
        self.ledger.lock().unwrap().calls.clone()
    }

    pub fn refunds(&self) -> Vec<(String, String, f64)> {
        self.ledger.lock().unwrap().refunds.clone()
    }

    fn note(&self, call: String) {
        self.ledger.lock().unwrap().calls.push(call);
    }
}

/// The desk's registry: all four tools, in this order.
pub fn registry() -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    registry
        .register(Box::new(OrdersLookupTool))
        .register(Box::new(ShippingTrackTool))
        .register(Box::new(RefundsIssueTool))
        .register(Box::new(AccountsCloseTool));
    registry
}

fn desk(ctx: &ToolCallContext) -> Result<Arc<SupportDesk>, StrataError> {
    ctx.service::<SupportDesk>()
        .ok_or_else(|| StrataError::ExecutionAborted("no SupportDesk service".into()))
}

#[derive(Deserialize, StrataSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrderArgs {
    order_id: String,
}

#[strata_tool(
    name = "orders.lookup",
    description = "Looks an order up by its id: customer, total, status, days since delivery."
)]
async fn orders_lookup(args: OrderArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    let desk = desk(&ctx)?;
    desk.note(format!("orders.lookup({})", args.order_id));
    // An unknown order is an answer, not an error: the model should hear
    // `null`, "no such order".
    let order = desk.orders.get(args.order_id.as_str());
    Ok(serde_json::to_string(&order).expect("an order serializes"))
}

#[strata_tool(
    name = "shipping.track",
    description = "Where an order's parcel is now."
)]
async fn shipping_track(args: OrderArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    let desk = desk(&ctx)?;
    desk.note(format!("shipping.track({})", args.order_id));
    Ok(match desk.orders.get(args.order_id.as_str()) {
        Some(order) if order.status == "in transit" => {
            "in transit: left the Rotterdam hub, no delivery date yet".to_string()
        }
        _ => "delivered".to_string(),
    })
}

#[derive(Deserialize, StrataSchema)]
#[serde(rename_all = "camelCase")]
pub struct RefundArgs {
    order_id: String,
    amount: f64,
    reason: String,
}

/// `refunds.issue`, written by hand rather than with `#[strata_tool]`.
///
/// A `#[strata_tool]` function's own error is dropped by the generated code:
/// the registry sees `ToolExecutionFailure`, and the agent can only say `tool
/// 'refunds.issue' failed: the tool returned an error`. A tool whose failure
/// has to say why returns `StrataError::ToolCallFailed { tool, message }`, and
/// for that it implements `StrataTool` itself, reusing the derived schema of
/// its arguments.
pub struct RefundsIssueTool;

impl StrataTool for RefundsIssueTool {
    fn name(&self) -> &'static str {
        "refunds.issue"
    }

    fn schema(&self) -> serde_json::Value {
        serde_json::json!({
            "name": "refunds.issue",
            "description": "Refunds an amount of an order to the customer's card. Returns the refund id.",
            "parameters": RefundArgs::json_schema(),
        })
    }

    fn execute<'a>(&'a self, raw_args: &'a str) -> BoxFuture<'a, Result<String, StrataError>> {
        self.execute_with(raw_args, ToolCallContext::default())
    }

    fn execute_with<'a>(
        &'a self,
        raw_args: &'a str,
        ctx: ToolCallContext,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            // Arguments that don't fit are the caller's mistake: the agent
            // refuses `ToolArgumentParseFailure` back to the model.
            let args: RefundArgs = serde_json::from_str(raw_args)
                .map_err(|e| StrataError::ToolArgumentParseFailure(e.to_string()))?;
            let desk = desk(&ctx)?;
            desk.note(format!(
                "refunds.issue({}, {}, {})",
                args.order_id, args.amount, args.reason
            ));
            if let Some(message) = &desk.gateway_failure {
                return Err(StrataError::ToolCallFailed {
                    tool: self.name().to_string(),
                    message: message.clone(),
                });
            }
            let mut ledger = desk.ledger.lock().unwrap();
            let id = format!("RF-{:04}", ledger.refunds.len() + 1);
            ledger
                .refunds
                .push((id.clone(), args.order_id, args.amount));
            Ok(id)
        })
    }
}

#[derive(Deserialize, StrataSchema)]
#[serde(rename_all = "camelCase")]
pub struct CustomerArgs {
    customer_id: String,
}

#[strata_tool(
    name = "accounts.close",
    description = "Closes a customer's account. Not for the support agent."
)]
async fn accounts_close(args: CustomerArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    desk(&ctx)?.note(format!("accounts.close({})", args.customer_id));
    Ok("closed".to_string())
}
