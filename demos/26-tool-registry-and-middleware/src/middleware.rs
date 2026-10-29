//! The tool middleware: an audit log, a refund policy, an argument rewrite and
//! a read cache, plus the `ChainTrace` they write the chain each call took into.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;
use datalium_strata::StrataError;
use datalium_strata::tools::{Next, ToolCall, ToolMiddleware};
use serde_json::Value;

type Outcome = Result<String, StrataError>;

/// The order middleware is entered in, shared by every middleware below (and
/// the backend), so the demo can print the chain one call took.
#[derive(Clone, Default)]
pub struct ChainTrace(Arc<Mutex<Vec<String>>>);

impl ChainTrace {
    pub fn enter(&self, step: &str) {
        self.0.lock().unwrap().push(step.to_string());
    }

    pub fn drain(&self) -> String {
        let steps: Vec<String> = self.0.lock().unwrap().drain(..).collect();
        steps.join(" -> ")
    }
}

/// Registered first, so it is outermost: it sees every call, including the
/// ones a middleware inside it denies or answers from a cache, and, through
/// [`ToolMiddleware::on_refused`], the ones the registry refuses before any
/// middleware runs.
pub struct AuditLog {
    trace: ChainTrace,
    lines: Mutex<Vec<String>>,
}

impl AuditLog {
    pub fn new(trace: ChainTrace) -> Arc<Self> {
        Arc::new(Self {
            trace,
            lines: Mutex::default(),
        })
    }

    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    fn write(&self, call: &ToolCall, outcome: &str) {
        let ctx = call.context();
        self.lines.lock().unwrap().push(format!(
            "{} {} {}: {outcome}",
            ctx.caller_id().unwrap_or("-"),
            ctx.call_id().unwrap_or("-"),
            call.name()
        ));
    }
}

impl ToolMiddleware for AuditLog {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            self.trace.enter("audit");
            // `next.run` takes the call, so keep a copy to write the line from.
            let result = next.run(call.clone()).await;
            let outcome = match &result {
                Ok(_) => "ok".to_string(),
                Err(StrataError::ToolCallDenied { reason, .. }) => format!("denied ({reason})"),
                Err(other) => format!("failed ({other:?})"),
            };
            self.write(&call, &outcome);
            result
        })
    }

    fn on_refused<'a>(
        &'a self,
        call: &'a ToolCall,
        refusal: &'a StrataError,
    ) -> BoxFuture<'a, Result<(), StrataError>> {
        let outcome = match refusal {
            StrataError::ToolNotPermitted(_) => "refused (not permitted)",
            _ => "refused (no such tool)",
        };
        self.write(call, outcome);
        Box::pin(async { Ok(()) })
    }
}

/// A refund limit per tenant. The tenant comes from the run's metadata, which
/// reached the call through `ToolCallContext::from(&node_ctx)`; the tool
/// itself knows nothing about it.
pub struct RefundPolicy {
    trace: ChainTrace,
    limits: HashMap<String, f64>,
}

impl RefundPolicy {
    pub fn new(trace: ChainTrace, limits: impl IntoIterator<Item = (&'static str, f64)>) -> Self {
        Self {
            trace,
            limits: limits
                .into_iter()
                .map(|(tenant, limit)| (tenant.to_string(), limit))
                .collect(),
        }
    }
}

fn denied(call: &ToolCall, reason: String) -> StrataError {
    StrataError::ToolCallDenied {
        tool: call.name().to_string(),
        reason,
    }
}

impl ToolMiddleware for RefundPolicy {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            self.trace.enter("policy");
            if call.name() != "billing.refund" {
                return next.run(call).await;
            }
            let tenant = call.context().metadata().get("tenant").cloned();
            let Some((tenant, limit)) =
                tenant.and_then(|t| self.limits.get(&t).map(|&limit| (t, limit)))
            else {
                return Err(denied(
                    &call,
                    "the call carries no known tenant".to_string(),
                ));
            };
            let amount = serde_json::from_str::<Value>(call.arguments())
                .ok()
                .and_then(|args| args["amount"].as_f64());
            match amount {
                Some(amount) if amount <= limit => next.run(call).await,
                Some(_) => Err(denied(
                    &call,
                    format!("tenant {tenant} may refund at most {limit:.2} without a human"),
                )),
                None => Err(denied(&call, "the refund names no amount".to_string())),
            }
        })
    }
}

/// Rewrites the arguments: trims and upper-cases a `sku`, so `" ab-12 "` and
/// `"AB-12"` are the same call.
pub struct NormalizeSku {
    trace: ChainTrace,
}

impl NormalizeSku {
    pub fn new(trace: ChainTrace) -> Self {
        Self { trace }
    }
}

impl ToolMiddleware for NormalizeSku {
    fn invoke<'a>(&'a self, mut call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        self.trace.enter("normalize");
        if let Ok(mut args) = serde_json::from_str::<Value>(call.arguments())
            && let Some(sku) = args["sku"].as_str()
        {
            args["sku"] = Value::String(sku.trim().to_uppercase());
            call = call.with_arguments(args.to_string());
        }
        next.run(call)
    }
}

/// Answers repeated read-only calls without running the tool (a
/// short-circuit, which the registry reports as succeeded). Registered after
/// [`NormalizeSku`], so it sees normalized arguments, and keyed by tenant, so
/// one tenant's answer is never served to another.
pub struct ReadCache {
    trace: ChainTrace,
    cacheable: Vec<&'static str>,
    results: Mutex<HashMap<String, String>>,
}

impl ReadCache {
    pub fn new(trace: ChainTrace, cacheable: &[&'static str]) -> Self {
        Self {
            trace,
            cacheable: cacheable.to_vec(),
            results: Mutex::default(),
        }
    }
}

/// The arguments re-serialized: serde_json orders an object's keys, so equal
/// arguments make equal keys.
fn canonical(json: &str) -> String {
    serde_json::from_str::<Value>(json).map_or_else(|_| json.to_string(), |v| v.to_string())
}

impl ToolMiddleware for ReadCache {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            if !self.cacheable.contains(&call.name()) {
                self.trace.enter("cache (skip)");
                return next.run(call).await;
            }
            let tenant = call.context().metadata().get("tenant").cloned();
            let key = format!(
                "{}|{}|{}",
                tenant.unwrap_or_default(),
                call.name(),
                canonical(call.arguments())
            );
            let cached = self.results.lock().unwrap().get(&key).cloned();
            if let Some(cached) = cached {
                self.trace.enter("cache (hit)");
                return Ok(cached);
            }
            self.trace.enter("cache (miss)");
            let result = next.run(call).await?;
            self.results.lock().unwrap().insert(key, result.clone());
            Ok(result)
        })
    }
}
