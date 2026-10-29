//! Subgraph composition (`design/15-Subgraph-Composition.md`).
//!
//! A support desk. A supervisor node (`Triage`) reads a customer ticket and
//! routes it -- with ordinary conditional edges -- to one of two specialist
//! agents. Each agent is a *compiled graph of its own*, with its own state
//! type, added to the desk graph as a single node with `.add_subgraph(...)`:
//!
//! - `BillingAgent` -- `BillingCase` state; looks up the invoice, refunds a
//!   duplicate charge (a routing decision *inside* the child), drafts a reply.
//! - `TechAgent` -- `TechCase` state; runs diagnostics, then a guarded
//!   apply-fix loop until the problem is gone. The diagnostics are themselves a
//!   subgraph (`Probe` state), so the tech agent nests one level deeper.
//!
//! The desk's `Ticket` knows nothing about `BillingCase`, `TechCase` or `Probe`.
//! The only interface between a graph and its child is two mapper closures: one
//! builds the child's starting state from the parent's, the other folds the
//! child's finished state back in.
//!
//! What the scenarios show:
//!
//! 1. **Composition** -- a billing ticket, routed to a child graph with its own
//!    state and its own conditional edge.
//! 2. **Nesting** -- a tech ticket, whose agent loops internally and runs a
//!    subgraph of its own.
//! 3. **A child recovers itself** -- a flaky API is retried by the child's own
//!    recovery policy; the desk never sees the failure.
//! 4. **The parent's policy applies to the whole child** -- when the child's
//!    retries run out on a transient error, the desk's `retry` re-runs the
//!    *entire* child, and its `fallback` routes to `Escalate` on the state from
//!    *before* the agent ran.
//! 5. **Failures keep their kind, their path and their place** -- a fatal error
//!    two levels down is not retried at any level, and the error names every
//!    level. With no fallback, the desk persists a `Failed` checkpoint of its
//!    own state that nests each child's own `Failed` checkpoint, so once the
//!    cause is fixed a resume re-enters the innermost child at the node that
//!    failed.
//! 6. **A human in the loop, inside a child** (`design/16-Nested-Interrupts.md`,
//!    `design/21-Typed-Human-Signals.md`) -- a refund needs approval, so
//!    `BillingAgent` has an interrupt boundary of its own. It freezes the
//!    *whole* run: the desk's checkpoint nests the agent's, and a later
//!    `resume` re-enters the agent exactly where it stopped. The human's typed
//!    override goes to the agent's own state, and the recorded decision belongs
//!    to the agent's level: the desk's nodes don't see it.
//! 7. **The desk's exported definition carries each child's whole graph**
//!    (`design/22-Graph-Structure-In-Definitions.md`).
//!
//! No live service and no LLM -- the "agents" are deterministic stand-ins, so
//! the demo runs offline and prints the same thing every time.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::schema::{
    CheckpointStatus, GraphDefinition, GraphNodeKind, HumanDecision,
    NESTED_CHECKPOINT_SCHEMA_VERSION,
};
use datalium_strata::serde_json::{Value, json};
use datalium_strata::store::CheckpointStore;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

// --- The desk's state, and the children's ----------------------------------

/// The parent graph's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ticket {
    message: String,
    category: Option<String>,
    reply: Option<String>,
    log: Vec<String>,
}

impl Ticket {
    fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
            category: None,
            reply: None,
            log: Vec::new(),
        }
    }
}

/// `BillingAgent`'s state. `credit_cents` is a goodwill credit; only a
/// reviewer's override sets it (scenario 6).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BillingCase {
    charge_cents: i64,
    duplicate: bool,
    reply: String,
    steps: Vec<String>,
    credit_cents: i64,
}

/// `TechAgent`'s state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TechCase {
    attempts: u32,
    fixed: bool,
    reply: String,
    steps: Vec<String>,
}

/// The diagnostics subgraph's state, nested inside `TechAgent`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Probe {
    findings: Vec<String>,
}

fn dollars(cents: i64) -> String {
    format!("${}.{:02}", cents / 100, cents % 100)
}

// --- Simulated back ends, and what the demo counts --------------------------

/// The billing system. Times out for its first `fail_first` calls.
struct BillingApi {
    fail_first: u32,
    calls: AtomicU32,
}

impl BillingApi {
    fn new(fail_first: u32) -> Arc<Self> {
        Arc::new(Self {
            fail_first,
            calls: AtomicU32::new(0),
        })
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    /// The customer's latest invoice: `(amount in cents, charged twice?)`.
    fn latest_invoice(&self) -> Result<(i64, bool), NodeError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call <= self.fail_first {
            println!("    billing API call {call}: timed out");
            return Err(NodeError::Transient("billing API timed out".to_string()));
        }
        println!("    billing API call {call}: ok");
        Ok((4999, true))
    }
}

#[derive(Clone, Copy)]
enum Device {
    Healthy,
    /// Its firmware is too old to diagnose remotely -- retrying can't help.
    OldFirmware,
}

/// What each recording node saw in `NodeContext::decision()`, by node path, in
/// the order they ran.
#[derive(Default)]
struct DecisionLog {
    seen: Mutex<Vec<(String, Option<HumanDecision>)>>,
}

impl DecisionLog {
    fn record(&self, ctx: &NodeContext) {
        self.seen
            .lock()
            .unwrap()
            .push((ctx.node_path().to_string(), ctx.decision().cloned()));
    }

    fn seen(&self) -> Vec<(String, Option<HumanDecision>)> {
        self.seen.lock().unwrap().clone()
    }
}

/// How the desk reacts when an agent fails.
struct DeskPolicy {
    retries: u32,
    fallback: bool,
}

const NO_RETRIES: DeskPolicy = DeskPolicy {
    retries: 0,
    fallback: false,
};

// --- The children -----------------------------------------------------------

/// `LookupInvoice` -> (`IssueRefund` if it was a duplicate) -> `DraftReply`.
///
/// With `needs_approval` (scenario 6) a human has to sign off first:
/// `LookupInvoice` -> (`AwaitApproval` -> `IssueRefund` if it was a duplicate)
/// -> `DraftReply`, where `AwaitApproval` is an interrupt boundary.
fn billing_agent(
    api: &Arc<BillingApi>,
    own_retries: u32,
    needs_approval: bool,
    decisions: Option<Arc<DecisionLog>>,
) -> CompiledGraph<BillingCase> {
    let api = api.clone();
    let mut graph = StateGraph::<BillingCase>::new()
        .add_fallible_node("LookupInvoice", move |case: &BillingCase| {
            let (charge_cents, duplicate) = api.latest_invoice()?;
            let mut next = case.clone();
            next.charge_cents = charge_cents;
            next.duplicate = duplicate;
            next.steps.push(format!(
                "found invoice for {}{}",
                dollars(charge_cents),
                if duplicate { " (charged twice)" } else { "" }
            ));
            Ok(next)
        })
        .set_recovery_policy("LookupInvoice", RecoveryPolicy::retry(own_retries))
        // An async node, because it reads its `NodeContext`: after an approval,
        // the agent's level has a decision -- who approved, at which boundary.
        .add_async_node(
            "IssueRefund",
            move |mut case: BillingCase, ctx: NodeContext| {
                if let Some(decisions) = &decisions {
                    decisions.record(&ctx);
                }
                let credit = if case.credit_cents > 0 {
                    format!(" and credited {}", dollars(case.credit_cents))
                } else {
                    String::new()
                };
                let approved_by = ctx
                    .decision()
                    .map(|decision| format!(" (approved by {})", decision.reviewer))
                    .unwrap_or_default();
                case.steps.push(format!(
                    "refunded {}{credit}{approved_by}",
                    dollars(case.charge_cents)
                ));
                async move { case }
            },
        )
        .add_node("DraftReply", |mut case: BillingCase| {
            case.reply = if case.duplicate {
                let mut reply = format!(
                    "We charged you twice and have refunded {}.",
                    dollars(case.charge_cents)
                );
                if case.credit_cents > 0 {
                    reply += &format!(
                        " We have also added a {} credit.",
                        dollars(case.credit_cents)
                    );
                }
                reply
            } else {
                "Your invoice is correct as issued.".to_string()
            };
            case.steps.push("drafted reply".to_string());
            case
        });

    // The refund step is `AwaitApproval` when a human has to sign off first. It
    // gets an ordinary edge to `IssueRefund`; the interrupt boundary is what
    // stops the run there, once the node has finished.
    let refund_step = if needs_approval {
        graph = graph
            .add_node("AwaitApproval", |mut case: BillingCase| {
                case.steps.push(format!(
                    "refund of {} awaits approval",
                    dollars(case.charge_cents)
                ));
                case
            })
            .add_interrupt_boundary("AwaitApproval")
            .add_edge("AwaitApproval", "IssueRefund");
        "AwaitApproval"
    } else {
        "IssueRefund"
    };

    graph
        // A routing decision inside the child: refund only a duplicate charge.
        .add_conditional_edge("LookupInvoice", refund_step, |case: &BillingCase| {
            case.duplicate
        })
        .add_edge("LookupInvoice", "DraftReply")
        .add_edge("IssueRefund", "DraftReply")
        .compile()
        .expect("billing agent is structurally valid")
}

/// `CheckNetwork` -> `CheckFirmware` -> `Summarize`, over its own `Probe` state.
fn diagnostics(device: Device, network_checks: Option<Arc<AtomicU32>>) -> CompiledGraph<Probe> {
    StateGraph::<Probe>::new()
        .add_node("CheckNetwork", move |mut probe: Probe| {
            if let Some(count) = &network_checks {
                count.fetch_add(1, Ordering::SeqCst);
            }
            probe.findings.push("network ok".to_string());
            probe
        })
        .add_fallible_node("CheckFirmware", move |probe: &Probe| match device {
            Device::Healthy => {
                let mut next = probe.clone();
                next.findings.push("firmware 2.1 ok".to_string());
                Ok(next)
            }
            Device::OldFirmware => Err(NodeError::Fatal(
                "firmware 1.0 is too old to diagnose remotely".to_string(),
            )),
        })
        .add_node("Summarize", |mut probe: Probe| {
            let checks = probe.findings.len();
            probe.findings.push(format!("{checks} checks passed"));
            probe
        })
        .add_edge("CheckNetwork", "CheckFirmware")
        .add_edge("CheckFirmware", "Summarize")
        .compile()
        .expect("diagnostics graph is structurally valid")
}

/// `Diagnose` (a subgraph) -> `ApplyFix` (loops until fixed, at most 3 times)
/// -> `DraftReply`. The loop lives entirely inside the child: the desk never
/// sees it.
fn tech_agent(device: Device, network_checks: Option<Arc<AtomicU32>>) -> CompiledGraph<TechCase> {
    StateGraph::<TechCase>::new()
        .add_subgraph(
            "Diagnose",
            diagnostics(device, network_checks),
            |_: &TechCase| Probe::default(),
            |case: &TechCase, probe: Probe| {
                let mut next = case.clone();
                next.steps.extend(
                    probe
                        .findings
                        .into_iter()
                        .map(|finding| format!("diagnostics: {finding}")),
                );
                next
            },
        )
        .add_node("ApplyFix", |mut case: TechCase| {
            case.attempts += 1;
            // The simulated device only responds to the second attempt.
            case.fixed = case.attempts >= 2;
            case.steps.push(format!(
                "fix attempt {}: {}",
                case.attempts,
                if case.fixed { "resolved" } else { "no change" }
            ));
            case
        })
        .add_node("DraftReply", |mut case: TechCase| {
            case.reply = format!("Fixed after {} attempt(s).", case.attempts);
            case.steps.push("drafted reply".to_string());
            case
        })
        .add_conditional_edge("ApplyFix", "ApplyFix", |case: &TechCase| {
            !case.fixed && case.attempts < 3
        })
        .add_edge("ApplyFix", "DraftReply")
        .add_edge("Diagnose", "ApplyFix")
        .compile()
        .expect("tech agent is structurally valid")
}

// --- The desk -----------------------------------------------------------------

/// `Triage` -> `BillingAgent` | `TechAgent` | `GeneralReply` -> `SendReply`.
///
/// Both agents share one recovery policy. When it names a fallback,
/// `Escalate` is added as that node -- it gets no ordinary edge in, so the
/// recovery policy is the only way there.
fn support_desk(
    billing: CompiledGraph<BillingCase>,
    tech: CompiledGraph<TechCase>,
    policy: &DeskPolicy,
    decisions: Option<Arc<DecisionLog>>,
) -> CompiledGraph<Ticket> {
    let mut graph = StateGraph::<Ticket>::new()
        // The supervisor: classify the ticket. (Keyword matching stands in for
        // an LLM call.)
        .add_node("Triage", |mut ticket: Ticket| {
            let text = ticket.message.to_lowercase();
            let category = if ["charge", "invoice", "refund"]
                .iter()
                .any(|word| text.contains(word))
            {
                "billing"
            } else if ["router", "wifi", "crash"]
                .iter()
                .any(|word| text.contains(word))
            {
                "tech"
            } else {
                "general"
            };
            ticket.log.push(format!("triage: {category}"));
            ticket.category = Some(category.to_string());
            ticket
        })
        .add_subgraph(
            "BillingAgent",
            billing,
            |_: &Ticket| BillingCase::default(),
            |ticket: &Ticket, case: BillingCase| {
                let mut next = ticket.clone();
                next.reply = Some(case.reply);
                next.log.extend(
                    case.steps
                        .into_iter()
                        .map(|step| format!("billing: {step}")),
                );
                next
            },
        )
        .add_subgraph(
            "TechAgent",
            tech,
            |_: &Ticket| TechCase::default(),
            |ticket: &Ticket, case: TechCase| {
                let mut next = ticket.clone();
                next.reply = Some(case.reply);
                next.log
                    .extend(case.steps.into_iter().map(|step| format!("tech: {step}")));
                next
            },
        )
        .add_node("GeneralReply", |mut ticket: Ticket| {
            ticket.reply = Some("Thanks for getting in touch.".to_string());
            ticket
        })
        .add_async_node("SendReply", move |mut ticket: Ticket, ctx: NodeContext| {
            if let Some(decisions) = &decisions {
                decisions.record(&ctx);
            }
            ticket.log.push("reply sent".to_string());
            async move { ticket }
        })
        .add_conditional_edge("Triage", "BillingAgent", |ticket: &Ticket| {
            ticket.category.as_deref() == Some("billing")
        })
        .add_conditional_edge("Triage", "TechAgent", |ticket: &Ticket| {
            ticket.category.as_deref() == Some("tech")
        })
        .add_edge("Triage", "GeneralReply")
        .add_edge("BillingAgent", "SendReply")
        .add_edge("TechAgent", "SendReply")
        .add_edge("GeneralReply", "SendReply");

    let mut recovery = RecoveryPolicy::retry(policy.retries);
    if policy.fallback {
        graph = graph
            .add_node("Escalate", |mut ticket: Ticket| {
                ticket.reply = Some("A human agent will contact you shortly.".to_string());
                ticket.log.push("escalated to a human".to_string());
                ticket
            })
            .add_edge("Escalate", "SendReply");
        recovery = recovery.with_fallback("Escalate");
    }
    graph
        .set_recovery_policy("BillingAgent", recovery.clone())
        .set_recovery_policy("TechAgent", recovery)
        .compile()
        .expect("support desk is structurally valid")
}

// --- The scenarios --------------------------------------------------------------

#[tokio::main]
async fn main() {
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    // --- Scenario 1: a child graph with its own state and routing. ---

    println!("=== Scenario 1: a billing ticket -- BillingAgent is a subgraph ===");
    let desk = support_desk(
        billing_agent(&BillingApi::new(0), 0, false, None),
        tech_agent(Device::Healthy, None),
        &NO_RETRIES,
        None,
    );
    let ticket = completed(
        desk.run(
            "ticket-1",
            Ticket::new("I was charged twice for invoice 1042"),
            store.clone(),
        )
        .await,
    );
    print_ticket(&ticket);
    println!(
        "  The desk's Ticket has no invoice or refund fields -- the agent's own BillingCase did the work, and the output mapper folded its steps back in."
    );
    check(
        ticket.log
            == [
                "triage: billing",
                "billing: found invoice for $49.99 (charged twice)",
                "billing: refunded $49.99",
                "billing: drafted reply",
                "reply sent",
            ],
        "expected the billing agent's steps folded into the desk's log",
    );
    check(
        ticket.reply.as_deref() == Some("We charged you twice and have refunded $49.99."),
        "expected the refund reply",
    );

    // --- Scenario 2: a child with a loop, and a grandchild. ---

    println!();
    println!("=== Scenario 2: a tech ticket -- TechAgent loops, and nests a subgraph ===");
    let desk = support_desk(
        billing_agent(&BillingApi::new(0), 0, false, None),
        tech_agent(Device::Healthy, None),
        &NO_RETRIES,
        None,
    );
    let ticket = completed(
        desk.run(
            "ticket-2",
            Ticket::new("My router keeps crashing"),
            store.clone(),
        )
        .await,
    );
    print_ticket(&ticket);
    println!(
        "  Supervisor -> TechAgent -> Diagnose: three graphs deep, and the loop inside TechAgent is invisible to the desk."
    );
    check(
        ticket.log == fixed_tech_log(),
        "expected the nested diagnostics and the fix loop in the log",
    );

    // --- Scenario 3: the child recovers itself. ---

    println!();
    println!("=== Scenario 3: the billing API times out twice -- the child's own retry(2) ===");
    let api = BillingApi::new(2);
    let desk = support_desk(
        billing_agent(&api, 2, false, None),
        tech_agent(Device::Healthy, None),
        &NO_RETRIES,
        None,
    );
    let ticket = completed(
        desk.run(
            "ticket-3",
            Ticket::new("Please refund my invoice"),
            store.clone(),
        )
        .await,
    );
    println!(
        "  Completed after {} billing API calls. The desk's policy (retry 0) never fired: the child handled it.",
        api.calls()
    );
    println!("  Reply: {}", ticket.reply.as_deref().unwrap_or("(none)"));
    check(
        api.calls() == 3,
        "expected the child to retry twice (3 calls)",
    );
    check(
        store.load("ticket-3").await.unwrap().is_none(),
        "expected no checkpoint: nothing failed from the desk's point of view",
    );
    check(
        !ticket.log.iter().any(|line| line.contains("timed out")),
        "expected the timeouts to stay inside the child",
    );

    // --- Scenario 4: the parent's policy applies to the whole child. ---

    println!();
    println!(
        "=== Scenario 4: the billing API is down -- child retry(1), desk retry(1) + fallback 'Escalate' ==="
    );
    let api = BillingApi::new(u32::MAX);
    let desk = support_desk(
        billing_agent(&api, 1, false, None),
        tech_agent(Device::Healthy, None),
        &DeskPolicy {
            retries: 1,
            fallback: true,
        },
        None,
    );
    let ticket = completed(
        desk.run(
            "ticket-4",
            Ticket::new("Where is my refund?"),
            store.clone(),
        )
        .await,
    );
    println!(
        "  {} billing API calls: the child makes 2 per run (1 + its retry), and the desk's retry re-ran the whole child once.",
        api.calls()
    );
    print_ticket(&ticket);
    println!(
        "  Escalate ran on the ticket from before BillingAgent started, so none of the child's steps are in the log."
    );
    check(api.calls() == 4, "expected 2 child runs of 2 calls each");
    check(
        ticket.log == ["triage: billing", "escalated to a human", "reply sent"],
        "expected the fallback to run on the pre-agent state",
    );

    // --- Scenario 5: a fatal error two levels down. ---

    println!();
    println!(
        "=== Scenario 5: the device firmware is too old -- fatal in Diagnose, desk retry(3), no fallback ==="
    );
    let retry_three = DeskPolicy {
        retries: 3,
        fallback: false,
    };
    let desk = support_desk(
        billing_agent(&BillingApi::new(0), 0, false, None),
        tech_agent(Device::OldFirmware, None),
        &retry_three,
        None,
    );
    match desk
        .run("ticket-5", Ticket::new("My wifi is down"), store.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => {
            println!("  Failed: '{node}' gave up after {attempts} attempt(s):");
            println!("    {message}");
            println!(
                "  The error names every level, and the fatal kind survived both hand-offs, so none of the 3 retries was used."
            );
            check(
                node == "TechAgent" && attempts == 1,
                "expected a single attempt at TechAgent",
            );
            check(
                message
                    == "subgraph 'TechAgent' failed at 'Diagnose' after 1 attempt(s): \
                        subgraph 'Diagnose' failed at 'CheckFirmware' after 1 attempt(s): \
                        firmware 1.0 is too old to diagnose remotely",
                "expected the failure message to name every level",
            );
        }
        other => fail(&format!(
            "expected Failed(NodeFailed), got {}",
            describe(&other)
        )),
    }

    let checkpoint = store
        .load("ticket-5")
        .await
        .unwrap()
        .unwrap_or_else(|| fail("expected a Failed checkpoint"));
    let error_kind = checkpoint
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("error_kind"))
        .cloned()
        .unwrap_or_default();
    println!(
        "  Persisted checkpoint: {:?} at '{}', error_kind = {error_kind}.",
        checkpoint.status,
        checkpoint.suspended_node.as_deref().unwrap_or("?"),
    );
    println!(
        "  Its frozen state is the desk's Ticket from before TechAgent ran: log = {}",
        checkpoint.frozen_state["log"]
    );
    check(
        checkpoint.status == CheckpointStatus::Failed,
        "expected a Failed checkpoint",
    );
    check(error_kind == "fatal", "expected the fatal kind to survive");
    check(
        checkpoint.frozen_state["log"] == json!(["triage: tech"]),
        "expected the pre-agent ticket in the checkpoint",
    );

    // Each failed subgraph node nests its child's own Failed checkpoint, down
    // to the node that failed.
    println!(
        "  It nests each child's own Failed checkpoint (schema_version {}):",
        checkpoint.schema_version
    );
    let mut levels = Vec::new();
    let mut level = checkpoint.nested.as_deref();
    while let Some(nested) = level {
        println!(
            "    thread '{}': {:?} at '{}', state {}",
            nested.thread_id,
            nested.status,
            nested.suspended_node.as_deref().unwrap_or("?"),
            nested.frozen_state
        );
        levels.push((
            nested.thread_id.clone(),
            nested.status,
            nested.suspended_node.clone().unwrap_or_default(),
            nested.frozen_state.clone(),
        ));
        level = nested.nested.as_deref();
    }
    check(
        checkpoint.schema_version == NESTED_CHECKPOINT_SCHEMA_VERSION,
        "expected a nested checkpoint to be schema 1.1.0",
    );
    check(
        levels
            == [
                (
                    "TechAgent".to_string(),
                    CheckpointStatus::Failed,
                    "Diagnose".to_string(),
                    json!({"attempts": 0, "fixed": false, "reply": "", "steps": []}),
                ),
                (
                    "Diagnose".to_string(),
                    CheckpointStatus::Failed,
                    "CheckFirmware".to_string(),
                    json!({"findings": ["network ok"]}),
                ),
            ],
        "expected TechAgent's and Diagnose's own Failed checkpoints, nested",
    );

    let failed_at = checkpoint.waiting_at();
    println!("  Failed at: {}", failed_at.join("/"));
    check(
        failed_at == ["TechAgent", "Diagnose", "CheckFirmware"],
        "expected the failure to be recorded at the innermost node",
    );

    // Later -- the device's firmware has been updated. A new desk (and a new
    // counter of network checks) resumes the Failed thread.
    println!("  ... the device's firmware is updated, and the ticket is resumed ...");
    let network_checks = Arc::new(AtomicU32::new(0));
    let desk = support_desk(
        billing_agent(&BillingApi::new(0), 0, false, None),
        tech_agent(Device::Healthy, Some(network_checks.clone())),
        &retry_three,
        None,
    );
    let ticket = completed(
        desk.resume(
            "ticket-5",
            HumanSignal::approve("ops@support.example"),
            store.clone(),
        )
        .await,
    );
    print_ticket(&ticket);
    let checks = network_checks.load(Ordering::SeqCst);
    println!(
        "  {checks} network checks after the resume: it re-entered Diagnose at CheckFirmware, with the finding it had kept."
    );
    check(
        checks == 0,
        "expected the resume to skip the checks that had already passed",
    );
    check(
        ticket.log == fixed_tech_log(),
        "expected the resumed run to finish the tech ticket",
    );

    // --- Scenario 6: a human's approval, inside a child. ---

    println!();
    println!(
        "=== Scenario 6: a refund needs a human's approval -- an interrupt boundary inside BillingAgent ==="
    );
    let api = BillingApi::new(0);
    let desk = support_desk(
        billing_agent(&api, 0, true, None),
        tech_agent(Device::Healthy, None),
        &NO_RETRIES,
        None,
    );
    match desk
        .run(
            "ticket-6",
            Ticket::new("I was charged twice for invoice 1042"),
            store.clone(),
        )
        .await
    {
        GraphExecutionResult::Interrupted {
            suspended_node,
            next_node,
            frozen_state,
            ..
        } => {
            println!(
                "  The whole run froze: suspended at '{suspended_node}', and a resume will re-enter '{next_node}'."
            );
            println!(
                "  The desk's frozen Ticket is the one from before BillingAgent started: log = {}",
                json!(frozen_state.log)
            );
            check(
                suspended_node == "BillingAgent" && next_node == "BillingAgent",
                "expected the run to freeze at the subgraph node",
            );
            check(
                frozen_state.log == ["triage: billing"],
                "expected the pre-agent ticket to be frozen",
            );
        }
        other => fail(&format!("expected Interrupted, got {}", describe(&other))),
    }
    check(
        api.calls() == 1,
        "expected one billing API call before the freeze",
    );

    // What was persisted: the desk's checkpoint, with the agent's own inside it.
    let frozen = store
        .load("ticket-6")
        .await
        .unwrap()
        .unwrap_or_else(|| fail("expected the desk to persist a checkpoint"));
    let child = frozen
        .nested
        .as_deref()
        .unwrap_or_else(|| fail("expected the agent's checkpoint nested in the desk's"));
    println!(
        "  Persisted: schema_version {}, with the agent's own checkpoint nested inside:",
        frozen.schema_version
    );
    println!(
        "    thread '{}' waiting at '{}', its steps so far = {}",
        child.thread_id,
        child.suspended_node.as_deref().unwrap_or("?"),
        child.frozen_state["steps"]
    );
    check(
        frozen.schema_version == NESTED_CHECKPOINT_SCHEMA_VERSION,
        "expected a nested checkpoint to be schema 1.1.0",
    );
    check(
        child.thread_id == "BillingAgent"
            && child.suspended_node.as_deref() == Some("AwaitApproval"),
        "expected the agent to wait at AwaitApproval",
    );
    check(
        child.frozen_state["steps"]
            == json!([
                "found invoice for $49.99 (charged twice)",
                "refund of $49.99 awaits approval"
            ]),
        "expected the agent's steps so far in its own checkpoint",
    );

    // Later -- a different process, so a new desk and a new billing API whose
    // call counter starts at zero: any call the agent made to it would show.
    println!("  ... a reviewer approves, and adds a $5.00 goodwill credit ...");
    let later_api = BillingApi::new(0);
    let decisions = Arc::new(DecisionLog::default());
    let desk = support_desk(
        billing_agent(&later_api, 0, true, Some(decisions.clone())),
        tech_agent(Device::Healthy, None),
        &NO_RETRIES,
        Some(decisions.clone()),
    );
    // A typed override: `credit_cents` is a number in `BillingCase`, the agent's
    // state. The desk's `Ticket` has no such field. A bare `HumanSignal` is
    // `LevelSignals::new(signal)`: it answers the innermost level that froze.
    let approval = HumanSignal::approve("dana@support.example").with_override("credit_cents", 500);
    let ticket = completed(desk.resume("ticket-6", approval, store.clone()).await);
    print_ticket(&ticket);
    println!(
        "  {} billing API calls after the resume: the agent picked up at IssueRefund, right after its boundary, instead of starting over.",
        later_api.calls()
    );
    println!(
        "  The override went to BillingAgent's own state ('credit_cents' is a BillingCase field) -- the desk's Ticket has no such field."
    );
    check(
        later_api.calls() == 0,
        "expected the resumed agent not to call the billing API again",
    );
    check(
        ticket.log
            == [
                "triage: billing",
                "billing: found invoice for $49.99 (charged twice)",
                "billing: refund of $49.99 awaits approval",
                "billing: refunded $49.99 and credited $5.00 (approved by dana@support.example)",
                "billing: drafted reply",
                "reply sent",
            ],
        "expected the agent to resume after its boundary with the approval applied",
    );
    check(
        ticket.reply.as_deref()
            == Some(
                "We charged you twice and have refunded $49.99. We have also added a $5.00 credit.",
            ),
        "expected the credit in the reply",
    );

    // The decision is recorded at the level whose boundary was answered: the
    // agent's.
    let seen = decisions.seen();
    for (path, decision) in &seen {
        let saw = match decision {
            Some(decision) => format!(
                "{}'s approval at '{}'",
                decision.reviewer, decision.boundary
            ),
            None => "no decision".to_string(),
        };
        println!("  {path} saw {saw}");
    }
    let summary: Vec<(&str, Option<&str>, Option<Value>)> = seen
        .iter()
        .map(|(path, decision)| {
            (
                path.as_str(),
                decision.as_ref().map(|d| d.reviewer.as_str()),
                decision.as_ref().map(|d| json!(d.overrides)),
            )
        })
        .collect();
    check(
        summary
            == [
                (
                    "BillingAgent/IssueRefund",
                    Some("dana@support.example"),
                    Some(json!({"credit_cents": 500})),
                ),
                ("SendReply", None, None),
            ],
        "expected the decision inside the agent and none at the desk",
    );
    println!("  The decision belongs to BillingAgent's level. The desk's own nodes don't see it.");

    // A frozen checkpoint is resumed once.
    match desk
        .resume("ticket-6", HumanSignal::approve("x"), store.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            println!("  Resuming the same checkpoint again is refused: {message}");
            check(
                message == "checkpoint for thread 'ticket-6' was already resumed",
                "expected the second resume to be refused",
            );
        }
        other => fail(&format!(
            "expected Failed(ExecutionAborted), got {}",
            describe(&other)
        )),
    }

    // --- Step 7: the parent's definition carries its children. ---

    println!();
    println!("=== Step 7: the desk's exported definition carries each child's whole graph ===");
    let definition = support_desk(
        billing_agent(&BillingApi::new(0), 0, true, None),
        tech_agent(Device::Healthy, None),
        &DeskPolicy {
            retries: 1,
            fallback: true,
        },
        None,
    )
    .to_definition();
    print_definition("SupportDesk", &definition, "  ");
    let tech = subgraph(&definition, "TechAgent");
    let diagnose = tech.and_then(|tech| subgraph(tech, "Diagnose"));
    let billing = subgraph(&definition, "BillingAgent");
    check(
        diagnose.map(node_names)
            == Some(vec![
                "CheckNetwork".to_string(),
                "CheckFirmware".to_string(),
                "Summarize".to_string(),
            ]),
        "expected the definition to nest the grandchild's graph",
    );
    check(
        billing.is_some_and(|billing| {
            billing.schema_version == "1.6.0"
                && billing
                    .nodes
                    .iter()
                    .any(|node| node.name == "AwaitApproval" && node.interrupt.is_some())
        }),
        "expected BillingAgent's level to export its boundary at schema_version 1.6.0",
    );
    check(
        definition.schema_version == "1.1.0"
            && tech.map(|tech| tech.schema_version.as_str()) == Some("1.1.0")
            && diagnose.map(|diagnose| diagnose.schema_version.as_str()) == Some("1.0.0"),
        "expected each level's own schema_version",
    );
    println!("  One definition describes all three levels, boundary included.");

    println!();
    println!(
        "=== Done: a supervisor routed to child graphs with their own state, failures kept their kind, their path and their place, the desk's policy governed each child as one node, and a human's approval froze the desk mid-agent and resumed inside it. ==="
    );
}

/// What a healthy tech ticket's log ends up as (scenarios 2 and 5).
fn fixed_tech_log() -> [&'static str; 8] {
    [
        "triage: tech",
        "tech: diagnostics: network ok",
        "tech: diagnostics: firmware 2.1 ok",
        "tech: diagnostics: 2 checks passed",
        "tech: fix attempt 1: no change",
        "tech: fix attempt 2: resolved",
        "tech: drafted reply",
        "reply sent",
    ]
}

fn print_ticket(ticket: &Ticket) {
    println!("  Reply: {}", ticket.reply.as_deref().unwrap_or("(none)"));
    println!("  Log:");
    for line in &ticket.log {
        println!("    - {line}");
    }
}

/// One line per level: its nodes, a subgraph's child indented under it.
fn print_definition(name: &str, definition: &GraphDefinition, indent: &str) {
    let nodes: Vec<String> = definition
        .nodes
        .iter()
        .map(|node| match node.interrupt {
            Some(_) => format!("{} [boundary]", node.name),
            None => node.name.clone(),
        })
        .collect();
    println!(
        "{indent}{name} (schema_version {}): {}",
        definition.schema_version,
        nodes.join(", ")
    );
    for node in &definition.nodes {
        if let (Some(GraphNodeKind::Subgraph), Some(child)) = (node.kind, node.graph.as_deref()) {
            print_definition(&node.name, child, &format!("{indent}  "));
        }
    }
}

/// The definition of the subgraph node `name`, if `definition` has one.
fn subgraph<'a>(definition: &'a GraphDefinition, name: &str) -> Option<&'a GraphDefinition> {
    definition
        .nodes
        .iter()
        .find(|node| node.name == name && node.kind == Some(GraphNodeKind::Subgraph))
        .and_then(|node| node.graph.as_deref())
}

fn node_names(definition: &GraphDefinition) -> Vec<String> {
    definition
        .nodes
        .iter()
        .map(|node| node.name.clone())
        .collect()
}

fn completed(result: GraphExecutionResult<Ticket>) -> Ticket {
    match result {
        GraphExecutionResult::Completed(ticket) => ticket,
        other => fail(&format!("expected Completed, got {}", describe(&other))),
    }
}

fn describe(result: &GraphExecutionResult<Ticket>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Interrupted { .. } => "Interrupted".to_string(),
        GraphExecutionResult::Failed(err) => format!("Failed({err:?})"),
        GraphExecutionResult::Cancelled { node, .. } => format!("Cancelled at '{node}'"),
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
