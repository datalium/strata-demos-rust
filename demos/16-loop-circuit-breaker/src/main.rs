//! The loop circuit breaker: STRATA-SPEC-004's max node hops
//! (`design/24-Loop-Circuit-Breaker.md`).
//!
//! A meeting-scheduling agent: `ProposeSlot` -> `CheckCalendars` -> back to
//! `ProposeSlot` while someone is busy, else `SendInvite`. The back edge is
//! guarded, so `.compile()` accepts the graph: validation is structural and
//! can't know whether the guard ever turns false. With an attendee who is on
//! leave all week it never does, and the agent re-plans forever. In a real
//! agent every pass is an LLM call.
//!
//! `.compile_with(CompileOptions::new().with_max_node_hops(12))` caps every
//! thread at 12 node starts:
//!
//! 1. A meeting that can be booked converges in 7 hops and never notices the limit.
//! 2. One that can't is refused before hop 13: `StrataError::HopLimitExceeded`,
//!    a `Failed` checkpoint with `error_kind` `hop_limit`, and the count in its
//!    metadata as `strata.hops`.
//! 3. A retry isn't a hop: a flaky calendar API doubles the handler calls, not the hops.
//! 4. The count survives interrupts: with an organizer reviewing every
//!    proposal, each freeze and resume (by a freshly built graph, as another
//!    process would) carries `strata.hops` forward, and the thread trips at
//!    exactly the hop a run without interrupts does.
//! 5. `RunConfig::with_max_node_hops` replaces the limit for one run: an
//!    operator fixes the attendee list and lets the tripped thread go on; a
//!    test runs the graph with a tighter limit.
//!
//! Everything is deterministic: the calendars are a `match` and the agent
//! proposes slots in order.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult, HumanSignal};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{
    CompileOptions, CompiledGraph, NodeError, RecoveryPolicy, StateGraph,
};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use serde_json::json;

const SLOTS: [&str; 5] = [
    "Mon 09:00",
    "Mon 14:00",
    "Tue 10:00",
    "Tue 15:00",
    "Wed 11:00",
];

/// The calendars: Ravi is busy on Monday, Dana is on leave all week.
fn busy_slots(attendee: &str) -> &'static [&'static str] {
    match attendee {
        "Ravi" => &["Mon 09:00", "Mon 14:00"],
        "Dana" => &SLOTS,
        _ => &[],
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Meeting {
    title: String,
    attendees: Vec<String>,
    proposals: usize,
    slot: Option<String>,
    conflicts: Vec<String>,
    tried: Vec<String>,
    invite: Option<String>,
}

impl Meeting {
    fn new(title: &str, attendees: &[&str]) -> Self {
        Self {
            title: title.to_string(),
            attendees: attendees.iter().map(|a| a.to_string()).collect(),
            proposals: 0,
            slot: None,
            conflicts: Vec::new(),
            tried: Vec::new(),
            invite: None,
        }
    }
}

#[tokio::main]
async fn main() {
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let limit12 = || CompileOptions::new().with_max_node_hops(12);
    let can_book = Meeting::new("Q4 planning", &["Alice", "Ravi"]);
    let cannot_book = Meeting::new("Offsite", &["Alice", "Bob", "Dana"]);
    println!(
        "Calendars: Ravi is busy {}; Dana is on leave all week.",
        busy_slots("Ravi").join(" and ")
    );
    println!(
        "The agent proposes, in order: {}, then starts over.",
        SLOTS.join(", ")
    );

    // --- Scenario 1: a meeting that can be booked converges under the limit. ---

    println!();
    println!("=== Scenario 1: Alice + Ravi -- the loop converges, the limit is never reached ===");
    let calls = Calls::default();
    let graph = build(&calls, limit12(), false, false).expect("the scheduler compiles");
    let booked = completed(graph.run("mtg-1", can_book.clone(), store.clone()).await);
    println!(
        "  Compiled with max_node_hops = {:?}.",
        graph.max_node_hops()
    );
    println!("  Tried: {}", booked.tried.join(" | "));
    println!(
        "  Nodes started: {} ({calls}). Invite: {}",
        calls.started(),
        booked.invite.as_deref().unwrap_or("none")
    );
    check(
        graph.max_node_hops() == Some(12),
        "expected the compiled limit on the graph",
    );
    check(
        calls.started() == 7,
        "expected 7 hops: three proposals, three checks, one invite",
    );

    // --- Scenario 2: a meeting that can't be booked is stopped before hop 13. ---

    println!();
    println!(
        "=== Scenario 2: Alice + Bob + Dana -- the agent re-plans until the breaker trips ==="
    );
    let stuck_calls = Calls::default();
    let stuck_graph = build(&stuck_calls, limit12(), false, false).expect("the scheduler compiles");
    let result = stuck_graph
        .run("mtg-2", cannot_book.clone(), store.clone())
        .await;
    let caught_as_failed = matches!(result, GraphExecutionResult::Failed(_));
    let tripped = trip(&store, "mtg-2", result).await;
    let checkpoint = latest(&store, "mtg-2").await;
    println!(
        "  HopLimitExceeded: node '{}', hops {}, limit {}",
        tripped.node, tripped.hops, tripped.limit
    );
    println!("  Message: {}", tripped.message);
    println!(
        "  It is a StrataError inside Failed, so an existing `Failed(_)` arm catches it: {caught_as_failed}"
    );
    println!(
        "  Nodes started: {} ({stuck_calls}) -- the 13th never ran.",
        stuck_calls.started()
    );
    print_checkpoint(&checkpoint);
    check(caught_as_failed, "expected the trip to be a Failed result");
    check(
        tripped.is("ProposeSlot", 12, 12),
        "expected the trip before the 7th proposal",
    );
    check(
        tripped.message == "hop limit of 12 reached: node 'ProposeSlot' would be hop 13",
        "expected the engine's message",
    );
    check(
        (
            stuck_calls.of("ProposeSlot"),
            stuck_calls.of("CheckCalendars"),
            stuck_calls.of("SendInvite"),
        ) == (6, 6, 0),
        "expected 12 nodes to run",
    );
    check(
        checkpoint.status == CheckpointStatus::Failed
            && checkpoint.suspended_node.as_deref() == Some("ProposeSlot")
            && checkpoint.next_node.as_deref() == Some("ProposeSlot"),
        "expected a failed checkpoint at the refused node",
    );
    check(
        meta(&checkpoint, "error_kind") == Some("hop_limit"),
        "expected error_kind hop_limit",
    );
    check(
        meta(&checkpoint, "strata.hops") == Some("12"),
        "expected strata.hops = 12",
    );
    check(
        meta(&checkpoint, "attempts").is_none(),
        "expected no attempts: nothing was attempted",
    );
    check(
        checkpoint.frozen_state["proposals"] == 6,
        "expected the last committed state, six proposals",
    );

    // --- Scenario 3: retries are not hops. ---

    println!();
    println!(
        "=== Scenario 3: the calendar API fails once per check -- retry(1) on CheckCalendars ==="
    );
    let calls = Calls::default();
    let flaky7 = build(
        &calls,
        CompileOptions::new().with_max_node_hops(7),
        true,
        false,
    )
    .expect("the scheduler compiles");
    completed(flaky7.run("mtg-3", can_book.clone(), store.clone()).await);
    println!(
        "  max_node_hops = 7: completed. Handler calls: {} ({calls}); nodes started: {}.",
        calls.handler_calls(),
        calls.started()
    );
    check(
        calls.of("CheckCalendars") == 6 && calls.started() == 7 && calls.handler_calls() == 10,
        "expected 6 calendar attempts inside 7 hops",
    );
    let calls = Calls::default();
    let flaky6 = build(
        &calls,
        CompileOptions::new().with_max_node_hops(6),
        true,
        false,
    )
    .expect("the scheduler compiles");
    let result = flaky6.run("mtg-3b", can_book.clone(), store.clone()).await;
    let tripped = trip(&store, "mtg-3b", result).await;
    println!("  max_node_hops = 6: {}", tripped.message);
    println!("  With a limit of N the N-th node runs; only the node that would be N+1 is refused.");
    check(
        tripped.is("SendInvite", 6, 6),
        "expected the trip at the invite, hop 7",
    );

    // --- Scenario 4: the count survives interrupts and resumes. ---

    println!();
    println!(
        "=== Scenario 4: the organizer reviews every proposal -- an interrupt boundary inside the loop ==="
    );
    let calls = Calls::default();
    let mut result = build(&calls, limit12(), false, true)
        .expect("the scheduler compiles")
        .run("mtg-4", cannot_book.clone(), store.clone())
        .await;
    let mut hops_at_each_freeze = Vec::new();
    while let GraphExecutionResult::Interrupted {
        suspended_node,
        frozen_state,
        ..
    } = &result
    {
        check(hops_at_each_freeze.len() < 20, "expected the loop to trip");
        let round = hops_at_each_freeze.len();
        let hops = meta(&latest(&store, "mtg-4").await, "strata.hops")
            .unwrap_or("none")
            .to_string();
        let label = if round == 0 {
            "run".to_string()
        } else {
            format!("resume {round}")
        };
        println!(
            "  {label:<9} -> Interrupted after {suspended_node} (proposal {}: {}), strata.hops = {hops}",
            frozen_state.proposals,
            frozen_state.slot.as_deref().unwrap_or("-")
        );
        hops_at_each_freeze.push(hops);

        // A graph built from scratch, as another process would: the count
        // comes from the checkpoint.
        result = build(&calls, limit12(), false, true)
            .expect("the scheduler compiles")
            .resume("mtg-4", HumanSignal::approve("organizer"), store.clone())
            .await;
    }
    let tripped = trip(&store, "mtg-4", result).await;
    println!(
        "  resume {:<2} -> {}",
        hops_at_each_freeze.len(),
        tripped.message
    );
    println!(
        "  Nodes started across {} runs: {} ({calls}) -- the same trip as scenario 2.",
        hops_at_each_freeze.len() + 1,
        calls.started()
    );
    check(
        hops_at_each_freeze == ["1", "3", "5", "7", "9", "11"],
        "expected strata.hops to carry on across every freeze",
    );
    check(
        tripped.is("ProposeSlot", 12, 12),
        "expected the same trip as an uninterrupted run",
    );
    check(
        (calls.of("ProposeSlot"), calls.of("CheckCalendars")) == (6, 6),
        "expected the same 12 nodes as an uninterrupted run",
    );

    // --- Scenario 5: RunConfig::with_max_node_hops replaces the limit for one run. ---

    println!();
    println!(
        "=== Scenario 5: RunConfig::with_max_node_hops -- one run's limit, higher or lower ==="
    );
    let started_before = stuck_calls.started();
    let result = stuck_graph
        .resume("mtg-2", HumanSignal::approve("ops-oncall"), store.clone())
        .await;
    let tripped = trip(&store, "mtg-2", result).await;
    println!(
        "  Resuming scenario 2's thread as it is: {} (nodes run: {})",
        tripped.message,
        stuck_calls.started() - started_before
    );
    check(
        stuck_calls.started() == started_before && tripped.is("ProposeSlot", 12, 12),
        "expected the resume to trip again before running anything",
    );

    // The operator drops Dana from the invite and gives the thread room to finish.
    let started_before = stuck_calls.started();
    let fixed_up =
        HumanSignal::approve("ops-oncall").with_override("attendees", json!(["Alice", "Bob"]));
    let booked = completed(
        stuck_graph
            .resume_with(
                "mtg-2",
                fixed_up,
                store.clone(),
                RunConfig::new().with_max_node_hops(20),
            )
            .await,
    );
    println!(
        "  Resumed with attendees = [Alice, Bob] and RunConfig::new().with_max_node_hops(20):"
    );
    println!(
        "    nodes run: {} (hops 13-15, counted on from the stored 12). Invite: {}",
        stuck_calls.started() - started_before,
        booked.invite.as_deref().unwrap_or("none")
    );
    check(
        stuck_calls.started() - started_before == 3 && booked.invite.is_some(),
        "expected propose, check, invite",
    );

    let result = build(&Calls::default(), limit12(), false, false)
        .expect("the scheduler compiles")
        .run_with(
            "mtg-5",
            can_book.clone(),
            store.clone(),
            RunConfig::new().with_max_node_hops(5),
        )
        .await;
    let tripped = trip(&store, "mtg-5", result).await;
    println!(
        "  A test runs the bookable meeting with RunConfig::new().with_max_node_hops(5): {}",
        tripped.message
    );
    check(
        tripped.is("CheckCalendars", 5, 5),
        "expected a lower per-run limit to replace the compiled one",
    );

    let uncapped = build(&Calls::default(), CompileOptions::new(), false, false)
        .expect("the scheduler compiles");
    let result = uncapped
        .run_with(
            "mtg-6",
            cannot_book.clone(),
            store.clone(),
            RunConfig::new().with_max_node_hops(4),
        )
        .await;
    let tripped = trip(&store, "mtg-6", result).await;
    println!(
        "  A graph compiled with no limit (max_node_hops = {:?}) gets one for a run: {}",
        uncapped.max_node_hops(),
        tripped.message
    );
    check(
        uncapped.max_node_hops().is_none() && tripped.is("ProposeSlot", 4, 4),
        "expected the per-run limit to apply to an uncapped graph",
    );

    // --- A limit below 1 is refused, at compile time and per run. ---

    println!();
    println!("=== Step 6: a limit below 1 is refused, by compile_with and by run_with ===");
    match build(
        &Calls::default(),
        CompileOptions::new().with_max_node_hops(0),
        false,
        false,
    ) {
        Err(StrataError::CompilationFailed(message)) => {
            println!("  compile_with: CompilationFailed({message:?})");
            check(
                message == "max_node_hops must be at least 1",
                "expected the compile-time refusal",
            );
        }
        Err(other) => fail(&format!("expected CompilationFailed, got {other:?}")),
        Ok(_) => fail("expected max_node_hops = 0 to be refused"),
    }
    let refused = uncapped
        .run_with(
            "mtg-7",
            cannot_book.clone(),
            store.clone(),
            RunConfig::new().with_max_node_hops(0),
        )
        .await;
    match refused {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            println!("  run_with:     Failed(ExecutionAborted({message:?})), nothing written");
            check(
                message == "a run's max_node_hops must be at least 1",
                "expected the per-run refusal",
            );
        }
        _ => fail("expected a per-run limit of 0 to be refused"),
    }
    check(
        store.load("mtg-7").await.expect("store works").is_none(),
        "expected the refused run to write nothing",
    );

    println!();
    println!(
        "=== Done: the breaker stopped a loop that never converged, counted through retries and interrupts, and let one run go further. ==="
    );
}

/// The scheduling graph. `flaky_calendar` makes the first attempt of every
/// calendar check fail transiently; `organizer_reviews` puts an interrupt
/// boundary on `ProposeSlot`.
fn build(
    calls: &Calls,
    options: CompileOptions,
    flaky_calendar: bool,
    organizer_reviews: bool,
) -> Result<CompiledGraph<Meeting>, StrataError> {
    let (propose, check_calendars, invite) = (calls.clone(), calls.clone(), calls.clone());
    let graph = StateGraph::<Meeting>::new()
        .add_node("ProposeSlot", move |mut meeting: Meeting| {
            propose.ran("ProposeSlot", true);
            meeting.slot = Some(SLOTS[meeting.proposals % SLOTS.len()].to_string());
            meeting.proposals += 1;
            meeting
        })
        .add_async_fallible_node(
            "CheckCalendars",
            move |mut meeting: Meeting, ctx: NodeContext| {
                let calls = check_calendars.clone();
                async move {
                    // A retry is a handler call, not a node start.
                    calls.ran("CheckCalendars", ctx.attempt() == 1);
                    if flaky_calendar && ctx.attempt() == 1 {
                        return Err(NodeError::Transient(
                            "calendar API returned 503".to_string(),
                        ));
                    }
                    let slot = meeting.slot.clone().unwrap_or_default();
                    let busy: Vec<String> = meeting
                        .attendees
                        .iter()
                        .filter(|a| busy_slots(a).contains(&slot.as_str()))
                        .cloned()
                        .collect();
                    let outcome = if busy.is_empty() {
                        format!("{slot} free")
                    } else {
                        format!("{slot} {} busy", busy.join(", "))
                    };
                    meeting.tried.push(outcome);
                    meeting.conflicts = busy;
                    Ok(meeting)
                }
            },
        )
        .set_recovery_policy("CheckCalendars", RecoveryPolicy::retry(1))
        .add_node("SendInvite", move |mut meeting: Meeting| {
            invite.ran("SendInvite", true);
            meeting.invite = Some(format!(
                "'{}' {} for {}",
                meeting.title,
                meeting.slot.as_deref().unwrap_or("-"),
                meeting.attendees.join(", ")
            ));
            meeting
        })
        .add_edge("ProposeSlot", "CheckCalendars")
        .add_conditional_edge("CheckCalendars", "ProposeSlot", |meeting: &Meeting| {
            !meeting.conflicts.is_empty()
        })
        .add_edge("CheckCalendars", "SendInvite");
    let graph = if organizer_reviews {
        graph.add_interrupt_boundary("ProposeSlot")
    } else {
        graph
    };
    graph.compile_with(options)
}

/// A hop-limit trip, with the message the engine wrote on its checkpoint.
struct Trip {
    node: String,
    hops: u32,
    limit: u32,
    message: String,
}

impl Trip {
    fn is(&self, node: &str, hops: u32, limit: u32) -> bool {
        (self.node.as_str(), self.hops, self.limit) == (node, hops, limit)
    }
}

/// Expects `result` to be the breaker tripping on `thread`. `StrataError`
/// has no `Display`; the readable message is the `error` the engine wrote on
/// the `Failed` checkpoint, checked here against the error's fields.
async fn trip(
    store: &Arc<dyn CheckpointStore>,
    thread: &str,
    result: GraphExecutionResult<Meeting>,
) -> Trip {
    let (node, hops, limit) = match result {
        GraphExecutionResult::Failed(StrataError::HopLimitExceeded { node, hops, limit }) => {
            (node, hops, limit)
        }
        GraphExecutionResult::Failed(other) => fail(&format!(
            "{thread}: expected HopLimitExceeded, got {other:?}"
        )),
        _ => fail(&format!("{thread}: expected HopLimitExceeded")),
    };
    let checkpoint = latest(store, thread).await;
    let message = meta(&checkpoint, "error").unwrap_or_default().to_string();
    check(
        message
            == format!(
                "hop limit of {limit} reached: node '{node}' would be hop {}",
                hops + 1
            ),
        "expected the checkpoint's error to describe the trip",
    );
    check(
        meta(&checkpoint, "error_kind") == Some("hop_limit")
            && meta(&checkpoint, "strata.hops") == Some(hops.to_string().as_str()),
        "expected a hop_limit checkpoint carrying the count",
    );
    Trip {
        node,
        hops,
        limit,
        message,
    }
}

fn completed(result: GraphExecutionResult<Meeting>) -> Meeting {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => fail(&format!("expected Completed, got {err:?}")),
        _ => fail("expected Completed"),
    }
}

async fn latest(store: &Arc<dyn CheckpointStore>, thread: &str) -> Checkpoint {
    store
        .load(thread)
        .await
        .expect("the in-memory store doesn't fail")
        .unwrap_or_else(|| fail(&format!("expected a checkpoint for {thread}")))
}

fn meta<'a>(checkpoint: &'a Checkpoint, key: &str) -> Option<&'a str> {
    checkpoint.metadata.as_ref()?.get(key).map(String::as_str)
}

fn print_checkpoint(checkpoint: &Checkpoint) {
    let mut metadata: Vec<_> = checkpoint.metadata.iter().flatten().collect();
    metadata.sort();
    let metadata: Vec<String> = metadata.iter().map(|(k, v)| format!("{k} = {v}")).collect();
    println!(
        "  Checkpoint: {:?} at {}, {} proposals in the frozen state",
        checkpoint.status,
        checkpoint.next_node.as_deref().unwrap_or("-"),
        checkpoint.frozen_state["proposals"]
    );
    println!("    metadata {{ {} }}", metadata.join(", "));
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

/// Counts handler calls per node, and node starts (a retry is a call but not a start).
#[derive(Clone, Default)]
struct Calls(Arc<Mutex<Counts>>);

#[derive(Default)]
struct Counts {
    per_node: HashMap<&'static str, u32>,
    started: u32,
}

impl Calls {
    fn ran(&self, node: &'static str, first_attempt: bool) {
        let mut counts = self.0.lock().unwrap();
        *counts.per_node.entry(node).or_default() += 1;
        if first_attempt {
            counts.started += 1;
        }
    }

    fn of(&self, node: &str) -> u32 {
        self.0
            .lock()
            .unwrap()
            .per_node
            .get(node)
            .copied()
            .unwrap_or_default()
    }

    fn started(&self) -> u32 {
        self.0.lock().unwrap().started
    }

    fn handler_calls(&self) -> u32 {
        self.0.lock().unwrap().per_node.values().sum()
    }
}

impl fmt::Display for Calls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ProposeSlot {}, CheckCalendars {}, SendInvite {}",
            self.of("ProposeSlot"),
            self.of("CheckCalendars"),
            self.of("SendInvite")
        )
    }
}
