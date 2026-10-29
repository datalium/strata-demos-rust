//! Parallel group controls and join policies
//! (`design/26-Parallel-Group-Controls-And-Joins.md`).
//!
//! Demo 10's group waits for every branch and merges them all. This demo is
//! about the options that change when a group is done and what its reducer
//! sees, on a shipping desk that asks three carriers (swiftship, parcelpro,
//! budgetpost) to quote for one parcel:
//!
//!   1. `first_success`       -- a race: the first quote wins, the others are
//!      cancelled (`LostRace`).
//!   2. A failure mid-race    -- doesn't end the race while another carrier
//!      could still win.
//!   3. `max_concurrency(1)`  -- with `first_success` it is a failover chain: a
//!      carrier is only asked when the one before it failed, and the rest never
//!      start.
//!   4. `first_matching`      -- the first quote that passes a test, not just
//!      the first to arrive; with no match, `keep_succeeded` falls back to
//!      every answer, `FailGroup` fails.
//!   5. `quorum(2)`           -- two quotes, then pick the cheaper.
//!   6. Transient or fatal    -- a group that can't meet its join fails
//!      transiently only if a retry could meet it.
//!   7. `All` + `keep_succeeded` -- an outcomes reducer over every branch, with
//!      a branch timeout and a concurrency limit, on tokio's paused clock.
//!
//! Who won and why the others stopped comes from run events: `JoinDecided`
//! names the deciding branches, and `BranchCompleted`'s `reason` says why a
//! branch ended early (`LostRace`, `SiblingFailed`, `TimedOut`).
//!
//! Nothing here depends on timing. A carrier answers at once, never (it waits
//! for its token), when the run's events show another carrier has settled, or
//! (scenario 7) after a delay on tokio's paused clock, so the order of every
//! race is fixed and the output is the same on every run.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{
    BranchEndReason, BranchResult, RunEvent, RunEventKind, RunObserver,
};
use datalium_strata::parallel::{
    BranchOutcomes, BranchStatus, Join, ParallelBranches, ParallelGroupOptions,
};
use datalium_strata::{CancellationToken, StrataError};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::Instant;

/// How long the demo waits for something that should already have happened.
/// On the paused clock this is virtual time: a wait that would hang ends at
/// once with a failed check instead.
const PATIENCE: Duration = Duration::from_secs(10);

const RATING_API_DOWN: &str = "503 from the rating API";
const TOO_HEAVY: &str = "parcels over 2 kg not accepted";

// The whole demo runs on one thread with tokio's clock paused (tokio's
// `test-util` feature): scenario 7's 30 s and 45 s cost no real time, and the
// order every branch is polled in is fixed.
#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    let parcel = Shipment::for_parcel("2.4 kg, Lisbon -> Berlin");

    // --- Scenario 1: first_success. ---

    println!("=== Scenario 1: first_success -- three carriers race, the first quote wins ===");
    let desk = Desk::new();
    let graph = quote_graph(
        desk.carriers()
            .ask("swiftship", Wait::Never, quote(2450, 1))
            .ask("parcelpro", Wait::Now, quote(1420, 3))
            .ask("budgetpost", Wait::Never, quote(690, 5)),
        ParallelGroupOptions::new().with_join(Join::first_success()),
        None,
    );
    let shipment = expect_completed(desk.run(&graph, parcel.clone()).await);
    desk.print_story(false);
    desk.print_outcomes();
    println!("  Chosen: {} ({})", chosen(&shipment), note(&shipment));
    let swiftship_dropped = desk.cancellation_seen("swiftship").await;
    let budgetpost_dropped = desk.cancellation_seen("budgetpost").await;
    println!(
        "  swiftship and budgetpost both had their cancellation token fire before the group dropped their futures."
    );
    check(
        swiftship_dropped && budgetpost_dropped,
        "expected both losers' tokens to fire",
    );
    check(
        carrier(&shipment) == Some("parcelpro"),
        "expected parcelpro to win the race",
    );
    let outcomes = desk.outcomes();
    check(
        outcomes.join_met() && outcomes.deciding() == ["parcelpro"],
        "expected parcelpro to decide the join",
    );
    check(
        statuses(&outcomes)
            == [
                BranchStatus::Cancelled,
                BranchStatus::Succeeded,
                BranchStatus::Cancelled,
            ],
        "expected the losers to be Cancelled",
    );
    check(
        desk.story(false)
            == [
                "completed Quote/parcelpro Succeeded",
                "decided   first_success, met, deciding [parcelpro]",
                "completed Quote/swiftship Cancelled LostRace",
                "completed Quote/budgetpost Cancelled LostRace",
            ],
        "expected JoinDecided between the winner's BranchCompleted and the losers'",
    );

    // --- Scenario 2: a failure doesn't end a race that can still be won. ---

    println!();
    println!("=== Scenario 2: swiftship errors first -- the race goes on ===");
    let desk = Desk::new();
    let graph = quote_graph(
        desk.carriers()
            .ask("swiftship", Wait::Now, refuses(transient(RATING_API_DOWN)))
            .ask("parcelpro", Wait::After("Quote/swiftship"), quote(1420, 3))
            .ask("budgetpost", Wait::Never, quote(690, 5)),
        ParallelGroupOptions::new().with_join(Join::first_success()),
        None,
    );
    let shipment = expect_completed(desk.run(&graph, parcel.clone()).await);
    desk.print_story(false);
    println!("  Chosen: {}", chosen(&shipment));
    check(
        desk.story(false)
            == [
                "completed Quote/swiftship Failed -- 503 from the rating API",
                "completed Quote/parcelpro Succeeded",
                "decided   first_success, met, deciding [parcelpro]",
                "completed Quote/budgetpost Cancelled LostRace",
            ],
        "expected the race to survive swiftship's failure",
    );

    // --- Scenario 3: max_concurrency(1) + first_success = a failover chain. ---

    println!();
    println!(
        "=== Scenario 3: max_concurrency(1) + first_success -- ask in order of preference, stop at the first answer ==="
    );
    let desk = Desk::new();
    let graph = quote_graph(
        desk.carriers()
            .ask("swiftship", Wait::Now, refuses(transient(RATING_API_DOWN)))
            .ask("parcelpro", Wait::Now, quote(1420, 3))
            .ask("budgetpost", Wait::Now, quote(690, 5)),
        ParallelGroupOptions::new()
            .with_join(Join::first_success())
            .with_max_concurrency(1),
        None,
    );
    expect_completed(desk.run(&graph, parcel.clone()).await);
    desk.print_story(true);
    desk.print_outcomes();
    println!(
        "  Carriers asked: {} (peak in flight: {}). budgetpost was never called and has no events.",
        desk.started().join(", "),
        desk.peak()
    );
    check(
        desk.started() == ["swiftship", "parcelpro"] && desk.calls("budgetpost") == 0,
        "expected budgetpost never to start",
    );
    check(desk.peak() == 1, "expected one carrier at a time");
    check(
        desk.story(true)
            == [
                "started   Quote/swiftship",
                "completed Quote/swiftship Failed -- 503 from the rating API",
                "started   Quote/parcelpro",
                "completed Quote/parcelpro Succeeded",
                "decided   first_success, met, deciding [parcelpro]",
            ],
        "expected a held-back branch to get no events once the race is won",
    );
    check(
        desk.outcomes().get("budgetpost").map(|o| o.status) == Some(BranchStatus::Cancelled),
        "expected the reducer to see budgetpost as Cancelled",
    );

    // --- Scenario 4: first_matching, with and without a match. ---

    println!();
    println!(
        "=== Scenario 4a: first_matching(\"delivered within 2 days\") -- the first quote isn't good enough ==="
    );
    let within_two_days =
        Join::first_matching(|s: &Shipment| s.offer.as_ref().is_some_and(|q| q.days <= 2))
            .described("delivered within 2 days");
    let desk = Desk::new();
    let graph = quote_graph(
        desk.carriers()
            .ask("swiftship", Wait::After("Quote/budgetpost"), quote(2450, 1))
            .ask("parcelpro", Wait::Never, quote(1420, 3))
            .ask("budgetpost", Wait::Now, quote(690, 5)),
        ParallelGroupOptions::new().with_join(within_two_days.clone()),
        None,
    );
    let shipment = expect_completed(desk.run(&graph, parcel.clone()).await);
    desk.print_story(false);
    println!(
        "  Chosen: {}. budgetpost answered first and Succeeded, but only JoinDecided says it didn't win.",
        chosen(&shipment)
    );
    check(
        carrier(&shipment) == Some("swiftship"),
        "expected the first matching quote to win",
    );
    check(
        desk.story(false)
            == [
                "completed Quote/budgetpost Succeeded",
                "completed Quote/swiftship Succeeded",
                "decided   first_matching, met, deciding [swiftship]",
                "completed Quote/parcelpro Cancelled LostRace",
            ],
        "expected first_matching to skip a succeeded branch that didn't match",
    );

    println!();
    println!("=== Scenario 4b: no quote matches -- keep_succeeded reduces over every answer ===");
    // Every carrier answers at once, and none within two days.
    fn slow_carriers(desk: &Desk) -> Carriers<'_, Shipment> {
        desk.carriers()
            .ask("swiftship", Wait::Now, quote(1980, 3))
            .ask("parcelpro", Wait::Now, quote(1420, 4))
            .ask("budgetpost", Wait::Now, quote(690, 5))
    }
    let desk = Desk::new();
    let graph = quote_graph(
        slow_carriers(&desk),
        ParallelGroupOptions::new()
            .with_join(within_two_days.clone())
            .keep_succeeded(),
        None,
    );
    let shipment = expect_completed(desk.run(&graph, parcel.clone()).await);
    let last = desk.story(false).last().cloned().unwrap_or_default();
    println!("  {last}");
    desk.print_outcomes();
    println!("  Chosen: {} ({})", chosen(&shipment), note(&shipment));
    let outcomes = desk.outcomes();
    check(
        !outcomes.join_met() && outcomes.deciding().is_empty(),
        "expected an unmet join",
    );
    check(
        statuses(&outcomes)
            .iter()
            .all(|s| *s == BranchStatus::Succeeded),
        "expected every answer to reach the reducer",
    );
    check(
        carrier(&shipment) == Some("swiftship"),
        "expected the fallback to pick the fastest quote",
    );
    check(
        last == "decided   first_matching, met: no, deciding []",
        "expected JoinDecided to report the miss",
    );

    println!();
    println!("=== Scenario 4c: the same miss under the default PartialResults::FailGroup ===");
    let desk = Desk::new();
    let graph = quote_graph(
        slow_carriers(&desk),
        ParallelGroupOptions::new().with_join(within_two_days),
        None,
    );
    let (attempts, message) = expect_node_failed(desk.run(&graph, parcel.clone()).await);
    let kind = desk.error_kind().await;
    println!("  Failed: {message} -- {kind} (a retry asks again, and quotes change)");
    check(
        message == "no branch matched the join (3 succeeded)",
        "expected the no-match message",
    );
    check(
        attempts == 1 && kind == "transient",
        "expected a no-match to be transient",
    );

    // --- Scenario 5: quorum(2), plain reducer. ---

    println!();
    println!("=== Scenario 5: quorum(2) -- wait for two quotes, take the cheaper ===");
    let desk = Desk::new();
    let handed: Arc<Mutex<Vec<String>>> = Arc::default();
    let record = handed.clone();
    let graph = StateGraph::<Shipment>::new()
        .add_parallel_group_with(
            "Quote",
            desk.carriers()
                .ask("swiftship", Wait::Never, quote(2450, 1))
                .ask("parcelpro", Wait::After("Quote/budgetpost"), quote(1420, 3))
                .ask("budgetpost", Wait::Now, quote(690, 5))
                .branches,
            move |entry: &Shipment, offers: Vec<Shipment>| {
                let quotes: Vec<Quote> = offers.into_iter().filter_map(|s| s.offer).collect();
                *record.lock().unwrap() = quotes.iter().map(|q| q.carrier.clone()).collect();
                Shipment {
                    chosen: quotes.into_iter().min_by_key(|q| q.cents),
                    note: Some("cheaper of two".into()),
                    ..entry.clone()
                }
            },
            ParallelGroupOptions::new().with_join(Join::quorum(2)),
        )
        .compile()
        .expect("the quorum graph is valid");
    let shipment = expect_completed(desk.run(&graph, parcel.clone()).await);
    desk.print_story(false);
    let handed = handed.lock().unwrap().clone();
    println!(
        "  The plain reducer was handed: [{}] -- declaration order, not completion order.",
        handed.join(", ")
    );
    println!("  Chosen: {}", chosen(&shipment));
    check(
        handed == ["parcelpro", "budgetpost"],
        "expected the reducer to get the deciding states in declaration order",
    );
    check(
        desk.story(false)
            .iter()
            .any(|l| l == "decided   quorum, met, deciding [budgetpost, parcelpro]"),
        "expected deciding in completion order",
    );
    check(
        carrier(&shipment) == Some("budgetpost"),
        "expected the cheaper quote",
    );

    // --- Scenario 6: an unmet join -- can a retry still meet it? ---

    println!();
    println!(
        "=== Scenario 6a: quorum(2), two carriers refuse for good -- RecoveryPolicy::retry(2) can't help ==="
    );
    let desk = Desk::new();
    let after_parcelpro = Wait::After("Quote/parcelpro");
    let graph = quote_graph(
        desk.carriers()
            .ask("swiftship", after_parcelpro, refuses(fatal(TOO_HEAVY)))
            .ask("parcelpro", Wait::Now, quote(1420, 3))
            .ask(
                "budgetpost",
                after_parcelpro,
                refuses(fatal("no service to postcode 10115")),
            ),
        ParallelGroupOptions::new().with_join(Join::quorum(2)),
        Some(RecoveryPolicy::retry(2)),
    );
    let (attempts, message) = expect_node_failed(desk.run(&graph, parcel.clone()).await);
    let kind = desk.error_kind().await;
    println!("  Failed after {attempts} attempt(s), {kind}:");
    println!("    {message}");
    println!("  3 branches - 2 fatal failures = 1, fewer than the 2 needed, so no retry was made.");
    check(
        attempts == 1 && kind == "fatal",
        "expected a fatal failure with no retry",
    );
    check(
        message
            == "quorum of 2 not reached: 1 of 3 branches succeeded; branch 'swiftship': parcels over 2 kg not accepted; branch 'budgetpost': no service to postcode 10115",
        "expected the quorum message to name every failure in declaration order",
    );

    println!();
    println!(
        "=== Scenario 6b: one refuses for good, one is rate limited once -- RecoveryPolicy::retry(2) meets the quorum ==="
    );
    let desk = Desk::new();
    let budgetpost_calls = Mutex::new(0);
    let rate_limited_once = move |carrier: &'static str, shipment: Shipment| {
        let mut calls = budgetpost_calls.lock().unwrap();
        *calls += 1;
        if *calls == 1 {
            Err(transient("rate limited"))
        } else {
            quote(690, 5)(carrier, shipment)
        }
    };
    let graph = quote_graph(
        desk.carriers()
            .ask("swiftship", after_parcelpro, refuses(fatal(TOO_HEAVY)))
            .ask("parcelpro", Wait::Now, quote(1420, 3))
            .ask("budgetpost", after_parcelpro, rate_limited_once),
        ParallelGroupOptions::new().with_join(Join::quorum(2)),
        Some(RecoveryPolicy::retry(2)),
    );
    expect_completed(desk.run(&graph, parcel.clone()).await);
    let retries: Vec<(Option<u32>, String)> = desk
        .events()
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::RetryScheduled { error, .. } => Some((e.attempt, error.clone())),
            _ => None,
        })
        .collect();
    check(retries.len() == 1, "expected exactly one RetryScheduled");
    let (retry_attempt, retry_error) = retries[0].clone();
    println!("  Attempt 1 failed transiently (3 branches - 1 fatal = 2, enough for the quorum):");
    println!("    {retry_error}");
    let deciding = desk.outcomes().deciding().to_vec();
    println!(
        "  Attempt 2 met the quorum, deciding [{}].",
        deciding.join(", ")
    );
    println!(
        "  Calls -- swiftship: {}, parcelpro: {}, budgetpost: {}. The retry re-ran parcelpro too: there is no per-branch retry.",
        desk.calls("swiftship"),
        desk.calls("parcelpro"),
        desk.calls("budgetpost")
    );
    check(
        retry_attempt == Some(1),
        "expected one retry, after attempt 1",
    );
    check(
        retry_error
            == "quorum of 2 not reached: 1 of 3 branches succeeded; branch 'swiftship': parcels over 2 kg not accepted; branch 'budgetpost': rate limited",
        "expected the first attempt's quorum message",
    );
    check(
        deciding == ["parcelpro", "budgetpost"],
        "expected the retry to meet the quorum",
    );
    check(
        desk.calls("parcelpro") == 2 && desk.calls("budgetpost") == 2,
        "expected the retry to re-run every branch",
    );

    // --- Scenario 7: All + keep_succeeded, a branch timeout and a limit, on the paused clock. ---

    println!();
    println!(
        "=== Scenario 7: tracking five parcels -- 2 at a time, 45 s each, keep what answered ==="
    );
    let desk = Desk::new();
    let thirty_seconds = Wait::For(Duration::from_secs(30));
    let seen: Arc<Mutex<Option<BranchOutcomes<Board>>>> = Arc::default();
    let keep = seen.clone();
    let unknown_number = refuses(fatal("unknown tracking number NL-77Q"));
    let graph = StateGraph::<Board>::new()
        .add_parallel_group_with_outcomes(
            "Track",
            desk.carriers()
                .ask("swiftship", Wait::Never, says("never answers"))
                .ask("parcelpro", thirty_seconds, says("out for delivery"))
                .ask(
                    "budgetpost",
                    thirty_seconds,
                    says("in transit, Leipzig hub"),
                )
                .ask("northline", Wait::Now, unknown_number)
                .ask("coastal", thirty_seconds, says("delivered"))
                .branches,
            move |entry: &Board, results: BranchOutcomes<Board>| {
                let mut board = entry.clone();
                for r in results.outcomes() {
                    match (&r.state, &r.error) {
                        (Some(state), _) => board.lines.push(format!(
                            "{}: {}",
                            r.name,
                            state.status.as_deref().unwrap_or_default()
                        )),
                        (None, error) => board.gaps.push(format!(
                            "{}: {:?} -- {}",
                            r.name,
                            r.status,
                            error.as_ref().map(NodeError::message).unwrap_or_default()
                        )),
                    }
                }
                *keep.lock().unwrap() = Some(results);
                board
            },
            ParallelGroupOptions::new()
                .with_max_concurrency(2)
                .with_branch_timeout(Duration::from_secs(45))
                .keep_succeeded(),
        )
        .compile()
        .expect("the tracking graph is valid");
    let board = expect_completed(desk.run(&graph, Board::for_order("order 4471")).await);
    println!(
        "  Started, in declaration order: {}; peak in flight: {}.",
        desk.started().join(", "),
        desk.peak()
    );
    println!("  Tracked ({}):", board.order);
    for line in &board.lines {
        println!("    - {line}");
    }
    println!("  Gaps:");
    for gap in &board.gaps {
        println!("    - {gap}");
    }
    let outcomes = seen.lock().unwrap().clone().expect("the reducer ran");
    let join_decided = desk
        .events()
        .iter()
        .any(|e| matches!(e.kind, RunEventKind::JoinDecided { .. }));
    println!(
        "  join_met: {} (an All join with failures), and no JoinDecided event: an All group never emits one.",
        outcomes.join_met()
    );
    let (asked, answered) = desk.timing("budgetpost");
    println!(
        "  budgetpost was asked at {} s and answered at {} s on the paused clock: its 45 s timeout starts when it starts, so it wasn't charged for the wait.",
        asked.as_secs(),
        answered.map(|d| d.as_secs()).unwrap_or_default()
    );
    check(
        desk.started()
            == [
                "swiftship",
                "parcelpro",
                "budgetpost",
                "northline",
                "coastal",
            ],
        "expected starts in declaration order",
    );
    check(desk.peak() == 2, "expected at most two branches in flight");
    check(
        statuses(&outcomes)
            == [
                BranchStatus::TimedOut,
                BranchStatus::Succeeded,
                BranchStatus::Succeeded,
                BranchStatus::Failed,
                BranchStatus::Succeeded,
            ],
        "expected every outcome, in declaration order",
    );
    check(
        outcomes.get("swiftship").and_then(|o| o.error.clone())
            == Some(transient("branch 'swiftship' timed out after 45000 ms")),
        "expected a branch timeout to be a transient failure",
    );
    check(
        matches!(
            outcomes.get("northline").and_then(|o| o.error.clone()),
            Some(NodeError::Fatal(_))
        ),
        "expected northline's own error kind",
    );
    check(!outcomes.join_met(), "expected an unmet All join");
    check(!join_decided, "expected no JoinDecided for an All group");
    check(
        desk.events().iter().any(|e| {
            e.node_path.as_deref() == Some("Track/swiftship")
                && matches!(
                    e.kind,
                    RunEventKind::BranchCompleted {
                        result: BranchResult::Failed,
                        reason: Some(BranchEndReason::TimedOut),
                        ..
                    }
                )
        }),
        "expected swiftship's BranchCompleted to say TimedOut",
    );
    check(
        asked == Duration::from_secs(30) && answered == Some(Duration::from_secs(60)),
        "expected budgetpost to wait 30 s for a slot, then run 30 s",
    );

    // --- Step 8: what .compile() refuses. ---

    println!();
    println!("=== Step 8: a quorum the group can't reach is refused at .compile() ===");
    let compile = |options: ParallelGroupOptions<Shipment>| {
        StateGraph::<Shipment>::new()
            .add_parallel_group_with(
                "Quote",
                ParallelBranches::new()
                    .branch("swiftship", |s: &Shipment| Ok(s.clone()))
                    .branch("parcelpro", |s: &Shipment| Ok(s.clone()))
                    .branch("budgetpost", |s: &Shipment| Ok(s.clone())),
                |entry: &Shipment, _: Vec<Shipment>| entry.clone(),
                options,
            )
            .compile()
    };
    match compile(ParallelGroupOptions::new().with_join(Join::quorum(4))) {
        Err(StrataError::GraphInvalid(result)) => {
            check(result.errors.len() == 1, "expected exactly one error");
            let error = &result.errors[0];
            println!("  {} at '{}': {}", error.code, error.node, error.message);
            check(
                error.code == "STRATA_ERR_INVALID_QUORUM" && error.node == "Quote",
                "expected STRATA_ERR_INVALID_QUORUM",
            );
        }
        Err(other) => fail(&format!("expected GraphInvalid, got {other:?}")),
        Ok(_) => fail("expected quorum(4) of 3 branches to be refused"),
    }
    match compile(ParallelGroupOptions::new().with_max_concurrency(0)) {
        Err(StrataError::CompilationFailed(message)) => {
            println!("  with_max_concurrency(0) is refused by .compile() too: {message}");
        }
        Err(other) => fail(&format!("expected CompilationFailed, got {other:?}")),
        Ok(_) => fail("expected with_max_concurrency(0) to be refused"),
    }

    println!();
    println!(
        "=== Done: every join decided as it should, and the events said who won and why the rest stopped. ==="
    );
}

// --- The quote desk's graph. ---

/// One group, `Quote`, whose outcomes reducer takes the winner's quote when the
/// join was met, and otherwise the fastest quote that came back.
fn quote_graph(
    carriers: Carriers<'_, Shipment>,
    options: ParallelGroupOptions<Shipment>,
    policy: Option<RecoveryPolicy>,
) -> CompiledGraph<Shipment> {
    let seen = carriers.desk.outcomes.clone();
    let mut graph = StateGraph::<Shipment>::new().add_parallel_group_with_outcomes(
        "Quote",
        carriers.branches,
        move |entry: &Shipment, results: BranchOutcomes<Shipment>| {
            let mut next = entry.clone();
            if let Some(winner) = results.deciding().first() {
                next.chosen = results
                    .get(winner)
                    .and_then(|o| o.state.as_ref())
                    .and_then(|s| s.offer.clone());
                next.note = Some(format!("{winner} decided the join"));
            } else {
                next.chosen = results
                    .succeeded()
                    .filter_map(|s| s.offer.clone())
                    .min_by_key(|q| q.days);
                next.note = Some("no quote met the join: the fastest that answered".into());
            }
            *seen.lock().unwrap() = Some(results);
            next
        },
        options,
    );
    if let Some(policy) = policy {
        graph = graph.set_recovery_policy("Quote", policy);
    }
    graph.compile().expect("the quote graph is valid")
}

/// A carrier's answer: given its name and the state the group started with,
/// its own result state, or the error it fails with.
trait Answer<T>: Fn(&'static str, T) -> Result<T, NodeError> + Send + Sync + 'static {}

impl<T, F> Answer<T> for F where
    F: Fn(&'static str, T) -> Result<T, NodeError> + Send + Sync + 'static
{
}

fn quote(cents: u32, days: u32) -> impl Answer<Shipment> {
    move |carrier, shipment| {
        Ok(Shipment {
            offer: Some(Quote {
                carrier: carrier.into(),
                cents,
                days,
            }),
            ..shipment
        })
    }
}

fn says(status: &'static str) -> impl Answer<Board> {
    move |_, board| {
        Ok(Board {
            status: Some(status.into()),
            ..board
        })
    }
}

fn refuses<T>(error: NodeError) -> impl Answer<T> {
    move |_, _| Err(error.clone())
}

fn transient(message: &str) -> NodeError {
    NodeError::Transient(message.into())
}

fn fatal(message: &str) -> NodeError {
    NodeError::Fatal(message.into())
}

// --- State. ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Quote {
    carrier: String,
    /// Euro cents, so the demo needs no decimal type.
    cents: u32,
    days: u32,
}

impl fmt::Display for Quote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (euros, cents) = (self.cents / 100, self.cents % 100);
        write!(
            f,
            "{} EUR {euros}.{cents:02}, {} day(s)",
            self.carrier, self.days
        )
    }
}

/// The parcel, the quote one branch came back with (`offer`), and the one the
/// reducer chose.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Shipment {
    parcel: String,
    offer: Option<Quote>,
    chosen: Option<Quote>,
    note: Option<String>,
}

impl Shipment {
    fn for_parcel(parcel: &str) -> Self {
        Self {
            parcel: parcel.into(),
            offer: None,
            chosen: None,
            note: None,
        }
    }
}

/// Scenario 7's state: what each carrier said about its parcel, and who didn't
/// say.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Board {
    order: String,
    status: Option<String>,
    lines: Vec<String>,
    gaps: Vec<String>,
}

impl Board {
    fn for_order(order: &str) -> Self {
        Self {
            order: order.into(),
            status: None,
            lines: Vec::new(),
            gaps: Vec::new(),
        }
    }
}

fn chosen(shipment: &Shipment) -> String {
    shipment
        .chosen
        .as_ref()
        .map_or_else(|| "nothing".into(), Quote::to_string)
}

fn carrier(shipment: &Shipment) -> Option<&str> {
    shipment.chosen.as_ref().map(|q| q.carrier.as_str())
}

fn note(shipment: &Shipment) -> &str {
    shipment.note.as_deref().unwrap_or_default()
}

fn statuses<S>(outcomes: &BranchOutcomes<S>) -> Vec<BranchStatus> {
    outcomes.outcomes().iter().map(|o| o.status).collect()
}

// --- The desk: one scenario's carriers and run. ---

/// How a carrier's branch waits before it answers.
#[derive(Clone, Copy)]
enum Wait {
    /// Answers at once.
    Now,
    /// Never answers: waits until its token fires.
    Never,
    /// Answers once the run's events show the branch at this path settled in
    /// the same attempt of the group (a retried group's branches report the
    /// new attempt).
    After(&'static str),
    /// Answers after this long on tokio's (paused) clock.
    For(Duration),
}

/// When a carrier was last asked, and when it answered, on the paused clock,
/// from when its desk was opened.
type Timing = HashMap<&'static str, (Duration, Option<Duration>)>;

/// One scenario's carriers and run: it builds the branches, counts calls,
/// notes which branches were dropped with their token cancelled, and records
/// the run's events, which a branch can wait on to answer after a sibling
/// settled.
struct Desk {
    timeline: Arc<Timeline>,
    store: Arc<InMemoryCheckpointStore>,
    calls: Arc<Mutex<HashMap<&'static str, usize>>>,
    timing: Arc<Mutex<Timing>>,
    opened: Instant,
    cancelled: Arc<Watch>,
    outcomes: Arc<Mutex<Option<BranchOutcomes<Shipment>>>>,
}

/// A group's branches, one async branch per carrier, built against a desk.
struct Carriers<'d, T> {
    desk: &'d Desk,
    branches: ParallelBranches<T>,
}

impl<T> Carriers<'_, T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Adds `carrier`'s async branch: it waits as `wait` says, then answers.
    /// A guard notes when the group drops the branch's future with its token
    /// already cancelled: a group aborts the branches it gives up on.
    fn ask(mut self, carrier: &'static str, wait: Wait, answer: impl Answer<T>) -> Self {
        let answer = Arc::new(answer);
        let desk = self.desk;
        let (calls, timing, timeline, cancelled, opened) = (
            desk.calls.clone(),
            desk.timing.clone(),
            desk.timeline.clone(),
            desk.cancelled.clone(),
            desk.opened,
        );
        self.branches = self
            .branches
            .async_branch(carrier, move |state: T, ctx: NodeContext| {
                *calls.lock().unwrap().entry(carrier).or_default() += 1;
                let asked = opened.elapsed();
                timing.lock().unwrap().insert(carrier, (asked, None));
                let guard = DropWatch {
                    carrier,
                    token: ctx.cancellation().clone(),
                    cancelled: cancelled.clone(),
                };
                let (answer, timing, timeline) = (answer.clone(), timing.clone(), timeline.clone());
                async move {
                    let _guard = guard;
                    match wait {
                        Wait::Now => {}
                        Wait::Never => {
                            ctx.cancellation().cancelled().await;
                            return Err(fatal("cancelled"));
                        }
                        Wait::After(path) => timeline.settled(path, ctx.attempt()).await,
                        Wait::For(delay) => tokio::time::sleep(delay).await,
                    }
                    let result = answer(carrier, state);
                    let answered = Some(opened.elapsed());
                    timing.lock().unwrap().insert(carrier, (asked, answered));
                    result
                }
            });
        self
    }
}

impl Desk {
    fn new() -> Self {
        Self {
            timeline: Arc::new(Timeline::default()),
            store: Arc::new(InMemoryCheckpointStore::new()),
            calls: Arc::default(),
            timing: Arc::default(),
            opened: Instant::now(),
            cancelled: Arc::default(),
            outcomes: Arc::default(),
        }
    }

    fn carriers<T>(&self) -> Carriers<'_, T> {
        Carriers {
            desk: self,
            branches: ParallelBranches::new(),
        }
    }

    /// Runs `graph` with the desk's observer, then waits for `RunCompleted`:
    /// events are delivered off the executor, so the last ones can arrive
    /// after `run_with` returns.
    async fn run<S>(&self, graph: &CompiledGraph<S>, start: S) -> GraphExecutionResult<S>
    where
        S: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    {
        let store: Arc<dyn CheckpointStore> = self.store.clone();
        let config = RunConfig::new().with_observer(self.timeline.clone());
        let result = graph.run_with("desk", start, store, config).await;
        let run_completed = self
            .timeline
            .wait_for(|e| matches!(e.kind, RunEventKind::RunCompleted { .. }));
        check(
            tokio::time::timeout(PATIENCE, run_completed).await.is_ok(),
            "expected RunCompleted to arrive",
        );
        result
    }

    fn events(&self) -> Vec<RunEvent> {
        self.timeline.events.lock().unwrap().clone()
    }

    fn outcomes(&self) -> BranchOutcomes<Shipment> {
        self.outcomes
            .lock()
            .unwrap()
            .clone()
            .expect("the reducer ran")
    }

    fn calls(&self, carrier: &str) -> usize {
        let calls = self.calls.lock().unwrap();
        calls.get(carrier).copied().unwrap_or_default()
    }

    fn timing(&self, carrier: &str) -> (Duration, Option<Duration>) {
        let timing = self.timing.lock().unwrap();
        timing.get(carrier).copied().unwrap_or_default()
    }

    /// Waits until `carrier`'s branch was dropped with its token cancelled. A
    /// group doesn't wait for the branches it gives up on, so this can land
    /// just after the run returns.
    async fn cancellation_seen(&self, carrier: &'static str) -> bool {
        tokio::time::timeout(PATIENCE, self.cancelled.wait(carrier))
            .await
            .is_ok()
    }

    /// The branches the group started, in the order it started them
    /// (`BranchStarted`).
    fn started(&self) -> Vec<String> {
        self.events()
            .iter()
            .filter(|e| matches!(e.kind, RunEventKind::BranchStarted))
            .filter_map(|e| e.node_path.as_deref())
            .map(|path| path.split_once('/').map_or(path, |(_, b)| b).to_string())
            .collect()
    }

    /// The most branches the group had started and not yet settled, replayed
    /// from its events. This is what `with_max_concurrency` bounds. It is
    /// counted from the events, not inside the branches, because a group
    /// stops waiting for a branch the moment its token fires (a timeout, a
    /// lost race) and starts the next one.
    fn peak(&self) -> usize {
        let (mut open, mut peak) = (0usize, 0usize);
        for e in self.events() {
            match e.kind {
                RunEventKind::BranchStarted => open += 1,
                RunEventKind::BranchCompleted { .. } => open -= 1,
                _ => {}
            }
            peak = peak.max(open);
        }
        peak
    }

    /// The failed checkpoint's error kind, `transient` or `fatal`.
    async fn error_kind(&self) -> String {
        let checkpoint = self
            .store
            .load("desk")
            .await
            .expect("the store loads")
            .expect("a failed run leaves a checkpoint");
        checkpoint
            .metadata
            .and_then(|m| m.get("error_kind").cloned())
            .unwrap_or_default()
    }

    /// The group's branch and join events, one line each, in order.
    fn story(&self, with_starts: bool) -> Vec<String> {
        self.events()
            .iter()
            .filter_map(|e| line(e, with_starts))
            .collect()
    }

    fn print_story(&self, with_starts: bool) {
        println!("  Events:");
        for line in self.story(with_starts) {
            println!("    {line}");
        }
    }

    fn print_outcomes(&self) {
        let outcomes = self.outcomes();
        let seen: Vec<String> = outcomes
            .outcomes()
            .iter()
            .map(|o| format!("{} {:?}", o.name, o.status))
            .collect();
        println!(
            "  Reducer saw (declaration order): {}; join_met: {}",
            seen.join(", "),
            outcomes.join_met()
        );
    }
}

/// One branch or join event as a line of the story; `None` for the rest.
fn line(event: &RunEvent, with_starts: bool) -> Option<String> {
    let path = event.node_path.as_deref().unwrap_or_default();
    match &event.kind {
        RunEventKind::BranchStarted if with_starts => Some(format!("started   {path}")),
        RunEventKind::BranchCompleted {
            result,
            reason,
            error,
            ..
        } => {
            let reason = reason.map(|r| format!(" {r:?}")).unwrap_or_default();
            let error = error
                .as_ref()
                .map(|e| format!(" -- {e}"))
                .unwrap_or_default();
            Some(format!("completed {path} {result:?}{reason}{error}"))
        }
        RunEventKind::JoinDecided {
            policy,
            met,
            deciding,
        } => Some(format!(
            "decided   {policy}, {}, deciding [{}]",
            if *met { "met" } else { "met: no" },
            deciding.join(", ")
        )),
        _ => None,
    }
}

/// Dropped with a branch's future. If the branch's token had fired by then,
/// the group gave up on it: it notes the carrier.
struct DropWatch {
    carrier: &'static str,
    token: CancellationToken,
    cancelled: Arc<Watch>,
}

impl Drop for DropWatch {
    fn drop(&mut self) {
        if self.token.is_cancelled() {
            self.cancelled.note(self.carrier);
        }
    }
}

/// The carriers whose branch was dropped with its token cancelled, and a way
/// to wait for one.
#[derive(Default)]
struct Watch {
    seen: Mutex<Vec<&'static str>>,
    changed: Notify,
}

impl Watch {
    fn note(&self, carrier: &'static str) {
        self.seen.lock().unwrap().push(carrier);
        self.changed.notify_waiters();
    }

    async fn wait(&self, carrier: &str) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.seen.lock().unwrap().contains(&carrier) {
                return;
            }
            changed.await;
        }
    }
}

/// A run observer that keeps every event and lets a caller wait for one. The
/// engine calls `on_event` in sequence order, one event at a time, on a tokio
/// task off the executor.
#[derive(Default)]
struct Timeline {
    events: Mutex<Vec<RunEvent>>,
    changed: Notify,
}

impl RunObserver for Timeline {
    fn on_event(&self, event: &RunEvent) {
        self.events.lock().unwrap().push(event.clone());
        self.changed.notify_waiters();
    }
}

impl Timeline {
    /// Resolves once an event matching `matches` has been seen (at once, if
    /// one has).
    async fn wait_for(&self, matches: impl Fn(&RunEvent) -> bool) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.events.lock().unwrap().iter().any(&matches) {
                return;
            }
            changed.await;
        }
    }

    /// Resolves once the branch at `path` has settled in `attempt` of its
    /// group.
    async fn settled(&self, path: &str, attempt: u32) {
        self.wait_for(|e| {
            matches!(e.kind, RunEventKind::BranchCompleted { .. })
                && e.node_path.as_deref() == Some(path)
                && e.attempt == Some(attempt)
        })
        .await
    }
}

// --- Helpers. ---

fn expect_completed<S>(result: GraphExecutionResult<S>) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(error) => fail(&format!("expected Completed, got {error:?}")),
        _ => fail("expected Completed"),
    }
}

/// A `NodeFailed`'s attempts and message.
fn expect_node_failed<S>(result: GraphExecutionResult<S>) -> (u32, String) {
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            attempts, message, ..
        }) => (attempts, message),
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
