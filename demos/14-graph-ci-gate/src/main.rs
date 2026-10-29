//! `strata graph validate` as a CI gate (STRATA-SPEC-004's other entry point).
//!
//! `.compile()` rejects a structurally broken graph at startup -- but that is a
//! check you only get by *running* the program. CI wants the same verdict
//! without running anything, from a file: `strata graph validate --path graphs/`.
//!
//! The path from code to that file is `StateGraph::to_definition()`, which
//! exports a graph's structure **without validating it**. (`.compile()` can't
//! be the export step: it refuses to hand back a broken graph, and a broken
//! graph is exactly what the gate exists to catch.)
//!
//! Seven graphs live in one "repo": four healthy ones and three that each
//! break a different STRATA-SPEC-004 rule. What the scenarios show:
//!
//! 1. **A broken pull request** -- all seven are exported to `graphs/*.json`
//!    and the real `strata` binary checks the folder in one call. It reports
//!    every file, names each error, and exits non-zero. The demo also checks
//!    that for every graph the CLI and `.compile()` return the *same*
//!    diagnostics.
//! 2. **A trap the export avoids** -- a compiled graph's `definition` field
//!    holds only the edges the executor routes over, so a node reachable only
//!    through a recovery policy's fallback looks like an orphan.
//!    `to_definition()` adds the fallback route, so the CLI agrees with
//!    `.compile()`.
//! 3. **An interrupt boundary in the file** -- payout-approval's `Approve` node
//!    is a boundary with a reject route. The export marks it (`"interrupt"`,
//!    `schema_version` 1.6.0, `design/48-Boundaries-In-Definitions.md`) and
//!    `strata graph view` says so.
//! 4. **The fix** -- the same seven graphs with three small changes: exit 0,
//!    and all seven really do compile.
//! 5. **When the gate can't check** -- a file that isn't a graph, a hand edit
//!    that leaves a reject route on a node that is no longer a boundary, an
//!    empty folder, a mistyped path. All exit 2, never a silent pass.
//!
//! The `strata` CLI is the release binary of `datalium/strata` (not on
//! crates.io): put it on the `PATH` as `strata`, or set `STRATA_BIN` to its
//! path. Pass
//! `--out <dir>` to keep the final export -- the .NET twin writes
//! byte-identical files for all seven graphs, so `diff -r` between the two
//! folders is the cross-language check.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use datalium_strata::StrataError;
use datalium_strata::graph::{NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::schema::GraphDefinition;
use serde_json::Value;

/// One state type for every graph: nothing here ever *runs*, only the shape matters.
#[derive(Debug, Clone)]
struct Run {
    attempt: u32,
}

fn node(run: Run) -> Run {
    run
}

/// Builds a graph. The flag picks the broken (`false`) or fixed (`true`) version;
/// the healthy graphs ignore it.
type Build = fn(bool) -> StateGraph<Run>;

const GRAPHS: [(&str, Build); 7] = [
    ("order-pipeline", order_pipeline),
    ("document-review", document_review),
    ("support-desk", support_desk),
    ("refund-flow", refund_flow),
    ("report-builder", report_builder),
    ("import-job", import_job),
    ("payout-approval", payout_approval),
];

/// Where the `strata` binary for each platform is published.
const CLI_RELEASE: &str = "https://github.com/datalium/strata/releases/tag/v0.9.0-preview.1";

/// The broken versions' diagnostics: one STRATA-SPEC-004 rule each.
const BROKEN: [(&str, &str); 3] = [
    ("refund-flow", "STRATA_ERR_UNGUARDED_CYCLE"),
    ("report-builder", "STRATA_ERR_ORPHAN"),
    ("import-job", "STRATA_ERR_MISSING_ENTRY"),
];

/// `ManualReview` is reachable only through `Charge`'s fallback: no ordinary edge enters it.
fn order_pipeline(_fixed: bool) -> StateGraph<Run> {
    StateGraph::<Run>::new()
        .add_node("Validate", node)
        .add_fallible_node("Charge", |s: &Run| Ok::<Run, NodeError>(s.clone()))
        .add_node("Ship", node)
        .add_node("ManualReview", node)
        .add_edge("Validate", "Charge")
        .add_edge("Charge", "Ship")
        .set_recovery_policy(
            "Charge",
            RecoveryPolicy::retry(2).with_fallback("ManualReview"),
        )
}

/// A retry loop that is bounded by a runtime predicate.
fn document_review(_fixed: bool) -> StateGraph<Run> {
    StateGraph::<Run>::new()
        .add_node("Draft", node)
        .add_node("Review", node)
        .add_node("Publish", node)
        .add_edge("Draft", "Review")
        .add_conditional_edge("Review", "Draft", |s: &Run| s.attempt < 3)
        .add_edge("Review", "Publish")
}

/// A supervisor branching to two agents that rejoin.
fn support_desk(_fixed: bool) -> StateGraph<Run> {
    StateGraph::<Run>::new()
        .add_node("Triage", node)
        .add_node("Billing", node)
        .add_node("Tech", node)
        .add_node("Close", node)
        .add_conditional_edge("Triage", "Billing", |s: &Run| s.attempt == 0)
        .add_edge("Triage", "Tech")
        .add_edge("Billing", "Close")
        .add_edge("Tech", "Close")
}

/// A human approves a payout. `Approve` is an interrupt boundary: a rejection
/// continues at `Refuse`, which no ordinary edge enters. The export writes
/// `interrupt` on `Approve` and the route as an `on_reject` edge.
fn payout_approval(_fixed: bool) -> StateGraph<Run> {
    StateGraph::<Run>::new()
        .add_node("Prepare", node)
        .add_node("Approve", node)
        .add_node("Pay", node)
        .add_node("Refuse", node)
        .add_edge("Prepare", "Approve")
        .add_edge("Approve", "Pay")
        .add_interrupt_boundary_with_reject("Approve", "Refuse")
}

/// Broken: `Approve` loops back to `Request` on a plain edge, so nothing bounds
/// the loop. Fixed: the edge gets a predicate.
fn refund_flow(fixed: bool) -> StateGraph<Run> {
    let graph = StateGraph::<Run>::new()
        .add_node("Request", node)
        .add_node("Approve", node)
        .add_node("Payout", node)
        .add_edge("Request", "Approve");
    let graph = if fixed {
        graph.add_conditional_edge("Approve", "Request", |s: &Run| s.attempt < 2)
    } else {
        graph.add_edge("Approve", "Request")
    };
    graph.add_edge("Approve", "Payout")
}

/// Broken: `Archive` is added but nothing leads to it. Fixed: `Render` does.
fn report_builder(fixed: bool) -> StateGraph<Run> {
    let graph = StateGraph::<Run>::new()
        .add_node("Collect", node)
        .add_node("Render", node)
        .add_node("Archive", node)
        .add_edge("Collect", "Render");
    if fixed {
        graph.add_edge("Render", "Archive")
    } else {
        graph
    }
}

/// Broken: the entry point names a node that doesn't exist. Fixed: it doesn't.
fn import_job(fixed: bool) -> StateGraph<Run> {
    let graph = StateGraph::<Run>::new()
        .add_node("Fetch", node)
        .add_node("Store", node)
        .add_edge("Fetch", "Store");
    if fixed {
        graph
    } else {
        graph.set_entry_point("Load")
    }
}

/// The error codes `.compile()` reports for a graph; empty if it compiles.
fn compile_codes(graph: StateGraph<Run>) -> Vec<String> {
    match graph.compile() {
        Ok(_) => Vec::new(),
        Err(StrataError::GraphInvalid(result)) => {
            result.errors.iter().map(|e| e.code.to_string()).collect()
        }
        Err(other) => fail(&format!("unexpected compile error: {other:?}")),
    }
}

/// 2-space indent and LF newlines, plus a final newline: the same bytes the
/// .NET twin writes.
fn to_json(definition: &GraphDefinition) -> String {
    serde_json::to_string_pretty(definition).unwrap() + "\n"
}

/// Writes every graph's export to `dir/<name>.json`, *without* compiling it.
fn export_all(dir: &Path, fixed: bool) {
    std::fs::create_dir_all(dir).unwrap();
    for (name, build) in GRAPHS {
        std::fs::write(
            dir.join(format!("{name}.json")),
            to_json(&build(fixed).to_definition()),
        )
        .unwrap();
    }
}

fn read_definition(path: &Path) -> GraphDefinition {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap())
        .unwrap_or_else(|e| fail(&format!("{} is not a definition: {e}", path.display())))
}

/// The `strata` binary: `STRATA_BIN` if set, else `strata` on the `PATH`.
fn cli() -> OsString {
    std::env::var_os("STRATA_BIN").unwrap_or_else(|| OsString::from("strata"))
}

/// Checks that the CLI starts, and says which version the demo drives.
fn prepare_cli() {
    let bin = cli();
    let version = Command::new(&bin)
        .arg("--version")
        .output()
        .unwrap_or_else(|e| {
            fail(&format!(
                "could not run `{}` ({e}). Download the strata CLI for your platform from \
                 {CLI_RELEASE} and put it on the PATH as `strata`, or set STRATA_BIN to its path",
                Path::new(&bin).display()
            ))
        });
    check(version.status.success(), "`strata --version` failed");
    println!(
        "Using the strata CLI: {}\n",
        String::from_utf8_lossy(&version.stdout).trim()
    );
}

/// `strata <args>` run from `cwd`, so paths in its report are short and relative.
fn strata(cwd: &Path, args: &[&str]) -> std::process::Output {
    Command::new(cli())
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap_or_else(|e| fail(&format!("could not start strata: {e}")))
}

/// What CI sees: the exit code, the human report, and the JSON report.
struct Gate {
    code: i32,
    human: String,
    stderr: String,
    json: Value,
}

fn gate(cwd: &Path, path: &str) -> Gate {
    let human = strata(cwd, &["graph", "validate", "--path", path]);
    let json = strata(cwd, &["graph", "validate", "--json", "--path", path]);
    Gate {
        code: human
            .status
            .code()
            .unwrap_or_else(|| fail("strata was killed by a signal")),
        human: String::from_utf8_lossy(&human.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&human.stderr).trim().to_string(),
        // Nothing on stdout: the CLI could not check the path at all.
        json: serde_json::from_slice(&json.stdout).unwrap_or(Value::Null),
    }
}

/// The error codes the CLI reported for `name.json`.
fn cli_codes(report: &Gate, name: &str) -> Vec<String> {
    let entry = report.json["graphs"]
        .as_array()
        .and_then(|graphs| {
            graphs.iter().find(|g| {
                g["path"]
                    .as_str()
                    .is_some_and(|path| Path::new(path).file_stem() == Some(name.as_ref()))
            })
        })
        .unwrap_or_else(|| fail(&format!("the CLI report has no entry for {name}")));
    entry["errors"]
        .as_array()
        .map(|errors| {
            errors
                .iter()
                .filter_map(|e| e["code"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn main() {
    let keep = std::env::args()
        .skip_while(|a| a != "--out")
        .nth(1)
        .map(PathBuf::from);

    let root = std::env::temp_dir().join(format!("strata-graph-gate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let graphs = root.join("graphs");

    prepare_cli();

    // 1. A broken pull request ------------------------------------------------
    println!("== 1. A broken pull request: seven graphs exported, one CLI call ==\n");
    export_all(&graphs, false);
    println!("$ strata graph validate --path graphs");
    let broken = gate(&root, "graphs");
    print!("{}", indent(&broken.human));
    println!("  (exit code {})\n", broken.code);
    check(
        broken.code == 1,
        "a PR with structural errors must fail the gate",
    );
    check(
        broken.json["checked"] == 7 && broken.json["invalid_count"] == 3,
        "expected 3 of 7 invalid",
    );

    println!("The CLI runs the same check as .compile() -- same diagnostics, every graph:\n");
    println!(
        "  {:<17} {:<29} strata graph validate",
        "graph", ".compile()"
    );
    for (name, build) in GRAPHS {
        let expected: Vec<String> = BROKEN
            .iter()
            .filter(|(broken_name, _)| *broken_name == name)
            .map(|(_, code)| code.to_string())
            .collect();
        let from_compile = compile_codes(build(false));
        let from_cli = cli_codes(&broken, name);
        check(
            from_compile == expected,
            &format!("{name}: .compile() gave {from_compile:?}"),
        );
        check(
            from_cli == expected,
            &format!("{name}: strata graph validate gave {from_cli:?}"),
        );
        println!(
            "  {:<17} {:<29} {}",
            name,
            shown(&from_compile),
            shown(&from_cli)
        );
    }

    // 2. A trap the export avoids ---------------------------------------------
    println!("\n== 2. Why export with to_definition(), not the `definition` field ==\n");
    println!("order-pipeline's ManualReview node is reachable only through Charge's");
    println!("fallback. The compiled graph's `definition` field holds just the edges the");
    println!("executor routes over, so it has no edge into ManualReview:\n");
    let compiled = order_pipeline(true)
        .compile()
        .unwrap_or_else(|e| fail(&format!("order-pipeline should compile: {e:?}")));
    let trap = root.join("trap");
    std::fs::create_dir_all(&trap).unwrap();
    std::fs::write(
        trap.join("routing-edges-only.json"),
        to_json(&compiled.definition),
    )
    .unwrap();
    std::fs::write(
        trap.join("to-definition.json"),
        to_json(&compiled.to_definition()),
    )
    .unwrap();
    let trap_report = gate(&root, "trap");
    print!("{}", indent(&trap_report.human));
    check(
        cli_codes(&trap_report, "routing-edges-only") == ["STRATA_ERR_ORPHAN"],
        "routing edges alone should look orphaned",
    );
    check(
        cli_codes(&trap_report, "to-definition").is_empty(),
        "to_definition() should be clean",
    );
    println!("\n  a false alarm on a graph .compile() accepted; to_definition() adds the");
    println!("  fallback as a guarded `on_failure` edge, so both agree.");

    // 3. An interrupt boundary in the file ------------------------------------
    println!("\n== 3. An interrupt boundary is in the file, with its reject route ==\n");
    let payout = read_definition(&graphs.join("payout-approval.json"));
    let approve = payout
        .nodes
        .iter()
        .find(|n| n.name == "Approve")
        .unwrap_or_else(|| fail("payout-approval has no Approve node"));
    let reject_edge = payout
        .edges
        .iter()
        .find(|e| e.condition.as_deref() == Some("on_reject"))
        .unwrap_or_else(|| fail("payout-approval has no on_reject edge"));
    println!(
        "payout-approval.json is schema_version {}: a level with a boundary.",
        payout.schema_version
    );
    println!("The Approve node, and the route a rejection takes, as exported:\n");
    println!("  {}", serde_json::to_string(approve).unwrap());
    println!("  {}", serde_json::to_string(reject_edge).unwrap());
    check(
        payout.schema_version == "1.6.0",
        "a level with an interrupt boundary is schema_version 1.6.0",
    );
    check(
        approve.interrupt.is_some(),
        "the boundary node carries `interrupt`",
    );
    check(
        reject_edge.from == "Approve" && reject_edge.to == "Refuse" && reject_edge.guarded,
        "the reject route is a guarded edge to Refuse",
    );

    println!("\n$ strata graph view --path graphs/payout-approval.json");
    let view = strata(
        &root,
        &["graph", "view", "--path", "graphs/payout-approval.json"],
    );
    let view_out = String::from_utf8_lossy(&view.stdout);
    print!("{}", indent(&view_out));
    check(view.status.success(), "graph view should read the file");
    check(
        view_out.contains("Approve: interrupt boundary, rejects to Refuse"),
        "graph view should name the boundary and its route",
    );
    println!("\n  Refuse has no ordinary edge in. Like ManualReview, it is reachable only through");
    println!("  a route, and the exported on_reject edge is what keeps it from looking orphaned.");

    // 4. The fix --------------------------------------------------------------
    println!("\n== 4. The fix: three small changes, same seven files ==\n");
    export_all(&graphs, true);
    println!("$ strata graph validate --path graphs");
    let fixed = gate(&root, "graphs");
    print!("{}", indent(&fixed.human));
    println!("  (exit code {})", fixed.code);
    check(fixed.code == 0, "the fixed graphs should pass the gate");
    for (name, build) in GRAPHS {
        check(
            compile_codes(build(true)).is_empty(),
            &format!("{name} should compile once fixed"),
        );
    }
    println!("\nAll seven also compile -- the gate said yes to graphs the runtime accepts.");

    // 5. When the gate can't check --------------------------------------------
    println!("\n== 5. When the gate can't check something, it doesn't pass ==\n");
    std::fs::write(
        graphs.join("notes.json"),
        "{ \"todo\": \"add more graphs\" }",
    )
    .unwrap();
    let unreadable = gate(&root, "graphs");
    println!("a file in graphs/ that isn't a graph definition:");
    print!("{}", indent(&unreadable.human));
    println!("  (exit code {})", unreadable.code);
    check(
        unreadable.code == 2 && unreadable.json["unreadable_count"] == 1,
        "a non-graph file must exit 2",
    );
    std::fs::remove_file(graphs.join("notes.json")).unwrap();

    // Someone edits the file by hand and drops Approve's `interrupt`, but not
    // its reject route. At a 1.6.0 level that is a shape no engine writes: the
    // CLI refuses the file (exit 2), and so does `GraphDefinition::check_shape()`,
    // the check it runs, with the same complaint.
    let edited = root.join("edited");
    std::fs::create_dir_all(&edited).unwrap();
    let mut hand_edit = read_definition(&graphs.join("payout-approval.json"));
    for node in hand_edit.nodes.iter_mut().filter(|n| n.name == "Approve") {
        node.interrupt = None;
    }
    std::fs::write(edited.join("payout-approval.json"), to_json(&hand_edit)).unwrap();
    let shape = gate(&root, "edited");
    println!("\na hand edit that drops Approve's `interrupt` but keeps its on_reject edge:");
    print!("{}", indent(&shape.human));
    println!("  (exit code {})", shape.code);
    check(
        shape.code == 2 && shape.json["unreadable_count"] == 1,
        "a boundary-less on_reject edge must exit 2",
    );
    match hand_edit.check_shape() {
        Ok(()) => fail("check_shape() should refuse the hand edit"),
        Err(message) => {
            println!("  Rust's GraphDefinition::check_shape() agrees: {message}");
            check(
                message.contains("on_reject") && message.contains("Approve"),
                "check_shape() should name the stray on_reject edge",
            );
            check(
                shape.human.contains(&message),
                "the CLI should refuse the file with check_shape()'s message",
            );
        }
    }

    std::fs::create_dir_all(root.join("empty")).unwrap();
    let empty = gate(&root, "empty");
    println!(
        "\nan empty folder:\n  {}  (exit code {})",
        empty.stderr, empty.code
    );
    check(empty.code == 2, "an empty folder must exit 2");

    let typo = gate(&root, "graphz");
    println!(
        "\na mistyped path:\n  {}  (exit code {})",
        typo.stderr, typo.code
    );
    check(typo.code == 2, "a mistyped path must exit 2");

    println!("\nExit codes: 0 every graph valid | 1 structural errors | 2 could not check.");

    if let Some(out) = keep {
        std::fs::create_dir_all(&out).unwrap();
        for (name, _) in GRAPHS {
            let file = format!("{name}.json");
            std::fs::copy(graphs.join(&file), out.join(&file)).unwrap();
        }
        println!(
            "\nFixed export kept in {} for diffing against the other runtime.",
            std::path::absolute(&out).unwrap_or(out).display()
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

fn shown(codes: &[String]) -> String {
    if codes.is_empty() {
        "ok".to_string()
    } else {
        codes.join(",")
    }
}

/// Each non-blank line of the CLI's output, indented by two spaces.
fn indent(text: &str) -> String {
    text.lines()
        .filter(|line| !line.is_empty())
        .map(|line| format!("  {line}\n"))
        .collect()
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
