# 13 — subgraph-composition

Subgraph composition (`design/15-Subgraph-Composition.md` and
`design/16-Nested-Interrupts.md` in [`datalium/strata`](https://github.com/datalium/strata)): a **support desk** whose
supervisor node routes each customer ticket to one of two specialist agents.
Each agent is a *compiled graph of its own*, with its own state type, added to
the desk with `.add_subgraph(...)`, so to the desk it is one ordinary node.

- `BillingAgent` (`BillingCase` state): `LookupInvoice` -> `IssueRefund` (only
  for a duplicate charge, a conditional edge *inside* the child) -> `DraftReply`.
  In scenario 6 a refund needs sign-off, so it gets an `AwaitApproval` step in
  front of `IssueRefund` that is an **interrupt boundary**
- `TechAgent` (`TechCase` state): `Diagnose` -> `ApplyFix` (a guarded self-loop
  until the problem is fixed) -> `DraftReply`. `Diagnose` is itself a subgraph
  (`Probe` state), so the desk nests **three graphs deep**

The desk's `Ticket` knows nothing about any of those state types. The whole
interface between a graph and its child is two closures: `input` builds the
child's starting state from the parent's, and `output` folds the child's
finished state back into the parent's.

`01`-`12` are single flat graphs. This is the first demo where a graph is a
*part* of another. The .NET twin is
[`strata-demos-dotnet/demos/13-subgraph-composition`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/13-subgraph-composition):
the same story, scenarios and checks.

## What it demonstrates

- `.add_subgraph(name, child, input, output)`: `child` is a `CompiledGraph<T>`
  (or an `Arc` of one, so one instance can back several nodes);
  `input: Fn(&S) -> T`; `output: Fn(&S, T) -> S`, which also receives the state
  the node started with
- **A supervisor is just conditional edges.** `Triage` classifies the ticket
  (keyword matching stands in for an LLM) and `.add_conditional_edge(...)`s pick
  the agent: the same routing as demo 04, with whole graphs as targets
- **The child runs as a real graph**: its own edges, conditional edges, loops
  and recovery policies all apply inside the node. Scenario 2's fix loop is
  invisible to the desk
- **Recovery composes.** A subgraph is a fallible node, so `RecoveryPolicy`
  works on it as in [`08-error-recovery`](../08-error-recovery):
  - **Scenario 3**: the child's *own* `retry(2)` absorbs two API timeouts; the
    desk's policy (`retry(0)`) never fires and no checkpoint is written
  - **Scenario 4**: the child's retries run out on a *transient* error, so the
    desk sees a transient failure. `retry(1)` re-runs the **whole child** (4 API
    calls = 2 runs x 2), then `.with_fallback("Escalate")` runs on the ticket from
    **before** the agent started, so none of the child's partial work leaks
- **Failures keep their kind, their path and their place (scenario 5).** A
  `NodeError::Fatal` in the nested diagnostics is fatal at every level: not
  retried by the child, `TechAgent` or the desk (`retry(3)` unused), and the
  `StrataError::NodeFailed` message names each level. With no fallback the desk
  persists a `Failed` checkpoint holding **its own** `Ticket`. That checkpoint
  nests `TechAgent`'s own `Failed` checkpoint (`checkpoint.nested`), which nests
  `Diagnose`'s, down to `CheckFirmware`, the node that failed
  (`checkpoint.waiting_at()` = `TechAgent/Diagnose/CheckFirmware`). Once the
  firmware is updated, `resume` on the desk re-enters `Diagnose` at
  `CheckFirmware` with the finding it had kept: `CheckNetwork` doesn't run
  again, and neither does `Triage`
- **A human in the loop, inside a child (scenario 6).** A child may have
  `.add_interrupt_boundary(...)`s of its own:
  - the boundary freezes the **whole run**. `run` returns `Interrupted` with the
    *subgraph node* (`BillingAgent`) as `suspended_node` and `next_node`, and the
    desk's frozen `Ticket` is the one from **before** the agent started
  - the desk's checkpoint **nests the agent's** (`checkpoint.nested`,
    `schema_version` `1.1.0`): thread `BillingAgent`, waiting at `AwaitApproval`,
    holding the agent's own steps so far
  - `resume` on the **desk** re-enters the agent where it stopped: the billing
    API is not called again and `LookupInvoice` does not re-run. `resume` takes
    `impl Into<LevelSignals>`, and a bare `HumanSignal` becomes
    `LevelSignals::new(signal)`: it answers the innermost level that froze. So
    its typed override
    (`HumanSignal::approve("dana@support.example").with_override("credit_cents", 500)`)
    goes to the agent's **own** state: `credit_cents` is a `BillingCase` field
    (set as a JSON number, not a string), and the desk's `Ticket` has none
  - the resume records a `HumanDecision` at the **agent's level**.
    `BillingAgent/IssueRefund` (an `add_async_node`, so it gets a `NodeContext`)
    reads the reviewer from `ctx.decision()`; the desk's `SendReply`, one level
    up, sees `None`. The demo checks each node's `ctx.node_path()`, the reviewer
    and the recorded overrides
  - the demo resumes with a brand-new desk and billing API, as a separate
    process would, and a second resume of the same checkpoint is refused
    (`StrataError::ExecutionAborted`)
- **The definition carries the children (step 7).** `desk.to_definition()`
  (`design/22-Graph-Structure-In-Definitions.md`) exports each subgraph node as
  `kind: "subgraph"` (`GraphNodeKind::Subgraph`) with the child's whole
  definition under `graph`, recursively. Each level has its own
  `schema_version`: `1.6.0` for `BillingAgent`, whose `AwaitApproval` boundary
  is exported as `interrupt` (`design/48-Boundaries-In-Definitions.md`), `1.1.0`
  for a level with a composite node, `1.0.0` for the plain `Diagnose`

## What it does *not* cover

- **Streaming inside a subgraph.** A child may stream: under `run_streaming` its
  chunks reach the desk's stream named `<subgraph>/<child node>`, nested to any
  depth, and a plain `run` of a desk whose child streams fails at the subgraph
  node before any of the child runs. None of these agents stream; see
  `design/15-Subgraph-Composition.md`, "Streaming inside a subgraph", and
  [`06-token-streaming`](../06-token-streaming) for streaming itself
- **Validating the exported definition.** Step 7 prints it but doesn't write
  it out. `strata graph validate` checks every level of such a file and names
  a child's errors by path (`TechAgent/Diagnose`); the CLI gate is
  [`14-graph-ci-gate`](../14-graph-ci-gate)
- **No durable store or CLI.** Everything uses the in-memory store, so the
  "other process" is simulated by building a new desk. With a SQLite store (as
  in [`05-hitl-interruption`](../05-hitl-interruption)) the checkpoint lands in
  the shared log and `strata thread inspect` shows where it waits
  (`BillingAgent/AwaitApproval`)
- **A child is never a thread of its own.** A child's freeze or failure is
  persisted only nested inside the desk's checkpoint. Only the outermost
  thread is loaded, resumed and listed
- **Answering several levels in one resume**
  (`LevelSignals::new(signal).at(&["BillingAgent"], other)`, or `.at(&[], ..)`
  for the desk), and a reject route inside a child. The reject route itself is
  in [`05-hitl-interruption`](../05-hitl-interruption). A child doesn't inherit
  its parent's decision; pass what it needs through `input`
- **Running a subgraph once per item of a list.** `.add_map(...)` does that:
  see [`18-map-fan-out`](../18-map-fan-out). Here the set of children is fixed
  when the desk is built (routing between them is dynamic)
- **No live service and no LLM.** Triage is keyword matching and the billing
  API is a counter, so the demo runs offline and prints the same thing every
  time

## Run it

```bash
cargo run -p subgraph-composition
```

Instant. Output of a real run:

```
=== Scenario 1: a billing ticket -- BillingAgent is a subgraph ===
    billing API call 1: ok
  Reply: We charged you twice and have refunded $49.99.
  Log:
    - triage: billing
    - billing: found invoice for $49.99 (charged twice)
    - billing: refunded $49.99
    - billing: drafted reply
    - reply sent
  The desk's Ticket has no invoice or refund fields -- the agent's own BillingCase did the work, and the output mapper folded its steps back in.

=== Scenario 2: a tech ticket -- TechAgent loops, and nests a subgraph ===
  Reply: Fixed after 2 attempt(s).
  Log:
    - triage: tech
    - tech: diagnostics: network ok
    - tech: diagnostics: firmware 2.1 ok
    - tech: diagnostics: 2 checks passed
    - tech: fix attempt 1: no change
    - tech: fix attempt 2: resolved
    - tech: drafted reply
    - reply sent
  Supervisor -> TechAgent -> Diagnose: three graphs deep, and the loop inside TechAgent is invisible to the desk.

=== Scenario 3: the billing API times out twice -- the child's own retry(2) ===
    billing API call 1: timed out
    billing API call 2: timed out
    billing API call 3: ok
  Completed after 3 billing API calls. The desk's policy (retry 0) never fired: the child handled it.
  Reply: We charged you twice and have refunded $49.99.

=== Scenario 4: the billing API is down -- child retry(1), desk retry(1) + fallback 'Escalate' ===
    billing API call 1: timed out
    billing API call 2: timed out
    billing API call 3: timed out
    billing API call 4: timed out
  4 billing API calls: the child makes 2 per run (1 + its retry), and the desk's retry re-ran the whole child once.
  Reply: A human agent will contact you shortly.
  Log:
    - triage: billing
    - escalated to a human
    - reply sent
  Escalate ran on the ticket from before BillingAgent started, so none of the child's steps are in the log.

=== Scenario 5: the device firmware is too old -- fatal in Diagnose, desk retry(3), no fallback ===
  Failed: 'TechAgent' gave up after 1 attempt(s):
    subgraph 'TechAgent' failed at 'Diagnose' after 1 attempt(s): subgraph 'Diagnose' failed at 'CheckFirmware' after 1 attempt(s): firmware 1.0 is too old to diagnose remotely
  The error names every level, and the fatal kind survived both hand-offs, so none of the 3 retries was used.
  Persisted checkpoint: Failed at 'TechAgent', error_kind = fatal.
  Its frozen state is the desk's Ticket from before TechAgent ran: log = ["triage: tech"]
  It nests each child's own Failed checkpoint (schema_version 1.1.0):
    thread 'TechAgent': Failed at 'Diagnose', state {"attempts":0,"fixed":false,"reply":"","steps":[]}
    thread 'Diagnose': Failed at 'CheckFirmware', state {"findings":["network ok"]}
  Failed at: TechAgent/Diagnose/CheckFirmware
  ... the device's firmware is updated, and the ticket is resumed ...
  Reply: Fixed after 2 attempt(s).
  Log:
    - triage: tech
    - tech: diagnostics: network ok
    - tech: diagnostics: firmware 2.1 ok
    - tech: diagnostics: 2 checks passed
    - tech: fix attempt 1: no change
    - tech: fix attempt 2: resolved
    - tech: drafted reply
    - reply sent
  0 network checks after the resume: it re-entered Diagnose at CheckFirmware, with the finding it had kept.

=== Scenario 6: a refund needs a human's approval -- an interrupt boundary inside BillingAgent ===
    billing API call 1: ok
  The whole run froze: suspended at 'BillingAgent', and a resume will re-enter 'BillingAgent'.
  The desk's frozen Ticket is the one from before BillingAgent started: log = ["triage: billing"]
  Persisted: schema_version 1.1.0, with the agent's own checkpoint nested inside:
    thread 'BillingAgent' waiting at 'AwaitApproval', its steps so far = ["found invoice for $49.99 (charged twice)","refund of $49.99 awaits approval"]
  ... a reviewer approves, and adds a $5.00 goodwill credit ...
  Reply: We charged you twice and have refunded $49.99. We have also added a $5.00 credit.
  Log:
    - triage: billing
    - billing: found invoice for $49.99 (charged twice)
    - billing: refund of $49.99 awaits approval
    - billing: refunded $49.99 and credited $5.00 (approved by dana@support.example)
    - billing: drafted reply
    - reply sent
  0 billing API calls after the resume: the agent picked up at IssueRefund, right after its boundary, instead of starting over.
  The override went to BillingAgent's own state ('credit_cents' is a BillingCase field) -- the desk's Ticket has no such field.
  BillingAgent/IssueRefund saw dana@support.example's approval at 'AwaitApproval'
  SendReply saw no decision
  The decision belongs to BillingAgent's level. The desk's own nodes don't see it.
  Resuming the same checkpoint again is refused: checkpoint for thread 'ticket-6' was already resumed

=== Step 7: the desk's exported definition carries each child's whole graph ===
  SupportDesk (schema_version 1.1.0): Triage, BillingAgent, TechAgent, GeneralReply, SendReply, Escalate
    BillingAgent (schema_version 1.6.0): LookupInvoice, IssueRefund, DraftReply, AwaitApproval [boundary]
    TechAgent (schema_version 1.1.0): Diagnose, ApplyFix, DraftReply
      Diagnose (schema_version 1.0.0): CheckNetwork, CheckFirmware, Summarize
  One definition describes all three levels, boundary included.

=== Done: a supervisor routed to child graphs with their own state, failures kept their kind, their path and their place, the desk's policy governed each child as one node, and a human's approval froze the desk mid-agent and resumed inside it. ===
```

The demo checks everything that must not vary (the logs, the API call counts,
the failure message, the persisted checkpoints at every level, who saw the
decision and with which overrides, the exported child graphs and their schema
versions) and exits non-zero if one changes. State fields print in snake_case
(`attempts`, `steps`, `credit_cents`) where the .NET twin's are PascalCase, and
policies are `retry(n)` rather than `Retry(n)`; otherwise the output is the
twin's, line for line.

## Files

- `src/main.rs` — the whole demo, single file
- `Cargo.toml` — package name is `subgraph-composition` (folder keeps the `13-`
  prefix; Cargo package names can't start with a digit). Depends on
  `datalium-strata`, `datalium-strata-memory` (for `InMemoryCheckpointStore`),
  `serde`, and `tokio` (`macros`, `rt-multi-thread`) for the async `main`
