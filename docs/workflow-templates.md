# Bounded workflow templates

**Status: M12A-M12C and M13A-M13C complete; M13D in progress**

Magic Glass Box already has the durable primitives needed to build, review,
correct, approve, and land an exact candidate. The first Imitation Krab beta
proved those primitives by driving them manually. Workflow templates make that
choreography reusable without turning MGB into a general workflow engine.

A template is a fixed, versioned prescription over ordinary MGB objects. It
does not execute provider-native commands, grant authority, interpret arbitrary
graphs, or replace the candidate, gate, run, work-memory, interaction,
supervision, and integration ledgers.

## Three records, three jobs

| Record | Purpose |
| --- | --- |
| Template definition | Names a fixed topology, roles, stage contracts, allowed configuration fields, and completion rules |
| Saved configuration | Binds each role to an exact agent-profile version and selects permitted budgets and policy options |
| Workflow execution | Pins one definition revision, configuration revision, project context, work-item version, work thread, and source revision |

Definitions and configurations are immutable by version. Editing either
creates a new version; an active or historical execution never changes under
it. A configuration can reduce authority but cannot add credentials, network,
filesystem access, provider grants, capabilities, or limits absent from project
and profile policy.

Schema v32 makes those role pins prescription-generic. A configuration version
owns an immutable set of named role bindings rather than fixed producer and
reviewer columns, and each binding still points to one exact agent-profile
version. Built-in definitions are parsed through a bounded generic contract but
must also match MGB's exact local catalog. This is a storage and validation
seam, not permission to execute an unknown state machine: the implementation
director explicitly rejects every definition except `implementation-v1` before
it creates execution state.

Schema v33 registers `discovery-v1` against the same ledger. Its investigator
and coordinator roles are both read-only, distinct by profile and Krab
identity, and pinned to role-specific gateway capabilities. Registration does
not make it executable through the implementation director; its separate
bounded director is implemented independently in schema v35.

Schema v34 adds the Discovery execution ledger without widening the
prescription. One execution pins its definition and configuration revisions,
both role/profile revisions, project context, work-item version, work-thread
generation, source revision, Krab session and optional claim, and the exact
requester's Krab identity as decision owner. Append-only run bindings, typed
trace and ruling records, and transition events preserve exact attempt
provenance. The investigator can submit only a trace and the coordinator only
a ruling through their explicitly granted gateway operations. An `ASK_HUMAN`
ruling must name the current open question or choice addressed to that pinned
decision owner; a later `GO` or `NO_GO` must follow the latest answered
interaction.

Schema v35 connects that ledger to the durable supervisor. One version-bound
`discovery.advance` action drives each investigate, coordinate, and completion
stage. The bounded director creates only the prescription's exact pinned role,
waits for a terminal run plus its typed evidence, and records the evidence IDs
and digests in the transition event. A failed role run or a successful run
without its required evidence fails the execution; a stale action is harmless.
The investigator receives no Krab routing, while the coordinator receives only
the already-pinned session and optional claim needed for durable human
questions. An active Discovery execution also excludes implementation on the
same work thread and prevents that thread from being closed.

Schema v36 registers `advisory-review-v1` without making it executable. Its
fixed topology has three mutually independent read-only reviewers and one
independent read-only coordinator, with required role capabilities and bounded
configuration. Published contracts describe blind findings, cross-reviewer
challenges, and a triangulated report whose authority is literally
`advisory_only`. The prescription has no correction loop and none of its
artifacts can satisfy a candidate gate, approve landing, authorize source
mutation, or stand in for maintainer judgment.

Schema v37 adds its immutable execution and evidence ledger. An execution pins
the exact candidate, configuration, project context, work-item version, work
thread, requester, and four role/profile revisions. The four identities are
bound to seven distinct runs: three structurally blind reviews, three fresh
challenge passes, and one triangulation coordinator. Typed artifacts retain
exact run and attempt provenance; stage transitions require all prescribed
runs and artifacts, and preserve their IDs and digests. Blind assignments
contain no peer evidence, challenge assignments contain all blind reviews but
no peer challenges, and only the coordinator sees both complete sets.
Advisory assignment data remains separate from candidate-gate assignment data,
and exact-candidate reconstruction does not create gate authority. At this
checkpoint the worker gateway, durable director, and operator surface were
still deliberately absent.

Schema v38 connects that ledger to the worker gateway and durable supervisor.
Three typed submission operations retain exact run and active-attempt
provenance without accepting run identity from the worker. One version-bound
`advisory.advance` action drives three blind runs, three fresh challenge runs,
one triangulation run, and completion. Failed runs and successful runs missing
their required artifacts fail explicitly; active runs defer without spending
retry budget; stale actions settle harmlessly. This makes the seven-run
sequence autonomous and restart-safe. Its dedicated operator CLI and
control-room projection complete the M13C surface without granting gate,
approval, landing, or maintainer authority.

The dedicated `discovery` CLI keeps this prescription distinct from the
implementation workflow while reusing the generic immutable ledger. It can
create, revise, show, and list Discovery-only configurations, then start, show,
list, or cancel an execution. Starting requires every ordinary immutable pin
plus the exact Krab session; a claim is optional but, when supplied, is pinned
before either role runs. No command accepts free-form topology or authority.
The control room completes the operator surface with aggregate lifecycle
counts, bounded active/history projection, failed-execution attention, durable
supervisor-action visibility, and a separate browser table. It exposes typed
evidence identity and provenance without exposing evidence bodies, prompts,
profile commands, credentials, requests, work-ready briefs, or failure text.

The dedicated `advisory` CLI applies the same bounded shape to advisory review.
It can create and revise four-role configurations, read only advisory
configurations from the shared ledger, and start, show, list, or cancel an
execution pinned to one exact candidate and work-thread generation. The
control room projects exact-candidate and role identity, seven-run phase
progress, final-report provenance, and advisory attention while excluding all
finding, challenge, and report prose. Both the command surface and browser
label the result as advisory-only; neither can satisfy a candidate gate or
authorize landing.

M13D begins with a separate bounded scope-review contract. The reviewer
reports pass/fail evidence for immutable Discovery `MUST` keys and only the
base/head presence of its own `PROBE` findings. It cannot submit a probe
classification or choose the gate result: MGB derives both. This keeps
Discovery scope, reviewer extrapolation, and gate authority distinct.

Schema v39 adds the first durable connection between prescriptions. An
implementation start may name one completed Discovery execution; MGB accepts
only its final `GO` ruling when every immutable project, context, work-item,
thread, and source pin matches. The producer and reviewer run assignment then
carry that exact work-ready brief and its ruling provenance. The binding does
not merge the prescriptions, auto-start implementation, or let Discovery
evidence satisfy an implementation gate.

The initial built-in definition is `implementation-v1`. It codifies the loop
that the live beta exercised:

```text
build -> exact candidate -> independent review -> correction (if requested)
      -> fresh candidate and fresh gates -> maintainer disposition
      -> landing -> bounded outcome -> work-item completion
```

This is a fixed state machine, not a user-authored DAG. Correction is the one
bounded loop. Its round ceiling is configuration, and crossing that ceiling
opens a durable ruling interaction instead of silently creating another run.

## Roles and authority

`implementation-v1` has three judgment roles:

- the **producer** changes the pinned source and emits a retained result;
- the **reviewer** receives a reconstructed exact candidate in a read-only
  workspace and records an exact-head verdict; and
- the **maintainer** supplies product disposition through the existing
  maintainer gate when project policy requires it.

The control plane owns mechanical transitions, idempotent retries, supervision,
and evidence plumbing. It does not manufacture any role's judgment. In
particular:

- producer claims never satisfy review or validation gates;
- a reviewer cannot review its own run, profile, or Krab identity when the
  selected separation rule forbids it;
- changes requested always produce a child candidate with fresh authority;
- technical readiness never implies maintainer approval;
- provider availability never implies permission to land; and
- text in a work item, work log, result, or provider response never changes the
  workflow definition or authority.

## Stage artifacts

Each stage ends with an existing authoritative object plus a small bounded
workflow transition record:

| Stage | Authoritative evidence |
| --- | --- |
| Build | Successful run result and immutable candidate |
| Review | Policy-bound independent-review evidence for that candidate |
| Correction | Exact failed evidence or maintainer disposition, correction run, and child candidate |
| Approval | Derived gate readiness including optional maintainer disposition |
| Landing | Queue authority plus manual or provider landing receipt |
| Completion | A bounded outcome tied to the landed candidate and receipt |

Transition records point to these objects; they do not copy provider payloads,
agent transcripts, issue bodies, or raw test output. Advancing a workflow and
creating its next durable action must be atomic or safely idempotent, so a dead
supervisor cannot create two reviewers, two correction runs, or two landings.

## Honest manual and external landing

The manual actuator remains first class. A workflow may issue a manual landing
plan and later accept its nonce-bound operator-attested receipt. A configured
provider may instead return provider-observed evidence through the existing
service-operation ledger.

MGB does not backfill authority after an operator bypasses both paths. It may
retain a bounded observation that an external change exists in a future
contract, but that observation cannot be relabeled as an MGB-issued landing
receipt or used to claim an enforcement level MGB did not provide.

This distinction was exercised by the first beta. The approved Krab candidate
was pushed and squash-merged by the operator without first entering MGB's
integration queue. Its exact tree can be compared with the merge commit, and
the work-item completion reason can describe what happened, but the empty
integration ledger remains truthful.

## First delivery slices

### M12A: immutable prescription ledger

**Implemented in schema v28.**

- Ship the built-in `implementation-v1` definition.
- Version saved configurations and pin exact profile revisions.
- Start a workflow execution against one active work thread and exact project,
  context, work-item, thread generation/version, and source revisions.
- Project a bounded status without exposing prompts, review text, credentials,
  or provider payloads.

### M12B: build/review/correction director

**Implemented in schema v29.**

- Ordinary producer and candidate-backed reviewer runs are pinned to the exact
  workflow execution, correction round, role, profile revision, work item, and
  context.
- A typed v2 review entry creates exact-head verdict authority only for the
  bound reviewer attempt. The director derives review evidence and transitions
  from that record; summary prose never selects the outcome.
- Passing review runs every configured immutable validation in the existing
  networkless read-only sandbox. Failed validation, failed workers, and missing
  verdicts open durable rulings rather than inventing success.
- Reviewer or maintainer changes return their exact evidence to the configured
  producer. The next result becomes one child candidate with a fresh gate,
  reviewer, validation set, and optional maintainer disposition.
- One version-bound `workflow.advance` supervisor action owns each logical
  build, review, or maintainer stage. Waiting defers without consuming retry
  budget; process death is recovered by the ordinary leased-action path.
- The configured correction ceiling stops the loop and opens an explicit
  ruling. CLI resolution may authorize one more bounded round or cancel the
  workflow; it cannot silently weaken another failure.
- A failed validation also stops at an explicit ruling. Continuing reruns only
  that same pinned validation against the same exact candidate, once; another
  failure requires another ruling and never becomes passing evidence.

### M12C: landing and closeout director

**Implemented in schemas v30-v31.**

- Enqueue only the exact ready candidate and preserve dependency/refold rules.
- Use either the configured provider binding or an explicitly issued manual
  plan; never fall back from one to the other implicitly.
- Complete the work item only after a matching landing receipt and bounded
  final outcome exist.
- Prove the entire flow on a real repository, including one changes-requested
  round, maintainer approval, exact-head landing, and restart recovery.

The director binds one integration-queue item to the exact workflow round,
candidate, and gate policy. Binding and the `workflow.landing_enqueued` event
commit together. Dependencies, serialized ownership, gate readiness, and
refold requirements remain properties of the existing integration queue; the
workflow does not reimplement or weaken them.

Correction rounds form a different shape from independent sibling work: each
child candidate is based on the exact changes-requested head before it. Schema
v31 records that complete contiguous, independently gated chain. The final
reviewed head may use the workflow's pinned source as its landing compare base
only while the observed integration ref still equals that source. Any unrelated
movement remains a real refold, with a new candidate and fresh gates. This keeps
the reviewed correction stack intact without turning chain acceptance into a
general bypass of the integration queue.

Manual mode deliberately stops for `workflow manual-plan`, then waits for the
ordinary nonce-bound receipt. A `not_landed` receipt releases the lane and the
control room requests an explicit new plan. Provider mode deliberately stops
until `workflow provider-target` has pinned one resource and merge method and
an active `landing.invoke@1` capability exists. Failed or provider-confirmed
`not_landed` attempts open a ruling bound to that exact provider landing; a
`continue` decision authorizes one retry and no more. Authority from the wrong
mode or target opens a non-continuable mismatch ruling.

After an exact matching `landed` receipt, the director enters `completion`.
One SQLite transaction rechecks the current gate and receipt, rejects active
runs or unfinished assignments, appends a bounded `outcome` with explicit
control-plane/workflow provenance, records the workflow completion, seals the
work thread, and marks the execution completed. A crash can therefore expose
the whole closeout or none of it.

The live exit proof completed on 2026-10-24 against Imitation Krab PR #3. It
exercised one changes-requested round, fresh independent review, maintainer
approval, repeated process replacement, immutable correction-chain recovery,
an explicit failed-landing ruling and one authorized retry, append-only
provider deployment rotation, provider-observed head-enforced landing, bounded
outcome creation, and sealed closeout. The target checkout remained clean;
worker networking stayed denied; provider credentials remained outside worker
plans. Already-successful publish and open effects were recovered from their
durable receipts rather than replayed after controller rotation.

Each slice passed its RED-first happy- and sad-path suite, branch-aware coverage
gate, focused mutation gate, and honest checkpoint. M12C's live proof moved
every mechanical transition through durable state; the two judgment points
remained explicit maintainer approval and the bounded landing-retry ruling.

## Deliberate non-goals

- arbitrary workflow graphs, expressions, loops, or visual composition;
- a public plugin marketplace;
- embedding GitHub, Jira, SSH, or model-provider mechanics in a template;
- copying agent transcripts into control-plane state;
- letting prompts define authority or stage completion;
- treating a three-agent advisory review as landing authority; and
- release-management or multi-repository workflows in the first pass.

The advisory contract, configuration, execution ledger, structural blindness,
worker gateway, bounded director, and non-gating operator surface are now
fixed. Heterogeneous work providers and controlled remote execution remain
later milestones. The completed
`implementation-v1` proof, schema-v32 named-role ledger, schema-v34 Discovery
evidence ledger, schema-v35 bounded director, and bounded Discovery operator
surface establish the immutable prescription seam they reuse.
