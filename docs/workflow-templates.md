# Bounded workflow templates

**Status: M12A and M12B implemented; M12C next**

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

### M12C: landing and closeout director

- Enqueue only the exact ready candidate and preserve dependency/refold rules.
- Use either the configured provider binding or an explicitly issued manual
  plan; never fall back from one to the other implicitly.
- Complete the work item only after a matching landing receipt and bounded
  final outcome exist.
- Prove the entire flow on a real repository, including one changes-requested
  round, maintainer approval, exact-head landing, and restart recovery.

Each slice gets its own RED-first happy and sad path suite, branch-aware
coverage gate, focused mutation gate, and honest checkpoint. M12C's live proof
must not rely on private operator knowledge to move a durable transition.

## Deliberate non-goals

- arbitrary workflow graphs, expressions, loops, or visual composition;
- a public plugin marketplace;
- embedding GitHub, Jira, SSH, or model-provider mechanics in a template;
- copying agent transcripts into control-plane state;
- letting prompts define authority or stage completion;
- treating a three-agent advisory review as landing authority; and
- release-management or multi-repository workflows in the first pass.

Discovery, adversarial review configurations, heterogeneous work providers,
and controlled remote execution remain later milestones. They should reuse the
same immutable prescription seam only after `implementation-v1` proves it.
