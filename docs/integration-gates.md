# Integration candidates and gates

> **MILESTONES 7A–7C IMPLEMENTED:** Trusted candidates, exact-input gates,
> hard dependencies, deterministic refolds, a serialized integration queue,
> argv-only manual Git plans, and nonce-bound receipts are implemented. MGB
> still performs no push, forge query, pull-request operation, or remote merge.

Magic Glass Box owns integration semantics: what exact Git tree is proposed,
what evidence applies to it, what must land first, whether the current base is
still the one that was validated, and what bounded receipt the actuator
returned. Provider-specific API calls and reusable credentials are outside this
boundary.

Milestone 7A implements trusted candidate materialization, Milestone 7B the
exact-input gate ledger, and Milestone 7C the provider-neutral integration
queue and manual landing actuator. See
[candidates.md](candidates.md) and [roadmap.md](roadmap.md).

## Candidate authority

A candidate starts as an immutable Git commit and tree produced from one pinned
base plus one verified retained result. Work-log `candidate_revision` text is
coordination evidence, not candidate authority. The candidate ledger binds the
trusted object IDs to their producing result, run, attempt, context revision,
project, and work thread.

Later integration authority must bind the full tuple:

- service/repository identity;
- integration ref;
- expected base commit;
- candidate head and tree;
- gate-policy revision; and
- the operation and actor or supervisor authority using it.

Changing any bound input produces new authority. Evidence for one tuple cannot
be replayed onto another.

## Exact-input gate ledger

A candidate cannot become ready until every gate required by its pinned policy
has current evidence. Evidence is append-only and records its producer, exact
inputs, bounded outcome, and creation time—not raw command output, provider
payloads, or agent transcripts.

Database schema v13 stores an immutable policy manifest and evidence stream.
Each policy binds the candidate, local project/repository identity, pinned
project-context revision and work thread, normalized integration ref, expected
base, candidate head and tree, Git object format, and policy revision. Policies
and candidates are immutable, so any affected input change creates a new tuple
and starts with no evidence; evidence is never carried forward by inference.
Readiness is derived from the newest evidence for every required gate, even
when the append-only stream is longer than an operator listing page.

### Three distinct inputs

MGB does not ask one prompt or one green check to stand in for product
judgment. A well-described project supplies three separate inputs:

1. The versioned project context is the durable charter: what the product is,
   what it is not, its design preferences, and its engineering discipline.
2. The immutable work-item version is the particular brief: what this change
   must accomplish and its constraints.
3. The exact-candidate gate policy is the acceptance process: independent
   review, validation, and—when selected—maintainer disposition.

Every run remains pinned to the context version it started with. Revising a
charter changes future runs; it does not silently rewrite historical candidate
authority. If product feedback applies to an already-produced candidate, the
maintainer-review record below carries that bounded exact-candidate judgment.

### Independent review

Review must come from a separate run and satisfy an explicit separation policy.
A different run ID does not by itself prove independence: it may still be the
same producing profile or identity. The gate therefore records both producer
and reviewer provenance and evaluates the configured rule.

`gate review-create` durably binds a normal agent run to one policy and
candidate. The run coordinator reconstructs and snapshots that candidate from
the private candidate store rather than resolving the same object ID in the
operator checkout. Retries preserve the binding; reassignment rechecks the
separation rule. Review evidence is accepted only from a successful attempt of
such a bound run. A generic run that merely reports the same source revision is
not review authority.

Review profiles are required to use `workspace_mode: read_only`. That check is
performed when review runs and replacements are created and again when review
evidence is accepted; it is not merely a reviewer prompt convention.

A reviewer requesting changes records actionable feedback as a `review`
work-log entry before its attempt exits, with `candidate_revision` set to the
exact reviewed head. After failed evidence is recorded, `gate changes-create`
requires that current failed evidence and the note authored by its exact
reviewer attempt. It creates an ordinary developer run whose workspace starts
at the changes-requested candidate, and exposes the selected review entry
explicitly through
`run.describe`. Retry or reassignment preserves this binding. The developer's
result becomes a new child candidate; the prior candidate, policy, and failed
evidence remain immutable, and the child requires a fresh policy and
independent review. Cache recovery follows the durable changes-requested
ancestry so a corrected candidate can still be reconstructed when its parent
is an MGB-generated commit rather than a commit present in the source checkout.

### Maintainer product disposition

Schema v27 adds an optional `maintainer-approval` gate. It is deliberately not
agent-review evidence. An operator records a bounded `approved` or
`changes_requested` disposition against one policy's full exact-input tuple.
Summary and actionable details are retained; no agent transcript or provider
payload is involved. Dispositions are immutable and append-only, while only the
latest one determines readiness.

`changes_requested` means the candidate remains viable but is not authorized
to land. `gate changes-create` can bind a developer run directly to that exact
maintainer record, and `run.describe` exposes its source and bounded feedback.
Retry and reassignment preserve the binding. The resulting child candidate
requires an entirely fresh policy, review, validation, and maintainer
disposition.

The current authority model is intentionally single-operator: any active local
MGB user with access to the private control-plane state may record the
disposition. It does not yet model repository-specific maintainer roles or a
multi-person approval quorum. The redacted control room exposes gate state and
record identity, not review text.

### Candidate and combined-tree validation

Candidate-only checks are insufficient when two individually valid changes
compose into an invalid integration tree. The readiness decision must validate
the tree as it would exist on the expected base.

Local validation covers the policy's exact candidate or folded tree. Each configured
command is pinned to an immutable Docker image ID and executes with a read-only
workspace, Docker network mode `none`, no gateway, and no credentials. The
ledger retains exit status, duration, image and command identities, and a hash
and byte count of output; output itself is discarded. A folded policy resolves
the derived base/head/tree for both validation and independent-review
workspaces, including deterministic recovery after private-cache loss.

Some checks described as “post-landing-only” can be reproduced against a folded
tree before landing; those are readiness gates. A check that truly cannot run
until after the integration ref moves is post-landing monitoring unless a
staging branch or provider merge queue supplies equivalent pre-landing
revalidation. It must not be represented as an already-passed readiness gate.

## Operator surface

Create and inspect an exact policy from the published request contract:

```console
magic-glass-box gate policy-create cnd_<id> \
  --policy examples/gate-policy-request.json --created-by operator
magic-glass-box gate policy-show gpl_<id>
magic-glass-box gate policy-list --candidate cnd_<id>
```

Create a candidate-backed reviewer, execute it manually or through an opted-in
supervision policy, and record the bounded decision:

```console
magic-glass-box gate review-create gpl_<id> --profile Reviewer \
  --requested-by operator --expected-policy-revision <sha256>
magic-glass-box run execute run_<id>
magic-glass-box gate review-record gpl_<id> --reviewer-attempt att_<id> \
  --outcome passed --recorded-by operator \
  --expected-policy-revision <sha256>
```

When review fails, return the exact candidate and durable changes-requested
entry to a developer profile:

```console
magic-glass-box gate review-record gpl_<id> --reviewer-attempt att_<id> \
  --outcome failed --recorded-by operator \
  --expected-policy-revision <sha256>
magic-glass-box gate changes-create gpl_<id> \
  --review-evidence gev_<id> --review-entry wle_<id> --profile Producer \
  --requested-by operator --expected-policy-revision <sha256>
magic-glass-box run execute run_<id>
magic-glass-box candidate create att_<id> --created-by operator
# create a fresh policy and independent review for the new cnd_<id>
```

When the policy includes `"maintainer_approval": true`, record product judgment
through its separate published request contract:

```console
magic-glass-box gate maintainer-record gpl_<id> \
  --review examples/maintainer-review-request.json --recorded-by operator \
  --expected-policy-revision <sha256>
magic-glass-box gate maintainer-list gpl_<id>
magic-glass-box gate changes-create gpl_<id> \
  --maintainer-review mrv_<id> --profile Producer \
  --requested-by operator --expected-policy-revision <sha256>
```

Agent-review feedback and maintainer feedback are mutually exclusive sources
for one correction run; MGB never manufactures one kind of provenance from the
other. An item may be queued for visibility while gates are pending, but claim,
manual-plan, and provider-landing authority all recheck readiness and remain
closed until the latest maintainer disposition is `approved`.

The repository also contains
`scripts/check_live_imitation_krab_beta.py`, an explicitly opted-in real-account
proof of this choreography. Its connector invocation limit applies to the
whole sidecar process across every seat, while the provider grant supplies the
durable monetary ceiling. If the connector disappears after a producer has
created a candidate, `--resume-review-run run_<failed-review>` starts an audited
review retry against the retained exact candidate instead of rerunning the
producer. The proof prints push and pull-request commands as handoff material
but never executes them.

`scripts/check_live_imitation_krab_maintainer_round.py` continues an existing
beta candidate from a bounded external `changes_requested` document. It
replays the feedback through the developer seat, performs a fresh independent
review and local validation, and then stops honestly at pending maintainer
approval. It neither approves on the maintainer's behalf nor emits landing
authority.

Run a pinned local check and inspect current state:

```console
magic-glass-box gate validate gpl_<id> unit --recorded-by operator \
  --expected-policy-revision <sha256>
magic-glass-box gate evidence-list gpl_<id>
magic-glass-box gate readiness gpl_<id>
```

The request and durable manifest/evidence shapes are published in `schemas/`.
`make gate-test` repeats the real Git/SQLite candidate, review, recovery, and
gate-ledger proof without requiring a live hosting service.

Queue a ready policy, lease its ref lane, and obtain a one-time manual plan:

```console
magic-glass-box integration enqueue gpl_<id> --created-by operator
magic-glass-box integration claim iqm_<id> --owner supervisor-1 \
  --expected-version <version> --lease-seconds 60
magic-glass-box integration plan iqm_<id> --owner supervisor-1 \
  --expected-version <claimed-version> --remote origin --issued-by operator
```

Repeating `enqueue` with the exact policy is idempotent. Supplying another
policy for a candidate that already has a queue item fails explicitly; MGB
never silently substitutes the queue item's earlier or folded policy for the
policy the operator named.

Before a plan is issued, queued, claimed, or refold-required work may instead
be cancelled explicitly:

```console
magic-glass-box integration cancel iqm_<id> \
  --expected-version <version> --cancelled-by operator
```

`--remote` is a syntactically bounded Git remote name, never a URL or
credential-bearing transport string. MGB does not read the operator checkout's
remote configuration; the operator must run the returned commands in the
intended checkout where that name is configured. The `plan` response contains
two argv arrays and the only copy of the raw receipt nonce. Execute each array
directly; do not join it into shell text. Then submit a document matching
`landing-receipt-request-v1.schema.json`:

```console
magic-glass-box integration receipt \
  --document landing-receipt-request.json --recorded-by operator
```

After a landed receipt advances the durable ref, every remaining sibling is
`refold_required`. Rebuild it from the original result, create an entirely new
fold-bound policy, collect fresh evidence, and activate it:

```console
magic-glass-box integration refold iqm_<id> \
  --expected-version <version> --created-by operator
magic-glass-box gate policy-create cnd_<id> --fold ifd_<id> \
  --policy gate-policy-request.json --created-by operator
# run review and validation for the new gpl_<id>
magic-glass-box integration activate iqm_<id> --fold ifd_<id> \
  --policy gpl_<id> --expected-version <version> --actor operator
```

Hard ordering is explicit and cycle-checked:

```console
magic-glass-box integration dependency-add wki_<dependent> wki_<prerequisite> \
  --created-by operator
magic-glass-box integration dependency-list --project project-key --active-only
```

`make integration-test` runs the ledger failures and a real local-bare-remote
proof in which A lands, B is invalidated and refolded, and B lands with the
combined tree.

### Base-advance invalidation

The initial correctness rule is intentionally conservative: land one candidate
at a time, then invalidate and re-fold every remaining sibling onto the new
integration head. Review and validation run again against the resulting exact
head/tree.

This already covers changelogs, manifests, generated indexes, and other
semantically contended paths even when Git reports no textual conflict. A future
selective-invalidation optimization may model contended paths explicitly, but
it must prove equivalence to the refold-all rule before weakening it.

### Work-item dependencies

Hard dependency order is durable state, not an inference from file diffs or
message text. If B depends on A, B cannot receive landing authority until A has
a verified landing receipt. Cycles are rejected. Cost-aware ordering among
otherwise independent candidates is optional future optimization.

## Landing is control-plane work

Workers may produce results and review evidence, then exit. Waiting for review,
validation, an integration lease, or a remote provider may outlive any worker.
Landing therefore advances through durable control-plane actions rather than a
worker-held capability. M7C exposes these actions through the CLI; automatic
queue driving is a later supervisor integration.

The landing lease serializes the expected integration ref. Before issuing an
actuator plan, the supervisor rechecks dependencies, gate evidence, expected
base, head/tree, and policy revision in one authority decision.

## Manual actuator and truthful assurance

Until a provider add-on is explicitly bound to the integration queue, the state
machine stops at a structured human-executable plan. M10's generic service-
sidecar ledger does not by itself confer landing authority. M11's provider
landing path now translates this queue's already-verified exact inputs into a
project-bound service operation; the manual path remains available when no
provider is configured. Arguments
and body files are represented separately; MGB does not emit an interpolated
shell program containing untrusted text. The plan contains the evidence/request
digest and every revision known at authorization time. Its issuance response
carries a separate one-time nonce; only the nonce hash is retained.

The imported receipt must bind that nonce and report what the actuator says it
did. MGB validates the receipt against the issued authority and records the
assurance supported by MGB's evidence; without a dedicated actuator
integration, it does not independently observe the remote forge. The assurance
model distinguishes these levels:

- **base-and-head enforced** — the actuator atomically required both expected
  values;
- **head enforced** — a provider actuator fixed the candidate head but could
  not atomically compare the expected base; or
- **operator attested** — the provider offered no machine-verifiable
  precondition through the selected path.

Manual receipt schema v1 recognizes `base_and_head_enforced` for legacy or
future machine-observed imports and `operator_attested` for the current manual
path. The separate provider-landing receipt v1 contract records
`head_enforced`. These levels are not interchangeable. In particular, GitHub
CLI's
`--match-head-commit` requires the pull-request head to match; its documented
contract does not include an expected-base parameter. A plan using that option
must not claim base-and-head compare-and-swap. See the
[GitHub CLI merge contract](https://cli.github.com/manual/gh_pr_merge).

If the one-time nonce is lost before a receipt is recorded, reissue only the
nonce authority without releasing the serialized landing lane:

```console
magic-glass-box integration plan-reissue lpl_<id> \
  --expected-generation <generation> --reissued-by operator
```

Reissue appends an immutable nonce generation, returns the new raw nonce once,
and immediately invalidates every earlier nonce. It does not rebuild the plan,
change its authorized revisions, or allow a sibling queue item to pass it.
Manual receipt imports are recorded as **operator attested** because MGB does
not observe the remote ref. The plan's **base-and-head enforced** value describes
the generated `--force-with-lease` actuator contract; it is not promoted into a
claim that MGB independently verified execution.

## Provider actuator

When a project binds `landing.invoke` v1, `integration provider-issue`
atomically rechecks the lane, dependencies, gates, expected base, candidate
head/tree, and policy revision; resolves the active project binding; creates
the exact service operation; and moves the queue item to `awaiting_receipt`.
The binding and service operation are immutable pins, so a later unbind cannot
retarget already-issued authority.

`integration provider-execute` drives the ordinary invoke/reconcile lifecycle
and settles its normalized receipt. A denied or conflicting operation releases
the queue back to `queued`. A successful provider response is considered
landed only when its own remote identity appears in its provider-confirmed
closed-reference set. The queue advances to the provider's actual observed base
revision, not blindly to the candidate head. See
[github-provider.md](github-provider.md).

## Receipts

A landing receipt is bounded, immutable metadata containing the request digest,
assurance level, actuator identity, outcome, and observed integration revision.
Provider-confirmed closed work references may be attached after a
credential-bearing connector reads them; they are never inferred from proposed
change-body prose.

The generic Git actuator accepts `landed` only when the observed ref is the
planned head. A `not_landed` receipt must report a valid object ID in the
plan's object format and must not report the planned head. When it reports the
expected base, the item returns to `queued` without advancing the ref. When it
reports a third revision, MGB records that revision as the newly observed
integration baseline and moves the item and every queued sibling to
`refold_required`. The receipt therefore resolves issued authority truthfully
without calling drift either success or an unchanged failure. Provider
landings apply the same drift/refold rule using the sidecar's normalized actual
revision and retain uncertain effects for bounded reconciliation.

## Deliberate deferrals

- Additional provider CI, approval, and closed-reference implementations wait
  for connectors implementing the same bounded semantic capabilities.
- Selective sibling invalidation and cost-aware ordering wait until the strict
  serialized/refold-all path is proven.
- Multi-operator role policy is not implied by the current single-operator
  local deployment. It requires its own durable authority design.
- No integration state silently synchronizes imported work-item content back to
  its source service.
