# Integration candidates and gates

> **MILESTONES 7A AND 7B IMPLEMENTED:** Trusted candidates, exact-input gate
> policies, candidate-backed independent-review runs, local validation, bounded
> evidence, and derived readiness are implemented. The dependency/refold queue,
> manual landing plan, landing receipt, push, remote review/CI/approval query,
> and merge actuator remain design-only Milestone 7C work.

Magic Glass Box owns integration semantics: what exact Git tree is proposed,
what evidence applies to it, what must land first, whether the current base is
still the one that was validated, and what bounded receipt the actuator
returned. Provider-specific API calls and reusable credentials are outside this
boundary.

Milestone 7A implements trusted candidate materialization and Milestone 7B
implements the exact-input gate ledger. Milestone 7C remains planned. See
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

### Candidate and combined-tree validation

Candidate-only checks are insufficient when two individually valid changes
compose into an invalid integration tree. The readiness decision must validate
the tree as it would exist on the expected base.

Milestone 7B local validation covers the single-candidate tree. Each configured
command is pinned to an immutable Docker image ID and executes with a read-only
workspace, Docker network mode `none`, no gateway, and no credentials. The
ledger retains exit status, duration, image and command identities, and a hash
and byte count of output; output itself is discarded. Combined-tree validation
after dependencies and refolding belongs to Milestone 7C.

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
Landing therefore advances through durable supervisor actions rather than a
worker-held capability.

The landing lease serializes the expected integration ref. Before issuing an
actuator plan, the supervisor rechecks dependencies, gate evidence, expected
base, head/tree, and policy revision in one authority decision.

## Manual actuator and truthful assurance

With no service sidecar configured, the state machine stops at a structured
human-executable plan. Arguments and body files are represented separately; MGB
does not emit an interpolated shell program containing untrusted text. The plan
contains a nonce/request digest and every revision known at authorization time.

The imported receipt must bind that nonce and report what was actually done.
The control plane records the assurance that the actuator established:

- **base-and-head enforced** — the actuator atomically required both expected
  values;
- **head enforced** — the candidate head was fixed but the base was not; or
- **operator attested** — the provider offered no machine-verifiable
  precondition through the selected path.

These levels are not interchangeable. In particular, GitHub CLI's
`--match-head-commit` requires the pull-request head to match; its documented
contract does not include an expected-base parameter. A plan using that option
must not claim base-and-head compare-and-swap. See the
[GitHub CLI merge contract](https://cli.github.com/manual/gh_pr_merge).

## Receipts

A landing receipt is bounded, immutable metadata containing the request digest,
assurance level, actuator identity, merge method, and landed integration
revision. Provider-confirmed closed work references may be attached after a
credential-bearing connector reads them; they are never inferred from proposed
change-body prose.

Provider data may settle asynchronously. Receipts may therefore remain pending
until bounded reconciliation confirms the landed ref and closing effects. An
unknown outcome is not silently converted into failure or success.

## Deliberate deferrals

- Remote CI, provider approvals, and provider-confirmed closed references wait
  for a connector implementing the relevant semantic capabilities.
- Selective sibling invalidation and cost-aware ordering wait until the strict
  serialized/refold-all path is proven.
- Multi-operator role policy is not implied by the current single-operator
  local deployment. It requires its own durable authority design.
- No integration state silently synchronizes imported work-item content back to
  its source service.
