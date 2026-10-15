# Integration candidates and gates

> **DESIGN ONLY — NOT IMPLEMENTED:** Except for the Milestone 7A candidate
> authority summarized below, this document specifies planned Milestones 7B
> and 7C. Magic Glass Box does not yet have a gate ledger, readiness decision,
> dependency/refold queue, manual landing plan, landing receipt, push, review,
> CI, approval, or merge actuator.

Magic Glass Box owns integration semantics: what exact Git tree is proposed,
what evidence applies to it, what must land first, whether the current base is
still the one that was validated, and what bounded receipt the actuator
returned. Provider-specific API calls and reusable credentials are outside this
boundary.

Milestone 7A implements only trusted candidate materialization. Milestones 7B
and 7C remain planned. See [candidates.md](candidates.md) and
[roadmap.md](roadmap.md).

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

### Independent review

Review must come from a separate run and satisfy an explicit separation policy.
A different run ID does not by itself prove independence: it may still be the
same producing profile or identity. The gate therefore records both producer
and reviewer provenance and evaluates the configured rule.

### Candidate and combined-tree validation

Candidate-only checks are insufficient when two individually valid changes
compose into an invalid integration tree. The readiness decision must validate
the tree as it would exist on the expected base.

Some checks described as “post-landing-only” can be reproduced against a folded
tree before landing; those are readiness gates. A check that truly cannot run
until after the integration ref moves is post-landing monitoring unless a
staging branch or provider merge queue supplies equivalent pre-landing
revalidation. It must not be represented as an already-passed readiness gate.

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
