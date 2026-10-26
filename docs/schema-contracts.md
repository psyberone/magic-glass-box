# Published JSON Schema boundary

The files in `schemas/` are portable Draft 2020-12 structural contracts. They
describe object shape, required fields, scalar types, enums, item counts, and
the path and identity constraints that standard JSON Schema can express.
Examples in `examples/` must pass both those schemas and the corresponding
runtime parser.

Milestone 7B publishes separate contracts for the operator-authored gate-policy
request, the resolved immutable policy manifest, and review/validation evidence:
`gate-policy-request-v1.schema.json`, `gate-policy-v1.schema.json`, and
`gate-evidence-v1.schema.json`. The request permits mutable image references;
the durable policy contains only resolved immutable image IDs.

Milestone 7C adds `integration-fold-v1.schema.json`,
`landing-plan-v1.schema.json`, `landing-receipt-request-v1.schema.json`, and
`landing-receipt-v1.schema.json`. Exact command derivation, nonce hashing,
outcome-to-observed-revision binding, and durable ledger relationships remain
runtime checks; the portable schemas provide structural preflight.

Milestone 7D adds `inference-connector-v1.schema.json`,
`inference-grant-v1.schema.json`, `inference-invocation-request-v1.schema.json`,
and `inference-invocation-response-v1.schema.json`. Runtime validation also
checks exact connector/grant compatibility, UTF-8 byte ceilings, full response
correlation, outcome consistency, and absolute transport deadlines. Schema v16
makes those contracts the exclusive normal inference-dispatch path and records
the complete connector authority tuple with each durable invocation outcome.
Schema v17 adds a durable `dispatching` state that is committed before the
connector call and may be finalized exactly once, preserving uncertainty when
post-dispatch persistence fails.

Control-plane schema v18 adds immutable attempt-owner takeover events and
increments optimistic attempt authority on every expired-lease claim. This is
an internal SQLite transition contract, not a new portable JSON document.

Milestone 8B extends both published invocation-response schemas with optional
`retry_after_seconds`. Portable validation bounds it to 1 through 2,592,000
and permits it only on an explicitly `failed` outcome. Runtime validation
enforces the same rule before the control plane classifies the provider error;
the timing field by itself never makes an ambiguous call replayable. Control-
plane schema v20 adds the internal `paused` run state and immutable provider-
recovery identity ledger.

Control-plane schema v21 adds immutable attempt recovery-checkpoint selections
and append-only application receipts. These are internal SQLite records over an
existing bounded result manifest, not a new portable worker-authored document.

Control-plane schema v22 adds append-only landing-plan nonce generations and
allows successful reported provider accounting to exceed its pre-dispatch
reservation. The portable inference-response v2 contract permits usage only on
successful outcomes; failed and ambiguous outcomes must carry `null` usage.
Connector-owned request expansion remains bounded separately from the worker's
request-byte authority.

Milestone 10 publishes `service-sidecar-v1.schema.json`,
`service-operation-v1.schema.json`,
`service-operation-request-v1.schema.json`, and
`service-operation-response-v1.schema.json`. Runtime validation additionally
enforces exact registration/operation authority, recursive credential-field
rejection, UTF-8 input limits, revision lengths by Git object format, response
correlation, and outcome/receipt consistency. Control-plane schema v23 adds
immutable registrations and operations plus one-way durable invoke/reconcile
attempts; raw provider payloads are never part of that ledger. A successful
receipt may optionally carry one bounded normalized JSON object or array. The
result is stored with the receipt and covered by its durable digest; receipts
without a result retain their original canonical shape.

Control-plane schema v24 adds append-preserving project capability bindings,
binding events, and immutable operation-to-binding pins. Binding creation
requires a core-defined capability version and mode plus an exact advertised
sidecar mapping. Unbinding disables new project-routed operations; it does not
rewrite existing operation authority. Published service schemas enforce the
core-owned modes for the currently defined `work.get`/`work.list` v1,
`change.get`/`change.list` v1, and `checks.read` v1 reads plus `refs.publish`
v1, `change.open` v1, and `landing.invoke` v1 effects while still permitting
operator-only extension names that cannot be project-bound.

Milestone 11 adds `provider-landing-v1.schema.json` and
`provider-landing-receipt-v1.schema.json`. The authority binds one queue/policy/
candidate/evidence snapshot to the exact project capability binding and service
operation. The receipt binds the provider's normalized service receipt, actual
observed integration revision, closed-reference set, and truthful
`head_enforced` assurance. Control-plane schema v25 stores those documents and
allows a landing receipt to originate from exactly one manual plan or provider
landing.

Control-plane schema v26 adds an internal immutable binding for
changes-requested runs. The binding joins one run to the prior policy and
candidate, the current failed independent-review evidence, and the exact
candidate-pinned `review` work-log entry authored by that reviewer attempt. It
is a relational control-plane invariant rather than a portable interchange
document, so it adds no public JSON Schema.

Control-plane schema v27 adds the independent maintainer-disposition lane. The
portable input is published as
`schemas/maintainer-review-request-v1.schema.json`: one bounded `approved` or
`changes_requested` decision with summary and details. Storage binds that input
to the policy's complete exact-candidate tuple and records it append-only.
Schema v27 also adds an internal immutable binding from a correction run to the
current `changes_requested` maintainer review. That relational binding has no
portable interchange schema. Gate-policy v1 remains backward compatible: the
optional `maintainer_approval` member may only be present as `true`, so omitted
legacy policies retain their original canonical bytes and revision digests.

Control-plane schema v28 adds the immutable workflow-template definition,
saved-configuration versions, and exact workflow-execution pins used by M12A.
`schemas/workflow-configuration-v1.schema.json` publishes the operator-authored
configuration surface: review separation, optional maintainer gate, immutable
validation image IDs and commands, correction-round ceiling, and explicit
manual or provider landing mode. Producer and reviewer profile bindings remain
relational control-plane authority and are supplied separately; the runtime
requires their workspace modes and selected separation rule to agree with the
configuration.

Control-plane schema v29 adds the M12B director ledger: immutable workflow run
and gate bindings, typed exact-head review-verdict authority, correction-round
state, durable workflow rulings, and workflow-scoped supervisor actions. The
portable `schemas/work-log-entry-v2.schema.json` contract is deliberately
narrower than the general work-log v1 contract: it is only a `review`, requires
an explicit `approved` or `changes_requested` outcome and exact candidate
revision, and requires actionable details for changes requested. Runtime and
storage additionally prove that the author is the policy-bound reviewer
attempt for that exact head; portable schema validation alone cannot grant
review authority.

Control-plane schema v30 adds immutable workflow-to-landing bindings, explicit
provider targets, and workflow completion records. Landing plans and provider
operations continue to use their existing published contracts; workflow
authority is relational and is rechecked against the exact active execution,
version, configured mode, queue item, provider resource, and merge method at
issuance and closeout. Schema v30 also permits one `outcome` work-log entry
authored by the trusted control plane rather than a worker attempt. That entry
names its actor and workflow execution, and the database transaction that
creates it also records completion, seals the thread, and completes the
execution. No new worker-authored portable document is introduced.

Control-plane schema v31 adds an immutable workflow landing-chain record for a
bounded correction stack. It binds every workflow round's candidate and gate
policy from the execution's pinned source through the final reviewed head, and
records the exact chain digest before the integration queue can return from
`refold_required` to `queued`. This is relational control-plane authority, not
a new portable input. It is accepted only when all rounds are contiguous,
changes-requested provenance is exact, every round stays in one project/thread/
ref/object-format scope, the final gate remains ready, and the current
integration ref still equals the workflow source.

Control-plane schema v32 replaces the implementation prescription's fixed
producer/reviewer configuration columns with immutable named-role bindings.
The existing configuration manifest and digest are preserved exactly. Built-in
definitions now pass through one bounded generic parser and an exact local
catalog, while each director still refuses definitions it does not implement.

Control-plane schema v33 registers the fixed `discovery-v1` prescription and
publishes `discovery-configuration-v1.schema.json`,
`discovery-trace-v1.schema.json`, and `discovery-ruling-v1.schema.json`.
Discovery configurations pin distinct read-only investigator and coordinator
profile revisions plus bounded output ceilings. Runtime validation additionally
enforces UTF-8 limits, safe relative evidence paths, unique semantic keys, and
source-reference relationships. A `GO` ruling is the only disposition that may
carry a work-ready brief, and that brief contains at most five MUST items.

Control-plane schema v34 adds relational Discovery execution, run-binding,
trace, ruling, and event records; it does not add another portable document
contract. An execution pins the exact v33 inputs, Krab routing, and one human
decision-owner identity before either role runs. Trace and ruling rows retain
their producing run and active attempt, canonical digest, and idempotency key.
The gateway accepts them only from the configured role at its fixed stage and
source. `ASK_HUMAN` rulings bind an open decision-owner interaction, while a
final `GO` or `NO_GO` after a question must reference the latest answered
interaction. These are control-plane authority relationships beyond the
portable JSON schemas.

Control-plane schema v35 extends the private supervisor-action ledger with the
version-bound `discovery.advance` operation and an exact Discovery execution
foreign key. The table-level checks keep run, implementation-workflow, and
Discovery subjects mutually exclusive. This is durable control-plane state,
not a portable document or a new worker-facing capability. Existing actions
are preserved during migration with a null Discovery subject.

Control-plane schema v36 registers the fixed `advisory-review-v1`
prescription and publishes `advisory-configuration-v1.schema.json`,
`advisory-blind-review-v1.schema.json`,
`advisory-challenge-review-v1.schema.json`, and
`advisory-report-v1.schema.json`. Runtime validation additionally enforces
bounded UTF-8 artifacts, safe evidence paths, semantic-key uniqueness,
cross-reviewer references, distinct read-only role identities, and the final
report's literal `advisory_only` authority.

Control-plane schema v37 adds relational advisory execution, phase-bound run,
blind-review, challenge-review, report, and event records. One execution pins
the exact candidate and configuration inputs. Its seven runs preserve
structural blindness and exact active-attempt provenance without creating an
integration-gate relationship.

Control-plane schema v38 adds the version-bound `advisory.advance` supervisor
operation and exact advisory-execution foreign key. Table checks keep run,
implementation, Discovery, and advisory action subjects mutually exclusive.
The migration preserves existing action rows with a null advisory subject;
the new subject is private control-plane state, not a portable document or a
new source of review authority.

`workflow-scope-review-v1.schema.json` defines the bounded reviewer-authored
input for M13D scope discipline. It carries one result for every immutable
Discovery `MUST` plus optional base/head `PROBE` observations. It deliberately
does not accept a probe classification, gate result, or verdict. MGB derives
the four-state probe matrix mechanically: head-only findings are regressions,
base-and-head findings are follow-up proposals, base-only findings are
resolved, and absent-on-both probes are clean. A failed `MUST` or regression
requires changes; no other row can block. The portable artifact alone grants
no review authority. The relational acceptance path must still bind it to the
exact implementation execution, candidate, policy, reviewer run and attempt,
and verify its `MUST` keys against the pinned work-ready brief.

Schema validation is useful adapter-side preflight, but it is not an acceptance
decision. Magic Glass Box's runtime acceptance paths are authoritative and
additionally enforce invariants that portable JSON Schema cannot express,
including:

- UTF-8 byte limits rather than Unicode code-point counts;
- uniqueness by one property of an object, such as artifact path;
- strictly increasing supervision schedules;
- relationships between fields and records; and
- repository-relative, portable-identity symlink-graph containment at
  workspace capture and destination application.

An input that passes a published schema may therefore still be rejected by the
runtime boundary that would act on it. Context-dependent graph checks are not
claims that every in-memory record constructor re-audits already admitted
private state. The conformance tests deliberately cover both the shared
structural surface and these documented runtime-only guards; they do not claim
that two different validation languages accept identical input sets.
