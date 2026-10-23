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
