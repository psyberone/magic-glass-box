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
