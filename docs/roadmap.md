# Execution roadmap

This roadmap is ordered by proof, not by calendar. Each milestone should leave
one demonstrable vertical slice and preserve the product contract in
[product-thesis.md](product-thesis.md).

## Foundation: reproducible sandboxed runs

**Status: complete**

- versioned project context and agent profiles;
- immutable run inputs and optimistic lifecycle transitions;
- tracked-files-only Git snapshots and pinned source revisions;
- digest-resolved, least-privilege Docker execution;
- named private-file credentials;
- Krab identity and metadata-only activity integration; and
- restart reconciliation and cleanup.

## Milestone 1: durable human interactions

Make a question part of durable work rather than a terminal call.

### 1A. Interaction ledger

**Status: complete**

- Introduce a versioned schema migration.
- Store interaction kind, state, recipient, run, and Krab object IDs.
- Never store question, answer, note, or transcript text.
- Couple opening, answering, cancellation, and run-state changes atomically.
- Enforce optimistic concurrency and one unresolved interaction per run.

### 1B. Krab transport

**Status: next**

- Add strictly validated, idempotent session-item write and read operations.
- Correlate request and response item IDs in Magic Glass Box.
- Verify creator, recipient, project, and session metadata before accepting a
  delivery as an interaction transition.
- Treat all titles and bodies as untrusted content.

### 1C. Agent interaction gateway

- Give a sandbox a run-scoped local capability, not a reusable Krab token.
- Accept typed `question`, `approval`, and `choice` requests.
- Route the request to the authorized human through Krab.
- Permit only the response for that run and interaction to return to the
  worker.

**Exit proof:** a run can wait for a private human response without Magic Glass
Box retaining its content, and the pending interaction survives an orchestrator
restart.

## Milestone 2: scoped authority broker

- Exchange a short-lived run capability for only the named credentials in the
  pinned profile.
- Implement the `krab_only` network path through a bounded local relay.
- Deny arbitrary bridge networking and direct access to administrator tokens.
- Record grants, revocation, and expiry as metadata without storing secret
  values.

**Exit proof:** a worker can use one permitted service while attempts to reach
other network destinations or credentials fail closed.

## Milestone 3: worker replacement and handoff

- Define a bounded, structured handoff envelope containing work state,
  decisions, artifacts, and pending interaction IDs—not a transcript.
- Re-queue interrupted waiting work after its response arrives.
- Resume from the pinned commit and immutable context with the same or a
  different runtime adapter.
- Distinguish attempts from the durable run so every worker start and outcome
  remains auditable.

**Exit proof:** terminate Agent A while it waits, answer later, and complete the
same run with Agent B.

## Milestone 4: local control room

- Show projects, users, roles, profiles, work claims, runs, and attempts.
- Make pending questions, approvals, owners, and blockers prominent.
- Expose policy and lifecycle metadata without private bodies or transcripts.
- Provide explicit cancel, retry, reassign, revoke, and reconcile actions.

**Exit proof:** an operator can understand and safely control every active run
without entering an agent container or reading its private conversation.

## Milestone 5: runtime adapter contract

- Publish the minimal adapter protocol and conformance tests.
- Support at least two meaningfully different headless agent runtimes.
- Keep runtime-specific prompts, flags, and output parsing outside the control
  plane.

**Exit proof:** the golden-path demonstration in the product thesis succeeds
with two interchangeable worker implementations.

## Continuous gates

Every milestone retains the existing RED-first workflow, happy-path and
sad-path coverage, at least 85% branch coverage, and at least an 80% mutation
kill rate. Security-sensitive boundaries also require a live integration test
against the real local dependency where practical.
