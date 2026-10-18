# Worker attempts and handoffs

A run is durable work. A worker owns only one disposable attempt at that work.
Magic Glass Box can therefore stop a worker while a human thinks and later
launch a replacement without treating a container, process, or transcript as
durable state.

## Attempt ledger

Every execution claim creates a monotonically numbered attempt before any
workspace or container is prepared. An attempt pins the run's immutable agent
profile revision and records:

- the runtime adapter identity;
- the resolved source revision and container/image identity;
- its orchestrator owner, heartbeat, expiring lease, and cancellation request;
- creation, start, handoff, and outcome events;
- exit status or a bounded failure reason;
- one immutable handoff, when supplied;
- one immutable host-captured workspace result after the worker stops;
- one immutable recovery-checkpoint selection, when applicable; and
- one immutable receipt proving that selection was applied before launch.

Attempt states are `preparing`, `running`, `handed_off`, `succeeded`, `failed`,
`cancelled`, and `interrupted`. The durable run keeps its own lifecycle. For
example, a stopped attempt can be `handed_off` while its run remains
`waiting_for_input`, then a new attempt can start after the response re-queues
that same run.

The coordinator records an adapter's stable `runtime_adapter` name. Docker is
the sandbox executor, not that runtime identity. A profile selects a default
adapter and an operator may select another compatible adapter for a replacement
attempt. The profile, context, work item, credential grants, and source commit
do not change with the adapter. The bundled Codex and Claude Code dialects and
their conformance boundary are documented in
[runtime-adapters.md](runtime-adapters.md).

## Handoff v1

The published contract is
[`schemas/handoff-v1.schema.json`](../schemas/handoff-v1.schema.json), with an
example in [`examples/handoff.json`](../examples/handoff.json). The schema is
portable structural preflight; runtime-only semantic and UTF-8 byte guards are
described in [schema-contracts.md](schema-contracts.md). It contains only:

- a work-state enum and short summary;
- bounded decision and next-step lists;
- artifact path/description metadata; and
- correlated pending interaction IDs.

The canonical document is limited to 24 KiB. Unknown fields, duplicate list
entries, unsafe artifact paths, malformed interaction IDs, and non-finite JSON
are rejected. A handoff does not contain chat history, prompts, private message
bodies, response bodies, credentials, patches, or arbitrary provider payloads.
Artifact entries are metadata references, not copied file content.

Each attempt may submit one immutable handoff. Retrying the identical document
is idempotent; trying to replace it with different content is a conflict. Its
pending interaction IDs must be the interaction records opened by that active
attempt. This check still succeeds if a very fast human response has already
answered the interaction. While the attempt remains active, the run
deliberately stays `waiting_for_input`; finalizing that attempt performs the
re-queue.

## Gateway flow

A profile that authorizes both `interaction.open` and `handoff.submit` follows
this sequence:

1. The worker opens a typed interaction. The gateway returns its durable ID and
   `agent_action: "submit_handoff_then_exit"`.
2. The worker submits a v1 handoff naming that interaction ID.
3. The gateway persists only the structured envelope and returns
   `agent_action: "exit"`.
4. The coordinator captures the attempt's bounded file result, then removes the
   container, mailbox, and mutable workspace and closes the attempt as
   `handed_off`.
5. A verified response marks the interaction answered. If the attempt has
   already closed, the run queues immediately; otherwise attempt finalization
   performs the transition.
6. A replacement attempt reconstructs a fresh workspace from the run's pinned
   commit, stages that pristine baseline, then verifies and applies the prior
   handed-off result as its recovery checkpoint. The container cannot start
   until the application receipt is durable.
7. `run.describe` includes the latest handoff, answered interaction metadata,
   and bodyless checkpoint provenance; `interaction.read` transiently retrieves
   only the correlated response from Krab. When the run's pinned supervision
   policy enables automatic execution, the long-lived supervisor launches this
   replacement; otherwise an operator launches it explicitly.

## File checkpoint recovery

The mutable worker directory itself is never inherited. Recovery always starts
with a newly materialized snapshot of the exact pinned revision and overlays a
trusted result bundle only after that bundle crossed the capture boundary.
This preserves the clean-workspace rule while avoiding re-derivation of work
already captured from a stopped worker.

Only a latest `handed_off` or `interrupted` attempt result is eligible as new
recovery authority. A normal nonzero failure, cancellation, or success does not
promote its output. Each target attempt stores an immutable selection containing
the source attempt, result ID, source revision, digest, changed-path count, and
changed-byte count. A failed or cancelled replacement may carry its already
accepted input forward, but a successful replacement cannot resurrect that
older checkpoint.

The coordinator records the target's pristine baseline before applying the
checkpoint. Consequently, if the replacement later stops, its result contains
both the recovered edits and its own edits relative to the original commit.
The next replacement needs one bundle rather than a fragile stack of patches.
Source-revision, digest, and result-summary mismatches fail before the sandbox
starts. The operator checkout is never a recovery destination.

If a worker exits while paused without a handoff, the durable interaction is
preserved but the attempt is recorded as `interrupted`. If the orchestrator
restarts after a handoff but before normal attempt finalization, reconciliation
waits for the ownership lease to expire, stops labeled resources, captures any
remaining result, and records the attempt as interrupted while retaining the
wait and handoff. The captured result is then eligible for the same replacement
recovery path. No live resource is inferred from the durable record.
