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
- creation, start, handoff, and outcome events;
- exit status or a bounded failure reason; and
- one immutable handoff, when supplied.

Attempt states are `preparing`, `running`, `handed_off`, `succeeded`, `failed`,
`cancelled`, and `interrupted`. The durable run keeps its own lifecycle. For
example, a stopped attempt can be `handed_off` while its run remains
`waiting_for_input`, then a new attempt can start after the response re-queues
that same run.

The coordinator records an adapter's stable `runtime_adapter` name. The
currently shipped CLI uses `docker`; another executor implementing the same
request and cleanup boundary can resume the pinned run under a different name.
The profile, context, work item, credential grants, and source commit do not
change with the adapter. Runtime conformance and a second shipped adapter are a
later milestone.

## Handoff v1

The published contract is
[`schemas/handoff-v1.schema.json`](../schemas/handoff-v1.schema.json), with an
example in [`examples/handoff.json`](../examples/handoff.json). It contains only:

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
changed the run from `waiting_for_input` back to `queued`.

## Gateway flow

A profile that authorizes both `interaction.open` and `handoff.submit` follows
this sequence:

1. The worker opens a typed interaction. The gateway returns its durable ID and
   `agent_action: "submit_handoff_then_exit"`.
2. The worker submits a v1 handoff naming that interaction ID.
3. The gateway persists only the structured envelope and returns
   `agent_action: "exit"`.
4. The coordinator removes the container, mailbox, and mutable workspace and
   closes the attempt as `handed_off`.
5. A verified response marks the interaction answered and atomically re-queues
   the same run.
6. A replacement attempt reconstructs a clean workspace from the run's pinned
   commit. `run.describe` includes the latest handoff and answered interaction
   metadata; `interaction.read` transiently retrieves only the correlated
   response from Krab.

The clean-workspace rule is deliberate: mutable state from a compromised or
failed worker is not silently promoted into the next worker's authority.
Workers should ask before making changes they cannot safely reconstruct.

If a worker exits while paused without a handoff, the durable interaction is
preserved but the attempt is recorded as `interrupted`. If the orchestrator
restarts after a handoff but before normal attempt finalization, reconciliation
also records the attempt as interrupted while retaining both the wait and its
handoff. No live resource is inferred from the durable record.
