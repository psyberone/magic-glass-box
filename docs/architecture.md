# Architecture

## Ownership boundaries

Magic Glass Box is the trusted control plane. It owns local users, Krab
identity bindings, versioned project context, versioned agent profiles, run
lifecycle, credential policy, container lifecycle, and pending human
interactions.

Imitation Krab is the communications authority. It owns Krab users, tokens,
project membership and roles, sessions, items, work claims, and message
delivery. Magic Glass Box refers to those objects by stable IDs and does not
duplicate private message bodies or transcripts.

Docker workers are disposable and untrusted. A worker receives one
tracked-files-only snapshot, one pinned context version, one pinned profile
version, and one run-scoped gateway endpoint. A gateway-only worker never
receives reusable credentials. It also never receives the Docker socket, a Krab
admin token, the control-plane database, another run's workspace, or the source
repository's Git metadata.

## Trust flow

```text
operator/browser
      |
      v
Magic Glass Box ------ stable IDs / metadata ----> imitation-krab
      |
      +---- run-scoped typed capability ----> sandboxed worker
      |
      <---- structured events/questions -----+
```

Krab metadata is structurally validated. Work-item titles are retained as
`untrusted_text`; they may be displayed but must never be interpreted as agent
instructions. Bodies, notes, and transcripts are not ingested through the
project-activity projection.

## Durable model

- A local user can be linked to one or more Krab identities. The binding uses the
  immutable Krab `user_id`; handles are cached only for display.
- A credential reference points to an external secret source. The current
  implementation supports private token files and never persists the token.
- A provider grant is an immutable, content-addressed outbound policy with a
  named external credential reference. Creation and revocation are audited;
  expiry is enforced without storing the secret value.
- Project context revisions are append-only and content-addressed.
- Forge-neutral work-item imports are keyed by project, source, and external
  ID. Changed content appends an immutable version; identical imports are
  idempotent and stale source revisions fail closed.
- Agent profile revisions are append-only and contain an image reference,
  argv-form command, workspace/network policy, and a dedicated Krab identity.
- A run pins exact context, profile, and work-item revisions. Later imports or
  edits cannot change an in-flight or historical run.
- A work item owns a durable coordination thread shared by all of its runs.
  Append-only work-log entries retain orchestrator-supplied run, attempt,
  profile, and runtime provenance; they are bounded evidence, not authority.
- Coordinator checkpoints compact the log only through an exact observed head
  under optimistic concurrency. Assignments pin an exact profile revision and
  use reclaimable leases, so a dead worker does not own work forever.
- Work-item completion is an operator-only, optimistic transition that requires
  terminal runs, resolved assignments, and a final outcome. It seals rather
  than deletes the thread. Explicit reopen creates an empty next generation;
  imports alone never reactivate work.
- A provider-capable run also pins each exact grant ID named by its profile.
  Revocation and expiry remain live kill switches for already-pinned runs.
- The requested source revision is resolved to a full Git commit and persisted
  in the transition to `running`.
- Every run transition is appended to an immutable event stream and guarded by
  optimistic concurrency.
- A human interaction records only its type, lifecycle, recipient, run, and
  Krab item IDs. Its question and answer remain private content in Krab.
- Opening an interaction sends an idempotent Krab session item only after the
  run, pinned agent identity, session, and active human binding pass local
  preflight checks. The returned project, session, creator, recipient, status,
  version, event sequence, and optional work-claim link are verified before
  the local transition is accepted.
- Opening an interaction atomically moves its run to `waiting_for_input`.
  Answering records the response item ID and re-queues the durable run so a
  replacement worker can resume it; terminal run transitions cancel any open
  interaction.
- A response is accepted only when its project and session match the run and
  its route reverses the request route from the authorized human to the pinned
  agent identity. Its title, body, and risk flags exist in memory only for the
  caller to deliver onward; Magic Glass Box does not write them to SQLite.

The Krab POST and the local SQLite transition cannot be one transaction. The
caller must retain and reuse its stable idempotency key until local correlation
succeeds. A retry then resolves to the same Krab item rather than sending a
duplicate private request.

## Execution lifecycle

For an execution request, Magic Glass Box:

1. atomically claims a queued run by transitioning it to `preparing`;
2. resolves its requested revision and exports only tracked Git files;
3. validates the named authorities and opens the run-scoped gateway;
4. resolves the image reference to an immutable digest and creates the sandbox;
5. records the commit, image, and container identity before starting it;
6. waits for the agent, removes all labeled containers, gateway files, and the
   snapshot, then
   records the exit status and terminal state.

Operational failures become audited `failed` runs only after cleanup succeeds.
If cleanup cannot be proven, the run remains active so `run reconcile` can retry
after a restart. Reconciliation finds containers by product and run-ID labels
rather than trusting an in-memory container handle. It also recognizes the
retired label namespace so an upgrade can clean interrupted legacy containers.

The Docker boundary rejects images with Dockerfile-declared volumes, validates
host paths before mounting them, runs as the host's non-root numeric identity,
uses a read-only root filesystem, drops all capabilities, applies resource
ceilings, and bounds local logs. Both `none` and `gateway_only` use Docker
network policy `none`; `gateway_only` adds a private committed-file mailbox.
The mailbox directory and response files are read-only in the container. Only
the fixed request payload and request commit files are writable. Sequence,
size, and SHA-256 commitments prevent torn reads and stale response confusion.
The retired `krab_only` mode fails closed.

The gateway binds run identity outside the agent-controlled request. Its
allowlist comes from the immutable profile revision. The current operations
cover run description, interactions, handoff, provider invocation, and the
typed work-memory read/append/checkpoint/assignment lifecycle; none accepts a
run ID or credential value. Provider calls additionally pin the destination,
method, model, headers, byte limits, timeout, and concurrency in
administrator-owned policy. Opening an interaction tells a handoff-capable
worker to submit bounded replacement state and exit. Cleanup removes its
resources, and the durable run remains `waiting_for_input`. A verified response
re-queues the run, and a replacement worker may retrieve only the response
correlated to that run. See [agent-gateway.md](agent-gateway.md),
[provider-broker.md](provider-broker.md), and [work-memory.md](work-memory.md).

## Work-item boundary

The core accepts only the normalized v1 document described in
[work-items.md](work-items.md). It has no forge client and performs no outbound
work-item action. Source adapters run outside the control plane and may only
submit content; project selection, assignment, profiles, credentials, network
policy, and lifecycle remain trusted local decisions. Imported content is
therefore never an authority channel.

## Planned slices

Implementation is ordered around demonstrable product guarantees rather than
independent subsystems. See [roadmap.md](roadmap.md) for the current sequence and
exit proofs. The durable interaction path, run-scoped gateway, scoped provider
broker, structured worker replacement, multi-agent work memory, and work-item
completion lifecycle are complete. The next slice is the local control room: an
operator view over projects, authority, work threads, assignments, runs,
attempts, interactions, and recovery actions without private message bodies or
transcripts. See
[worker-handoffs.md](worker-handoffs.md) and [work-memory.md](work-memory.md).
