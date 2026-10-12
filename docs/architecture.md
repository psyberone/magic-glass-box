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

Optional integration sidecars are trusted credential actuators, not workers.
They contain provider-specific GitHub, GitLab, or other forge mechanics behind
a small versioned operation protocol. A user may explicitly expose a local
configuration directory such as `GH_CONFIG_DIR` to the selected sidecar only;
it is never mounted into an agent container or copied into control-plane state.
Magic Glass Box remains responsible for deciding whether a specific operation,
repository, account, and exact revision are authorized.

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
      |
      +---- bounded authorized operation ----> optional forge sidecar
                                                    |
                                                    v
                                             GitHub / GitLab / forge
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
  argv-form executable prefix, default runtime adapter, workspace/network
  policy, and a dedicated Krab identity. Legacy profiles default to the
  compatibility `command` adapter without changing their content digest.
- A run pins exact context, profile, and work-item revisions. Later imports or
  edits cannot change an in-flight or historical run.
- A run also pins one immutable supervision policy. The default is manual;
  automatic launch and each finite aging schedule require explicit opt-in.
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
- Each active attempt has one orchestrator owner, heartbeat, and expiring
  lease. Reconciliation may take over only after that lease expires; invoking
  recovery cannot tear down a live attempt owned by another process.
- A cancellation request is durable and audited. The owning coordinator polls
  it through the lease heartbeat, stops the labeled container, captures any
  bounded result, cleans resources, and only then records terminal
  cancellation.
- Every started attempt binds one immutable result manifest and its
  content-addressed blobs before its workspace is removed. Results are inert
  evidence until an operator explicitly applies them to a checkout.
- Every run transition is appended to an immutable event stream and guarded by
  optimistic concurrency.
- Supervisor actions bind an operation to one subject version and use their own
  expiring claim lease. They do not replace run or attempt authority: launching
  still requires the queued-run claim and recovery still requires an expired
  attempt lease. Escalations bind one condition and threshold, retain no
  private content, and use finite policy-owned retries.
- Retrying or reassigning terminal work creates a new queued successor and an
  event on its predecessor. Retry preserves the exact context, profile,
  work-item, source, provider-authority, and communication pins. Reassignment
  preserves the work identity but selects an explicit current profile revision
  and its current provider authorities. The shared work thread carries durable
  memory across both operations; historical runs are never reopened or
  rewritten.
- A human interaction records only its type, lifecycle, recipient, run, and
  Krab item IDs. Its question and answer remain private content in Krab.
- Opening an interaction sends an idempotent Krab session item only after the
  run, pinned agent identity, session, and active human binding pass local
  preflight checks. The returned project, session, creator, recipient, status,
  version, event sequence, and optional work-claim link are verified before
  the local transition is accepted.
- Opening an interaction atomically moves its run to `waiting_for_input`.
  Answering records the response item ID. If its attempt is still live, the run
  remains waiting until that attempt exits; attempt finalization then queues a
  replacement. If no attempt is active, the answer queues the run immediately.
  Terminal run transitions cancel any open interaction.
- A response is accepted only when its project and session match the run and
  its route reverses the request route from the authorized human to the pinned
  agent identity. Its title, body, and risk flags exist in memory only for the
  caller to deliver onward; Magic Glass Box does not write them to SQLite.

The Krab POST and the local SQLite transition cannot be one transaction. The
caller must retain and reuse its stable idempotency key until local correlation
succeeds. A retry then resolves to the same Krab item rather than sending a
duplicate private request.

## Persistence boundary

`magic_glass_box.db` is the stable compatibility facade. Its implementation is
split beneath `magic_glass_box.storage` into schema and migration ownership,
SQLite connection ownership, and stores for the catalog, work-item lifecycle,
work memory, runs, attempts, interactions and provider activity, retained
artifacts, supervision, and composite query projections. The storage split
preserved schema v9; autonomous supervision advances it to schema v10.

Each mutation continues to own its complete SQLite transaction. Composite read
projections open one explicit read transaction and pass that connection to
nested store operations; nested repositories never discover or open a second
connection implicitly. A projection therefore represents one database state,
including its base run, events, attempts, interactions, provider authority, and
work memory.

## Execution lifecycle

A long-lived supervisor periodically materializes queued execution, expired
ownership recovery, and orphan reconciliation as durable actions. Multiple
processes may run: SQLite action leases select one dispatcher, while the
existing run and attempt transitions remain the final authority. The same pass
records due aging thresholds and delivers open-interaction reminders through
Krab. See [supervision.md](supervision.md).

For an execution request, Magic Glass Box:

1. validates the profile default or explicit attempt-level runtime adapter;
2. atomically claims a queued run by transitioning it to `preparing`;
3. asks the selected adapter for bounded argv, its explicit provider-grant
   route, and non-secret loopback environment;
4. resolves the requested revision and exports only tracked Git files;
5. validates the named authorities and opens the run-scoped gateway;
6. resolves the image reference to an immutable digest and creates the sandbox;
7. records the commit, image, and container identity before starting it;
8. waits for the agent while renewing its ownership lease and polling durable
   cancellation; Docker command timeouts remain bounded, and agent runtime has
   a six-hour default ceiling that the operator may override with
   `--run-timeout`;
9. captures bounded stdout transiently, lets the adapter validate only its
   completion envelope, and retains none of that output;
10. captures a bounded file-level result and binds its digest to the attempt;
11. removes all labeled containers, gateway files, and the disposable snapshot;
   and
12. records the normalized exit status and terminal state.

Operational failures become audited `failed` runs only after cleanup succeeds.
If a started worker fails, its recoverable changes are captured before cleanup.
If result capture fails, the container and gateway are cleaned, the workspace
is retained for manual recovery, and the attempt becomes terminal without a
result; an invalid or unbounded workspace is not retried forever. If cleanup
cannot be proven, the run remains active so `run reconcile` can retry after the
owner lease expires. Reconciliation finds containers by product and run-ID
labels rather than trusting an in-memory container handle. It also recognizes
the retired label namespace so an upgrade can clean interrupted legacy
containers.

The result store is trusted host state beside the control-plane database. It
contains a canonical manifest plus deduplicated changed-file blobs, never a Git
repository or an executable action. Applying a result verifies the database
digest, checks every affected path against its captured baseline before making
any change, and then reconstructs files, safe relative symlinks, deletions, and
executable modes. It never commits, pushes, opens a pull request, or needs forge
credentials. A future integration state machine may authorize those effects
through an optional sidecar, but neither the result artifact nor a worker gains
that authority. See [results.md](results.md).

The Docker boundary rejects images with Dockerfile-declared volumes, validates
host paths before mounting them, runs as the host's non-root numeric identity,
uses a read-only root filesystem, drops all capabilities, applies resource
ceilings, and bounds local logs. Runtime output has a 24 MiB read/parser cap
over Docker's two rotating 10 MiB local log files; small Docker control
responses retain a 1 MiB cap. Both `none` and `gateway_only` use Docker network
policy `none`; `gateway_only` adds a private committed-file mailbox.
Bundled vendor adapters put a loopback-only HTTP bridge inside that same
network namespace. It translates the vendor protocol to serialized mailbox
operations; only the trusted host broker owns an outbound socket or reusable
provider credential.
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
administrator-owned policy. Provider authority also requires a live running
attempt, including during the short `waiting_for_input` handoff window; a
parked run cannot call upstream. Opening an interaction tells a
handoff-capable worker to submit bounded replacement state and exit. Cleanup
removes its resources, and the durable run remains `waiting_for_input`. A
verified response becomes available to the current attempt, but a replacement
is queued only after that attempt has exited. See [agent-gateway.md](agent-gateway.md),
[provider-broker.md](provider-broker.md), [results.md](results.md), and
[work-memory.md](work-memory.md).

## Work-item boundary

The core accepts only the normalized v1 document described in
[work-items.md](work-items.md). It has no built-in forge client. Import adapters
run outside the control plane and may only submit content; project selection,
assignment, profiles, credentials, network policy, and lifecycle remain trusted
local decisions. Imported content is therefore never an authority channel.

Provider-specific outbound effects belong to optional sidecars. A worker asks
through a narrow `forge.*` gateway operation; the core checks durable gates and
pinned authority, then passes one bounded request to the sidecar. The sidecar
returns an idempotent receipt such as a remote change ID or landed revision.
Without a configured sidecar, the same state machine stops at a human-executable
command plan rather than silently weakening the gate.

## Planned slices

Implementation is ordered around demonstrable product guarantees rather than
independent subsystems. See [roadmap.md](roadmap.md) for the current sequence and
exit proofs. The durable interaction path, run-scoped gateway, scoped provider
broker, structured worker replacement, multi-agent work memory, work-item
completion, execution integrity, the redacted interactive control room, and the
storage-boundary refactor, runtime adapter contract, live dual-runtime proof,
autonomous supervision, and aging escalation are complete. Integration gate
state and provider-neutral sidecars are next. See
[control-room.md](control-room.md), [worker-handoffs.md](worker-handoffs.md),
[runtime-adapters.md](runtime-adapters.md), [supervision.md](supervision.md),
and [work-memory.md](work-memory.md).
