# Architecture

## Ownership boundaries

Magic Glass Box is the trusted control plane. It owns local users, Krab
identity bindings, versioned project context, versioned agent profiles, run
lifecycle, credential authority policy, container lifecycle, pending human
interactions, trusted candidate identity, and integration semantics. The target
boundary does not require the core process to possess an upstream AI-provider
credential.

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

Workspace preparation reads one pinned commit through sanitized Git plumbing:
one bounded tree listing and raw blob reads with verified sizes and SHA-1 or
SHA-256 object identities. Workers receive exact committed regular-file bytes,
executable modes, directories, and validated UTF-8 relative symlinks. Attributes
are inert data: checkout filters, CRLF and ident conversion, and export-subst
never run; export-ignore files remain visible and LFS pointers remain pointers.
Git-links appear as empty directories without fetching or recursing into
submodules. The complete path structure and symlink graph are validated before
materialization, with ordinary files and directories created before symlinks.
Git commands use a fresh private environment, disable global/system config,
hooks, fsmonitor, replacements, prompting, lazy fetching, and transports, and
bound output, tree entries, total blob bytes, and command execution time.

Optional capability providers are trusted credential actuators, not workers.
A provider implements a small versioned semantic surface; a sidecar is its
out-of-process deployment and isolation boundary. Explicit project bindings
route each enabled operation to one controller, a configured provider
instance. With no binding, the
operation is disabled rather than discovered, defaulted, or routed through a
raw client. The implemented v1 service protocol places provider-specific
GitHub and future service mechanics behind one bounded local Unix transport.
A user may explicitly expose a local configuration directory such as
`GH_CONFIG_DIR` to a selected provider only; it is never mounted into an agent
container or copied into control-plane state. Magic Glass Box owns durable
operation identity and decides whether a specific deployment, mapping,
account, scope, capability, limit, repository, and exact revision are
authorized. The first provider-specific implementation is M11's out-of-process
GitHub reference provider. See [github-provider.md](github-provider.md) and
[capability-providers.md](capability-providers.md).

The first routing slice is implemented in schema v24. The core owns each
bindable capability's version and read/effect mode; a sidecar may advertise an
operator-only extension but cannot make it agent-facing or redefine its safety
semantics. A project-routed operation and its active binding are checked and
pinned in one transaction, so an unbind cannot race a new operation into
existence. Existing operations retain their original authority for settlement;
sidecar revocation remains the hard stop.

Milestone 7D extracts AI-provider authentication and transport into a
credential-isolated inference connector. M7D.2 makes this the exclusive normal
dispatch path. The core authorizes the exact run, grant, connector deployment,
route, semantic protocol, model, limits, concurrency, and budget. The v1 or v2
local sidecar owns API keys or OAuth tokens, refresh, and upstream networking.
Protocol v2 additionally carries a pinned invocation allowance,
provider-enforced output bound, and normalized usage counters; the core
atomically enforces versioned run, project, and grant ceilings without gaining
the provider credential. The transport-neutral client seam preserves a future
authenticated external carrier. It exposes a bounded inference contract, not a
raw provider CLI or arbitrary HTTP proxy. See
[inference-connectors.md](inference-connectors.md).

The local connector's mode-`0700` directory and mode-`0600` Unix socket are its
client-authentication boundary and are never mounted into a worker. MGB owns
grant lifecycle and reservation; the credential sidecar owns route and
provider enforcement and intentionally cannot read or mirror the MGB database.
An external connector transport must add a revocable client capability rather
than treating public reachability as equivalent to that private socket.

## Threat-model rule: outside scary, inside safe

MGB treats every input crossing into the control plane as adversarial until it
has been validated and published: source repositories, worker workspaces,
import documents, message metadata, provider responses, and sidecar responses.
Those boundaries receive bounded parsing, path and type validation, capability
checks, and fail-closed publication.

After publication, MGB treats its private state directory, SQLite database,
retained results, derived candidate caches, and isolated containers as trusted
control-plane state. Inside that boundary it relies on ordinary permissions,
transactions, immutable records, digests, atomic replacement, and file locks.
It does not repeatedly defend against a hostile process with the operator's UID
rewriting `.mgb` between system calls.

The host, container runtime, kernel, and operator account are part of the
trusted computing base. If they are compromised, MGB cannot restore a reliable
security boundary by adding application-level inode races, PID leases, or
per-read database re-verification. Panopticon is intentionally different: it
may be dropped into a hostile repository on a hostile machine, so its own
threat model must not be generalized to MGB.

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
      +---- authorized model invocation ----> inference connector
      |                                              |
      |                                              v
      |                                         AI provider
      |
      +---- durable authorized action -----> capability provider sidecar
                                                    |
                                                    v
                                          GitHub / GitLab / Jira / service
```

Krab metadata is structurally validated. Work-item titles are retained as
`untrusted_text`; they may be displayed but must never be interpreted as agent
instructions. Bodies, notes, and transcripts are not ingested through the
project-activity projection.

## Durable model

- A local user can be linked to one or more Krab identities. The binding uses the
  immutable Krab `user_id`; handles are cached only for display.
- A credential reference points to an external secret source. Private token
  files remain supported for separately scoped non-inference authorities such
  as Krab and are never persisted. The core does not resolve AI-provider
  credentials.
- An inference grant is an immutable, content-addressed binding to a registered
  connector deployment, route, semantic protocol, exact model, limits, and
  credential-scope fingerprint. A protocol-v2 grant also pins one versioned,
  content-addressed budget policy, integer USD-micro rates, a per-invocation
  reservation and output bound, and run/project/grant ceilings. Creation and
  revocation are audited; expiry and budget admission are enforced without
  storing or locating the provider secret.
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
  Revocation and expiry remain live kill switches for already-pinned runs. A
  transactional dispatch reservation is the linearization point and enforces
  grant concurrency across control-plane processes.
- The requested source revision is resolved to a full Git commit and persisted
  in the transition to `running`.
- Each active attempt has one orchestrator owner, heartbeat, and expiring
  lease. Reconciliation may take over only after that lease expires. A
  takeover increments the attempt version and appends an immutable owner-change
  event; start, completion, and failure require both the current version and
  current owner. Invoking recovery therefore cannot tear down a live attempt
  or let a stale owner close work after takeover.
- A cancellation request is durable and audited. The owning coordinator polls
  it through the lease heartbeat, stops the labeled container, captures any
  bounded result, cleans resources, and only then records terminal
  cancellation.
- Every started attempt binds one immutable result manifest and its
  content-addressed blobs before its workspace is removed. Results are inert
  evidence until an operator explicitly applies them to a checkout.
- An operator may promote one strictly evidenced successful, zero-exit attempt
  result into one immutable trusted candidate. Its record, introduced in
  database schema v11, binds project, work thread, producing run and attempt,
  pinned context, exact base/head/tree, Git object format, result digest, and
  creator. Candidate construction writes one shared private bare repository
  per project and object format, never the source checkout. The bare repository
  is a derived cache; the immutable ledger and retained result can reconstruct
  it exactly when the pinned base remains cached or available from the project
  repository.
- An immutable schema-v13 gate policy binds a candidate to the repository,
  integration ref, expected base, head/tree, object format, and policy
  revision. Independent-review runs are durably bound to that exact candidate
  and receive its private snapshot; local validation runs networkless and
  credentialless against a read-only snapshot. Append-only bounded evidence
  derives readiness without retaining review transcripts or command output.
- Schema v26 adds immutable changes-requested run bindings. A correction run
  requires the current failed independent-review evidence plus an exact-head
  review work-log entry from that reviewer attempt, receives the prior
  candidate snapshot, and can produce a new child candidate without mutating
  the changes-requested gate tuple.
- Schema v27 adds a separate immutable maintainer-disposition stream and
  correction-run binding. A policy may require product approval independently
  of agent review and validation; the latest exact-candidate disposition drives
  readiness, while the complete history remains append-only. A
  `changes_requested` disposition can feed the same correction choreography
  without fabricating agent-review provenance.
- Schema v28 adds the bounded workflow prescription ledger. The built-in
  definition, saved configuration revision, and execution are distinct: an
  execution pins exact producer/reviewer profile revisions, context and work
  item versions, work-thread generation and version, source object ID, and
  configuration revision.
- Schema v29 adds the M12B director. Immutable per-round run and gate bindings,
  typed exact-head reviewer verdicts, durable workflow supervisor actions, and
  explicit rulings drive producer, candidate, independent review, validation,
  correction, and maintainer stages without treating agent prose as authority.
- Schema v30 adds the M12C landing binding, explicit provider target, and
  atomic completion record. The fixed director reuses the integration queue's
  dependency, refold, gate, and lease authority; a matching manual or provider
  receipt is required before one transaction records a control-plane outcome,
  seals the work thread, and completes the workflow.
- Schema v31 adds immutable authority for one contiguous workflow correction
  chain. It permits the final reviewed stacked head to land directly from the
  execution's pinned source only while that source is still the observed
  integration ref; real external drift continues through the ordinary refold
  and fresh-gate path.
- Schemas v32-v35 generalize immutable role bindings and add the bounded
  `discovery-v1` configuration, evidence ledger, and restart-safe director.
  Investigator and coordinator runs remain read-only; a `GO`, `NO_GO`, or
  `ASK_HUMAN` ruling is accepted only against the execution's exact pinned
  project, work, source, identities, and trace.
- Schemas v36-v38 add `advisory-review-v1`: three structurally blind reviews,
  three fresh challenge runs, and one separate triangulation run. Its final
  artifact is literally advisory-only and cannot satisfy a gate or landing
  decision.
- Schemas v39-v40 bind an implementation execution to one exact completed
  Discovery `GO` ruling and require its reviewer to submit a bounded
  scope-review artifact. MGB derives regressions, follow-ups, and the expected
  verdict before accepting the ordinary exact-head review entry.
- Schema v14 adds hard work-item landing dependencies, one serialized lane per
  project/ref, immutable derived folds, exact argv-only landing plans, and
  nonce-bound receipts. Landing one item advances the durable ref and marks
  every remaining sibling `refold_required`; its origin candidate is unchanged.
- Schema v15 adds immutable inference-connector registrations and lifecycle
  events plus exact connector-bound provider grants. Registrations contain a
  transport location and non-secret scope fingerprint, never an upstream
  credential or provider-native configuration path. A scope fingerprint is
  not a digest of bearer material.
- Schema v16 makes connector dispatch exclusive for normal execution and binds
  every durable invocation outcome to the exact connector, deployment, route,
  grant policy, scope, provider, protocol, model, and deadline. Historical
  direct grants and invocations remain readable for migration but are not
  dispatchable.
- Schema v17 records immutable provider-dispatch intent before a connector call
  and permits one terminal finalization. A process or storage failure after the
  external boundary therefore leaves durable uncertainty instead of erasing
  evidence and making the call appear safely retryable.
- Schema v19 adds protocol-v2 budget authority and content-free normalized
  usage. One `BEGIN IMMEDIATE` transaction checks every applicable ceiling and
  records the allowance before dispatch; successful provider counters release
  unused allowance or honestly account a paid overrun, known pre-dispatch
  failure releases it all, and uncertainty conservatively consumes the full
  reservation.
- Schema v20 adds provider-recovery identity and a first-class `paused` run
  state. Explicit quota, rate-limit, and overload failures atomically finalize
  the invocation and pause the same run under bounded timing. The supervisor
  requeues it only after its active attempt and open interactions are resolved;
  cancellation permanently settles the pending wake.
- Schema v21 pins one verified retained-result checkpoint and immutable apply
  receipt for destructive replacement recovery.
- Schema v22 retains every landing-plan nonce generation so lost output can be
  rotated without releasing the lane, exposes only the newest 32 generations
  in control-plane views, and admits successful reported provider usage above
  its pre-dispatch estimate.
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
- An operator binds that response through the versioned `interaction answer`
  CLI command or the loopback control room. The receipt contains only IDs,
  states, and versions. Automatic activity polling is intentionally absent
  until Krab supplies a trusted reply-to item ID; same-session routing alone
  is insufficient to distinguish an answer from unrelated conversation.

The Krab POST and the local SQLite transition cannot be one transaction. The
caller must retain and reuse its stable idempotency key until local correlation
succeeds. A retry then resolves to the same Krab item rather than sending a
duplicate private request.

## Persistence boundary

`magic_glass_box.db` is the stable compatibility facade. Its implementation is
split beneath `magic_glass_box.storage` into schema and migration ownership,
SQLite connection ownership, and stores for the catalog, work-item lifecycle,
work memory, runs, attempts, interactions and provider activity, retained
artifacts, trusted candidates, supervision, and composite query projections.
The storage split preserved schema v9; autonomous supervision advanced it to
schema v10, and trusted candidates plus their query-backed indexes introduced
schema v11. Cross-process provider dispatch reservations advanced it to schema
v12; exact-input gate policy and evidence state advanced it to schema v13; the
integration queue and manual landing ledger advanced it to schema v14.
Inference-connector registrations and grant bindings advance it to schema v15;
exclusive connector dispatch and its invocation authority ledger advance it to
schema v16. Durable pre-dispatch invocation intent and one-way finalization
advance it to schema v17. Audited attempt-owner takeover and stale-owner
transition fencing advance it to schema v18. Provider budget reservations and
normalized usage accounting advance it to schema v19. Quota-aware paused-run
recovery advances it to schema v20. Immutable attempt recovery selections and
application receipts advance it to schema v21. Append-only landing-nonce
generations and honest reported provider overruns advance it to schema v22.
Provider-neutral service-sidecar registrations, durable operations, and
dispatch/reconciliation attempts advance it to schema v23. Explicit capability
bindings, provider landing, exact gate-change provenance, maintainer review,
bounded workflow prescriptions, their build/review/correction director, and
their landing/closeout director and correction-chain authority advance it
through schemas v24-v31. Generic role bindings, Discovery, advisory review,
and mechanically enforced Discovery-to-implementation scope lineage advance
it through schemas v32-v41.

The filesystem trust boundary ends when untrusted worker output is captured.
Private control-plane database, result, and candidate files are then trusted
state protected by ordinary permissions, transactions, digests, atomic writes,
and file locks. MGB does not repeatedly model its own state directory as a
same-user adversarial filesystem.

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
records due aging thresholds and dispatches open-interaction reminders through
a separate bounded pool. Active claims are heartbeated without version drift,
reminder I/O has a timeout shorter than its claim lease, and process shutdown
drains for a finite interval before durable lease recovery takes over. A hard
coordinator-action deadline stops renewal and terminates an unhealthy
supervisor, allowing a replacement process to recover the durable action
without attempting unsafe in-process thread termination. See
[supervision.md](supervision.md).

Schema v41 bounds this loop at fleet scale. One per-tick candidate budget is
split across all action and escalation classes; SQL limits bound every source
read, and durable keyset cursors carry escalation fairness across process
restarts. Interaction history uses the same bounded keyset pattern. The schema
also pins a finite run/grant invocation allowance that is consumed atomically
before either protocol-v1 or protocol-v2 connector dispatch. Result-capture
failures persist a sanitized exception class on the attempt, while lease loss
and outcome-persistence races emit ID-only structured diagnostics.

For an execution request, Magic Glass Box:

1. validates the profile default or explicit attempt-level runtime adapter;
2. atomically claims a queued run by transitioning it to `preparing`;
3. asks the selected adapter for bounded argv, its explicit provider-grant
   route, and non-secret loopback environment;
4. resolves the requested revision and exports only tracked Git files;
5. records the pristine result baseline and, when the attempt pins a recovery
   checkpoint, verifies and applies it to this disposable snapshot and records
   an application receipt;
6. validates the named authorities and opens the run-scoped gateway;
7. resolves the image reference to an immutable digest and creates the sandbox;
8. records the commit, image, container identity, and recovery provenance
   before starting it;
9. waits for the agent while renewing its ownership lease and polling durable
   cancellation or provider-pause requests; either signal stops the labeled
   container, Docker command timeouts remain bounded, and agent runtime has a
   six-hour default ceiling that the operator may override with
   `--run-timeout`;
10. captures bounded stdout transiently with deterministic UTF-8 replacement
   decoding, lets the adapter validate only its completion envelope, and
   retains none of that output;
11. captures a bounded file-level result against the pristine baseline and
    binds its digest to the attempt;
12. removes all labeled containers, gateway files, and the disposable snapshot;
   and
13. records the normalized exit status and terminal state.

Operational failures become audited `failed` runs only after cleanup succeeds.
If a started worker fails, its recoverable changes are captured before cleanup.
If result capture fails, the container and gateway are cleaned, the workspace
is retained for manual recovery, and the attempt becomes terminal without a
result while retaining only the sanitized failure class; an invalid or
unbounded workspace is not retried forever. If cleanup
cannot be proven, the run remains active so `run reconcile` can retry after the
owner lease expires. Reconciliation finds containers by product and run-ID
labels rather than trusting an in-memory container handle. It also recognizes
the retired label namespace so an upgrade can clean interrupted legacy
containers.

Workspace cleanup may restore owner permissions on directories inside the
stopped worker's one disposable run root, without following symlinks, before
removing it. That admission-side normalization handles common read-only
dependency caches; it does not treat private control-plane state as a hostile
filesystem or weaken the rule that unexplained cleanup failure stays
recoverable and visible.

The result store is trusted host state beside the control-plane database. It
contains a canonical manifest plus deduplicated changed-file blobs, never a Git
repository or an executable action. Applying a result verifies the database
digest, checks every affected path against its captured baseline before making
any change, and rejects case/Unicode path aliases before reconstructing files,
safe relative symlinks, deletions, and executable modes. Symlink resolution
uses the same portable path identity at capture and apply. Apply requires a
quiescent target and is not one atomic whole-tree filesystem transaction; a
failure after mutation begins may require operator recovery. It never pushes,
opens a pull request, or needs service credentials. An operator may instead
materialize a deterministic candidate commit in the separate private candidate
store; neither the result artifact nor a worker gains that authority. See
[results.md](results.md) and [candidates.md](candidates.md).

The Docker boundary rejects images with Dockerfile-declared volumes, validates
host paths before mounting them, runs as the host's non-root numeric identity,
uses a read-only root filesystem, drops all capabilities, applies resource
ceilings, and bounds local logs. Runtime output has a 24 MiB read/parser cap
over Docker's two rotating 10 MiB local log files; small Docker control
responses retain a 1 MiB cap. Both `none` and `gateway_only` use Docker network
policy `none`; `gateway_only` adds a private committed-file mailbox.
Bundled vendor adapters put a loopback-only HTTP bridge inside that same
network namespace. It translates the vendor protocol to serialized mailbox
operations. MGB turns an authorized mailbox operation into one strict connector
invocation; only the separately launched connector owns an upstream socket or
reusable provider credential.
The mailbox directory and response files are read-only in the container. Only
the fixed request payload and request commit files are writable. Sequence,
size, and SHA-256 commitments prevent torn reads and stale response confusion.
The retired `krab_only` mode fails closed.

The gateway binds run identity outside the agent-controlled request. Its
allowlist comes from the immutable profile revision. The current operations
cover run description, interactions, handoff, provider invocation, and the
typed work-memory read/append/checkpoint/assignment lifecycle; none accepts a
run ID or credential value. Provider calls additionally pin the destination,
method, and headers inside the connector while MGB pins the connector
deployment, route, semantic protocol, model, scope fingerprint, byte limits,
timeout, and concurrency in administrator-owned authority. Provider authority
also requires a live running attempt, including during the short
`waiting_for_input` handoff window; a parked run cannot call upstream. Opening
The credential-free `gateway.catalog` meta-operation publishes only this
already-pinned allowlist. Its registry also selects each handler and validates
exact request/result field sets, while path-based provenance labels keep
project, prior-agent, human, and provider text explicitly untrusted through the
worker carrier.

Opening an interaction tells a
handoff-capable worker to submit bounded replacement state and exit. Cleanup
removes its resources, and the durable run remains `waiting_for_input`. A
verified response becomes available to the current attempt, but a replacement
is queued only after that attempt has exited. An explicit quota or backpressure
response instead moves the run to `paused`; the same worker is interrupted and
the supervisor cannot requeue the run before its bounded eligibility time or
while an interaction remains open. See [agent-gateway.md](agent-gateway.md),
[provider-broker.md](provider-broker.md), [results.md](results.md),
[worker-handoffs.md](worker-handoffs.md), and [work-memory.md](work-memory.md).

## Work-item boundary

The core accepts only the normalized v1 document described in
[work-items.md](work-items.md). It has no built-in forge client. Import adapters
run outside the control plane and may only submit content; project selection,
assignment, profiles, credentials, network policy, and lifecycle remain trusted
local decisions. Imported content is therefore never an authority channel.

Provider-specific operations remain outside the core. M10 implements their
provider-neutral actuator boundary and explicit project-to-controller routing.
M11 now binds that boundary to the integration queue for `landing.invoke` and
supplies the first out-of-process GitHub implementation. Its bounded read
receipts can carry normalized work/change results without retaining raw GitHub
payloads; `work.get` emits the existing forge-neutral work-item envelope. A
worker may provide a result or review, but cannot turn it into landing
authority. The core checks
the exact candidate or fold, dependencies, current ref, gates, lane lease, and
optimistic versions before issuing either a structured manual Git plan or one
atomically pinned provider operation. It holds no remote credential and gives
workers no raw `git`, raw `gh`, provider credential, or sidecar endpoint.

## Planned slices

Implementation is ordered around demonstrable product guarantees rather than
independent subsystems. See [roadmap.md](roadmap.md) for the current sequence and
exit proofs. The durable interaction path, run-scoped gateway, scoped provider
broker, structured worker replacement, multi-agent work memory, work-item
completion, execution integrity, the redacted interactive control room, and the
storage-boundary refactor, runtime adapter contract, live dual-runtime proof,
autonomous supervision, and aging escalation are complete. Trusted candidate
materialization, exact-input gates, dependency/refold queue, and manual landing
flow are implemented. The credential-isolated inference connector, its
real-account proof, provider usage/budget authority, quota-aware recovery, and
destructive replacement continuity are implemented. The provider-neutral
service-sidecar protocol and uncertain-effect recovery are implemented. The
GitHub reference add-on, normalized read surface, provider-landing ledger, and
lost-response live GitHub exit proof are complete. See
[inference-connectors.md](inference-connectors.md),
[integration-gates.md](integration-gates.md),
[service-sidecars.md](service-sidecars.md),
[github-provider.md](github-provider.md),
[capability-providers.md](capability-providers.md),
[control-room.md](control-room.md), [worker-handoffs.md](worker-handoffs.md),
[runtime-adapters.md](runtime-adapters.md), [supervision.md](supervision.md),
[work-memory.md](work-memory.md), and
[workflow-templates.md](workflow-templates.md). The M12 immutable prescription,
build/review/correction, and landing/closeout slices are implemented without
introducing a general workflow engine. The completed real-repository M12 exit
proof exercised correction, restart recovery, provider rotation, an explicit
landing retry ruling, exact provider landing, and sealed closeout.
