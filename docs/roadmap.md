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

## Foundation extension: forge-neutral work intake

**Status: complete**

- Publish a strict, versioned work-item JSON contract.
- Atomically insert, update, or recognize unchanged imported work.
- Reject stale and same-revision conflicting imports.
- Preserve immutable content versions without provider payloads or outbound
  synchronization.
- Require every new run to pin one exact work-item version.

**Exit proof:** repeated import is idempotent, a newer import appends a version,
and a run created before that update continues to resolve its original title
and brief.

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

**Status: complete**

- Add strictly validated, idempotent session-item write and read operations.
- Correlate request and response item IDs in Magic Glass Box.
- Verify creator, recipient, project, and session metadata before accepting a
  delivery as an interaction transition.
- Treat all titles and bodies as untrusted content.

### 1C. Agent interaction gateway

**Status: complete**

- Give a sandbox a run-scoped local capability, not a reusable Krab token.
- Accept typed `question`, `approval`, and `choice` requests.
- Route the request to the authorized human through Krab.
- Permit only the response for that run and interaction to return to the
  worker.
- Keep Docker networking disabled and expose only a bounded committed-file
  mailbox whose run identity and capability allowlist are fixed by the
  orchestrator.

**Exit proof:** a run can wait for a private human response without Magic Glass
Box retaining its content, and the pending interaction survives an orchestrator
restart. Unit tests cover both directions and restart reconciliation; the live
Docker proof confirms direct egress is denied while `run.describe` crosses the
gateway.

## Milestone 2: scoped provider broker

**Status: complete**

- Add provider/model operations to the same run-scoped gateway contract.
- Resolve only credentials named by the pinned profile inside the trusted
  orchestrator; never copy reusable provider secrets into a worker.
- Give provider adapters explicit destination, method, model, request-size,
  response-size, timeout, and concurrency policy.
- Keep arbitrary bridge networking and administrator tokens unavailable to the
  worker.
- Record grants, revocation, and expiry as metadata without storing secret
  values.

**Exit proof:** a worker with Docker networking disabled can use one permitted
provider/model through the gateway while undeclared providers, destinations,
credentials, and direct network attempts fail closed.

## Milestone 3: worker replacement and handoff

**Status: complete**

- Define a bounded, structured handoff envelope containing work state,
  decisions, artifacts, and pending interaction IDs—not a transcript.
- Re-queue interrupted waiting work after its response arrives.
- Resume from the pinned commit and immutable context with the same or a
  different runtime adapter.
- Distinguish attempts from the durable run so every worker start and outcome
  remains auditable.

**Exit proof:** terminate Agent A while it waits, answer later, and complete the
same run with Agent B. The automated vertical proof uses two distinct runtime
adapter identities, reconstructs Agent B's workspace from the commit pinned by
Agent A, and retains only the bounded handoff and interaction metadata.

## Milestone 3.5: durable multi-agent work memory

**Status: complete**

- Attach one durable coordination thread to the stable work item rather than a
  coordinator process or model session.
- Accept bounded, typed, append-only entries with orchestrator-supplied run,
  attempt, profile, and runtime provenance.
- Let coordinators publish optimistic checkpoints through the exact log head.
- Delegate bounded assignments to exact immutable profile revisions under
  reclaimable leases.
- Expose paging, append, checkpoint, assign, claim, and completion through the
  run-scoped gateway without forge comments or transcripts.

**Exit proof:** Claude- and Codex-identified workers claim separate assignments
and complete concurrently. A newly created coordinator reconstructs the same
work from the earlier checkpoint plus both independently authored result
entries. Stale checkpoints, wrong-profile claims, live-lease theft, changed
idempotent retries, and cross-thread references fail closed.

## Milestone 3.6: work completion and memory generations

**Status: complete**

- Require an explicit operator action rather than inferring work completion
  from one successful run.
- Refuse completion while runs or assignments remain active or the thread lacks
  a final typed outcome.
- Seal completed memory without deleting its append-only evidence.
- Keep imports lifecycle-neutral and require an optimistic explicit reopen.
- Start reopened work in a blank next-generation thread while historical runs
  retain their original thread.

**Exit proof:** an operator seals a quiescent thread at its exact version, old
runs retain read-only access, and no new run or write can target it. A later
import does not reactivate the item; explicit reopen creates generation two
with no log, checkpoint, or assignments.

## Milestone 3.7: execution integrity and retained results

**Status: complete**

- Separate short Docker command timeouts from an optional deliberate agent
  runtime limit; ordinary runs no longer inherit a hidden 30-second ceiling.
- Give every active attempt an owner, heartbeat, and expiring lease so recovery
  cannot destroy another live orchestrator's container.
- Replace the generic operator transition command with an audited cancellation
  request that makes the owner stop the actual container before terminal state.
- Keep answered runs waiting while their original attempt is live, then queue
  the replacement only as part of attempt finalization.
- Capture a bounded, content-addressed file result on the trusted host before
  workspace cleanup, including nonzero and recoverable failure outcomes.
- Verify retained artifacts against their database-bound digest and apply them
  only after a complete affected-path conflict preflight.
- Execute every published JSON Schema against its examples and representative
  runtime rejection cases; cover assignment lease enforcement and exact
  completion replay directly.

**Exit proof:** a worker may run beyond 30 seconds, be durably cancelled and
stopped, or lose its orchestrator without a concurrent recovery process
touching a live lease. Its bounded edits survive snapshot deletion, tampering
and local checkout conflicts fail closed, and applying the result requires no
Git forge credential or outbound operation.

## Milestone 4: local control room

### 4A. Redacted operational projection

**Status: complete**

- Read users, projects, profiles, work generations, assignments, runs,
  attempts, interactions, results, and authority bindings in one SQLite read
  transaction.
- Keep every active run visible while bounding only terminal history.
- Elevate pending interactions, owners, blocker evidence, cancellation
  requests, expired leases, and unavailable authority into one attention list.
- Optionally verify configured Krab identities and overlay their live roles and
  issue/pull-request claim queues without storing that external state.
- Expose visible work-item and assignment titles as untrusted text while
  excluding briefs, objectives, message bodies, work-log text, handoffs,
  transcripts, result paths, and secret-bearing policy values.

**Exit proof:** one CLI call discovers every active local run and its linked
metadata without knowing an object ID in advance. The local view comes from one
read transaction; a live Krab failure degrades per identity without hiding
local state or exposing credential references.

### 4B. Operator action surface

**Status: complete**

- Show projects, users, roles, profiles, work claims, runs, attempts, and
  retained results.
- Make pending questions, approvals, owners, and blockers prominent.
- Expose policy and lifecycle metadata without private bodies or transcripts.
- Provide explicit cancel, retry, reassign, revoke, and reconcile actions.
- Serve the browser only on a loopback IP behind per-process access and CSRF
  capabilities, with no scripts or external assets.
- Model retry and reassignment as optimistic, audited successor runs rather
  than rewriting a terminal run or any of its immutable pins.

**Exit proof:** an operator can understand and safely control every active run
without entering an agent container or reading its private conversation. HTTP
tests exercise all five actions, reject cross-origin and malformed requests,
escape hostile visible titles, and prove that private fields are not rendered.

## Milestone 4.5: storage boundaries

**Status: complete**

- Preserve `magic_glass_box.db.Database` as the stable persistence facade.
- Isolate schema and migrations from connection ownership and aggregate stores.
- Split catalog, work-item, work-memory, run, attempt, interaction/provider,
  artifact, and query-projection persistence along durable-model boundaries.
- Give composite run projections one explicit SQLite read transaction and pass
  its connection into nested repository reads.
- Keep schema v9 and every public persistence operation behaviorally compatible.

**Exit proof:** `get_run`, `get_run_execution`, and active-run iteration each
open exactly one read connection, begin one snapshot, and commit or roll back
that snapshot as a unit. The existing migration, lifecycle, concurrency, and
immutability suite passes without a schema migration.

## Milestone 5: runtime adapter contract

**Status: complete**

- Publish the minimal adapter protocol and conformance tests.
- Support at least two meaningfully different headless agent runtimes.
- Keep runtime-specific prompts, flags, and output parsing outside the control
  plane.

The bounded Python protocol, registry, legacy `command` adapter, Codex JSONL
adapter, Claude Code JSON-result adapter, profile default, per-attempt override,
transient output capture, control-room visibility, explicit runtime-to-grant
binding, committed-file client, loopback vendor bridge, and buffered SSE
provider adapters are implemented. Legacy profiles retain their content
digests. The live proof builds one pinned image containing the real Codex and
Claude Code CLIs, then runs each through the coordinator, real Docker executor,
committed-file gateway, trusted provider broker, and a host-only deterministic
SSE fixture. Both containers use Docker network policy `none`; reusable
credentials and transient model output remain outside durable worker state.

**Exit proof:** the golden-path demonstration in the product thesis succeeds
with two interchangeable worker implementations.

## Milestone 6: autonomous supervision and escalation

**Status: planned**

- Run reconciliation, queued execution, answered-work resumption, and bounded
  retry/backoff from one long-lived supervisor loop.
- Make automatic execution an explicit project or run policy rather than an
  implicit side effect of creating work.
- Attach aging policies to questions, idle attempts, expired ownership, and
  other operator-attention states.
- Emit deduplicated escalation events through Krab and the control room without
  retaining private message content.
- Preserve leader-safe, idempotent transitions so two orchestrators cannot
  launch or recover the same attempt.

**Exit proof:** queued work advances without a cron or button, an answered run
resumes automatically, an expired attempt is recovered without touching a live
lease, and an intentionally stalled question produces one bounded escalation
at each configured threshold.

## Milestone 7: integration state and forge sidecars

**Status: planned**

- Model candidate base/head revisions, exact-head independent review, CI and
  approval gates, inter-work-item landing order, and durable landing receipts.
- Invalidate head-bound gates when a candidate changes and enqueue a re-fold
  when its pinned base is no longer current.
- Keep provider-specific GitHub, GitLab, and future forge mechanics out of the
  core. Optional pinned sidecars implement a small versioned operation protocol.
- Give a sidecar—not a worker—the user's explicitly approved configuration
  directory such as `GH_CONFIG_DIR`. The worker receives only named `forge.*`
  capabilities through the existing run-scoped gateway.
- Have Magic Glass Box authorize the operation, repository, account, exact
  revision, limits, and idempotency key before the sidecar performs a push,
  opens a change, reads checks, or merges. Record bounded receipts, never the
  reusable credential.
- Retain a manual actuator: when no sidecar is configured, produce the exact
  commands and proposed change title/body while preserving the same gate state.

**Exit proof:** one candidate cannot become ready until a separate run reviews
its exact head and every configured gate passes; a base advance invalidates the
candidate until re-folded; then either a human or an authorized sidecar lands it
and records the resulting immutable revision without exposing forge credentials
to a worker or the control-plane database.

## Milestone 8: provider usage, budgets, and quota recovery

**Status: planned**

- Retain bounded token and usage metadata from provider responses without
  retaining prompts, responses, or transcripts.
- Enforce per-run, per-project, and per-grant ceilings using versioned policy.
- Classify quota and rate-limit responses, honor bounded retry timing, and pause
  recoverable work instead of treating every upstream refusal as terminal.
- Surface spend estimates and remaining policy budget in the control room.

**Exit proof:** a run cannot exceed its pinned budget, a simulated quota outage
pauses and later resumes the same durable work, and usage totals reconcile with
audited provider invocations.

## Milestone 9: destructive continuity proof

**Status: planned**

- Kill a worker during active file edits rather than at a clean interaction
  boundary.
- Retain a bounded, trusted checkpoint of recoverable work product alongside
  the existing structured handoff and work-memory log.
- Start a compatible replacement from the pinned revision plus that checkpoint
  and prove it completes without silently dropping or redoing accepted work.
- Use “replacement recovery” rather than “resume” anywhere the stronger
  continuity contract is not demonstrated.

**Exit proof:** an adversarial mid-task kill followed by replacement produces
the expected final result and audit trail with no unaccounted file loss.

## Continuous gates

Every milestone retains the existing RED-first workflow, happy-path and
sad-path coverage, at least 85% branch coverage, and at least an 80% mutation
kill rate. Security-sensitive boundaries also require a live integration test
against the real local dependency where practical.
