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
- Atomically recheck live authority and reserve provider capacity immediately
  before dispatch, enforcing concurrency across control-plane processes.

**Exit proof:** a worker with Docker networking disabled can use one permitted
provider/model through the gateway while undeclared providers, destinations,
credentials, and direct network attempts fail closed.

This slice originally proved the worker boundary with credential resolution
inside the trusted orchestrator. Milestone 7D.2 has since narrowed the boundary
again: the core retains authority and reservations while a separate inference
connector owns every upstream provider credential and network call.

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

- Separate short Docker command timeouts from a deliberate six-hour default
  agent runtime limit; operators may override the limit without making ordinary
  runs inherit a hidden 30-second ceiling.
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
provider adapters are implemented. The hardening pass also binds provider
authority to a live attempt, delegates provider duration to the host policy,
detaches gateway cleanup from blocked upstream calls, tolerates transient lease
storage contention within the safety window, expands the bounded runtime-log
envelope, and captures results using the source revision's pinned Git ignore
rules. Legacy profiles retain their content digests.

The live proof builds one pinned image containing the real Codex and Claude
Code CLIs, then launches three containers through the coordinator, real Docker
executor, committed-file gateway, separate reference inference connector, and
a host-only deterministic SSE fixture. A Codex worker opens a durable
interaction and hands off; a clean replacement reads that handoff and accepted
answer before finishing. The first attempt also creates an ignored dependency
tree larger than the proof's result path ceiling, demonstrating that only its
intended file is retained. Claude Code exercises the second protocol. Every
container uses Docker network policy `none`; reusable credentials, private
interaction content, and transient model output remain outside durable worker
state.

**Exit proof:** the golden-path demonstration in the product thesis succeeds
with two interchangeable worker implementations, including a real
interaction/handoff/replacement cycle.

## Milestone 6: autonomous supervision and escalation

**Status: complete**

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

The schema-v10 implementation pins an immutable policy to every run, defaults
to manual launch, persists version-bound actions and threshold-bound
escalations, and exposes both through the redacted control room. Action and
reminder failures use finite policy-owned backoff before becoming exhausted
attention. Krab reminders carry an empty body and a stable idempotency key.
The repeatable proof runs two competing supervisor identities against the real
durable state machine and satisfies every exit condition. See
[supervision.md](supervision.md). The end-of-milestone review additionally
proved SIGTERM-aware bounded draining, renewable action and reminder claims,
and isolation of reminder I/O from the main supervision loop.

The manual whole-tree checkpoint review found that the proofs had bypassed the
missing operator half of the interaction loop. The product now exposes
versioned answer actions in both the CLI and loopback control room; live Krab
evidence crosses that public boundary and confirms its receipt is metadata
only. Automatic response polling remains deliberately out of scope until Krab
can bind a response to its request with trusted reply metadata. The same
remediation added recursive symlink-graph containment at capture, archive, and
pre-apply boundaries, full supervisor retry spacing with independent lease
heartbeats, and privacy-safe daemon diagnostics.

## Milestone 7A: trusted candidate kernel

**Status: complete**

- Materialize one immutable Git candidate from a database-bound retained result
  and its pinned source commit.
- Give the candidate a real parent commit and exact tree/head object IDs without
  applying the result to the operator checkout.
- Construct the tree through a private index and verified Git objects. Do not
  run source-local hooks or filters, recurse into submodules, or copy untracked
  operator files.
- Persist the candidate's project, work thread, producing run and attempt,
  context revision, base, head, tree, object format, result digest, and creator
  in the immutable candidate ledger introduced by schema v11, with query-backed
  indexes.
- Check succeeded/zero-exit attempt and result authority at candidate creation;
  later reads trust the immutable ledger rather than replaying completion
  evidence on every query.
- Share one derived cache per project and object format; make creation
  idempotent, reconstruct a missing cache or ref from SQLite plus the retained
  result and the pinned base (cached or read from the project repository), and
  use targeted commit/tree inspection instead of whole-store fsck.
- List candidates by recent activity, project, producing run, or work thread
  without opening the result or Git stores.

**Exit proof:** `candidate create` turns a retained result into a deterministic
child of its pinned source commit; addition, modification, deletion, executable
mode, and every file/directory/symlink transition are represented in raw Git
objects. LFS remains a pointer and checkout presentation such as CRLF is not
hydrated. Repeating creation returns the same record, source-local hooks and
filters do not execute, dirty operator files do not enter the tree, and neither
the source checkout nor a hosting service is modified. Common umasks work,
complete cache loss is repaired deterministically while the pinned source
commit remains available, and candidate listing reads only the durable ledger.
See
[candidates.md](candidates.md).

The 2026-10-14 end-of-slice internal checkpoint closed M7A after the frozen
tree passed 556 tests, 87% branch coverage, the 25-test real Git/SQLite
candidate proof, and a clean 25,686-mutant run with 21,558 killed (83.93%).
The review found and corrected hostile flat-directory enumeration, source
checkout proof gaps, and cache-recovery documentation that overstated what can
be rebuilt without the pinned base. Complete cache loss still requires that
base to remain available from the project repository. M7B was not started as
part of this checkpoint.

## Milestone 7B: exact-input gate ledger

**Status: complete**

- Pin one versioned gate policy to the candidate's repository identity,
  integration ref, expected base, head/tree, and policy revision.
- Record independent-review evidence with producer and reviewer provenance and
  an explicit separation rule; a distinct run alone is not sufficient proof of
  independence.
- Bind reviewer runs durably to the policy candidate, materialize that exact
  private tree for every attempt and retry, and reject revision-only claims as
  review authority.
- Run configured local validation against the materialized candidate tree and
  retain bounded evidence rather than command output or transcripts.
- Invalidate every affected gate when its candidate or policy input changes.
- Model provider-neutral evidence without pretending the core has queried a
  remote CI or approval service.

**Exit proof:** a producer cannot satisfy its own configured independent-review
gate, evidence for another head/base/policy tuple is rejected, and a candidate
becomes ready only when every gate required by its pinned policy passes.

The 2026-10-15 end-of-slice internal checkpoint closed M7B after the frozen
tree passed 588 tests, 86% branch coverage, the 25-test exact-input gate proof,
and a clean-tree 27,879-mutant run with 23,435 killed (84.06%). The final audit
removed run-only review separation because another run ID is not independent,
and aligned the public/runtime validation-command argument ceiling with the
executor's 1,024-byte contract. It also confirmed candidate-backed reviewer
workspaces, unbound-evidence rejection, separation across replacement paths,
isolated bounded local evidence, Docker-lazy review-only policies, complete
readiness derivation, and control-room visibility. M7C was not started as part
of this checkpoint.

## Milestone 7C: integration queue and manual landing

**Status: complete**

- Give work items explicit hard landing dependencies and reject dependency
  cycles.
- Serialize integration through a leased durable landing action.
- Materialize and validate the combined tree that would exist after landing.
- After every base advance, conservatively invalidate and re-fold every
  remaining sibling before it can become ready again. Selective contended-path
  invalidation is a later optimization, not an initial correctness mechanism.
- Produce a structured human-executable plan and accept a nonce-bound landing
  receipt. Record the assurance actually established; never claim an atomic
  base-and-head guarantee when the selected actuator cannot enforce both.

**Exit proof:** item B cannot land before prerequisite A; landing A advances the
base and makes sibling B unready until its folded tree is regenerated and
revalidated. A manual plan contains no unresolved revision, and its imported
receipt is accepted only for the authorized operation and records the immutable
landed revision at its truthful assurance level. See
[integration-gates.md](integration-gates.md).

The 2026-10-15 end-of-slice checkpoint closed M7C after 615 tests at 86% branch
coverage, a 21-test integration proof against real local repositories and a
bare remote, and a clean 31,180-mutant run with 26,326 killed (84.43%). The
final audit also closed an authority ambiguity: replaying an enqueue is
idempotent only for the exact policy, while requesting a different policy for
the same queued candidate now fails explicitly. The accepted mutation run
classified every generated mutant; three already-audited nontermination/FIFO
mutants required Mutmut's own timeout signal to be delivered manually after
its macOS watchdog thread failed to remain live.

## Milestone 7D: credential-isolated inference connectors

**Status: complete — M7D.1 through M7D.3 and the live golden proof pass**

M7D uses three honest internal checkpoints rather than one large cutover:
**M7D.1** establishes durable connector authority and the strict local carrier,
**M7D.2** moves normal dispatch across that boundary and proves credential
containment with a networkless worker, and **M7D.3** adds the optional
provider-specific OAuth integration and bounded live proof. Each checkpoint
receives its own quality gate and internal review.

- Preserve the existing networkless worker-facing `provider.invoke` contract,
  immutable grant binding, exact model/limit policy, revocation, expiry, and
  cross-process concurrency reservation.
- Move provider API keys and OAuth registration, access, refresh, and network
  transport out of the MGB process into a narrow versioned inference
  connector. A connector may be a local sidecar or an operator-managed
  external integration.
- Permit an explicitly approved provider-native configuration directory only
  inside the connector. MGB does not parse that credential format. For OpenAI,
  the connector owns its own supported Sign in with ChatGPT registration and
  renewable session; it never borrows another Codex installation's private
  credential record.
- Bind grants to a registered connector identity, semantic protocol, exact
  operator-approved model mapping, and credential-scope fingerprint without
  storing or resolving the upstream credential in MGB.
- Expose neither a raw provider CLI nor an arbitrary HTTP proxy. Workers cannot
  choose connector endpoints, upstream destinations, accounts, headers,
  credentials, or model overrides.
- Treat a lost response as potentially quota-consuming and do not
  automatically replay an ambiguous invocation.

**Exit proof:** a deterministic fake connector completes one authorized
invocation while wrong connector, grant, protocol, model, and limit tuples fail
closed. A canary upstream credential remains absent from the worker container,
MGB state directory and SQLite database, retained results, runtime output, and
logs. See [inference-connectors.md](inference-connectors.md).

The 2026-10-15 M7D.1 checkpoint adds schema-v15 connector registration and
lifecycle state, immutable connector-bound grants without credential
references, four public strict contracts, absolute-deadline Unix transport,
and a separately launched deterministic fake connector. Connector grants are
visible but cannot enter run authority at that checkpoint. Its normal quality
gate passes with 638 tests and 86% total branch coverage.

The 2026-10-15 M7D.2 checkpoint advances to schema v16 and makes connector
dispatch exclusive for normal execution. MGB no longer creates direct grants,
resolves AI-provider credentials, or performs provider HTTP transport;
historical direct records remain readable and revocable. Every invocation
persists its exact connector authority tuple, lost post-dispatch replies are
durably ambiguous without automatic retry, and concurrency is enforced across
both a grant and its connector. The networkless Docker canary proof passes with
no credential exposure, and the real Codex and Claude Code CLIs both pass the
two-runtime proof through a separately launched reference connector and
deterministic host fixture. The normal gate passes with 650 tests, 964
subtests, clean lint, and 86% total branch coverage. At that checkpoint, the
M7D.3 real-account proof remained due. The completed-slice mutation gate now
passes with 29,433 of 35,432 mutants killed (83.07%), 5,939 survived, 36
uncovered, 24 timed out, and zero suspicious classifications.

The 2026-10-15 M7D.3 implementation adds a separately launched OpenAI
connector with its own dynamic Sign in with ChatGPT registration, private
renewable session, account model discovery, RS256 ID-token validation, strict
Responses transport, and redacted failure semantics. The MGB process retains
only the registered connector authority and non-secret credential-scope
fingerprint. Codex now starts with provider-hosted web search disabled, while
the connector accepts only the pinned CLI's known request fields and local
function/namespace tool declarations; unknown fields, hosted tools, and nested
hosted tools fail before HTTPS dispatch. The rebuilt credential-free
two-runtime Docker proof passes with only local Codex tools. Browser consent
and account model discovery also pass. The first one-attempt real-account run
launched the networkless worker but failed before producing a result. That
attempt exposed two issues instead of being papered over: successful OpenAI
SSE may omit `Content-Type`, and the proof harness raised before scanning or
reporting bounded failure evidence. RED-first corrections now accept only a
headerless body with a valid completed SSE terminal, preserve structured SSE
error codes, and run the credential-leak audit on both success and failure.
The normal suite now passes with 708 tests and 86% total branch coverage. The
clean mutation gate passes at 83.07%. On 2026-10-16, the operator separately
authorized one additional `gpt-6.1-sol` attempt with a six-invocation and
300-second ceiling. It passed using five connector invocations: the networkless
worker retained only `golden-proof.txt`, candidate creation and the exact
validation gate passed, and a two-command landing plan was issued but not
executed. The proof reported no credential exposure outside the connector
profile and denied direct worker networking. No automatic retry occurred. M7D
is complete.

## End-of-Milestone 7 operator golden proof

**Status: completed 2026-10-16**

This checkpoint does not add another product milestone. It exercises the
completed M1-M7 path with one real model account before budget work begins:

- Keep the deterministic, credential-free proofs as the mandatory repeatable
  gates. A paid or quota-consuming proof is explicit and never runs from
  ordinary test or CI targets.
- Build one project-specific worker image with its test and build dependencies
  already installed; the worker remains networkless.
- Begin with Codex through OpenAI's documented Sign in with ChatGPT flow. The
  inference connector owns its separately revocable OAuth registration and
  protected token record; MGB stores only its connector binding and bounded
  metadata. Neither component parses another Codex installation's private
  credential format, and no access or refresh token enters the worker.
- Use one deliberately small disposable work item, bounded wall time, no
  automatic retry, and an operator-configured app usage limit. Because this
  uses the same ChatGPT-plan quota pool as an interactive Codex session, the
  initial risk is treated like spawning one bounded subagent. Before M8A, that
  account-side limit is the quota backstop; MGB must not claim it enforced a
  dollar budget itself.
- Drive the work through real Docker execution, provider transport, retained
  result, candidate, exact-input gates, and the manual landing boundary, then
  inspect the redacted control room and durable evidence.

**Exit proof:** an explicitly opted-in real Codex run completes one disposable
task using the operator's ChatGPT-plan quota through the inference connector.
Reusable OAuth material remains absent from the container, MGB state and
SQLite, retained results, runtime output, and logs. The deterministic proofs
continue to cover all failure paths without spending quota. Subscription
authentication for any other runtime is not inferred from this proof and
requires its own supported connector.

The explicitly opted-in proof passed with one networkless Codex worker, five
connector invocations, one retained result path, a candidate at the exact
result revision, a passing validation gate, and an unexecuted landing plan.
The credential scan was clean and direct worker networking remained denied.

## Post-Milestone 7 external-audit stabilization

**Status: in progress — R1 through R5 complete**

Three independent cold promise audits follow M7. This is a correctness and
boundary-hardening sequence, not a compressed feature milestone. Repairs are
grouped by invariant so overlapping findings produce one coherent change:

1. **R1 — dispatch ambiguity and bounded subprocesses (complete):** schema v17
   records provider-dispatch intent before the connector call and permits one
   terminal finalization; committed mailbox failures poison the worker bridge;
   candidate Git output is streamed to its actual memory ceiling; concurrent
   initializers re-read schema state under one exclusive migration lock.
2. **R2 — portable result-path identity (complete):** case/normalization alias
   collisions and alias-composed symlink escapes fail at hostile capture and
   target apply boundaries; one-for-one parent-directory case renames remain
   supported, and private retained state is not treated as adversarial.
3. **R3 — ownership and landing recovery (complete):** live integration claims
   are untakeable; expired attempt takeover advances versioned, audited owner
   authority and fences stale transition writers; a truthful third-revision
   `not_landed` receipt advances the observed ref and requires refold. Schema
   v18 preserves prior attempt events while adding the owner-change event.
4. **R4 — operational boundary resilience (complete):** live Krab failures are
   reduced to redacted categories; Docker output uses deterministic replacement
   decoding; stopped-worker read-only directory trees are reclaimable; all
   shipped Unix sidecars isolate malformed and idle clients and clean their
   sockets on controlled termination; queued cancellation stays independent of
   Docker construction.
5. **R5 — authority and protocol closure (complete):** retries, supervision,
   and the control room now account for the complete live connector authority;
   inactive local users cannot author control-plane mutations; interaction
   answers require a pristine post-question item from the bound active user;
   nested model overrides are removed centrally; and the OpenAI connector
   admits only text-bearing local-tool history. OAuth refresh, SSE parsing,
   raw-byte accounting, JSON frame expansion, and candidate blob/symlink
   identity collisions have matching regression proofs.
6. **R6 — lower-risk contract cleanup and final proof:** remaining bounded
   Docker/CLI reads, strict timestamp and schema-checker alignment, CLI/UI and
   documentation cleanup, focused repros, full mutation, deterministic live
   proofs, and the internal promise audit.

Each checkpoint is RED-first with happy and sad paths. The completed boundary
must repeat the test, branch-coverage, mutation, deterministic live, and
explicitly authorized real-account proofs before M8 begins.

## Milestone 8A: provider usage and budget authority

**Status: planned**

- Retain bounded normalized token and usage metadata from provider responses
  without retaining prompts, responses, or transcripts.
- Pin versioned per-run, per-project, and per-grant ceilings.
- Reserve a bounded invocation allowance before dispatch so concurrent calls
  cannot collectively overspend a ceiling that each observed independently.
- Surface reconciled usage, spend estimates, and remaining policy budget in the
  control room.

**Exit proof:** concurrent invocations cannot exceed a pinned budget, usage
totals reconcile with audited provider calls, and no private request or response
content enters the accounting ledger.

## Milestone 8B: quota-aware recovery

**Status: planned**

- Classify provider quota exhaustion and rate limits separately from permanent
  upstream failure.
- Honor bounded provider retry timing and move recoverable work into a durable
  paused state rather than burning attempts.
- Let the supervisor wake eligible work without a human acting as a timer.

**Exit proof:** a simulated quota outage pauses the same durable work, consumes
no calls while ineligible, and later resumes it under its original pinned
authority and remaining budget.

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

## Milestone 10: provider-neutral service sidecars

**Status: planned**

> **DESIGN ONLY — NOT IMPLEMENTED:** No sidecar protocol, registration,
> transport, capability invocation, or reconciliation state exists yet.

These service sidecars reuse the isolation pattern proven by the M7D inference
connector but expose different semantic capabilities and durable-effect
reconciliation. They are not model-inference connectors.

- Register a pinned sidecar protocol version, connector identity, operator-owned
  service/repository mappings, and advertised semantic capability families such
  as `work.*`, `change.*`, `checks.*`, `refs.*`, and `landing.*`.
- Keep integration effects supervisor-driven. A worker may receive separately
  authorized bounded service reads; it never receives a raw provider client or
  reusable credential.
- Bind each operation to an account and credential-scope fingerprint, exact
  inputs, limits, and a durable operation record.
- Reconcile uncertain outcomes after process or transport loss; a caller-side
  idempotency key alone is not treated as proof that a provider deduplicated the
  effect.
- Bound transport, response size, duration, rate, concurrency, logging, and
  typed error behavior without storing the credential or raw provider payload.

**Exit proof:** a deterministic fake sidecar loses a successful response, then
a replacement supervisor reconciles the already-created remote object rather
than duplicating it. Wrong connector, account, scope, repository, revision, or
capability requests fail before invocation. See
[service-sidecars.md](service-sidecars.md).

## Milestone 11: GitHub reference sidecar

**Status: planned**

> **DESIGN ONLY — NOT IMPLEMENTED:** No GitHub add-on or `GH_CONFIG_DIR`
> integration exists yet.

- Package GitHub mechanics outside the control-plane core and mount the user's
  explicitly approved `GH_CONFIG_DIR` only into that add-on.
- Implement the applicable semantic capability families for bounded work and
  change reads, refs, checks/approvals, and landing receipts.
- Use deterministic remote identities and read the provider's actual landed
  revision and closed-reference set after the operation settles.
- Report the guarantee a GitHub operation can actually enforce. Head-only merge
  preconditions are not represented as expected-base-and-head compare-and-swap.

**Exit proof:** the reference add-on advances one exact authorized candidate,
survives a lost response without duplication, and returns provider-verified
bounded evidence while its reusable credential remains absent from the worker,
core database, and logs. GitLab, Jira, and other services remain independent
add-ons that implement only the capability families they support.

## Continuous gates

Every milestone retains the existing RED-first workflow, happy-path and
sad-path coverage, at least 85% branch coverage, and at least an 80% mutation
kill rate. Security-sensitive boundaries also require a live integration test
against the real local dependency where practical.
