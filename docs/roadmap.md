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
tree passed 556 tests, 87% aggregate coverage with branch measurement, the 25-test real Git/SQLite
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
tree passed 588 tests, 86% aggregate coverage with branch measurement, the
25-test exact-input gate proof,
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
gate passes with 638 tests and 86% aggregate coverage with branch measurement.

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
subtests, clean lint, and 86% aggregate coverage with branch measurement. At that checkpoint, the
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
The normal suite now passes with 708 tests and 86% aggregate coverage with branch measurement. The
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

**Status: completed 2026-10-17 — R1 through R6 complete**

Three independent cold promise audits followed M7. This was a correctness and
boundary-hardening sequence, not a compressed feature milestone. Repairs were
grouped by invariant so overlapping findings produced one coherent change:

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
6. **R6 — lower-risk contract cleanup and final proof (complete):** remaining
   bounded Docker/CLI reads, strict timestamp and schema-checker alignment,
   CLI/UI and documentation cleanup, focused repros, full mutation,
   deterministic live proofs, and the internal promise audit.

Each checkpoint was RED-first with happy and sad paths. The completed boundary
passed all 773 tests, Ruff formatting and lint, 86% aggregate coverage with
branch measurement, and a
clean-from-scratch 83.18% mutation score (31,103/37,393). All mutants were
classified: 6,227 survived, 27 timed out, 36 had no selected tests, and none
were suspicious, skipped, interrupted, or crashed. Mutmut's timeout-checker
thread lost a process-exit race at the tail; its three verified overdue
children received the same `SIGXCPU` it had intended and were recorded as
timeouts.

The supervisor, live Krab transport, committed-file gateway, provider gateway,
and rebuilt Codex/Claude runtime proofs passed. The first real-account repeat
made exactly one successful upstream call, then failed closed locally on a
new bounded Codex assistant `phase` field; no rejected continuation reached the
provider. After a RED-first allowlist repair, the single authorized retry
passed with one attempt, five `gpt-6.1-sol` connector invocations, one exact
retained result path, an exact-revision candidate, a passing validation gate,
denied direct worker networking, no credential exposure outside the connector
profile, and two landing commands issued but not executed.

## Milestone 8A: provider usage and budget authority

**Status: completed 2026-10-17**

- Retain bounded normalized token and usage metadata from provider responses
  without retaining prompts, responses, or transcripts.
- Pin versioned per-run, per-project, and per-grant ceilings.
- Reserve a bounded invocation allowance before dispatch so concurrent calls
  cannot collectively overspend a ceiling that each observed independently.
- Surface reconciled usage, spend estimates, and remaining policy budget in the
  control room.

**Exit proof:** concurrent reservations fail closed at invocation, run, grant,
and project ceilings; account consumption survives new runs and policy
revisions; exact reported usage releases unused allowance; and ambiguous usage
consumes the full reservation without retaining prompts or responses.

Schema v19 and connector protocol v2 passed 792 tests, Ruff format and lint,
86% aggregate coverage with branch measurement, and a clean-from-scratch 82.82% mutation score
(31,619/38,180). All mutants were classified: 6,497 survived, 28 timed out, 36
had no selected tests, and none were suspicious, skipped, interrupted, or
crashed. A single authorized real-account proof used one worker attempt and
five `gpt-6.1-sol` connector calls, reconciled 39,997 tokens to 23,877 µUSD of
operator-pinned estimated accounting under a hard $5 ceiling, retained one
exact result, passed its validation gate, denied direct worker networking,
kept credentials inside the connector profile, and issued two landing commands
without executing them. The estimate is policy accounting, not a provider
invoice or a promise about external quota enforcement.

## Milestone 8B: quota-aware recovery

**Status: completed 2026-10-18**

- Classify provider quota exhaustion, rate limits, and overload separately from
  permanent upstream failure without converting ambiguity into replay.
- Honor bounded provider retry timing and move recoverable work into a durable
  paused state. Stop and close the disposable attempt as interrupted without
  failing the same run.
- Let the supervisor wake eligible work without a human acting as a timer,
  while preserving manual launch policy and waiting for active attempts and
  open human interactions to resolve.

**Exit proof:** a simulated quota outage pauses the same durable work, consumes
no calls while ineligible, and later resumes it under its original pinned
authority and remaining budget.

Schema v20 proves the complete path with an explicit connector failure,
bounded retry timing, atomic invocation/recovery publication, worker-stop
signal, interrupted-attempt cleanup, exact eligibility boundary, and
supervisor requeue. The golden path retains the original run, source revision,
connector-bound grant, and budget ledger; a second provider call occurs only
after recovery. Manual work stops at `queued`, cancellation prevents a future
wake, generic transitions cannot forge or bypass `paused`, and an unanswered
Krab interaction can be answered during the pause but blocks wake-up until it
is resolved.

Schema v20 and M8B passed 808 tests, Ruff format and lint, and 86% branch
coverage. The identical production source passed a clean-from-scratch 82.91%
mutation gate (32,175/38,808) before final survivor-driven test-only hardening;
all mutants were classified: 6,571 survived, 26 timed out, 36 had no selected
tests, and none were suspicious, skipped, interrupted, or crashed. A focused
review then added exact lexical, delay, identifier, and diagnostic-provenance
assertions and targeted reruns killed all 24 behaviorally meaningful M8B
survivors. The four remaining M8B-named survivors are reviewed equivalents:
two overwritten initializer values, the timezone predicate equivalence for
`datetime.fromisoformat`, and a private guard already enforced by the public
atomic finalizer.

## Milestone 9: destructive continuity proof

**Status: completed 2026-10-18**

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

Schema v21 makes the existing verified result bundle the durable file
checkpoint for replacement recovery. Only results captured from `handed_off`
or `interrupted` attempts can become new recovery authority; failed,
cancelled, and successful attempts cannot promote incidental output. Each
replacement pins one immutable source attempt, result identity, source
revision, digest, path count, and byte count. The coordinator must stage the
pristine pinned revision, apply that exact checkpoint, verify the application
receipt, and record it before the replacement can enter `running`. Staging the
baseline first also keeps the replacement's eventual result aggregate against
the original revision instead of producing a patch-on-patch artifact.

The destructive golden proof launches a real child process in a real Git
snapshot workspace, waits until it has modified an accepted file, deleted a
tracked file, and written a partial new file, then kills it without a clean
handoff. Expired-owner reconciliation captures the untrusted workspace once,
validates and retains the exact result, removes the abandoned workspace, and
starts a replacement from a fresh snapshot plus that checkpoint. The
replacement observes the edit and deletion exactly, leaves the already
accepted file untouched, finishes the partial work, and produces an aggregate
result that reconstructs the expected final tree from a pristine snapshot.
The ledger and `run.describe` expose checkpoint provenance without exposing
manifest or file bodies.

Schema v21 and M9 passed 820 tests, Ruff format and lint, and 86% aggregate
coverage with branch measurement. The identical production source passed a
clean-from-scratch 83.06% mutation gate (32,513/39,146) before final
survivor-driven test-only hardening; all mutants were classified: 6,570
survived, 27 timed out, 36 had
no selected tests, and none were suspicious, skipped, interrupted, or crashed.
A focused review then added exact checkpoint-provenance, audit-receipt,
diagnostic, revision-mismatch, and cyclic-lineage assertions. Targeted reruns
killed all 37 selected behaviorally meaningful recovery-boundary survivors;
the remaining reviewed recovery-method survivor is SQLite keyword-case
equivalence for `BEGIN IMMEDIATE`.

## Post-Milestone 9 external-review stabilization

**Status: completed 2026-10-18**

Three independent cold reviews produced one boundary-focused remediation wave:

- reject Gitfile and `commondir` object-store redirects, symlinks or special
  entries beneath the object store, and non-empty local or HTTP alternates at
  source admission before snapshot and candidate import;
- retain every landing-plan nonce generation so a lost one-time nonce can be
  rotated without releasing the serialized ref lane, while exposing only the
  newest 32 generations in control-plane views;
- record current manual landing receipts as operator-attested evidence rather
  than claiming MGB independently verified the generated Git preconditions;
- require usage to be absent from failed or ambiguous inference replies, while
  retaining and charging successful provider-reported usage above the
  pre-dispatch estimate;
- separate worker request-byte authority from a 512-byte connector-owned model
  and output-limit expansion, and let workers narrow—but never raise—the pinned
  output-token ceiling;
- apply one monotonic HTTP deadline across all OpenAI and Krab phases, normalize
  allowed worker protocol headers case-insensitively, bound the worker bridge's
  idle sockets, and give the control room a capped daemon handler pool plus an
  absolute connection lifetime;
- validate the existing database state directory at entry, normalize direct
  work-item construction before chronological storage comparisons, and state
  the configured coverage metric accurately.

Schema v22 contains the append-only nonce authority and relaxed successful
reported-accounting constraint. The response-v2 portable contract now forbids
usage on non-success outcomes. The source repository is the hostile boundary;
once input is admitted, private control-plane state continues to use ordinary
permissions, transactions, digests, atomic publication, and locks rather than
same-user filesystem paranoia.

Schema v22 and the stabilization wave passed 841 tests, Ruff format and lint,
and 86% aggregate statement-plus-branch coverage. The candidate, gate, and
integration suites passed independently with 30, 25, and 25 tests. The
identical production source then passed a clean-from-scratch 83.09% mutation
gate (32,981/39,694): 6,642 survived, 35 timed out, 36 had no selected tests,
and none were suspicious, skipped, interrupted, or crashed.

## Milestone 10: provider-neutral service sidecars

**Status: completed 2026-10-19**

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

Schema v23 now carries immutable sidecar authority, durable semantic
operations, bounded dispatch attempts, leases, optimistic versions, typed
outcomes, rate/concurrency enforcement, and normalized receipts. Both the
deterministic client seam and the real Unix transport pass the lost-success
replacement proof with exactly one remote creation. The provider-specific
GitHub actuator and integration-queue binding intentionally remain M11.

Schema v23 and M10 passed 877 tests, Ruff format and lint, and 86% aggregate
statement-plus-branch coverage. The focused service-sidecar proof passed 33
tests and 24 subtests. The identical production source passed a
clean-from-scratch 83.23% mutation gate (34,506/41,459): 6,883 survived, 34
timed out, 36 had no selected tests, and none were suspicious, skipped,
interrupted, or crashed. A post-run survivor audit changed tests only and
reran all 1,291 mutants in the two M10 modules, killing 1,091 (84.51%) with
zero timeouts, uncovered mutants, suspicious results, skips, or crashes. This
killed meaningful authority, deadline, socket-boundary, retry, lease,
terminal-state, and reconciliation survivors.

### M10.1: M11-facing capability seams

**Status: completed 2026-10-19**

One bounded stabilization wave implemented the abstractions that the GitHub
reference provider immediately needs without introducing a generic plugin SDK:

- core-owned capability versions and read/effect modes, while leaving unknown
  syntactically valid extensions operator-only;
- explicit audited `(project, capability, version) -> sidecar mapping`
  bindings, with absence meaning disabled and no discovery or fallback;
- atomic operation-to-binding pins so unbinding cannot race new project-routed
  work into existence while already-created operations retain their history;
  and
- mode-aware recovery: ambiguous or expired reads are retryable, while effects
  remain uncertain and require reconciliation.

Schema v24 carries the bindings, append-only lifecycle events, and immutable
operation pins. The v1 repository mapping, provider-neutral transport, current
names, and direct operator-only operation path remain intact. M11 must use the
bound-operation path; it does not get a GitHub-specific project field.

Schema v24 and M10.1 passed 893 tests, Ruff format and lint, and 86% aggregate
statement-plus-branch coverage. A focused combined mutation pass over the two
changed service-sidecar contract/storage modules killed 170 of 212 selected
mutants (80.18%) with no timeouts, uncovered mutants, suspicious results,
skips, interruptions, or crashes. Survivor-driven tests closed project-scope,
inactive-project, protocol-version, identifier, and exact authorization gaps
before the score was accepted.

## Milestone 11: GitHub reference sidecar

**Status: completed 2026-10-21**

- Package GitHub mechanics outside the control-plane core and mount the user's
  explicitly approved `GH_CONFIG_DIR` only into that add-on.
- Implement the applicable semantic capability families for bounded work and
  change reads, refs, checks/approvals, and landing receipts.
- Use deterministic remote identities and read the provider's actual landed
  revision and closed-reference set after the operation settles.
- Report the guarantee a GitHub operation can actually enforce. Head-only merge
  preconditions are not represented as expected-base-and-head compare-and-swap.
- Consume schema v24 project bindings and atomically pinned operations rather
  than adding GitHub-specific routing state or resolving a controller in a
  separate transaction.

The current local implementation provides the separate
`magic-glass-box-github-provider` process, strict `GH_CONFIG_DIR` identity
fingerprinting, bounded `gh api` transport, exact candidate publication through
an allowlisted candidate root and `gh auth git-credential`, deterministic
change creation, paginated work/change metadata, import-ready exact work reads,
named latest checks/legacy statuses and reviewer decisions, and head-enforced
landing with actual-ref observation. Successful read receipts carry only a
bounded normalized result, never a raw GitHub response. Schema v25 atomically
binds integration authority to `landing.invoke`, records provider landing
requests/receipts, advances the actual provider revision, refolds on drift, and
projects pending operation state into the control room. Unit, schema,
migration, storage, CLI, and real-Unix carrier proofs are green.

The completed local checkpoint passes 974 tests and Ruff format/lint with 86%
aggregate statement-plus-branch coverage; the GitHub provider itself is at 90%.
The first focused provider/carrier/service-sidecar mutation wave killed an
effective 3,576 of 4,428 mutants (80.76%) after direct reruns of four timeout
mutants. The landing-ledger/service/storage wave killed an effective 5,304 of
5,943 (89.25%) after survivor-driven tests and direct reruns. A final clean pass
over the current GitHub provider killed 2,267 of 2,720 mutants (83.35%) with no
timeouts, uncovered mutants, suspicious results, skips, interruptions, or
crashes. Mutation-driven malformed-review tests then killed nine additional
semantic survivors directly, for an effective 2,276 of 2,720 (83.68%); the
remaining review-normalizer survivors change diagnostic wording only.

The authorized live proof used `psyberone/imitation-krab` at exact base
`50fcd1e55e8034e8ea3a931de0714e710e912b06`. The reference add-on published
candidate `f5c8d1fe58229a8e08515a0e07fba187a1758b33`, opened PR 1, passed its
bounded reads and zero-required-check gate, and squash-merged it. The harness
discarded the landing response and reconciled the already-landed PR to actual
base `83eaec71d6047e06f6a249852159c897871fc680` without issuing a second merge.
The first attempt found and then RED-first closed a carrier mismatch in which
`change.get` attached its head SHA to a metadata-only target; the head remains
available in the normalized result while the receipt now obeys the generic
target contract.

The post-live regression checkpoint passes 982 tests and Ruff format/lint with
86% aggregate statement-plus-branch coverage; the GitHub provider remains at
90%. A clean pass over the current provider killed 2,272 of 2,717 mutants
(83.62%) with no timeouts, unchecked mutants, crashes, or suspicious outcomes.
Survivor-driven assertions bind the exact-read HTTP status and require the
provider's merged-state field. The only surviving `change.get` mutants alter
its human-readable diagnostic label.

**Exit proof passed:** the reference add-on advanced one exact authorized
candidate, survived a lost response without duplication, and returned
provider-verified bounded evidence while its reusable credential remained
absent from the worker, core database, and logs. GitLab, Jira, and other
services remain independent add-ons that implement only the capability
families they support.

### First hands-on beta: producer/reviewer correction loop

**Status: complete, including live Imitation Krab exercise**

The first agent-written-code exercise uses `psyberone/imitation-krab` and two
ordinary durable runs: a producer and an independent reviewer with a different
profile and Krab identity. This remains an interim choreography rather than the
future workflow-template system.

Schema v26 closes the one missing return path. A reviewer requesting changes
must append
an actionable `review` work-log entry pinned to the exact candidate before the
attempt exits. `gate changes-create` accepts only the current failed
independent-review evidence and that exact reviewer-authored entry, then gives
a developer profile a fresh workspace rooted at the prior candidate.
Retries and reassignments keep the binding. A successful correction produces a
new child candidate with fresh gates; the prior candidate, policy, and
evidence are never rewritten. Reviewer profiles are structurally read-only,
and durable ancestry reconstructs corrected candidates after private-cache
loss.

The live proof must exercise both branches: request changes once and return the
feedback to the developer seat, then accept a freshly gated corrected candidate.
It must also retain the producer/reviewer negative preflight documented in the
internal beta contract.

The 2026-10-21 live proof did exactly that against Imitation Krab commit
`83eaec7`. The producer created a three-file health-command candidate. A
read-only reviewer requested a test-fixture correction because it attempted to create
temporary state inside the read-only source checkout. The developer correction
run began at that exact changes-requested head and changed only the test fixture. A new
reviewer approved the child, and the independent-review plus full-suite gates
became ready after all 102 tests passed. The negative preflight rejected both
the producer profile and a distinct profile sharing the producer identity.
Workers had no direct network or credential access; no branch, push, or pull
request operation ran. Accounted usage was $0.523237 against the $5 ceiling.

The exercise also proved restart recovery. The first connector process reached
an incorrectly configured 20-turn lifetime cap after the producer completed,
so the initial reviewer failed closed. MGB retained the producer candidate and
the failed policy-bound run; a fresh connector process and audited reviewer
retry resumed the exact candidate without rerunning the producer. The live
harness now defaults the connector lifetime cap to 64 turns while the durable
$5 budget remains the resource authority, and supports an explicit failed-
review resume path.

The first external product review then exposed a different missing boundary:
MGB had proved technical rigor but could not separately retain a maintainer's
judgment that a change belongs in Krab. Schema v27 adds an optional
exact-candidate maintainer gate, append-only `approved`/`changes_requested`
dispositions, and a provenance-preserving return path to the developer seat.
It also makes the project charter, work brief, and review gate explicit as
three different inputs. Passing technical gates no longer implies product
approval. The continuation proof must apply the four health-command
corrections, pass a fresh independent review and full suite, and stop at pending
maintainer approval rather than approving itself.

The 2026-10-22 continuation proof completed that choreography. The maintainer's
four-item disposition was bound to candidate `cnd_3b5a05d7530b5fe1306dcd314f25ca74`;
the producer returned a five-file child candidate
`cnd_5b49595ec00849e1193d36cbc913445d` at `6f0e67a`. A fresh read-only reviewer
approved the exact child after its 105-test suite passed, and MGB's separate
networkless full-suite validation passed. Readiness stopped at two passed gates
and one pending maintainer gate, so no landing authority or command was issued.
The continuation consumed $0.292140, bringing total live-beta accounting to
$0.815377 of the durable $5 ceiling. Credentials remained connector-only, both
workers had direct network denied, and the Imitation Krab checkout stayed clean
at its newer `50fcd1e` head. The report is retained in the private beta state as
`maintainer-round-report.json`; it is the honest preapproval checkpoint and was
not rewritten after the maintainer acted.

On 2026-10-23 the maintainer separately approved the exact candidate through
maintainer review `mrv_96825270dac45883decd5d0e2a3be406`. Readiness became
three of three gates passed. The operator then pushed exact head `6f0e67a`,
opened `psyberone/imitation-krab` PR 2, and the maintainer squash-merged it as
`7d70b045f0976985b493786ab138cebc81d65cc9`. The landed commit's tree
`f4b27729de354d16cfb7666e33bc024b3938aee3` exactly equals the approved
candidate tree. Imitation Krab subsequently released 0.6.4 on top. The target
checkout remained unmodified throughout the handoff.

That final step also exposed the next honest gap. The beta intentionally never
enqueued this candidate or issued a manual/provider landing plan, so MGB's
integration ledger correctly contains no landing receipt. It must not invent
one after the fact. `work-item complete` also correctly refused to seal the
thread: its latest entry is the independent review, not a coordinator-authored
final `outcome`. The code landed, but the operator still performed the
cross-stage choreography and closeout. Milestone 12 exists to make that proven
sequence durable and supervisor-driven.

The post-change local checkpoint passes 1,018 tests and Ruff format/lint with
86% aggregate statement-plus-branch coverage. A focused schema-v27 mutation
wave covered 5,780 mutants in every changed production function. It carried
forward 4,759 kills from the same production source, reran all 1,021 prior
survivors or unclassified mutants, and killed 562 of them. Survivor-driven
boundary and provenance assertions then killed 48 more, for a conservative
5,369/5,780 result (92.89%). The remaining survivors change diagnostic prose,
redundant internal fields, or pre-existing broad-function behavior outside the
new maintainer authority path. There were no timeouts, uncovered mutants,
suspicious outcomes, interruptions, or crashes in the focused waves.

The preceding schema-v26 checkpoint passed 1,002 tests and Ruff format/lint
with 86% aggregate statement-plus-branch coverage. A focused mutation wave
over the correction-run binding, replacement propagation, candidate creation and
recovery call site, review workspace, gate evidence, and execution projection
killed 82 of 98 mutants (83.67%). Survivor-driven tests added a genuinely cold
parent-cache recovery, the 64-byte work-item boundary, and exact review-evidence
metadata on replacement events. The 16 remaining mutants change diagnostic
labels or capitalization only; there were no timeouts, uncovered mutants,
suspicious outcomes, skips, interruptions, or crashes.

Because covered-line discovery did not instrument the newly imported recovery
module, a second isolated run deliberately mutated every one of its lines. The
first wave killed 73 of 92; direct survivor tests for cycles, the 32-ancestor
ceiling, every parent-identity field, recovery order, and exact result digests
killed ten more, for an effective 83 of 92 (90.22%). Its nine remaining
survivors change only `CandidateError` diagnostic text.

The broader [capability-provider model](capability-providers.md) does not expand
the M10 or M11 MVP. It names the semantic surface, provider implementation,
explicit project binding, and sidecar deployment separately. M11 remains one
GitHub provider. If a project binds no provider to an operation, that operation
is disabled; later Jira, GitLab, runner, or other providers are added only when
their real use cases justify a versioned capability contract.

## Milestone 12: bounded implementation workflow

**Status: in progress; M12A complete, M12B next**

M11 proved every important step of one real change, but the operator still
selected and invoked those steps. M12 packages that exact practice as one
fixed, versioned `implementation-v1` prescription. It is not an arbitrary
workflow engine. See [workflow-templates.md](workflow-templates.md).

### M12A: immutable prescription ledger

**Status: complete (2026-10-23)**

- Store immutable built-in definition revisions, saved configuration versions,
  and workflow executions as three separate records.
- Bind producer and reviewer roles to exact active profile versions. A saved
  configuration may reduce limits but may not grant authority absent from the
  selected profiles or project.
- Pin each execution to its exact project context, work-item version, active
  work-thread generation, source revision, definition revision, and
  configuration revision.
- Expose a bounded redacted projection and audited cancel operation. Definition
  or configuration edits affect only future executions.

**Exit proof:** after a process restart, MGB reconstructs the exact definition,
configuration, roles, input revisions, and current stage. Stale work-thread,
inactive pinned-role, context, work-item, and configuration inputs—and malformed
source object IDs—fail before any run is created, and a repeated start is
idempotent. An active workflow also prevents its work thread from being sealed
underneath it; audited cancellation releases the thread.

Schema v28 implements that proof with the immutable built-in
`implementation-v1` definition, separately versioned saved configurations, an
optimistic execution/event ledger, CLI and service surfaces, and a redacted
control-room projection. Starts bind the exact work-thread generation as well
as its optimistic version, which closes the otherwise ambiguous
generation-two/version-one reopen case. M12A deliberately records authority
only; it creates no producer run and claims no orchestration yet.

The final checkpoint passes 1,037 tests and Ruff format/lint with 87% aggregate
statement-plus-branch coverage. The workflow policy model is at 100% and its
storage layer is at 92%. An all-line Mutmut wave over the storage boundary
killed 853 of 1,014 mutants (84.12%), with no timeouts, unchecked mutants,
crashes, or suspicious outcomes. Independent digest oracles bind every field
of both immutable authority envelopes rather than merely accepting a
self-consistent stored hash. A separate focused check killed 10 of the 11
mutations on the new work-thread lifecycle guard; the survivor changes only
diagnostic wording. Mutmut 3.6 does not generate mutations inside the decorated
workflow dataclasses; their parser and serialization branches are covered
exhaustively instead.

### M12B: build/review/correction director

- Drive ordinary producer, candidate, gate, reviewer, validation, and
  correction primitives through durable supervisor actions.
- Make transition decisions from authoritative object state, never agent prose.
  A producer handoff cannot satisfy a review gate, and approval applies only to
  the reviewed exact head.
- Deliver changes-requested evidence to the configured producer, create one
  child candidate, and require fresh review and validation. Retries after a
  dead supervisor cannot duplicate a reviewer or correction run.
- Enforce a bounded correction-round ceiling. Crossing it opens a durable
  coordinator/maintainer interaction instead of silently scheduling another
  round.

**Exit proof:** a deterministic workflow requests changes once, survives an
orchestrator restart at each transition boundary, resumes the exact child, and
stops at maintainer disposition with exactly one durable action per stage.

### M12C: landing and closeout director

- Enqueue only the ready exact candidate and preserve M7 dependency, serialized
  lane, refold, and gate recheck rules.
- Select the configured provider route or explicitly issue a manual landing
  plan. Absence of a provider means disabled or manual according to pinned
  configuration; there is no credential or provider fallback.
- Require a matching provider or nonce-bound manual receipt before producing
  the final bounded outcome and sealing the work thread.
- Keep externally performed, non-authorized changes distinguishable from MGB
  landing receipts; observations never inherit an enforcement claim.

**Exit proof:** a second live repository change goes from imported work item to
merged exact candidate and completed work thread, including one rejection
round, maintainer approval, restart recovery, and credential containment. No
human must remember which durable command advances the next mechanical stage.

## Milestone 13: discovery and review prescriptions

**Status: candidate milestone; schedule after the M12 live proof**

- Add a bounded Discovery prescription that produces a structured ruling and
  work-ready brief without changing source.
- Distinguish gate review, which can produce exact-candidate evidence, from
  advisory multi-reviewer configurations such as The Furies, which cannot.
- Carry MUST items only from the immutable work brief, retain reviewer PROBE
  findings as follow-up proposals, and keep budgets fixed rather than allowing
  a review to expand indefinitely.
- Consider delta re-review only behind an explicit path allowlist and a
  mechanical diff classification; it remains off by default.

## Milestone 14: heterogeneous work provider

**Status: candidate milestone; first pressure test of non-repository resources**

- Advance resource bindings without pretending every external resource is a
  repository.
- Implement one bounded work-system provider—Jira is the leading real use
  case—for normalized list/get/import and explicitly selected status effects.
- Prove a mixed project whose work comes from Jira while candidates, checks,
  and landing remain on GitHub. Agents see only semantic capabilities and MGB
  correlation IDs.

## Milestone 15: controlled remote execution provider

**Status: candidate milestone**

- Define submit, inspect, cancel, reconcile, and evidence-collection semantics
  for an exact candidate and operator-defined test recipe.
- Implement the first controlled runner against an operator-owned remote host
  such as the existing Azure VM. The agent never receives SSH, hostname,
  arbitrary command, or reusable credential authority.
- Retain bounded result identity, exit status, timing, recipe/image identity,
  and output digest; collect artifacts through the same hostile-output boundary
  used for local workers.

## Milestone 16+: provider breadth by demonstrated need

**Status: candidates, not commitments**

- A second forge provider, likely GitLab, must pass the capability conformance
  suite and expose only the operations it truthfully supports.
- Bitbucket, raw Git actuators, additional work systems, interaction carriers,
  hosted agents, local agents, and inference providers remain independent
  additions selected by real use cases.
- Provider breadth does not weaken the rule that no binding means no
  capability. MGB never guesses a controller or exposes a provider-native
  escape hatch to an agent.

## Optional provider-auth track: Anthropic Console OAuth connector

**Status: planned, non-blocking — does not gate or reorder Milestones 8B–11**

This track is intentionally outside the numbered milestone sequence. It may be
scheduled after Milestone 11; pulling it forward requires an explicit roadmap
decision rather than silently displacing an existing item.

> **DESIGN ONLY — NOT IMPLEMENTED:** No Anthropic connector module, CLI,
> private configuration, example, or live proof exists yet. Claude Code
> currently runs only through the API-key reference connector.

Verified 2026-10-17: Anthropic publishes no equivalent of OpenAI's Sign in
with ChatGPT. Its Claude Code terms forbid third-party applications from
offering claude.ai login, routing requests through Free, Pro, or Max plan
credentials, or intermediating session tokens. Anthropic also publishes no
supported third-party route for presenting those subscription tokens outside
Claude Code. The M7D connector shape, which re-originates the HTTPS request
from its own process and holds the token in a connector-private store, is
exactly that prohibited pattern for a subscription token. MGB therefore
builds no subscription pass-through for Anthropic and never imitates Claude
Code's identity headers to obtain one. The supported analog is a Claude
Console OAuth profile: the ChatGPT connector's operator experience (sign in,
no static secret, short-lived refreshable token, revocation, non-secret scope
fingerprint) with API-rate billing to the Console organization, the same
economics as the existing API-key route.

**Subscription-auth watch:** revisit this boundary only when Anthropic
publishes a supported third-party flow for Claude subscription credentials.
That event triggers a fresh design and legal review; it does not automatically
reorder this roadmap. Any resulting adapter must preserve connector-owned
credentials and the existing worker-network isolation.

- Delegate sign-in to Anthropic's own `ant auth login`, passing
  `--no-browser` and `--workspace-id` through. Anthropic currently publishes
  no third-party OAuth client-registration flow, so the connector implements
  no authorization, token, or refresh exchange of its own. It runs the CLI with
  `ANTHROPIC_CONFIG_DIR` pointed at a connector-private directory and the
  fixed profile name `mgb`, and never reads `~/.config/anthropic`,
  `~/.claude`, or any other installation's credentials.
- Obtain the bearer token per invocation from
  `ant auth print-credentials --profile mgb --access-token`, which
  refreshes the roughly ten-minute token itself. The connector holds the
  token only for the duration of one invocation, serializes CLI calls
  under one lock, and never caches, logs, or returns it.
- Derive the credential-scope fingerprint from non-secret profile metadata
  read through `ant profile get --profile mgb --format json`: the SHA-256
  of canonical JSON `{"base_url", "organization_id", "workspace_id"}`. A
  profile whose authentication type is not `user_oauth`, whose base URL is
  not `https://api.anthropic.com`, or which lacks either ID fails closed.
- Send `Authorization: Bearer`, `anthropic-version: 2023-06-01`, and the
  profile's `anthropic-workspace-id`, and merge `oauth-2025-04-20` into
  the worker-forwarded `anthropic-beta` list by flag name, as the official
  SDK does. The connector sends neither `x-api-key` nor any Claude Code
  identity header; its user agent names this connector.
- Accept only the pinned Claude Code request shape before HTTPS dispatch,
  mirroring the OpenAI allowlist. A 2026-10-17 capture of
  `claude --print --bare` 2.1.302 sends `POST /v1/messages?beta=true`
  with exactly `messages`, `system`, `max_tokens`, `stream`, `tools`,
  `metadata`, `thinking`, `context_management`, and `output_config` (plus
  `model`, which the worker bridge removes); declares only the local
  `Bash`, `Edit`, and `Read` tools with `name`, `description`, and
  `input_schema`; and requests the betas `claude-code-20250219`,
  `context-1m-2025-08-07`, `interleaved-thinking-2025-05-14`,
  `thinking-token-count-2026-05-13`, `context-management-2025-06-27`,
  `prompt-caching-scope-2026-01-05`, `per-turn-control-2026-07-01`, and
  `effort-2025-11-24`. The capture is repeated against the image-pinned
  CLI before the allowlist is frozen, and each route pins its allowed
  beta set. Content is limited to `text`, `tool_use`, text-only
  `tool_result`, `thinking`, and `redacted_thinking` blocks with optional
  `cache_control`; `tool_choice` may name only declared tools; `metadata`
  is exactly `{"user_id": <string>}`; and the trailing `system`-role
  message carrying only `output_config.effort` is accepted solely when
  `per-turn-control-2026-07-01` is forwarded. Any other top-level field,
  beta flag, tool `type` (server, built-in, or MCP tools), `image`,
  `document`, or hosted-tool block fails closed.
- Replace the worker-selected model with the grant's pinned model, require
  `stream: true`, and clamp `max_tokens` (Claude Code asks for 64,000) to
  the v2 budget's `max_output_tokens`.
- Normalize Messages usage for M8A: `input_tokens` plus
  `cache_creation_input_tokens` from `message_start` become
  `input_tokens`, `cache_read_input_tokens` becomes `cached_input_tokens`,
  the final `message_delta` count becomes `output_tokens`, and reported
  thinking tokens become `reasoning_output_tokens`. A completed stream
  missing either usage object fails closed; cache-write premiums stay an
  estimate.
- Preserve Anthropic's machine-readable error type (`authentication_error`,
  `rate_limit_error`, `overloaded_error`, and so on) and HTTP status as
  bounded failure metadata so M8B can classify quota and overload
  responses. A truncated stream, non-SSE success body, or any
  post-dispatch loss is ambiguous and is never replayed.

If scheduled, this track lands as three internal checkpoints with M7D's
RED-first gates, the normal quality gate at each checkpoint, and the clean
mutation gate at the completed boundary:

1. **Anthropic.1 — profile ownership and operator surface:** add
   `src/magic_glass_box/anthropic_console.py` with an `AntCLI` protocol,
   `AntProcessCLI`, `AnthropicProfile`, `AnthropicConnectorRoute` (with an
   explicit `betas` list), and `AnthropicConnectorConfig`; add
   `src/magic_glass_box/anthropic_console_cli.py` with `login`, `status`,
   `models`, and `serve`; register the
   `magic-glass-box-anthropic-connector` entry point and
   `scripts/anthropic_console_connector.py`; and move the framed
   request/response helpers from the OpenAI CLI into `sidecar.py` so both
   connectors share one carrier. `AntProcessCLI` runs `ant` with only
   `PATH`, `HOME`, and `ANTHROPIC_CONFIG_DIR` in its environment, a
   30-second timeout, and a 64 KiB output ceiling; the profile directory
   must be an operator-owned mode-`0700` directory that is not a symlink.
   Tests in `tests/test_anthropic_console.py` and
   `tests/test_anthropic_console_cli.py` inject a fake CLI and prove that
   `status` never prints a token, that a scrubbed environment and explicit
   profile reach the CLI, that non-`user_oauth` and non-default base URL
   profiles are refused, and that the fingerprint is stable. No Python
   dependency is added.
2. **Anthropic.2 — bounded Messages transport:** add `AnthropicMessagesClient`
   over the existing `BoundedHTTPSClient` and `AnthropicConsoleConnector`,
   whose authority check mirrors `OpenAIChatGPTConnector` with
   `provider == "anthropic"` and `protocol == "anthropic-messages"`.
   Tests replay the captured request through the allowlist, assert the
   merged beta and bearer headers, reject every disallowed field and tool
   type before HTTP, classify terminal, error, and truncated streams, and
   fail closed on missing usage. The deterministic runtime proof
   additionally records the tool names Claude Code declares and asserts
   `Bash`, `Edit`, and `Read`, so a CLI bump that adds hosted tools fails
   the gate. The reference connector and `ClaudeCodeRuntimeAdapter` are
   unchanged: `scripts/reference_inference_connector.py` resolves its
   credential once at startup and `provider.py` lets static headers
   override forwarded ones, so neither can carry a rotating bearer token
   with a merged beta list.
3. **Anthropic.3 — bounded live proof:** add
   `scripts/check_live_anthropic_console.py` and
   `tests/test_live_anthropic_console.py` modeled on the OpenAI proof:
   `--i-understand-this-bills-the-console-org` is required, one attempt,
   no replay, `--max-invocations 6`, a 300-second ceiling, v2 budgets with
   a $1 per-call maximum and a $5 run/project/grant ceiling at rates
   pinned to the most expensive allowed model's list price, networkless
   worker and gate containers, the credential-leak audit on success and
   failure, and a recorded `ant --version`. Add
   `examples/inference-connector-anthropic.json` and
   `examples/inference-grant-anthropic.json`, and document the operator
   flow in [inference-connectors.md](inference-connectors.md).

**Exit proof:** a deterministic fake `ant` and SSE fixture complete one
authorized `anthropic-messages` invocation through the new connector while
wrong provider, protocol, scope, model, beta, tool type, and limit tuples
fail before HTTPS. One explicitly opted-in Claude Code run then completes a
disposable task billed to the operator's Console workspace; the token
exists only inside the connector process and Anthropic's own profile
directory, never in the worker container, MGB state or SQLite, retained
results, runtime output, or logs. The proof reports reconciled usage under
the pinned ceiling and stops at an issued landing plan. Subscription
authentication is not inferred from this proof. A Workload Identity
Federation profile for a daemonized connector is a later option that reuses
the same module. See Anthropic's
[CLI authentication](https://platform.claude.com/docs/en/cli-sdks-libraries/cli/authentication)
and
[Claude Code legal and compliance](https://code.claude.com/docs/en/legal-and-compliance)
pages.

## Continuous gates

Every milestone retains the existing RED-first workflow, happy-path and
sad-path coverage, at least 85% aggregate coverage with branch measurement,
and at least an 80% mutation kill rate. Security-sensitive boundaries also
require a live integration test against the real local dependency where
practical.
