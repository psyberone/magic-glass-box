# Durable work memory

A worker attempt is disposable, and a coordinator process is disposable too.
The stable unit is a work item. Magic Glass Box gives each imported work item
one active coordination thread and pins every run for that item to the thread.
This lets several workers report independently and lets a replacement
coordinator reconstruct the current state without inheriting a model session
or transcript.

## Three records, three jobs

Work memory deliberately separates:

- **work-log entries**, which are append-only evidence from workers and the
  trusted control plane;
- **checkpoints**, which are compact coordinator-authored views through one
  exact log sequence; and
- **assignments**, which delegate a bounded objective to one exact agent
  profile revision under a reclaimable lease.

The general v1 JSON contracts are published in
[`schemas/work-log-entry-v1.schema.json`](../schemas/work-log-entry-v1.schema.json),
[`schemas/work-memory-checkpoint-v1.schema.json`](../schemas/work-memory-checkpoint-v1.schema.json),
and
[`schemas/work-assignment-v1.schema.json`](../schemas/work-assignment-v1.schema.json).
Typed exact-candidate reviewer verdicts use the narrower
[`schemas/work-log-entry-v2.schema.json`](../schemas/work-log-entry-v2.schema.json)
contract. Examples live beside the other import examples.

A log entry has a typed kind, short summary, bounded details, artifact
references, related interaction IDs, and an optional candidate revision. The
orchestrator supplies its author run, attempt, profile revision, runtime
adapter, sequence, and timestamp. A worker cannot forge that provenance.
The sole non-worker form is the final `outcome` written by the M12 workflow
closeout transaction; it carries the acting local user and workflow execution
instead of invented run/attempt provenance.
Schema v2 is reserved for `review` entries and adds a required
`review_outcome` of `approved` or `changes_requested`. It requires the exact
candidate revision, and a changes-requested verdict requires at least one
actionable detail. Storage accepts that verdict authority only from a
policy-bound reviewer attempt against the named exact head and permits one
typed verdict per attempt. Summary wording remains ordinary evidence text.

A checkpoint contains a summary, accepted decisions, artifact references,
next steps, blockers, and an optional candidate revision. Creating one requires
the current thread version and must cover the current log head. If another
worker appends or changes an assignment after the coordinator reads, the stale
checkpoint fails instead of silently omitting that work. An identical retry is
idempotent.

An assignment contains only its title, objective, scope paths, acceptance
checks, and optional base revision. The coordinator selects an active profile
by name, but storage pins its exact immutable profile-version ID. Only a run
using that revision may claim it. Claims have bounded leases; after expiry, a
compatible replacement run can reclaim the assignment. Completion atomically
appends an `assignment_result` entry and closes the assignment. A v1 thread is
bounded to 16 assignments so even worst-case contracts fit the gateway's
response ceiling.
Assignment views retain the latest 16 lifecycle events plus a total count and
truncation flag, keeping repeated lease recovery from making gateway responses
unbounded.

## Replacement flow

1. A coordinator appends its plan and checkpoints the log head.
2. It creates independent assignments for the desired Claude, Codex, or other
   profiles.
3. Worker runs claim those assignments through their own run-bound gateways.
4. Each worker completes independently. SQLite serializes sequence allocation,
   so concurrent results cannot overwrite one another.
5. A replacement coordinator calls `run.describe`. It receives the latest
   checkpoint, the entries after that checkpoint, and assignment state.
6. If the tail is larger than the initial bounded page, it continues with
   `work.read` and the returned sequence cursor.

No coordinator election or model session is durable. The checkpoint plus the
append-only tail is the recovery protocol.

## Completion and a fresh cycle

Run success does not complete a work item. Completion is an explicit operator
action outside the worker gateway and succeeds only when:

- every run pinned to the active thread is terminal;
- every assignment is completed or cancelled; and
- the latest log entry is a typed `outcome`.

The operator supplies the exact observed thread version for ordinary manual
completion. A stale completion fails, and a successful completion appends an
immutable lifecycle event naming the operator and final outcome entry. The
fixed implementation workflow may instead perform the same checks and create
its bounded final outcome atomically after a matching landed receipt. It also
records the exact workflow, queue, candidate, policy, receipt, and observed
revision before completing the execution. The thread is then sealed: old runs
can still read its evidence, but no worker can append, checkpoint, assign,
claim, or complete more work against it.

Nothing is deleted. If work resumes, an operator explicitly reopens the work
item using the completed thread's exact version. Magic Glass Box creates a new
active generation with an empty log, no checkpoint, and no assignments. New
runs pin that generation; historical runs stay pinned to the sealed one.
Importing newer source content while a work item is completed does not reopen
it or create new authority.

```console
magic-glass-box work-item show wki_<id>
magic-glass-box work-item complete wki_<id> \
  --expected-thread-version <version> --completed-by operator
magic-glass-box work-item reopen wki_<id> \
  --expected-thread-version <version> --reopened-by operator
```

## Gateway operations

Profiles grant each operation separately:

- `work.read` pages through work memory;
- `work.append` adds an idempotent general v1 entry or typed review-v2 entry;
- `work.checkpoint` publishes an optimistic compact view;
- `work.assign` creates a profile-pinned assignment;
- `work.assignment.claim` acquires or reclaims its lease; and
- `work.assignment.complete` atomically records its result.

As with every gateway operation, the mailbox fixes run identity outside the
request. Payloads cannot select a work item, thread, project, credential,
provider, URL, or arbitrary destination.

## Trust and retention boundary

Work memory is structured operational evidence, not an authority channel.
Text in an entry or checkpoint cannot change profile, scope, credentials,
network policy, project membership, or lifecycle. Interaction references are
accepted only when they belong to a run on the same work thread. Artifact
paths are metadata references and must be safe relative POSIX paths; file
content is not copied into the log.

That authority separation does not make the prose safe instructions. A later
model receives earlier agent-authored summaries and details as structured but
still untrusted data. The self-describing gateway catalog labels replayed
work-memory, checkpoint, assignment, and handoff subtrees as
`untrusted_content` with explicit source provenance. Host-side storage and
capability checks remain authoritative even when instruction-shaped text is
replayed from an earlier producer.

The contracts reject message histories, private Krab bodies, provider payloads,
credentials, and transcripts. Nothing is exported to a source issue or pull
request. Entries, checkpoints, sealed threads, and lifecycle events are
immutable audit records. Completion changes which generation is active; it
never rewrites or deletes evidence.
