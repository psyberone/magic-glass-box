# Durable work memory

A worker attempt is disposable, and a coordinator process is disposable too.
The stable unit is a work item. Magic Glass Box gives each imported work item
one active coordination thread and pins every run for that item to the thread.
This lets several workers report independently and lets a replacement
coordinator reconstruct the current state without inheriting a model session
or transcript.

## Three records, three jobs

Work memory deliberately separates:

- **work-log entries**, which are append-only evidence from workers and
  coordinators;
- **checkpoints**, which are compact coordinator-authored views through one
  exact log sequence; and
- **assignments**, which delegate a bounded objective to one exact agent
  profile revision under a reclaimable lease.

The v1 JSON contracts are published in
[`schemas/work-log-entry-v1.schema.json`](../schemas/work-log-entry-v1.schema.json),
[`schemas/work-memory-checkpoint-v1.schema.json`](../schemas/work-memory-checkpoint-v1.schema.json),
and
[`schemas/work-assignment-v1.schema.json`](../schemas/work-assignment-v1.schema.json).
Examples live beside the other import examples.

A log entry has a typed kind, short summary, bounded details, artifact
references, related interaction IDs, and an optional candidate revision. The
orchestrator supplies its author run, attempt, profile revision, runtime
adapter, sequence, and timestamp. A worker cannot forge that provenance.

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

## Gateway operations

Profiles grant each operation separately:

- `work.read` pages through work memory;
- `work.append` adds an idempotent evidence entry;
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

The contracts reject message histories, private Krab bodies, provider payloads,
credentials, and transcripts. Nothing is exported to a source issue or pull
request. Entries and checkpoints are immutable audit records. A completion
policy may later seal a thread and compact what is presented as active memory;
it should not silently rewrite or delete evidence while work is active.
