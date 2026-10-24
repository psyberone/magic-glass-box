# Agent gateway

The agent gateway is the only control-plane capability exposed to a
`gateway_only` worker. Docker networking remains `none`; no Krab or provider
token enters the container. The orchestrator fixes the run identity,
credential-name policy, and operation allowlist before the worker starts.

## Profile contract

A profile opts into each operation explicitly:

```json
{
  "image": "agent@sha256:<digest>",
  "command": ["agent", "run"],
  "workspace_mode": "copy_on_write",
  "network_mode": "gateway_only",
  "credential_names": ["krab", "openai-main"],
  "gateway_capabilities": [
    "run.describe",
    "interaction.open",
    "interaction.read",
    "handoff.submit",
    "provider.invoke",
    "work.read",
    "work.append",
    "work.checkpoint",
    "work.assign",
    "work.assignment.claim",
    "work.assignment.complete"
  ]
}
```

`credential_names` are authority references, not container mounts. Interaction
operations require the `krab` name, and the orchestrator resolves the pinned
Krab identity only while performing that operation. Unknown capabilities,
duplicates, and interaction capabilities without `krab` fail validation.
`provider.invoke` requires at least one non-Krab grant name. Grant existence,
state, and expiry are checked when a profile revision is created, when a run
pins it, and again for every invocation.

## Carrier contract

The container receives `MAGIC_GLASS_BOX_GATEWAY_DIR=/run/magic-glass-box`.
That directory contains four mode-0600 regular files:

| File | Worker access | Purpose |
| --- | --- | --- |
| `request.json` | read/write | One UTF-8 JSON request followed by `\n` |
| `request.ready` | read/write | Commit for the complete request |
| `response.json` | read-only | One UTF-8 JSON response followed by `\n` |
| `response.ready` | read-only | Commit for the complete response |

The directory itself is mounted read-only. Docker overlays only the two request
files as writable bind mounts, so the worker cannot add paths or replace the
response files. This regular-file carrier works across native Linux and Docker
Desktop, where host Unix sockets do not cross the VM-backed bind boundary.

A commit is a newline-terminated, canonical JSON object:

```json
{"sequence":1,"sha256":"<64 lowercase hex characters>","size":123}
```

The worker writes and flushes `request.json`, then writes and flushes
`request.ready`. Sequences are positive, strictly increasing safe integers.
The orchestrator reads a request only when its exact byte length and SHA-256
match the commit. It writes and flushes `response.json` before publishing the
matching sequence in `response.ready`. Requests are limited to 3 MiB and
responses to 9 MiB so a 2 MiB vendor request or escaped 4 MiB buffered SSE
response still fits its committed envelope. Malformed frames, duplicate JSON
keys, non-finite numbers,
torn commits, oversized files, and replayed sequences fail closed.

Mailbox content is ephemeral transport state, never SQLite state. Normal run
cleanup removes it. Restart reconciliation removes abandoned mailbox files
while preserving durable `queued` and `waiting_for_input` run state.

## Request envelope

Every request has exactly four fields:

```json
{
  "version": 1,
  "request_id": "agent-generated-stable-id",
  "operation": "run.describe",
  "payload": {}
}
```

The request never carries a run ID, credential reference, URL, or token. Run
identity is bound to the mailbox by the orchestrator. A successful response
echoes `version` and `request_id`, sets `ok` to `true`, and contains `result`.
A failure sets `ok` to `false` and contains a bounded `{code, message}` error.
Unexpected storage, transport, and implementation errors are redacted.

## Operations

### `run.describe`

Payload: `{}`.

Returns the current run state and version, pinned project key and source
revision, sanitized project context, immutable work item, authority summary,
bounded interaction metadata, latest structured handoff, bodyless recovery
checkpoint provenance when the current attempt used one, and the initial
bounded work-memory page. Recovery provenance names only the source run,
attempt, result, revision, digest, path/byte counts, and application time. Work
memory contains the latest checkpoint, entries after it, and assignment state
with at most the latest 16 lifecycle events per assignment. It omits the host
repository path, image and command details, credential references, file bodies,
and secret values.

### Work-memory operations

`work.read` pages the bound work item's log from an optional sequence with a
limit of 1 through 16. `work.append` accepts one general v1 work-log entry or
one typed review-v2 entry, an optional assignment ID, and a stable idempotency
key. Review v2 is accepted only from a policy-bound reviewer attempt against
its exact candidate head. The gateway supplies run identity and storage
supplies attempt/profile/runtime provenance.

`work.checkpoint` accepts one v1 checkpoint, the exact log sequence it covers,
the observed thread version, and an idempotency key. Both the version and log
head must still match. `work.assign` similarly uses the observed thread
version, selects an active target profile by name, and stores its exact profile
revision with the v1 assignment document.

`work.assignment.claim` accepts an assignment ID, observed assignment version,
and a lease of 30 through 86,400 seconds. Only a run pinned to the target
profile revision can claim it; an unexpired lease cannot be stolen.
`work.assignment.complete` accepts the observed assignment version, stable
idempotency key, and an `assignment_result` entry. The result append and state
transition are atomic. Full contracts and recovery semantics are in
[work-memory.md](work-memory.md).

### `interaction.open`

Payload fields:

- `kind`: `question`, `approval`, or `choice`;
- `recipient_handle`: the bound human's Krab handle;
- `title` and `body`: private untrusted content sent to Krab; and
- `idempotency_key`: a stable retry key.

The gateway obtains the run's current optimistic version itself. A verified
Krab delivery opens durable metadata, transitions the run to
`waiting_for_input`, and returns
`agent_action: "submit_handoff_then_exit"` when the profile also authorizes
`handoff.submit`. Profiles without that capability retain the legacy `exit`
instruction; their attempt is explicitly marked interrupted because no
replacement state was supplied.

### `handoff.submit`

The payload is exactly the bounded v1 document published at
[`schemas/handoff-v1.schema.json`](../schemas/handoff-v1.schema.json). It names
the interaction ID returned by `interaction.open`, contains work-state metadata
rather than messages or a transcript, and has a 24 KiB canonical limit.

The handoff must match the active attempt and its interaction. An identical
retry is idempotent; different content cannot replace an accepted handoff. The
gateway returns `agent_action: "exit"`. The worker should then exit
successfully; its bounded file result is captured before its container,
mailbox, and mutable workspace are removed.

### `interaction.read`

Payload: `{"interaction_id":"int_<id>"}`.

Only an answered interaction belonging to the mailbox's bound run is accepted.
The orchestrator re-fetches the response from Krab and re-verifies project,
session, creator, recipient, item ID, and status before returning transient
untrusted title/body content. The response body is not written to SQLite.

### `provider.invoke`

Payload:

```json
{
  "grant": "openai-main",
  "request": {"input": "hello"}
}
```

The grant must be named by the pinned profile. The payload has no run ID,
destination, method, credential, or worker-selected model. The orchestrator
requires a live `running` attempt and permits the enclosing run to be either
`running` or `waiting_for_input`. The latter keeps a worker alive just long
enough to submit its handoff after opening a question; a parked run with no
live attempt has no provider authority. The orchestrator checks grant and
connector authority, then atomically rechecks revocation/expiry and
reserves grant- and connector-wide cross-process concurrency immediately before
dispatch. It constructs the strict invocation from the pinned connector,
route, semantic protocol, model, scope fingerprint, byte ceilings, and absolute
deadline. Only the separately launched connector owns the upstream credential
and network transport. The reservation is released after dispatch and expires
if its owning process dies.

The JSON object response is transient and explicitly untrusted. Only status,
byte counts, outcome, error category, timestamps, and the exact connector/grant
authority tuple are durable. A lost or invalid post-dispatch reply is recorded
as ambiguous and is never replayed automatically. See
[inference-connectors.md](inference-connectors.md).

An explicit quota, rate-limit, or overload response is different: the gateway
returns a redacted retryable error with a bounded delay after the invocation
and provider recovery are durably committed. The run is already `paused`, so a
worker-side vendor retry cannot dispatch another model call. Its loopback HTTP
bridge receives status 429 and `Retry-After`; the attempt heartbeat then asks
the coordinator to stop the disposable worker.

Every mailbox operation retains a worker-side deadline. Provider exchanges use
that deadline as a 30-second host-liveness window rather than an absolute model
turn limit. While a provider operation is pending, the trusted host publishes
monotonic progress frames through the read-only response side of the mailbox;
each fresh frame renews the worker's liveness window. Replayed or frozen frames
do not. A healthy long provider stream can therefore finish, while a dead
gateway causes the worker to release the mailbox lock and fail the request.
Closing a gateway detaches from an in-flight provider handler immediately;
cleanup does not wait for a blocked upstream operation, and the handler cannot
publish into the removed mailbox.

## Replacement and recovery

When the human response is accepted, the interaction becomes `answered`. If
the original attempt is still live, the run remains `waiting_for_input` until
that worker exits; attempt finalization then returns it to `queued`. A
replacement worker receives a fresh mailbox and a clean workspace reconstructed
from the already pinned commit. It calls `run.describe` to discover the prior
handoff and answered interaction, then calls `interaction.read`. The original
worker is not kept alive while a person is thinking, and its mutable filesystem
is not inherited automatically. An explicitly automatic run is launched again
by the durable supervisor; a manual run remains queued for the operator.

A provider-paused run follows the same replacement boundary but remains the
same run rather than creating a successor. The supervisor waits for the active
attempt to close, the retry time to arrive, and every open interaction to be
answered before requeueing. Human answers remain accepted during the pause.

Configured interaction-age thresholds produce a bodyless, idempotent reminder
for the existing Krab item. The worker gateway is not involved, no second
message body is created, and Magic Glass Box retains only threshold and event
metadata. See [supervision.md](supervision.md).

If the orchestrator restarts, reconciliation waits for the attempt owner lease
to expire before removing containers, snapshots, and mailbox files. It captures
recoverable file results and leaves `waiting_for_input` interactions intact.
Preparing or running work without a durable wait is failed explicitly because
its live worker can no longer be proven. An active attempt attached to a
durable wait is closed as interrupted while its handoff remains available.

## Current boundary

The gateway brokers run context, Krab interactions, structured handoffs, named
provider grants, and work-item-scoped coordination. It does not grant
transparent Internet access, relay arbitrary URLs, expose administrator
credentials, accept destinations selected by the worker, export forge
comments, or store transcripts.

Run the carrier and isolation proof with:

```console
PYTHONPATH=src python3.11 scripts/check_live_gateway.py
PYTHONPATH=src python3.11 scripts/check_live_provider_gateway.py
```
