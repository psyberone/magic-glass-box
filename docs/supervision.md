# Autonomous supervision

Magic Glass Box can advance explicitly opted-in work from one long-lived local
process. The supervisor is a dispatcher over durable state, not a second source
of authority: run versions, attempt ownership leases, and SQLite transactions
still decide whether an operation may happen.

Start it with:

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  supervisor run --poll-interval 2 --max-workers 4
```

The process initializes or migrates the database, prints its bounded service
configuration, and then performs reconciliation, escalation, and dispatch
passes until stopped. `SIGTERM` and `Ctrl-C` both enter the same graceful
shutdown path. That path stops discovery, waits up to 30 seconds for in-flight
dispatcher work, and then leaves any remaining claims for lease-based recovery.
`--run-timeout` controls the agent wall-clock limit; `--action-lease-seconds`
controls only claims on durable dispatcher work. `--action-timeout` bounds the
entire coordinator call and must exceed the agent limit. When omitted, it is
the run timeout plus five minutes for setup, result capture, and cleanup.
`--log-level` selects stderr diagnostic verbosity and defaults to `WARNING`.

Transient discovery failures use exponential backoff capped at 60 seconds.
The supervisor does not shorten that delay into another discovery pass merely
to maintain ownership; it renews live action and escalation claims separately
at the lease-heartbeat cadence. Operational diagnostics contain generated
object IDs and sanitized exception class names, never exception messages,
private interaction content, provider output, or credentials.

## Explicit run policy

Automatic launch is never implied by importing work or creating a run. A run
opts in by pinning a strict v1 policy at creation:

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  run create --project box-project --profile Worker \
  --work-item wki_<id> --requested-by operator \
  --supervision-policy examples/supervision-policy.json
```

Omitting `--supervision-policy` pins the safe manual default: automatic
execution is disabled and all escalation schedules are empty. Retry and
reassignment successors inherit their predecessor's exact policy.

The published contract is
[`schemas/supervision-policy-v1.schema.json`](../schemas/supervision-policy-v1.schema.json).
The portable schema checks each schedule's shape and entries; the runtime
semantic validator additionally requires strict increasing order. See
[schema-contracts.md](schema-contracts.md). Every schedule contains at most
eight entries and is bounded to 30 days. Its fields are:

| Field | Meaning |
| --- | --- |
| `auto_execute` | Permit the supervisor to claim a queued run and invoke its pinned runtime. |
| `retry_backoff_seconds` | Finite delays after dispatcher or reminder failure. One initial attempt plus these delays is allowed. |
| `interaction_escalation_seconds` | Ages at which an open interaction receives an idempotent Krab reminder. |
| `attempt_idle_escalation_seconds` | Ages since the latest owner heartbeat that become control-room escalation records. |
| `attempt_lease_expired_escalation_seconds` | Ages since lease expiry that become control-room escalation records. |
| `other_attention_escalation_seconds` | Ages for orphaned active runs, waiting runs without an interaction, and unavailable provider authority. Authority age begins at its first real revocation, expiry, missing/revoked connector, or unsupported historical-direct condition. |

A threshold of zero means “on the first pass at or after the condition begins.”
Schedules are evaluated against the database's UTC clock. Policy is immutable
once the run exists.

`auto_execute` governs new worker launches. Lease-aware reconciliation remains
active for already-started work even under the manual policy: leaving a dead
owner's run permanently stuck is not a safe manual mode. Reconciliation still
cannot take an attempt before its existing ownership lease expires.

## Durable dispatcher ledger

Discovery first inserts a version-bound `supervisor_action`; execution never
runs directly from a query result. The unique action identity is its operation,
subject ID, and subject version. A process then atomically claims that row under
a short lease before calling the coordinator.

This gives two independent safety layers:

1. competing supervisors cannot own the same durable action concurrently; and
2. the coordinator must still atomically claim the queued run or expired
   attempt under the existing lifecycle rules.

While an action is running, its dispatcher renews the claim without changing
the claim's optimistic-lock version. Polling and shutdown waits are capped by
the renewal cadence, so a healthy owner does not silently lose a long-running
claim. If a coordinator call exceeds the action timeout, the supervisor stops
renewing that claim, stops discovery, performs its bounded drain, and exits
nonzero so a service manager can replace it. Python worker threads are not
force-killed in-process. If the process dies, an action deadline is exceeded,
or a bounded shutdown expires, a replacement may reclaim the action after
lease expiry. The underlying run or attempt transition remains the final
defense against duplicate worker launch or premature recovery. An obsolete
action settles without repeating the effect. Operational failures retain only
a safe exception class, wait for the pinned finite backoff, and become
`exhausted` after the final attempt. There is no unbounded retry loop.

Answered interactions queue their run only when the previous attempt is gone.
An opted-in queued run is then discovered by the same normal action path; no
special in-memory resume signal is required.

Active `implementation-v1` workflows use the same durable action ledger. One
`workflow.advance` action is unique to the execution version that represents a
logical build, review, maintainer, landing, or completion stage. The director
may perform several
idempotent micro-steps while that action remains pending—for example creating a
producer, materializing its candidate, binding a policy, and creating the exact
reviewer. Waiting for a worker or maintainer defers the claim without consuming
its finite retry budget. A stage transition increments the execution version,
settles the old action, and makes exactly one next-stage action discoverable.

Failed producer/reviewer runs, missing typed verdicts, failed validations,
provider landing failures, landing-authority mismatches, cancellations, and an
exhausted correction budget open a durable workflow ruling. An open ruling
stops action discovery for that execution and appears in the control-room
attention projection. `workflow ruling-show` and `workflow ruling-resolve`
provide the explicit operator boundary. A correction-limit ruling may authorize
one additional bounded round. A validation-failed ruling may rerun that exact
immutable validation once, and an exact failed-provider-landing ruling may
authorize one retry of that attempt. Each retry requires its own ruling; neither
path can alter the pinned command, image, policy, or candidate. Other rulings
can only cancel.

Waiting at landing is not an operational failure and does not spend retry
budget. Manual workflows wait for an explicitly issued plan and receipt;
provider workflows wait for their explicit target and active capability;
dependency or ref drift waits for the existing queue's dependency/refold
actions. A landed receipt advances through a separate `completion` version so
a replacement supervisor can finish the atomic closeout after process death.

Active `discovery-v1` executions follow the same restart-safe shape through a
version-bound `discovery.advance` action. The Discovery director creates at
most the exact pinned investigator or coordinator run for the current stage,
then defers without spending retry budget while that run is active. A terminal
investigator must have produced its exact typed trace; a terminal coordinator
must have produced a final `GO` or `NO_GO` ruling. Those evidence IDs and
digests are retained in the stage-transition event before the next version is
discoverable. Failed runs and successful runs missing required evidence fail
the prescription explicitly. `ASK_HUMAN` remains on the ordinary durable
interaction path, so a replacement supervisor can continue after the answer
without an in-memory wake-up.

## Provider recovery

An explicit provider quota, rate-limit, or overload refusal creates one active
provider-recovery record and moves the same run to `paused`. The provider
invocation outcome, bounded retry delay, eligibility timestamp, category,
attempt, and recovery identity are committed atomically; prompt and response
content are not retained. Ambiguous calls and ordinary upstream failures do
not enter this path.

The attempt heartbeat reports the pause to its current owner, which stops the
container through the normal run-labeled cleanup path. A clean or failed exit
while the run is paused closes the disposable attempt as `interrupted` and
leaves the run paused. If the owner dies instead, ordinary lease-expiry
reconciliation performs the same cleanup; eligibility never authorizes the
supervisor to overlap a live attempt.

Every discovery pass also checks eligible recovery records. A run can move
from `paused` to `queued` only after its active attempt is gone and it has no
open interaction. Human answers remain accepted while provider recovery is
active, so an unanswered question cannot be skipped and an answer cannot be
lost. The recovery transition is optimistic, audited, and idempotent. A manual
policy stops at `queued`; only an immutable policy with `auto_execute` creates
the normal `run.execute` action. Cancellation settles the recovery as
`cancelled` and prevents a later wake-up.

The supervisor advances an interaction only after durable state says it is
answered. It sends age-based reminders but does not scan Krab activity and
guess which message is a response. Until the transport exposes trusted
reply-to correlation, an operator explicitly binds the response item through
the CLI or loopback control room.

## Escalation and privacy

Each escalation is unique by condition, subject, and configured threshold.
Local conditions become delivered control-room records immediately. Open
interactions use Imitation Krab's item-reminder endpoint with an empty JSON
object and a durable idempotency key. Before delivery, Magic Glass Box verifies
that the interaction is still open; an answer that wins the race causes the
reminder to be dismissed. Reminder delivery runs in a separate bounded worker
pool, carries an explicit HTTP timeout shorter than its renewable claim lease,
and therefore cannot stall the supervisor's action-discovery loop.

The escalation ledger contains IDs, kinds, thresholds, state, retry metadata,
and optional Krab event sequence only. It contains no question, answer, prompt,
work-log entry, response body, or transcript. A reminder identifies the
existing private item; it does not create or copy another message.

The CLI and browser control room expose:

- every visible run's pinned supervision policy;
- active, resumed, and cancelled provider recoveries with bounded eligibility
  metadata;
- pending, claimed, succeeded, and exhausted dispatcher actions;
- escalation threshold, channel, and delivery state;
- safe error classes for exhausted work; and
- `age_seconds` for current attention conditions.

## Proof boundary

Run the repeatable supervisor proof with:

```console
make live-supervisor
```

It uses the real database and schema-v10 supervision tables, action and
escalation ledgers, supervisor threads, lifecycle transitions, attempt leases,
and two competing supervisor identities. A deterministic actuator stands at
the already-tested coordinator interface, so this proof needs neither Docker
nor a model provider. The separate runtime-adapter proof exercises the real
coordinator and Docker path.

The proof demonstrates an explicitly automatic queued run, an answered run, a
manual queued run, one expired attempt, one still-live attempt, and a stalled
interaction with two thresholds. Every expected operation occurs once and all
durable evidence is checked after both supervisors finish.

`scripts/check_live_krab_transport.py` separately sends and idempotently
replays the empty-body reminder against a real local Imitation Krab HTTP API,
then verifies that private request and response content never enters Magic
Glass Box state.
