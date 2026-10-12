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
Every schedule is strictly increasing, contains at most eight entries, and is
bounded to 30 days. Its fields are:

| Field | Meaning |
| --- | --- |
| `auto_execute` | Permit the supervisor to claim a queued run and invoke its pinned runtime. |
| `retry_backoff_seconds` | Finite delays after dispatcher or reminder failure. One initial attempt plus these delays is allowed. |
| `interaction_escalation_seconds` | Ages at which an open interaction receives an idempotent Krab reminder. |
| `attempt_idle_escalation_seconds` | Ages since the latest owner heartbeat that become control-room escalation records. |
| `attempt_lease_expired_escalation_seconds` | Ages since lease expiry that become control-room escalation records. |
| `other_attention_escalation_seconds` | Ages for orphaned active runs, waiting runs without an interaction, and unavailable provider authority. |

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
- pending, claimed, succeeded, and exhausted dispatcher actions;
- escalation threshold, channel, and delivery state;
- safe error classes for exhausted work; and
- `age_seconds` for current attention conditions.

## Proof boundary

Run the repeatable supervisor proof with:

```console
make live-supervisor
```

It uses the real schema-v10 database, action and escalation ledgers, supervisor
threads, lifecycle transitions, attempt leases, and two competing supervisor
identities. A deterministic actuator stands at the already-tested coordinator
interface, so this proof needs neither Docker nor a model provider. The
separate runtime-adapter proof exercises the real coordinator and Docker path.

The proof demonstrates an explicitly automatic queued run, an answered run, a
manual queued run, one expired attempt, one still-live attempt, and a stalled
interaction with two thresholds. Every expected operation occurs once and all
durable evidence is checked after both supervisors finish.

`scripts/check_live_krab_transport.py` separately sends and idempotently
replays the empty-body reminder against a real local Imitation Krab HTTP API,
then verifies that private request and response content never enters Magic
Glass Box state.
