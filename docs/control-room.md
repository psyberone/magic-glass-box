# Control-room projection

The control room begins with a machine-readable operational projection rather
than a second source of truth. It combines one consistent local SQLite snapshot
with an optional, explicitly non-atomic live view from Imitation Krab.

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  control-room show
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  control-room show --history-limit 100 --refresh-krab
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  control-room serve --host 127.0.0.1 --port 8766
```

The default view performs no network or credential access. `--refresh-krab`
resolves each configured Krab identity on the trusted host, verifies its stable
user ID, reads that identity's current role on registered projects, and reads
the issue and pull-request claim queues through one visible project member.
Failure of one identity or queue degrades only that part of the projection;
local state remains available and the error is reported without a credential
reference or secret.

## Snapshot contract

All local collections are read inside one SQLite read transaction. The output
contains:

- local users and their configured Krab identity IDs;
- Krab instances and redacted identity metadata;
- projects and current context-version metadata;
- profiles, runtime adapters, explicit provider routes, and current
  execution-policy metadata;
- connector-bound provider grants with lifecycle state, grant kind, redacted
  policy metadata, and redacted connector identity/state; historical direct
  grants remain visible but unavailable for dispatch. Each run authority has a
  stable `unavailable_since` timestamp derived from the first grant,
  expiration, connector, or unsupported-authority condition;
- protocol-v2 provider-budget authorities with policy account, revision and
  digest, normalized token totals, reported estimates, finalized accounting,
  active reservation amount, policy consumption, scope ceilings, and remaining
  run/grant/project allowance. No prompt, response, or transcript content is
  part of this projection;
- provider-recovery records with category, source invocation and attempt IDs,
  bounded delay, eligibility timestamp, and active/resumed/cancelled state;
- current work-item generations and visible, explicitly untrusted titles;
- local profile-pinned assignments and their visible, untrusted titles;
- all active runs plus a bounded number of recent terminal runs;
- attempts, owners, leases, cancellation signals, interactions, retained
  result summaries, run-bound provider authorities, pinned supervision
  policies, candidate-backed review bindings, redacted exact-input gate
  readiness, hard landing dependencies, serialized integration queue state,
  folded policy identity, pending manual-plan or provider-landing IDs, pinned
  provider service-operation state, durable supervisor actions, and threshold
  escalation records; and
- an attention queue for pending interactions, open or expired assignments,
  blocker evidence, expired attempt leases, cancellation requests, inconsistent
  active runs, paused provider recovery, unavailable authorities, required
  refolds, pending manual receipts or provider landings, expired integration
  claims, and exhausted supervisor work. Every
  current attention entry includes its age in seconds at the snapshot's
  consistency timestamp.

`--history-limit` accepts 0 through 500 and limits only terminal runs. It never
hides an active run. The same bound selects the attempts, interactions,
results, candidates, gate policies, review-run bindings, and authority bindings
attached to the visible run set. Aggregate
counts—including action and escalation lifecycle states—still describe all
local state and the snapshot reports whether terminal history was truncated.

The optional Krab overlay contains only the roles of identities already
configured in Magic Glass Box. It is not a duplicate project-membership
database and does not require a Krab administrative credential. Work claims
contain trusted claim metadata from Krab; when a claim is correlated to an
imported local work item, the projection adds that item's untrusted title.
Krab remains authoritative for roles and claims.

## Redaction boundary

The projection intentionally excludes:

- Krab and provider credential references or values;
- work-item briefs, criteria, constraints, and context;
- assignment objectives and acceptance-check text;
- work-log and checkpoint text;
- interaction titles, questions, answers, and message history;
- handoff summaries and agent transcripts;
- retained result manifests, file paths, and file content;
- agent command arguments;
- landing-plan argv, private candidate-repository paths, remotes, and raw
  receipt nonces;
- transient runtime stdout and vendor result envelopes;
- supervisor exception messages or upstream response bodies; and
- provider credential prefixes and static-header values.

Provider destinations are displayed without query strings. Static-header names
remain visible because they are policy shape; their values do not. Work-item
and assignment titles are labeled `untrusted_title` and must be rendered as
text, never interpreted as instructions or markup.

The local timestamp is the consistency point for the SQLite projection. A live
Krab refresh has its own `refreshed_at` timestamp because no transaction can
span the two independent projects.

Live Krab failures are reduced to fixed operational categories at this
boundary. The projection may identify the failed identity or claim queue and
the local exception class, but it never copies an upstream error body, socket
path, operating-system diagnostic, or arbitrary exception message into JSON or
HTML. A failed identity or queue degrades only that overlay; the transactional
local snapshot remains available.

## Interactive browser

`control-room serve` prints a fresh private URL and serves the same projection
as a server-rendered local browser. It accepts only a numeric loopback address;
binding to a LAN or wildcard address fails. The random URL capability exists
only for that process. State-changing forms additionally require a distinct
CSRF capability and an exact same-origin request. Responses disable caching,
framing, referrers, and every content source except the nonce-bearing local
stylesheet. The page has no JavaScript and loads no external assets.

The Host header, path capability, form content type, body size, exact field
set, duplicate fields, CSRF token, and optimistic object version are all
checked before an action reaches the control plane. At most 32 accepted sockets
have handler authority at once. Each has both a 15-second socket-idle timeout
and a 15-second absolute lifetime, so slow-dripped headers or bodies cannot
retain daemonized handler capacity or delay server shutdown indefinitely.
Visible titles and every other rendered value are HTML-escaped. Errors returned
to the browser do not include credential, filesystem, upstream, or cleanup
details.

The process is a local administrative surface, not a multi-user authentication
server. Anyone who can read the printed private URL and reach the loopback
socket has operator access for the life of that process. Do not proxy or share
the URL.

## Explicit actions

| Action | Contract |
| --- | --- |
| Answer | Binds one operator-selected Krab response item to an open interaction under optimistic concurrency. The bound local user must still be active. The item is re-fetched and verified as a version-one `open.pending` item created after the question in the pinned project/session by the human sender for the agent recipient; its title and body are neither rendered nor retained. |
| Cancel | Records an optimistic cancellation request. A queued or waiting run becomes terminal immediately without constructing a Docker runtime; an owned attempt lazily opens the coordinator and stops through its heartbeat path. |
| Retry | Creates one queued successor to a terminal run with the same immutable context, profile, work-item version, work thread, source revision, Krab route, and exact provider grants. Revoked/expired grants, revoked/missing connectors, and historical direct authority all fail closed. |
| Reassign | Creates a queued successor to a terminal run with the same work identity and source but an explicitly selected profile's latest revision and current provider grants. |
| Revoke | Revokes an active provider grant under optimistic concurrency; already-pinned runs observe the live kill switch. |
| Reconcile | Recovers only expired or orphaned execution ownership using the existing lease-aware coordinator path. |

Retry and reassignment never reopen or edit the predecessor. The predecessor's
event stream records the successor ID and acting local user, and the successor
stores immutable lineage. Only one direct successor may be active at once.
Both operations require an active project and work-memory generation.

A reassigned run inherits the predecessor's Krab session and claim IDs so its
communication remains correlated, but Magic Glass Box does not change Krab's
authoritative session membership or claim assignee. Those must already permit
the selected profile, or interaction delivery will fail closed. This keeps the
control-plane action from becoming an implicit Krab administration write.

Retained-result application stays in the CLI because it requires an explicit
absolute checkout path and performs host filesystem writes. The browser shows
only result IDs, counts, byte totals, and digests.

The browser shows supervision state but does not start or stop the supervisor
process. Run the explicit local service command described in
[supervision.md](supervision.md). A run labeled `manual` will not launch merely
because the service is present; `automatic` means its immutable run policy
opted in.

Equivalent answer and successor commands are available without the browser:

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  interaction answer int_<id> --response-item itm_<id> \
  --expected-version <version>
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  run retry run_<id> --expected-version <version> --requested-by operator
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  run reassign run_<id> --profile Reviewer --expected-version <version> \
  --requested-by operator
```

The response item is an explicit operator choice. Magic Glass Box does not
currently infer an answer from arbitrary session activity: Imitation Krab does
not yet expose a trusted reply-to relationship, so automatic selection could
mistake an unrelated human-to-agent message for the answer.
