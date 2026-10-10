# Control-room projection

The control room begins with a machine-readable operational projection rather
than a second source of truth. It combines one consistent local SQLite snapshot
with an optional, explicitly non-atomic live view from Imitation Krab.

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  control-room show
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  control-room show --history-limit 100 --refresh-krab
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
- profiles and current execution-policy metadata;
- provider grants with lifecycle state and redacted policy metadata;
- current work-item generations and visible, explicitly untrusted titles;
- local profile-pinned assignments and their visible, untrusted titles;
- all active runs plus a bounded number of recent terminal runs;
- attempts, owners, leases, cancellation signals, interactions, retained
  result summaries, and run-bound provider authorities; and
- an attention queue for pending interactions, open or expired assignments,
  blocker evidence, expired attempt leases, cancellation requests, inconsistent
  active runs, and unavailable authorities.

`--history-limit` accepts 0 through 500 and limits only terminal runs. It never
hides an active run. The same bound selects the attempts, interactions,
results, and authority bindings attached to the visible run set. Aggregate
counts still describe all local state and the snapshot reports whether terminal
history was truncated.

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
- agent command arguments; and
- provider credential prefixes and static-header values.

Provider destinations are displayed without query strings. Static-header names
remain visible because they are policy shape; their values do not. Work-item
and assignment titles are labeled `untrusted_title` and must be rendered as
text, never interpreted as instructions or markup.

The local timestamp is the consistency point for the SQLite projection. A live
Krab refresh has its own `refreshed_at` timestamp because no transaction can
span the two independent projects.

## Current boundary

This first control-room slice is read-only. Existing explicit CLI operations
remain the action surface for cancellation, provider revocation, retained-result
application, and reconciliation. The next control-room slice can build a local
browser and explicit retry/reassignment workflows on this projection without
teaching every screen to query private tables or Krab independently.
