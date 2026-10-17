# Magic Glass Box

Magic Glass Box is a local-first control plane for running headless coding
agents in disposable containers. A single trusted orchestrator owns lifecycle,
policy, project context, credentials, and human interaction. Its sister project,
`imitation-krab`, remains the independent messaging and coordination authority.

This repository currently contains the first executable vertical slice:

- private SQLite control-plane state with immutable schema migrations;
- local users linked to stable Krab user IDs rather than bearer tokens;
- versioned project context and versioned agent profiles;
- a strict, forge-neutral work-item import contract with atomic create, update,
  unchanged, and stale-revision behavior;
- run records that pin context, profile, and immutable work-item versions while
  enforcing a state machine;
- a durable, metadata-only interaction ledger for typed questions, approvals,
  and choices, with atomic run-state transitions and optimistic concurrency;
- a bounded, typed Krab client for `whoami`, project inventory, project
  activity, and idempotent session-item write/read operations;
- verified human-interaction routing that correlates Krab item IDs while
  keeping private question and response content out of control-plane storage;
- a private-file credential resolver for non-inference authorities such as
  Krab that never copies token values into the database or a worker;
- a Docker execution adapter that resolves images to immutable digests,
  validates workspaces, rejects every credential-file payload, applies resource
  ceilings for memory, CPU, PIDs, temporary storage, and logs, applies privilege
  ceilings, and always attempts container cleanup;
- a runtime-adapter boundary with a compatibility command dialect plus Codex
  JSONL and Claude Code JSON launch/completion contracts, explicit
  runtime-to-grant routing, and a loopback-to-mailbox worker transport, audited
  per attempt;
- a run-scoped capability gateway that gives networkless workers typed access
  to pinned run context and asynchronous Krab interactions without mounting a
  reusable token;
- immutable, revocable, and expiring inference grants that pin one connector
  deployment, route, semantic protocol, model, request/response limits,
  timeout, scope fingerprint, and concurrency without storing or resolving an
  upstream provider credential;
- schema-v17 credential-isolated inference-connector registrations, exclusive
  connector dispatch, strict request/response envelopes, durable pre-dispatch
  intent and ambiguous outcomes, and a bounded Unix-socket carrier that
  isolates bad clients and removes its socket on controlled termination;
- schema-v19 provider-budget authority with normalized content-free usage,
  versioned USD-micro pricing and run/project/grant ceilings, atomic
  pre-dispatch allowance reservations, conservative ambiguity accounting, and
  reconciled control-room totals;
- raw Git-tree snapshots pinned to an immutable SHA-1 or SHA-256 commit, with
  committed blob bytes and executable modes preserved; attributes are inert,
  including CRLF/ident/export-subst conversion, filters, and export-ignore;
  LFS pointers stay pointers and Git-links become empty directories without
  submodule recursion;
- a run coordinator that records preparation, container identity, resolved
  source revision, exit status, and terminal state;
- owner-leased attempts with heartbeats, audited cancellation requests, real
  container stops, versioned and audited expired-owner takeover, stale-owner
  transition fencing, and reconciliation that touches only expired ownership;
- immutable host-captured workspace results that retain bounded file changes
  before disposable snapshots are removed, with explicit conflict-checked
  application and no Git hosting credentials;
- immutable trusted candidates that turn one strictly evidenced successful
  attempt result into a deterministic child commit of its pinned source
  revision inside one shared private Git store per project and object format,
  without touching the operator checkout or inheriting repository hooks and
  filters;
- immutable exact-input gate policies that bind repository/ref/base/head/tree
  identity, candidate-backed independent-review runs with an explicit profile
  and/or identity separation policy, networkless read-only local validation,
  append-only bounded evidence, and derived readiness without transcripts or
  command output;
- hard work-item landing dependencies, immutable deterministic refolds, one
  leased integration lane per ref, exact argv-only manual Git plans, and
  nonce-bound receipts that either confirm the unchanged base or truthfully
  advance an out-of-band observed ref and conservatively invalidate every
  remaining sibling without storing a Git-host credential;
- a durable attempt ledger and bounded structured handoff that let a clean
  replacement worker resume the same run from its pinned commit without a
  transcript or inherited mutable sandbox;
- a work-item-scoped, append-only coordination log with optimistic checkpoints,
  profile-pinned leased assignments, and enough provenance for a replacement
  coordinator to recover parallel Claude/Codex-style work;
- operator-controlled work completion that seals audit history and an explicit
  reopen into a blank next-generation memory thread;
- a transactionally consistent, redacted control-room projection with an
  optional live overlay of configured Krab identities' roles and claim queues,
  where upstream bodies and host-private failure details are reduced to fixed
  operational categories;
- a loopback-only, capability-addressed browser control room with explicit
  cancel, retry, reassign, revoke, and reconcile forms;
- a leader-safe long-lived supervisor with per-run opt-in, finite durable
  retries, renewable action ownership, graceful bounded shutdown, automatic
  answered-work replacement, lease-safe recovery, and nonblocking bodyless
  threshold reminders through Krab; and
- an administrative CLI that can execute, cancel, retry, or reassign runs,
  reconcile interrupted ones, inspect or apply retained results, materialize,
  reconstruct, list, or verify trusted candidates, and manage exact-input gate
  policy, review, validation, evidence, readiness, dependency, refold, queue,
  landing-plan, and receipt state.

The complete networkless path has been exercised against a live Git repository
and Docker engine. A run receives only the files tracked at its resolved commit;
the source checkout remains untouched, bounded edits are retained in a trusted
result bundle, and the container and snapshot are removed afterward. The
interaction transport has also been exercised over HTTP
against a real local `imitation-krab`: idempotent question delivery,
bodyless reminder replay, reverse-route response verification, and
metadata-only local persistence all pass. The same proof verifies the control
room's live role and claim-queue overlay without surfacing private interaction
content. The gateway carrier has
been exercised through the real Docker executor:
the worker has `--network none`, direct egress fails, and a typed request still
reaches the orchestrator. A second live proof reaches one policy-pinned local
provider through a separately launched connector while undeclared grants,
destinations, models, direct networking, and credential exposure all fail
closed. The retired `krab_only` mode continues to fail closed.

## Quick start

The control plane has no Python runtime dependencies beyond Python 3.11 or
newer. Executing agents also requires the Git and Docker CLIs plus a reachable
Docker engine. The local `init`, user, and status commands work without Krab;
project registration, identity verification, interactions, and live metadata
require a reachable `imitation-krab` instance and an appropriate user token.

```console
PYTHONPATH=src python3.11 -m magic_glass_box init
PYTHONPATH=src python3.11 -m magic_glass_box user add operator \
  --display-name "Local Operator"
PYTHONPATH=src python3.11 -m magic_glass_box status
```

These commands use the platform state directory. Passing `--db` is supported,
but the retained-result and candidate stores are created beside that database;
do not point it into a source checkout unless the whole state location is
ignored and treated as disposable local control-plane data.

Start an inference connector with its credentials in its own private state,
then register only its credential-free authority and a connector-bound grant:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  inference-connector register openai-personal \
  --spec examples/inference-connector-v2.json --created-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  provider connector-add openai-main-sidecar \
  --policy examples/inference-grant-budgeted.json --created-by operator
```

MGB does not launch a provider CLI, read the connector's credential store, or
perform the upstream provider request. Normal provider execution is exclusive
to the local sidecar. Protocol v2 carries a pinned reservation and normalized
usage; protocol v1 remains readable and dispatchable for legacy unmetered
connectors but has no MGB-enforced spend ceiling. The transport-neutral client
seam preserves a later authenticated external-integration carrier. Historical
direct grants remain readable and revocable for migration, but cannot be
created through the operator surface or dispatched. The boundary and current
checkpoint are documented in
[docs/inference-connectors.md](docs/inference-connectors.md).

The optional OpenAI connector owns its own Sign in with ChatGPT profile. Keep
that profile outside both the checkout and MGB state:

```console
python3.11 -m pip install '.[openai-connector]'
PYTHONPATH=src python3.11 scripts/openai_chatgpt_connector.py \
  login --profile-dir /absolute/private/openai-chatgpt-profile
PYTHONPATH=src python3.11 scripts/openai_chatgpt_connector.py \
  models --profile-dir /absolute/private/openai-chatgpt-profile
```

The browser callback binds only to `127.0.0.1`. Use `login --no-browser` when
the URL must be opened deliberately on the same host. The connector stores its
registration and renewable OAuth session in the private profile; MGB receives
only the resulting non-secret scope fingerprint and exact model binding. The
`login --callback-timeout SECONDS` option bounds the complete callback wait;
idle clients are dropped independently and a callback with the wrong OAuth
state is ignored. The
explicit, quota-consuming end-to-end proof is documented separately and never
runs from the ordinary test or CI targets.

Once a project, gateway-only profile, and run have been registered, execute or
recover its lifecycle with:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  work-item import examples/work-item.json \
  --project box-project --imported-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  run create --project box-project --profile Worker \
  --work-item wki_<id> --requested-by operator \
  --supervision-policy examples/supervision-policy.json
PYTHONPATH=src python3.11 -m magic_glass_box \
  supervisor run
PYTHONPATH=src python3.11 -m magic_glass_box \
  run execute run_<id> --runtime-adapter codex
PYTHONPATH=src python3.11 -m magic_glass_box \
  run cancel run_<id> --expected-version <version> \
  --requested-by operator --reason "Stop requested"
PYTHONPATH=src python3.11 -m magic_glass_box run reconcile
PYTHONPATH=src python3.11 -m magic_glass_box \
  control-room show --refresh-krab
PYTHONPATH=src python3.11 -m magic_glass_box \
  control-room serve
```

Agent execution has a six-hour wall-clock ceiling by default. Pass
`--run-timeout SECONDS` to choose another positive bound for an individual
attempt. The supervisor additionally bounds the complete coordinator call;
`--action-timeout SECONDS` must exceed the run timeout and otherwise defaults
to the run timeout plus five minutes for setup, capture, and cleanup.

The supervision policy is optional. Omitting it keeps the run manual even when
the supervisor service is running. Opted-in queued work, answered replacement
work, expired ownership, and configured attention thresholds advance from
durable state without an external cron. See
[docs/supervision.md](docs/supervision.md) for policy, privacy, retry, and
leader-safety semantics.

`control-room serve` prints a per-process private URL and binds only to
`127.0.0.1` by default. The browser can refresh live Krab role/claim metadata
and perform lifecycle actions, including binding an explicit Krab response
item to a pending interaction, but it never displays private interaction
bodies, work-log text, handoffs, result paths, or credential references.

The same answer boundary is available from the CLI:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  interaction answer int_<id> --response-item itm_<id> \
  --expected-version <version>
```

The operator selects the response item deliberately. Automatic Krab activity
polling is deferred until the messaging protocol exposes a trusted reply-to
item ID; a same-session message is not strong enough correlation.

After an attempt exits, verify or explicitly apply its retained changes:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  result show att_<id>
PYTHONPATH=src python3.11 -m magic_glass_box \
  result apply att_<id> --repository /absolute/path/to/checkout
```

Application performs a complete conflict preflight, but the target must remain
quiescent while files are changed. Case and Unicode-normalization aliases are
rejected before mutation, while a verified one-for-one case-only rename is
supported. The filesystem update is not one atomic whole-tree transaction; a
failure after mutation begins can therefore require operator recovery. To
create an isolated Git candidate instead of modifying the checkout:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  candidate create att_<id> --created-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  candidate show cnd_<id>
PYTHONPATH=src python3.11 -m magic_glass_box \
  candidate list --project project-key --limit 25
```

Candidate materialization creates a local child commit but does not push, open a
pull request, contact a hosting service, or hold GitHub/GitLab credentials. See
[docs/results.md](docs/results.md) for retention and application and
[docs/candidates.md](docs/candidates.md) for the candidate boundary.

Pin and evaluate gates for an immutable candidate without contacting a forge:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  gate policy-create cnd_<id> --policy examples/gate-policy-request.json \
  --created-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  gate review-create gpl_<id> --profile Reviewer --requested-by operator \
  --expected-policy-revision <sha256>
PYTHONPATH=src python3.11 -m magic_glass_box \
  gate validate gpl_<id> unit --recorded-by operator \
  --expected-policy-revision <sha256>
PYTHONPATH=src python3.11 -m magic_glass_box gate readiness gpl_<id>
```

Review runs receive the exact private candidate tree, not a same-named revision
from the operator checkout. Local validation uses an immutable image, read-only
workspace, no network, gateway, or credential, and retains only bounded hashed
metadata. See [docs/integration-gates.md](docs/integration-gates.md).

Once the exact policy is ready, queue and manually land it without giving MGB
a remote credential:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  integration enqueue gpl_<id> --created-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  integration claim iqm_<id> --owner supervisor-1 \
  --expected-version <version> --lease-seconds 60
PYTHONPATH=src python3.11 -m magic_glass_box \
  integration plan iqm_<id> --owner supervisor-1 \
  --expected-version <claimed-version> --remote origin --issued-by operator
```

The returned commands are argv arrays and are never executed by MGB. After the
operator executes them, `integration receipt` imports the nonce-bound outcome.
Any sibling must be refolded and fully re-gated before another plan can issue.

After all runs and assignments are terminal and the final work-log entry is an
`outcome`, seal or later reopen the local work cycle with:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  work-item complete wki_<id> --expected-thread-version <version> \
  --completed-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  work-item reopen wki_<id> --expected-thread-version <version> \
  --reopened-by operator
```

Work-item import is an idempotent upsert keyed by project, source, and external
ID. Magic Glass Box does not connect to, synchronize with, or mutate a source
forge. See [docs/work-items.md](docs/work-items.md) for the adapter contract and
[`schemas/work-item-v1.schema.json`](schemas/work-item-v1.schema.json) for the
published structural input schema. Runtime semantic validation remains the
acceptance authority; the boundary is documented in
[docs/schema-contracts.md](docs/schema-contracts.md).

The pinned project context must name an absolute path to a local Git repository.
If no source revision was supplied when the run was created, Magic Glass Box
resolves `HEAD`; either way, the full commit ID is persisted before the
container starts. A `gateway_only` worker receives no credential files: names
in its profile authorize specific host-side gateway operations. Profiles with
named authorities must use `gateway_only`; `none` means no named authority and
no reusable secret path. Agent output,
question and answer bodies, and transcripts are not durable control-plane
state; bounded workspace file changes are retained separately as inert result
artifacts. See [docs/agent-gateway.md](docs/agent-gateway.md) for the capability
and carrier contracts and
[docs/inference-connectors.md](docs/inference-connectors.md) for the implemented
connector authority and exclusive dispatch. Connector implementers can consult
[docs/provider-broker.md](docs/provider-broker.md) for the optional reference
protocol-adapter library. The replacement
contract and handoff schema are documented in
[docs/worker-handoffs.md](docs/worker-handoffs.md). Multi-agent coordination and
replacement-coordinator recovery are documented in
[docs/work-memory.md](docs/work-memory.md). Runtime launch and completion
contracts are documented in
[docs/runtime-adapters.md](docs/runtime-adapters.md). The redacted inventory
and live role/claim overlay are documented in
[docs/control-room.md](docs/control-room.md).

When running directly from a checkout, install the development environment;
the Makefile then uses `.venv/bin/python` by default while preserving explicit
`PYTHON=` overrides.

```console
python3.11 -m venv .venv
.venv/bin/python -m pip install -e '.[development]'
make test
```

An installed package exposes the `magic-glass-box` control-plane command and
the `magic-glass-box-worker` entry point used inside compatible vendor images.
New default state
is stored under `magic-glass-box/control-plane.db` in the platform state
directory. If that database does not exist but a database from the retired
working codename is present at `hexadecapus/hex.db`, it is reused in place.
The old `HEX_DB` and `HEX_STATE_DIR` overrides and Docker labels are recognized
only for migration and interrupted-run cleanup; new configuration uses the
`MAGIC_GLASS_BOX_` prefix and `io.magicglassbox` labels.

Development uses RED-first tests, an 85% branch-coverage floor, and an 80%
mutation kill-rate floor. The repeatable gates are:

```console
make quality
make mutation-clean
make candidate-test
make gate-test
make integration-test
make live-supervisor
```

With the sister repository checked out beside this one, the repeatable live
transport proof is:

```console
PYTHONPATH=src python3.11 scripts/check_live_krab_transport.py
```

Use `--krab-source /path/to/imitation-krab/src` for another checkout layout.

The network-isolation and gateway-carrier proof uses a local Python image and
the real Docker executor:

```console
PYTHONPATH=src python3.11 scripts/check_live_gateway.py
PYTHONPATH=src python3.11 scripts/check_live_provider_gateway.py
```

Use `--image` to select another local, volume-free image with Python installed.

The live runtime proof builds an intentionally narrow image with pinned Codex
and Claude Code CLIs, then runs three real networkless containers through the
Docker, provider-gateway, and separate inference-connector path against a
host-only deterministic protocol fixture. Codex opens a durable question,
submits a structured handoff, exits, and is replaced after an answer; the
replacement reads the durable context and finishes. The same proof verifies
that a generated ignored dependency tree is excluded while the worker's
retained file is captured, and Claude Code proves the second runtime seam:

```console
PYTHONPATH=src python3.11 scripts/check_live_runtime_adapters.py --build
```

It makes no remote provider request and needs no provider credential. See
[docs/runtime-adapters.md](docs/runtime-adapters.md) for the exact proof
boundary.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the exact scoring rules and workflow.

See [docs/product-thesis.md](docs/product-thesis.md) for the product contract,
[docs/roadmap.md](docs/roadmap.md) for the proof-oriented milestones, and
[docs/architecture.md](docs/architecture.md) for the trust boundaries and
execution flow.
