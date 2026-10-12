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
- a private-file credential resolver that never copies token values into the
  database;
- a Docker execution adapter that resolves images to immutable digests,
  validates workspaces and credential files, applies resource and privilege
  ceilings, and always attempts container cleanup;
- a runtime-adapter boundary with a compatibility command dialect plus Codex
  JSONL and Claude Code JSON launch/completion contracts, explicit
  runtime-to-grant routing, and a loopback-to-mailbox worker transport, audited
  per attempt;
- a run-scoped capability gateway that gives networkless workers typed access
  to pinned run context and asynchronous Krab interactions without mounting a
  reusable token;
- immutable, revocable, and expiring provider grants that pin destination,
  method, model, request/response limits, timeout, and concurrency while
  resolving provider secrets only inside the trusted orchestrator;
- tracked-files-only Git snapshots that resolve each run to an immutable commit;
- a run coordinator that records preparation, container identity, resolved
  source revision, exit status, and terminal state;
- owner-leased attempts with heartbeats, audited cancellation requests, real
  container stops, and reconciliation that touches only expired ownership;
- immutable host-captured workspace results that retain bounded file changes
  before disposable snapshots are removed, with explicit conflict-checked
  application and no Git hosting credentials;
- a durable attempt ledger and bounded structured handoff that let a clean
  replacement worker resume the same run from its pinned commit without a
  transcript or inherited mutable sandbox;
- a work-item-scoped, append-only coordination log with optimistic checkpoints,
  profile-pinned leased assignments, and enough provenance for a replacement
  coordinator to recover parallel Claude/Codex-style work;
- operator-controlled work completion that seals audit history and an explicit
  reopen into a blank next-generation memory thread;
- a transactionally consistent, redacted control-room projection with an
  optional live overlay of configured Krab identities' roles and claim queues;
- a loopback-only, capability-addressed browser control room with explicit
  cancel, retry, reassign, revoke, and reconcile forms; and
- an administrative CLI that can execute, cancel, retry, or reassign runs,
  reconcile interrupted ones, and inspect or apply retained results.

The complete networkless path has been exercised against a live Git repository
and Docker engine. A run receives only the files tracked at its resolved commit;
the source checkout remains untouched, bounded edits are retained in a trusted
result bundle, and the container and snapshot are removed afterward. The
interaction transport has also been exercised over HTTP
against a real local `imitation-krab`: idempotent question delivery,
reverse-route response verification, and metadata-only local persistence all
pass. The same proof verifies the control room's live role and claim-queue
overlay without surfacing private interaction content. The gateway carrier has
been exercised through the real Docker executor:
the worker has `--network none`, direct egress fails, and a typed request still
reaches the orchestrator. A second live proof reaches one policy-pinned local
provider while undeclared grants, destinations, models, direct networking, and
credential exposure all fail closed. The retired `krab_only` mode continues to
fail closed.

## Quick start

The control plane has no Python runtime dependencies beyond Python 3.11 or
newer. Executing agents also requires the Git and Docker CLIs plus a reachable
Docker engine.

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db init
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db user add operator \
  --display-name "Local Operator"
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db status
```

Register an externally stored provider credential and immutable policy before
creating a profile that names the grant:

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  provider add openai-main --policy examples/provider-policy.json \
  --credential-file ./private-credentials/openai.key --created-by operator
```

Once a project, gateway-only profile, and run have been registered, execute or
recover its lifecycle with:

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  work-item import examples/work-item.json \
  --project box-project --imported-by operator
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  run create --project box-project --profile Worker \
  --work-item wki_<id> --requested-by operator
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  run execute run_<id> --runtime-adapter codex
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  run cancel run_<id> --expected-version <version> \
  --requested-by operator --reason "Stop requested"
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db run reconcile
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  control-room show --refresh-krab
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  control-room serve
```

Agent execution has a six-hour wall-clock ceiling by default. Pass
`--run-timeout SECONDS` to choose another positive bound for an individual
attempt.

`control-room serve` prints a per-process private URL and binds only to
`127.0.0.1` by default. The browser can refresh live Krab role/claim metadata
and perform lifecycle actions, but it never displays private interaction
bodies, work-log text, handoffs, result paths, or credential references.

After an attempt exits, verify or explicitly apply its retained changes:

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  result show att_<id>
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  result apply att_<id> --repository /absolute/path/to/checkout
```

Application performs a complete conflict preflight. Magic Glass Box does not
commit, push, open a pull request, or hold GitHub/GitLab credentials. See
[docs/results.md](docs/results.md) for the retention and application contract.

After all runs and assignments are terminal and the final work-log entry is an
`outcome`, seal or later reopen the local work cycle with:

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  work-item complete wki_<id> --expected-thread-version <version> \
  --completed-by operator
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  work-item reopen wki_<id> --expected-thread-version <version> \
  --reopened-by operator
```

Work-item import is an idempotent upsert keyed by project, source, and external
ID. Magic Glass Box does not connect to, synchronize with, or mutate a source
forge. See [docs/work-items.md](docs/work-items.md) for the adapter contract and
[`schemas/work-item-v1.schema.json`](schemas/work-item-v1.schema.json) for the
published input schema.

The pinned project context must name an absolute path to a local Git repository.
If no source revision was supplied when the run was created, Magic Glass Box
resolves `HEAD`; either way, the full commit ID is persisted before the
container starts. A `gateway_only` worker receives no credential files: names
in its profile authorize specific host-side gateway operations. Agent output,
question and answer bodies, and transcripts are not durable control-plane
state; bounded workspace file changes are retained separately as inert result
artifacts. See [docs/agent-gateway.md](docs/agent-gateway.md) for the capability
and carrier contracts and [docs/provider-broker.md](docs/provider-broker.md)
for provider policy and secret-handling details. The replacement contract and
handoff schema are documented in
[docs/worker-handoffs.md](docs/worker-handoffs.md). Multi-agent coordination and
replacement-coordinator recovery are documented in
[docs/work-memory.md](docs/work-memory.md). Runtime launch and completion
contracts are documented in
[docs/runtime-adapters.md](docs/runtime-adapters.md). The redacted inventory
and live role/claim overlay are documented in
[docs/control-room.md](docs/control-room.md).

When running directly from a checkout, set `PYTHONPATH=src` or install the
package into a virtual environment.

```console
PYTHONPATH=src python3.11 -m unittest discover -s tests -v
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
make mutation
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
Docker and provider-gateway path against a host-only deterministic protocol
fixture. Codex opens a durable question, submits a structured handoff, exits,
and is replaced after an answer; the replacement reads the durable context and
finishes. The same proof verifies that a generated ignored dependency tree is
excluded while the worker's retained file is captured, and Claude Code proves
the second runtime seam:

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
