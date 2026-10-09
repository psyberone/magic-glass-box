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
- tracked-files-only Git snapshots that resolve each run to an immutable commit;
- a run coordinator that records preparation, container identity, resolved
  source revision, exit status, and terminal state; and
- an administrative CLI that can execute queued runs and reconcile interrupted
  ones.

The complete networkless path has been exercised against a live Git repository
and Docker engine. A run receives only the files tracked at its resolved commit;
the source checkout remains untouched and the container and snapshot are
removed afterward. The interaction transport has also been exercised over HTTP
against a real local `imitation-krab`: idempotent question delivery,
reverse-route response verification, and metadata-only local persistence all
pass. The run-scoped agent gateway is next. `krab_only` continues to fail closed
until its scoped relay exists.

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

Once a project, networkless profile, and run have been registered, execute or
recover its lifecycle with:

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  work-item import examples/work-item.json \
  --project box-project --imported-by operator
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  run create --project box-project --profile Worker \
  --work-item wki_<id> --requested-by operator
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db run execute run_<id>
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db run reconcile
```

Work-item import is an idempotent upsert keyed by project, source, and external
ID. Magic Glass Box does not connect to, synchronize with, or mutate a source
forge. See [docs/work-items.md](docs/work-items.md) for the adapter contract and
[`schemas/work-item-v1.schema.json`](schemas/work-item-v1.schema.json) for the
published input schema.

The pinned project context must name an absolute path to a local Git repository.
If no source revision was supplied when the run was created, Magic Glass Box
resolves `HEAD`; either way, the full commit ID is persisted before the
container starts. Only credentials named by the pinned profile are mounted.
Agent output, question and answer bodies, and transcripts are not persisted by
this slice.

When running directly from a checkout, set `PYTHONPATH=src` or install the
package into a virtual environment.

```console
PYTHONPATH=src python3.11 -m unittest discover -s tests -v
```

An installed package exposes the `magic-glass-box` command. New default state
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

See [CONTRIBUTING.md](CONTRIBUTING.md) for the exact scoring rules and workflow.

See [docs/product-thesis.md](docs/product-thesis.md) for the product contract,
[docs/roadmap.md](docs/roadmap.md) for the proof-oriented milestones, and
[docs/architecture.md](docs/architecture.md) for the trust boundaries and
execution flow.
