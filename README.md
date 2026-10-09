# Magic Glass Box

Magic Glass Box is a local-first control plane for running headless coding
agents in disposable containers. A single trusted orchestrator owns lifecycle,
policy, project context, credentials, and human interaction. Its sister project,
`imitation-krab`, remains the independent messaging and coordination authority.

This repository currently contains the first executable vertical slice:

- private SQLite control-plane state with immutable schema migrations;
- local users linked to stable Krab user IDs rather than bearer tokens;
- versioned project context and versioned agent profiles;
- run records that pin both versions and enforce a state machine;
- a durable, metadata-only interaction ledger for typed questions, approvals,
  and choices, with atomic run-state transitions and optimistic concurrency;
- a bounded, typed Krab client for `whoami`, project inventory, and project
  activity;
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
removed afterward. `krab_only` deliberately fails closed until Magic Glass Box
has a scoped Krab relay. The relay, credential broker, and human-question
workflow are the next vertical slices.

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
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db run execute run_<id>
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db run reconcile
```

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

See [CONTRIBUTING.md](CONTRIBUTING.md) for the exact scoring rules and workflow.

See [docs/product-thesis.md](docs/product-thesis.md) for the product contract,
[docs/roadmap.md](docs/roadmap.md) for the proof-oriented milestones, and
[docs/architecture.md](docs/architecture.md) for the trust boundaries and
execution flow.
