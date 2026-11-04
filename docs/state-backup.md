# Control-plane state backup and restore

Magic Glass Box can publish one coherent private backup of its authoritative
control-plane state while the SQLite database is live, including when WAL mode
has committed pages that are not present in the main database file. It uses
SQLite's online backup API, copies the retained result bundles named by that
snapshot, verifies the complete set, and atomically renames the private staging
directory into place only after verification succeeds.

Create a backup outside the live state directory:

```console
magic-glass-box --db /private/mgb/control-plane.db \
  backup create /private/backups/mgb-2026-11-03
magic-glass-box backup verify /private/backups/mgb-2026-11-03
```

The published directory contains exactly:

- `control-plane.db`, a self-contained SQLite snapshot with no required WAL;
- `results/`, the authoritative retained result bundles referenced by that
  database snapshot; and
- `manifest.json`, a canonical schema-v1 inventory containing the creation
  time, MGB version and optional build revision, database schema/version/hash,
  and aggregate retained-result identity.

The directory and its child directories use mode `0700`; regular files use
mode `0600`. The destination must not exist and must be outside the live state
root. An interrupted create leaves no published partial backup and never
replaces a prior backup.

## Restore contract

Verification is read-only. Restore first verifies the source, copies it into a
private staging directory, verifies the restored database and result set again,
and publishes only to a new path:

```console
magic-glass-box backup restore /private/backups/mgb-2026-11-03 \
  /private/mgb-restored
magic-glass-box --db /private/mgb-restored/control-plane.db status
```

Restore deliberately has no in-place mode and does not switch a running
orchestrator to the new directory. Stop the old process, inspect the restored
state with `status`, and then start the replacement process with the restored
database path. Existing restore targets are never overwritten.

The candidate Git store is a reconstructable cache and is excluded. Normal
candidate access can reconstruct its objects from the restored database and
verified result bundles. Workspaces and run gateways are ephemeral and are
also excluded. Their absence after restore is intentional.

Credentials remain operator-managed outside MGB state. The database may retain
credential *references*, but no referenced token, provider profile, SSH key,
or sidecar-owned secret is copied into the backup. Back up and restore those
private stores separately under the provider's own procedure, then preserve or
deliberately revise their referenced paths before resuming affected work.

This is a disaster-recovery snapshot, not an encrypted archive, incremental
backup system, retention policy, or live failover mechanism. The operator owns
encryption at rest, off-machine replication, rotation, and restore drills. MGB
owns coherence and verification at its boundary: a missing result, changed
digest, unexpected entry, non-private mode, database integrity error, foreign
key violation, or unsupported schema causes verification and restore to fail
closed.
