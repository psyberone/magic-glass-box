# Retained workspace results

A worker's container and writable snapshot are disposable; its bounded file
changes are not. Magic Glass Box captures those changes on the trusted host
before deleting the snapshot and binds their digest to the durable attempt.
This is a local handoff boundary, not a forge integration.

## Result shape

Before the worker starts, the orchestrator records a baseline of the pristine
tracked-files snapshot. That baseline also pins every tracked `.gitignore`
file. After the container has stopped, it compares the baseline with the final
workspace and retains:

- added, modified, and deleted regular files;
- executable versus non-executable file mode;
- safe relative symlink targets; and
- the full pinned source revision.

The canonical manifest is immutable. Changed file bodies are stored as
SHA-256-addressed blobs, so identical content is retained once. Unchanged files
and empty directories are not copied. `.git` paths, absolute or escaping
symlinks, sockets, devices, FIFOs, and other special files fail closed.

Untracked files ignored by the pinned rules are excluded before the workspace
path ceiling is counted. Matching uses Git's `check-ignore` semantics with
global excludes disabled. Every baseline path remains protected, so a tracked
file is still compared even if an ignore rule matches it. `.gitignore` files
are always compared as ordinary work: replacing one with `*` cannot conceal
the worker's other changes because capture uses the immutable baseline copy,
not the worker-edited rules. If the pinned rules cannot be read securely or
evaluated by Git, capture fails closed.

One result is bounded to 100,000 workspace paths, 64 MiB of changed file
content, and a 16 MiB manifest. The result and staging roots must be private
host-owned directories; manifests and blobs are written with private modes.
The database stores the manifest, digest, path count, and changed-byte count,
but file bodies remain in the adjacent result store.

## Lifecycle and recovery

Result capture happens after the executor has stopped and removed the worker
container but before gateway and workspace cleanup. It applies to successful,
nonzero, cancelled, and recoverable post-start failure outcomes. Only after the
result is recorded may the attempt become terminal.

If capture or its database binding fails, Magic Glass Box cleans the container
and gateway, retains the workspace for manual recovery, and terminalizes the
attempt without a result after cleanup succeeds. This prevents an unbounded or
invalid workspace from wedging the run forever while preserving the files an
operator may need to inspect. Reconciliation applies the same rule to an
expired running attempt; it does not repeatedly retry a capture that violated
the result boundary. Cleanup failures still leave the attempt active for later
reconciliation, and reconciliation never treats a currently leased attempt as
abandoned.

## Inspect and apply

Use the attempt ID shown by `run show`:

```console
magic-glass-box result show att_<id>
magic-glass-box result apply att_<id> --repository /absolute/path/to/checkout
```

`show` verifies the filesystem manifest and every required blob against the
database-bound digest before returning metadata. `apply` performs the same
verification, then inspects every affected destination path. If any path no
longer matches the captured baseline, no path is changed. After a successful
preflight it reconstructs deletions, files, modes, and safe symlinks using
atomic replacement for written paths.

The operation is intentionally explicit. Results are never applied to the
source checkout automatically and never become authority for a replacement
worker. Magic Glass Box does not stage or commit Git changes, push a branch,
open a pull request, contact GitHub or GitLab, or store a forge credential. The
operator remains responsible for reviewing the local result and choosing any
subsequent Git commands.
