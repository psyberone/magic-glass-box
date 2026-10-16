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

Gitlinks are represented as empty directories because submodules are never
initialized or recursed. Those placeholders are materialized mode `0555` so
ordinary package managers and build tools fail immediately instead of writing
into them accidentally. This is an early usability guard, not a security
boundary against the same container user; capture still rejects any Gitlink
placeholder that is no longer an unchanged empty directory.

The canonical manifest is immutable. Changed file bodies are stored as
SHA-256-addressed blobs, so identical content is retained once. Unchanged files
and empty directories are not copied. Baseline and changed file bodies use the
exact bytes exposed by the raw Git-tree workspace: checkout filters,
end-of-line conversion, `ident`, export substitutions, and LFS hydration do not
rewrite them. Applying a result writes those retained bytes unchanged.

`.git` paths and their case-insensitive, HFS-ignorable, NTFS short-name,
trailing-dot/space, and alternate-stream aliases fail closed. Absolute or
escaping symlinks, sockets, devices, FIFOs, and other special files fail closed
as well. Every admitted workspace path also has one portable identity formed
from its case-folded, canonically decomposed components. Two leaves, or two
directory spellings, that would be distinct in a Linux worker but collapse on
a default macOS filesystem are rejected before publication.

Symlink containment is evaluated as a graph using that same portable identity:
sibling links are recursively expanded before `..` is interpreted, so two
individually plausible links cannot compose into an escape through a case or
Unicode-normalization alias. The same graph check runs again before applying a
retained bundle, including bundles captured by an older build and chains that
cross unchanged checkout links.

Untracked files ignored by the pinned rules are excluded before the workspace
path ceiling is counted. Matching uses Git's `check-ignore` semantics with
global excludes disabled. Every baseline path remains protected, so a tracked
file is still compared even if an ignore rule matches it. `.gitignore` files
are always compared as ordinary work: replacing one with `*` cannot conceal
the worker's other changes because capture uses the immutable baseline copy,
not the worker-edited rules. If the pinned rules cannot be read securely or
evaluated by Git, capture fails closed. Ignored directories are pruned, but
every directory entry capture does inspect counts toward a separate 200,000
entry safety ceiling. Enumeration and ignore matching use bounded batches, so
a very large flat directory is rejected without first accumulating all of its
entries in memory.

One result is bounded to 100,000 retained workspace paths, 200,000 inspected
workspace entries, 64 MiB of changed file content, and a 16 MiB manifest. The
result and staging roots must be private host-owned directories; manifests and
blobs are written with private modes.
The changed-content ceiling is logical output size: two output paths count
twice even when their identical bytes are stored as one digest-addressed blob.
Every occurrence of one digest must declare the same size.
The database stores the manifest, digest, path count, and changed-byte count,
but file bodies remain in the adjacent result store.

Capture enumerates and stats the stopped workspace before hashing file bodies.
Additions and size-changing replacements whose aggregate already exceeds the
changed-content allowance are rejected without reading their contents. Files
whose size still matches a baseline file must be hashed to determine whether
they changed, but the total bytes capture will hash are capped at the 512 MiB
snapshot ceiling plus the 64 MiB change allowance.

These are capture and retention bounds, not a portable hard disk quota on the
live workspace. A copy-on-write workspace is currently a host bind mount, and
Docker cannot apply an aggregate size limit to that mount consistently across
supported hosts. A worker can therefore consume more host disk while it is
running before capture rejects its output. Deployments that require a hard
runtime storage ceiling must place the workspace root on an operator-managed
quota-limited filesystem. Magic Glass Box does not claim to provide that
ceiling itself yet.

Captured paths use the shared repository-path contract: UTF-8 relative POSIX
paths, at most 4,096 bytes and 128 components, with no empty, `.`/`..`,
backslash, absolute, or cross-platform `.git` alias components. These limits are
intentional fail-closed portability boundaries rather than claims that Git
itself has the same limits on every host.

The worker workspace is the adversarial boundary. Capture uses no-follow reads,
before/after file identity checks, type validation, and a final Gitlink check
because the stopped worker controlled those paths. Once the validated bundle is
atomically published beneath the private result root, it is trusted
control-plane state. Later reads remain size- and digest-bounded for integrity,
but do not repeatedly defend against a same-user attacker swapping private
state between system calls.

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
verification in one pass, then performs one complete conflict preflight and
symlink-graph check across every affected destination path. Mode-only changes
stream the exact baseline body into a fresh inode and atomically install the
new mode; they never `chmod` a possibly shared inode. Written files and
symlinks also use atomic leaf replacement.

The preflight rejects portable aliases independently in the baseline and final
path sets before changing the checkout. A one-for-one case-only rename remains
valid: the differently spelled destination must resolve to the exact captured
deletion, including when a parent directory changes case. Two final paths that
collapse to one destination never qualify as a rename.

A retained file or symlink can replace a directory only when a complete
no-follow preflight proves that every leaf below that directory is an exact
expected deletion and every directory is required to reach one of those
leaves. An untracked or ignored leaf, an extra empty directory, a changed leaf,
or an empty directory at the replacement path rejects the whole apply before
mutation. On success, expected leaves are unlinked deepest-first, only the
proven-empty directories are removed with `rmdir`, and the replacement leaf is
installed atomically.

The inverse transition is also explicit: an exact baseline file or symlink may
become a directory containing added descendants. During preflight, that
verified leaf masks those not-yet-reachable descendants; it is not removed
until every affected path has passed preflight. Apply then unlinks the leaf,
creates only the required parent directories, and atomically installs each
retained descendant. A missing, changed, or wrong-kind baseline leaf rejects
the operation before any path is changed.

`result apply` requires exclusive ownership of a quiescent destination from
the preflight until it returns. Concurrent destination writers are not
supported: there remains an unavoidable operating-system race between a final
validation and its following unlink or replace call. The multi-path operation
is not a filesystem transaction and does not promise whole-tree rollback after
mutation starts. Apply results only to a disposable clean checkout; if an I/O
error or unsupported concurrent mutation occurs during the destructive phase,
discard that checkout and retry from the known baseline.

The operation is intentionally explicit. Results are never applied to the
source checkout automatically and never become authority for a replacement
worker. `result apply` does not stage or commit Git changes, push a branch, open
a pull request, contact a hosting service, or store a service credential.

## Promote to a trusted candidate

A terminal attempt result pinned to a work item may instead be materialized as
an isolated Git candidate:

```console
magic-glass-box candidate create att_<id> --created-by operator
```

This verifies the result again and constructs a deterministic child commit of
the pinned source revision in a separate private bare repository. It does not
modify the operator checkout or contact a remote service. Candidate provenance,
object construction, and inspection are documented in
[candidates.md](candidates.md).
