# Trusted candidates

A retained result is inert file evidence. A candidate is the deterministic Git
commit Magic Glass Box can later submit to review and integration gates.
Candidate creation is an explicit operator action; a successful worker does not
create one automatically.

## Trust boundary

Magic Glass Box treats the worker workspace as untrusted while capturing a
result. Capture validates path aliases, file types, symlink containment, Gitlink
boundaries, and size limits before publishing the result bundle.

After publication, the private state directory, SQLite database, retained result
store, and candidate bare repositories are trusted control-plane state. They use
normal integrity mechanisms:

- private state roots;
- SQLite constraints, foreign keys, transactions, and immutable-row triggers;
- bounded reads and content digests for accidental corruption;
- atomic temporary-file publication;
- one ordinary interprocess file lock per candidate store; and
- deterministic reconstruction when a cache object or ref is absent.

The control plane does not model an attacker with a same-user shell rewriting
`.mgb` between individual system calls. It therefore does not maintain recovery
watermarks, PID leases, ref-age leases, inode-identity loops, or a stale-cache
reconciler.

## Authority and identity

Creation accepts one retained result from a succeeded, zero-exit attempt pinned
to a durable work-item thread and project context. The storage transaction
checks that authority, including that the candidate base is the result's pinned
source revision, when inserting the candidate. Later candidate reads trust
the immutable ledger instead of replaying the attempt's complete history on
every query.

Database schema v11 introduced the immutable candidate ledger and only the
indexes used by candidate discovery. Each `CandidateManifest` v1 binds:

- project, pinned context revision, and work thread;
- producing run, attempt, and retained-result record;
- exact base commit, candidate commit, tree, and Git object format;
- retained-result SHA-256 digest; and
- the local user who requested materialization.

One retained result has at most one candidate. An identical retry is
idempotent; a retry that would produce different identity is rejected.

## Object construction

Candidate construction never checks files out and never modifies the operator
repository. It imports the exact local base commit into a private bare
repository, seeds a temporary Git index with `read-tree`, applies only the
verified manifest paths with `update-index --index-info`, writes the tree, and
creates one deterministic child commit with `commit-tree`.

Regular files become raw blobs with `hash-object -w --no-filters`. Git
attributes, checkout filters, CRLF conversion, `ident`, and LFS hydration cannot
change their bytes. Executable modes, deletions, and UTF-8 relative symlinks are
represented directly. Shared path validation rejects `.git` aliases across
supported filesystems. The complete resulting symlink graph must remain inside
the repository. A worker cannot change a staged Gitlink, and candidate creation
also rejects a manifest path at or below a Gitlink.

The base tree is listed once. Required base objects are streamed through one
`cat-file --batch` process. Regular file bodies are hashed without being held
in memory; only bounded symlink targets are retained. The base import and
workspace snapshot share the same 512 MiB ceiling, so a valid deletion cannot
become unpromotable at a smaller candidate-only buffer limit. Post-images are
deduplicated and hashed in one bounded batch. File/directory/symlink transitions
are accepted only when the manifest completely describes the affected tracked
leaves.

The commit has the pinned base as its single parent. Its message binds the base
and result digest, while fixed author, committer, and timestamp make identical
input deterministic. The SQLite record—not the commit message—is project and
work authority.

Git receives a fresh home and configuration environment. System and global
configuration, credential helpers, hooks, replacement objects, lazy fetch,
automatic maintenance, filesystem monitors, prompts, and submodule recursion
are disabled. Only local file transport is enabled for base import.

## Cache and recovery

Candidate repositories are derived caches beside the database:

```text
candidates/
  projects/
    <sha256(project-id)>/
      <sha1-or-sha256>/
        .lock
        repository.git/
```

Deterministic refs make lookup explicit:

```text
refs/mgb/bases/<base-object-id>
refs/mgb/candidates/<candidate-object-id>
```

`candidate show` reads the immutable ledger and inspects the named cached
candidate first. A healthy cache therefore remains readable without the source
checkout or retained-result store. If targeted inspection fails, recovery loads
the retained result and rebuilds from the cached base commit. The external
source checkout is consulted only when that base object is absent, including
after complete cache loss. A reconstructed head and tree must exactly match the
ledger before the command returns. Idempotent `candidate create` retries use the
same inspect-first path.

Inspection is targeted: it checks the named ref, exact raw commit, parent,
tree, provenance message, base commit, and tree object. It does not run
whole-repository `git fsck --strict` on every read. The cache has no independent
authority and no garbage-collection reconciliation API.

Repository paths are UTF-8 relative POSIX paths with a 4,096-byte and
128-component ceiling. Empty components, `.`/`..`, backslashes, absolute paths,
and cross-platform `.git` aliases fail closed. A staged Gitlink is represented
as an empty placeholder directory and must remain unchanged; capture rejects a
replacement of either the Gitlink or any parent with a symlink. That failure
retains the stopped workspace for operator recovery rather than silently
discarding worker output.

## Operator surface

Create a candidate:

```console
magic-glass-box candidate create att_<id> --created-by operator \
  --operation-timeout 60
```

Show it, rebuilding a missing cache if necessary:

```console
magic-glass-box candidate show cnd_<id> --operation-timeout 60
```

Discover durable candidates without opening the result or Git stores:

```console
magic-glass-box candidate list
magic-glass-box candidate list --project project-key --limit 25
magic-glass-box candidate list --run run_<id>
magic-glass-box candidate list --work-thread wth_<id>
```

Candidate operations perform no push, remote fetch, pull-request creation,
remote CI query, approval query, or merge. Exact-input review and local gates
are implemented by Milestone 7B; Milestone 7C adds separate immutable folds,
ordering, and nonce-bound manual landing without mutating the origin candidate.
When an independent review rejects a candidate, a changes-requested run starts
from that exact retained head. Its successful result materializes as a new
child candidate instead of rewriting the rejected candidate. The immutable
changes-requested binding also provides the ancestry needed to reconstruct the
parent and child after loss of the private candidate cache.
See [integration-gates.md](integration-gates.md).

## Repeatable proof

Run the candidate boundary against real local Git repositories and SQLite with:

```console
make candidate-test
```

The automated suite covers deterministic SHA-1 and SHA-256 construction,
common group-writable umasks, hostile ambient Git configuration, raw bytes,
executable modes, symlinks, directory/leaf transitions, Gitlink containment,
concurrent idempotence, inspect-first reads without source/result availability,
cached-base repair, targeted inspection, immutable-ledger discovery, and
complete cache reconstruction from the database, retained result, and pinned
base commit in the project repository.
