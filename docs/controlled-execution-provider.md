# Controlled remote execution provider

Milestone 15 adds a provider-neutral remote-validation surface without giving
an agent a remote shell. MGB authorizes an exact candidate, an immutable recipe
identity, and four separately bindable operations: `execution.submit`,
`execution.inspect`, `execution.cancel`, and `execution.collect` v1. The first
carrier uses SSH to reach an operator-owned runner, but SSH is an implementation
detail of that controller rather than worker authority.

## Boundary and data flow

```text
operator/supervisor
  -> durable project-bound service operation
  -> local execution provider over a private Unix socket
  -> raw snapshot of the exact candidate commit
  -> one fixed SSH destination and forced remote helper
  -> one pinned Docker image and operator-authored recipe
  -> normalized status or bounded evidence receipt
```

The worker and the work item never receive the SSH host, account, key,
known-hosts file, provider socket, Docker socket, recipe command, or reusable
credential. The core stores only the ordinary service-sidecar authority plus
normalized execution results. The SSH carrier's private mode-`0600`
configuration owns the fixed destination, credential files, remote helper, and
logical-repository-to-candidate-store mapping.

The candidate carrier does not use `git archive`, a checkout, a forge, or a
network transport. It reuses MGB's raw Git snapshot boundary, verifies the
exact SHA-1 or SHA-256 commit and every blob identity, and packages a
deterministic bounded archive. Attributes are inert: `export-ignore` does not
remove files, `export-subst` does not rewrite bytes, filters do not execute,
LFS pointers remain pointers, and Git-links remain empty directories. Every
status and evidence result retains the archive byte count and SHA-256 digest in
addition to the candidate commit.

## Durable semantics

- `execution.submit` is an effect. Its remote identity is derived from the
  durable submission operation ID, so the same operation cannot create a
  second job. The remote runner atomically records the exact job, source
  digest, submission time, and timeout before it creates a container.
- Submission reconciliation carries the same exact candidate archive. It may
  resume a remote intent that was already recorded, including a crash between
  container creation and start, but it returns `not_found` and creates nothing
  when no remote intent exists.
- `execution.inspect` is a retryable observation of that submission identity.
  It cannot select a different candidate, recipe, image, repository, or host.
- `execution.cancel` is an effect. The runner records a cancellation marker
  before stopping the container. Reconciliation may finish only a cancellation
  whose marker already exists; it never invents a cancellation after a lost
  pre-dispatch request.
- `execution.collect` is a retryable read available only after a terminal
  result. It returns normalized identity, exit status, timestamps, bounded
  captured-output size and digest, and a deterministic evidence bundle.

Effects retain the existing service-operation uncertainty rules. A lost submit
or cancel reply is never replayed as a fresh effect. Reads remain safely
retryable. Follow-up operations are derived from the successful submission
receipt and retain its service operation ID, controller, project, resource,
candidate, recipe, image, and execution identity.

Recipe timeouts are enforced whenever the runner is observed by submit,
reconcile, inspect, cancel, or collect. The MGB supervisor or operator must
therefore keep inspecting an active job; v1 does not claim an independent
remote watchdog that survives a complete control-plane outage.

## Remote runner confinement

The remote mode-`0600` configuration allowlists logical repositories and
recipes. A recipe digest covers its image digest, fixed argv, timeout and stop
interval, CPU, memory, PID, and evidence ceilings. Docker is invoked with:

- `--pull never` and an immutable `sha256:` image ID;
- no network, a read-only root filesystem, all capabilities dropped, and
  `no-new-privileges`;
- fixed CPU, memory, PID, temporary-filesystem, output-log, and stop bounds;
- the `local` log driver with one uncompressed bounded file (Docker 29 rejects
  compression when `max-file=1`);
- the candidate workspace mounted read-only; and
- no inherited Docker client configuration or registry credential.

The private per-job directory remains mode `0700`. Inside it, the candidate
mount root and directories are read/traverse-only to non-owners (`0755`), with
regular files normalized to `0644` or `0755`, so an image-pinned non-root user
can actually read the source without gaining a writable host path.

An image may optionally emit artifacts by writing one gzip-compressed tar file
to the path in `MGB_ARTIFACT_PATH`. That path is a single host file inside the
private per-job directory, not a host directory mount. A Docker `fsize` rlimit
bounds it while the container runs. An empty file means no artifacts. After
exit, the runner treats a non-empty file as hostile: it rejects traversal,
unsafe symlinks, duplicate or aliased paths, hard links and special files,
oversized content, and excessive path counts before extracting into quarantine.
The ordinary `WorkspaceResultStore` boundary then captures and packages the
verified changes. Artifact content and raw stdout/stderr do not enter
control-plane logs or SQLite. Raw output remains only in the bounded remote
Docker log, while the normalized receipt carries the artifact bundle plus
output size and digest.
Even zero changed artifact paths produce a non-empty, digest-bound normalized
result bundle; `artifact_count`, not bundle byte length, expresses an empty
artifact set.

The upload file is mode `0666` only so arbitrary non-root image users can write
the bind-mounted file. Its per-job directory and every ancestor controlled by
the runner are mode `0700`; it is not a shared host path.

Archive headers are consumed through a counted stream into a private
quarantine. The runner enforces path and expanded-byte ceilings while reading;
it does not first construct an attacker-sized in-memory member list. Files move
into the capture workspace only after the complete path and symlink graph is
valid.

## SSH deployment

Use a dedicated key and remote account. The key's `authorized_keys` entry
should force the one helper and disable every other SSH surface, for example:

```text
restrict,command="/usr/local/bin/magic-glass-box-remote-runner invoke --config /srv/magic-glass-box/remote-runner.json" ssh-ed25519 AAAA...
```

The local carrier also supplies a fixed host, port, account, identity file,
private known-hosts file, and absolute helper path. It disables password and
keyboard authentication, agent and socket forwarding, TTY allocation, local
commands, ambient SSH configuration, and permissive host-key discovery. The
credential-scope fingerprint covers the key bytes, known-hosts bytes,
destination, helper, and candidate-source mapping without exposing any of
those values to the core or a worker.

Install the same MGB version on the remote host, place
`examples/remote-runner-config.json` as a private file, and inspect its
non-secret recipe identities:

```console
magic-glass-box-remote-runner inspect \
  --config /srv/magic-glass-box/remote-runner.json
```

On the control-plane host, prepare the private carrier config from
`examples/execution-provider-config.json`, then inspect its non-secret
credential identity:

```console
magic-glass-box-execution-provider inspect \
  --config /absolute/private/execution-provider.json
```

Copy the reported credential-scope digest and the remote recipe/image digests
into the service-sidecar registration and submission authority. Start the
local carrier only after registering and binding the desired operations:

```console
magic-glass-box-execution-provider serve \
  --config /absolute/private/execution-provider.json \
  --spec /absolute/private/execution-service-sidecar.json \
  --socket-path /absolute/private/execution.sock \
  --max-invocations 100

magic-glass-box service-sidecar execution-submit-create \
  --project demo-project --candidate cnd_<id> \
  --recipe-id quality --recipe-sha256 <sha256> \
  --image-id sha256:<digest> --request-key quality-1 \
  --created-by operator

magic-glass-box service-sidecar operation-execute sop_<id> \
  --owner supervisor --expected-version 1

magic-glass-box service-sidecar execution-followup-create sop_<submit-id> \
  --action inspect --request-key inspect-1 --created-by operator
```

Create and execute a `collect` follow-up after the status is terminal. A lost
submit or cancel response uses the existing `operation-reconcile` command on
that same operation ID. Do not create a replacement effect merely because its
first response was lost.

## Repeatable live proof

`docker/m15-remote-proof.Dockerfile` builds the non-root proof image and
`scripts/check_live_remote_execution.py` drives the explicitly authorized
end-to-end check. The script requires a clean repository with both `HEAD` and
`HEAD^`, creates a real candidate for that delta, starts a private local
carrier, and creates two real jobs on the configured remote runner. Its proof
root must not already exist.

First build the image on the remote host, put its immutable image ID into two
operator-owned recipes named `m15-proof` and `cancel-proof`, and inspect the
runner to obtain their recipe digests. Install a dedicated SSH key whose
`authorized_keys` entry forces that exact runner and configuration. Prepare a
mode-`0600` carrier connection document containing only the fixed SSH and Git
executables, destination, account, credential-file paths, and absolute remote
helper path. Then run:

```console
PYTHONPATH=src .venv/bin/python scripts/check_live_remote_execution.py \
  --repository "$PWD" \
  --proof-root /absolute/private/new-proof-root \
  --proof-id m15-forge-YYYYMMDD-a \
  --connection-config /absolute/private/connection.json \
  --image-id sha256:<image-digest> \
  --quality-recipe-sha256 <quality-recipe-digest> \
  --cancel-recipe-sha256 <cancel-recipe-digest> \
  --confirmation I_UNDERSTAND_THIS_USES_FORGE
```

The proof discards the first successful submit response, requires the durable
operation to enter `uncertain`, and reconciles it to the same remote execution.
It then waits for success, collects one artifact, submits a second job, cancels
it, and collects a zero-path evidence bundle. Finally it scans SQLite,
worker-visible files, carrier streams, and the private report for the raw SSH
key, known-hosts material, and private credential paths. Keep the private
report and remote job state until the milestone audit is complete.

The accepted M15 proof ran on 2026-10-27 against source commit `fc87018`. It
passed both execution paths and an independent host-side inspection of network,
mount, user, capability, image, resource, log, and credential boundaries. The
two earlier stopped attempts and their RED-first remediations remain documented
in the ignored internal proof report.

## Published contracts and limits

`schemas/execution-status-result-v1.schema.json` and
`schemas/execution-evidence-result-v1.schema.json` publish the normalized
results. The four service-sidecar schema generations now recognize the four
core execution capabilities with their fixed modes. Runnable documents live
under `examples/execution-*.json`; the two private configuration examples are
templates and must never contain real credential material in this repository.

This milestone is a controlled validator, not a general remote-interaction
sidecar. There is no hostname parameter, shell endpoint, arbitrary command,
raw SSH/`docker` access, provider credential injection, remote workspace edit,
or automatic deployment surface.
