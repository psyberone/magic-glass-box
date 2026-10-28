# Containerized remote runner appliance

**Status: design only; planned Milestone 19 candidate after audit, dogfood, and
the first reusable-template delivery.**

The M15 SSH carrier proved the controlled-execution protocol against an
operator-owned host. Its first deployment installs the MGB runner and its
dependencies directly into a private host environment. The appliance design
makes Docker the only substantial remote runtime prerequisite: MGB reaches one
pinned, already-running controller container through a forced SSH command, and
all actual work still runs in fresh pinned job containers.

This changes deployment mechanics, not authority. It does not add a shell
capability, an agent-selected host, or arbitrary Docker access.

## Shape

```text
MGB execution carrier
        |
        | framed request on stdin; framed response on stdout
        v
dedicated forced-command SSH key
        |
        | fixed docker exec against one exact controller container
        v
containerized MGB runner controller
        |
        | exact candidate + operator-authored recipe
        v
fresh pinned job container
```

The controller and job roles remain separate:

- The **runner controller** is a long-lived protocol endpoint. It validates
  requests, owns durable remote job state, and drives job-container lifecycle.
- A **job container** is disposable and recipe-specific. Candidate validation,
  tests, tools, and optional artifact production happen there, never in the
  long-lived controller filesystem.

Running jobs directly through `docker exec` inside the controller would allow
one job to contaminate later work, share mutable process state, and evade
per-job image and resource identity. `docker exec` is therefore only the SSH
transport into the controller protocol. It is not the workload primitive.

## Remote prerequisite

An appliance-backed SSH controller requires:

1. OpenSSH and Docker on the remote machine.
2. An operator-loaded runner image pinned by immutable image ID. MGB never
   pulls or selects it during an invocation.
3. One already-running controller container with a read-only root, fixed
   non-root user, no external network, bounded resources, a private persistent
   state volume, and access to one explicitly selected Docker engine.
4. A dedicated SSH key whose `authorized_keys` entry uses `restrict` and a
   fixed command equivalent to:

   ```text
   /usr/bin/docker exec -i --user 65532:65532 <exact-container-id> /usr/local/bin/mgb-runner invoke
   ```

5. An inspected deployment identity registered with the MGB execution
   provider before any project capability is bound.

The forced command pins an exact controller ID, not a caller-provided name.
It ignores `SSH_ORIGINAL_COMMAND`, forwards no SSH environment, allocates no
TTY, and accepts only the bounded protocol document on stdin. A small fixed
host bridge may replace the literal command when deployment health checks are
needed, but it may only inspect and enter that one configured controller. It
must never evaluate caller text or expose a general host-command path.

## Docker authority

A container with the ordinary rootful Docker socket can generally obtain
host-root-equivalent authority. Putting the runner in a container standardizes
its environment; it does not make that socket harmless.

The preferred deployment therefore uses a dedicated rootless Docker daemon
owned by the remote runner account. The controller may manage only that
daemon's images, volumes, and job containers. Compromise then reaches the
runner account's isolated engine rather than silently becoming host root.

Rootful Docker may remain an explicitly accepted compatibility mode, labelled
as host-authority mode in inspection and registration. There is no silent
fallback from rootless to rootful. A generic HTTP “Docker socket proxy” is not
treated as a security boundary: any API broad enough to create arbitrary
containers and mounts is still broad authority.

## Protocol and authority

The appliance reuses the M15 submit, reconcile, inspect, cancel, and collect
contract. A deployment inspection should additionally bind:

- runner protocol and schema versions;
- controller image ID and deployment/configuration digest;
- persistent-state identity;
- rootless or explicitly accepted host-authority engine mode;
- supported repository object formats and bounded archive limits;
- recipe IDs, immutable recipe digests, and job image IDs; and
- controller and job resource ceilings.

The agent never supplies an SSH destination, controller identity, executable,
Docker argument, job image, or recipe argv. There is no generic
`exec(command)` operation. A requested “raw command” must first become an
operator-authored, digest-bound recipe and then run in a disposable job
container under an explicitly bound capability. Operator diagnostics remain
administrative actions and do not become agent authority.

Candidate archives continue to arrive over stdin through the existing raw Git
snapshot boundary. No source checkout, credential directory, or project path is
mounted from the SSH host into the controller. Results leave only through the
existing bounded status and evidence documents.

## Lifecycle

- **Install:** load the pinned image, create the private state volume, start the
  controller with fixed limits, inspect it, then install the forced key.
- **Restart:** restart the same container ID with its durable state volume; an
  interrupted invocation is reconciled through the ordinary operation ledger.
- **Upgrade:** create and inspect a new controller beside the old one, drain
  active jobs, atomically switch the forced-command entry and registered
  deployment identity, then retire the old controller. Tags such as `latest`
  never authorize an upgrade.
- **Removal:** revoke the project binding and SSH key before removing the
  controller or state. Historical operations retain the old deployment
  identity even when reconciliation is no longer possible.

## Acceptance proof

M19 is complete only when a repeatable live proof demonstrates all of the
following:

- the remote host has no MGB Python package or Git dependency outside the
  image, while the protocol still operates through Docker and OpenSSH;
- a requested SSH shell command is ignored and reaches only the fixed runner
  entrypoint;
- a stopped, missing, or substituted controller fails closed;
- the inspection identity distinguishes rootless from explicitly accepted
  host-authority mode;
- controller restart preserves durable job identity and supports reconciliation
  without creating a duplicate;
- exact-candidate success, lost-response recovery, cancellation, timeout, and
  bounded evidence collection still pass;
- two jobs cannot contaminate one another through controller filesystem or
  process state;
- no generic command, Docker, SSH, credential, or host-mount authority reaches
  a worker; and
- credentials and private host/container paths remain absent from core state,
  worker-visible material, and normalized evidence.

Until that proof passes, the M15 host-installed runner remains the accepted
implementation and this document makes no portability or security claim for
the appliance deployment.
