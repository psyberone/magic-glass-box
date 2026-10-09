# Architecture

## Ownership boundaries

Magic Glass Box is the trusted control plane. It owns local users, Krab
identity bindings, versioned project context, versioned agent profiles, run
lifecycle, credential policy, container lifecycle, and pending human
interactions.

Imitation Krab is the communications authority. It owns Krab users, tokens,
project membership and roles, sessions, items, work claims, and message
delivery. Magic Glass Box refers to those objects by stable IDs and does not
duplicate private message bodies or transcripts.

Docker workers are disposable and untrusted. A worker receives one
tracked-files-only snapshot, one pinned context version, one pinned profile
version, and only the credentials authorized for that run. It never receives
the Docker socket, a Krab admin token, the control-plane database, another
run's workspace, or the source repository's Git metadata.

## Trust flow

```text
operator/browser
      |
      v
Magic Glass Box ------ stable IDs / metadata ----> imitation-krab
      |
      +---- short-lived, scoped material ----> sandboxed worker
      |
      <---- structured events/questions -----+
```

Krab metadata is structurally validated. Work-item titles are retained as
`untrusted_text`; they may be displayed but must never be interpreted as agent
instructions. Bodies, notes, and transcripts are not ingested through the
project-activity projection.

## Durable model

- A local user can be linked to one or more Krab identities. The binding uses the
  immutable Krab `user_id`; handles are cached only for display.
- A credential reference points to an external secret source. The current
  implementation supports private token files and never persists the token.
- Project context revisions are append-only and content-addressed.
- Agent profile revisions are append-only and contain an image reference,
  argv-form command, workspace/network policy, and a dedicated Krab identity.
- A run pins exact context and profile revisions. Later edits cannot change an
  in-flight or historical run.
- The requested source revision is resolved to a full Git commit and persisted
  in the transition to `running`.
- Every run transition is appended to an immutable event stream and guarded by
  optimistic concurrency.
- A human interaction records only its type, lifecycle, recipient, run, and
  Krab item IDs. Its question and answer remain private content in Krab.
- Opening an interaction atomically moves its run to `waiting_for_input`.
  Answering records the response item ID and re-queues the durable run so a
  replacement worker can resume it; terminal run transitions cancel any open
  interaction.

## Execution lifecycle

For an execution request, Magic Glass Box:

1. atomically claims a queued run by transitioning it to `preparing`;
2. resolves its requested revision and exports only tracked Git files;
3. resolves exactly the credential names in the pinned profile;
4. resolves the image reference to an immutable digest and creates the sandbox;
5. records the commit, image, and container identity before starting it;
6. waits for the agent, removes all labeled containers and the snapshot, then
   records the exit status and terminal state.

Operational failures become audited `failed` runs only after cleanup succeeds.
If cleanup cannot be proven, the run remains active so `run reconcile` can retry
after a restart. Reconciliation finds containers by product and run-ID labels
rather than trusting an in-memory container handle. It also recognizes the
retired label namespace so an upgrade can clean interrupted legacy containers.

The Docker boundary rejects images with Dockerfile-declared volumes, validates
host paths before mounting them, runs as the host's non-root numeric identity,
uses a read-only root filesystem, drops all capabilities, applies resource
ceilings, and bounds local logs. It currently supports network policy `none`.
`krab_only` fails closed rather than granting a general bridge network while a
scoped relay is absent.

## Planned slices

Implementation is ordered around demonstrable product guarantees rather than
independent subsystems. See [roadmap.md](roadmap.md) for the current sequence and
exit proofs. The metadata-only durable interaction ledger is complete. The next
slice is its Krab transport, followed by the run-scoped agent gateway.
