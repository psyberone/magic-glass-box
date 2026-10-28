# Provider-neutral service sidecars

Milestone 10 implements the credential-free core half of the service-sidecar
boundary: immutable registration, exact semantic operation authority, a bounded
Unix-socket protocol, a durable attempt ledger, and uncertain-effect
reconciliation. Provider-specific code does not execute in the core. Milestone
11 supplies the first out-of-process reference provider for GitHub and
completed its live repository proof. Milestone 14 now advances the wire
contract for a non-repository work controller without changing v1.

Service sidecars are optional credential-bearing add-ons. They execute
already-authorized provider mechanics; they do not own candidate readiness,
dependency order, budget policy, or landing policy. “Sidecar” describes their
deployment and isolation. The broader extension model calls them
**capability providers**: project policy maps small core-owned semantic surfaces
to explicit controllers—configured provider instances. See
[capability-providers.md](capability-providers.md).

They are distinct from Milestone 7D
[inference connectors](inference-connectors.md). Both isolate upstream
credentials behind a narrow versioned API, but inference calls are transient,
quota-consuming model operations while service sidecars perform durable reads
and effects that may require provider-side reconciliation after a lost reply.

This boundary is broader than a “forge API.” GitHub may support work items,
changes, refs, checks, and landing, while a Jira connector may support only work
operations. The core speaks semantic capability families and does not require
every service to imitate a code host.

## Trust boundary

A sandboxed worker is untrusted, networkless, and disposable. It never receives
a reusable service credential, provider configuration directory, raw provider
client, arbitrary destination URL, or sidecar transport endpoint.

A sidecar is a trusted credential actuator selected and configured by the
operator. A local configuration directory such as `GH_CONFIG_DIR` may be
exposed only to that sidecar. It is never mounted into a worker or copied into
control-plane state.

```text
worker -- bounded read/proposal --> Magic Glass Box
                                      |
supervisor -- durable action ---------+
                                      |
                                      v
                              authorized operation
                                      |
                                      v
                             pinned service sidecar
                                      |
                                      v
                              external provider API
```

Integration effects such as opening a change, pushing a ref, reading gates for
readiness, or landing a candidate are trusted control-plane actions. The M10
operator surface creates, invokes, and reconciles them; no worker gateway
capability exposes the sidecar transport. A future worker read capability may
be added separately, but a worker proposal cannot become integration authority.

## Semantic capabilities

The protocol advertises a version and a subset of bounded families such as:

- `work.*` — list/get normalized work metadata and explicitly authorized work
  effects when a later core contract defines one;
- `change.*` — create or inspect a proposed code change;
- `checks.*` — read normalized CI and approval evidence;
- `refs.*` — inspect or create exact repository refs; and
- `landing.*` — invoke and reconcile one authorized landing; and
- `execution.*` — submit, inspect, cancel, and collect bounded evidence for an
  exact candidate under an operator-pinned remote recipe.

Operations are individually versioned and paginated or otherwise bounded. A
successful read receipt may carry one bounded normalized JSON result in
addition to its identity and evidence digest. The sidecar normalizes provider
responses; raw payloads, comments, transcripts, and unrestricted message bodies
do not cross into the core. `work.get` v1 must return the canonical bounded
provider-neutral work-item envelope, including its execution brief.
`work.list` v1 must return the core-owned bounded reference window; provider
cursors, status names, query syntax, and raw descriptions are not result
fields. A successful get can be fed deliberately into `work-item
import-operation`. No connector silently imports, exports, or synchronizes
work. `work.state` v1 is the initial work effect: it names only the core-owned
`done` state and returns the strict provider-neutral state result. Provider
status and transition identifiers remain private configuration and mechanics.

Provider routing is an explicit allowlist. If a project does not bind a
capability to a provider, that surface is unavailable and is not advertised to
an agent. MGB never discovers a default controller or falls back to a raw
provider client. Bindings may be granular enough to allow a read while
disabling a related mutation, but remain semantic operations rather than a
mirror of every vendor endpoint.

The core currently defines `work.state` v1, `refs.publish` v1, `change.open` v1,
`landing.invoke` v1, `execution.submit` v1, and `execution.cancel` v1 as
effects. `work.get`, `work.list`, `change.get`, `change.list`, `checks.read`,
`execution.inspect`, and `execution.collect` v1 are reads. A provider must
advertise the matching version and mode before it can be bound. Syntactically valid
experimental capability names may still be registered for deliberate
operator-only use, but cannot be project-bound until the core defines their
durable meaning.

## Registration and authority

Before use, a sidecar registers:

- protocol version and connector identity;
- supported capability versions;
- operator-owned service, host, resource, and account mappings;
- an effective credential-scope fingerprint without the credential value; and
- transport, response-size, duration, rate, and concurrency limits.

Protocol v1 uses an operator-owned Unix socket beneath a mode-`0700` directory,
with a mode-`0600` socket. Frames are length-prefixed, JSON is strict and
duplicate-free, and one absolute deadline bounds connect, send, and receive.
An external carrier would need its own revocable client authentication; public
reachability is not authentication. Worker-supplied hosts, repositories,
account names, or socket paths never select the connector.

The published v1 registration and operation shapes keep their original
`repository` and `resource_id` fields and exact canonical digests. Protocol v2
uses a core-owned `{kind, external_id}` resource plus an `object_id`. The
initial kinds are `repository` and `work_project`; non-repository targets cannot
carry Git object revisions. Schema and protocol generations must match, and a
mapping and target must name the identical resource. The provider name remains
separate, so Jira, another issue system, and an operator-defined adapter all use
`work_project` rather than minting provider-native authority types.

Every invocation binds the operation, mapping, account, scope fingerprint,
exact revisions or work-object IDs, limits, and durable operation ID. A
project-routed operation additionally pins its active capability binding in the
same SQLite transaction that creates the operation. Credential rotation changes
the fingerprint and invalidates authority that required the old scope.

## Effects, retries, and reconciliation

A caller-supplied idempotency key is useful local correlation but is not proof
that GitHub, GitLab, Jira, or another provider deduplicated an effect. The core
therefore owns a durable operation ledger with an invoke/status/reconcile
lifecycle.

Connectors use deterministic remote identities wherever the provider permits:
an exact branch ref, a change head/base pair, or a bounded marker associated
with the durable operation. If the provider completed an effect but the reply
was lost, a replacement supervisor asks the connector to discover that effect
and return the existing receipt rather than issuing it again.

Typed outcomes distinguish denial, conflict, retryable transport failure,
provider throttling, uncertain effect, and confirmed success. A failure before
dispatch becomes retryable. For an effect, a lost or malformed reply after
dispatch becomes uncertain and cannot be invoked again; an expired effect lease
is likewise abandoned into the uncertain state. Only reconciliation may then
report success, continued uncertainty, or absence of the remote effect. A read
cannot create a duplicate external effect, so an ambiguous reply or expired
lease becomes retryable and never enters reconciliation. Logs and SQLite
records contain operation IDs, bounded classifications, normalized receipts,
and digests—never credential values or raw provider bodies.

Every registration pins request and response byte limits, timeout, concurrency,
and attempts per minute. Each operation may narrow but never expand those
limits. Exact duplicate idempotency keys resolve to the existing operation;
reuse with different authority or input fails closed. Revocation, optimistic
versions, concurrency, rate, connector deployment, mapping, account, scope,
repository, revision, capability, and mode are rechecked before dispatch.

## Reads carry credentials too

Remote check state, approvals, change metadata, and closed-reference sets can
require the same reusable credential as writes. Those reads stay in the
sidecar. The core receives only normalized evidence bound to the exact request
and candidate tuple; workers receive only fields allowed by their own named
capability.

## Operator surface and published contracts

Registration and operation documents are published as Draft 2020-12 schemas in
`schemas/service-sidecar-v1.schema.json` and
`schemas/service-operation-v1.schema.json`, with the resource-neutral generation
in the corresponding `*-v2.schema.json` files. Each generation also publishes
its correlated operation request and response. Runnable v1 and v2 examples live
in `examples/`.

```console
magic-glass-box service-sidecar register example \
  --spec examples/service-sidecar.json --created-by operator
magic-glass-box capability bind demo-project change.open --version 1 \
  --sidecar example --mapping primary --bound-by operator
magic-glass-box service-sidecar operation-create \
  --request examples/service-operation.json --binding cpb_<id> \
  --created-by operator
magic-glass-box service-sidecar operation-execute sop_<id> \
  --owner supervisor --expected-version 1
magic-glass-box service-sidecar operation-reconcile sop_<id> \
  --owner replacement-supervisor --expected-version 2
```

`capability show/list/unbind` expose the project route and its append-only
lifecycle. Unbinding stops new bound operations without rewriting existing
ones. `service-sidecar show`, `list`, `operation-show`, `operation-list`, and
`revoke` expose only bounded authority and lifecycle metadata. Omitting
`--binding` creates a deliberate operator-only operation and is not the path
M11 automation will use. The deterministic and real-Unix golden proofs both
lose a successful invoke response, replace the supervisor, reconcile the
existing object, and assert that the remote create count is one.

## Reference add-ons

Milestone 11's separately executed
[GitHub reference provider](github-provider.md) uses the user's selected
`GH_CONFIG_DIR`, lists bounded normalized work/change metadata, returns exact
import-ready work, publishes exact candidate refs from an allowlisted candidate
root, opens deterministic changes, reads named checks/approvals, and binds
head-enforced landing to the existing integration authority. The credential
directory and candidate root are process configuration, not worker-visible or
stored operation credentials. Milestone 14's separately executed
[Jira reference provider](jira-provider.md) proves the same boundary for a v2
`work_project`: its private file owns the Jira base URL, account, API token, and
core-state-to-provider-status mapping, while MGB and workers receive only
normalized work contracts. GitLab and future services remain independent
add-ons implementing only the families they support.

Milestone 15's separately executed
[controlled execution provider](controlled-execution-provider.md) derives an
exact raw candidate snapshot, uses one fixed credential-isolated SSH carrier,
and invokes only an operator-pinned remote recipe and image. Submit and cancel
retain effect uncertainty and reconciliation; inspect and evidence collection
remain retryable reads. Neither a hostname nor a command is an operation
parameter or worker capability.

This capability-provider direction does not expand M10 or M11. M10 proves the
credential-free operation and reconciliation kernel; M11 proves one GitHub
provider against v1. M14's explicit v2 resource authority prevents Jira
projects and future bounded resources from being disguised as repositories.

No provider-specific SDK, CLI, credential, configuration path, status
vocabulary, or merge quirk belongs in the Magic Glass Box core. Core-owned
states such as `done` are semantic outcomes, not aliases for a Jira status.
