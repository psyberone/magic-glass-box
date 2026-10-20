# Provider-neutral service sidecars

Milestone 10 implements the credential-free core half of the service-sidecar
boundary: immutable registration, exact semantic operation authority, a bounded
Unix-socket protocol, a durable attempt ledger, and uncertain-effect
reconciliation. No provider-specific add-on ships in the core. GitHub mechanics
and `GH_CONFIG_DIR` handling remain Milestone 11 work.

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
  mutations;
- `change.*` — create or inspect a proposed code change;
- `checks.*` — read normalized CI and approval evidence;
- `refs.*` — inspect or create exact repository refs; and
- `landing.*` — invoke and reconcile one authorized landing.

Operations are individually versioned and paginated or otherwise bounded. The
sidecar normalizes provider responses; raw payloads and unrestricted issue or
message bodies do not cross into the core. Existing work-item imports remain
the authority-neutral content intake path, and no connector silently exports or
synchronizes imported text.

Provider routing is an explicit allowlist. If a project does not bind a
capability to a provider, that surface is unavailable and is not advertised to
an agent. MGB never discovers a default controller or falls back to a raw
provider client. Bindings may be granular enough to allow a read while
disabling a related mutation, but remain semantic operations rather than a
mirror of every vendor endpoint.

## Registration and authority

Before use, a sidecar registers:

- protocol version and connector identity;
- supported capability versions;
- operator-owned service, host, repository, and account mappings;
- an effective credential-scope fingerprint without the credential value; and
- transport, response-size, duration, rate, and concurrency limits.

Protocol v1 uses an operator-owned Unix socket beneath a mode-`0700` directory,
with a mode-`0600` socket. Frames are length-prefixed, JSON is strict and
duplicate-free, and one absolute deadline bounds connect, send, and receive.
An external carrier would need its own revocable client authentication; public
reachability is not authentication. Worker-supplied hosts, repositories,
account names, or socket paths never select the connector.

Every invocation binds the operation, mapping, account, scope fingerprint,
exact revisions or work-object IDs, limits, and durable operation ID. Credential
rotation changes the fingerprint and invalidates authority that required the
old scope.

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
dispatch becomes retryable; a lost or malformed reply after dispatch becomes
uncertain and cannot be invoked again. An expired dispatch lease is also
conservatively abandoned into the uncertain state before its concurrency lane
is reused. Only reconciliation may then report success, continued uncertainty,
or absence of the remote effect. Logs and SQLite records contain operation IDs,
bounded classifications, normalized receipts, and digests—never credential
values or raw provider bodies.

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
`schemas/service-operation-v1.schema.json`. The on-wire correlated envelopes
are `service-operation-request-v1.schema.json` and
`service-operation-response-v1.schema.json`; runnable examples live in
`examples/`.

```console
magic-glass-box service-sidecar register example \
  --spec examples/service-sidecar.json --created-by operator
magic-glass-box service-sidecar operation-create \
  --request examples/service-operation.json --created-by operator
magic-glass-box service-sidecar operation-execute sop_<id> \
  --owner supervisor --expected-version 1
magic-glass-box service-sidecar operation-reconcile sop_<id> \
  --owner replacement-supervisor --expected-version 2
```

`show`, `list`, `operation-show`, `operation-list`, and `revoke` expose only
bounded authority and lifecycle metadata. The deterministic and real-Unix
golden proofs both lose a successful invoke response, replace the supervisor,
reconcile the existing object, and assert that the remote create count is one.

## Reference add-ons

Milestone 11 supplies a separately packaged GitHub reference add-on using the
user's existing `GH_CONFIG_DIR` and binds applicable operations to the existing
integration authority. GitLab, Jira, and future services remain independent
add-ons implementing only the families they support.

This capability-provider direction does not expand M10 or M11. M10 proves the
credential-free operation and reconciliation kernel; M11 proves one GitHub
provider against it. Later heterogeneous providers may require a new versioned
resource-binding shape rather than forcing Jira projects, runners, or other
resources into v1's repository-shaped mapping.

No provider-specific SDK, CLI, credential, configuration path, status vocabulary,
or merge quirk belongs in the Magic Glass Box core.
