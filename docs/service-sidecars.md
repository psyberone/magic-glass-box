# Provider-neutral service sidecars

> **DESIGN ONLY — NOT IMPLEMENTED:** Magic Glass Box currently has no service
> sidecar protocol, registration, connector transport, provider read, push,
> change-opening, check-query, comment, close, or landing operation. Everything
> below is planned architecture for Milestone 10 and later.

Service sidecars are optional credential-bearing add-ons planned for Milestone
10 and later. They execute already-authorized provider mechanics; they do not
own candidate readiness, dependency order, budget policy, or landing policy.

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
readiness, or landing a candidate are supervisor-driven durable actions. A
worker may receive separately authorized bounded service reads or propose an
operation, but it cannot convert that proposal into integration authority.

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

## Registration and authority

Before use, a sidecar registers:

- protocol version and connector identity;
- supported capability versions;
- operator-owned service, host, repository, and account mappings;
- an effective credential-scope fingerprint without the credential value; and
- transport, response-size, duration, rate, and concurrency limits.

The transport is local and authenticated—for example, an operator-owned Unix
socket or loopback channel with a per-process capability. Worker-supplied hosts,
repositories, account names, or socket paths never select the connector.

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
provider throttling, uncertain effect, and confirmed success. Logs contain
operation IDs and bounded classifications, never credential values or raw
provider bodies.

## Reads carry credentials too

Remote check state, approvals, change metadata, and closed-reference sets can
require the same reusable credential as writes. Those reads stay in the
sidecar. The core receives only normalized evidence bound to the exact request
and candidate tuple; workers receive only fields allowed by their own named
capability.

## Reference add-ons

Milestone 10 proves the protocol and recovery behavior against a deterministic
fake connector. Milestone 11 supplies a separately packaged GitHub reference
add-on using the user's existing `GH_CONFIG_DIR`. GitLab, Jira, and future
services remain independent add-ons implementing only the families they
support.

No provider-specific SDK, CLI, credential, configuration path, status vocabulary,
or merge quirk belongs in the Magic Glass Box core.
