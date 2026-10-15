# Credential-isolated inference connectors

> **DESIGN ONLY — PLANNED FOR MILESTONE 7D:** The current scoped provider
> broker keeps credentials out of workers but resolves them inside the MGB
> process. Before the first real-account golden proof, credential-bearing AI
> transport moves behind the connector boundary described here.

An inference connector is a narrow API server that performs an already
authorized model invocation. It may run as a local sidecar or as an
operator-managed external integration. It is not a raw HTTP proxy, provider
CLI, model router, or general network escape hatch.

## Ownership boundary

Magic Glass Box owns:

- the immutable run-to-grant binding;
- the selected semantic protocol, exact model, and request/response ceilings;
- revocation, expiry, deadlines, concurrency reservations, and later budget
  reservations;
- authorization before dispatch and bounded metadata after completion; and
- the worker-facing `provider.invoke` capability.

The connector owns:

- upstream API keys or OAuth registration, access, and refresh material;
- authentication refresh and provider-specific network transport;
- provider protocol quirks that do not belong in the core; and
- a private credential store outside the MGB state directory.

The worker remains networkless and receives neither set of credentials. It
knows only its run-scoped MGB gateway and a non-secret grant name. MGB may use a
revocable transport capability to authenticate itself to a connector, but that
capability cannot be used directly with the upstream model provider.

A local connector process or container receives no project checkout, worker
workspace, Docker socket, MGB database, retained-result store, or Krab
credential. Its filesystem authority is limited to its own configuration and
credential state; project content crosses it only as the transient bounded
model request that the authorized provider would receive anyway.

```text
networkless worker
      |
      | run-scoped provider.invoke
      v
Magic Glass Box -- exact authorized invocation --> inference connector
      |                                                   |
      | grant, limits, reservation, audit                 | provider/OAuth
      | (no upstream credential)                          | credential
      v                                                   v
 durable control state                              model provider
```

## Bounded contract

A connector registration pins a protocol version, connector identity,
deployment identity, supported semantic protocol families, operator-approved
model mappings, credential-scope fingerprint, and transport limits. The
registration contains no upstream credential or path to one.

After MGB has authorized and reserved an invocation, it sends only a bounded
operation containing:

- a unique invocation ID and immutable grant/policy digest;
- the selected semantic protocol and exact configured model;
- the request byte stream, deadline, and response ceiling; and
- correlation needed for bounded usage and outcome metadata.

The worker cannot supply a connector endpoint, upstream destination, account,
credential, or model override. The connector does not expose arbitrary URLs,
headers, commands, filesystem paths, or a pass-through vendor client. Its
response is limited to the provider stream plus normalized status, byte counts,
usage metadata when available, and the identities needed to bind the result to
the invocation. Request and response content remains transient.

Local deployments should prefer an operator-owned Unix socket. Authenticated
loopback or external transports may be supported, but their endpoint and
identity are operator configuration rather than worker input. An external
transport credential, if required, is a revocable connector capability—not an
upstream provider credential.

## OAuth and subscription-backed runtimes

Interactive sign-in occurs in the connector. Access and refresh tokens remain
in its private store and are never mounted into a worker, copied into `.mgb`,
or returned by the connector API. MGB records only the connector identity,
credential-scope fingerprint, lifecycle state, and bounded invocation
metadata.

OpenAI's supported Sign in with ChatGPT flow is compatible with this boundary:
an authorized application may send its OAuth access token directly to the
public streaming Responses API. The connector therefore owns its own
registration, sign-in, refresh serialization, and protected token store, then
forwards the already-authorized `openai-responses` request. It does not reuse or
parse another Codex installation's private authentication files. The
open-source dynamic-registration flow requires neither a client secret nor a
partner API key; its issued client ID and tokens still belong exclusively to
the connector. See
[Registration and sign-in](https://developers.openai.com/siwc/token-sharing-open-source/sign-in),
[Models and inference](https://developers.openai.com/siwc/token-sharing-open-source/models-and-inference)
and
[Accounts and sessions](https://developers.openai.com/siwc/token-sharing-open-source/profiles-and-sessions).

A local connector may receive an explicitly approved, dedicated
provider-native configuration directory and run the provider's supported CLI
or service process against it. Only the connector gets that mount. MGB neither
parses the provider's private credential format nor treats another tool's
incidental on-disk layout as its own API.

For the first Codex golden proof, quota consumption is treated like one bounded
subagent invocation because it uses the operator's existing ChatGPT-plan pool:
one tiny disposable task, one attempt, a hard wall-clock limit, and no automatic
retry. This is a quota guard, not a claim that MGB can enforce a dollar budget;
that authority arrives in Milestone 8A. The proof uses MGB's own connector
registration and OAuth session; it does not borrow the interactive Codex CLI's
credential record.

## Failure and retry semantics

An invocation whose response is lost may already have consumed quota. The core
therefore records an ambiguous outcome and does not automatically replay it.
Connector operation IDs provide correlation, not a false promise that every
provider deduplicates inference. Milestone 8 adds normalized usage accounting,
budget authority, and quota-aware recovery on top of this boundary.

## Migration and proof

Milestone 7D replaces provider-grant credential references with connector
bindings while preserving the existing worker-facing capability and the
run/model/limit/concurrency checks already proven by Milestone 2. Krab and
other non-inference credential paths are not silently migrated into this
contract.

The deterministic exit proof uses a fake connector with a canary credential
and verifies that wrong connector identities, protocols, models, grant digests,
and limits fail closed. It also verifies that the canary never enters the
worker container, MGB database, retained results, runtime output, or logs. Only
after that proof passes does the opt-in real Codex connector exercise an actual
subscription-backed invocation. M7D begins with a compatibility probe for the
provider-supported CLI or service flow. If that route cannot serve the bounded
connector contract, the proof stops; credentials are not copied into the core
or worker as a fallback.
