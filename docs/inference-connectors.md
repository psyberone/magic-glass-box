# Credential-isolated inference connectors

> **M7D.2 IMPLEMENTED; OPTIONAL REAL-ACCOUNT PROOF PLANNED FOR M7D.3:** Schema
> v16 makes connector-bound grants the exclusive normal inference path. MGB
> authorizes and audits each bounded call but neither resolves an upstream
> provider credential nor performs provider network transport. Historical
> direct grants remain readable and revocable for migration only.

An inference connector is a narrow API server that performs an already
authorized model invocation. The implemented v1 carrier is a local
operator-owned Unix socket suitable for a sidecar. The client seam and strict
envelopes are transport-neutral so an operator-managed external integration can
be added without changing worker authority, but MGB does not yet ship a native
authenticated remote carrier. It is not a raw HTTP proxy, provider CLI, model
router, or general network escape hatch.

## Current operator surface

An operator registers a credential-free connector description, then binds a
provider grant to one exact connector deployment, route, scope fingerprint,
semantic protocol, model, and set of limits:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  inference-connector register openai-personal \
  --spec examples/inference-connector.json --created-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  provider connector-add openai-main \
  --policy examples/inference-grant.json --created-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  inference-connector list
```

There is no credential-file argument on either authority path. Connector
rotation creates a new logical registration and grant; it never silently
rebinds an existing grant. Multiple connector instances may use the same
pinned deployment digest while retaining distinct socket paths, route sets,
and credential-scope fingerprints.

Connector lifecycle is deliberately external to MGB. The operator starts a
local sidecar or configures an external integration at the registered endpoint;
only that process receives provider credentials or a provider-native
configuration directory. `scripts/reference_inference_connector.py` is a
proof/reference implementation for API-key-backed protocol adapters, not a
built-in credential service or an automatically managed daemon.

The scope fingerprint identifies an operator-defined non-secret account and
permission scope. It must be derived from canonical non-secret metadata or an
opaque random scope ID, never by hashing an API key, access token, refresh
token, or other bearer material.

The four public Draft 2020-12 contracts live in `schemas/` and have matching
examples in `examples/`: connector registration, connector-bound grant,
invocation request, and invocation response. The carrier uses one four-byte
big-endian length followed by strict finite UTF-8 JSON. Every response repeats
the complete correlation tuple. A failure before connection is unavailable;
any malformed, late, mismatched, or lost reply after dispatch is ambiguous and
is never retried automatically.

The ordinary test suite launches `scripts/fake_inference_connector.py` as a
separate process and completes a deterministic bounded invocation over a real
Unix socket. The D.2 Docker proof adds a connector-private canary credential,
then verifies exclusive connector dispatch, networkless-worker containment,
durable ambiguous outcomes, and removal of direct credential resolution from
normal MGB execution.

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
- correlation needed for bounded byte and outcome metadata.

The worker cannot supply a connector endpoint, upstream destination, account,
credential, or model override. The connector does not expose arbitrary URLs,
headers, commands, filesystem paths, or a pass-through vendor client. Its
response is limited to the provider stream plus normalized status, byte counts,
and the identities needed to bind the result to the invocation. Request and
response content remains transient. Normalized usage metadata is added under
Milestone 8's accounting authority rather than being implied by v1.

Local deployments use an operator-owned Unix socket. A future authenticated
loopback or external transport must preserve the same client contract; its
endpoint and identity remain operator configuration rather than worker input.
An external transport credential, if required, is a revocable connector
capability—not an upstream provider credential.

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
therefore records an `ambiguous` outcome and does not automatically replay it.
A failure before connection is recorded as unavailable/failed; a malformed,
late, mismatched, or lost reply after dispatch is ambiguous. Connector
operation IDs provide correlation, not a false promise that every provider
deduplicates inference. The worker's loopback provider bridge is permanently
poisoned after an ambiguous result, so even a vendor CLI retry fails locally
without another gateway or provider dispatch. Milestone 8 adds normalized
usage accounting, budget authority, and quota-aware recovery on top of this
boundary.

## Migration and proof

M7D.1 added connector bindings beside historical direct grants while
preserving the worker-facing capability contract. M7D.2 makes connector
dispatch exclusive: connector grants may enter new run authority, while
historical direct records remain readable and revocable but cannot be created
through the CLI or dispatched. No connector is inferred from a legacy
credential reference. Krab and other non-inference credential paths are not
silently migrated into this contract.

The deterministic exit proof now passes with a fake connector and canary
credential. Wrong connector identities, protocols, models, grant digests, and
limits fail closed; the canary stays absent from the worker container, MGB
state and SQLite, retained results, runtime output, and connector logs. The
separate two-runtime Docker proof also drives the real Codex and Claude Code
CLIs through a reference connector to a host-only deterministic upstream.

M7D.3 is the explicitly opted-in real-account proof. It begins with a
compatibility probe for the provider-supported OAuth/service flow and performs
one bounded invocation with no automatic retry. If that route cannot serve the
connector contract, the proof stops; credentials are not copied into the core
or worker as a fallback.
