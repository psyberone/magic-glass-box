# Credential-isolated inference connectors

> **M7D.3 COMPLETE; REAL-ACCOUNT GOLDEN PROOF PASSED 2026-10-16:** Schema v17
> makes connector-bound grants the exclusive normal inference path. The
> optional OpenAI connector owns its Sign in with ChatGPT registration,
> renewable session, model discovery, and Responses transport. MGB authorizes
> and audits each bounded call but neither resolves an upstream credential nor
> performs provider network transport. Browser consent and model discovery
> succeeded. The first one-attempt live run exposed a provider-compatibility
> and proof-diagnostics defect; both received RED-first fixes. A separately
> authorized second attempt then passed the complete result, candidate, gate,
> and manual-landing path without exposing credentials or worker networking.

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

The request and response byte ceilings count the canonical provider request
and raw provider response, respectively—not JSON framing added by the local
carrier. The response frame remains independently bounded to the worst-case
six-byte JSON escape for each raw byte plus finite correlation overhead. A
connector must clamp failure counters to the pinned raw-byte ceilings.

The ordinary test suite launches `scripts/fake_inference_connector.py` as a
separate process and completes a deterministic bounded invocation over a real
Unix socket. The D.2 Docker proof adds a connector-private canary credential,
then verifies exclusive connector dispatch, networkless-worker containment,
durable ambiguous outcomes, and removal of direct credential resolution from
normal MGB execution.

The optional ChatGPT connector, API-key reference connector, and deterministic
proof connector share one local listener policy. The socket lives in an
operator-owned mode-`0700` directory and uses mode `0600`; each accepted client
has a 30-second I/O timeout. Truncated, malformed, oversized, abandoned, or
broken-pipe clients are isolated to that connection and do not consume a
successful invocation slot. A controlled `SIGTERM` unwinds the listener and
removes its socket before exit, so an operator can restart the sidecar without
manual stale-path cleanup.

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
credential, or model override. MGB removes a nested provider-body model before
dispatch and supplies the separately pinned model in the authority envelope;
connectors still reject a body model as defense in depth. The connector does
not expose arbitrary URLs, headers, commands, filesystem paths, or a
pass-through vendor client. Its response is limited to the provider stream
plus normalized status, byte counts, and the identities needed to bind the
result to the invocation. Request and response content remains transient.
Normalized usage metadata is added under Milestone 8's accounting authority
rather than being implied by v1.

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

### OpenAI connector operator flow

Install the optional connector dependency, then create a dedicated profile
outside the source checkout and the MGB state directory:

```console
python3.11 -m pip install '.[openai-connector]'
PYTHONPATH=src python3.11 scripts/openai_chatgpt_connector.py \
  login --profile-dir /absolute/private/openai-chatgpt-profile
PYTHONPATH=src python3.11 scripts/openai_chatgpt_connector.py \
  status --profile-dir /absolute/private/openai-chatgpt-profile
PYTHONPATH=src python3.11 scripts/openai_chatgpt_connector.py \
  models --profile-dir /absolute/private/openai-chatgpt-profile
```

`login` opens the system browser and listens only on an ephemeral
`127.0.0.1` callback. `--no-browser` prints the one-use authorization URL for
deliberate opening on that same host. The profile directory must be owned by
the current user with mode `0700`; its host, registration, session, and refresh
lock records are private. `status` returns account metadata and the non-secret
credential-scope fingerprint but never a token. `models` obtains the current
account-visible catalog with the connector-owned access token.

Serving requires a private connector configuration that exactly matches the
credential-free MGB registration: connector ID, deployment digest, route,
scope fingerprint, model set, byte ceilings, and timeout. The service binds a
mode-`0600` Unix socket in an operator-owned mode-`0700` directory. It accepts
the same strict framed v1 invocation contract as the reference connector; it
does not expose OAuth, arbitrary URLs, raw HTTP, or a provider CLI over that
socket.

The OpenAI route additionally enforces the current Sign in with ChatGPT
requirements before any HTTPS dispatch: `store` is false, `stream` is true,
the model is connector-pinned, unsupported or unknown top-level fields fail
closed, and only Codex's local `function` and `namespace` tool declarations are
accepted. Input is limited to text messages, reasoning continuity, declared
local function calls, and text-only outputs linked to an earlier call ID.
`tool_choice` may select only those declared local tools, and `include` may
request only encrypted reasoning continuity. MGB starts Codex with
`web_search="disabled"`; hosted web search, MCP, file/image inputs, file search,
code interpreter, computer use, image generation, unknown future item/tool
types, and hosted tools nested inside a namespace are rejected by the
connector. This prevents the inference sidecar from becoming an undeclared
general egress path. The provider-specific restrictions track OpenAI's
documented
[preview limitations](https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations),
with a deliberately narrower local-tool policy for MGB workers.

An OAuth refresh response may rotate only the access or refresh token. When it
omits `id_token`, the connector preserves the identity claims and ID token that
were validated at interactive sign-in instead of revalidating the now-expired
identity token. A newly returned ID token is always validated before the
session is replaced.

For a disposable real-account proof after sign-in, first select a slug from
`models`, then run exactly one explicitly opted-in attempt:

```console
PYTHONPATH=src python3.11 scripts/check_live_openai_chatgpt.py \
  --profile-dir /absolute/private/openai-chatgpt-profile \
  --model MODEL_SLUG --build --max-invocations 6 --run-timeout 300 \
  --i-understand-this-uses-live-quota
```

The proof refuses to start without the opt-in flag. It builds the pinned
runtime, allows one run attempt with no automatic provider replay, requires
both the worker and exact-input gate containers to use `--network none`, and
stops at an issued landing plan without executing it. `--max-invocations`
bounds the agent loop but is not a dollar budget. Until Milestone 8A, configure
the app's usage limit in ChatGPT settings as the spend/quota backstop.

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

An invocation whose response is lost may already have consumed quota. Before
crossing the connector boundary, the core durably records a `dispatching`
invocation. It finalizes that record exactly once after a verified response.
A stranded `dispatching` record is conservative evidence of an uncertain call;
the core does not automatically replay it.
A failure before connection is recorded as unavailable/failed; a malformed,
late, mismatched, or lost reply after dispatch is ambiguous. Connector
operation IDs provide correlation, not a false promise that every provider
deduplicates inference. The worker's loopback provider bridge is permanently
poisoned after an explicit ambiguous result, a committed mailbox timeout, or a
malformed post-dispatch reply, so even a vendor CLI retry fails locally without
another gateway or provider dispatch. The bounded dispatch reservation is not
released until terminal ledger finalization succeeds. Milestone 8 adds
normalized usage accounting, budget authority, and quota-aware recovery on top
of this boundary.

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

M7D.3 implements the provider-supported OAuth and Responses flow in the
separate OpenAI connector. Browser consent and account-visible model discovery
now pass. The first bounded live run launched exactly one networkless worker
but failed before producing a result. Comparison with
[OpenAI's official DevKit](https://github.com/openai/sign-in-with-chatgpt-devkit/blob/main/packages/local/src/responses.ts)
identified that successful streaming responses may omit `Content-Type`, while
the initial adapter required it; the adapter now validates such responses by
their complete SSE terminal event and still rejects an explicitly wrong
content type. Structured top-level SSE errors also retain their
machine-readable code. The live harness now performs its credential-leak scan
on failure and reports only bounded allowlisted audit metadata. Following a
fresh explicit opt-in, one additional bounded attempt passed using five of six
allowed connector invocations. It retained `golden-proof.txt`, created a
candidate, passed the exact validation gate, and issued a landing plan without
executing its commands. Direct worker networking remained denied and reusable
credentials remained confined to the connector profile. No automatic retry
occurred, and credentials are not copied into the core or worker as a fallback.
