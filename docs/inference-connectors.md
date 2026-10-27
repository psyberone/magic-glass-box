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
>
> **M8A IMPLEMENTED:** Schema v19 adds protocol-v2 budget authority. MGB
> reserves an operator-priced invocation allowance atomically against pinned
> run, project, and grant ceilings, retains only normalized token counters and
> integer USD-micro accounting, and exposes reconciled totals in the control
> room. Prompt, response, and transcript content remain outside the ledger.
>
> **M8B IMPLEMENTED:** Schema v20 records explicit quota, rate-limit, and
> overload refusals as durable provider recoveries. The same run enters
> `paused`, its disposable worker is stopped through the lease heartbeat, and
> the supervisor requeues it only after the bounded eligibility time, active
> attempt cleanup, and any open human interaction are all resolved.

An inference connector is a narrow API server that performs an already
authorized model invocation. The implemented v1/v2 carrier is a local
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
  --spec examples/inference-connector-v2.json --created-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  provider connector-add openai-main \
  --policy examples/inference-grant-budgeted.json --created-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  inference-connector list
PYTHONPATH=src python3.11 -m magic_glass_box \
  inference-connector show openai-personal
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
protocol-v1 proof/reference implementation for API-key-backed adapters, not a
built-in credential service or an automatically managed daemon. It rejects v2
before provider dispatch because the generic adapter does not normalize vendor
usage. The built-in OpenAI connector and deterministic fake implement v2.

Its private configuration is intentionally distinct from the public
registration document. It binds the assigned connector identity and deployment
digest before listing credential-owning routes:

```json
{
  "connector_id": "ifc_<32 lowercase hex characters>",
  "deployment_sha256": "<64 lowercase hex characters>",
  "routes": [
    {
      "name": "openai-main",
      "credential_scope_sha256": "<64 lowercase hex characters>",
      "policy": {
        "provider": "openai",
        "adapter": "openai_responses",
        "destination": "https://api.openai.com/v1/responses",
        "method": "POST",
        "model": "gpt-example",
        "credential_header": "Authorization",
        "credential_prefix": "Bearer ",
        "static_headers": {},
        "request_bytes": 32768,
        "response_bytes": 65536,
        "timeout_ms": 30000,
        "max_concurrency": 1
      },
      "credential_file": "/absolute/private/provider-token"
    }
  ]
}
```

Obtain `connector_id` and the pinned deployment digest from
`inference-connector show`. The config and credential files remain private
sidecar state and are never passed to MGB or a worker.

The scope fingerprint identifies an operator-defined non-secret account and
permission scope. It must be derived from canonical non-secret metadata or an
opaque random scope ID, never by hashing an API key, access token, refresh
token, or other bearer material.

The public Draft 2020-12 contracts live in `schemas/` and have matching examples
in `examples/`: connector registration, connector-bound grant, the legacy v1
invocation request/response, and the budgeted v2 invocation request/response.
The carrier uses one four-byte big-endian length followed by strict finite
UTF-8 JSON. Every response repeats the complete correlation tuple, including
the exact v2 budget authority. A failure before connection is unavailable; any
malformed, late, mismatched, or lost reply after dispatch is ambiguous and is
never retried automatically.

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
manual stale-path cleanup. Immediately before connecting, MGB requires the
configured path itself—not a symlink—to be a Unix socket owned by the current
operator. This is a boundary admission check, not continuous surveillance of a
machine already controlled by the same user.

## Ownership boundary

Magic Glass Box owns:

- the immutable run-to-grant binding;
- the selected semantic protocol, exact model, and request/response ceilings;
- revocation, expiry, deadlines, concurrency reservations, versioned budget
  policy, and atomic allowance reservations;
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
- correlation needed for bounded byte and outcome metadata;
- for v2, the budget-policy digest, per-call allowance, output-token ceiling,
  and integer token rates needed to normalize the result independently.

The worker cannot supply a connector endpoint, upstream destination, account,
credential, or model override. MGB removes a nested provider-body model before
dispatch and supplies the separately pinned model in the authority envelope;
connectors still reject a body model as defense in depth. The connector does
not expose arbitrary URLs, headers, commands, filesystem paths, or a
pass-through vendor client. Its response is limited to the provider stream
plus normalized status, byte counts, and the identities needed to bind the
result to the invocation. Request and response content remains transient.
An explicitly failed response may additionally carry only an integer
`retry_after_seconds` from 1 second through 30 days. The field is invalid on a
successful, denied, busy, or ambiguous response; it is timing evidence, not
permission for the connector or worker to replay a call.
V2 adds only `input_tokens`, `cached_input_tokens`, `output_tokens`, and
`reasoning_output_tokens`; the subset counters are not charged twice. V1
remains available for legacy unmetered connectors and carries no MGB-enforced
spend authority.

## Budget and accounting authority

A v2 grant embeds one immutable budget policy. `account` is the stable logical
spend pool, `revision` is its operator-visible policy revision, and the policy
digest binds every rate and ceiling. All monetary values are non-floating-point
integer micro-USD. They are operator-pinned estimates, not a claim that MGB can
read a provider invoice or subscription balance.

The policy supplies:

- one allowance reserved for every invocation;
- a maximum output-token count and input/cached-input/output rates per million
  tokens;
- lifetime ceilings for that account within the run, immutable grant, and
  project ledger.

Changing a policy means creating a new immutable grant. Project and run
consumption is aggregated by stable account across revisions, so changing a
revision does not erase prior spend. A deliberately new account starts a new
budget epoch and is therefore an explicit operator authority change.

Before a v2 grant can be registered, its invocation allowance must cover a
conservative reservation estimate: four input tokens for every permitted
worker-request byte, plus the maximum output tokens at the pinned rates. This
is an admission estimate, not a physical upper bound on provider-reported input
usage; providers may add their own instructions and tool formatting. The v2
request repeats that authority. The OpenAI connector injects
`max_output_tokens` after validating the worker body. A worker may request a
smaller value but cannot remove or raise the provider-side bound. Connector-
owned model and output-limit fields have a separate 512-byte expansion
allowance and do not consume the worker's request-byte authority. Other v2
connectors must enforce an equivalent output bound for their provider. A
protocol-v1 request has no MGB budget ceiling to raise; when it supplies its own
valid `max_output_tokens`, the OpenAI connector forwards that caller cap.
It also has no protocol-neutral per-run invocation-count ceiling today. Its
effective bounds are the run lifecycle, connector concurrency/deadline,
provider quota/rate limits, and any connector-specific output cap. Autonomous
profiles should therefore use budgeted protocol v2. Milestone 13.6 will either
add an exact run-to-grant invocation ceiling for v1 or refuse v1 authority to
autonomous profiles before dispatch.

Immediately before dispatch, one SQLite `BEGIN IMMEDIATE` transaction totals
existing consumption and active reservations for all three scopes. It either
records the new `dispatching` row and full allowance or denies the call before
the sidecar sees it. Concurrent control-plane processes therefore cannot each
observe the same remaining allowance and collectively overspend it.

A verified successful provider usage report replaces the reservation with its
rounded-up estimate and releases the remainder. If actual paid usage exceeds
the estimate reserved before dispatch, MGB retains and accounts the larger
reported amount; subsequent calls are denied wherever the remaining ceiling is
smaller than the per-invocation allowance. A known pre-connector failure
releases the allowance.
Non-success replies may not attach partial usage. Missing, malformed, or lost
post-dispatch accounting consumes the full reservation conservatively. The
control room separates reported estimate, finalized accounting, active
reservation, total policy consumption, and remaining run/grant/project
allowance. `reconciled` is false while a reservation is active or any
invocation remains conservatively accounted.

Local deployments use an operator-owned Unix socket. A future authenticated
loopback or external transport must preserve the same client contract; its
endpoint and identity remain operator configuration rather than worker input.
An external transport credential, if required, is a revocable connector
capability—not an upstream provider credential.

For the local transport, the mode-`0700` operator-owned socket directory and
mode-`0600` socket are the connector's client-authentication boundary. The
socket is never mounted into a worker. Grant lifecycle, run authority, atomic
reservation, and policy-digest selection remain MGB responsibilities; the
credential sidecar deliberately has no MGB database or duplicate grant store
from which it could independently re-authorize a grant ID. It validates the
pinned route, deployment, provider protocol, model, credential scope, byte and
time ceilings, and v2 budget envelope as defense in depth. Grant IDs and
digests are correlation fields, not bearer credentials. A hostile process
already running as the operator could also read connector-owned profile state
and is outside MGB's local-host threat boundary. Any future loopback or remote
carrier must replace the private-socket assumption with an explicit revocable
client capability.

The control room computes run, grant, and project consumption with grouped SQL
aggregates; it does not materialize the lifetime invocation ledger in Python.
Run-detail projections return at most the newest 1,000 invocation records in
chronological order, while the complete immutable ledger and all-scope totals
remain durable in SQLite.

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
deliberate opening on that same host. `--callback-timeout` bounds the complete
wait, idle TCP clients are dropped, and only a callback carrying the expected
OAuth state can complete the attempt. The profile directory must be owned by
the current user with mode `0700`; its host, registration, session, and refresh
lock records are private. `status` returns account metadata and the non-secret
credential-scope fingerprint but never a token. `models` obtains the current
account-visible catalog with the connector-owned access token.

Serving requires a private connector configuration that exactly matches the
credential-free MGB registration: connector ID, deployment digest, route,
scope fingerprint, model set, byte ceilings, and timeout. The service binds a
mode-`0600` Unix socket in an operator-owned mode-`0700` directory. It accepts
the strict framed v1 and v2 invocation contracts; it does not expose OAuth,
arbitrary URLs, raw HTTP, or a provider CLI over that socket. V2 completed
Responses streams must contain structurally valid usage. The connector reduces
that provider object to the four normalized counters and fails closed if usage
is absent or exceeds the pinned output bound. Provider-reported input overhead
and monetary reservation overruns remain successful, honestly accounted paid
work rather than becoming discarded ambiguous responses.

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
stops at an issued landing plan without executing it. The harness pins protocol
v2 to a $1 estimated maximum per call and a $5 run/project/grant ceiling;
`--max-invocations` bounds the agent loop independently. An account-side app
usage limit remains the final provider-owned backstop because MGB's integer
rates are a local estimate, not the provider's billing ledger.

A local connector may receive an explicitly approved, dedicated
provider-native configuration directory and run the provider's supported CLI
or service process against it. Only the connector gets that mount. MGB neither
parses the provider's private credential format nor treats another tool's
incidental on-disk layout as its own API.

For the first Codex golden proof, quota consumption was treated like one bounded
subagent invocation because it used the operator's existing ChatGPT-plan pool:
one tiny disposable task, one attempt, a hard wall-clock limit, and no automatic
retry. M8A adds a separately pinned local dollar estimate and hard dispatch
ceiling; it still does not claim access to the subscription's provider-side
quota or invoice. The proof uses MGB's own connector registration and OAuth
session; it does not borrow the interactive Codex CLI's credential record.

## Failure and retry semantics

An invocation whose response is lost may already have consumed quota. Before
crossing the connector boundary, the core durably records a `dispatching`
invocation. It finalizes that record exactly once after a verified response.
A stranded `dispatching` record is conservative evidence of an uncertain call;
the core does not automatically replay it. For v2 it also remains a full active
allowance until finalized.
A failure before connection is recorded as unavailable/failed; a malformed,
late, mismatched, or lost reply after dispatch is ambiguous. Connector
operation IDs provide correlation, not a false promise that every provider
deduplicates inference. The worker's loopback provider bridge is permanently
poisoned after an explicit ambiguous result, a committed mailbox timeout, or a
malformed post-dispatch reply, so even a vendor CLI retry fails locally without
another gateway or provider dispatch. The bounded dispatch reservation is not
released until terminal ledger finalization succeeds. V2 reported usage
reconciles that allowance; ambiguous or missing accounting consumes it in full.
An explicit connector failure is recoverable only when its bounded error code
or status identifies quota exhaustion, rate limiting, or provider overload.
Quota markers default to a 15-minute delay; rate-limit and overload markers,
HTTP 429, and HTTP 529 default to 60 seconds. Connector-supplied
`retry_after_seconds` overrides that default after being capped at 30 days;
the OpenAI connector derives it from a numeric upstream `Retry-After` header.
Ordinary 5xx failures remain permanent, and an ambiguous outcome is never
converted into recovery merely because retry timing was present.

Both the OpenAI HTTPS client and Krab client apply one monotonic deadline across
connection, request transmission, response headers, and bounded body reads.
Progress on a slow-dripping socket does not reset that total call budget.

Invocation finalization and creation of the provider-recovery record happen in
one SQLite transaction. The run moves from `running` or the short
`waiting_for_input` continuation window to `paused`; generic run transitions
cannot forge or bypass that state. Lease renewal tells the owning coordinator
to stop the disposable container, and completion or failure closes that
attempt as `interrupted` without failing the run. The worker-facing HTTP bridge
returns a redacted 429 with the bounded `Retry-After`, while the run-scoped
gateway prevents another provider dispatch once the pause is durable.

The supervisor never wakes a paused run while an attempt is still active or a
human interaction remains open. An operator may answer that interaction while
the provider is paused; the answer stays durable and the next eligible pass
requeues the same run. `auto_execute` still controls whether that queued run is
launched. A manual run is requeued but not started. Cancellation marks the
recovery cancelled, and no later timer can resurrect it. Recovery reuses the
same run, source revision, connector-bound grant, and remaining budget ledger;
it does not create a successor or reset consumption.

## Migration and proof

M7D.1 added connector bindings beside historical direct grants while
preserving the worker-facing capability contract. M7D.2 makes connector
dispatch exclusive: connector grants may enter new run authority, while
historical direct records remain readable and revocable but cannot be created
through the CLI or dispatched. No connector is inferred from a legacy
credential reference. Krab and other non-inference credential paths are not
silently migrated into this contract.

The deterministic exit proof now passes with a fake connector and canary
credential. Wrong connector identities, protocols, models, response
correlation, and limits fail closed; MGB alone selects the pinned grant and
policy digests before entering the private transport. The canary stays absent
from the worker container, MGB state and SQLite, retained results, runtime
output, and connector logs. The
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
