# Scoped provider broker

> **CURRENT IMPLEMENTATION AND MIGRATION NOTE:** This broker already keeps
> reusable credentials out of workers, but the MGB process currently resolves
> the credential and performs provider HTTP transport. M7D.1 now provides
> schema-v15 connector authority and a strict Unix-socket carrier; those new
> grants remain non-dispatchable until M7D.2 moves normal execution across the
> boundary and retires direct credential resolution. See
> [inference-connectors.md](inference-connectors.md).

The provider broker lets a networkless worker call one administrator-defined
AI endpoint without receiving a reusable credential or choosing where the
request goes. A provider grant is a named, immutable policy. An agent profile
names that grant and opts into `provider.invoke`; each run then pins the exact
grant ID alongside its other immutable inputs.

## Configure a grant

The policy document fixes the protocol adapter, destination, HTTP method, model,
credential placement, static headers, request and response ceilings, timeout,
and concurrency limit. It contains no secret:

```console
chmod 0700 ./private-credentials
chmod 0600 ./private-credentials/openai.key
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  provider add openai-main \
  --policy examples/provider-policy.json \
  --credential-file ./private-credentials/openai.key \
  --created-by operator
```

`examples/provider-policy.json` is the Responses form;
`examples/anthropic-provider-policy.json` shows the Messages form and pins the
required Anthropic API version as a static header.

The credential file must be owned by the current user, live in a mode-0700
directory, be a mode-0600 regular file with one link, and contain one non-empty
printable ASCII secret. Magic Glass Box validates it during registration but
persists only its `file:` reference. It resolves the value again only while
performing an authorized invocation.

`provider show openai-main` returns policy and lifecycle metadata but omits the
credential reference. A policy cannot be edited in place. Revoke it with an
optimistic version check:

```console
PYTHONPATH=src python3.11 -m magic_glass_box --db ./control-plane.db \
  provider revoke openai-main --expected-version 1 --revoked-by operator
```

Revoked and expired grants fail closed for both new runs and existing pinned
runs. Replacing policy or credentials requires a new named grant, making the
change explicit in later profile and run revisions.

An agent profile may name an authority only with `gateway_only` networking.
`krab` requires an interaction capability, and every other named authority
requires `provider.invoke`. Profiles that cannot exercise a named authority are
rejected at creation rather than failing after a worker starts. No executor mode
mounts a reusable credential file into a worker.

## Invocation boundary

The worker may submit only:

```json
{
  "version": 1,
  "request_id": "agent-generated-stable-id",
  "operation": "provider.invoke",
  "payload": {
    "grant": "openai-main",
    "request": {"input": "untrusted provider input"}
  }
}
```

The named grant must appear in the pinned profile. The worker cannot include a
destination, method, credential, or top-level model override. The trusted
broker injects the pinned model and credential, enforces byte and concurrency
limits, and does not follow redirects. HTTPS is mandatory except for explicit
loopback destinations used by local adapters and integration tests.

Credential resolution happens before a short SQLite transaction atomically
rechecks the live run attempt, grant pin, revocation, and expiry and reserves a
grant concurrency slot. That committed reservation is the dispatch boundary:
revocation committed first prevents the call; revocation committed afterward
does not cancel a request already in flight. Reservations enforce the grant's
limit across control-plane processes, are released after dispatch, and expire
after a dead process stops renewing its short lease so crashed dispatchers
cannot strand capacity. A live dispatcher renews the reservation for the full
adapter call, including a slow bounded response, and fails the invocation if it
cannot maintain that authority. The broker retains a process-local semaphore as
defense in depth, not as the cross-process authority.

Three adapters are available:

- `json_http` sends one finite JSON object and accepts one finite JSON object;
- `openai_responses` accepts a model-free Responses request, requires
  streaming, and buffers one bounded SSE response; and
- `anthropic_messages` does the same for the Anthropic Messages dialect.

Vendor requests may carry only protocol feature negotiation that the CLI needs:
`openai-beta` and `x-codex-beta-features` for Responses, or `anthropic-beta`
and `anthropic-version` for Messages. All authorization, destination, session,
client-metadata, telemetry, and arbitrary headers from the worker are dropped.
Policy `static_headers` are applied afterward and therefore pin or override an
allowed value when required. The request and response ceilings are at most 2
MiB and 4 MiB respectively; a policy may choose smaller values.

The transient response includes the grant name, provider, pinned model, HTTP
status, byte counts, invocation ID, and an `untrusted_response` object. A vendor
adapter's object contains only its protocol, content type, and buffered SSE
text so the worker bridge can replay it. Provider output remains untrusted
agent input.

## Durable audit boundary

SQLite stores immutable grant creation/revocation events, run-to-grant pins,
short-lived dispatch reservations, and invocation metadata: outcome, HTTP
status, byte counts, error category, and timestamps. It does not store
credential values, provider request bodies, provider response bodies, or
conversation transcripts. The committed-file mailbox is ephemeral and removed
during normal cleanup or reconciliation.

Run the live isolation proof with a real Docker worker and a local HTTP
provider:

```console
PYTHONPATH=src python3.11 scripts/check_live_provider_gateway.py
```

The proof requires direct egress, undeclared grants, worker-selected
destinations, and worker-selected models to fail while exactly one pinned
provider request succeeds and produces a metadata-only audit record.
