# Scoped provider broker

The provider broker lets a networkless worker call one administrator-defined
AI endpoint without receiving a reusable credential or choosing where the
request goes. A provider grant is a named, immutable policy. An agent profile
names that grant and opts into `provider.invoke`; each run then pins the exact
grant ID alongside its other immutable inputs.

## Configure a grant

The policy document fixes the adapter, destination, HTTP method, model,
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
limits, accepts only finite JSON object responses, and does not follow
redirects. HTTPS is mandatory except for explicit loopback destinations used
by local adapters and integration tests.

The transient response includes the grant name, provider, pinned model, HTTP
status, byte counts, invocation ID, and an `untrusted_response` object. Provider
output remains untrusted agent input.

## Durable audit boundary

SQLite stores immutable grant creation/revocation events, run-to-grant pins,
and invocation metadata: outcome, HTTP status, byte counts, error category,
and timestamps. It does not store credential values, provider request bodies,
provider response bodies, or conversation transcripts. The committed-file
mailbox is ephemeral and removed during normal cleanup or reconciliation.

Run the live isolation proof with a real Docker worker and a local HTTP
provider:

```console
PYTHONPATH=src python3.11 scripts/check_live_provider_gateway.py
```

The proof requires direct egress, undeclared grants, worker-selected
destinations, and worker-selected models to fail while exactly one pinned
provider request succeeds and produces a metadata-only audit record.
