# Connector-side provider adapters

> **MIGRATION NOTE:** The direct in-process provider broker was retired from
> normal MGB execution in Milestone 7D.2. MGB no longer accepts provider
> credential files, resolves provider secrets, or performs provider HTTP
> transport. Historical direct grants remain readable and revocable, but they
> cannot be created through the operator surface or dispatched.

The `magic_glass_box.provider` module remains as a bounded protocol-adapter
library used by the proof/reference inference connector. Keeping that transport
code reusable does not move its authority back into the control plane: only the
separately launched connector imports credentials and invokes it. Production
connectors may implement the public inference protocol independently.

See [inference-connectors.md](inference-connectors.md) for the normal operator,
authority, carrier, and failure contracts.

The optional production-shaped OpenAI implementation lives in
`magic_glass_box.openai_chatgpt` and the
`magic-glass-box-openai-connector` entry point. It owns a dedicated Sign in
with ChatGPT profile and calls only the public streaming Responses endpoint.
It does not reuse this historical API-key adapter's credential loading or move
provider transport back into the control plane. Installation, sign-in, strict
tool/body policy, and the explicit live proof are documented in
[inference-connectors.md](inference-connectors.md#openai-connector-operator-flow).

## Reference-connector boundary

`scripts/reference_inference_connector.py` accepts a private connector-owned
configuration. Each route maps one public inference route and non-secret scope
fingerprint to:

- a connector-private credential file;
- a provider protocol adapter and upstream destination;
- an exact model and allowed static protocol headers; and
- request, response, timeout, and connector-local concurrency limits.

That configuration is not registered in MGB, mounted into a worker, or stored
in SQLite. The reference connector validates private credential files using
the same strict mode/ownership checks used elsewhere, then retains the secret
only in its own process. `examples/provider-policy.json` and
`examples/anthropic-provider-policy.json` are connector-internal adapter
examples; they are not MGB provider-grant documents.

MGB registers only the credential-free connector and inference grant:

```console
PYTHONPATH=src python3.11 -m magic_glass_box \
  inference-connector register openai-personal \
  --spec examples/inference-connector.json --created-by operator
PYTHONPATH=src python3.11 -m magic_glass_box \
  provider connector-add openai-main \
  --policy examples/inference-grant.json --created-by operator
```

The connector's private route must independently match the registered route,
provider, semantic protocol, exact model, scope fingerprint, and byte ceilings.
A mismatch fails inside the connector rather than widening authority.

## Protocol adapters

The reference library supplies three finite adapters:

- `json_http` sends one bounded JSON object and accepts one bounded JSON object;
- `openai_responses` accepts a model-free Responses request, requires
  streaming, and buffers one bounded SSE response; and
- `anthropic_messages` does the same for the Anthropic Messages dialect.

Vendor requests may carry only the small feature-negotiation allowlist needed
by the worker CLI: `openai-beta` and `x-codex-beta-features` for Responses, or
`anthropic-beta` and `anthropic-version` for Messages. Authorization,
destination, session, client-metadata, telemetry, and arbitrary worker headers
are discarded. Connector-private static headers are applied afterward.

The worker cannot select a destination, method, credential, connector endpoint,
account, or model. MGB strips a model nested in the provider body before any
third-party connector receives the request, then sends one strict authorized
inference envelope with the separately pinned model. The connector validates
that envelope against its private route before adapting the transient request.
Redirects are not followed. Provider output remains untrusted agent input and
request/response bodies are not made durable.

## Durable audit boundary

MGB stores the exact connector, deployment, route, policy digest, scope
fingerprint, provider, protocol, model, deadline, outcome, byte counts, status,
error category, and timestamps. It does not store the connector's private
adapter policy, destination, credential path or value, provider request body,
provider response body, or transcript.

The connector returns `succeeded`, `failed`, or `ambiguous` through the strict
v1 response. A lost or invalid post-dispatch reply is durably ambiguous and is
never replayed automatically. Connector-local adapter errors cannot grant new
authority or cause MGB to fall back to the historical direct path.

Run the deterministic isolation proof with a real Docker worker and a separate
fake connector:

```console
PYTHONPATH=src python3.11 scripts/check_live_provider_gateway.py
```

The proof requires direct egress, undeclared grants, worker-selected
destinations, and worker-selected models to fail while exactly one pinned
connector request succeeds. A connector-private canary credential must remain
absent from MGB state, the worker, mailbox traffic, runtime output, and logs.
