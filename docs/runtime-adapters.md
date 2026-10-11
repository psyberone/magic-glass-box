# Runtime adapters

Runtime adapters translate Magic Glass Box's durable run semantics into one
headless agent CLI invocation. They are deliberately separate from the Docker
executor: Docker owns isolation and lifecycle, while an adapter owns only its
runtime's argv, non-secret environment, bootstrap prompt, and transient
completion format.

## Profile and attempt identity

An agent profile may name a default adapter:

```json
{
  "image": "magic-glass-box/codex-agent@sha256:<digest>",
  "command": ["codex"],
  "runtime_adapter": "codex",
  "runtime_provider_grant": "openai-main",
  "workspace_mode": "copy_on_write",
  "network_mode": "gateway_only",
  "credential_names": ["krab", "openai-main"],
  "gateway_capabilities": ["run.describe", "provider.invoke"]
}
```

`command` is an executable prefix supplied by the immutable image contract.
The `codex` and `claude-code` adapters require exactly one executable; their
flags and prompts do not live in the profile. The compatibility `command`
adapter executes the complete argv unchanged. Profiles created before this
contract omit `runtime_adapter`, retain their original content digest, and use
`command`.

The bundled vendor adapters also require `runtime_provider_grant`. It must name
one entry in `credential_names`, and the profile must authorize both
`run.describe` and `provider.invoke`. This is a route selection, not a secret:
the named grant pins the actual endpoint, protocol adapter, model, credential,
and limits on the trusted host. Profiles without the field retain their old
digest. A `command` profile may set it so an explicit per-attempt vendor
adapter override has the same deterministic route.

`run execute --runtime-adapter NAME` may select a different compatible adapter
for one attempt. The immutable profile, context, work item, source revision,
and authorities do not change. Every attempt records the adapter actually
used, so replacement is visible without pretending the worker runtime is
durable state.

## Python protocol

The public protocol is `magic_glass_box.runtime.RuntimeAdapter`:

```python
class RuntimeAdapter(Protocol):
    name: str

    def launch(
        self,
        command: tuple[str, ...],
        *,
        provider_grant: str | None,
    ) -> RuntimeLaunch: ...

    def complete(self, *, exit_code: int, stdout: str) -> RuntimeCompletion: ...
```

`RuntimeLaunch` contains a bounded argv tuple and at most 32 bounded,
non-secret environment entries. The Docker boundary rejects attempts to
override `HOME`, `CI`, run identity, credential paths, or the gateway path.
Adapters cannot alter mounts, networking, resource limits, image identity, or
workspace policy.

`RuntimeCompletion` contains only an exit code. Docker stdout is capped at 1
MiB, parsed in memory after the container exits, and discarded. It is never
written to SQLite, a result manifest, a work log, or the control room. A
non-zero process exit remains the outcome even if its output is malformed, so
private error output is not parsed into a durable failure reason. A zero exit
with a malformed or incomplete success envelope fails closed.

`RuntimeAdapterRegistry` validates names, rejects duplicates and unknown
selections, and validates every launch and completion value returned across
the adapter seam. `tests/test_runtime.py` is the conformance suite used by all
bundled adapters; third-party adapters should run the same registry-level
happy and sad paths.

## Bundled adapters

| Name | Launch contract | Successful completion |
| --- | --- | --- |
| `command` | Execute the profile argv unchanged | Process exit code `0` |
| `codex` | `magic-glass-box-worker` wraps an ephemeral `codex exec --json` configured for a local Responses endpoint | Final JSONL event is `turn.completed` |
| `claude-code` | `magic-glass-box-worker` wraps bare, non-interactive `claude --print --output-format json` against a local Anthropic endpoint | JSON result reports `type: result`, `subtype: success`, and `is_error: false` |

The Codex JSONL shape follows the official
[Codex automation guidance](https://developers.openai.com/blog/eval-skills),
and the Claude invocation follows the official
[Claude Code CLI reference](https://code.claude.com/docs/en/cli-usage).
Unknown event fields are tolerated; duplicate keys, non-finite JSON,
non-object envelopes, missing terminal events, and false success reports are
rejected.

Both vendor adapters tell the worker to start from `run.describe`, use only
declared gateway operations, leave changes in `/workspace`, and avoid Git
publication. The prompt contains no work-item body, response body, credential,
or provider configuration.

## Worker-side provider transport

A runtime adapter is not a network escape hatch. It does not mount a vendor
token, select the upstream model, or enable container egress. A compatible
`gateway_only` image installs this package and its `magic-glass-box-worker`
entry point alongside the vendor CLI. The adapter launches that entry point,
which binds only `127.0.0.1:8766` inside the container and starts the vendor
process as its child.

Codex receives a custom Responses provider URL and a fixed, explicitly
non-secret sentinel token. Claude Code receives an Anthropic-compatible base
URL and the same kind of sentinel. The loopback bridge accepts only
`POST /v1/responses` for Codex or `POST /v1/messages` for Claude, requires a
streaming JSON request, removes the worker-supplied model, and submits the body
through the profile's exact `runtime_provider_grant`. It discards every inbound
HTTP header except the small protocol-feature allowlist documented in
[provider-broker.md](provider-broker.md).

The trusted broker injects the pinned model and reusable credential, performs
the sole outbound request, buffers the bounded SSE response, and returns it
through the ephemeral committed-file mailbox. The bridge replays those bytes
to the vendor CLI. Buffering adds first-token latency, but it preserves Docker
`--network none`, keeps the carrier finite, and avoids putting a socket or
credential in the worker. Neither request nor response bodies are durable.

The same entry point exposes bounded gateway calls to the agent:

```console
magic-glass-box-worker call run.describe '{}'
magic-glass-box-worker call work.read '{"after_sequence":null,"limit":16}'
```

Independent bridge and tool processes serialize access with an advisory lock
on the fixed request file. Sequence, size, digest, response correlation, and
timeouts are verified on every exchange. The image remains incompatible if it
contains only a stock vendor CLI and not this worker entry point.

The remaining Milestone 5 exit proof is a live, single golden-path
demonstration using that worker-side provider transport with both bundled
vendor dialects.
