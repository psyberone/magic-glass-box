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

    def launch(self, command: tuple[str, ...]) -> RuntimeLaunch: ...

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
| `codex` | Non-interactive, ephemeral `codex exec --json` under the outer sandbox | Final JSONL event is `turn.completed` |
| `claude-code` | Non-interactive `claude --print --output-format json` with no session persistence or permission prompt | JSON result reports `type: result`, `subtype: success`, and `is_error: false` |

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

## Network and credential boundary

A runtime adapter is not a network escape hatch. It does not mount a vendor
token, select a model, or enable container egress. A compatible
`gateway_only` image must include a worker-side transport that routes its model
traffic through the declared provider grant and committed-file gateway. A
stock vendor CLI image that expects direct Internet access or a reusable token
is therefore not compatible by itself.

The remaining Milestone 5 exit proof is a live, single golden-path
demonstration using that worker-side provider transport with both bundled
vendor dialects.
