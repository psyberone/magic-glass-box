#!/usr/bin/env python3
"""Run both real vendor CLIs through the networkless worker transport."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from magic_glass_box.credentials import PrivateFileCredentialResolver
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    ProjectContext,
    ProviderGrantSpec,
    RunState,
    WorkItemEnvelope,
)
from magic_glass_box.executor import (
    DockerCLI,
    DockerExecutor,
    DockerImage,
    SandboxPlan,
)
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.orchestration import RunCoordinator
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

PROOF_TEXT = "MGB_RUNTIME_PROOF_OK"
CODEX_MODEL = "magic-glass-box-pinned"
ANTHROPIC_MODEL = "claude-opus-5-5"
RUNTIME_IMAGE = "magic-glass-box/runtime-proof:local"
CODEX_VERSION = "0.161.0"
CLAUDE_CODE_VERSION = "2.1.296"


def _sse_event(name: str, value: dict[str, Any]) -> bytes:
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True)
    return f"event: {name}\ndata: {encoded}\n\n".encode()


def _openai_sse(model: str) -> bytes:
    message = {
        "id": "msg_mgb_runtime_proof",
        "type": "message",
        "status": "completed",
        "role": "assistant",
        "content": [
            {
                "type": "output_text",
                "text": PROOF_TEXT,
                "annotations": [],
                "logprobs": [],
            }
        ],
    }
    response = {
        "id": "resp_mgb_runtime_proof",
        "object": "response",
        "created_at": 1_700_000_000,
        "status": "completed",
        "background": False,
        "error": None,
        "incomplete_details": None,
        "instructions": None,
        "max_output_tokens": None,
        "model": model,
        "output": [message],
        "parallel_tool_calls": True,
        "previous_response_id": None,
        "reasoning": {"effort": "medium", "summary": None},
        "store": False,
        "temperature": 1.0,
        "text": {"format": {"type": "text"}},
        "tool_choice": "auto",
        "tools": [],
        "top_logprobs": 0,
        "top_p": 1.0,
        "truncation": "disabled",
        "usage": {
            "input_tokens": 1,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens": 1,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 2,
        },
        "metadata": {},
    }
    created = {**response, "status": "in_progress", "output": [], "usage": None}
    events = (
        ("response.created", {"type": "response.created", "response": created}),
        (
            "response.output_item.added",
            {
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {**message, "status": "in_progress", "content": []},
            },
        ),
        (
            "response.content_part.added",
            {
                "type": "response.content_part.added",
                "item_id": message["id"],
                "output_index": 0,
                "content_index": 0,
                "part": {
                    "type": "output_text",
                    "text": "",
                    "annotations": [],
                    "logprobs": [],
                },
            },
        ),
        (
            "response.output_text.delta",
            {
                "type": "response.output_text.delta",
                "item_id": message["id"],
                "output_index": 0,
                "content_index": 0,
                "delta": PROOF_TEXT,
                "logprobs": [],
            },
        ),
        (
            "response.output_text.done",
            {
                "type": "response.output_text.done",
                "item_id": message["id"],
                "output_index": 0,
                "content_index": 0,
                "text": PROOF_TEXT,
                "logprobs": [],
            },
        ),
        (
            "response.content_part.done",
            {
                "type": "response.content_part.done",
                "item_id": message["id"],
                "output_index": 0,
                "content_index": 0,
                "part": message["content"][0],
            },
        ),
        (
            "response.output_item.done",
            {
                "type": "response.output_item.done",
                "output_index": 0,
                "item": message,
            },
        ),
        (
            "response.completed",
            {"type": "response.completed", "response": response},
        ),
    )
    frames = []
    for sequence, (name, value) in enumerate(events):
        frames.append(_sse_event(name, {**value, "sequence_number": sequence}))
    frames.append(b"data: [DONE]\n\n")
    return b"".join(frames)


def _anthropic_sse(model: str) -> bytes:
    events = (
        (
            "message_start",
            {
                "type": "message_start",
                "message": {
                    "id": "msg_mgb_runtime_proof",
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {
                        "input_tokens": 1,
                        "cache_creation_input_tokens": 0,
                        "cache_read_input_tokens": 0,
                        "output_tokens": 0,
                    },
                },
            },
        ),
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": PROOF_TEXT},
            },
        ),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    )
    return b"".join(_sse_event(name, value) for name, value in events)


@dataclass(frozen=True)
class ProviderObservation:
    protocol: str
    credential_valid: bool
    model: object
    stream: object


class _ProviderFixture:
    def __init__(self, openai_secret: str, anthropic_secret: str):
        self.openai_secret = openai_secret
        self.anthropic_secret = anthropic_secret
        self.observations: list[ProviderObservation] = []
        self._lock = threading.Lock()

    def handler(self) -> type[BaseHTTPRequestHandler]:
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                routes = {
                    "/v1/responses": "openai-responses",
                    "/v1/messages": "anthropic-messages",
                }
                protocol = routes.get(self.path)
                declared = self.headers.get("Content-Length", "")
                if protocol is None or not declared.isdecimal():
                    self.send_error(404 if protocol is None else 400)
                    return
                size = int(declared)
                if not 1 <= size <= 2 * 1024 * 1024:
                    self.send_error(413)
                    return
                try:
                    value = json.loads(self.rfile.read(size))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    self.send_error(400)
                    return
                if not isinstance(value, dict):
                    self.send_error(400)
                    return
                if protocol == "openai-responses":
                    credential_valid = self.headers.get("Authorization") == (
                        "Bearer " + fixture.openai_secret
                    )
                else:
                    credential_valid = (
                        self.headers.get("x-api-key") == fixture.anthropic_secret
                    )
                observation = ProviderObservation(
                    protocol,
                    credential_valid,
                    value.get("model"),
                    value.get("stream"),
                )
                with fixture._lock:
                    fixture.observations.append(observation)
                if not credential_valid:
                    self.send_error(401)
                    return
                body = (
                    _openai_sse(str(value.get("model")))
                    if protocol == "openai-responses"
                    else _anthropic_sse(str(value.get("model")))
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, _format: str, *args: object) -> None:
                return

        return Handler


class _RecordingDockerEngine:
    def __init__(self, delegate: DockerCLI):
        self.delegate = delegate
        self.plans: list[SandboxPlan] = []

    def inspect_image(self, reference: str) -> DockerImage:
        return self.delegate.inspect_image(reference)

    def create(self, plan: SandboxPlan) -> str:
        self.plans.append(plan)
        return self.delegate.create(plan)

    def start(self, container_id: str) -> None:
        self.delegate.start(container_id)

    def wait(self, container_id: str) -> int:
        return self.delegate.wait(container_id)

    def logs(self, container_id: str) -> str:
        return self.delegate.logs(container_id)

    def stop(self, container_id: str) -> None:
        self.delegate.stop(container_id)

    def remove(self, container_id: str) -> None:
        self.delegate.remove(container_id)

    def managed_containers(self, run_id: str) -> tuple[str, ...]:
        return self.delegate.managed_containers(run_id)


def _provider_policy(port: int, runtime: str) -> ProviderGrantSpec:
    if runtime == "codex":
        adapter = "openai_responses"
        path = "/v1/responses"
        model = CODEX_MODEL
        credential_header = "Authorization"
        credential_prefix = "Bearer "
        provider = "openai"
    elif runtime == "claude-code":
        adapter = "anthropic_messages"
        path = "/v1/messages"
        model = ANTHROPIC_MODEL
        credential_header = "x-api-key"
        credential_prefix = ""
        provider = "anthropic"
    else:
        raise ValueError(f"unsupported proof runtime: {runtime}")
    return ProviderGrantSpec.from_mapping(
        {
            "provider": provider,
            "adapter": adapter,
            "destination": f"http://127.0.0.1:{port}{path}",
            "method": "POST",
            "model": model,
            "credential_header": credential_header,
            "credential_prefix": credential_prefix,
            "static_headers": {},
            "request_bytes": 2 * 1024 * 1024,
            "response_bytes": 4 * 1024 * 1024,
            "timeout_ms": 10_000,
            "max_concurrency": 1,
        }
    )


def _assert_observations(observations: tuple[ProviderObservation, ...]) -> None:
    if tuple(item.protocol for item in observations) != (
        "openai-responses",
        "anthropic-messages",
    ):
        raise RuntimeError("vendor requests did not traverse both pinned protocols")
    if not all(item.credential_valid for item in observations):
        raise RuntimeError("provider credential was not injected by the trusted broker")
    expected_models = (CODEX_MODEL, ANTHROPIC_MODEL)
    if tuple(item.model for item in observations) != expected_models:
        raise RuntimeError("provider model was not pinned by policy")
    if not all(item.stream is True for item in observations):
        raise RuntimeError("vendor request did not preserve streaming mode")


def _assert_sandbox_plans(
    plans: tuple[SandboxPlan, ...], credentials: tuple[str, ...]
) -> None:
    if len(plans) != 2:
        raise RuntimeError("proof did not launch both runtime sandboxes")
    for plan in plans:
        arguments = plan.create_args
        try:
            network = arguments[arguments.index("--network") + 1]
        except (ValueError, IndexError) as exc:
            raise RuntimeError(
                "runtime sandbox has no explicit network policy"
            ) from exc
        if network != "none":
            raise RuntimeError("runtime sandbox was not networkless")
        serialized = "\x00".join(arguments)
        if any(secret and secret in serialized for secret in credentials):
            raise RuntimeError("provider credential entered a sandbox plan")
        if "/run/secrets/magic-glass-box" in serialized:
            raise RuntimeError("gateway-only runtime received a credential mount")


def _run_checked(command: list[str], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(  # noqa: S603
        command,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"command failed: {detail[:4096]}")
    return completed.stdout


def _create_repository(root: Path) -> Path:
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("Git CLI was not found")
    repository = root / "repository"
    repository.mkdir(mode=0o700)
    _run_checked([git, "init", "--quiet", "--initial-branch", "main"], cwd=repository)
    (repository / "README.md").write_text("runtime proof\n", encoding="utf-8")
    _run_checked([git, "add", "README.md"], cwd=repository)
    _run_checked(
        [
            git,
            "-c",
            "user.name=Magic Glass Box",
            "-c",
            "user.email=proof@magic-glass-box.invalid",
            "commit",
            "--quiet",
            "-m",
            "Create runtime proof fixture",
        ],
        cwd=repository,
    )
    return repository


def _write_secret(directory: Path, name: str, value: str) -> Path:
    path = directory / name
    path.write_text(value + "\n", encoding="ascii")
    os.chmod(path, 0o600)
    return path


def _prepare_runs(
    control: ControlPlane,
    *,
    repository: Path,
    image: str,
    provider_port: int,
    credential_directory: Path,
    openai_secret: str,
    anthropic_secret: str,
) -> dict[str, str]:
    database = control.database
    database.initialize()
    user = database.create_user("operator", "Runtime Proof Operator")
    instance = database.create_krab_instance("Proof Krab", "http://127.0.0.1:8765")
    identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity("usr_" + "1" * 32, "runtime_proof", 1),
        "file:/not-mounted/krab.token",
    )
    database.create_project(
        instance_id=instance["id"],
        krab_project_id="prj_" + "2" * 32,
        krab_project_key="runtime-proof",
        display_name="Runtime proof",
        context=ProjectContext.from_mapping(
            {
                "purpose": "Prove interchangeable networkless runtime adapters",
                "repository_path": str(repository),
            }
        ),
        created_by=user["id"],
    )
    work_item = database.import_work_item(
        project_key="runtime-proof",
        envelope=WorkItemEnvelope.from_mapping(
            {
                "schema_version": 1,
                "source": "manual",
                "external_id": "live-runtime-proof",
                "source_updated_at": "2026-10-11T12:00:00Z",
                "title": "Prove interchangeable runtime adapters",
                "brief": "Run both vendor CLIs through the scoped provider gateway.",
            }
        ),
        imported_by=user["id"],
    )
    grants = {
        "codex": ("openai-proof", openai_secret),
        "claude-code": ("anthropic-proof", anthropic_secret),
    }
    runs: dict[str, str] = {}
    for runtime, (grant, secret) in grants.items():
        control.add_provider_grant(
            name=grant,
            spec=_provider_policy(provider_port, runtime),
            credential_path=_write_secret(credential_directory, grant, secret),
            expires_at=None,
            created_by_handle="operator",
        )
        profile_name = "Codex Proof" if runtime == "codex" else "Claude Proof"
        profile = AgentProfileSpec.create(
            image=image,
            command=("codex",) if runtime == "codex" else ("claude",),
            runtime_adapter=runtime,
            runtime_provider_grant=grant,
            network_mode=NetworkMode.GATEWAY_ONLY,
            credential_names=(grant,),
            gateway_capabilities=(
                GatewayCapability.RUN_DESCRIBE,
                GatewayCapability.PROVIDER_INVOKE,
            ),
        )
        database.create_agent_profile(
            name=profile_name,
            spec=profile,
            krab_identity_id=identity["id"],
            created_by=user["id"],
        )
        run = database.create_run(
            project_key="runtime-proof",
            profile_name=profile_name,
            work_item_id=work_item["id"],
            requested_by=user["id"],
        )
        runs[runtime] = run["id"]
    return runs


def _build_image(image: str, timeout: float) -> None:
    docker = shutil.which("docker")
    if docker is None:
        raise RuntimeError("Docker CLI was not found")
    repository = Path(__file__).resolve().parents[1]
    command = [
        docker,
        "build",
        "--file",
        str(repository / "docker" / "runtime-proof.Dockerfile"),
        "--tag",
        image,
        str(repository),
    ]
    try:
        completed = subprocess.run(command, timeout=timeout, check=False)  # noqa: S603
    except subprocess.TimeoutExpired:
        raise RuntimeError("runtime proof image build timed out") from None
    if completed.returncode != 0:
        raise RuntimeError("runtime proof image build failed")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=RUNTIME_IMAGE)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--build-timeout", type=float, default=600.0)
    parser.add_argument("--docker-timeout", type=float, default=60.0)
    parser.add_argument("--run-timeout", type=float, default=180.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not hasattr(os, "getuid") or os.getuid() <= 0 or os.getgid() <= 0:
        raise RuntimeError("live runtime proof requires a non-root POSIX user")
    if args.build:
        _build_image(args.image, args.build_timeout)

    openai_secret = "openai_" + secrets.token_urlsafe(32)
    anthropic_secret = "anthropic_" + secrets.token_urlsafe(32)
    fixture = _ProviderFixture(openai_secret, anthropic_secret)
    server = ThreadingHTTPServer(("127.0.0.1", 0), fixture.handler())
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        with tempfile.TemporaryDirectory(
            prefix="mgb-live-runtime-",
            dir="/tmp",  # noqa: S108
        ) as temporary:
            root = Path(temporary)
            os.chmod(root, 0o700)
            credential_directory = root / "credentials"
            credential_directory.mkdir(mode=0o700)
            repository = _create_repository(root)
            database = Database(root / "control-plane.db")
            resolver = PrivateFileCredentialResolver()
            control = ControlPlane(database, credential_resolver=resolver)
            runs = _prepare_runs(
                control,
                repository=repository,
                image=args.image,
                provider_port=server.server_port,
                credential_directory=credential_directory,
                openai_secret=openai_secret,
                anthropic_secret=anthropic_secret,
            )
            docker_engine = _RecordingDockerEngine(
                DockerCLI(
                    command_timeout=args.docker_timeout,
                    run_timeout=args.run_timeout,
                )
            )
            gateways = FileGatewayManager(root / "gateways", control)
            coordinator = RunCoordinator(
                database,
                DockerExecutor(docker_engine),
                WorkspaceManager(root / "workspaces", GitSnapshotter()),
                resolver,
                gateways,
                WorkspaceResultStore(root / "results"),
                lease_seconds=30,
                heartbeat_interval=5,
            )
            completed: dict[str, dict[str, Any]] = {}
            try:
                for runtime in ("codex", "claude-code"):
                    completed[runtime] = coordinator.execute(runs[runtime])
            finally:
                gateways.close()

            for runtime, run in completed.items():
                if run["state"] != RunState.SUCCEEDED.value:
                    raise RuntimeError(f"{runtime} proof run did not succeed")
                attempts = run["attempts"]
                if (
                    len(attempts) != 1
                    or attempts[0]["runtime_adapter"] != runtime
                    or attempts[0]["state"] != "succeeded"
                    or attempts[0]["result"]["changed_paths"] != 0
                ):
                    raise RuntimeError(f"{runtime} attempt evidence is incomplete")
                invocations = database.list_provider_invocations(runs[runtime])
                if (
                    len(invocations) != 1
                    or invocations[0]["outcome"] != "succeeded"
                    or invocations[0]["http_status"] != 200
                ):
                    raise RuntimeError(f"{runtime} provider invocation was not audited")
                if docker_engine.managed_containers(runs[runtime]):
                    raise RuntimeError(f"{runtime} container was not removed")

            secrets_used = (openai_secret, anthropic_secret)
            _assert_observations(tuple(fixture.observations))
            _assert_sandbox_plans(tuple(docker_engine.plans), secrets_used)
            durable = database.path.read_bytes()
            if any(secret.encode() in durable for secret in secrets_used):
                raise RuntimeError("provider credential leaked into durable state")
            if PROOF_TEXT.encode() in durable:
                raise RuntimeError("transient model output leaked into durable state")
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(5)

    print(
        json.dumps(
            {
                "adapters": {
                    "claude-code": {
                        "cli_version": CLAUDE_CODE_VERSION,
                        "protocol": "anthropic-messages",
                        "state": "succeeded",
                    },
                    "codex": {
                        "cli_version": CODEX_VERSION,
                        "protocol": "openai-responses",
                        "state": "succeeded",
                    },
                },
                "credential_exposure": "none",
                "direct_network": "denied",
                "provider_fixture": "host-only deterministic SSE",
                "runtime_output_persisted": False,
                "status": "passed",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
