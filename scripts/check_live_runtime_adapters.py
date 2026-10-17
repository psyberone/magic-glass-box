#!/usr/bin/env python3
"""Run both real vendor CLIs through the networkless worker transport."""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from magic_glass_box.control_room_web import OperatorActions
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
from magic_glass_box.inference import InferenceConnectorSpec, InferenceGrantSpec
from magic_glass_box.krab import (
    KrabIdentity,
    KrabUserRef,
    SessionItem,
    SessionItemHistoryEvent,
)
from magic_glass_box.orchestration import RunCoordinator
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

PROOF_TEXT = "MGB_RUNTIME_PROOF_OK"
PRIVATE_QUESTION = "Which deterministic proof path should continue?"
PRIVATE_RESPONSE = "Continue the pinned replacement proof."
CODEX_MODEL = "magic-glass-box-pinned"
ANTHROPIC_MODEL = "claude-opus-5-5"
RUNTIME_IMAGE = "magic-glass-box/runtime-proof:local"
CODEX_VERSION = "0.161.0"
CLAUDE_CODE_VERSION = "2.1.296"
AGENT_USER_ID = "usr_" + "1" * 32
REVIEWER_USER_ID = "usr_" + "3" * 32
SESSION_ID = "ses_" + "4" * 32
REQUEST_ITEM_ID = "itm_" + "5" * 32
RESPONSE_ITEM_ID = "itm_" + "6" * 32
AGENT_TOKEN = f"krab_{AGENT_USER_ID}_" + "A" * 43
REVIEWER_TOKEN = f"krab_{REVIEWER_USER_ID}_" + "B" * 43


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


def _openai_tool_sse(model: str, command: str, request_number: int) -> bytes:
    item_id = f"fc_mgb_runtime_proof_{request_number}"
    call_id = f"call_mgb_runtime_proof_{request_number}"
    arguments = json.dumps(
        {"cmd": command, "yield_time_ms": 30_000},
        separators=(",", ":"),
        sort_keys=True,
    )
    call = {
        "id": item_id,
        "type": "function_call",
        "status": "completed",
        "arguments": arguments,
        "call_id": call_id,
        "name": "exec_command",
    }
    response = {
        "id": f"resp_mgb_runtime_proof_{request_number}",
        "object": "response",
        "created_at": 1_700_000_000 + request_number,
        "status": "completed",
        "background": False,
        "error": None,
        "incomplete_details": None,
        "instructions": None,
        "max_output_tokens": None,
        "model": model,
        "output": [call],
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
                "item": {**call, "status": "in_progress", "arguments": ""},
            },
        ),
        (
            "response.function_call_arguments.delta",
            {
                "type": "response.function_call_arguments.delta",
                "item_id": item_id,
                "output_index": 0,
                "delta": arguments,
            },
        ),
        (
            "response.function_call_arguments.done",
            {
                "type": "response.function_call_arguments.done",
                "item_id": item_id,
                "output_index": 0,
                "arguments": arguments,
            },
        ),
        (
            "response.output_item.done",
            {
                "type": "response.output_item.done",
                "output_index": 0,
                "item": call,
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


def _session_item(
    *,
    item_id: str,
    creator: KrabUserRef,
    recipient: KrabUserRef,
    title: str,
    body: str,
) -> SessionItem:
    created_sequence = 7 if item_id == REQUEST_ITEM_ID else 8
    return SessionItem(
        item_id=item_id,
        project_key="runtime-proof",
        session_id=SESSION_ID,
        creator=creator,
        recipient=recipient,
        work_claim=None,
        status="open.pending",
        version=1,
        created_at="2026-10-11T12:00:00Z",
        updated_at="2026-10-11T12:00:00Z",
        event_seq=7 if item_id == REQUEST_ITEM_ID else None,
        untrusted_title=title,
        untrusted_body=body,
        content_risk_flags=("untrusted_text",),
        history=(
            SessionItemHistoryEvent(
                event_seq=created_sequence,
                kind="item.created",
                actor=creator,
                from_status=None,
                to_status="open.pending",
                created_at="2026-10-11T12:00:00Z",
                untrusted_note=None,
                content_risk_flags=(),
            ),
        ),
    )


class _RuntimeProofKrabClient:
    def __init__(self) -> None:
        self.identity = KrabIdentity(AGENT_USER_ID, "runtime_proof", 1)
        self.request_item = _session_item(
            item_id=REQUEST_ITEM_ID,
            creator=KrabUserRef(AGENT_USER_ID, "runtime_proof"),
            recipient=KrabUserRef(REVIEWER_USER_ID, "reviewer"),
            title="Runtime proof question",
            body=PRIVATE_QUESTION,
        )
        self.response_item = _session_item(
            item_id=RESPONSE_ITEM_ID,
            creator=KrabUserRef(REVIEWER_USER_ID, "reviewer"),
            recipient=KrabUserRef(AGENT_USER_ID, "runtime_proof"),
            title="Runtime proof response",
            body=PRIVATE_RESPONSE,
        )

    def whoami(self) -> KrabIdentity:
        return self.identity

    def create_session_item(
        self, project_key: str, session_id: str, **kwargs: Any
    ) -> SessionItem:
        if (
            project_key != "runtime-proof"
            or session_id != SESSION_ID
            or kwargs.get("recipient_handle") != "reviewer"
            or kwargs.get("body") != PRIVATE_QUESTION
        ):
            raise RuntimeError("runtime proof emitted an unexpected interaction")
        return self.request_item

    def get_session_item(
        self, project_key: str, session_id: str, item_id: str
    ) -> SessionItem:
        if project_key != "runtime-proof" or session_id != SESSION_ID:
            raise RuntimeError("runtime proof read an unexpected interaction")
        if item_id == REQUEST_ITEM_ID:
            return self.request_item
        if item_id == RESPONSE_ITEM_ID:
            return self.response_item
        raise RuntimeError("runtime proof read an unexpected interaction")


@dataclass(frozen=True)
class ProviderObservation:
    protocol: str
    credential_valid: bool
    model: object
    stream: object
    request_number: int


def _gateway_command(operation: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return " ".join(
        (
            "magic-glass-box-worker",
            "call",
            shlex.quote(operation),
            shlex.quote(encoded),
        )
    )


def _request_interaction_id(request: dict[str, Any]) -> str:
    identifiers = set(re.findall(r"int_[0-9a-f]{32}", json.dumps(request)))
    if len(identifiers) != 1:
        raise RuntimeError("vendor request did not carry one interaction identifier")
    return identifiers.pop()


def _codex_fixture_response(
    request_number: int, model: str, request: dict[str, Any]
) -> bytes:
    if request_number == 1:
        create_files = (
            "from pathlib import Path; "
            "root=Path('node_modules/runtime-proof'); "
            "root.mkdir(parents=True, exist_ok=True); "
            "[(root / f'dependency-{index}.txt').write_text('ignored\\n', "
            "encoding='utf-8') for index in range(64)]; "
            "Path('runtime-proof.txt').write_text('retained work\\n', "
            "encoding='utf-8')"
        )
        question = _gateway_command(
            "interaction.open",
            {
                "body": PRIVATE_QUESTION,
                "idempotency_key": "runtime-proof-question-1",
                "kind": "question",
                "recipient_handle": "reviewer",
                "title": "Runtime proof question",
            },
        )
        command = f"python3 -c {shlex.quote(create_files)} && {question}"
        return _openai_tool_sse(model, command, request_number)
    if request_number == 2:
        interaction_id = _request_interaction_id(request)
        command = _gateway_command(
            "handoff.submit",
            {
                "artifacts": [
                    {
                        "description": "Retained proof work",
                        "path": "runtime-proof.txt",
                    }
                ],
                "decisions": ["Dependency output stays ignored."],
                "next_steps": ["Read the accepted response."],
                "pending_interaction_ids": [interaction_id],
                "schema_version": 1,
                "summary": "Continue from the durable interaction response.",
                "work_state": "waiting_for_input",
            },
        )
        return _openai_tool_sse(model, command, request_number)
    if request_number == 3:
        return _openai_sse(model)
    if request_number == 4:
        return _openai_tool_sse(
            model,
            _gateway_command("run.describe", {}),
            request_number,
        )
    if request_number == 5:
        interaction_id = _request_interaction_id(request)
        return _openai_tool_sse(
            model,
            _gateway_command("interaction.read", {"interaction_id": interaction_id}),
            request_number,
        )
    if request_number == 6:
        return _openai_sse(model)
    raise RuntimeError("Codex made an unexpected provider request")


class _ProviderFixture:
    def __init__(self, openai_secret: str, anthropic_secret: str):
        self.openai_secret = openai_secret
        self.anthropic_secret = anthropic_secret
        self.observations: list[ProviderObservation] = []
        self._request_counts = {
            "openai-responses": 0,
            "anthropic-messages": 0,
        }
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
                with fixture._lock:
                    fixture._request_counts[protocol] += 1
                    request_number = fixture._request_counts[protocol]
                    observation = ProviderObservation(
                        protocol,
                        credential_valid,
                        value.get("model"),
                        value.get("stream"),
                        request_number,
                    )
                    fixture.observations.append(observation)
                if not credential_valid:
                    self.send_error(401)
                    return
                body = (
                    _codex_fixture_response(
                        request_number, str(value.get("model")), value
                    )
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


def _inference_connector_policy(socket_path: Path) -> InferenceConnectorSpec:
    return InferenceConnectorSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_protocol_version": 1,
            "deployment_sha256": "c" * 64,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "routes": [
                {
                    "name": "openai-proof",
                    "provider": "openai",
                    "protocol": "openai-responses",
                    "models": [CODEX_MODEL],
                    "credential_scope_sha256": "d" * 64,
                },
                {
                    "name": "anthropic-proof",
                    "provider": "anthropic",
                    "protocol": "anthropic-messages",
                    "models": [ANTHROPIC_MODEL],
                    "credential_scope_sha256": "e" * 64,
                },
            ],
            "request_bytes": 2 * 1024 * 1024,
            "response_bytes": 4 * 1024 * 1024,
            "timeout_ms": 10_000,
            "max_concurrency": 2,
        }
    )


def _inference_grant_policy(runtime: str) -> InferenceGrantSpec:
    if runtime == "codex":
        route = "openai-proof"
        provider = "openai"
        protocol = "openai-responses"
        model = CODEX_MODEL
        scope = "d" * 64
    elif runtime == "claude-code":
        route = "anthropic-proof"
        provider = "anthropic"
        protocol = "anthropic-messages"
        model = ANTHROPIC_MODEL
        scope = "e" * 64
    else:
        raise ValueError(f"unsupported proof runtime: {runtime}")
    return InferenceGrantSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_name": "runtime-sidecar",
            "deployment_sha256": "c" * 64,
            "route": route,
            "credential_scope_sha256": scope,
            "provider": provider,
            "protocol": protocol,
            "model": model,
            "request_bytes": 2 * 1024 * 1024,
            "response_bytes": 4 * 1024 * 1024,
            "timeout_ms": 10_000,
            "max_concurrency": 1,
        }
    )


def _assert_observations(observations: tuple[ProviderObservation, ...]) -> None:
    expected_protocols = ("openai-responses",) * 6 + ("anthropic-messages",)
    if tuple(item.protocol for item in observations) != expected_protocols:
        raise RuntimeError("vendor requests did not traverse the full proof sequence")
    if tuple(item.request_number for item in observations) != (*range(1, 7), 1):
        raise RuntimeError("provider fixture request sequence is inconsistent")
    if not all(item.credential_valid for item in observations):
        raise RuntimeError("provider credential was not injected by the connector")
    expected_models = (CODEX_MODEL,) * 6 + (ANTHROPIC_MODEL,)
    if tuple(item.model for item in observations) != expected_models:
        raise RuntimeError("provider model was not pinned by policy")
    if not all(item.stream is True for item in observations):
        raise RuntimeError("vendor request did not preserve streaming mode")


def _assert_sandbox_plans(
    plans: tuple[SandboxPlan, ...], credentials: tuple[str, ...]
) -> None:
    if len(plans) != 3:
        raise RuntimeError("proof did not launch the handoff and runtime sandboxes")
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


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


def _create_repository(root: Path) -> Path:
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("Git CLI was not found")
    repository = root / "repository"
    repository.mkdir(mode=0o700)
    _run_checked([git, "init", "--quiet", "--initial-branch", "main"], cwd=repository)
    (repository / "README.md").write_text("runtime proof\n", encoding="utf-8")
    (repository / ".gitignore").write_text("node_modules/\n", encoding="utf-8")
    _run_checked([git, "add", "README.md", ".gitignore"], cwd=repository)
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
    connector_socket: Path,
    credential_directory: Path,
) -> dict[str, str]:
    database = control.database
    database.initialize()
    user = database.create_user("operator", "Runtime Proof Operator")
    reviewer = database.create_user("reviewer", "Runtime Proof Reviewer")
    instance = database.create_krab_instance("Proof Krab", "http://127.0.0.1:8765")
    agent_token_path = _write_secret(credential_directory, "krab", AGENT_TOKEN)
    reviewer_token_path = _write_secret(
        credential_directory, "reviewer-krab", REVIEWER_TOKEN
    )
    identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity(AGENT_USER_ID, "runtime_proof", 1),
        "file:" + str(agent_token_path),
    )
    database.store_krab_identity(
        instance["id"],
        KrabIdentity(REVIEWER_USER_ID, "reviewer", 1),
        "file:" + str(reviewer_token_path),
        user_id=reviewer["id"],
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
    control.register_inference_connector(
        name="runtime-sidecar",
        spec=_inference_connector_policy(connector_socket),
        created_by_handle="operator",
    )
    grants = {
        "codex": "openai-proof",
        "claude-code": "anthropic-proof",
    }
    runs: dict[str, str] = {}
    for runtime, grant in grants.items():
        control.add_connector_provider_grant(
            name=grant,
            spec=_inference_grant_policy(runtime),
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
            credential_names=("krab", grant),
            gateway_capabilities=(
                GatewayCapability.RUN_DESCRIBE,
                GatewayCapability.INTERACTION_OPEN,
                GatewayCapability.INTERACTION_READ,
                GatewayCapability.HANDOFF_SUBMIT,
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
            krab_session_id=SESSION_ID,
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
    connector_process: subprocess.Popen[str] | None = None
    try:
        with tempfile.TemporaryDirectory(
            prefix="mgb-live-runtime-",
            dir="/tmp",  # noqa: S108
        ) as temporary:
            root = Path(temporary)
            os.chmod(root, 0o700)
            credential_directory = root / "credentials"
            credential_directory.mkdir(mode=0o700)
            connector_directory = root / "connector-private"
            connector_directory.mkdir(mode=0o700)
            connector_socket = connector_directory / "inference.sock"
            openai_credential = _write_secret(
                connector_directory, "openai", openai_secret
            )
            anthropic_credential = _write_secret(
                connector_directory, "anthropic", anthropic_secret
            )
            connector_config = connector_directory / "config.json"
            repository = _create_repository(root)
            database = Database(root / "control-plane.db")
            resolver = PrivateFileCredentialResolver()
            krab_client = _RuntimeProofKrabClient()

            def client_factory(_base_url: str, token: str) -> Any:
                if token != AGENT_TOKEN:
                    raise RuntimeError(
                        "runtime proof resolved an unexpected Krab token"
                    )
                return krab_client

            control = ControlPlane(
                database,
                credential_resolver=resolver,
                client_factory=client_factory,
            )
            runs = _prepare_runs(
                control,
                repository=repository,
                image=args.image,
                connector_socket=connector_socket,
                credential_directory=credential_directory,
            )
            connector = database.get_inference_connector("runtime-sidecar")
            connector_config.write_text(
                json.dumps(
                    {
                        "connector_id": connector["id"],
                        "deployment_sha256": connector["deployment_sha256"],
                        "routes": [
                            {
                                "name": "openai-proof",
                                "credential_scope_sha256": "d" * 64,
                                "policy": _provider_policy(
                                    server.server_port, "codex"
                                ).to_dict(),
                                "credential_file": str(openai_credential),
                            },
                            {
                                "name": "anthropic-proof",
                                "credential_scope_sha256": "e" * 64,
                                "policy": _provider_policy(
                                    server.server_port, "claude-code"
                                ).to_dict(),
                                "credential_file": str(anthropic_credential),
                            },
                        ],
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            os.chmod(connector_config, 0o600)
            project_root = Path(__file__).resolve().parents[1]
            connector_environment = {
                "PATH": os.environ.get("PATH", ""),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": str(project_root / "src"),
            }
            connector_process = subprocess.Popen(  # noqa: S603
                [
                    sys.executable,
                    str(project_root / "scripts" / "reference_inference_connector.py"),
                    "--socket-path",
                    str(connector_socket),
                    "--config",
                    str(connector_config),
                    "--max-invocations",
                    "7",
                ],
                cwd=project_root,
                env=connector_environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if connector_socket.exists() or connector_process.poll() is not None:
                    break
                time.sleep(0.01)
            if not connector_socket.exists():
                stdout, stderr = connector_process.communicate(timeout=2)
                raise RuntimeError(
                    f"runtime inference connector did not start: {stdout}{stderr}"
                )
            docker_engine = _RecordingDockerEngine(
                DockerCLI(
                    command_timeout=args.docker_timeout,
                    run_timeout=args.run_timeout,
                )
            )
            gateways = FileGatewayManager(root / "gateways", control)
            result_store = WorkspaceResultStore(root / "results", max_paths=16)
            coordinator = RunCoordinator(
                database,
                DockerExecutor(docker_engine),
                WorkspaceManager(root / "workspaces", GitSnapshotter()),
                gateways,
                result_store,
                lease_seconds=30,
                heartbeat_interval=5,
            )
            completed: dict[str, dict[str, Any]] = {}
            try:
                handed_off = coordinator.execute(runs["codex"])
                if handed_off["state"] != RunState.WAITING_FOR_INPUT.value:
                    raise RuntimeError("Codex did not stop at the durable interaction")
                if (
                    len(handed_off["attempts"]) != 1
                    or handed_off["attempts"][0]["state"] != "handed_off"
                    or handed_off["attempts"][0]["handoff"] is None
                ):
                    raise RuntimeError("Codex did not retain its structured handoff")
                first_manifest = result_store.inspect(handed_off["attempts"][0]["id"])
                if [change.path for change in first_manifest.changes] != [
                    "runtime-proof.txt"
                ]:
                    raise RuntimeError(
                        "pinned ignore rules did not exclude the dependency tree"
                    )
                interactions = handed_off["interactions"]
                if len(interactions) != 1 or interactions[0]["state"] != "open":
                    raise RuntimeError("Codex did not leave one durable interaction")
                accepted = OperatorActions(control, coordinator).answer(
                    interactions[0]["id"],
                    response_item_id=RESPONSE_ITEM_ID,
                    expected_version=interactions[0]["version"],
                )
                if accepted.run_state != RunState.QUEUED.value:
                    raise RuntimeError("accepted input did not queue a replacement")
                completed["codex"] = coordinator.execute(runs["codex"])
                completed["claude-code"] = coordinator.execute(runs["claude-code"])
            finally:
                gateways.close()

            for runtime, run in completed.items():
                if run["state"] != RunState.SUCCEEDED.value:
                    raise RuntimeError(f"{runtime} proof run did not succeed")
                attempts = run["attempts"]
                if runtime == "codex":
                    if (
                        len(attempts) != 2
                        or [attempt["runtime_adapter"] for attempt in attempts]
                        != ["codex", "codex"]
                        or [attempt["state"] for attempt in attempts]
                        != ["handed_off", "succeeded"]
                        or [attempt["result"]["changed_paths"] for attempt in attempts]
                        != [1, 0]
                        or run["interactions"][0]["state"] != "answered"
                    ):
                        raise RuntimeError("Codex replacement evidence is incomplete")
                    expected_invocations = 6
                else:
                    if (
                        len(attempts) != 1
                        or attempts[0]["runtime_adapter"] != runtime
                        or attempts[0]["state"] != "succeeded"
                        or attempts[0]["result"]["changed_paths"] != 0
                    ):
                        raise RuntimeError(f"{runtime} attempt evidence is incomplete")
                    expected_invocations = 1
                invocations = database.list_provider_invocations(runs[runtime])
                if (
                    len(invocations) != expected_invocations
                    or any(item["outcome"] != "succeeded" for item in invocations)
                    or any(item["dispatch_kind"] != "connector" for item in invocations)
                    or any(item["http_status"] != 200 for item in invocations)
                ):
                    raise RuntimeError(f"{runtime} provider invocation was not audited")
                if docker_engine.managed_containers(runs[runtime]):
                    raise RuntimeError(f"{runtime} container was not removed")

            secrets_used = (
                openai_secret,
                anthropic_secret,
                AGENT_TOKEN,
                REVIEWER_TOKEN,
            )
            _assert_observations(tuple(fixture.observations))
            _assert_sandbox_plans(tuple(docker_engine.plans), secrets_used)
            durable = database.path.read_bytes()
            if any(secret.encode() in durable for secret in secrets_used):
                raise RuntimeError("provider credential leaked into durable state")
            if PROOF_TEXT.encode() in durable:
                raise RuntimeError("transient model output leaked into durable state")
            if (
                PRIVATE_QUESTION.encode() in durable
                or PRIVATE_RESPONSE.encode() in durable
            ):
                raise RuntimeError(
                    "private interaction content leaked into durable state"
                )
            connector_stdout, connector_stderr = connector_process.communicate(
                timeout=5
            )
            if connector_process.returncode != 0 or connector_stderr:
                raise RuntimeError(
                    "runtime inference connector failed: "
                    + connector_stdout
                    + connector_stderr
                )
            connector_report = json.loads(connector_stdout)
            if connector_report != {"invocations": 7, "status": "stopped"}:
                raise RuntimeError("runtime connector invocation count is inconsistent")
            if any(secret in connector_stdout for secret in secrets_used):
                raise RuntimeError("provider credential leaked into connector logs")
    finally:
        if connector_process is not None:
            _stop_process(connector_process)
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
                "durable_interaction": "handoff and replacement passed",
                "direct_network": "denied",
                "ignored_dependency_tree": "excluded by pinned policy",
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
