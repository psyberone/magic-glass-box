#!/usr/bin/env python3
"""Prove a networkless Docker worker can use only one pinned provider grant."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import tempfile
import threading
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
from magic_glass_box.executor import DockerCLI, DockerExecutor, SandboxRequest
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.runtime import RuntimeLaunch
from magic_glass_box.service import ControlPlane

AGENT_PROGRAM = r"""
import hashlib
import json
import os
import socket
import time
from pathlib import Path

probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
probe.settimeout(0.25)
try:
    probe.connect(("1.1.1.1", 443))
except OSError:
    pass
else:
    raise SystemExit("worker unexpectedly reached an external network")
finally:
    probe.close()

root = Path(os.environ["MAGIC_GLASS_BOX_GATEWAY_DIR"])
if sorted(path.name for path in root.iterdir()) != [
    "request.json", "request.ready", "response.json", "response.ready"
]:
    raise SystemExit("gateway-only worker received an unexpected mounted file")

def replace(path, value):
    with path.open("r+b") as stream:
        stream.truncate()
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())

def exchange(sequence, operation, payload):
    request = {
        "operation": operation,
        "payload": payload,
        "request_id": f"live-provider-{sequence:04d}",
        "version": 1,
    }
    raw = (json.dumps(request, separators=(",", ":"), sort_keys=True) + "\n").encode()
    marker = {
        "sequence": sequence,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size": len(raw),
    }
    replace(root / "request.json", raw)
    replace(
        root / "request.ready",
        (json.dumps(marker, separators=(",", ":"), sort_keys=True) + "\n").encode(),
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        ready_raw = (root / "response.ready").read_bytes()
        if ready_raw:
            try:
                ready = json.loads(ready_raw)
            except json.JSONDecodeError:
                time.sleep(0.025)
                continue
            if ready.get("sequence") == sequence:
                response_raw = (root / "response.json").read_bytes()
                if (
                    len(response_raw) == ready["size"]
                    and hashlib.sha256(response_raw).hexdigest() == ready["sha256"]
                ):
                    return json.loads(response_raw)
        time.sleep(0.025)
    raise SystemExit("gateway response timed out")

valid = exchange(
    1,
    "provider.invoke",
    {"grant": "example-main", "request": {"input": "live hello"}},
)
if not valid["ok"]:
    raise SystemExit("declared provider grant was denied")
result = valid["result"]
if (
    result["provider"] != "example-ai"
    or result["model"] != "pinned-model"
    or result["untrusted_response"] != {"output_text": "provider reached"}
):
    raise SystemExit("provider response did not preserve pinned policy")

undeclared = exchange(
    2,
    "provider.invoke",
    {"grant": "other-provider", "request": {"input": "no"}},
)
if undeclared["ok"] or undeclared["error"]["code"] != "forbidden":
    raise SystemExit("undeclared provider grant did not fail closed")

destination = exchange(
    3,
    "provider.invoke",
    {
        "grant": "example-main",
        "request": {"input": "no"},
        "destination": "https://attacker.example",
    },
)
if destination["ok"] or destination["error"]["code"] != "invalid_request":
    raise SystemExit("worker-selected destination did not fail closed")

model = exchange(
    4,
    "provider.invoke",
    {"grant": "example-main", "request": {"model": "worker-model"}},
)
if model["ok"] or model["error"]["code"] != "invalid_request":
    raise SystemExit("worker-selected model did not fail closed")
"""


class RecordingProvider:
    def __init__(self, credential: str):
        self.credential = credential
        self.requests: list[dict[str, Any]] = []

    def handler(self) -> type[BaseHTTPRequestHandler]:
        recorder = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/v1/invoke":
                    self.send_error(404)
                    return
                length = int(self.headers.get("Content-Length", "0"))
                value = json.loads(self.rfile.read(length))
                recorder.requests.append(
                    {
                        "authorization": self.headers.get("Authorization"),
                        "body": value,
                    }
                )
                raw = b'{"output_text":"provider reached"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, _format: str, *args: object) -> None:
                return

        return Handler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="python:3.12-slim")
    parser.add_argument("--docker-timeout", type=float, default=60.0)
    return parser.parse_args()


def _provider_policy(port: int) -> ProviderGrantSpec:
    return ProviderGrantSpec.from_mapping(
        {
            "provider": "example-ai",
            "adapter": "json_http",
            "destination": f"http://127.0.0.1:{port}/v1/invoke",
            "method": "POST",
            "model": "pinned-model",
            "credential_header": "Authorization",
            "credential_prefix": "Bearer ",
            "static_headers": {},
            "request_bytes": 4096,
            "response_bytes": 4096,
            "timeout_ms": 5000,
            "max_concurrency": 1,
        }
    )


def _prepare_run(
    control: ControlPlane,
    secret_path: Path,
    policy: ProviderGrantSpec,
    image: str,
) -> tuple[str, AgentProfileSpec]:
    database = control.database
    database.initialize()
    user = database.create_user("operator", "Live Proof Operator")
    instance = database.create_krab_instance("Proof Krab", "http://127.0.0.1:8765")
    identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity("usr_" + "1" * 32, "proof_agent", 1),
        "file:/not-mounted/krab.token",
    )
    database.create_project(
        instance_id=instance["id"],
        krab_project_id="prj_" + "2" * 32,
        krab_project_key="provider-proof",
        display_name="Provider proof",
        context=ProjectContext.from_mapping({"purpose": "Prove provider isolation"}),
        created_by=user["id"],
    )
    work_item = database.import_work_item(
        project_key="provider-proof",
        envelope=WorkItemEnvelope.from_mapping(
            {
                "schema_version": 1,
                "source": "manual",
                "external_id": "live-provider-proof",
                "source_updated_at": "2026-10-09T12:00:00Z",
                "title": "Prove scoped provider access",
                "brief": "Reach one pinned provider from a networkless worker.",
            }
        ),
        imported_by=user["id"],
    )
    control.add_provider_grant(
        name="example-main",
        spec=policy,
        credential_path=secret_path,
        expires_at=None,
        created_by_handle="operator",
    )
    profile = AgentProfileSpec.create(
        image=image,
        command=("python", "/workspace/provider_probe.py"),
        network_mode=NetworkMode.GATEWAY_ONLY,
        credential_names=("example-main",),
        gateway_capabilities=(GatewayCapability.PROVIDER_INVOKE,),
    )
    database.create_agent_profile(
        name="Provider Proof Worker",
        spec=profile,
        krab_identity_id=identity["id"],
        created_by=user["id"],
    )
    run = database.create_run(
        project_key="provider-proof",
        profile_name="Provider Proof Worker",
        work_item_id=work_item["id"],
        requested_by=user["id"],
    )
    preparing = database.transition_run(
        run["id"], target=RunState.PREPARING, expected_version=1
    )
    running = database.transition_run(
        run["id"],
        target=RunState.RUNNING,
        expected_version=preparing["version"],
        source_revision="live-proof",
    )
    return running["id"], profile


def main() -> int:
    args = parse_args()
    if not hasattr(os, "getuid") or os.getuid() <= 0 or os.getgid() <= 0:
        raise RuntimeError("live provider proof requires a non-root POSIX user")
    credential = "live_" + secrets.token_urlsafe(32)
    recorder = RecordingProvider(credential)
    server = ThreadingHTTPServer(("127.0.0.1", 0), recorder.handler())
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        with tempfile.TemporaryDirectory(
            prefix="mgb-live-provider-",
            dir="/tmp",  # noqa: S108
        ) as temporary:
            root = Path(temporary)
            os.chmod(root, 0o700)
            secret_path = root / "provider.secret"
            secret_path.write_text(credential + "\n", encoding="ascii")
            os.chmod(secret_path, 0o600)
            database = Database(root / "control-plane.db")
            control = ControlPlane(
                database,
                credential_resolver=PrivateFileCredentialResolver(),
            )
            run_id, pinned_profile = _prepare_run(
                control,
                secret_path,
                _provider_policy(server.server_port),
                args.image,
            )
            workspace = root / "workspace"
            workspace.mkdir(mode=0o700)
            (workspace / "provider_probe.py").write_text(
                AGENT_PROGRAM, encoding="utf-8"
            )
            manager = FileGatewayManager(root / "gateways", control)
            try:
                endpoint = manager.open(
                    run_id,
                    capabilities=pinned_profile.gateway_capabilities,
                    credential_names=pinned_profile.credential_names,
                )
                result = DockerExecutor(
                    DockerCLI(
                        command_timeout=args.docker_timeout,
                        run_timeout=args.docker_timeout,
                    )
                ).execute(
                    SandboxRequest(
                        run_id=run_id,
                        profile=pinned_profile,
                        workspace=workspace,
                        launch=RuntimeLaunch(pinned_profile.command),
                        credential_files={},
                        gateway_directory=endpoint.directory,
                    )
                )
                gateway_bytes = b"".join(
                    path.read_bytes() for path in endpoint.directory.iterdir()
                )
            finally:
                manager.cleanup(run_id)
            if result.exit_code != 0:
                raise RuntimeError(f"live provider worker exited {result.exit_code}")
            if recorder.requests != [
                {
                    "authorization": "Bearer " + credential,
                    "body": {"input": "live hello", "model": "pinned-model"},
                }
            ]:
                raise RuntimeError(
                    "provider did not receive exactly the pinned request"
                )
            invocations = database.list_provider_invocations(run_id)
            if len(invocations) != 1 or invocations[0]["outcome"] != "succeeded":
                raise RuntimeError("successful provider invocation was not audited")
            if credential.encode("ascii") in database.path.read_bytes():
                raise RuntimeError("provider secret leaked into durable state")
            if credential.encode("ascii") in gateway_bytes:
                raise RuntimeError("provider secret leaked across the worker gateway")
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(5)
    print(
        json.dumps(
            {
                "audited_invocations": 1,
                "credential_exposure": "none",
                "direct_network": "denied",
                "permitted_provider": "reached",
                "undeclared_authority": "denied",
                "worker_selected_destination": "denied",
                "worker_selected_model": "denied",
                "status": "passed",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
