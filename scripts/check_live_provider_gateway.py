#!/usr/bin/env python3
"""Prove a networkless Docker worker can use only one pinned provider grant."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    ProjectContext,
    WorkItemEnvelope,
)
from magic_glass_box.executor import (
    DockerCLI,
    DockerExecutor,
    SandboxPlan,
    SandboxRequest,
)
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.inference import InferenceConnectorSpec, InferenceGrantSpec
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
    or result["untrusted_response"]
    != {"output_text": "deterministic fake response"}
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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="python:3.12-slim")
    parser.add_argument("--docker-timeout", type=float, default=60.0)
    return parser.parse_args(argv)


def _connector_policy(socket_path: Path) -> InferenceConnectorSpec:
    return InferenceConnectorSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_protocol_version": 1,
            "deployment_sha256": "a" * 64,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "routes": [
                {
                    "name": "fake-main",
                    "provider": "example-ai",
                    "protocol": "example-json",
                    "models": ["pinned-model"],
                    "credential_scope_sha256": "b" * 64,
                }
            ],
            "request_bytes": 4096,
            "response_bytes": 4096,
            "timeout_ms": 5000,
            "max_concurrency": 1,
        }
    )


def _provider_policy() -> InferenceGrantSpec:
    return InferenceGrantSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_name": "example-sidecar",
            "deployment_sha256": "a" * 64,
            "route": "fake-main",
            "credential_scope_sha256": "b" * 64,
            "provider": "example-ai",
            "protocol": "example-json",
            "model": "pinned-model",
            "request_bytes": 4096,
            "response_bytes": 4096,
            "timeout_ms": 5000,
            "max_concurrency": 1,
        }
    )


def _prepare_run(
    control: ControlPlane,
    socket_path: Path,
    policy: InferenceGrantSpec,
    image: str,
) -> tuple[str, AgentProfileSpec, dict[str, Any]]:
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
    control.register_inference_connector(
        name="example-sidecar",
        spec=_connector_policy(socket_path),
        created_by_handle="operator",
    )
    control.add_connector_provider_grant(
        name="example-main",
        spec=policy,
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
    attempt = database.begin_run_attempt(
        run["id"],
        runtime_adapter="command",
        owner_id="live-provider-proof",
        expected_run_version=run["version"],
    )
    return run["id"], profile, attempt


def _assert_proof(
    *,
    exit_code: int,
    connector_invocations: int,
    audit_invocations: list[dict[str, Any]],
    credential: str,
    scanned_surfaces: Mapping[str, bytes],
) -> None:
    if exit_code != 0:
        raise RuntimeError(f"live provider worker exited {exit_code}")
    if connector_invocations != 1:
        raise RuntimeError("connector did not receive exactly one pinned invocation")
    if (
        len(audit_invocations) != 1
        or audit_invocations[0]["outcome"] != "succeeded"
        or audit_invocations[0]["dispatch_kind"] != "connector"
        or audit_invocations[0]["provider"] != "example-ai"
        or audit_invocations[0]["protocol"] != "example-json"
        or audit_invocations[0]["model"] != "pinned-model"
        or audit_invocations[0]["route"] != "fake-main"
    ):
        raise RuntimeError("successful provider invocation was not audited")
    credential_bytes = credential.encode("ascii")
    for surface, content in scanned_surfaces.items():
        if credential_bytes in content:
            raise RuntimeError(f"provider secret leaked into {surface}")


def _collect_regular_files(root: Path) -> bytes:
    values: list[bytes] = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and not path.is_symlink():
            values.append(path.read_bytes())
    return b"".join(values)


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


def main() -> int:
    args = parse_args()
    if not hasattr(os, "getuid") or os.getuid() <= 0 or os.getgid() <= 0:
        raise RuntimeError("live provider proof requires a non-root POSIX user")
    credential = "live_" + secrets.token_urlsafe(32)
    repository = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(
        prefix="mgb-live-provider-",
        dir="/tmp",  # noqa: S108
    ) as temporary:
        proof_root = Path(temporary)
        os.chmod(proof_root, 0o700)
        connector_root = proof_root / "connector-private"
        connector_root.mkdir(mode=0o700)
        credential_path = connector_root / "provider.canary"
        credential_path.write_text(credential + "\n", encoding="ascii")
        os.chmod(credential_path, 0o600)
        socket_path = connector_root / "inference.sock"
        environment = {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(repository / "src"),
        }
        process = subprocess.Popen(  # noqa: S603 - fixed local proof argv
            [
                sys.executable,
                str(repository / "scripts" / "fake_inference_connector.py"),
                "--socket-path",
                str(socket_path),
                "--credential-file",
                str(credential_path),
                "--max-invocations",
                "1",
            ],
            cwd=repository,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if socket_path.exists() or process.poll() is not None:
                    break
                time.sleep(0.01)
            if not socket_path.exists():
                stdout, stderr = process.communicate(timeout=2)
                raise RuntimeError(
                    f"inference connector did not start: {stdout}{stderr}"
                )

            mgb_root = proof_root / "mgb-state"
            mgb_root.mkdir(mode=0o700)
            database = Database(mgb_root / "control-plane.db")
            control = ControlPlane(database)
            run_id, pinned_profile, attempt = _prepare_run(
                control,
                socket_path,
                _provider_policy(),
                args.image,
            )
            workspace = mgb_root / "workspace"
            workspace.mkdir(mode=0o700)
            (workspace / "provider_probe.py").write_text(
                AGENT_PROGRAM, encoding="utf-8"
            )
            manager = FileGatewayManager(mgb_root / "gateways", control)
            try:
                endpoint = manager.open(
                    run_id,
                    capabilities=pinned_profile.gateway_capabilities,
                    credential_names=pinned_profile.credential_names,
                )

                def before_start(plan: SandboxPlan, container_id: str) -> None:
                    database.start_run_attempt(
                        attempt["id"],
                        owner_id="live-provider-proof",
                        expected_attempt_version=attempt["version"],
                        expected_run_version=attempt["run_version"],
                        source_revision="live-proof",
                        container_id=container_id,
                        container_name=plan.container_name,
                        image_id=plan.image_id,
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
                    ),
                    before_start=before_start,
                )
                gateway_bytes = b"".join(
                    path.read_bytes() for path in endpoint.directory.iterdir()
                )
            finally:
                manager.cleanup(run_id)
            connector_stdout, connector_stderr = process.communicate(timeout=5)
            if process.returncode != 0 or connector_stderr:
                raise RuntimeError(
                    "inference connector failed: " + connector_stdout + connector_stderr
                )
            connector_report = json.loads(connector_stdout)
            invocations = database.list_provider_invocations(run_id)
            _assert_proof(
                exit_code=result.exit_code,
                connector_invocations=connector_report.get("invocations"),
                audit_invocations=invocations,
                credential=credential,
                scanned_surfaces={
                    "MGB state": _collect_regular_files(mgb_root),
                    "worker gateway": gateway_bytes,
                    "worker runtime output": result.stdout.encode("utf-8"),
                    "connector logs": (connector_stdout + connector_stderr).encode(
                        "utf-8"
                    ),
                },
            )
        finally:
            _stop_process(process)
    print(
        json.dumps(
            {
                "audited_invocations": 1,
                "credential_exposure": "none",
                "direct_network": "denied",
                "connector_dispatch": "exclusive",
                "permitted_provider": "reached through connector",
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
