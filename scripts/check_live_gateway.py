#!/usr/bin/env python3
"""Exercise the exact gateway mailbox through a networkless Docker worker."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from magic_glass_box.domain import AgentProfileSpec, GatewayCapability, NetworkMode
from magic_glass_box.executor import DockerCLI, DockerExecutor, SandboxRequest
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.runtime import RuntimeLaunch

RUN_ID = "run_" + "7" * 32

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

def replace(path, value):
    with path.open("r+b") as stream:
        stream.truncate()
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())

def exchange(sequence, operation, payload):
    request_id = f"live-proof-{sequence:04d}"
    request = {
        "operation": operation,
        "payload": payload,
        "request_id": request_id,
        "version": 1,
    }
    raw = (
        json.dumps(request, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()
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
                    response = json.loads(response_raw)
                    if response.get("request_id") != request_id:
                        raise SystemExit("gateway response correlation changed")
                    return response
        time.sleep(0.025)
    raise SystemExit("gateway response timed out")

described = exchange(1, "run.describe", {})
result = described.get("result", {})
if not (
    described.get("ok")
    and result.get("run_id") == os.environ["MAGIC_GLASS_BOX_RUN_ID"]
    and result.get("handoff") is None
    and result.get("recovery_checkpoint") is None
    and result.get("work_memory") is None
):
    raise SystemExit("gateway returned an invalid run description")
catalog_version = result.get("gateway_catalog", {}).get("catalog_version")
catalog_response = exchange(
    2,
    "gateway.catalog",
    {"catalog_version": catalog_version},
)
catalog = catalog_response.get("result", {})
if not (
    catalog_response.get("ok")
    and [item.get("operation") for item in catalog.get("operations", [])]
    == ["run.describe"]
    and catalog.get("schema_documents") == {}
    and catalog_response.get("field_provenance")
    == [
        {
            "path": "/result",
            "source": "gateway_enforcement",
            "trust_class": "authority",
        }
    ]
):
    raise SystemExit("gateway returned an invalid catalog")
denied = exchange(
    3,
    "provider.invoke",
    {"grant": "ungranted", "request": {"input": "must not dispatch"}},
)
if denied.get("ok") or denied.get("error", {}).get("code") != "forbidden":
    raise SystemExit("ungranted operation did not fail closed")
"""


class ProofDatabase:
    def __init__(self) -> None:
        self.describe_calls = 0

    def get_run_execution(self, run_id: str) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped the proof run")
        self.describe_calls += 1
        return {
            "state": "running",
            "version": 3,
            "source_revision": "a" * 40,
            "project_key": "gateway-proof",
            "context": {
                "purpose": "Prove the networkless gateway carrier",
                "instructions": [],
                "repository_path": "/host/path/must/not/escape",
                "default_branch": "main",
                "build_commands": [],
                "test_commands": [],
                "allowed_tools": [],
            },
            "profile": {
                "image": "redacted",
                "command": ["python"],
                "workspace_mode": "read_only",
                "network_mode": "gateway_only",
                "credential_names": [],
                "gateway_capabilities": ["run.describe"],
            },
            "work_item": None,
            "handoff": None,
            "recovery_checkpoint": None,
            "review_assignment": None,
            "changes_requested": None,
            "work_memory": None,
        }

    def get_run(self, run_id: str) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped the proof run")
        return {
            "id": RUN_ID,
            "state": "running",
            "version": 3,
            "interactions": [],
        }

    def get_run_interaction_page(
        self,
        run_id: str,
        *,
        limit: int,
        before_interaction_id: str | None = None,
    ) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped the proof run")
        if limit != 128 or before_interaction_id is not None:
            raise AssertionError("gateway requested an invalid interaction page")
        return {
            "interaction_count": 0,
            "interactions": [],
            "interactions_truncated": False,
            "next_before_interaction_id": None,
        }


class ProofControl:
    def __init__(self) -> None:
        self.database = ProofDatabase()

    def open_interaction(self, run_id: str, **kwargs: Any) -> dict[str, Any]:
        raise AssertionError("proof worker was not granted interaction.open")

    def read_interaction_response(self, run_id: str, interaction_id: str) -> Any:
        raise AssertionError("proof worker was not granted interaction.read")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="python:3.12-slim")
    parser.add_argument("--docker-timeout", type=float, default=60.0)
    return parser.parse_args(argv)


def _assert_proof(*, exit_code: int, describe_calls: int) -> None:
    if exit_code != 0:
        raise RuntimeError(f"live gateway worker exited {exit_code}")
    if describe_calls != 1:
        raise RuntimeError("live gateway request was not dispatched exactly once")


def main() -> int:
    args = parse_args()
    if not hasattr(os, "getuid") or os.getuid() <= 0 or os.getgid() <= 0:
        raise RuntimeError("live gateway proof requires a non-root POSIX user")
    control = ProofControl()
    with tempfile.TemporaryDirectory(
        prefix="mgb-live-gateway-",
        dir="/tmp",  # noqa: S108
    ) as temporary:
        root = Path(temporary)
        os.chmod(root, 0o700)
        workspace = root / "workspace"
        workspace.mkdir(mode=0o700)
        (workspace / "gateway_probe.py").write_text(AGENT_PROGRAM, encoding="utf-8")
        manager = FileGatewayManager(root / "gateways", control)
        try:
            endpoint = manager.open(
                RUN_ID,
                capabilities=(GatewayCapability.RUN_DESCRIBE,),
                credential_names=(),
            )
            profile = AgentProfileSpec.create(
                image=args.image,
                command=("python", "/workspace/gateway_probe.py"),
                network_mode=NetworkMode.GATEWAY_ONLY,
                credential_names=(),
                gateway_capabilities=(GatewayCapability.RUN_DESCRIBE,),
            )
            result = DockerExecutor(
                DockerCLI(
                    command_timeout=args.docker_timeout,
                    run_timeout=args.docker_timeout,
                )
            ).execute(
                SandboxRequest(
                    run_id=RUN_ID,
                    profile=profile,
                    workspace=workspace,
                    launch=RuntimeLaunch(profile.command),
                    credential_files={},
                    gateway_directory=endpoint.directory,
                )
            )
        finally:
            manager.cleanup(RUN_ID)
        _assert_proof(
            exit_code=result.exit_code,
            describe_calls=control.database.describe_calls,
        )
    print(
        json.dumps(
            {
                "direct_network": "denied",
                "catalog_discovered": True,
                "image_id": result.image_id,
                "status": "passed",
                "transport": "committed-file-mailbox",
                "ungranted_operation": "denied",
                "worker_gateway_exchanges": 3,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
