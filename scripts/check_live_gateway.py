#!/usr/bin/env python3
"""Exercise the exact gateway mailbox through a networkless Docker worker."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from magic_glass_box.domain import AgentProfileSpec, GatewayCapability, NetworkMode
from magic_glass_box.executor import DockerCLI, DockerExecutor, SandboxRequest
from magic_glass_box.gateway import FileGatewayManager

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
request = {
    "operation": "run.describe",
    "payload": {},
    "request_id": "live-proof-0001",
    "version": 1,
}
raw = (json.dumps(request, separators=(",", ":"), sort_keys=True) + "\n").encode()
marker = {
    "sequence": 1,
    "sha256": hashlib.sha256(raw).hexdigest(),
    "size": len(raw),
}

def replace(path, value):
    with path.open("r+b") as stream:
        stream.truncate()
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())

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
        if ready.get("sequence") == 1:
            response_raw = (root / "response.json").read_bytes()
            if (
                len(response_raw) == ready["size"]
                and hashlib.sha256(response_raw).hexdigest() == ready["sha256"]
            ):
                response = json.loads(response_raw)
                if response["ok"] and response["result"]["run_id"] == os.environ[
                    "MAGIC_GLASS_BOX_RUN_ID"
                ]:
                    raise SystemExit(0)
                raise SystemExit("gateway returned an invalid response")
    time.sleep(0.025)
raise SystemExit("gateway response timed out")
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


class ProofControl:
    def __init__(self) -> None:
        self.database = ProofDatabase()

    def open_interaction(self, run_id: str, **kwargs: Any) -> dict[str, Any]:
        raise AssertionError("proof worker was not granted interaction.open")

    def read_interaction_response(self, run_id: str, interaction_id: str) -> Any:
        raise AssertionError("proof worker was not granted interaction.read")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="python:3.12-slim")
    parser.add_argument("--docker-timeout", type=float, default=60.0)
    return parser.parse_args()


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
            result = DockerExecutor(DockerCLI(timeout=args.docker_timeout)).execute(
                SandboxRequest(
                    run_id=RUN_ID,
                    profile=profile,
                    workspace=workspace,
                    credential_files={},
                    gateway_directory=endpoint.directory,
                )
            )
        finally:
            manager.cleanup(RUN_ID)
        if result.exit_code != 0:
            raise RuntimeError(f"live gateway worker exited {result.exit_code}")
        if control.database.describe_calls != 1:
            raise RuntimeError("live gateway request was not dispatched exactly once")
    print(
        json.dumps(
            {
                "direct_network": "denied",
                "gateway_requests": control.database.describe_calls,
                "image_id": result.image_id,
                "status": "passed",
                "transport": "committed-file-mailbox",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
