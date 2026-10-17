from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from magic_glass_box.inference import (
    InferenceInvocationRequest,
    InferenceInvocationResponse,
    UnixInferenceConnectorClient,
)
from scripts.fake_inference_connector import _response_for
from scripts.fake_inference_connector import main as fake_connector_main


def connect_when_ready(
    client: socket.socket,
    socket_path: Path,
    *,
    timeout: float = 3.0,
) -> None:
    """Connect after bind/listen readiness, not merely socket-file creation."""

    deadline = time.monotonic() + timeout
    while True:
        try:
            client.connect(str(socket_path))
            return
        except (ConnectionRefusedError, FileNotFoundError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)


class FakeInferenceConnectorProcessTests(unittest.TestCase):
    def test_v2_usage_respects_the_authorized_output_token_bound(self) -> None:
        request = InferenceInvocationRequest.from_mapping(
            {
                "schema_version": 1,
                "connector_protocol_version": 2,
                "invocation_id": "pinv_" + "1" * 32,
                "connector_id": "ifc_" + "2" * 32,
                "deployment_sha256": "a" * 64,
                "route": "chatgpt-main",
                "grant_id": "pgr_" + "3" * 32,
                "policy_sha256": "c" * 64,
                "credential_scope_sha256": "b" * 64,
                "provider": "openai",
                "protocol": "openai-responses",
                "model": "gpt-test",
                "deadline": (datetime.now(UTC) + timedelta(seconds=3)).isoformat(
                    timespec="microseconds"
                ),
                "request_limit_bytes": 1024,
                "response_limit_bytes": 32768,
                "budget": {
                    "policy_sha256": "d" * 64,
                    "currency": "USD",
                    "reservation_microusd": 1,
                    "max_output_tokens": 1,
                    "input_microusd_per_million_tokens": 0,
                    "cached_input_microusd_per_million_tokens": 0,
                    "output_microusd_per_million_tokens": 0,
                },
                "request": {"body": {"input": "hello"}},
            }
        )

        response = InferenceInvocationResponse.from_mapping(_response_for(request))

        self.assertIsNotNone(response.usage)
        assert response.usage is not None
        self.assertEqual(response.usage.output_tokens, 1)

    def test_fake_connector_refuses_to_replace_an_existing_path(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:
            existing = Path(temporary) / "do-not-delete"
            existing.write_text("operator data", encoding="utf-8")

            with self.assertRaisesRegex(SystemExit, "already exists"):
                fake_connector_main(["--socket-path", str(existing)])

            self.assertEqual(existing.read_text(encoding="utf-8"), "operator data")

    def test_fake_connector_rejects_a_non_private_credential_file(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:
            root = Path(temporary)
            credential = root / "provider.canary"
            credential.write_text("private-value\n", encoding="ascii")
            credential.chmod(0o644)

            with self.assertRaisesRegex(SystemExit, "private regular file"):
                fake_connector_main(
                    [
                        "--socket-path",
                        str(root / "fake.sock"),
                        "--credential-file",
                        str(credential),
                    ]
                )

    def test_separate_process_serves_one_deterministic_bounded_invocation(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:
            socket_path = Path(temporary) / "fake.sock"
            credential_path = Path(temporary) / "provider.canary"
            credential = "connector-only-canary"  # noqa: S105
            credential_path.write_text(credential + "\n", encoding="ascii")
            credential_path.chmod(0o600)
            environment = {
                "PATH": os.environ.get("PATH", ""),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": str(repository / "src"),
            }
            mutant = os.environ.get("MUTANT_UNDER_TEST")
            if mutant is not None:
                environment["MUTANT_UNDER_TEST"] = mutant
            process = subprocess.Popen(  # noqa: S603 - fixed local proof argv
                [
                    sys.executable,
                    str(repository / "scripts" / "fake_inference_connector.py"),
                    "--socket-path",
                    str(socket_path),
                    "--max-invocations",
                    "1",
                    "--credential-file",
                    str(credential_path),
                ],
                cwd=repository,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.addCleanup(self._stop_process, process)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                if socket_path.exists() or process.poll() is not None:
                    break
                time.sleep(0.01)
            if not socket_path.exists():
                stdout, stderr = process.communicate(timeout=1)
                self.fail(f"fake connector did not start: {stdout}{stderr}")

            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as malformed:
                connect_when_ready(malformed, socket_path)

            request = InferenceInvocationRequest.from_mapping(
                {
                    "schema_version": 1,
                    "connector_protocol_version": 1,
                    "invocation_id": "pinv_" + "1" * 32,
                    "connector_id": "ifc_" + "2" * 32,
                    "deployment_sha256": "a" * 64,
                    "route": "chatgpt-main",
                    "grant_id": "pgr_" + "3" * 32,
                    "policy_sha256": "c" * 64,
                    "credential_scope_sha256": "b" * 64,
                    "provider": "openai",
                    "protocol": "openai-responses",
                    "model": "gpt-test",
                    "deadline": (datetime.now(UTC) + timedelta(seconds=3)).isoformat(
                        timespec="microseconds"
                    ),
                    "request_limit_bytes": 16384,
                    "response_limit_bytes": 32768,
                    "request": {"body": {"input": "hello"}},
                }
            )

            response = UnixInferenceConnectorClient(socket_path).invoke(request)
            stdout, stderr = process.communicate(timeout=3)

        self.assertEqual(process.returncode, 0)
        self.assertEqual(stderr, "")
        self.assertNotIn(credential, stdout)
        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(
            response.untrusted_response,
            {"output_text": "deterministic fake response"},
        )
        self.assertEqual(
            json.loads(stdout),
            {"invocations": 1, "status": "stopped"},
        )

    @staticmethod
    def _stop_process(process: subprocess.Popen[str]) -> None:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1)


if __name__ == "__main__":
    unittest.main()
