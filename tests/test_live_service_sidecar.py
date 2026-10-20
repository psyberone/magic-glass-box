from __future__ import annotations

import json
import os
import socket
import struct
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any

from magic_glass_box.db import Database
from magic_glass_box.domain import ValidationError, canonical_json
from magic_glass_box.service import ControlPlane
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceSidecarAmbiguousError,
)
from magic_glass_box.sidecar import serve_unix_socket
from tests.test_service_sidecars import operation_spec, sidecar_spec


class LiveServiceSidecarProofTests(unittest.TestCase):
    def test_lost_success_is_reconciled_over_real_unix_transport(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            os.chmod(root, 0o700)
            socket_path = root / "service.sock"
            database = Database(root / "control-plane.db")
            database.initialize()
            user = database.create_user("operator", "Operator")
            database.register_service_sidecar(
                name="example-sidecar",
                spec=sidecar_spec(socket_path),
                created_by=user["id"],
            )
            operation = database.create_service_operation(
                spec=operation_spec(),
                created_by=user["id"],
            )
            remote: dict[str, dict[str, Any]] = {}
            create_count = 0

            def handler(connection: socket.socket) -> None:
                nonlocal create_count
                length = struct.unpack("!I", self._recv_exact(connection, 4))[0]
                request = ServiceOperationRequest.from_mapping(
                    json.loads(self._recv_exact(connection, length).decode("utf-8"))
                )
                if request.phase == "invoke":
                    if request.input_sha256 not in remote:
                        create_count += 1
                        remote[request.input_sha256] = {
                            "remote_identity": "change-42",
                            "observed_revision": "d" * 40,
                            "evidence_sha256": "e" * 64,
                            "closed_references": [],
                        }
                    return
                response = request.correlation_dict()
                response.update(
                    {
                        "outcome": "succeeded",
                        "provider_status": 200,
                        "request_bytes": len(request.canonical.encode("utf-8")),
                        "response_bytes": len(
                            canonical_json(remote[request.input_sha256]).encode("utf-8")
                        ),
                        "error_code": None,
                        "retry_after_seconds": None,
                        "receipt": remote[request.input_sha256],
                    }
                )
                payload = canonical_json(response).encode("utf-8")
                connection.sendall(struct.pack("!I", len(payload)) + payload)

            completed: list[int] = []

            def serve() -> None:
                completed.append(
                    serve_unix_socket(
                        socket_path,
                        2,
                        handler,
                        recoverable_errors=(ValidationError,),
                    )
                )

            server = threading.Thread(target=serve, daemon=True)
            server.start()
            deadline = time.monotonic() + 10
            while not socket_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(socket_path.exists())

            first_supervisor = ControlPlane(database)
            with self.assertRaises(ServiceSidecarAmbiguousError):
                first_supervisor.execute_service_operation(
                    operation["id"],
                    owner_id="supervisor-one",
                    expected_version=1,
                )
            uncertain = database.get_service_operation(operation["id"])
            self.assertEqual(uncertain["state"], "uncertain")

            replacement_supervisor = ControlPlane(database)
            recovered = replacement_supervisor.reconcile_service_operation(
                operation["id"],
                owner_id="supervisor-two",
                expected_version=uncertain["version"],
            )
            server.join(timeout=10)

            self.assertFalse(server.is_alive())
            self.assertEqual(completed, [2])
            self.assertEqual(create_count, 1)
            self.assertEqual(len(remote), 1)
            self.assertEqual(recovered["state"], "succeeded")
            self.assertEqual(
                [attempt["outcome"] for attempt in recovered["attempts"]],
                ["ambiguous", "succeeded"],
            )
            self.assertFalse(socket_path.exists())

    @staticmethod
    def _recv_exact(connection: socket.socket, length: int) -> bytes:
        chunks: list[bytes] = []
        remaining = length
        while remaining:
            chunk = connection.recv(remaining)
            if not chunk:
                raise RuntimeError("fake service sidecar peer closed early")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)


if __name__ == "__main__":
    unittest.main()
