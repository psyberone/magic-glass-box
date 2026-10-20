from __future__ import annotations

import copy
import json
import os
import socket
import stat
import struct
import tempfile
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from magic_glass_box import service_sidecar
from magic_glass_box.domain import ValidationError
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceOperationSpec,
    ServiceSidecarAmbiguousError,
    ServiceSidecarSpec,
    ServiceSidecarUnavailableError,
    UnixServiceSidecarClient,
)


def sidecar_spec(socket_path: Path, **overrides: object) -> ServiceSidecarSpec:
    value: dict[str, object] = {
        "schema_version": 1,
        "connector_protocol_version": 1,
        "deployment_sha256": "a" * 64,
        "transport": {"kind": "unix", "socket_path": str(socket_path)},
        "capabilities": [
            {"name": "change.open", "version": 1, "mode": "effect"},
            {"name": "checks.read", "version": 1, "mode": "read"},
        ],
        "mappings": [
            {
                "name": "primary",
                "service": "example-forge",
                "host": "forge.example.test",
                "repository": "acme/widgets",
                "account": "automation",
                "credential_scope_sha256": "b" * 64,
            }
        ],
        "request_bytes": 32768,
        "response_bytes": 65536,
        "timeout_ms": 30000,
        "max_concurrency": 2,
        "max_requests_per_minute": 30,
    }
    value.update(overrides)
    return ServiceSidecarSpec.from_mapping(value)


def operation_spec(**overrides: object) -> ServiceOperationSpec:
    value: dict[str, object] = {
        "schema_version": 1,
        "connector_name": "example-sidecar",
        "deployment_sha256": "a" * 64,
        "mapping": {
            "name": "primary",
            "service": "example-forge",
            "host": "forge.example.test",
            "repository": "acme/widgets",
            "account": "automation",
            "credential_scope_sha256": "b" * 64,
        },
        "capability": {"name": "change.open", "version": 1, "mode": "effect"},
        "idempotency_key": "open-candidate-42",
        "target": {
            "repository": "acme/widgets",
            "resource_id": "candidate-42",
            "object_format": "sha1",
            "expected_revision": "c" * 40,
            "desired_revision": "d" * 40,
        },
        "parameters": {"title": "Candidate 42", "draft": True},
        "request_bytes": 16384,
        "response_bytes": 32768,
        "timeout_ms": 10000,
    }
    value.update(overrides)
    return ServiceOperationSpec.from_mapping(value)


class ServiceSidecarContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.socket_path = Path(self.temporary.name) / "service.sock"

    def test_registration_and_operation_bind_exact_authority(self) -> None:
        connector = sidecar_spec(self.socket_path)
        operation = operation_spec()

        connector.validate_operation("example-sidecar", operation)
        self.assertEqual(
            ServiceSidecarSpec.from_mapping(connector.to_dict()), connector
        )
        self.assertEqual(
            ServiceOperationSpec.from_mapping(operation.to_dict()), operation
        )
        self.assertEqual(len(operation.input_sha256), 64)
        self.assertNotIn("credential", operation.parameters)

        mismatches = (
            {"connector_name": "other-sidecar"},
            {"deployment_sha256": "e" * 64},
            {
                "mapping": {
                    **operation.mapping.to_dict(),
                    "account": "wrong-account",
                }
            },
            {
                "mapping": {
                    **operation.mapping.to_dict(),
                    "credential_scope_sha256": "f" * 64,
                }
            },
            {
                "mapping": {
                    **operation.mapping.to_dict(),
                    "repository": "acme/wrong",
                },
                "target": {
                    **operation.target.to_dict(),
                    "repository": "acme/wrong",
                },
            },
            {
                "capability": {
                    "name": "landing.invoke",
                    "version": 1,
                    "mode": "effect",
                }
            },
        )
        for overrides in mismatches:
            with self.subTest(overrides=overrides):
                changed = operation_spec(**overrides)
                with self.assertRaises(ValidationError):
                    connector.validate_operation("example-sidecar", changed)

    def test_contracts_reject_ambiguous_or_credential_bearing_shapes(self) -> None:
        base = sidecar_spec(self.socket_path).to_dict()
        cases = []
        duplicate_capability = copy.deepcopy(base)
        duplicate_capability["capabilities"].append(
            copy.deepcopy(duplicate_capability["capabilities"][0])
        )
        cases.append(duplicate_capability)
        duplicate_mapping = copy.deepcopy(base)
        duplicate_mapping["mappings"].append(
            copy.deepcopy(duplicate_mapping["mappings"][0])
        )
        cases.append(duplicate_mapping)
        credential_value = copy.deepcopy(base)
        credential_value["api_token"] = "never-store-this"  # noqa: S105
        cases.append(credential_value)
        for candidate in cases:
            with self.subTest(candidate=candidate), self.assertRaises(ValidationError):
                ServiceSidecarSpec.from_mapping(candidate)

        for mapping_change in (
            {"host": "forge.example.test:65536"},
            {"host": "forge..example.test"},
            {"repository": "https://operator:secret@forge.example.test/acme/widgets"},
            {"repository": "acme/\x1fwidgets"},
            {"repository": "acme/\x7fwidgets"},
        ):
            unsafe_mapping = copy.deepcopy(base)
            unsafe_mapping["mappings"][0].update(mapping_change)
            with (
                self.subTest(mapping=mapping_change),
                self.assertRaises(ValidationError),
            ):
                ServiceSidecarSpec.from_mapping(unsafe_mapping)

        with self.assertRaises(ValidationError):
            operation_spec(
                target={
                    **operation_spec().target.to_dict(),
                    "repository": "acme/other",
                }
            )
        with self.assertRaises(ValidationError):
            operation_spec(
                target={
                    **operation_spec().target.to_dict(),
                    "desired_revision": "not-an-object-id",
                }
            )
        with self.assertRaises(ValidationError):
            operation_spec(parameters={"access_token": "never-store-this"})
        for parameters in (
            {"nested": {"apiKey": "never-store-this"}},
            {"credential-ref": "file:/private/credentials"},
        ):
            with (
                self.subTest(parameters=parameters),
                self.assertRaises(ValidationError),
            ):
                operation_spec(parameters=parameters)
        with self.assertRaises(ValidationError):
            operation_spec(
                capability={"name": "change/invalid", "version": 1, "mode": "effect"}
            )
        oversized = operation_spec(request_bytes=32769)
        with self.assertRaises(ValidationError):
            sidecar_spec(self.socket_path).validate_operation(
                "example-sidecar", oversized
            )

        accepted = sidecar_spec(
            self.socket_path,
            mappings=[
                {
                    **sidecar_spec(self.socket_path).mappings[0].to_dict(),
                    "host": "FORGE.EXAMPLE.TEST:65535",
                }
            ],
        )
        self.assertEqual(accepted.mappings[0].host, "forge.example.test:65535")

    def test_wire_json_rejects_duplicates_non_finite_values_and_invalid_utf8(
        self,
    ) -> None:
        cases = (
            (b'{"field":1,"field":2}', "duplicate fields"),
            (b'{"field":NaN}', "finite JSON"),
            (b"{\xff}", "UTF-8 JSON"),
        )
        for payload, message in cases:
            with (
                self.subTest(payload=payload),
                self.assertRaisesRegex(ValidationError, message),
            ):
                service_sidecar._strict_json_object(payload)

    def test_response_is_typed_bounded_and_correlated(self) -> None:
        operation = operation_spec()
        deadline = (datetime.now(UTC) + timedelta(seconds=30)).isoformat(
            timespec="microseconds"
        )
        request = ServiceOperationRequest.create(
            operation_id="sop_" + "1" * 32,
            connector_id="ssc_" + "2" * 32,
            connector_protocol_version=1,
            operation=operation,
            phase="invoke",
            deadline=deadline,
        )
        response_value = request.correlation_dict()
        response_value.update(
            {
                "outcome": "succeeded",
                "provider_status": 201,
                "request_bytes": 512,
                "response_bytes": 128,
                "error_code": None,
                "retry_after_seconds": None,
                "receipt": {
                    "remote_identity": "change-42",
                    "observed_revision": "d" * 40,
                    "evidence_sha256": "e" * 64,
                    "closed_references": [],
                },
            }
        )
        response = ServiceOperationResponse.from_mapping(response_value)
        response.validate_correlation(request)
        self.assertEqual(response.receipt.remote_identity, "change-42")
        self.assertNotIn("body", response.to_dict())

        wrong = copy.deepcopy(response_value)
        wrong["input_sha256"] = "f" * 64
        with self.assertRaises(ValidationError):
            ServiceOperationResponse.from_mapping(wrong).validate_correlation(request)

        inconsistent = copy.deepcopy(response_value)
        inconsistent["outcome"] = "uncertain"
        with self.assertRaises(ValidationError):
            ServiceOperationResponse.from_mapping(inconsistent)

        not_found = copy.deepcopy(response_value)
        not_found.update(
            {
                "phase": "reconcile",
                "outcome": "not_found",
                "provider_status": 404,
                "error_code": "not_found",
                "receipt": None,
            }
        )
        reconciled = ServiceOperationResponse.from_mapping(not_found)
        self.assertEqual(reconciled.outcome, "not_found")

    def test_unix_transport_is_bounded_and_post_dispatch_loss_is_ambiguous(
        self,
    ) -> None:
        request = self._request()
        with self.assertRaises(ServiceSidecarUnavailableError):
            UnixServiceSidecarClient(self.socket_path).invoke(request)

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o600)
        listener.listen(1)

        def lose_reply() -> None:
            with listener:
                connection, _ = listener.accept()
                with connection:
                    length = struct.unpack("!I", self._recv_exact(connection, 4))[0]
                    self._recv_exact(connection, length)

        thread = threading.Thread(target=lose_reply, daemon=True)
        thread.start()
        with self.assertRaises(ServiceSidecarAmbiguousError):
            UnixServiceSidecarClient(self.socket_path).invoke(request)
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())

    def test_unix_transport_rejects_a_non_private_socket_before_connect(self) -> None:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(listener.close)
        listener.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o666)  # noqa: S103 - unsafe test fixture
        listener.listen(1)

        with self.assertRaises(ServiceSidecarUnavailableError):
            UnixServiceSidecarClient(self.socket_path).invoke(self._request())

    def test_unix_transport_rejects_an_unsafe_parent_and_a_regular_file(self) -> None:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o600)
        listener.listen(1)
        os.chmod(
            self.socket_path.parent,
            0o755,  # noqa: S103 - deliberately unsafe boundary fixture
        )
        try:
            with self.assertRaisesRegex(
                ServiceSidecarUnavailableError, "private Unix socket"
            ):
                UnixServiceSidecarClient(self.socket_path).invoke(
                    self._request(
                        deadline=(
                            datetime.now(UTC) + timedelta(milliseconds=200)
                        ).isoformat(timespec="microseconds")
                    )
                )
        finally:
            os.chmod(self.socket_path.parent, 0o700)
            listener.close()
            self.socket_path.unlink(missing_ok=True)

        self.socket_path.write_text("not a socket", encoding="utf-8")
        self.socket_path.chmod(0o600)
        with self.assertRaisesRegex(
            ServiceSidecarUnavailableError, "private Unix socket"
        ):
            UnixServiceSidecarClient(self.socket_path).invoke(self._request())

    def test_unix_transport_rejects_a_socket_owned_by_another_user(self) -> None:
        socket_info = SimpleNamespace(st_mode=stat.S_IFSOCK | 0o600, st_uid=502)
        parent_info = SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=501)
        with (
            mock.patch.object(Path, "lstat", side_effect=[socket_info, parent_info]),
            mock.patch("magic_glass_box.service_sidecar.os.getuid", return_value=501),
            self.assertRaisesRegex(
                ServiceSidecarUnavailableError, "private Unix socket"
            ),
        ):
            service_sidecar._validate_unix_socket(str(self.socket_path))

    def test_unix_transport_accepts_only_a_correlated_typed_response(self) -> None:
        request = self._request()
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o600)
        listener.listen(1)

        def reply() -> None:
            with listener:
                connection, _ = listener.accept()
                with connection:
                    length = struct.unpack("!I", self._recv_exact(connection, 4))[0]
                    received = json.loads(
                        self._recv_exact(connection, length).decode("utf-8")
                    )
                    response = {
                        key: received[key] for key in request.correlation_dict()
                    }
                    response.update(
                        {
                            "outcome": "succeeded",
                            "provider_status": 201,
                            "request_bytes": 512,
                            "response_bytes": 128,
                            "error_code": None,
                            "retry_after_seconds": None,
                            "receipt": {
                                "remote_identity": "change-42",
                                "observed_revision": "d" * 40,
                                "evidence_sha256": "e" * 64,
                                "closed_references": [],
                            },
                        }
                    )
                    payload = json.dumps(
                        response, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8")
                    connection.sendall(struct.pack("!I", len(payload)) + payload)

        thread = threading.Thread(target=reply, daemon=True)
        thread.start()
        response = UnixServiceSidecarClient(self.socket_path).invoke(request)
        response.validate_correlation(request)
        self.assertEqual(response.receipt.remote_identity, "change-42")
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())

    def test_unix_transport_enforces_deadline_and_socket_timeout_authority(
        self,
    ) -> None:
        expired = self._request(
            deadline=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(
                timespec="microseconds"
            )
        )
        with (
            mock.patch("magic_glass_box.service_sidecar.socket.socket") as factory,
            self.assertRaisesRegex(
                ServiceSidecarUnavailableError, "deadline has expired"
            ),
        ):
            UnixServiceSidecarClient(self.socket_path).invoke(expired)
        factory.assert_not_called()

        connection = mock.Mock()
        connection.sendall.side_effect = OSError("simulated post-dispatch loss")
        with (
            mock.patch(
                "magic_glass_box.service_sidecar._validate_unix_socket"
            ) as validate,
            mock.patch(
                "magic_glass_box.service_sidecar._remaining_timeout",
                side_effect=[4.0, 3.0],
            ),
            mock.patch(
                "magic_glass_box.service_sidecar.socket.socket",
                return_value=connection,
            ) as factory,
            self.assertRaises(ServiceSidecarAmbiguousError),
        ):
            UnixServiceSidecarClient(self.socket_path).invoke(self._request())

        validate.assert_called_once_with(str(self.socket_path))
        factory.assert_called_once_with(socket.AF_UNIX, socket.SOCK_STREAM)
        self.assertEqual(
            connection.settimeout.call_args_list,
            [mock.call(4.0), mock.call(3.0)],
        )
        connection.close.assert_called_once_with()

    def _request(self, *, deadline: str | None = None) -> ServiceOperationRequest:
        return ServiceOperationRequest.create(
            operation_id="sop_" + "1" * 32,
            connector_id="ssc_" + "2" * 32,
            connector_protocol_version=1,
            operation=operation_spec(),
            phase="invoke",
            deadline=deadline
            or (datetime.now(UTC) + timedelta(seconds=5)).isoformat(
                timespec="microseconds"
            ),
        )

    @staticmethod
    def _recv_exact(connection: socket.socket, length: int) -> bytes:
        chunks: list[bytes] = []
        remaining = length
        while remaining:
            chunk = connection.recv(remaining)
            if not chunk:
                raise RuntimeError("test peer closed early")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)


if __name__ == "__main__":
    unittest.main()
