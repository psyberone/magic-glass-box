from __future__ import annotations

import copy
import json
import socket
import struct
import tempfile
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from magic_glass_box.domain import ValidationError
from magic_glass_box.inference import (
    InferenceConnectorAmbiguousError,
    InferenceConnectorError,
    InferenceConnectorSpec,
    InferenceConnectorUnavailableError,
    InferenceGrantSpec,
    InferenceInvocationRequest,
    InferenceInvocationResponse,
    UnixInferenceConnectorClient,
)


def connector_mapping(socket_path: Path) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "connector_protocol_version": 1,
        "deployment_sha256": "a" * 64,
        "transport": {"kind": "unix", "socket_path": str(socket_path)},
        "routes": [
            {
                "name": "chatgpt-main",
                "provider": "openai",
                "protocol": "openai-responses",
                "models": ["gpt-test", "gpt-test-mini"],
                "credential_scope_sha256": "b" * 64,
            }
        ],
        "request_bytes": 32768,
        "response_bytes": 65536,
        "timeout_ms": 30000,
        "max_concurrency": 2,
    }


def grant_mapping() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "connector_name": "openai-personal",
        "deployment_sha256": "a" * 64,
        "route": "chatgpt-main",
        "credential_scope_sha256": "b" * 64,
        "provider": "openai",
        "protocol": "openai-responses",
        "model": "gpt-test",
        "request_bytes": 16384,
        "response_bytes": 32768,
        "timeout_ms": 10000,
        "max_concurrency": 1,
    }


def request_mapping() -> dict[str, Any]:
    deadline = datetime.now(UTC) + timedelta(seconds=10)
    return {
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
        "deadline": deadline.isoformat(timespec="microseconds"),
        "request_limit_bytes": 16384,
        "response_limit_bytes": 32768,
        "request": {
            "protocol": "openai-responses",
            "body": {"input": "hello", "stream": True},
            "headers": {},
        },
    }


def response_for(
    request: InferenceInvocationRequest,
    *,
    outcome: str = "succeeded",
) -> dict[str, Any]:
    value = {
        key: request.to_dict()[key]
        for key in (
            "schema_version",
            "connector_protocol_version",
            "invocation_id",
            "connector_id",
            "deployment_sha256",
            "route",
            "grant_id",
            "policy_sha256",
            "credential_scope_sha256",
            "provider",
            "protocol",
            "model",
            "deadline",
            "request_limit_bytes",
            "response_limit_bytes",
        )
    }
    value.update(
        {
            "outcome": outcome,
            "provider_status": 200 if outcome == "succeeded" else 503,
            "request_bytes": 83,
            "response_bytes": 32,
            "error_code": None if outcome == "succeeded" else "upstream_error",
            "response": {"output_text": "done"} if outcome == "succeeded" else None,
        }
    )
    return value


def read_frame(connection: socket.socket) -> dict[str, Any]:
    header = connection.recv(4)
    if len(header) != 4:
        raise AssertionError("missing frame header")
    remaining = struct.unpack("!I", header)[0]
    chunks: list[bytes] = []
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise AssertionError("truncated frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    value = json.loads(b"".join(chunks))
    if not isinstance(value, dict):
        raise AssertionError("frame is not an object")
    return value


def write_frame(connection: socket.socket, value: dict[str, Any]) -> None:
    payload = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    connection.sendall(struct.pack("!I", len(payload)) + payload)


class InferenceContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.socket_path = Path(self.temporary.name) / "inference.sock"

    def test_connector_and_grant_specs_are_strict_bounded_and_compatible(
        self,
    ) -> None:
        connector = InferenceConnectorSpec.from_mapping(
            connector_mapping(self.socket_path)
        )
        grant = InferenceGrantSpec.from_mapping(grant_mapping())

        self.assertEqual(connector.to_dict(), connector_mapping(self.socket_path))
        self.assertEqual(grant.to_dict(), grant_mapping())
        connector.validate_grant("openai-personal", grant)
        self.assertEqual(len(connector.digest), 64)
        self.assertEqual(len(grant.digest), 64)
        self.assertNotIn(
            "credential",
            connector.canonical.lower().replace("credential_scope", "scope"),
        )

    def test_connector_and_grant_reject_unknown_unsafe_and_unapproved_values(
        self,
    ) -> None:
        connector_cases = (
            ({"extra": True}, "unknown fields"),
            ({"connector_protocol_version": 2}, "protocol_version"),
            ({"deployment_sha256": "not-a-digest"}, "deployment"),
            (
                {"transport": {"kind": "unix", "socket_path": "relative.sock"}},
                "absolute",
            ),
            ({"routes": []}, "routes"),
            ({"request_bytes": 0}, "request_bytes"),
            ({"max_concurrency": 17}, "max_concurrency"),
        )
        for override, message in connector_cases:
            candidate = connector_mapping(self.socket_path)
            candidate.update(override)
            with (
                self.subTest(connector=override),
                self.assertRaisesRegex(ValidationError, message),
            ):
                InferenceConnectorSpec.from_mapping(candidate)

        grant_cases = (
            ({"extra": True}, "unknown fields"),
            ({"connector_name": "Bad Name"}, "connector_name"),
            ({"model": " \n "}, "model"),
            ({"credential_scope_sha256": "secret"}, "scope"),
            ({"timeout_ms": 121000}, "timeout_ms"),
        )
        for override, message in grant_cases:
            candidate = grant_mapping()
            candidate.update(override)
            with (
                self.subTest(grant=override),
                self.assertRaisesRegex(ValidationError, message),
            ):
                InferenceGrantSpec.from_mapping(candidate)

        connector = InferenceConnectorSpec.from_mapping(
            connector_mapping(self.socket_path)
        )
        for override, message in (
            ({"deployment_sha256": "d" * 64}, "deployment"),
            ({"route": "other"}, "route"),
            ({"model": "not-approved"}, "model"),
            ({"request_bytes": 65536}, "request_bytes"),
            ({"max_concurrency": 3}, "max_concurrency"),
        ):
            incompatible = grant_mapping()
            incompatible.update(override)
            with (
                self.subTest(incompatible=override),
                self.assertRaisesRegex(ValidationError, message),
            ):
                connector.validate_grant(
                    "openai-personal",
                    InferenceGrantSpec.from_mapping(incompatible),
                )

    def test_invocation_envelopes_round_trip_and_enforce_consistency(self) -> None:
        request_value = request_mapping()
        request = InferenceInvocationRequest.from_mapping(request_value)
        self.assertEqual(request.to_dict(), request_value)
        response = InferenceInvocationResponse.from_mapping(response_for(request))
        self.assertEqual(response.untrusted_response, {"output_text": "done"})
        response.validate_correlation(request)

        for change, message in (
            ({"model": " \n "}, "model"),
            ({"request_limit_bytes": 1}, "request"),
            ({"deadline": "not-a-time"}, "deadline"),
            ({"request": {"input": float("nan")}}, "finite JSON"),
        ):
            candidate = request_mapping()
            candidate.update(change)
            with (
                self.subTest(request=change),
                self.assertRaisesRegex(ValidationError, message),
            ):
                InferenceInvocationRequest.from_mapping(candidate)

        inconsistent = response_for(request)
        inconsistent["error_code"] = "should-not-exist"
        with self.assertRaisesRegex(ValidationError, "successful"):
            InferenceInvocationResponse.from_mapping(inconsistent)

        failed = response_for(request, outcome="failed")
        failed["response"] = {"must": "not survive"}
        with self.assertRaisesRegex(ValidationError, "failed"):
            InferenceInvocationResponse.from_mapping(failed)

        mismatched = InferenceInvocationResponse.from_mapping(response_for(request))
        different = copy.deepcopy(request_mapping())
        different["policy_sha256"] = "d" * 64
        with self.assertRaisesRegex(ValidationError, "correlation"):
            mismatched.validate_correlation(
                InferenceInvocationRequest.from_mapping(different)
            )

    def test_invocation_envelopes_do_not_share_mutable_content(self) -> None:
        request_value = request_mapping()
        request = InferenceInvocationRequest.from_mapping(request_value)
        request_value["request"]["body"]["input"] = "changed by caller"
        request.request["body"]["input"] = "changed through property"
        exported_request = request.to_dict()
        exported_request["request"]["body"]["input"] = "changed after export"

        self.assertEqual(request.request["body"]["input"], "hello")
        self.assertEqual(request.to_dict()["request"]["body"]["input"], "hello")

        response_value = response_for(request)
        response = InferenceInvocationResponse.from_mapping(response_value)
        response_value["response"]["output_text"] = "changed by connector"
        assert response.untrusted_response is not None
        response.untrusted_response["output_text"] = "changed through property"
        exported_response = response.to_dict()
        exported_response["response"]["output_text"] = "changed after export"

        self.assertEqual(response.untrusted_response, {"output_text": "done"})
        self.assertEqual(response.to_dict()["response"], {"output_text": "done"})


class UnixInferenceConnectorClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.socket_path = Path(self.temporary.name) / "inference.sock"
        self.request = InferenceInvocationRequest.from_mapping(request_mapping())

    def start_server(self, responder: Any) -> tuple[threading.Thread, threading.Event]:
        ready = threading.Event()

        def serve() -> None:
            self.socket_path.unlink(missing_ok=True)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(self.socket_path))
                listener.listen(1)
                ready.set()
                connection, _ = listener.accept()
                with connection:
                    responder(connection)

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(1))
        return thread, ready

    def test_one_framed_invocation_succeeds_without_a_credential(self) -> None:
        seen: list[dict[str, Any]] = []

        def respond(connection: socket.socket) -> None:
            seen.append(read_frame(connection))
            parsed = InferenceInvocationRequest.from_mapping(seen[0])
            write_frame(connection, response_for(parsed))

        thread, _ = self.start_server(respond)
        response = UnixInferenceConnectorClient(self.socket_path).invoke(self.request)
        thread.join(1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(response.untrusted_response, {"output_text": "done"})
        self.assertEqual(seen, [self.request.to_dict()])
        serialized = json.dumps(seen)
        for forbidden in ("api_key", "access_token", "credential_ref", "secret"):
            self.assertNotIn(forbidden, serialized)

    def test_unavailable_before_connect_is_distinct_from_ambiguous_after_send(
        self,
    ) -> None:
        client = UnixInferenceConnectorClient(self.socket_path)
        with self.assertRaises(InferenceConnectorUnavailableError):
            client.invoke(self.request)

        with (
            mock.patch(
                "magic_glass_box.inference.socket.socket",
                side_effect=OSError("socket creation failed"),
            ),
            self.assertRaisesRegex(InferenceConnectorUnavailableError, "unavailable"),
        ):
            client.invoke(self.request)

        class FailsBeforeSend:
            def __init__(self) -> None:
                self.timeout_calls = 0
                self.sent = False
                self.closed = False

            def settimeout(self, _timeout: float) -> None:
                self.timeout_calls += 1
                if self.timeout_calls == 2:
                    raise TimeoutError("expired before send")

            def connect(self, _path: str) -> None:
                return None

            def sendall(self, _payload: bytes) -> None:
                self.sent = True

            def close(self) -> None:
                self.closed = True

        before_send = FailsBeforeSend()
        with (
            mock.patch(
                "magic_glass_box.inference.socket.socket", return_value=before_send
            ),
            self.assertRaises(InferenceConnectorUnavailableError),
        ):
            client.invoke(self.request)
        self.assertFalse(before_send.sent)
        self.assertTrue(before_send.closed)

        def close_after_read(connection: socket.socket) -> None:
            read_frame(connection)

        thread, _ = self.start_server(close_after_read)
        with self.assertRaisesRegex(InferenceConnectorAmbiguousError, "ambiguous"):
            client.invoke(self.request)
        thread.join(1)

    def test_malformed_oversized_and_mismatched_responses_fail_ambiguous(
        self,
    ) -> None:
        def malformed(connection: socket.socket) -> None:
            read_frame(connection)
            payload = b'{"outcome":"failed","outcome":"succeeded"}'
            connection.sendall(struct.pack("!I", len(payload)) + payload)

        thread, _ = self.start_server(malformed)
        with self.assertRaisesRegex(InferenceConnectorAmbiguousError, "ambiguous"):
            UnixInferenceConnectorClient(self.socket_path).invoke(self.request)
        thread.join(1)

        def oversized(connection: socket.socket) -> None:
            read_frame(connection)
            connection.sendall(struct.pack("!I", 10 * 1024 * 1024))

        thread, _ = self.start_server(oversized)
        with self.assertRaisesRegex(InferenceConnectorAmbiguousError, "ambiguous"):
            UnixInferenceConnectorClient(self.socket_path).invoke(self.request)
        thread.join(1)

        def mismatched(connection: socket.socket) -> None:
            read_frame(connection)
            value = response_for(self.request)
            value["connector_id"] = "ifc_" + "f" * 32
            write_frame(connection, value)

        thread, _ = self.start_server(mismatched)
        with self.assertRaisesRegex(InferenceConnectorAmbiguousError, "ambiguous"):
            UnixInferenceConnectorClient(self.socket_path).invoke(self.request)
        thread.join(1)

    def test_expired_deadline_and_invalid_socket_path_fail_before_transport(
        self,
    ) -> None:
        expired = request_mapping()
        expired["deadline"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat(
            timespec="microseconds"
        )
        with self.assertRaisesRegex(InferenceConnectorUnavailableError, "deadline"):
            UnixInferenceConnectorClient(self.socket_path).invoke(
                InferenceInvocationRequest.from_mapping(expired)
            )
        with self.assertRaisesRegex(InferenceConnectorError, "absolute"):
            UnixInferenceConnectorClient(Path("relative.sock"))

    def test_each_transport_operation_uses_the_remaining_absolute_deadline(
        self,
    ) -> None:
        response_payload = json.dumps(
            response_for(self.request), separators=(",", ":"), sort_keys=True
        ).encode()

        class RecordingSocket:
            def __init__(self) -> None:
                self.timeouts: list[float] = []
                self.receives = [
                    struct.pack("!I", len(response_payload)),
                    response_payload,
                ]
                self.sent = b""
                self.closed = False

            def settimeout(self, timeout: float) -> None:
                self.timeouts.append(timeout)

            def connect(self, path: str) -> None:
                self.path = path

            def sendall(self, payload: bytes) -> None:
                self.sent = payload

            def recv(self, length: int) -> bytes:
                chunk = self.receives.pop(0)
                self.assert_length = length
                return chunk

            def close(self) -> None:
                self.closed = True

        deadline = datetime.fromisoformat(self.request.deadline)

        class SteppingDateTime:
            current = deadline - timedelta(seconds=5)

            @classmethod
            def fromisoformat(cls, value: str) -> datetime:
                return datetime.fromisoformat(value)

            @classmethod
            def now(cls, _timezone: object) -> datetime:
                result = cls.current
                cls.current += timedelta(milliseconds=250)
                return result

        connection = RecordingSocket()
        with (
            mock.patch(
                "magic_glass_box.inference.socket.socket", return_value=connection
            ),
            mock.patch("magic_glass_box.inference.datetime", SteppingDateTime),
        ):
            response = UnixInferenceConnectorClient(self.socket_path).invoke(
                self.request
            )

        self.assertEqual(response.outcome, "succeeded")
        self.assertTrue(connection.closed)
        self.assertGreaterEqual(len(connection.timeouts), 4)
        self.assertTrue(
            all(
                earlier > later > 0
                for earlier, later in zip(
                    connection.timeouts,
                    connection.timeouts[1:],
                    strict=False,
                )
            )
        )


if __name__ == "__main__":
    unittest.main()
