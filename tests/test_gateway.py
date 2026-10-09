from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from magic_glass_box.db import ConflictError, NotFoundError, StorageError
from magic_glass_box.domain import GatewayCapability, ValidationError, canonical_json
from magic_glass_box.gateway import (
    GATEWAY_REQUEST_NAME,
    GATEWAY_REQUEST_READY_NAME,
    GATEWAY_RESPONSE_NAME,
    GATEWAY_RESPONSE_READY_NAME,
    MAX_GATEWAY_REQUEST_BYTES,
    FileGatewayManager,
    GatewayError,
    RunGateway,
    default_gateway_root,
)
from magic_glass_box.krab import KrabError
from magic_glass_box.service import AcceptedInteractionResponse

RUN_ID = "run_" + "1" * 32
OTHER_RUN_ID = "run_" + "2" * 32
INTERACTION_ID = "int_" + "3" * 32


class FakeGatewayDatabase:
    def __init__(self) -> None:
        self.interactions = [
            {
                "id": INTERACTION_ID,
                "kind": "question",
                "state": "answered",
                "version": 2,
                "request_item_id": "itm_" + "6" * 32,
                "response_item_id": "itm_" + "7" * 32,
                "recipient": {"krab_handle": "reviewer"},
            }
        ]

    def get_run_execution(self, run_id: str) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped its bound run")
        return {
            "id": RUN_ID,
            "state": "running",
            "version": 3,
            "source_revision": "a" * 40,
            "project_key": "box-project",
            "context": {
                "purpose": "Build the box",
                "instructions": ["Keep changes focused"],
                "repository_path": "/private/host/repository",
                "default_branch": "main",
                "build_commands": [],
                "test_commands": [["make", "quality"]],
                "allowed_tools": ["git"],
            },
            "profile": {
                "image": "agent@sha256:secret-host-policy",
                "command": ["agent", "run"],
                "workspace_mode": "copy_on_write",
                "network_mode": "gateway_only",
                "credential_names": ["krab"],
                "gateway_capabilities": [
                    "run.describe",
                    "interaction.open",
                    "interaction.read",
                ],
            },
            "work_item_id": "wki_" + "4" * 32,
            "work_item_version_id": "wiv_" + "5" * 32,
            "work_item_version": 2,
            "work_item": {
                "id": "wki_" + "4" * 32,
                "version": 2,
                "source": "manual",
                "external_id": "local-7",
                "source_updated_at": "2026-10-09T12:00:00.000000Z",
                "schema_version": 1,
                "title": "Build the gateway",
                "brief": "Use one capability channel.",
                "acceptance_criteria": [],
                "constraints": [],
                "context": [],
                "labels": [],
                "priority": "normal",
            },
            "credential_references": {"krab": "file:/private/agent.token"},
        }

    def get_run(self, run_id: str) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped its bound run")
        return {
            "id": RUN_ID,
            "state": "waiting_for_input",
            "version": 4,
            "interactions": self.interactions,
        }

    def get_run_transport(self, run_id: str) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped its bound run")
        return {"version": 3}


class FakeGatewayControl:
    def __init__(self) -> None:
        self.database = FakeGatewayDatabase()
        self.open_calls: list[tuple[str, dict[str, Any]]] = []
        self.read_calls: list[tuple[str, str]] = []

    def open_interaction(self, run_id: str, **kwargs: Any) -> dict[str, Any]:
        self.open_calls.append((run_id, kwargs))
        return {
            "id": INTERACTION_ID,
            "state": "open",
            "version": 1,
            "run_state": "waiting_for_input",
            "run_version": 4,
            "request_item_id": "itm_" + "6" * 32,
        }

    def read_interaction_response(
        self, run_id: str, interaction_id: str
    ) -> AcceptedInteractionResponse:
        self.read_calls.append((run_id, interaction_id))
        return AcceptedInteractionResponse(
            interaction_id=interaction_id,
            run_id=run_id,
            response_item_id="itm_" + "7" * 32,
            interaction_state="answered",
            interaction_version=2,
            run_state="running",
            run_version=6,
            untrusted_title="Human response",
            untrusted_body="Proceed with option B.",
            content_risk_flags=("untrusted_text",),
        )


def request(operation: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "version": 1,
        "request_id": "req-12345678",
        "operation": operation,
        "payload": payload or {},
    }


class RunGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.control = FakeGatewayControl()
        self.gateway = RunGateway(
            self.control,
            RUN_ID,
            capabilities=tuple(GatewayCapability),
            credential_names=("krab",),
        )

    def test_describe_exposes_pinned_agent_inputs_without_host_authority(self) -> None:
        response = self.gateway.handle(request("run.describe"))

        self.assertTrue(response["ok"])
        result = response["result"]
        self.assertEqual(result["run_id"], RUN_ID)
        self.assertEqual(result["state"], "waiting_for_input")
        self.assertEqual(result["version"], 4)
        self.assertEqual(result["work_item"]["title"], "Build the gateway")
        self.assertEqual(result["context"]["purpose"], "Build the box")
        self.assertEqual(
            result["interactions"],
            [
                {
                    "id": INTERACTION_ID,
                    "kind": "question",
                    "recipient_handle": "reviewer",
                    "request_item_id": "itm_" + "6" * 32,
                    "response_item_id": "itm_" + "7" * 32,
                    "state": "answered",
                    "version": 2,
                }
            ],
        )
        encoded = json.dumps(result, sort_keys=True)
        self.assertNotIn("repository_path", encoded)
        self.assertNotIn("credential_references", encoded)
        self.assertNotIn("agent.token", encoded)
        self.assertNotIn("secret-host-policy", encoded)

    def test_describe_bounds_historical_interaction_metadata(self) -> None:
        self.control.database.interactions = [
            {
                "id": "int_" + f"{index:032x}",
                "kind": "question",
                "state": "answered",
                "version": 2,
                "request_item_id": "itm_" + f"{index:032x}",
                "response_item_id": "itm_" + f"{index + 1000:032x}",
                "recipient": {"krab_handle": "reviewer"},
            }
            for index in range(130)
        ]

        result = self.gateway.handle(request("run.describe"))["result"]

        self.assertEqual(result["interaction_count"], 130)
        self.assertTrue(result["interactions_truncated"])
        self.assertEqual(len(result["interactions"]), 128)
        self.assertEqual(result["interactions"][0]["id"], "int_" + f"{2:032x}")

    def test_open_uses_bound_run_and_current_version_then_instructs_exit(self) -> None:
        response = self.gateway.handle(
            request(
                "interaction.open",
                {
                    "kind": "approval",
                    "recipient_handle": "reviewer",
                    "title": "Approve the migration?",
                    "body": "The migration is ready for review.",
                    "idempotency_key": "gateway.request:0001",
                },
            )
        )

        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["interaction_id"], INTERACTION_ID)
        self.assertEqual(response["result"]["agent_action"], "exit")
        self.assertEqual(
            self.control.open_calls,
            [
                (
                    RUN_ID,
                    {
                        "kind": "approval",
                        "recipient_krab_handle": "reviewer",
                        "title": "Approve the migration?",
                        "body": "The migration is ready for review.",
                        "idempotency_key": "gateway.request:0001",
                        "expected_run_version": 3,
                    },
                )
            ],
        )

    def test_read_can_only_request_a_response_for_the_bound_run(self) -> None:
        response = self.gateway.handle(
            request("interaction.read", {"interaction_id": INTERACTION_ID})
        )
        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["untrusted_body"], "Proceed with option B.")
        self.assertEqual(self.control.read_calls, [(RUN_ID, INTERACTION_ID)])

        cross_run = self.gateway.handle(
            request(
                "interaction.read",
                {
                    "interaction_id": INTERACTION_ID,
                    "run_id": OTHER_RUN_ID,
                },
            )
        )
        self.assertFalse(cross_run["ok"])
        self.assertEqual(cross_run["error"]["code"], "invalid_request")
        self.assertEqual(self.control.read_calls, [(RUN_ID, INTERACTION_ID)])

    def test_protocol_and_capability_errors_fail_closed(self) -> None:
        limited = RunGateway(
            self.control,
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        cases = (
            ({**request("run.describe"), "extra": True}, "invalid_request"),
            ({**request("run.describe"), "version": 2}, "invalid_request"),
            (request("unknown.operation"), "invalid_request"),
            (request("interaction.open"), "forbidden"),
        )
        for candidate, code in cases:
            with self.subTest(code=code, candidate=candidate):
                response = limited.handle(candidate)
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], code)

        without_credential = RunGateway(
            self.control,
            RUN_ID,
            capabilities=(GatewayCapability.INTERACTION_OPEN,),
            credential_names=(),
        )
        denied = without_credential.handle(request("interaction.open"))
        self.assertFalse(denied["ok"])
        self.assertEqual(denied["error"]["code"], "forbidden")

        invalid_id = limited.handle({**request("run.describe"), "request_id": "short"})
        self.assertIsNone(invalid_id["request_id"])
        self.assertEqual(invalid_id["error"]["code"], "invalid_request")

    def test_constructor_rejects_invalid_binding_policy(self) -> None:
        cases = (
            ("run-bad", (GatewayCapability.RUN_DESCRIBE,), ("krab",), "run"),
            (RUN_ID, ("provider.anywhere",), ("krab",), "capability"),
            (
                RUN_ID,
                (GatewayCapability.RUN_DESCRIBE, GatewayCapability.RUN_DESCRIBE),
                ("krab",),
                "duplicates",
            ),
            (RUN_ID, (GatewayCapability.RUN_DESCRIBE,), ("",), "credential"),
            (
                RUN_ID,
                (GatewayCapability.RUN_DESCRIBE,),
                ("krab", "krab"),
                "credential",
            ),
            (
                RUN_ID,
                (GatewayCapability.RUN_DESCRIBE,),
                (["krab"],),
                "credential",
            ),
        )
        for run_id, capabilities, credentials, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(GatewayError, message),
            ):
                RunGateway(
                    self.control,
                    run_id,
                    capabilities=capabilities,
                    credential_names=credentials,
                )

    def test_operation_payloads_reject_bad_fields_without_dispatch(self) -> None:
        valid_open = {
            "kind": "question",
            "recipient_handle": "reviewer",
            "title": "Need input",
            "body": "Choose a path.",
            "idempotency_key": "gateway.request:0002",
        }
        cases = (
            request("run.describe", {"extra": True}),
            request("interaction.open", {**valid_open, "kind": "unknown"}),
            request("interaction.open", {**valid_open, "recipient_handle": "BAD"}),
            request("interaction.open", {**valid_open, "title": ""}),
            request("interaction.open", {**valid_open, "idempotency_key": "bad key"}),
            request("interaction.read", {"interaction_id": "int_bad"}),
        )
        for candidate in cases:
            with self.subTest(operation=candidate["operation"]):
                response = self.gateway.handle(candidate)
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], "invalid_request")
        self.assertEqual(self.control.open_calls, [])
        self.assertEqual(self.control.read_calls, [])

    def test_domain_and_dependency_failures_are_typed_and_secrets_redacted(
        self,
    ) -> None:
        cases = (
            (ValidationError("bad input"), "invalid_request", "bad input"),
            (NotFoundError("missing"), "not_found", "missing"),
            (ConflictError("stale"), "conflict", "stale"),
            (KrabError("SECRET upstream"), "upstream_error", "upstream request failed"),
            (StorageError("SECRET database"), "internal_error", "operation failed"),
            (RuntimeError("SECRET crash"), "internal_error", "operation failed"),
        )
        for failure, code, message in cases:
            with (
                self.subTest(code=code),
                mock.patch.object(self.gateway, "_describe", side_effect=failure),
            ):
                response = self.gateway.handle(request("run.describe"))
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], code)
                self.assertIn(message, response["error"]["message"])
                if "SECRET" in str(failure):
                    self.assertNotIn("SECRET", json.dumps(response))


class FileGatewayManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(dir="/tmp")  # noqa: S108
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "gateways"
        self.control = FakeGatewayControl()
        self.manager = FileGatewayManager(
            self.root, self.control, poll_interval=0.01, request_timeout=0.25
        )
        self.addCleanup(self.manager.close)
        self.sequence = 0

    def exchange(self, directory: Path, value: bytes) -> dict[str, Any]:
        self.sequence += 1
        self.commit(directory, value, sequence=self.sequence)
        return self.wait_response(directory, self.sequence)

    def commit(
        self,
        directory: Path,
        value: bytes,
        *,
        sequence: int,
        digest: str | None = None,
    ) -> None:
        (directory / GATEWAY_REQUEST_NAME).write_bytes(value)
        marker = {
            "sequence": sequence,
            "sha256": digest or hashlib.sha256(value).hexdigest(),
            "size": len(value),
        }
        (directory / GATEWAY_REQUEST_READY_NAME).write_text(
            canonical_json(marker) + "\n", encoding="ascii"
        )

    def wait_response(self, directory: Path, sequence: int) -> dict[str, Any]:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            ready = (directory / GATEWAY_RESPONSE_READY_NAME).read_bytes()
            if ready:
                response_marker = json.loads(ready)
                if response_marker["sequence"] == sequence:
                    raw = (directory / GATEWAY_RESPONSE_NAME).read_bytes()
                    self.assertEqual(response_marker["size"], len(raw))
                    self.assertEqual(
                        response_marker["sha256"], hashlib.sha256(raw).hexdigest()
                    )
                    return json.loads(raw)
            time.sleep(0.01)
        self.fail("gateway response timed out")

    def test_mailbox_is_private_framed_and_removed_on_cleanup(self) -> None:
        endpoint = self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        self.assertEqual(endpoint.directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(
            sorted(path.name for path in endpoint.directory.iterdir()),
            sorted(
                (
                    GATEWAY_REQUEST_NAME,
                    GATEWAY_REQUEST_READY_NAME,
                    GATEWAY_RESPONSE_NAME,
                    GATEWAY_RESPONSE_READY_NAME,
                )
            ),
        )
        for path in endpoint.directory.iterdir():
            self.assertTrue(stat.S_ISREG(path.lstat().st_mode))
            self.assertEqual(path.lstat().st_mode & 0o777, 0o600)

        response = self.exchange(
            endpoint.directory,
            (json.dumps(request("run.describe")) + "\n").encode(),
        )
        self.assertTrue(response["ok"])
        self.assertEqual(response["request_id"], "req-12345678")
        self.assertTrue(self.manager.cleanup(RUN_ID))
        self.assertFalse(endpoint.directory.exists())
        self.assertFalse(self.manager.cleanup(RUN_ID))

    def test_malformed_oversized_and_duplicate_requests_are_bounded(self) -> None:
        endpoint = self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        for raw in (
            b"not-json\n",
            b'{"version":1,"version":1}\n',
            b"{" + b"x" * MAX_GATEWAY_REQUEST_BYTES + b"}\n",
        ):
            with self.subTest(size=len(raw)):
                response = self.exchange(endpoint.directory, raw)
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], "invalid_request")

    def test_commit_mismatch_and_replayed_sequence_fail_closed(self) -> None:
        endpoint = self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        raw = (json.dumps(request("run.describe")) + "\n").encode()
        self.sequence = 1
        self.commit(endpoint.directory, raw, sequence=1, digest="0" * 64)
        invalid_commit = self.wait_response(endpoint.directory, 1)
        self.assertFalse(invalid_commit["ok"])
        self.assertIn("commit", invalid_commit["error"]["message"])

        valid = self.exchange(endpoint.directory, raw)
        self.assertTrue(valid["ok"])
        self.commit(endpoint.directory, raw, sequence=1)
        replayed = self.wait_response(endpoint.directory, 1)
        self.assertFalse(replayed["ok"])
        self.assertIn("not increasing", replayed["error"]["message"])

    def test_duplicate_or_unsafe_endpoint_state_is_rejected(self) -> None:
        self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        with self.assertRaisesRegex(GatewayError, "already active"):
            self.manager.open(
                RUN_ID,
                capabilities=(GatewayCapability.RUN_DESCRIBE,),
                credential_names=("krab",),
            )

        other_root = Path(self.temporary.name) / "other"
        other_root.mkdir(mode=0o755)
        with self.assertRaisesRegex(GatewayError, "mode 0700"):
            FileGatewayManager(other_root, self.control).open(
                OTHER_RUN_ID,
                capabilities=(GatewayCapability.RUN_DESCRIBE,),
                credential_names=("krab",),
            )

    def test_default_root_is_short_stable_and_overrideable(self) -> None:
        database = Path("/a/very/long/private/location/control-plane.db")
        with mock.patch.dict(os.environ, {}, clear=True):
            first = default_gateway_root(database)
            second = default_gateway_root(database)
            other = default_gateway_root(Path("/another/control-plane.db"))
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        self.assertEqual(first.parent, Path("/tmp"))  # noqa: S108
        self.assertNotIn("private", str(first))

        override = Path(self.temporary.name) / "runtime"
        with mock.patch.dict(
            os.environ, {"MAGIC_GLASS_BOX_RUNTIME_DIR": str(override)}, clear=True
        ):
            self.assertEqual(default_gateway_root(database).parent, override)


if __name__ == "__main__":
    unittest.main()
