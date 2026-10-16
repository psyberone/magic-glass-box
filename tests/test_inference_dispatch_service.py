from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from magic_glass_box.db import SCHEMA_VERSION, ConflictError, Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    ProjectContext,
    ProviderGrantSpec,
    WorkItemEnvelope,
    canonical_json,
)
from magic_glass_box.inference import (
    InferenceConnectorAmbiguousError,
    InferenceConnectorSpec,
    InferenceConnectorUnavailableError,
    InferenceGrantSpec,
    InferenceInvocationRequest,
    InferenceInvocationResponse,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.provider import ProviderError, ProviderUpstreamError
from magic_glass_box.service import AcceptedProviderResponse, ControlPlane


def connector_spec(socket_path: Path) -> InferenceConnectorSpec:
    return InferenceConnectorSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_protocol_version": 1,
            "deployment_sha256": "a" * 64,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "routes": [
                {
                    "name": "chat-main",
                    "provider": "example-ai",
                    "protocol": "example-json",
                    "models": ["bounded-model"],
                    "credential_scope_sha256": "b" * 64,
                }
            ],
            "request_bytes": 4096,
            "response_bytes": 8192,
            "timeout_ms": 10000,
            "max_concurrency": 1,
        }
    )


def grant_spec() -> InferenceGrantSpec:
    return InferenceGrantSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_name": "example-sidecar",
            "deployment_sha256": "a" * 64,
            "route": "chat-main",
            "credential_scope_sha256": "b" * 64,
            "provider": "example-ai",
            "protocol": "example-json",
            "model": "bounded-model",
            "request_bytes": 2048,
            "response_bytes": 4096,
            "timeout_ms": 5000,
            "max_concurrency": 1,
        }
    )


def direct_policy() -> ProviderGrantSpec:
    return ProviderGrantSpec.from_mapping(
        {
            "provider": "example-ai",
            "adapter": "json_http",
            "destination": "https://provider.invalid/v1/invoke",
            "method": "POST",
            "model": "legacy-model",
            "credential_header": "Authorization",
            "credential_prefix": "Bearer ",
            "static_headers": {},
            "request_bytes": 2048,
            "response_bytes": 4096,
            "timeout_ms": 5000,
            "max_concurrency": 1,
        }
    )


class RecordingConnectorClient:
    def __init__(self) -> None:
        self.calls: list[InferenceInvocationRequest] = []
        self.started = threading.Event()
        self.release: threading.Event | None = None
        self.error: Exception | None = None
        self.outcome = "succeeded"
        self.error_code: str | None = None
        self.provider_status: int | None = 200

    def invoke(
        self, request: InferenceInvocationRequest
    ) -> InferenceInvocationResponse:
        self.calls.append(request)
        self.started.set()
        if self.release is not None and not self.release.wait(timeout=2):
            raise RuntimeError("connector test release timed out")
        if self.error is not None:
            raise self.error
        response = request.to_dict()
        response.pop("request")
        response.update(
            {
                "outcome": self.outcome,
                "provider_status": self.provider_status,
                "request_bytes": len(canonical_json(request.request).encode("utf-8")),
                "response_bytes": 37 if self.outcome == "succeeded" else 0,
                "error_code": self.error_code,
                "response": (
                    {"output": [{"text": "done"}]}
                    if self.outcome == "succeeded"
                    else None
                ),
            }
        )
        return InferenceInvocationResponse.from_mapping(response)


class ExplodingCredentialResolver:
    def resolve_secret(self, _reference: str) -> str:
        raise AssertionError("MGB attempted to resolve a provider credential")


class InferenceDispatchServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "1" * 32, "agent_one", 1),
            "file:/private/krab-agent-one",
            user_id=self.user["id"],
        )
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping({"purpose": "Dispatch inference"}),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "inference-dispatch",
                    "source_updated_at": "2026-10-15T12:00:00Z",
                    "title": "Dispatch through a connector",
                    "brief": "Keep provider credentials outside the control plane.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.socket_path = self.root / "connector.sock"
        self.connector = self.database.register_inference_connector(
            name="example-sidecar",
            spec=connector_spec(self.socket_path),
            created_by=self.user["id"],
        )
        self.grant = self.database.create_connector_provider_grant(
            name="example-main",
            spec=grant_spec(),
            expires_at=None,
            created_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Provider Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc",
                command=["agent"],
                credential_names=["example-main"],
                gateway_capabilities=[GatewayCapability.PROVIDER_INVOKE],
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )
        self.client = RecordingConnectorClient()
        self.client_paths: list[Path] = []

        def client_factory(path: Path) -> RecordingConnectorClient:
            self.client_paths.append(path)
            return self.client

        self.control = ControlPlane(
            self.database,
            credential_resolver=ExplodingCredentialResolver(),  # type: ignore[arg-type]
            inference_client_factory=client_factory,
        )

    def create_run(self) -> dict[str, Any]:
        run = self.database.create_run(
            project_key="box-project",
            profile_name="Provider Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
        )
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="codex",
            owner_id="orchestrator-inference-test",
            expected_run_version=run["version"],
        )
        self.database.start_run_attempt(
            attempt["id"],
            owner_id="orchestrator-inference-test",
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision="abc123",
            container_id="b" * 64,
            container_name="magic-glass-box-inference-test",
            image_id="sha256:" + "a" * 64,
        )
        return self.database.get_run(run["id"])

    def test_connector_is_the_only_dispatch_path_and_ledger_pins_authority(
        self,
    ) -> None:
        run = self.create_run()

        accepted = self.control.invoke_provider(
            run["id"],
            grant_name="example-main",
            request={"input": "hello"},
        )

        self.assertIsInstance(accepted, AcceptedProviderResponse)
        self.assertEqual(accepted.untrusted_response, {"output": [{"text": "done"}]})
        self.assertEqual(self.client_paths, [self.socket_path])
        self.assertEqual(len(self.client.calls), 1)
        dispatched = self.client.calls[0]
        self.assertEqual(dispatched.connector_id, self.connector["id"])
        self.assertEqual(dispatched.grant_id, self.grant["id"])
        self.assertEqual(dispatched.policy_sha256, grant_spec().digest)
        self.assertEqual(dispatched.route, "chat-main")
        self.assertEqual(dispatched.model, "bounded-model")
        self.assertEqual(dispatched.request, {"input": "hello"})

        invocations = self.database.list_provider_invocations(run["id"])
        self.assertEqual(len(invocations), 1)
        invocation = invocations[0]
        self.assertEqual(invocation["id"], accepted.invocation_id)
        self.assertEqual(invocation["id"], dispatched.invocation_id)
        self.assertEqual(invocation["dispatch_kind"], "connector")
        self.assertEqual(invocation["inference_connector_id"], self.connector["id"])
        self.assertEqual(invocation["deployment_sha256"], "a" * 64)
        self.assertEqual(invocation["route"], "chat-main")
        self.assertEqual(invocation["policy_sha256"], grant_spec().digest)
        self.assertEqual(invocation["credential_scope_sha256"], "b" * 64)
        self.assertEqual(invocation["provider"], "example-ai")
        self.assertEqual(invocation["protocol"], "example-json")
        self.assertEqual(invocation["model"], "bounded-model")
        durable = self.database.path.read_bytes()
        self.assertNotIn(b"hello", durable)
        self.assertNotIn(b"done", durable)

    def test_pre_dispatch_unavailable_and_post_dispatch_ambiguity_are_distinct(
        self,
    ) -> None:
        run = self.create_run()
        self.client.error = InferenceConnectorUnavailableError("private detail")
        with self.assertRaisesRegex(ProviderUpstreamError, "unavailable"):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "one"}
            )

        self.client.error = InferenceConnectorAmbiguousError("private detail")
        with self.assertRaisesRegex(ProviderError, "ambiguous"):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "two"}
            )

        self.assertEqual(len(self.client.calls), 2)
        invocations = self.database.list_provider_invocations(run["id"])
        self.assertEqual(
            [(item["outcome"], item["error_code"]) for item in invocations],
            [
                ("failed", "connector_unavailable"),
                ("ambiguous", "connector_ambiguous"),
            ],
        )

    def test_client_construction_failure_is_audited_and_releases_capacity(
        self,
    ) -> None:
        run = self.create_run()
        control = ControlPlane(
            self.database,
            inference_client_factory=lambda _path: (_ for _ in ()).throw(
                RuntimeError("private connector construction detail")
            ),
        )

        with self.assertRaisesRegex(ProviderUpstreamError, "unavailable"):
            control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "one"}
            )

        invocations = self.database.list_provider_invocations(run["id"])
        self.assertEqual(
            [(item["outcome"], item["error_code"]) for item in invocations],
            [("failed", "connector_unavailable")],
        )
        with self.database.connect() as connection:
            active = connection.execute(
                "SELECT COUNT(*) FROM provider_dispatch_reservations"
            ).fetchone()[0]
        self.assertEqual(active, 0)

    def test_connector_reported_failure_and_ambiguity_are_durable(self) -> None:
        run = self.create_run()
        self.client.outcome = "failed"
        self.client.error_code = "quota_exhausted"
        self.client.provider_status = 429
        with self.assertRaises(ProviderUpstreamError):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "one"}
            )

        self.client.outcome = "ambiguous"
        self.client.error_code = "reply_lost"
        self.client.provider_status = None
        with self.assertRaisesRegex(ProviderError, "ambiguous"):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "two"}
            )

        invocations = self.database.list_provider_invocations(run["id"])
        self.assertEqual(
            [
                (item["outcome"], item["http_status"], item["error_code"])
                for item in invocations
            ],
            [
                ("failed", 429, "quota_exhausted"),
                ("ambiguous", None, "reply_lost"),
            ],
        )

    def test_transport_neutral_client_cannot_bypass_response_validation(self) -> None:
        run = self.create_run()
        original_invoke = self.client.invoke

        def invalid_success(
            request: InferenceInvocationRequest,
        ) -> InferenceInvocationResponse:
            response = original_invoke(request)
            return replace(response, _response_json=None)

        self.client.invoke = invalid_success  # type: ignore[method-assign]

        with self.assertRaisesRegex(ProviderError, "ambiguous"):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "one"}
            )

        invocations = self.database.list_provider_invocations(run["id"])
        self.assertEqual(
            [(item["outcome"], item["error_code"]) for item in invocations],
            [("ambiguous", "connector_ambiguous")],
        )

    def test_post_dispatch_storage_failure_remains_durably_ambiguous(self) -> None:
        run = self.create_run()

        with (
            mock.patch.object(
                self.database,
                "finalize_provider_invocation",
                side_effect=sqlite3.OperationalError("private database detail"),
            ),
            self.assertRaisesRegex(ProviderError, "ambiguous") as raised,
        ):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "one"}
            )

        self.assertNotIn("private database detail", str(raised.exception))
        self.assertEqual(len(self.client.calls), 1)
        invocations = self.database.list_provider_invocations(run["id"])
        self.assertEqual(
            [(item["outcome"], item["error_code"]) for item in invocations],
            [("dispatching", None)],
        )
        with self.database.connect() as connection:
            reservations = connection.execute(
                "SELECT COUNT(*) FROM provider_dispatch_reservations"
            ).fetchone()[0]
        self.assertEqual(reservations, 1)

    def test_revoked_connector_fails_atomic_reservation_before_dispatch(self) -> None:
        run = self.create_run()
        self.database.revoke_inference_connector(
            "example-sidecar",
            expected_version=1,
            revoked_by=self.user["id"],
        )

        with self.assertRaisesRegex(ConflictError, "provider grant is unavailable"):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "no"}
            )

        self.assertEqual(self.client.calls, [])

    def test_legacy_direct_grant_is_readable_but_never_dispatched(self) -> None:
        run = self.create_run()
        direct = self.database.create_provider_grant(
            name="legacy-direct",
            spec=direct_policy(),
            credential_ref="file:/private/provider-secret",
            expires_at=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
            created_by=self.user["id"],
        )
        with self.database.connect() as connection:
            connection.execute(
                "INSERT INTO run_provider_grants "
                "(run_id, authority_name, provider_grant_id, created_at) "
                "VALUES (?, 'legacy-direct', ?, ?)",
                (run["id"], direct["id"], self.database.now()),
            )

        with self.assertRaisesRegex(ProviderError, "connector-bound"):
            self.control.invoke_provider(
                run["id"],
                grant_name="legacy-direct",
                request={"input": "must not leave MGB"},
            )

        self.assertEqual(self.client.calls, [])
        invocation = self.database.list_provider_invocations(run["id"])[0]
        self.assertEqual(invocation["dispatch_kind"], "direct")
        self.assertEqual(invocation["outcome"], "denied")
        self.assertEqual(invocation["error_code"], "legacy_direct_denied")

    def test_schema_fifteen_migrates_historical_direct_invocations_once(self) -> None:
        run = self.create_run()
        direct = self.database.create_provider_grant(
            name="legacy-direct",
            spec=direct_policy(),
            credential_ref="file:/private/provider-secret",
            expires_at=None,
            created_by=self.user["id"],
        )
        with self.database.connect() as connection:
            connection.execute(
                "INSERT INTO run_provider_grants "
                "(run_id, authority_name, provider_grant_id, created_at) "
                "VALUES (?, 'legacy-direct', ?, ?)",
                (run["id"], direct["id"], self.database.now()),
            )
        with self.assertRaises(ProviderError):
            self.control.invoke_provider(
                run["id"], grant_name="legacy-direct", request={"input": "no"}
            )
        invocation_id = self.database.list_provider_invocations(run["id"])[0]["id"]

        with sqlite3.connect(self.database.path) as connection:
            connection.executescript(
                """
                PRAGMA foreign_keys = OFF;
                DROP TRIGGER provider_invocation_immutable_update;
                DROP TRIGGER provider_invocation_immutable_delete;
                DROP INDEX provider_invocation_run;
                DROP INDEX provider_invocation_connector;
                ALTER TABLE provider_invocations RENAME TO provider_invocations_v16;
                CREATE TABLE provider_invocations (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    provider_grant_id TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK (
                        outcome IN ('succeeded', 'failed', 'denied', 'busy')
                    ),
                    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 0),
                    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 0),
                    http_status INTEGER CHECK (http_status BETWEEN 100 AND 599),
                    error_code TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
                    FOREIGN KEY (provider_grant_id)
                        REFERENCES provider_grants(id) ON DELETE RESTRICT
                ) STRICT;
                INSERT INTO provider_invocations (
                    id, run_id, provider_grant_id, outcome, request_bytes,
                    response_bytes, http_status, error_code, started_at, finished_at
                )
                SELECT
                    id, run_id, provider_grant_id, outcome, request_bytes,
                    response_bytes, http_status, error_code, started_at, finished_at
                FROM provider_invocations_v16;
                DROP TABLE provider_invocations_v16;
                CREATE INDEX provider_invocation_run
                    ON provider_invocations(run_id, started_at, id);
                CREATE TRIGGER provider_invocation_immutable_update
                BEFORE UPDATE ON provider_invocations
                BEGIN
                    SELECT RAISE(ABORT, 'provider invocations are immutable');
                END;
                CREATE TRIGGER provider_invocation_immutable_delete
                BEFORE DELETE ON provider_invocations
                BEGIN
                    SELECT RAISE(ABORT, 'provider invocations are immutable');
                END;
                PRAGMA user_version = 15;
                PRAGMA foreign_keys = ON;
                """
            )

        first = self.database.initialize()
        second = self.database.initialize()
        migrated = self.database.list_provider_invocations(run["id"])[0]

        self.assertEqual(first["schema_version"], SCHEMA_VERSION)
        self.assertEqual(second, first)
        self.assertEqual(migrated["id"], invocation_id)
        self.assertEqual(migrated["dispatch_kind"], "direct")
        self.assertIsNone(migrated["inference_connector_id"])
        self.assertEqual(migrated["outcome"], "denied")


if __name__ == "__main__":
    unittest.main()
