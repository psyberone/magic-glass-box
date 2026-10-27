from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

from magic_glass_box.db import (
    CAPABILITY_BINDING_SCHEMA,
    SCHEMA_VERSION,
    SERVICE_SIDECAR_SCHEMA,
    ConcurrentUpdateError,
    ConflictError,
    Database,
)
from magic_glass_box.domain import ValidationError
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceOperationSpec,
)
from magic_glass_box.storage.schema import SCHEMA_V41
from tests.test_service_sidecars import (
    operation_spec,
    resource_operation_spec,
    resource_sidecar_spec,
    sidecar_spec,
)


def successful_response(
    request: ServiceOperationRequest, *, result: object | None = None
) -> ServiceOperationResponse:
    receipt = {
        "remote_identity": "change-42",
        "observed_revision": "d" * 40,
        "evidence_sha256": "e" * 64,
        "closed_references": [],
    }
    if result is not None:
        receipt["result"] = result
    value = request.correlation_dict()
    value.update(
        {
            "outcome": "succeeded",
            "provider_status": 201,
            "request_bytes": 512,
            "response_bytes": 128,
            "error_code": None,
            "retry_after_seconds": None,
            "receipt": receipt,
        }
    )
    return ServiceOperationResponse.from_mapping(value)


def error_response(
    request: ServiceOperationRequest,
    *,
    outcome: str,
    provider_status: int,
    error_code: str,
    retry_after_seconds: int | None = None,
) -> ServiceOperationResponse:
    value = request.correlation_dict()
    value.update(
        {
            "outcome": outcome,
            "provider_status": provider_status,
            "request_bytes": 128,
            "response_bytes": 64,
            "error_code": error_code,
            "retry_after_seconds": retry_after_seconds,
            "receipt": None,
        }
    )
    return ServiceOperationResponse.from_mapping(value)


class ServiceSidecarStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        self.spec = sidecar_spec(
            self.root / "service.sock",
            max_concurrency=1,
            max_requests_per_minute=10,
        )

    def register(self, *, name: str = "example-sidecar") -> dict[str, object]:
        return self.database.register_service_sidecar(
            name=name,
            spec=self.spec,
            created_by=self.user["id"],
        )

    def create_operation(
        self, spec: ServiceOperationSpec | None = None
    ) -> dict[str, object]:
        return self.database.create_service_operation(
            spec=spec or operation_spec(),
            created_by=self.user["id"],
        )

    def test_schema_twenty_four_registers_immutable_redacted_authority(self) -> None:
        self.assertEqual(SCHEMA_VERSION, 42)
        self.assertIn("CREATE TABLE service_sidecars", SERVICE_SIDECAR_SCHEMA)
        self.assertIn(
            "CREATE TABLE project_capability_bindings", CAPABILITY_BINDING_SCHEMA
        )

        sidecar = self.register()
        self.assertTrue(str(sidecar["id"]).startswith("ssc_"))
        self.assertEqual(sidecar["state"], "active")
        self.assertEqual(sidecar["version"], 1)
        self.assertEqual(sidecar["spec"], self.spec.to_dict())
        self.assertEqual(sidecar["content_sha256"], self.spec.digest)
        self.assertEqual(
            [event["kind"] for event in sidecar["events"]],
            ["service_sidecar.created"],
        )
        self.assertEqual(self.database.list_service_sidecars(), [sidecar])
        self.assertEqual(self.database.status()["counts"]["service_sidecars"], 1)

        serialized = json.dumps(sidecar, sort_keys=True)
        for forbidden in ("api_key", "access_token", "refresh_token", "password"):
            self.assertNotIn(forbidden, serialized)

        with closing(self.database.connect()) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE service_sidecars SET spec_json = '{}' WHERE id = ?",
                    (sidecar["id"],),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM service_sidecar_events WHERE service_sidecar_id = ?",
                    (sidecar["id"],),
                )

    def test_schema_twenty_four_migrates_schema_twenty_two_once(self) -> None:
        with closing(self.database.connect()) as connection, connection:
            connection.execute("DROP TABLE workflow_scope_reviews")
            connection.execute("DROP TABLE workflow_scope_bindings")
            connection.execute("DROP TABLE advisory_reports")
            connection.execute("DROP TABLE advisory_challenge_reviews")
            connection.execute("DROP TABLE advisory_blind_reviews")
            connection.execute("DROP TABLE advisory_run_bindings")
            connection.execute("DROP TABLE advisory_execution_events")
            connection.execute("DROP TABLE advisory_executions")
            connection.execute("DROP TABLE discovery_rulings")
            connection.execute("DROP TABLE discovery_traces")
            connection.execute("DROP TABLE discovery_run_bindings")
            connection.execute("DROP TABLE discovery_execution_events")
            connection.execute("DROP TABLE discovery_executions")
            connection.execute("DROP TABLE workflow_completions")
            connection.execute("DROP TABLE workflow_provider_targets")
            connection.execute("DROP TABLE workflow_landing_chains")
            connection.execute("DROP TABLE workflow_landing_bindings")
            connection.execute("DROP TABLE workflow_rulings")
            connection.execute("DROP TABLE integration_review_verdicts")
            connection.execute("DROP TABLE workflow_gate_bindings")
            connection.execute("DROP TABLE workflow_run_bindings")
            connection.execute("DROP TABLE workflow_execution_events")
            connection.execute("DROP TABLE workflow_executions")
            connection.execute("DROP TABLE workflow_configuration_role_bindings")
            connection.execute("DROP TABLE workflow_configuration_versions")
            connection.execute("DROP TABLE workflow_configurations")
            connection.execute("DROP TABLE workflow_template_definitions")
            connection.execute("DROP TABLE integration_gate_maintainer_change_runs")
            connection.execute("DROP TABLE integration_maintainer_reviews")
            connection.execute("DROP TABLE integration_gate_change_runs")
            connection.execute("DROP TABLE integration_provider_landings")
            connection.execute("DROP TABLE service_operation_capability_bindings")
            connection.execute("DROP TABLE project_capability_binding_events")
            connection.execute("DROP TABLE project_capability_bindings")
            connection.execute("DROP TABLE service_operation_attempts")
            connection.execute("DROP TABLE service_operations")
            connection.execute("DROP TABLE service_sidecar_events")
            connection.execute("DROP TABLE service_sidecars")
            connection.execute("PRAGMA user_version = 22")

        first = self.database.initialize()
        second = self.database.initialize()

        self.assertEqual(first["schema_version"], SCHEMA_VERSION)
        self.assertEqual(second, first)
        with closing(self.database.connect()) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table'"
                )
            }
        self.assertTrue(
            {
                "service_sidecars",
                "service_sidecar_events",
                "service_operations",
                "service_operation_attempts",
                "project_capability_bindings",
                "project_capability_binding_events",
                "service_operation_capability_bindings",
            }.issubset(tables)
        )
        with closing(self.database.connect()) as connection:
            operation_columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(service_operations)")
            }
        self.assertIn("resource_kind", operation_columns)
        self.assertIn("resource_external_id", operation_columns)
        self.assertNotIn("repository", operation_columns)

    def test_v2_work_resource_authority_is_stored_and_dispatched_exactly(self) -> None:
        spec = resource_sidecar_spec(self.root / "work.sock")
        sidecar = self.database.register_service_sidecar(
            name="example-work-sidecar",
            spec=spec,
            created_by=self.user["id"],
        )
        operation_specification = resource_operation_spec()
        operation = self.database.create_service_operation(
            spec=operation_specification,
            created_by=self.user["id"],
        )

        self.assertEqual(sidecar["schema_version"], 2)
        self.assertEqual(sidecar["spec"], spec.to_dict())
        self.assertEqual(operation["schema_version"], 2)
        self.assertEqual(operation["spec"], operation_specification.to_dict())
        with closing(self.database.connect()) as connection:
            stored = connection.execute(
                "SELECT resource_kind, resource_external_id "
                "FROM service_operations WHERE id = ?",
                (operation["id"],),
            ).fetchone()
        self.assertEqual(stored["resource_kind"], "work_project")
        self.assertEqual(stored["resource_external_id"], "ABC")

        dispatch = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="invoke",
            owner_id="supervisor-one",
            expected_version=1,
            lease_seconds=30,
        )
        request = ServiceOperationRequest.from_mapping(dispatch["request"])
        self.assertEqual(request.schema_version, 2)
        self.assertEqual(request.connector_protocol_version, 2)
        self.assertEqual(request.mapping.resource.kind, "work_project")
        self.assertEqual(request.mapping.resource.external_id, "ABC")
        self.assertEqual(request.target.object_id, "ABC-42")

    def test_schema_forty_two_preserves_legacy_repository_operations(self) -> None:
        legacy_path = self.root / "schema-v41.db"
        with sqlite3.connect(legacy_path) as connection:
            connection.executescript(SCHEMA_V41)
            connection.execute("PRAGMA user_version = 41")
        legacy_path.chmod(0o600)
        legacy = Database(legacy_path)
        user = legacy.create_user("legacy-operator", "Legacy Operator")
        registered = legacy.register_service_sidecar(
            name="legacy-sidecar",
            spec=self.spec,
            created_by=user["id"],
        )
        operation = operation_spec()
        operation_id = "sop_11111111111111111111111111111111"
        timestamp = "2026-10-27T12:00:00.000000+00:00"
        with closing(legacy.connect()) as connection, connection:
            connection.execute(
                "INSERT INTO service_operations "
                "(id, service_sidecar_id, state, version, schema_version, "
                "spec_json, content_sha256, sidecar_content_sha256, "
                "deployment_sha256, mapping_name, service, host, repository, "
                "account, credential_scope_sha256, capability_name, "
                "capability_version, capability_mode, idempotency_key, "
                "input_sha256, request_bytes, response_bytes, timeout_ms, "
                "created_by, created_at, updated_at) "
                "VALUES (?, ?, 'pending', 1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    operation_id,
                    registered["id"],
                    operation.canonical,
                    operation.digest,
                    self.spec.digest,
                    operation.deployment_sha256,
                    operation.mapping.name,
                    operation.mapping.service,
                    operation.mapping.host,
                    operation.mapping.repository,
                    operation.mapping.account,
                    operation.mapping.credential_scope_sha256,
                    operation.capability.name,
                    operation.capability.version,
                    operation.capability.mode,
                    operation.idempotency_key,
                    operation.input_sha256,
                    operation.request_bytes,
                    operation.response_bytes,
                    operation.timeout_ms,
                    user["id"],
                    timestamp,
                    timestamp,
                ),
            )

        first = legacy.initialize()
        second = legacy.initialize()

        self.assertEqual(first, second)
        self.assertEqual(first["schema_version"], 42)
        migrated = legacy.get_service_operation(operation_id)
        self.assertEqual(migrated["spec"], operation.to_dict())
        with closing(legacy.connect()) as connection:
            stored = connection.execute(
                "SELECT resource_kind, resource_external_id "
                "FROM service_operations WHERE id = ?",
                (operation_id,),
            ).fetchone()
        self.assertEqual(
            dict(stored),
            {
                "resource_kind": "repository",
                "resource_external_id": "acme/widgets",
            },
        )

    def test_operation_creation_is_exact_idempotent_and_fail_closed(self) -> None:
        self.register()
        operation = self.create_operation()

        self.assertTrue(str(operation["id"]).startswith("sop_"))
        self.assertEqual(operation["state"], "pending")
        self.assertEqual(operation["version"], 1)
        self.assertEqual(operation["spec"], operation_spec().to_dict())
        self.assertEqual(operation["input_sha256"], operation_spec().input_sha256)
        self.assertEqual(operation["attempts"], [])
        self.assertEqual(
            set(operation),
            {
                "attempts",
                "completed_at",
                "content_sha256",
                "created_at",
                "created_by",
                "eligible_at",
                "id",
                "input_sha256",
                "project_capability_binding_id",
                "schema_version",
                "sidecar",
                "spec",
                "state",
                "updated_at",
                "version",
            },
        )
        self.assertEqual(
            set(operation["sidecar"]),
            {
                "connector_protocol_version",
                "content_sha256",
                "id",
                "name",
                "spec",
                "state",
                "version",
            },
        )
        self.assertEqual(self.create_operation(), operation)
        self.assertEqual(self.database.status()["counts"]["service_operations"], 1)

        changed = operation_spec(parameters={"title": "Changed", "draft": True})
        with self.assertRaises(ConflictError):
            self.create_operation(changed)
        wrong_account = operation_spec(
            mapping={**operation_spec().mapping.to_dict(), "account": "wrong"}
        )
        with self.assertRaises(ConflictError):
            self.create_operation(wrong_account)
        wrong_capability = operation_spec(
            capability={"name": "landing.invoke", "version": 1, "mode": "effect"}
        )
        with self.assertRaises(ConflictError):
            self.create_operation(wrong_capability)
        self.assertEqual(self.database.status()["counts"]["service_operations"], 1)

    def test_successful_attempt_is_audited_and_terminal(self) -> None:
        self.register()
        operation = self.create_operation()
        dispatch = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="invoke",
            owner_id="supervisor-one",
            expected_version=operation["version"],
            lease_seconds=30,
        )
        self.assertEqual(dispatch["operation"]["state"], "dispatching")
        self.assertEqual(dispatch["operation"]["version"], 2)
        request = ServiceOperationRequest.from_mapping(dispatch["request"])

        completed = self.database.complete_service_operation_attempt(
            dispatch["attempt"]["id"],
            owner_id="supervisor-one",
            expected_operation_version=2,
            response=successful_response(
                request,
                result={"items": [{"external_id": "42", "title": "Candidate 42"}]},
            ),
        )
        self.assertEqual(completed["state"], "succeeded")
        self.assertEqual(completed["version"], 3)
        self.assertEqual(completed["attempts"][0]["outcome"], "succeeded")
        self.assertEqual(
            set(completed["attempts"][0]),
            {
                "deadline",
                "error_code",
                "finished_at",
                "id",
                "lease_expires_at",
                "outcome",
                "owner_id",
                "phase",
                "provider_status",
                "receipt",
                "request_bytes",
                "response_bytes",
                "retry_after_seconds",
                "sequence",
                "started_at",
            },
        )
        self.assertEqual(
            completed["attempts"][0]["receipt"]["remote_identity"], "change-42"
        )
        self.assertEqual(
            self.database.get_service_operation(operation["id"])["attempts"][0][
                "receipt"
            ]["result"],
            {"items": [{"external_id": "42", "title": "Candidate 42"}]},
        )
        with self.assertRaises(ConflictError):
            self.database.begin_service_operation_attempt(
                operation["id"],
                phase="invoke",
                owner_id="supervisor-one",
                expected_version=3,
                lease_seconds=30,
            )

    def test_ambiguous_effect_requires_reconciliation_before_reinvoke(self) -> None:
        self.register()
        operation = self.create_operation()
        first = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="invoke",
            owner_id="supervisor-one",
            expected_version=1,
            lease_seconds=30,
        )
        uncertain = self.database.fail_service_operation_attempt(
            first["attempt"]["id"],
            owner_id="supervisor-one",
            expected_operation_version=2,
            outcome="ambiguous",
            error_code="response_lost",
        )
        self.assertEqual(uncertain["state"], "uncertain")
        self.assertEqual(uncertain["version"], 3)
        with self.assertRaises(ConflictError):
            self.database.begin_service_operation_attempt(
                operation["id"],
                phase="invoke",
                owner_id="supervisor-two",
                expected_version=3,
                lease_seconds=30,
            )

        reconciliation = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="reconcile",
            owner_id="supervisor-two",
            expected_version=3,
            lease_seconds=30,
        )
        request = ServiceOperationRequest.from_mapping(reconciliation["request"])
        response = successful_response(request)
        recovered = self.database.complete_service_operation_attempt(
            reconciliation["attempt"]["id"],
            owner_id="supervisor-two",
            expected_operation_version=4,
            response=response,
        )
        self.assertEqual(recovered["state"], "succeeded")
        self.assertEqual(
            [attempt["phase"] for attempt in recovered["attempts"]],
            ["invoke", "reconcile"],
        )
        self.assertEqual(
            [attempt["outcome"] for attempt in recovered["attempts"]],
            ["ambiguous", "succeeded"],
        )

    def test_ambiguous_read_is_retryable_and_cannot_be_reconciled(self) -> None:
        self.register()
        operation = self.create_operation(
            operation_spec(
                capability={"name": "checks.read", "version": 1, "mode": "read"},
                idempotency_key="read-checks-42",
                parameters={"check_suite": "required"},
            )
        )
        started_at = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
        with mock.patch(
            "magic_glass_box.storage.service_sidecars.datetime", wraps=datetime
        ) as clock:
            clock.now.return_value = started_at
            first = self.database.begin_service_operation_attempt(
                operation["id"],
                phase="invoke",
                owner_id="supervisor-one",
                expected_version=1,
                lease_seconds=30,
            )
            retryable = self.database.fail_service_operation_attempt(
                first["attempt"]["id"],
                owner_id="supervisor-one",
                expected_operation_version=2,
                outcome="ambiguous",
                error_code="response_lost",
            )
            self.assertEqual(retryable["state"], "retryable")
            self.assertEqual(retryable["attempts"][0]["outcome"], "ambiguous")
            self.assertEqual(
                retryable["eligible_at"],
                (started_at + timedelta(seconds=5)).isoformat(timespec="microseconds"),
            )
            with self.assertRaisesRegex(ConflictError, "read operation"):
                self.database.begin_service_operation_attempt(
                    operation["id"],
                    phase="reconcile",
                    owner_id="supervisor-two",
                    expected_version=3,
                    lease_seconds=30,
                )
            clock.now.return_value = started_at + timedelta(seconds=5)
            resumed = self.database.begin_service_operation_attempt(
                operation["id"],
                phase="invoke",
                owner_id="supervisor-two",
                expected_version=3,
                lease_seconds=30,
            )
        self.assertEqual(resumed["operation"]["state"], "dispatching")

    def test_reconciliation_not_found_reauthorizes_one_fresh_invoke(self) -> None:
        self.register()
        operation = self.create_operation()
        invoked = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="invoke",
            owner_id="supervisor-one",
            expected_version=1,
            lease_seconds=30,
        )
        uncertain = self.database.fail_service_operation_attempt(
            invoked["attempt"]["id"],
            owner_id="supervisor-one",
            expected_operation_version=2,
            outcome="ambiguous",
            error_code="response_lost",
        )
        reconciliation = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="reconcile",
            owner_id="supervisor-two",
            expected_version=uncertain["version"],
            lease_seconds=30,
        )
        request = ServiceOperationRequest.from_mapping(reconciliation["request"])
        response_value = request.correlation_dict()
        response_value.update(
            {
                "outcome": "not_found",
                "provider_status": 404,
                "request_bytes": 128,
                "response_bytes": 64,
                "error_code": "not_found",
                "retry_after_seconds": None,
                "receipt": None,
            }
        )
        absent = self.database.complete_service_operation_attempt(
            reconciliation["attempt"]["id"],
            owner_id="supervisor-two",
            expected_operation_version=reconciliation["operation"]["version"],
            response=ServiceOperationResponse.from_mapping(response_value),
        )

        self.assertEqual(absent["state"], "pending")
        replay = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="invoke",
            owner_id="supervisor-three",
            expected_version=absent["version"],
            lease_seconds=30,
        )
        self.assertEqual(replay["operation"]["state"], "dispatching")
        self.assertEqual(
            [attempt["phase"] for attempt in replay["operation"]["attempts"]],
            ["invoke", "reconcile", "invoke"],
        )

    def test_concurrency_version_and_revocation_fail_before_dispatch(self) -> None:
        sidecar = self.register()
        first = self.create_operation()
        second = self.create_operation(
            operation_spec(
                idempotency_key="open-candidate-43",
                target={
                    **operation_spec().target.to_dict(),
                    "resource_id": "candidate-43",
                },
            )
        )
        active = self.database.begin_service_operation_attempt(
            first["id"],
            phase="invoke",
            owner_id="supervisor-one",
            expected_version=1,
            lease_seconds=30,
        )
        with self.assertRaises(ConflictError):
            self.database.begin_service_operation_attempt(
                second["id"],
                phase="invoke",
                owner_id="supervisor-two",
                expected_version=1,
                lease_seconds=30,
            )
        with self.assertRaises(ConcurrentUpdateError):
            self.database.fail_service_operation_attempt(
                active["attempt"]["id"],
                owner_id="supervisor-one",
                expected_operation_version=1,
                outcome="unavailable",
                error_code="unavailable",
            )
        released = self.database.fail_service_operation_attempt(
            active["attempt"]["id"],
            owner_id="supervisor-one",
            expected_operation_version=2,
            outcome="unavailable",
            error_code="unavailable",
        )
        self.assertEqual(released["state"], "retryable")
        self.assertIsNotNone(released["eligible_at"])
        eligible_at = datetime.fromisoformat(str(released["eligible_at"]))
        updated_at = datetime.fromisoformat(str(released["updated_at"]))
        self.assertIsNotNone(eligible_at.tzinfo)
        self.assertGreater(eligible_at, updated_at)

        revoked = self.database.revoke_service_sidecar(
            "example-sidecar",
            expected_version=sidecar["version"],
            revoked_by=self.user["id"],
        )
        self.assertEqual(revoked["state"], "revoked")
        self.assertEqual(
            [event["sequence"] for event in revoked["events"]],
            [1, 2],
        )
        with self.assertRaises(ConflictError):
            self.create_operation(
                operation_spec(
                    idempotency_key="revoked-sidecar-operation",
                    target={
                        **operation_spec().target.to_dict(),
                        "resource_id": "candidate-after-revocation",
                    },
                )
            )
        with self.assertRaises(ConflictError):
            self.database.begin_service_operation_attempt(
                second["id"],
                phase="invoke",
                owner_id="supervisor-two",
                expected_version=1,
                lease_seconds=30,
            )

        with self.assertRaises(ConflictError):
            self.create_operation(
                operation_spec(
                    connector_name="missing-sidecar",
                    idempotency_key="missing-sidecar-operation",
                )
            )

    def test_rate_limit_counts_released_attempts_without_holding_concurrency(
        self,
    ) -> None:
        self.spec = sidecar_spec(
            self.root / "service.sock",
            max_concurrency=1,
            max_requests_per_minute=1,
        )
        self.register()
        first = self.create_operation()
        second = self.create_operation(
            operation_spec(
                idempotency_key="open-candidate-43",
                target={
                    **operation_spec().target.to_dict(),
                    "resource_id": "candidate-43",
                },
            )
        )
        started_at = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
        with mock.patch(
            "magic_glass_box.storage.service_sidecars.datetime", wraps=datetime
        ) as clock:
            clock.now.return_value = started_at
            active = self.database.begin_service_operation_attempt(
                first["id"],
                phase="invoke",
                owner_id="supervisor-one",
                expected_version=1,
                lease_seconds=30,
            )
            self.database.fail_service_operation_attempt(
                active["attempt"]["id"],
                owner_id="supervisor-one",
                expected_operation_version=2,
                outcome="unavailable",
                error_code="unavailable",
            )
            with self.assertRaisesRegex(ConflictError, "rate limit"):
                self.database.begin_service_operation_attempt(
                    second["id"],
                    phase="invoke",
                    owner_id="supervisor-two",
                    expected_version=1,
                    lease_seconds=30,
                )
            clock.now.return_value = started_at + timedelta(seconds=61)
            released = self.database.begin_service_operation_attempt(
                second["id"],
                phase="invoke",
                owner_id="supervisor-two",
                expected_version=1,
                lease_seconds=30,
            )
        self.assertEqual(released["operation"]["state"], "dispatching")

    def test_connector_throttle_delays_the_same_operation(self) -> None:
        self.register()
        operation = self.create_operation(operation_spec(timeout_ms=100))
        started_at = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
        with mock.patch(
            "magic_glass_box.storage.service_sidecars.datetime", wraps=datetime
        ) as clock:
            clock.now.return_value = started_at
            dispatch = self.database.begin_service_operation_attempt(
                operation["id"],
                phase="invoke",
                owner_id="supervisor-one",
                expected_version=1,
                lease_seconds=5,
            )
            request = ServiceOperationRequest.from_mapping(dispatch["request"])
            response_value = request.correlation_dict()
            response_value.update(
                {
                    "outcome": "throttled",
                    "provider_status": 429,
                    "request_bytes": 128,
                    "response_bytes": 64,
                    "error_code": "rate_limited",
                    "retry_after_seconds": 60,
                    "receipt": None,
                }
            )
            throttled = self.database.complete_service_operation_attempt(
                dispatch["attempt"]["id"],
                owner_id="supervisor-one",
                expected_operation_version=dispatch["operation"]["version"],
                response=ServiceOperationResponse.from_mapping(response_value),
            )
            self.assertEqual(throttled["state"], "throttled")
            self.assertEqual(
                throttled["eligible_at"],
                (started_at + timedelta(seconds=60)).isoformat(timespec="microseconds"),
            )
            with self.assertRaisesRegex(ConflictError, "not eligible"):
                self.database.begin_service_operation_attempt(
                    operation["id"],
                    phase="invoke",
                    owner_id="supervisor-two",
                    expected_version=throttled["version"],
                    lease_seconds=5,
                )
            clock.now.return_value = started_at + timedelta(seconds=60)
            resumed = self.database.begin_service_operation_attempt(
                operation["id"],
                phase="invoke",
                owner_id="supervisor-two",
                expected_version=throttled["version"],
                lease_seconds=5,
            )
        self.assertEqual(resumed["operation"]["state"], "dispatching")

    def test_retryable_response_delays_then_reauthorizes_the_same_operation(
        self,
    ) -> None:
        self.register()
        operation = self.create_operation(operation_spec(timeout_ms=100))
        started_at = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
        with mock.patch(
            "magic_glass_box.storage.service_sidecars.datetime", wraps=datetime
        ) as clock:
            clock.now.return_value = started_at
            dispatch = self.database.begin_service_operation_attempt(
                operation["id"],
                phase="invoke",
                owner_id="supervisor-one",
                expected_version=1,
                lease_seconds=5,
            )
            request = ServiceOperationRequest.from_mapping(dispatch["request"])
            retryable = self.database.complete_service_operation_attempt(
                dispatch["attempt"]["id"],
                owner_id="supervisor-one",
                expected_operation_version=2,
                response=error_response(
                    request,
                    outcome="retryable",
                    provider_status=503,
                    error_code="temporarily_unavailable",
                    retry_after_seconds=7,
                ),
            )
            self.assertEqual(retryable["state"], "retryable")
            self.assertEqual(
                retryable["eligible_at"],
                (started_at + timedelta(seconds=7)).isoformat(timespec="microseconds"),
            )
            clock.now.return_value = started_at + timedelta(seconds=7)
            resumed = self.database.begin_service_operation_attempt(
                operation["id"],
                phase="invoke",
                owner_id="supervisor-two",
                expected_version=retryable["version"],
                lease_seconds=5,
            )
        self.assertEqual(resumed["operation"]["state"], "dispatching")

    def test_denied_and_conflict_responses_are_terminal(self) -> None:
        self.register()
        for index, (outcome, status) in enumerate(
            (("denied", 403), ("conflict", 409)), start=1
        ):
            with self.subTest(outcome=outcome):
                operation = self.create_operation(
                    operation_spec(
                        idempotency_key=f"terminal-{outcome}",
                        target={
                            **operation_spec().target.to_dict(),
                            "resource_id": f"terminal-candidate-{index}",
                        },
                    )
                )
                dispatch = self.database.begin_service_operation_attempt(
                    operation["id"],
                    phase="invoke",
                    owner_id="supervisor-one",
                    expected_version=1,
                    lease_seconds=30,
                )
                request = ServiceOperationRequest.from_mapping(dispatch["request"])
                completed = self.database.complete_service_operation_attempt(
                    dispatch["attempt"]["id"],
                    owner_id="supervisor-one",
                    expected_operation_version=2,
                    response=error_response(
                        request,
                        outcome=outcome,
                        provider_status=status,
                        error_code=outcome,
                    ),
                )
                self.assertEqual(completed["state"], outcome)
                self.assertEqual(completed["completed_at"], completed["updated_at"])

    def test_attempt_lease_exactly_outlives_transport_deadline(self) -> None:
        self.register()
        operation = self.create_operation(operation_spec(timeout_ms=25_002))
        with self.assertRaisesRegex(ValidationError, "outlive"):
            self.database.begin_service_operation_attempt(
                operation["id"],
                phase="invoke",
                owner_id="supervisor-one",
                expected_version=1,
                lease_seconds=26,
            )
        dispatch = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="invoke",
            owner_id="supervisor-one",
            expected_version=1,
            lease_seconds=27,
        )
        self.assertEqual(dispatch["operation"]["state"], "dispatching")

    def test_expired_effect_lease_becomes_uncertain_before_lane_reuse(self) -> None:
        self.register()
        first = self.create_operation(operation_spec(timeout_ms=100))
        second = self.create_operation(
            operation_spec(
                idempotency_key="open-candidate-43",
                target={
                    **operation_spec().target.to_dict(),
                    "resource_id": "candidate-43",
                },
                timeout_ms=100,
            )
        )
        started_at = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
        with mock.patch(
            "magic_glass_box.storage.service_sidecars.datetime", wraps=datetime
        ) as clock:
            clock.now.return_value = started_at
            self.database.begin_service_operation_attempt(
                first["id"],
                phase="invoke",
                owner_id="supervisor-one",
                expected_version=1,
                lease_seconds=5,
            )
            clock.now.return_value = started_at + timedelta(seconds=6)
            claimed = self.database.begin_service_operation_attempt(
                second["id"],
                phase="invoke",
                owner_id="supervisor-two",
                expected_version=1,
                lease_seconds=5,
            )

        expired = self.database.get_service_operation(first["id"])
        self.assertEqual(expired["state"], "uncertain")
        self.assertEqual(expired["version"], 3)
        self.assertEqual(expired["attempts"][0]["outcome"], "abandoned")
        self.assertEqual(claimed["operation"]["state"], "dispatching")

    def test_expired_read_lease_becomes_retryable_before_lane_reuse(self) -> None:
        self.register()
        first = self.create_operation(
            operation_spec(
                capability={"name": "checks.read", "version": 1, "mode": "read"},
                idempotency_key="read-checks-expiring",
                parameters={"check_suite": "required"},
                timeout_ms=100,
            )
        )
        second = self.create_operation(
            operation_spec(
                idempotency_key="open-after-read-expiry",
                target={
                    **operation_spec().target.to_dict(),
                    "resource_id": "candidate-after-read-expiry",
                },
                timeout_ms=100,
            )
        )
        started_at = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
        with mock.patch(
            "magic_glass_box.storage.service_sidecars.datetime", wraps=datetime
        ) as clock:
            clock.now.return_value = started_at
            self.database.begin_service_operation_attempt(
                first["id"],
                phase="invoke",
                owner_id="supervisor-one",
                expected_version=1,
                lease_seconds=5,
            )
            clock.now.return_value = started_at + timedelta(seconds=6)
            claimed = self.database.begin_service_operation_attempt(
                second["id"],
                phase="invoke",
                owner_id="supervisor-two",
                expected_version=1,
                lease_seconds=5,
            )

        expired = self.database.get_service_operation(first["id"])
        self.assertEqual(expired["state"], "retryable")
        self.assertEqual(expired["version"], 3)
        self.assertEqual(expired["attempts"][0]["outcome"], "abandoned")
        self.assertIsNotNone(expired["eligible_at"])
        self.assertEqual(claimed["operation"]["state"], "dispatching")


if __name__ == "__main__":
    unittest.main()
