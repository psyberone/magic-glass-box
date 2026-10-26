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

from magic_glass_box.control_room import ControlRoom
from magic_glass_box.db import SCHEMA_VERSION, ConflictError, Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    InteractionKind,
    ProjectContext,
    ProviderGrantSpec,
    RunState,
    SupervisionPolicy,
    ValidationError,
    WorkItemEnvelope,
    canonical_json,
)
from magic_glass_box.inference import (
    MAX_PROVIDER_RETRY_AFTER_SECONDS,
    InferenceConnectorAmbiguousError,
    InferenceConnectorSpec,
    InferenceConnectorUnavailableError,
    InferenceGrantSpec,
    InferenceInvocationRequest,
    InferenceInvocationResponse,
    InferenceUsage,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.provider import (
    ProviderBudgetError,
    ProviderError,
    ProviderQuotaError,
    ProviderRateLimitError,
    ProviderUpstreamError,
)
from magic_glass_box.results import ResultManifest
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
        self.retry_after_seconds: int | None = None
        self.usage: dict[str, int] = {
            "input_tokens": 20,
            "cached_input_tokens": 5,
            "output_tokens": 5,
            "reasoning_output_tokens": 1,
        }

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
        if request.connector_protocol_version == 2:
            response["usage"] = self.usage if self.outcome == "succeeded" else None
        if self.retry_after_seconds is not None:
            response["retry_after_seconds"] = self.retry_after_seconds
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

    def create_run(
        self,
        *,
        source_revision: str = "abc123",
        krab_session_id: str | None = None,
    ) -> dict[str, Any]:
        run = self.database.create_run(
            project_key="box-project",
            profile_name="Provider Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            krab_session_id=krab_session_id,
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
            source_revision=source_revision,
            container_id="b" * 64,
            container_name="magic-glass-box-inference-test",
            image_id="sha256:" + "a" * 64,
        )
        return self.database.get_run(run["id"])

    def _budgeted_grant_spec(
        self,
        *,
        invocation_microusd: int = 10_000,
        run_microusd: int = 10_028,
        project_microusd: int = 100_000,
        grant_microusd: int = 100_000,
        revision: str = "2026-10-17",
    ) -> InferenceGrantSpec:
        grant = grant_spec().to_dict()
        grant.update(
            {
                "connector_name": "budget-sidecar",
                "max_concurrency": 2,
                "budget": {
                    "schema_version": 1,
                    "account": "personal-example",
                    "revision": revision,
                    "currency": "USD",
                    "invocation_microusd": invocation_microusd,
                    "max_output_tokens": 100,
                    "run_microusd": run_microusd,
                    "project_microusd": project_microusd,
                    "grant_microusd": grant_microusd,
                    "input_microusd_per_million_tokens": 1_000_000,
                    "cached_input_microusd_per_million_tokens": 500_000,
                    "output_microusd_per_million_tokens": 2_000_000,
                },
            }
        )
        return InferenceGrantSpec.from_mapping(grant)

    def _create_budgeted_profile(
        self,
        *,
        grant_name: str,
        profile_name: str,
        invocation_microusd: int = 10_000,
        run_microusd: int = 10_028,
        project_microusd: int = 100_000,
        grant_microusd: int = 100_000,
        revision: str = "2026-10-17",
    ) -> None:
        self.database.create_connector_provider_grant(
            name=grant_name,
            spec=self._budgeted_grant_spec(
                invocation_microusd=invocation_microusd,
                run_microusd=run_microusd,
                project_microusd=project_microusd,
                grant_microusd=grant_microusd,
                revision=revision,
            ),
            expires_at=None,
            created_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name=profile_name,
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc",
                command=["agent"],
                credential_names=[grant_name],
                gateway_capabilities=[GatewayCapability.PROVIDER_INVOKE],
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )

    def _start_run_for_profile(
        self,
        profile_name: str,
        *,
        marker: str,
        supervision_policy: SupervisionPolicy | None = None,
    ) -> dict[str, Any]:
        run = self.database.create_run(
            project_key="box-project",
            profile_name=profile_name,
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            supervision_policy=supervision_policy,
        )
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="codex",
            owner_id="orchestrator-budget-test",
            expected_run_version=run["version"],
        )
        self.database.start_run_attempt(
            attempt["id"],
            owner_id="orchestrator-budget-test",
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision="abc123",
            container_id=marker * 64,
            container_name=f"magic-glass-box-budget-test-{marker}",
            image_id="sha256:" + "d" * 64,
        )
        return self.database.get_run(run["id"])

    def create_budgeted_run(
        self,
        *,
        invocation_microusd: int = 10_000,
        run_microusd: int = 10_028,
        project_microusd: int = 100_000,
        grant_microusd: int = 100_000,
        revision: str = "2026-10-17",
        supervision_policy: SupervisionPolicy | None = None,
    ) -> dict[str, Any]:
        connector = connector_spec(self.socket_path).to_dict()
        connector["connector_protocol_version"] = 2
        connector["max_concurrency"] = 2
        self.database.register_inference_connector(
            name="budget-sidecar",
            spec=InferenceConnectorSpec.from_mapping(connector),
            created_by=self.user["id"],
        )
        self._create_budgeted_profile(
            grant_name="budget-main",
            profile_name="Budget Worker",
            invocation_microusd=invocation_microusd,
            run_microusd=run_microusd,
            project_microusd=project_microusd,
            grant_microusd=grant_microusd,
            revision=revision,
        )
        return self._start_run_for_profile(
            "Budget Worker",
            marker="c",
            supervision_policy=supervision_policy,
        )

    def test_budget_reservation_blocks_concurrent_overspend_then_reconciles(
        self,
    ) -> None:
        run = self.create_budgeted_run()
        self.client.release = threading.Event()
        first_result: list[AcceptedProviderResponse] = []
        first_error: list[BaseException] = []

        def invoke_first() -> None:
            try:
                first_result.append(
                    self.control.invoke_provider(
                        run["id"],
                        grant_name="budget-main",
                        request={"input": "private first request"},
                    )
                )
            except BaseException as exc:  # pragma: no cover - diagnostic capture
                first_error.append(exc)

        thread = threading.Thread(target=invoke_first)
        thread.start()
        self.assertTrue(self.client.started.wait(timeout=2))
        inflight = ControlRoom(self.database).snapshot()["provider_budgets"][0]
        self.assertEqual(inflight["active_reservations"], 1)
        self.assertEqual(inflight["reserved_microusd"], 10_000)
        self.assertEqual(inflight["accounted_microusd"], 0)
        self.assertEqual(inflight["policy_consumed_microusd"], 10_000)
        self.assertEqual(inflight["remaining_microusd"]["run"], 28)
        with self.assertRaises(ProviderBudgetError):
            self.control.invoke_provider(
                run["id"],
                grant_name="budget-main",
                request={"input": "private second request"},
            )
        self.assertEqual(len(self.client.calls), 1)

        self.client.release.set()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(first_error, [])
        self.assertEqual(len(first_result), 1)
        self.assertEqual(first_result[0].usage.total_tokens, 25)
        self.assertEqual(first_result[0].estimated_microusd, 28)

        invocations = self.database.list_provider_invocations(run["id"])
        succeeded = next(item for item in invocations if item["outcome"] == "succeeded")
        denied = next(
            item for item in invocations if item["error_code"] == "budget_exhausted"
        )
        self.assertEqual(succeeded["accounting_status"], "reported")
        self.assertEqual(succeeded["reserved_microusd"], 10_000)
        self.assertEqual(succeeded["accounted_microusd"], 28)
        self.assertEqual(succeeded["input_tokens"], 20)
        self.assertIsNone(denied["budget_policy_sha256"])
        encoded = canonical_json({"invocations": invocations})
        self.assertNotIn("private first request", encoded)
        self.assertNotIn("private second request", encoded)

        # Reconciliation releases the unused allowance, so the next call fits.
        self.client.release = None
        accepted = self.control.invoke_provider(
            run["id"],
            grant_name="budget-main",
            request={"input": "third"},
        )
        self.assertEqual(accepted.estimated_microusd, 28)

        snapshot = ControlRoom(self.database).snapshot()
        budget = snapshot["provider_budgets"][0]
        self.assertEqual(budget["account"], "personal-example")
        self.assertEqual(budget["usage"]["input_tokens"], 40)
        self.assertEqual(budget["usage"]["output_tokens"], 10)
        self.assertEqual(budget["reported_invocations"], 2)
        self.assertEqual(budget["conservative_invocations"], 0)
        self.assertEqual(budget["estimated_microusd"], 56)
        self.assertEqual(budget["accounted_microusd"], 56)
        self.assertEqual(budget["reserved_microusd"], 0)
        self.assertEqual(budget["policy_consumed_microusd"], 56)
        self.assertEqual(
            budget["scope_consumed_microusd"],
            {"run": 56, "grant": 56, "project": 56},
        )
        self.assertEqual(
            budget["remaining_microusd"],
            {"run": 9_972, "grant": 99_944, "project": 99_944},
        )
        self.assertTrue(budget["reconciled"])
        self.assertNotIn("private", canonical_json(snapshot))

    def test_budget_snapshot_aggregates_invocations_in_sql(self) -> None:
        run = self.create_budgeted_run()
        self.control.invoke_provider(
            run["id"], grant_name="budget-main", request={"input": "bounded"}
        )
        statements: list[str] = []
        with self.database.connect() as connection:
            connection.set_trace_callback(statements.append)
            budgets = ControlRoom._provider_budgets(connection, [run["id"]])
            connection.set_trace_callback(None)

        invocation_queries = [
            statement
            for statement in statements
            if "FROM provider_invocations" in statement
        ]
        self.assertEqual(len(budgets), 1)
        self.assertTrue(invocation_queries)
        self.assertTrue(
            all("GROUP BY" in statement for statement in invocation_queries),
            invocation_queries,
        )

    def test_grant_budget_consumption_persists_across_runs(self) -> None:
        first_run = self.create_budgeted_run(
            run_microusd=100_000,
            project_microusd=100_000,
            grant_microusd=10_020,
        )
        accepted = self.control.invoke_provider(
            first_run["id"],
            grant_name="budget-main",
            request={"input": "first"},
        )
        self.assertEqual(accepted.estimated_microusd, 28)

        second_run = self._start_run_for_profile("Budget Worker", marker="e")
        with self.assertRaises(ProviderBudgetError):
            self.control.invoke_provider(
                second_run["id"],
                grant_name="budget-main",
                request={"input": "second"},
            )

        self.assertEqual(len(self.client.calls), 1)
        budget = next(
            item
            for item in ControlRoom(self.database).snapshot()["provider_budgets"]
            if item["run_id"] == second_run["id"]
        )
        self.assertEqual(
            budget["scope_consumed_microusd"],
            {"run": 0, "grant": 28, "project": 28},
        )
        self.assertEqual(
            budget["remaining_microusd"],
            {"run": 100_000, "grant": 9_992, "project": 99_972},
        )

    def test_project_budget_consumption_persists_across_policy_revisions(
        self,
    ) -> None:
        first_run = self.create_budgeted_run(
            run_microusd=100_000,
            project_microusd=10_020,
            grant_microusd=100_000,
        )
        accepted = self.control.invoke_provider(
            first_run["id"],
            grant_name="budget-main",
            request={"input": "first"},
        )
        self.assertEqual(accepted.estimated_microusd, 28)

        self._create_budgeted_profile(
            grant_name="budget-main-v2",
            profile_name="Budget Worker v2",
            run_microusd=100_000,
            project_microusd=10_020,
            grant_microusd=100_000,
            revision="2026-10-18",
        )
        second_run = self._start_run_for_profile("Budget Worker v2", marker="e")
        with self.assertRaises(ProviderBudgetError):
            self.control.invoke_provider(
                second_run["id"],
                grant_name="budget-main-v2",
                request={"input": "second"},
            )

        self.assertEqual(len(self.client.calls), 1)
        budget = next(
            item
            for item in ControlRoom(self.database).snapshot()["provider_budgets"]
            if item["run_id"] == second_run["id"]
        )
        self.assertEqual(budget["revision"], "2026-10-18")
        self.assertEqual(
            budget["scope_consumed_microusd"],
            {"run": 0, "grant": 0, "project": 28},
        )
        self.assertEqual(
            budget["remaining_microusd"],
            {"run": 100_000, "grant": 100_000, "project": 9_992},
        )

    def test_provider_invocation_history_is_bounded_and_returns_latest(self) -> None:
        run = self.create_budgeted_run(run_microusd=20_000)
        self.control.invoke_provider(
            run["id"], grant_name="budget-main", request={"input": "first"}
        )
        self.control.invoke_provider(
            run["id"], grant_name="budget-main", request={"input": "second"}
        )
        history = self.database.list_provider_invocations(run["id"])

        latest = self.database.list_provider_invocations(run["id"], limit=1)

        self.assertEqual(latest, history[-1:])
        with self.assertRaisesRegex(ValidationError, "limit"):
            self.database.list_provider_invocations(run["id"], limit=0)

    def test_ambiguous_budgeted_call_conservatively_consumes_full_allowance(
        self,
    ) -> None:
        run = self.create_budgeted_run()
        self.client.error = InferenceConnectorAmbiguousError("lost reply")

        with self.assertRaises(ProviderError):
            self.control.invoke_provider(
                run["id"], grant_name="budget-main", request={"input": "private"}
            )
        invocation = self.database.list_provider_invocations(run["id"])[0]
        self.assertEqual(invocation["outcome"], "ambiguous")
        self.assertEqual(invocation["accounting_status"], "conservative")
        self.assertEqual(invocation["accounted_microusd"], 10_000)
        budget = ControlRoom(self.database).snapshot()["provider_budgets"][0]
        self.assertEqual(budget["accounted_microusd"], 10_000)
        self.assertEqual(budget["reserved_microusd"], 0)
        self.assertEqual(budget["policy_consumed_microusd"], 10_000)
        self.assertFalse(budget["reconciled"])

        self.client.error = None
        with self.assertRaises(ProviderBudgetError):
            self.control.invoke_provider(
                run["id"], grant_name="budget-main", request={"input": "retry"}
            )
        self.assertEqual(len(self.client.calls), 1)

    def test_non_success_usage_cannot_undercharge_an_ambiguous_call(self) -> None:
        run = self.create_budgeted_run()
        original_invoke = self.client.invoke

        def ambiguous_with_usage(
            request: InferenceInvocationRequest,
        ) -> InferenceInvocationResponse:
            self.client.outcome = "ambiguous"
            self.client.error_code = "reply_lost"
            self.client.provider_status = None
            response = original_invoke(request)
            return replace(
                response,
                usage=InferenceUsage(
                    input_tokens=1,
                    cached_input_tokens=0,
                    output_tokens=0,
                    reasoning_output_tokens=0,
                ),
            )

        self.client.invoke = ambiguous_with_usage  # type: ignore[method-assign]

        with self.assertRaisesRegex(ProviderError, "ambiguous"):
            self.control.invoke_provider(
                run["id"], grant_name="budget-main", request={"input": "one"}
            )

        invocation = self.database.list_provider_invocations(run["id"])[0]
        self.assertEqual(invocation["outcome"], "ambiguous")
        self.assertEqual(invocation["accounting_status"], "conservative")
        self.assertEqual(invocation["accounted_microusd"], 10_000)
        self.assertIsNone(invocation["input_tokens"])

    def test_successful_reported_overrun_is_retained_and_blocks_future_spend(
        self,
    ) -> None:
        run = self.create_budgeted_run(run_microusd=30_000)
        self.client.usage = {
            "input_tokens": 20_000,
            "cached_input_tokens": 0,
            "output_tokens": 5,
            "reasoning_output_tokens": 1,
        }

        accepted = self.control.invoke_provider(
            run["id"], grant_name="budget-main", request={"input": "one"}
        )

        self.assertEqual(accepted.estimated_microusd, 20_010)
        invocation = self.database.list_provider_invocations(run["id"])[0]
        self.assertEqual(invocation["outcome"], "succeeded")
        self.assertEqual(invocation["accounting_status"], "reported")
        self.assertEqual(invocation["reserved_microusd"], 10_000)
        self.assertEqual(invocation["accounted_microusd"], 20_010)
        budget = ControlRoom(self.database).snapshot()["provider_budgets"][0]
        self.assertEqual(budget["remaining_microusd"]["run"], 9_990)
        with self.assertRaises(ProviderBudgetError):
            self.control.invoke_provider(
                run["id"], grant_name="budget-main", request={"input": "two"}
            )
        self.assertEqual(len(self.client.calls), 1)

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
        self.assertNotIn(b'{"output":[{"text":"done"}]}', durable)

    def test_nested_model_override_is_removed_before_connector_dispatch(self) -> None:
        run = self.create_run()

        self.control.invoke_provider(
            run["id"],
            grant_name="example-main",
            request={
                "protocol": "example-json",
                "body": {"model": "worker-selected", "input": "hello"},
                "headers": {},
            },
        )

        self.assertEqual(
            self.client.calls[0].request,
            {
                "protocol": "example-json",
                "body": {"input": "hello"},
                "headers": {},
            },
        )

    def test_oversized_request_is_audited_as_request_error_with_actual_bytes(
        self,
    ) -> None:
        run = self.create_run()
        request = {"input": "x" * 3000}
        expected_bytes = len(canonical_json(request).encode("utf-8"))

        with self.assertRaisesRegex(ValidationError, "size limit"):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request=request
            )

        self.assertEqual(self.client.calls, [])
        invocation = self.database.list_provider_invocations(run["id"])[0]
        self.assertEqual(invocation["outcome"], "denied")
        self.assertEqual(invocation["error_code"], "request_too_large")
        self.assertEqual(invocation["request_bytes"], expected_bytes)

    def test_nonfinite_request_is_audited_before_connector_dispatch(self) -> None:
        run = self.create_run()

        with self.assertRaisesRegex(ValidationError, "finite JSON"):
            self.control.invoke_provider(
                run["id"],
                grant_name="example-main",
                request={"input": float("nan")},
            )

        self.assertEqual(self.client.calls, [])
        invocation = self.database.list_provider_invocations(run["id"])[0]
        self.assertEqual(invocation["outcome"], "denied")
        self.assertEqual(invocation["error_code"], "invalid_request")
        self.assertEqual(invocation["request_bytes"], 0)

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
        self.client.error_code = "server_error"
        self.client.provider_status = 503
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
                ("failed", 503, "server_error"),
                ("ambiguous", None, "reply_lost"),
            ],
        )

    def test_provider_recovery_validation_has_exact_delay_and_id_bounds(self) -> None:
        for delay in (1, MAX_PROVIDER_RETRY_AFTER_SECONDS):
            with self.subTest(valid_delay=delay):
                self.assertEqual(
                    self.database._validate_provider_recovery(
                        category="rate_limited",
                        retry_after_seconds=delay,
                    ),
                    ("rate_limited", delay),
                )

        for delay in (
            True,
            None,
            "1",
            1.0,
            0,
            MAX_PROVIDER_RETRY_AFTER_SECONDS + 1,
        ):
            with (
                self.subTest(invalid_delay=delay),
                self.assertRaisesRegex(ValidationError, "retry_after_seconds"),
            ):
                self.database._validate_provider_recovery(
                    category="rate_limited",
                    retry_after_seconds=delay,
                )

        with self.assertRaises(ValidationError) as raised:
            self.database.list_provider_recoveries("r" * 65)
        self.assertEqual(str(raised.exception), "run_id exceeds 64 UTF-8 bytes")

    def test_quota_pause_wakes_same_run_with_pinned_authority_and_budget(self) -> None:
        automatic = SupervisionPolicy.from_mapping(
            {
                **SupervisionPolicy.disabled().to_dict(),
                "auto_execute": True,
            }
        )
        run = self.create_budgeted_run(
            run_microusd=30_000,
            supervision_policy=automatic,
        )
        original_attempt = run["attempts"][-1]
        original_grant = run["provider_grants"][0]
        self.client.outcome = "failed"
        self.client.error_code = "quota_exhausted"
        self.client.provider_status = 429
        self.client.retry_after_seconds = 120

        with self.assertRaises(ProviderQuotaError) as raised:
            self.control.invoke_provider(
                run["id"], grant_name="budget-main", request={"input": "first"}
            )

        self.assertEqual(raised.exception.retry_after_seconds, 120)
        paused = self.database.get_run(run["id"])
        self.assertEqual(paused["state"], "paused")
        self.assertEqual(len(paused["provider_recoveries"]), 1)
        recovery = paused["provider_recoveries"][0]
        self.assertRegex(recovery["id"], r"^prc_[0-9a-f]{32}$")
        self.assertEqual(self.database.list_provider_recoveries(run["id"]), [recovery])
        self.assertEqual(recovery["state"], "active")
        self.assertEqual(recovery["category"], "quota_exhausted")
        self.assertEqual(recovery["attempt_id"], original_attempt["id"])
        self.assertEqual(
            recovery["invocation_id"], paused["provider_invocations"][0]["id"]
        )
        self.assertEqual(recovery["retry_after_seconds"], 120)
        self.assertEqual(
            datetime.fromisoformat(recovery["eligible_at"])
            - datetime.fromisoformat(recovery["created_at"]),
            timedelta(seconds=120),
        )
        lease_control = self.database.renew_run_attempt_lease(
            original_attempt["id"],
            owner_id="orchestrator-budget-test",
            lease_seconds=60,
        )
        self.assertTrue(lease_control["pause_requested"])
        self.assertEqual(lease_control["provider_recovery_id"], recovery["id"])
        pause_event = paused["events"][-1]
        self.assertEqual(pause_event["kind"], "run.provider_paused")
        self.assertEqual(pause_event["from_state"], "running")
        self.assertEqual(pause_event["to_state"], "paused")
        self.assertEqual(
            pause_event["metadata"],
            {
                "attempt_id": original_attempt["id"],
                "category": "quota_exhausted",
                "eligible_at": recovery["eligible_at"],
                "invocation_id": recovery["invocation_id"],
                "recovery_id": recovery["id"],
                "retry_after_seconds": 120,
            },
        )
        self.assertEqual(len(self.client.calls), 1)
        snapshot = ControlRoom(self.database).snapshot()
        self.assertEqual(snapshot["provider_recoveries"], [recovery])
        self.assertEqual(snapshot["summary"]["runs"]["paused"], 1)
        provider_attention = next(
            item for item in snapshot["attention"] if item["kind"] == "provider.paused"
        )
        self.assertEqual(provider_attention["run_id"], run["id"])
        self.assertEqual(provider_attention["category"], "quota_exhausted")
        self.assertFalse(provider_attention["eligible"])

        with self.assertRaisesRegex(ConflictError, "not running: paused"):
            self.control.invoke_provider(
                run["id"], grant_name="budget-main", request={"input": "too soon"}
            )
        self.assertEqual(len(self.client.calls), 1)
        eligible_at = datetime.fromisoformat(recovery["eligible_at"])
        with mock.patch.object(
            self.database,
            "now",
            return_value=eligible_at.isoformat(timespec="microseconds"),
        ):
            active_actions = self.database.enqueue_supervisor_actions()
        self.assertEqual(
            [item["kind"] for item in active_actions], ["attempt.reconcile"]
        )
        self.assertEqual(self.database.get_run(run["id"])["state"], "paused")
        interrupted = self.database.fail_run_attempt(
            original_attempt["id"],
            owner_id="orchestrator-budget-test",
            expected_attempt_version=original_attempt["version"],
            reason="provider recovery pause requested",
            interrupted=True,
        )
        self.assertEqual(interrupted["state"], "interrupted")
        self.assertEqual(self.database.get_run(run["id"])["state"], "paused")

        with mock.patch.object(
            self.database,
            "now",
            return_value=(eligible_at - timedelta(microseconds=1)).isoformat(
                timespec="microseconds"
            ),
        ):
            self.assertEqual(self.database.enqueue_supervisor_actions(), [])
        with mock.patch.object(
            self.database,
            "now",
            return_value=eligible_at.isoformat(timespec="microseconds"),
        ):
            actions = self.database.enqueue_supervisor_actions()

        self.assertEqual(
            [(item["kind"], item["run_id"]) for item in actions],
            [
                ("run.execute", run["id"]),
            ],
        )
        queued = self.database.get_run(run["id"])
        self.assertEqual(queued["state"], "queued")
        self.assertEqual(queued["provider_recoveries"][0]["state"], "resumed")
        resume_event = queued["events"][-1]
        self.assertEqual(resume_event["kind"], "run.provider_resumed")
        self.assertEqual(resume_event["from_state"], "paused")
        self.assertEqual(resume_event["to_state"], "queued")
        self.assertEqual(
            resume_event["metadata"],
            {
                "category": "quota_exhausted",
                "invocation_id": recovery["invocation_id"],
                "recovery_id": recovery["id"],
            },
        )
        replacement = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="codex",
            owner_id="orchestrator-budget-test",
            expected_run_version=queued["version"],
        )
        self.database.start_run_attempt(
            replacement["id"],
            owner_id="orchestrator-budget-test",
            expected_attempt_version=replacement["version"],
            expected_run_version=replacement["run_version"],
            source_revision="abc123",
            container_id="f" * 64,
            container_name="magic-glass-box-budget-test-f",
            image_id="sha256:" + "d" * 64,
        )
        self.client.outcome = "succeeded"
        self.client.error_code = None
        self.client.provider_status = 200
        self.client.retry_after_seconds = None

        accepted = self.control.invoke_provider(
            run["id"], grant_name="budget-main", request={"input": "continued"}
        )

        self.assertEqual(accepted.grant_name, "budget-main")
        self.assertEqual(len(self.client.calls), 2)
        continued = self.database.get_run(run["id"])
        self.assertEqual(continued["id"], run["id"])
        self.assertEqual(continued["source_revision"], "abc123")
        self.assertEqual(
            continued["provider_grants"][0]["grant_id"],
            original_grant["grant_id"],
        )
        budget = next(
            item
            for item in ControlRoom(self.database).snapshot()["provider_budgets"]
            if item["run_id"] == run["id"]
        )
        self.assertEqual(budget["conservative_invocations"], 1)
        self.assertEqual(budget["reported_invocations"], 1)
        self.assertEqual(budget["remaining_microusd"]["run"], 19_972)

    def test_recoverable_rate_signals_pause_but_503_remains_permanent(self) -> None:
        rate_limited = self.create_run()
        self.client.outcome = "failed"
        self.client.error_code = "too_many_requests"
        self.client.provider_status = 429

        with self.assertRaises(ProviderRateLimitError) as raised:
            self.control.invoke_provider(
                rate_limited["id"],
                grant_name="example-main",
                request={"input": "one"},
            )
        self.assertGreater(raised.exception.retry_after_seconds, 0)
        self.assertEqual(self.database.get_run(rate_limited["id"])["state"], "paused")

        overloaded = self.create_run()
        self.client.error_code = "overloaded_error"
        self.client.provider_status = 529
        with self.assertRaises(ProviderRateLimitError) as overloaded_error:
            self.control.invoke_provider(
                overloaded["id"],
                grant_name="example-main",
                request={"input": "overloaded"},
            )
        self.assertEqual(overloaded_error.exception.retry_after_seconds, 60)
        self.assertEqual(self.database.get_run(overloaded["id"])["state"], "paused")

        permanent = self.create_run()
        self.client.error_code = "server_error"
        self.client.provider_status = 503
        with self.assertRaises(ProviderUpstreamError):
            self.control.invoke_provider(
                permanent["id"],
                grant_name="example-main",
                request={"input": "two"},
            )
        self.assertEqual(self.database.get_run(permanent["id"])["state"], "running")

    def test_recovery_classification_uses_independent_codes_and_statuses(self) -> None:
        cases = (
            ("quota_exhausted", 400, ProviderQuotaError, 900),
            ("server_error", 429, ProviderRateLimitError, 60),
            ("server_error", 529, ProviderRateLimitError, 60),
            ("overloaded_error", 503, ProviderRateLimitError, 60),
        )
        for error_code, status, error_type, retry_after in cases:
            run = self.create_run()
            self.client.outcome = "failed"
            self.client.error_code = error_code
            self.client.provider_status = status
            self.client.retry_after_seconds = None
            with self.subTest(error_code=error_code, status=status):
                with self.assertRaises(error_type) as raised:
                    self.control.invoke_provider(
                        run["id"],
                        grant_name="example-main",
                        request={"input": "classification"},
                    )
                self.assertEqual(raised.exception.retry_after_seconds, retry_after)
                recovery = self.database.get_run(run["id"])["provider_recoveries"][0]
                self.assertEqual(recovery["retry_after_seconds"], retry_after)

    def test_manual_provider_recovery_wakes_without_launch_authority(self) -> None:
        run = self.create_run()
        attempt = run["attempts"][-1]
        self.client.outcome = "failed"
        self.client.error_code = "rate_limit_exceeded"
        self.client.provider_status = 429
        self.client.retry_after_seconds = 30
        with self.assertRaises(ProviderRateLimitError):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "one"}
            )
        recovery = self.database.get_run(run["id"])["provider_recoveries"][0]
        self.database.fail_run_attempt(
            attempt["id"],
            owner_id="orchestrator-inference-test",
            expected_attempt_version=attempt["version"],
            reason="provider recovery pause requested",
            interrupted=True,
        )
        paused = self.database.get_run(run["id"])
        with self.assertRaisesRegex(ConflictError, "provider recovery"):
            self.database.transition_run(
                run["id"],
                target=RunState.QUEUED,
                expected_version=paused["version"],
            )

        with mock.patch.object(
            self.database, "now", return_value=recovery["eligible_at"]
        ):
            actions = self.database.enqueue_supervisor_actions()

        self.assertEqual(actions, [])
        resumed = self.database.get_run(run["id"])
        self.assertEqual(resumed["state"], "queued")
        self.assertEqual(resumed["provider_recoveries"][0]["state"], "resumed")
        self.assertEqual(len(self.client.calls), 1)

    def test_recovery_waits_for_open_interaction_then_wakes_answered_work(self) -> None:
        run = self.create_run(krab_session_id="ses_" + "3" * 32)
        attempt = run["attempts"][-1]
        interaction = self.database.open_interaction(
            run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "4" * 32,
            expected_run_version=run["version"],
        )
        self.client.outcome = "failed"
        self.client.error_code = "quota_exhausted"
        self.client.provider_status = 429
        self.client.retry_after_seconds = 30

        with self.assertRaises(ProviderQuotaError):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "one"}
            )

        paused = self.database.get_run(run["id"])
        recovery = paused["provider_recoveries"][0]
        self.assertEqual(paused["state"], "paused")
        self.assertEqual(paused["events"][-1]["from_state"], "waiting_for_input")
        self.database.fail_run_attempt(
            attempt["id"],
            owner_id="orchestrator-inference-test",
            expected_attempt_version=attempt["version"],
            reason="provider recovery pause requested",
            interrupted=True,
        )
        with mock.patch.object(
            self.database, "now", return_value=recovery["eligible_at"]
        ):
            self.assertEqual(self.database.enqueue_supervisor_actions(), [])
        self.assertEqual(self.database.get_run(run["id"])["state"], "paused")

        answered = self.database.answer_interaction(
            interaction["id"],
            response_item_id="itm_" + "5" * 32,
            expected_version=interaction["version"],
        )
        self.assertEqual(answered["state"], "answered")
        self.assertEqual(answered["run_state"], "paused")
        with mock.patch.object(
            self.database, "now", return_value=recovery["eligible_at"]
        ):
            self.assertEqual(self.database.enqueue_supervisor_actions(), [])
        resumed = self.database.get_run(run["id"])
        self.assertEqual(resumed["state"], "queued")
        self.assertEqual(resumed["provider_recoveries"][0]["state"], "resumed")

    def test_cancelling_a_paused_run_cancels_its_future_wake(self) -> None:
        run = self.create_run()
        attempt = run["attempts"][-1]
        self.client.outcome = "failed"
        self.client.error_code = "quota_exhausted"
        self.client.provider_status = 429
        self.client.retry_after_seconds = 30
        with self.assertRaises(ProviderQuotaError):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "one"}
            )
        paused = self.database.get_run(run["id"])
        self.database.fail_run_attempt(
            attempt["id"],
            owner_id="orchestrator-inference-test",
            expected_attempt_version=attempt["version"],
            reason="provider recovery pause requested",
            interrupted=True,
        )

        cancelled = self.database.request_run_cancellation(
            run["id"],
            requested_by=self.user["id"],
            expected_run_version=paused["version"],
            reason="operator cancelled paused work",
        )

        self.assertEqual(cancelled["state"], "cancelled")
        recovery = cancelled["provider_recoveries"][0]
        self.assertEqual(recovery["state"], "cancelled")
        after_eligibility = (
            datetime.fromisoformat(recovery["eligible_at"]) + timedelta(seconds=1)
        ).isoformat(timespec="microseconds")
        with mock.patch.object(self.database, "now", return_value=after_eligibility):
            self.assertEqual(self.database.enqueue_supervisor_actions(), [])
        self.assertEqual(len(self.client.calls), 1)

    def test_worker_completion_during_provider_pause_preserves_recovery(self) -> None:
        source_revision = "a" * 40
        run = self.create_run(source_revision=source_revision)
        attempt = run["attempts"][-1]
        self.client.outcome = "failed"
        self.client.error_code = "quota_exhausted"
        self.client.provider_status = 429
        self.client.retry_after_seconds = 30
        with self.assertRaises(ProviderQuotaError):
            self.control.invoke_provider(
                run["id"], grant_name="example-main", request={"input": "one"}
            )
        self.database.record_attempt_result(
            attempt["id"], ResultManifest(1, attempt["id"], source_revision, ())
        )

        completed = self.database.complete_run_attempt(
            attempt["id"],
            owner_id="orchestrator-inference-test",
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id="sha256:" + "a" * 64,
        )

        self.assertEqual(completed["state"], "interrupted")
        self.assertEqual(completed["run_state"], "paused")
        paused = self.database.get_run(run["id"])
        self.assertEqual(paused["state"], "paused")
        self.assertEqual(paused["provider_recoveries"][0]["state"], "active")

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

    def test_transport_neutral_client_cannot_bypass_usage_bounds(self) -> None:
        run = self.create_budgeted_run()
        original_invoke = self.client.invoke

        def excessive_usage(
            request: InferenceInvocationRequest,
        ) -> InferenceInvocationResponse:
            response = original_invoke(request)
            return replace(
                response,
                usage=InferenceUsage(
                    input_tokens=20,
                    cached_input_tokens=5,
                    output_tokens=101,
                    reasoning_output_tokens=1,
                ),
            )

        self.client.invoke = excessive_usage  # type: ignore[method-assign]

        with self.assertRaisesRegex(ProviderError, "ambiguous"):
            self.control.invoke_provider(
                run["id"], grant_name="budget-main", request={"input": "one"}
            )

        invocation = self.database.list_provider_invocations(run["id"])[0]
        self.assertEqual(invocation["outcome"], "ambiguous")
        self.assertEqual(invocation["error_code"], "connector_ambiguous")
        self.assertEqual(invocation["accounting_status"], "conservative")
        self.assertEqual(invocation["accounted_microusd"], 10_000)
        budget = ControlRoom(self.database).snapshot()["provider_budgets"][0]
        self.assertEqual(budget["active_reservations"], 0)
        self.assertEqual(budget["conservative_invocations"], 1)
        with self.assertRaises(ProviderBudgetError):
            self.control.invoke_provider(
                run["id"], grant_name="budget-main", request={"input": "retry"}
            )
        self.assertEqual(len(self.client.calls), 1)

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
                DROP TABLE workflow_scope_bindings;
                DROP TABLE advisory_reports;
                DROP TABLE advisory_challenge_reviews;
                DROP TABLE advisory_blind_reviews;
                DROP TABLE advisory_run_bindings;
                DROP TABLE advisory_execution_events;
                DROP TABLE advisory_executions;
                DROP TABLE workflow_completions;
                DROP TABLE workflow_provider_targets;
                DROP TABLE workflow_landing_chains;
                DROP TABLE workflow_landing_bindings;
                DROP TABLE workflow_rulings;
                DROP TABLE integration_review_verdicts;
                DROP TABLE workflow_gate_bindings;
                DROP TABLE workflow_run_bindings;
                DROP TABLE workflow_execution_events;
                DROP TABLE workflow_executions;
                DROP TABLE workflow_configuration_role_bindings;
                DROP TABLE workflow_configuration_versions;
                DROP TABLE workflow_configurations;
                DROP TABLE discovery_rulings;
                DROP TABLE discovery_traces;
                DROP TABLE discovery_run_bindings;
                DROP TABLE discovery_execution_events;
                DROP TABLE discovery_executions;
                DROP TABLE workflow_template_definitions;
                DROP TABLE integration_gate_maintainer_change_runs;
                DROP TABLE integration_maintainer_reviews;
                DROP TABLE integration_gate_change_runs;
                DROP TABLE service_operation_capability_bindings;
                DROP TABLE project_capability_binding_events;
                DROP TABLE project_capability_bindings;
                DROP TABLE service_operation_attempts;
                DROP TABLE service_operations;
                DROP TABLE service_sidecar_events;
                DROP TABLE service_sidecars;
                DROP TRIGGER integration_landing_plan_nonce_immutable_update;
                DROP TRIGGER integration_landing_plan_nonce_immutable_delete;
                DROP TABLE integration_landing_plan_nonces;
                DROP TABLE attempt_recovery_applications;
                DROP TABLE attempt_recovery_checkpoints;
                DROP INDEX attempt_result_identity;
                DROP TABLE provider_recoveries;
                DROP TABLE integration_provider_landings;
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
