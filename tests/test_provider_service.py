from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from typing import Any

from magic_glass_box.credentials import PrivateFileCredentialResolver
from magic_glass_box.db import ConflictError, Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    InteractionKind,
    ProjectContext,
    ProviderGrantSpec,
    RunState,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.provider import (
    ProviderBroker,
    ProviderInvocationResult,
    ProviderUpstreamError,
)
from magic_glass_box.service import AcceptedProviderResponse, ControlPlane


def policy() -> ProviderGrantSpec:
    return ProviderGrantSpec.from_mapping(
        {
            "provider": "example-ai",
            "adapter": "json_http",
            "destination": "https://provider.example/v1/invoke",
            "method": "POST",
            "model": "bounded-model",
            "credential_header": "Authorization",
            "credential_prefix": "Bearer ",
            "static_headers": {},
            "request_bytes": 4096,
            "response_bytes": 8192,
            "timeout_ms": 10000,
            "max_concurrency": 1,
        }
    )


class RecordingAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[ProviderGrantSpec, str, dict[str, Any]]] = []
        self.failure: ProviderUpstreamError | None = None

    def invoke(
        self,
        spec: ProviderGrantSpec,
        credential: str,
        request: dict[str, Any],
    ) -> ProviderInvocationResult:
        self.calls.append((spec, credential, request))
        if self.failure is not None:
            raise self.failure
        return ProviderInvocationResult(
            status_code=200,
            request_bytes=51,
            response_bytes=37,
            untrusted_response={"output": [{"text": "done"}]},
        )


class ProviderServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.secret = "provider-secret-value"  # noqa: S105
        self.secret_path = self.root / "provider.secret"
        self.secret_path.write_text(self.secret + "\n", encoding="ascii")
        os.chmod(self.secret_path, 0o600)
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
            context=ProjectContext.from_mapping({"purpose": "Broker requests"}),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "provider-service",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Broker a provider request",
                    "brief": "Use only the run-scoped provider authority.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.adapter = RecordingAdapter()
        self.control = ControlPlane(
            self.database,
            credential_resolver=PrivateFileCredentialResolver(),
            provider_broker=ProviderBroker(adapters={"json_http": self.adapter}),
        )
        self.grant = self.control.add_provider_grant(
            name="example-main",
            spec=policy(),
            credential_path=self.secret_path,
            expires_at=None,
            created_by_handle="operator",
        )
        self.database.create_agent_profile(
            name="Provider Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc",
                command=["agent"],
                credential_names=["krab", "example-main"],
                gateway_capabilities=[GatewayCapability.PROVIDER_INVOKE],
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )

    def create_run(
        self, *, running: bool = True, session_id: str | None = None
    ) -> dict[str, object]:
        run = self.database.create_run(
            project_key="box-project",
            profile_name="Provider Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            krab_session_id=session_id,
        )
        if running:
            attempt = self.database.begin_run_attempt(
                str(run["id"]),
                runtime_adapter="codex",
                owner_id="orchestrator-provider-test",
                expected_run_version=int(run["version"]),
            )
            self.database.start_run_attempt(
                str(attempt["id"]),
                expected_attempt_version=int(attempt["version"]),
                expected_run_version=int(attempt["run_version"]),
                source_revision="abc123",
                container_id="b" * 64,
                container_name="magic-glass-box-provider-test",
                image_id="sha256:" + "a" * 64,
            )
            run = self.database.get_run(str(run["id"]))
        return run

    def test_add_and_invoke_keep_secret_transient_and_audit_only_metadata(self) -> None:
        self.assertNotIn("credential_ref", self.grant)
        self.assertNotIn(self.secret.encode("ascii"), self.database.path.read_bytes())
        run = self.create_run()

        accepted = self.control.invoke_provider(
            str(run["id"]),
            grant_name="example-main",
            request={"input": "hello"},
        )

        self.assertIsInstance(accepted, AcceptedProviderResponse)
        self.assertEqual(accepted.grant_name, "example-main")
        self.assertEqual(accepted.provider, "example-ai")
        self.assertEqual(accepted.model, "bounded-model")
        self.assertEqual(accepted.untrusted_response, {"output": [{"text": "done"}]})
        self.assertEqual(
            self.adapter.calls,
            [(policy(), self.secret, {"input": "hello"})],
        )
        invocations = self.database.list_provider_invocations(str(run["id"]))
        self.assertEqual(len(invocations), 1)
        self.assertEqual(invocations[0]["outcome"], "succeeded")
        self.assertEqual(invocations[0]["request_bytes"], 51)
        self.assertEqual(invocations[0]["response_bytes"], 37)
        database_bytes = self.database.path.read_bytes()
        self.assertNotIn(self.secret.encode("ascii"), database_bytes)
        self.assertNotIn(b"hello", database_bytes)
        self.assertNotIn(b"done", database_bytes)

    def test_non_running_and_revoked_run_authority_fail_before_resolution(self) -> None:
        queued = self.create_run(running=False)
        with self.assertRaisesRegex(ConflictError, "run is not running"):
            self.control.invoke_provider(
                str(queued["id"]),
                grant_name="example-main",
                request={"input": "hello"},
            )
        self.assertEqual(self.adapter.calls, [])

        running = self.create_run()
        self.control.revoke_provider_grant(
            "example-main", expected_version=1, revoked_by_handle="operator"
        )
        with self.assertRaisesRegex(ConflictError, "provider grant.*unavailable"):
            self.control.invoke_provider(
                str(running["id"]),
                grant_name="example-main",
                request={"input": "hello"},
            )
        self.assertEqual(self.adapter.calls, [])

    def test_waiting_run_keeps_provider_authority_only_for_its_live_attempt(
        self,
    ) -> None:
        run = self.create_run(running=False, session_id="ses_" + "8" * 32)
        attempt = self.database.begin_run_attempt(
            str(run["id"]),
            runtime_adapter="codex",
            owner_id="orchestrator-provider-test",
            expected_run_version=int(run["version"]),
        )
        running = self.database.start_run_attempt(
            str(attempt["id"]),
            expected_attempt_version=int(attempt["version"]),
            expected_run_version=int(attempt["run_version"]),
            source_revision="abc123",
            container_id="b" * 64,
            container_name="magic-glass-box-provider-test",
            image_id="sha256:" + "a" * 64,
        )
        self.database.open_interaction(
            str(run["id"]),
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=str(self.identity["id"]),
            request_item_id="itm_" + "9" * 32,
            expected_run_version=int(running["run_version"]),
        )

        accepted = self.control.invoke_provider(
            str(run["id"]),
            grant_name="example-main",
            request={"input": "submit the durable handoff"},
        )

        self.assertEqual(accepted.model, "bounded-model")
        self.assertEqual(len(self.adapter.calls), 1)

    def test_running_state_without_a_live_attempt_has_no_provider_authority(
        self,
    ) -> None:
        run = self.database.create_run(
            project_key="box-project",
            profile_name="Provider Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
        )
        preparing = self.database.transition_run(
            str(run["id"]), target=RunState.PREPARING, expected_version=1
        )
        running = self.database.transition_run(
            str(run["id"]),
            target=RunState.RUNNING,
            expected_version=int(preparing["version"]),
            source_revision="abc123",
        )

        with self.assertRaisesRegex(ConflictError, "run is not running"):
            self.control.invoke_provider(
                str(running["id"]),
                grant_name="example-main",
                request={"input": "must remain denied"},
            )

        self.assertEqual(self.adapter.calls, [])

        waiting = self.database.transition_run(
            str(running["id"]),
            target=RunState.WAITING_FOR_INPUT,
            expected_version=int(running["version"]),
        )
        with self.assertRaisesRegex(ConflictError, "run is not running"):
            self.control.invoke_provider(
                str(waiting["id"]),
                grant_name="example-main",
                request={"input": "must remain denied"},
            )
        self.assertEqual(self.adapter.calls, [])

    def test_upstream_failure_is_redacted_and_metadata_is_audited(self) -> None:
        run = self.create_run()
        self.adapter.failure = ProviderUpstreamError(
            "provider request failed",
            status_code=429,
            request_bytes=50,
            response_bytes=25,
        )
        with self.assertRaises(ProviderUpstreamError):
            self.control.invoke_provider(
                str(run["id"]),
                grant_name="example-main",
                request={"input": "sensitive request"},
            )
        invocation = self.database.list_provider_invocations(str(run["id"]))[0]
        self.assertEqual(invocation["outcome"], "failed")
        self.assertEqual(invocation["http_status"], 429)
        self.assertEqual(invocation["error_code"], "upstream_error")
        serialized = str(invocation)
        self.assertNotIn("sensitive", serialized)
        self.assertNotIn(self.secret, serialized)


if __name__ == "__main__":
    unittest.main()
