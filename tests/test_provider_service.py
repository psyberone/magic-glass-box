from __future__ import annotations

import os
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from magic_glass_box.db import ConflictError, Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    InteractionKind,
    ProjectContext,
    RunState,
    WorkItemEnvelope,
    canonical_json,
)
from magic_glass_box.inference import (
    InferenceConnectorSpec,
    InferenceGrantSpec,
    InferenceInvocationRequest,
    InferenceInvocationResponse,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.provider import (
    ProviderBusyError,
    ProviderError,
    ProviderUpstreamError,
)
from magic_glass_box.service import AcceptedProviderResponse, ControlPlane


def policy() -> InferenceGrantSpec:
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
            "request_bytes": 4096,
            "response_bytes": 8192,
            "timeout_ms": 10000,
            "max_concurrency": 1,
        }
    )


def connector_policy(socket_path: Path) -> InferenceConnectorSpec:
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


class RecordingConnector:
    def __init__(self) -> None:
        self.calls: list[InferenceInvocationRequest] = []
        self.outcome = "succeeded"
        self.error_code: str | None = None
        self.provider_status: int | None = 200

    def invoke(
        self,
        request: InferenceInvocationRequest,
    ) -> InferenceInvocationResponse:
        self.calls.append(request)
        response = request.to_dict()
        response.pop("request")
        response.update(
            {
                "outcome": self.outcome,
                "provider_status": self.provider_status,
                "request_bytes": len(canonical_json(request.request).encode("utf-8")),
                "response_bytes": 37 if self.outcome == "succeeded" else 25,
                "error_code": self.error_code,
                "response": (
                    {"output": [{"text": "done"}]}
                    if self.outcome == "succeeded"
                    else None
                ),
            }
        )
        return InferenceInvocationResponse.from_mapping(response)


class BlockingConnector(RecordingConnector):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def invoke(
        self,
        request: InferenceInvocationRequest,
    ) -> InferenceInvocationResponse:
        self.started.set()
        if not self.release.wait(timeout=2):
            raise RuntimeError("provider test release timed out")
        return super().invoke(request)


class ProviderServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
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
        self.connector = RecordingConnector()
        self.control = ControlPlane(
            self.database,
            inference_client_factory=lambda _path: self.connector,
        )
        self.database.register_inference_connector(
            name="example-sidecar",
            spec=connector_policy(self.root / "example-sidecar.sock"),
            created_by=self.user["id"],
        )
        self.grant = self.control.add_connector_provider_grant(
            name="example-main",
            spec=policy(),
            expires_at=None,
            created_by_handle="operator",
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

    def test_add_and_invoke_use_connector_and_audit_only_metadata(self) -> None:
        self.assertNotIn("credential_ref", self.grant)
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
        self.assertEqual(len(self.connector.calls), 1)
        self.assertEqual(self.connector.calls[0].request, {"input": "hello"})
        invocations = self.database.list_provider_invocations(str(run["id"]))
        self.assertEqual(len(invocations), 1)
        self.assertEqual(invocations[0]["outcome"], "succeeded")
        self.assertEqual(invocations[0]["request_bytes"], 17)
        self.assertEqual(invocations[0]["response_bytes"], 37)
        database_bytes = self.database.path.read_bytes()
        self.assertNotIn(b"hello", database_bytes)
        self.assertNotIn(b"done", database_bytes)
        with closing(self.database.connect()) as connection:
            active = connection.execute(
                "SELECT COUNT(*) FROM provider_dispatch_reservations"
            ).fetchone()[0]
        self.assertEqual(active, 0)

    def test_release_failure_does_not_erase_a_completed_invocation(self) -> None:
        run = self.create_run()

        with mock.patch.object(
            self.database,
            "release_provider_dispatch",
            side_effect=RuntimeError("private release failure"),
        ):
            accepted = self.control.invoke_provider(
                str(run["id"]),
                grant_name="example-main",
                request={"input": "hello"},
            )

        self.assertIsInstance(accepted, AcceptedProviderResponse)
        invocations = self.database.list_provider_invocations(str(run["id"]))
        self.assertEqual(
            [(item["outcome"], item["error_code"]) for item in invocations],
            [("succeeded", None)],
        )

    def test_dispatch_reservations_release_and_reclaim_expired_slots(self) -> None:
        run = self.create_run()
        first_time = "2026-10-14T12:00:00.000000+00:00"
        later_time = "2026-10-14T12:00:02.000000+00:00"
        with mock.patch.object(self.database, "now", return_value=first_time):
            first = self.database.reserve_provider_dispatch(
                str(run["id"]),
                "example-main",
                expected_grant_id=str(self.grant["id"]),
                lease_seconds=1,
            )
            busy = self.database.reserve_provider_dispatch(
                str(run["id"]),
                "example-main",
                expected_grant_id=str(self.grant["id"]),
                lease_seconds=1,
            )
        self.assertIsNotNone(first)
        self.assertIsNone(busy)

        with mock.patch.object(self.database, "now", return_value=later_time):
            replacement = self.database.reserve_provider_dispatch(
                str(run["id"]),
                "example-main",
                expected_grant_id=str(self.grant["id"]),
                lease_seconds=1,
            )
        self.assertIsNotNone(replacement)
        assert first is not None and replacement is not None
        self.assertNotEqual(first["id"], replacement["id"])
        self.assertTrue(self.database.release_provider_dispatch(replacement["id"]))
        self.assertFalse(self.database.release_provider_dispatch(replacement["id"]))

        for lease_seconds in (0, True, 301):
            with (
                self.subTest(lease_seconds=lease_seconds),
                self.assertRaisesRegex(ValueError, "between 1 and 300"),
            ):
                self.database.reserve_provider_dispatch(
                    str(run["id"]),
                    "example-main",
                    expected_grant_id=str(self.grant["id"]),
                    lease_seconds=lease_seconds,
                )

    def test_dispatch_reservation_renewal_extends_only_a_live_claim(self) -> None:
        run = self.create_run()
        started = "2026-10-14T12:00:00.000000+00:00"
        renewed = "2026-10-14T12:00:00.500000+00:00"
        after_original_expiry = "2026-10-14T12:00:01.250000+00:00"
        after_renewed_expiry = "2026-10-14T12:00:02.750000+00:00"
        with mock.patch.object(self.database, "now", return_value=started):
            reservation = self.database.reserve_provider_dispatch(
                str(run["id"]),
                "example-main",
                expected_grant_id=str(self.grant["id"]),
                lease_seconds=1,
            )
        assert reservation is not None

        with mock.patch.object(self.database, "now", return_value=renewed):
            extended = self.database.renew_provider_dispatch(
                reservation["id"], lease_seconds=2
            )
        self.assertEqual(extended["id"], reservation["id"])
        self.assertGreater(extended["expires_at"], reservation["expires_at"])

        with mock.patch.object(
            self.database, "now", return_value=after_original_expiry
        ):
            self.assertIsNone(
                self.database.reserve_provider_dispatch(
                    str(run["id"]),
                    "example-main",
                    expected_grant_id=str(self.grant["id"]),
                    lease_seconds=1,
                )
            )

        with (
            mock.patch.object(self.database, "now", return_value=after_renewed_expiry),
            self.assertRaisesRegex(ConflictError, "reservation is unavailable"),
        ):
            self.database.renew_provider_dispatch(reservation["id"], lease_seconds=2)

    def test_connector_concurrency_limit_is_shared_across_distinct_grants(
        self,
    ) -> None:
        second = self.database.create_connector_provider_grant(
            name="example-secondary",
            spec=policy(),
            expires_at=None,
            created_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Two Grant Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc",
                command=["agent"],
                credential_names=["example-main", "example-secondary"],
                gateway_capabilities=[GatewayCapability.PROVIDER_INVOKE],
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )
        run = self.database.create_run(
            project_key="box-project",
            profile_name="Two Grant Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
        )
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="codex",
            owner_id="orchestrator-provider-test",
            expected_run_version=run["version"],
        )
        self.database.start_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision="abc123",
            container_id="c" * 64,
            container_name="magic-glass-box-two-grant-test",
            image_id="sha256:" + "a" * 64,
        )

        first = self.database.reserve_provider_dispatch(
            run["id"],
            "example-main",
            expected_grant_id=self.grant["id"],
            lease_seconds=30,
        )
        blocked = self.database.reserve_provider_dispatch(
            run["id"],
            "example-secondary",
            expected_grant_id=second["id"],
            lease_seconds=30,
        )

        self.assertIsNotNone(first)
        self.assertIsNone(blocked)
        assert first is not None
        self.assertTrue(self.database.release_provider_dispatch(first["id"]))

    def test_non_running_and_revoked_run_authority_fail_before_resolution(self) -> None:
        queued = self.create_run(running=False)
        with self.assertRaisesRegex(ConflictError, "run is not running"):
            self.control.invoke_provider(
                str(queued["id"]),
                grant_name="example-main",
                request={"input": "hello"},
            )
        self.assertEqual(self.connector.calls, [])

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
        self.assertEqual(self.connector.calls, [])

    def test_revocation_committed_before_dispatch_fails_closed(self) -> None:
        run = self.create_run()
        connector = RecordingConnector()
        control = ControlPlane(
            self.database,
            inference_client_factory=lambda _path: connector,
        )
        reserve_started = threading.Event()
        reserve_release = threading.Event()
        original_reserve = self.database.reserve_provider_dispatch

        def reserve(*args: object, **kwargs: object) -> object:
            reserve_started.set()
            if not reserve_release.wait(timeout=2):
                raise RuntimeError("reservation test release timed out")
            return original_reserve(*args, **kwargs)  # type: ignore[arg-type]

        outcomes: list[object] = []

        def invoke() -> None:
            try:
                outcomes.append(
                    control.invoke_provider(
                        str(run["id"]),
                        grant_name="example-main",
                        request={"input": "must not dispatch"},
                    )
                )
            except Exception as exc:  # noqa: BLE001 - asserted below
                outcomes.append(exc)

        with mock.patch.object(
            self.database, "reserve_provider_dispatch", side_effect=reserve
        ):
            worker = threading.Thread(target=invoke)
            worker.start()
            self.assertTrue(reserve_started.wait(timeout=1))
            self.control.revoke_inference_connector(
                "example-sidecar", expected_version=1, revoked_by_handle="operator"
            )
            reserve_release.set()
            worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], ConflictError)
        self.assertEqual(connector.calls, [])
        invocations = self.database.list_provider_invocations(str(run["id"]))
        self.assertEqual(
            [(item["outcome"], item["error_code"]) for item in invocations],
            [("denied", "authority_unavailable")],
        )

    def test_concurrency_limit_is_shared_by_control_plane_instances(self) -> None:
        run = self.create_run()
        first_connector = BlockingConnector()
        second_connector = RecordingConnector()
        first_control = ControlPlane(
            self.database,
            inference_client_factory=lambda _path: first_connector,
        )
        second_control = ControlPlane(
            self.database,
            inference_client_factory=lambda _path: second_connector,
        )
        first_outcomes: list[object] = []

        def invoke_first() -> None:
            try:
                first_outcomes.append(
                    first_control.invoke_provider(
                        str(run["id"]),
                        grant_name="example-main",
                        request={"input": "first"},
                    )
                )
            except Exception as exc:  # noqa: BLE001 - asserted below
                first_outcomes.append(exc)

        worker = threading.Thread(target=invoke_first)
        worker.start()
        self.assertTrue(first_connector.started.wait(timeout=1))
        try:
            with self.assertRaises(ProviderBusyError):
                second_control.invoke_provider(
                    str(run["id"]),
                    grant_name="example-main",
                    request={"input": "second"},
                )
        finally:
            first_connector.release.set()
            worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(first_outcomes), 1)
        self.assertIsInstance(first_outcomes[0], AcceptedProviderResponse)
        self.assertEqual(second_connector.calls, [])
        invocations = self.database.list_provider_invocations(str(run["id"]))
        self.assertEqual(
            sorted(item["outcome"] for item in invocations),
            ["busy", "succeeded"],
        )

    def test_live_provider_dispatch_renews_its_cross_process_slot(self) -> None:
        run = self.create_run()
        connector = BlockingConnector()
        control = ControlPlane(
            self.database,
            inference_client_factory=lambda _path: connector,
        )
        renewed = threading.Event()
        original_renew = self.database.renew_provider_dispatch
        outcomes: list[object] = []

        def renew(*args: object, **kwargs: object) -> object:
            result = original_renew(*args, **kwargs)  # type: ignore[arg-type]
            renewed.set()
            return result

        def invoke() -> None:
            try:
                outcomes.append(
                    control.invoke_provider(
                        str(run["id"]),
                        grant_name="example-main",
                        request={"input": "slow"},
                    )
                )
            except Exception as exc:  # noqa: BLE001 - asserted below
                outcomes.append(exc)

        with (
            mock.patch.object(
                self.database, "renew_provider_dispatch", side_effect=renew
            ),
            mock.patch(
                "magic_glass_box.service.PROVIDER_DISPATCH_RENEW_INTERVAL_SECONDS",
                0.01,
            ),
        ):
            worker = threading.Thread(target=invoke)
            worker.start()
            self.assertTrue(connector.started.wait(timeout=1))
            try:
                self.assertTrue(renewed.wait(timeout=1))
            finally:
                connector.release.set()
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], AcceptedProviderResponse)

    def test_provider_dispatch_renewal_failure_fails_closed_and_releases(self) -> None:
        run = self.create_run()
        connector = BlockingConnector()
        control = ControlPlane(
            self.database,
            inference_client_factory=lambda _path: connector,
        )
        renewal_attempted = threading.Event()
        outcomes: list[object] = []

        def reject_renewal(*_args: object, **_kwargs: object) -> None:
            renewal_attempted.set()
            raise RuntimeError("private database detail")

        def invoke() -> None:
            try:
                outcomes.append(
                    control.invoke_provider(
                        str(run["id"]),
                        grant_name="example-main",
                        request={"input": "slow"},
                    )
                )
            except Exception as exc:  # noqa: BLE001 - asserted below
                outcomes.append(exc)

        with (
            mock.patch.object(
                self.database,
                "renew_provider_dispatch",
                side_effect=reject_renewal,
            ),
            mock.patch(
                "magic_glass_box.service.PROVIDER_DISPATCH_RENEW_INTERVAL_SECONDS",
                0.01,
            ),
        ):
            worker = threading.Thread(target=invoke)
            worker.start()
            self.assertTrue(connector.started.wait(timeout=1))
            try:
                self.assertTrue(renewal_attempted.wait(timeout=1))
            finally:
                connector.release.set()
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], ProviderError)
        self.assertNotIn("private database detail", str(outcomes[0]))
        with closing(self.database.connect()) as connection:
            active = connection.execute(
                "SELECT COUNT(*) FROM provider_dispatch_reservations"
            ).fetchone()[0]
        self.assertEqual(active, 0)
        invocations = self.database.list_provider_invocations(str(run["id"]))
        self.assertEqual(
            [(item["outcome"], item["error_code"]) for item in invocations],
            [("ambiguous", "reservation_lost")],
        )

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
        self.assertEqual(len(self.connector.calls), 1)

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

        self.assertEqual(self.connector.calls, [])

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
        self.assertEqual(self.connector.calls, [])

    def test_upstream_failure_is_redacted_and_metadata_is_audited(self) -> None:
        run = self.create_run()
        self.connector.outcome = "failed"
        self.connector.error_code = "quota_exhausted"
        self.connector.provider_status = 429
        with self.assertRaises(ProviderUpstreamError):
            self.control.invoke_provider(
                str(run["id"]),
                grant_name="example-main",
                request={"input": "sensitive request"},
            )
        invocation = self.database.list_provider_invocations(str(run["id"]))[0]
        self.assertEqual(invocation["outcome"], "failed")
        self.assertEqual(invocation["http_status"], 429)
        self.assertEqual(invocation["error_code"], "quota_exhausted")
        serialized = str(invocation)
        self.assertNotIn("sensitive", serialized)


if __name__ == "__main__":
    unittest.main()
