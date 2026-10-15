from __future__ import annotations

import os
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from typing import Any
from unittest import mock

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
    ProviderBusyError,
    ProviderError,
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


class BlockingAdapter(RecordingAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def invoke(
        self,
        spec: ProviderGrantSpec,
        credential: str,
        request: dict[str, Any],
    ) -> ProviderInvocationResult:
        self.calls.append((spec, credential, request))
        self.started.set()
        if not self.release.wait(timeout=2):
            raise RuntimeError("provider test release timed out")
        return ProviderInvocationResult(
            status_code=200,
            request_bytes=51,
            response_bytes=37,
            untrusted_response={"output": [{"text": "done"}]},
        )


class BlockingSecretResolver:
    def __init__(self, secret: str) -> None:
        self.secret = secret
        self.started = threading.Event()
        self.release = threading.Event()

    def resolve_secret(self, reference: str) -> str:
        self.started.set()
        if not self.release.wait(timeout=2):
            raise RuntimeError("credential test release timed out")
        return self.secret


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
        with closing(self.database.connect()) as connection:
            active = connection.execute(
                "SELECT COUNT(*) FROM provider_dispatch_reservations"
            ).fetchone()[0]
        self.assertEqual(active, 0)

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

    def test_revocation_committed_before_dispatch_fails_closed(self) -> None:
        run = self.create_run()
        resolver = BlockingSecretResolver(self.secret)
        adapter = RecordingAdapter()
        control = ControlPlane(
            self.database,
            credential_resolver=resolver,  # type: ignore[arg-type]
            provider_broker=ProviderBroker(adapters={"json_http": adapter}),
        )
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

        worker = threading.Thread(target=invoke)
        worker.start()
        self.assertTrue(resolver.started.wait(timeout=1))
        self.control.revoke_provider_grant(
            "example-main", expected_version=1, revoked_by_handle="operator"
        )
        resolver.release.set()
        worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], ConflictError)
        self.assertEqual(adapter.calls, [])
        invocations = self.database.list_provider_invocations(str(run["id"]))
        self.assertEqual(
            [(item["outcome"], item["error_code"]) for item in invocations],
            [("denied", "authority_unavailable")],
        )

    def test_concurrency_limit_is_shared_by_control_plane_instances(self) -> None:
        run = self.create_run()
        first_adapter = BlockingAdapter()
        second_adapter = RecordingAdapter()
        first_control = ControlPlane(
            self.database,
            credential_resolver=PrivateFileCredentialResolver(),
            provider_broker=ProviderBroker(adapters={"json_http": first_adapter}),
        )
        second_control = ControlPlane(
            self.database,
            credential_resolver=PrivateFileCredentialResolver(),
            provider_broker=ProviderBroker(adapters={"json_http": second_adapter}),
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
        self.assertTrue(first_adapter.started.wait(timeout=1))
        try:
            with self.assertRaises(ProviderBusyError):
                second_control.invoke_provider(
                    str(run["id"]),
                    grant_name="example-main",
                    request={"input": "second"},
                )
        finally:
            first_adapter.release.set()
            worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(first_outcomes), 1)
        self.assertIsInstance(first_outcomes[0], AcceptedProviderResponse)
        self.assertEqual(second_adapter.calls, [])
        invocations = self.database.list_provider_invocations(str(run["id"]))
        self.assertEqual(
            sorted(item["outcome"] for item in invocations),
            ["busy", "succeeded"],
        )

    def test_live_provider_dispatch_renews_its_cross_process_slot(self) -> None:
        run = self.create_run()
        adapter = BlockingAdapter()
        control = ControlPlane(
            self.database,
            credential_resolver=PrivateFileCredentialResolver(),
            provider_broker=ProviderBroker(adapters={"json_http": adapter}),
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
            self.assertTrue(adapter.started.wait(timeout=1))
            try:
                self.assertTrue(renewed.wait(timeout=1))
            finally:
                adapter.release.set()
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], AcceptedProviderResponse)

    def test_provider_dispatch_renewal_failure_fails_closed_and_releases(self) -> None:
        run = self.create_run()
        adapter = BlockingAdapter()
        control = ControlPlane(
            self.database,
            credential_resolver=PrivateFileCredentialResolver(),
            provider_broker=ProviderBroker(adapters={"json_http": adapter}),
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
            self.assertTrue(adapter.started.wait(timeout=1))
            try:
                self.assertTrue(renewal_attempted.wait(timeout=1))
            finally:
                adapter.release.set()
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
            [("denied", "request_denied")],
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
