from __future__ import annotations

import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from magic_glass_box.credentials import PrivateFileCredentialResolver
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    HandoffEnvelope,
    InteractionKind,
    NetworkMode,
    ProjectContext,
    ProviderGrantSpec,
    RunState,
    WorkItemEnvelope,
    WorkspaceMode,
)
from magic_glass_box.executor import SandboxPlan, SandboxRequest, SandboxResult
from magic_glass_box.gateway import GatewayEndpoint
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.orchestration import RunCoordinator, RunOrchestrationError
from magic_glass_box.results import CapturedResult, ResultManifest
from magic_glass_box.runtime import RuntimeAdapterError, RuntimeLaunch
from magic_glass_box.service import ControlPlane
from magic_glass_box.workspace import PreparedWorkspace

USER_ID = "usr_" + "1" * 32
PROJECT_ID = "prj_" + "2" * 32
CONTAINER_ID = "b" * 64
IMAGE_ID = "sha256:" + "a" * 64
COMMIT = "c" * 40
TOKEN = f"krab_{USER_ID}_" + "A" * 43


class FakeWorkspaceManager:
    def __init__(self, root: Path):
        self.root = root
        self.prepare_error: Exception | None = None
        self.cleanup_error: Exception | None = None
        self.prepare_calls: list[tuple[str, Path, str | None]] = []
        self.cleanup_calls: list[str] = []

    def prepare(
        self, run_id: str, repository: Path, revision: str | None
    ) -> PreparedWorkspace:
        self.prepare_calls.append((run_id, repository, revision))
        if self.prepare_error is not None:
            raise self.prepare_error
        workspace = self.root / run_id
        workspace.mkdir(mode=0o700, exist_ok=False)
        return PreparedWorkspace(workspace, COMMIT)

    def cleanup(self, run_id: str) -> bool:
        self.cleanup_calls.append(run_id)
        if self.cleanup_error is not None:
            raise self.cleanup_error
        workspace = self.root / run_id
        if not workspace.exists():
            return False
        workspace.rmdir()
        return True

    def locate(self, run_id: str) -> Path | None:
        workspace = self.root / run_id
        return workspace if workspace.exists() else None


class FakeExecutor:
    def __init__(self):
        self.exit_code = 0
        self.stdout = ""
        self.execute_error: Exception | None = None
        self.error_after_callback = False
        self.skip_callback = False
        self.inconsistent_result = False
        self.cleanup_error: Exception | None = None
        self.during_execute = None
        self.requests: list[SandboxRequest] = []
        self.cleanup_calls: list[str] = []
        self.stop_calls: list[str] = []
        self.stop_results: list[int] = []
        self.stopped = threading.Event()

    def execute(self, request: SandboxRequest, *, before_start=None) -> SandboxResult:
        self.requests.append(request)
        if self.execute_error is not None and not self.error_after_callback:
            raise self.execute_error
        plan = SandboxPlan(
            request.run_id,
            "magic-glass-box-" + request.run_id.removeprefix("run_"),
            IMAGE_ID,
            (),
        )
        if not self.skip_callback and before_start is not None:
            before_start(plan, CONTAINER_ID)
        if self.during_execute is not None:
            self.during_execute(request)
        if self.execute_error is not None:
            raise self.execute_error
        return SandboxResult(
            "d" * 64 if self.inconsistent_result else CONTAINER_ID,
            plan.container_name,
            IMAGE_ID,
            self.exit_code,
            self.stdout,
        )

    def cleanup_run(self, run_id: str) -> int:
        self.cleanup_calls.append(run_id)
        if self.cleanup_error is not None:
            raise self.cleanup_error
        return 0

    def stop_run(self, run_id: str) -> int:
        self.stop_calls.append(run_id)
        result = self.stop_results.pop(0) if self.stop_results else 1
        if result:
            self.stopped.set()
        return result


class FakeResultStore:
    def __init__(self, root: Path):
        self.root = root
        self.stage_error: Exception | None = None
        self.capture_error: Exception | None = None
        self.stage_calls: list[tuple[str, Path, str]] = []
        self.capture_calls: list[tuple[str, Path]] = []
        self.discard_calls: list[str] = []

    def stage(self, attempt_id: str, workspace: Path, revision: str) -> None:
        self.stage_calls.append((attempt_id, workspace, revision))
        if self.stage_error is not None:
            raise self.stage_error

    def capture(self, attempt_id: str, workspace: Path) -> CapturedResult:
        self.capture_calls.append((attempt_id, workspace))
        if self.capture_error is not None:
            raise self.capture_error
        directory = self.root / attempt_id
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        return CapturedResult(ResultManifest(1, attempt_id, COMMIT, ()), directory)

    def discard_stage(self, attempt_id: str) -> bool:
        self.discard_calls.append(attempt_id)
        return True


class FakeGatewayManager:
    def __init__(self, root: Path):
        self.root = root
        self.open_error: Exception | None = None
        self.cleanup_error: Exception | None = None
        self.open_calls: list[tuple[str, tuple[object, ...], tuple[str, ...]]] = []
        self.cleanup_calls: list[str] = []

    def open(
        self,
        run_id: str,
        *,
        capabilities: tuple[object, ...],
        credential_names: tuple[str, ...],
    ) -> GatewayEndpoint:
        self.open_calls.append((run_id, capabilities, credential_names))
        if self.open_error is not None:
            raise self.open_error
        return GatewayEndpoint(self.root / run_id)

    def cleanup(self, run_id: str) -> bool:
        self.cleanup_calls.append(run_id)
        if self.cleanup_error is not None:
            raise self.cleanup_error
        return True


class RunCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.repository = self.root / "repository"
        self.repository.mkdir(mode=0o700)
        self.token = self.root / "krab.token"
        self.token.write_text(TOKEN + "\n", encoding="ascii")
        os.chmod(self.token, 0o600)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Local Operator")
        provider_secret = self.root / "provider.secret"
        provider_secret.write_text("provider-test-secret\n", encoding="ascii")
        os.chmod(provider_secret, 0o600)
        ControlPlane(
            self.database,
            credential_resolver=PrivateFileCredentialResolver(),
        ).add_provider_grant(
            name="provider-main",
            spec=ProviderGrantSpec.from_mapping(
                {
                    "provider": "test-provider",
                    "adapter": "openai_responses",
                    "destination": "http://127.0.0.1:9999/v1/responses",
                    "method": "POST",
                    "model": "pinned-test-model",
                    "credential_header": "Authorization",
                    "credential_prefix": "Bearer ",
                    "static_headers": {},
                    "request_bytes": 2 * 1024 * 1024,
                    "response_bytes": 4 * 1024 * 1024,
                    "timeout_ms": 1000,
                    "max_concurrency": 1,
                }
            ),
            credential_path=provider_secret,
            expires_at=None,
            created_by_handle="operator",
        )
        instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity(USER_ID, "agent_one", 1),
            "file:" + str(self.token),
            user_id=self.user["id"],
        )
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id=PROJECT_ID,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Build Magic Glass Box",
                    "repository_path": str(self.repository),
                }
            ),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "orchestration-test-item",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Exercise run orchestration",
                    "brief": "Provide a pinned assignment for execution tests.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.create(
                image="agent:local",
                command=["agent"],
                runtime_provider_grant="provider-main",
                workspace_mode=WorkspaceMode.COPY_ON_WRITE,
                network_mode=NetworkMode.GATEWAY_ONLY,
                credential_names=["krab", "provider-main"],
                gateway_capabilities=[
                    GatewayCapability.RUN_DESCRIBE,
                    GatewayCapability.INTERACTION_OPEN,
                    GatewayCapability.INTERACTION_READ,
                    GatewayCapability.HANDOFF_SUBMIT,
                    GatewayCapability.PROVIDER_INVOKE,
                ],
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )
        self.workspaces = FakeWorkspaceManager(self.root / "workspaces")
        self.workspaces.root.mkdir(mode=0o700)
        self.executor = FakeExecutor()
        self.gateways = FakeGatewayManager(self.root / "gateways")
        self.results = FakeResultStore(self.root / "results")
        self.coordinator = RunCoordinator(
            self.database,
            self.executor,
            self.workspaces,
            PrivateFileCredentialResolver(),
            self.gateways,
            self.results,
            orchestrator_id="orchestrator-test",
            heartbeat_interval=30,
        )

    def create_run(
        self,
        revision: str | None = "main",
        *,
        session_id: str | None = None,
    ) -> dict[str, object]:
        return self.database.create_run(
            project_key="box-project",
            profile_name="Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision=revision,
            krab_session_id=session_id,
        )

    def test_success_runs_pinned_inputs_and_records_structured_events(self) -> None:
        run = self.create_run(session_id="ses_" + "4" * 32)

        finished = self.coordinator.execute(run["id"])

        self.assertEqual(finished["state"], "succeeded")
        self.assertEqual(finished["version"], 4)
        self.assertEqual(finished["source_revision"], COMMIT)
        self.assertEqual(
            [event["to_state"] for event in finished["events"]],
            ["queued", "preparing", "running", "succeeded"],
        )
        self.assertEqual(
            finished["events"][2]["metadata"],
            {
                "container_id": CONTAINER_ID,
                "container_name": "magic-glass-box-" + run["id"].removeprefix("run_"),
                "image_id": IMAGE_ID,
                "source_revision": COMMIT,
            },
        )
        attempt = finished["attempts"][0]
        self.assertEqual(
            finished["events"][3]["metadata"],
            {
                "exit_code": 0,
                "image_id": IMAGE_ID,
                "result_id": attempt["result"]["id"],
                "result_sha256": attempt["result"]["content_sha256"],
            },
        )
        self.assertEqual(finished["events"][3]["kind"], "run.transitioned")
        self.assertEqual(finished["events"][3]["from_state"], "running")
        request = self.executor.requests[0]
        self.assertEqual(request.run_id, run["id"])
        self.assertEqual(request.workspace, self.workspaces.root / run["id"])
        self.assertEqual(request.launch, RuntimeLaunch(("agent",)))
        self.assertEqual(request.credential_files, {})
        self.assertEqual(
            request.gateway_directory,
            self.gateways.root / run["id"],
        )
        self.assertEqual(
            self.gateways.open_calls,
            [
                (
                    run["id"],
                    (
                        GatewayCapability.RUN_DESCRIBE,
                        GatewayCapability.INTERACTION_OPEN,
                        GatewayCapability.INTERACTION_READ,
                        GatewayCapability.HANDOFF_SUBMIT,
                        GatewayCapability.PROVIDER_INVOKE,
                    ),
                    ("krab", "provider-main"),
                )
            ],
        )
        self.assertEqual(
            self.workspaces.prepare_calls,
            [(run["id"], self.repository, "main")],
        )
        self.assertEqual(self.executor.cleanup_calls, [run["id"]])
        self.assertEqual(self.workspaces.cleanup_calls, [run["id"]])
        self.assertEqual(self.gateways.cleanup_calls, [run["id"]])
        self.assertEqual(attempt["owner_id"], "orchestrator-test")
        self.assertEqual(attempt["runtime_adapter"], "command")
        self.assertEqual(attempt["result"]["manifest"]["changes"], [])
        self.assertEqual(
            self.results.stage_calls,
            [(attempt["id"], self.workspaces.root / run["id"], COMMIT)],
        )
        self.assertEqual(
            self.results.capture_calls,
            [(attempt["id"], self.workspaces.root / run["id"])],
        )
        self.assertEqual(self.results.discard_calls, [attempt["id"]])

    def test_gateway_interaction_suspends_instead_of_completing_the_run(self) -> None:
        run = self.create_run(session_id="ses_" + "4" * 32)

        def open_interaction(_request: SandboxRequest) -> None:
            current = self.database.get_run(run["id"])
            self.database.open_interaction(
                run["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id=self.identity["id"],
                request_item_id="itm_" + "9" * 32,
                expected_run_version=current["version"],
            )

        self.executor.during_execute = open_interaction
        suspended = self.coordinator.execute(run["id"])

        self.assertEqual(suspended["state"], "waiting_for_input")
        self.assertEqual(suspended["version"], 4)
        self.assertEqual(suspended["events"][-1]["kind"], "run.transitioned")
        self.assertEqual(
            suspended["interactions"][0]["events"][-1]["kind"],
            "interaction.opened",
        )
        self.assertEqual(self.executor.cleanup_calls, [run["id"]])
        self.assertEqual(self.gateways.cleanup_calls, [run["id"]])

    def test_answered_handoff_resumes_same_run_with_a_replacement_worker(self) -> None:
        run = self.create_run(session_id="ses_" + "4" * 32)

        def hand_off(_request: SandboxRequest) -> None:
            current = self.database.get_run(run["id"])
            interaction = self.database.open_interaction(
                run["id"],
                kind=InteractionKind.CHOICE,
                recipient_krab_identity_id=self.identity["id"],
                request_item_id="itm_" + "7" * 32,
                expected_run_version=current["version"],
            )
            self.database.create_attempt_handoff(
                run["id"],
                HandoffEnvelope.from_mapping(
                    {
                        "schema_version": 1,
                        "work_state": "waiting_for_input",
                        "summary": "The implementation is ready for a policy choice.",
                        "decisions": ["Keep the source repository untouched."],
                        "artifacts": [
                            {
                                "path": "src/magic_glass_box/orchestration.py",
                                "description": "Coordinator area inspected by Agent A.",
                            }
                        ],
                        "next_steps": ["Apply the accepted policy and run tests."],
                        "pending_interaction_ids": [interaction["id"]],
                    }
                ),
            )

        self.executor.stdout = (
            '{"type":"thread.started","thread_id":"abc"}\n'
            '{"type":"turn.completed","usage":{}}\n'
        )
        self.executor.during_execute = hand_off
        waiting = self.coordinator.execute(run["id"], runtime_adapter="codex")

        self.assertEqual(waiting["state"], "waiting_for_input")
        self.assertEqual(len(waiting["attempts"]), 1)
        first_attempt = waiting["attempts"][0]
        self.assertEqual(first_attempt["state"], "handed_off")
        self.assertEqual(first_attempt["runtime_adapter"], "codex")
        self.assertEqual(
            first_attempt["handoff"]["pending_interaction_ids"],
            [waiting["interactions"][0]["id"]],
        )

        answered = self.database.answer_interaction(
            waiting["interactions"][0]["id"],
            response_item_id="itm_" + "8" * 32,
            expected_version=1,
        )
        self.assertEqual(answered["run_state"], "queued")

        replacement_executor = FakeExecutor()
        replacement_executor.stdout = (
            '{"type":"result","subtype":"success","is_error":false,"result":"done"}\n'
        )
        replacement_workspaces = FakeWorkspaceManager(self.workspaces.root)
        replacement = RunCoordinator(
            self.database,
            replacement_executor,
            replacement_workspaces,
            PrivateFileCredentialResolver(),
            self.gateways,
            self.results,
            orchestrator_id="orchestrator-replacement",
            heartbeat_interval=30,
        )
        finished = replacement.execute(run["id"], runtime_adapter="claude-code")

        self.assertEqual(finished["id"], run["id"])
        self.assertEqual(finished["state"], "succeeded")
        self.assertEqual(
            replacement_workspaces.prepare_calls,
            [(run["id"], self.repository, COMMIT)],
        )
        self.assertEqual(
            [
                (attempt["sequence"], attempt["runtime_adapter"], attempt["state"])
                for attempt in finished["attempts"]
            ],
            [(1, "codex", "handed_off"), (2, "claude-code", "succeeded")],
        )
        self.assertEqual(
            replacement_executor.requests[0].profile,
            self.executor.requests[0].profile,
        )
        self.assertNotEqual(
            replacement_executor.requests[0].launch,
            self.executor.requests[0].launch,
        )

    def test_nonzero_agent_exit_is_a_durable_failed_run(self) -> None:
        run = self.create_run(None)
        self.executor.exit_code = 17

        finished = self.coordinator.execute(run["id"])

        self.assertEqual(finished["state"], "failed")
        self.assertEqual(finished["source_revision"], COMMIT)
        result = finished["attempts"][0]["result"]
        self.assertEqual(
            finished["events"][-1]["metadata"],
            {
                "exit_code": 17,
                "image_id": IMAGE_ID,
                "reason": "agent exited with code 17",
                "result_id": result["id"],
                "result_sha256": result["content_sha256"],
            },
        )
        self.assertEqual(self.workspaces.prepare_calls[0][2], None)

    def test_unknown_adapter_does_not_claim_the_queued_run(self) -> None:
        run = self.create_run()

        with self.assertRaisesRegex(RuntimeAdapterError, "unknown runtime adapter"):
            self.coordinator.execute(run["id"], runtime_adapter="missing")

        current = self.database.get_run(run["id"])
        self.assertEqual(current["state"], "queued")
        self.assertEqual(current["attempts"], [])
        self.assertEqual(self.executor.requests, [])

    def test_malformed_success_output_fails_without_retaining_the_output(self) -> None:
        run = self.create_run()
        self.executor.stdout = "malformed SECRET transcript"

        with self.assertRaisesRegex(RunOrchestrationError, "run execution failed"):
            self.coordinator.execute(run["id"], runtime_adapter="codex")

        failed = self.database.get_run(run["id"])
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed["attempts"][0]["runtime_adapter"], "codex")
        self.assertNotIn("SECRET", repr(failed))

    def test_failures_before_and_after_container_creation_are_audited(self) -> None:
        for after_callback, expected_from, expected_version in (
            (False, "preparing", 3),
            (True, "running", 4),
        ):
            with self.subTest(after_callback=after_callback):
                run = self.create_run()
                self.executor.execute_error = RuntimeError("docker broke")
                self.executor.error_after_callback = after_callback
                capture_count = len(self.results.capture_calls)
                discard_count = len(self.results.discard_calls)

                with self.assertRaisesRegex(RunOrchestrationError, "execution failed"):
                    self.coordinator.execute(run["id"])

                failed = self.database.get_run(run["id"])
                self.assertEqual(failed["state"], "failed")
                self.assertEqual(failed["version"], expected_version)
                self.assertEqual(failed["events"][-1]["from_state"], expected_from)
                self.assertEqual(
                    failed["events"][-1]["metadata"],
                    {"reason": "execution failed: RuntimeError"},
                )
                attempt = failed["attempts"][0]
                if after_callback:
                    self.assertIsNotNone(attempt["result"])
                    self.assertEqual(len(self.results.capture_calls), capture_count + 1)
                else:
                    self.assertIsNone(attempt["result"])
                    self.assertEqual(len(self.results.capture_calls), capture_count)
                self.assertEqual(len(self.results.discard_calls), discard_count + 1)
                self.executor.execute_error = None
                self.executor.error_after_callback = False

    def test_missing_context_and_unauthorized_credentials_fail_closed(self) -> None:
        missing_context = self.create_run()
        connection = self.database.connect()
        self.addCleanup(connection.close)
        context = ProjectContext.from_mapping({"purpose": "No repository"})
        connection.execute("DROP TRIGGER project_context_immutable_update")
        connection.execute(
            "UPDATE project_context_versions SET context_json = ? WHERE id = ?",
            (context.canonical, missing_context["context_version_id"]),
        )
        with self.assertRaisesRegex(RunOrchestrationError, "execution failed"):
            self.coordinator.execute(missing_context["id"])
        self.assertEqual(
            self.database.get_run(missing_context["id"])["state"], "failed"
        )
        self.assertEqual(self.executor.requests, [])

        restored = ProjectContext.from_mapping(
            {
                "purpose": "Build Magic Glass Box",
                "repository_path": str(self.repository),
            }
        )
        connection.execute(
            "UPDATE project_context_versions "
            "SET context_json = ?, content_sha256 = ? WHERE id = ?",
            (
                restored.canonical,
                restored.digest,
                missing_context["context_version_id"],
            ),
        )

        unsupported = self.create_run()
        execution = self.database.get_run_execution(unsupported["id"])
        execution["profile"]["credential_names"] = ["unknown"]
        with (
            mock.patch.object(
                self.database, "get_run_execution", return_value=execution
            ),
            self.assertRaisesRegex(RunOrchestrationError, "execution failed"),
        ):
            self.coordinator.execute(unsupported["id"])
        self.assertEqual(self.database.get_run(unsupported["id"])["state"], "failed")

    def test_cleanup_failure_leaves_run_active_for_reconciliation(self) -> None:
        run = self.create_run()
        self.executor.execute_error = RuntimeError("docker broke")
        self.executor.cleanup_error = RuntimeError("daemon unavailable")

        with self.assertRaisesRegex(RunOrchestrationError, "cleanup incomplete"):
            self.coordinator.execute(run["id"])

        self.assertEqual(self.database.get_run(run["id"])["state"], "preparing")
        self.assertEqual(self.workspaces.cleanup_calls, [run["id"]])

    def test_result_capture_failure_retains_workspace_for_expired_lease_recovery(
        self,
    ) -> None:
        run = self.create_run()
        self.results.capture_error = RuntimeError("disk full")

        with self.assertRaisesRegex(RunOrchestrationError, "result capture incomplete"):
            self.coordinator.execute(run["id"])

        active = self.database.get_run(run["id"])
        self.assertEqual(active["state"], "running")
        self.assertTrue((self.workspaces.root / run["id"]).exists())
        self.assertEqual(self.executor.cleanup_calls, [run["id"]])
        self.assertEqual(self.gateways.cleanup_calls, [run["id"]])
        self.assertEqual(self.workspaces.cleanup_calls, [])

    def test_executor_failure_and_capture_failure_retain_recoverable_workspace(
        self,
    ) -> None:
        run = self.create_run()
        self.executor.execute_error = RuntimeError("docker wait failed")
        self.executor.error_after_callback = True
        self.results.capture_error = RuntimeError("disk full")

        with self.assertRaisesRegex(RunOrchestrationError, "capture incomplete"):
            self.coordinator.execute(run["id"])

        active = self.database.get_run(run["id"])
        self.assertEqual(active["state"], "running")
        self.assertIsNone(active["attempts"][0]["result"])
        self.assertTrue((self.workspaces.root / run["id"]).exists())
        self.assertEqual(self.executor.cleanup_calls, [run["id"]])
        self.assertEqual(self.gateways.cleanup_calls, [run["id"]])
        self.assertEqual(self.workspaces.cleanup_calls, [])

    def test_cancel_request_stops_the_owned_container_before_terminal_state(
        self,
    ) -> None:
        run = self.create_run()
        coordinator = RunCoordinator(
            self.database,
            self.executor,
            self.workspaces,
            PrivateFileCredentialResolver(),
            self.gateways,
            self.results,
            orchestrator_id="orchestrator-cancel-test",
            heartbeat_interval=0.01,
        )

        def request_cancel(_request: SandboxRequest) -> None:
            current = self.database.get_run(run["id"])
            requested = self.database.request_run_cancellation(
                run["id"],
                requested_by=self.user["id"],
                expected_run_version=current["version"],
                reason="stop now",
            )
            self.assertEqual(requested["state"], "running")
            self.assertTrue(self.executor.stopped.wait(timeout=1))

        self.executor.during_execute = request_cancel
        cancelled = coordinator.execute(run["id"])

        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(self.executor.stop_calls, [run["id"]])
        self.assertEqual(cancelled["attempts"][0]["state"], "cancelled")

    def test_cancel_monitor_retries_until_the_container_can_be_stopped(self) -> None:
        run = self.create_run()
        self.executor.stop_results = [0, 1]
        coordinator = RunCoordinator(
            self.database,
            self.executor,
            self.workspaces,
            PrivateFileCredentialResolver(),
            self.gateways,
            self.results,
            orchestrator_id="orchestrator-cancel-race-test",
            heartbeat_interval=0.01,
        )

        def request_cancel(_request: SandboxRequest) -> None:
            current = self.database.get_run(run["id"])
            self.database.request_run_cancellation(
                run["id"],
                requested_by=self.user["id"],
                expected_run_version=current["version"],
                reason="stop during container discovery",
            )
            self.assertTrue(self.executor.stopped.wait(timeout=1))

        self.executor.during_execute = request_cancel
        cancelled = coordinator.execute(run["id"])

        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(self.executor.stop_calls, [run["id"], run["id"]])

    def test_all_cleanup_boundaries_are_attempted_and_reported(self) -> None:
        run = self.create_run()
        self.executor.execute_error = RuntimeError("docker broke")
        self.executor.cleanup_error = RuntimeError("daemon unavailable")
        self.gateways.cleanup_error = RuntimeError("gateway busy")
        self.workspaces.cleanup_error = RuntimeError("workspace busy")

        with self.assertRaisesRegex(
            RunOrchestrationError, "cleanup incomplete"
        ) as raised:
            self.coordinator.execute(run["id"])

        self.assertEqual(len(raised.exception.__notes__), 3)
        self.assertIn("daemon unavailable", raised.exception.__notes__[0])
        self.assertIn("gateway busy", raised.exception.__notes__[1])
        self.assertIn("workspace busy", raised.exception.__notes__[2])
        self.assertEqual(self.database.get_run(run["id"])["state"], "preparing")

    def test_executor_contract_violations_fail_the_run(self) -> None:
        for attribute in ("skip_callback", "inconsistent_result"):
            with self.subTest(attribute=attribute):
                run = self.create_run()
                setattr(self.executor, attribute, True)
                with self.assertRaisesRegex(RunOrchestrationError, "execution failed"):
                    self.coordinator.execute(run["id"])
                self.assertEqual(self.database.get_run(run["id"])["state"], "failed")
                setattr(self.executor, attribute, False)

    def test_claim_and_terminal_recording_failures_are_explicit(self) -> None:
        claim = self.create_run()
        with (
            mock.patch.object(
                self.database,
                "begin_run_attempt",
                side_effect=RuntimeError("database busy"),
            ),
            self.assertRaisesRegex(RunOrchestrationError, "could not be claimed"),
        ):
            self.coordinator.execute(claim["id"])
        self.assertEqual(self.database.get_run(claim["id"])["state"], "queued")

        terminal = self.create_run()
        with (
            mock.patch.object(
                self.database,
                "complete_run_attempt",
                side_effect=RuntimeError("database busy"),
            ),
            self.assertRaisesRegex(
                RunOrchestrationError, "outcome could not be recorded"
            ),
        ):
            self.coordinator.execute(terminal["id"])
        self.assertEqual(self.database.get_run(terminal["id"])["state"], "running")

    def test_malformed_credential_projection_fails_closed(self) -> None:
        run = self.create_run()
        execution = self.database.get_run_execution(run["id"])
        execution["credential_references"] = []
        with (
            mock.patch.object(
                self.database, "get_run_execution", return_value=execution
            ),
            self.assertRaisesRegex(RunOrchestrationError, "execution failed"),
        ):
            self.coordinator.execute(run["id"])
        self.assertEqual(self.database.get_run(run["id"])["state"], "failed")

    def test_duplicate_execution_is_rejected_without_new_side_effects(self) -> None:
        run = self.create_run()
        self.coordinator.execute(run["id"])
        self.executor.requests.clear()

        with self.assertRaisesRegex(RunOrchestrationError, "not queued"):
            self.coordinator.execute(run["id"])

        self.assertEqual(self.executor.requests, [])

    def test_reconcile_cleans_interrupted_runs_and_leaves_queued_runs_alone(
        self,
    ) -> None:
        queued = self.create_run()
        preparing = self.create_run()
        running = self.create_run()
        self.database.transition_run(
            preparing["id"], target=RunState.PREPARING, expected_version=1
        )
        self.database.transition_run(
            running["id"], target=RunState.PREPARING, expected_version=1
        )
        self.database.transition_run(
            running["id"], target=RunState.RUNNING, expected_version=2
        )
        for run in (preparing, running):
            (self.workspaces.root / run["id"]).mkdir(mode=0o700)

        reconciled = self.coordinator.reconcile()

        self.assertEqual(
            {run["id"] for run in reconciled}, {preparing["id"], running["id"]}
        )
        self.assertEqual(self.database.get_run(queued["id"])["state"], "queued")
        for run in (preparing, running):
            failed = self.database.get_run(run["id"])
            self.assertEqual(failed["state"], "failed")
            self.assertEqual(
                failed["events"][-1]["metadata"],
                {
                    "reason": "orphaned run has no active attempt",
                    "reconciled": True,
                },
            )

    def test_reconcile_does_not_infer_ownership_of_a_waiting_run_without_attempt(
        self,
    ) -> None:
        run = self.create_run(session_id="ses_" + "4" * 32)
        self.database.transition_run(
            run["id"], target=RunState.PREPARING, expected_version=1
        )
        running = self.database.transition_run(
            run["id"], target=RunState.RUNNING, expected_version=2
        )
        interaction = self.database.open_interaction(
            run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "8" * 32,
            expected_run_version=running["version"],
        )
        (self.workspaces.root / run["id"]).mkdir(mode=0o700)

        self.assertEqual(self.coordinator.reconcile(), [])

        preserved = self.database.get_run(run["id"])
        self.assertEqual(preserved["state"], "waiting_for_input")
        self.assertEqual(preserved["interactions"][0]["id"], interaction["id"])
        self.assertEqual(preserved["interactions"][0]["state"], "open")
        self.assertEqual(self.executor.cleanup_calls, [])
        self.assertEqual(self.gateways.cleanup_calls, [])
        self.assertTrue((self.workspaces.root / run["id"]).exists())

    def test_reconcile_skips_a_live_owned_attempt(self) -> None:
        run = self.create_run()
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="agent-live",
            owner_id="orchestrator-live",
            expected_run_version=1,
            lease_seconds=60,
        )
        self.database.start_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision=COMMIT,
            container_id=CONTAINER_ID,
            container_name="magic-glass-box-live",
            image_id=IMAGE_ID,
        )
        (self.workspaces.root / run["id"]).mkdir(mode=0o700)

        self.assertEqual(self.coordinator.reconcile(), [])

        current = self.database.get_run(run["id"])
        self.assertEqual(current["state"], "running")
        self.assertEqual(current["attempts"][0]["state"], "running")
        self.assertEqual(self.executor.cleanup_calls, [])

    def test_reconcile_closes_crashed_attempt_without_losing_its_handoff(self) -> None:
        run = self.create_run(session_id="ses_" + "4" * 32)
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="agent-a",
            owner_id="orchestrator-crashed",
            expected_run_version=1,
        )
        running = self.database.start_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision=COMMIT,
            container_id=CONTAINER_ID,
            container_name="magic-glass-box-crashed",
            image_id=IMAGE_ID,
        )
        interaction = self.database.open_interaction(
            run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "6" * 32,
            expected_run_version=running["run_version"],
        )
        self.database.create_attempt_handoff(
            run["id"],
            HandoffEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "work_state": "waiting_for_input",
                    "summary": "The worker stopped after asking for input.",
                    "decisions": [],
                    "artifacts": [],
                    "next_steps": ["Read the response."],
                    "pending_interaction_ids": [interaction["id"]],
                }
            ),
        )
        (self.workspaces.root / run["id"]).mkdir(mode=0o700)
        self.results.stage(running["id"], self.workspaces.root / run["id"], COMMIT)
        connection = self.database.connect()
        connection.execute(
            "UPDATE run_attempts SET lease_expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000000+00:00", running["id"]),
        )
        connection.commit()
        connection.close()

        self.assertEqual(self.coordinator.reconcile(), [])

        preserved = self.database.get_run(run["id"])
        self.assertEqual(preserved["state"], "waiting_for_input")
        self.assertEqual(preserved["attempts"][0]["state"], "interrupted")
        self.assertEqual(
            preserved["attempts"][0]["handoff"]["pending_interaction_ids"],
            [interaction["id"]],
        )
        self.assertEqual(
            preserved["attempts"][0]["events"][-1]["kind"],
            "attempt.interrupted",
        )

    def test_reconcile_leaves_cleanup_failures_active_and_reports_them(self) -> None:
        run = self.create_run()
        self.database.transition_run(
            run["id"], target=RunState.PREPARING, expected_version=1
        )
        (self.workspaces.root / run["id"]).mkdir(mode=0o700)
        self.executor.cleanup_error = RuntimeError("daemon unavailable")

        with self.assertRaisesRegex(
            RunOrchestrationError, "reconciliation incomplete for 1 run"
        ) as raised:
            self.coordinator.reconcile()

        self.assertIn(run["id"], raised.exception.__notes__[0])
        self.assertEqual(self.database.get_run(run["id"])["state"], "preparing")
        self.assertFalse((self.workspaces.root / run["id"]).exists())


if __name__ == "__main__":
    unittest.main()
