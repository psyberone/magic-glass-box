from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from magic_glass_box.credentials import PrivateFileCredentialResolver
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    InteractionKind,
    NetworkMode,
    ProjectContext,
    RunState,
    WorkItemEnvelope,
    WorkspaceMode,
)
from magic_glass_box.executor import SandboxPlan, SandboxRequest, SandboxResult
from magic_glass_box.gateway import GatewayEndpoint
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.orchestration import RunCoordinator, RunOrchestrationError
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


class FakeExecutor:
    def __init__(self):
        self.exit_code = 0
        self.execute_error: Exception | None = None
        self.error_after_callback = False
        self.skip_callback = False
        self.inconsistent_result = False
        self.cleanup_error: Exception | None = None
        self.during_execute = None
        self.requests: list[SandboxRequest] = []
        self.cleanup_calls: list[str] = []

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
        )

    def cleanup_run(self, run_id: str) -> int:
        self.cleanup_calls.append(run_id)
        if self.cleanup_error is not None:
            raise self.cleanup_error
        return 0


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
                command=["agent", "run"],
                workspace_mode=WorkspaceMode.COPY_ON_WRITE,
                network_mode=NetworkMode.GATEWAY_ONLY,
                credential_names=["krab"],
                gateway_capabilities=[
                    GatewayCapability.RUN_DESCRIBE,
                    GatewayCapability.INTERACTION_OPEN,
                    GatewayCapability.INTERACTION_READ,
                ],
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )
        self.workspaces = FakeWorkspaceManager(self.root / "workspaces")
        self.workspaces.root.mkdir(mode=0o700)
        self.executor = FakeExecutor()
        self.gateways = FakeGatewayManager(self.root / "gateways")
        self.coordinator = RunCoordinator(
            self.database,
            self.executor,
            self.workspaces,
            PrivateFileCredentialResolver(),
            self.gateways,
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
        self.assertEqual(
            finished["events"][3]["metadata"],
            {"exit_code": 0, "image_id": IMAGE_ID},
        )
        request = self.executor.requests[0]
        self.assertEqual(request.run_id, run["id"])
        self.assertEqual(request.workspace, self.workspaces.root / run["id"])
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
                    ),
                    ("krab",),
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

    def test_nonzero_agent_exit_is_a_durable_failed_run(self) -> None:
        run = self.create_run(None)
        self.executor.exit_code = 17

        finished = self.coordinator.execute(run["id"])

        self.assertEqual(finished["state"], "failed")
        self.assertEqual(finished["source_revision"], COMMIT)
        self.assertEqual(
            finished["events"][-1]["metadata"],
            {
                "exit_code": 17,
                "image_id": IMAGE_ID,
                "reason": "agent exited with code 17",
            },
        )
        self.assertEqual(self.workspaces.prepare_calls[0][2], None)

    def test_failures_before_and_after_container_creation_are_audited(self) -> None:
        for after_callback, expected_from, expected_version in (
            (False, "preparing", 3),
            (True, "running", 4),
        ):
            with self.subTest(after_callback=after_callback):
                run = self.create_run()
                self.executor.execute_error = RuntimeError("docker broke")
                self.executor.error_after_callback = after_callback

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
                "transition_run",
                side_effect=RuntimeError("database busy"),
            ),
            self.assertRaisesRegex(RunOrchestrationError, "could not be claimed"),
        ):
            self.coordinator.execute(claim["id"])
        self.assertEqual(self.database.get_run(claim["id"])["state"], "queued")

        terminal = self.create_run()
        transition = self.database.transition_run

        def fail_terminal(run_id: str, **kwargs: object):
            if kwargs["target"] is RunState.SUCCEEDED:
                raise RuntimeError("database busy")
            return transition(run_id, **kwargs)

        with (
            mock.patch.object(
                self.database, "transition_run", side_effect=fail_terminal
            ),
            self.assertRaisesRegex(
                RunOrchestrationError, "terminal state could not be recorded"
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
                    "reason": "orchestrator restart reconciliation",
                    "reconciled": True,
                },
            )

    def test_reconcile_preserves_a_pending_interaction_without_live_resources(
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
        self.assertEqual(self.executor.cleanup_calls, [run["id"]])
        self.assertEqual(self.gateways.cleanup_calls, [run["id"]])
        self.assertFalse((self.workspaces.root / run["id"]).exists())

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
