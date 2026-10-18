from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    NetworkMode,
    ProjectContext,
    WorkItemEnvelope,
)
from magic_glass_box.executor import SandboxPlan, SandboxRequest, SandboxResult
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.orchestration import RunCoordinator
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

IMAGE_ID = "sha256:" + "a" * 64
CONTAINER_ID = "b" * 64


class _RecoveryExecutor:
    def __init__(self) -> None:
        self.cleanup_calls: list[str] = []
        self.stop_calls: list[str] = []

    def execute(self, request: SandboxRequest, *, before_start=None) -> SandboxResult:
        raise AssertionError("reconciliation must not launch a worker")

    def cleanup_run(self, run_id: str) -> int:
        self.cleanup_calls.append(run_id)
        return 1

    def stop_run(self, run_id: str) -> int:
        self.stop_calls.append(run_id)
        return 1


class _ReplacementExecutor(_RecoveryExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.observed_checkpoint = False

    def execute(self, request: SandboxRequest, *, before_start=None) -> SandboxResult:
        plan = SandboxPlan(
            request.run_id,
            "magic-glass-box-" + request.run_id.removeprefix("run_"),
            IMAGE_ID,
            (),
        )
        if before_start is not None:
            before_start(plan, CONTAINER_ID)
        self.observed_checkpoint = (
            (request.workspace / "accepted.txt").read_text(encoding="utf-8")
            == "accepted by the killed worker\n"
            and not (request.workspace / "deleted.txt").exists()
            and (request.workspace / "partial.txt").read_text(encoding="utf-8")
            == "half complete\n"
        )
        if not self.observed_checkpoint:
            raise AssertionError("replacement did not receive the exact checkpoint")
        (request.workspace / "partial.txt").write_text(
            "replacement completed this\n", encoding="utf-8"
        )
        (request.workspace / "finished.txt").write_text(
            "replacement finished\n", encoding="utf-8"
        )
        return SandboxResult(
            CONTAINER_ID,
            plan.container_name,
            IMAGE_ID,
            0,
            "",
        )


class DestructiveReplacementRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        git = shutil.which("git")
        if git is None:
            self.skipTest("Git is required for replacement recovery proof")
        self.git = Path(git)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.repository = self.root / "repository"
        self.repository.mkdir(mode=0o700)
        self._git("init", "--quiet")
        self._git("config", "user.name", "Recovery Test")
        self._git("config", "user.email", "recovery@example.invalid")
        (self.repository / "accepted.txt").write_text(
            "base accepted\n", encoding="utf-8"
        )
        (self.repository / "deleted.txt").write_text("remove me\n", encoding="utf-8")
        self._git("add", "accepted.txt", "deleted.txt")
        self._git("commit", "--quiet", "-m", "base")
        self.base_revision = self._git("rev-parse", "HEAD").stdout.strip()

        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "1" * 32, "worker", 1),
            "file:/private/unused.token",
            user_id=self.user["id"],
        )
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="recovery-project",
            display_name="Recovery Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Prove destructive replacement recovery",
                    "repository_path": str(self.repository),
                }
            ),
            created_by=self.user["id"],
        )
        item = self.database.import_work_item(
            project_key="recovery-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "destructive-recovery",
                    "source_updated_at": "2026-10-18T12:00:00Z",
                    "title": "Survive a worker killed mid-edit",
                    "brief": "Recover every bounded edit in a replacement worker.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:test",
                command=["agent", "run"],
                network_mode=NetworkMode.NONE.value,
            ),
            krab_identity_id=identity["id"],
            created_by=self.user["id"],
        )
        self.run = self.database.create_run(
            project_key="recovery-project",
            profile_name="Worker",
            work_item_id=item["id"],
            requested_by=self.user["id"],
            source_revision=self.base_revision,
        )
        self.workspaces = WorkspaceManager(
            self.root / "workspaces", GitSnapshotter(binary=self.git)
        )
        self.results = WorkspaceResultStore(self.root / "results", git_binary=self.git)

    def _git(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - absolute discovered Git binary
            [str(self.git), "-C", str(self.repository), *arguments],
            check=True,
            capture_output=True,
            text=True,
        )

    def test_killed_mid_edit_worker_recovers_without_file_loss_or_redo(self) -> None:
        preparing = self.database.begin_run_attempt(
            self.run["id"],
            runtime_adapter="command",
            owner_id="orchestrator-killed",
            expected_run_version=self.run["version"],
        )
        workspace = self.workspaces.prepare(
            self.run["id"], self.repository, self.base_revision
        )
        self.results.stage(preparing["id"], workspace.path, workspace.revision)
        running = self.database.start_run_attempt(
            preparing["id"],
            owner_id="orchestrator-killed",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=workspace.revision,
            container_id=CONTAINER_ID,
            container_name="killed-worker",
            image_id=IMAGE_ID,
        )

        script = "\n".join(
            (
                "import pathlib, time",
                f"root = pathlib.Path({str(workspace.path)!r})",
                "(root / 'accepted.txt').write_text("
                "'accepted by the killed worker\\n', encoding='utf-8')",
                "(root / 'deleted.txt').unlink()",
                "(root / 'partial.txt').write_text("
                "'half complete\\n', encoding='utf-8')",
                "print('MID_EDIT', flush=True)",
                "time.sleep(60)",
            )
        )
        worker = subprocess.Popen(  # noqa: S603
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def stop_worker() -> None:
            if worker.poll() is None:
                worker.kill()
                worker.communicate(timeout=5)

        self.addCleanup(stop_worker)
        assert worker.stdout is not None
        self.assertEqual(worker.stdout.readline().strip(), "MID_EDIT")
        worker.kill()
        worker.communicate(timeout=5)
        self.assertNotEqual(worker.returncode, 0)

        with self.database.connect() as connection:
            connection.execute(
                "UPDATE run_attempts SET lease_expires_at = ? WHERE id = ?",
                ("2000-01-01T00:00:00.000000+00:00", running["id"]),
            )
            connection.commit()
        recovery_executor = _RecoveryExecutor()
        recovery = RunCoordinator(
            self.database,
            recovery_executor,
            self.workspaces,
            None,
            self.results,
            orchestrator_id="orchestrator-recovery",
            heartbeat_interval=30,
        )

        reconciled = recovery.reconcile(self.run["id"])

        self.assertEqual([item["id"] for item in reconciled], [self.run["id"]])
        failed = self.database.get_run(self.run["id"])
        original = failed["attempts"][0]
        self.assertEqual(original["state"], "interrupted")
        self.assertEqual(
            [change["path"] for change in original["result"]["manifest"]["changes"]],
            ["accepted.txt", "deleted.txt", "partial.txt"],
        )
        self.assertFalse(workspace.path.exists())

        successor = self.database.retry_run(
            failed["id"],
            requested_by=self.user["id"],
            expected_run_version=failed["version"],
        )
        replacement_executor = _ReplacementExecutor()
        replacement = RunCoordinator(
            self.database,
            replacement_executor,
            self.workspaces,
            None,
            self.results,
            orchestrator_id="orchestrator-replacement",
            heartbeat_interval=30,
        )

        finished = replacement.execute(successor["id"])

        self.assertTrue(replacement_executor.observed_checkpoint)
        self.assertEqual(finished["state"], "succeeded")
        final_attempt = finished["attempts"][0]
        checkpoint = final_attempt["recovery_checkpoint"]
        self.assertEqual(checkpoint["source_attempt_id"], original["id"])
        self.assertEqual(checkpoint["result_id"], original["result"]["id"])
        self.assertIsNotNone(checkpoint["applied_at"])
        self.assertEqual(
            final_attempt["events"][1]["metadata"]["recovery_checkpoint_id"],
            checkpoint["id"],
        )
        self.assertEqual(
            [
                change["path"]
                for change in final_attempt["result"]["manifest"]["changes"]
            ],
            ["accepted.txt", "deleted.txt", "finished.txt", "partial.txt"],
        )

        materialized = self.root / "materialized"
        materialized.mkdir(mode=0o700)
        GitSnapshotter(binary=self.git).snapshot(
            self.repository, self.base_revision, materialized
        )
        self.results.apply(
            final_attempt["id"],
            materialized,
            expected_digest=final_attempt["result"]["content_sha256"],
        )
        self.assertEqual(
            (materialized / "accepted.txt").read_text(encoding="utf-8"),
            "accepted by the killed worker\n",
        )
        self.assertFalse((materialized / "deleted.txt").exists())
        self.assertEqual(
            (materialized / "partial.txt").read_text(encoding="utf-8"),
            "replacement completed this\n",
        )
        self.assertEqual(
            (materialized / "finished.txt").read_text(encoding="utf-8"),
            "replacement finished\n",
        )


if __name__ == "__main__":
    unittest.main()
