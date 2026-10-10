from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from magic_glass_box.db import SCHEMA, ConflictError, Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    HandoffEnvelope,
    InteractionKind,
    ProjectContext,
    ValidationError,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import FileState, ResultChange, ResultManifest

OWNER_ID = "orchestrator-test-owner"


class RunAttemptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Database(Path(self.temporary.name) / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "1" * 32, "agent_one", 1),
            "file:/private/agent.token",
            user_id=self.user["id"],
        )
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping({"purpose": "Test worker replacement"}),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "attempt-test",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Replace a waiting worker",
                    "brief": "Exercise attempt and handoff persistence.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.from_mapping(
                {"image": "agent:local", "command": ["agent", "run"]}
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )

    def create_run(self) -> dict[str, object]:
        return self.database.create_run(
            project_key="box-project",
            profile_name="Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision="main",
            krab_session_id="ses_" + "3" * 32,
        )

    @staticmethod
    def envelope(interaction_id: str, *, summary: str = "Waiting for input"):
        return HandoffEnvelope.from_mapping(
            {
                "schema_version": 1,
                "work_state": "waiting_for_input",
                "summary": summary,
                "decisions": ["Preserve the pinned authority."],
                "artifacts": [
                    {
                        "path": "src/worker.py",
                        "description": "Relevant source path; content is not copied.",
                    }
                ],
                "next_steps": ["Read the accepted response."],
                "pending_interaction_ids": [interaction_id],
            }
        )

    def running_attempt(self):
        run = self.create_run()
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id=OWNER_ID,
            expected_run_version=1,
        )
        running = self.database.start_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision="a" * 40,
            container_id="b" * 64,
            container_name="magic-glass-box-test",
            image_id="sha256:" + "c" * 64,
        )
        return run, running

    @staticmethod
    def manifest(attempt_id: str, *, changed: bool = False) -> ResultManifest:
        changes = ()
        if changed:
            changes = (
                ResultChange(
                    "new.txt",
                    "added",
                    None,
                    FileState(
                        kind="file",
                        mode=0o644,
                        size=4,
                        sha256="d" * 64,
                    ),
                ),
            )
        return ResultManifest(1, attempt_id, "a" * 40, changes)

    def record_result(self, attempt_id: str) -> dict[str, object]:
        return self.database.record_attempt_result(
            attempt_id, self.manifest(attempt_id)
        )

    def test_attempts_and_handoffs_are_auditable_and_idempotent(self) -> None:
        run, running = self.running_attempt()
        interaction = self.database.open_interaction(
            run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "4" * 32,
            expected_run_version=running["run_version"],
        )
        envelope = self.envelope(interaction["id"])

        first = self.database.create_attempt_handoff(run["id"], envelope)
        repeated = self.database.create_attempt_handoff(run["id"], envelope)
        with self.assertRaisesRegex(ConflictError, "different handoff"):
            self.database.create_attempt_handoff(
                run["id"], self.envelope(interaction["id"], summary="Changed")
            )
        self.record_result(running["id"])
        finished_attempt = self.database.complete_run_attempt(
            running["id"],
            expected_attempt_version=running["version"],
            exit_code=0,
            image_id="sha256:" + "c" * 64,
        )

        self.assertEqual(repeated, first)
        self.assertEqual(finished_attempt["state"], "handed_off")
        self.assertEqual(
            [event["kind"] for event in finished_attempt["events"]],
            [
                "attempt.created",
                "attempt.started",
                "attempt.handoff_submitted",
                "attempt.completed",
            ],
        )
        self.assertEqual(
            finished_attempt["events"][1]["metadata"],
            {
                "container_id": "b" * 64,
                "container_name": "magic-glass-box-test",
                "image_id": "sha256:" + "c" * 64,
                "source_revision": "a" * 40,
            },
        )
        self.assertNotIn("transcript", str(first))
        visible = self.database.get_run(run["id"])
        self.assertEqual(visible["state"], "waiting_for_input")
        self.assertEqual(visible["attempts"], [finished_attempt])
        self.assertEqual(self.database.status()["counts"]["run_attempts"], 1)
        self.assertEqual(self.database.status()["counts"]["attempt_handoffs"], 1)
        self.assertEqual(self.database.status()["counts"]["attempt_results"], 1)
        self.assertEqual(
            self.database.get_run_execution(run["id"])["handoff"]["id"],
            first["id"],
        )

    def test_handoff_must_match_the_active_wait_and_missing_handoff_is_audited(
        self,
    ) -> None:
        run, running = self.running_attempt()
        with self.assertRaisesRegex(ConflictError, "not waiting"):
            self.database.create_attempt_handoff(
                run["id"], self.envelope("int_" + "5" * 32)
            )
        interaction = self.database.open_interaction(
            run["id"],
            kind=InteractionKind.APPROVAL,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "6" * 32,
            expected_run_version=running["run_version"],
        )
        with self.assertRaisesRegex(ConflictError, "do not match"):
            self.database.create_attempt_handoff(
                run["id"], self.envelope("int_" + "5" * 32)
            )
        invalid_state = self.envelope(interaction["id"]).to_dict()
        invalid_state["work_state"] = "in_progress"
        with self.assertRaisesRegex(ValidationError, "waiting_for_input"):
            self.database.create_attempt_handoff(
                run["id"], HandoffEnvelope.from_mapping(invalid_state)
            )

        self.record_result(running["id"])
        finished = self.database.complete_run_attempt(
            running["id"],
            expected_attempt_version=running["version"],
            exit_code=0,
            image_id="sha256:" + "c" * 64,
        )
        self.assertEqual(finished["state"], "interrupted")
        self.assertIn("without a handoff", finished["reason"])
        self.assertEqual(self.database.get_run(run["id"])["state"], "waiting_for_input")

    def test_fast_response_does_not_race_handoff_persistence(self) -> None:
        run, running = self.running_attempt()
        interaction = self.database.open_interaction(
            run["id"],
            kind=InteractionKind.CHOICE,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "8" * 32,
            expected_run_version=running["run_version"],
        )
        answered = self.database.answer_interaction(
            interaction["id"],
            response_item_id="itm_" + "9" * 32,
            expected_version=1,
        )
        self.assertEqual(answered["run_state"], "waiting_for_input")

        handoff = self.database.create_attempt_handoff(
            run["id"], self.envelope(interaction["id"])
        )
        self.record_result(running["id"])
        completed = self.database.complete_run_attempt(
            running["id"],
            expected_attempt_version=running["version"],
            exit_code=0,
            image_id="sha256:" + "c" * 64,
        )

        self.assertEqual(handoff["pending_interaction_ids"], [interaction["id"]])
        self.assertEqual(completed["state"], "handed_off")
        self.assertEqual(completed["run_state"], "queued")

    def test_direct_attempt_failure_is_atomic_and_audited(self) -> None:
        run = self.create_run()
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id=OWNER_ID,
            expected_run_version=1,
        )

        failed = self.database.fail_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            reason="workspace preparation failed",
        )

        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed["run_state"], "failed")
        self.assertEqual(failed["reason"], "workspace preparation failed")
        self.assertEqual(failed["events"][-1]["kind"], "attempt.failed")
        self.assertEqual(failed["events"][-1]["from_state"], "preparing")
        run_event = self.database.get_run(run["id"])["events"][-1]
        self.assertEqual(run_event["kind"], "run.transitioned")
        self.assertEqual(run_event["from_state"], "preparing")
        self.assertEqual(run_event["to_state"], "failed")
        self.assertEqual(
            run_event["metadata"], {"reason": "workspace preparation failed"}
        )

    def test_successful_attempt_terminally_completes_its_run(self) -> None:
        run, running = self.running_attempt()
        result = self.record_result(running["id"])

        completed = self.database.complete_run_attempt(
            running["id"],
            expected_attempt_version=running["version"],
            exit_code=0,
            image_id="sha256:" + "c" * 64,
        )

        self.assertEqual(completed["state"], "succeeded")
        self.assertEqual(completed["run_state"], "succeeded")
        self.assertEqual(completed["result"], result)
        event = self.database.get_run(run["id"])["events"][-1]
        self.assertEqual(event["kind"], "run.transitioned")
        self.assertEqual(event["from_state"], "running")
        self.assertEqual(event["to_state"], "succeeded")

    def test_running_attempt_requires_a_captured_result_before_completion(self) -> None:
        _, running = self.running_attempt()

        with self.assertRaisesRegex(ConflictError, "result has not been captured"):
            self.database.complete_run_attempt(
                running["id"],
                expected_attempt_version=running["version"],
                exit_code=0,
                image_id="sha256:" + "c" * 64,
            )

    def test_result_metadata_is_immutable_content_addressed_and_idempotent(
        self,
    ) -> None:
        _, running = self.running_attempt()
        manifest = self.manifest(running["id"], changed=True)

        first = self.database.record_attempt_result(running["id"], manifest)
        repeated = self.database.record_attempt_result(running["id"], manifest)

        self.assertEqual(repeated, first)
        self.assertEqual(first["attempt_id"], running["id"])
        self.assertEqual(first["content_sha256"], manifest.digest)
        self.assertEqual(first["changed_paths"], 1)
        self.assertEqual(first["changed_bytes"], 4)
        self.assertEqual(first["manifest"], manifest.to_dict())
        with self.assertRaisesRegex(ConflictError, "different result"):
            self.database.record_attempt_result(
                running["id"], self.manifest(running["id"])
            )

        connection = self.database.connect()
        self.addCleanup(connection.close)
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE attempt_results SET changed_paths = 0 WHERE id = ?",
                (first["id"],),
            )

    def test_attempt_lease_renewal_and_expired_takeover_are_owner_checked(
        self,
    ) -> None:
        run = self.create_run()
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id=OWNER_ID,
            expected_run_version=1,
            lease_seconds=30,
        )
        self.assertEqual(attempt["owner_id"], OWNER_ID)
        self.assertIsNotNone(attempt["heartbeat_at"])
        self.assertIsNotNone(attempt["lease_expires_at"])

        renewed = self.database.renew_run_attempt_lease(
            attempt["id"], owner_id=OWNER_ID, lease_seconds=60
        )
        self.assertEqual(renewed["owner_id"], OWNER_ID)
        with self.assertRaisesRegex(ConflictError, "owned by another orchestrator"):
            self.database.renew_run_attempt_lease(
                attempt["id"], owner_id="orchestrator-other", lease_seconds=60
            )
        with self.assertRaisesRegex(ConflictError, "lease has not expired"):
            self.database.claim_expired_run_attempt(
                attempt["id"], owner_id="orchestrator-other", lease_seconds=60
            )

        connection = self.database.connect()
        connection.execute(
            "UPDATE run_attempts SET lease_expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000000+00:00", attempt["id"]),
        )
        connection.commit()
        connection.close()
        claimed = self.database.claim_expired_run_attempt(
            attempt["id"], owner_id="orchestrator-other", lease_seconds=60
        )
        self.assertEqual(claimed["owner_id"], "orchestrator-other")

    def test_cancel_request_is_audited_then_terminal_only_after_worker_exit(
        self,
    ) -> None:
        run, running = self.running_attempt()

        requested = self.database.request_run_cancellation(
            run["id"],
            requested_by=self.user["id"],
            expected_run_version=running["run_version"],
            reason="operator requested stop",
        )

        self.assertEqual(requested["state"], "running")
        self.assertEqual(requested["events"][-1]["kind"], "run.cancel_requested")
        active = requested["attempts"][-1]
        self.assertEqual(active["cancel_requested_by"], self.user["id"])
        self.assertEqual(active["cancel_reason"], "operator requested stop")
        control = self.database.renew_run_attempt_lease(
            running["id"], owner_id=OWNER_ID, lease_seconds=60
        )
        self.assertTrue(control["cancel_requested"])

        self.record_result(running["id"])
        completed = self.database.complete_run_attempt(
            running["id"],
            expected_attempt_version=running["version"],
            exit_code=143,
            image_id="sha256:" + "c" * 64,
        )
        self.assertEqual(completed["state"], "cancelled")
        self.assertEqual(completed["run_state"], "cancelled")
        self.assertEqual(
            self.database.get_run(run["id"])["events"][-1]["to_state"],
            "cancelled",
        )

    def test_attempt_event_and_handoff_records_are_immutable(self) -> None:
        run, running = self.running_attempt()
        interaction = self.database.open_interaction(
            run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "7" * 32,
            expected_run_version=running["run_version"],
        )
        handoff = self.database.create_attempt_handoff(
            run["id"], self.envelope(interaction["id"])
        )
        connection = self.database.connect()
        self.addCleanup(connection.close)
        for statement, values in (
            (
                "UPDATE run_attempt_events SET metadata_json = '{}' "
                "WHERE attempt_id = ?",
                (running["id"],),
            ),
            (
                "DELETE FROM attempt_handoffs WHERE id = ?",
                (handoff["id"],),
            ),
            (
                "UPDATE run_attempts SET runtime_adapter = 'other' WHERE id = ?",
                (running["id"],),
            ),
        ):
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(statement, values)


class AttemptMigrationTests(unittest.TestCase):
    def test_schema_four_migrates_once_without_losing_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-four.db"
            connection = sqlite3.connect(path)
            attempt_start = SCHEMA.index("CREATE TABLE run_attempts")
            connection.executescript(SCHEMA[:attempt_start])
            connection.execute(
                "INSERT INTO users "
                "(id, handle, display_name, active, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?)",
                ("boxu_" + "1" * 32, "operator", "Operator", "then", "then"),
            )
            connection.execute("PRAGMA user_version = 4")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], 8)
            self.assertEqual(second, first)
            self.assertEqual(first["counts"]["users"], 1)
            self.assertEqual(first["counts"]["run_attempts"], 0)
            with closing(database.connect()) as migrated:
                tables = {
                    row[0]
                    for row in migrated.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table'"
                    )
                }
            self.assertIn("run_attempts", tables)
            self.assertIn("run_attempt_events", tables)
            self.assertIn("attempt_handoffs", tables)
            self.assertIn("attempt_results", tables)


if __name__ == "__main__":
    unittest.main()
