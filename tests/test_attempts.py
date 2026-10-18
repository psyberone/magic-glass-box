from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from magic_glass_box.db import (
    SCHEMA,
    SCHEMA_VERSION,
    ConflictError,
    Database,
    NotFoundError,
    StorageError,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    HandoffEnvelope,
    InteractionKind,
    ProjectContext,
    RunState,
    ValidationError,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import FileState, ResultChange, ResultManifest
from magic_glass_box.storage.schema import (
    ATTEMPT_OWNER_AUDIT_SCHEMA,
    PROVIDER_BUDGET_SCHEMA,
    PROVIDER_RECOVERY_SCHEMA,
    RECOVERY_CHECKPOINT_SCHEMA,
    RUN_PAUSED_STATE_MIGRATION,
)

OWNER_ID = "orchestrator-test-owner"
IMAGE_ID = "sha256:" + "c" * 64


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
            owner_id=OWNER_ID,
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision="a" * 40,
            container_id="b" * 64,
            container_name="magic-glass-box-test",
            image_id=IMAGE_ID,
        )
        return run, running

    def test_generic_run_transition_cannot_bypass_active_attempt_authority(
        self,
    ) -> None:
        for target in (
            RunState.WAITING_FOR_INPUT,
            RunState.SUCCEEDED,
            RunState.FAILED,
            RunState.CANCELLED,
        ):
            run, _attempt = self.running_attempt()
            current = self.database.get_run(run["id"])
            with (
                self.subTest(target=target),
                self.assertRaisesRegex(ConflictError, "active attempt"),
            ):
                self.database.transition_run(
                    run["id"],
                    target=target,
                    expected_version=current["version"],
                )

    def test_start_attempt_validates_boundaries_and_records_exact_audit(self) -> None:
        defaults = {
            "owner_id": OWNER_ID,
            "expected_attempt_version": 1,
            "expected_run_version": 2,
            "source_revision": "a" * 40,
            "container_id": "b" * 64,
            "container_name": "magic-glass-box-test",
            "image_id": IMAGE_ID,
        }
        with self.assertRaises(NotFoundError):
            self.database.start_run_attempt("x" * 64, **defaults)
        for value, message in (
            (None, "attempt_id must be text"),
            ("", "attempt_id must not be empty"),
            ("x" * 65, "attempt_id exceeds 64 UTF-8 bytes"),
        ):
            with (
                self.subTest(attempt_id=value),
                self.assertRaises(ValidationError) as raised,
            ):
                self.database.start_run_attempt(  # type: ignore[arg-type]
                    value, **defaults
                )
            self.assertEqual(str(raised.exception), message)

        for field in (
            "source_revision",
            "container_id",
            "container_name",
            "image_id",
        ):
            for value, suffix in (
                (None, "must be text"),
                ("", "must not be empty"),
                ("x" * 513, "exceeds 512 UTF-8 bytes"),
            ):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValidationError) as raised,
                ):
                    self.database.start_run_attempt(
                        "att_" + "0" * 32,
                        **(defaults | {field: value}),  # type: ignore[dict-item]
                    )
                self.assertEqual(str(raised.exception), f"{field} {suffix}")

            run = self.create_run()
            attempt = self.database.begin_run_attempt(
                run["id"],
                runtime_adapter="docker",
                owner_id=OWNER_ID,
                expected_run_version=1,
            )
            started = self.database.start_run_attempt(
                attempt["id"],
                **(defaults | {field: "x" * 512}),
            )
            self.assertEqual(started[field], "x" * 512)

        for field in ("expected_attempt_version", "expected_run_version"):
            for value in (True, 0, 1.5, "1"):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValidationError) as raised,
                ):
                    self.database.start_run_attempt(
                        "att_" + "0" * 32,
                        **(defaults | {field: value}),  # type: ignore[dict-item]
                    )
                self.assertEqual(
                    str(raised.exception), f"{field} must be a positive integer"
                )

        run = self.create_run()
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id=OWNER_ID,
            expected_run_version=1,
        )
        started = self.database.start_run_attempt(attempt["id"], **defaults)
        expected_metadata = {
            "container_id": "b" * 64,
            "container_name": "magic-glass-box-test",
            "image_id": IMAGE_ID,
            "source_revision": "a" * 40,
        }
        self.assertEqual(started["state"], "running")
        self.assertEqual(started["version"], 2)
        self.assertEqual(started["run_state"], "running")
        self.assertEqual(started["run_version"], 3)
        self.assertEqual(started["events"][-1]["sequence"], 2)
        self.assertEqual(started["events"][-1]["kind"], "attempt.started")
        self.assertEqual(started["events"][-1]["from_state"], "preparing")
        self.assertEqual(started["events"][-1]["to_state"], "running")
        self.assertEqual(started["events"][-1]["metadata"], expected_metadata)
        run_event = self.database.get_run(run["id"])["events"][-1]
        self.assertEqual(run_event["sequence"], 3)
        self.assertEqual(run_event["from_state"], "preparing")
        self.assertEqual(run_event["to_state"], "running")
        self.assertEqual(run_event["metadata"], expected_metadata)

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
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            exit_code=0,
            image_id=IMAGE_ID,
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
                "image_id": IMAGE_ID,
                "source_revision": "a" * 40,
            },
        )
        self.assertEqual(
            finished_attempt["events"][-1]["metadata"],
            {
                "exit_code": 0,
                "handoff_id": first["id"],
                "image_id": IMAGE_ID,
                "result_id": finished_attempt["result"]["id"],
                "result_sha256": finished_attempt["result"]["content_sha256"],
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
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            exit_code=0,
            image_id=IMAGE_ID,
        )
        self.assertEqual(finished["state"], "interrupted")
        self.assertEqual(
            finished["reason"], "worker exited while paused without a handoff"
        )
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
        result = self.record_result(running["id"])
        completed = self.database.complete_run_attempt(
            running["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            exit_code=0,
            image_id=IMAGE_ID,
        )

        self.assertEqual(handoff["pending_interaction_ids"], [interaction["id"]])
        self.assertEqual(completed["state"], "handed_off")
        self.assertEqual(completed["run_state"], "queued")
        self.assertEqual(completed["run_version"], 5)
        run_event = self.database.get_run(run["id"])["events"][-1]
        self.assertEqual(run_event["sequence"], 5)
        self.assertEqual(run_event["from_state"], "waiting_for_input")
        self.assertEqual(run_event["to_state"], "queued")
        self.assertEqual(
            run_event["metadata"],
            {
                "attempt_id": running["id"],
                "result_id": result["id"],
                "result_sha256": result["content_sha256"],
            },
        )

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
            owner_id=OWNER_ID,
            expected_attempt_version=attempt["version"],
            reason="workspace preparation failed",
        )

        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed["run_state"], "failed")
        self.assertEqual(failed["reason"], "workspace preparation failed")
        self.assertEqual(failed["events"][-1]["kind"], "attempt.failed")
        self.assertEqual(failed["events"][-1]["from_state"], "preparing")
        self.assertEqual(failed["events"][-1]["to_state"], "failed")
        self.assertEqual(
            failed["events"][-1]["metadata"],
            {"reason": "workspace preparation failed"},
        )
        run_event = self.database.get_run(run["id"])["events"][-1]
        self.assertEqual(run_event["kind"], "run.transitioned")
        self.assertEqual(run_event["from_state"], "preparing")
        self.assertEqual(run_event["to_state"], "failed")
        self.assertEqual(
            run_event["metadata"], {"reason": "workspace preparation failed"}
        )

    def test_fail_attempt_validates_boundaries_and_records_interruption(self) -> None:
        defaults = {
            "owner_id": OWNER_ID,
            "expected_attempt_version": 1,
            "reason": "cleanup failed",
        }
        with self.assertRaises(NotFoundError):
            self.database.fail_run_attempt("x" * 64, **defaults)
        for value, message in (
            (None, "attempt_id must be text"),
            ("", "attempt_id must not be empty"),
            ("x" * 65, "attempt_id exceeds 64 UTF-8 bytes"),
        ):
            with (
                self.subTest(attempt_id=value),
                self.assertRaises(ValidationError) as raised,
            ):
                self.database.fail_run_attempt(  # type: ignore[arg-type]
                    value, **defaults
                )
            self.assertEqual(str(raised.exception), message)

        for value, message in (
            (None, "reason must be text"),
            ("", "reason must not be empty"),
            ("x" * 4097, "reason exceeds 4096 UTF-8 bytes"),
        ):
            with (
                self.subTest(reason=value),
                self.assertRaises(ValidationError) as raised,
            ):
                self.database.fail_run_attempt(
                    "att_" + "0" * 32,
                    **(defaults | {"reason": value}),  # type: ignore[dict-item]
                )
            self.assertEqual(str(raised.exception), message)

        for value in (True, 0, 1.5, "1"):
            with (
                self.subTest(version=value),
                self.assertRaises(ValidationError) as raised,
            ):
                self.database.fail_run_attempt(
                    "att_" + "0" * 32,
                    **(defaults | {"expected_attempt_version": value}),  # type: ignore[dict-item]
                )
            self.assertEqual(
                str(raised.exception),
                "expected_attempt_version must be a positive integer",
            )
        for value in (None, 0, 1, "true"):
            with (
                self.subTest(interrupted=value),
                self.assertRaises(ValidationError) as raised,
            ):
                self.database.fail_run_attempt(
                    "att_" + "0" * 32,
                    **defaults,
                    interrupted=value,  # type: ignore[arg-type]
                )
            self.assertEqual(str(raised.exception), "interrupted must be boolean")

        run = self.create_run()
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id=OWNER_ID,
            expected_run_version=1,
        )
        interrupted = self.database.fail_run_attempt(
            attempt["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=1,
            reason="x" * 4096,
            interrupted=True,
        )
        self.assertEqual(interrupted["state"], "interrupted")
        self.assertEqual(interrupted["run_state"], "failed")
        self.assertEqual(interrupted["reason"], "x" * 4096)
        self.assertEqual(interrupted["events"][-1]["kind"], "attempt.interrupted")
        self.assertEqual(interrupted["events"][-1]["sequence"], 2)
        self.assertEqual(interrupted["events"][-1]["from_state"], "preparing")
        self.assertEqual(interrupted["events"][-1]["to_state"], "interrupted")
        self.assertEqual(interrupted["events"][-1]["metadata"], {"reason": "x" * 4096})
        run_event = self.database.get_run(run["id"])["events"][-1]
        self.assertEqual(run_event["sequence"], 3)
        self.assertEqual(run_event["from_state"], "preparing")
        self.assertEqual(run_event["to_state"], "failed")
        self.assertEqual(
            run_event["metadata"], {"reason": "x" * 4096, "reconciled": True}
        )

    def test_successful_attempt_terminally_completes_its_run(self) -> None:
        run, running = self.running_attempt()
        result = self.record_result(running["id"])

        completed = self.database.complete_run_attempt(
            running["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            exit_code=0,
            image_id=IMAGE_ID,
        )

        self.assertEqual(completed["state"], "succeeded")
        self.assertEqual(completed["run_state"], "succeeded")
        self.assertEqual(completed["run_version"], 4)
        self.assertEqual(completed["exit_code"], 0)
        self.assertIsNone(completed["reason"])
        self.assertEqual(completed["result"], result)
        expected_metadata = {
            "exit_code": 0,
            "image_id": IMAGE_ID,
            "result_id": result["id"],
            "result_sha256": result["content_sha256"],
        }
        self.assertEqual(completed["events"][-1]["kind"], "attempt.completed")
        self.assertEqual(completed["events"][-1]["from_state"], "running")
        self.assertEqual(completed["events"][-1]["to_state"], "succeeded")
        self.assertEqual(completed["events"][-1]["metadata"], expected_metadata)
        event = self.database.get_run(run["id"])["events"][-1]
        self.assertEqual(event["kind"], "run.transitioned")
        self.assertEqual(event["sequence"], 4)
        self.assertEqual(event["from_state"], "running")
        self.assertEqual(event["to_state"], "succeeded")
        self.assertEqual(event["metadata"], expected_metadata)

    def test_attempt_completion_validates_exact_input_boundaries(self) -> None:
        arguments = {
            "owner_id": OWNER_ID,
            "expected_attempt_version": 1,
            "exit_code": 0,
            "image_id": IMAGE_ID,
        }
        for attempt_id, message in (
            (None, "attempt_id must be text"),
            ("", "attempt_id must not be empty"),
            ("x" * 65, "attempt_id exceeds 64 UTF-8 bytes"),
        ):
            with (
                self.subTest(attempt_id=attempt_id),
                self.assertRaises(ValidationError) as raised,
            ):
                self.database.complete_run_attempt(  # type: ignore[arg-type]
                    attempt_id, **arguments
                )
            self.assertEqual(str(raised.exception), message)
        with self.assertRaises(NotFoundError):
            self.database.complete_run_attempt("x" * 64, **arguments)

        for image_id, message in (
            (None, "image_id must be text"),
            ("", "image_id must not be empty"),
            ("x" * 513, "image_id exceeds 512 UTF-8 bytes"),
        ):
            with (
                self.subTest(image_id=image_id),
                self.assertRaises(ValidationError) as raised,
            ):
                self.database.complete_run_attempt(
                    "att_" + "0" * 32,
                    **(arguments | {"image_id": image_id}),  # type: ignore[dict-item]
                )
            self.assertEqual(str(raised.exception), message)
        with self.assertRaises(NotFoundError):
            self.database.complete_run_attempt(
                "att_" + "0" * 32,
                **(arguments | {"image_id": "x" * 512}),
            )

        for version in (True, 0, 1.5, "1"):
            with (
                self.subTest(expected_attempt_version=version),
                self.assertRaisesRegex(ValidationError, "expected_attempt_version"),
            ):
                self.database.complete_run_attempt(
                    "att_" + "0" * 32,
                    **(arguments | {"expected_attempt_version": version}),  # type: ignore[dict-item]
                )

        run = self.create_run()
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id=OWNER_ID,
            expected_run_version=1,
        )
        with self.assertRaisesRegex(ConflictError, "not running"):
            self.database.complete_run_attempt(
                preparing["id"],
                owner_id=OWNER_ID,
                expected_attempt_version=1,
                exit_code=0,
                image_id=IMAGE_ID,
            )

        for exit_code in (True, 1.5, -(1 << 31) - 1, 1 << 31):
            with (
                self.subTest(exit_code=exit_code),
                self.assertRaisesRegex(ValidationError, "exit_code"),
            ):
                self.database.complete_run_attempt(
                    "att_" + "0" * 32,
                    **(arguments | {"exit_code": exit_code}),  # type: ignore[dict-item]
                )

        _, running = self.running_attempt()
        result = self.record_result(running["id"])
        completed = self.database.complete_run_attempt(
            running["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            exit_code=-(1 << 31),
            image_id=IMAGE_ID,
        )
        self.assertEqual(completed["state"], "failed")
        self.assertEqual(completed["run_state"], "failed")
        self.assertEqual(completed["exit_code"], -(1 << 31))
        self.assertEqual(completed["reason"], "agent exited with code -2147483648")
        expected_metadata = {
            "exit_code": -(1 << 31),
            "image_id": IMAGE_ID,
            "reason": "agent exited with code -2147483648",
            "result_id": result["id"],
            "result_sha256": result["content_sha256"],
        }
        self.assertEqual(completed["events"][-1]["metadata"], expected_metadata)
        self.assertEqual(completed["events"][-1]["to_state"], "failed")

    def test_running_attempt_requires_a_captured_result_before_completion(self) -> None:
        _, running = self.running_attempt()

        with self.assertRaises(ConflictError) as raised:
            self.database.complete_run_attempt(
                running["id"],
                owner_id=OWNER_ID,
                expected_attempt_version=running["version"],
                exit_code=0,
                image_id=IMAGE_ID,
            )
        self.assertEqual(
            str(raised.exception), "run attempt result has not been captured"
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

    def test_interrupted_result_is_pinned_and_applied_before_replacement_start(
        self,
    ) -> None:
        run, running = self.running_attempt()
        result = self.database.record_attempt_result(
            running["id"], self.manifest(running["id"], changed=True)
        )
        interrupted = self.database.fail_run_attempt(
            running["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            reason="worker process disappeared",
            interrupted=True,
        )
        successor = self.database.retry_run(
            run["id"],
            requested_by=self.user["id"],
            expected_run_version=interrupted["run_version"],
        )

        preparing = self.database.begin_run_attempt(
            successor["id"],
            runtime_adapter="docker",
            owner_id="replacement-owner",
            expected_run_version=successor["version"],
        )

        checkpoint = preparing["recovery_checkpoint"]
        self.assertEqual(
            set(checkpoint),
            {
                "applied_at",
                "attempt_id",
                "changed_bytes",
                "changed_paths",
                "content_sha256",
                "created_at",
                "id",
                "result_id",
                "source_attempt_id",
                "source_revision",
                "source_run_id",
            },
        )
        self.assertTrue(checkpoint["id"].startswith("rcp_"))
        self.assertEqual(checkpoint["attempt_id"], preparing["id"])
        self.assertEqual(checkpoint["source_run_id"], run["id"])
        self.assertEqual(checkpoint["source_attempt_id"], running["id"])
        self.assertEqual(checkpoint["result_id"], result["id"])
        self.assertEqual(checkpoint["content_sha256"], result["content_sha256"])
        self.assertEqual(checkpoint["changed_paths"], 1)
        self.assertEqual(checkpoint["changed_bytes"], 4)
        self.assertIsNotNone(checkpoint["created_at"])
        self.assertIsNone(checkpoint["applied_at"])
        self.assertEqual(
            preparing["events"][-1]["metadata"],
            {
                "recovery_checkpoint_id": checkpoint["id"],
                "recovery_result_sha256": result["content_sha256"],
                "recovery_source_attempt_id": running["id"],
            },
        )
        with self.assertRaises(ConflictError) as raised:
            self.database.start_run_attempt(
                preparing["id"],
                owner_id="replacement-owner",
                expected_attempt_version=preparing["version"],
                expected_run_version=preparing["run_version"],
                source_revision="a" * 40,
                container_id="d" * 64,
                container_name="replacement-worker",
                image_id=IMAGE_ID,
            )
        self.assertEqual(
            str(raised.exception),
            "run attempt recovery checkpoint has not been applied",
        )
        with self.assertRaises(ConflictError) as raised:
            self.database.record_recovery_checkpoint_application(
                preparing["id"], owner_id="wrong-owner"
            )
        self.assertEqual(
            str(raised.exception), "run attempt is owned by another orchestrator"
        )

        applied = self.database.record_recovery_checkpoint_application(
            preparing["id"], owner_id="replacement-owner"
        )
        repeated_application = self.database.record_recovery_checkpoint_application(
            preparing["id"], owner_id="replacement-owner"
        )
        self.assertEqual(repeated_application, applied)
        self.assertEqual(applied["checkpoint_id"], checkpoint["id"])
        self.assertEqual(applied["attempt_id"], preparing["id"])
        self.assertIsNotNone(applied["applied_at"])
        with self.assertRaises(ConflictError) as raised:
            self.database.start_run_attempt(
                preparing["id"],
                owner_id="replacement-owner",
                expected_attempt_version=preparing["version"],
                expected_run_version=preparing["run_version"],
                source_revision="e" * 40,
                container_id="d" * 64,
                container_name="replacement-worker",
                image_id=IMAGE_ID,
            )
        self.assertEqual(
            str(raised.exception),
            "run attempt recovery checkpoint source revision changed",
        )
        started = self.database.start_run_attempt(
            preparing["id"],
            owner_id="replacement-owner",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision="a" * 40,
            container_id="d" * 64,
            container_name="replacement-worker",
            image_id=IMAGE_ID,
        )
        self.assertEqual(
            started["events"][-1]["metadata"],
            {
                "container_id": "d" * 64,
                "container_name": "replacement-worker",
                "image_id": IMAGE_ID,
                "recovery_checkpoint_id": checkpoint["id"],
                "recovery_result_id": result["id"],
                "recovery_result_sha256": result["content_sha256"],
                "recovery_source_attempt_id": running["id"],
                "source_revision": "a" * 40,
            },
        )
        self.assertEqual(
            self.database.get_run_execution(successor["id"])["recovery_checkpoint"][
                "id"
            ],
            checkpoint["id"],
        )
        with self.assertRaises(ConflictError) as raised:
            self.database.record_recovery_checkpoint_application(
                preparing["id"], owner_id="replacement-owner"
            )
        self.assertEqual(str(raised.exception), "run attempt is not preparing: running")

        connection = self.database.connect()
        self.addCleanup(connection.close)
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE attempt_recovery_checkpoints SET changed_paths = 0 "
                "WHERE id = ?",
                (checkpoint["id"],),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "DELETE FROM attempt_recovery_applications WHERE checkpoint_id = ?",
                (checkpoint["id"],),
            )

    def test_application_receipt_requires_a_selected_checkpoint(self) -> None:
        run = self.create_run()
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id=OWNER_ID,
            expected_run_version=run["version"],
        )

        with self.assertRaisesRegex(NotFoundError, "checkpoint not found"):
            self.database.record_recovery_checkpoint_application(
                preparing["id"], owner_id=OWNER_ID
            )

    def test_application_receipt_validates_identifier_boundaries(self) -> None:
        for arguments, message in (
            (
                {"attempt_id": None, "owner_id": OWNER_ID},
                "attempt_id must be text",
            ),
            (
                {"attempt_id": "x" * 65, "owner_id": OWNER_ID},
                "attempt_id exceeds 64 UTF-8 bytes",
            ),
            (
                {"attempt_id": "x" * 64, "owner_id": None},
                "owner_id must be text",
            ),
            (
                {"attempt_id": "x" * 64, "owner_id": "x" * 129},
                "owner_id exceeds 128 UTF-8 bytes",
            ),
        ):
            with (
                self.subTest(arguments=arguments),
                self.assertRaises(ValidationError) as raised,
            ):
                self.database.record_recovery_checkpoint_application(  # type: ignore[arg-type]
                    **arguments
                )
            self.assertEqual(str(raised.exception), message)

        with self.assertRaises(NotFoundError):
            self.database.record_recovery_checkpoint_application(
                "x" * 64, owner_id="x" * 128
            )

    def test_recovery_selection_fails_closed_on_a_cyclic_lineage(self) -> None:
        connection = mock.Mock()
        connection.execute.side_effect = (
            mock.Mock(
                fetchone=mock.Mock(return_value={"predecessor_run_id": "run_cycle"})
            ),
            mock.Mock(fetchone=mock.Mock(return_value=None)),
            AssertionError("recovery selection queried past the detected cycle"),
        )

        with self.assertRaises(StorageError) as raised:
            self.database._select_recovery_result(  # noqa: SLF001
                connection,
                "run_cycle",
            )
        self.assertEqual(
            str(raised.exception), "run replacement lineage contains a cycle"
        )
        self.assertEqual(connection.execute.call_count, 2)

    def test_recovery_selection_rejects_a_changed_run_revision(self) -> None:
        run, running = self.running_attempt()
        self.record_result(running["id"])
        interrupted = self.database.fail_run_attempt(
            running["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            reason="worker process disappeared",
            interrupted=True,
        )
        successor = self.database.retry_run(
            run["id"],
            requested_by=self.user["id"],
            expected_run_version=interrupted["run_version"],
        )
        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE runs SET source_revision = ? WHERE id = ?",
                ("e" * 40, successor["id"]),
            )
            connection.commit()

        with self.assertRaises(StorageError) as raised:
            self.database.begin_run_attempt(
                successor["id"],
                runtime_adapter="docker",
                owner_id="replacement-owner",
                expected_run_version=successor["version"],
            )
        self.assertEqual(
            str(raised.exception),
            "recovery checkpoint source revision does not match run",
        )

    def test_failed_output_is_not_promoted_to_a_recovery_checkpoint(self) -> None:
        run, running = self.running_attempt()
        self.record_result(running["id"])
        failed = self.database.complete_run_attempt(
            running["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            exit_code=17,
            image_id=IMAGE_ID,
        )
        successor = self.database.retry_run(
            run["id"],
            requested_by=self.user["id"],
            expected_run_version=failed["run_version"],
        )

        preparing = self.database.begin_run_attempt(
            successor["id"],
            runtime_adapter="docker",
            owner_id="replacement-owner",
            expected_run_version=successor["version"],
        )

        self.assertIsNone(preparing["recovery_checkpoint"])

    def test_failed_replacement_carries_forward_only_its_pinned_checkpoint(
        self,
    ) -> None:
        run, running = self.running_attempt()
        result = self.record_result(running["id"])
        interrupted = self.database.fail_run_attempt(
            running["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            reason="worker process disappeared",
            interrupted=True,
        )
        first_replacement = self.database.retry_run(
            run["id"],
            requested_by=self.user["id"],
            expected_run_version=interrupted["run_version"],
        )
        preparing = self.database.begin_run_attempt(
            first_replacement["id"],
            runtime_adapter="docker",
            owner_id="replacement-one",
            expected_run_version=first_replacement["version"],
        )
        failed = self.database.fail_run_attempt(
            preparing["id"],
            owner_id="replacement-one",
            expected_attempt_version=preparing["version"],
            reason="workspace preparation failed",
        )
        second_replacement = self.database.retry_run(
            first_replacement["id"],
            requested_by=self.user["id"],
            expected_run_version=failed["run_version"],
        )

        carried = self.database.begin_run_attempt(
            second_replacement["id"],
            runtime_adapter="docker",
            owner_id="replacement-two",
            expected_run_version=second_replacement["version"],
        )["recovery_checkpoint"]

        self.assertEqual(carried["source_attempt_id"], running["id"])
        self.assertEqual(carried["result_id"], result["id"])
        self.assertEqual(carried["content_sha256"], result["content_sha256"])

    def test_successful_replacement_does_not_resurrect_its_input_checkpoint(
        self,
    ) -> None:
        run, running = self.running_attempt()
        self.record_result(running["id"])
        interrupted = self.database.fail_run_attempt(
            running["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            reason="worker process disappeared",
            interrupted=True,
        )
        replacement = self.database.retry_run(
            run["id"],
            requested_by=self.user["id"],
            expected_run_version=interrupted["run_version"],
        )
        preparing = self.database.begin_run_attempt(
            replacement["id"],
            runtime_adapter="docker",
            owner_id="replacement-owner",
            expected_run_version=replacement["version"],
        )
        self.database.record_recovery_checkpoint_application(
            preparing["id"], owner_id="replacement-owner"
        )
        active = self.database.start_run_attempt(
            preparing["id"],
            owner_id="replacement-owner",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision="a" * 40,
            container_id="d" * 64,
            container_name="replacement-worker",
            image_id=IMAGE_ID,
        )
        self.record_result(active["id"])
        succeeded = self.database.complete_run_attempt(
            active["id"],
            owner_id="replacement-owner",
            expected_attempt_version=active["version"],
            exit_code=0,
            image_id=IMAGE_ID,
        )
        clean_retry = self.database.retry_run(
            replacement["id"],
            requested_by=self.user["id"],
            expected_run_version=succeeded["run_version"],
        )

        next_attempt = self.database.begin_run_attempt(
            clean_retry["id"],
            runtime_adapter="docker",
            owner_id="clean-retry-owner",
            expected_run_version=clean_retry["version"],
        )

        self.assertIsNone(next_attempt["recovery_checkpoint"])

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
        self.assertEqual(claimed["version"], attempt["version"] + 1)
        self.assertEqual(claimed["events"][-1]["kind"], "attempt.owner_changed")
        self.assertEqual(claimed["events"][-1]["from_state"], "preparing")
        self.assertEqual(claimed["events"][-1]["to_state"], "preparing")
        self.assertEqual(
            claimed["events"][-1]["metadata"],
            {
                "new_owner_id": "orchestrator-other",
                "previous_owner_id": OWNER_ID,
            },
        )
        start = {
            "expected_attempt_version": claimed["version"],
            "expected_run_version": claimed["run_version"],
            "source_revision": "a" * 40,
            "container_id": "b" * 64,
            "container_name": "magic-glass-box-test",
            "image_id": IMAGE_ID,
        }
        with self.assertRaisesRegex(ConflictError, "owned by another orchestrator"):
            self.database.start_run_attempt(claimed["id"], owner_id=OWNER_ID, **start)
        started = self.database.start_run_attempt(
            claimed["id"], owner_id="orchestrator-other", **start
        )
        self.assertEqual(started["state"], "running")

    def test_cancel_request_is_audited_then_terminal_only_after_worker_exit(
        self,
    ) -> None:
        run, running = self.running_attempt()
        interaction = self.database.open_interaction(
            run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "a" * 32,
            expected_run_version=running["run_version"],
        )

        requested = self.database.request_run_cancellation(
            run["id"],
            requested_by=self.user["id"],
            expected_run_version=interaction["run_version"],
            reason="operator requested stop",
        )

        self.assertEqual(requested["state"], "waiting_for_input")
        self.assertEqual(requested["events"][-1]["kind"], "run.cancel_requested")
        active = requested["attempts"][-1]
        self.assertEqual(active["cancel_requested_by"], self.user["id"])
        self.assertEqual(active["cancel_reason"], "operator requested stop")
        control = self.database.renew_run_attempt_lease(
            running["id"], owner_id=OWNER_ID, lease_seconds=60
        )
        self.assertTrue(control["cancel_requested"])

        result = self.record_result(running["id"])
        completed = self.database.complete_run_attempt(
            running["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=running["version"],
            exit_code=143,
            image_id=IMAGE_ID,
        )
        self.assertEqual(completed["state"], "cancelled")
        self.assertEqual(completed["run_state"], "cancelled")
        self.assertEqual(completed["run_version"], 6)
        self.assertEqual(completed["exit_code"], 143)
        self.assertEqual(completed["reason"], "operator requested stop")
        attempt_metadata = {
            "exit_code": 143,
            "image_id": IMAGE_ID,
            "reason": "operator requested stop",
            "result_id": result["id"],
            "result_sha256": result["content_sha256"],
        }
        self.assertEqual(completed["events"][-1]["metadata"], attempt_metadata)
        run_event = self.database.get_run(run["id"])["events"][-1]
        self.assertEqual(run_event["sequence"], 6)
        self.assertEqual(run_event["to_state"], "cancelled")
        self.assertEqual(
            run_event["metadata"],
            attempt_metadata | {"requested_by": self.user["id"]},
        )
        cancelled_interaction = self.database.get_run(run["id"])["interactions"][0]
        self.assertEqual(cancelled_interaction["id"], interaction["id"])
        self.assertEqual(cancelled_interaction["state"], "cancelled")
        self.assertEqual(cancelled_interaction["version"], 2)
        self.assertIsNotNone(cancelled_interaction["resolved_at"])
        self.assertEqual(
            cancelled_interaction["events"][-1]["kind"], "interaction.cancelled"
        )

    def test_running_takeover_fences_stale_owner_terminal_writes(self) -> None:
        _, running = self.running_attempt()
        self.record_result(running["id"])
        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE run_attempts SET lease_expires_at = ? WHERE id = ?",
                ("2000-01-01T00:00:00.000000+00:00", running["id"]),
            )
            connection.commit()
        claimed = self.database.claim_expired_run_attempt(
            running["id"], owner_id="orchestrator-other", lease_seconds=60
        )

        with self.assertRaisesRegex(ConflictError, "owned by another orchestrator"):
            self.database.complete_run_attempt(
                claimed["id"],
                owner_id=OWNER_ID,
                expected_attempt_version=claimed["version"],
                exit_code=0,
                image_id=IMAGE_ID,
            )
        with self.assertRaisesRegex(ConflictError, "owned by another orchestrator"):
            self.database.fail_run_attempt(
                claimed["id"],
                owner_id=OWNER_ID,
                expected_attempt_version=claimed["version"],
                reason="stale owner attempted cleanup",
            )

        completed = self.database.complete_run_attempt(
            claimed["id"],
            owner_id="orchestrator-other",
            expected_attempt_version=claimed["version"],
            exit_code=0,
            image_id=IMAGE_ID,
        )
        self.assertEqual(completed["state"], "succeeded")
        self.assertEqual(
            [event["kind"] for event in completed["events"][-2:]],
            ["attempt.owner_changed", "attempt.completed"],
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
    def test_schema_seventeen_preserves_attempt_events_and_immutability(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-seventeen.db"
            tail = (
                ATTEMPT_OWNER_AUDIT_SCHEMA
                + PROVIDER_BUDGET_SCHEMA
                + RUN_PAUSED_STATE_MIGRATION
                + PROVIDER_RECOVERY_SCHEMA
                + RECOVERY_CHECKPOINT_SCHEMA
            )
            self.assertTrue(SCHEMA.endswith(tail))
            historical_schema = SCHEMA[: -len(tail)]
            self.assertEqual(
                hashlib.sha256(historical_schema.encode()).hexdigest(),
                "d60985b6a8aa69e910c330a8399b4f2e728ae213c94ae017eb648a50b5bfbb3f",
            )
            with closing(sqlite3.connect(path)) as connection:
                connection.executescript(historical_schema)
                connection.execute("PRAGMA user_version = 17")
                connection.commit()
            os.chmod(path, 0o600)

            database = Database(path)
            user = database.create_user("operator", "Operator")
            instance = database.create_krab_instance(
                "Local Krab", "http://127.0.0.1:8765"
            )
            identity = database.store_krab_identity(
                instance["id"],
                KrabIdentity("usr_" + "1" * 32, "agent_one", 1),
                "file:/private/agent.token",
                user_id=user["id"],
            )
            database.create_project(
                instance_id=instance["id"],
                krab_project_id="prj_" + "2" * 32,
                krab_project_key="migration-project",
                display_name="Migration Project",
                context=ProjectContext.from_mapping(
                    {"purpose": "Preserve attempt audit events"}
                ),
                created_by=user["id"],
            )
            item = database.import_work_item(
                project_key="migration-project",
                envelope=WorkItemEnvelope.from_mapping(
                    {
                        "schema_version": 1,
                        "source": "manual",
                        "external_id": "migration-item",
                        "source_updated_at": "2026-10-16T12:00:00Z",
                        "title": "Migrate attempt events",
                        "brief": "Prove the v17 audit stream survives v18.",
                    }
                ),
                imported_by=user["id"],
            )
            database.create_agent_profile(
                name="Worker",
                spec=AgentProfileSpec.from_mapping(
                    {"image": "agent:local", "command": ["agent", "run"]}
                ),
                krab_identity_id=identity["id"],
                created_by=user["id"],
            )
            with mock.patch.object(
                database, "_list_provider_recoveries", return_value=[]
            ):
                run = database.create_run(
                    project_key="migration-project",
                    profile_name="Worker",
                    work_item_id=item["id"],
                    requested_by=user["id"],
                    source_revision="a" * 40,
                )

            def legacy_attempt(attempt_id: str) -> dict[str, object]:
                with closing(database.connect()) as connection:
                    row = connection.execute(
                        "SELECT ra.id, ra.state, ra.version, "
                        "r.version AS run_version FROM run_attempts ra "
                        "JOIN runs r ON r.id = ra.run_id WHERE ra.id = ?",
                        (attempt_id,),
                    ).fetchone()
                self.assertIsNotNone(row)
                return dict(row)

            with mock.patch.object(
                database, "get_run_attempt", side_effect=legacy_attempt
            ):
                preparing = database.begin_run_attempt(
                    run["id"],
                    runtime_adapter="docker",
                    owner_id=OWNER_ID,
                    expected_run_version=run["version"],
                )
            now = database.now()
            with closing(database.connect()) as connection:
                connection.execute(
                    "UPDATE run_attempts SET state = 'running', version = 2, "
                    "source_revision = ?, container_id = ?, container_name = ?, "
                    "image_id = ?, started_at = ? WHERE id = ?",
                    (
                        "a" * 40,
                        "b" * 64,
                        "migration-worker",
                        IMAGE_ID,
                        now,
                        preparing["id"],
                    ),
                )
                connection.execute(
                    "UPDATE runs SET state = 'running', version = 3, "
                    "source_revision = ?, started_at = ?, updated_at = ? "
                    "WHERE id = ?",
                    ("a" * 40, now, now, run["id"]),
                )
                connection.execute(
                    "INSERT INTO run_attempt_events "
                    "(attempt_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, 2, "
                    "'attempt.started', 'preparing', 'running', '{}', ?)",
                    (preparing["id"], now),
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, 3, "
                    "'run.transitioned', 'preparing', 'running', '{}', ?)",
                    (run["id"], now),
                )
                connection.commit()
            running = preparing
            with closing(database.connect()) as connection:
                before = [
                    tuple(row)
                    for row in connection.execute(
                        "SELECT id, attempt_id, sequence, kind, from_state, "
                        "to_state, metadata_json, created_at "
                        "FROM run_attempt_events ORDER BY id"
                    )
                ]

            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
            self.assertEqual(second, first)
            self.assertEqual(
                database.get_run_attempt(running["id"])["state"], "running"
            )
            with closing(database.connect()) as connection:
                after = [
                    tuple(row)
                    for row in connection.execute(
                        "SELECT id, attempt_id, sequence, kind, from_state, "
                        "to_state, metadata_json, created_at "
                        "FROM run_attempt_events ORDER BY id"
                    )
                ]
                table_sql = connection.execute(
                    "SELECT sql FROM sqlite_schema "
                    "WHERE type = 'table' AND name = 'run_attempt_events'"
                ).fetchone()[0]
                run_table_sql = connection.execute(
                    "SELECT sql FROM sqlite_schema "
                    "WHERE type = 'table' AND name = 'runs'"
                ).fetchone()[0]
                recovery_table = connection.execute(
                    "SELECT 1 FROM sqlite_schema "
                    "WHERE type = 'table' AND name = 'provider_recoveries'"
                ).fetchone()
                self.assertIn("attempt.owner_changed", table_sql)
                self.assertIn("'paused'", run_table_sql)
                self.assertIsNotNone(recovery_table)
                self.assertEqual(
                    connection.execute("PRAGMA foreign_key_check").fetchall(), []
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE run_attempt_events SET metadata_json = '{}' "
                        "WHERE attempt_id = ?",
                        (running["id"],),
                    )
            self.assertEqual(after, before)

    def test_schema_eighteen_fixture_is_pinned_and_migrates_budget_schema(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-eighteen.db"
            tail = (
                PROVIDER_BUDGET_SCHEMA
                + RUN_PAUSED_STATE_MIGRATION
                + PROVIDER_RECOVERY_SCHEMA
                + RECOVERY_CHECKPOINT_SCHEMA
            )
            self.assertTrue(SCHEMA.endswith(tail))
            historical_schema = SCHEMA[: -len(tail)]
            self.assertEqual(
                hashlib.sha256(historical_schema.encode()).hexdigest(),
                "3aadec58ec078154cda90837b38cac335fd1d82d400a3cfa5d6bbd8612f211e4",
            )
            with closing(sqlite3.connect(path)) as connection:
                connection.executescript(historical_schema)
                connection.execute("PRAGMA user_version = 18")
                connection.commit()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
            self.assertEqual(second, first)
            with closing(database.connect()) as connection:
                columns = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(provider_invocations)"
                    )
                }
                indexes = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA index_list(provider_invocations)"
                    )
                }
            self.assertIn("budget_policy_sha256", columns)
            self.assertIn("accounted_microusd", columns)
            self.assertIn("provider_invocation_budget_grant", indexes)

    def test_schema_twenty_migrates_recovery_checkpoint_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-twenty.db"
            self.assertTrue(SCHEMA.endswith(RECOVERY_CHECKPOINT_SCHEMA))
            historical_schema = SCHEMA[: -len(RECOVERY_CHECKPOINT_SCHEMA)]
            self.assertEqual(
                hashlib.sha256(historical_schema.encode()).hexdigest(),
                "e89d6945978ef7d1652a3c9ee954b6f7b48348226206a2351b3c6f462b11430a",
            )
            with closing(sqlite3.connect(path)) as connection:
                connection.executescript(historical_schema)
                connection.execute("PRAGMA user_version = 20")
                connection.commit()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
            self.assertEqual(second, first)
            self.assertEqual(first["counts"]["attempt_recovery_checkpoints"], 0)
            self.assertEqual(first["counts"]["attempt_recovery_applications"], 0)
            with closing(database.connect()) as connection:
                tables = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table'"
                    )
                }
                self.assertEqual(
                    connection.execute("PRAGMA foreign_key_check").fetchall(), []
                )
            self.assertIn("attempt_recovery_checkpoints", tables)
            self.assertIn("attempt_recovery_applications", tables)

    def test_schema_four_migrates_existing_attempt_state(self) -> None:
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

            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
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
