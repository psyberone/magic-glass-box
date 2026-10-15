from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

from magic_glass_box.db import (
    ATTEMPT_SCHEMA,
    INTERACTION_SCHEMA,
    PROVIDER_SCHEMA,
    SCHEMA_V1,
    WORK_ITEM_SCHEMA,
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
    StorageError,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    InteractionKind,
    ProjectContext,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
    WorkLogEntry,
    WorkLogKind,
    WorkMemoryCheckpoint,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import ResultManifest
from magic_glass_box.service import ControlPlane


def log_entry(
    summary: str = "Worker found the failing boundary.",
    *,
    kind: str = "finding",
) -> WorkLogEntry:
    return WorkLogEntry.from_mapping(
        {
            "schema_version": 1,
            "kind": kind,
            "summary": summary,
            "details": ["The failure is reproducible with the focused test."],
            "artifacts": [
                {
                    "path": "tests/test_work_memory.py",
                    "description": "Focused regression coverage.",
                }
            ],
            "related_interaction_ids": [],
            "candidate_revision": "a" * 40,
        }
    )


def checkpoint(
    summary: str = "The plan is ready for parallel work",
) -> WorkMemoryCheckpoint:
    return WorkMemoryCheckpoint.from_mapping(
        {
            "schema_version": 1,
            "summary": summary,
            "decisions": ["Keep coordination state local to the work item."],
            "artifacts": [
                {
                    "path": "docs/architecture.md",
                    "description": "The durable-memory boundary.",
                }
            ],
            "next_steps": ["Collect independent worker results."],
            "open_blockers": [],
            "candidate_revision": "a" * 40,
        }
    )


def assignment(title: str = "Review persistence") -> WorkAssignmentSpec:
    return WorkAssignmentSpec.from_mapping(
        {
            "schema_version": 1,
            "title": title,
            "objective": "Review the durable work-memory implementation.",
            "scope_paths": ["src/magic_glass_box/db.py"],
            "acceptance_checks": ["Run the focused test suite."],
            "base_revision": "a" * 40,
        }
    )


class WorkMemoryContractTests(unittest.TestCase):
    def test_published_schemas_and_examples_match_the_v1_contracts(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        contracts = (
            ("work-log-entry", WorkLogEntry.from_mapping),
            ("work-memory-checkpoint", WorkMemoryCheckpoint.from_mapping),
            ("work-assignment", WorkAssignmentSpec.from_mapping),
        )
        for name, factory in contracts:
            with self.subTest(name=name):
                schema = json.loads(
                    (repository / f"schemas/{name}-v1.schema.json").read_text(
                        encoding="utf-8"
                    )
                )
                example = json.loads(
                    (repository / f"examples/{name}.json").read_text(encoding="utf-8")
                )
                value = factory(example)
                self.assertEqual(schema["$id"], f"urn:magic-glass-box:schema:{name}:v1")
                self.assertFalse(schema["additionalProperties"])
                self.assertEqual(schema["properties"]["schema_version"]["const"], 1)
                self.assertEqual(value.to_dict(), example)
                for forbidden in ("credentials", "messages", "transcript"):
                    self.assertNotIn(forbidden, schema["properties"])

    def test_contracts_are_strict_bounded_and_content_addressed(self) -> None:
        entry = log_entry()
        memory = checkpoint()
        task = assignment()

        self.assertIs(entry.kind, WorkLogKind.FINDING)
        self.assertEqual(entry.to_dict()["kind"], "finding")
        self.assertEqual(len(entry.digest), 64)
        self.assertEqual(len(memory.digest), 64)
        self.assertEqual(len(task.digest), 64)
        self.assertNotIn("transcript", json.dumps(entry.to_dict()))

        for factory, value, message in (
            (
                WorkLogEntry.from_mapping,
                {**entry.to_dict(), "authority": "admin"},
                "unknown fields",
            ),
            (
                WorkLogEntry.from_mapping,
                {**entry.to_dict(), "kind": "message"},
                "kind is invalid",
            ),
            (
                WorkLogEntry.from_mapping,
                {**entry.to_dict(), "details": ["same", "same"]},
                "details entries must be unique",
            ),
            (
                WorkMemoryCheckpoint.from_mapping,
                {**memory.to_dict(), "summary": "  "},
                "summary must not be blank",
            ),
            (
                WorkAssignmentSpec.from_mapping,
                {**task.to_dict(), "scope_paths": ["../secrets"]},
                "safe relative POSIX path",
            ),
            (
                WorkAssignmentSpec.from_mapping,
                {**task.to_dict(), "schema_version": True},
                "schema_version must be 1",
            ),
        ):
            with (
                self.subTest(factory=factory.__qualname__),
                self.assertRaisesRegex(ValidationError, message),
            ):
                factory(value)

    def test_contracts_reject_bad_references_and_oversized_content(self) -> None:
        entry = log_entry().to_dict()
        with self.assertRaisesRegex(ValidationError, "interaction identifier"):
            WorkLogEntry.from_mapping(
                {**entry, "related_interaction_ids": ["int_not-valid"]}
            )
        with self.assertRaisesRegex(ValidationError, "content exceeds"):
            WorkLogEntry.from_mapping(
                {**entry, "details": ["x" * 2048 + str(index) for index in range(16)]}
            )


class WorkMemoryDatabaseTests(unittest.TestCase):
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
            context=ProjectContext.from_mapping({"purpose": "Coordinate agents"}),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "memory-test",
                    "source_updated_at": "2026-10-10T12:00:00Z",
                    "title": "Keep agent work memory",
                    "brief": "Coordinate several disposable workers.",
                }
            ),
            imported_by=self.user["id"],
        )
        for name in ("Coordinator", "Claude Worker", "Codex Worker"):
            self.database.create_agent_profile(
                name=name,
                spec=AgentProfileSpec.from_mapping(
                    {"image": "agent:local", "command": ["agent", "run"]}
                ),
                krab_identity_id=self.identity["id"],
                created_by=self.user["id"],
            )

    def create_run(
        self,
        profile: str,
        *,
        work_item_id: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, object]:
        return self.database.create_run(
            project_key="box-project",
            profile_name=profile,
            work_item_id=work_item_id or self.work_item["id"],
            requested_by=self.user["id"],
            source_revision="main",
            krab_session_id=session_id,
        )

    def running_run(
        self,
        profile: str,
        *,
        adapter: str = "docker",
        work_item_id: str | None = None,
        session_id: str | None = None,
    ) -> tuple[dict[str, object], dict[str, object]]:
        run = self.create_run(profile, work_item_id=work_item_id, session_id=session_id)
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter=adapter,
            owner_id="orchestrator-work-memory-test",
            expected_run_version=1,
        )
        running = self.database.start_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision="a" * 40,
            container_id="b" * 64,
            container_name="memory-test",
            image_id="sha256:" + "c" * 64,
        )
        self.database.record_attempt_result(
            running["id"],
            ResultManifest(1, running["id"], "a" * 40, ()),
        )
        return run, running

    def create_assignment(
        self,
        coordinator_id: str,
        target: str,
        *,
        key: str,
        title: str,
    ) -> dict[str, object]:
        memory = self.database.get_work_memory_for_run(coordinator_id)
        return self.database.create_work_assignment(
            coordinator_id,
            target_profile_name=target,
            spec=assignment(title),
            expected_thread_version=memory["version"],
            idempotency_key=key,
        )

    def test_runs_share_the_work_items_durable_memory_thread(self) -> None:
        coordinator = self.create_run("Coordinator")
        worker = self.create_run("Claude Worker")

        first = self.database.get_work_memory_for_run(coordinator["id"])
        second = self.database.get_work_memory_for_run(worker["id"])

        self.assertEqual(first["thread_id"], second["thread_id"])
        self.assertEqual(first["work_item_id"], self.work_item["id"])
        self.assertEqual(first["state"], "active")
        self.assertEqual(first["version"], 1)
        self.assertEqual(first["log_sequence"], 0)
        self.assertIsNone(first["checkpoint"])
        self.assertEqual(first["entries"], [])
        self.assertEqual(first["assignments"], [])
        counts = self.database.status()["counts"]
        self.assertEqual(counts["work_threads"], 1)

    def test_completion_seals_memory_and_reopen_starts_a_fresh_generation(
        self,
    ) -> None:
        coordinator, attempt = self.running_run("Coordinator")
        outcome = self.database.append_work_log(
            coordinator["id"],
            log_entry("The work is complete.", kind="outcome"),
            idempotency_key="outcome:complete",
        )
        before = self.database.get_work_memory_for_run(coordinator["id"])
        self.database.complete_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id=attempt["image_id"],
        )

        completed = self.database.complete_work_item(
            self.work_item["id"],
            completed_by=self.user["id"],
            expected_thread_version=before["version"],
            reason="Acceptance checks passed.",
        )

        self.assertEqual(completed["thread_id"], before["thread_id"])
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["version"], before["version"] + 1)
        self.assertEqual(completed["completed_at"], completed["updated_at"])
        self.assertEqual(
            completed["events"],
            [
                {
                    "sequence": 1,
                    "kind": "work_thread.created",
                    "from_state": None,
                    "to_state": "active",
                    "actor": "operator",
                    "metadata": {},
                    "created_at": completed["created_at"],
                },
                {
                    "sequence": 2,
                    "kind": "work_thread.completed",
                    "from_state": "active",
                    "to_state": "completed",
                    "actor": "operator",
                    "metadata": {
                        "outcome_entry_id": outcome["id"],
                        "reason": "Acceptance checks passed.",
                    },
                    "created_at": completed["completed_at"],
                },
            ],
        )
        self.assertEqual(
            self.database.get_work_memory_for_run(coordinator["id"])["state"],
            "completed",
        )
        with self.assertRaisesRegex(ConflictError, "work thread is not active"):
            self.database.append_work_log(
                coordinator["id"],
                log_entry(kind="progress"),
                idempotency_key="after:completion",
            )
        with self.assertRaisesRegex(ConflictError, "reopen it"):
            self.create_run("Coordinator")

        refreshed = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "memory-test",
                    "source_updated_at": "2026-10-11T12:00:00Z",
                    "title": "Keep agent work memory",
                    "brief": "Coordinate a fresh cycle of disposable workers.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.assertEqual(refreshed["import_status"], "updated")
        self.assertEqual(refreshed["state"], "completed")
        self.assertEqual(refreshed["work_thread"]["thread_id"], completed["thread_id"])

        reopened = self.database.reopen_work_item(
            self.work_item["id"],
            reopened_by=self.user["id"],
            expected_thread_version=completed["version"],
        )
        self.assertNotEqual(reopened["thread_id"], completed["thread_id"])
        self.assertEqual(reopened["generation"], 2)
        self.assertEqual(reopened["state"], "active")
        self.assertEqual(reopened["version"], 1)
        self.assertEqual(reopened["log_sequence"], 0)
        self.assertEqual(
            reopened["events"][0]["metadata"],
            {"previous_thread_id": completed["thread_id"]},
        )
        replacement = self.create_run("Coordinator")
        self.assertEqual(replacement["work_item_version"], 2)
        fresh = self.database.get_work_memory_for_run(replacement["id"])
        self.assertEqual(fresh["thread_id"], reopened["thread_id"])
        self.assertIsNone(fresh["checkpoint"])
        self.assertEqual(fresh["entries"], [])
        self.assertEqual(fresh["assignments"], [])

    def test_completion_requires_a_quiescent_thread_and_final_outcome(self) -> None:
        initial = self.database.get_work_item(self.work_item["id"])["work_thread"]
        with self.assertRaises(ConflictError) as missing_outcome:
            self.database.complete_work_item(
                self.work_item["id"],
                completed_by=self.user["id"],
                expected_thread_version=initial["version"],
            )
        self.assertEqual(
            str(missing_outcome.exception),
            "work item requires a final outcome entry",
        )

        coordinator, attempt = self.running_run("Coordinator")
        self.database.append_work_log(
            coordinator["id"],
            log_entry(kind="outcome"),
            idempotency_key="outcome:active-run",
        )
        active = self.database.get_work_memory_for_run(coordinator["id"])
        with self.assertRaises(ConflictError) as active_runs:
            self.database.complete_work_item(
                self.work_item["id"],
                completed_by=self.user["id"],
                expected_thread_version=active["version"],
            )
        self.assertEqual(str(active_runs.exception), "work item has active runs")

        task = self.create_assignment(
            coordinator["id"],
            "Codex Worker",
            key="assignment:unfinished",
            title="Unfinished review",
        )
        self.database.append_work_log(
            coordinator["id"],
            log_entry("Work is otherwise complete.", kind="outcome"),
            idempotency_key="outcome:unfinished-assignment",
        )
        ready = self.database.get_work_memory_for_run(coordinator["id"])
        self.database.complete_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id=attempt["image_id"],
        )
        with self.assertRaises(ConflictError) as unfinished_assignments:
            self.database.complete_work_item(
                self.work_item["id"],
                completed_by=self.user["id"],
                expected_thread_version=ready["version"],
            )
        self.assertEqual(
            str(unfinished_assignments.exception),
            "work item has unfinished assignments",
        )
        self.assertEqual(
            self.database.get_work_memory_for_run(coordinator["id"])["assignments"][0][
                "id"
            ],
            task["id"],
        )

    def test_completion_and_reopen_are_optimistic_and_audited(self) -> None:
        with self.assertRaises(ConcurrentUpdateError):
            self.database.complete_work_item(
                self.work_item["id"],
                completed_by=self.user["id"],
                expected_thread_version=99,
            )
        with self.assertRaises(ConflictError) as already_active:
            self.database.reopen_work_item(
                self.work_item["id"],
                reopened_by=self.user["id"],
                expected_thread_version=1,
            )
        self.assertEqual(str(already_active.exception), "work item is already active")

        coordinator, attempt = self.running_run("Coordinator")
        outcome = self.database.append_work_log(
            coordinator["id"],
            log_entry(kind="outcome"),
            idempotency_key="outcome:no-reason",
        )
        ready = self.database.get_work_memory_for_run(coordinator["id"])
        self.database.complete_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id=attempt["image_id"],
        )
        completed = self.database.complete_work_item(
            self.work_item["id"],
            completed_by=self.user["id"],
            expected_thread_version=ready["version"],
        )
        self.assertEqual(
            completed["events"][-1]["metadata"],
            {"outcome_entry_id": outcome["id"]},
        )
        with self.assertRaises(ConflictError) as already_completed:
            self.database.complete_work_item(
                self.work_item["id"],
                completed_by=self.user["id"],
                expected_thread_version=completed["version"],
            )
        self.assertEqual(
            str(already_completed.exception), "work item is already completed"
        )
        with self.assertRaises(ConcurrentUpdateError):
            self.database.reopen_work_item(
                self.work_item["id"],
                reopened_by=self.user["id"],
                expected_thread_version=99,
            )

        connection = self.database.connect()
        self.addCleanup(connection.close)
        event = connection.execute(
            "SELECT id FROM work_thread_events LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(event)
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE work_thread_events SET metadata_json = '{}' WHERE id = ?",
                (event["id"],),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "DELETE FROM work_threads WHERE id = ?",
                (self.work_item["work_thread"]["thread_id"],),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE work_threads SET version = version + 1 WHERE id = ?",
                (completed["thread_id"],),
            )

    def test_work_item_lifecycle_rejects_invalid_or_missing_authority(self) -> None:
        for method, arguments, message in (
            (
                self.database.complete_work_item,
                {
                    "completed_by": self.user["id"],
                    "expected_thread_version": 0,
                },
                "expected_thread_version must be a positive integer",
            ),
            (
                self.database.complete_work_item,
                {
                    "completed_by": self.user["id"],
                    "expected_thread_version": 1,
                    "reason": "x" * 4097,
                },
                "reason exceeds 4096 UTF-8 bytes",
            ),
            (
                self.database.reopen_work_item,
                {
                    "reopened_by": self.user["id"],
                    "expected_thread_version": True,
                },
                "expected_thread_version must be a positive integer",
            ),
        ):
            with (
                self.subTest(method=method.__name__, message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                method(self.work_item["id"], **arguments)

        missing_user = "boxu_" + "f" * 32
        with self.assertRaises(NotFoundError) as missing_completion_actor:
            self.database.complete_work_item(
                self.work_item["id"],
                completed_by=missing_user,
                expected_thread_version=1,
            )
        self.assertEqual(
            str(missing_completion_actor.exception),
            f"active local user not found: {missing_user}",
        )
        missing_item = "wki_" + "f" * 32
        with self.assertRaises(NotFoundError) as missing_completion_item:
            self.database.complete_work_item(
                missing_item,
                completed_by=self.user["id"],
                expected_thread_version=1,
            )
        self.assertEqual(
            str(missing_completion_item.exception),
            f"work item not found: {missing_item}",
        )
        with self.assertRaises(NotFoundError) as missing_reopen_actor:
            self.database.reopen_work_item(
                self.work_item["id"],
                reopened_by=missing_user,
                expected_thread_version=1,
            )
        self.assertEqual(
            str(missing_reopen_actor.exception),
            f"active local user not found: {missing_user}",
        )
        with self.assertRaises(NotFoundError) as missing_reopen_item:
            self.database.reopen_work_item(
                missing_item,
                reopened_by=self.user["id"],
                expected_thread_version=1,
            )
        self.assertEqual(
            str(missing_reopen_item.exception),
            f"work item not found: {missing_item}",
        )

    def test_concurrent_completion_and_reopen_have_single_winners(self) -> None:
        coordinator, attempt = self.running_run("Coordinator")
        self.database.append_work_log(
            coordinator["id"],
            log_entry(kind="outcome"),
            idempotency_key="outcome:concurrent",
        )
        ready = self.database.get_work_memory_for_run(coordinator["id"])
        self.database.complete_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id=attempt["image_id"],
        )

        def complete() -> str:
            try:
                return self.database.complete_work_item(
                    self.work_item["id"],
                    completed_by=self.user["id"],
                    expected_thread_version=ready["version"],
                )["state"]
            except ConcurrentUpdateError:
                return "stale"

        with ThreadPoolExecutor(max_workers=2) as pool:
            completion_results = list(pool.map(lambda _: complete(), range(2)))
        self.assertCountEqual(completion_results, ["completed", "stale"])
        completed = self.database.get_work_item(self.work_item["id"])["work_thread"]
        self.assertEqual(
            [event["kind"] for event in completed["events"]],
            ["work_thread.created", "work_thread.completed"],
        )

        def reopen() -> str:
            try:
                return self.database.reopen_work_item(
                    self.work_item["id"],
                    reopened_by=self.user["id"],
                    expected_thread_version=completed["version"],
                )["state"]
            except ConcurrentUpdateError:
                return "stale"

        with ThreadPoolExecutor(max_workers=2) as pool:
            reopen_results = list(pool.map(lambda _: reopen(), range(2)))
        self.assertCountEqual(reopen_results, ["active", "stale"])
        current = self.database.get_work_item(self.work_item["id"])["work_thread"]
        self.assertEqual(current["generation"], 2)
        self.assertEqual(current["events"][0]["actor"], "operator")

    def test_work_item_lifecycle_fails_closed_on_corrupt_or_missing_state(self) -> None:
        connection = self.database.connect()
        self.addCleanup(connection.close)
        thread_id = self.work_item["work_thread"]["thread_id"]
        connection.execute("DROP TRIGGER work_thread_event_immutable_update")
        connection.execute(
            "UPDATE work_thread_events SET metadata_json = '{' "
            "WHERE work_thread_id = ?",
            (thread_id,),
        )
        with self.assertRaises(StorageError) as invalid_event:
            self.database.get_work_item(self.work_item["id"])
        self.assertEqual(
            str(invalid_event.exception), "stored work-thread event is invalid"
        )
        missing_thread = "wth_" + "f" * 32
        with self.assertRaises(NotFoundError) as not_found:
            Database._work_thread_record(connection, missing_thread)
        self.assertEqual(
            str(not_found.exception), f"work thread not found: {missing_thread}"
        )

    def test_work_writes_require_a_running_attempt_for_provenance(self) -> None:
        queued = self.create_run("Coordinator")

        with self.assertRaises(ConflictError) as missing_attempt:
            self.database.append_work_log(
                queued["id"], log_entry(), idempotency_key="entry:no-attempt"
            )

        self.assertEqual(
            str(missing_attempt.exception), "run has no active running attempt"
        )

    def test_work_log_interaction_references_stay_within_the_thread(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        same_thread_run, _ = self.running_run(
            "Coordinator", session_id="ses_" + "3" * 32
        )
        same_thread_interaction = self.database.open_interaction(
            same_thread_run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "4" * 32,
            expected_run_version=self.database.get_run(same_thread_run["id"])[
                "version"
            ],
        )
        related_entry = WorkLogEntry.from_mapping(
            {
                **log_entry("The reviewer question is relevant.").to_dict(),
                "related_interaction_ids": [same_thread_interaction["id"]],
            }
        )
        stored = self.database.append_work_log(
            coordinator["id"],
            related_entry,
            idempotency_key="entry:same-thread-interaction",
        )
        self.assertEqual(
            stored["related_interaction_ids"], [same_thread_interaction["id"]]
        )

        other_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "other-memory-test",
                    "source_updated_at": "2026-10-10T12:00:00Z",
                    "title": "Other work item",
                    "brief": "Keep interaction references isolated.",
                }
            ),
            imported_by=self.user["id"],
        )
        other_run, _ = self.running_run(
            "Coordinator",
            work_item_id=other_item["id"],
            session_id="ses_" + "5" * 32,
        )
        other_interaction = self.database.open_interaction(
            other_run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "6" * 32,
            expected_run_version=self.database.get_run(other_run["id"])["version"],
        )
        cross_thread_entry = WorkLogEntry.from_mapping(
            {
                **log_entry("This reference must be rejected.").to_dict(),
                "related_interaction_ids": [other_interaction["id"]],
            }
        )
        with self.assertRaises(ConflictError) as cross_thread:
            self.database.append_work_log(
                coordinator["id"],
                cross_thread_entry,
                idempotency_key="entry:cross-thread-interaction",
            )
        self.assertEqual(
            str(cross_thread.exception),
            "related interaction does not belong to work thread",
        )

    def test_parallel_workers_survive_a_coordinator_replacement(self) -> None:
        coordinator, _ = self.running_run("Coordinator", adapter="claude")
        plan = self.database.append_work_log(
            coordinator["id"],
            log_entry("Split the review into two assignments", kind="plan"),
            idempotency_key="plan:0001",
        )
        before_checkpoint = self.database.get_work_memory_for_run(coordinator["id"])
        saved = self.database.create_work_memory_checkpoint(
            coordinator["id"],
            checkpoint(),
            through_sequence=plan["sequence"],
            expected_thread_version=before_checkpoint["version"],
            idempotency_key="checkpoint:0001",
        )
        claude_task = self.create_assignment(
            coordinator["id"], "Claude Worker", key="assign:claude", title="Review DB"
        )
        codex_task = self.create_assignment(
            coordinator["id"], "Codex Worker", key="assign:codex", title="Review API"
        )
        claude, _ = self.running_run("Claude Worker", adapter="claude")
        codex, _ = self.running_run("Codex Worker", adapter="codex")
        claude_claim = self.database.claim_work_assignment(
            claude["id"],
            claude_task["id"],
            expected_assignment_version=1,
            lease_seconds=600,
        )
        codex_claim = self.database.claim_work_assignment(
            codex["id"],
            codex_task["id"],
            expected_assignment_version=1,
            lease_seconds=600,
        )

        def finish(
            run_id: str, assignment_id: str, version: int, summary: str, key: str
        ) -> dict[str, object]:
            return self.database.complete_work_assignment(
                run_id,
                assignment_id,
                expected_assignment_version=version,
                result=log_entry(summary, kind="assignment_result"),
                idempotency_key=key,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda values: finish(*values),
                    (
                        (
                            claude["id"],
                            claude_task["id"],
                            claude_claim["version"],
                            "Claude verified the persistence boundary.",
                            "result:claude",
                        ),
                        (
                            codex["id"],
                            codex_task["id"],
                            codex_claim["version"],
                            "Codex verified the gateway boundary.",
                            "result:codex",
                        ),
                    ),
                )
            )

        replacement = self.create_run("Coordinator")
        memory = self.database.get_work_memory_for_run(replacement["id"])

        self.assertEqual(saved["through_sequence"], 1)
        self.assertEqual(memory["checkpoint"]["id"], saved["id"])
        self.assertEqual({entry["sequence"] for entry in memory["entries"]}, {2, 3})
        self.assertEqual(
            {entry["author"]["profile_name"] for entry in memory["entries"]},
            {"Claude Worker", "Codex Worker"},
        )
        self.assertEqual(
            {entry["author"]["runtime_adapter"] for entry in memory["entries"]},
            {"claude", "codex"},
        )
        self.assertEqual({item["state"] for item in results}, {"completed"})
        self.assertEqual(
            {item["state"] for item in memory["assignments"]}, {"completed"}
        )

    def test_log_retries_are_idempotent_and_changed_retries_conflict(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        first = self.database.append_work_log(
            coordinator["id"], log_entry(), idempotency_key="entry:0001"
        )
        repeated = self.database.append_work_log(
            coordinator["id"], log_entry(), idempotency_key="entry:0001"
        )
        with self.assertRaises(ConflictError) as changed_retry:
            self.database.append_work_log(
                coordinator["id"],
                log_entry("Changed retry"),
                idempotency_key="entry:0001",
            )

        self.assertEqual(
            str(changed_retry.exception),
            "idempotency key already names a different work-log entry",
        )
        self.assertEqual(repeated, first)
        self.assertEqual(
            self.database.get_work_memory_for_run(coordinator["id"])["log_sequence"],
            1,
        )

    def test_work_log_pages_from_a_stable_sequence_cursor(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        for index in range(18):
            self.database.append_work_log(
                coordinator["id"],
                log_entry(f"Finding {index}"),
                idempotency_key=f"entry:{index:04d}",
            )

        first = self.database.get_work_memory_for_run(coordinator["id"])
        second = self.database.get_work_memory_for_run(
            coordinator["id"], after_sequence=first["next_after_sequence"]
        )

        self.assertEqual(
            [entry["sequence"] for entry in first["entries"]], list(range(1, 17))
        )
        self.assertTrue(first["entries_truncated"])
        self.assertEqual(first["next_after_sequence"], 16)
        self.assertEqual([entry["sequence"] for entry in second["entries"]], [17, 18])
        self.assertFalse(second["entries_truncated"])
        self.assertIsNone(second["next_after_sequence"])

        for kwargs, message in (
            ({"after_sequence": -1}, "after_sequence"),
            ({"after_sequence": True}, "after_sequence"),
            ({"limit": 0}, "limit"),
            ({"limit": True}, "limit"),
        ):
            with self.assertRaisesRegex(ValidationError, message):
                self.database.get_work_memory_for_run(coordinator["id"], **kwargs)

    def test_checkpoint_rejects_a_stale_or_incomplete_view(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        initial = self.database.get_work_memory_for_run(coordinator["id"])
        self.database.append_work_log(
            coordinator["id"], log_entry(), idempotency_key="entry:0001"
        )
        current = self.database.get_work_memory_for_run(coordinator["id"])

        with self.assertRaisesRegex(ConflictError, "thread version"):
            self.database.create_work_memory_checkpoint(
                coordinator["id"],
                checkpoint(),
                through_sequence=1,
                expected_thread_version=initial["version"],
                idempotency_key="checkpoint:stale",
            )
        with self.assertRaisesRegex(ConflictError, "latest log sequence"):
            self.database.create_work_memory_checkpoint(
                coordinator["id"],
                checkpoint(),
                through_sequence=0,
                expected_thread_version=current["version"],
                idempotency_key="checkpoint:incomplete",
            )

    def test_assignment_enforces_profile_lease_and_provenance(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        task = self.create_assignment(
            coordinator["id"], "Claude Worker", key="assign:one", title="Review"
        )
        self.assertEqual(
            task["target"],
            {
                "profile_name": "Claude Worker",
                "profile_version_id": task["target"]["profile_version_id"],
                "profile_version": 1,
            },
        )
        self.assertTrue(task["target"]["profile_version_id"].startswith("apv_"))
        wrong_worker, _ = self.running_run("Codex Worker")
        with self.assertRaisesRegex(ConflictError, "target profile version"):
            self.database.claim_work_assignment(
                wrong_worker["id"],
                task["id"],
                expected_assignment_version=1,
                lease_seconds=600,
            )

        first_worker, _ = self.running_run("Claude Worker")
        claimed = self.database.claim_work_assignment(
            first_worker["id"],
            task["id"],
            expected_assignment_version=1,
            lease_seconds=600,
        )
        competing_worker, _ = self.running_run("Claude Worker")
        with self.assertRaisesRegex(ConflictError, "claimed by another run"):
            self.database.claim_work_assignment(
                competing_worker["id"],
                task["id"],
                expected_assignment_version=claimed["version"],
                lease_seconds=600,
            )
        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE work_assignments SET lease_expires_at = ? WHERE id = ?",
                ("2000-01-01T00:00:00.000000+00:00", task["id"]),
            )
        replacement, _ = self.running_run("Claude Worker")
        reclaimed = self.database.claim_work_assignment(
            replacement["id"],
            task["id"],
            expected_assignment_version=claimed["version"],
            lease_seconds=600,
        )

        self.assertEqual(reclaimed["claimed_by_run_id"], replacement["id"])
        self.assertEqual(
            [event["kind"] for event in reclaimed["events"]],
            ["assignment.created", "assignment.claimed", "assignment.reclaimed"],
        )

    def test_assignment_retry_survives_a_target_profile_revision(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        old_worker, _ = self.running_run("Claude Worker")
        memory = self.database.get_work_memory_for_run(coordinator["id"])
        first = self.database.create_work_assignment(
            coordinator["id"],
            target_profile_name="Claude Worker",
            spec=assignment(),
            expected_thread_version=memory["version"],
            idempotency_key="assign:stable",
        )
        self.database.add_agent_profile_version(
            name="Claude Worker",
            spec=AgentProfileSpec.from_mapping(
                {"image": "agent:revision-two", "command": ["agent", "run"]}
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )

        repeated = self.database.create_work_assignment(
            coordinator["id"],
            target_profile_name="Claude Worker",
            spec=assignment(),
            expected_thread_version=memory["version"],
            idempotency_key="assign:stable",
        )
        new_worker, _ = self.running_run("Claude Worker")

        self.assertEqual(repeated, first)
        with self.assertRaisesRegex(ConflictError, "target profile version"):
            self.database.claim_work_assignment(
                new_worker["id"],
                first["id"],
                expected_assignment_version=1,
                lease_seconds=600,
            )
        claimed = self.database.claim_work_assignment(
            old_worker["id"],
            first["id"],
            expected_assignment_version=1,
            lease_seconds=600,
        )
        self.assertEqual(claimed["claimed_by_run_id"], old_worker["id"])

    def test_assignment_history_is_bounded_for_gateway_safe_reads(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        task = self.create_assignment(
            coordinator["id"], "Claude Worker", key="assign:history", title="Review"
        )
        first_worker, _ = self.running_run("Claude Worker")
        second_worker, _ = self.running_run("Claude Worker")
        current = self.database.claim_work_assignment(
            first_worker["id"],
            task["id"],
            expected_assignment_version=task["version"],
            lease_seconds=600,
        )
        for index in range(17):
            with closing(self.database.connect()) as connection:
                connection.execute(
                    "UPDATE work_assignments SET lease_expires_at = ? WHERE id = ?",
                    ("2000-01-01T00:00:00.000000+00:00", task["id"]),
                )
            claimant = second_worker if index % 2 == 0 else first_worker
            current = self.database.claim_work_assignment(
                claimant["id"],
                task["id"],
                expected_assignment_version=current["version"],
                lease_seconds=600,
            )

        stored = self.database.get_work_memory_for_run(coordinator["id"])[
            "assignments"
        ][0]
        self.assertEqual(stored["event_count"], 19)
        self.assertTrue(stored["events_truncated"])
        self.assertEqual(len(stored["events"]), 16)
        self.assertEqual(
            [event["sequence"] for event in stored["events"]], list(range(4, 20))
        )

    def test_work_thread_has_a_bounded_assignment_set(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        for index in range(16):
            created = self.create_assignment(
                coordinator["id"],
                "Codex Worker",
                key=f"assign:{index:04d}",
                title=f"Review {index}",
            )
            self.assertEqual(created["sequence"], index + 1)

        with self.assertRaisesRegex(ConflictError, "assignment limit"):
            self.create_assignment(
                coordinator["id"],
                "Codex Worker",
                key="assign:overflow",
                title="Too many",
            )

    def test_control_plane_exposes_the_complete_coordination_boundary(self) -> None:
        control = ControlPlane(self.database)
        coordinator, _ = self.running_run("Coordinator")
        entry = control.append_work_log(
            coordinator["id"], log_entry(kind="plan"), idempotency_key="plan:service"
        )
        memory = control.read_work_memory(coordinator["id"])
        saved = control.checkpoint_work_memory(
            coordinator["id"],
            checkpoint(),
            through_sequence=entry["sequence"],
            expected_thread_version=memory["version"],
            idempotency_key="checkpoint:service",
        )
        memory = control.read_work_memory(coordinator["id"])
        task = control.create_work_assignment(
            coordinator["id"],
            target_profile_name="Codex Worker",
            spec=assignment(),
            expected_thread_version=memory["version"],
            idempotency_key="assignment:service",
        )
        worker, _ = self.running_run("Codex Worker")
        claimed = control.claim_work_assignment(
            worker["id"],
            task["id"],
            expected_assignment_version=task["version"],
            lease_seconds=600,
        )
        completed = control.complete_work_assignment(
            worker["id"],
            task["id"],
            expected_assignment_version=claimed["version"],
            result=log_entry(kind="assignment_result"),
            idempotency_key="result:service",
        )

        self.assertEqual(saved["through_sequence"], 1)
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(
            control.read_work_memory(coordinator["id"])["entries"][0]["sequence"],
            2,
        )
        with self.assertRaises(ConflictError) as terminal_claim:
            control.claim_work_assignment(
                worker["id"],
                task["id"],
                expected_assignment_version=completed["version"],
                lease_seconds=600,
            )
        self.assertEqual(
            str(terminal_claim.exception),
            "work assignment cannot be claimed: completed",
        )

    def test_work_log_rejects_unvalidated_content_and_unknown_assignments(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        with self.assertRaises(ValidationError) as invalid_entry:
            self.database.append_work_log(
                coordinator["id"],
                {},  # type: ignore[arg-type]
                idempotency_key="entry:invalid",
            )
        self.assertEqual(
            str(invalid_entry.exception),
            "entry must be a validated WorkLogEntry",
        )

        missing_assignment_id = "was_" + "f" * 32
        with self.assertRaises(NotFoundError) as missing_assignment:
            self.database.append_work_log(
                coordinator["id"],
                log_entry(),
                assignment_id=missing_assignment_id,
                idempotency_key="entry:missing-assignment",
            )
        self.assertEqual(
            str(missing_assignment.exception),
            f"work assignment not found: {missing_assignment_id}",
        )

    def test_assignment_linked_logs_require_the_exact_live_lease(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        task = self.create_assignment(
            coordinator["id"],
            "Claude Worker",
            key="assign:linked-log",
            title="Write linked findings",
        )
        first_worker, first_attempt = self.running_run("Claude Worker")
        second_worker, second_attempt = self.running_run("Claude Worker")
        self.database.claim_work_assignment(
            first_worker["id"],
            task["id"],
            expected_assignment_version=task["version"],
            lease_seconds=600,
        )

        with self.assertRaisesRegex(ConflictError, "claimed by another run"):
            self.database.append_work_log(
                second_worker["id"],
                log_entry("Wrong run cannot append."),
                assignment_id=task["id"],
                idempotency_key="entry:wrong-run",
            )

        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE work_assignments SET claimed_by_run_id = ? WHERE id = ?",
                (second_worker["id"], task["id"]),
            )
        with self.assertRaisesRegex(ConflictError, "claimed by another run"):
            self.database.append_work_log(
                second_worker["id"],
                log_entry("Wrong attempt cannot append."),
                assignment_id=task["id"],
                idempotency_key="entry:wrong-attempt",
            )

        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE work_assignments SET claimed_by_attempt_id = ?, "
                "lease_expires_at = ? WHERE id = ?",
                (
                    second_attempt["id"],
                    "2000-01-01T00:00:00.000000+00:00",
                    task["id"],
                ),
            )
        with self.assertRaisesRegex(ConflictError, "lease has expired"):
            self.database.append_work_log(
                second_worker["id"],
                log_entry("Expired worker cannot append."),
                assignment_id=task["id"],
                idempotency_key="entry:expired",
            )

        other_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "foreign-assignment-thread",
                    "source_updated_at": "2026-10-10T12:00:00Z",
                    "title": "Foreign thread",
                    "brief": "Keep assignment provenance thread-scoped.",
                }
            ),
            imported_by=self.user["id"],
        )
        foreign_coordinator, _ = self.running_run(
            "Coordinator", work_item_id=other_item["id"]
        )
        foreign_task = self.create_assignment(
            foreign_coordinator["id"],
            "Claude Worker",
            key="assign:foreign-thread",
            title="Foreign task",
        )
        with self.assertRaisesRegex(ConflictError, "another thread"):
            self.database.append_work_log(
                first_worker["id"],
                log_entry("Foreign assignment cannot be linked."),
                assignment_id=foreign_task["id"],
                idempotency_key="entry:foreign-thread",
            )

        self.assertNotEqual(first_attempt["id"], second_attempt["id"])

    def test_assignment_completion_retry_is_exactly_idempotent(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        task = self.create_assignment(
            coordinator["id"],
            "Claude Worker",
            key="assign:completion-retry",
            title="Return one result",
        )
        worker, _ = self.running_run("Claude Worker")
        claimed = self.database.claim_work_assignment(
            worker["id"],
            task["id"],
            expected_assignment_version=task["version"],
            lease_seconds=600,
        )
        result = log_entry("The review is complete.", kind="assignment_result")
        first = self.database.complete_work_assignment(
            worker["id"],
            task["id"],
            expected_assignment_version=claimed["version"],
            result=result,
            idempotency_key="result:exact-retry",
        )
        repeated = self.database.complete_work_assignment(
            worker["id"],
            task["id"],
            expected_assignment_version=claimed["version"],
            result=result,
            idempotency_key="result:exact-retry",
        )

        self.assertEqual(repeated, first)
        memory = self.database.get_work_memory_for_run(coordinator["id"])
        self.assertEqual(memory["log_sequence"], 1)
        self.assertEqual(memory["assignments"][0]["state"], "completed")

    def test_assignment_result_must_come_from_the_current_lease_holder(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        task = self.create_assignment(
            coordinator["id"], "Claude Worker", key="assign:one", title="Review"
        )
        claude, _ = self.running_run("Claude Worker")
        claim = self.database.claim_work_assignment(
            claude["id"],
            task["id"],
            expected_assignment_version=1,
            lease_seconds=600,
        )
        codex, _ = self.running_run("Codex Worker")
        with self.assertRaisesRegex(ConflictError, "claimed by another run"):
            self.database.complete_work_assignment(
                codex["id"],
                task["id"],
                expected_assignment_version=claim["version"],
                result=log_entry(kind="assignment_result"),
                idempotency_key="result:wrong",
            )
        with self.assertRaisesRegex(ValidationError, "assignment_result"):
            self.database.complete_work_assignment(
                claude["id"],
                task["id"],
                expected_assignment_version=claim["version"],
                result=log_entry(kind="progress"),
                idempotency_key="result:wrong-kind",
            )

    def test_work_memory_records_are_immutable(self) -> None:
        coordinator, _ = self.running_run("Coordinator")
        entry = self.database.append_work_log(
            coordinator["id"], log_entry(), idempotency_key="entry:0001"
        )
        view = self.database.get_work_memory_for_run(coordinator["id"])
        saved = self.database.create_work_memory_checkpoint(
            coordinator["id"],
            checkpoint(),
            through_sequence=1,
            expected_thread_version=view["version"],
            idempotency_key="checkpoint:0001",
        )
        connection = self.database.connect()
        self.addCleanup(connection.close)
        for statement, values in (
            (
                "UPDATE work_log_entries SET content_json = '{}' WHERE id = ?",
                (entry["id"],),
            ),
            ("DELETE FROM work_memory_checkpoints WHERE id = ?", (saved["id"],)),
        ):
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(statement, values)


class WorkMemoryMigrationTests(unittest.TestCase):
    def test_schema_five_backfills_threads_and_existing_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-five.db"
            connection = sqlite3.connect(path)
            connection.executescript(
                SCHEMA_V1
                + INTERACTION_SCHEMA
                + WORK_ITEM_SCHEMA
                + PROVIDER_SCHEMA
                + ATTEMPT_SCHEMA
            )
            user_id = "boxu_" + "1" * 32
            instance_id = "ksi_" + "2" * 32
            identity_id = "kid_" + "3" * 32
            project_id = "boxp_" + "4" * 32
            context_id = "ctx_" + "5" * 32
            profile_id = "agp_" + "6" * 32
            profile_version_id = "apv_" + "7" * 32
            item_id = "wki_" + "8" * 32
            item_version_id = "wiv_" + "9" * 32
            run_id = "run_" + "a" * 32
            context = ProjectContext.from_mapping({"purpose": "Existing work"})
            profile = AgentProfileSpec.from_mapping(
                {"image": "agent:local", "command": ["agent"]}
            )
            item = WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "existing",
                    "source_updated_at": "2026-10-10T12:00:00Z",
                    "title": "Existing item",
                    "brief": "Preserve this item during migration.",
                }
            )
            connection.execute(
                "INSERT INTO users VALUES "
                "(?, 'operator', 'Operator', 1, 'then', 'then')",
                (user_id,),
            )
            connection.execute(
                "INSERT INTO krab_instances VALUES (?, 'Krab', "
                "'http://127.0.0.1:8765', 'then', 'then')",
                (instance_id,),
            )
            connection.execute(
                "INSERT INTO krab_identities VALUES "
                "(?, ?, ?, 'agent_one', 1, 'file:/private/token', "
                "'then', 'then', 'then')",
                (identity_id, instance_id, "usr_" + "b" * 32),
            )
            connection.execute(
                "INSERT INTO projects VALUES "
                "(?, ?, ?, 'box-project', 'Box', 1, 'then', 'then')",
                (project_id, instance_id, "prj_" + "c" * 32),
            )
            connection.execute(
                "INSERT INTO project_context_versions VALUES "
                "(?, ?, 1, ?, ?, ?, 'then')",
                (context_id, project_id, context.canonical, context.digest, user_id),
            )
            connection.execute(
                "INSERT INTO agent_profiles VALUES (?, 'Worker', 1, ?, 'then', 'then')",
                (profile_id, user_id),
            )
            connection.execute(
                "INSERT INTO agent_profile_versions VALUES "
                "(?, ?, 1, ?, ?, ?, ?, 'then')",
                (
                    profile_version_id,
                    profile_id,
                    profile.canonical,
                    profile.digest,
                    identity_id,
                    user_id,
                ),
            )
            connection.execute(
                "INSERT INTO work_items VALUES "
                "(?, ?, 'manual', 'existing', ?, ?, 'then', 'then')",
                (item_id, project_id, item.source_updated_at, user_id),
            )
            connection.execute(
                "INSERT INTO work_item_versions VALUES (?, ?, 1, ?, ?, ?, ?, 'then')",
                (
                    item_version_id,
                    item_id,
                    item.canonical,
                    item.digest,
                    item.source_updated_at,
                    user_id,
                ),
            )
            connection.execute(
                "INSERT INTO runs "
                "(id, project_id, context_version_id, profile_version_id, "
                "requested_by, state, version, created_at, updated_at, "
                "work_item_version_id) VALUES (?, ?, ?, ?, ?, 'queued', 1, "
                "'then', 'then', ?)",
                (
                    run_id,
                    project_id,
                    context_id,
                    profile_version_id,
                    user_id,
                    item_version_id,
                ),
            )
            connection.execute("PRAGMA user_version = 5")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], 12)
            self.assertEqual(second, first)
            self.assertEqual(first["counts"]["work_threads"], 1)
            self.assertEqual(first["counts"]["work_thread_events"], 1)
            with closing(database.connect()) as migrated:
                row = migrated.execute(
                    "SELECT r.work_thread_id, wt.work_item_id, e.kind, "
                    "e.actor_id FROM runs r JOIN work_threads wt "
                    "ON wt.id = r.work_thread_id JOIN work_thread_events e "
                    "ON e.work_thread_id = wt.id "
                    "WHERE r.id = ?",
                    (run_id,),
                ).fetchone()
            self.assertEqual(row["work_item_id"], item_id)
            self.assertTrue(row["work_thread_id"].startswith("wth_"))
            self.assertEqual(row["kind"], "work_thread.created")
            self.assertEqual(row["actor_id"], user_id)


if __name__ == "__main__":
    unittest.main()
