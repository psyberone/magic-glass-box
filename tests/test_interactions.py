from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from magic_glass_box.db import (
    SCHEMA_V1,
    SCHEMA_VERSION,
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
    RunState,
    ValidationError,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity


class InteractionDatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Database(Path(self.temporary.name) / "control-plane.db")
        self.database.initialize()
        self.operator = self.database.create_user("operator", "Local Operator")
        self.reviewer = self.database.create_user("reviewer", "Human Reviewer")
        self.instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.agent_identity = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + "1" * 32, "agent_one", 1),
            "file:/private/agent-token",
        )
        self.reviewer_identity = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + "2" * 32, "reviewer", 1),
            "file:/private/reviewer-token",
            user_id=self.reviewer["id"],
        )
        context = ProjectContext.from_mapping(
            {"purpose": "Prove that work outlives workers"}
        )
        self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=context,
            created_by=self.operator["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "interaction-test-item",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Exercise durable interactions",
                    "brief": "Provide a pinned assignment for interaction tests.",
                }
            ),
            imported_by=self.operator["id"],
        )
        self.database.create_agent_profile(
            name="Disposable Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc",
                command=["agent"],
                network_mode="none",
            ),
            krab_identity_id=self.agent_identity["id"],
            created_by=self.operator["id"],
        )

    def running_run(self, *, with_session: bool = True) -> dict[str, object]:
        run = self.database.create_run(
            project_key="box-project",
            profile_name="Disposable Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.operator["id"],
            krab_session_id=("ses_" + "4" * 32) if with_session else None,
        )
        self.database.transition_run(
            run["id"], target=RunState.PREPARING, expected_version=1
        )
        return self.database.transition_run(
            run["id"], target=RunState.RUNNING, expected_version=2
        )

    def open_interaction(self, run: dict[str, object]) -> dict[str, object]:
        return self.database.open_interaction(
            run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.reviewer_identity["id"],
            request_item_id="itm_" + "5" * 32,
            expected_run_version=run["version"],
        )

    def test_question_and_answer_are_durable_metadata_without_content(self) -> None:
        run = self.running_run()
        interaction = self.open_interaction(run)

        self.assertTrue(interaction["id"].startswith("int_"))
        self.assertEqual(interaction["kind"], "question")
        self.assertEqual(interaction["state"], "open")
        self.assertEqual(interaction["version"], 1)
        self.assertEqual(interaction["run_state"], "waiting_for_input")
        self.assertEqual(interaction["run_version"], 4)
        self.assertEqual(interaction["project_key"], "box-project")
        self.assertEqual(interaction["krab_session_id"], "ses_" + "4" * 32)
        self.assertEqual(interaction["request_item_id"], "itm_" + "5" * 32)
        self.assertIsNone(interaction["response_item_id"])
        self.assertIsNone(interaction["resolved_at"])
        self.assertEqual(
            interaction["recipient"],
            {
                "local_user_id": self.reviewer["id"],
                "local_user_handle": "reviewer",
                "krab_identity_id": self.reviewer_identity["id"],
                "krab_user_id": "usr_" + "2" * 32,
                "krab_handle": "reviewer",
            },
        )
        self.assertEqual(
            [
                {
                    key: event[key]
                    for key in (
                        "sequence",
                        "kind",
                        "from_state",
                        "to_state",
                        "krab_item_id",
                    )
                }
                for event in interaction["events"]
            ],
            [
                {
                    "sequence": 1,
                    "kind": "interaction.opened",
                    "from_state": None,
                    "to_state": "open",
                    "krab_item_id": "itm_" + "5" * 32,
                }
            ],
        )

        encoded = json.dumps(interaction, sort_keys=True)
        for forbidden in ("title", "body", "prompt", "answer", "transcript"):
            self.assertNotIn(f'"{forbidden}":', encoded)

        visible_run = self.database.get_run(run["id"])
        self.assertEqual(
            [item["id"] for item in visible_run["interactions"]],
            [interaction["id"]],
        )
        self.assertEqual(
            {
                key: visible_run["events"][-1][key]
                for key in ("sequence", "kind", "from_state", "to_state", "metadata")
            },
            {
                "sequence": 4,
                "kind": "run.transitioned",
                "from_state": "running",
                "to_state": "waiting_for_input",
                "metadata": {
                    "interaction_id": interaction["id"],
                    "interaction_kind": "question",
                    "recipient_krab_user_id": "usr_" + "2" * 32,
                    "request_item_id": "itm_" + "5" * 32,
                },
            },
        )
        self.assertNotIn('"body":', json.dumps(visible_run, sort_keys=True))

        answered = self.database.answer_interaction(
            interaction["id"],
            response_item_id="itm_" + "6" * 32,
            expected_version=1,
        )
        self.assertEqual(answered["state"], "answered")
        self.assertEqual(answered["version"], 2)
        self.assertEqual(answered["response_item_id"], "itm_" + "6" * 32)
        self.assertIsNotNone(answered["resolved_at"])
        self.assertEqual(answered["run_state"], "queued")
        self.assertEqual(answered["run_version"], 5)
        self.assertEqual(
            [event["kind"] for event in answered["events"]],
            ["interaction.opened", "interaction.answered"],
        )
        self.assertEqual(
            {
                key: self.database.get_run(run["id"])["events"][-1][key]
                for key in ("sequence", "kind", "from_state", "to_state", "metadata")
            },
            {
                "sequence": 5,
                "kind": "run.transitioned",
                "from_state": "waiting_for_input",
                "to_state": "queued",
                "metadata": {
                    "interaction_id": interaction["id"],
                    "response_item_id": "itm_" + "6" * 32,
                },
            },
        )
        self.assertEqual(
            [item["id"] for item in self.database.list_run_interactions(run["id"])],
            [interaction["id"]],
        )

    def test_inactive_bound_user_cannot_win_answer_race_at_storage_boundary(
        self,
    ) -> None:
        run = self.running_run()
        interaction = self.open_interaction(run)
        with self.database.connect() as connection:
            connection.execute(
                "UPDATE users SET active = 0 WHERE id = ?", (self.reviewer["id"],)
            )

        with self.assertRaisesRegex(ConflictError, "inactive"):
            self.database.answer_interaction(
                interaction["id"],
                response_item_id="itm_" + "6" * 32,
                expected_version=interaction["version"],
            )

        current = self.database.get_interaction(interaction["id"])
        self.assertEqual((current["state"], current["version"]), ("open", 1))

    def test_open_requires_current_running_run_session_and_bound_recipient(
        self,
    ) -> None:
        running = self.running_run()
        queued = self.database.create_run(
            project_key="box-project",
            profile_name="Disposable Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.operator["id"],
            krab_session_id="ses_" + "7" * 32,
        )
        no_session = self.running_run(with_session=False)
        unbound = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + "8" * 32, "unbound_user", 1),
            "file:/private/unbound-token",
        )
        other_instance = self.database.create_krab_instance(
            "Other Krab", "http://127.0.0.1:9876"
        )
        other_identity = self.database.store_krab_identity(
            other_instance["id"],
            KrabIdentity("usr_" + "9" * 32, "reviewer_other", 1),
            "file:/private/reviewer-other-token",
            user_id=self.reviewer["id"],
        )

        cases = (
            (
                queued,
                self.reviewer_identity["id"],
                queued["version"],
                "run is not running",
                ConflictError,
            ),
            (
                no_session,
                self.reviewer_identity["id"],
                no_session["version"],
                "Krab session",
                ConflictError,
            ),
            (
                running,
                self.reviewer_identity["id"],
                2,
                "run version",
                ConcurrentUpdateError,
            ),
            (
                running,
                unbound["id"],
                running["version"],
                "bound local user",
                NotFoundError,
            ),
            (
                running,
                other_identity["id"],
                running["version"],
                "Krab instance",
                ConflictError,
            ),
        )
        for candidate, identity_id, version, message, error_type in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(error_type, message),
            ):
                self.database.open_interaction(
                    candidate["id"],
                    kind=InteractionKind.APPROVAL,
                    recipient_krab_identity_id=identity_id,
                    request_item_id="itm_" + "a" * 32,
                    expected_run_version=version,
                )

        for version in (True, 0, 1.5, "3"):
            with (
                self.subTest(version=version),
                self.assertRaisesRegex(ValidationError, "positive integer"),
            ):
                self.database.open_interaction(
                    running["id"],
                    kind=InteractionKind.CHOICE,
                    recipient_krab_identity_id=self.reviewer_identity["id"],
                    request_item_id="itm_" + "b" * 32,
                    expected_run_version=version,  # type: ignore[arg-type]
                )
        with self.assertRaisesRegex(ValidationError, "Krab itm identifier"):
            self.database.open_interaction(
                running["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id=self.reviewer_identity["id"],
                request_item_id="not-an-item",
                expected_run_version=running["version"],
            )
        with self.assertRaisesRegex(ValidationError, "interaction kind"):
            self.database.open_interaction(
                running["id"],
                kind="question",  # type: ignore[arg-type]
                recipient_krab_identity_id=self.reviewer_identity["id"],
                request_item_id="itm_" + "b" * 32,
                expected_run_version=running["version"],
            )
        with self.assertRaisesRegex(NotFoundError, "run not found"):
            self.database.open_interaction(
                "run_" + "f" * 32,
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id=self.reviewer_identity["id"],
                request_item_id="itm_" + "b" * 32,
                expected_run_version=1,
            )
        with self.assertRaisesRegex(NotFoundError, "recipient Krab identity"):
            self.database.open_interaction(
                running["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id="kid_" + "f" * 32,
                request_item_id="itm_" + "b" * 32,
                expected_run_version=running["version"],
            )

    def test_request_item_is_unique_and_failed_open_is_atomic(self) -> None:
        request_item_id = "itm_" + "e" * 32
        first = self.running_run()
        second = self.running_run()
        self.database.open_interaction(
            first["id"],
            kind=InteractionKind.APPROVAL,
            recipient_krab_identity_id=self.reviewer_identity["id"],
            request_item_id=request_item_id,
            expected_run_version=first["version"],
        )
        with self.assertRaisesRegex(ConflictError, "could not be opened"):
            self.database.open_interaction(
                second["id"],
                kind=InteractionKind.CHOICE,
                recipient_krab_identity_id=self.reviewer_identity["id"],
                request_item_id=request_item_id,
                expected_run_version=second["version"],
            )
        unchanged = self.database.get_run(second["id"])
        self.assertEqual((unchanged["state"], unchanged["version"]), ("running", 3))
        self.assertEqual(unchanged["interactions"], [])

    def test_answer_is_optimistic_and_requires_open_waiting_interaction(self) -> None:
        run = self.running_run()
        interaction = self.open_interaction(run)

        for version in (True, 0, 1.5, "1"):
            with (
                self.subTest(version=version),
                self.assertRaisesRegex(ValidationError, "positive integer"),
            ):
                self.database.answer_interaction(
                    interaction["id"],
                    response_item_id="itm_" + "c" * 32,
                    expected_version=version,  # type: ignore[arg-type]
                )
        with self.assertRaisesRegex(ValidationError, "must differ"):
            self.database.answer_interaction(
                interaction["id"],
                response_item_id=interaction["request_item_id"],
                expected_version=1,
            )
        with self.assertRaisesRegex(ConcurrentUpdateError, "interaction version"):
            self.database.answer_interaction(
                interaction["id"],
                response_item_id="itm_" + "c" * 32,
                expected_version=2,
            )
        with self.assertRaisesRegex(NotFoundError, "interaction not found"):
            self.database.answer_interaction(
                "int_" + "f" * 32,
                response_item_id="itm_" + "c" * 32,
                expected_version=1,
            )

        self.database.transition_run(
            run["id"], target=RunState.FAILED, expected_version=4
        )
        cancelled = self.database.get_interaction(interaction["id"])
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(cancelled["version"], 2)
        self.assertIsNotNone(cancelled["resolved_at"])
        self.assertEqual(cancelled["events"][-1]["kind"], "interaction.cancelled")
        with self.assertRaisesRegex(ConflictError, "interaction is not open"):
            self.database.answer_interaction(
                interaction["id"],
                response_item_id="itm_" + "d" * 32,
                expected_version=2,
            )

    def test_response_item_is_unique_and_failed_answer_is_atomic(self) -> None:
        first = self.open_interaction(self.running_run())
        second_run = self.running_run()
        second = self.database.open_interaction(
            second_run["id"],
            kind=InteractionKind.CHOICE,
            recipient_krab_identity_id=self.reviewer_identity["id"],
            request_item_id="itm_" + "7" * 32,
            expected_run_version=second_run["version"],
        )
        response_item_id = "itm_" + "8" * 32
        self.database.answer_interaction(
            first["id"], response_item_id=response_item_id, expected_version=1
        )
        with self.assertRaisesRegex(ConflictError, "could not be answered"):
            self.database.answer_interaction(
                second["id"],
                response_item_id=response_item_id,
                expected_version=1,
            )
        unchanged = self.database.get_interaction(second["id"])
        self.assertEqual((unchanged["state"], unchanged["version"]), ("open", 1))
        self.assertEqual(unchanged["run_state"], "waiting_for_input")

    def test_answer_rejects_an_open_interaction_whose_run_is_not_waiting(self) -> None:
        interaction = self.open_interaction(self.running_run())
        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE runs SET state = 'running' WHERE id = ?",
                (interaction["run_id"],),
            )
        with self.assertRaisesRegex(ConflictError, "run is not waiting"):
            self.database.answer_interaction(
                interaction["id"],
                response_item_id="itm_" + "9" * 32,
                expected_version=1,
            )

    def test_open_interaction_blocks_manual_resume_and_event_mutation(self) -> None:
        run = self.running_run()
        interaction = self.open_interaction(run)
        with self.assertRaisesRegex(ValidationError, "invalid run transition"):
            self.database.transition_run(
                run["id"], target=RunState.RUNNING, expected_version=4
            )
        with self.assertRaisesRegex(ConflictError, "open interaction"):
            self.database.transition_run(
                run["id"], target=RunState.QUEUED, expected_version=4
            )

        connection = self.database.connect()
        self.addCleanup(connection.close)
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "DELETE FROM interaction_events WHERE interaction_id = ?",
                (interaction["id"],),
            )

        with self.assertRaisesRegex(NotFoundError, "interaction not found"):
            self.database.get_interaction("int_" + "f" * 32)
        with self.assertRaisesRegex(NotFoundError, "run not found"):
            self.database.list_run_interactions("run_" + "f" * 32)

    def test_status_counts_interactions(self) -> None:
        self.assertEqual(self.database.status()["counts"]["interactions"], 0)
        self.open_interaction(self.running_run())
        self.assertEqual(self.database.status()["counts"]["interactions"], 1)

    def test_interaction_history_is_keyset_paged_and_run_views_are_bounded(
        self,
    ) -> None:
        run = self.running_run()
        with self.database.connect() as connection:
            for index in range(300):
                interaction_id = "int_" + f"{index:032x}"
                request_item_id = "itm_" + f"{index:032x}"
                created_at = (
                    f"2026-10-20T12:{index // 60:02d}:{index % 60:02d}.000000+00:00"
                )
                connection.execute(
                    "INSERT INTO interactions "
                    "(id, run_id, kind, state, version, "
                    "recipient_krab_identity_id, request_item_id, response_item_id, "
                    "created_at, updated_at, resolved_at, attempt_id) "
                    "VALUES (?, ?, 'question', 'cancelled', 1, ?, ?, NULL, "
                    "?, ?, ?, NULL)",
                    (
                        interaction_id,
                        run["id"],
                        self.reviewer_identity["id"],
                        request_item_id,
                        created_at,
                        created_at,
                        created_at,
                    ),
                )
                connection.execute(
                    "INSERT INTO interaction_events "
                    "(interaction_id, sequence, kind, from_state, to_state, "
                    "krab_item_id, created_at) VALUES "
                    "(?, 1, 'interaction.opened', NULL, 'open', ?, ?)",
                    (interaction_id, request_item_id, created_at),
                )

        latest = self.database.get_run_interaction_page(run["id"], limit=64)
        self.assertEqual(latest["interaction_count"], 300)
        self.assertTrue(latest["interactions_truncated"])
        self.assertEqual(len(latest["interactions"]), 64)
        self.assertEqual(latest["interactions"][0]["id"], "int_" + f"{236:032x}")
        self.assertEqual(latest["interactions"][-1]["id"], "int_" + f"{299:032x}")
        self.assertEqual(
            latest["next_before_interaction_id"],
            latest["interactions"][0]["id"],
        )

        previous = self.database.get_run_interaction_page(
            run["id"],
            limit=64,
            before_interaction_id=latest["next_before_interaction_id"],
        )
        self.assertEqual(previous["interactions"][0]["id"], "int_" + f"{172:032x}")
        self.assertEqual(previous["interactions"][-1]["id"], "int_" + f"{235:032x}")
        self.assertTrue(
            {item["id"] for item in latest["interactions"]}.isdisjoint(
                item["id"] for item in previous["interactions"]
            )
        )

        visible = self.database.get_run(run["id"])
        self.assertEqual(visible["interaction_count"], 300)
        self.assertTrue(visible["interactions_truncated"])
        self.assertEqual(len(visible["interactions"]), 128)

        single = self.database.get_run_interaction_page(run["id"], limit=1)
        self.assertEqual(len(single["interactions"]), 1)
        self.assertEqual(single["interactions"][0]["id"], "int_" + f"{299:032x}")
        self.assertTrue(single["interactions_truncated"])

        maximum = self.database.get_run_interaction_page(run["id"], limit=256)
        self.assertEqual(len(maximum["interactions"]), 256)
        self.assertTrue(maximum["interactions_truncated"])

        terminal = self.database.get_run_interaction_page(
            run["id"],
            limit=64,
            before_interaction_id="int_" + f"{64:032x}",
        )
        self.assertEqual(len(terminal["interactions"]), 64)
        self.assertFalse(terminal["interactions_truncated"])
        self.assertIsNone(terminal["next_before_interaction_id"])

        for invalid in (True, 0, 257):
            with (
                self.subTest(limit=invalid),
                self.assertRaisesRegex(ValidationError, "limit"),
            ):
                self.database.get_run_interaction_page(
                    run["id"],
                    limit=invalid,  # type: ignore[arg-type]
                )
        with self.assertRaisesRegex(ValidationError, "before_interaction_id"):
            self.database.get_run_interaction_page(
                run["id"], before_interaction_id="x" * 65
            )
        with self.assertRaisesRegex(NotFoundError, "cursor"):
            self.database.get_run_interaction_page(
                run["id"], before_interaction_id="int_" + "f" * 32
            )


class InteractionMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)

    def test_schema_one_migrates_existing_interactions(self) -> None:
        path = Path(self.temporary.name) / "legacy.db"
        connection = sqlite3.connect(path)
        connection.executescript(SCHEMA_V1)
        connection.execute(
            "INSERT INTO users "
            "(id, handle, display_name, active, created_at, updated_at) "
            "VALUES (?, ?, ?, 1, ?, ?)",
            ("boxu_" + "1" * 32, "operator", "Operator", "then", "then"),
        )
        connection.execute("PRAGMA user_version = 1")
        connection.commit()
        connection.close()
        os.chmod(path, 0o600)

        database = Database(path)
        first = database.initialize()
        second = database.initialize()
        self.assertEqual(first["schema_version"], SCHEMA_VERSION)
        self.assertEqual(second, first)
        self.assertEqual(first["counts"]["users"], 1)
        self.assertEqual(first["counts"]["interactions"], 0)
        self.assertEqual(first["counts"]["work_items"], 0)
        with closing(database.connect()) as migrated:
            tables = {
                row[0]
                for row in migrated.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table'"
                )
            }
        self.assertIn("interactions", tables)
        self.assertIn("interaction_events", tables)
        self.assertIn("work_items", tables)

    def test_missing_and_broken_migrations_fail_closed(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        self.addCleanup(connection.close)
        connection.execute("PRAGMA user_version = 1")
        with (
            mock.patch("magic_glass_box.storage.core.MIGRATIONS", {}),
            self.assertRaisesRegex(StorageError, "no migration"),
        ):
            Database._migrate(connection, 1)
        self.assertFalse(connection.in_transaction)

        with (
            mock.patch.object(
                Database,
                "_execute_statements",
                side_effect=sqlite3.OperationalError("broken"),
            ),
            self.assertRaisesRegex(StorageError, "cannot migrate"),
        ):
            Database._migrate(connection, 1)
        self.assertFalse(connection.in_transaction)

    def test_newer_schema_is_still_rejected(self) -> None:
        path = Path(self.temporary.name) / "future.db"
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA user_version = 99")
        connection.close()
        os.chmod(path, 0o600)
        with self.assertRaisesRegex(StorageError, "newer than supported"):
            Database(path).initialize()


if __name__ == "__main__":
    unittest.main()
