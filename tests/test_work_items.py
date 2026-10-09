from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from magic_glass_box.db import (
    INTERACTION_SCHEMA,
    SCHEMA_V1,
    ConflictError,
    Database,
    NotFoundError,
    StorageError,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    ValidationError,
    WorkItemEnvelope,
    WorkItemPriority,
)
from magic_glass_box.krab import KrabIdentity


def envelope(
    *,
    updated_at: str = "2026-10-09T12:00:00Z",
    brief: str = "Implement a forge-neutral work-item import.",
    external_id: str = "psyberone/magic-glass-box#42",
) -> WorkItemEnvelope:
    return WorkItemEnvelope.from_mapping(
        {
            "schema_version": 1,
            "source": "github",
            "external_id": external_id,
            "source_updated_at": updated_at,
            "title": "Add durable work-item imports",
            "brief": brief,
            "acceptance_criteria": [
                "Identical imports are idempotent",
                "Runs pin an immutable version",
            ],
            "constraints": ["Do not call a forge API"],
            "context": [
                {"type": "workspace_path", "value": "docs/architecture.md"},
                {"type": "note", "value": "Treat imported text as untrusted."},
            ],
            "labels": ["orchestration", "security"],
            "priority": "high",
        }
    )


class WorkItemEnvelopeTests(unittest.TestCase):
    def test_published_schema_and_example_match_the_v1_contract(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        schema = json.loads(
            (repository / "schemas/work-item-v1.schema.json").read_text(
                encoding="utf-8"
            )
        )
        example = json.loads(
            (repository / "examples/work-item.json").read_text(encoding="utf-8")
        )
        item = WorkItemEnvelope.from_mapping(example)
        self.assertEqual(schema["$id"], "urn:magic-glass-box:schema:work-item:v1")
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(schema["properties"]["schema_version"]["const"], 1)
        self.assertEqual(item.title, "Add run-scoped credential grants")
        for forbidden in ("credentials", "profile", "project", "url"):
            self.assertNotIn(forbidden, schema["properties"])

    def test_envelope_is_strict_normalized_and_content_addressed(self) -> None:
        item = WorkItemEnvelope.from_mapping(
            {
                "schema_version": 1,
                "source": "github",
                "external_id": "owner/repository#17",
                "source_updated_at": "2026-10-09T07:00:00-05:00",
                "title": "Keep imported work durable",
                "brief": "Preserve enough normalized context for an agent.",
                "acceptance_criteria": ["Import is atomic"],
                "constraints": ["No outbound synchronization"],
                "context": [
                    {"type": "workspace_path", "value": "docs/architecture.md"},
                    {"type": "note", "value": "The source is untrusted."},
                ],
                "labels": ["work-item"],
                "priority": "urgent",
            }
        )

        self.assertEqual(item.source_updated_at, "2026-10-09T12:00:00.000000Z")
        self.assertIs(item.priority, WorkItemPriority.URGENT)
        self.assertEqual(
            item.to_dict(),
            {
                "acceptance_criteria": ["Import is atomic"],
                "brief": "Preserve enough normalized context for an agent.",
                "constraints": ["No outbound synchronization"],
                "context": [
                    {"type": "workspace_path", "value": "docs/architecture.md"},
                    {"type": "note", "value": "The source is untrusted."},
                ],
                "external_id": "owner/repository#17",
                "labels": ["work-item"],
                "priority": "urgent",
                "schema_version": 1,
                "source": "github",
                "source_updated_at": "2026-10-09T12:00:00.000000Z",
                "title": "Keep imported work durable",
            },
        )
        self.assertEqual(len(item.digest), 64)
        later = WorkItemEnvelope.from_mapping(
            {**item.to_dict(), "source_updated_at": "2026-10-10T12:00:00Z"}
        )
        self.assertEqual(later.digest, item.digest)
        self.assertEqual(later.content, item.content)

    def test_envelope_defaults_optional_agent_context(self) -> None:
        item = WorkItemEnvelope.from_mapping(
            {
                "schema_version": 1,
                "source": "manual",
                "external_id": "local-1",
                "source_updated_at": "2026-10-09T12:00:00Z",
                "title": "Small task",
                "brief": "Do the small task.",
            }
        )
        self.assertEqual(item.acceptance_criteria, ())
        self.assertEqual(item.constraints, ())
        self.assertEqual(item.context, ())
        self.assertEqual(item.labels, ())
        self.assertIs(item.priority, WorkItemPriority.NORMAL)

    def test_envelope_rejects_identity_schema_and_timestamp_errors(self) -> None:
        base = envelope().to_dict()
        cases = (
            ([], "work item must be an object"),
            ({**base, "unknown": True}, "unknown fields"),
            ({**base, "schema_version": 2}, "schema_version must be 1"),
            ({**base, "schema_version": True}, "schema_version must be 1"),
            ({**base, "source": "Git Hub"}, "source must match"),
            ({**base, "external_id": ""}, "external_id must not be empty"),
            ({**base, "source_updated_at": "not-a-date"}, "valid RFC 3339"),
            (
                {**base, "source_updated_at": "2026-10-09T12:00:00"},
                "include a timezone",
            ),
            ({**base, "title": " \n\t"}, "title must not be blank"),
            ({**base, "brief": "  "}, "brief must not be blank"),
            (
                {**base, "acceptance_criteria": ["  "]},
                "acceptance_criteria entries must not be blank",
            ),
            ({**base, "priority": "critical"}, "priority is invalid"),
        )
        for value, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                WorkItemEnvelope.from_mapping(value)  # type: ignore[arg-type]

    def test_envelope_rejects_unsafe_or_malformed_context(self) -> None:
        base = envelope().to_dict()
        cases = (
            ({**base, "context": "docs/architecture.md"}, "context must be a list"),
            ({**base, "context": ["note"]}, r"context\[0\] must be an object"),
            (
                {**base, "context": [{"type": "url", "value": "https://x"}]},
                r"context\[0\] type is invalid",
            ),
            (
                {
                    **base,
                    "context": [{"type": "note", "value": "x", "authority": "admin"}],
                },
                "unknown fields",
            ),
            (
                {
                    **base,
                    "context": [{"type": "workspace_path", "value": "/etc/passwd"}],
                },
                "safe relative POSIX path",
            ),
            (
                {
                    **base,
                    "context": [{"type": "workspace_path", "value": "docs\\secret"}],
                },
                "safe relative POSIX path",
            ),
            (
                {
                    **base,
                    "context": [{"type": "workspace_path", "value": "docs/../secret"}],
                },
                "safe relative POSIX path",
            ),
            (
                {
                    **base,
                    "context": [
                        {"type": "note", "value": "same"},
                        {"type": "note", "value": "same"},
                    ],
                },
                "context entries must be unique",
            ),
        )
        for value, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                WorkItemEnvelope.from_mapping(value)

    def test_envelope_bounds_collections_and_total_content(self) -> None:
        base = envelope().to_dict()
        for value, message in (
            ({**base, "labels": "security"}, "labels must be a list"),
            ({**base, "labels": ["same", "same"]}, "labels entries must be unique"),
            (
                {**base, "acceptance_criteria": ["x"] * 129},
                "at most 128",
            ),
            (
                {
                    **base,
                    "context": [
                        {"type": "note", "value": str(index)} for index in range(129)
                    ],
                },
                "at most 128",
            ),
            (
                {
                    **base,
                    "acceptance_criteria": [
                        f"{index:03d}" + "x" * 4093 for index in range(65)
                    ],
                },
                "content exceeds 262144 UTF-8 bytes",
            ),
        ):
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                WorkItemEnvelope.from_mapping(value)


class WorkItemDatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "control-plane.db"
        self.database = Database(self.path)
        self.database.initialize()
        self.user = self.database.create_user("operator", "Local Operator")
        self.instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.identity = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + "1" * 32, "agent_one", 1),
            "file:/private/agent-token",
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping({"purpose": "Build the box"}),
            created_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc", command=["agent"], network_mode="none"
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )

    def import_item(self, item: WorkItemEnvelope | None = None) -> dict[str, object]:
        return self.database.import_work_item(
            project_key="box-project",
            envelope=item or envelope(),
            imported_by=self.user["id"],
        )

    def test_import_creates_normalized_version_and_reports_it(self) -> None:
        imported = self.import_item()

        self.assertEqual(imported["import_status"], "created")
        self.assertTrue(imported["id"].startswith("wki_"))
        self.assertTrue(imported["work_item_version_id"].startswith("wiv_"))
        self.assertEqual(imported["project_key"], "box-project")
        self.assertEqual(imported["source"], "github")
        self.assertEqual(imported["external_id"], "psyberone/magic-glass-box#42")
        self.assertEqual(imported["version"], 1)
        self.assertEqual(imported["content"], envelope().content)
        self.assertEqual(imported["content_sha256"], envelope().digest)
        self.assertEqual(imported["imported_by"], "operator")
        self.assertEqual(self.database.status()["counts"]["work_items"], 1)
        stored = self.database.get_work_item(imported["id"])
        self.assertNotIn("import_status", stored)
        self.assertEqual(stored, {key: imported[key] for key in stored})

    def test_import_is_idempotent_and_advances_observed_source_revision(self) -> None:
        first = self.import_item()
        same_revision = self.import_item()
        later = self.import_item(envelope(updated_at="2026-10-10T12:00:00Z"))

        self.assertEqual(same_revision["import_status"], "unchanged")
        self.assertEqual(later["import_status"], "unchanged")
        self.assertEqual(later["id"], first["id"])
        self.assertEqual(later["version"], 1)
        self.assertEqual(later["source_updated_at"], "2026-10-10T12:00:00.000000Z")
        with closing(self.database.connect()) as connection:
            versions = connection.execute(
                "SELECT COUNT(*) FROM work_item_versions WHERE work_item_id = ?",
                (first["id"],),
            ).fetchone()[0]
        self.assertEqual(versions, 1)

    def test_newer_changed_content_appends_an_immutable_version(self) -> None:
        first = self.import_item()
        updated = self.import_item(
            envelope(
                updated_at="2026-10-10T12:00:00Z",
                brief="Implement the normalized import and immutable history.",
            )
        )

        self.assertEqual(updated["import_status"], "updated")
        self.assertEqual(updated["id"], first["id"])
        self.assertEqual(updated["version"], 2)
        self.assertNotEqual(
            updated["work_item_version_id"], first["work_item_version_id"]
        )
        self.assertNotEqual(updated["content_sha256"], first["content_sha256"])
        with (
            closing(self.database.connect()) as connection,
            self.assertRaises(sqlite3.IntegrityError),
        ):
            connection.execute(
                "UPDATE work_item_versions SET version = 99 WHERE id = ?",
                (first["work_item_version_id"],),
            )

    def test_a_later_import_can_restore_earlier_content_as_a_new_version(self) -> None:
        first = self.import_item()
        self.import_item(
            envelope(
                updated_at="2026-10-10T12:00:00Z",
                brief="Temporarily changed assignment.",
            )
        )
        restored = self.import_item(envelope(updated_at="2026-10-11T12:00:00Z"))
        self.assertEqual(restored["import_status"], "updated")
        self.assertEqual(restored["version"], 3)
        self.assertEqual(restored["content_sha256"], first["content_sha256"])

    def test_stale_and_conflicting_revisions_fail_without_mutation(self) -> None:
        first = self.import_item()
        self.import_item(envelope(updated_at="2026-10-10T12:00:00Z"))

        with self.assertRaisesRegex(ConflictError, "stale"):
            self.import_item(
                envelope(
                    updated_at="2026-10-09T18:00:00Z",
                    brief="This stale change must not win.",
                )
            )
        with self.assertRaisesRegex(ConflictError, "same source revision"):
            self.import_item(
                envelope(
                    updated_at="2026-10-10T12:00:00Z",
                    brief="This ambiguous change must not win.",
                )
            )
        stored = self.database.get_work_item(first["id"])
        self.assertEqual(stored["version"], 1)
        self.assertEqual(stored["content"], envelope().content)

    def test_import_identity_is_scoped_to_project_and_requires_local_authority(
        self,
    ) -> None:
        first = self.import_item()
        other = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="other-project",
            display_name="Other Project",
            context=ProjectContext.from_mapping({"purpose": "Other work"}),
            created_by=self.user["id"],
        )
        second = self.database.import_work_item(
            project_key="other-project",
            envelope=envelope(),
            imported_by=self.user["id"],
        )
        self.assertNotEqual(second["id"], first["id"])
        self.assertNotEqual(other["id"], self.project["id"])

        with self.assertRaisesRegex(NotFoundError, "active project"):
            self.database.import_work_item(
                project_key="missing-project",
                envelope=envelope(),
                imported_by=self.user["id"],
            )
        connection = self.database.connect()
        self.addCleanup(connection.close)
        connection.execute(
            "UPDATE projects SET active = 0 WHERE id = ?", (self.project["id"],)
        )
        with self.assertRaisesRegex(NotFoundError, "active project"):
            self.import_item(envelope(external_id="inactive-project-item"))
        connection.execute(
            "UPDATE projects SET active = 1 WHERE id = ?", (self.project["id"],)
        )
        with self.assertRaisesRegex(ConflictError, "could not be imported"):
            self.database.import_work_item(
                project_key="box-project",
                envelope=envelope(external_id="new-item"),
                imported_by="boxu_" + "f" * 32,
            )
        with self.assertRaisesRegex(ValidationError, "WorkItemEnvelope"):
            self.database.import_work_item(
                project_key="box-project",
                envelope={},  # type: ignore[arg-type]
                imported_by=self.user["id"],
            )

    def test_missing_and_corrupt_work_items_fail_closed(self) -> None:
        with self.assertRaisesRegex(NotFoundError, "work item not found"):
            self.database.get_work_item("wki_" + "f" * 32)
        imported = self.import_item()
        run = self.database.create_run(
            project_key="box-project",
            profile_name="Worker",
            work_item_id=imported["id"],
            requested_by=self.user["id"],
        )
        connection = self.database.connect()
        self.addCleanup(connection.close)
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE work_items SET external_id = 'changed' WHERE id = ?",
                (imported["id"],),
            )
        connection.execute("DROP TRIGGER work_item_version_immutable_update")
        connection.execute(
            "UPDATE work_item_versions SET content_sha256 = ? WHERE id = ?",
            ("0" * 64, imported["work_item_version_id"]),
        )
        with self.assertRaisesRegex(StorageError, "digest does not match"):
            self.database.get_work_item(imported["id"])
        with self.assertRaisesRegex(StorageError, "pinned run data is invalid"):
            self.database.get_run_execution(run["id"])

    def test_import_rejects_a_corrupt_item_without_a_content_version(self) -> None:
        imported = self.import_item()
        with closing(self.database.connect()) as connection:
            connection.execute("DROP TRIGGER work_item_version_immutable_delete")
            connection.execute(
                "DELETE FROM work_item_versions WHERE work_item_id = ?",
                (imported["id"],),
            )
        with self.assertRaisesRegex(StorageError, "no content version"):
            self.import_item(envelope(updated_at="2026-10-10T12:00:00Z"))

    def test_runs_pin_the_current_work_item_version(self) -> None:
        first_import = self.import_item()
        first_run = self.database.create_run(
            project_key="box-project",
            profile_name="Worker",
            work_item_id=first_import["id"],
            requested_by=self.user["id"],
        )
        second_import = self.import_item(
            envelope(
                updated_at="2026-10-10T12:00:00Z",
                brief="This is the second immutable assignment.",
            )
        )
        second_run = self.database.create_run(
            project_key="box-project",
            profile_name="Worker",
            work_item_id=first_import["id"],
            requested_by=self.user["id"],
        )

        pinned_first = self.database.get_run(first_run["id"])
        pinned_second = self.database.get_run(second_run["id"])
        self.assertEqual(pinned_first["work_item_version"], 1)
        self.assertEqual(
            pinned_first["work_item_title"], "Add durable work-item imports"
        )
        self.assertNotIn(
            "Implement a forge-neutral work-item import.",
            json.dumps(pinned_first),
        )
        self.assertEqual(pinned_second["work_item_version"], 2)
        self.assertEqual(
            pinned_first["work_item_version_id"],
            first_import["work_item_version_id"],
        )
        self.assertEqual(
            pinned_second["work_item_version_id"],
            second_import["work_item_version_id"],
        )
        self.assertEqual(
            self.database.get_run_execution(first_run["id"])["work_item"]["brief"],
            "Implement a forge-neutral work-item import.",
        )
        self.assertEqual(
            self.database.get_run_execution(second_run["id"])["work_item"]["brief"],
            "This is the second immutable assignment.",
        )

        self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="other-project",
            display_name="Other Project",
            context=ProjectContext.from_mapping({"purpose": "Other work"}),
            created_by=self.user["id"],
        )
        with self.assertRaisesRegex(NotFoundError, "work item not found for project"):
            self.database.create_run(
                project_key="other-project",
                profile_name="Worker",
                work_item_id=first_import["id"],
                requested_by=self.user["id"],
            )


class WorkItemMigrationTests(unittest.TestCase):
    def test_schema_two_migrates_once_without_losing_existing_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-two.db"
            connection = sqlite3.connect(path)
            connection.executescript(SCHEMA_V1 + INTERACTION_SCHEMA)
            connection.execute(
                "INSERT INTO users "
                "(id, handle, display_name, active, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?)",
                ("boxu_" + "1" * 32, "operator", "Operator", "then", "then"),
            )
            connection.execute("PRAGMA user_version = 2")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()
            self.assertEqual(first["schema_version"], 4)
            self.assertEqual(second, first)
            self.assertEqual(first["counts"]["users"], 1)
            self.assertEqual(first["counts"]["work_items"], 0)
            with closing(database.connect()) as migrated:
                tables = {
                    row[0]
                    for row in migrated.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table'"
                    )
                }
                run_columns = {
                    row[1] for row in migrated.execute("PRAGMA table_info(runs)")
                }
            self.assertIn("work_items", tables)
            self.assertIn("work_item_versions", tables)
            self.assertIn("work_item_version_id", run_columns)


if __name__ == "__main__":
    unittest.main()
