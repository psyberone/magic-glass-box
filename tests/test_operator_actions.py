from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from magic_glass_box.db import (
    RUN_REPLACEMENT_SCHEMA,
    SCHEMA,
    SCHEMA_VERSION,
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    ProjectContext,
    ProviderGrantSpec,
    ValidationError,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.service import ControlPlane


class OperatorRunActionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Database(Path(self.temporary.name) / "control-plane.db")
        self.database.initialize()
        self.operator = self.database.create_user("operator", "Operator")
        instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "1" * 32, "worker_one", 1),
            "file:/private/worker-one.token",
            user_id=self.operator["id"],
        )
        reviewer_identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "2" * 32, "reviewer_one", 1),
            "file:/private/reviewer-one.token",
        )
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping(
                {"purpose": "Exercise explicit operator actions"}
            ),
            created_by=self.operator["id"],
        )
        self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.create(
                image="worker@sha256:one", command=["worker", "run"]
            ),
            krab_identity_id=identity["id"],
            created_by=self.operator["id"],
        )
        self.database.create_agent_profile(
            name="Reviewer",
            spec=AgentProfileSpec.create(
                image="reviewer@sha256:one", command=["reviewer", "run"]
            ),
            krab_identity_id=reviewer_identity["id"],
            created_by=self.operator["id"],
        )
        self.item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "operator-actions",
                    "source_updated_at": "2026-10-10T12:00:00Z",
                    "title": "Explicit operator actions",
                    "brief": "Retry or reassign without rewriting history.",
                }
            ),
            imported_by=self.operator["id"],
        )
        self.control = ControlPlane(self.database)

    def create_run(self) -> dict[str, object]:
        return self.database.create_run(
            project_key="box-project",
            profile_name="Worker",
            work_item_id=self.item["id"],
            requested_by=self.operator["id"],
            source_revision="a" * 40,
            krab_session_id="ses_" + "4" * 32,
            krab_claim_id="clm_" + "5" * 32,
        )

    def terminal_run(self) -> dict[str, object]:
        run = self.create_run()
        return self.database.request_run_cancellation(
            run["id"],
            requested_by=self.operator["id"],
            expected_run_version=run["version"],
            reason="Prepare a terminal source run",
        )

    def test_retry_creates_an_audited_successor_with_identical_pins(self) -> None:
        source = self.terminal_run()

        successor = self.control.retry_run(
            source["id"],
            requested_by_handle="operator",
            expected_run_version=source["version"],
        )

        self.assertEqual(successor["state"], "queued")
        self.assertEqual(successor["predecessor_run_id"], source["id"])
        self.assertEqual(successor["replacement_kind"], "retry")
        for field in (
            "project_key",
            "context_version_id",
            "profile_version_id",
            "work_item_version_id",
            "work_thread_id",
            "source_revision",
            "krab_session_id",
            "krab_claim_id",
        ):
            self.assertEqual(successor[field], source[field], field)

        refreshed = self.database.get_run(source["id"])
        self.assertEqual(refreshed["version"], source["version"] + 1)
        self.assertEqual(refreshed["state"], "cancelled")
        self.assertEqual(refreshed["events"][-1]["kind"], "run.retry_created")
        self.assertEqual(
            refreshed["events"][-1]["metadata"],
            {
                "replacement_run_id": successor["id"],
                "requested_by": self.operator["id"],
            },
        )
        self.assertEqual(successor["events"][0]["kind"], "run.created")
        self.assertEqual(
            successor["events"][0]["metadata"],
            {"predecessor_run_id": source["id"], "replacement_kind": "retry"},
        )
        projected = {
            run["id"]: run
            for run in self.control.control_room_snapshot(history_limit=25)["runs"]
        }
        self.assertEqual(projected[successor["id"]]["predecessor_run_id"], source["id"])
        self.assertEqual(projected[successor["id"]]["replacement_kind"], "retry")

    def test_reassign_uses_the_named_profile_without_changing_work_identity(
        self,
    ) -> None:
        source = self.terminal_run()

        successor = self.control.reassign_run(
            source["id"],
            profile_name="Reviewer",
            requested_by_handle="operator",
            expected_run_version=source["version"],
        )

        self.assertEqual(successor["replacement_kind"], "reassign")
        self.assertEqual(successor["profile_name"], "Reviewer")
        self.assertNotEqual(
            successor["profile_version_id"], source["profile_version_id"]
        )
        self.assertEqual(successor["context_version_id"], source["context_version_id"])
        self.assertEqual(
            successor["work_item_version_id"], source["work_item_version_id"]
        )
        self.assertEqual(successor["work_thread_id"], source["work_thread_id"])
        self.assertEqual(successor["source_revision"], source["source_revision"])
        self.assertEqual(successor["krab_session_id"], source["krab_session_id"])
        self.assertEqual(successor["krab_claim_id"], source["krab_claim_id"])
        self.assertEqual(
            self.database.get_run(source["id"])["events"][-1]["kind"],
            "run.reassigned",
        )

    def test_replacement_requires_terminal_current_state_and_active_thread(
        self,
    ) -> None:
        active = self.create_run()
        with self.assertRaisesRegex(ConflictError, "not terminal"):
            self.control.retry_run(
                active["id"],
                requested_by_handle="operator",
                expected_run_version=active["version"],
            )

        source = self.terminal_run()
        successor = self.control.retry_run(
            source["id"],
            requested_by_handle="operator",
            expected_run_version=source["version"],
        )
        with self.assertRaisesRegex(ConcurrentUpdateError, "expected"):
            self.control.reassign_run(
                source["id"],
                profile_name="Reviewer",
                requested_by_handle="operator",
                expected_run_version=source["version"],
            )
        refreshed = self.database.get_run(source["id"])
        with self.assertRaisesRegex(ConflictError, "active successor"):
            self.control.retry_run(
                source["id"],
                requested_by_handle="operator",
                expected_run_version=refreshed["version"],
            )
        self.assertEqual(successor["predecessor_run_id"], source["id"])

    def test_reassign_rejects_unknown_profile_without_mutating_source(self) -> None:
        source = self.terminal_run()

        with self.assertRaisesRegex(NotFoundError, "profile"):
            self.control.reassign_run(
                source["id"],
                profile_name="Missing",
                requested_by_handle="operator",
                expected_run_version=source["version"],
            )

        unchanged = self.database.get_run(source["id"])
        self.assertEqual(unchanged["version"], source["version"])
        self.assertEqual(unchanged["events"], source["events"])

    def test_retry_refuses_revoked_pinned_authority(self) -> None:
        grant = self.database.create_provider_grant(
            name="provider-main",
            spec=ProviderGrantSpec.from_mapping(
                {
                    "provider": "openai",
                    "adapter": "json_http",
                    "destination": "https://api.openai.com/v1/responses",
                    "method": "POST",
                    "model": "gpt-test",
                    "credential_header": "Authorization",
                    "credential_prefix": "Bearer ",
                    "static_headers": {},
                    "request_bytes": 32768,
                    "response_bytes": 1048576,
                    "timeout_ms": 30000,
                    "max_concurrency": 1,
                }
            ),
            credential_ref="secret-file:/private/provider.key",
            expires_at=None,
            created_by=self.operator["id"],
        )
        worker = self.database.get_agent_profile("Worker")
        self.database.add_agent_profile_version(
            name="Worker",
            spec=AgentProfileSpec.create(
                image="worker@sha256:two",
                command=["worker", "run"],
                credential_names=["provider-main"],
                gateway_capabilities=[GatewayCapability.PROVIDER_INVOKE],
            ),
            krab_identity_id=worker["krab_identity_id"],
            created_by=self.operator["id"],
        )
        source = self.terminal_run()
        first_retry = self.control.retry_run(
            source["id"],
            requested_by_handle="operator",
            expected_run_version=source["version"],
        )
        self.assertEqual(
            [(row["name"], row["grant_id"]) for row in first_retry["provider_grants"]],
            [("provider-main", grant["id"])],
        )
        self.database.request_run_cancellation(
            first_retry["id"],
            requested_by=self.operator["id"],
            expected_run_version=first_retry["version"],
            reason="Finish the first retry",
        )
        source = self.database.get_run(source["id"])
        self.database.revoke_provider_grant(
            "provider-main",
            expected_version=grant["version"],
            revoked_by=self.operator["id"],
        )

        with self.assertRaisesRegex(ConflictError, "pinned provider grant"):
            self.control.retry_run(
                source["id"],
                requested_by_handle="operator",
                expected_run_version=source["version"],
            )

        unchanged = self.database.get_run(source["id"])
        self.assertEqual(unchanged["version"], source["version"])
        self.assertEqual(unchanged["events"], source["events"])

    def test_replacement_rejects_inactive_boundaries_and_same_profile(self) -> None:
        source = self.terminal_run()
        worker = self.database.get_agent_profile("Worker")
        self.database.add_agent_profile_version(
            name="Worker",
            spec=AgentProfileSpec.create(
                image="worker@sha256:newer",
                command=["worker", "run", "--newer"],
            ),
            krab_identity_id=worker["krab_identity_id"],
            created_by=self.operator["id"],
        )
        with self.assertRaisesRegex(ConflictError, "already assigned"):
            self.control.reassign_run(
                source["id"],
                profile_name="Worker",
                requested_by_handle="operator",
                expected_run_version=source["version"],
            )

        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE work_threads SET state = 'completed', "
                "completed_at = updated_at "
                "WHERE id = ?",
                (source["work_thread_id"],),
            )
        with self.assertRaisesRegex(ConflictError, "thread"):
            self.control.retry_run(
                source["id"],
                requested_by_handle="operator",
                expected_run_version=source["version"],
            )

    def test_replacement_rejects_an_inactive_project(self) -> None:
        source = self.terminal_run()
        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE projects SET active = 0 WHERE krab_project_key = ?",
                ("box-project",),
            )
        with self.assertRaisesRegex(ConflictError, "project"):
            self.control.retry_run(
                source["id"],
                requested_by_handle="operator",
                expected_run_version=source["version"],
            )

    def test_retry_rejects_inactive_profile_and_cross_instance_reassignment(
        self,
    ) -> None:
        source = self.terminal_run()
        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE agent_profiles SET active = 0 WHERE name = 'Worker'"
            )
        with self.assertRaisesRegex(ConflictError, "profile is not active"):
            self.control.retry_run(
                source["id"],
                requested_by_handle="operator",
                expected_run_version=source["version"],
            )

        other_instance = self.database.create_krab_instance(
            "Other Krab", "http://127.0.0.1:9765"
        )
        other_identity = self.database.store_krab_identity(
            other_instance["id"],
            KrabIdentity("usr_" + "9" * 32, "other_worker", 1),
            "file:/private/other-worker.token",
        )
        self.database.create_agent_profile(
            name="Other Worker",
            spec=AgentProfileSpec.create(
                image="other@sha256:one", command=["other", "run"]
            ),
            krab_identity_id=other_identity["id"],
            created_by=self.operator["id"],
        )
        with self.assertRaisesRegex(ConflictError, "different Krab instances"):
            self.control.reassign_run(
                source["id"],
                profile_name="Other Worker",
                requested_by_handle="operator",
                expected_run_version=source["version"],
            )

    def test_replacement_validation_and_missing_authority_fail_without_writes(
        self,
    ) -> None:
        source = self.terminal_run()
        with self.assertRaisesRegex(ValidationError, "positive integer"):
            self.database.retry_run(
                source["id"],
                requested_by=self.operator["id"],
                expected_run_version=True,
            )
        with self.assertRaisesRegex(NotFoundError, "operator"):
            self.database.retry_run(
                source["id"],
                requested_by="boxu_" + "f" * 32,
                expected_run_version=source["version"],
            )
        with self.assertRaisesRegex(NotFoundError, "run"):
            self.database.retry_run(
                "run_" + "f" * 32,
                requested_by=self.operator["id"],
                expected_run_version=1,
            )
        self.assertEqual(self.database.get_run(source["id"]), source)

    def test_replacement_lineage_is_immutable_at_the_storage_boundary(self) -> None:
        source = self.terminal_run()
        successor = self.control.retry_run(
            source["id"],
            requested_by_handle="operator",
            expected_run_version=source["version"],
        )

        with (
            closing(self.database.connect()) as connection,
            self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"),
        ):
            connection.execute(
                "UPDATE runs SET predecessor_run_id = NULL, "
                "replacement_kind = NULL WHERE id = ?",
                (successor["id"],),
            )


class OperatorActionMigrationTests(unittest.TestCase):
    def test_schema_eight_migrates_replacement_lineage_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-eight.db"
            connection = sqlite3.connect(path)
            start = SCHEMA.index(RUN_REPLACEMENT_SCHEMA)
            connection.executescript(SCHEMA[:start])
            connection.execute(
                "INSERT INTO users "
                "(id, handle, display_name, active, created_at, updated_at) "
                "VALUES (?, 'operator', 'Operator', 1, 'then', 'then')",
                ("boxu_" + "1" * 32,),
            )
            connection.execute("PRAGMA user_version = 8")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
            self.assertEqual(second, first)
            self.assertEqual(first["counts"]["users"], 1)
            with closing(database.connect()) as migrated:
                columns = {
                    row[1] for row in migrated.execute("PRAGMA table_info(runs)")
                }
                indexes = {
                    row[0]
                    for row in migrated.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'index' "
                        "AND tbl_name = 'runs'"
                    )
                }
            self.assertIn("predecessor_run_id", columns)
            self.assertIn("replacement_kind", columns)
            self.assertIn("run_one_active_successor", indexes)


if __name__ == "__main__":
    unittest.main()
