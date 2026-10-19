from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

from magic_glass_box.db import (
    SCHEMA_VERSION,
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
    StorageError,
    default_db_path,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    ProviderGrantSpec,
    RunState,
    ValidationError,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity


class DatabaseTests(unittest.TestCase):
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
            KrabIdentity(
                user_id="usr_" + "1" * 32,
                handle="agent_one",
                token_version=1,
            ),
            "file:/private/krab-agent-one",
        )
        self.context = ProjectContext.from_mapping(
            {
                "purpose": "Maintain Magic Glass Box",
                "instructions": ["Run tests"],
                "test_commands": [["python", "-m", "unittest"]],
            }
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=self.context,
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "database-test-item",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Exercise database runs",
                    "brief": "Provide a pinned assignment to database tests.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.spec = AgentProfileSpec.create(
            image="agent@sha256:abc",
            command=["codex", "exec"],
        )
        self.profile = self.database.create_agent_profile(
            name="Codex Worker",
            spec=self.spec,
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )

    def test_database_timestamps_are_explicitly_utc(self) -> None:
        self.assertEqual(
            datetime.fromisoformat(Database.now()).utcoffset(),
            timedelta(0),
        )

    def test_database_directory_error_does_not_misidentify_a_symlink(self) -> None:
        directory = Path(self.temporary.name) / "not-a-database"
        directory.mkdir()

        with self.assertRaises(StorageError) as raised:
            Database(directory).initialize()

        self.assertEqual(str(raised.exception), "database must be a regular file")

    def test_database_rows_require_exact_column_names(self) -> None:
        connection = self.database.connect()
        self.addCleanup(connection.close)
        row = connection.execute("SELECT 7 AS exact_name, 9 AS other_name").fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row["exact_name"], 7)
        self.assertEqual(row[0], 7)
        self.assertEqual(row[-1], 9)
        self.assertEqual(row[:], (7, 9))
        self.assertEqual(row.keys(), ["exact_name", "other_name"])
        self.assertEqual(dict(row), {"exact_name": 7, "other_name": 9})
        with self.assertRaises(IndexError):
            row["EXACT_NAME"]
        with self.assertRaises(IndexError):
            row[2]

    def test_sqlite_operational_failures_do_not_escape_the_storage_boundary(
        self,
    ) -> None:
        lock = self.database.connect()
        lock.execute("BEGIN IMMEDIATE")
        original_connect = self.database.connect

        def short_timeout_connect() -> sqlite3.Connection:
            connection = original_connect()
            connection.execute("PRAGMA busy_timeout = 1")
            return connection

        try:
            with (
                mock.patch.object(
                    self.database,
                    "connect",
                    side_effect=short_timeout_connect,
                ),
                self.assertRaisesRegex(StorageError, "database operation failed"),
            ):
                self.database.create_user("contended", "Contended Writer")
        finally:
            lock.rollback()
            lock.close()

    def test_stale_migration_version_is_rechecked_under_exclusive_lock(self) -> None:
        connection = sqlite3.connect(self.database.path)
        denied_actions = {
            sqlite3.SQLITE_DROP_INDEX,
            sqlite3.SQLITE_DROP_TABLE,
            sqlite3.SQLITE_DROP_TRIGGER,
        }

        def reject_destructive_reapplication(
            action: int,
            _arg1: str | None,
            _arg2: str | None,
            _database: str | None,
            _source: str | None,
        ) -> int:
            return (
                sqlite3.SQLITE_DENY if action in denied_actions else sqlite3.SQLITE_OK
            )

        connection.set_authorizer(reject_destructive_reapplication)
        try:
            Database._migrate(connection, SCHEMA_VERSION - 1)
        finally:
            connection.close()

        self.assertEqual(self.database.status()["schema_version"], SCHEMA_VERSION)

    def test_concurrent_initializers_converge_on_one_current_schema(self) -> None:
        path = Path(self.temporary.name) / "concurrent-control-plane.db"

        def initialize() -> int:
            return int(Database(path).initialize()["schema_version"])

        with ThreadPoolExecutor(max_workers=6) as pool:
            versions = list(pool.map(lambda _index: initialize(), range(6)))

        self.assertEqual(versions, [SCHEMA_VERSION] * 6)
        self.assertEqual(Database(path).status()["schema_version"], SCHEMA_VERSION)

    def test_snapshot_transaction_failures_use_the_storage_error_boundary(self) -> None:
        connection = mock.Mock()
        connection.execute.side_effect = sqlite3.OperationalError("database is locked")

        with (
            mock.patch.object(self.database, "connect", return_value=connection),
            self.assertRaisesRegex(StorageError, "database operation failed"),
            self.database._read_snapshot(),
        ):
            self.fail("snapshot should not begin")

        connection.close.assert_called_once_with()

    def test_get_krab_instance_validates_with_the_public_field_name(self) -> None:
        with self.assertRaises(ValidationError) as invalid:
            self.database.get_krab_instance("not/valid")
        self.assertEqual(
            str(invalid.exception),
            "instance name must be 1 to 64 restricted display characters",
        )

    def create_run(self) -> dict[str, object]:
        return self.database.create_run(
            project_key="box-project",
            profile_name="Codex Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision="abc123",
        )

    def test_composite_run_reads_share_one_snapshot_connection(self) -> None:
        run = self.create_run()
        readers = (
            ("run", lambda: self.database.get_run(run["id"])),
            ("execution", lambda: self.database.get_run_execution(run["id"])),
            ("active runs", lambda: list(self.database.iter_active_runs())),
        )
        original_connect = self.database.connect

        for label, reader in readers:
            statements: list[str] = []

            def traced_connect(trace: list[str] = statements) -> sqlite3.Connection:
                connection = original_connect()
                connection.set_trace_callback(trace.append)
                return connection

            with (
                self.subTest(reader=label),
                mock.patch.object(
                    self.database,
                    "connect",
                    side_effect=traced_connect,
                ) as connect,
            ):
                reader()

            self.assertEqual(connect.call_count, 1)
            self.assertEqual(statements[0], "BEGIN")
            self.assertEqual(statements[-1], "COMMIT")

    def test_composite_read_rolls_back_a_failed_snapshot(self) -> None:
        run = self.create_run()
        connection = self.database.connect()
        try:
            connection.execute("DROP TRIGGER work_item_version_immutable_update")
            connection.execute(
                "UPDATE work_item_versions SET content_json = 'not-json' WHERE id = ?",
                (run["work_item_version_id"],),
            )
        finally:
            connection.close()

        statements: list[str] = []
        original_connect = self.database.connect

        def traced_connect() -> sqlite3.Connection:
            traced = original_connect()
            traced.set_trace_callback(statements.append)
            return traced

        with (
            mock.patch.object(
                self.database,
                "connect",
                side_effect=traced_connect,
            ) as connect,
            self.assertRaisesRegex(StorageError, "pinned work item data is invalid"),
        ):
            self.database.get_run(run["id"])

        self.assertEqual(connect.call_count, 1)
        self.assertEqual(statements[0], "BEGIN")
        self.assertEqual(statements[-1], "ROLLBACK")

    def test_database_is_private_and_reports_counts(self) -> None:
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        status = self.database.status()
        self.assertEqual(status["schema_version"], SCHEMA_VERSION)
        self.assertEqual(status["counts"]["users"], 1)
        self.assertEqual(status["counts"]["projects"], 1)

    def test_existing_state_directory_must_already_be_private(self) -> None:
        state = Path(self.temporary.name) / "unsafe-state"
        state.mkdir()
        os.chmod(state, 0o777)  # noqa: S103 - intentionally unsafe fixture

        with self.assertRaisesRegex(StorageError, "state directory must be private"):
            Database(state / "control-plane.db").initialize()

        self.assertFalse((state / "control-plane.db").exists())

    def test_state_directory_must_not_be_a_symlink(self) -> None:
        target = Path(self.temporary.name) / "private-state"
        target.mkdir(mode=0o700)
        state = Path(self.temporary.name) / "state-link"
        state.symlink_to(target, target_is_directory=True)

        with self.assertRaisesRegex(StorageError, "state directory"):
            Database(state / "control-plane.db").initialize()

        self.assertFalse((target / "control-plane.db").exists())

    def test_status_rejects_broken_foreign_key_state(self) -> None:
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "INSERT INTO user_krab_bindings "
                "(user_id, krab_identity_id, created_at) VALUES (?, ?, ?)",
                ("boxu_" + "f" * 32, self.identity["id"], "then"),
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(StorageError, "foreign key"):
            self.database.status()

    def test_default_database_follows_documented_environment_precedence(self) -> None:
        home = Path(self.temporary.name) / "home"
        state_override = Path(self.temporary.name) / "explicit-state"
        xdg_override = Path(self.temporary.name) / "xdg-state"
        db_override = Path(self.temporary.name) / "explicit.db"
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch("magic_glass_box.storage.core.Path.home", return_value=home),
        ):
            self.assertEqual(
                default_db_path(),
                home / ".local" / "state" / "magic-glass-box" / "control-plane.db",
            )
        with mock.patch.dict(
            os.environ,
            {"XDG_STATE_HOME": str(xdg_override)},
            clear=True,
        ):
            self.assertEqual(
                default_db_path(),
                xdg_override / "magic-glass-box" / "control-plane.db",
            )
        with mock.patch.dict(
            os.environ,
            {
                "MAGIC_GLASS_BOX_STATE_DIR": str(state_override),
                "MAGIC_GLASS_BOX_DB": str(db_override),
                "HEX_STATE_DIR": str(state_override),
                "XDG_STATE_HOME": str(xdg_override),
                "HEX_DB": str(Path(self.temporary.name) / "legacy.db"),
            },
            clear=True,
        ):
            self.assertEqual(default_db_path(), db_override)

        legacy_state = Path(self.temporary.name) / "legacy-state"
        legacy_db = Path(self.temporary.name) / "legacy.db"
        with mock.patch.dict(
            os.environ,
            {"HEX_STATE_DIR": str(legacy_state), "HEX_DB": str(legacy_db)},
            clear=True,
        ):
            self.assertEqual(default_db_path(), legacy_db)

        with mock.patch.dict(
            os.environ,
            {"MAGIC_GLASS_BOX_STATE_DIR": str(state_override)},
            clear=True,
        ):
            self.assertEqual(default_db_path(), state_override / "control-plane.db")
        with mock.patch.dict(
            os.environ,
            {"HEX_STATE_DIR": str(legacy_state)},
            clear=True,
        ):
            self.assertEqual(default_db_path(), legacy_state / "hex.db")

    def test_legacy_state_is_reused_until_a_new_database_exists(self) -> None:
        xdg = Path(self.temporary.name) / "xdg"
        legacy = xdg / "hexadecapus" / "hex.db"
        legacy.parent.mkdir(parents=True)
        legacy.touch()
        environment = {"XDG_STATE_HOME": str(xdg)}
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(default_db_path(), legacy)

            current = xdg / "magic-glass-box" / "control-plane.db"
            current.parent.mkdir(parents=True)
            current.touch()
            self.assertEqual(default_db_path(), current)

    def test_created_records_have_complete_shapes_and_id_namespaces(self) -> None:
        records = (
            (self.user, "boxu_"),
            (self.instance, "ksi_"),
            (self.identity, "kid_"),
            (self.project, "boxp_"),
            (self.profile, "agp_"),
        )
        for record, prefix in records:
            with self.subTest(prefix=prefix):
                self.assertTrue(record["id"].startswith(prefix))
                self.assertEqual(len(record["id"]), len(prefix) + 32)
                datetime.fromisoformat(record["created_at"])

        self.assertEqual(
            {key: self.user[key] for key in ("handle", "display_name", "active")},
            {"handle": "operator", "display_name": "Local Operator", "active": 1},
        )
        self.assertEqual(self.user["created_at"], self.user["updated_at"])
        self.assertEqual(self.instance["name"], "Local Krab")
        self.assertEqual(self.instance["base_url"], "http://127.0.0.1:8765")
        self.assertEqual(self.identity["krab_instance_id"], self.instance["id"])
        self.assertEqual(self.identity["krab_user_id"], "usr_" + "1" * 32)
        self.assertEqual(self.identity["handle"], "agent_one")
        self.assertEqual(self.identity["token_version"], 1)
        self.assertEqual(
            self.identity["credential_ref"], "file:/private/krab-agent-one"
        )
        self.assertIsNone(self.identity["local_user_id"])
        self.assertTrue(self.project["context_version_id"].startswith("ctx_"))
        self.assertEqual(self.project["context_version"], 1)
        self.assertEqual(self.project["context"], self.context.to_dict())
        self.assertEqual(self.project["content_sha256"], self.context.digest)
        self.assertTrue(self.profile["profile_version_id"].startswith("apv_"))
        self.assertEqual(self.profile["profile_version"], 1)
        self.assertEqual(self.profile["spec"], self.spec.to_dict())
        self.assertEqual(self.profile["content_sha256"], self.spec.digest)
        self.assertEqual(self.profile["krab_identity_id"], self.identity["id"])

    def test_run_pins_context_and_profile_versions(self) -> None:
        first = self.create_run()
        self.database.add_project_context(
            "box-project",
            ProjectContext.from_mapping({"purpose": "New purpose"}),
            self.user["id"],
        )
        self.database.add_agent_profile_version(
            name="Codex Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:def",
                command=["codex", "exec", "--new"],
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )
        second = self.create_run()
        first_again = self.database.get_run(first["id"])
        self.assertEqual(first_again["context_version"], 1)
        self.assertEqual(first_again["profile_version"], 1)
        self.assertEqual(second["context_version"], 2)
        self.assertEqual(second["profile_version"], 2)

        execution = self.database.get_run_execution(first["id"])
        self.assertEqual(execution["context"], self.context.to_dict())
        self.assertEqual(execution["profile"], self.spec.to_dict())
        self.assertEqual(
            execution["credential_references"],
            {"krab": self.identity["credential_ref"]},
        )
        self.assertEqual(execution["project_key"], "box-project")

    def test_run_persists_complete_request_and_creation_event(self) -> None:
        session_id = "ses_" + "4" * 32
        claim_id = "clm_" + "5" * 32
        run = self.database.create_run(
            project_key="box-project",
            profile_name="Codex Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision="abc123",
            krab_session_id=session_id,
            krab_claim_id=claim_id,
        )
        self.assertTrue(run["id"].startswith("run_"))
        self.assertEqual(len(run["id"]), 36)
        self.assertEqual(run["project_key"], "box-project")
        self.assertEqual(run["context_version_id"], self.project["context_version_id"])
        self.assertEqual(run["context_version"], 1)
        self.assertEqual(run["profile_version_id"], self.profile["profile_version_id"])
        self.assertEqual(run["profile_version"], 1)
        self.assertEqual(run["profile_name"], "Codex Worker")
        self.assertEqual(run["requested_by"], "operator")
        self.assertEqual(run["state"], "queued")
        self.assertEqual(run["version"], 1)
        self.assertEqual(run["source_revision"], "abc123")
        self.assertEqual(run["krab_session_id"], session_id)
        self.assertEqual(run["krab_claim_id"], claim_id)
        self.assertIsNone(run["started_at"])
        self.assertIsNone(run["finished_at"])
        self.assertEqual(run["created_at"], run["updated_at"])
        self.assertEqual(
            [
                {
                    key: event[key]
                    for key in (
                        "sequence",
                        "kind",
                        "from_state",
                        "to_state",
                        "metadata",
                    )
                }
                for event in run["events"]
            ],
            [
                {
                    "sequence": 1,
                    "kind": "run.created",
                    "from_state": None,
                    "to_state": "queued",
                    "metadata": {},
                }
            ],
        )

    def test_run_transitions_are_optimistic_and_audited(self) -> None:
        run = self.create_run()
        preparing = self.database.transition_run(
            run["id"], target=RunState.PREPARING, expected_version=1
        )
        self.assertEqual(preparing["version"], 2)
        self.assertIsNone(preparing["started_at"])
        with self.assertRaises(ConcurrentUpdateError):
            self.database.transition_run(
                run["id"], target=RunState.CANCELLED, expected_version=1
            )
        running = self.database.transition_run(
            run["id"],
            target=RunState.RUNNING,
            expected_version=2,
            metadata={
                "container_id": "b" * 64,
                "image_id": "sha256:" + "a" * 64,
            },
            source_revision="c" * 40,
        )
        self.assertIsNotNone(running["started_at"])
        self.assertEqual(running["source_revision"], "c" * 40)
        self.assertEqual(
            running["events"][-1]["metadata"],
            {
                "container_id": "b" * 64,
                "image_id": "sha256:" + "a" * 64,
            },
        )
        waiting = self.database.transition_run(
            run["id"], target=RunState.WAITING_FOR_INPUT, expected_version=3
        )
        with self.assertRaises(ValidationError):
            self.database.transition_run(
                run["id"], target=RunState.SUCCEEDED, expected_version=4
            )
        failed = self.database.transition_run(
            run["id"],
            target=RunState.FAILED,
            expected_version=4,
            reason="operator timeout",
        )
        self.assertEqual(failed["state"], "failed")
        self.assertIsNotNone(failed["finished_at"])
        self.assertEqual(len(failed["events"]), 5)
        self.assertEqual(
            failed["events"][-1]["metadata"], {"reason": "operator timeout"}
        )
        self.assertEqual(waiting["state"], "waiting_for_input")

    def test_requeueing_a_waiting_run_preserves_original_start_time(self) -> None:
        run = self.create_run()
        self.database.transition_run(
            run["id"], target=RunState.PREPARING, expected_version=1
        )
        running = self.database.transition_run(
            run["id"], target=RunState.RUNNING, expected_version=2
        )
        self.database.transition_run(
            run["id"], target=RunState.WAITING_FOR_INPUT, expected_version=3
        )
        requeued = self.database.transition_run(
            run["id"], target=RunState.QUEUED, expected_version=4
        )
        self.assertEqual(requeued["started_at"], running["started_at"])
        self.assertIsNone(requeued["finished_at"])
        self.assertEqual(requeued["state"], "queued")
        self.assertEqual(
            [event["sequence"] for event in requeued["events"]],
            [1, 2, 3, 4, 5],
        )

    def test_generic_transition_cannot_forge_provider_pause(self) -> None:
        run = self.create_run()
        preparing = self.database.transition_run(
            run["id"], target=RunState.PREPARING, expected_version=run["version"]
        )
        running = self.database.transition_run(
            run["id"],
            target=RunState.RUNNING,
            expected_version=preparing["version"],
        )

        with self.assertRaisesRegex(ConflictError, "provider recovery"):
            self.database.transition_run(
                run["id"],
                target=RunState.PAUSED,
                expected_version=running["version"],
            )

    def test_run_input_boundaries_and_validation_errors(self) -> None:
        accepted = self.database.create_run(
            project_key="box-project",
            profile_name="Codex Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision="x" * 512,
        )
        self.assertEqual(accepted["source_revision"], "x" * 512)
        with self.assertRaisesRegex(
            ValidationError, "source_revision exceeds 512 UTF-8 bytes"
        ):
            self.database.create_run(
                project_key="box-project",
                profile_name="Codex Worker",
                work_item_id=self.work_item["id"],
                requested_by=self.user["id"],
                source_revision="x" * 513,
            )
        for version in (True, 0, 1.0, "1"):
            with (
                self.subTest(version=version),
                self.assertRaisesRegex(
                    ValidationError, "expected_version must be a positive integer"
                ),
            ):
                self.database.transition_run(
                    accepted["id"],
                    target=RunState.CANCELLED,
                    expected_version=version,  # type: ignore[arg-type]
                )
        with self.assertRaisesRegex(ValidationError, "source revision.*running"):
            self.database.transition_run(
                accepted["id"],
                target=RunState.CANCELLED,
                expected_version=1,
                source_revision="a" * 40,
            )
        with self.assertRaisesRegex(ValidationError, "reserved.*reason"):
            self.database.transition_run(
                accepted["id"],
                target=RunState.PREPARING,
                expected_version=1,
                reason="start",
                metadata={"reason": "collision"},
            )
        for metadata, message in (
            ([], "must be an object"),
            ({1: "value"}, "must be an object"),
            ({"number": float("nan")}, "finite JSON"),
            ({"detail": "x" * (16 * 1024)}, "exceeds 16384"),
        ):
            with (
                self.subTest(metadata=metadata),
                self.assertRaisesRegex(ValidationError, message),
            ):
                self.database.transition_run(
                    accepted["id"],
                    target=RunState.PREPARING,
                    expected_version=1,
                    metadata=metadata,  # type: ignore[arg-type]
                )

    def test_execution_projection_rejects_missing_and_corrupt_records(self) -> None:
        with self.assertRaisesRegex(NotFoundError, "run not found"):
            self.database.get_run_execution("run_" + "f" * 32)
        run = self.create_run()
        connection = self.database.connect()
        self.addCleanup(connection.close)
        connection.execute("DROP TRIGGER project_context_immutable_update")
        connection.execute(
            "UPDATE project_context_versions SET context_json = 'not-json' "
            "WHERE id = ?",
            (run["context_version_id"],),
        )
        with self.assertRaisesRegex(StorageError, "pinned run data is invalid"):
            self.database.get_run_execution(run["id"])

    def test_inactive_projects_and_profiles_cannot_start_runs(self) -> None:
        connection = self.database.connect()
        self.addCleanup(connection.close)
        connection.execute(
            "UPDATE projects SET active = 0 WHERE id = ?", (self.project["id"],)
        )
        with self.assertRaisesRegex(NotFoundError, "active project not found"):
            self.create_run()
        connection.execute(
            "UPDATE projects SET active = 1 WHERE id = ?", (self.project["id"],)
        )
        connection.execute(
            "UPDATE agent_profiles SET active = 0 WHERE id = ?",
            (self.profile["id"],),
        )
        with self.assertRaisesRegex(NotFoundError, "active agent profile not found"):
            self.create_run()

    def test_inactive_users_cannot_author_control_plane_mutations(self) -> None:
        provider_spec = ProviderGrantSpec.from_mapping(
            {
                "provider": "openai",
                "adapter": "json_http",
                "destination": "https://api.openai.com/v1/responses",
                "method": "POST",
                "model": "gpt-test",
                "credential_header": "Authorization",
                "credential_prefix": "Bearer ",
                "static_headers": {},
                "request_bytes": 16384,
                "response_bytes": 32768,
                "timeout_ms": 10000,
                "max_concurrency": 1,
            }
        )
        existing_grant = self.database.create_provider_grant(
            name="existing-direct",
            spec=provider_spec,
            credential_ref="env:OPENAI_API_KEY",
            expires_at=None,
            created_by=self.user["id"],
        )
        with self.database.connect() as connection:
            connection.execute(
                "UPDATE users SET active = 0 WHERE id = ?", (self.user["id"],)
            )

        operations = {
            "provider grant creation": lambda: self.database.create_provider_grant(
                name="inactive-direct",
                spec=provider_spec,
                credential_ref="env:OPENAI_API_KEY",
                expires_at=None,
                created_by=self.user["id"],
            ),
            "provider grant revocation": lambda: self.database.revoke_provider_grant(
                existing_grant["name"],
                expected_version=existing_grant["version"],
                revoked_by=self.user["id"],
            ),
            "identity binding": lambda: self.database.store_krab_identity(
                self.instance["id"],
                KrabIdentity("usr_" + "9" * 32, "other_agent", 1),
                "file:/private/other-agent",
                user_id=self.user["id"],
            ),
            "project creation": lambda: self.database.create_project(
                instance_id=self.instance["id"],
                krab_project_id="prj_" + "9" * 32,
                krab_project_key="inactive-project",
                display_name="Inactive Project",
                context=self.context,
                created_by=self.user["id"],
            ),
            "project context": lambda: self.database.add_project_context(
                "box-project",
                ProjectContext.from_mapping({"purpose": "Inactive mutation"}),
                self.user["id"],
            ),
            "profile creation": lambda: self.database.create_agent_profile(
                name="Inactive Worker",
                spec=self.spec,
                krab_identity_id=self.identity["id"],
                created_by=self.user["id"],
            ),
            "profile version": lambda: self.database.add_agent_profile_version(
                name="Codex Worker",
                spec=self.spec,
                krab_identity_id=self.identity["id"],
                created_by=self.user["id"],
            ),
            "work-item import": lambda: self.database.import_work_item(
                project_key="box-project",
                envelope=WorkItemEnvelope.from_mapping(
                    {
                        "schema_version": 1,
                        "source": "manual",
                        "external_id": "inactive-import",
                        "source_updated_at": "2026-10-10T12:00:00Z",
                        "title": "Inactive import",
                        "brief": "This mutation must be rejected.",
                    }
                ),
                imported_by=self.user["id"],
            ),
            "run creation": self.create_run,
        }
        for label, operation in operations.items():
            with (
                self.subTest(operation=label),
                self.assertRaisesRegex(ConflictError, "inactive"),
            ):
                operation()

    def test_revision_and_event_rows_are_immutable(self) -> None:
        run = self.create_run()
        connection = self.database.connect()
        self.addCleanup(connection.close)
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE project_context_versions SET version = 99 WHERE id = ?",
                (self.project["context_version_id"],),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute("DELETE FROM run_events WHERE run_id = ?", (run["id"],))

    def test_duplicate_context_is_rejected(self) -> None:
        with self.assertRaisesRegex(ConflictError, "identical"):
            self.database.add_project_context(
                "box-project", self.context, self.user["id"]
            )

    def test_profile_from_another_krab_instance_cannot_run_project(self) -> None:
        other_instance = self.database.create_krab_instance(
            "Other Krab", "http://127.0.0.1:9876"
        )
        other_identity = self.database.store_krab_identity(
            other_instance["id"],
            KrabIdentity(
                user_id="usr_" + "3" * 32,
                handle="agent_two",
                token_version=1,
            ),
            "file:/private/krab-agent-two",
        )
        self.database.create_agent_profile(
            name="Wrong Instance",
            spec=self.spec,
            krab_identity_id=other_identity["id"],
            created_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "different Krab instances"):
            self.database.create_run(
                project_key="box-project",
                profile_name="Wrong Instance",
                work_item_id=self.work_item["id"],
                requested_by=self.user["id"],
            )

    def test_database_symlink_is_rejected(self) -> None:
        target = Path(self.temporary.name) / "target.db"
        target.write_bytes(b"")
        os.chmod(target, 0o600)
        link = Path(self.temporary.name) / "link.db"
        link.symlink_to(target)
        with self.assertRaisesRegex(Exception, "symlink"):
            Database(link).initialize()


if __name__ == "__main__":
    unittest.main()
