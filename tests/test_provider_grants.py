from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

from magic_glass_box.db import (
    PROVIDER_DISPATCH_SCHEMA,
    SCHEMA,
    SCHEMA_VERSION,
    ConcurrentUpdateError,
    ConflictError,
    Database,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    ProjectContext,
    ProviderGrantSpec,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity


def provider_policy() -> ProviderGrantSpec:
    return ProviderGrantSpec.from_mapping(
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
            "max_concurrency": 2,
        }
    )


class ProviderGrantDatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Database(Path(self.temporary.name) / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Local Operator")
        instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity(
                user_id="usr_" + "1" * 32,
                handle="agent_one",
                token_version=1,
            ),
            "file:/private/krab-agent-one",
        )
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping({"purpose": "Test grants"}),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "provider-grant-test",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Test a provider grant",
                    "brief": "Exercise a run-scoped provider authority.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.spec = provider_policy()

    def add_grant(self, *, expires_at: str | None = None) -> dict[str, object]:
        return self.database.create_provider_grant(
            name="openai-main",
            spec=self.spec,
            credential_ref="file:/private/openai-main",
            expires_at=expires_at,
            created_by=self.user["id"],
        )

    def add_profile(self) -> dict[str, object]:
        return self.database.create_agent_profile(
            name="Provider Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc",
                command=["codex", "exec"],
                credential_names=["openai-main"],
                gateway_capabilities=[GatewayCapability.PROVIDER_INVOKE],
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )

    def add_run(self) -> dict[str, object]:
        return self.database.create_run(
            project_key="box-project",
            profile_name="Provider Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
        )

    def test_grant_is_immutable_audited_and_public_views_hide_reference(self) -> None:
        grant = self.add_grant()
        self.assertTrue(str(grant["id"]).startswith("pgr_"))
        self.assertEqual(grant["name"], "openai-main")
        self.assertEqual(grant["state"], "active")
        self.assertEqual(grant["version"], 1)
        self.assertEqual(grant["policy"], self.spec.to_dict())
        self.assertEqual(grant["content_sha256"], self.spec.digest)
        self.assertNotIn("credential_ref", grant)
        self.assertNotIn("private", str(grant))
        self.assertEqual(
            [event["kind"] for event in grant["events"]],
            ["provider_grant.created"],
        )
        self.assertEqual(self.database.status()["counts"]["provider_grants"], 1)

        with closing(self.database.connect()) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE provider_grants SET policy_json = '{}' WHERE id = ?",
                    (grant["id"],),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM provider_grant_events WHERE provider_grant_id = ?",
                    (grant["id"],),
                )

    def test_run_pins_exact_grant_and_exposes_only_named_references(self) -> None:
        grant = self.add_grant()
        self.add_profile()
        run = self.add_run()

        execution = self.database.get_run_execution(str(run["id"]))
        self.assertEqual(
            execution["credential_references"],
            {
                "krab": "file:/private/krab-agent-one",
                "openai-main": "file:/private/openai-main",
            },
        )
        authority = self.database.get_run_provider_grant(str(run["id"]), "openai-main")
        self.assertEqual(authority["id"], grant["id"])
        self.assertEqual(authority["credential_ref"], "file:/private/openai-main")
        self.assertEqual(authority["policy"], self.spec.to_dict())

        visible = self.database.get_run(str(run["id"]))
        self.assertEqual(
            visible["provider_grants"],
            [
                {
                    "authority_name": "openai-main",
                    "grant_id": grant["id"],
                    "name": "openai-main",
                    "provider": "openai",
                    "model": "gpt-test",
                    "state": "active",
                    "expires_at": None,
                }
            ],
        )
        self.assertNotIn("credential_ref", str(visible))
        self.assertNotIn("destination", str(visible["provider_grants"]))

    def test_missing_revoked_and_expired_grants_fail_closed(self) -> None:
        with self.assertRaisesRegex(ConflictError, "provider grant.*unavailable"):
            self.add_profile()

        self.add_grant()
        self.add_profile()
        run = self.add_run()
        revoked = self.database.revoke_provider_grant(
            "openai-main",
            expected_version=1,
            revoked_by=self.user["id"],
        )
        self.assertEqual(revoked["state"], "revoked")
        self.assertEqual(revoked["version"], 2)
        self.assertEqual(
            [event["kind"] for event in revoked["events"]],
            ["provider_grant.created", "provider_grant.revoked"],
        )
        with self.assertRaisesRegex(ConflictError, "provider grant.*unavailable"):
            self.database.get_run_provider_grant(str(run["id"]), "openai-main")
        with self.assertRaisesRegex(ConcurrentUpdateError, "version is 2"):
            self.database.revoke_provider_grant(
                "openai-main",
                expected_version=1,
                revoked_by=self.user["id"],
            )

        future_name = "future-provider"
        expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        self.database.create_provider_grant(
            name=future_name,
            spec=self.spec,
            credential_ref="file:/private/future-provider",
            expires_at=expires_at,
            created_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "provider grant.*unavailable"):
            self.database.create_agent_profile(
                name="Expired Worker",
                spec=AgentProfileSpec.create(
                    image="agent@sha256:abc",
                    command=["codex", "exec"],
                    credential_names=[future_name],
                    gateway_capabilities=[GatewayCapability.PROVIDER_INVOKE],
                ),
                krab_identity_id=self.identity["id"],
                created_by=self.user["id"],
            )

    def test_invocation_audit_stores_metadata_but_no_request_or_response(self) -> None:
        grant = self.add_grant()
        self.add_profile()
        run = self.add_run()
        record = self.database.record_provider_invocation(
            run_id=str(run["id"]),
            provider_grant_id=str(grant["id"]),
            outcome="succeeded",
            request_bytes=42,
            response_bytes=84,
            http_status=200,
            error_code=None,
            started_at="2026-10-09T12:00:00Z",
        )
        self.assertEqual(record["outcome"], "succeeded")
        self.assertEqual(record["request_bytes"], 42)
        self.assertEqual(record["response_bytes"], 84)
        self.assertNotIn("body", str(record))
        self.assertNotIn("credential", str(record))
        visible = self.database.get_run(str(run["id"]))
        self.assertEqual(visible["provider_invocations"], [record])


class ProviderGrantMigrationTests(unittest.TestCase):
    def test_schema_three_migrates_once_without_losing_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-three.db"
            connection = sqlite3.connect(path)
            # The first three schemas are the current SCHEMA prefix before the
            # provider-grant migration is appended.
            provider_start = SCHEMA.index("CREATE TABLE provider_grants")
            connection.executescript(SCHEMA[:provider_start])
            connection.execute(
                "INSERT INTO users "
                "(id, handle, display_name, active, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?)",
                ("boxu_" + "1" * 32, "operator", "Operator", "then", "then"),
            )
            connection.execute("PRAGMA user_version = 3")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()
            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
            self.assertEqual(second, first)
            self.assertEqual(first["counts"]["users"], 1)
            self.assertEqual(first["counts"]["provider_grants"], 0)
            with closing(database.connect()) as migrated:
                tables = {
                    row[0]
                    for row in migrated.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table'"
                    )
                }
            self.assertIn("provider_grants", tables)
            self.assertIn("run_provider_grants", tables)
            self.assertIn("provider_invocations", tables)

    def test_schema_twelve_adds_provider_dispatch_reservations_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-eleven.db"
            connection = sqlite3.connect(path)
            dispatch_start = SCHEMA.index(PROVIDER_DISPATCH_SCHEMA)
            connection.executescript(SCHEMA[:dispatch_start])
            connection.execute("PRAGMA user_version = 11")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
            self.assertEqual(second, first)
            with closing(database.connect()) as migrated:
                table = migrated.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table' "
                    "AND name = 'provider_dispatch_reservations'"
                ).fetchone()
                index = migrated.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'index' "
                    "AND name = 'provider_dispatch_grant_expiry'"
                ).fetchone()
            self.assertIsNotNone(table)
            self.assertIsNotNone(index)


if __name__ == "__main__":
    unittest.main()
