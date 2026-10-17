from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from magic_glass_box.db import (
    INFERENCE_CONNECTOR_SCHEMA,
    SCHEMA,
    SCHEMA_VERSION,
    ConcurrentUpdateError,
    ConflictError,
    Database,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    ProviderGrantSpec,
)
from magic_glass_box.inference import InferenceConnectorSpec, InferenceGrantSpec
from magic_glass_box.krab import KrabIdentity


def connector_spec(socket_path: Path, **overrides: object) -> InferenceConnectorSpec:
    value: dict[str, object] = {
        "schema_version": 1,
        "connector_protocol_version": 1,
        "deployment_sha256": "a" * 64,
        "transport": {"kind": "unix", "socket_path": str(socket_path)},
        "routes": [
            {
                "name": "chatgpt-main",
                "provider": "openai",
                "protocol": "openai-responses",
                "models": ["gpt-test", "gpt-test-mini"],
                "credential_scope_sha256": "b" * 64,
            }
        ],
        "request_bytes": 32768,
        "response_bytes": 65536,
        "timeout_ms": 30000,
        "max_concurrency": 2,
    }
    value.update(overrides)
    return InferenceConnectorSpec.from_mapping(value)


def grant_spec(**overrides: object) -> InferenceGrantSpec:
    value: dict[str, object] = {
        "schema_version": 1,
        "connector_name": "openai-personal",
        "deployment_sha256": "a" * 64,
        "route": "chatgpt-main",
        "credential_scope_sha256": "b" * 64,
        "provider": "openai",
        "protocol": "openai-responses",
        "model": "gpt-test",
        "request_bytes": 16384,
        "response_bytes": 32768,
        "timeout_ms": 10000,
        "max_concurrency": 1,
    }
    value.update(overrides)
    return InferenceGrantSpec.from_mapping(value)


def direct_spec() -> ProviderGrantSpec:
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
            "request_bytes": 16384,
            "response_bytes": 32768,
            "timeout_ms": 10000,
            "max_concurrency": 1,
        }
    )


class InferenceConnectorStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Local Operator")
        self.connector_spec = connector_spec(self.root / "connector.sock")

    def register(self, *, name: str = "openai-personal") -> dict[str, object]:
        return self.database.register_inference_connector(
            name=name,
            spec=self.connector_spec,
            created_by=self.user["id"],
        )

    def test_registration_is_immutable_audited_and_contains_no_credential(self) -> None:
        connector = self.register()

        self.assertTrue(str(connector["id"]).startswith("ifc_"))
        self.assertEqual(connector["name"], "openai-personal")
        self.assertEqual(connector["state"], "active")
        self.assertEqual(connector["version"], 1)
        self.assertEqual(connector["spec"], self.connector_spec.to_dict())
        self.assertEqual(connector["content_sha256"], self.connector_spec.digest)
        self.assertEqual(
            [event["kind"] for event in connector["events"]],
            ["inference_connector.created"],
        )
        self.assertEqual(
            self.database.list_inference_connectors(),
            [connector],
        )
        self.assertEqual(self.database.status()["counts"]["inference_connectors"], 1)

        serialized = json.dumps(connector, sort_keys=True)
        for forbidden in ("api_key", "access_token", "refresh_token", "secret"):
            self.assertNotIn(forbidden, serialized)

        with closing(self.database.connect()) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE inference_connectors SET spec_json = '{}' WHERE id = ?",
                    (connector["id"],),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM inference_connector_events "
                    "WHERE inference_connector_id = ?",
                    (connector["id"],),
                )

    def test_connector_grant_binds_exact_registration_without_credential_ref(
        self,
    ) -> None:
        connector = self.register()
        spec = grant_spec()
        grant = self.database.create_connector_provider_grant(
            name="openai-main",
            spec=spec,
            expires_at=None,
            created_by=self.user["id"],
        )

        self.assertEqual(grant["grant_kind"], "connector")
        self.assertEqual(grant["policy"], spec.to_dict())
        self.assertNotIn("credential_ref", grant)
        self.assertEqual(
            grant["connector"],
            {
                "id": connector["id"],
                "name": "openai-personal",
                "state": "active",
                "version": 1,
                "connector_protocol_version": 1,
                "deployment_sha256": "a" * 64,
                "route": "chatgpt-main",
                "credential_scope_sha256": "b" * 64,
                "provider": "openai",
                "protocol": "openai-responses",
                "model": "gpt-test",
            },
        )
        self.assertEqual(
            self.database.status()["counts"]["provider_connector_bindings"], 1
        )

        with closing(self.database.connect()) as connection:
            stored = connection.execute(
                "SELECT grant_kind, credential_ref FROM provider_grants WHERE id = ?",
                (grant["id"],),
            ).fetchone()
            self.assertEqual(
                dict(stored), {"grant_kind": "connector", "credential_ref": ""}
            )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE provider_connector_bindings SET route = 'other' "
                    "WHERE provider_grant_id = ?",
                    (grant["id"],),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE provider_grants SET grant_kind = 'direct' WHERE id = ?",
                    (grant["id"],),
                )

    def test_connector_grant_enters_run_authority_after_cutover(self) -> None:
        self.register()
        self.database.create_connector_provider_grant(
            name="openai-main",
            spec=grant_spec(),
            expires_at=None,
            created_by=self.user["id"],
        )
        instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "1" * 32, "agent_one", 1),
            "file:/private/krab.token",
            user_id=self.user["id"],
        )

        profile = self.database.create_agent_profile(
            name="Connector Worker",
            spec=AgentProfileSpec.create(
                image="worker@sha256:abc",
                command=["worker"],
                credential_names=["openai-main"],
                gateway_capabilities=[GatewayCapability.PROVIDER_INVOKE],
            ),
            krab_identity_id=identity["id"],
            created_by=self.user["id"],
        )

        self.assertEqual(profile["spec"]["credential_names"], ["openai-main"])

    def test_incompatible_or_inactive_connector_fails_without_partial_grant(
        self,
    ) -> None:
        self.register()
        for override, message in (
            ({"deployment_sha256": "d" * 64}, "deployment"),
            ({"route": "other"}, "route"),
            ({"model": "unapproved"}, "model"),
            ({"request_bytes": 65536}, "request_bytes"),
        ):
            with (
                self.subTest(override=override),
                self.assertRaisesRegex(ConflictError, message),
            ):
                self.database.create_connector_provider_grant(
                    name="openai-main",
                    spec=grant_spec(**override),
                    expires_at=None,
                    created_by=self.user["id"],
                )
        self.assertEqual(self.database.status()["counts"]["provider_grants"], 0)

        revoked = self.database.revoke_inference_connector(
            "openai-personal",
            expected_version=1,
            revoked_by=self.user["id"],
        )
        self.assertEqual(revoked["state"], "revoked")
        self.assertEqual(revoked["version"], 2)
        self.assertEqual(
            [event["kind"] for event in revoked["events"]],
            ["inference_connector.created", "inference_connector.revoked"],
        )
        with self.assertRaisesRegex(ConflictError, "connector is unavailable"):
            self.database.create_connector_provider_grant(
                name="openai-main",
                spec=grant_spec(),
                expires_at=None,
                created_by=self.user["id"],
            )
        with self.assertRaisesRegex(ConcurrentUpdateError, "version is 2"):
            self.database.revoke_inference_connector(
                "openai-personal",
                expected_version=1,
                revoked_by=self.user["id"],
            )

    def test_inactive_users_cannot_manage_connectors_or_connector_grants(self) -> None:
        connector = self.register()
        with self.database.connect() as connection:
            connection.execute(
                "UPDATE users SET active = 0 WHERE id = ?", (self.user["id"],)
            )

        operations = {
            "register": lambda: self.database.register_inference_connector(
                name="other-connector",
                spec=connector_spec(self.root / "other.sock"),
                created_by=self.user["id"],
            ),
            "revoke": lambda: self.database.revoke_inference_connector(
                connector["name"],
                expected_version=connector["version"],
                revoked_by=self.user["id"],
            ),
            "grant": lambda: self.database.create_connector_provider_grant(
                name="inactive-grant",
                spec=grant_spec(),
                expires_at=None,
                created_by=self.user["id"],
            ),
        }
        for label, operation in operations.items():
            with (
                self.subTest(operation=label),
                self.assertRaisesRegex(ConflictError, "inactive"),
            ):
                operation()

    def test_rotation_creates_new_identity_and_does_not_rebind_old_grant(self) -> None:
        old = self.register()
        old_grant = self.database.create_connector_provider_grant(
            name="openai-main",
            spec=grant_spec(),
            expires_at=None,
            created_by=self.user["id"],
        )
        rotated_spec = connector_spec(
            self.root / "connector-rotated.sock",
            deployment_sha256="d" * 64,
            routes=[
                {
                    "name": "chatgpt-main",
                    "provider": "openai",
                    "protocol": "openai-responses",
                    "models": ["gpt-test"],
                    "credential_scope_sha256": "e" * 64,
                }
            ],
        )
        rotated = self.database.register_inference_connector(
            name="openai-personal-rotated",
            spec=rotated_spec,
            created_by=self.user["id"],
        )
        new_grant = self.database.create_connector_provider_grant(
            name="openai-rotated",
            spec=grant_spec(
                connector_name="openai-personal-rotated",
                deployment_sha256="d" * 64,
                credential_scope_sha256="e" * 64,
            ),
            expires_at=None,
            created_by=self.user["id"],
        )

        self.assertNotEqual(old["id"], rotated["id"])
        self.assertEqual(old_grant["connector"]["id"], old["id"])
        self.assertEqual(new_grant["connector"]["id"], rotated["id"])
        self.assertEqual(
            self.database.get_provider_grant("openai-main")["connector"]["id"],
            old["id"],
        )

    def test_distinct_connector_instances_may_share_one_deployment(self) -> None:
        first = self.register()
        second_spec = connector_spec(
            self.root / "second.sock",
            routes=[
                {
                    "name": "chatgpt-secondary",
                    "provider": "openai",
                    "protocol": "openai-responses",
                    "models": ["gpt-test"],
                    "credential_scope_sha256": "c" * 64,
                }
            ],
        )

        second = self.database.register_inference_connector(
            name="openai-secondary",
            spec=second_spec,
            created_by=self.user["id"],
        )

        self.assertNotEqual(first["id"], second["id"])
        self.assertEqual(first["deployment_sha256"], second["deployment_sha256"])


class InferenceConnectorMigrationTests(unittest.TestCase):
    def test_schema_fourteen_migrates_direct_grants_and_connector_authority_once(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-fourteen.db"
            connection = sqlite3.connect(path)
            connector_start = SCHEMA.index(INFERENCE_CONNECTOR_SCHEMA)
            schema_fourteen = SCHEMA[:connector_start]
            connection.executescript(schema_fourteen)
            user_id = "boxu_" + "1" * 32
            connection.execute(
                "INSERT INTO users "
                "(id, handle, display_name, active, created_at, updated_at) "
                "VALUES (?, 'operator', 'Operator', 1, 'then', 'then')",
                (user_id,),
            )
            spec = direct_spec()
            connection.execute(
                "INSERT INTO provider_grants "
                "(id, name, state, version, policy_json, content_sha256, "
                "credential_ref, created_by, created_at, updated_at) "
                "VALUES (?, 'legacy-direct', 'active', 1, ?, ?, ?, ?, 'then', 'then')",
                (
                    "pgr_" + "2" * 32,
                    spec.canonical,
                    spec.digest,
                    "file:/private/provider",
                    user_id,
                ),
            )
            connection.execute(
                "INSERT INTO provider_grant_events "
                "(provider_grant_id, sequence, kind, actor_id, created_at) "
                "VALUES (?, 1, 'provider_grant.created', ?, 'then')",
                ("pgr_" + "2" * 32, user_id),
            )
            connection.execute("PRAGMA user_version = 14")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(SCHEMA_VERSION, 18)
            self.assertEqual(first["schema_version"], 18)
            self.assertEqual(second, first)
            legacy = database.get_provider_grant("legacy-direct")
            self.assertEqual(legacy["grant_kind"], "direct")
            self.assertEqual(legacy["policy"], spec.to_dict())
            with closing(database.connect()) as migrated:
                tables = {
                    row[0]
                    for row in migrated.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table'"
                    )
                }
            self.assertIn("inference_connectors", tables)
            self.assertIn("provider_connector_bindings", tables)


if __name__ == "__main__":
    unittest.main()
