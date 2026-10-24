from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from magic_glass_box.db import (
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
)
from magic_glass_box.domain import ProjectContext, ValidationError
from tests.test_service_sidecars import operation_spec, sidecar_spec


class ProjectCapabilityBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        self.instance = self.database.create_krab_instance(
            "local", "http://localhost:8765"
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "1" * 32,
            krab_project_key="demo-project",
            display_name="Demo Project",
            context=ProjectContext.from_mapping({"purpose": "Prove routing"}),
            created_by=self.user["id"],
        )
        self.primary = self.database.register_service_sidecar(
            name="primary-sidecar",
            spec=sidecar_spec(self.root / "primary.sock"),
            created_by=self.user["id"],
        )
        self.secondary = self.database.register_service_sidecar(
            name="secondary-sidecar",
            spec=sidecar_spec(self.root / "secondary.sock", deployment_sha256="c" * 64),
            created_by=self.user["id"],
        )

    def bind(self, *, sidecar_name: str = "primary-sidecar") -> dict[str, object]:
        return self.database.bind_project_capability(
            project_key="demo-project",
            capability_name="change.open",
            capability_version=1,
            sidecar_name=sidecar_name,
            mapping_name="primary",
            bound_by=self.user["id"],
        )

    def test_binding_is_explicit_exact_and_idempotent(self) -> None:
        binding = self.bind()

        self.assertTrue(str(binding["id"]).startswith("cpb_"))
        self.assertEqual(binding["state"], "active")
        self.assertEqual(binding["version"], 1)
        self.assertEqual(binding["project"]["key"], "demo-project")
        self.assertEqual(
            binding["capability"],
            {"name": "change.open", "version": 1, "mode": "effect"},
        )
        self.assertEqual(binding["controller"]["sidecar_name"], "primary-sidecar")
        self.assertEqual(binding["controller"]["mapping"]["name"], "primary")
        self.assertEqual(binding["events"][0]["kind"], "capability.bound")
        self.assertEqual(self.bind(), binding)
        self.assertEqual(
            self.database.get_project_capability_binding(binding["id"]), binding
        )
        self.assertEqual(
            self.database.resolve_project_capability("demo-project", "change.open", 1),
            binding,
        )
        self.assertEqual(self.database.list_project_capability_bindings(), [binding])
        self.assertEqual(
            self.database.status()["counts"]["project_capability_bindings"], 1
        )

        with self.assertRaisesRegex(ConflictError, "already bound"):
            self.bind(sidecar_name="secondary-sidecar")

    def test_absence_unbind_and_revocation_fail_closed_without_fallback(self) -> None:
        with self.assertRaisesRegex(ConflictError, "project capability is unavailable"):
            self.database.resolve_project_capability("demo-project", "checks.read", 1)
        with self.assertRaisesRegex(ValidationError, "core-defined"):
            self.database.bind_project_capability(
                project_key="demo-project",
                capability_name="work.operator_extension",
                capability_version=1,
                sidecar_name="primary-sidecar",
                mapping_name="primary",
                bound_by=self.user["id"],
            )

        binding = self.bind()
        with self.assertRaises(ConcurrentUpdateError):
            self.database.unbind_project_capability(
                binding["id"],
                expected_version=2,
                unbound_by=self.user["id"],
            )
        unbound = self.database.unbind_project_capability(
            binding["id"],
            expected_version=1,
            unbound_by=self.user["id"],
        )
        self.assertEqual(unbound["state"], "unbound")
        self.assertEqual(unbound["version"], 2)
        self.assertEqual(
            [event["kind"] for event in unbound["events"]],
            ["capability.bound", "capability.unbound"],
        )
        self.assertEqual(self.database.list_project_capability_bindings(), [])
        self.assertEqual(
            self.database.list_project_capability_bindings(include_unbound=True),
            [unbound],
        )
        with self.assertRaisesRegex(ConflictError, "project capability is unavailable"):
            self.database.resolve_project_capability("demo-project", "change.open", 1)

        rebound = self.bind(sidecar_name="secondary-sidecar")
        self.assertNotEqual(rebound["id"], binding["id"])
        self.database.revoke_service_sidecar(
            "secondary-sidecar",
            expected_version=self.secondary["version"],
            revoked_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "project capability is unavailable"):
            self.database.resolve_project_capability("demo-project", "change.open", 1)

    def test_binding_rejects_missing_provider_support_or_mapping(self) -> None:
        with self.assertRaisesRegex(ConflictError, "does not advertise"):
            self.database.bind_project_capability(
                project_key="demo-project",
                capability_name="landing.invoke",
                capability_version=1,
                sidecar_name="primary-sidecar",
                mapping_name="primary",
                bound_by=self.user["id"],
            )

        with self.assertRaisesRegex(ConflictError, "^service sidecar is unavailable$"):
            self.database.bind_project_capability(
                project_key="demo-project",
                capability_name="change.open",
                capability_version=1,
                sidecar_name="missing-sidecar",
                mapping_name="primary",
                bound_by=self.user["id"],
            )

    def test_binding_queries_validate_ids_versions_and_project_scope(self) -> None:
        binding = self.bind()
        with self.assertRaisesRegex(
            ValidationError, "^project capability binding id is invalid$"
        ):
            self.database.get_project_capability_binding("invalid")
        with self.assertRaisesRegex(
            ValidationError, "^service operation id is invalid$"
        ):
            self.database.get_service_operation("invalid")
        with self.assertRaisesRegex(
            ValidationError, "^expected_version must be a positive integer$"
        ):
            self.database.unbind_project_capability(
                binding["id"], expected_version=0, unbound_by=self.user["id"]
            )

        second_project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="second-project",
            display_name="Second Project",
            context=ProjectContext.from_mapping({"purpose": "Prove project scoping"}),
            created_by=self.user["id"],
        )
        second_binding = self.database.bind_project_capability(
            project_key=second_project["krab_project_key"],
            capability_name="change.open",
            capability_version=1,
            sidecar_name="primary-sidecar",
            mapping_name="primary",
            bound_by=self.user["id"],
        )
        self.assertEqual(
            self.database.list_project_capability_bindings(project_key="demo-project"),
            [binding],
        )
        self.assertEqual(
            self.database.list_project_capability_bindings(
                project_key="second-project"
            ),
            [second_binding],
        )
        with self.assertRaisesRegex(
            NotFoundError, "^project not found: missing-project$"
        ):
            self.database.list_project_capability_bindings(
                project_key="missing-project"
            )

    def test_resolution_rejects_unknown_and_inactive_projects(self) -> None:
        with self.assertRaisesRegex(
            ConflictError, "^project capability is unavailable$"
        ):
            self.database.resolve_project_capability(
                "missing-project", "change.open", 1
            )

        self.bind()
        with closing(self.database.connect()) as connection, connection:
            connection.execute(
                "UPDATE projects SET active = 0 WHERE id = ?", (self.project["id"],)
            )
        with self.assertRaisesRegex(
            ConflictError, "^project capability is unavailable$"
        ):
            self.database.resolve_project_capability("demo-project", "change.open", 1)

    def test_bound_operation_is_atomically_pinned_and_unbind_stops_new_work(
        self,
    ) -> None:
        binding = self.bind()
        operation = self.database.create_service_operation(
            spec=operation_spec(connector_name="primary-sidecar"),
            created_by=self.user["id"],
            project_capability_binding_id=binding["id"],
        )
        self.assertEqual(operation["project_capability_binding_id"], binding["id"])
        self.assertEqual(
            self.database.status()["counts"]["service_operation_capability_bindings"],
            1,
        )
        self.assertEqual(
            self.database.create_service_operation(
                spec=operation_spec(connector_name="primary-sidecar"),
                created_by=self.user["id"],
                project_capability_binding_id=binding["id"],
            ),
            operation,
        )
        with (
            closing(self.database.connect()) as connection,
            self.assertRaises(sqlite3.IntegrityError),
        ):
            connection.execute(
                "UPDATE service_operation_capability_bindings "
                "SET project_capability_binding_id = ? WHERE operation_id = ?",
                (binding["id"], operation["id"]),
            )

        self.database.unbind_project_capability(
            binding["id"],
            expected_version=1,
            unbound_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "project capability is unavailable"):
            self.database.create_service_operation(
                spec=operation_spec(
                    connector_name="primary-sidecar",
                    idempotency_key="open-after-unbind",
                    target={
                        **operation_spec().target.to_dict(),
                        "resource_id": "candidate-after-unbind",
                    },
                ),
                created_by=self.user["id"],
                project_capability_binding_id=binding["id"],
            )

        existing = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="invoke",
            owner_id="supervisor-one",
            expected_version=1,
            lease_seconds=30,
        )
        self.assertEqual(existing["operation"]["state"], "dispatching")

    def test_bound_operation_must_match_exact_controller_and_capability(self) -> None:
        binding = self.bind()
        with self.assertRaisesRegex(
            ConflictError,
            "^project capability binding does not authorize operation$",
        ):
            self.database.create_service_operation(
                spec=operation_spec(
                    connector_name="secondary-sidecar",
                    deployment_sha256="c" * 64,
                    idempotency_key="wrong-controller",
                ),
                created_by=self.user["id"],
                project_capability_binding_id=binding["id"],
            )
        with self.assertRaisesRegex(
            ConflictError,
            "^project capability binding does not authorize operation$",
        ):
            self.database.create_service_operation(
                spec=operation_spec(
                    connector_name="primary-sidecar",
                    capability={
                        "name": "checks.read",
                        "version": 1,
                        "mode": "read",
                    },
                    idempotency_key="wrong-capability",
                    parameters={"check_suite": "required"},
                ),
                created_by=self.user["id"],
                project_capability_binding_id=binding["id"],
            )
        with self.assertRaisesRegex(ConflictError, "mapping"):
            self.database.bind_project_capability(
                project_key="demo-project",
                capability_name="change.open",
                capability_version=1,
                sidecar_name="primary-sidecar",
                mapping_name="missing",
                bound_by=self.user["id"],
            )

    def test_schema_twenty_four_migrates_schema_twenty_three_once(self) -> None:
        with closing(self.database.connect()) as connection, connection:
            connection.execute("DROP TABLE workflow_completions")
            connection.execute("DROP TABLE workflow_provider_targets")
            connection.execute("DROP TABLE workflow_landing_bindings")
            connection.execute("DROP TABLE workflow_rulings")
            connection.execute("DROP TABLE integration_review_verdicts")
            connection.execute("DROP TABLE workflow_gate_bindings")
            connection.execute("DROP TABLE workflow_run_bindings")
            connection.execute("DROP TABLE workflow_execution_events")
            connection.execute("DROP TABLE workflow_executions")
            connection.execute("DROP TABLE workflow_configuration_versions")
            connection.execute("DROP TABLE workflow_configurations")
            connection.execute("DROP TABLE workflow_template_definitions")
            connection.execute("DROP TABLE integration_gate_maintainer_change_runs")
            connection.execute("DROP TABLE integration_maintainer_reviews")
            connection.execute("DROP TABLE integration_gate_change_runs")
            connection.execute("DROP TABLE integration_provider_landings")
            connection.execute("DROP TABLE service_operation_capability_bindings")
            connection.execute("DROP TABLE project_capability_binding_events")
            connection.execute("DROP TABLE project_capability_bindings")
            connection.execute("PRAGMA user_version = 23")

        first = self.database.initialize()
        second = self.database.initialize()

        self.assertEqual(first["schema_version"], 30)
        self.assertEqual(second, first)
        with closing(self.database.connect()) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table'"
                )
            }
        self.assertTrue(
            {
                "project_capability_bindings",
                "project_capability_binding_events",
                "service_operation_capability_bindings",
            }.issubset(tables)
        )

    def test_binding_identity_and_events_are_tamper_evident(self) -> None:
        binding = self.bind()
        with closing(self.database.connect()) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE project_capability_bindings "
                    "SET mapping_name = 'other' WHERE id = ?",
                    (binding["id"],),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM project_capability_binding_events "
                    "WHERE binding_id = ?",
                    (binding["id"],),
                )


if __name__ == "__main__":
    unittest.main()
