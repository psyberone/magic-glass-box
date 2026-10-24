from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from magic_glass_box.db import (
    SCHEMA,
    WORKFLOW_DIRECTOR_SCHEMA,
    WORKFLOW_TEMPLATE_SCHEMA,
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
    StorageError,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    SupervisionPolicy,
    ValidationError,
    WorkItemEnvelope,
    WorkLogEntry,
    canonical_json,
    content_sha256,
)
from magic_glass_box.gates import ReviewSeparation
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import ResultManifest
from magic_glass_box.service import ControlPlane
from magic_glass_box.workflows import (
    IMPLEMENTATION_TEMPLATE,
    IMPLEMENTATION_TEMPLATE_ID,
    IMPLEMENTATION_TEMPLATE_REVISION,
    WorkflowConfigurationSpec,
)


class WorkflowConfigurationSpecTests(unittest.TestCase):
    @staticmethod
    def value(**overrides: object) -> dict[str, object]:
        return {
            "schema_version": 1,
            "review_separation": "different_profile_and_identity",
            "maintainer_approval": True,
            "validations": [
                {
                    "name": "unit",
                    "image_id": "sha256:" + "1" * 64,
                    "command": ["python", "-m", "unittest"],
                }
            ],
            "max_correction_rounds": 3,
            "landing_mode": "manual",
            **overrides,
        }

    def test_configuration_normalizes_and_hashes_bounded_policy(self) -> None:
        spec = WorkflowConfigurationSpec.from_mapping(self.value())

        self.assertEqual(spec.to_dict(), self.value())
        self.assertEqual(len(spec.digest), 64)
        self.assertEqual(
            WorkflowConfigurationSpec.from_mapping(spec.to_dict()).digest,
            spec.digest,
        )

    def test_configuration_rejects_unknown_missing_and_invalid_policy(self) -> None:
        missing = self.value()
        missing.pop("landing_mode")
        duplicate_validations = self.value()["validations"] * 2
        too_many_validations = self.value()["validations"] * 33
        cases = (
            None,
            {**self.value(), "unexpected": True},
            missing,
            self.value(schema_version=True),
            self.value(schema_version=2),
            self.value(review_separation="same_agent"),
            self.value(maintainer_approval=1),
            self.value(validations={}),
            self.value(validations=too_many_validations),
            self.value(validations=[{"name": "broken"}]),
            self.value(validations=duplicate_validations),
            self.value(max_correction_rounds=True),
            self.value(max_correction_rounds=0),
            self.value(max_correction_rounds=9),
            self.value(landing_mode="automatic"),
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                WorkflowConfigurationSpec.from_mapping(value)  # type: ignore[arg-type]


class WorkflowStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Database(Path(self.temporary.name) / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Local Operator")
        self.instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.producer_identity = self.create_identity("producer", "1")
        self.reviewer_identity = self.create_identity("reviewer", "2")
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="workflow-project",
            display_name="Workflow Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Prove bounded workflows",
                    "instructions": ["Keep authority explicit"],
                    "test_commands": [["python", "-m", "unittest"]],
                }
            ),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="workflow-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "workflow-1",
                    "source_updated_at": "2026-10-23T12:00:00Z",
                    "title": "Exercise the implementation workflow",
                    "brief": "Prove immutable workflow authority.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.producer = self.create_profile(
            "Producer", self.producer_identity["id"], workspace_mode="copy_on_write"
        )
        self.reviewer = self.create_profile(
            "Reviewer", self.reviewer_identity["id"], workspace_mode="read_only"
        )

    def create_identity(self, handle: str, fill: str) -> dict[str, object]:
        return self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity(
                user_id="usr_" + fill * 32,
                handle=handle,
                token_version=1,
            ),
            f"file:/private/{handle}",
        )

    def create_profile(
        self, name: str, identity_id: str, *, workspace_mode: str
    ) -> dict[str, object]:
        return self.database.create_agent_profile(
            name=name,
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc",
                command=["agent", "run"],
                workspace_mode=workspace_mode,
                network_mode="none",
            ),
            krab_identity_id=identity_id,
            created_by=self.user["id"],
        )

    @staticmethod
    def configuration_spec(**overrides: object) -> WorkflowConfigurationSpec:
        value = {
            "schema_version": 1,
            "review_separation": "different_profile_and_identity",
            "maintainer_approval": True,
            "validations": [],
            "max_correction_rounds": 3,
            "landing_mode": "manual",
            **overrides,
        }
        return WorkflowConfigurationSpec.from_mapping(value)

    def create_configuration(self, **overrides: object) -> dict[str, object]:
        return self.database.create_workflow_configuration(
            project_key="workflow-project",
            name="default",
            spec=self.configuration_spec(**overrides),
            producer_profile_name="Producer",
            reviewer_profile_name="Reviewer",
            created_by=self.user["id"],
        )

    def start(
        self,
        *,
        configuration_version: int = 1,
        context_version_id: str | None = None,
        work_item_version_id: str | None = None,
        thread_generation: int = 1,
        thread_version: int = 1,
        idempotency_key: str = "start-workflow-1",
    ) -> dict[str, object]:
        return self.database.start_workflow_execution(
            project_key="workflow-project",
            configuration_name="default",
            work_item_id=self.work_item["id"],
            source_revision="a" * 40,
            expected_configuration_version=configuration_version,
            expected_context_version_id=(
                context_version_id or self.project["context_version_id"]
            ),
            expected_work_item_version_id=(
                work_item_version_id or self.work_item["work_item_version_id"]
            ),
            expected_thread_generation=thread_generation,
            expected_thread_version=thread_version,
            idempotency_key=idempotency_key,
            requested_by=self.user["id"],
        )

    def test_configuration_pins_definition_policy_and_role_revisions(self) -> None:
        configuration = self.create_configuration()

        self.assertEqual(
            set(configuration),
            {
                "id",
                "project_id",
                "project_key",
                "name",
                "workflow_configuration_version_id",
                "version",
                "content_sha256",
                "template",
                "spec",
                "roles",
                "created_by",
                "created_at",
                "configuration_created_at",
            },
        )
        self.assertEqual(configuration["project_id"], self.project["id"])
        self.assertEqual(configuration["name"], "default")
        self.assertEqual(configuration["project_key"], "workflow-project")
        self.assertEqual(configuration["version"], 1)
        self.assertEqual(
            configuration["template"],
            {
                "id": IMPLEMENTATION_TEMPLATE_ID,
                "key": "implementation-v1",
                "version": 1,
                "revision": IMPLEMENTATION_TEMPLATE_REVISION,
                "built_in": True,
                "created_at": "2026-10-23T00:00:00.000000+00:00",
            },
        )
        self.assertEqual(configuration["spec"], self.configuration_spec().to_dict())
        expected_manifest = {
            "schema_version": 1,
            "template_definition_id": IMPLEMENTATION_TEMPLATE_ID,
            "spec": self.configuration_spec().to_dict(),
            "roles": {
                "producer": self.producer["profile_version_id"],
                "reviewer": self.reviewer["profile_version_id"],
            },
        }
        self.assertEqual(
            configuration["content_sha256"],
            content_sha256(canonical_json(expected_manifest)),
        )
        self.assertEqual(
            configuration["roles"],
            {
                "producer": {
                    "profile_id": self.producer["id"],
                    "profile_name": "Producer",
                    "profile_version_id": self.producer["profile_version_id"],
                    "profile_version": 1,
                    "workspace_mode": "copy_on_write",
                    "krab_identity_id": self.producer_identity["id"],
                    "krab_handle": "producer",
                },
                "reviewer": {
                    "profile_id": self.reviewer["id"],
                    "profile_name": "Reviewer",
                    "profile_version_id": self.reviewer["profile_version_id"],
                    "profile_version": 1,
                    "workspace_mode": "read_only",
                    "krab_identity_id": self.reviewer_identity["id"],
                    "krab_handle": "reviewer",
                },
            },
        )
        self.assertEqual(configuration["created_by"], "operator")
        self.assertEqual(
            configuration["created_at"], configuration["configuration_created_at"]
        )
        self.assertRegex(configuration["id"], r"^wfc_[0-9a-f]{32}$")
        self.assertRegex(
            configuration["workflow_configuration_version_id"],
            r"^wfv_[0-9a-f]{32}$",
        )
        self.assertRegex(
            configuration["created_at"],
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00$",
        )
        definitions = self.database.list_workflow_template_definitions()
        self.assertEqual(
            definitions,
            [
                {
                    "id": IMPLEMENTATION_TEMPLATE_ID,
                    "key": "implementation-v1",
                    "version": 1,
                    "revision": IMPLEMENTATION_TEMPLATE_REVISION,
                    "built_in": True,
                    "definition": IMPLEMENTATION_TEMPLATE,
                    "created_at": "2026-10-23T00:00:00.000000+00:00",
                }
            ],
        )
        self.assertNotIn("credential_ref", repr(configuration))
        counts = self.database.status()["counts"]
        self.assertEqual(counts["workflow_template_definitions"], 1)
        self.assertEqual(counts["workflow_configurations"], 1)
        self.assertEqual(counts["workflow_configuration_versions"], 1)

    def test_configuration_rejects_role_or_workspace_boundary_violations(self) -> None:
        connection = self.database.connect()
        self.addCleanup(connection.close)
        connection.execute(
            "UPDATE projects SET active = 0 WHERE id = ?", (self.project["id"],)
        )
        with self.assertRaisesRegex(NotFoundError, "active project"):
            self.create_configuration()
        connection.execute(
            "UPDATE projects SET active = 1 WHERE id = ?", (self.project["id"],)
        )

        with self.assertRaisesRegex(NotFoundError, "active agent profile"):
            self.database.create_workflow_configuration(
                project_key="workflow-project",
                name="missing-reviewer",
                spec=self.configuration_spec(),
                producer_profile_name="Producer",
                reviewer_profile_name="Missing Reviewer",
                created_by=self.user["id"],
            )

        connection.execute(
            "UPDATE agent_profiles SET active = 0 WHERE id = ?",
            (self.reviewer["id"],),
        )
        with self.assertRaisesRegex(NotFoundError, "active agent profile"):
            self.database.create_workflow_configuration(
                project_key="workflow-project",
                name="inactive-reviewer",
                spec=self.configuration_spec(),
                producer_profile_name="Producer",
                reviewer_profile_name="Reviewer",
                created_by=self.user["id"],
            )
        connection.execute(
            "UPDATE agent_profiles SET active = 1 WHERE id = ?",
            (self.reviewer["id"],),
        )

        with self.assertRaisesRegex(ConflictError, "distinct profiles"):
            self.database.create_workflow_configuration(
                project_key="workflow-project",
                name="same-profile",
                spec=self.configuration_spec(
                    review_separation=ReviewSeparation.DIFFERENT_PROFILE.value
                ),
                producer_profile_name="Producer",
                reviewer_profile_name="Producer",
                created_by=self.user["id"],
            )

        read_only_producer = self.create_profile(
            "Read-only Producer",
            self.producer_identity["id"],
            workspace_mode="read_only",
        )
        with self.assertRaisesRegex(ConflictError, "copy-on-write"):
            self.database.create_workflow_configuration(
                project_key="workflow-project",
                name="read-only-producer",
                spec=self.configuration_spec(),
                producer_profile_name=read_only_producer["name"],
                reviewer_profile_name="Reviewer",
                created_by=self.user["id"],
            )

        writable_reviewer = self.create_profile(
            "Writable Reviewer",
            self.reviewer_identity["id"],
            workspace_mode="copy_on_write",
        )
        with self.assertRaisesRegex(ConflictError, "read-only"):
            self.database.create_workflow_configuration(
                project_key="workflow-project",
                name="writable-reviewer",
                spec=self.configuration_spec(),
                producer_profile_name="Producer",
                reviewer_profile_name=writable_reviewer["name"],
                created_by=self.user["id"],
            )

        shared_identity = self.create_profile(
            "Shared Identity Reviewer",
            self.producer_identity["id"],
            workspace_mode="read_only",
        )
        with self.assertRaisesRegex(ConflictError, "distinct Krab identities"):
            self.database.create_workflow_configuration(
                project_key="workflow-project",
                name="shared-identity",
                spec=self.configuration_spec(),
                producer_profile_name="Producer",
                reviewer_profile_name=shared_identity["name"],
                created_by=self.user["id"],
            )

        shared_identity_allowed = self.database.create_workflow_configuration(
            project_key="workflow-project",
            name="profile-separated",
            spec=self.configuration_spec(
                review_separation=ReviewSeparation.DIFFERENT_PROFILE.value
            ),
            producer_profile_name="Producer",
            reviewer_profile_name=shared_identity["name"],
            created_by=self.user["id"],
        )
        self.assertEqual(shared_identity_allowed["version"], 1)

    def test_configuration_versions_are_selectable_and_lists_return_latest(
        self,
    ) -> None:
        first = self.create_configuration()
        second = self.database.revise_workflow_configuration(
            project_key="workflow-project",
            name="default",
            spec=self.configuration_spec(
                max_correction_rounds=4, landing_mode="provider"
            ),
            producer_profile_name="Producer",
            reviewer_profile_name="Reviewer",
            created_by=self.user["id"],
        )

        self.assertEqual(
            self.database.get_workflow_configuration(
                "workflow-project", "default", version=1
            )["content_sha256"],
            first["content_sha256"],
        )
        self.assertEqual(
            self.database.get_workflow_configuration("workflow-project", "default"),
            second,
        )
        self.assertEqual(self.database.list_workflow_configurations(), [second])
        self.assertEqual(
            self.database.list_workflow_configurations(project_key="workflow-project"),
            [second],
        )
        self.assertEqual(
            self.database.list_workflow_configurations(project_key="another-project"),
            [],
        )

        with self.assertRaises(NotFoundError):
            self.database.get_workflow_configuration(
                "workflow-project", "default", version=3
            )
        with self.assertRaisesRegex(ConflictError, "identical"):
            self.database.revise_workflow_configuration(
                project_key="workflow-project",
                name="default",
                spec=self.configuration_spec(
                    max_correction_rounds=4, landing_mode="provider"
                ),
                producer_profile_name="Producer",
                reviewer_profile_name="Reviewer",
                created_by=self.user["id"],
            )
        with self.assertRaisesRegex(ConflictError, "already exists"):
            self.create_configuration()
        with self.assertRaises(NotFoundError):
            self.database.revise_workflow_configuration(
                project_key="workflow-project",
                name="missing",
                spec=self.configuration_spec(),
                producer_profile_name="Producer",
                reviewer_profile_name="Reviewer",
                created_by=self.user["id"],
            )

    def test_control_plane_resolves_actor_handles_across_workflow_lifecycle(
        self,
    ) -> None:
        control = ControlPlane(self.database)
        first = control.create_workflow_configuration(
            project_key="workflow-project",
            name="service",
            spec=self.configuration_spec(),
            producer_profile_name="Producer",
            reviewer_profile_name="Reviewer",
            created_by_handle="operator",
        )
        second = control.revise_workflow_configuration(
            project_key="workflow-project",
            name="service",
            spec=self.configuration_spec(max_correction_rounds=4),
            producer_profile_name="Producer",
            reviewer_profile_name="Reviewer",
            created_by_handle="operator",
        )

        self.assertEqual(
            control.get_workflow_configuration(
                project_key="workflow-project", name="service", version=1
            ),
            first,
        )
        self.assertEqual(
            control.list_workflow_configurations(project_key="workflow-project"),
            [second],
        )
        execution = control.start_workflow_execution(
            project_key="workflow-project",
            configuration_name="service",
            work_item_id=self.work_item["id"],
            source_revision="a" * 40,
            expected_configuration_version=2,
            expected_context_version_id=self.project["context_version_id"],
            expected_work_item_version_id=self.work_item["work_item_version_id"],
            expected_thread_generation=1,
            expected_thread_version=1,
            idempotency_key="service-workflow",
            requested_by_handle="operator",
        )
        self.assertEqual(control.get_workflow_execution(execution["id"]), execution)
        self.assertEqual(
            control.list_workflow_executions(
                project_key="workflow-project", state="active", limit=1
            ),
            [execution],
        )
        cancelled = control.cancel_workflow_execution(
            execution["id"],
            expected_version=1,
            cancelled_by_handle="operator",
            reason="service boundary verified",
        )
        self.assertEqual(cancelled["state"], "cancelled")

    def test_execution_pins_exact_inputs_and_start_is_idempotent(self) -> None:
        configuration = self.create_configuration()
        execution = self.start()
        repeated = self.start()

        self.assertEqual(repeated, execution)
        self.assertRegex(execution["id"], r"^wfx_[0-9a-f]{32}$")
        self.assertEqual(
            set(execution),
            {
                "id",
                "project_id",
                "project_key",
                "template_definition_id",
                "configuration_version_id",
                "configuration_id",
                "configuration_version",
                "configuration_name",
                "context_version_id",
                "context_version",
                "work_item_id",
                "work_item_version_id",
                "work_item_version",
                "work_thread_id",
                "work_thread_generation",
                "source_revision",
                "state",
                "stage",
                "correction_round",
                "version",
                "content_sha256",
                "cancel_reason",
                "requested_by",
                "created_at",
                "updated_at",
                "finished_at",
                "template",
                "roles",
                "spec",
                "events",
                "rulings",
                "landing",
                "completion",
            },
        )
        self.assertIsNone(execution["landing"])
        self.assertIsNone(execution["completion"])
        expected_request = {
            "schema_version": 1,
            "project_key": "workflow-project",
            "configuration_name": "default",
            "work_item_id": self.work_item["id"],
            "source_revision": "a" * 40,
            "expected_configuration_version": 1,
            "expected_context_version_id": self.project["context_version_id"],
            "expected_work_item_version_id": self.work_item["work_item_version_id"],
            "expected_thread_generation": 1,
            "expected_thread_version": 1,
        }
        self.assertEqual(
            execution["content_sha256"],
            content_sha256(canonical_json(expected_request)),
        )
        self.assertEqual(execution["project_id"], self.project["id"])
        self.assertEqual(execution["project_key"], "workflow-project")
        self.assertEqual(
            execution["template_definition_id"], IMPLEMENTATION_TEMPLATE_ID
        )
        self.assertEqual(execution["configuration_id"], configuration["id"])
        self.assertEqual(execution["configuration_version"], 1)
        self.assertEqual(execution["configuration_name"], "default")
        self.assertEqual(execution["state"], "active")
        self.assertEqual(execution["stage"], "build")
        self.assertEqual(execution["correction_round"], 0)
        self.assertEqual(execution["version"], 1)
        self.assertEqual(
            execution["configuration_version_id"],
            configuration["workflow_configuration_version_id"],
        )
        self.assertEqual(
            execution["context_version_id"], self.project["context_version_id"]
        )
        self.assertEqual(
            execution["work_item_version_id"],
            self.work_item["work_item_version_id"],
        )
        self.assertEqual(execution["context_version"], 1)
        self.assertEqual(execution["work_item_id"], self.work_item["id"])
        self.assertEqual(execution["work_item_version"], 1)
        self.assertRegex(execution["work_thread_id"], r"^wth_[0-9a-f]{32}$")
        self.assertEqual(execution["work_thread_generation"], 1)
        self.assertEqual(execution["source_revision"], "a" * 40)
        self.assertIsNone(execution["cancel_reason"])
        self.assertEqual(execution["requested_by"], "operator")
        self.assertEqual(execution["created_at"], execution["updated_at"])
        self.assertIsNone(execution["finished_at"])
        self.assertEqual(execution["template"], configuration["template"])
        self.assertEqual(execution["roles"], configuration["roles"])
        self.assertEqual(execution["spec"], configuration["spec"])
        self.assertEqual(
            execution["events"],
            [
                {
                    "sequence": 1,
                    "kind": "workflow.started",
                    "from_state": None,
                    "to_state": "active",
                    "actor": "operator",
                    "created_at": execution["created_at"],
                    "metadata": {},
                }
            ],
        )
        self.assertEqual(execution["rulings"], [])
        self.assertEqual(self.database.status()["counts"]["runs"], 0)

        restarted = Database(self.database.path)
        restarted.initialize()
        self.assertEqual(restarted.get_workflow_execution(execution["id"]), execution)

    def test_start_rejects_stale_authority_and_parallel_thread_execution(self) -> None:
        self.create_configuration()
        for kwargs in (
            {"configuration_version": 2},
            {"context_version_id": "ctx_" + "f" * 32},
            {"work_item_version_id": "wiv_" + "f" * 32},
            {"thread_generation": 2},
            {"thread_version": 2},
        ):
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaises(
                    (ConcurrentUpdateError, ConflictError, NotFoundError)
                ),
            ):
                self.start(**kwargs)

        first = self.start()
        with self.assertRaisesRegex(ConflictError, "active workflow"):
            self.start(idempotency_key="start-workflow-2")
        self.assertEqual(self.database.get_workflow_execution(first["id"]), first)

    def test_start_rejects_inactive_authority_and_foreign_work_item(self) -> None:
        self.create_configuration()
        connection = self.database.connect()
        self.addCleanup(connection.close)
        connection.execute(
            "UPDATE agent_profiles SET active = 0 WHERE id = ?",
            (self.producer["id"],),
        )
        with self.assertRaisesRegex(ConflictError, "profile is inactive"):
            self.start()
        connection.execute(
            "UPDATE agent_profiles SET active = 1 WHERE id = ?",
            (self.producer["id"],),
        )

        connection.execute(
            "UPDATE projects SET active = 0 WHERE id = ?", (self.project["id"],)
        )
        with self.assertRaisesRegex(NotFoundError, "active project"):
            self.start()
        connection.execute(
            "UPDATE projects SET active = 1 WHERE id = ?", (self.project["id"],)
        )

        self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "9" * 32,
            krab_project_key="foreign-project",
            display_name="Foreign Project",
            context=ProjectContext.from_mapping({"purpose": "Stay separate"}),
            created_by=self.user["id"],
        )
        foreign_item = self.database.import_work_item(
            project_key="foreign-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "foreign-1",
                    "source_updated_at": "2026-10-23T13:00:00Z",
                    "title": "Do not cross project authority",
                    "brief": "This item belongs elsewhere.",
                }
            ),
            imported_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "another project"):
            self.database.start_workflow_execution(
                project_key="workflow-project",
                configuration_name="default",
                work_item_id=foreign_item["id"],
                source_revision="a" * 40,
                expected_configuration_version=1,
                expected_context_version_id=self.project["context_version_id"],
                expected_work_item_version_id=foreign_item["work_item_version_id"],
                expected_thread_generation=1,
                expected_thread_version=1,
                idempotency_key="foreign-work-item",
                requested_by=self.user["id"],
            )

    def test_start_rejects_reused_key_with_different_authority(self) -> None:
        self.create_configuration()
        execution = self.start()

        with self.assertRaisesRegex(ConflictError, "idempotency key"):
            self.database.start_workflow_execution(
                project_key="workflow-project",
                configuration_name="default",
                work_item_id=self.work_item["id"],
                source_revision="b" * 40,
                expected_configuration_version=1,
                expected_context_version_id=self.project["context_version_id"],
                expected_work_item_version_id=self.work_item["work_item_version_id"],
                expected_thread_generation=1,
                expected_thread_version=1,
                idempotency_key="start-workflow-1",
                requested_by=self.user["id"],
            )
        self.assertEqual(
            self.database.get_workflow_execution(execution["id"]), execution
        )

    def test_execution_queries_are_bounded_and_filter_by_project_and_state(
        self,
    ) -> None:
        self.create_configuration()
        execution = self.start()

        self.assertEqual(self.database.list_workflow_executions(), [execution])
        self.assertEqual(
            self.database.list_workflow_executions(
                project_key="workflow-project", state="active", limit=1
            ),
            [execution],
        )
        self.assertEqual(self.database.list_workflow_executions(state="cancelled"), [])
        for kwargs in ({"state": "stuck"}, {"limit": 0}, {"limit": True}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValidationError):
                self.database.list_workflow_executions(**kwargs)
        with self.assertRaises(NotFoundError):
            self.database.get_workflow_execution("wfx_" + "f" * 32)

    def test_revision_does_not_rewrite_an_existing_execution(self) -> None:
        self.create_configuration()
        execution = self.start()
        revised_profile = self.database.add_agent_profile_version(
            name="Producer",
            spec=AgentProfileSpec.create(
                image="agent@sha256:def",
                command=["agent", "run"],
                workspace_mode="copy_on_write",
                network_mode="none",
            ),
            krab_identity_id=self.producer_identity["id"],
            created_by=self.user["id"],
        )
        revision = self.database.revise_workflow_configuration(
            project_key="workflow-project",
            name="default",
            spec=self.configuration_spec(max_correction_rounds=4),
            producer_profile_name="Producer",
            reviewer_profile_name="Reviewer",
            created_by=self.user["id"],
        )

        self.assertEqual(revision["version"], 2)
        self.assertEqual(
            revision["roles"]["producer"]["profile_version_id"],
            revised_profile["profile_version_id"],
        )
        stored = self.database.get_workflow_execution(execution["id"])
        self.assertEqual(stored["configuration_version"], 1)
        self.assertEqual(
            stored["roles"]["producer"]["profile_version_id"],
            self.producer["profile_version_id"],
        )
        self.assertEqual(self.start(), execution)
        with self.assertRaisesRegex(ConflictError, "configuration version"):
            self.start(idempotency_key="new-start-on-stale-config")

    def test_cancel_is_optimistic_audited_and_terminal(self) -> None:
        self.create_configuration()
        execution = self.start()

        with self.assertRaises(ConcurrentUpdateError):
            self.database.cancel_workflow_execution(
                execution["id"],
                expected_version=2,
                cancelled_by=self.user["id"],
                reason="wrong version",
            )
        cancelled = self.database.cancel_workflow_execution(
            execution["id"],
            expected_version=1,
            cancelled_by=self.user["id"],
            reason="operator stopped the workflow",
        )
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(cancelled["version"], 2)
        self.assertEqual(cancelled["events"][-1]["kind"], "workflow.cancelled")
        self.assertEqual(
            cancelled["events"][-1]["metadata"],
            {"reason": "operator stopped the workflow"},
        )
        with self.assertRaisesRegex(ConflictError, "not active"):
            self.database.cancel_workflow_execution(
                execution["id"],
                expected_version=2,
                cancelled_by=self.user["id"],
                reason="again",
            )
        self.assertEqual(
            self.database.list_workflow_executions(state="cancelled"), [cancelled]
        )

    def test_active_workflow_keeps_its_work_thread_open_until_cancelled(self) -> None:
        self.create_configuration()
        run = self.database.create_run(
            project_key="workflow-project",
            profile_name="Producer",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision="a" * 40,
        )
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id="workflow-lifecycle-test",
            expected_run_version=1,
        )
        running = self.database.start_run_attempt(
            attempt["id"],
            owner_id="workflow-lifecycle-test",
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision="a" * 40,
            container_id="b" * 64,
            container_name="workflow-lifecycle-test",
            image_id="sha256:" + "c" * 64,
        )
        self.database.record_attempt_result(
            running["id"],
            ResultManifest(1, running["id"], "a" * 40, ()),
        )
        self.database.append_work_log(
            run["id"],
            WorkLogEntry.from_mapping(
                {
                    "schema_version": 1,
                    "kind": "outcome",
                    "summary": "The preparatory work is complete.",
                    "details": [],
                    "artifacts": [],
                    "related_interaction_ids": [],
                    "candidate_revision": "a" * 40,
                }
            ),
            idempotency_key="workflow-lifecycle-outcome",
        )
        memory = self.database.get_work_memory_for_run(run["id"])
        self.database.complete_run_attempt(
            running["id"],
            owner_id="workflow-lifecycle-test",
            expected_attempt_version=running["version"],
            exit_code=0,
            image_id=running["image_id"],
        )
        execution = self.start(thread_version=memory["version"])

        with self.assertRaisesRegex(ConflictError, "active workflow"):
            self.database.complete_work_item(
                self.work_item["id"],
                completed_by=self.user["id"],
                expected_thread_version=memory["version"],
            )

        self.database.cancel_workflow_execution(
            execution["id"],
            expected_version=1,
            cancelled_by=self.user["id"],
            reason="release the work thread",
        )
        completed = self.database.complete_work_item(
            self.work_item["id"],
            completed_by=self.user["id"],
            expected_thread_version=memory["version"],
        )
        self.assertEqual(completed["state"], "completed")
        with self.assertRaisesRegex(ConflictError, "work thread is not active"):
            self.start(
                thread_version=completed["version"],
                idempotency_key="start-on-completed-thread",
            )

    def test_start_and_cancel_validate_boundary_values(self) -> None:
        self.create_configuration()
        common = {
            "project_key": "workflow-project",
            "configuration_name": "default",
            "work_item_id": self.work_item["id"],
            "expected_configuration_version": 1,
            "expected_context_version_id": self.project["context_version_id"],
            "expected_work_item_version_id": self.work_item["work_item_version_id"],
            "expected_thread_generation": 1,
            "expected_thread_version": 1,
            "idempotency_key": "valid-key",
            "requested_by": self.user["id"],
        }
        for source_revision in (None, "a" * 39, "g" * 40):
            with (
                self.subTest(source_revision=source_revision),
                self.assertRaises(ValidationError),
            ):
                self.database.start_workflow_execution(
                    source_revision=source_revision, **common
                )
        for field in (
            "expected_configuration_version",
            "expected_thread_generation",
            "expected_thread_version",
        ):
            invalid = {**common, "source_revision": "a" * 64, field: 0}
            with self.subTest(field=field), self.assertRaises(ValidationError):
                self.database.start_workflow_execution(**invalid)
        for field in (
            "work_item_id",
            "expected_context_version_id",
            "expected_work_item_version_id",
        ):
            invalid = {**common, "source_revision": "a" * 40, field: "x" * 65}
            with self.subTest(field=field), self.assertRaises(ValidationError):
                self.database.start_workflow_execution(**invalid)
        invalid_key = {**common, "source_revision": "a" * 40, "idempotency_key": " "}
        with self.assertRaises(ValidationError):
            self.database.start_workflow_execution(**invalid_key)

        execution = self.database.start_workflow_execution(
            source_revision="a" * 64, **common
        )
        with self.assertRaises(ValidationError):
            self.database.cancel_workflow_execution(
                execution["id"],
                expected_version=1,
                cancelled_by=self.user["id"],
                reason="   ",
            )
        for execution_id, reason in (
            ("x" * 65, "stop"),
            (execution["id"], "x" * 4097),
        ):
            with (
                self.subTest(execution_id=execution_id, reason=len(reason)),
                self.assertRaises(ValidationError),
            ):
                self.database.cancel_workflow_execution(
                    execution_id,
                    expected_version=1,
                    cancelled_by=self.user["id"],
                    reason=reason,
                )
        with self.assertRaises(NotFoundError):
            self.database.cancel_workflow_execution(
                "wfx_" + "f" * 32,
                expected_version=1,
                cancelled_by=self.user["id"],
                reason="stop",
            )

    def test_definition_configuration_versions_and_events_are_immutable(self) -> None:
        configuration = self.create_configuration()
        execution = self.start()
        connection = self.database.connect()
        self.addCleanup(connection.close)

        statements = (
            (
                "UPDATE workflow_template_definitions SET content_sha256 = ? "
                "WHERE id = ?",
                ("0" * 64, configuration["template"]["id"]),
            ),
            (
                "UPDATE workflow_configuration_versions SET version = 9 WHERE id = ?",
                (configuration["workflow_configuration_version_id"],),
            ),
            (
                "DELETE FROM workflow_execution_events WHERE execution_id = ?",
                (execution["id"],),
            ),
        )
        for statement, parameters in statements:
            with (
                self.subTest(statement=statement),
                self.assertRaises(sqlite3.IntegrityError),
            ):
                connection.execute(statement, parameters)

    def test_schema_thirty_migrates_schema_twenty_seven_once(self) -> None:
        path = Path(self.temporary.name) / "schema-27.db"
        with sqlite3.connect(path) as connection:
            connection.executescript(SCHEMA[: SCHEMA.index(WORKFLOW_TEMPLATE_SCHEMA)])
            connection.execute("PRAGMA user_version = 27")
        path.chmod(0o600)

        migrated = Database(path)
        status = migrated.initialize()
        repeated = migrated.initialize()

        self.assertEqual(status["schema_version"], 30)
        self.assertEqual(repeated["schema_version"], 30)
        definitions = migrated.list_workflow_template_definitions()
        self.assertEqual(len(definitions), 1)
        self.assertEqual(definitions[0]["key"], "implementation-v1")

    def test_schema_thirty_preserves_schema_twenty_eight_workflow_state(
        self,
    ) -> None:
        path = Path(self.temporary.name) / "schema-28.db"
        with sqlite3.connect(path) as connection:
            connection.executescript(SCHEMA[: SCHEMA.index(WORKFLOW_DIRECTOR_SCHEMA)])
            connection.execute("PRAGMA user_version = 28")
        path.chmod(0o600)
        historical = Database(path)
        user = historical.create_user("legacy-operator", "Legacy Operator")
        instance = historical.create_krab_instance(
            "Legacy Krab", "http://127.0.0.1:8765"
        )
        producer_identity = historical.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "4" * 32, "legacy-producer", 1),
            "file:/private/legacy-producer",
        )
        reviewer_identity = historical.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "5" * 32, "legacy-reviewer", 1),
            "file:/private/legacy-reviewer",
        )
        project = historical.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "6" * 32,
            krab_project_key="legacy-workflow",
            display_name="Legacy Workflow",
            context=ProjectContext.from_mapping(
                {"purpose": "Preserve an M12A execution"}
            ),
            created_by=user["id"],
        )
        item = historical.import_work_item(
            project_key="legacy-workflow",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "legacy-workflow-1",
                    "source_updated_at": "2026-10-23T12:00:00Z",
                    "title": "Preserve the active execution",
                    "brief": "Migrate without rewriting M12A authority.",
                }
            ),
            imported_by=user["id"],
        )
        for name, identity, workspace_mode in (
            ("Legacy Producer", producer_identity, "copy_on_write"),
            ("Legacy Reviewer", reviewer_identity, "read_only"),
        ):
            historical.create_agent_profile(
                name=name,
                spec=AgentProfileSpec.create(
                    image="agent@sha256:legacy",
                    command=["agent", "run"],
                    workspace_mode=workspace_mode,
                    network_mode="none",
                ),
                krab_identity_id=identity["id"],
                created_by=user["id"],
            )
        historical.create_workflow_configuration(
            project_key="legacy-workflow",
            name="default",
            spec=self.configuration_spec(),
            producer_profile_name="Legacy Producer",
            reviewer_profile_name="Legacy Reviewer",
            created_by=user["id"],
        )
        with self.assertRaises(StorageError):
            historical.start_workflow_execution(
                project_key="legacy-workflow",
                configuration_name="default",
                work_item_id=item["id"],
                source_revision="d" * 40,
                expected_configuration_version=1,
                expected_context_version_id=project["context_version_id"],
                expected_work_item_version_id=item["work_item_version_id"],
                expected_thread_generation=1,
                expected_thread_version=1,
                idempotency_key="legacy-workflow-start",
                requested_by=user["id"],
            )
        with self.assertRaises(StorageError):
            historical.create_run(
                project_key="legacy-workflow",
                profile_name="Legacy Producer",
                work_item_id=item["id"],
                requested_by=user["id"],
                source_revision="d" * 40,
                supervision_policy=SupervisionPolicy.from_mapping(
                    {
                        "schema_version": 1,
                        "auto_execute": True,
                        "retry_backoff_seconds": [5, 30, 300],
                        "interaction_escalation_seconds": [],
                        "attempt_idle_escalation_seconds": [],
                        "attempt_lease_expired_escalation_seconds": [],
                        "other_attention_escalation_seconds": [],
                    }
                ),
            )
        with historical.connect() as connection:
            execution_id = connection.execute(
                "SELECT id FROM workflow_executions"
            ).fetchone()["id"]
            run_id = connection.execute("SELECT id FROM runs").fetchone()["id"]
            now = historical.now()
            connection.execute(
                "INSERT INTO supervisor_actions "
                "(id, kind, subject_id, subject_version, run_id, state, version, "
                "attempt_count, max_attempts, retry_backoff_json, available_at, "
                "created_at, updated_at) VALUES (?, 'run.execute', ?, 1, ?, "
                "'pending', 1, 0, 4, '[5,30,300]', ?, ?, ?)",
                ("act_" + "7" * 32, run_id, run_id, now, now, now),
            )

        migrated = Database(path)
        status = migrated.initialize()
        execution = migrated.get_workflow_execution(execution_id)
        action = migrated.get_supervisor_action("act_" + "7" * 32)

        self.assertEqual(status["schema_version"], 30)
        self.assertEqual(execution["correction_round"], 0)
        self.assertEqual(execution["events"][0]["kind"], "workflow.started")
        self.assertEqual(action["run_id"], run_id)
        self.assertIsNone(action["workflow_execution_id"])
        self.assertEqual(status["counts"]["workflow_run_bindings"], 0)
        self.assertEqual(status["counts"]["workflow_gate_bindings"], 0)
        self.assertEqual(status["counts"]["integration_review_verdicts"], 0)
        self.assertEqual(status["counts"]["workflow_rulings"], 0)
        self.assertEqual(status["counts"]["workflow_landing_bindings"], 0)
        self.assertEqual(status["counts"]["workflow_provider_targets"], 0)
        self.assertEqual(status["counts"]["workflow_completions"], 0)

    def test_schema_thirty_preserves_schema_twenty_nine_logs_and_rulings(
        self,
    ) -> None:
        self.create_configuration()
        execution = self.start()
        ruling = self.database.open_workflow_ruling(
            execution["id"],
            expected_version=execution["version"],
            kind="correction_limit",
        )
        run = self.database.create_run(
            project_key="workflow-project",
            profile_name="Producer",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision="a" * 40,
        )
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id="schema-29-author",
            expected_run_version=run["version"],
        )
        attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id="schema-29-author",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision="a" * 40,
            container_id="4" * 64,
            container_name="schema-29-author",
            image_id="sha256:" + "5" * 64,
        )
        entry = self.database.append_work_log(
            run["id"],
            WorkLogEntry.from_mapping(
                {
                    "schema_version": 1,
                    "kind": "progress",
                    "summary": "Legacy workflow work remains attributable.",
                    "details": [],
                    "artifacts": [],
                    "related_interaction_ids": [],
                    "candidate_revision": None,
                }
            ),
            idempotency_key="schema-29-progress",
        )

        with sqlite3.connect(self.database.path) as connection:
            connection.executescript(
                """
                PRAGMA foreign_keys = OFF;
                DROP TABLE workflow_completions;
                DROP TABLE workflow_provider_targets;
                DROP TABLE workflow_landing_bindings;

                DROP TRIGGER work_log_entry_immutable_update;
                DROP TRIGGER work_log_entry_immutable_delete;
                DROP INDEX work_log_thread_sequence;
                CREATE TABLE work_log_entries_v29 (
                    id TEXT PRIMARY KEY,
                    work_thread_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK (sequence >= 1),
                    kind TEXT NOT NULL CHECK (
                        kind IN (
                            'plan', 'progress', 'finding', 'decision', 'blocker',
                            'verification', 'review', 'assignment_result', 'outcome'
                        )
                    ),
                    content_json TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    author_run_id TEXT NOT NULL,
                    author_attempt_id TEXT NOT NULL,
                    assignment_id TEXT,
                    idempotency_key TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (work_thread_id, sequence),
                    UNIQUE (work_thread_id, author_run_id, idempotency_key),
                    FOREIGN KEY (work_thread_id)
                        REFERENCES work_threads(id) ON DELETE RESTRICT,
                    FOREIGN KEY (author_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
                    FOREIGN KEY (author_attempt_id)
                        REFERENCES run_attempts(id) ON DELETE RESTRICT,
                    FOREIGN KEY (assignment_id)
                        REFERENCES work_assignments(id) ON DELETE RESTRICT
                ) STRICT;
                INSERT INTO work_log_entries_v29 (
                    id, work_thread_id, sequence, kind, content_json,
                    content_sha256, author_run_id, author_attempt_id,
                    assignment_id, idempotency_key, created_at
                )
                SELECT
                    id, work_thread_id, sequence, kind, content_json,
                    content_sha256, author_run_id, author_attempt_id,
                    assignment_id, idempotency_key, created_at
                FROM work_log_entries;
                DROP TABLE work_log_entries;
                ALTER TABLE work_log_entries_v29 RENAME TO work_log_entries;
                CREATE INDEX work_log_thread_sequence
                    ON work_log_entries(work_thread_id, sequence);
                CREATE TRIGGER work_log_entry_immutable_update
                BEFORE UPDATE ON work_log_entries
                BEGIN
                    SELECT RAISE(ABORT, 'work-log entries are immutable');
                END;
                CREATE TRIGGER work_log_entry_immutable_delete
                BEFORE DELETE ON work_log_entries
                BEGIN
                    SELECT RAISE(ABORT, 'work-log entries are immutable');
                END;

                DROP TRIGGER workflow_ruling_identity_immutable;
                DROP TRIGGER workflow_ruling_immutable_delete;
                DROP INDEX workflow_ruling_one_open;
                CREATE TABLE workflow_rulings_v29 (
                    id TEXT PRIMARY KEY,
                    execution_id TEXT NOT NULL,
                    execution_version INTEGER NOT NULL CHECK (execution_version >= 1),
                    correction_round INTEGER NOT NULL CHECK (
                        correction_round BETWEEN 0 AND 8
                    ),
                    kind TEXT NOT NULL CHECK (
                        kind IN (
                            'correction_limit', 'producer_failed',
                            'reviewer_failed', 'review_verdict_missing',
                            'validation_failed'
                        )
                    ),
                    state TEXT NOT NULL CHECK (state IN ('open', 'resolved')),
                    version INTEGER NOT NULL CHECK (version >= 1),
                    policy_id TEXT,
                    run_id TEXT,
                    gate_name TEXT,
                    resolved_by TEXT,
                    decision TEXT CHECK (
                        decision IS NULL OR decision IN ('continue', 'cancel')
                    ),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    resolved_at TEXT,
                    UNIQUE (execution_id, execution_version, kind),
                    FOREIGN KEY (execution_id)
                        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
                    FOREIGN KEY (policy_id)
                        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
                    FOREIGN KEY (resolved_by) REFERENCES users(id) ON DELETE RESTRICT,
                    CHECK (
                        (state = 'open' AND resolved_by IS NULL
                            AND decision IS NULL AND resolved_at IS NULL)
                        OR
                        (state = 'resolved' AND resolved_by IS NOT NULL
                            AND decision IS NOT NULL AND resolved_at IS NOT NULL)
                    )
                ) STRICT;
                INSERT INTO workflow_rulings_v29 (
                    id, execution_id, execution_version, correction_round,
                    kind, state, version, policy_id, run_id, gate_name,
                    resolved_by, decision, created_at, updated_at, resolved_at
                )
                SELECT
                    id, execution_id, execution_version, correction_round,
                    kind, state, version, policy_id, run_id, gate_name,
                    resolved_by, decision, created_at, updated_at, resolved_at
                FROM workflow_rulings;
                DROP TABLE workflow_rulings;
                ALTER TABLE workflow_rulings_v29 RENAME TO workflow_rulings;
                CREATE UNIQUE INDEX workflow_ruling_one_open
                    ON workflow_rulings(execution_id) WHERE state = 'open';
                CREATE TRIGGER workflow_ruling_identity_immutable
                BEFORE UPDATE OF
                    id, execution_id, execution_version, correction_round, kind,
                    policy_id, run_id, gate_name, created_at
                ON workflow_rulings
                BEGIN
                    SELECT RAISE(ABORT, 'workflow ruling identity is immutable');
                END;
                CREATE TRIGGER workflow_ruling_immutable_delete
                BEFORE DELETE ON workflow_rulings
                BEGIN
                    SELECT RAISE(ABORT, 'workflow rulings cannot be deleted');
                END;
                PRAGMA user_version = 29;
                PRAGMA foreign_keys = ON;
                """
            )

        migrated = Database(self.database.path)
        first = migrated.initialize()
        second = migrated.initialize()
        memory = migrated.get_work_memory_for_run(run["id"])
        migrated_ruling = migrated.get_workflow_ruling(ruling["id"])

        self.assertEqual(first, second)
        self.assertEqual(first["schema_version"], 30)
        self.assertEqual(memory["entries"][0]["id"], entry["id"])
        self.assertEqual(
            memory["entries"][0]["author"],
            {
                "run_id": run["id"],
                "attempt_id": attempt["id"],
                "profile_name": "Producer",
                "profile_version_id": self.producer["profile_version_id"],
                "profile_version": 1,
                "runtime_adapter": "command",
            },
        )
        self.assertEqual(migrated_ruling["kind"], "correction_limit")
        self.assertIsNone(migrated_ruling["provider_landing_id"])
        self.assertIsNone(migrated_ruling["landing_plan_id"])


if __name__ == "__main__":
    unittest.main()
