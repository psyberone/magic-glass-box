from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError

from magic_glass_box.db import SCHEMA_VERSION, ConflictError, Database, NotFoundError
from magic_glass_box.discovery import (
    DiscoveryConfigurationSpec,
    DiscoveryRuling,
    DiscoveryTrace,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    ValidationError,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.service import ControlPlane
from magic_glass_box.workflows import (
    ADVISORY_REVIEW_TEMPLATE_ID,
    DISCOVERY_TEMPLATE_ID,
    DISCOVERY_TEMPLATE_REVISION,
)


def trace_value(**overrides: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "summary": "The requested change belongs in the existing catalog layer.",
        "findings": [
            {
                "key": "catalog-boundary",
                "classification": "constraint",
                "summary": "The catalog already owns immutable profile revisions.",
                "source_refs": [{"kind": "brief", "index": None}],
                "evidence": [
                    {
                        "path": "src/magic_glass_box/storage/catalog.py",
                        "line_start": 141,
                        "line_end": 165,
                        "summary": "Creation and revision authority live together.",
                    }
                ],
            }
        ],
        "recommended_checks": ["Run the focused catalog tests."],
        "open_questions": [],
        **overrides,
    }


def ruling_value(**overrides: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "disposition": "GO",
        "summary": "The change is bounded and ready for implementation.",
        "trace_finding_keys": ["catalog-boundary"],
        "interaction_id": None,
        "work_ready_brief": {
            "objective": "Add the requested catalog primitive without widening it.",
            "must_items": [
                {
                    "key": "preserve-authority",
                    "requirement": "Keep profile revisions immutable.",
                    "source_refs": [{"kind": "brief", "index": None}],
                    "finding_keys": ["catalog-boundary"],
                }
            ],
            "scope_paths": ["src/magic_glass_box/storage/catalog.py"],
            "verification_checks": ["Run the focused catalog tests."],
            "non_goals": ["Do not add provider-specific behavior."],
            "follow_up_proposals": [],
        },
        **overrides,
    }


class DiscoveryContractTests(unittest.TestCase):
    def test_configuration_trace_and_go_ruling_are_canonical(self) -> None:
        configuration = DiscoveryConfigurationSpec.from_mapping(
            {
                "schema_version": 1,
                "max_findings": 16,
                "max_must_items": 5,
                "max_follow_up_proposals": 8,
            }
        )
        trace = DiscoveryTrace.from_mapping(trace_value())
        ruling = DiscoveryRuling.from_mapping(ruling_value())

        self.assertEqual(configuration.max_must_items, 5)
        self.assertEqual(trace.findings[0].key, "catalog-boundary")
        self.assertEqual(ruling.disposition, "GO")
        self.assertEqual(
            ruling.work_ready_brief.must_items[0].key, "preserve-authority"
        )
        for record in (configuration, trace, ruling):
            self.assertEqual(len(record.digest), 64)
            self.assertEqual(json.loads(record.canonical), record.to_dict())

    def test_contracts_reject_ambiguous_or_unbounded_authority(self) -> None:
        configuration_cases = (
            None,
            {
                "schema_version": 1.0,
                "max_findings": 16,
                "max_must_items": 5,
                "max_follow_up_proposals": 8,
            },
            {
                "schema_version": 1,
                "max_findings": 33,
                "max_must_items": 5,
                "max_follow_up_proposals": 8,
            },
            {
                "schema_version": 1,
                "max_findings": 16,
                "max_must_items": 6,
                "max_follow_up_proposals": 8,
            },
        )
        for value in configuration_cases:
            with self.subTest(configuration=value), self.assertRaises(ValidationError):
                DiscoveryConfigurationSpec.from_mapping(value)  # type: ignore[arg-type]

        trace_cases = (
            trace_value(findings=[]),
            trace_value(
                findings=[
                    trace_value()["findings"][0],
                    trace_value()["findings"][0],
                ]
            ),
            trace_value(
                findings=[
                    {
                        **trace_value()["findings"][0],  # type: ignore[index]
                        "evidence": [
                            {
                                "path": "../secret",
                                "line_start": 1,
                                "line_end": 1,
                                "summary": "unsafe",
                            }
                        ],
                    }
                ]
            ),
        )
        for value in trace_cases:
            with self.subTest(trace=value), self.assertRaises(ValidationError):
                DiscoveryTrace.from_mapping(value)

        too_many_musts = [
            {
                "key": f"must-{index}",
                "requirement": f"Requirement {index}",
                "source_refs": [{"kind": "brief", "index": None}],
                "finding_keys": ["catalog-boundary"],
            }
            for index in range(6)
        ]
        ruling_cases = (
            ruling_value(disposition="ASK_HUMAN"),
            ruling_value(disposition="NO_GO"),
            ruling_value(
                work_ready_brief={
                    **ruling_value()["work_ready_brief"],  # type: ignore[arg-type]
                    "must_items": too_many_musts,
                }
            ),
        )
        for value in ruling_cases:
            with self.subTest(ruling=value), self.assertRaises(ValidationError):
                DiscoveryRuling.from_mapping(value)

    def test_trace_rejects_invalid_sources_evidence_and_text(self) -> None:
        finding = trace_value()["findings"][0]  # type: ignore[index]
        cases = (
            trace_value(summary="   "),
            trace_value(recommended_checks=["same", "same"]),
            trace_value(
                findings=[
                    {**finding, "key": "Bad Key"},  # type: ignore[arg-type]
                ]
            ),
            trace_value(
                findings=[
                    {**finding, "classification": "opinion"},  # type: ignore[arg-type]
                ]
            ),
            trace_value(
                findings=[
                    {
                        **finding,  # type: ignore[arg-type]
                        "source_refs": [{"kind": "other", "index": None}],
                    }
                ]
            ),
            trace_value(
                findings=[
                    {
                        **finding,  # type: ignore[arg-type]
                        "source_refs": [{"kind": "brief", "index": 0}],
                    }
                ]
            ),
            trace_value(
                findings=[
                    {
                        **finding,  # type: ignore[arg-type]
                        "source_refs": [
                            {"kind": "brief", "index": None},
                            {"kind": "brief", "index": None},
                        ],
                    }
                ]
            ),
            trace_value(
                findings=[
                    {
                        **finding,  # type: ignore[arg-type]
                        "evidence": [
                            {
                                "path": "src/app.py",
                                "line_start": 2,
                                "line_end": 1,
                                "summary": "backwards",
                            }
                        ],
                    }
                ]
            ),
            trace_value(
                findings=[
                    {
                        **finding,  # type: ignore[arg-type]
                        "evidence": [
                            {
                                "path": f"src/{index}.py",
                                "line_start": 1,
                                "line_end": 1,
                                "summary": "evidence",
                            }
                            for index in range(9)
                        ],
                    }
                ]
            ),
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                DiscoveryTrace.from_mapping(value)

        acceptance = trace_value(
            findings=[
                {
                    **finding,  # type: ignore[arg-type]
                    "source_refs": [{"kind": "acceptance_criterion", "index": 0}],
                }
            ]
        )
        self.assertEqual(
            DiscoveryTrace.from_mapping(acceptance).findings[0].source_refs[0].index,
            0,
        )

    def test_ruling_variants_and_brief_uniqueness_are_explicit(self) -> None:
        interaction_id = "int_" + "1" * 32
        ask = DiscoveryRuling.from_mapping(
            ruling_value(
                disposition="ASK_HUMAN",
                interaction_id=interaction_id,
                work_ready_brief=None,
            )
        )
        no_go = DiscoveryRuling.from_mapping(
            ruling_value(disposition="NO_GO", work_ready_brief=None)
        )
        self.assertEqual(ask.interaction_id, interaction_id)
        self.assertEqual(no_go.disposition, "NO_GO")

        brief = ruling_value()["work_ready_brief"]
        must = brief["must_items"][0]  # type: ignore[index]
        cases = (
            ruling_value(disposition="MAYBE"),
            ruling_value(interaction_id="not-an-interaction"),
            ruling_value(work_ready_brief=None),
            ruling_value(
                disposition="ASK_HUMAN", interaction_id=None, work_ready_brief=None
            ),
            ruling_value(trace_finding_keys=[]),
            ruling_value(
                work_ready_brief={
                    **brief,  # type: ignore[arg-type]
                    "must_items": [must, must],
                }
            ),
            ruling_value(
                work_ready_brief={
                    **brief,  # type: ignore[arg-type]
                    "scope_paths": ["src/app.py", "src/app.py"],
                }
            ),
            ruling_value(
                work_ready_brief={
                    **brief,  # type: ignore[arg-type]
                    "scope_paths": ["/private/app.py"],
                }
            ),
            ruling_value(
                work_ready_brief={
                    **brief,  # type: ignore[arg-type]
                    "verification_checks": [],
                }
            ),
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                DiscoveryRuling.from_mapping(value)

    def test_published_schemas_match_runtime_shapes(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        values = {
            "discovery-configuration-v1.schema.json": {
                "schema_version": 1,
                "max_findings": 16,
                "max_must_items": 5,
                "max_follow_up_proposals": 8,
            },
            "discovery-trace-v1.schema.json": trace_value(),
            "discovery-ruling-v1.schema.json": ruling_value(),
        }
        for name, value in values.items():
            with self.subTest(schema=name):
                schema = json.loads((repository / "schemas" / name).read_text())
                validator = Draft202012Validator(schema)
                validator.validate(value)
                invalid = {**value, "unknown": True}
                with self.assertRaises(SchemaValidationError):
                    validator.validate(invalid)


class DiscoveryConfigurationStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Database(Path(self.temporary.name) / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Local Operator")
        self.instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="discovery-project",
            display_name="Discovery Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Prove bounded discovery",
                    "instructions": ["Keep scope explicit"],
                }
            ),
            created_by=self.user["id"],
        )
        self.investigator_identity = self.identity("investigator", "1")
        self.coordinator_identity = self.identity("coordinator", "2")
        self.investigator = self.profile(
            "Investigator",
            self.investigator_identity["id"],
            capabilities=["run.describe", "discovery.trace.submit"],
        )
        self.coordinator = self.profile(
            "Coordinator",
            self.coordinator_identity["id"],
            credentials=["krab"],
            capabilities=[
                "run.describe",
                "interaction.open",
                "interaction.read",
                "handoff.submit",
                "discovery.ruling.submit",
            ],
        )

    def identity(self, handle: str, fill: str) -> dict[str, object]:
        return self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity(
                user_id="usr_" + fill * 32,
                handle=handle,
                token_version=1,
            ),
            f"file:/private/{handle}",
        )

    def profile(
        self,
        name: str,
        identity_id: str,
        *,
        credentials: list[str] | None = None,
        capabilities: list[str] | None = None,
        workspace_mode: str = "read_only",
    ) -> dict[str, object]:
        return self.database.create_agent_profile(
            name=name,
            spec=AgentProfileSpec.create(
                image="agent@sha256:discovery",
                command=["agent", "run"],
                workspace_mode=workspace_mode,
                network_mode="gateway_only",
                credential_names=credentials or [],
                gateway_capabilities=capabilities or [],
            ),
            krab_identity_id=identity_id,
            created_by=self.user["id"],
        )

    @staticmethod
    def spec(**overrides: object) -> DiscoveryConfigurationSpec:
        return DiscoveryConfigurationSpec.from_mapping(
            {
                "schema_version": 1,
                "max_findings": 16,
                "max_must_items": 5,
                "max_follow_up_proposals": 8,
                **overrides,
            }
        )

    def test_catalog_and_configuration_pin_exact_discovery_authority(self) -> None:
        definitions = self.database.list_workflow_template_definitions()
        discovery = next(item for item in definitions if item["key"] == "discovery-v1")
        self.assertEqual(discovery["id"], DISCOVERY_TEMPLATE_ID)
        self.assertEqual(discovery["revision"], DISCOVERY_TEMPLATE_REVISION)

        configuration = self.database.create_discovery_configuration(
            project_key="discovery-project",
            name="default-discovery",
            spec=self.spec(),
            investigator_profile_name="Investigator",
            coordinator_profile_name="Coordinator",
            created_by=self.user["id"],
        )

        self.assertEqual(configuration["template"]["key"], "discovery-v1")
        self.assertEqual(configuration["spec"], self.spec().to_dict())
        self.assertEqual(set(configuration["roles"]), {"investigator", "coordinator"})
        self.assertEqual(
            configuration["roles"]["investigator"]["profile_version_id"],
            self.investigator["profile_version_id"],
        )
        self.assertEqual(
            self.database.get_workflow_configuration(
                "discovery-project", "default-discovery"
            ),
            configuration,
        )

    def test_discovery_configuration_reads_exclude_other_prescriptions(self) -> None:
        from magic_glass_box.workflows import WorkflowConfigurationSpec

        discovery = self.database.create_discovery_configuration(
            project_key="discovery-project",
            name="default-discovery",
            spec=self.spec(),
            investigator_profile_name="Investigator",
            coordinator_profile_name="Coordinator",
            created_by=self.user["id"],
        )
        producer = self.profile(
            "Producer",
            self.identity("producer", "5")["id"],
            capabilities=[],
            workspace_mode="copy_on_write",
        )
        reviewer = self.profile(
            "Reviewer",
            self.identity("reviewer", "6")["id"],
            capabilities=[],
        )
        self.database.create_workflow_configuration(
            project_key="discovery-project",
            name="implementation",
            spec=WorkflowConfigurationSpec.from_mapping(
                {
                    "schema_version": 1,
                    "review_separation": "different_profile_and_identity",
                    "maintainer_approval": True,
                    "validations": [],
                    "max_correction_rounds": 1,
                    "landing_mode": "manual",
                }
            ),
            producer_profile_name=producer["name"],
            reviewer_profile_name=reviewer["name"],
            created_by=self.user["id"],
        )
        latest = self.database.revise_discovery_configuration(
            project_key="discovery-project",
            name="default-discovery",
            spec=self.spec(max_findings=8),
            investigator_profile_name="Investigator",
            coordinator_profile_name="Coordinator",
            created_by=self.user["id"],
        )
        self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "4" * 32,
            krab_project_key="other-discovery-project",
            display_name="Other Discovery Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Prove project-bounded configuration reads",
                    "instructions": ["Do not cross project boundaries"],
                }
            ),
            created_by=self.user["id"],
        )
        self.database.create_discovery_configuration(
            project_key="other-discovery-project",
            name="other-discovery",
            spec=self.spec(),
            investigator_profile_name="Investigator",
            coordinator_profile_name="Coordinator",
            created_by=self.user["id"],
        )

        self.assertEqual(
            self.database.get_discovery_configuration(
                "discovery-project", "default-discovery", version=1
            ),
            discovery,
        )
        self.assertEqual(
            self.database.list_discovery_configurations(
                project_key="discovery-project"
            ),
            [latest],
        )
        with self.assertRaisesRegex(NotFoundError, "discovery configuration"):
            self.database.get_discovery_configuration(
                "discovery-project", "implementation"
            )

        control = ControlPlane(self.database)
        self.assertEqual(
            control.get_discovery_configuration(
                project_key="discovery-project",
                name="default-discovery",
                version=1,
            ),
            discovery,
        )
        self.assertEqual(
            control.list_discovery_configurations(project_key="discovery-project"),
            [latest],
        )

    def test_configuration_requires_distinct_bounded_read_only_roles(self) -> None:
        coordinator_capabilities = [
            "run.describe",
            "interaction.open",
            "interaction.read",
            "handoff.submit",
            "discovery.ruling.submit",
        ]
        writable = self.profile(
            "Writable Coordinator",
            self.identity("writable", "4")["id"],
            credentials=["krab"],
            capabilities=coordinator_capabilities,
            workspace_mode="copy_on_write",
        )
        missing_capability = self.profile(
            "Underpowered Coordinator",
            self.identity("underpowered", "5")["id"],
            credentials=["krab"],
            capabilities=coordinator_capabilities[:-1],
        )
        shared_identity = self.profile(
            "Shared Identity Coordinator",
            self.investigator_identity["id"],
            credentials=["krab"],
            capabilities=coordinator_capabilities,
        )
        cases = (
            ("Investigator", "Investigator"),
            ("Investigator", shared_identity["name"]),
            ("Investigator", writable["name"]),
            ("Investigator", missing_capability["name"]),
        )
        for index, (investigator, coordinator) in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(ConflictError):
                self.database.create_discovery_configuration(
                    project_key="discovery-project",
                    name=f"invalid-{index}",
                    spec=self.spec(),
                    investigator_profile_name=investigator,
                    coordinator_profile_name=coordinator,
                    created_by=self.user["id"],
                )

    def test_revision_cannot_change_a_configuration_prescription(self) -> None:
        from magic_glass_box.workflows import WorkflowConfigurationSpec

        producer = self.profile(
            "Producer",
            self.identity("producer", "5")["id"],
            capabilities=[],
            workspace_mode="copy_on_write",
        )
        reviewer = self.profile(
            "Reviewer",
            self.identity("reviewer", "6")["id"],
            capabilities=[],
        )
        self.database.create_workflow_configuration(
            project_key="discovery-project",
            name="implementation",
            spec=WorkflowConfigurationSpec.from_mapping(
                {
                    "schema_version": 1,
                    "review_separation": "different_profile_and_identity",
                    "maintainer_approval": True,
                    "validations": [],
                    "max_correction_rounds": 1,
                    "landing_mode": "manual",
                }
            ),
            producer_profile_name=producer["name"],
            reviewer_profile_name=reviewer["name"],
            created_by=self.user["id"],
        )

        with self.assertRaisesRegex(ConflictError, "prescription"):
            self.database.revise_discovery_configuration(
                project_key="discovery-project",
                name="implementation",
                spec=self.spec(),
                investigator_profile_name="Investigator",
                coordinator_profile_name="Coordinator",
                created_by=self.user["id"],
            )

    def test_schema_thirty_three_adds_discovery_without_rewriting_m12(self) -> None:
        with self.database.connect() as connection:
            connection.executescript(
                """
                DROP TRIGGER discovery_ruling_immutable_delete;
                DROP TRIGGER discovery_ruling_immutable_update;
                DROP TRIGGER discovery_trace_immutable_delete;
                DROP TRIGGER discovery_trace_immutable_update;
                DROP TRIGGER discovery_run_binding_immutable_delete;
                DROP TRIGGER discovery_run_binding_immutable_update;
                DROP TRIGGER discovery_execution_event_immutable_delete;
                DROP TRIGGER discovery_execution_event_immutable_update;
                DROP TRIGGER discovery_execution_immutable_delete;
                DROP TRIGGER discovery_execution_transition;
                DROP TRIGGER discovery_execution_identity_immutable;
                DROP TABLE discovery_rulings;
                DROP TABLE discovery_traces;
                DROP TABLE discovery_run_bindings;
                DROP TABLE discovery_execution_events;
                DROP TABLE discovery_executions;
                """
            )
            connection.execute(
                "DROP TRIGGER workflow_template_definition_immutable_delete"
            )
            connection.execute(
                "DELETE FROM workflow_template_definitions WHERE id IN (?, ?)",
                (ADVISORY_REVIEW_TEMPLATE_ID, DISCOVERY_TEMPLATE_ID),
            )
            connection.execute("PRAGMA user_version = 32")

        migrated = Database(self.database.path)
        first = migrated.initialize()
        second = migrated.initialize()

        self.assertEqual(SCHEMA_VERSION, 36)
        self.assertEqual(first, second)
        self.assertEqual(first["schema_version"], SCHEMA_VERSION)
        definitions = migrated.list_workflow_template_definitions()
        self.assertEqual(
            {definition["key"] for definition in definitions},
            {"advisory-review-v1", "discovery-v1", "implementation-v1"},
        )


if __name__ == "__main__":
    unittest.main()
