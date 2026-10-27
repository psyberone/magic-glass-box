from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError

from magic_glass_box.advisory import (
    AdvisoryBlindReview,
    AdvisoryChallengeReview,
    AdvisoryConfigurationSpec,
    AdvisoryReport,
)
from magic_glass_box.db import SCHEMA_VERSION, ConflictError, Database, NotFoundError
from magic_glass_box.domain import AgentProfileSpec, ProjectContext, ValidationError
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.service import ControlPlane
from magic_glass_box.workflows import (
    ADVISORY_REVIEW_TEMPLATE_ID,
    ADVISORY_REVIEW_TEMPLATE_REVISION,
    WorkflowConfigurationSpec,
)


def blind_review_value(**overrides: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "summary": "The candidate has one concurrency risk worth challenging.",
        "risk_level": "high",
        "findings": [
            {
                "key": "claim-race",
                "category": "correctness",
                "severity": "major",
                "summary": "A stale claim may overwrite the current owner.",
                "rationale": "The write does not visibly bind the observed version.",
                "evidence": [
                    {
                        "path": "src/magic_glass_box/storage/supervision.py",
                        "line_start": 401,
                        "line_end": 418,
                        "summary": "The claim update is the relevant boundary.",
                    }
                ],
                "suggested_check": "Race two owners against the same version.",
            }
        ],
        **overrides,
    }


def challenge_review_value(**overrides: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "summary": "The concurrency concern survives challenge.",
        "challenges": [
            {
                "key": "confirm-claim-race",
                "target_role": "reviewer_one",
                "target_finding_key": "claim-race",
                "stance": "support",
                "summary": "The proposed race is observable at the update boundary.",
                "evidence": [
                    {
                        "path": "tests/test_supervision.py",
                        "line_start": 210,
                        "line_end": 232,
                        "summary": "Existing tests do not race two claimants.",
                    }
                ],
            }
        ],
        **overrides,
    }


def advisory_report_value(**overrides: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "authority": "advisory_only",
        "attention": "urgent",
        "summary": "Independent review converged on one major concern.",
        "items": [
            {
                "key": "claim-race",
                "classification": "corroborated",
                "severity": "major",
                "summary": "The ownership update needs an exact-version race test.",
                "finding_refs": [
                    {
                        "reviewer_role": "reviewer_one",
                        "finding_key": "claim-race",
                    }
                ],
                "challenge_refs": [
                    {
                        "reviewer_role": "reviewer_two",
                        "challenge_key": "confirm-claim-race",
                    }
                ],
                "recommended_action": "Add the race test before relying on the path.",
            }
        ],
        "follow_up_checks": ["Run the focused supervision mutation wave."],
        **overrides,
    }


class AdvisoryContractTests(unittest.TestCase):
    def test_configuration_and_artifacts_are_canonical_and_advisory_only(self) -> None:
        configuration = AdvisoryConfigurationSpec.from_mapping(
            {
                "schema_version": 1,
                "max_findings_per_reviewer": 16,
                "max_challenges_per_reviewer": 16,
                "max_report_items": 32,
            }
        )
        blind = AdvisoryBlindReview.from_mapping(blind_review_value())
        challenge = AdvisoryChallengeReview.from_mapping(challenge_review_value())
        report = AdvisoryReport.from_mapping(advisory_report_value())

        self.assertEqual(configuration.max_findings_per_reviewer, 16)
        self.assertEqual(blind.findings[0].key, "claim-race")
        self.assertEqual(challenge.challenges[0].stance, "support")
        self.assertEqual(report.authority, "advisory_only")
        self.assertEqual(report.items[0].classification, "corroborated")
        for record in (configuration, blind, challenge, report):
            self.assertEqual(len(record.digest), 64)
            self.assertEqual(json.loads(record.canonical), record.to_dict())

    def test_contracts_reject_unbounded_or_gate_shaped_authority(self) -> None:
        configuration_cases = (
            None,
            {
                "schema_version": 2,
                "max_findings_per_reviewer": 16,
                "max_challenges_per_reviewer": 16,
                "max_report_items": 32,
            },
            {
                "schema_version": True,
                "max_findings_per_reviewer": 16,
                "max_challenges_per_reviewer": 16,
                "max_report_items": 32,
            },
            {
                "schema_version": 1,
                "max_findings_per_reviewer": True,
                "max_challenges_per_reviewer": 16,
                "max_report_items": 32,
            },
            {
                "schema_version": 1,
                "max_findings_per_reviewer": 33,
                "max_challenges_per_reviewer": 16,
                "max_report_items": 32,
            },
            {
                "schema_version": 1,
                "max_findings_per_reviewer": 16,
                "max_challenges_per_reviewer": 0,
                "max_report_items": 32,
            },
        )
        for value in configuration_cases:
            with self.subTest(configuration=value), self.assertRaises(ValidationError):
                AdvisoryConfigurationSpec.from_mapping(value)  # type: ignore[arg-type]

        finding = blind_review_value()["findings"][0]  # type: ignore[index]
        blind_cases = (
            blind_review_value(risk_level="approved"),
            blind_review_value(findings=[finding, finding]),
            blind_review_value(
                findings=[{**finding, "key": "NOT-A-VALID-KEY!"}]  # type: ignore[arg-type]
            ),
            {**blind_review_value(), "gate_outcome": "passed"},
        )
        for value in blind_cases:
            with self.subTest(blind=value), self.assertRaises(ValidationError):
                AdvisoryBlindReview.from_mapping(value)

        unsafe_paths = (
            "../private",
            "/private",
            "src//module.py",
            "src\\module.py",
            ".",
            "a" * 1025,
        )
        for path in unsafe_paths:
            unsafe_finding = {
                **finding,  # type: ignore[arg-type]
                "evidence": [
                    {
                        "path": path,
                        "line_start": 1,
                        "line_end": 1,
                        "summary": "unsafe",
                    }
                ],
            }
            with self.subTest(path=path), self.assertRaises(ValidationError):
                AdvisoryBlindReview.from_mapping(
                    blind_review_value(findings=[unsafe_finding])
                )

        challenge = challenge_review_value()["challenges"][0]  # type: ignore[index]
        challenge_cases = (
            challenge_review_value(
                challenges=[{**challenge, "target_role": "coordinator"}]
            ),
            challenge_review_value(challenges=[{**challenge, "stance": "approve"}]),
            challenge_review_value(challenges=[challenge, challenge]),
        )
        for value in challenge_cases:
            with self.subTest(challenge=value), self.assertRaises(ValidationError):
                AdvisoryChallengeReview.from_mapping(value)

        item = advisory_report_value()["items"][0]  # type: ignore[index]
        report_cases = (
            advisory_report_value(authority="gate"),
            advisory_report_value(attention="passed"),
            advisory_report_value(items=[{**item, "finding_refs": []}]),
            advisory_report_value(items=[item, item]),
            {**advisory_report_value(), "landing_approved": True},
        )
        for value in report_cases:
            with self.subTest(report=value), self.assertRaises(ValidationError):
                AdvisoryReport.from_mapping(value)

    def test_inclusive_limits_accept_their_exact_boundary(self) -> None:
        minimum = AdvisoryConfigurationSpec.from_mapping(
            {
                "schema_version": 1,
                "max_findings_per_reviewer": 1,
                "max_challenges_per_reviewer": 1,
                "max_report_items": 1,
            }
        )
        maximum = AdvisoryConfigurationSpec.from_mapping(
            {
                "schema_version": 1,
                "max_findings_per_reviewer": 32,
                "max_challenges_per_reviewer": 32,
                "max_report_items": 64,
            }
        )
        self.assertEqual(minimum.max_findings_per_reviewer, 1)
        self.assertEqual(maximum.max_report_items, 64)

        checks = [f"{index:02d}-" + "x" * 2045 for index in range(31)] + ["z"]
        value = advisory_report_value(
            attention="none",
            items=[],
            summary="s",
            follow_up_checks=checks,
        )
        partial = AdvisoryReport.from_mapping(value)
        missing_bytes = 65_536 - len(partial.canonical.encode("utf-8"))
        self.assertGreater(missing_bytes, 0)
        self.assertLessEqual(1 + missing_bytes, 2048)
        checks[-1] = "z" * (1 + missing_bytes)

        exact_limit = AdvisoryReport.from_mapping(value)
        self.assertEqual(len(exact_limit.follow_up_checks), 32)
        self.assertEqual(len(exact_limit.canonical.encode("utf-8")), 65_536)

    def test_follow_up_checks_require_a_json_array(self) -> None:
        with self.assertRaises(ValidationError):
            AdvisoryReport.from_mapping(
                advisory_report_value(follow_up_checks=("not", "a", "list"))
            )

    def test_empty_clean_review_and_report_remain_explicit_not_approvals(self) -> None:
        blind = AdvisoryBlindReview.from_mapping(
            blind_review_value(
                risk_level="none",
                findings=[],
                summary="No bounded concerns were found.",
            )
        )
        report = AdvisoryReport.from_mapping(
            advisory_report_value(
                attention="none",
                items=[],
                summary="The advisory review found no concerns.",
                follow_up_checks=[],
            )
        )

        self.assertEqual(blind.findings, ())
        self.assertEqual(report.items, ())
        self.assertEqual(report.authority, "advisory_only")

    def test_published_schemas_match_runtime_shapes(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        values = {
            "advisory-configuration-v1.schema.json": {
                "schema_version": 1,
                "max_findings_per_reviewer": 16,
                "max_challenges_per_reviewer": 16,
                "max_report_items": 32,
            },
            "advisory-blind-review-v1.schema.json": blind_review_value(),
            "advisory-challenge-review-v1.schema.json": challenge_review_value(),
            "advisory-report-v1.schema.json": advisory_report_value(),
        }
        for name, value in values.items():
            with self.subTest(schema=name):
                schema = json.loads((repository / "schemas" / name).read_text())
                validator = Draft202012Validator(schema)
                validator.check_schema(schema)
                validator.validate(value)
                with self.assertRaises(SchemaValidationError):
                    validator.validate({**value, "unknown": True})


class AdvisoryConfigurationStorageTests(unittest.TestCase):
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
            krab_project_key="advisory-project",
            display_name="Advisory Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Prove bounded advisory review",
                    "instructions": ["Advisory evidence never becomes gate evidence"],
                }
            ),
            created_by=self.user["id"],
        )
        self.reviewer_capabilities = [
            "run.describe",
            "advisory.blind.submit",
            "advisory.challenge.submit",
        ]
        self.reviewer_one = self.profile(
            "Reviewer One", self.identity("reviewer_one", "1")["id"]
        )
        self.reviewer_two = self.profile(
            "Reviewer Two", self.identity("reviewer_two", "2")["id"]
        )
        self.reviewer_three = self.profile(
            "Reviewer Three", self.identity("reviewer_three", "3")["id"]
        )
        self.coordinator = self.profile(
            "Advisory Coordinator",
            self.identity("advisory_coordinator", "4")["id"],
            capabilities=["run.describe", "advisory.report.submit"],
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
        capabilities: list[str] | None = None,
        workspace_mode: str = "read_only",
    ) -> dict[str, object]:
        return self.database.create_agent_profile(
            name=name,
            spec=AgentProfileSpec.create(
                image="agent@sha256:advisory",
                command=["agent", "review"],
                workspace_mode=workspace_mode,
                network_mode="gateway_only",
                gateway_capabilities=(
                    self.reviewer_capabilities if capabilities is None else capabilities
                ),
            ),
            krab_identity_id=identity_id,
            created_by=self.user["id"],
        )

    @staticmethod
    def spec(**overrides: object) -> AdvisoryConfigurationSpec:
        return AdvisoryConfigurationSpec.from_mapping(
            {
                "schema_version": 1,
                "max_findings_per_reviewer": 16,
                "max_challenges_per_reviewer": 16,
                "max_report_items": 32,
                **overrides,
            }
        )

    def create_configuration(self, name: str = "the-furies") -> dict[str, object]:
        return self.database.create_advisory_configuration(
            project_key="advisory-project",
            name=name,
            spec=self.spec(),
            reviewer_one_profile_name="Reviewer One",
            reviewer_two_profile_name="Reviewer Two",
            reviewer_three_profile_name="Reviewer Three",
            coordinator_profile_name="Advisory Coordinator",
            created_by=self.user["id"],
        )

    def test_catalog_and_configuration_pin_three_reviewers_and_coordinator(
        self,
    ) -> None:
        definition = next(
            item
            for item in self.database.list_workflow_template_definitions()
            if item["key"] == "advisory-review-v1"
        )
        self.assertEqual(definition["id"], ADVISORY_REVIEW_TEMPLATE_ID)
        self.assertEqual(definition["revision"], ADVISORY_REVIEW_TEMPLATE_REVISION)

        configuration = self.create_configuration()

        self.assertEqual(configuration["template"]["key"], "advisory-review-v1")
        self.assertEqual(configuration["spec"], self.spec().to_dict())
        self.assertEqual(
            set(configuration["roles"]),
            {"reviewer_one", "reviewer_two", "reviewer_three", "coordinator"},
        )
        self.assertEqual(
            configuration["roles"]["reviewer_one"]["profile_version_id"],
            self.reviewer_one["profile_version_id"],
        )
        self.assertEqual(
            self.database.get_workflow_configuration("advisory-project", "the-furies"),
            configuration,
        )

    def test_advisory_namespace_is_project_and_prescription_bounded(self) -> None:
        advisory = self.create_configuration()
        producer = self.profile(
            "Producer",
            self.identity("producer", "5")["id"],
            capabilities=[],
            workspace_mode="copy_on_write",
        )
        reviewer = self.profile(
            "Gate Reviewer",
            self.identity("gate_reviewer", "6")["id"],
            capabilities=[],
        )
        self.database.create_workflow_configuration(
            project_key="advisory-project",
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
        self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "7" * 32,
            krab_project_key="other-advisory-project",
            display_name="Other Advisory Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Prove project-bounded advisory configuration reads",
                    "instructions": ["Do not cross projects"],
                }
            ),
            created_by=self.user["id"],
        )
        self.database.create_advisory_configuration(
            project_key="other-advisory-project",
            name="other-furies",
            spec=self.spec(),
            reviewer_one_profile_name="Reviewer One",
            reviewer_two_profile_name="Reviewer Two",
            reviewer_three_profile_name="Reviewer Three",
            coordinator_profile_name="Advisory Coordinator",
            created_by=self.user["id"],
        )

        self.assertEqual(
            self.database.list_advisory_configurations(project_key="advisory-project"),
            [advisory],
        )
        with self.assertRaisesRegex(NotFoundError, "advisory configuration"):
            self.database.get_advisory_configuration(
                "advisory-project", "implementation"
            )

        control = ControlPlane(self.database)
        self.assertEqual(
            control.get_advisory_configuration(
                project_key="advisory-project", name="the-furies", version=1
            ),
            advisory,
        )
        self.assertEqual(
            control.list_advisory_configurations(project_key="advisory-project"),
            [advisory],
        )

    def test_service_revision_preserves_exact_historical_role_and_policy_pins(
        self,
    ) -> None:
        control = ControlPlane(self.database)
        first = control.create_advisory_configuration(
            project_key="advisory-project",
            name="service-furies",
            spec=self.spec(),
            reviewer_one_profile_name="Reviewer One",
            reviewer_two_profile_name="Reviewer Two",
            reviewer_three_profile_name="Reviewer Three",
            coordinator_profile_name="Advisory Coordinator",
            created_by_handle="operator",
        )
        second = control.revise_advisory_configuration(
            project_key="advisory-project",
            name="service-furies",
            spec=self.spec(max_report_items=16),
            reviewer_one_profile_name="Reviewer One",
            reviewer_two_profile_name="Reviewer Two",
            reviewer_three_profile_name="Reviewer Three",
            coordinator_profile_name="Advisory Coordinator",
            created_by_handle="operator",
        )

        self.assertEqual((first["version"], second["version"]), (1, 2))
        self.assertNotEqual(first["content_sha256"], second["content_sha256"])
        self.assertEqual(
            control.get_advisory_configuration(
                project_key="advisory-project",
                name="service-furies",
                version=1,
            ),
            first,
        )

    def test_configuration_requires_distinct_read_only_bounded_roles(self) -> None:
        shared_identity = self.profile(
            "Shared Identity Reviewer",
            self.reviewer_one["krab_identity_id"],
        )
        writable = self.profile(
            "Writable Reviewer",
            self.identity("writable", "8")["id"],
            workspace_mode="copy_on_write",
        )
        underpowered = self.profile(
            "Underpowered Reviewer",
            self.identity("underpowered", "9")["id"],
            capabilities=self.reviewer_capabilities[:-1],
        )
        underpowered_coordinator = self.profile(
            "Underpowered Coordinator",
            self.identity("underpowered_coordinator", "a")["id"],
            capabilities=["run.describe"],
        )
        cases = (
            ("Reviewer One", "Reviewer One", "Reviewer Three", "Advisory Coordinator"),
            (
                "Reviewer One",
                shared_identity["name"],
                "Reviewer Three",
                "Advisory Coordinator",
            ),
            (
                writable["name"],
                "Reviewer Two",
                "Reviewer Three",
                "Advisory Coordinator",
            ),
            (
                underpowered["name"],
                "Reviewer Two",
                "Reviewer Three",
                "Advisory Coordinator",
            ),
            ("Reviewer One", "Reviewer Two", "Reviewer Three", "Reviewer One"),
            (
                "Reviewer One",
                "Reviewer Two",
                "Reviewer Three",
                underpowered_coordinator["name"],
            ),
        )
        for index, roles in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(ConflictError):
                self.database.create_advisory_configuration(
                    project_key="advisory-project",
                    name=f"invalid-{index}",
                    spec=self.spec(),
                    reviewer_one_profile_name=roles[0],
                    reviewer_two_profile_name=roles[1],
                    reviewer_three_profile_name=roles[2],
                    coordinator_profile_name=roles[3],
                    created_by=self.user["id"],
                )

    def test_generic_writer_rejects_tampered_definition_or_role_surface(self) -> None:
        roles = {
            "reviewer_one": "Reviewer One",
            "reviewer_two": "Reviewer Two",
            "reviewer_three": "Reviewer Three",
            "coordinator": "Advisory Coordinator",
        }
        cases = (
            ("0" * 64, roles),
            (
                ADVISORY_REVIEW_TEMPLATE_REVISION,
                {
                    role: profile
                    for role, profile in roles.items()
                    if role != "coordinator"
                },
            ),
        )
        for index, (revision, role_names) in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(ValidationError):
                self.database._write_prescription_configuration(
                    project_key="advisory-project",
                    name=f"tampered-{index}",
                    spec=self.spec(),
                    role_profile_names=role_names,
                    template_definition_id=ADVISORY_REVIEW_TEMPLATE_ID,
                    template_revision=revision,
                    created_by=self.user["id"],
                    create=True,
                )

    def test_schema_thirty_six_registers_advisory_without_rewriting_m13b(
        self,
    ) -> None:
        with self.database.connect() as connection:
            connection.execute("DROP TABLE workflow_scope_reviews")
            connection.execute("DROP TABLE workflow_scope_bindings")
            connection.execute("DROP TABLE advisory_reports")
            connection.execute("DROP TABLE advisory_challenge_reviews")
            connection.execute("DROP TABLE advisory_blind_reviews")
            connection.execute("DROP TABLE advisory_run_bindings")
            connection.execute("DROP TABLE advisory_execution_events")
            connection.execute("DROP TABLE advisory_executions")
            connection.execute(
                "DROP TRIGGER workflow_template_definition_immutable_delete"
            )
            connection.execute(
                "DELETE FROM workflow_template_definitions WHERE id = ?",
                (ADVISORY_REVIEW_TEMPLATE_ID,),
            )
            connection.execute("PRAGMA user_version = 35")

        migrated = Database(self.database.path)
        first = migrated.initialize()
        second = migrated.initialize()

        self.assertEqual(SCHEMA_VERSION, 41)
        self.assertEqual(first, second)
        self.assertEqual(first["schema_version"], SCHEMA_VERSION)
        self.assertEqual(
            {item["key"] for item in migrated.list_workflow_template_definitions()},
            {"advisory-review-v1", "discovery-v1", "implementation-v1"},
        )


if __name__ == "__main__":
    unittest.main()
