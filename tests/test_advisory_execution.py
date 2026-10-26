from __future__ import annotations

import copy
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path

from magic_glass_box.advisory import (
    AdvisoryBlindReview,
    AdvisoryChallengeReview,
    AdvisoryConfigurationSpec,
    AdvisoryReport,
)
from magic_glass_box.candidates import GitCandidateStore
from magic_glass_box.db import (
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    ValidationError,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.orchestration import CandidateReviewWorkspaces
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.storage.schema import SCHEMA_VERSION
from magic_glass_box.workflows import ADVISORY_REVIEW_TEMPLATE_ID
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

OWNER_ID = "advisory-ledger-test"
IMAGE_ID = "sha256:" + "9" * 64


def blind_value(role: str, *, finding_count: int = 1) -> dict[str, object]:
    findings = [
        {
            "key": f"{role.replace('_', '-')}-{index}",
            "category": "correctness",
            "severity": "major",
            "summary": f"Finding {index} from {role}.",
            "rationale": "The exact candidate demonstrates this behavior.",
            "evidence": [
                {
                    "path": "reviewed.py",
                    "line_start": 1,
                    "line_end": 2,
                    "summary": "The returned value changed.",
                }
            ],
            "suggested_check": "Exercise value() directly.",
        }
        for index in range(finding_count)
    ]
    return {
        "schema_version": 1,
        "summary": f"Independent assessment by {role}.",
        "risk_level": "moderate" if findings else "none",
        "findings": findings,
    }


def challenge_value(
    role: str,
    *,
    target_role: str,
    target_finding_key: str,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "summary": f"Peer challenge by {role}.",
        "challenges": [
            {
                "key": f"challenge-{role.replace('_', '-')}",
                "target_role": target_role,
                "target_finding_key": target_finding_key,
                "stance": "refine",
                "summary": "The risk is real but the scope can be more precise.",
                "evidence": [
                    {
                        "path": "reviewed.py",
                        "line_start": 1,
                        "line_end": 2,
                        "summary": "The candidate provides narrower evidence.",
                    }
                ],
            }
        ],
    }


def report_value() -> dict[str, object]:
    return {
        "schema_version": 1,
        "authority": "advisory_only",
        "attention": "urgent",
        "summary": "The reviewers agree that one correction deserves attention.",
        "items": [
            {
                "key": "returned-value",
                "classification": "corroborated",
                "severity": "major",
                "summary": "The changed return value needs an explicit contract.",
                "finding_refs": [
                    {
                        "reviewer_role": "reviewer_one",
                        "finding_key": "reviewer-one-0",
                    },
                    {
                        "reviewer_role": "reviewer_two",
                        "finding_key": "reviewer-two-0",
                    },
                ],
                "challenge_refs": [
                    {
                        "reviewer_role": "reviewer_three",
                        "challenge_key": "challenge-reviewer-three",
                    }
                ],
                "recommended_action": "Document and test the new return value.",
            }
        ],
        "follow_up_checks": ["Run the focused public-contract test."],
    }


def git(binary: Path, *arguments: str, cwd: Path) -> str:
    completed = subprocess.run(  # noqa: S603 - test-controlled Git executable
        [str(binary), *arguments],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=True,
    )
    return completed.stdout.decode("utf-8", "strict").strip()


class AdvisoryExecutionStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        discovered = shutil.which("git")
        if discovered is None:
            self.skipTest("Git CLI is required")
        self.git_binary = Path(discovered)
        self.repository = self.root / "source"
        self.repository.mkdir()
        git(
            self.git_binary,
            "init",
            "--quiet",
            "--initial-branch=main",
            cwd=self.repository,
        )
        (self.repository / "reviewed.py").write_text(
            "def value():\n    return 'before'\n", encoding="utf-8"
        )
        git(self.git_binary, "add", ".", cwd=self.repository)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "base",
            cwd=self.repository,
        )
        self.base_revision = git(
            self.git_binary, "rev-parse", "HEAD", cwd=self.repository
        )

        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.results = WorkspaceResultStore(
            self.root / "results", git_binary=self.git_binary
        )
        self.attempt_number = 0
        self.user = self.database.create_user("operator", "Local Operator")
        self.instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "1" * 32,
            krab_project_key="advisory-execution",
            display_name="Advisory Execution",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Prove bounded advisory execution",
                    "repository_path": str(self.repository),
                    "instructions": ["Advisory evidence has no gate authority"],
                }
            ),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="advisory-execution",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "advisory-execution-1",
                    "source_updated_at": "2026-10-25T18:00:00Z",
                    "title": "Review one exact candidate",
                    "brief": "Triangulate findings without creating gate authority.",
                    "acceptance_criteria": [
                        "Three reviewers remain independent during blind review."
                    ],
                }
            ),
            imported_by=self.user["id"],
        )
        producer_identity = self.identity("producer", "2")
        self.database.create_agent_profile(
            name="Producer",
            spec=AgentProfileSpec.create(
                image="agent@sha256:producer",
                command=["agent", "produce"],
                workspace_mode="copy_on_write",
                network_mode="none",
            ),
            krab_identity_id=producer_identity["id"],
            created_by=self.user["id"],
        )
        self.candidate = self.create_candidate()

        self.reviewer_capabilities = [
            "run.describe",
            "advisory.blind.submit",
            "advisory.challenge.submit",
        ]
        self.reviewer_one = self.profile("Reviewer One", "3")
        self.reviewer_two = self.profile("Reviewer Two", "4")
        self.reviewer_three = self.profile("Reviewer Three", "5")
        self.coordinator = self.profile(
            "Advisory Coordinator",
            "6",
            capabilities=["run.describe", "advisory.report.submit"],
        )
        self.configuration = self.database.create_advisory_configuration(
            project_key="advisory-execution",
            name="the-furies",
            spec=AdvisoryConfigurationSpec.from_mapping(
                {
                    "schema_version": 1,
                    "max_findings_per_reviewer": 4,
                    "max_challenges_per_reviewer": 4,
                    "max_report_items": 8,
                }
            ),
            reviewer_one_profile_name="Reviewer One",
            reviewer_two_profile_name="Reviewer Two",
            reviewer_three_profile_name="Reviewer Three",
            coordinator_profile_name="Advisory Coordinator",
            created_by=self.user["id"],
        )

    def identity(self, handle: str, fill: str) -> dict[str, object]:
        return self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + fill * 32, handle, 1),
            f"file:/private/{handle}",
        )

    def profile(
        self,
        name: str,
        fill: str,
        *,
        capabilities: list[str] | None = None,
    ) -> dict[str, object]:
        return self.database.create_agent_profile(
            name=name,
            spec=AgentProfileSpec.create(
                image="agent@sha256:advisory",
                command=["agent", "review"],
                workspace_mode="read_only",
                network_mode="gateway_only",
                gateway_capabilities=(
                    self.reviewer_capabilities if capabilities is None else capabilities
                ),
            ),
            krab_identity_id=self.identity(name.lower().replace(" ", "_"), fill)["id"],
            created_by=self.user["id"],
        )

    def create_candidate(self) -> dict[str, object]:
        run = self.database.create_run(
            project_key="advisory-execution",
            profile_name="Producer",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision=self.base_revision,
        )
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id=OWNER_ID,
            expected_run_version=run["version"],
        )
        attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=self.base_revision,
            container_id="a" * 64,
            container_name="advisory-producer",
            image_id=IMAGE_ID,
        )
        workspace = self.root / "producer-workspace"
        workspace.mkdir()
        GitSnapshotter(binary=self.git_binary).snapshot(
            self.repository, self.base_revision, workspace
        )
        self.results.stage(attempt["id"], workspace, self.base_revision)
        (workspace / "reviewed.py").write_text(
            "def value():\n    return 'after'\n", encoding="utf-8"
        )
        captured = self.results.capture(attempt["id"], workspace)
        self.database.record_attempt_result(attempt["id"], captured.manifest)
        self.database.complete_run_attempt(
            attempt["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id=IMAGE_ID,
        )
        store = GitCandidateStore(self.root / "candidates", binary=self.git_binary)
        return ControlPlane(self.database).create_candidate(
            attempt["id"],
            created_by_handle="operator",
            result_store=self.results,
            candidate_store=store,
        )

    def start(self, **overrides: object) -> dict[str, object]:
        arguments: dict[str, object] = {
            "project_key": "advisory-execution",
            "configuration_name": "the-furies",
            "candidate_id": self.candidate["id"],
            "expected_configuration_version": 1,
            "expected_context_version_id": self.project["context_version_id"],
            "expected_work_item_version_id": self.work_item["work_item_version_id"],
            "expected_thread_generation": 1,
            "expected_thread_version": 1,
            "idempotency_key": "start-advisory-1",
            "requested_by": self.user["id"],
        }
        arguments.update(overrides)
        return self.database.start_advisory_execution(**arguments)

    def create_role_run(
        self,
        execution: dict[str, object],
        stage: str,
        role: str,
        **overrides: object,
    ) -> dict[str, object]:
        arguments: dict[str, object] = {
            "project_key": "advisory-execution",
            "profile_name": execution["roles"][role]["profile_name"],
            "work_item_id": self.work_item["id"],
            "requested_by": self.user["id"],
            "source_revision": self.candidate["head_revision"],
            "advisory_execution_id": execution["id"],
            "advisory_stage": stage,
            "advisory_role": role,
        }
        arguments.update(overrides)
        return self.database.create_run(**arguments)

    def run_attempt(
        self,
        run: dict[str, object],
        submit: Callable[[dict[str, object]], object] | None = None,
        *,
        exit_code: int = 0,
    ) -> dict[str, object]:
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id=OWNER_ID,
            expected_run_version=run["version"],
        )
        self.attempt_number += 1
        attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=self.candidate["head_revision"],
            container_id=f"{self.attempt_number:064x}",
            container_name=f"advisory-{self.attempt_number}",
            image_id=IMAGE_ID,
        )
        if submit is not None:
            submit(attempt)
        workspace = self.root / f"review-workspace-{self.attempt_number}"
        workspace.mkdir()
        (workspace / "reviewed.py").write_text(
            "def value():\n    return 'after'\n", encoding="utf-8"
        )
        self.results.stage(attempt["id"], workspace, self.candidate["head_revision"])
        captured = self.results.capture(attempt["id"], workspace)
        self.database.record_attempt_result(attempt["id"], captured.manifest)
        return self.database.complete_run_attempt(
            attempt["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=attempt["version"],
            exit_code=exit_code,
            image_id=IMAGE_ID,
        )

    def complete_blind_phase(
        self, execution: dict[str, object]
    ) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
        runs: dict[str, dict[str, object]] = {}
        for role in ("reviewer_one", "reviewer_two", "reviewer_three"):
            run = self.create_role_run(execution, "blind_review", role)
            runs[role] = run
            self.run_attempt(
                run,
                lambda _attempt, role=role, run=run: (
                    self.database.submit_advisory_blind_review(
                        run["id"],
                        review=AdvisoryBlindReview.from_mapping(blind_value(role)),
                        idempotency_key=f"blind-{role}",
                    )
                ),
            )
        return (
            self.database.advance_advisory_execution(
                execution["id"], expected_version=execution["version"]
            ),
            runs,
        )

    def complete_challenge_phase(
        self, execution: dict[str, object]
    ) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
        targets = {
            "reviewer_one": ("reviewer_two", "reviewer-two-0"),
            "reviewer_two": ("reviewer_three", "reviewer-three-0"),
            "reviewer_three": ("reviewer_one", "reviewer-one-0"),
        }
        runs: dict[str, dict[str, object]] = {}
        for role, (target_role, target_key) in targets.items():
            run = self.create_role_run(execution, "challenge", role)
            runs[role] = run
            self.run_attempt(
                run,
                self.challenge_submission(run, role, target_role, target_key),
            )
        return (
            self.database.advance_advisory_execution(
                execution["id"], expected_version=execution["version"]
            ),
            runs,
        )

    def record_challenge(
        self,
        run: dict[str, object],
        role: str,
        target_role: str,
        target_key: str,
    ) -> dict[str, object]:
        return self.database.submit_advisory_challenge_review(
            run["id"],
            review=AdvisoryChallengeReview.from_mapping(
                challenge_value(
                    role,
                    target_role=target_role,
                    target_finding_key=target_key,
                )
            ),
            idempotency_key=f"challenge-{role}",
        )

    def challenge_submission(
        self,
        run: dict[str, object],
        role: str,
        target_role: str,
        target_key: str,
    ) -> Callable[[dict[str, object]], object]:
        return lambda _attempt: self.record_challenge(
            run, role, target_role, target_key
        )

    def test_start_pins_exact_candidate_roles_and_request_idempotently(self) -> None:
        execution = self.start()

        self.assertEqual(self.start(), execution)
        self.assertEqual(execution["state"], "active")
        self.assertEqual(execution["stage"], "blind_review")
        self.assertEqual(execution["template"]["id"], ADVISORY_REVIEW_TEMPLATE_ID)
        self.assertEqual(execution["candidate"]["id"], self.candidate["id"])
        self.assertEqual(
            execution["candidate"]["head_revision"],
            self.candidate["head_revision"],
        )
        self.assertEqual(
            execution["context_version_id"], self.project["context_version_id"]
        )
        self.assertEqual(
            execution["work_item_version_id"],
            self.work_item["work_item_version_id"],
        )
        self.assertEqual(
            set(execution["roles"]),
            {"reviewer_one", "reviewer_two", "reviewer_three", "coordinator"},
        )
        self.assertEqual(execution["runs"], {})
        self.assertEqual(execution["blind_reviews"], {})
        self.assertEqual(execution["challenge_reviews"], {})
        self.assertIsNone(execution["report"])
        self.assertEqual(
            self.database.list_advisory_executions(
                project_key="advisory-execution", state="active"
            ),
            [execution],
        )

    def test_start_rejects_stale_or_ambiguous_authority(self) -> None:
        execution = self.start()

        with self.assertRaisesRegex(ConflictError, "idempotency key"):
            self.start(expected_thread_version=2)
        with self.assertRaises(ConflictError):
            self.start(
                expected_configuration_version=2,
                idempotency_key="stale-configuration-version",
            )
        with self.assertRaises(NotFoundError):
            self.start(
                configuration_name="missing-configuration",
                idempotency_key="missing-configuration",
            )
        with self.assertRaises(NotFoundError):
            self.start(
                project_key="missing-project",
                idempotency_key="missing-project",
            )
        for field, value in (
            ("expected_context_version_id", "ctx_" + "f" * 32),
            ("expected_work_item_version_id", "wiv_" + "f" * 32),
            ("expected_thread_generation", 2),
            ("expected_thread_version", 2),
        ):
            with self.subTest(field=field), self.assertRaises(ConflictError):
                self.start(idempotency_key=f"stale-{field}", **{field: value})
        with self.assertRaises(ConflictError):
            self.start(idempotency_key="another-active-review")
        with self.assertRaises(NotFoundError):
            self.database.get_advisory_execution("arx_" + "f" * 32)
        with self.assertRaises(NotFoundError):
            self.start(
                candidate_id="cand_" + "f" * 32,
                idempotency_key="missing-candidate",
            )
        self.assertEqual(execution["version"], 1)

    def test_start_validates_bounds_and_service_resolves_operator_handles(self) -> None:
        for field, value in (
            ("expected_configuration_version", True),
            ("expected_configuration_version", 0),
            ("expected_thread_generation", 0),
            ("expected_thread_version", -1),
            ("idempotency_key", "contains spaces"),
        ):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                self.start(**{field: value})

        service = ControlPlane(self.database)
        execution = service.start_advisory_execution(
            project_key="advisory-execution",
            configuration_name="the-furies",
            candidate_id=self.candidate["id"],
            expected_configuration_version=1,
            expected_context_version_id=self.project["context_version_id"],
            expected_work_item_version_id=self.work_item["work_item_version_id"],
            expected_thread_generation=1,
            expected_thread_version=1,
            idempotency_key="service-start",
            requested_by_handle="operator",
        )

        self.assertEqual(service.get_advisory_execution(execution["id"]), execution)
        self.assertEqual(
            service.list_advisory_executions(
                project_key="advisory-execution", state="active", limit=1
            ),
            [execution],
        )
        cancelled = service.cancel_advisory_execution(
            execution["id"],
            expected_version=execution["version"],
            cancelled_by_handle="operator",
            reason="Service boundary cancellation.",
        )
        self.assertEqual(cancelled["state"], "cancelled")

    def test_phase_run_binding_pins_profile_stage_and_candidate_without_gate(
        self,
    ) -> None:
        execution = self.start()

        run = self.create_role_run(execution, "blind_review", "reviewer_one")
        stored = self.database.get_advisory_execution(execution["id"])
        assignment = self.database.get_run_execution(run["id"])

        self.assertEqual(run["advisory_execution_id"], execution["id"])
        self.assertEqual(run["advisory_stage"], "blind_review")
        self.assertEqual(run["advisory_role"], "reviewer_one")
        self.assertEqual(
            stored["runs"]["blind_review"]["reviewer_one"]["run_id"],
            run["id"],
        )
        self.assertEqual(
            assignment["advisory_assignment"],
            {
                "execution_id": execution["id"],
                "stage": "blind_review",
                "role": "reviewer_one",
                "candidate": execution["candidate"],
                "blind_reviews": {},
                "challenge_reviews": {},
            },
        )
        self.assertIsNone(assignment["review_assignment"])
        self.assertIsNone(assignment["gate_policy_id"])

        common = {
            "project_key": "advisory-execution",
            "profile_name": "Reviewer Two",
            "work_item_id": self.work_item["id"],
            "requested_by": self.user["id"],
            "source_revision": self.candidate["head_revision"],
        }
        with self.assertRaisesRegex(ValidationError, "binding is incomplete"):
            self.database.create_run(
                **common,
                advisory_execution_id=execution["id"],
            )
        with self.assertRaisesRegex(ValidationError, "integration gate policy"):
            self.database.create_run(
                **common,
                advisory_execution_id=execution["id"],
                advisory_stage="blind_review",
                advisory_role="reviewer_two",
                gate_policy_id="gp_" + "f" * 32,
            )
        with self.assertRaisesRegex(ValidationError, "Krab routing"):
            self.database.create_run(
                **common,
                advisory_execution_id=execution["id"],
                advisory_stage="blind_review",
                advisory_role="reviewer_two",
                krab_session_id="ses_" + "f" * 32,
            )
        with self.assertRaisesRegex(ValidationError, "another directed execution"):
            self.database.create_run(
                **common,
                advisory_execution_id=execution["id"],
                advisory_stage="blind_review",
                advisory_role="reviewer_two",
                discovery_execution_id="dsc_" + "f" * 32,
                discovery_role="investigator",
            )

        bad_cases = (
            {"source_revision": self.base_revision},
            {"profile_name": "Reviewer Two"},
            {"advisory_stage": "challenge"},
        )
        for index, overrides in enumerate(bad_cases):
            with self.subTest(case=index), self.assertRaises(ConflictError):
                self.create_role_run(
                    execution,
                    "blind_review",
                    "reviewer_one",
                    **overrides,
                )
        with self.assertRaises(ValidationError):
            self.create_role_run(
                execution,
                "blind_review",
                "reviewer_one",
                advisory_role="coordinator",
            )
        with self.assertRaises(ConflictError):
            self.create_role_run(execution, "blind_review", "reviewer_one")

    def test_exact_attempt_artifacts_advance_structurally_blind_stages(self) -> None:
        execution = self.start()
        challenge, blind_runs = self.complete_blind_phase(execution)

        self.assertEqual(challenge["stage"], "challenge")
        self.assertEqual(challenge["version"], 2)
        self.assertEqual(set(challenge["blind_reviews"]), set(blind_runs))
        blind_event_artifacts = {
            item["role"]: item
            for item in challenge["events"][-1]["metadata"]["artifacts"]
        }
        self.assertEqual(
            {
                key: challenge["events"][-1]["metadata"][key]
                for key in ("from_stage", "to_stage")
            },
            {"from_stage": "blind_review", "to_stage": "challenge"},
        )
        blind_attempt_ids = {
            role: challenge["blind_reviews"][role]["attempt_id"] for role in blind_runs
        }
        for role, run in blind_runs.items():
            self.assertEqual(challenge["blind_reviews"][role]["run_id"], run["id"])
            self.assertEqual(blind_event_artifacts[role]["run_id"], run["id"])
            self.assertEqual(
                blind_event_artifacts[role]["artifact_id"],
                challenge["blind_reviews"][role]["id"],
            )
            self.assertEqual(
                blind_event_artifacts[role]["attempt_id"],
                challenge["blind_reviews"][role]["attempt_id"],
            )
            self.assertEqual(
                blind_event_artifacts[role]["content_sha256"],
                challenge["blind_reviews"][role]["content_sha256"],
            )
            assignment = self.database.get_run_execution(run["id"])
            self.assertEqual(assignment["advisory_assignment"]["blind_reviews"], {})
            self.assertEqual(assignment["advisory_assignment"]["challenge_reviews"], {})

        first_challenge = self.create_role_run(challenge, "challenge", "reviewer_one")
        challenge_assignment = self.database.get_run_execution(first_challenge["id"])
        self.assertEqual(
            set(challenge_assignment["advisory_assignment"]["blind_reviews"]),
            {"reviewer_one", "reviewer_two", "reviewer_three"},
        )
        self.assertEqual(
            challenge_assignment["advisory_assignment"]["challenge_reviews"], {}
        )
        with self.assertRaisesRegex(ConflictError, "challenge runs are incomplete"):
            self.database.advance_advisory_execution(
                execution["id"], expected_version=challenge["version"]
            )

        # Use the already-bound first reviewer and then create the other two.
        targets = {
            "reviewer_one": ("reviewer_two", "reviewer-two-0"),
            "reviewer_two": ("reviewer_three", "reviewer-three-0"),
            "reviewer_three": ("reviewer_one", "reviewer-one-0"),
        }
        challenge_runs = {"reviewer_one": first_challenge}
        for role, (target_role, target_key) in targets.items():
            run = challenge_runs.get(role) or self.create_role_run(
                challenge, "challenge", role
            )
            challenge_runs[role] = run
            self.run_attempt(
                run,
                self.challenge_submission(run, role, target_role, target_key),
            )
        triangulate = self.database.advance_advisory_execution(
            execution["id"], expected_version=challenge["version"]
        )

        self.assertEqual(triangulate["stage"], "triangulate")
        self.assertEqual(triangulate["version"], 3)
        self.assertEqual(set(triangulate["challenge_reviews"]), set(challenge_runs))
        challenge_event_artifacts = {
            item["role"]: item
            for item in triangulate["events"][-1]["metadata"]["artifacts"]
        }
        self.assertEqual(
            {
                key: triangulate["events"][-1]["metadata"][key]
                for key in ("from_stage", "to_stage")
            },
            {"from_stage": "challenge", "to_stage": "triangulate"},
        )
        for role, run in challenge_runs.items():
            artifact = triangulate["challenge_reviews"][role]
            self.assertEqual(
                challenge_event_artifacts[role],
                {
                    "role": role,
                    "run_id": run["id"],
                    "attempt_id": artifact["attempt_id"],
                    "artifact_id": artifact["id"],
                    "content_sha256": artifact["content_sha256"],
                },
            )
        with self.assertRaisesRegex(ConflictError, "triangulation run is incomplete"):
            self.database.advance_advisory_execution(
                execution["id"], expected_version=triangulate["version"]
            )
        coordinator = self.create_role_run(triangulate, "triangulate", "coordinator")
        coordinator_assignment = self.database.get_run_execution(coordinator["id"])
        self.assertEqual(
            set(coordinator_assignment["advisory_assignment"]["blind_reviews"]),
            set(blind_runs),
        )
        self.assertEqual(
            set(coordinator_assignment["advisory_assignment"]["challenge_reviews"]),
            set(challenge_runs),
        )
        self.run_attempt(
            coordinator,
            lambda attempt: self.database.submit_advisory_report(
                coordinator["id"],
                report=AdvisoryReport.from_mapping(report_value()),
                idempotency_key="advisory-report",
            ),
        )
        completion = self.database.advance_advisory_execution(
            execution["id"], expected_version=triangulate["version"]
        )
        completed = self.database.complete_advisory_execution(
            execution["id"], expected_version=completion["version"]
        )

        self.assertEqual(completion["stage"], "completion")
        self.assertEqual(
            completion["events"][-1]["metadata"]["artifacts"],
            [
                {
                    "role": "coordinator",
                    "run_id": coordinator["id"],
                    "attempt_id": completion["report"]["attempt_id"],
                    "artifact_id": completion["report"]["id"],
                    "content_sha256": completion["report"]["content_sha256"],
                }
            ],
        )
        self.assertEqual(
            {
                key: completion["events"][-1]["metadata"][key]
                for key in ("from_stage", "to_stage")
            },
            {"from_stage": "triangulate", "to_stage": "completion"},
        )
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["version"], 5)
        self.assertEqual(completed["report"]["authority"], "advisory_only")
        self.assertEqual(completed["report"]["run_id"], coordinator["id"])
        self.assertNotIn(completed["report"]["attempt_id"], blind_attempt_ids.values())
        self.assertEqual(completed["events"][-1]["metadata"], {})
        self.assertEqual(
            [event["kind"] for event in completed["events"]],
            [
                "advisory.started",
                "advisory.blind_completed",
                "advisory.challenge_completed",
                "advisory.triangulation_completed",
                "advisory.completed",
            ],
        )
        with self.database.connect() as connection:
            gate_rows = connection.execute(
                "SELECT COUNT(*) FROM integration_gate_policies WHERE candidate_id = ?",
                (self.candidate["id"],),
            ).fetchone()[0]
            review_rows = connection.execute(
                "SELECT COUNT(*) FROM integration_gate_review_runs "
                "WHERE candidate_id = ?",
                (self.candidate["id"],),
            ).fetchone()[0]
        self.assertEqual((gate_rows, review_rows), (0, 0))

    def test_blind_submission_requires_bound_running_attempt_and_bounds(self) -> None:
        execution = self.start()
        review = AdvisoryBlindReview.from_mapping(blind_value("reviewer_one"))
        with self.assertRaises(ValidationError):
            self.database.submit_advisory_blind_review(
                "run_" + "f" * 32,
                review={},  # type: ignore[arg-type]
                idempotency_key="invalid-review",
            )
        with self.assertRaises(NotFoundError):
            self.database.submit_advisory_blind_review(
                "run_" + "f" * 32,
                review=review,
                idempotency_key="unbound-review",
            )

        first_run = self.create_role_run(execution, "blind_review", "reviewer_one")
        with self.assertRaisesRegex(ConflictError, "active worker attempt"):
            self.database.submit_advisory_blind_review(
                first_run["id"],
                review=review,
                idempotency_key="queued-review",
            )

        submitted: list[dict[str, object]] = []

        def submit_first(_attempt: dict[str, object]) -> None:
            first = self.database.submit_advisory_blind_review(
                first_run["id"], review=review, idempotency_key="blind-one"
            )
            self.assertEqual(
                set(first),
                {
                    "id",
                    "execution_id",
                    "reviewer_role",
                    "run_id",
                    "attempt_id",
                    "review",
                    "content_sha256",
                    "idempotency_key",
                    "created_at",
                },
            )
            self.assertEqual(first["execution_id"], execution["id"])
            self.assertEqual(first["reviewer_role"], "reviewer_one")
            self.assertEqual(first["review"], review.to_dict())
            self.assertEqual(first["content_sha256"], review.digest)
            self.assertEqual(first["idempotency_key"], "blind-one")
            repeated = self.database.submit_advisory_blind_review(
                first_run["id"], review=review, idempotency_key="blind-one"
            )
            self.assertEqual(repeated, first)
            submitted.append(first)
            with self.assertRaisesRegex(ConflictError, "idempotency key"):
                self.database.submit_advisory_blind_review(
                    first_run["id"],
                    review=AdvisoryBlindReview.from_mapping(
                        blind_value("reviewer_one", finding_count=0)
                    ),
                    idempotency_key="blind-one",
                )
            with self.assertRaisesRegex(ConflictError, "already submitted"):
                self.database.submit_advisory_blind_review(
                    first_run["id"],
                    review=review,
                    idempotency_key="blind-one-again",
                )

        self.run_attempt(first_run, submit_first)
        self.assertEqual(submitted[0]["run_id"], first_run["id"])
        with self.assertRaisesRegex(ConflictError, "active worker attempt"):
            self.database.submit_advisory_blind_review(
                first_run["id"],
                review=review,
                idempotency_key="after-completion",
            )

        second_run = self.create_role_run(execution, "blind_review", "reviewer_two")

        def submit_second(_attempt: dict[str, object]) -> None:
            with self.assertRaisesRegex(ConflictError, "configured finding limit"):
                self.database.submit_advisory_blind_review(
                    second_run["id"],
                    review=AdvisoryBlindReview.from_mapping(
                        blind_value("reviewer_two", finding_count=5)
                    ),
                    idempotency_key="too-many-findings",
                )
            self.database.submit_advisory_blind_review(
                second_run["id"],
                review=AdvisoryBlindReview.from_mapping(blind_value("reviewer_two")),
                idempotency_key="blind-two",
            )

        self.run_attempt(second_run, submit_second)
        with self.assertRaisesRegex(ConflictError, "runs are incomplete"):
            self.database.advance_advisory_execution(
                execution["id"], expected_version=execution["version"]
            )

    def test_challenge_and_report_references_are_exact_and_bounded(self) -> None:
        execution = self.start()
        challenge, _blind_runs = self.complete_blind_phase(execution)
        challenge_run = self.create_role_run(challenge, "challenge", "reviewer_one")

        def submit_challenge(_attempt: dict[str, object]) -> None:
            with self.assertRaises(ValidationError):
                self.database.submit_advisory_challenge_review(
                    challenge_run["id"],
                    review={},  # type: ignore[arg-type]
                    idempotency_key="invalid-challenge",
                )
            with self.assertRaisesRegex(ConflictError, "another stage"):
                self.database.submit_advisory_blind_review(
                    challenge_run["id"],
                    review=AdvisoryBlindReview.from_mapping(
                        blind_value("reviewer_one")
                    ),
                    idempotency_key="late-blind-review",
                )
            with self.assertRaisesRegex(ConflictError, "another role"):
                self.database.submit_advisory_report(
                    challenge_run["id"],
                    report=AdvisoryReport.from_mapping(report_value()),
                    idempotency_key="reviewer-report",
                )
            self_target = AdvisoryChallengeReview.from_mapping(
                challenge_value(
                    "reviewer_one",
                    target_role="reviewer_one",
                    target_finding_key="reviewer-one-0",
                )
            )
            with self.assertRaisesRegex(ConflictError, "another reviewer"):
                self.database.submit_advisory_challenge_review(
                    challenge_run["id"],
                    review=self_target,
                    idempotency_key="self-target",
                )
            unknown = AdvisoryChallengeReview.from_mapping(
                challenge_value(
                    "reviewer_one",
                    target_role="reviewer_two",
                    target_finding_key="not-recorded",
                )
            )
            with self.assertRaisesRegex(ConflictError, "unknown blind finding"):
                self.database.submit_advisory_challenge_review(
                    challenge_run["id"],
                    review=unknown,
                    idempotency_key="unknown-target",
                )
            too_many_value = challenge_value(
                "reviewer_one",
                target_role="reviewer_two",
                target_finding_key="reviewer-two-0",
            )
            prototype = too_many_value["challenges"][0]
            too_many_value["challenges"] = [
                {
                    **prototype,
                    "key": f"challenge-{index}",
                    "target_finding_key": f"unknown-{index}",
                }
                for index in range(5)
            ]
            with self.assertRaisesRegex(ConflictError, "configured limit"):
                self.database.submit_advisory_challenge_review(
                    challenge_run["id"],
                    review=AdvisoryChallengeReview.from_mapping(too_many_value),
                    idempotency_key="too-many-challenges",
                )
            valid = AdvisoryChallengeReview.from_mapping(
                challenge_value(
                    "reviewer_one",
                    target_role="reviewer_two",
                    target_finding_key="reviewer-two-0",
                )
            )
            first = self.database.submit_advisory_challenge_review(
                challenge_run["id"],
                review=valid,
                idempotency_key="challenge-one",
            )
            self.assertEqual(
                set(first),
                {
                    "id",
                    "execution_id",
                    "reviewer_role",
                    "run_id",
                    "attempt_id",
                    "review",
                    "content_sha256",
                    "idempotency_key",
                    "created_at",
                },
            )
            self.assertEqual(first["execution_id"], execution["id"])
            self.assertEqual(first["reviewer_role"], "reviewer_one")
            self.assertEqual(first["review"], valid.to_dict())
            self.assertEqual(first["content_sha256"], valid.digest)
            self.assertEqual(first["idempotency_key"], "challenge-one")
            self.assertEqual(
                self.database.submit_advisory_challenge_review(
                    challenge_run["id"],
                    review=valid,
                    idempotency_key="challenge-one",
                ),
                first,
            )
            changed = AdvisoryChallengeReview.from_mapping(
                {
                    **challenge_value(
                        "reviewer_one",
                        target_role="reviewer_two",
                        target_finding_key="reviewer-two-0",
                    ),
                    "summary": "A different challenge under the same key.",
                }
            )
            with self.assertRaisesRegex(ConflictError, "idempotency key"):
                self.database.submit_advisory_challenge_review(
                    challenge_run["id"],
                    review=changed,
                    idempotency_key="challenge-one",
                )
            with self.assertRaisesRegex(ConflictError, "already submitted"):
                self.database.submit_advisory_challenge_review(
                    challenge_run["id"],
                    review=valid,
                    idempotency_key="challenge-one-again",
                )

        self.run_attempt(challenge_run, submit_challenge)
        for role, target_role, target_key in (
            ("reviewer_two", "reviewer_three", "reviewer-three-0"),
            ("reviewer_three", "reviewer_one", "reviewer-one-0"),
        ):
            run = self.create_role_run(challenge, "challenge", role)
            self.run_attempt(
                run,
                self.challenge_submission(run, role, target_role, target_key),
            )
        triangulate = self.database.advance_advisory_execution(
            execution["id"], expected_version=challenge["version"]
        )
        coordinator = self.create_role_run(triangulate, "triangulate", "coordinator")

        def submit_report(_attempt: dict[str, object]) -> None:
            with self.assertRaises(ValidationError):
                self.database.submit_advisory_report(
                    coordinator["id"],
                    report={},  # type: ignore[arg-type]
                    idempotency_key="invalid-report",
                )
            unknown_value = copy.deepcopy(report_value())
            unknown_value["items"][0]["finding_refs"][0]["finding_key"] = "missing"
            with self.assertRaisesRegex(ConflictError, "unknown blind finding"):
                self.database.submit_advisory_report(
                    coordinator["id"],
                    report=AdvisoryReport.from_mapping(unknown_value),
                    idempotency_key="unknown-report-ref",
                )
            unknown_challenge_value = copy.deepcopy(report_value())
            unknown_challenge_value["items"][0]["challenge_refs"][0][
                "challenge_key"
            ] = "missing"
            with self.assertRaisesRegex(ConflictError, "unknown challenge"):
                self.database.submit_advisory_report(
                    coordinator["id"],
                    report=AdvisoryReport.from_mapping(unknown_challenge_value),
                    idempotency_key="unknown-challenge-ref",
                )
            too_many_value = copy.deepcopy(report_value())
            prototype = too_many_value["items"][0]
            too_many_value["items"] = [
                {**copy.deepcopy(prototype), "key": f"report-{index}"}
                for index in range(9)
            ]
            with self.assertRaisesRegex(ConflictError, "configured item limit"):
                self.database.submit_advisory_report(
                    coordinator["id"],
                    report=AdvisoryReport.from_mapping(too_many_value),
                    idempotency_key="too-many-report-items",
                )
            report = AdvisoryReport.from_mapping(report_value())
            first = self.database.submit_advisory_report(
                coordinator["id"],
                report=report,
                idempotency_key="report-one",
            )
            self.assertEqual(
                set(first),
                {
                    "id",
                    "execution_id",
                    "run_id",
                    "attempt_id",
                    "authority",
                    "report",
                    "content_sha256",
                    "idempotency_key",
                    "created_at",
                },
            )
            self.assertEqual(first["execution_id"], execution["id"])
            self.assertEqual(first["authority"], "advisory_only")
            self.assertEqual(first["report"], report.to_dict())
            self.assertEqual(first["content_sha256"], report.digest)
            self.assertEqual(first["idempotency_key"], "report-one")
            self.assertEqual(
                self.database.submit_advisory_report(
                    coordinator["id"],
                    report=report,
                    idempotency_key="report-one",
                ),
                first,
            )
            changed_value = copy.deepcopy(report_value())
            changed_value["summary"] = "A different report under the same key."
            with self.assertRaisesRegex(ConflictError, "idempotency key"):
                self.database.submit_advisory_report(
                    coordinator["id"],
                    report=AdvisoryReport.from_mapping(changed_value),
                    idempotency_key="report-one",
                )
            with self.assertRaisesRegex(ConflictError, "already has a report"):
                self.database.submit_advisory_report(
                    coordinator["id"],
                    report=report,
                    idempotency_key="report-one-again",
                )
            with self.assertRaisesRegex(ConflictError, "have not succeeded"):
                self.database.advance_advisory_execution(
                    execution["id"], expected_version=triangulate["version"]
                )

        self.run_attempt(coordinator, submit_report)

    def test_phase_advance_requires_successful_runs_and_exact_evidence(self) -> None:
        execution = self.start()
        for role in ("reviewer_one", "reviewer_two", "reviewer_three"):
            run = self.create_role_run(execution, "blind_review", role)
            self.run_attempt(
                run,
                lambda _attempt, role=role, run=run: (
                    self.database.submit_advisory_blind_review(
                        run["id"],
                        review=AdvisoryBlindReview.from_mapping(blind_value(role)),
                        idempotency_key=f"failed-phase-{role}",
                    )
                ),
                exit_code=1 if role == "reviewer_one" else 0,
            )
        with self.assertRaisesRegex(ConflictError, "have not succeeded"):
            self.database.advance_advisory_execution(
                execution["id"], expected_version=execution["version"]
            )
        self.database.fail_advisory_execution(
            execution["id"],
            expected_version=execution["version"],
            reason="A reviewer run failed.",
        )

        missing = self.start(idempotency_key="missing-advisory-evidence")
        for role in ("reviewer_one", "reviewer_two", "reviewer_three"):
            run = self.create_role_run(missing, "blind_review", role)
            if role == "reviewer_one":
                self.run_attempt(run)
            else:
                self.run_attempt(
                    run,
                    lambda _attempt, role=role, run=run: (
                        self.database.submit_advisory_blind_review(
                            run["id"],
                            review=AdvisoryBlindReview.from_mapping(blind_value(role)),
                            idempotency_key=f"missing-phase-{role}",
                        )
                    ),
                )
        with self.assertRaisesRegex(ConflictError, "evidence is incomplete"):
            self.database.advance_advisory_execution(
                missing["id"], expected_version=missing["version"]
            )

    def test_advisory_candidate_workspace_recovers_without_a_gate_policy(self) -> None:
        candidate_root = self.root / "candidates"
        shutil.rmtree(candidate_root)
        workspaces = WorkspaceManager(
            self.root / "advisory-workspaces",
            GitSnapshotter(binary=self.git_binary),
        )
        prepared = CandidateReviewWorkspaces(
            self.database,
            workspaces,
            self.results,
            GitCandidateStore(candidate_root, binary=self.git_binary),
        ).prepare_candidate("run_" + "e" * 32, self.candidate["id"])

        self.assertEqual(prepared.revision, self.candidate["head_revision"])
        self.assertEqual(
            (prepared.path / "reviewed.py").read_text(encoding="utf-8"),
            "def value():\n    return 'after'\n",
        )
        self.assertTrue(workspaces.cleanup("run_" + "e" * 32))

    def test_terminal_transitions_preserve_requester_authority_and_versions(
        self,
    ) -> None:
        execution = self.start()
        bound_run = self.create_role_run(execution, "blind_review", "reviewer_one")
        preparing = self.database.begin_run_attempt(
            bound_run["id"],
            runtime_adapter="command",
            owner_id=OWNER_ID,
            expected_run_version=bound_run["version"],
        )
        active_attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=self.candidate["head_revision"],
            container_id="f" * 64,
            container_name="cancelled-advisory-reviewer",
            image_id=IMAGE_ID,
        )
        with self.assertRaisesRegex(ConflictError, "not completing"):
            self.database.complete_advisory_execution(
                execution["id"], expected_version=execution["version"]
            )
        with self.assertRaises(ConcurrentUpdateError):
            self.database.advance_advisory_execution(
                execution["id"], expected_version=execution["version"] + 1
            )
        another = self.database.create_user("another", "Another Operator")
        with self.assertRaisesRegex(ConflictError, "pinned advisory requester"):
            self.database.cancel_advisory_execution(
                execution["id"],
                expected_version=execution["version"],
                cancelled_by=another["id"],
                reason="Not my execution.",
            )
        cancelled = self.database.cancel_advisory_execution(
            execution["id"],
            expected_version=execution["version"],
            cancelled_by=self.user["id"],
            reason="Operator stopped the advisory review.",
        )

        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(cancelled["version"], 2)
        self.assertEqual(
            cancelled["cancel_reason"], "Operator stopped the advisory review."
        )
        self.assertIsNone(cancelled["failure_reason"])
        self.assertEqual(cancelled["events"][-1]["kind"], "advisory.cancelled")
        self.assertEqual(
            cancelled["events"][-1]["metadata"],
            {"reason": "Operator stopped the advisory review."},
        )
        with self.assertRaisesRegex(ConflictError, "not accepting"):
            self.database.submit_advisory_blind_review(
                bound_run["id"],
                review=AdvisoryBlindReview.from_mapping(blind_value("reviewer_one")),
                idempotency_key="cancelled-submission",
            )
        self.assertEqual(active_attempt["state"], "running")
        with self.assertRaisesRegex(ConflictError, "cannot advance"):
            self.database.advance_advisory_execution(
                execution["id"], expected_version=cancelled["version"]
            )
        with self.assertRaisesRegex(ConflictError, "not active"):
            self.database.fail_advisory_execution(
                execution["id"],
                expected_version=cancelled["version"],
                reason="Cannot fail a cancelled execution.",
            )
        with self.assertRaises(ConflictError):
            self.create_role_run(cancelled, "blind_review", "reviewer_two")
        with (
            self.database.connect() as connection,
            self.assertRaises(sqlite3.IntegrityError),
        ):
            connection.execute(
                "UPDATE advisory_run_bindings SET role = 'reviewer_two' "
                "WHERE run_id = ?",
                (bound_run["id"],),
            )

        failed_execution = self.start(idempotency_key="failed-advisory")
        failed = self.database.fail_advisory_execution(
            failed_execution["id"],
            expected_version=failed_execution["version"],
            reason="A pinned provider budget was exhausted.",
        )
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(
            failed["failure_reason"], "A pinned provider budget was exhausted."
        )
        self.assertEqual(failed["events"][-1]["kind"], "advisory.failed")
        self.assertEqual(
            failed["events"][-1]["metadata"],
            {"reason": "A pinned provider budget was exhausted."},
        )
        self.assertEqual(
            [
                item["id"]
                for item in self.database.list_advisory_executions(
                    project_key="advisory-execution", state="failed", limit=1
                )
            ],
            [failed_execution["id"]],
        )
        for arguments in (
            {"limit": True},
            {"limit": 0},
            {"limit": 501},
            {"state": "unknown"},
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValidationError):
                self.database.list_advisory_executions(**arguments)

    def test_schema_thirty_seven_adds_advisory_ledger_once(self) -> None:
        with self.database.connect() as connection:
            connection.executescript(
                """
                DROP TABLE advisory_reports;
                DROP TABLE advisory_challenge_reviews;
                DROP TABLE advisory_blind_reviews;
                DROP TABLE advisory_run_bindings;
                DROP TABLE advisory_execution_events;
                DROP TABLE advisory_executions;
                PRAGMA user_version = 36;
                """
            )

        migrated = Database(self.database.path)
        first = migrated.initialize()
        second = migrated.initialize()

        self.assertEqual(SCHEMA_VERSION, 37)
        self.assertEqual(first, second)
        self.assertEqual(first["schema_version"], SCHEMA_VERSION)
        with migrated.connect() as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table' "
                    "AND name LIKE 'advisory_%'"
                )
            }
        self.assertEqual(
            tables,
            {
                "advisory_blind_reviews",
                "advisory_challenge_reviews",
                "advisory_execution_events",
                "advisory_executions",
                "advisory_reports",
                "advisory_run_bindings",
            },
        )

    def test_active_advisory_keeps_its_work_thread_open(self) -> None:
        self.start()

        with self.assertRaisesRegex(ConflictError, "active advisory prescription"):
            self.database.complete_work_item(
                self.work_item["id"],
                completed_by=self.user["id"],
                expected_thread_version=1,
                reason="must not close around an active advisory review",
            )


if __name__ == "__main__":
    unittest.main()
