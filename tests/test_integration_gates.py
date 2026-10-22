from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from collections.abc import Callable
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest import mock

from magic_glass_box.candidates import CandidateManifest, GitCandidateStore
from magic_glass_box.control_room import ControlRoom
from magic_glass_box.db import (
    GATE_CHANGE_SCHEMA,
    GATE_SCHEMA,
    SCHEMA,
    SCHEMA_VERSION,
    ConflictError,
    Database,
    NotFoundError,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    NetworkMode,
    ProjectContext,
    ValidationError,
    WorkItemEnvelope,
    WorkLogEntry,
    WorkspaceMode,
)
from magic_glass_box.executor import DockerImage, SandboxPlan, SandboxResult
from magic_glass_box.gates import (
    GATE_EVIDENCE_SCHEMA_VERSION,
    GATE_POLICY_SCHEMA_VERSION,
    ExactGateInputs,
    GateError,
    GateOutcome,
    GatePolicyManifest,
    GatePolicySpec,
    ReviewEvidence,
    ReviewSeparation,
    RunProvenance,
    ValidationEvidence,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.orchestration import (
    CandidateReviewWorkspaces,
    RunCoordinator,
    RunOrchestrationError,
)
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager


def git(binary: Path, *arguments: str, cwd: Path | None = None) -> str:
    completed = subprocess.run(  # noqa: S603 - test-controlled Git executable
        [str(binary), *arguments],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=True,
    )
    return completed.stdout.decode("utf-8", "strict").strip()


class GatePolicySpecTests(unittest.TestCase):
    def policy(self) -> GatePolicySpec:
        return GatePolicySpec.from_mapping(
            {
                "schema_version": GATE_POLICY_SCHEMA_VERSION,
                "review_separation": "different_profile_and_identity",
                "validations": [
                    {
                        "name": "unit",
                        "image_id": "sha256:" + "1" * 64,
                        "command": ["python", "-m", "unittest"],
                    }
                ],
            }
        )

    def test_round_trip_is_canonical_and_names_every_gate(self) -> None:
        policy = self.policy()

        self.assertEqual(GatePolicySpec.from_mapping(policy.to_dict()), policy)
        self.assertEqual(
            policy.review_separation,
            ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
        )
        self.assertEqual(
            policy.required_gates,
            ("independent-review", "validation:unit"),
        )
        self.assertEqual(len(policy.revision), 64)
        self.assertEqual(policy.canonical, policy.canonical)

    def test_rejects_empty_unknown_mutable_or_ambiguous_policy_inputs(self) -> None:
        valid = self.policy().to_dict()
        bad_cases = (
            (
                {"schema_version": 1, "review_separation": None, "validations": []},
                "gate",
            ),
            ({**valid, "unknown": True}, "unknown"),
            ({**valid, "schema_version": True}, "schema_version"),
            ({**valid, "review_separation": "same-run"}, "review"),
            (
                {
                    "schema_version": 1,
                    "review_separation": "different_run",
                    "validations": [],
                },
                "review",
            ),
            (
                {
                    **valid,
                    "validations": [
                        {
                            "name": "unit",
                            "image_id": "python:latest",
                            "command": ["test"],
                        }
                    ],
                },
                "image",
            ),
            (
                {
                    **valid,
                    "validations": [
                        valid["validations"][0],
                        valid["validations"][0],
                    ],
                },
                "unique",
            ),
            (
                {
                    **valid,
                    "validations": [
                        {
                            **valid["validations"][0],
                            "command": ["x" * 1025],
                        }
                    ],
                },
                "argument",
            ),
        )
        for value, message in bad_cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(GateError, message),
            ):
                GatePolicySpec.from_mapping(value)


class GatePolicyManifestTests(unittest.TestCase):
    def policy(self) -> GatePolicySpec:
        return GatePolicySpec.from_mapping(
            {
                "schema_version": 1,
                "review_separation": "different_profile",
                "validations": [],
            }
        )

    def candidate(self) -> dict[str, str]:
        return {
            "id": "cnd_" + "1" * 32,
            "project_id": "boxp_" + "2" * 32,
            "context_version_id": "ctx_" + "3" * 32,
            "work_thread_id": "wth_" + "4" * 32,
            "base_revision": "5" * 40,
            "head_revision": "6" * 40,
            "tree_revision": "7" * 40,
            "object_format": "sha1",
        }

    def test_bind_pins_the_complete_exact_input_tuple(self) -> None:
        manifest = GatePolicyManifest.bind(
            self.candidate(), default_branch="main", policy=self.policy()
        )

        self.assertEqual(manifest.repository_identity, self.candidate()["project_id"])
        self.assertEqual(manifest.integration_ref, "refs/heads/main")
        self.assertEqual(manifest.policy_revision, self.policy().revision)
        self.assertEqual(GatePolicyManifest.from_mapping(manifest.to_dict()), manifest)
        self.assertEqual(len(manifest.digest), 64)

    def test_rejects_tampered_revision_object_tuple_and_unsafe_ref(self) -> None:
        manifest = GatePolicyManifest.bind(
            self.candidate(), default_branch="main", policy=self.policy()
        )
        for field, bad in (
            ("policy_revision", "0" * 64),
            ("head_revision", "0" * 64),
            ("integration_ref", "refs/heads/../main"),
            ("candidate_id", "run_" + "1" * 32),
        ):
            value = manifest.to_dict()
            value[field] = bad
            with self.subTest(field=field), self.assertRaises(GateError):
                GatePolicyManifest.from_mapping(value)


class GateEvidenceManifestTests(unittest.TestCase):
    def policy_manifest(self) -> GatePolicyManifest:
        return GatePolicyManifest.bind(
            {
                "id": "cnd_" + "1" * 32,
                "project_id": "boxp_" + "2" * 32,
                "context_version_id": "ctx_" + "3" * 32,
                "work_thread_id": "wth_" + "4" * 32,
                "base_revision": "5" * 40,
                "head_revision": "6" * 40,
                "tree_revision": "7" * 40,
                "object_format": "sha1",
            },
            default_branch="main",
            policy=GatePolicySpec.from_mapping(
                {
                    "schema_version": 1,
                    "review_separation": "different_profile_and_identity",
                    "validations": [
                        {
                            "name": "unit",
                            "image_id": "sha256:" + "8" * 64,
                            "command": ["python", "-m", "unittest"],
                        }
                    ],
                }
            ),
        )

    def provenance(self, digit: str) -> RunProvenance:
        return RunProvenance(
            run_id="run_" + digit * 32,
            attempt_id="att_" + digit * 32,
            profile_id="agp_" + digit * 32,
            profile_version_id="apv_" + digit * 32,
            identity_id="kid_" + digit * 32,
            requested_by="boxu_" + digit * 32,
        )

    def test_review_evidence_round_trip_binds_both_provenances_and_exact_inputs(
        self,
    ) -> None:
        manifest = self.policy_manifest()
        evidence = ReviewEvidence(
            schema_version=GATE_EVIDENCE_SCHEMA_VERSION,
            inputs=ExactGateInputs.from_policy("gpl_" + "9" * 32, manifest),
            outcome=GateOutcome.PASSED,
            separation=ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
            producer=self.provenance("a"),
            reviewer=self.provenance("b"),
        )

        self.assertEqual(ReviewEvidence.from_mapping(evidence.to_dict()), evidence)
        self.assertEqual(evidence.gate_name, "independent-review")
        self.assertEqual(len(evidence.digest), 64)
        evidence.inputs.assert_matches(manifest)

    def test_validation_evidence_round_trip_retains_only_bounded_metadata(self) -> None:
        manifest = self.policy_manifest()
        validation = manifest.policy.validation("unit")
        evidence = ValidationEvidence(
            schema_version=GATE_EVIDENCE_SCHEMA_VERSION,
            inputs=ExactGateInputs.from_policy("gpl_" + "9" * 32, manifest),
            outcome=GateOutcome.PASSED,
            validation_name="unit",
            image_id=validation.image_id,
            command_sha256=validation.command_sha256,
            sandbox_id="run_" + "c" * 32,
            exit_code=0,
            duration_ms=123,
            output_sha256="d" * 64,
            output_bytes=42,
        )

        self.assertEqual(ValidationEvidence.from_mapping(evidence.to_dict()), evidence)
        self.assertEqual(evidence.gate_name, "validation:unit")
        self.assertNotIn("output", evidence.to_dict())
        self.assertEqual(len(evidence.digest), 64)

    def test_rejects_self_review_separation_mismatch_and_exit_outcome_mismatch(
        self,
    ) -> None:
        manifest = self.policy_manifest()
        inputs = ExactGateInputs.from_policy("gpl_" + "9" * 32, manifest)
        producer = self.provenance("a")
        with self.assertRaisesRegex(GateError, "reviewer run"):
            ReviewEvidence(
                GATE_EVIDENCE_SCHEMA_VERSION,
                inputs,
                GateOutcome.PASSED,
                ReviewSeparation.DIFFERENT_PROFILE,
                producer,
                producer,
            )
        with self.assertRaisesRegex(GateError, "profile"):
            ReviewEvidence(
                GATE_EVIDENCE_SCHEMA_VERSION,
                inputs,
                GateOutcome.PASSED,
                ReviewSeparation.DIFFERENT_PROFILE,
                producer,
                RunProvenance(
                    run_id="run_" + "b" * 32,
                    attempt_id="att_" + "b" * 32,
                    profile_id=producer.profile_id,
                    profile_version_id="apv_" + "b" * 32,
                    identity_id="kid_" + "b" * 32,
                    requested_by="boxu_" + "b" * 32,
                ),
            )
        with self.assertRaisesRegex(GateError, "identity"):
            ReviewEvidence(
                GATE_EVIDENCE_SCHEMA_VERSION,
                inputs,
                GateOutcome.PASSED,
                ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
                producer,
                RunProvenance(
                    run_id="run_" + "b" * 32,
                    attempt_id="att_" + "b" * 32,
                    profile_id="agp_" + "b" * 32,
                    profile_version_id="apv_" + "b" * 32,
                    identity_id=producer.identity_id,
                    requested_by="boxu_" + "b" * 32,
                ),
            )
        with self.assertRaisesRegex(GateError, "exit code"):
            ValidationEvidence(
                GATE_EVIDENCE_SCHEMA_VERSION,
                inputs,
                GateOutcome.PASSED,
                "unit",
                "sha256:" + "8" * 64,
                "e" * 64,
                "run_" + "c" * 32,
                1,
                1,
                "d" * 64,
                0,
            )


class GateLedgerTests(unittest.TestCase):
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
        git(self.git_binary, "init", "--quiet", cwd=self.repository)
        (self.repository / "file.txt").write_text("before\n", encoding="utf-8")
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
        self.base = git(self.git_binary, "rev-parse", "HEAD", cwd=self.repository)

        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        self.instance = self.database.create_krab_instance(
            "Local", "http://127.0.0.1:8765"
        )
        self.producer_identity = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + "1" * 32, "producer", 1),
            "file:/private/producer",
            user_id=self.user["id"],
        )
        self.reviewer_identity = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + "2" * 32, "reviewer", 1),
            "file:/private/reviewer",
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="project",
            display_name="Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Test exact-input gates",
                    "repository_path": str(self.repository),
                    "default_branch": "main",
                }
            ),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "gate-ledger",
                    "source_updated_at": "2026-10-14T00:00:00Z",
                    "title": "Gate ledger",
                    "brief": "Prove exact-input readiness",
                }
            ),
            imported_by=self.user["id"],
        )
        self.producer_profile = self.database.create_agent_profile(
            name="Producer",
            spec=AgentProfileSpec.from_mapping(
                {
                    "image": "worker:local",
                    "command": ["worker", "run"],
                    "network_mode": "none",
                }
            ),
            krab_identity_id=self.producer_identity["id"],
            created_by=self.user["id"],
        )
        self.reviewer_profile = self.database.create_agent_profile(
            name="Reviewer",
            spec=AgentProfileSpec.from_mapping(
                {
                    "image": "reviewer:local",
                    "command": ["reviewer", "run"],
                    "network_mode": "none",
                    "workspace_mode": "read_only",
                }
            ),
            krab_identity_id=self.reviewer_identity["id"],
            created_by=self.user["id"],
        )
        producer_run = self.database.create_run(
            project_key="project",
            profile_name="Producer",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision=self.base,
        )
        self.producer_attempt = self._succeed_attempt(
            producer_run, source_revision=self.base, changed=True, digit="4"
        )
        self.results = self._results
        self.candidate_store = GitCandidateStore(
            self.root / "candidates", binary=self.git_binary
        )
        self.control = ControlPlane(self.database)
        self.candidate = self.control.create_candidate(
            self.producer_attempt["id"],
            created_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidate_store,
        )

    def _succeed_attempt(
        self,
        run: dict[str, object],
        *,
        source_revision: str,
        changed: bool,
        digit: str,
        while_running: Callable[[dict[str, object]], None] | None = None,
    ) -> dict[str, object]:
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id="gate-test",
            expected_run_version=run["version"],
        )
        attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id="gate-test",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=source_revision,
            container_id=digit * 64,
            container_name=f"gate-test-{digit}",
            image_id="sha256:" + digit * 64,
        )
        workspace = self.root / f"workspace-{digit}"
        workspace.mkdir()
        (workspace / "file.txt").write_text("before\n", encoding="utf-8")
        result_store = getattr(
            self, "_results", WorkspaceResultStore(self.root / "results")
        )
        self._results = result_store
        result_store.stage(attempt["id"], workspace, source_revision)
        if changed:
            (workspace / "file.txt").write_text("after\n", encoding="utf-8")
        captured = result_store.capture(attempt["id"], workspace)
        self.database.record_attempt_result(attempt["id"], captured.manifest)
        if while_running is not None:
            while_running(attempt)
        return self.database.complete_run_attempt(
            attempt["id"],
            owner_id="gate-test",
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id="sha256:" + digit * 64,
        )

    def rejected_review(
        self,
        policy: dict[str, object],
        *,
        candidate_revision: str | None = None,
        summary: str = "Changes requested",
        details: list[str] | None = None,
        digit: str = "5",
    ) -> tuple[dict[str, object], dict[str, object]]:
        review_run = self.control.create_review_run(
            policy["id"],
            profile_name="Reviewer",
            requested_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        review_entry: dict[str, object] = {}

        def record_changes(_attempt: dict[str, object]) -> None:
            review_entry.update(
                self.control.append_work_log(
                    review_run["id"],
                    WorkLogEntry.from_mapping(
                        {
                            "schema_version": 1,
                            "kind": "review",
                            "summary": summary,
                            "details": (
                                [
                                    "Preserve the acknowledgement before closing "
                                    "the item."
                                ]
                                if details is None
                                else details
                            ),
                            "artifacts": [],
                            "related_interaction_ids": [],
                            "candidate_revision": candidate_revision
                            or self.candidate["head_revision"],
                        }
                    ),
                    idempotency_key="review-changes-requested",
                )
            )

        review_attempt = self._succeed_attempt(
            review_run,
            source_revision=self.candidate["head_revision"],
            changed=False,
            digit=digit,
            while_running=record_changes,
        )
        evidence = self.control.record_review_evidence(
            policy["id"],
            reviewer_attempt_id=review_attempt["id"],
            outcome="failed",
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        return evidence, review_entry

    def policy_spec(self) -> GatePolicySpec:
        return GatePolicySpec.from_mapping(
            {
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "validations": [
                    {
                        "name": "unit",
                        "image_id": "sha256:" + "9" * 64,
                        "command": ["python", "-m", "unittest"],
                    }
                ],
            }
        )

    def policy_manifest(self) -> GatePolicyManifest:
        return GatePolicyManifest.bind(
            self.candidate,
            default_branch="main",
            policy=self.policy_spec(),
        )

    def create_policy(self) -> dict[str, object]:
        return self.database.create_gate_policy(
            self.policy_manifest(), created_by=self.user["id"]
        )

    def create_same_identity_reviewer_profile(self) -> dict[str, object]:
        return self.database.create_agent_profile(
            name="SameIdentityReviewer",
            spec=AgentProfileSpec.from_mapping(
                {
                    "image": "same-identity-reviewer:local",
                    "command": ["reviewer", "run"],
                    "network_mode": "none",
                    "workspace_mode": "read_only",
                }
            ),
            krab_identity_id=self.producer_identity["id"],
            created_by=self.user["id"],
        )

    def provenance(
        self,
        *,
        run_id: str,
        attempt_id: str,
        profile: dict[str, object],
        identity: dict[str, object],
    ) -> RunProvenance:
        return RunProvenance(
            run_id=run_id,
            attempt_id=attempt_id,
            profile_id=profile["id"],
            profile_version_id=profile["profile_version_id"],
            identity_id=identity["id"],
            requested_by=self.user["id"],
        )

    def reviewer_attempt(
        self, policy: dict[str, object]
    ) -> tuple[dict[str, object], RunProvenance]:
        run = self.control.create_review_run(
            policy["id"],
            profile_name="Reviewer",
            requested_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        attempt = self._succeed_attempt(
            run,
            source_revision=self.candidate["head_revision"],
            changed=False,
            digit="5",
        )
        return attempt, self.provenance(
            run_id=run["id"],
            attempt_id=attempt["id"],
            profile=self.reviewer_profile,
            identity=self.reviewer_identity,
        )

    def producer_provenance(self) -> RunProvenance:
        return self.provenance(
            run_id=self.producer_attempt["run_id"],
            attempt_id=self.producer_attempt["id"],
            profile=self.producer_profile,
            identity=self.producer_identity,
        )

    def test_policy_create_is_exact_idempotent_immutable_and_queryable(self) -> None:
        created = self.create_policy()
        replayed = self.create_policy()

        self.assertEqual(replayed, created)
        self.assertEqual(created["manifest"], self.policy_manifest().to_dict())
        self.assertEqual(self.database.get_gate_policy(created["id"]), created)
        self.assertEqual(
            self.database.list_gate_policies(candidate_id=self.candidate["id"]),
            [created],
        )
        self.assertEqual(
            self.database.list_gate_policies(project_id=self.project["id"]), [created]
        )
        self.assertEqual(
            self.database.list_gate_policies(
                work_thread_id=self.candidate["work_thread_id"]
            ),
            [created],
        )
        with self.assertRaisesRegex(ConflictError, "candidate"):
            self.database.create_gate_policy(
                replace(self.policy_manifest(), head_revision="f" * 40),
                created_by=self.user["id"],
            )
        with closing(self.database.connect()) as connection:
            with self.assertRaises(sqlite3.IntegrityError), connection:
                connection.execute(
                    "UPDATE integration_gate_policies SET head_revision = ? "
                    "WHERE id = ?",
                    ("e" * 40, created["id"]),
                )
            with self.assertRaises(sqlite3.IntegrityError), connection:
                connection.execute(
                    "DELETE FROM integration_gate_policies WHERE id = ?",
                    (created["id"],),
                )

    def test_append_only_evidence_derives_readiness_from_latest_gate_outcomes(
        self,
    ) -> None:
        policy = self.create_policy()
        inputs = ExactGateInputs.from_policy(policy["id"], self.policy_manifest())
        readiness = self.database.gate_readiness(policy["id"])
        self.assertFalse(readiness["ready"])
        self.assertEqual(readiness["pending_count"], 2)

        _, reviewer = self.reviewer_attempt(policy)
        review = ReviewEvidence(
            GATE_EVIDENCE_SCHEMA_VERSION,
            inputs,
            GateOutcome.PASSED,
            ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
            self.producer_provenance(),
            reviewer,
        )
        recorded_review = self.database.record_gate_evidence(
            review, recorded_by=self.user["id"]
        )
        self.assertEqual(recorded_review["gate_name"], "independent-review")
        self.assertEqual(
            self.database.record_gate_evidence(review, recorded_by=self.user["id"]),
            recorded_review,
        )

        validation = self.policy_spec().validation("unit")
        failed = ValidationEvidence(
            GATE_EVIDENCE_SCHEMA_VERSION,
            inputs,
            GateOutcome.FAILED,
            "unit",
            validation.image_id,
            validation.command_sha256,
            "run_" + "6" * 32,
            1,
            20,
            "7" * 64,
            4,
        )
        self.database.record_gate_evidence(failed, recorded_by=self.user["id"])
        self.assertEqual(self.database.gate_readiness(policy["id"])["failed_count"], 1)

        passed = replace(
            failed,
            outcome=GateOutcome.PASSED,
            sandbox_id="run_" + "8" * 32,
            exit_code=0,
            output_sha256="9" * 64,
        )
        self.database.record_gate_evidence(passed, recorded_by=self.user["id"])
        readiness = self.database.gate_readiness(policy["id"])
        self.assertTrue(readiness["ready"])
        self.assertEqual(readiness["passed_count"], 2)
        self.assertEqual(readiness["failed_count"], 0)
        self.assertEqual(readiness["pending_count"], 0)
        self.assertEqual(
            [
                item["sequence"]
                for item in self.database.list_gate_evidence(policy["id"])
            ],
            [3, 2, 1],
        )

    def test_readiness_considers_every_gate_beyond_the_history_page(self) -> None:
        policy = self.create_policy()
        inputs = ExactGateInputs.from_policy(policy["id"], self.policy_manifest())
        _, reviewer = self.reviewer_attempt(policy)
        review = ReviewEvidence(
            GATE_EVIDENCE_SCHEMA_VERSION,
            inputs,
            GateOutcome.PASSED,
            ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
            self.producer_provenance(),
            reviewer,
        )
        self.database.record_gate_evidence(review, recorded_by=self.user["id"])
        validation = self.policy_spec().validation("unit")
        rows = []
        for sequence in range(2, 1002):
            evidence = ValidationEvidence(
                GATE_EVIDENCE_SCHEMA_VERSION,
                inputs,
                GateOutcome.PASSED,
                "unit",
                validation.image_id,
                validation.command_sha256,
                f"run_{sequence:032x}",
                0,
                sequence,
                f"{sequence:064x}",
                0,
            )
            rows.append(
                (
                    f"gev_{sequence:032x}",
                    policy["id"],
                    sequence,
                    evidence.schema_version,
                    "local_validation",
                    evidence.gate_name,
                    evidence.outcome.value,
                    evidence.canonical,
                    evidence.digest,
                    inputs.candidate_id,
                    inputs.repository_identity,
                    inputs.integration_ref,
                    inputs.expected_base_revision,
                    inputs.head_revision,
                    inputs.tree_revision,
                    inputs.object_format,
                    inputs.policy_revision,
                    self.user["id"],
                    f"2026-10-14T00:00:{sequence % 60:02d}+00:00",
                )
            )
        with closing(self.database.connect()) as connection, connection:
            connection.executemany(
                "INSERT INTO integration_gate_evidence "
                "(id, policy_id, sequence, schema_version, kind, gate_name, "
                "outcome, evidence_json, content_sha256, candidate_id, "
                "repository_identity, integration_ref, expected_base_revision, "
                "head_revision, tree_revision, object_format, policy_revision, "
                "recorded_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

        readiness = self.database.gate_readiness(policy["id"])

        self.assertTrue(readiness["ready"])
        self.assertEqual(readiness["passed_count"], 2)
        self.assertEqual(
            {gate["name"]: gate["sequence"] for gate in readiness["gates"]},
            {"independent-review": 1, "validation:unit": 1001},
        )

    def test_evidence_rejects_false_provenance_tuple_and_gate(self) -> None:
        policy = self.create_policy()
        manifest = self.policy_manifest()
        inputs = ExactGateInputs.from_policy(policy["id"], manifest)
        _, reviewer = self.reviewer_attempt(policy)
        review = ReviewEvidence(
            GATE_EVIDENCE_SCHEMA_VERSION,
            inputs,
            GateOutcome.PASSED,
            ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
            self.producer_provenance(),
            reviewer,
        )
        with self.assertRaisesRegex(ConflictError, "provenance"):
            self.database.record_gate_evidence(
                replace(
                    review,
                    reviewer=replace(reviewer, requested_by="boxu_" + "f" * 32),
                ),
                recorded_by=self.user["id"],
            )
        with self.assertRaisesRegex(ConflictError, "exact"):
            self.database.record_gate_evidence(
                replace(
                    review,
                    inputs=replace(inputs, head_revision="f" * 40),
                ),
                recorded_by=self.user["id"],
            )
        validation = self.policy_spec().validation("unit")
        with self.assertRaisesRegex(ConflictError, "validation"):
            self.database.record_gate_evidence(
                ValidationEvidence(
                    GATE_EVIDENCE_SCHEMA_VERSION,
                    inputs,
                    GateOutcome.PASSED,
                    "unit",
                    "sha256:" + "a" * 64,
                    validation.command_sha256,
                    "run_" + "b" * 32,
                    0,
                    1,
                    "c" * 64,
                    0,
                ),
                recorded_by=self.user["id"],
            )
        with self.assertRaises(NotFoundError):
            self.database.gate_readiness("gpl_" + "f" * 32)

    def test_control_plane_resolves_images_before_pinning_a_policy(self) -> None:
        resolver = mock.Mock(return_value=DockerImage("sha256:" + "d" * 64, ()))
        request = {
            "schema_version": 1,
            "review_separation": "different_profile_and_identity",
            "validations": [
                {
                    "name": "unit",
                    "image": "validator:local",
                    "command": ["python", "-m", "unittest"],
                }
            ],
        }

        created = self.control.create_gate_policy(
            self.candidate["id"],
            request,
            created_by_handle="operator",
            image_resolver=resolver,
        )

        resolver.assert_called_once_with("validator:local")
        stored = created["manifest"]["policy"]["validations"][0]
        self.assertEqual(stored["image_id"], "sha256:" + "d" * 64)
        self.assertNotIn("image", stored)
        with self.assertRaisesRegex(GateError, "volumes"):
            self.control.create_gate_policy(
                self.candidate["id"],
                request,
                created_by_handle="operator",
                image_resolver=lambda _reference: DockerImage(
                    "sha256:" + "e" * 64, ("/state",)
                ),
            )

    def test_control_plane_records_review_from_database_provenance_only(self) -> None:
        policy = self.create_policy()
        reviewer_attempt, _ = self.reviewer_attempt(policy)

        evidence = self.control.record_review_evidence(
            policy["id"],
            reviewer_attempt_id=reviewer_attempt["id"],
            outcome="passed",
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )

        self.assertEqual(evidence["outcome"], "passed")
        self.assertEqual(evidence["reviewer_attempt_id"], reviewer_attempt["id"])
        self.assertEqual(
            self.database.gate_readiness(policy["id"])["gates"][0]["state"],
            "passed",
        )
        with self.assertRaisesRegex(ConflictError, "revision"):
            self.control.record_review_evidence(
                policy["id"],
                reviewer_attempt_id=reviewer_attempt["id"],
                outcome="passed",
                recorded_by_handle="operator",
                expected_policy_revision="f" * 64,
            )

    def test_review_run_rejects_the_producer_profile_at_creation(self) -> None:
        policy = self.create_policy()

        with self.assertRaisesRegex(ConflictError, "profile"):
            self.control.create_review_run(
                policy["id"],
                profile_name="Producer",
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )

        self.assertEqual(self.database.status()["counts"]["runs"], 1)

    def test_review_run_requires_a_read_only_reviewer_profile(self) -> None:
        policy = self.create_policy()
        self.database.create_agent_profile(
            name="MutableReviewer",
            spec=AgentProfileSpec.from_mapping(
                {
                    "image": "mutable-reviewer:local",
                    "command": ["reviewer", "run"],
                    "network_mode": "none",
                }
            ),
            krab_identity_id=self.reviewer_identity["id"],
            created_by=self.user["id"],
        )

        with self.assertRaisesRegex(ConflictError, "read-only"):
            self.control.create_review_run(
                policy["id"],
                profile_name="MutableReviewer",
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )

        self.assertEqual(self.database.status()["counts"]["runs"], 1)

    def test_run_creation_rejects_an_oversized_work_item_identifier(self) -> None:
        with self.assertRaisesRegex(ValidationError, "work_item_id"):
            self.database.create_run(
                project_key="project",
                profile_name="Producer",
                work_item_id="w" * 65,
                requested_by=self.user["id"],
                source_revision=self.base,
            )

    def test_review_run_rejects_a_distinct_profile_with_the_producer_identity(
        self,
    ) -> None:
        policy = self.create_policy()
        self.create_same_identity_reviewer_profile()

        with self.assertRaisesRegex(ConflictError, "identity"):
            self.control.create_review_run(
                policy["id"],
                profile_name="SameIdentityReviewer",
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )

        self.assertEqual(self.database.status()["counts"]["runs"], 1)

    def test_active_review_keeps_its_gate_visible_when_terminal_history_is_zero(
        self,
    ) -> None:
        policy = self.create_policy()
        run = self.control.create_review_run(
            policy["id"],
            profile_name="Reviewer",
            requested_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )

        snapshot = ControlRoom(self.database).snapshot(history_limit=0)

        self.assertEqual([item["id"] for item in snapshot["runs"]], [run["id"]])
        self.assertEqual(
            [item["id"] for item in snapshot["gate_policies"]], [policy["id"]]
        )
        self.assertEqual(snapshot["runs"][0]["gate_policy_id"], policy["id"])
        self.assertEqual(
            snapshot["runs"][0]["review_candidate_id"], self.candidate["id"]
        )
        self.assertEqual(snapshot["summary"]["integration_gate_review_runs"], 1)

    def test_review_retry_preserves_binding_and_reassign_cannot_bypass_separation(
        self,
    ) -> None:
        policy = self.create_policy()
        run = self.control.create_review_run(
            policy["id"],
            profile_name="Reviewer",
            requested_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        self.assertEqual(run["gate_policy_id"], policy["id"])
        self.assertEqual(run["review_candidate_id"], self.candidate["id"])
        self._succeed_attempt(
            run,
            source_revision=self.candidate["head_revision"],
            changed=False,
            digit="5",
        )
        finished = self.database.get_run(run["id"])

        with self.assertRaisesRegex(ConflictError, "profile"):
            self.database.reassign_run(
                run["id"],
                profile_name="Producer",
                requested_by=self.user["id"],
                expected_run_version=finished["version"],
            )

        self.create_same_identity_reviewer_profile()
        with self.assertRaisesRegex(ConflictError, "identity"):
            self.database.reassign_run(
                run["id"],
                profile_name="SameIdentityReviewer",
                requested_by=self.user["id"],
                expected_run_version=finished["version"],
            )

        replacement = self.database.retry_run(
            run["id"],
            requested_by=self.user["id"],
            expected_run_version=finished["version"],
        )
        execution = self.database.get_run_execution(replacement["id"])
        self.assertEqual(execution["gate_policy_id"], policy["id"])
        self.assertEqual(execution["review_candidate_id"], self.candidate["id"])

    def test_failed_review_hands_exact_candidate_and_feedback_back_to_developer(
        self,
    ) -> None:
        policy = self.create_policy()
        evidence, review_entry = self.rejected_review(policy)

        changes_run = self.control.create_changes_run(
            policy["id"],
            review_evidence_id=evidence["id"],
            review_entry_id=review_entry["id"],
            profile_name="Producer",
            requested_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        execution = self.database.get_run_execution(changes_run["id"])

        self.assertEqual(changes_run["change_policy_id"], policy["id"])
        self.assertEqual(changes_run["change_candidate_id"], self.candidate["id"])
        self.assertEqual(
            changes_run["source_revision"], self.candidate["head_revision"]
        )
        self.assertEqual(
            execution["changes_requested"]["review_entry"]["id"],
            review_entry["id"],
        )
        self.assertEqual(
            execution["changes_requested"]["review_entry"]["details"],
            ["Preserve the acknowledgement before closing the item."],
        )

        observed: list[str] = []

        class Executor:
            def execute(inner_self, request: object, *, before_start=None):
                observed.append(
                    (request.workspace / "file.txt").read_text(encoding="utf-8")
                )
                (request.workspace / "file.txt").write_text(
                    "corrected\n", encoding="utf-8"
                )
                plan = SandboxPlan(
                    request.run_id,
                    "magic-glass-box-changes",
                    "sha256:" + "6" * 64,
                    (),
                )
                assert before_start is not None
                before_start(plan, "6" * 64)
                return SandboxResult(
                    "6" * 64,
                    plan.container_name,
                    plan.image_id,
                    0,
                )

            @staticmethod
            def cleanup_run(_run_id: str) -> int:
                return 0

            @staticmethod
            def stop_run(_run_id: str) -> int:
                return 1

        workspaces = WorkspaceManager(
            self.root / "changes-workspaces", GitSnapshotter(binary=self.git_binary)
        )
        candidate_workspaces = CandidateReviewWorkspaces(
            self.database,
            workspaces,
            self.results,
            self.candidate_store,
        )
        coordinator = RunCoordinator(
            self.database,
            Executor(),
            workspaces,
            None,
            self.results,
            candidate_workspaces=candidate_workspaces,
            heartbeat_interval=30,
        )

        finished = coordinator.execute(changes_run["id"])
        shutil.rmtree(self.root / "candidates")
        corrected = self.control.create_candidate(
            finished["attempts"][0]["id"],
            created_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidate_store,
        )

        self.assertEqual(observed, ["after\n"])
        self.assertEqual(corrected["base_revision"], self.candidate["head_revision"])
        self.assertNotEqual(corrected["head_revision"], self.candidate["head_revision"])
        corrected_policy = self.database.create_gate_policy(
            GatePolicyManifest.bind(
                corrected,
                default_branch="main",
                policy=self.policy_spec(),
            ),
            created_by=self.user["id"],
        )
        shutil.rmtree(self.root / "candidates")
        recovered = candidate_workspaces.prepare(
            "run_" + "8" * 32, corrected_policy["id"]
        )
        self.assertEqual(
            (recovered.path / "file.txt").read_text(encoding="utf-8"),
            "corrected\n",
        )
        workspaces.cleanup("run_" + "8" * 32)
        self.assertEqual(
            self.database.get_gate_policy(policy["id"])["candidate_id"],
            self.candidate["id"],
        )
        self.assertFalse(self.control.gate_readiness(policy["id"])["ready"])

        replacement = self.database.retry_run(
            finished["id"],
            requested_by=self.user["id"],
            expected_run_version=finished["version"],
        )
        self.assertEqual(
            replacement["events"][0]["metadata"]["review_evidence_id"],
            evidence["id"],
        )
        self.assertEqual(
            replacement["events"][0]["metadata"]["review_entry_id"],
            review_entry["id"],
        )
        replacement_execution = self.database.get_run_execution(replacement["id"])
        self.assertEqual(
            replacement_execution["change_candidate_id"], self.candidate["id"]
        )
        self.assertEqual(
            replacement_execution["changes_requested"]["review_entry"]["id"],
            review_entry["id"],
        )

    def test_changes_run_requires_the_current_failed_review(self) -> None:
        policy = self.create_policy()
        failed, review_entry = self.rejected_review(policy)

        passed = self.control.record_review_evidence(
            policy["id"],
            reviewer_attempt_id=failed["reviewer_attempt_id"],
            outcome="passed",
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )

        with self.assertRaisesRegex(ConflictError, "current"):
            self.control.create_changes_run(
                policy["id"],
                review_evidence_id=failed["id"],
                review_entry_id=review_entry["id"],
                profile_name="Producer",
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )
        with self.assertRaisesRegex(ConflictError, "failed"):
            self.control.create_changes_run(
                policy["id"],
                review_evidence_id=passed["id"],
                review_entry_id=review_entry["id"],
                profile_name="Producer",
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )

    def test_changes_run_requires_an_actionable_changes_requested_note(self) -> None:
        policy = self.create_policy()
        failed, approved_entry = self.rejected_review(
            policy,
            summary="APPROVED — exact candidate is ready",
            details=["No changes are required."],
        )

        with self.assertRaisesRegex(ConflictError, "changes-requested note"):
            self.control.create_changes_run(
                policy["id"],
                review_evidence_id=failed["id"],
                review_entry_id=approved_entry["id"],
                profile_name="Producer",
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )

        policy = self.create_policy()
        failed, empty_entry = self.rejected_review(policy, details=[], digit="7")
        with self.assertRaisesRegex(ConflictError, "actionable"):
            self.control.create_changes_run(
                policy["id"],
                review_evidence_id=failed["id"],
                review_entry_id=empty_entry["id"],
                profile_name="Producer",
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )

    def test_changes_run_requires_feedback_pinned_to_the_rejected_candidate(
        self,
    ) -> None:
        policy = self.create_policy()
        failed, review_entry = self.rejected_review(
            policy, candidate_revision=self.candidate["base_revision"]
        )

        with self.assertRaisesRegex(ConflictError, "exact candidate"):
            self.control.create_changes_run(
                policy["id"],
                review_evidence_id=failed["id"],
                review_entry_id=review_entry["id"],
                profile_name="Producer",
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )

    def test_review_evidence_rejects_an_unbound_same_head_run(self) -> None:
        policy = self.create_policy()
        run = self.database.create_run(
            project_key="project",
            profile_name="Reviewer",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision=self.candidate["head_revision"],
        )
        attempt = self._succeed_attempt(
            run,
            source_revision=self.candidate["head_revision"],
            changed=False,
            digit="6",
        )
        evidence = ReviewEvidence(
            GATE_EVIDENCE_SCHEMA_VERSION,
            ExactGateInputs.from_policy(policy["id"], self.policy_manifest()),
            GateOutcome.PASSED,
            ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
            self.producer_provenance(),
            self.provenance(
                run_id=run["id"],
                attempt_id=attempt["id"],
                profile=self.reviewer_profile,
                identity=self.reviewer_identity,
            ),
        )

        with self.assertRaisesRegex(ConflictError, "bound"):
            self.database.record_gate_evidence(evidence, recorded_by=self.user["id"])

    def test_reviewer_executes_the_exact_candidate_after_cache_recovery(self) -> None:
        policy = self.create_policy()
        run = self.control.create_review_run(
            policy["id"],
            profile_name="Reviewer",
            requested_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        review_execution = self.database.get_run_execution(run["id"])
        self.assertEqual(
            review_execution["review_assignment"],
            {
                "candidate_id": self.candidate["id"],
                "head_revision": self.candidate["head_revision"],
                "policy_id": policy["id"],
                "policy_revision": policy["policy_revision"],
                "review_separation": "different_profile_and_identity",
            },
        )
        shutil.rmtree(self.root / "candidates")
        observed: list[str] = []

        class Executor:
            def execute(inner_self, request: object, *, before_start=None):
                observed.append(
                    (request.workspace / "file.txt").read_text(encoding="utf-8")
                )
                plan = SandboxPlan(
                    request.run_id,
                    "magic-glass-box-review",
                    "sha256:" + "7" * 64,
                    (),
                )
                assert before_start is not None
                before_start(plan, "7" * 64)
                return SandboxResult(
                    "7" * 64,
                    plan.container_name,
                    plan.image_id,
                    0,
                )

            @staticmethod
            def cleanup_run(_run_id: str) -> int:
                return 0

            @staticmethod
            def stop_run(_run_id: str) -> int:
                return 1

        workspaces = WorkspaceManager(
            self.root / "review-workspaces", GitSnapshotter(binary=self.git_binary)
        )
        candidate_workspaces = CandidateReviewWorkspaces(
            self.database,
            workspaces,
            self.results,
            self.candidate_store,
        )
        coordinator = RunCoordinator(
            self.database,
            Executor(),
            workspaces,
            None,
            self.results,
            candidate_workspaces=candidate_workspaces,
            heartbeat_interval=30,
        )

        finished = coordinator.execute(run["id"])

        self.assertEqual(finished["state"], "succeeded")
        self.assertEqual(finished["source_revision"], self.candidate["head_revision"])
        self.assertEqual(observed, ["after\n"])
        attempt = finished["attempts"][0]
        evidence = self.control.record_review_evidence(
            policy["id"],
            reviewer_attempt_id=attempt["id"],
            outcome="passed",
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        self.assertEqual(evidence["outcome"], "passed")

    def test_candidate_backed_review_fails_closed_without_workspace_provider(
        self,
    ) -> None:
        policy = self.create_policy()
        run = self.control.create_review_run(
            policy["id"],
            profile_name="Reviewer",
            requested_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        workspaces = WorkspaceManager(
            self.root / "review-workspaces", GitSnapshotter(binary=self.git_binary)
        )

        class Executor:
            @staticmethod
            def execute(_request: object, *, before_start=None):
                raise AssertionError("review must fail before sandbox execution")

            @staticmethod
            def cleanup_run(_run_id: str) -> int:
                return 0

            @staticmethod
            def stop_run(_run_id: str) -> int:
                return 1

        coordinator = RunCoordinator(
            self.database,
            Executor(),
            workspaces,
            None,
            self.results,
            heartbeat_interval=30,
        )

        with self.assertRaises(RunOrchestrationError):
            coordinator.execute(run["id"])

        self.assertEqual(self.database.get_run(run["id"])["state"], "failed")

    def test_local_validation_requests_exact_networkless_sandbox_and_discards_output(
        self,
    ) -> None:
        policy = self.create_policy()
        requests: list[object] = []

        class Executor:
            def execute(inner_self, request: object) -> SandboxResult:
                requests.append(request)
                self.assertEqual(
                    request.profile.workspace_mode, WorkspaceMode.READ_ONLY
                )
                self.assertEqual(request.profile.network_mode, NetworkMode.NONE)
                self.assertEqual(request.credential_files, {})
                self.assertIsNone(request.gateway_directory)
                self.assertEqual(
                    (request.workspace / "file.txt").read_text(encoding="utf-8"),
                    "after\n",
                )
                return SandboxResult(
                    container_id="a" * 64,
                    container_name="validation",
                    image_id="sha256:" + "9" * 64,
                    exit_code=0,
                    stdout="private validation output",
                )

        evidence = self.control.run_local_validation(
            policy["id"],
            validation_name="unit",
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
            result_store=self.results,
            candidate_store=self.candidate_store,
            executor=Executor(),
        )

        self.assertEqual(len(requests), 1)
        self.assertEqual(evidence["outcome"], "passed")
        self.assertNotIn("private validation output", str(evidence))
        self.assertEqual(
            evidence["evidence"]["output_bytes"],
            len(b"private validation output"),
        )
        self.assertEqual(
            self.database.gate_readiness(policy["id"])["gates"][1]["state"],
            "passed",
        )
        workspaces = self.database.path.parent / "gate-workspaces"
        self.assertEqual(list(workspaces.iterdir()), [])

    def test_local_validation_records_a_nonzero_exit_as_failed(self) -> None:
        policy = self.create_policy()

        class Executor:
            @staticmethod
            def execute(_request: object) -> SandboxResult:
                return SandboxResult(
                    container_id="a" * 64,
                    container_name="validation",
                    image_id="sha256:" + "9" * 64,
                    exit_code=17,
                    stdout="private failure details",
                )

        evidence = self.control.run_local_validation(
            policy["id"],
            validation_name="unit",
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
            result_store=self.results,
            candidate_store=self.candidate_store,
            executor=Executor(),
        )

        self.assertEqual(evidence["outcome"], "failed")
        self.assertEqual(evidence["evidence"]["exit_code"], 17)
        self.assertNotIn("private failure details", str(evidence))
        readiness = self.database.gate_readiness(policy["id"])
        self.assertFalse(readiness["ready"])
        self.assertEqual(readiness["failed_count"], 1)

    def test_local_validation_rejects_image_drift_without_recording_evidence(
        self,
    ) -> None:
        policy = self.create_policy()

        class Executor:
            @staticmethod
            def execute(_request: object) -> SandboxResult:
                return SandboxResult(
                    container_id="a" * 64,
                    container_name="validation",
                    image_id="sha256:" + "8" * 64,
                    exit_code=0,
                )

        with self.assertRaisesRegex(ConflictError, "image identity"):
            self.control.run_local_validation(
                policy["id"],
                validation_name="unit",
                recorded_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
                result_store=self.results,
                candidate_store=self.candidate_store,
                executor=Executor(),
            )

        self.assertEqual(self.database.list_gate_evidence(policy["id"]), [])
        workspaces = self.database.path.parent / "gate-workspaces"
        self.assertEqual(list(workspaces.iterdir()), [])

    def test_local_validation_recovers_candidate_cache_and_cleans_up_on_failure(
        self,
    ) -> None:
        policy = self.create_policy()
        shutil.rmtree(self.root / "candidates")

        class Executor:
            def execute(inner_self, request: object) -> SandboxResult:
                self.assertEqual(
                    (request.workspace / "file.txt").read_text(encoding="utf-8"),
                    "after\n",
                )
                raise RuntimeError("validator unavailable")

        with self.assertRaisesRegex(RuntimeError, "validator unavailable"):
            self.control.run_local_validation(
                policy["id"],
                validation_name="unit",
                recorded_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
                result_store=self.results,
                candidate_store=self.candidate_store,
                executor=Executor(),
            )

        self.assertEqual(self.database.list_gate_evidence(policy["id"]), [])
        self.candidate_store.inspect(
            CandidateManifest.from_mapping(self.candidate["manifest"])
        )
        workspaces = self.database.path.parent / "gate-workspaces"
        self.assertEqual(list(workspaces.iterdir()), [])


class GateMigrationTests(unittest.TestCase):
    def test_schema_twenty_five_adds_immutable_change_run_bindings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "control-plane.db"
            historical_schema = SCHEMA[: SCHEMA.index(GATE_CHANGE_SCHEMA)]
            with closing(sqlite3.connect(path)) as connection:
                connection.executescript(historical_schema)
                connection.execute("PRAGMA user_version = 25")
                connection.commit()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
            self.assertEqual(second, first)
            self.assertEqual(first["counts"]["integration_gate_change_runs"], 0)
            with closing(database.connect()) as connection:
                table = connection.execute(
                    "SELECT sql FROM sqlite_schema WHERE type = 'table' "
                    "AND name = 'integration_gate_change_runs'"
                ).fetchone()
                triggers = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'trigger' "
                        "AND tbl_name = 'integration_gate_change_runs'"
                    )
                }
            self.assertIsNotNone(table)
            self.assertEqual(
                triggers,
                {
                    "integration_gate_change_run_immutable_delete",
                    "integration_gate_change_run_immutable_update",
                },
            )

    def test_schema_twelve_migrates_existing_gate_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "control-plane.db"
            connection = sqlite3.connect(path)
            connection.executescript(SCHEMA[: SCHEMA.index(GATE_SCHEMA)])
            connection.execute("PRAGMA user_version = 12")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
            self.assertEqual(second, first)
            self.assertEqual(first["counts"]["integration_gate_policies"], 0)
            self.assertEqual(first["counts"]["integration_gate_review_runs"], 0)
            self.assertEqual(first["counts"]["integration_gate_change_runs"], 0)
            self.assertEqual(first["counts"]["integration_gate_evidence"], 0)
            with closing(database.connect()) as connection:
                tables = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table' "
                        "AND name LIKE 'integration_gate_%'"
                    )
                }
        self.assertEqual(
            tables,
            {
                "integration_gate_policies",
                "integration_gate_review_runs",
                "integration_gate_change_runs",
                "integration_gate_evidence",
            },
        )

    def test_fresh_schema_has_only_the_gate_query_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Database(Path(temporary) / "control-plane.db")
            database.initialize()
            with closing(database.connect()) as connection:
                names = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'index' "
                        "AND tbl_name LIKE 'integration_gate_%' AND sql IS NOT NULL"
                    )
                }
        self.assertEqual(
            names,
            {
                "integration_gate_change_evidence",
                "integration_gate_evidence_candidate_created",
                "integration_gate_evidence_policy_gate_sequence",
                "integration_gate_evidence_policy_sequence",
                "integration_gate_policy_candidate_created",
                "integration_gate_policy_project_created",
                "integration_gate_policy_thread_created",
            },
        )


if __name__ == "__main__":
    unittest.main()
