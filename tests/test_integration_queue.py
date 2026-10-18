from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from magic_glass_box.candidates import (
    CANDIDATE_SCHEMA_VERSION,
    CandidateManifest,
    GitCandidateStore,
    MaterializedCandidate,
)
from magic_glass_box.db import (
    INTEGRATION_SCHEMA,
    SCHEMA,
    SCHEMA_VERSION,
    ConcurrentUpdateError,
    ConflictError,
    Database,
)
from magic_glass_box.domain import AgentProfileSpec, ProjectContext, WorkItemEnvelope
from magic_glass_box.gates import (
    GATE_EVIDENCE_SCHEMA_VERSION,
    ExactGateInputs,
    GateError,
    GateOutcome,
    GatePolicyManifest,
    GatePolicySpec,
    ValidationEvidence,
)
from magic_glass_box.integration import (
    INTEGRATION_FOLD_SCHEMA_VERSION,
    LANDING_PLAN_SCHEMA_VERSION,
    LANDING_RECEIPT_SCHEMA_VERSION,
    FoldManifest,
    IntegrationError,
    LandingAssurance,
    LandingOutcome,
    LandingPlan,
    LandingReceipt,
    LandingReceiptRequest,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.orchestration import CandidateReviewWorkspaces
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


class IntegrationContractTests(unittest.TestCase):
    def candidate(self) -> dict[str, object]:
        return {
            "id": "cnd_" + "1" * 32,
            "project_id": "boxp_" + "2" * 32,
            "context_version_id": "ctx_" + "3" * 32,
            "work_thread_id": "wth_" + "4" * 32,
            "attempt_id": "att_" + "5" * 32,
            "attempt_result_id": "rsl_" + "6" * 32,
            "producer_run_id": "run_" + "7" * 32,
            "base_revision": "8" * 40,
            "head_revision": "9" * 40,
            "tree_revision": "a" * 40,
            "object_format": "sha1",
            "result_content_sha256": "b" * 64,
        }

    def candidate_manifest(self) -> CandidateManifest:
        candidate = self.candidate()
        return CandidateManifest(
            schema_version=1,
            attempt_id=candidate["attempt_id"],
            attempt_result_id=candidate["attempt_result_id"],
            producer_run_id=candidate["producer_run_id"],
            project_id=candidate["project_id"],
            context_version_id=candidate["context_version_id"],
            work_thread_id=candidate["work_thread_id"],
            base_revision=candidate["base_revision"],
            result_content_sha256=candidate["result_content_sha256"],
            head_revision=candidate["head_revision"],
            tree_revision=candidate["tree_revision"],
            object_format=candidate["object_format"],
        )

    def fold(self) -> FoldManifest:
        return FoldManifest.create(
            self.candidate(),
            integration_ref="refs/heads/main",
            generation=1,
            materialized=MaterializedCandidate(
                base_revision="c" * 40,
                head_revision="d" * 40,
                tree_revision="e" * 40,
                object_format="sha1",
            ),
        )

    def test_fold_round_trip_preserves_origin_and_builds_exact_revision(self) -> None:
        fold = self.fold()

        self.assertEqual(fold.schema_version, INTEGRATION_FOLD_SCHEMA_VERSION)
        self.assertEqual(FoldManifest.from_mapping(fold.to_dict()), fold)
        self.assertEqual(len(fold.digest), 64)
        revision = fold.to_candidate_manifest(self.candidate_manifest())
        self.assertEqual(revision.base_revision, "c" * 40)
        self.assertEqual(revision.head_revision, "d" * 40)
        self.assertEqual(revision.tree_revision, "e" * 40)
        self.assertEqual(revision.result_content_sha256, "b" * 64)

    def test_fold_rejects_wrong_origin_format_generation_and_ref(self) -> None:
        valid = self.fold().to_dict()
        for field, value in (
            ("candidate_id", "run_" + "1" * 32),
            ("generation", 0),
            ("integration_ref", "refs/heads/../main"),
            ("head_revision", "f" * 64),
            ("result_content_sha256", "0" * 63),
        ):
            document = {**valid, field: value}
            with self.subTest(field=field), self.assertRaises(IntegrationError):
                FoldManifest.from_mapping(document)

    def test_fold_binds_a_fresh_gate_policy_to_its_exact_revision(self) -> None:
        candidate = self.candidate()
        fold = self.fold()
        policy = GatePolicySpec.from_mapping(
            {
                "schema_version": 1,
                "review_separation": None,
                "validations": [
                    {
                        "name": "unit",
                        "image_id": "sha256:" + "1" * 64,
                        "command": ["python", "-m", "unittest"],
                    }
                ],
            }
        )

        manifest = GatePolicyManifest.bind_revision(
            candidate,
            fold.to_dict(),
            default_branch="main",
            policy=policy,
        )

        self.assertEqual(manifest.candidate_id, candidate["id"])
        self.assertEqual(manifest.expected_base_revision, fold.base_revision)
        self.assertEqual(manifest.head_revision, fold.head_revision)
        self.assertEqual(manifest.tree_revision, fold.tree_revision)
        with self.assertRaisesRegex(GateError, "another candidate"):
            GatePolicyManifest.bind_revision(
                candidate,
                {**fold.to_dict(), "candidate_id": "cnd_" + "f" * 32},
                default_branch="main",
                policy=policy,
            )

    def plan(self, repository: Path) -> LandingPlan:
        return LandingPlan.create(
            queue_item_id="iqm_" + "1" * 32,
            queue_version=3,
            policy_id="gpl_" + "2" * 32,
            policy_revision="3" * 64,
            candidate_id="cnd_" + "4" * 32,
            fold_id="ifd_" + "5" * 32,
            repository_identity="boxp_" + "6" * 32,
            integration_ref="refs/heads/main",
            expected_base_revision="7" * 40,
            head_revision="8" * 40,
            tree_revision="9" * 40,
            object_format="sha1",
            evidence_snapshot_sha256="a" * 64,
            candidate_repository=repository,
            remote="origin",
        )

    def test_plan_is_argv_only_exact_and_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repository = Path(raw).resolve() / "repository.git"
            plan = self.plan(repository)

        self.assertEqual(plan.schema_version, LANDING_PLAN_SCHEMA_VERSION)
        self.assertEqual(plan.actuator, "git-force-with-lease-v1")
        self.assertEqual(plan.assurance, LandingAssurance.BASE_AND_HEAD_ENFORCED)
        self.assertEqual(
            plan.commands,
            (
                (
                    "git",
                    "fetch",
                    "--no-tags",
                    str(repository),
                    "8" * 40,
                ),
                (
                    "git",
                    "push",
                    "--force-with-lease=refs/heads/main:" + "7" * 40,
                    "origin",
                    "8" * 40 + ":refs/heads/main",
                ),
            ),
        )
        self.assertEqual(LandingPlan.from_mapping(plan.to_dict()), plan)
        self.assertEqual(len(plan.digest), 64)

    def test_plan_rejects_option_remote_tampering_and_symbolic_revisions(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repository = Path(raw).resolve() / "repository.git"
            with self.assertRaisesRegex(IntegrationError, "remote"):
                self.plan(repository).__class__.create(
                    queue_item_id="iqm_" + "1" * 32,
                    queue_version=3,
                    policy_id="gpl_" + "2" * 32,
                    policy_revision="3" * 64,
                    candidate_id="cnd_" + "4" * 32,
                    fold_id=None,
                    repository_identity="boxp_" + "6" * 32,
                    integration_ref="refs/heads/main",
                    expected_base_revision="7" * 40,
                    head_revision="8" * 40,
                    tree_revision="9" * 40,
                    object_format="sha1",
                    evidence_snapshot_sha256="a" * 64,
                    candidate_repository=repository,
                    remote="--upload-pack=steal",
                )
            with self.assertRaisesRegex(IntegrationError, "remote"):
                self.plan(repository).__class__.create(
                    queue_item_id="iqm_" + "1" * 32,
                    queue_version=3,
                    policy_id="gpl_" + "2" * 32,
                    policy_revision="3" * 64,
                    candidate_id="cnd_" + "4" * 32,
                    fold_id=None,
                    repository_identity="boxp_" + "6" * 32,
                    integration_ref="refs/heads/main",
                    expected_base_revision="7" * 40,
                    head_revision="8" * 40,
                    tree_revision="9" * 40,
                    object_format="sha1",
                    evidence_snapshot_sha256="a" * 64,
                    candidate_repository=repository,
                    remote="https://operator:secret@example.invalid/repository.git",
                )
            plan = self.plan(repository)
        tampered = plan.to_dict()
        tampered["commands"][1][4] = "HEAD:refs/heads/main"
        with self.assertRaisesRegex(IntegrationError, "commands"):
            LandingPlan.from_mapping(tampered)

    def test_receipt_binds_nonce_request_outcome_and_truthful_assurance(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            plan = self.plan(Path(raw).resolve() / "repository.git")
        plan_id = "lpl_" + "c" * 32
        nonce = "d" * 64
        request = LandingReceiptRequest.from_mapping(
            {
                "schema_version": LANDING_RECEIPT_SCHEMA_VERSION,
                "plan_id": plan_id,
                "request_sha256": plan.digest,
                "nonce": nonce,
                "outcome": "landed",
                "observed_revision": plan.head_revision,
            }
        )
        receipt = LandingReceipt.from_request(request, plan_id=plan_id, plan=plan)

        self.assertEqual(receipt.outcome, LandingOutcome.LANDED)
        self.assertEqual(receipt.assurance, LandingAssurance.BASE_AND_HEAD_ENFORCED)
        self.assertNotIn("nonce", receipt.to_dict())
        self.assertEqual(LandingReceipt.from_mapping(receipt.to_dict()), receipt)

        not_landed = LandingReceipt.from_request(
            LandingReceiptRequest.from_mapping(
                {
                    "schema_version": 1,
                    "plan_id": plan_id,
                    "request_sha256": plan.digest,
                    "nonce": nonce,
                    "outcome": "not_landed",
                    "observed_revision": plan.expected_base_revision,
                }
            ),
            plan_id=plan_id,
            plan=plan,
        )
        self.assertEqual(not_landed.assurance, LandingAssurance.OPERATOR_ATTESTED)
        drifted_not_landed = LandingReceipt.from_request(
            LandingReceiptRequest.from_mapping(
                {
                    "schema_version": 1,
                    "plan_id": plan_id,
                    "request_sha256": plan.digest,
                    "nonce": nonce,
                    "outcome": "not_landed",
                    "observed_revision": "a" * 40,
                }
            ),
            plan_id=plan_id,
            plan=plan,
        )
        self.assertEqual(drifted_not_landed.observed_revision, "a" * 40)

    def test_receipt_rejects_wrong_plan_digest_nonce_shape_and_revision(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            plan = self.plan(Path(raw).resolve() / "repository.git")
        plan_id = "lpl_" + "c" * 32
        valid = {
            "schema_version": 1,
            "plan_id": plan_id,
            "request_sha256": plan.digest,
            "nonce": "d" * 64,
            "outcome": "landed",
            "observed_revision": plan.head_revision,
        }
        for field, value in (
            ("plan_id", "gpl_" + "c" * 32),
            ("request_sha256", "0" * 64),
            ("nonce", "short"),
            ("observed_revision", plan.expected_base_revision),
            ("outcome", "unknown"),
        ):
            request = {**valid, field: value}
            with self.subTest(field=field), self.assertRaises(IntegrationError):
                parsed = LandingReceiptRequest.from_mapping(request)
                LandingReceipt.from_request(parsed, plan_id=plan_id, plan=plan)
        for observed_revision in (plan.head_revision, "a" * 64):
            request = {
                **valid,
                "outcome": "not_landed",
                "observed_revision": observed_revision,
            }
            with (
                self.subTest(not_landed_revision=observed_revision),
                self.assertRaisesRegex(IntegrationError, "observed revision"),
            ):
                LandingReceipt.from_request(
                    LandingReceiptRequest.from_mapping(request),
                    plan_id=plan_id,
                    plan=plan,
                )


class IntegrationMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "control-plane.db"

    def test_fresh_schema_has_integration_tables_and_query_indexes(self) -> None:
        database = Database(self.path)
        status = database.initialize()

        self.assertEqual(SCHEMA_VERSION, 21)
        self.assertEqual(status["schema_version"], SCHEMA_VERSION)
        with closing(database.connect()) as connection:
            objects = {
                (row["type"], row["name"])
                for row in connection.execute(
                    "SELECT type, name FROM sqlite_schema "
                    "WHERE name LIKE 'integration_%' "
                    "OR name LIKE 'work_item_landing_%'"
                )
            }
        for table in (
            "work_item_landing_dependencies",
            "work_item_landing_dependency_events",
            "integration_refs",
            "integration_queue_items",
            "integration_folds",
            "integration_queue_events",
            "integration_landing_plans",
            "integration_landing_receipts",
        ):
            self.assertIn(("table", table), objects)
        for index in (
            "work_item_landing_dependency_active",
            "integration_ref_identity",
            "integration_queue_one_active_thread",
            "integration_queue_one_lane_owner",
            "integration_queue_state_created",
            "integration_fold_item_generation",
            "integration_plan_one_pending",
        ):
            self.assertIn(("index", index), objects)

    def test_schema_thirteen_migrates_existing_integration_state(self) -> None:
        schema_thirteen = SCHEMA[: SCHEMA.index(INTEGRATION_SCHEMA)]
        with closing(sqlite3.connect(self.path)) as connection:
            connection.executescript(schema_thirteen)
            connection.execute("PRAGMA user_version = 13")
            connection.commit()
        os.chmod(self.path, 0o600)

        database = Database(self.path)
        first = database.initialize()
        second = database.initialize()

        self.assertEqual(first["schema_version"], SCHEMA_VERSION)
        self.assertEqual(second["schema_version"], SCHEMA_VERSION)
        with closing(database.connect()) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM sqlite_schema "
                "WHERE type = 'table' AND name = 'integration_queue_items'"
            ).fetchone()[0]
        self.assertEqual(count, 1)


class IntegrationLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repository = self.root / "source"
        self.repository.mkdir()
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        self.instance = self.database.create_krab_instance(
            "Local", "http://127.0.0.1:8765"
        )
        self.identity = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + "1" * 32, "worker", 1),
            "file:/private/worker",
            user_id=self.user["id"],
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="project",
            display_name="Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Test integration queue",
                    "repository_path": str(self.repository),
                    "default_branch": "main",
                }
            ),
            created_by=self.user["id"],
        )
        self.profile = self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.from_mapping(
                {"image": "worker:local", "command": ["worker", "run"]}
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )
        self.results = WorkspaceResultStore(self.root / "results")
        self.base = "a" * 40
        self.item_a, self.candidate_a = self._candidate("a", "b", "c")
        self.item_b, self.candidate_b = self._candidate("b", "d", "e")

    def _work_item(self, label: str) -> dict[str, object]:
        return self.database.import_work_item(
            project_key="project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": f"integration-{label}",
                    "source_updated_at": "2026-10-15T00:00:00Z",
                    "title": f"Integration {label.upper()}",
                    "brief": f"Land {label}",
                }
            ),
            imported_by=self.user["id"],
        )

    def _candidate(
        self, label: str, head_digit: str, tree_digit: str
    ) -> tuple[dict[str, object], dict[str, object]]:
        item = self._work_item(label)
        run = self.database.create_run(
            project_key="project",
            profile_name="Worker",
            work_item_id=item["id"],
            requested_by=self.user["id"],
            source_revision=self.base,
        )
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id=f"integration-{label}",
            expected_run_version=run["version"],
        )
        attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id=f"integration-{label}",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=self.base,
            container_id=head_digit * 64,
            container_name=f"integration-{label}",
            image_id="sha256:" + head_digit * 64,
        )
        workspace = self.root / f"workspace-{label}"
        workspace.mkdir()
        (workspace / f"{label}.txt").write_text("before\n", encoding="utf-8")
        self.results.stage(attempt["id"], workspace, self.base)
        (workspace / f"{label}.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(attempt["id"], workspace)
        result = self.database.record_attempt_result(attempt["id"], captured.manifest)
        finished = self.database.complete_run_attempt(
            attempt["id"],
            owner_id=f"integration-{label}",
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id="sha256:" + head_digit * 64,
        )
        manifest = CandidateManifest(
            schema_version=CANDIDATE_SCHEMA_VERSION,
            attempt_id=attempt["id"],
            attempt_result_id=result["id"],
            producer_run_id=finished["run_id"],
            project_id=self.project["id"],
            context_version_id=run["context_version_id"],
            work_thread_id=run["work_thread_id"],
            base_revision=self.base,
            result_content_sha256=captured.manifest.digest,
            head_revision=head_digit * 40,
            tree_revision=tree_digit * 40,
            object_format="sha1",
        )
        return item, self.database.create_candidate(
            manifest, created_by=self.user["id"]
        )

    @staticmethod
    def policy_spec() -> GatePolicySpec:
        return GatePolicySpec.from_mapping(
            {
                "schema_version": 1,
                "review_separation": None,
                "validations": [
                    {
                        "name": "unit",
                        "image_id": "sha256:" + "9" * 64,
                        "command": ["python", "-m", "unittest"],
                    }
                ],
            }
        )

    def _policy(
        self,
        candidate: dict[str, object],
        *,
        revision: dict[str, object] | None = None,
        passed: bool = True,
    ) -> dict[str, object]:
        manifest = (
            GatePolicyManifest.bind(
                candidate, default_branch="main", policy=self.policy_spec()
            )
            if revision is None
            else GatePolicyManifest.bind_revision(
                candidate,
                revision,
                default_branch="main",
                policy=self.policy_spec(),
            )
        )
        policy = self.database.create_gate_policy(manifest, created_by=self.user["id"])
        if passed:
            validation = self.policy_spec().validation("unit")
            evidence = ValidationEvidence(
                schema_version=GATE_EVIDENCE_SCHEMA_VERSION,
                inputs=ExactGateInputs.from_policy(policy["id"], manifest),
                outcome=GateOutcome.PASSED,
                validation_name="unit",
                image_id=validation.image_id,
                command_sha256=validation.command_sha256,
                sandbox_id="run_" + policy["id"][4:],
                exit_code=0,
                duration_ms=1,
                output_sha256="f" * 64,
                output_bytes=0,
            )
            self.database.record_gate_evidence(evidence, recorded_by=self.user["id"])
        return policy

    def _land(
        self, queue: dict[str, object], *, owner: str = "supervisor"
    ) -> dict[str, object]:
        claimed = self.database.claim_integration_item(
            queue["id"],
            owner_id=owner,
            expected_version=queue["version"],
            lease_seconds=30,
        )
        issued = self.database.issue_landing_plan(
            queue["id"],
            owner_id=owner,
            expected_version=claimed["version"],
            candidate_repository=self.root / "candidates" / "repository.git",
            remote="origin",
            issued_by=self.user["id"],
        )
        plan = LandingPlan.from_mapping(issued["plan"])
        request = LandingReceiptRequest.from_mapping(
            {
                "schema_version": 1,
                "plan_id": issued["id"],
                "request_sha256": plan.digest,
                "nonce": issued["nonce"],
                "outcome": "landed",
                "observed_revision": plan.head_revision,
            }
        )
        return self.database.record_landing_receipt(
            request, recorded_by=self.user["id"]
        )

    def _assert_last_queue_event(
        self,
        queue: dict[str, object],
        *,
        sequence: int,
        kind: str,
        from_state: str | None,
        to_state: str,
        actor: str,
        metadata: dict[str, object],
    ) -> None:
        event = queue["events"][-1]
        self.assertEqual(
            {
                "sequence": event["sequence"],
                "kind": event["kind"],
                "from_state": event["from_state"],
                "to_state": event["to_state"],
                "actor": event["actor"],
                "metadata": event["metadata"],
            },
            {
                "sequence": sequence,
                "kind": kind,
                "from_state": from_state,
                "to_state": to_state,
                "actor": actor,
                "metadata": metadata,
            },
        )

    def test_dependencies_reject_self_cross_project_and_cycles(self) -> None:
        dependency = self.database.add_landing_dependency(
            self.item_b["id"],
            self.item_a["id"],
            created_by=self.user["id"],
        )
        self.assertEqual(
            self.database.add_landing_dependency(
                self.item_b["id"],
                self.item_a["id"],
                created_by=self.user["id"],
            ),
            dependency,
        )
        with self.assertRaisesRegex(ConflictError, "itself"):
            self.database.add_landing_dependency(
                self.item_a["id"],
                self.item_a["id"],
                created_by=self.user["id"],
            )
        with self.assertRaisesRegex(ConflictError, "cycle"):
            self.database.add_landing_dependency(
                self.item_a["id"],
                self.item_b["id"],
                created_by=self.user["id"],
            )

        other = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="other",
            display_name="Other",
            context=ProjectContext.from_mapping(
                {"purpose": "Other project", "default_branch": "main"}
            ),
            created_by=self.user["id"],
        )
        other_item = self.database.import_work_item(
            project_key="other",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "other",
                    "source_updated_at": "2026-10-15T00:00:00Z",
                    "title": "Other",
                    "brief": "Other",
                }
            ),
            imported_by=self.user["id"],
        )
        self.assertNotEqual(other["id"], self.project["id"])
        with self.assertRaisesRegex(ConflictError, "same project"):
            self.database.add_landing_dependency(
                self.item_a["id"],
                other_item["id"],
                created_by=self.user["id"],
            )

    def test_dependency_removal_reactivation_and_issued_authority_lock(self) -> None:
        dependency = self.database.add_landing_dependency(
            self.item_b["id"], self.item_a["id"], created_by=self.user["id"]
        )
        with self.assertRaisesRegex(ConcurrentUpdateError, "version changed"):
            self.database.remove_landing_dependency(
                dependency["id"],
                expected_version=dependency["version"] + 1,
                removed_by=self.user["id"],
            )
        removed = self.database.remove_landing_dependency(
            dependency["id"],
            expected_version=dependency["version"],
            removed_by=self.user["id"],
        )
        self.assertEqual(removed["state"], "removed")
        self.assertEqual(removed["version"], dependency["version"] + 1)
        self.assertEqual(
            self.database.remove_landing_dependency(
                dependency["id"],
                expected_version=removed["version"],
                removed_by=self.user["id"],
            ),
            removed,
        )
        self.assertEqual(
            self.database.list_landing_dependencies(
                project_id=self.project["id"], active_only=True
            ),
            [],
        )
        self.assertEqual(
            len(self.database.list_landing_dependencies(project_id=self.project["id"])),
            1,
        )

        reactivated = self.database.add_landing_dependency(
            self.item_b["id"], self.item_a["id"], created_by=self.user["id"]
        )
        self.assertEqual(reactivated["version"], removed["version"] + 1)
        self.assertEqual(
            [event["kind"] for event in reactivated["events"]],
            ["dependency.added", "dependency.removed", "dependency.added"],
        )
        removed_again = self.database.remove_landing_dependency(
            dependency["id"],
            expected_version=reactivated["version"],
            removed_by=self.user["id"],
        )

        queue = self.database.enqueue_integration(
            self._policy(self.candidate_b)["id"], created_by=self.user["id"]
        )
        claimed = self.database.claim_integration_item(
            queue["id"],
            owner_id="supervisor-b",
            expected_version=queue["version"],
            lease_seconds=30,
        )
        self.database.issue_landing_plan(
            queue["id"],
            owner_id="supervisor-b",
            expected_version=claimed["version"],
            candidate_repository=self.root / "repository.git",
            remote="origin",
            issued_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "authority is issued"):
            self.database.add_landing_dependency(
                self.item_b["id"],
                self.item_a["id"],
                created_by=self.user["id"],
            )
        self.assertEqual(
            self.database.list_landing_dependencies()[0]["version"],
            removed_again["version"],
        )

    def test_active_dependency_cannot_be_removed_after_authority_is_issued(
        self,
    ) -> None:
        queue_a = self.database.enqueue_integration(
            self._policy(self.candidate_a)["id"], created_by=self.user["id"]
        )
        self._land(queue_a, owner="supervisor-a")

        original_base = self.base
        self.base = self.candidate_a["head_revision"]
        try:
            item_c, candidate_c = self._candidate("c", "6", "7")
        finally:
            self.base = original_base
        dependency = self.database.add_landing_dependency(
            item_c["id"], self.item_a["id"], created_by=self.user["id"]
        )
        queue_c = self.database.enqueue_integration(
            self._policy(candidate_c)["id"], created_by=self.user["id"]
        )
        claimed_c = self.database.claim_integration_item(
            queue_c["id"],
            owner_id="supervisor-c",
            expected_version=queue_c["version"],
            lease_seconds=30,
        )
        self.database.issue_landing_plan(
            queue_c["id"],
            owner_id="supervisor-c",
            expected_version=claimed_c["version"],
            candidate_repository=self.root / "repository.git",
            remote="origin",
            issued_by=self.user["id"],
        )

        with self.assertRaisesRegex(ConflictError, "authority is issued"):
            self.database.remove_landing_dependency(
                dependency["id"],
                expected_version=dependency["version"],
                removed_by=self.user["id"],
            )
        self.assertEqual(
            self.database.list_landing_dependencies(
                project_id=self.project["id"], active_only=True
            ),
            [dependency],
        )

    def test_dependency_refold_and_fresh_gate_golden_state_machine(self) -> None:
        policy_a = self._policy(self.candidate_a)
        policy_b = self._policy(self.candidate_b)
        self.database.add_landing_dependency(
            self.item_b["id"], self.item_a["id"], created_by=self.user["id"]
        )
        queue_a = self.database.enqueue_integration(
            policy_a["id"], created_by=self.user["id"]
        )
        queue_b = self.database.enqueue_integration(
            policy_b["id"], created_by=self.user["id"]
        )

        with self.assertRaisesRegex(ConflictError, "dependencies"):
            self.database.claim_integration_item(
                queue_b["id"],
                owner_id="supervisor-b",
                expected_version=queue_b["version"],
                lease_seconds=30,
            )
        landed_a = self._land(queue_a)
        self.assertEqual(landed_a["queue_item"]["state"], "landed")
        stale_b = self.database.get_integration_queue_item(queue_b["id"])
        self.assertEqual(stale_b["state"], "refold_required")
        self.assertEqual(stale_b["version"], queue_b["version"] + 1)
        self.assertEqual(stale_b["event_sequence"], queue_b["event_sequence"] + 1)
        self._assert_last_queue_event(
            stale_b,
            sequence=stale_b["event_sequence"],
            kind="queue.refold_required",
            from_state="queued",
            to_state="refold_required",
            actor="system",
            metadata={
                "landed_queue_item_id": queue_a["id"],
                "new_base_revision": self.candidate_a["head_revision"],
            },
        )
        self.assertEqual(
            landed_a["integration_ref"]["current_revision"],
            self.candidate_a["head_revision"],
        )
        with self.assertRaisesRegex(ConflictError, "refold"):
            self.database.claim_integration_item(
                stale_b["id"],
                owner_id="supervisor-b",
                expected_version=stale_b["version"],
                lease_seconds=30,
            )

        fold_manifest = FoldManifest.create(
            self.candidate_b,
            integration_ref="refs/heads/main",
            generation=1,
            materialized=MaterializedCandidate(
                base_revision=self.candidate_a["head_revision"],
                head_revision="6" * 40,
                tree_revision="7" * 40,
                object_format="sha1",
            ),
        )
        with self.assertRaisesRegex(ConflictError, "does not match the current ref"):
            self.database.record_integration_fold(
                stale_b["id"],
                replace(fold_manifest, context_version_id="ctx_" + "0" * 32),
                expected_version=stale_b["version"],
                created_by=self.user["id"],
            )
        fold = self.database.record_integration_fold(
            stale_b["id"],
            fold_manifest,
            expected_version=stale_b["version"],
            created_by=self.user["id"],
        )
        self.assertEqual(
            self.database.record_integration_fold(
                stale_b["id"],
                fold_manifest,
                expected_version=stale_b["version"],
                created_by=self.user["id"],
            ),
            fold,
        )
        with self.assertRaisesRegex(ConflictError, "fresh gate policy"):
            self.database.activate_integration_fold(
                stale_b["id"],
                fold_id=fold["id"],
                policy_id=policy_b["id"],
                expected_version=stale_b["version"],
                actor=self.user["id"],
            )
        fresh_policy = self._policy(
            self.candidate_b, revision=fold["manifest"], passed=True
        )
        _, candidate_c = self._candidate("c", "8", "9")
        stale_policy_c = self._policy(candidate_c)
        queue_c = self.database.enqueue_integration(
            stale_policy_c["id"], created_by=self.user["id"]
        )
        self.assertEqual(queue_c["state"], "refold_required")
        fold_manifest_c = FoldManifest.create(
            candidate_c,
            integration_ref="refs/heads/main",
            generation=1,
            materialized=MaterializedCandidate(
                base_revision=self.candidate_a["head_revision"],
                head_revision="8" * 40,
                tree_revision="9" * 40,
                object_format="sha1",
            ),
        )
        fold_c = self.database.record_integration_fold(
            queue_c["id"],
            fold_manifest_c,
            expected_version=queue_c["version"],
            created_by=self.user["id"],
        )
        fresh_policy_c = self._policy(
            candidate_c, revision=fold_c["manifest"], passed=True
        )
        with self.assertRaisesRegex(ConflictError, "belongs elsewhere"):
            self.database.activate_integration_fold(
                stale_b["id"],
                fold_id=fold_c["id"],
                policy_id=fresh_policy_c["id"],
                expected_version=stale_b["version"],
                actor=self.user["id"],
            )
        inactive = self.database.create_user("inactive", "Inactive")
        with closing(self.database.connect()) as connection, connection:
            connection.execute(
                "UPDATE users SET active = 0 WHERE id = ?", (inactive["id"],)
            )
        with self.assertRaisesRegex(ConflictError, "inactive"):
            self.database.activate_integration_fold(
                stale_b["id"],
                fold_id=fold["id"],
                policy_id=fresh_policy["id"],
                expected_version=stale_b["version"],
                actor=inactive["id"],
            )
        ready_b = self.database.activate_integration_fold(
            stale_b["id"],
            fold_id=fold["id"],
            policy_id=fresh_policy["id"],
            expected_version=stale_b["version"],
            actor=self.user["id"],
        )
        self.assertEqual(ready_b["state"], "queued")
        self.assertNotEqual(ready_b["current_policy_id"], policy_b["id"])
        self.assertEqual(ready_b["version"], stale_b["version"] + 1)
        self.assertEqual(ready_b["event_sequence"], stale_b["event_sequence"] + 1)
        self._assert_last_queue_event(
            ready_b,
            sequence=ready_b["event_sequence"],
            kind="queue.refolded",
            from_state="refold_required",
            to_state="queued",
            actor=self.user["id"],
            metadata={"fold_id": fold["id"], "policy_id": fresh_policy["id"]},
        )

        landed_b = self._land(ready_b, owner="supervisor-b")
        self.assertEqual(landed_b["queue_item"]["state"], "landed")
        self.assertEqual(landed_b["integration_ref"]["current_revision"], "6" * 40)

    def test_plan_requires_fresh_passing_evidence_and_receipt_is_nonce_bound(
        self,
    ) -> None:
        policy = self._policy(self.candidate_a, passed=False)
        queue = self.database.enqueue_integration(
            policy["id"], created_by=self.user["id"]
        )
        with self.assertRaisesRegex(ConflictError, "gates"):
            self.database.claim_integration_item(
                queue["id"],
                owner_id="supervisor",
                expected_version=queue["version"],
                lease_seconds=30,
            )

        manifest = GatePolicyManifest.from_mapping(policy["manifest"])
        validation = self.policy_spec().validation("unit")
        self.database.record_gate_evidence(
            ValidationEvidence(
                GATE_EVIDENCE_SCHEMA_VERSION,
                ExactGateInputs.from_policy(policy["id"], manifest),
                GateOutcome.PASSED,
                "unit",
                validation.image_id,
                validation.command_sha256,
                "run_" + "8" * 32,
                0,
                1,
                "f" * 64,
                0,
            ),
            recorded_by=self.user["id"],
        )
        claimed = self.database.claim_integration_item(
            queue["id"],
            owner_id="supervisor",
            expected_version=queue["version"],
            lease_seconds=30,
        )
        dependency = self.database.add_landing_dependency(
            self.item_a["id"], self.item_b["id"], created_by=self.user["id"]
        )
        with self.assertRaisesRegex(ConflictError, "dependencies"):
            self.database.issue_landing_plan(
                queue["id"],
                owner_id="supervisor",
                expected_version=claimed["version"],
                candidate_repository=self.root / "repository.git",
                remote="origin",
                issued_by=self.user["id"],
            )
        self.database.remove_landing_dependency(
            dependency["id"],
            expected_version=dependency["version"],
            removed_by=self.user["id"],
        )
        readiness = self.database.gate_readiness(policy["id"])
        issued = self.database.issue_landing_plan(
            queue["id"],
            owner_id="supervisor",
            expected_version=claimed["version"],
            candidate_repository=self.root / "repository.git",
            remote="origin",
            issued_by=self.user["id"],
        )
        self.assertEqual(issued["evidence_snapshot"], readiness)
        plan = LandingPlan.from_mapping(issued["plan"])
        awaiting = self.database.get_integration_queue_item(queue["id"])
        self.assertEqual(awaiting["version"], claimed["version"] + 1)
        self.assertEqual(awaiting["event_sequence"], claimed["event_sequence"] + 1)
        self._assert_last_queue_event(
            awaiting,
            sequence=awaiting["event_sequence"],
            kind="queue.plan_issued",
            from_state="claimed",
            to_state="awaiting_receipt",
            actor=self.user["id"],
            metadata={
                "plan_id": issued["id"],
                "request_sha256": plan.digest,
                "evidence_snapshot_sha256": issued["evidence_snapshot_sha256"],
            },
        )
        wrong = LandingReceiptRequest(
            1,
            issued["id"],
            plan.digest,
            "0" * 64,
            LandingOutcome.LANDED,
            plan.head_revision,
        )
        with self.assertRaisesRegex(ConflictError, "nonce"):
            self.database.record_landing_receipt(wrong, recorded_by=self.user["id"])
        request = replace(wrong, nonce=issued["nonce"])
        accepted = self.database.record_landing_receipt(
            request, recorded_by=self.user["id"]
        )
        self.assertEqual(accepted["receipt"]["outcome"], "landed")
        landed = accepted["queue_item"]
        self.assertEqual(landed["version"], awaiting["version"] + 1)
        self.assertEqual(landed["event_sequence"], awaiting["event_sequence"] + 1)
        self._assert_last_queue_event(
            landed,
            sequence=landed["event_sequence"],
            kind="queue.landed",
            from_state="awaiting_receipt",
            to_state="landed",
            actor=self.user["id"],
            metadata={"plan_id": issued["id"], "receipt_id": accepted["receipt"]["id"]},
        )
        with self.assertRaisesRegex(ConflictError, "resolved"):
            self.database.record_landing_receipt(request, recorded_by=self.user["id"])
        self.assertNotIn(
            issued["nonce"], str(self.database.get_landing_plan(issued["id"]))
        )

    def test_lane_is_serialized_and_an_expired_preplan_claim_is_reclaimed(
        self,
    ) -> None:
        queue_a = self.database.enqueue_integration(
            self._policy(self.candidate_a)["id"], created_by=self.user["id"]
        )
        queue_b = self.database.enqueue_integration(
            self._policy(self.candidate_b)["id"], created_by=self.user["id"]
        )
        claimed_a = self.database.claim_integration_item(
            queue_a["id"],
            owner_id="supervisor-a",
            expected_version=queue_a["version"],
            lease_seconds=30,
        )
        with self.assertRaisesRegex(ConflictError, "already claimed"):
            self.database.claim_integration_item(
                queue_a["id"],
                owner_id="supervisor-b",
                expected_version=claimed_a["version"],
                lease_seconds=30,
            )
        with self.assertRaisesRegex(ConflictError, "claimed"):
            self.database.claim_integration_item(
                queue_b["id"],
                owner_id="supervisor-b",
                expected_version=queue_b["version"],
                lease_seconds=30,
            )
        with closing(self.database.connect()) as connection, connection:
            connection.execute(
                "UPDATE integration_queue_items SET lease_expires_at = ? WHERE id = ?",
                ("2000-01-01T00:00:00.000000+00:00", queue_a["id"]),
            )

        claimed_b = self.database.claim_integration_item(
            queue_b["id"],
            owner_id="supervisor-b",
            expected_version=queue_b["version"],
            lease_seconds=30,
        )

        self.assertEqual(claimed_b["state"], "claimed")
        expired_a = self.database.get_integration_queue_item(queue_a["id"])
        self.assertEqual(expired_a["state"], "queued")
        self.assertEqual(expired_a["version"], claimed_a["version"] + 1)
        self.assertEqual(expired_a["event_sequence"], claimed_a["event_sequence"] + 1)
        self._assert_last_queue_event(
            expired_a,
            sequence=expired_a["event_sequence"],
            kind="queue.claim_expired",
            from_state="claimed",
            to_state="queued",
            actor="system",
            metadata={"expired_owner_id": "supervisor-a"},
        )

        with closing(self.database.connect()) as connection, connection:
            connection.execute(
                "UPDATE integration_queue_items SET lease_expires_at = ? WHERE id = ?",
                ("2000-01-01T00:00:00.000000+00:00", queue_b["id"]),
            )
        reclaimed_b = self.database.claim_integration_item(
            queue_b["id"],
            owner_id="supervisor-c",
            expected_version=claimed_b["version"],
            lease_seconds=30,
        )
        self.assertEqual(reclaimed_b["state"], "claimed")
        self.assertEqual(reclaimed_b["owner_id"], "supervisor-c")
        self.assertEqual(reclaimed_b["version"], claimed_b["version"] + 2)
        self.assertEqual(reclaimed_b["event_sequence"], claimed_b["event_sequence"] + 2)
        self.assertEqual(
            [event["kind"] for event in reclaimed_b["events"][-2:]],
            ["queue.claim_expired", "queue.claimed"],
        )
        expired_event, claimed_event = reclaimed_b["events"][-2:]
        self.assertEqual(
            {
                "sequence": expired_event["sequence"],
                "from_state": expired_event["from_state"],
                "to_state": expired_event["to_state"],
                "actor": expired_event["actor"],
                "metadata": expired_event["metadata"],
            },
            {
                "sequence": claimed_b["event_sequence"] + 1,
                "from_state": "claimed",
                "to_state": "queued",
                "actor": "system",
                "metadata": {"expired_owner_id": "supervisor-b"},
            },
        )
        self.assertEqual(
            {
                "sequence": claimed_event["sequence"],
                "from_state": claimed_event["from_state"],
                "to_state": claimed_event["to_state"],
                "actor": claimed_event["actor"],
                "metadata": claimed_event["metadata"],
            },
            {
                "sequence": claimed_b["event_sequence"] + 2,
                "from_state": "queued",
                "to_state": "claimed",
                "actor": "supervisor-c",
                "metadata": {"lease_expires_at": reclaimed_b["lease_expires_at"]},
            },
        )

    def test_claim_release_and_plan_enforce_owner_version_and_live_lease(
        self,
    ) -> None:
        policy = self._policy(self.candidate_a)
        queue = self.database.enqueue_integration(
            policy["id"], created_by=self.user["id"]
        )
        self.assertEqual(
            self.database.enqueue_integration(policy["id"], created_by=self.user["id"]),
            queue,
        )
        self.assertEqual(
            [item["id"] for item in self.database.list_integration_queue()],
            [queue["id"]],
        )
        claimed = self.database.claim_integration_item(
            queue["id"],
            owner_id="supervisor-a",
            expected_version=queue["version"],
            lease_seconds=30,
        )
        self.assertEqual(claimed["version"], queue["version"] + 1)
        self.assertEqual(claimed["event_sequence"], queue["event_sequence"] + 1)
        self._assert_last_queue_event(
            claimed,
            sequence=claimed["event_sequence"],
            kind="queue.claimed",
            from_state="queued",
            to_state="claimed",
            actor="supervisor-a",
            metadata={"lease_expires_at": claimed["lease_expires_at"]},
        )
        with self.assertRaisesRegex(ConflictError, "not owned"):
            self.database.release_integration_claim(
                queue["id"],
                owner_id="supervisor-b",
                expected_version=claimed["version"],
            )
        with self.assertRaisesRegex(ConcurrentUpdateError, "version changed"):
            self.database.release_integration_claim(
                queue["id"],
                owner_id="supervisor-a",
                expected_version=queue["version"],
            )
        released = self.database.release_integration_claim(
            queue["id"],
            owner_id="supervisor-a",
            expected_version=claimed["version"],
        )
        self.assertEqual(released["state"], "queued")
        self.assertEqual(released["version"], claimed["version"] + 1)
        self.assertEqual(released["event_sequence"], claimed["event_sequence"] + 1)
        self._assert_last_queue_event(
            released,
            sequence=released["event_sequence"],
            kind="queue.claim_released",
            from_state="claimed",
            to_state="queued",
            actor="supervisor-a",
            metadata={},
        )

        reclaimed = self.database.claim_integration_item(
            queue["id"],
            owner_id="supervisor-a",
            expected_version=released["version"],
            lease_seconds=30,
        )
        with self.assertRaisesRegex(ConflictError, "not owned"):
            self.database.issue_landing_plan(
                queue["id"],
                owner_id="supervisor-b",
                expected_version=reclaimed["version"],
                candidate_repository=self.root / "repository.git",
                remote="origin",
                issued_by=self.user["id"],
            )
        with self.assertRaisesRegex(ConcurrentUpdateError, "version changed"):
            self.database.issue_landing_plan(
                queue["id"],
                owner_id="supervisor-a",
                expected_version=released["version"],
                candidate_repository=self.root / "repository.git",
                remote="origin",
                issued_by=self.user["id"],
            )
        boundary = "2026-10-15T12:00:00.000000+00:00"
        with closing(self.database.connect()) as connection, connection:
            connection.execute(
                "UPDATE integration_queue_items SET lease_expires_at = ? WHERE id = ?",
                (boundary, queue["id"]),
            )
        with (
            patch.object(self.database, "now", return_value=boundary),
            self.assertRaisesRegex(ConflictError, "lease expired"),
        ):
            self.database.issue_landing_plan(
                queue["id"],
                owner_id="supervisor-a",
                expected_version=reclaimed["version"],
                candidate_repository=self.root / "repository.git",
                remote="origin",
                issued_by=self.user["id"],
            )

    def test_enqueue_is_idempotent_only_for_the_exact_policy(self) -> None:
        policy = self._policy(self.candidate_a)
        queue = self.database.enqueue_integration(
            policy["id"], created_by=self.user["id"]
        )

        self.assertEqual(
            self.database.enqueue_integration(policy["id"], created_by=self.user["id"]),
            queue,
        )

        stricter_spec = GatePolicySpec.from_mapping(
            {
                "schema_version": 1,
                "review_separation": None,
                "validations": [
                    {
                        "name": "strict",
                        "image_id": "sha256:" + "9" * 64,
                        "command": ["python", "-m", "unittest", "discover"],
                    }
                ],
            }
        )
        stricter = self.database.create_gate_policy(
            GatePolicyManifest.bind(
                self.candidate_a,
                default_branch="main",
                policy=stricter_spec,
            ),
            created_by=self.user["id"],
        )

        with self.assertRaisesRegex(
            ConflictError, "already queued with another policy"
        ):
            self.database.enqueue_integration(
                stricter["id"], created_by=self.user["id"]
            )

    def test_cancellation_is_optimistic_audited_and_stops_at_issued_authority(
        self,
    ) -> None:
        queue_a = self.database.enqueue_integration(
            self._policy(self.candidate_a)["id"], created_by=self.user["id"]
        )
        with self.assertRaisesRegex(ConcurrentUpdateError, "version changed"):
            self.database.cancel_integration_item(
                queue_a["id"],
                expected_version=queue_a["version"] + 1,
                cancelled_by=self.user["id"],
            )
        cancelled_a = self.database.cancel_integration_item(
            queue_a["id"],
            expected_version=queue_a["version"],
            cancelled_by=self.user["id"],
        )
        self.assertEqual(cancelled_a["state"], "cancelled")
        self.assertEqual(cancelled_a["version"], queue_a["version"] + 1)
        self.assertEqual(cancelled_a["event_sequence"], queue_a["event_sequence"] + 1)
        self.assertIsNotNone(cancelled_a["cancelled_at"])
        self._assert_last_queue_event(
            cancelled_a,
            sequence=cancelled_a["event_sequence"],
            kind="queue.cancelled",
            from_state="queued",
            to_state="cancelled",
            actor=self.user["id"],
            metadata={},
        )
        with self.assertRaisesRegex(ConflictError, "cancelled"):
            self.database.enqueue_integration(
                cancelled_a["current_policy_id"], created_by=self.user["id"]
            )
        with self.assertRaisesRegex(ConflictError, "cannot be cancelled"):
            self.database.cancel_integration_item(
                queue_a["id"],
                expected_version=cancelled_a["version"],
                cancelled_by=self.user["id"],
            )

        queue_b = self.database.enqueue_integration(
            self._policy(self.candidate_b)["id"], created_by=self.user["id"]
        )
        claimed_b = self.database.claim_integration_item(
            queue_b["id"],
            owner_id="supervisor-b",
            expected_version=queue_b["version"],
            lease_seconds=30,
        )
        cancelled_b = self.database.cancel_integration_item(
            queue_b["id"],
            expected_version=claimed_b["version"],
            cancelled_by=self.user["id"],
        )
        self.assertEqual(cancelled_b["state"], "cancelled")
        self.assertEqual(cancelled_b["version"], claimed_b["version"] + 1)
        self.assertEqual(cancelled_b["event_sequence"], claimed_b["event_sequence"] + 1)
        self.assertIsNone(cancelled_b["owner_id"])
        self.assertIsNone(cancelled_b["lease_expires_at"])
        self._assert_last_queue_event(
            cancelled_b,
            sequence=cancelled_b["event_sequence"],
            kind="queue.cancelled",
            from_state="claimed",
            to_state="cancelled",
            actor=self.user["id"],
            metadata={"cancelled_owner_id": "supervisor-b"},
        )

        _, candidate_c = self._candidate("c", "f", "1")
        queue_c = self.database.enqueue_integration(
            self._policy(candidate_c)["id"], created_by=self.user["id"]
        )
        claimed_c = self.database.claim_integration_item(
            queue_c["id"],
            owner_id="supervisor-c",
            expected_version=queue_c["version"],
            lease_seconds=30,
        )
        issued_c = self.database.issue_landing_plan(
            queue_c["id"],
            owner_id="supervisor-c",
            expected_version=claimed_c["version"],
            candidate_repository=self.root / "repository.git",
            remote="origin",
            issued_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "authority has been issued"):
            self.database.cancel_integration_item(
                queue_c["id"],
                expected_version=self.database.get_integration_queue_item(
                    queue_c["id"]
                )["version"],
                cancelled_by=self.user["id"],
            )
        plan_c = LandingPlan.from_mapping(issued_c["plan"])
        self.database.record_landing_receipt(
            LandingReceiptRequest(
                1,
                issued_c["id"],
                plan_c.digest,
                issued_c["nonce"],
                LandingOutcome.LANDED,
                plan_c.head_revision,
            ),
            recorded_by=self.user["id"],
        )

        _, candidate_d = self._candidate("d", "2", "3")
        queue_d = self.database.enqueue_integration(
            self._policy(candidate_d)["id"],
            created_by=self.user["id"],
        )
        self.assertEqual(queue_d["state"], "refold_required")
        cancelled_d = self.database.cancel_integration_item(
            queue_d["id"],
            expected_version=queue_d["version"],
            cancelled_by=self.user["id"],
        )
        self.assertEqual(cancelled_d["state"], "cancelled")

    def test_not_landed_releases_lane_without_advancing_ref(self) -> None:
        queue = self.database.enqueue_integration(
            self._policy(self.candidate_a)["id"], created_by=self.user["id"]
        )
        claimed = self.database.claim_integration_item(
            queue["id"],
            owner_id="supervisor",
            expected_version=queue["version"],
            lease_seconds=30,
        )
        issued = self.database.issue_landing_plan(
            queue["id"],
            owner_id="supervisor",
            expected_version=claimed["version"],
            candidate_repository=self.root / "repository.git",
            remote="origin",
            issued_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "authority is issued"):
            self.database.add_landing_dependency(
                self.item_a["id"],
                self.item_b["id"],
                created_by=self.user["id"],
            )
        plan = LandingPlan.from_mapping(issued["plan"])
        awaiting = self.database.get_integration_queue_item(queue["id"])
        result = self.database.record_landing_receipt(
            LandingReceiptRequest(
                1,
                issued["id"],
                plan.digest,
                issued["nonce"],
                LandingOutcome.NOT_LANDED,
                plan.expected_base_revision,
            ),
            recorded_by=self.user["id"],
        )

        self.assertEqual(result["queue_item"]["state"], "queued")
        self.assertEqual(result["queue_item"]["version"], awaiting["version"] + 1)
        self.assertEqual(
            result["queue_item"]["event_sequence"], awaiting["event_sequence"] + 1
        )
        self._assert_last_queue_event(
            result["queue_item"],
            sequence=result["queue_item"]["event_sequence"],
            kind="queue.not_landed",
            from_state="awaiting_receipt",
            to_state="queued",
            actor=self.user["id"],
            metadata={
                "plan_id": issued["id"],
                "receipt_id": result["receipt"]["id"],
            },
        )
        self.assertEqual(result["integration_ref"]["current_revision"], self.base)
        self.assertEqual(result["receipt"]["assurance"], "operator_attested")
        dependency = self.database.add_landing_dependency(
            self.item_a["id"],
            self.item_b["id"],
            created_by=self.user["id"],
        )
        self.assertEqual(dependency["state"], "active")

    def test_not_landed_at_a_drifted_ref_advances_base_and_requires_refold(
        self,
    ) -> None:
        queue = self.database.enqueue_integration(
            self._policy(self.candidate_a)["id"], created_by=self.user["id"]
        )
        sibling_queue = self.database.enqueue_integration(
            self._policy(self.candidate_b)["id"], created_by=self.user["id"]
        )
        claimed = self.database.claim_integration_item(
            queue["id"],
            owner_id="supervisor",
            expected_version=queue["version"],
            lease_seconds=30,
        )
        issued = self.database.issue_landing_plan(
            queue["id"],
            owner_id="supervisor",
            expected_version=claimed["version"],
            candidate_repository=self.root / "repository.git",
            remote="origin",
            issued_by=self.user["id"],
        )
        plan = LandingPlan.from_mapping(issued["plan"])
        drifted_revision = "8" * 40
        awaiting = self.database.get_integration_queue_item(queue["id"])

        result = self.database.record_landing_receipt(
            LandingReceiptRequest(
                1,
                issued["id"],
                plan.digest,
                issued["nonce"],
                LandingOutcome.NOT_LANDED,
                drifted_revision,
            ),
            recorded_by=self.user["id"],
        )

        refold = result["queue_item"]
        self.assertEqual(refold["state"], "refold_required")
        self.assertIsNone(refold["current_fold_id"])
        self.assertEqual(refold["version"], awaiting["version"] + 1)
        self.assertEqual(
            result["integration_ref"]["current_revision"], drifted_revision
        )
        self.assertEqual(
            result["integration_ref"]["version"],
            awaiting["integration_ref_version"] + 1,
        )
        self._assert_last_queue_event(
            refold,
            sequence=refold["event_sequence"],
            kind="queue.refold_required",
            from_state="awaiting_receipt",
            to_state="refold_required",
            actor=self.user["id"],
            metadata={
                "new_base_revision": drifted_revision,
                "plan_id": issued["id"],
                "receipt_id": result["receipt"]["id"],
            },
        )
        self.assertEqual(result["receipt"]["outcome"], "not_landed")
        self.assertEqual(result["receipt"]["observed_revision"], drifted_revision)
        sibling = self.database.get_integration_queue_item(sibling_queue["id"])
        self.assertEqual(sibling["state"], "refold_required")
        self.assertIsNone(sibling["current_fold_id"])
        self.assertEqual(sibling["version"], sibling_queue["version"] + 1)
        self._assert_last_queue_event(
            sibling,
            sequence=sibling["event_sequence"],
            kind="queue.refold_required",
            from_state="queued",
            to_state="refold_required",
            actor="system",
            metadata={
                "drifted_queue_item_id": queue["id"],
                "new_base_revision": drifted_revision,
            },
        )

    def test_receipt_rejects_each_stale_queue_authority_member(self) -> None:
        policy = self._policy(self.candidate_a)
        queue = self.database.enqueue_integration(
            policy["id"], created_by=self.user["id"]
        )
        claimed = self.database.claim_integration_item(
            queue["id"],
            owner_id="supervisor",
            expected_version=queue["version"],
            lease_seconds=30,
        )
        issued = self.database.issue_landing_plan(
            queue["id"],
            owner_id="supervisor",
            expected_version=claimed["version"],
            candidate_repository=self.root / "repository.git",
            remote="origin",
            issued_by=self.user["id"],
        )
        plan = LandingPlan.from_mapping(issued["plan"])
        request = LandingReceiptRequest(
            1,
            issued["id"],
            plan.digest,
            issued["nonce"],
            LandingOutcome.LANDED,
            plan.head_revision,
        )
        awaiting = self.database.get_integration_queue_item(queue["id"])
        other_policy = self._policy(self.candidate_b)

        for column, stale_value in (
            ("state", "queued"),
            ("version", awaiting["version"] + 1),
            ("current_policy_id", other_policy["id"]),
        ):
            with self.subTest(column=column):
                with closing(self.database.connect()) as connection, connection:
                    connection.execute(
                        f"UPDATE integration_queue_items SET {column} = ? WHERE id = ?",  # noqa: S608 - allowlisted test column
                        (stale_value, queue["id"]),
                    )
                with self.assertRaisesRegex(ConflictError, "authority is stale"):
                    self.database.record_landing_receipt(
                        request, recorded_by=self.user["id"]
                    )
                with closing(self.database.connect()) as connection, connection:
                    connection.execute(
                        f"UPDATE integration_queue_items SET {column} = ? WHERE id = ?",  # noqa: S608 - allowlisted test column
                        (awaiting[column], queue["id"]),
                    )

        with closing(self.database.connect()) as connection, connection:
            connection.execute(
                "UPDATE integration_refs SET current_revision = ? WHERE id = ?",
                ("f" * 40, awaiting["integration_ref_id"]),
            )
        with self.assertRaisesRegex(ConflictError, "authority is stale"):
            self.database.record_landing_receipt(request, recorded_by=self.user["id"])
        with closing(self.database.connect()) as connection, connection:
            connection.execute(
                "UPDATE integration_refs SET current_revision = ? WHERE id = ?",
                (awaiting["current_revision"], awaiting["integration_ref_id"]),
            )

        accepted = self.database.record_landing_receipt(
            request, recorded_by=self.user["id"]
        )
        self.assertEqual(accepted["queue_item"]["state"], "landed")


class IntegrationGoldenPathTests(unittest.TestCase):
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
        (self.repository / "a.txt").write_text("base-a\n", encoding="utf-8")
        (self.repository / "b.txt").write_text("base-b\n", encoding="utf-8")
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
        self.remote = self.root / "remote.git"
        git(self.git_binary, "init", "--quiet", "--bare", str(self.remote))
        git(
            self.git_binary,
            "remote",
            "add",
            "origin",
            str(self.remote),
            cwd=self.repository,
        )
        git(self.git_binary, "push", "--quiet", "origin", "main", cwd=self.repository)

        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        self.instance = self.database.create_krab_instance(
            "Local", "http://127.0.0.1:8765"
        )
        self.identity = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + "1" * 32, "worker", 1),
            "file:/private/worker",
            user_id=self.user["id"],
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="project",
            display_name="Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Real Git integration proof",
                    "repository_path": str(self.repository),
                    "default_branch": "main",
                }
            ),
            created_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.from_mapping(
                {"image": "worker:local", "command": ["worker", "run"]}
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )
        self.results = WorkspaceResultStore(
            self.root / "results", git_binary=self.git_binary
        )
        self.candidates = GitCandidateStore(
            self.root / "candidates", binary=self.git_binary
        )
        self.control = ControlPlane(self.database)
        self.item_a, self.candidate_a = self._candidate("a", "landed-a\n", "4")
        self.item_b, self.candidate_b = self._candidate("b", "landed-b\n", "5")

    @staticmethod
    def policy_spec() -> GatePolicySpec:
        return GatePolicySpec.from_mapping(
            {
                "schema_version": 1,
                "review_separation": None,
                "validations": [
                    {
                        "name": "unit",
                        "image_id": "sha256:" + "9" * 64,
                        "command": ["python", "-m", "unittest"],
                    }
                ],
            }
        )

    def _candidate(
        self, label: str, body: str, digit: str
    ) -> tuple[dict[str, object], dict[str, object]]:
        item = self.database.import_work_item(
            project_key="project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": f"golden-{label}",
                    "source_updated_at": "2026-10-15T00:00:00Z",
                    "title": f"Golden {label.upper()}",
                    "brief": f"Change {label}.txt",
                }
            ),
            imported_by=self.user["id"],
        )
        run = self.database.create_run(
            project_key="project",
            profile_name="Worker",
            work_item_id=item["id"],
            requested_by=self.user["id"],
            source_revision=self.base,
        )
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id=f"golden-{label}",
            expected_run_version=run["version"],
        )
        attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id=f"golden-{label}",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=self.base,
            container_id=digit * 64,
            container_name=f"golden-{label}",
            image_id="sha256:" + digit * 64,
        )
        workspace = self.root / f"workspace-{label}"
        workspace.mkdir()
        GitSnapshotter(binary=self.git_binary).snapshot(
            self.repository, self.base, workspace
        )
        self.results.stage(attempt["id"], workspace, self.base)
        (workspace / f"{label}.txt").write_text(body, encoding="utf-8")
        captured = self.results.capture(attempt["id"], workspace)
        self.database.record_attempt_result(attempt["id"], captured.manifest)
        finished = self.database.complete_run_attempt(
            attempt["id"],
            owner_id=f"golden-{label}",
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id="sha256:" + digit * 64,
        )
        self.assertEqual(finished["state"], "succeeded")
        return item, self.control.create_candidate(
            attempt["id"],
            created_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidates,
        )

    def _policy(
        self,
        candidate: dict[str, object],
        *,
        fold: dict[str, object] | None = None,
    ) -> dict[str, object]:
        manifest = (
            GatePolicyManifest.bind(
                candidate, default_branch="main", policy=self.policy_spec()
            )
            if fold is None
            else GatePolicyManifest.bind_revision(
                candidate,
                fold["manifest"],
                default_branch="main",
                policy=self.policy_spec(),
            )
        )
        policy = self.database.create_gate_policy(manifest, created_by=self.user["id"])
        validation = self.policy_spec().validation("unit")
        self.database.record_gate_evidence(
            ValidationEvidence(
                GATE_EVIDENCE_SCHEMA_VERSION,
                ExactGateInputs.from_policy(policy["id"], manifest),
                GateOutcome.PASSED,
                "unit",
                validation.image_id,
                validation.command_sha256,
                "run_" + policy["id"][4:],
                0,
                1,
                "f" * 64,
                0,
            ),
            recorded_by=self.user["id"],
        )
        return policy

    def _execute_and_receipt(
        self, queue: dict[str, object], *, owner: str
    ) -> tuple[LandingPlan, dict[str, object]]:
        claimed = self.control.claim_integration_item(
            queue["id"],
            owner_id=owner,
            expected_version=queue["version"],
            lease_seconds=30,
        )
        issued = self.control.issue_landing_plan(
            queue["id"],
            owner_id=owner,
            expected_version=claimed["version"],
            remote="origin",
            issued_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidates,
        )
        plan = LandingPlan.from_mapping(issued["plan"])
        for command in plan.commands:
            subprocess.run(  # noqa: S603 - exact argv emitted by tested code
                command,
                cwd=self.repository,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                check=True,
            )
        receipt = self.control.record_landing_receipt(
            {
                "schema_version": 1,
                "plan_id": issued["id"],
                "request_sha256": plan.digest,
                "nonce": issued["nonce"],
                "outcome": "landed",
                "observed_revision": plan.head_revision,
            },
            recorded_by_handle="operator",
        )
        return plan, receipt

    def test_two_candidates_land_in_order_through_real_force_with_lease(self) -> None:
        policy_a = self._policy(self.candidate_a)
        policy_b = self._policy(self.candidate_b)
        self.control.add_landing_dependency(
            self.item_b["id"],
            self.item_a["id"],
            created_by_handle="operator",
        )
        queue_a = self.control.enqueue_integration(
            policy_a["id"], created_by_handle="operator"
        )
        queue_b = self.control.enqueue_integration(
            policy_b["id"], created_by_handle="operator"
        )
        with self.assertRaisesRegex(ConflictError, "dependencies"):
            self.control.claim_integration_item(
                queue_b["id"],
                owner_id="supervisor-b",
                expected_version=queue_b["version"],
                lease_seconds=30,
            )

        plan_a, receipt_a = self._execute_and_receipt(queue_a, owner="supervisor-a")
        self.assertEqual(plan_a.expected_base_revision, self.base)
        self.assertEqual(
            git(self.git_binary, f"--git-dir={self.remote}", "rev-parse", "main"),
            plan_a.head_revision,
        )
        stale_b = self.control.get_integration_queue_item(queue_b["id"])
        self.assertEqual(stale_b["state"], "refold_required")
        self.assertEqual(receipt_a["receipt"]["assurance"], "base_and_head_enforced")
        snapshot = self.control.control_room_snapshot()
        self.assertEqual(snapshot["summary"]["integration_queue"]["landed"], 1)
        self.assertEqual(snapshot["summary"]["landing_dependencies"]["active"], 1)
        visible_b = next(
            item
            for item in snapshot["integration_queue"]
            if item["id"] == queue_b["id"]
        )
        self.assertEqual(visible_b["state"], "refold_required")
        self.assertEqual(visible_b["untrusted_work_item_title"], "Golden B")
        self.assertTrue(
            any(
                item["kind"] == "integration.refold_required"
                and item["subject_id"] == queue_b["id"]
                for item in snapshot["attention"]
            )
        )

        fold_b = self.control.refold_integration_item(
            stale_b["id"],
            expected_version=stale_b["version"],
            created_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidates,
        )
        replayed_fold_b = self.control.refold_integration_item(
            stale_b["id"],
            expected_version=stale_b["version"],
            created_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidates,
        )
        self.assertEqual(replayed_fold_b, fold_b)
        policy_b_folded = self._policy(self.candidate_b, fold=fold_b)
        ready_b = self.control.activate_integration_fold(
            stale_b["id"],
            fold_id=fold_b["id"],
            policy_id=policy_b_folded["id"],
            expected_version=stale_b["version"],
            actor_handle="operator",
        )
        shutil.rmtree(self.candidates.root)
        workspaces = WorkspaceManager(
            self.root / "review-workspaces",
            GitSnapshotter(binary=self.git_binary),
        )
        review_run_id = "run_" + "8" * 32
        prepared = CandidateReviewWorkspaces(
            self.database,
            workspaces,
            self.results,
            self.candidates,
        ).prepare(review_run_id, policy_b_folded["id"])
        self.assertEqual(
            (prepared.path / "a.txt").read_text(encoding="utf-8"), "landed-a\n"
        )
        self.assertEqual(
            (prepared.path / "b.txt").read_text(encoding="utf-8"), "landed-b\n"
        )
        self.assertTrue(workspaces.cleanup(review_run_id))
        plan_b, receipt_b = self._execute_and_receipt(ready_b, owner="supervisor-b")

        self.assertEqual(plan_b.expected_base_revision, plan_a.head_revision)
        self.assertEqual(plan_b.fold_id, fold_b["id"])
        self.assertNotEqual(policy_b_folded["id"], policy_b["id"])
        self.assertEqual(receipt_b["queue_item"]["state"], "landed")
        self.assertEqual(
            git(self.git_binary, f"--git-dir={self.remote}", "rev-parse", "main"),
            plan_b.head_revision,
        )
        self.assertEqual(
            git(
                self.git_binary,
                f"--git-dir={self.remote}",
                "show",
                f"{plan_b.head_revision}:a.txt",
            ),
            "landed-a",
        )
        self.assertEqual(
            git(
                self.git_binary,
                f"--git-dir={self.remote}",
                "show",
                f"{plan_b.head_revision}:b.txt",
            ),
            "landed-b",
        )


if __name__ == "__main__":
    unittest.main()
