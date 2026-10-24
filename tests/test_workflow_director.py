from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from magic_glass_box.candidates import GitCandidateStore
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    ValidationError,
    WorkItemEnvelope,
    WorkLogEntry,
    canonical_json,
)
from magic_glass_box.executor import SandboxResult
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.storage.errors import ConcurrentUpdateError, ConflictError
from magic_glass_box.supervision import Supervisor
from magic_glass_box.workflow_director import WORKFLOW_RUN_SUPERVISION, WorkflowDirector
from magic_glass_box.workflows import WorkflowConfigurationSpec


def git(binary: Path, *arguments: str, cwd: Path) -> str:
    completed = subprocess.run(  # noqa: S603 - test-controlled Git executable
        [str(binary), *arguments],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=True,
    )
    return completed.stdout.decode("utf-8", "strict").strip()


class WorkflowDirectorTests(unittest.TestCase):
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
        (self.repository / "feature.txt").write_text("base\n", encoding="utf-8")
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
        instance = self.database.create_krab_instance("Local", "http://127.0.0.1:8765")
        producer_identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "1" * 32, "producer", 1),
            "file:/private/producer",
            user_id=self.user["id"],
        )
        reviewer_identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "2" * 32, "reviewer", 1),
            "file:/private/reviewer",
        )
        self.project = self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="workflow-project",
            display_name="Workflow Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Prove restart-safe implementation workflows",
                    "repository_path": str(self.repository),
                    "default_branch": "main",
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
                    "external_id": "workflow-director-1",
                    "source_updated_at": "2026-10-23T12:00:00Z",
                    "title": "Exercise one correction round",
                    "brief": "Build, review, correct, and request maintainer approval.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.producer = self.database.create_agent_profile(
            name="Producer",
            spec=AgentProfileSpec.create(
                image="producer@sha256:abc",
                command=["producer", "run"],
                network_mode="none",
                workspace_mode="copy_on_write",
            ),
            krab_identity_id=producer_identity["id"],
            created_by=self.user["id"],
        )
        self.reviewer = self.database.create_agent_profile(
            name="Reviewer",
            spec=AgentProfileSpec.create(
                image="reviewer@sha256:def",
                command=["reviewer", "run"],
                network_mode="none",
                workspace_mode="read_only",
            ),
            krab_identity_id=reviewer_identity["id"],
            created_by=self.user["id"],
        )
        self.configuration = self.database.create_workflow_configuration(
            project_key="workflow-project",
            name="default",
            spec=WorkflowConfigurationSpec.from_mapping(
                {
                    "schema_version": 1,
                    "review_separation": "different_profile_and_identity",
                    "maintainer_approval": True,
                    "validations": [],
                    "max_correction_rounds": 2,
                    "landing_mode": "manual",
                }
            ),
            producer_profile_name="Producer",
            reviewer_profile_name="Reviewer",
            created_by=self.user["id"],
        )
        self.execution = self.database.start_workflow_execution(
            project_key="workflow-project",
            configuration_name="default",
            work_item_id=self.work_item["id"],
            source_revision=self.base,
            expected_configuration_version=1,
            expected_context_version_id=self.project["context_version_id"],
            expected_work_item_version_id=self.work_item["work_item_version_id"],
            expected_thread_generation=1,
            expected_thread_version=1,
            idempotency_key="director-golden-path",
            requested_by=self.user["id"],
        )
        self.results = WorkspaceResultStore(self.root / "results")
        self.candidates = GitCandidateStore(
            self.root / "candidates", binary=self.git_binary
        )
        self.control = ControlPlane(self.database)
        self.workspace_sequence = 0

    def director(self) -> WorkflowDirector:
        # No validation gates are configured in this proof, so the executor is inert.
        return WorkflowDirector(
            self.control,
            self.results,
            self.candidates,
            object(),  # type: ignore[arg-type]
        )

    def assert_transition_event(
        self,
        event: dict[str, object],
        *,
        kind: str,
        from_stage: str,
        to_stage: str,
        from_round: int,
        to_round: int,
        metadata: dict[str, object],
    ) -> None:
        self.assertEqual(event["kind"], kind)
        self.assertEqual(
            event["metadata"],
            {
                **metadata,
                "from_stage": from_stage,
                "to_stage": to_stage,
                "from_correction_round": from_round,
                "to_correction_round": to_round,
            },
        )

    def start_execution(
        self,
        *,
        configuration_name: str,
        external_id: str,
        spec: dict[str, object],
    ) -> dict[str, object]:
        item = self.database.import_work_item(
            project_key="workflow-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": external_id,
                    "source_updated_at": "2026-10-23T13:00:00Z",
                    "title": "Exercise a focused director boundary",
                    "brief": "Build and independently review one exact candidate.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.database.create_workflow_configuration(
            project_key="workflow-project",
            name=configuration_name,
            spec=WorkflowConfigurationSpec.from_mapping(spec),
            producer_profile_name="Producer",
            reviewer_profile_name="Reviewer",
            created_by=self.user["id"],
        )
        return self.database.start_workflow_execution(
            project_key="workflow-project",
            configuration_name=configuration_name,
            work_item_id=item["id"],
            source_revision=self.base,
            expected_configuration_version=1,
            expected_context_version_id=self.project["context_version_id"],
            expected_work_item_version_id=item["work_item_version_id"],
            expected_thread_generation=1,
            expected_thread_version=1,
            idempotency_key=f"director-{external_id}",
            requested_by=self.user["id"],
        )

    def finish_run(
        self,
        run: dict[str, object],
        *,
        source_revision: str,
        before: str,
        after: str,
        review_outcome: str | None = None,
        exit_code: int = 0,
    ) -> dict[str, object]:
        self.workspace_sequence += 1
        owner = f"workflow-test-{self.workspace_sequence}"
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id=owner,
            expected_run_version=run["version"],
        )
        attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id=owner,
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=source_revision,
            container_id=f"{self.workspace_sequence:x}" * 64,
            container_name=owner,
            image_id="sha256:" + f"{self.workspace_sequence:x}" * 64,
        )
        workspace = self.root / f"workspace-{self.workspace_sequence}"
        workspace.mkdir()
        (workspace / "feature.txt").write_text(before, encoding="utf-8")
        self.results.stage(attempt["id"], workspace, source_revision)
        (workspace / "feature.txt").write_text(after, encoding="utf-8")
        captured = self.results.capture(attempt["id"], workspace)
        self.database.record_attempt_result(attempt["id"], captured.manifest)
        if review_outcome is not None:
            self.control.append_work_log(
                run["id"],
                WorkLogEntry.from_mapping(
                    {
                        "schema_version": 2,
                        "kind": "review",
                        "summary": "Independent exact-head review is complete.",
                        "details": (
                            ["Preserve the compatibility behavior in a focused test."]
                            if review_outcome == "changes_requested"
                            else []
                        ),
                        "artifacts": [],
                        "related_interaction_ids": [],
                        "candidate_revision": source_revision,
                        "review_outcome": review_outcome,
                    }
                ),
                idempotency_key=f"review-verdict-{self.workspace_sequence}",
            )
        return self.database.complete_run_attempt(
            attempt["id"],
            owner_id=owner,
            expected_attempt_version=attempt["version"],
            exit_code=exit_code,
            image_id=attempt["image_id"],
        )

    def drive_build_to_review(
        self, execution: dict[str, object], *, before: str, after: str
    ) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
        self.assertFalse(
            self.director().advance(
                execution["id"], expected_version=execution["version"]
            )
        )
        producer = self.database.find_workflow_run(
            execution["id"], execution["correction_round"], "producer"
        )
        self.assertIsNotNone(producer)
        producer_execution = self.database.get_run_execution(producer["id"])
        self.assertEqual(
            producer_execution["supervision_policy"],
            WORKFLOW_RUN_SUPERVISION.to_dict(),
        )
        self.assertFalse(
            self.director().advance(
                execution["id"], expected_version=execution["version"]
            )
        )
        self.finish_run(
            producer,
            source_revision=producer["source_revision"],
            before=before,
            after=after,
        )
        self.assertFalse(
            self.director().advance(
                execution["id"], expected_version=execution["version"]
            )
        )
        self.assertFalse(
            self.director().advance(
                execution["id"], expected_version=execution["version"]
            )
        )
        self.assertTrue(
            self.director().advance(
                execution["id"], expected_version=execution["version"]
            )
        )
        advanced = self.database.get_workflow_execution(execution["id"])
        gate = self.database.get_workflow_gate(
            execution["id"], execution["correction_round"]
        )
        reviewer = self.database.find_workflow_run(
            execution["id"], execution["correction_round"], "reviewer"
        )
        self.assertIsNotNone(gate)
        self.assertIsNotNone(reviewer)
        self.assertEqual(advanced["stage"], "review")
        reviewer_execution = self.database.get_run_execution(reviewer["id"])
        self.assertEqual(
            reviewer_execution["source_revision"],
            self.database.get_gate_policy(gate["policy_id"])["head_revision"],
        )
        self.assertEqual(
            reviewer_execution["supervision_policy"],
            WORKFLOW_RUN_SUPERVISION.to_dict(),
        )
        self.assert_transition_event(
            advanced["events"][-1],
            kind="workflow.review_started",
            from_stage="build",
            to_stage="review",
            from_round=execution["correction_round"],
            to_round=execution["correction_round"],
            metadata={
                "producer_run_id": producer["id"],
                "candidate_id": gate["candidate_id"],
                "policy_id": gate["policy_id"],
                "reviewer_run_id": reviewer["id"],
            },
        )
        self.assertFalse(
            self.director().advance(
                advanced["id"], expected_version=advanced["version"]
            )
        )
        return advanced, gate, reviewer

    def test_restart_safe_change_round_reaches_exact_maintainer_handoff(self) -> None:
        # A later profile revision must not rewrite the workflow's pinned producer.
        self.database.add_agent_profile_version(
            name="Producer",
            spec=AgentProfileSpec.create(
                image="producer@sha256:new",
                command=["producer", "run"],
                network_mode="none",
                workspace_mode="copy_on_write",
            ),
            krab_identity_id=self.database.get_agent_profile("Producer")[
                "krab_identity_id"
            ],
            created_by=self.user["id"],
        )

        review_one, gate_one, reviewer_one = self.drive_build_to_review(
            self.execution, before="base\n", after="first\n"
        )
        producer_one = self.database.find_workflow_run(
            self.execution["id"], 0, "producer"
        )
        self.assertEqual(
            producer_one["profile_version_id"],
            self.configuration["roles"]["producer"]["profile_version_id"],
        )
        self.finish_run(
            reviewer_one,
            source_revision=self.database.get_gate_policy(gate_one["policy_id"])[
                "head_revision"
            ],
            before="first\n",
            after="first\n",
            review_outcome="changes_requested",
        )
        self.assertTrue(
            self.director().advance(
                review_one["id"], expected_version=review_one["version"]
            )
        )
        correction = self.database.get_workflow_execution(review_one["id"])
        self.assertEqual(
            (correction["stage"], correction["correction_round"]), ("build", 1)
        )
        verdict_one = self.database.get_typed_review_verdict(
            gate_one["policy_id"], reviewer_one["id"]
        )
        evidence_one = next(
            item
            for item in self.database.list_gate_evidence(gate_one["policy_id"])
            if item["gate_name"] == "independent-review"
        )
        self.assert_transition_event(
            correction["events"][-1],
            kind="workflow.correction_started",
            from_stage="review",
            to_stage="build",
            from_round=0,
            to_round=1,
            metadata={
                "policy_id": gate_one["policy_id"],
                "feedback_source": "independent_review",
                "review_evidence_id": evidence_one["id"],
                "review_entry_id": verdict_one["work_log_entry_id"],
            },
        )

        review_two, gate_two, reviewer_two = self.drive_build_to_review(
            correction, before="first\n", after="second\n"
        )
        first_candidate = self.database.get_candidate(gate_one["candidate_id"])
        second_candidate = self.database.get_candidate(gate_two["candidate_id"])
        self.assertEqual(
            second_candidate["base_revision"], first_candidate["head_revision"]
        )
        self.assertNotEqual(gate_two["policy_id"], gate_one["policy_id"])
        self.finish_run(
            reviewer_two,
            source_revision=self.database.get_gate_policy(gate_two["policy_id"])[
                "head_revision"
            ],
            before="second\n",
            after="second\n",
            review_outcome="approved",
        )
        self.assertTrue(
            self.director().advance(
                review_two["id"], expected_version=review_two["version"]
            )
        )
        maintainer = self.database.get_workflow_execution(review_two["id"])
        self.assertEqual(maintainer["stage"], "maintainer")
        evidence_two = next(
            item
            for item in self.database.list_gate_evidence(gate_two["policy_id"])
            if item["gate_name"] == "independent-review"
        )
        policy_two = self.database.get_gate_policy(gate_two["policy_id"])
        self.assert_transition_event(
            maintainer["events"][-1],
            kind="workflow.maintainer_pending",
            from_stage="review",
            to_stage="maintainer",
            from_round=1,
            to_round=1,
            metadata={
                "candidate_id": gate_two["candidate_id"],
                "policy_id": gate_two["policy_id"],
                "review_evidence_id": evidence_two["id"],
                "head_revision": policy_two["head_revision"],
            },
        )
        self.assertFalse(
            self.director().advance(
                maintainer["id"], expected_version=maintainer["version"]
            )
        )

        disposition = self.control.record_maintainer_review(
            gate_two["policy_id"],
            review_request={
                "schema_version": 1,
                "decision": "approved",
                "summary": "Product fit approved for landing.",
                "details": [],
            },
            recorded_by_handle="operator",
            expected_policy_revision=policy_two["policy_revision"],
        )
        self.assertTrue(
            self.director().advance(
                maintainer["id"], expected_version=maintainer["version"]
            )
        )
        ready = self.database.get_workflow_execution(maintainer["id"])
        self.assertEqual((ready["stage"], ready["correction_round"]), ("landing", 1))
        self.assert_transition_event(
            ready["events"][-1],
            kind="workflow.landing_ready",
            from_stage="maintainer",
            to_stage="landing",
            from_round=1,
            to_round=1,
            metadata={
                "candidate_id": gate_two["candidate_id"],
                "policy_id": gate_two["policy_id"],
                "maintainer_review_id": disposition["id"],
                "head_revision": policy_two["head_revision"],
            },
        )
        self.assertTrue(
            self.director().advance(ready["id"], expected_version=maintainer["version"])
        )
        self.assertTrue(
            self.director().advance(ready["id"], expected_version=ready["version"])
        )
        self.assertEqual(self.database.status()["counts"]["workflow_run_bindings"], 4)

    def test_cancelled_execution_is_a_noop_at_its_current_version(self) -> None:
        cancelled = self.control.cancel_workflow_execution(
            self.execution["id"],
            expected_version=self.execution["version"],
            cancelled_by_handle="operator",
            reason="Do not start this prescription.",
        )

        self.assertTrue(
            self.director().advance(
                cancelled["id"], expected_version=cancelled["version"]
            )
        )
        self.assertIsNone(
            self.database.find_workflow_run(cancelled["id"], 0, "producer")
        )

    def test_gate_binding_is_idempotent_and_producer_scoped(self) -> None:
        director = self.director()
        self.assertFalse(
            director.advance(
                self.execution["id"], expected_version=self.execution["version"]
            )
        )
        producer = self.database.find_workflow_run(self.execution["id"], 0, "producer")
        self.assertIsNotNone(producer)
        self.finish_run(
            producer,
            source_revision=producer["source_revision"],
            before="base\n",
            after="candidate\n",
        )
        self.assertFalse(
            director.advance(
                self.execution["id"], expected_version=self.execution["version"]
            )
        )
        candidates = self.database.list_candidates(
            producer_run_id=producer["id"], limit=2
        )
        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        policy = self.control.create_gate_policy(
            candidate["id"],
            {
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "validations": [],
                "maintainer_approval": True,
            },
            created_by_handle="operator",
            image_resolver=lambda image: self.fail(
                f"empty validation policy unexpectedly resolved {image}"
            ),
        )

        binding = self.database.bind_workflow_gate(
            self.execution["id"],
            correction_round=0,
            candidate_id=candidate["id"],
            policy_id=policy["id"],
        )
        self.assertEqual(
            binding, self.database.get_workflow_gate(self.execution["id"], 0)
        )
        self.assertEqual(
            self.database.bind_workflow_gate(
                self.execution["id"],
                correction_round=0,
                candidate_id=candidate["id"],
                policy_id=policy["id"],
            ),
            binding,
        )
        with self.assertRaisesRegex(ConflictError, "another gate policy"):
            self.database.bind_workflow_gate(
                self.execution["id"],
                correction_round=0,
                candidate_id="can_" + "f" * 32,
                policy_id=policy["id"],
            )

        foreign = self.start_execution(
            configuration_name="foreign-gate",
            external_id="workflow-director-foreign-gate",
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": True,
                "validations": [],
                "max_correction_rounds": 2,
                "landing_mode": "manual",
            },
        )
        with self.assertRaisesRegex(ConflictError, "not produced by this round"):
            self.database.bind_workflow_gate(
                foreign["id"],
                correction_round=0,
                candidate_id=candidate["id"],
                policy_id=policy["id"],
            )

    def test_transition_guards_round_bounds_and_metadata_budget(self) -> None:
        transition = {
            "expected_version": self.execution["version"],
            "expected_correction_round": 0,
            "target_stage": "review",
            "target_correction_round": 0,
            "event_kind": "workflow.review_started",
            "metadata": {},
        }
        with self.assertRaises(ValidationError):
            self.database.advance_workflow_execution(
                self.execution["id"], expected_stage="invalid", **transition
            )
        with self.assertRaises(ValidationError):
            self.database.advance_workflow_execution(
                self.execution["id"],
                **{
                    **transition,
                    "expected_stage": "build",
                    "target_stage": "invalid",
                },
            )
        with self.assertRaises(ConcurrentUpdateError):
            self.database.advance_workflow_execution(
                self.execution["id"], expected_stage="review", **transition
            )
        with self.assertRaises(ConcurrentUpdateError):
            self.database.advance_workflow_execution(
                self.execution["id"],
                **{
                    **transition,
                    "expected_stage": "build",
                    "expected_correction_round": 1,
                },
            )

        metadata_overhead = len(canonical_json({"pad": ""}).encode("utf-8"))
        exact_metadata = {"pad": "x" * (16 * 1024 - metadata_overhead)}
        self.assertEqual(len(canonical_json(exact_metadata).encode("utf-8")), 16 * 1024)
        accepted = self.database.advance_workflow_execution(
            self.execution["id"],
            **{
                **transition,
                "expected_stage": "build",
                "metadata": exact_metadata,
            },
        )
        self.assertEqual(accepted["stage"], "review")

        oversized = self.start_execution(
            configuration_name="metadata-budget",
            external_id="workflow-director-metadata-budget",
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": False,
                "validations": [],
                "max_correction_rounds": 2,
                "landing_mode": "manual",
            },
        )
        too_large = {"pad": "x" * (16 * 1024 + 1 - metadata_overhead)}
        self.assertEqual(len(canonical_json(too_large).encode("utf-8")), 16 * 1024 + 1)
        with self.assertRaises(ValidationError):
            self.database.advance_workflow_execution(
                oversized["id"],
                expected_version=oversized["version"],
                expected_stage="build",
                expected_correction_round=0,
                target_stage="review",
                target_correction_round=0,
                event_kind="workflow.review_started",
                metadata=too_large,
            )
        self.assertEqual(
            self.database.get_workflow_execution(oversized["id"])["version"],
            oversized["version"],
        )

        for invalid_round in (True, 1.5, -1, 9):
            with self.subTest(invalid_round=invalid_round):
                with self.assertRaises(ValidationError):
                    self.database.get_workflow_gate(oversized["id"], invalid_round)
                with self.assertRaises(ValidationError):
                    self.database.find_workflow_run(
                        oversized["id"], invalid_round, "producer"
                    )
        self.assertIsNone(self.database.get_workflow_gate(oversized["id"], 8))
        self.assertIsNone(
            self.database.find_workflow_run(oversized["id"], 8, "producer")
        )

    def test_failed_producer_opens_a_ruling_without_creating_a_candidate(self) -> None:
        self.assertFalse(
            self.director().advance(
                self.execution["id"], expected_version=self.execution["version"]
            )
        )
        producer = self.database.find_workflow_run(self.execution["id"], 0, "producer")
        self.finish_run(
            producer,
            source_revision=producer["source_revision"],
            before="base\n",
            after="partial\n",
            exit_code=17,
        )

        self.assertTrue(
            self.director().advance(
                self.execution["id"], expected_version=self.execution["version"]
            )
        )

        stopped = self.database.get_workflow_execution(self.execution["id"])
        self.assertEqual(stopped["stage"], "build")
        self.assertEqual(
            [(ruling["kind"], ruling["run_id"]) for ruling in stopped["rulings"]],
            [("producer_failed", producer["id"])],
        )
        self.assertEqual(
            self.database.list_candidates(producer_run_id=producer["id"]), []
        )
        ruling = stopped["rulings"][0]
        self.assertEqual(stopped["events"][-1]["kind"], "workflow.ruling_opened")
        self.assertEqual(
            stopped["events"][-1]["metadata"],
            {
                "ruling_id": ruling["id"],
                "kind": "producer_failed",
                "correction_round": 0,
                "policy_id": None,
                "run_id": producer["id"],
                "gate_name": None,
            },
        )
        repeated = self.database.open_workflow_ruling(
            stopped["id"],
            expected_version=stopped["version"],
            kind="producer_failed",
            run_id=producer["id"],
        )
        self.assertEqual(repeated["id"], ruling["id"])
        repeated_execution = self.database.get_workflow_execution(stopped["id"])
        self.assertEqual(len(repeated_execution["rulings"]), 1)
        self.assertEqual(
            sum(
                event["kind"] == "workflow.ruling_opened"
                for event in repeated_execution["events"]
            ),
            1,
        )
        with self.assertRaisesRegex(ConflictError, "correction-limit"):
            self.control.resolve_workflow_ruling(
                ruling["id"],
                expected_version=ruling["version"],
                decision="continue",
                resolved_by_handle="operator",
            )
        resolved = self.control.resolve_workflow_ruling(
            ruling["id"],
            expected_version=ruling["version"],
            decision="cancel",
            resolved_by_handle="operator",
        )
        self.assertEqual(
            (resolved["state"], resolved["decision"]), ("resolved", "cancel")
        )
        self.assertEqual(resolved["resolved_by"], "operator")
        cancelled = self.database.get_workflow_execution(self.execution["id"])
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(cancelled["stage"], "build")
        self.assertEqual(
            cancelled["events"][-1]["metadata"],
            {
                "ruling_id": ruling["id"],
                "decision": "cancel",
                "from_stage": "build",
                "to_stage": "build",
                "from_correction_round": 0,
                "to_correction_round": 0,
            },
        )
        with self.assertRaisesRegex(ConflictError, "not open"):
            self.control.resolve_workflow_ruling(
                ruling["id"],
                expected_version=resolved["version"],
                decision="cancel",
                resolved_by_handle="operator",
            )

    def test_failed_or_silent_reviewer_opens_the_exact_ruling(self) -> None:
        cases = (
            ("failed", 17, "reviewer_failed"),
            ("silent", 0, "review_verdict_missing"),
        )
        for suffix, exit_code, expected_kind in cases:
            with self.subTest(case=suffix):
                execution = self.start_execution(
                    configuration_name=f"reviewer-{suffix}",
                    external_id=f"workflow-director-reviewer-{suffix}",
                    spec={
                        "schema_version": 1,
                        "review_separation": "different_profile_and_identity",
                        "maintainer_approval": False,
                        "validations": [],
                        "max_correction_rounds": 2,
                        "landing_mode": "manual",
                    },
                )
                review, gate, reviewer = self.drive_build_to_review(
                    execution, before="base\n", after=f"{suffix}\n"
                )
                self.finish_run(
                    reviewer,
                    source_revision=self.database.get_gate_policy(gate["policy_id"])[
                        "head_revision"
                    ],
                    before=f"{suffix}\n",
                    after=f"{suffix}\n",
                    exit_code=exit_code,
                )

                self.assertTrue(
                    self.director().advance(
                        review["id"], expected_version=review["version"]
                    )
                )

                stopped = self.database.get_workflow_execution(execution["id"])
                self.assertEqual(stopped["stage"], "review")
                self.assertEqual(
                    [
                        (ruling["kind"], ruling["policy_id"], ruling["run_id"])
                        for ruling in stopped["rulings"]
                    ],
                    [(expected_kind, gate["policy_id"], reviewer["id"])],
                )

    def test_maintainer_changes_create_a_provenance_bound_correction(self) -> None:
        review, gate, reviewer = self.drive_build_to_review(
            self.execution, before="base\n", after="needs-product-change\n"
        )
        self.finish_run(
            reviewer,
            source_revision=self.database.get_gate_policy(gate["policy_id"])[
                "head_revision"
            ],
            before="needs-product-change\n",
            after="needs-product-change\n",
            review_outcome="approved",
        )
        self.assertTrue(
            self.director().advance(review["id"], expected_version=review["version"])
        )
        maintainer = self.database.get_workflow_execution(self.execution["id"])
        policy = self.database.get_gate_policy(gate["policy_id"])
        disposition = self.control.record_maintainer_review(
            gate["policy_id"],
            review_request={
                "schema_version": 1,
                "decision": "changes_requested",
                "summary": "Keep the command surface narrower.",
                "details": ["Remove the unbounded fallback before resubmitting."],
            },
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )

        self.assertTrue(
            self.director().advance(
                maintainer["id"], expected_version=maintainer["version"]
            )
        )
        correction = self.database.get_workflow_execution(self.execution["id"])
        self.assertEqual(
            (correction["stage"], correction["correction_round"]), ("build", 1)
        )
        self.assert_transition_event(
            correction["events"][-1],
            kind="workflow.correction_started",
            from_stage="maintainer",
            to_stage="build",
            from_round=0,
            to_round=1,
            metadata={
                "policy_id": gate["policy_id"],
                "feedback_source": "maintainer_review",
                "maintainer_review_id": disposition["id"],
            },
        )
        self.assertFalse(
            self.director().advance(
                correction["id"], expected_version=correction["version"]
            )
        )
        producer = self.database.find_workflow_run(correction["id"], 1, "producer")
        run = self.database.get_run_execution(producer["id"])
        self.assertEqual(run["changes_requested"]["source"], "maintainer_review")
        self.assertEqual(run["changes_requested"]["policy_id"], gate["policy_id"])
        self.assertEqual(
            run["changes_requested"]["maintainer_review"]["id"], disposition["id"]
        )

    def test_supervisor_keeps_one_durable_deferred_action_for_the_stage(self) -> None:
        class Coordinator:
            def execute(self, _run_id: str) -> dict[str, object]:
                return {}

            def reconcile(self, _run_id: str | None = None) -> list[dict[str, object]]:
                return []

        class Director:
            def __init__(self) -> None:
                self.calls: list[tuple[str, int]] = []

            def advance(self, execution_id: str, *, expected_version: int) -> bool:
                self.calls.append((execution_id, expected_version))
                return False

        class Transport:
            def remind_interaction(self, *_args: object, **_kwargs: object) -> int:
                return 1

        created = self.database.enqueue_supervisor_actions()
        repeated = self.database.enqueue_supervisor_actions()
        self.assertEqual(len(created), 1)
        self.assertEqual(repeated, [])
        self.assertEqual(created[0]["kind"], "workflow.advance")
        self.assertIsNone(created[0]["run_id"])
        self.assertEqual(created[0]["workflow_execution_id"], self.execution["id"])

        director = Director()
        supervisor = Supervisor(
            self.database,
            Coordinator(),
            Transport(),
            workflow_director=director,
            poll_interval=0.05,
            action_lease_seconds=10,
            action_timeout=1,
        )
        supervisor.tick(wait=True)

        action = self.database.list_supervisor_actions()[0]
        self.assertEqual(action["state"], "pending")
        self.assertEqual(action["attempt_count"], 0)
        self.assertEqual(action["max_attempts"], 4)
        self.assertEqual(director.calls, [(self.execution["id"], 1)])
        self.assertEqual(len(self.database.list_supervisor_actions()), 1)

    def test_supervisor_records_exactly_one_completed_action_per_transition(
        self,
    ) -> None:
        class Coordinator:
            @staticmethod
            def execute(_run_id: str) -> dict[str, object]:
                return {}

            @staticmethod
            def reconcile(_run_id: str | None = None) -> list[dict[str, object]]:
                return []

        class Transport:
            @staticmethod
            def remind_interaction(*_args: object, **_kwargs: object) -> int:
                return 1

        director = self.director()
        supervisor = Supervisor(
            self.database,
            Coordinator(),
            Transport(),
            workflow_director=director,
            poll_interval=0.05,
            action_lease_seconds=10,
            action_timeout=1,
        )
        build_action = self.database.enqueue_supervisor_actions()[0]
        self.assertFalse(director.advance(self.execution["id"], expected_version=1))
        producer = self.database.find_workflow_run(self.execution["id"], 0, "producer")
        self.finish_run(
            producer,
            source_revision=producer["source_revision"],
            before="base\n",
            after="supervised\n",
        )
        self.assertFalse(director.advance(self.execution["id"], expected_version=1))
        self.assertFalse(director.advance(self.execution["id"], expected_version=1))
        supervisor.tick(wait=True)
        self.assertEqual(
            self.database.get_supervisor_action(build_action["id"])["state"],
            "succeeded",
        )

        review = self.database.get_workflow_execution(self.execution["id"])
        reviewer = self.database.find_workflow_run(self.execution["id"], 0, "reviewer")
        gate = self.database.get_workflow_gate(self.execution["id"], 0)
        self.finish_run(
            reviewer,
            source_revision=self.database.get_gate_policy(gate["policy_id"])[
                "head_revision"
            ],
            before="supervised\n",
            after="supervised\n",
            review_outcome="approved",
        )
        review_action = self.database.enqueue_supervisor_actions()[0]
        supervisor.tick(wait=True)
        self.assertEqual(
            self.database.get_supervisor_action(review_action["id"])["state"],
            "succeeded",
        )

        maintainer = self.database.get_workflow_execution(self.execution["id"])
        policy = self.database.get_gate_policy(gate["policy_id"])
        self.control.record_maintainer_review(
            gate["policy_id"],
            review_request={
                "schema_version": 1,
                "decision": "approved",
                "summary": "The exact candidate fits the project.",
                "details": [],
            },
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        maintainer_action = self.database.enqueue_supervisor_actions()[0]
        supervisor.tick(wait=True)

        actions = self.database.list_supervisor_actions()
        self.assertEqual(len(actions), 3)
        self.assertEqual(
            {(action["subject_version"], action["state"]) for action in actions},
            {
                (1, "succeeded"),
                (review["version"], "succeeded"),
                (maintainer["version"], "succeeded"),
            },
        )
        self.assertEqual(
            self.database.get_supervisor_action(maintainer_action["id"])["state"],
            "succeeded",
        )
        self.assertEqual(
            self.database.get_workflow_execution(self.execution["id"])["stage"],
            "landing",
        )
        self.assertEqual(self.database.enqueue_supervisor_actions(), [])

    def test_correction_limit_ruling_requires_explicit_resolution(self) -> None:
        review_zero = self.database.advance_workflow_execution(
            self.execution["id"],
            expected_version=1,
            expected_stage="build",
            expected_correction_round=0,
            target_stage="review",
            target_correction_round=0,
            event_kind="workflow.review_started",
            metadata={},
        )
        build_one = self.database.advance_workflow_execution(
            review_zero["id"],
            expected_version=review_zero["version"],
            expected_stage="review",
            expected_correction_round=0,
            target_stage="build",
            target_correction_round=1,
            event_kind="workflow.correction_started",
            metadata={},
        )
        review_one = self.database.advance_workflow_execution(
            build_one["id"],
            expected_version=build_one["version"],
            expected_stage="build",
            expected_correction_round=1,
            target_stage="review",
            target_correction_round=1,
            event_kind="workflow.review_started",
            metadata={},
        )
        ruling = self.database.open_workflow_ruling(
            review_one["id"],
            expected_version=review_one["version"],
            kind="correction_limit",
        )

        self.assertEqual(ruling["state"], "open")
        opened = self.database.get_workflow_execution(review_one["id"])
        self.assertEqual(opened["events"][-1]["kind"], "workflow.ruling_opened")
        self.assertEqual(
            opened["events"][-1]["metadata"],
            {
                "ruling_id": ruling["id"],
                "kind": "correction_limit",
                "correction_round": 1,
                "policy_id": None,
                "run_id": None,
                "gate_name": None,
            },
        )
        self.assertEqual(self.database.enqueue_supervisor_actions(), [])
        continued = self.control.resolve_workflow_ruling(
            ruling["id"],
            expected_version=1,
            decision="continue",
            resolved_by_handle="operator",
        )
        self.assertEqual(
            (continued["state"], continued["decision"]), ("resolved", "continue")
        )
        self.assertEqual(continued["resolved_by"], "operator")
        resumed = self.database.get_workflow_execution(review_one["id"])
        self.assertEqual((resumed["stage"], resumed["correction_round"]), ("build", 2))
        self.assertEqual(resumed["events"][-1]["kind"], "workflow.ruling_resolved")
        self.assertEqual(
            resumed["events"][-1]["metadata"],
            {
                "ruling_id": ruling["id"],
                "decision": "continue",
                "from_stage": "review",
                "to_stage": "build",
                "from_correction_round": 1,
                "to_correction_round": 2,
            },
        )
        actions = self.database.enqueue_supervisor_actions()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["subject_version"], resumed["version"])

    def test_ruling_resolution_rejects_a_changed_execution(self) -> None:
        ruling = self.database.open_workflow_ruling(
            self.execution["id"],
            expected_version=self.execution["version"],
            kind="correction_limit",
        )
        moved = self.database.advance_workflow_execution(
            self.execution["id"],
            expected_version=self.execution["version"],
            expected_stage="build",
            expected_correction_round=0,
            target_stage="review",
            target_correction_round=0,
            event_kind="workflow.review_started",
            metadata={},
        )
        self.assertEqual((moved["stage"], moved["correction_round"]), ("review", 0))

        with self.assertRaisesRegex(ConflictError, "changed after the ruling"):
            self.control.resolve_workflow_ruling(
                ruling["id"],
                expected_version=ruling["version"],
                decision="continue",
                resolved_by_handle="operator",
            )
        self.assertEqual(
            self.database.get_workflow_ruling(ruling["id"])["state"], "open"
        )

    def test_cancel_is_a_supported_ruling_resolution(self) -> None:
        ruling = self.database.open_workflow_ruling(
            self.execution["id"],
            expected_version=self.execution["version"],
            kind="correction_limit",
        )

        resolved = self.control.resolve_workflow_ruling(
            ruling["id"],
            expected_version=ruling["version"],
            decision="cancel",
            resolved_by_handle="operator",
        )

        self.assertEqual(
            (resolved["state"], resolved["decision"], resolved["resolved_by"]),
            ("resolved", "cancel", "operator"),
        )
        execution = self.database.get_workflow_execution(self.execution["id"])
        self.assertEqual(
            (execution["state"], execution["stage"]), ("cancelled", "build")
        )

    def test_absolute_correction_round_limit_cannot_be_overridden(self) -> None:
        execution = self.start_execution(
            configuration_name="absolute-ceiling",
            external_id="workflow-director-absolute-ceiling",
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": False,
                "validations": [],
                "max_correction_rounds": 8,
                "landing_mode": "manual",
            },
        )
        current = execution
        for correction_round in range(8):
            current = self.database.advance_workflow_execution(
                current["id"],
                expected_version=current["version"],
                expected_stage="build",
                expected_correction_round=correction_round,
                target_stage="review",
                target_correction_round=correction_round,
                event_kind="workflow.review_started",
                metadata={},
            )
            current = self.database.advance_workflow_execution(
                current["id"],
                expected_version=current["version"],
                expected_stage="review",
                expected_correction_round=correction_round,
                target_stage="build",
                target_correction_round=correction_round + 1,
                event_kind="workflow.correction_started",
                metadata={},
            )
        self.assertEqual((current["stage"], current["correction_round"]), ("build", 8))
        ruling = self.database.open_workflow_ruling(
            current["id"],
            expected_version=current["version"],
            kind="correction_limit",
        )

        with self.assertRaisesRegex(ConflictError, "absolute correction-round limit"):
            self.control.resolve_workflow_ruling(
                ruling["id"],
                expected_version=ruling["version"],
                decision="continue",
                resolved_by_handle="operator",
            )
        self.assertIsNone(self.database.get_workflow_gate(current["id"], 8))
        self.assertIsNone(self.database.find_workflow_run(current["id"], 8, "producer"))

    def test_director_opens_ruling_instead_of_exceeding_correction_ceiling(
        self,
    ) -> None:
        execution = self.start_execution(
            configuration_name="ceiling",
            external_id="workflow-director-ceiling",
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": False,
                "validations": [],
                "max_correction_rounds": 1,
                "landing_mode": "manual",
            },
        )

        review_zero, gate_zero, reviewer_zero = self.drive_build_to_review(
            execution, before="base\n", after="ceiling-one\n"
        )
        self.finish_run(
            reviewer_zero,
            source_revision=self.database.get_gate_policy(gate_zero["policy_id"])[
                "head_revision"
            ],
            before="ceiling-one\n",
            after="ceiling-one\n",
            review_outcome="changes_requested",
        )
        self.assertTrue(
            self.director().advance(
                review_zero["id"], expected_version=review_zero["version"]
            )
        )
        correction = self.database.get_workflow_execution(execution["id"])
        review_one, gate_one, reviewer_one = self.drive_build_to_review(
            correction, before="ceiling-one\n", after="ceiling-two\n"
        )
        self.finish_run(
            reviewer_one,
            source_revision=self.database.get_gate_policy(gate_one["policy_id"])[
                "head_revision"
            ],
            before="ceiling-two\n",
            after="ceiling-two\n",
            review_outcome="changes_requested",
        )
        self.assertTrue(
            self.director().advance(
                review_one["id"], expected_version=review_one["version"]
            )
        )

        stopped = self.database.get_workflow_execution(execution["id"])
        self.assertEqual((stopped["stage"], stopped["correction_round"]), ("review", 1))
        self.assertEqual(len(stopped["rulings"]), 1)
        self.assertEqual(
            (stopped["rulings"][0]["kind"], stopped["rulings"][0]["state"]),
            ("correction_limit", "open"),
        )
        self.assertEqual(stopped["rulings"][0]["policy_id"], gate_one["policy_id"])
        self.assertTrue(
            self.director().advance(stopped["id"], expected_version=stopped["version"])
        )
        self.assertEqual(
            len(self.database.get_workflow_execution(execution["id"])["rulings"]), 1
        )
        self.assertIsNone(
            self.database.find_workflow_run(execution["id"], 2, "producer")
        )

    def test_approved_review_reuses_exact_evidence_and_runs_validation_once(
        self,
    ) -> None:
        execution = self.start_execution(
            configuration_name="validated",
            external_id="workflow-director-validation",
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": False,
                "validations": [
                    {
                        "name": "unit",
                        "image_id": "sha256:" + "9" * 64,
                        "command": ["python", "-m", "unittest"],
                    }
                ],
                "max_correction_rounds": 2,
                "landing_mode": "manual",
            },
        )
        review, gate, reviewer = self.drive_build_to_review(
            execution, before="base\n", after="validated\n"
        )
        completed = self.finish_run(
            reviewer,
            source_revision=self.database.get_gate_policy(gate["policy_id"])[
                "head_revision"
            ],
            before="validated\n",
            after="validated\n",
            review_outcome="approved",
        )
        verdict = self.database.get_typed_review_verdict(
            gate["policy_id"], reviewer["id"]
        )
        existing = self.control.record_review_evidence(
            gate["policy_id"],
            reviewer_attempt_id=verdict["reviewer_attempt_id"],
            outcome="passed",
            recorded_by_handle="operator",
            expected_policy_revision=self.database.get_gate_policy(gate["policy_id"])[
                "policy_revision"
            ],
        )
        requests: list[object] = []

        class Executor:
            @staticmethod
            def execute(request: object) -> SandboxResult:
                requests.append(request)
                return SandboxResult(
                    container_id="a" * 64,
                    container_name="validation",
                    image_id="sha256:" + "9" * 64,
                    exit_code=0,
                )

        validation = self.control.run_local_validation(
            gate["policy_id"],
            validation_name="unit",
            recorded_by_handle="operator",
            expected_policy_revision=self.database.get_gate_policy(gate["policy_id"])[
                "policy_revision"
            ],
            result_store=self.results,
            candidate_store=self.candidates,
            executor=Executor(),
        )

        class UnexpectedExecutor:
            @staticmethod
            def execute(_request: object) -> SandboxResult:
                raise AssertionError("durable validation evidence must be reused")

        director = WorkflowDirector(
            self.control, self.results, self.candidates, UnexpectedExecutor()
        )
        self.assertTrue(
            director.advance(review["id"], expected_version=review["version"])
        )

        evidence = self.database.list_gate_evidence(gate["policy_id"])
        reviews = [
            item for item in evidence if item["gate_name"] == "independent-review"
        ]
        validations = [
            item for item in evidence if item["gate_name"] == "validation:unit"
        ]
        self.assertEqual([item["id"] for item in reviews], [existing["id"]])
        self.assertEqual(len(validations), 1)
        self.assertEqual(validations[0]["outcome"], "passed")
        self.assertEqual(len(requests), 1)
        self.assertEqual(validations[0]["id"], validation["id"])
        self.assertEqual(completed["state"], "succeeded")
        landed = self.database.get_workflow_execution(execution["id"])
        self.assertEqual(landed["stage"], "landing")
        self.assert_transition_event(
            landed["events"][-1],
            kind="workflow.landing_ready",
            from_stage="review",
            to_stage="landing",
            from_round=0,
            to_round=0,
            metadata={
                "candidate_id": gate["candidate_id"],
                "policy_id": gate["policy_id"],
                "review_evidence_id": existing["id"],
                "head_revision": self.database.get_gate_policy(gate["policy_id"])[
                    "head_revision"
                ],
            },
        )

    def test_failed_validation_opens_a_durable_ruling(self) -> None:
        execution = self.start_execution(
            configuration_name="failing-validation",
            external_id="workflow-director-failing-validation",
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": False,
                "validations": [
                    {
                        "name": "unit",
                        "image_id": "sha256:" + "9" * 64,
                        "command": ["python", "-m", "unittest"],
                    }
                ],
                "max_correction_rounds": 2,
                "landing_mode": "manual",
            },
        )
        review, gate, reviewer = self.drive_build_to_review(
            execution, before="base\n", after="invalid\n"
        )
        self.finish_run(
            reviewer,
            source_revision=self.database.get_gate_policy(gate["policy_id"])[
                "head_revision"
            ],
            before="invalid\n",
            after="invalid\n",
            review_outcome="approved",
        )

        class Executor:
            @staticmethod
            def execute(_request: object) -> SandboxResult:
                return SandboxResult(
                    container_id="b" * 64,
                    container_name="validation",
                    image_id="sha256:" + "9" * 64,
                    exit_code=17,
                )

        director = WorkflowDirector(
            self.control, self.results, self.candidates, Executor()
        )
        self.assertTrue(
            director.advance(review["id"], expected_version=review["version"])
        )

        stopped = self.database.get_workflow_execution(execution["id"])
        self.assertEqual(stopped["stage"], "review")
        self.assertEqual(
            [
                (ruling["kind"], ruling["gate_name"], ruling["state"])
                for ruling in stopped["rulings"]
            ],
            [("validation_failed", "validation:unit", "open")],
        )
        self.assertEqual(stopped["rulings"][0]["policy_id"], gate["policy_id"])
        self.assertFalse(self.database.gate_readiness(gate["policy_id"])["ready"])


if __name__ == "__main__":
    unittest.main()
