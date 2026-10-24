from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from magic_glass_box.candidates import GitCandidateStore
from magic_glass_box.control_room import ControlRoom
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
    WorkLogEntry,
    canonical_json,
)
from magic_glass_box.executor import SandboxResult
from magic_glass_box.integration import LandingReceiptRequest
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceSidecarSpec,
)
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

    def drive_approved_to_landing(
        self, execution: dict[str, object]
    ) -> tuple[dict[str, object], dict[str, object]]:
        review, gate, reviewer = self.drive_build_to_review(
            execution, before="base\n", after="approved\n"
        )
        policy = self.database.get_gate_policy(gate["policy_id"])
        self.finish_run(
            reviewer,
            source_revision=policy["head_revision"],
            before="approved\n",
            after="approved\n",
            review_outcome="approved",
        )
        self.assertTrue(
            self.director().advance(review["id"], expected_version=review["version"])
        )
        current = self.database.get_workflow_execution(execution["id"])
        if current["stage"] == "maintainer":
            disposition = self.control.record_maintainer_review(
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
            self.assertEqual(disposition["decision"], "approved")
            self.assertTrue(
                self.director().advance(
                    current["id"], expected_version=current["version"]
                )
            )
            current = self.database.get_workflow_execution(execution["id"])
        self.assertEqual(current["stage"], "landing")
        return current, gate

    def issue_provider_workflow_landing(
        self, *, configuration_name: str, external_id: str
    ) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
        execution = self.start_execution(
            configuration_name=configuration_name,
            external_id=external_id,
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": False,
                "validations": [],
                "max_correction_rounds": 2,
                "landing_mode": "provider",
            },
        )
        landing, gate = self.drive_approved_to_landing(execution)
        self.assertTrue(
            self.director().advance(landing["id"], expected_version=landing["version"])
        )
        enqueued = self.database.get_workflow_execution(execution["id"])
        sidecar = ServiceSidecarSpec.from_mapping(
            {
                "schema_version": 1,
                "connector_protocol_version": 1,
                "deployment_sha256": "1" * 64,
                "transport": {
                    "kind": "unix",
                    "socket_path": str(self.root / "github.sock"),
                },
                "capabilities": [
                    {"name": "landing.invoke", "version": 1, "mode": "effect"}
                ],
                "mappings": [
                    {
                        "name": "primary",
                        "service": "github",
                        "host": "github.com",
                        "repository": "acme/widgets",
                        "account": "mgb-bot",
                        "credential_scope_sha256": "2" * 64,
                    }
                ],
                "request_bytes": 128 * 1024,
                "response_bytes": 512 * 1024,
                "timeout_ms": 30000,
                "max_concurrency": 2,
                "max_requests_per_minute": 60,
            }
        )
        self.database.register_service_sidecar(
            name="github-main", spec=sidecar, created_by=self.user["id"]
        )
        self.database.bind_project_capability(
            project_key="workflow-project",
            capability_name="landing.invoke",
            capability_version=1,
            sidecar_name="github-main",
            mapping_name="primary",
            bound_by=self.user["id"],
        )
        target = self.control.configure_workflow_provider_landing(
            enqueued["id"],
            expected_version=enqueued["version"],
            resource_id="42",
            merge_method="squash",
            configured_by_handle="operator",
        )
        self.assertEqual(
            target,
            {
                "execution_id": enqueued["id"],
                "queue_item_id": enqueued["landing"]["queue_item_id"],
                "resource_id": "42",
                "merge_method": "squash",
                "created_by": "operator",
                "created_at": target["created_at"],
            },
        )
        repeated = self.control.configure_workflow_provider_landing(
            enqueued["id"],
            expected_version=enqueued["version"],
            resource_id="42",
            merge_method="squash",
            configured_by_handle="operator",
        )
        self.assertEqual(repeated, target)
        with self.assertRaises(ConcurrentUpdateError):
            self.control.configure_workflow_provider_landing(
                enqueued["id"],
                expected_version=enqueued["version"] + 1,
                resource_id="42",
                merge_method="squash",
                configured_by_handle="operator",
            )
        with self.assertRaisesRegex(ConflictError, "another provider target"):
            self.control.configure_workflow_provider_landing(
                enqueued["id"],
                expected_version=enqueued["version"],
                resource_id="43",
                merge_method="squash",
                configured_by_handle="operator",
            )
        self.assertFalse(
            self.director().advance(
                enqueued["id"], expected_version=enqueued["version"]
            )
        )
        claimed = self.database.get_workflow_execution(enqueued["id"])
        self.assertEqual(claimed["landing"]["queue_state"], "claimed")
        self.assertFalse(
            self.director().advance(claimed["id"], expected_version=claimed["version"])
        )
        issued_execution = self.database.get_workflow_execution(claimed["id"])
        self.assertEqual(issued_execution["landing"]["queue_state"], "awaiting_receipt")
        self.assertEqual(
            issued_execution["landing"]["provider_target"],
            {
                "resource_id": target["resource_id"],
                "merge_method": target["merge_method"],
                "created_by": target["created_by"],
                "created_at": target["created_at"],
            },
        )
        issued = self.control.get_provider_landing(
            issued_execution["landing"]["provider_landing_id"]
        )
        return issued_execution, gate, issued

    def complete_provider_operation(
        self,
        issued: dict[str, object],
        *,
        outcome: str,
        observed_revision: str | None = None,
    ) -> None:
        operation = issued["operation"]
        dispatch = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="invoke",
            owner_id="replacement-provider",
            expected_version=operation["version"],
            lease_seconds=60,
        )
        request = ServiceOperationRequest.from_mapping(dispatch["request"])
        response = ServiceOperationResponse.from_mapping(
            {
                **request.correlation_dict(),
                "outcome": outcome,
                "provider_status": 200 if outcome == "succeeded" else 403,
                "request_bytes": 64,
                "response_bytes": 128,
                "error_code": None if outcome == "succeeded" else "github-denied",
                "retry_after_seconds": None,
                "receipt": (
                    {
                        "remote_identity": "github:github.com/acme/widgets/pull/42",
                        "observed_revision": observed_revision,
                        "evidence_sha256": "3" * 64,
                        "closed_references": ["github:github.com/acme/widgets/pull/42"],
                    }
                    if outcome == "succeeded"
                    else None
                ),
            }
        )
        self.database.complete_service_operation_attempt(
            dispatch["attempt"]["id"],
            owner_id="replacement-provider",
            expected_operation_version=dispatch["operation"]["version"],
            response=response,
        )

    def test_manual_landing_receipt_closes_the_exact_workflow_atomically(self) -> None:
        landing, gate = self.drive_approved_to_landing(self.execution)

        self.assertTrue(
            self.director().advance(landing["id"], expected_version=landing["version"])
        )
        enqueued = self.database.get_workflow_execution(landing["id"])
        self.assertEqual(enqueued["stage"], "landing")
        self.assertEqual(enqueued["landing"]["mode"], "manual")
        self.assertEqual(enqueued["landing"]["candidate_id"], gate["candidate_id"])
        self.assertEqual(enqueued["landing"]["policy_id"], gate["policy_id"])
        self.assertEqual(enqueued["landing"]["queue_state"], "queued")
        self.assert_transition_event(
            enqueued["events"][-1],
            kind="workflow.landing_enqueued",
            from_stage="landing",
            to_stage="landing",
            from_round=0,
            to_round=0,
            metadata={
                "candidate_id": gate["candidate_id"],
                "policy_id": gate["policy_id"],
                "queue_item_id": enqueued["landing"]["queue_item_id"],
            },
        )

        issued = self.control.issue_workflow_manual_landing(
            enqueued["id"],
            expected_version=enqueued["version"],
            remote="origin",
            issued_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidates,
        )
        self.assertIn("nonce", issued)
        self.assertEqual(issued["queue_item_id"], enqueued["landing"]["queue_item_id"])
        self.assertEqual(issued["state"], "pending")

        recorded = self.control.record_landing_receipt(
            LandingReceiptRequest.from_mapping(
                {
                    "schema_version": 1,
                    "plan_id": issued["id"],
                    "request_sha256": issued["content_sha256"],
                    "nonce": issued["nonce"],
                    "outcome": "landed",
                    "observed_revision": issued["plan"]["head_revision"],
                }
            ).to_dict(),
            recorded_by_handle="operator",
        )
        self.assertEqual(recorded["queue_item"]["state"], "landed")

        self.assertTrue(
            self.director().advance(
                enqueued["id"], expected_version=enqueued["version"]
            )
        )
        completion = self.database.get_workflow_execution(enqueued["id"])
        self.assertEqual(completion["stage"], "completion")
        self.assertEqual(completion["landing"]["receipt_id"], recorded["receipt"]["id"])
        self.assertEqual(completion["landing"]["receipt_kind"], "manual")
        self.assert_transition_event(
            completion["events"][-1],
            kind="workflow.landing_recorded",
            from_stage="landing",
            to_stage="completion",
            from_round=0,
            to_round=0,
            metadata={
                "candidate_id": gate["candidate_id"],
                "policy_id": gate["policy_id"],
                "queue_item_id": enqueued["landing"]["queue_item_id"],
                "receipt_id": recorded["receipt"]["id"],
            },
        )

        replacement = self.director()
        self.assertTrue(
            replacement.advance(
                completion["id"], expected_version=completion["version"]
            )
        )
        finished = self.database.get_workflow_execution(completion["id"])
        self.assertEqual(
            (finished["state"], finished["stage"]), ("completed", "completion")
        )
        self.assertEqual(
            finished["completion"],
            {
                "execution_id": finished["id"],
                "queue_item_id": enqueued["landing"]["queue_item_id"],
                "receipt_id": recorded["receipt"]["id"],
                "outcome_entry_id": finished["events"][-1]["metadata"][
                    "outcome_entry_id"
                ],
                "candidate_id": gate["candidate_id"],
                "policy_id": gate["policy_id"],
                "observed_revision": issued["plan"]["head_revision"],
                "completed_by": "operator",
                "created_at": finished["finished_at"],
            },
        )
        self.assertEqual(
            finished["completion"]["receipt_id"], recorded["receipt"]["id"]
        )
        self.assertEqual(finished["completion"]["candidate_id"], gate["candidate_id"])
        self.assertEqual(
            finished["completion"]["outcome_entry_id"],
            finished["events"][-1]["metadata"]["outcome_entry_id"],
        )
        self.assert_transition_event(
            finished["events"][-1],
            kind="workflow.completed",
            from_stage="completion",
            to_stage="completion",
            from_round=0,
            to_round=0,
            metadata={
                "candidate_id": gate["candidate_id"],
                "policy_id": gate["policy_id"],
                "queue_item_id": enqueued["landing"]["queue_item_id"],
                "receipt_id": recorded["receipt"]["id"],
                "outcome_entry_id": finished["completion"]["outcome_entry_id"],
            },
        )
        shown_item = self.database.get_work_item(self.work_item["id"])
        self.assertEqual(shown_item["state"], "completed")
        self.assertEqual(
            shown_item["work_thread"]["events"][-1],
            {
                "sequence": 2,
                "kind": "work_thread.completed",
                "from_state": "active",
                "to_state": "completed",
                "actor": "operator",
                "metadata": {
                    "outcome_entry_id": finished["completion"]["outcome_entry_id"],
                    "landing_receipt_id": recorded["receipt"]["id"],
                    "workflow_execution_id": finished["id"],
                },
                "created_at": finished["completion"]["created_at"],
            },
        )
        producer = self.database.find_workflow_run(finished["id"], 0, "producer")
        memory = self.database.get_work_memory_for_run(producer["id"])
        outcome = memory["entries"][-1]
        self.assertEqual(
            set(outcome),
            {
                "id",
                "thread_id",
                "sequence",
                "content_sha256",
                "assignment_id",
                "author",
                "created_at",
                "schema_version",
                "kind",
                "summary",
                "details",
                "artifacts",
                "related_interaction_ids",
                "candidate_revision",
            },
        )
        self.assertEqual(outcome["kind"], "outcome")
        self.assertEqual(outcome["candidate_revision"], issued["plan"]["head_revision"])
        self.assertEqual(
            outcome["author"],
            {
                "type": "control_plane",
                "actor": "operator",
                "workflow_execution_id": finished["id"],
            },
        )

    def test_landing_binding_and_transition_are_one_restart_safe_transaction(
        self,
    ) -> None:
        landing, _gate = self.drive_approved_to_landing(self.execution)
        with self.database.connect() as connection:
            connection.execute(
                "CREATE TRIGGER test_reject_landing_event "
                "BEFORE INSERT ON workflow_execution_events "
                "WHEN NEW.kind = 'workflow.landing_enqueued' BEGIN "
                "SELECT RAISE(ABORT, 'simulated event failure'); END"
            )
        with self.assertRaises(ConflictError):
            self.director().advance(landing["id"], expected_version=landing["version"])
        unchanged = self.database.get_workflow_execution(landing["id"])
        self.assertEqual(unchanged["version"], landing["version"])
        self.assertIsNone(unchanged["landing"])
        queued = self.control.list_integration_queue(project_key="workflow-project")
        self.assertEqual(len(queued), 1)

        with self.database.connect() as connection:
            connection.execute("DROP TRIGGER test_reject_landing_event")
        self.assertTrue(
            self.director().advance(landing["id"], expected_version=landing["version"])
        )
        recovered = self.database.get_workflow_execution(landing["id"])
        self.assertEqual(recovered["landing"]["queue_item_id"], queued[0]["id"])
        self.assertEqual(recovered["events"][-1]["kind"], "workflow.landing_enqueued")
        self.assertEqual(len(self.database.list_integration_queue()), 1)
        repeated = self.database.bind_workflow_landing(
            landing["id"],
            expected_version=landing["version"],
            correction_round=landing["correction_round"],
            candidate_id=recovered["landing"]["candidate_id"],
            policy_id=recovered["landing"]["initial_policy_id"],
            queue_item_id=recovered["landing"]["queue_item_id"],
        )
        self.assertEqual(
            repeated,
            {
                "execution_id": landing["id"],
                "correction_round": landing["correction_round"],
                "candidate_id": recovered["landing"]["candidate_id"],
                "policy_id": recovered["landing"]["initial_policy_id"],
                "queue_item_id": queued[0]["id"],
                "created_at": recovered["landing"]["created_at"],
            },
        )
        with self.assertRaises(ConcurrentUpdateError):
            self.database.bind_workflow_landing(
                landing["id"],
                expected_version=recovered["version"],
                correction_round=landing["correction_round"],
                candidate_id=recovered["landing"]["candidate_id"],
                policy_id=recovered["landing"]["initial_policy_id"],
                queue_item_id=recovered["landing"]["queue_item_id"],
            )

    def test_landing_binding_refuses_cancelled_queue_and_execution(self) -> None:
        landing, gate = self.drive_approved_to_landing(self.execution)
        queue = self.control.enqueue_integration(
            gate["policy_id"], created_by_handle="operator"
        )
        cancelled_queue = self.control.cancel_integration_item(
            queue["id"],
            expected_version=queue["version"],
            cancelled_by_handle="operator",
        )
        with self.assertRaisesRegex(ConflictError, "queue is cancelled"):
            self.database.bind_workflow_landing(
                landing["id"],
                expected_version=landing["version"],
                correction_round=landing["correction_round"],
                candidate_id=gate["candidate_id"],
                policy_id=gate["policy_id"],
                queue_item_id=cancelled_queue["id"],
            )
        self.assertIsNone(
            self.database.get_workflow_execution(landing["id"])["landing"]
        )

        other = self.start_execution(
            configuration_name="cancelled-before-binding",
            external_id="workflow-director-cancelled-before-binding",
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": False,
                "validations": [],
                "max_correction_rounds": 2,
                "landing_mode": "manual",
            },
        )
        other_landing, other_gate = self.drive_approved_to_landing(other)
        other_queue = self.control.enqueue_integration(
            other_gate["policy_id"], created_by_handle="operator"
        )
        cancelled_execution = self.control.cancel_workflow_execution(
            other_landing["id"],
            expected_version=other_landing["version"],
            cancelled_by_handle="operator",
            reason="Exercise the inactive-execution fence.",
        )
        with self.assertRaisesRegex(ConflictError, "not ready for landing"):
            self.database.bind_workflow_landing(
                cancelled_execution["id"],
                expected_version=cancelled_execution["version"],
                correction_round=cancelled_execution["correction_round"],
                candidate_id=other_gate["candidate_id"],
                policy_id=other_gate["policy_id"],
                queue_item_id=other_queue["id"],
            )
        self.assertIsNone(
            self.database.get_workflow_execution(cancelled_execution["id"])["landing"]
        )

    def test_control_room_names_manual_landing_actions_without_nonce_content(
        self,
    ) -> None:
        landing, _gate = self.drive_approved_to_landing(self.execution)
        self.assertTrue(
            self.director().advance(landing["id"], expected_version=landing["version"])
        )
        enqueued = self.database.get_workflow_execution(landing["id"])
        snapshot = ControlRoom(self.database).snapshot()
        projected = next(
            item for item in snapshot["workflows"] if item["id"] == landing["id"]
        )
        self.assertEqual(projected["landing_mode"], "manual")
        self.assertEqual(
            projected["landing_queue_item_id"], enqueued["landing"]["queue_item_id"]
        )
        self.assertEqual(projected["landing_queue_state"], "queued")
        self.assertIsNone(projected["landing_plan_id"])
        self.assertIn(
            "workflow.manual_plan_required",
            {
                item["kind"]
                for item in snapshot["attention"]
                if item.get("workflow_execution_id") == landing["id"]
            },
        )

        issued = self.control.issue_workflow_manual_landing(
            enqueued["id"],
            expected_version=enqueued["version"],
            remote="origin",
            issued_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidates,
        )
        self.assertIn("nonce", issued)
        awaiting = ControlRoom(self.database).snapshot()
        projected = next(
            item for item in awaiting["workflows"] if item["id"] == landing["id"]
        )
        self.assertEqual(projected["landing_plan_id"], issued["id"])
        self.assertEqual(projected["landing_plan_state"], "pending")
        encoded = canonical_json(awaiting)
        self.assertNotIn(issued["nonce"], encoded)
        self.assertIn(
            "workflow.manual_receipt_pending",
            {
                item["kind"]
                for item in awaiting["attention"]
                if item.get("workflow_execution_id") == landing["id"]
            },
        )
        self.control.record_landing_receipt(
            LandingReceiptRequest.from_mapping(
                {
                    "schema_version": 1,
                    "plan_id": issued["id"],
                    "request_sha256": issued["content_sha256"],
                    "nonce": issued["nonce"],
                    "outcome": "not_landed",
                    "observed_revision": issued["plan"]["expected_base_revision"],
                }
            ).to_dict(),
            recorded_by_handle="operator",
        )
        retry = ControlRoom(self.database).snapshot()
        projected = next(
            item for item in retry["workflows"] if item["id"] == landing["id"]
        )
        self.assertEqual(projected["landing_queue_state"], "queued")
        self.assertEqual(projected["landing_plan_state"], "not_landed")
        self.assertIn(
            "workflow.manual_retry_required",
            {
                item["kind"]
                for item in retry["attention"]
                if item.get("workflow_execution_id") == landing["id"]
            },
        )

    def test_completion_rolls_back_outcome_and_seal_until_work_is_quiescent(
        self,
    ) -> None:
        landing, _gate = self.drive_approved_to_landing(self.execution)
        self.assertTrue(
            self.director().advance(landing["id"], expected_version=landing["version"])
        )
        enqueued = self.database.get_workflow_execution(landing["id"])
        issued = self.control.issue_workflow_manual_landing(
            enqueued["id"],
            expected_version=enqueued["version"],
            remote="origin",
            issued_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidates,
        )
        self.control.record_landing_receipt(
            LandingReceiptRequest.from_mapping(
                {
                    "schema_version": 1,
                    "plan_id": issued["id"],
                    "request_sha256": issued["content_sha256"],
                    "nonce": issued["nonce"],
                    "outcome": "landed",
                    "observed_revision": issued["plan"]["head_revision"],
                }
            ).to_dict(),
            recorded_by_handle="operator",
        )
        self.assertTrue(
            self.director().advance(
                enqueued["id"], expected_version=enqueued["version"]
            )
        )
        completion = self.database.get_workflow_execution(enqueued["id"])
        coordinator = self.database.create_run(
            project_key="workflow-project",
            profile_name="Producer",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision=issued["plan"]["head_revision"],
        )
        with self.assertRaisesRegex(ConflictError, "active runs"):
            self.director().advance(
                completion["id"], expected_version=completion["version"]
            )
        unchanged = self.database.get_workflow_execution(completion["id"])
        self.assertEqual(
            (unchanged["state"], unchanged["stage"]), ("active", "completion")
        )
        self.assertIsNone(unchanged["completion"])
        memory = self.database.get_work_memory_for_run(coordinator["id"])
        self.assertEqual(memory["state"], "active")
        self.assertNotIn("outcome", {entry["kind"] for entry in memory["entries"]})

        preparing = self.database.begin_run_attempt(
            coordinator["id"],
            runtime_adapter="command",
            owner_id="closeout-assignment-author",
            expected_run_version=coordinator["version"],
        )
        attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id="closeout-assignment-author",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=issued["plan"]["head_revision"],
            container_id="e" * 64,
            container_name="closeout-assignment-author",
            image_id="sha256:" + "f" * 64,
        )
        memory = self.database.get_work_memory_for_run(coordinator["id"])
        assignment = self.control.create_work_assignment(
            coordinator["id"],
            target_profile_name="Reviewer",
            spec=WorkAssignmentSpec.from_mapping(
                {
                    "schema_version": 1,
                    "title": "Hold closeout",
                    "objective": "Prove unfinished delegated work fences completion.",
                    "scope_paths": ["feature.txt"],
                    "acceptance_checks": ["Observe atomic rollback."],
                    "base_revision": issued["plan"]["head_revision"],
                }
            ),
            expected_thread_version=memory["version"],
            idempotency_key="hold-workflow-closeout",
        )
        workspace = self.root / "closeout-assignment-workspace"
        workspace.mkdir()
        (workspace / "feature.txt").write_text("approved\n", encoding="utf-8")
        self.results.stage(attempt["id"], workspace, issued["plan"]["head_revision"])
        captured = self.results.capture(attempt["id"], workspace)
        self.database.record_attempt_result(attempt["id"], captured.manifest)
        self.database.complete_run_attempt(
            attempt["id"],
            owner_id="closeout-assignment-author",
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id=attempt["image_id"],
        )

        with self.assertRaisesRegex(ConflictError, "unfinished assignments"):
            self.director().advance(
                completion["id"], expected_version=completion["version"]
            )
        unchanged = self.database.get_workflow_execution(completion["id"])
        self.assertEqual(
            (unchanged["state"], unchanged["stage"]), ("active", "completion")
        )
        self.assertIsNone(unchanged["completion"])
        memory = self.database.get_work_memory_for_run(coordinator["id"])
        self.assertEqual(memory["state"], "active")
        self.assertEqual(memory["assignments"][0]["id"], assignment["id"])
        self.assertNotIn("outcome", {entry["kind"] for entry in memory["entries"]})

    def test_provider_target_is_invoked_settled_and_closed_by_replacements(
        self,
    ) -> None:
        issued_execution, gate, issued = self.issue_provider_workflow_landing(
            configuration_name="provider-landing",
            external_id="workflow-director-provider-landing",
        )
        self.assertEqual(issued["operation"]["spec"]["target"]["resource_id"], "42")
        self.assertEqual(
            issued["operation"]["spec"]["target"]["desired_revision"],
            self.database.get_gate_policy(gate["policy_id"])["head_revision"],
        )

        self.complete_provider_operation(
            issued, outcome="succeeded", observed_revision="f" * 40
        )

        self.assertFalse(
            self.director().advance(
                issued_execution["id"], expected_version=issued_execution["version"]
            )
        )
        landed = self.database.get_workflow_execution(issued_execution["id"])
        self.assertEqual(landed["landing"]["queue_state"], "landed")
        self.assertEqual(landed["landing"]["receipt_kind"], "provider")
        self.assertEqual(landed["landing"]["receipt_observed_revision"], "f" * 40)
        self.assertTrue(
            self.director().advance(landed["id"], expected_version=landed["version"])
        )
        completion = self.database.get_workflow_execution(landed["id"])
        self.assertEqual(completion["stage"], "completion")
        self.assertTrue(
            self.director().advance(
                completion["id"], expected_version=completion["version"]
            )
        )
        finished = self.database.get_workflow_execution(completion["id"])
        self.assertEqual(finished["state"], "completed")
        self.assertEqual(finished["completion"]["observed_revision"], "f" * 40)

    def test_provider_failure_requires_explicit_ruling_for_each_retry(self) -> None:
        execution, _gate, issued = self.issue_provider_workflow_landing(
            configuration_name="provider-failure",
            external_id="workflow-director-provider-failure",
        )
        first_landing_id = issued["provider_landing"]["id"]
        self.complete_provider_operation(issued, outcome="denied")

        self.assertFalse(
            self.director().advance(
                execution["id"], expected_version=execution["version"]
            )
        )
        failed = self.database.get_workflow_execution(execution["id"])
        self.assertEqual(failed["landing"]["queue_state"], "queued")
        self.assertEqual(failed["landing"]["provider_landing_state"], "failed")
        self.assertTrue(
            self.director().advance(failed["id"], expected_version=failed["version"])
        )
        ruled = self.database.get_workflow_execution(failed["id"])
        ruling = ruled["rulings"][-1]
        self.assertEqual(ruling["kind"], "landing_failed")
        self.assertEqual(ruling["provider_landing_id"], first_landing_id)
        self.assertEqual(ruling["state"], "open")
        self.assertEqual(ruled["landing"]["provider_landing_id"], first_landing_id)

        resolved = self.control.resolve_workflow_ruling(
            ruling["id"],
            expected_version=ruling["version"],
            decision="continue",
            resolved_by_handle="operator",
        )
        self.assertEqual(resolved["decision"], "continue")
        retry = self.database.get_workflow_execution(ruled["id"])
        self.assertEqual((retry["stage"], retry["correction_round"]), ("landing", 0))
        self.assertFalse(
            self.director().advance(retry["id"], expected_version=retry["version"])
        )
        claimed = self.database.get_workflow_execution(retry["id"])
        with self.assertRaisesRegex(ValidationError, "supplied together"):
            self.control.issue_provider_landing(
                claimed["landing"]["queue_item_id"],
                owner_id=f"workflow-{claimed['id']}",
                expected_version=claimed["landing"]["queue_version"],
                resource_id="42",
                merge_method="squash",
                issued_by_handle="operator",
                workflow_execution_id=claimed["id"],
            )
        with self.assertRaisesRegex(ConflictError, "workflow target"):
            self.control.issue_provider_landing(
                claimed["landing"]["queue_item_id"],
                owner_id=f"workflow-{claimed['id']}",
                expected_version=claimed["landing"]["queue_version"],
                resource_id="43",
                merge_method="squash",
                issued_by_handle="operator",
                workflow_execution_id=claimed["id"],
                workflow_execution_version=claimed["version"],
            )
        with self.assertRaisesRegex(ConflictError, "workflow target"):
            self.control.issue_provider_landing(
                claimed["landing"]["queue_item_id"],
                owner_id=f"workflow-{claimed['id']}",
                expected_version=claimed["landing"]["queue_version"],
                resource_id="42",
                merge_method="merge",
                issued_by_handle="operator",
                workflow_execution_id=claimed["id"],
                workflow_execution_version=claimed["version"],
            )
        with self.assertRaises(ConcurrentUpdateError):
            self.control.issue_provider_landing(
                claimed["landing"]["queue_item_id"],
                owner_id=f"workflow-{claimed['id']}",
                expected_version=claimed["landing"]["queue_version"],
                resource_id="42",
                merge_method="squash",
                issued_by_handle="operator",
                workflow_execution_id=claimed["id"],
                workflow_execution_version=ruled["version"],
            )
        self.assertFalse(
            self.director().advance(claimed["id"], expected_version=claimed["version"])
        )
        reissued = self.database.get_workflow_execution(retry["id"])
        second_landing_id = reissued["landing"]["provider_landing_id"]
        self.assertNotEqual(second_landing_id, first_landing_id)

        second = self.control.get_provider_landing(second_landing_id)
        self.complete_provider_operation(second, outcome="denied")
        self.assertFalse(
            self.director().advance(
                reissued["id"], expected_version=reissued["version"]
            )
        )
        failed_again = self.database.get_workflow_execution(reissued["id"])
        self.assertTrue(
            self.director().advance(
                failed_again["id"], expected_version=failed_again["version"]
            )
        )
        reruled = self.database.get_workflow_execution(reissued["id"])
        self.assertEqual(len(reruled["rulings"]), 2)
        self.assertEqual(
            reruled["rulings"][-1]["provider_landing_id"], second_landing_id
        )
        self.assertEqual(reruled["rulings"][-1]["state"], "open")

    def test_provider_landing_waits_without_consuming_retries_when_dependency_blocks(
        self,
    ) -> None:
        execution = self.start_execution(
            configuration_name="provider-dependency",
            external_id="workflow-director-provider-dependency",
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": False,
                "validations": [],
                "max_correction_rounds": 2,
                "landing_mode": "provider",
            },
        )
        prerequisite = self.database.import_work_item(
            project_key="workflow-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "workflow-prerequisite",
                    "source_updated_at": "2026-10-23T14:00:00Z",
                    "title": "Land the prerequisite first",
                    "brief": "This item deliberately remains unlanded.",
                }
            ),
            imported_by=self.user["id"],
        )
        dependency = self.control.add_landing_dependency(
            execution["work_item_id"],
            prerequisite["id"],
            created_by_handle="operator",
        )
        landing, _gate = self.drive_approved_to_landing(execution)
        self.assertTrue(
            self.director().advance(landing["id"], expected_version=landing["version"])
        )
        enqueued = self.database.get_workflow_execution(execution["id"])
        self.control.configure_workflow_provider_landing(
            enqueued["id"],
            expected_version=enqueued["version"],
            resource_id="42",
            merge_method="squash",
            configured_by_handle="operator",
        )

        self.assertFalse(
            self.director().advance(
                enqueued["id"], expected_version=enqueued["version"]
            )
        )
        waiting = self.database.get_workflow_execution(enqueued["id"])
        self.assertEqual(waiting["landing"]["blocked_dependency_count"], 1)
        self.assertEqual(waiting["landing"]["queue_state"], "queued")
        self.assertIsNone(waiting["landing"]["owner_id"])
        self.assertEqual(waiting["rulings"], [])

        self.control.remove_landing_dependency(
            dependency["id"],
            expected_version=dependency["version"],
            removed_by_handle="operator",
        )
        self.assertFalse(
            self.director().advance(waiting["id"], expected_version=waiting["version"])
        )
        unavailable = self.database.get_workflow_execution(waiting["id"])
        self.assertEqual(unavailable["landing"]["blocked_dependency_count"], 0)
        self.assertFalse(unavailable["landing"]["provider_available"])
        self.assertEqual(unavailable["landing"]["queue_state"], "queued")
        snapshot = ControlRoom(self.database).snapshot()
        self.assertIn(
            "workflow.provider_capability_required",
            {
                item["kind"]
                for item in snapshot["attention"]
                if item.get("workflow_execution_id") == waiting["id"]
            },
        )

    def test_landing_mode_never_falls_back_to_an_incompatible_authority(self) -> None:
        provider = self.start_execution(
            configuration_name="provider-no-fallback",
            external_id="workflow-director-provider-no-fallback",
            spec={
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": False,
                "validations": [],
                "max_correction_rounds": 2,
                "landing_mode": "provider",
            },
        )
        landing, _gate = self.drive_approved_to_landing(provider)
        self.assertTrue(
            self.director().advance(landing["id"], expected_version=landing["version"])
        )
        enqueued = self.database.get_workflow_execution(provider["id"])
        with self.assertRaisesRegex(ConflictError, "provider landing"):
            self.control.issue_workflow_manual_landing(
                enqueued["id"],
                expected_version=enqueued["version"],
                remote="origin",
                issued_by_handle="operator",
                result_store=self.results,
                candidate_store=self.candidates,
            )
        queue = self.control.claim_integration_item(
            enqueued["landing"]["queue_item_id"],
            owner_id="generic-operator",
            expected_version=enqueued["landing"]["queue_version"],
            lease_seconds=60,
        )
        with self.assertRaisesRegex(ValidationError, "supplied together"):
            self.control.issue_landing_plan(
                queue["id"],
                owner_id="generic-operator",
                expected_version=queue["version"],
                remote="origin",
                issued_by_handle="operator",
                result_store=self.results,
                candidate_store=self.candidates,
                workflow_execution_id=enqueued["id"],
            )
        with self.assertRaisesRegex(ConflictError, "provider landing"):
            self.control.issue_landing_plan(
                queue["id"],
                owner_id="generic-operator",
                expected_version=queue["version"],
                remote="origin",
                issued_by_handle="operator",
                result_store=self.results,
                candidate_store=self.candidates,
                workflow_execution_id=enqueued["id"],
                workflow_execution_version=enqueued["version"],
            )
        issued = self.control.issue_landing_plan(
            queue["id"],
            owner_id="generic-operator",
            expected_version=queue["version"],
            remote="origin",
            issued_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidates,
        )
        self.assertTrue(
            self.director().advance(
                enqueued["id"], expected_version=enqueued["version"]
            )
        )
        ruled = self.database.get_workflow_execution(enqueued["id"])
        ruling = ruled["rulings"][-1]
        self.assertEqual(ruling["kind"], "landing_authority_mismatch")
        self.assertEqual(ruling["landing_plan_id"], issued["id"])
        self.assertIsNone(ruling["provider_landing_id"])
        with self.assertRaisesRegex(ConflictError, "continue the workflow"):
            self.control.resolve_workflow_ruling(
                ruling["id"],
                expected_version=ruling["version"],
                decision="continue",
                resolved_by_handle="operator",
            )

        manual_landing, _gate = self.drive_approved_to_landing(self.execution)
        self.assertTrue(
            self.director().advance(
                manual_landing["id"], expected_version=manual_landing["version"]
            )
        )
        manual = self.database.get_workflow_execution(self.execution["id"])
        with self.assertRaisesRegex(ConflictError, "manual landing"):
            self.control.configure_workflow_provider_landing(
                manual["id"],
                expected_version=manual["version"],
                resource_id="42",
                merge_method="squash",
                configured_by_handle="operator",
            )
        with self.assertRaises(ConcurrentUpdateError):
            self.control.issue_workflow_manual_landing(
                manual["id"],
                expected_version=manual["version"] + 1,
                remote="origin",
                issued_by_handle="operator",
                result_store=self.results,
                candidate_store=self.candidates,
            )

    def test_manual_landing_issuance_rejects_cancelled_workflow_at_landing_stage(
        self,
    ) -> None:
        manual_landing, _gate = self.drive_approved_to_landing(self.execution)
        self.assertTrue(
            self.director().advance(
                manual_landing["id"], expected_version=manual_landing["version"]
            )
        )
        manual = self.database.get_workflow_execution(self.execution["id"])
        self.control.issue_workflow_manual_landing(
            manual["id"],
            expected_version=manual["version"],
            remote="origin",
            issued_by_handle="operator",
            result_store=self.results,
            candidate_store=self.candidates,
        )
        cancelled_manual = self.control.cancel_workflow_execution(
            manual["id"],
            expected_version=manual["version"],
            cancelled_by_handle="operator",
            reason="Exercise the landing issuance fence.",
        )
        manual_queue = self.database.get_integration_queue_item(
            cancelled_manual["landing"]["queue_item_id"]
        )
        with self.assertRaisesRegex(ConflictError, "workflow is not awaiting landing"):
            self.control.issue_landing_plan(
                manual_queue["id"],
                owner_id=f"workflow-{cancelled_manual['id']}",
                expected_version=manual_queue["version"],
                remote="origin",
                issued_by_handle="operator",
                result_store=self.results,
                candidate_store=self.candidates,
                workflow_execution_id=cancelled_manual["id"],
                workflow_execution_version=cancelled_manual["version"],
            )

        oversized_execution_id = "x" * 65
        with self.assertRaises(ValidationError):
            self.database.issue_landing_plan(
                "queue",
                owner_id="owner",
                expected_version=1,
                candidate_repository=self.repository,
                remote="origin",
                issued_by=self.user["id"],
                workflow_execution_id=oversized_execution_id,
                workflow_execution_version=1,
            )

    def test_provider_landing_issuance_rejects_cancelled_workflow_at_landing_stage(
        self,
    ) -> None:
        provider, _provider_gate, _issued = self.issue_provider_workflow_landing(
            configuration_name="provider-cancelled-issuance",
            external_id="workflow-director-provider-cancelled-issuance",
        )
        cancelled_provider = self.control.cancel_workflow_execution(
            provider["id"],
            expected_version=provider["version"],
            cancelled_by_handle="operator",
            reason="Exercise the provider issuance fence.",
        )
        provider_queue = self.database.get_integration_queue_item(
            cancelled_provider["landing"]["queue_item_id"]
        )
        with self.assertRaisesRegex(ConflictError, "workflow is not awaiting landing"):
            self.control.issue_provider_landing(
                provider_queue["id"],
                owner_id=f"workflow-{cancelled_provider['id']}",
                expected_version=provider_queue["version"],
                resource_id="42",
                merge_method="squash",
                issued_by_handle="operator",
                workflow_execution_id=cancelled_provider["id"],
                workflow_execution_version=cancelled_provider["version"],
            )
        oversized_execution_id = "x" * 65
        with self.assertRaises(ValidationError):
            self.database.issue_provider_landing(
                "queue",
                owner_id="owner",
                expected_version=1,
                resource_id="42",
                parameters={},
                issued_by=self.user["id"],
                workflow_execution_id=oversized_execution_id,
                workflow_execution_version=1,
            )

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

        self.assertEqual(
            self.database.get_supervisor_action(maintainer_action["id"])["state"],
            "succeeded",
        )
        landing = self.database.get_workflow_execution(self.execution["id"])
        self.assertEqual(landing["stage"], "landing")
        landing_action = self.database.enqueue_supervisor_actions()[0]
        supervisor.tick(wait=True)
        enqueued = self.database.get_workflow_execution(self.execution["id"])
        self.assertEqual(enqueued["landing"]["queue_state"], "queued")
        waiting_action = self.database.enqueue_supervisor_actions()[0]
        supervisor.tick(wait=True)

        actions = self.database.list_supervisor_actions()
        self.assertEqual(len(actions), 5)
        self.assertEqual(
            {(action["subject_version"], action["state"]) for action in actions},
            {
                (1, "succeeded"),
                (review["version"], "succeeded"),
                (maintainer["version"], "succeeded"),
                (landing["version"], "succeeded"),
                (enqueued["version"], "pending"),
            },
        )
        self.assertEqual(
            self.database.get_supervisor_action(landing_action["id"])["state"],
            "succeeded",
        )
        self.assertEqual(
            self.database.get_supervisor_action(waiting_action["id"])["state"],
            "pending",
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
