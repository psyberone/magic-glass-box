from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .candidates import GitCandidateStore
from .domain import SupervisionPolicy
from .executor import DockerExecutor
from .results import WorkspaceResultStore
from .service import ControlPlane


@dataclass(frozen=True)
class _PinnedImage:
    image_id: str
    declared_volumes: tuple[str, ...] = ()


WORKFLOW_RUN_SUPERVISION = SupervisionPolicy.from_mapping(
    {
        "schema_version": 1,
        "auto_execute": True,
        "retry_backoff_seconds": [5, 30, 300],
        "interaction_escalation_seconds": [1800, 7200],
        "attempt_idle_escalation_seconds": [1800, 7200],
        "attempt_lease_expired_escalation_seconds": [0, 900],
        "other_attention_escalation_seconds": [1800, 7200],
    }
)


class WorkflowDirector:
    """Advance the fixed implementation prescription from durable evidence."""

    def __init__(
        self,
        control: ControlPlane,
        result_store: WorkspaceResultStore,
        candidate_store: GitCandidateStore,
        validation_executor: DockerExecutor,
    ) -> None:
        self.control = control
        self.database = control.database
        self.result_store = result_store
        self.candidate_store = candidate_store
        self.validation_executor = validation_executor

    def advance(self, execution_id: str, *, expected_version: int) -> bool:
        execution = self.database.get_workflow_execution(execution_id)
        if execution["state"] != "active" or execution["version"] != expected_version:
            return True
        if any(ruling["state"] == "open" for ruling in execution["rulings"]):
            return True
        if execution["stage"] == "build":
            return self._advance_build(execution)
        if execution["stage"] == "review":
            return self._advance_review(execution)
        if execution["stage"] == "maintainer":
            return self._advance_maintainer(execution)
        if execution["stage"] == "landing":
            return self._advance_landing(execution)
        if execution["stage"] == "completion":
            self.database.complete_workflow_execution(
                execution["id"], expected_version=execution["version"]
            )
            return True
        return True

    def _advance_build(self, execution: dict[str, Any]) -> bool:
        correction_round = execution["correction_round"]
        producer = self.database.find_workflow_run(
            execution["id"], correction_round, "producer"
        )
        if producer is None:
            self._create_producer(execution)
            return False
        if self._run_is_active(producer):
            return False
        if producer["state"] != "succeeded":
            return self._ruling(execution, "producer_failed", run_id=producer["id"])

        candidate = self._candidate_for_run(producer)
        if candidate is None:
            attempt = self._successful_attempt(producer)
            self.control.create_candidate(
                attempt["id"],
                created_by_handle=execution["requested_by"],
                result_store=self.result_store,
                candidate_store=self.candidate_store,
            )
            return False

        gate = self.database.get_workflow_gate(execution["id"], correction_round)
        if gate is None:
            policy = self._create_gate_policy(execution, candidate["id"])
            self.database.bind_workflow_gate(
                execution["id"],
                correction_round=correction_round,
                candidate_id=candidate["id"],
                policy_id=policy["id"],
            )
            return False

        reviewer = self.database.find_workflow_run(
            execution["id"], correction_round, "reviewer"
        )
        if reviewer is None:
            self._create_reviewer(execution, gate["policy_id"])
        self.database.advance_workflow_execution(
            execution["id"],
            expected_version=execution["version"],
            expected_stage="build",
            expected_correction_round=correction_round,
            target_stage="review",
            target_correction_round=correction_round,
            event_kind="workflow.review_started",
            metadata={
                "producer_run_id": producer["id"],
                "candidate_id": gate["candidate_id"],
                "policy_id": gate["policy_id"],
                "reviewer_run_id": (
                    reviewer["id"]
                    if reviewer is not None
                    else self.database.find_workflow_run(
                        execution["id"], correction_round, "reviewer"
                    )["id"]
                ),
            },
        )
        return True

    def _advance_review(self, execution: dict[str, Any]) -> bool:
        correction_round = execution["correction_round"]
        gate = self._required_gate(execution)
        reviewer = self.database.find_workflow_run(
            execution["id"], correction_round, "reviewer"
        )
        if reviewer is None:
            raise RuntimeError("workflow review stage has no reviewer run")
        if self._run_is_active(reviewer):
            return False
        if reviewer["state"] != "succeeded":
            return self._ruling(
                execution,
                "reviewer_failed",
                policy_id=gate["policy_id"],
                run_id=reviewer["id"],
            )
        verdict = self.database.get_typed_review_verdict(
            gate["policy_id"], reviewer["id"]
        )
        if verdict is None:
            return self._ruling(
                execution,
                "review_verdict_missing",
                policy_id=gate["policy_id"],
                run_id=reviewer["id"],
            )
        evidence = self.control.record_review_evidence(
            gate["policy_id"],
            reviewer_attempt_id=verdict["reviewer_attempt_id"],
            outcome=("passed" if verdict["outcome"] == "approved" else "failed"),
            recorded_by_handle=execution["requested_by"],
            expected_policy_revision=self.database.get_gate_policy(gate["policy_id"])[
                "policy_revision"
            ],
        )
        if verdict["outcome"] == "changes_requested":
            return self._request_correction(
                execution,
                policy_id=gate["policy_id"],
                evidence_id=evidence["id"],
                review_entry_id=verdict["work_log_entry_id"],
                source="independent_review",
            )

        evidence_by_name: dict[str, dict[str, Any]] = {}
        for item in self.database.list_gate_evidence(gate["policy_id"]):
            # Gate evidence is newest first. Preserve the first record so a
            # later retry cannot be shadowed by stale evidence for that gate.
            evidence_by_name.setdefault(item["gate_name"], item)
        for validation in execution["spec"]["validations"]:
            gate_name = "validation:" + validation["name"]
            recorded = evidence_by_name.get(gate_name)
            if recorded is None or self._validation_retry_authorized(
                execution,
                policy_id=gate["policy_id"],
                gate_name=gate_name,
                evidence_id=recorded["id"],
            ):
                recorded = self.control.run_local_validation(
                    gate["policy_id"],
                    validation_name=validation["name"],
                    recorded_by_handle=execution["requested_by"],
                    expected_policy_revision=self.database.get_gate_policy(
                        gate["policy_id"]
                    )["policy_revision"],
                    result_store=self.result_store,
                    candidate_store=self.candidate_store,
                    executor=self.validation_executor,
                )
            if recorded["outcome"] != "passed":
                return self._ruling(
                    execution,
                    "validation_failed",
                    policy_id=gate["policy_id"],
                    gate_name=gate_name,
                )

        target_stage = (
            "maintainer" if execution["spec"]["maintainer_approval"] else "landing"
        )
        readiness = self.database.gate_readiness(gate["policy_id"])
        if target_stage == "landing" and not readiness["ready"]:
            raise RuntimeError("workflow gate readiness contradicts recorded evidence")
        self.database.advance_workflow_execution(
            execution["id"],
            expected_version=execution["version"],
            expected_stage="review",
            expected_correction_round=correction_round,
            target_stage=target_stage,
            target_correction_round=correction_round,
            event_kind=(
                "workflow.maintainer_pending"
                if target_stage == "maintainer"
                else "workflow.landing_ready"
            ),
            metadata={
                "candidate_id": gate["candidate_id"],
                "policy_id": gate["policy_id"],
                "review_evidence_id": evidence["id"],
                "head_revision": readiness["head_revision"],
            },
        )
        return True

    @staticmethod
    def _validation_retry_authorized(
        execution: dict[str, Any],
        *,
        policy_id: str,
        gate_name: str,
        evidence_id: str,
    ) -> bool:
        ruling = next(
            (
                item
                for item in reversed(execution["rulings"])
                if item["kind"] == "validation_failed"
                and item["policy_id"] == policy_id
                and item["gate_name"] == gate_name
            ),
            None,
        )
        if (
            ruling is None
            or ruling["state"] != "resolved"
            or ruling["decision"] != "continue"
            or ruling["execution_version"] + 1 != execution["version"]
        ):
            return False
        resolution = next(
            (
                event
                for event in reversed(execution["events"])
                if event["kind"] == "workflow.ruling_resolved"
                and event["metadata"]["ruling_id"] == ruling["id"]
            ),
            None,
        )
        return (
            resolution is not None
            and resolution["metadata"].get("decision") == "continue"
            and resolution["metadata"].get("failed_evidence_id") == evidence_id
        )

    def _advance_maintainer(self, execution: dict[str, Any]) -> bool:
        gate = self._required_gate(execution)
        reviews = self.database.list_maintainer_reviews(gate["policy_id"])
        if not reviews:
            return False
        review = reviews[0]
        if review["decision"] == "changes_requested":
            return self._request_correction(
                execution,
                policy_id=gate["policy_id"],
                maintainer_review_id=review["id"],
                source="maintainer_review",
            )
        readiness = self.database.gate_readiness(gate["policy_id"])
        if not readiness["ready"]:
            raise RuntimeError("maintainer approval did not make the exact gate ready")
        self.database.advance_workflow_execution(
            execution["id"],
            expected_version=execution["version"],
            expected_stage="maintainer",
            expected_correction_round=execution["correction_round"],
            target_stage="landing",
            target_correction_round=execution["correction_round"],
            event_kind="workflow.landing_ready",
            metadata={
                "candidate_id": gate["candidate_id"],
                "policy_id": gate["policy_id"],
                "maintainer_review_id": review["id"],
                "head_revision": readiness["head_revision"],
            },
        )
        return True

    def _advance_landing(self, execution: dict[str, Any]) -> bool:
        gate = self._required_gate(execution)
        landing = execution["landing"]
        if landing is None:
            readiness = self.database.gate_readiness(gate["policy_id"])
            if not readiness["ready"]:
                raise RuntimeError("workflow landing gate is not ready")
            queue = self.control.enqueue_integration(
                gate["policy_id"], created_by_handle=execution["requested_by"]
            )
            self.database.bind_workflow_landing(
                execution["id"],
                expected_version=execution["version"],
                correction_round=execution["correction_round"],
                candidate_id=gate["candidate_id"],
                policy_id=gate["policy_id"],
                queue_item_id=queue["id"],
            )
            return True

        mode = execution["spec"]["landing_mode"]
        if mode == "provider" and landing["plan_id"] is not None:
            return self._ruling(
                execution,
                "landing_authority_mismatch",
                policy_id=landing["policy_id"],
                landing_plan_id=landing["plan_id"],
            )
        if (
            mode == "provider"
            and landing["provider_landing_id"] is not None
            and landing["provider_target"] is None
        ):
            return self._ruling(
                execution,
                "landing_authority_mismatch",
                policy_id=landing["policy_id"],
                provider_landing_id=landing["provider_landing_id"],
            )
        if mode == "manual" and landing["provider_landing_id"] is not None:
            return self._ruling(
                execution,
                "landing_authority_mismatch",
                policy_id=landing["policy_id"],
                provider_landing_id=landing["provider_landing_id"],
            )
        if landing["queue_state"] == "cancelled":
            return self._ruling(
                execution, "landing_cancelled", policy_id=landing["policy_id"]
            )

        if landing["queue_state"] != "landed":
            if mode == "provider":
                return self._advance_provider_landing(execution, landing)
            return False
        if landing["receipt_outcome"] != "landed":
            raise RuntimeError("workflow landed queue has no landed receipt")
        if landing["receipt_kind"] != mode:
            return self._ruling(
                execution,
                "landing_authority_mismatch",
                policy_id=landing["policy_id"],
                provider_landing_id=(
                    landing["provider_landing_id"]
                    if landing["receipt_kind"] == "provider"
                    else None
                ),
                landing_plan_id=(
                    landing["plan_id"] if landing["receipt_kind"] == "manual" else None
                ),
            )
        if mode == "provider":
            issued = self.control.get_provider_landing(landing["provider_landing_id"])
            operation = issued["operation"]
            target = landing["provider_target"]
            if (
                operation["spec"]["target"]["resource_id"] != target["resource_id"]
                or operation["spec"]["parameters"]["merge_method"]
                != target["merge_method"]
            ):
                return self._ruling(
                    execution,
                    "landing_authority_mismatch",
                    policy_id=landing["policy_id"],
                    provider_landing_id=landing["provider_landing_id"],
                )
        self.database.advance_workflow_execution(
            execution["id"],
            expected_version=execution["version"],
            expected_stage="landing",
            expected_correction_round=execution["correction_round"],
            target_stage="completion",
            target_correction_round=execution["correction_round"],
            event_kind="workflow.landing_recorded",
            metadata={
                "candidate_id": landing["candidate_id"],
                "policy_id": landing["policy_id"],
                "queue_item_id": landing["queue_item_id"],
                "receipt_id": landing["receipt_id"],
            },
        )
        return True

    def _advance_provider_landing(
        self, execution: dict[str, Any], landing: dict[str, Any]
    ) -> bool:
        target = landing["provider_target"]
        if target is None or landing["queue_state"] == "refold_required":
            return False
        if landing["provider_landing_state"] in {"failed", "not_landed"} and not any(
            ruling["kind"] == "landing_failed"
            and ruling["state"] == "resolved"
            and ruling["decision"] == "continue"
            and ruling["provider_landing_id"] == landing["provider_landing_id"]
            and ruling["execution_version"] + 1 == execution["version"]
            for ruling in execution["rulings"]
        ):
            return self._ruling(
                execution,
                "landing_failed",
                policy_id=landing["policy_id"],
                provider_landing_id=landing["provider_landing_id"],
            )
        if landing["blocked_dependency_count"] or not landing["gates_ready"]:
            return False
        if not landing["provider_available"]:
            return False
        owner_id = f"workflow-{execution['id']}"
        if landing["queue_state"] == "queued":
            self.control.claim_integration_item(
                landing["queue_item_id"],
                owner_id=owner_id,
                expected_version=landing["queue_version"],
                lease_seconds=300,
            )
            return False
        if landing["queue_state"] == "claimed":
            if landing["owner_id"] != owner_id:
                return False
            if landing["lease_expires_at"] <= self.database.now():
                self.control.claim_integration_item(
                    landing["queue_item_id"],
                    owner_id=owner_id,
                    expected_version=landing["queue_version"],
                    lease_seconds=300,
                )
                return False
            self.control.issue_provider_landing(
                landing["queue_item_id"],
                owner_id=owner_id,
                expected_version=landing["queue_version"],
                resource_id=target["resource_id"],
                merge_method=target["merge_method"],
                issued_by_handle=execution["requested_by"],
                workflow_execution_id=execution["id"],
                workflow_execution_version=execution["version"],
            )
            return False
        if landing["queue_state"] != "awaiting_receipt":
            raise RuntimeError("workflow integration queue state is invalid")
        if landing["provider_landing_id"] is None:
            raise RuntimeError("provider workflow has no provider landing authority")
        issued = self.control.get_provider_landing(landing["provider_landing_id"])
        operation = issued["operation"]
        if (
            operation["spec"]["target"]["resource_id"] != target["resource_id"]
            or operation["spec"]["parameters"]["merge_method"] != target["merge_method"]
        ):
            raise RuntimeError("provider landing does not match workflow target")
        self.control.execute_provider_landing(
            landing["provider_landing_id"],
            owner_id=owner_id,
            settled_by_handle=execution["requested_by"],
        )
        return False

    def _create_producer(self, execution: dict[str, Any]) -> dict[str, Any]:
        correction_round = execution["correction_round"]
        kwargs: dict[str, Any] = {}
        source_revision = execution["source_revision"]
        if correction_round > 0:
            previous = self.database.get_workflow_gate(
                execution["id"], correction_round - 1
            )
            if previous is None:
                raise RuntimeError("workflow correction has no previous gate")
            policy = self.database.get_gate_policy(previous["policy_id"])
            source_revision = policy["head_revision"]
            maintainer_reviews = self.database.list_maintainer_reviews(
                previous["policy_id"]
            )
            if (
                maintainer_reviews
                and maintainer_reviews[0]["decision"] == "changes_requested"
            ):
                kwargs["change_policy_id"] = previous["policy_id"]
                kwargs["change_maintainer_review_id"] = maintainer_reviews[0]["id"]
            else:
                reviewer = self.database.find_workflow_run(
                    execution["id"], correction_round - 1, "reviewer"
                )
                if reviewer is None:
                    raise RuntimeError("workflow correction has no reviewer run")
                verdict = self.database.get_typed_review_verdict(
                    previous["policy_id"], reviewer["id"]
                )
                evidence = next(
                    (
                        item
                        for item in self.database.list_gate_evidence(
                            previous["policy_id"]
                        )
                        if item["gate_name"] == "independent-review"
                        and item["outcome"] == "failed"
                        and item["reviewer_run_id"] == reviewer["id"]
                    ),
                    None,
                )
                if verdict is None or evidence is None:
                    raise RuntimeError(
                        "workflow correction has no exact failed review evidence"
                    )
                kwargs.update(
                    {
                        "change_policy_id": previous["policy_id"],
                        "change_review_evidence_id": evidence["id"],
                        "change_review_entry_id": verdict["work_log_entry_id"],
                    }
                )
        requester = self.database.get_user(execution["requested_by"])
        return self.database.create_run(
            project_key=execution["project_key"],
            profile_name=execution["roles"]["producer"]["profile_name"],
            work_item_id=execution["work_item_id"],
            requested_by=requester["id"],
            source_revision=source_revision,
            supervision_policy=WORKFLOW_RUN_SUPERVISION,
            workflow_execution_id=execution["id"],
            workflow_round=correction_round,
            workflow_role="producer",
            **kwargs,
        )

    def _create_reviewer(
        self, execution: dict[str, Any], policy_id: str
    ) -> dict[str, Any]:
        policy = self.database.get_gate_policy(policy_id)
        requester = self.database.get_user(execution["requested_by"])
        return self.database.create_run(
            project_key=execution["project_key"],
            profile_name=execution["roles"]["reviewer"]["profile_name"],
            work_item_id=execution["work_item_id"],
            requested_by=requester["id"],
            source_revision=policy["head_revision"],
            supervision_policy=WORKFLOW_RUN_SUPERVISION,
            gate_policy_id=policy_id,
            workflow_execution_id=execution["id"],
            workflow_round=execution["correction_round"],
            workflow_role="reviewer",
        )

    def _create_gate_policy(
        self, execution: dict[str, Any], candidate_id: str
    ) -> dict[str, Any]:
        spec = execution["spec"]
        request: dict[str, Any] = {
            "schema_version": 1,
            "review_separation": spec["review_separation"],
            "validations": [
                {
                    "name": validation["name"],
                    "image": validation["image_id"],
                    "command": validation["command"],
                }
                for validation in spec["validations"]
            ],
        }
        if spec["maintainer_approval"]:
            request["maintainer_approval"] = True
        return self.control.create_gate_policy(
            candidate_id,
            request,
            created_by_handle=execution["requested_by"],
            image_resolver=lambda image: _PinnedImage(image_id=image),
        )

    def _request_correction(
        self,
        execution: dict[str, Any],
        *,
        policy_id: str,
        source: str,
        evidence_id: str | None = None,
        review_entry_id: str | None = None,
        maintainer_review_id: str | None = None,
    ) -> bool:
        correction_round = execution["correction_round"]
        if correction_round >= execution["spec"]["max_correction_rounds"]:
            return self._ruling(execution, "correction_limit", policy_id=policy_id)
        metadata = {"policy_id": policy_id, "feedback_source": source}
        if evidence_id is not None:
            metadata["review_evidence_id"] = evidence_id
            metadata["review_entry_id"] = review_entry_id
        if maintainer_review_id is not None:
            metadata["maintainer_review_id"] = maintainer_review_id
        self.database.advance_workflow_execution(
            execution["id"],
            expected_version=execution["version"],
            expected_stage=execution["stage"],
            expected_correction_round=correction_round,
            target_stage="build",
            target_correction_round=correction_round + 1,
            event_kind="workflow.correction_started",
            metadata=metadata,
        )
        return True

    def _ruling(
        self,
        execution: dict[str, Any],
        kind: str,
        *,
        policy_id: str | None = None,
        run_id: str | None = None,
        gate_name: str | None = None,
        provider_landing_id: str | None = None,
        landing_plan_id: str | None = None,
    ) -> bool:
        self.database.open_workflow_ruling(
            execution["id"],
            expected_version=execution["version"],
            kind=kind,
            policy_id=policy_id,
            run_id=run_id,
            gate_name=gate_name,
            provider_landing_id=provider_landing_id,
            landing_plan_id=landing_plan_id,
        )
        return True

    def _required_gate(self, execution: dict[str, Any]) -> dict[str, Any]:
        gate = self.database.get_workflow_gate(
            execution["id"], execution["correction_round"]
        )
        if gate is None:
            raise RuntimeError("workflow stage has no exact gate binding")
        return gate

    def _candidate_for_run(self, run: dict[str, Any]) -> dict[str, Any] | None:
        candidates = self.database.list_candidates(producer_run_id=run["id"], limit=2)
        if len(candidates) > 1:
            raise RuntimeError("workflow producer has multiple candidates")
        return None if not candidates else candidates[0]

    @staticmethod
    def _run_is_active(run: dict[str, Any]) -> bool:
        return run["state"] in {
            "queued",
            "preparing",
            "running",
            "waiting_for_input",
            "paused",
        }

    @staticmethod
    def _successful_attempt(run: dict[str, Any]) -> dict[str, Any]:
        attempts = [
            attempt
            for attempt in run["attempts"]
            if attempt["state"] == "succeeded" and attempt["result"] is not None
        ]
        if not attempts:
            raise RuntimeError("successful workflow run has no retained result")
        return attempts[-1]
