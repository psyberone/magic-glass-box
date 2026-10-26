from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .domain import SupervisionPolicy
from .service import ControlPlane
from .storage.advisory import ADVISORY_REVIEWER_ROLES

ADVISORY_RUN_SUPERVISION = SupervisionPolicy.from_mapping(
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


class AdvisoryDirector:
    """Advance one restart-safe advisory review from durable phase evidence."""

    def __init__(self, control: ControlPlane) -> None:
        self.database = control.database

    def advance(self, execution_id: str, *, expected_version: int) -> bool:
        execution = self.database.get_advisory_execution(execution_id)
        if execution["state"] != "active" or execution["version"] != expected_version:
            return True
        if execution["stage"] == "blind_review":
            return self._advance_reviewers(
                execution,
                stage="blind_review",
                evidence_name="blind_reviews",
                evidence_label="advisory blind review",
            )
        if execution["stage"] == "challenge":
            return self._advance_reviewers(
                execution,
                stage="challenge",
                evidence_name="challenge_reviews",
                evidence_label="advisory challenge review",
            )
        if execution["stage"] == "triangulate":
            return self._advance_coordinator(execution)
        if execution["stage"] == "completion":
            self.database.complete_advisory_execution(
                execution["id"], expected_version=execution["version"]
            )
            return True
        raise RuntimeError("stored advisory execution stage is invalid")

    def _advance_reviewers(
        self,
        execution: dict[str, Any],
        *,
        stage: str,
        evidence_name: str,
        evidence_label: str,
    ) -> bool:
        runs = execution["runs"].get(stage, {})
        missing = [role for role in ADVISORY_REVIEWER_ROLES if role not in runs]
        if missing:
            for role in missing:
                self._create_role_run(execution, stage=stage, role=role)
            return False
        ordered_runs = [runs[role] for role in ADVISORY_REVIEWER_ROLES]
        if any(self._run_is_active(run) for run in ordered_runs):
            return False
        for role, run in zip(ADVISORY_REVIEWER_ROLES, ordered_runs, strict=True):
            if run["state"] != "succeeded":
                return self._fail_for_run(execution, run, role)
        evidence = execution[evidence_name]
        for role, run in zip(ADVISORY_REVIEWER_ROLES, ordered_runs, strict=True):
            artifact = evidence.get(role)
            if artifact is None or artifact["run_id"] != run["run_id"]:
                self.database.fail_advisory_execution(
                    execution["id"],
                    expected_version=execution["version"],
                    reason=(
                        f"{role} run {run['run_id']} succeeded without an "
                        f"{evidence_label}"
                    ),
                )
                return True
        self.database.advance_advisory_execution(
            execution["id"], expected_version=execution["version"]
        )
        return True

    def _advance_coordinator(self, execution: dict[str, Any]) -> bool:
        run = execution["runs"].get("triangulate", {}).get("coordinator")
        if run is None:
            self._create_role_run(execution, stage="triangulate", role="coordinator")
            return False
        if self._run_is_active(run):
            return False
        if run["state"] != "succeeded":
            return self._fail_for_run(execution, run, "coordinator")
        report = execution["report"]
        if report is None or report["run_id"] != run["run_id"]:
            self.database.fail_advisory_execution(
                execution["id"],
                expected_version=execution["version"],
                reason=(
                    f"coordinator run {run['run_id']} succeeded without an "
                    "advisory report"
                ),
            )
            return True
        self.database.advance_advisory_execution(
            execution["id"], expected_version=execution["version"]
        )
        return True

    def _create_role_run(
        self, execution: Mapping[str, Any], *, stage: str, role: str
    ) -> dict[str, Any]:
        requester = self.database.get_user(execution["requested_by"])
        return self.database.create_run(
            project_key=execution["project_key"],
            profile_name=execution["roles"][role]["profile_name"],
            work_item_id=execution["work_item_id"],
            requested_by=requester["id"],
            source_revision=execution["candidate"]["head_revision"],
            supervision_policy=ADVISORY_RUN_SUPERVISION,
            advisory_execution_id=execution["id"],
            advisory_stage=stage,
            advisory_role=role,
        )

    def _fail_for_run(
        self, execution: Mapping[str, Any], run: Mapping[str, Any], role: str
    ) -> bool:
        self.database.fail_advisory_execution(
            execution["id"],
            expected_version=execution["version"],
            reason=f"{role} run {run['run_id']} ended in state {run['state']}",
        )
        return True

    @staticmethod
    def _run_is_active(run: Mapping[str, Any]) -> bool:
        return run["state"] in {
            "queued",
            "preparing",
            "running",
            "waiting_for_input",
            "paused",
        }
