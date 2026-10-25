from __future__ import annotations

from typing import Any

from .domain import SupervisionPolicy
from .service import ControlPlane

DISCOVERY_RUN_SUPERVISION = SupervisionPolicy.from_mapping(
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


class DiscoveryDirector:
    """Advance the restart-safe Discovery prescription from durable evidence."""

    def __init__(self, control: ControlPlane) -> None:
        self.database = control.database

    def advance(self, execution_id: str, *, expected_version: int) -> bool:
        execution = self.database.get_discovery_execution(execution_id)
        if execution["state"] != "active" or execution["version"] != expected_version:
            return True
        if execution["stage"] == "investigate":
            return self._advance_investigate(execution)
        if execution["stage"] == "coordinate":
            return self._advance_coordinate(execution)
        if execution["stage"] == "completion":
            self.database.complete_discovery_execution(
                execution["id"], expected_version=execution["version"]
            )
            return True
        raise RuntimeError("stored Discovery execution stage is invalid")

    def _advance_investigate(self, execution: dict[str, Any]) -> bool:
        run = execution["runs"].get("investigator")
        if run is None:
            self._create_role_run(execution, "investigator")
            return False
        if self._run_is_active(run):
            return False
        if run["state"] != "succeeded":
            return self._fail_for_run(execution, run, "investigator")
        trace = execution["trace"]
        if trace is None or trace["run_id"] != run["run_id"]:
            self.database.fail_discovery_execution(
                execution["id"],
                expected_version=execution["version"],
                reason=(
                    f"investigator run {run['run_id']} succeeded without a "
                    "Discovery trace"
                ),
            )
            return True
        self.database.advance_discovery_execution(
            execution["id"],
            expected_version=execution["version"],
            expected_stage="investigate",
            target_stage="coordinate",
            event_kind="discovery.investigation_completed",
            metadata={
                "investigator_run_id": run["run_id"],
                "trace_id": trace["id"],
                "trace_sha256": trace["content_sha256"],
            },
        )
        return True

    def _advance_coordinate(self, execution: dict[str, Any]) -> bool:
        run = execution["runs"].get("coordinator")
        if run is None:
            self._create_role_run(execution, "coordinator")
            return False
        if self._run_is_active(run):
            return False
        if run["state"] != "succeeded":
            return self._fail_for_run(execution, run, "coordinator")
        ruling = execution["final_ruling"]
        if ruling is None or ruling["run_id"] != run["run_id"]:
            self.database.fail_discovery_execution(
                execution["id"],
                expected_version=execution["version"],
                reason=(
                    f"coordinator run {run['run_id']} succeeded without a final "
                    "Discovery ruling"
                ),
            )
            return True
        self.database.advance_discovery_execution(
            execution["id"],
            expected_version=execution["version"],
            expected_stage="coordinate",
            target_stage="completion",
            event_kind="discovery.coordination_completed",
            metadata={
                "coordinator_run_id": run["run_id"],
                "ruling_id": ruling["id"],
                "ruling_sha256": ruling["content_sha256"],
                "disposition": ruling["disposition"],
            },
        )
        return True

    def _create_role_run(self, execution: dict[str, Any], role: str) -> dict[str, Any]:
        requester = self.database.get_user(execution["requested_by"])
        coordinator = role == "coordinator"
        return self.database.create_run(
            project_key=execution["project_key"],
            profile_name=execution["roles"][role]["profile_name"],
            work_item_id=execution["work_item_id"],
            requested_by=requester["id"],
            source_revision=execution["source_revision"],
            krab_session_id=execution["krab_session_id"] if coordinator else None,
            krab_claim_id=execution["krab_claim_id"] if coordinator else None,
            supervision_policy=DISCOVERY_RUN_SUPERVISION,
            discovery_execution_id=execution["id"],
            discovery_role=role,
        )

    def _fail_for_run(
        self, execution: dict[str, Any], run: dict[str, Any], role: str
    ) -> bool:
        self.database.fail_discovery_execution(
            execution["id"],
            expected_version=execution["version"],
            reason=f"{role} run {run['run_id']} ended in state {run['state']}",
        )
        return True

    @staticmethod
    def _run_is_active(run: dict[str, Any]) -> bool:
        return run["state"] in {
            "queued",
            "preparing",
            "running",
            "waiting_for_input",
            "paused",
        }
