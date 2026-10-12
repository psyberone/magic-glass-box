#!/usr/bin/env python3
"""Exercise the durable supervisor loop against a real SQLite state machine."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    InteractionKind,
    ProjectContext,
    SupervisionPolicy,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.supervision import Supervisor


class ProofDatabase(Database):
    def __init__(self, path: Path, clock: datetime) -> None:
        super().__init__(path)
        self.clock = clock

    def now(self) -> str:
        return self.clock.isoformat(timespec="microseconds")


class ProofCoordinator:
    """Deterministic actuator at the already-proven coordinator boundary."""

    def __init__(self, database: ProofDatabase) -> None:
        self.database = database
        self.execute_calls: list[str] = []
        self.reconcile_calls: list[str] = []
        self._lock = threading.Lock()

    def execute(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            self.execute_calls.append(run_id)
        run = self.database.get_run(run_id)
        attempt = self.database.begin_run_attempt(
            run_id,
            runtime_adapter="command",
            owner_id="proof-worker",
            expected_run_version=run["version"],
            lease_seconds=60,
        )
        self.database.fail_run_attempt(
            attempt["id"],
            expected_attempt_version=attempt["version"],
            reason="deterministic proof worker exited",
        )
        return self.database.get_run(run_id)

    def reconcile(self, run_id: str | None = None) -> list[dict[str, Any]]:
        if run_id is None:
            return []
        with self._lock:
            self.reconcile_calls.append(run_id)
        reconciled: list[dict[str, Any]] = []
        for candidate in self.database.list_expired_run_attempts():
            if candidate["run_id"] != run_id:
                continue
            claimed = self.database.claim_expired_run_attempt(
                candidate["id"],
                owner_id="proof-recovery",
                lease_seconds=60,
            )
            self.database.fail_run_attempt(
                claimed["id"],
                expected_attempt_version=claimed["version"],
                reason="deterministic expired-owner recovery",
                interrupted=True,
            )
            reconciled.append(self.database.get_run(run_id))
        return reconciled


class ProofReminderTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, float]] = []
        self._lock = threading.Lock()

    def remind_interaction(
        self,
        interaction_id: str,
        *,
        idempotency_key: str,
        timeout: float,
    ) -> int:
        with self._lock:
            self.calls.append((interaction_id, idempotency_key, timeout))
            return len(self.calls)


def automatic_policy() -> SupervisionPolicy:
    return SupervisionPolicy.from_mapping(
        {
            "schema_version": 1,
            "auto_execute": True,
            "retry_backoff_seconds": [1, 5],
            "interaction_escalation_seconds": [10, 30],
            "attempt_idle_escalation_seconds": [],
            "attempt_lease_expired_escalation_seconds": [0],
            "other_attention_escalation_seconds": [],
        }
    )


def _drive_competing(supervisors: tuple[Supervisor, Supervisor]) -> None:
    errors: list[BaseException] = []
    lock = threading.Lock()

    def tick(supervisor: Supervisor) -> None:
        try:
            supervisor.tick(wait=True)
        except BaseException as exc:
            with lock:
                errors.append(exc)

    for _ in range(4):
        threads = [threading.Thread(target=tick, args=(item,)) for item in supervisors]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        if any(thread.is_alive() for thread in threads):
            raise RuntimeError("supervisor proof tick did not terminate")
        if errors:
            raise RuntimeError("supervisor proof tick failed") from errors[0]


def _create_fixture(
    database: ProofDatabase,
) -> tuple[dict[str, dict[str, Any]], str]:
    database.initialize()
    user = database.create_user("operator", "Proof Operator")
    instance = database.create_krab_instance("Proof Krab", "http://127.0.0.1:8765")
    identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity("usr_" + "1" * 32, "proof_worker", 1),
        "file:/proof/krab.token",
        user_id=user["id"],
    )
    database.create_project(
        instance_id=instance["id"],
        krab_project_id="prj_" + "2" * 32,
        krab_project_key="proof-project",
        display_name="Supervisor proof",
        context=ProjectContext.from_mapping(
            {"purpose": "Exercise autonomous supervision deterministically"}
        ),
        created_by=user["id"],
    )
    database.create_agent_profile(
        name="Proof Worker",
        spec=AgentProfileSpec.from_mapping(
            {"image": "proof@sha256:abc", "command": ["proof-worker"]}
        ),
        krab_identity_id=identity["id"],
        created_by=user["id"],
    )
    item = database.import_work_item(
        project_key="proof-project",
        envelope=WorkItemEnvelope.from_mapping(
            {
                "schema_version": 1,
                "source": "manual",
                "external_id": "live-supervisor-proof",
                "source_updated_at": "2026-10-12T12:00:00Z",
                "title": "Exercise the supervisor",
                "brief": "Prove autonomous advancement and safe recovery.",
            }
        ),
        imported_by=user["id"],
    )

    def create(
        policy: SupervisionPolicy | None = None, *, session: bool = False
    ) -> dict[str, Any]:
        return database.create_run(
            project_key="proof-project",
            profile_name="Proof Worker",
            work_item_id=item["id"],
            requested_by=user["id"],
            supervision_policy=policy,
            krab_session_id="ses_" + "3" * 32 if session else None,
        )

    runs = {
        "manual": create(),
        "automatic": create(automatic_policy()),
        "answered": create(automatic_policy(), session=True),
        "expired": create(),
        "live": create(),
        "stalled": create(automatic_policy(), session=True),
    }

    answered_attempt = database.begin_run_attempt(
        runs["answered"]["id"],
        runtime_adapter="command",
        owner_id="answered-owner",
        expected_run_version=runs["answered"]["version"],
        lease_seconds=60,
    )
    answered_attempt = database.start_run_attempt(
        answered_attempt["id"],
        expected_attempt_version=answered_attempt["version"],
        expected_run_version=answered_attempt["run_version"],
        source_revision="a" * 40,
        container_id="b" * 64,
        container_name="mgb-proof-answered",
        image_id="sha256:" + "c" * 64,
    )
    answered_interaction = database.open_interaction(
        runs["answered"]["id"],
        kind=InteractionKind.QUESTION,
        recipient_krab_identity_id=identity["id"],
        request_item_id="itm_" + "4" * 32,
        expected_run_version=answered_attempt["run_version"],
    )
    database.fail_run_attempt(
        answered_attempt["id"],
        expected_attempt_version=answered_attempt["version"],
        reason="proof worker handed back control",
        interrupted=True,
    )
    database.answer_interaction(
        answered_interaction["id"],
        response_item_id="itm_" + "5" * 32,
        expected_version=answered_interaction["version"],
    )

    expired_attempt = database.begin_run_attempt(
        runs["expired"]["id"],
        runtime_adapter="command",
        owner_id="expired-owner",
        expected_run_version=runs["expired"]["version"],
        lease_seconds=10,
    )
    live_attempt = database.begin_run_attempt(
        runs["live"]["id"],
        runtime_adapter="command",
        owner_id="live-owner",
        expected_run_version=runs["live"]["version"],
        lease_seconds=60,
    )
    stalled_attempt = database.begin_run_attempt(
        runs["stalled"]["id"],
        runtime_adapter="command",
        owner_id="stalled-owner",
        expected_run_version=runs["stalled"]["version"],
        lease_seconds=3600,
    )
    stalled_attempt = database.start_run_attempt(
        stalled_attempt["id"],
        expected_attempt_version=stalled_attempt["version"],
        expected_run_version=stalled_attempt["run_version"],
        source_revision="d" * 40,
        container_id="e" * 64,
        container_name="mgb-proof-stalled",
        image_id="sha256:" + "f" * 64,
    )
    stalled_interaction = database.open_interaction(
        runs["stalled"]["id"],
        kind=InteractionKind.QUESTION,
        recipient_krab_identity_id=identity["id"],
        request_item_id="itm_" + "6" * 32,
        expected_run_version=stalled_attempt["run_version"],
    )
    runs["expired_attempt"] = expired_attempt
    runs["live_attempt"] = live_attempt
    return runs, stalled_interaction["id"]


def run_proof() -> dict[str, Any]:
    start = datetime(2027, 1, 1, tzinfo=UTC)
    with tempfile.TemporaryDirectory(prefix="mgb-supervisor-proof-") as temporary:
        root = Path(temporary)
        os.chmod(root, 0o700)
        database = ProofDatabase(root / "control-plane.db", start)
        runs, stalled_interaction_id = _create_fixture(database)
        coordinator = ProofCoordinator(database)
        reminders = ProofReminderTransport()
        supervisors = (
            Supervisor(
                database,
                coordinator,
                reminders,
                supervisor_id="proof-supervisor-one",
                max_workers=1,
            ),
            Supervisor(
                database,
                coordinator,
                reminders,
                supervisor_id="proof-supervisor-two",
                max_workers=1,
            ),
        )
        try:
            database.clock = start + timedelta(seconds=10)
            _drive_competing(supervisors)
            database.clock = start + timedelta(seconds=30)
            _drive_competing(supervisors)
        finally:
            for supervisor in supervisors:
                supervisor.close(wait=True)

        actions = database.list_supervisor_actions()
        escalations = [
            item
            for item in database.list_supervisor_escalations()
            if item["kind"] == "interaction.pending"
            and item["subject_id"] == stalled_interaction_id
        ]
        action_subjects = [
            (item["kind"], item["subject_id"], item["subject_version"])
            for item in actions
        ]
        evidence = {
            "automatic_run_state": database.get_run(runs["automatic"]["id"])["state"],
            "answered_run_state": database.get_run(runs["answered"]["id"])["state"],
            "manual_run_state": database.get_run(runs["manual"]["id"])["state"],
            "expired_attempt_state": database.get_run_attempt(
                runs["expired_attempt"]["id"]
            )["state"],
            "live_attempt_state": database.get_run_attempt(runs["live_attempt"]["id"])[
                "state"
            ],
            "execute_counts": {
                "automatic": coordinator.execute_calls.count(runs["automatic"]["id"]),
                "answered": coordinator.execute_calls.count(runs["answered"]["id"]),
                "manual": coordinator.execute_calls.count(runs["manual"]["id"]),
            },
            "reconcile_counts": {
                "expired": coordinator.reconcile_calls.count(runs["expired"]["id"]),
                "live": coordinator.reconcile_calls.count(runs["live"]["id"]),
            },
            "action_states": sorted(item["state"] for item in actions),
            "duplicate_action_subjects": len(action_subjects)
            != len(set(action_subjects)),
            "reminder_count": len(reminders.calls),
            "reminder_timeouts": sorted({call[2] for call in reminders.calls}),
            "reminder_thresholds": sorted(
                item["threshold_seconds"] for item in escalations
            ),
            "reminder_states": sorted(item["state"] for item in escalations),
            "reminder_payload_fields": ["idempotency_key", "interaction_id"],
        }
        _assert_proof(evidence)
        return evidence


def _assert_proof(evidence: Mapping[str, Any]) -> None:
    if evidence.get("automatic_run_state") == "queued":
        raise RuntimeError("automatic queued run did not advance")
    if evidence.get("answered_run_state") == "queued":
        raise RuntimeError("answered run did not resume")
    if evidence.get("manual_run_state") != "queued":
        raise RuntimeError("manual run advanced without explicit policy")
    if evidence.get("expired_attempt_state") != "interrupted":
        raise RuntimeError("expired attempt was not recovered")
    if evidence.get("live_attempt_state") != "preparing":
        raise RuntimeError("live attempt was touched by recovery")
    if evidence.get("execute_counts") != {
        "automatic": 1,
        "answered": 1,
        "manual": 0,
    }:
        raise RuntimeError("run execution was not exactly once per authorized subject")
    if evidence.get("reconcile_counts") != {"expired": 1, "live": 0}:
        raise RuntimeError("lease recovery did not preserve live ownership")
    if evidence.get("duplicate_action_subjects") is not False:
        raise RuntimeError("duplicate durable action subjects were recorded")
    if any(state != "succeeded" for state in evidence.get("action_states", [])):
        raise RuntimeError("durable supervisor action did not settle")
    if evidence.get("reminder_count") != 2:
        raise RuntimeError("stalled interaction reminders were not deduplicated")
    if evidence.get("reminder_timeouts") != [30.0]:
        raise RuntimeError("reminder transport did not receive its bounded timeout")
    if evidence.get("reminder_thresholds") != [10, 30]:
        raise RuntimeError("configured escalation thresholds were not delivered")
    if evidence.get("reminder_states") != ["delivered", "delivered"]:
        raise RuntimeError("escalation records did not settle")
    if evidence.get("reminder_payload_fields") != [
        "idempotency_key",
        "interaction_id",
    ]:
        raise RuntimeError("reminder transport accepted private content")


def main() -> int:
    print(json.dumps(run_proof(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
