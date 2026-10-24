from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any

from ..domain import SupervisionPolicy, ValidationError, require_string
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)
from .schema import MAX_ATTEMPT_LEASE_SECONDS, MIN_ATTEMPT_LEASE_SECONDS

ERROR_CODE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
ACTIVE_ACTION_STATES = ("pending", "claimed")


class SupervisionStorage:
    def enqueue_supervisor_actions(self) -> list[dict[str, Any]]:
        """Materialize version-bound execution and recovery work idempotently."""

        now = self.now()
        inserted: list[str] = []
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                due_recoveries = connection.execute(
                    "SELECT pr.id, pr.run_id, pr.category, pr.invocation_id, "
                    "pr.eligible_at, r.version, r.supervision_policy_json "
                    "FROM provider_recoveries pr JOIN runs r ON r.id = pr.run_id "
                    "WHERE pr.state = 'active' AND pr.eligible_at <= ? "
                    "AND r.state = 'paused' AND NOT EXISTS ("
                    "SELECT 1 FROM run_attempts ra WHERE ra.run_id = pr.run_id "
                    "AND ra.state IN ('preparing', 'running')) AND NOT EXISTS ("
                    "SELECT 1 FROM interactions i WHERE i.run_id = pr.run_id "
                    "AND i.state = 'open') "
                    "ORDER BY pr.eligible_at, pr.created_at, pr.id",
                    (now,),
                ).fetchall()
                for recovery in due_recoveries:
                    resolved = connection.execute(
                        "UPDATE provider_recoveries SET state = 'resumed', "
                        "version = version + 1, updated_at = ?, resolved_at = ? "
                        "WHERE id = ? AND state = 'active'",
                        (now, now, recovery["id"]),
                    ).rowcount
                    changed = connection.execute(
                        "UPDATE runs SET state = 'queued', version = version + 1, "
                        "updated_at = ?, finished_at = NULL WHERE id = ? "
                        "AND version = ? AND state = 'paused'",
                        (
                            now,
                            recovery["run_id"],
                            recovery["version"],
                        ),
                    ).rowcount
                    if resolved != 1 or changed != 1:
                        raise ConflictError(
                            "provider recovery changed while it was waking"
                        )
                    metadata = json.dumps(
                        {
                            "category": recovery["category"],
                            "invocation_id": recovery["invocation_id"],
                            "recovery_id": recovery["id"],
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    connection.execute(
                        "INSERT INTO run_events "
                        "(run_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) VALUES (?, ?, "
                        "'run.provider_resumed', 'paused', 'queued', ?, ?)",
                        (
                            recovery["run_id"],
                            recovery["version"] + 1,
                            metadata,
                            now,
                        ),
                    )
                queued = connection.execute(
                    "SELECT id, version, supervision_policy_json FROM runs "
                    "WHERE state = 'queued' ORDER BY created_at, id"
                ).fetchall()
                for run in queued:
                    policy = self._stored_supervision_policy(
                        run["supervision_policy_json"]
                    )
                    if policy.auto_execute:
                        action_id = self._insert_supervisor_action(
                            connection,
                            kind="run.execute",
                            subject_id=run["id"],
                            subject_version=run["version"],
                            run_id=run["id"],
                            policy=policy,
                            now=now,
                        )
                        if action_id is not None:
                            inserted.append(action_id)

                expired = connection.execute(
                    "SELECT ra.id, ra.version, ra.run_id, "
                    "r.supervision_policy_json FROM run_attempts ra "
                    "JOIN runs r ON r.id = ra.run_id "
                    "WHERE ra.state IN ('preparing', 'running') "
                    "AND (ra.lease_expires_at IS NULL OR ra.lease_expires_at <= ?) "
                    "ORDER BY ra.created_at, ra.id",
                    (now,),
                ).fetchall()
                for attempt in expired:
                    policy = self._stored_supervision_policy(
                        attempt["supervision_policy_json"]
                    )
                    action_id = self._insert_supervisor_action(
                        connection,
                        kind="attempt.reconcile",
                        subject_id=attempt["id"],
                        subject_version=attempt["version"],
                        run_id=attempt["run_id"],
                        policy=policy,
                        now=now,
                    )
                    if action_id is not None:
                        inserted.append(action_id)

                orphaned = connection.execute(
                    "SELECT r.id, r.version, r.supervision_policy_json FROM runs r "
                    "WHERE r.state IN ('preparing', 'running') AND NOT EXISTS ("
                    "SELECT 1 FROM run_attempts ra WHERE ra.run_id = r.id "
                    "AND ra.state IN ('preparing', 'running')) "
                    "ORDER BY r.created_at, r.id"
                ).fetchall()
                for run in orphaned:
                    policy = self._stored_supervision_policy(
                        run["supervision_policy_json"]
                    )
                    action_id = self._insert_supervisor_action(
                        connection,
                        kind="run.reconcile",
                        subject_id=run["id"],
                        subject_version=run["version"],
                        run_id=run["id"],
                        policy=policy,
                        now=now,
                    )
                    if action_id is not None:
                        inserted.append(action_id)

                workflows = connection.execute(
                    "SELECT e.id, e.version FROM workflow_executions e "
                    "WHERE e.state = 'active' "
                    "AND e.stage IN ("
                    "'build', 'review', 'maintainer', 'landing', 'completion') "
                    "AND NOT EXISTS (SELECT 1 FROM workflow_rulings wr "
                    "WHERE wr.execution_id = e.id AND wr.state = 'open') "
                    "ORDER BY e.created_at, e.id"
                ).fetchall()
                workflow_policy = SupervisionPolicy.disabled()
                for workflow in workflows:
                    action_id = self._insert_supervisor_action(
                        connection,
                        kind="workflow.advance",
                        subject_id=workflow["id"],
                        subject_version=workflow["version"],
                        run_id=None,
                        workflow_execution_id=workflow["id"],
                        policy=workflow_policy,
                        now=now,
                    )
                    if action_id is not None:
                        inserted.append(action_id)
                connection.commit()
            except (ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("supervisor actions could not be enqueued") from exc
        return [self.get_supervisor_action(action_id) for action_id in inserted]

    def _insert_supervisor_action(
        self,
        connection: sqlite3.Connection,
        *,
        kind: str,
        subject_id: str,
        subject_version: int,
        run_id: str | None,
        workflow_execution_id: str | None = None,
        policy: SupervisionPolicy,
        now: str,
    ) -> str | None:
        action_id = self.new_id("act")
        changed = connection.execute(
            "INSERT OR IGNORE INTO supervisor_actions "
            "(id, kind, subject_id, subject_version, run_id, "
            "workflow_execution_id, state, version, "
            "attempt_count, max_attempts, retry_backoff_json, available_at, "
            "created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'pending', 1, 0, ?, ?, ?, ?, ?)",
            (
                action_id,
                kind,
                subject_id,
                subject_version,
                run_id,
                workflow_execution_id,
                len(policy.retry_backoff_seconds) + 1,
                json.dumps(list(policy.retry_backoff_seconds), separators=(",", ":")),
                now,
                now,
                now,
            ),
        ).rowcount
        return action_id if changed == 1 else None

    def claim_supervisor_action(
        self, *, owner_id: str, lease_seconds: int = 60
    ) -> dict[str, Any] | None:
        owner_id = self._supervisor_owner(owner_id)
        self._supervisor_lease_seconds(lease_seconds)
        now = self.now()
        expires_at = self._timestamp_after(now, lease_seconds)
        claimed_id: str | None = None
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE supervisor_actions SET state = 'exhausted', "
                    "version = version + 1, owner_id = NULL, "
                    "lease_expires_at = NULL, last_error_code = 'LeaseExpired', "
                    "updated_at = ?, finished_at = ? WHERE state = 'claimed' "
                    "AND lease_expires_at <= ? AND attempt_count >= max_attempts",
                    (now, now, now),
                )
                row = connection.execute(
                    "SELECT id, version FROM supervisor_actions WHERE "
                    "(state = 'pending' AND available_at <= ? "
                    "AND attempt_count < max_attempts) OR "
                    "(state = 'claimed' AND lease_expires_at <= ? "
                    "AND attempt_count < max_attempts) "
                    "ORDER BY available_at, created_at, id LIMIT 1",
                    (now, now),
                ).fetchone()
                if row is not None:
                    changed = connection.execute(
                        "UPDATE supervisor_actions SET state = 'claimed', "
                        "version = version + 1, attempt_count = attempt_count + 1, "
                        "owner_id = ?, lease_expires_at = ?, updated_at = ? "
                        "WHERE id = ? AND version = ?",
                        (owner_id, expires_at, now, row["id"], row["version"]),
                    ).rowcount
                    if changed != 1:
                        raise ConcurrentUpdateError(
                            "supervisor action changed concurrently"
                        )
                    claimed_id = row["id"]
                connection.commit()
            except (ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("supervisor action could not be claimed") from exc
        return None if claimed_id is None else self.get_supervisor_action(claimed_id)

    def complete_supervisor_action(
        self,
        action_id: str,
        *,
        owner_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        return self._finish_supervisor_action(
            action_id,
            owner_id=owner_id,
            expected_version=expected_version,
            error_code=None,
        )

    def renew_supervisor_action_lease(
        self,
        action_id: str,
        *,
        owner_id: str,
        expected_version: int,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        """Keep an in-flight action claim live without changing its version."""

        action_id = require_string(action_id, field="action_id", max_bytes=64)
        owner_id = self._supervisor_owner(owner_id)
        self._positive_version(expected_version)
        self._supervisor_lease_seconds(lease_seconds)
        now = self.now()
        expires_at = self._timestamp_after(now, lease_seconds)
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, version, owner_id FROM supervisor_actions "
                    "WHERE id = ?",
                    (action_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"supervisor action not found: {action_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"supervisor action version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["state"] != "claimed" or row["owner_id"] != owner_id:
                    raise ConflictError(
                        "supervisor action is not owned by this process"
                    )
                changed = connection.execute(
                    "UPDATE supervisor_actions SET lease_expires_at = ?, "
                    "updated_at = ? WHERE id = ? AND state = 'claimed' "
                    "AND version = ? AND owner_id = ?",
                    (expires_at, now, action_id, expected_version, owner_id),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "supervisor action changed concurrently"
                    )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "supervisor action lease could not be renewed"
                ) from exc
        return self.get_supervisor_action(action_id)

    def defer_supervisor_action(
        self,
        action_id: str,
        *,
        owner_id: str,
        expected_version: int,
        available_at: str,
    ) -> dict[str, Any]:
        """Release a claimed action after its underlying lease was renewed."""

        action_id = require_string(action_id, field="action_id", max_bytes=64)
        owner_id = self._supervisor_owner(owner_id)
        self._positive_version(expected_version)
        now = self.now()
        if self._aware_timestamp(
            available_at, field="action availability"
        ) <= self._aware_timestamp(now, field="database clock"):
            raise ValidationError("available_at must be a future timestamp")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, version, owner_id, attempt_count "
                    "FROM supervisor_actions WHERE id = ?",
                    (action_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"supervisor action not found: {action_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"supervisor action version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["state"] != "claimed" or row["owner_id"] != owner_id:
                    raise ConflictError(
                        "supervisor action is not owned by this process"
                    )
                if row["attempt_count"] < 1:
                    raise StorageError("stored supervisor action claim is invalid")
                changed = connection.execute(
                    "UPDATE supervisor_actions SET state = 'pending', "
                    "version = version + 1, attempt_count = attempt_count - 1, "
                    "available_at = ?, owner_id = NULL, lease_expires_at = NULL, "
                    "last_error_code = NULL, updated_at = ? "
                    "WHERE id = ? AND version = ?",
                    (available_at, now, action_id, expected_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "supervisor action changed concurrently"
                    )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("supervisor action could not be deferred") from exc
        return self.get_supervisor_action(action_id)

    def fail_supervisor_action(
        self,
        action_id: str,
        *,
        owner_id: str,
        expected_version: int,
        error_code: str,
    ) -> dict[str, Any]:
        error_code = self._error_code(error_code)
        return self._finish_supervisor_action(
            action_id,
            owner_id=owner_id,
            expected_version=expected_version,
            error_code=error_code,
        )

    def _finish_supervisor_action(
        self,
        action_id: str,
        *,
        owner_id: str,
        expected_version: int,
        error_code: str | None,
    ) -> dict[str, Any]:
        action_id = require_string(action_id, field="action_id", max_bytes=64)
        owner_id = self._supervisor_owner(owner_id)
        self._positive_version(expected_version)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, version, owner_id, attempt_count, max_attempts, "
                    "retry_backoff_json, available_at FROM supervisor_actions "
                    "WHERE id = ?",
                    (action_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"supervisor action not found: {action_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"supervisor action version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["state"] != "claimed" or row["owner_id"] != owner_id:
                    raise ConflictError(
                        "supervisor action is not owned by this process"
                    )
                if error_code is None:
                    state = "succeeded"
                    available_at = row["available_at"]
                    finished_at = now
                elif row["attempt_count"] >= row["max_attempts"]:
                    state = "exhausted"
                    available_at = row["available_at"]
                    finished_at = now
                else:
                    backoff = self._stored_backoff(row["retry_backoff_json"])
                    index = row["attempt_count"] - 1
                    if index < 0 or index >= len(backoff):
                        raise StorageError(
                            "stored supervisor action backoff is invalid"
                        )
                    state = "pending"
                    available_at = self._timestamp_after(now, backoff[index])
                    finished_at = None
                changed = connection.execute(
                    "UPDATE supervisor_actions SET state = ?, version = version + 1, "
                    "available_at = ?, owner_id = NULL, lease_expires_at = NULL, "
                    "last_error_code = ?, updated_at = ?, finished_at = ? "
                    "WHERE id = ? AND version = ?",
                    (
                        state,
                        available_at,
                        error_code,
                        now,
                        finished_at,
                        action_id,
                        expected_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "supervisor action changed concurrently"
                    )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("supervisor action could not be updated") from exc
        return self.get_supervisor_action(action_id)

    def get_supervisor_action(self, action_id: str) -> dict[str, Any]:
        action_id = require_string(action_id, field="action_id", max_bytes=64)
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM supervisor_actions WHERE id = ?", (action_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"supervisor action not found: {action_id}")
        return self._supervisor_action_record(row)

    def list_supervisor_actions(self) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM supervisor_actions ORDER BY created_at DESC, id"
            ).fetchall()
        return [self._supervisor_action_record(row) for row in rows]

    def create_due_supervisor_escalations(self) -> list[dict[str, Any]]:
        """Record each configured age threshold once without message content."""

        now = self.now()
        now_value = self._aware_timestamp(now, field="database clock")
        inserted: list[str] = []
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                interactions = connection.execute(
                    "SELECT i.id, i.run_id, i.created_at, "
                    "r.supervision_policy_json FROM interactions i "
                    "JOIN runs r ON r.id = i.run_id WHERE i.state = 'open' "
                    "ORDER BY i.created_at, i.id"
                ).fetchall()
                for row in interactions:
                    policy = self._stored_supervision_policy(
                        row["supervision_policy_json"]
                    )
                    inserted.extend(
                        self._insert_due_escalations(
                            connection,
                            run_id=row["run_id"],
                            kind="interaction.pending",
                            subject_type="interaction",
                            subject_id=row["id"],
                            age_started_at=row["created_at"],
                            thresholds=policy.interaction_escalation_seconds,
                            channel="krab_reminder",
                            policy=policy,
                            now=now,
                            now_value=now_value,
                        )
                    )

                attempts = connection.execute(
                    "SELECT ra.id, ra.run_id, ra.created_at, ra.started_at, "
                    "ra.heartbeat_at, ra.lease_expires_at, "
                    "r.supervision_policy_json "
                    "FROM run_attempts ra JOIN runs r ON r.id = ra.run_id "
                    "WHERE ra.state IN ('preparing', 'running') "
                    "ORDER BY ra.created_at, ra.id"
                ).fetchall()
                for row in attempts:
                    policy = self._stored_supervision_policy(
                        row["supervision_policy_json"]
                    )
                    inserted.extend(
                        self._insert_due_escalations(
                            connection,
                            run_id=row["run_id"],
                            kind="attempt.idle",
                            subject_type="attempt",
                            subject_id=row["id"],
                            age_started_at=(
                                row["heartbeat_at"]
                                or row["started_at"]
                                or row["created_at"]
                            ),
                            thresholds=policy.attempt_idle_escalation_seconds,
                            channel="control_room",
                            policy=policy,
                            now=now,
                            now_value=now_value,
                        )
                    )
                    if row["lease_expires_at"] is not None:
                        inserted.extend(
                            self._insert_due_escalations(
                                connection,
                                run_id=row["run_id"],
                                kind="attempt.lease_expired",
                                subject_type="attempt",
                                subject_id=row["id"],
                                age_started_at=row["lease_expires_at"],
                                thresholds=(
                                    policy.attempt_lease_expired_escalation_seconds
                                ),
                                channel="control_room",
                                policy=policy,
                                now=now,
                                now_value=now_value,
                            )
                        )

                missing_attempts = connection.execute(
                    "SELECT r.id, r.updated_at, r.supervision_policy_json FROM runs r "
                    "WHERE r.state IN ('preparing', 'running') AND NOT EXISTS ("
                    "SELECT 1 FROM run_attempts ra WHERE ra.run_id = r.id "
                    "AND ra.state IN ('preparing', 'running')) "
                    "ORDER BY r.updated_at, r.id"
                ).fetchall()
                for row in missing_attempts:
                    policy = self._stored_supervision_policy(
                        row["supervision_policy_json"]
                    )
                    inserted.extend(
                        self._insert_due_escalations(
                            connection,
                            run_id=row["id"],
                            kind="run.missing_active_attempt",
                            subject_type="run",
                            subject_id=row["id"],
                            age_started_at=row["updated_at"],
                            thresholds=policy.other_attention_escalation_seconds,
                            channel="control_room",
                            policy=policy,
                            now=now,
                            now_value=now_value,
                        )
                    )

                waiting = connection.execute(
                    "SELECT r.id, r.updated_at, r.supervision_policy_json FROM runs r "
                    "WHERE r.state = 'waiting_for_input' AND NOT EXISTS ("
                    "SELECT 1 FROM interactions i WHERE i.run_id = r.id "
                    "AND i.state = 'open') AND NOT EXISTS ("
                    "SELECT 1 FROM run_attempts ra WHERE ra.run_id = r.id "
                    "AND ra.state IN ('preparing', 'running')) "
                    "ORDER BY r.updated_at, r.id"
                ).fetchall()
                for row in waiting:
                    policy = self._stored_supervision_policy(
                        row["supervision_policy_json"]
                    )
                    inserted.extend(
                        self._insert_due_escalations(
                            connection,
                            run_id=row["id"],
                            kind="run.waiting_without_interaction",
                            subject_type="run",
                            subject_id=row["id"],
                            age_started_at=row["updated_at"],
                            thresholds=policy.other_attention_escalation_seconds,
                            channel="control_room",
                            policy=policy,
                            now=now,
                            now_value=now_value,
                        )
                    )

                unavailable = connection.execute(
                    "SELECT r.id AS run_id, r.updated_at, r.supervision_policy_json, "
                    "rpg.authority_name, pg.id AS grant_id, pg.state, "
                    "pg.grant_kind, pg.expires_at, pg.revoked_at, "
                    "rpg.created_at AS authority_created_at, "
                    "pcb.inference_connector_id, c.state AS connector_state, "
                    "c.revoked_at AS connector_revoked_at "
                    "FROM runs r JOIN run_provider_grants rpg "
                    "ON rpg.run_id = r.id JOIN provider_grants pg "
                    "ON pg.id = rpg.provider_grant_id "
                    "LEFT JOIN provider_connector_bindings pcb "
                    "ON pcb.provider_grant_id = pg.id "
                    "LEFT JOIN inference_connectors c "
                    "ON c.id = pcb.inference_connector_id WHERE r.state IN "
                    "('queued', 'preparing', 'running', 'waiting_for_input', "
                    "'paused') "
                    "AND (pg.grant_kind != 'connector' OR pg.state != 'active' OR "
                    "pcb.inference_connector_id IS NULL OR c.state IS NULL OR "
                    "c.state != 'active' OR "
                    "(pg.expires_at IS NOT NULL AND pg.expires_at <= ?)) "
                    "ORDER BY r.updated_at, r.id, rpg.authority_name",
                    (now,),
                ).fetchall()
                for row in unavailable:
                    policy = self._stored_supervision_policy(
                        row["supervision_policy_json"]
                    )
                    unavailable_times: list[str] = []
                    if row["state"] != "active" and row["revoked_at"] is not None:
                        unavailable_times.append(row["revoked_at"])
                    if row["expires_at"] is not None and row["expires_at"] <= now:
                        unavailable_times.append(row["expires_at"])
                    if row["grant_kind"] != "connector":
                        unavailable_times.append(row["authority_created_at"])
                    elif row["connector_state"] != "active":
                        unavailable_times.append(
                            row["connector_revoked_at"] or row["authority_created_at"]
                        )
                    age_started_at = (
                        min(unavailable_times)
                        if unavailable_times
                        else row["updated_at"]
                    )
                    inserted.extend(
                        self._insert_due_escalations(
                            connection,
                            run_id=row["run_id"],
                            kind="authority.unavailable",
                            subject_type="provider_grant",
                            subject_id=(
                                row["run_id"]
                                + ":"
                                + row["grant_id"]
                                + ":"
                                + row["authority_name"]
                            ),
                            age_started_at=age_started_at,
                            thresholds=policy.other_attention_escalation_seconds,
                            channel="control_room",
                            policy=policy,
                            now=now,
                            now_value=now_value,
                        )
                    )
                connection.commit()
            except (ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "supervisor escalations could not be recorded"
                ) from exc
        return [self.get_supervisor_escalation(item_id) for item_id in inserted]

    def _insert_due_escalations(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        kind: str,
        subject_type: str,
        subject_id: str,
        age_started_at: str,
        thresholds: tuple[int, ...],
        channel: str,
        policy: SupervisionPolicy,
        now: str,
        now_value: datetime,
    ) -> list[str]:
        started = self._aware_timestamp(age_started_at, field="attention age")
        elapsed = (now_value - started).total_seconds()
        if elapsed < 0:
            return []
        age_seconds = int(elapsed)
        inserted: list[str] = []
        for threshold in thresholds:
            if threshold > age_seconds:
                continue
            escalation_id = self.new_id("esc")
            local_only = channel == "control_room"
            state = "delivered" if local_only else "pending"
            changed = connection.execute(
                "INSERT OR IGNORE INTO supervisor_escalations "
                "(id, run_id, kind, subject_type, subject_id, threshold_seconds, "
                "age_started_at, channel, state, version, attempt_count, "
                "max_attempts, retry_backoff_json, idempotency_key, available_at, "
                "created_at, updated_at, delivered_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, ?)",
                (
                    escalation_id,
                    run_id,
                    kind,
                    subject_type,
                    subject_id,
                    threshold,
                    age_started_at,
                    channel,
                    state,
                    len(policy.retry_backoff_seconds) + 1,
                    json.dumps(
                        list(policy.retry_backoff_seconds), separators=(",", ":")
                    ),
                    "mgb.escalation:" + escalation_id,
                    now,
                    now,
                    now,
                    now if local_only else None,
                ),
            ).rowcount
            if changed == 1:
                inserted.append(escalation_id)
        return inserted

    def claim_supervisor_escalation(
        self, *, owner_id: str, lease_seconds: int = 60
    ) -> dict[str, Any] | None:
        owner_id = self._supervisor_owner(owner_id)
        self._supervisor_lease_seconds(lease_seconds)
        now = self.now()
        expires_at = self._timestamp_after(now, lease_seconds)
        claimed_id: str | None = None
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE supervisor_escalations SET state = 'exhausted', "
                    "version = version + 1, owner_id = NULL, "
                    "lease_expires_at = NULL, last_error_code = 'LeaseExpired', "
                    "updated_at = ?, delivered_at = ? WHERE state = 'claimed' "
                    "AND lease_expires_at <= ? AND attempt_count >= max_attempts",
                    (now, now, now),
                )
                row = connection.execute(
                    "SELECT id, version FROM supervisor_escalations WHERE "
                    "channel = 'krab_reminder' AND ((state = 'pending' "
                    "AND available_at <= ? AND attempt_count < max_attempts) OR "
                    "(state = 'claimed' AND lease_expires_at <= ? "
                    "AND attempt_count < max_attempts)) "
                    "ORDER BY available_at, created_at, id LIMIT 1",
                    (now, now),
                ).fetchone()
                if row is not None:
                    changed = connection.execute(
                        "UPDATE supervisor_escalations SET state = 'claimed', "
                        "version = version + 1, attempt_count = attempt_count + 1, "
                        "owner_id = ?, lease_expires_at = ?, updated_at = ? "
                        "WHERE id = ? AND version = ?",
                        (owner_id, expires_at, now, row["id"], row["version"]),
                    ).rowcount
                    if changed != 1:
                        raise ConcurrentUpdateError(
                            "supervisor escalation changed concurrently"
                        )
                    claimed_id = row["id"]
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "supervisor escalation could not be claimed"
                ) from exc
        return (
            None if claimed_id is None else self.get_supervisor_escalation(claimed_id)
        )

    def complete_supervisor_escalation(
        self,
        escalation_id: str,
        *,
        owner_id: str,
        expected_version: int,
        krab_event_seq: int,
    ) -> dict[str, Any]:
        if (
            isinstance(krab_event_seq, bool)
            or not isinstance(krab_event_seq, int)
            or krab_event_seq < 1
        ):
            raise ValidationError("krab_event_seq must be a positive integer")
        return self._finish_supervisor_escalation(
            escalation_id,
            owner_id=owner_id,
            expected_version=expected_version,
            outcome="delivered",
            error_code=None,
            krab_event_seq=krab_event_seq,
        )

    def renew_supervisor_escalation_lease(
        self,
        escalation_id: str,
        *,
        owner_id: str,
        expected_version: int,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        """Keep an in-flight reminder claim live without changing its version."""

        escalation_id = require_string(
            escalation_id, field="escalation_id", max_bytes=64
        )
        owner_id = self._supervisor_owner(owner_id)
        self._positive_version(expected_version)
        self._supervisor_lease_seconds(lease_seconds)
        now = self.now()
        expires_at = self._timestamp_after(now, lease_seconds)
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, version, owner_id FROM supervisor_escalations "
                    "WHERE id = ?",
                    (escalation_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(
                        f"supervisor escalation not found: {escalation_id}"
                    )
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"supervisor escalation version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["state"] != "claimed" or row["owner_id"] != owner_id:
                    raise ConflictError(
                        "supervisor escalation is not owned by this process"
                    )
                changed = connection.execute(
                    "UPDATE supervisor_escalations SET lease_expires_at = ?, "
                    "updated_at = ? WHERE id = ? AND state = 'claimed' "
                    "AND version = ? AND owner_id = ?",
                    (expires_at, now, escalation_id, expected_version, owner_id),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "supervisor escalation changed concurrently"
                    )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "supervisor escalation lease could not be renewed"
                ) from exc
        return self.get_supervisor_escalation(escalation_id)

    def dismiss_supervisor_escalation(
        self,
        escalation_id: str,
        *,
        owner_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        return self._finish_supervisor_escalation(
            escalation_id,
            owner_id=owner_id,
            expected_version=expected_version,
            outcome="dismissed",
            error_code=None,
            krab_event_seq=None,
        )

    def fail_supervisor_escalation(
        self,
        escalation_id: str,
        *,
        owner_id: str,
        expected_version: int,
        error_code: str,
    ) -> dict[str, Any]:
        return self._finish_supervisor_escalation(
            escalation_id,
            owner_id=owner_id,
            expected_version=expected_version,
            outcome=None,
            error_code=self._error_code(error_code),
            krab_event_seq=None,
        )

    def _finish_supervisor_escalation(
        self,
        escalation_id: str,
        *,
        owner_id: str,
        expected_version: int,
        outcome: str | None,
        error_code: str | None,
        krab_event_seq: int | None,
    ) -> dict[str, Any]:
        escalation_id = require_string(
            escalation_id, field="escalation_id", max_bytes=64
        )
        owner_id = self._supervisor_owner(owner_id)
        self._positive_version(expected_version)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, version, owner_id, attempt_count, max_attempts, "
                    "retry_backoff_json, available_at FROM supervisor_escalations "
                    "WHERE id = ?",
                    (escalation_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(
                        f"supervisor escalation not found: {escalation_id}"
                    )
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"supervisor escalation version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["state"] != "claimed" or row["owner_id"] != owner_id:
                    raise ConflictError(
                        "supervisor escalation is not owned by this process"
                    )
                if outcome is not None:
                    state = outcome
                    available_at = row["available_at"]
                    delivered_at = now
                elif row["attempt_count"] >= row["max_attempts"]:
                    state = "exhausted"
                    available_at = row["available_at"]
                    delivered_at = now
                else:
                    backoff = self._stored_backoff(row["retry_backoff_json"])
                    index = row["attempt_count"] - 1
                    if index < 0 or index >= len(backoff):
                        raise StorageError(
                            "stored supervisor escalation backoff is invalid"
                        )
                    state = "pending"
                    available_at = self._timestamp_after(now, backoff[index])
                    delivered_at = None
                changed = connection.execute(
                    "UPDATE supervisor_escalations SET state = ?, "
                    "version = version + 1, available_at = ?, owner_id = NULL, "
                    "lease_expires_at = NULL, last_error_code = ?, "
                    "krab_event_seq = ?, updated_at = ?, delivered_at = ? "
                    "WHERE id = ? AND version = ?",
                    (
                        state,
                        available_at,
                        error_code,
                        krab_event_seq,
                        now,
                        delivered_at,
                        escalation_id,
                        expected_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "supervisor escalation changed concurrently"
                    )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "supervisor escalation could not be updated"
                ) from exc
        return self.get_supervisor_escalation(escalation_id)

    def get_supervisor_escalation(self, escalation_id: str) -> dict[str, Any]:
        escalation_id = require_string(
            escalation_id, field="escalation_id", max_bytes=64
        )
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM supervisor_escalations WHERE id = ?",
                (escalation_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"supervisor escalation not found: {escalation_id}")
        return self._supervisor_escalation_record(row)

    def list_supervisor_escalations(self) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM supervisor_escalations ORDER BY created_at DESC, id"
            ).fetchall()
        return [self._supervisor_escalation_record(row) for row in rows]

    @classmethod
    def _supervisor_escalation_record(cls, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["retry_backoff_seconds"] = list(
            cls._stored_backoff(result.pop("retry_backoff_json"))
        )
        return result

    @classmethod
    def _supervisor_action_record(cls, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["retry_backoff_seconds"] = list(
            cls._stored_backoff(result.pop("retry_backoff_json"))
        )
        return result

    @staticmethod
    def _stored_supervision_policy(raw: object) -> SupervisionPolicy:
        try:
            value = json.loads(raw) if isinstance(raw, str) else None
            return SupervisionPolicy.from_mapping(value)
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored supervision policy is invalid") from exc

    @staticmethod
    def _stored_backoff(raw: object) -> tuple[int, ...]:
        try:
            value = json.loads(raw) if isinstance(raw, str) else None
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored supervisor action backoff is invalid") from exc
        if (
            not isinstance(value, list)
            or len(value) > 8
            or any(
                isinstance(entry, bool)
                or not isinstance(entry, int)
                or not 1 <= entry <= 2_592_000
                for entry in value
            )
            or any(
                current <= previous
                for previous, current in zip(value, value[1:], strict=False)
            )
        ):
            raise StorageError("stored supervisor action backoff is invalid")
        return tuple(value)

    @staticmethod
    def _timestamp_after(now: str, seconds: int) -> str:
        current = SupervisionStorage._aware_timestamp(now, field="database clock")
        return (current + timedelta(seconds=seconds)).isoformat(timespec="microseconds")

    @staticmethod
    def _aware_timestamp(value: object, *, field: str) -> datetime:
        if not isinstance(value, str):
            raise StorageError(f"{field} returned an invalid timestamp")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise StorageError(f"{field} returned an invalid timestamp") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise StorageError(f"{field} returned an invalid timestamp")
        return parsed

    @staticmethod
    def _supervisor_owner(value: object) -> str:
        return require_string(value, field="owner_id", max_bytes=128)

    @staticmethod
    def _supervisor_lease_seconds(value: object) -> int:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not MIN_ATTEMPT_LEASE_SECONDS <= value <= MAX_ATTEMPT_LEASE_SECONDS
        ):
            raise ValidationError(
                "lease_seconds must be between "
                f"{MIN_ATTEMPT_LEASE_SECONDS} and {MAX_ATTEMPT_LEASE_SECONDS}"
            )
        return value

    @staticmethod
    def _positive_version(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError("expected_version must be a positive integer")
        return value

    @staticmethod
    def _error_code(value: object) -> str:
        if not isinstance(value, str) or ERROR_CODE_RE.fullmatch(value) is None:
            raise ValidationError("error_code must be a bounded machine code")
        return value
