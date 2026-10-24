from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ..domain import ValidationError, canonical_json, content_sha256, require_string
from ..gates import GatePolicyManifest
from ..integration import (
    FoldManifest,
    IntegrationError,
    LandingAssurance,
    LandingOutcome,
    LandingPlan,
    LandingReceipt,
    LandingReceiptRequest,
    ProviderLandingReceipt,
    ProviderLandingRequest,
)
from ..service_sidecar import (
    ServiceCapability,
    ServiceMapping,
    ServiceOperationReceipt,
    ServiceOperationSpec,
    ServiceSidecarSpec,
    core_service_capability,
)
from .errors import ConcurrentUpdateError, ConflictError, NotFoundError, StorageError

MIN_INTEGRATION_LEASE_SECONDS = 1
MAX_INTEGRATION_LEASE_SECONDS = 3600
MAX_VISIBLE_LANDING_NONCE_GENERATIONS = 32


class IntegrationStorage:
    """Durable dependency, refold, and landing authority."""

    @staticmethod
    def _expected_version(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("expected_version must be a positive integer")
        return value

    @staticmethod
    def _lease_seconds(value: object) -> int:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not MIN_INTEGRATION_LEASE_SECONDS
            <= value
            <= MAX_INTEGRATION_LEASE_SECONDS
        ):
            raise ValueError(
                "lease_seconds must be between "
                f"{MIN_INTEGRATION_LEASE_SECONDS} and "
                f"{MAX_INTEGRATION_LEASE_SECONDS}"
            )
        return value

    @staticmethod
    def _dependency_record(
        connection: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        events = connection.execute(
            "SELECT sequence, kind, actor_id, created_at "
            "FROM work_item_landing_dependency_events "
            "WHERE dependency_id = ? ORDER BY sequence",
            (row["id"],),
        ).fetchall()
        return {
            **dict(row),
            "events": [dict(event) for event in events],
        }

    @staticmethod
    def _dependency_row(
        connection: sqlite3.Connection, dependency_id: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT id, project_id, dependent_work_item_id, "
            "prerequisite_work_item_id, state, version, created_by, removed_by, "
            "created_at, updated_at, removed_at "
            "FROM work_item_landing_dependencies WHERE id = ?",
            (dependency_id,),
        ).fetchone()

    @staticmethod
    def _dependency_mutable(
        connection: sqlite3.Connection, dependent_work_item_id: str
    ) -> None:
        issued = connection.execute(
            "SELECT 1 FROM integration_queue_items "
            "WHERE work_item_id = ? AND state = 'awaiting_receipt' LIMIT 1",
            (dependent_work_item_id,),
        ).fetchone()
        if issued is not None:
            raise ConflictError(
                "landing dependencies cannot change while authority is issued"
            )

    @staticmethod
    def _dependency_would_cycle(
        connection: sqlite3.Connection,
        dependent_work_item_id: str,
        prerequisite_work_item_id: str,
    ) -> bool:
        return (
            connection.execute(
                "WITH RECURSIVE prerequisites(work_item_id) AS ("
                "SELECT prerequisite_work_item_id "
                "FROM work_item_landing_dependencies "
                "WHERE dependent_work_item_id = ? AND state = 'active' "
                "UNION "
                "SELECT d.prerequisite_work_item_id "
                "FROM work_item_landing_dependencies d "
                "JOIN prerequisites p "
                "ON d.dependent_work_item_id = p.work_item_id "
                "WHERE d.state = 'active'"
                ") SELECT 1 FROM prerequisites WHERE work_item_id = ? LIMIT 1",
                (prerequisite_work_item_id, dependent_work_item_id),
            ).fetchone()
            is not None
        )

    def add_landing_dependency(
        self,
        dependent_work_item_id: str,
        prerequisite_work_item_id: str,
        *,
        created_by: str,
    ) -> dict[str, Any]:
        dependent_work_item_id = require_string(
            dependent_work_item_id, field="dependent_work_item_id", max_bytes=64
        )
        prerequisite_work_item_id = require_string(
            prerequisite_work_item_id,
            field="prerequisite_work_item_id",
            max_bytes=64,
        )
        created_by = require_string(created_by, field="created_by", max_bytes=64)
        if dependent_work_item_id == prerequisite_work_item_id:
            raise ConflictError("a work item cannot depend on itself")
        now = self.now()
        dependency_id = self.new_id("ldp")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="dependency creator")
                items = connection.execute(
                    "SELECT id, project_id FROM work_items WHERE id IN (?, ?)",
                    (dependent_work_item_id, prerequisite_work_item_id),
                ).fetchall()
                projects = {row["id"]: row["project_id"] for row in items}
                if dependent_work_item_id not in projects:
                    raise NotFoundError(
                        f"work item not found: {dependent_work_item_id}"
                    )
                if prerequisite_work_item_id not in projects:
                    raise NotFoundError(
                        f"work item not found: {prerequisite_work_item_id}"
                    )
                if (
                    projects[dependent_work_item_id]
                    != projects[prerequisite_work_item_id]
                ):
                    raise ConflictError("landing dependencies require the same project")
                existing = connection.execute(
                    "SELECT id, project_id, dependent_work_item_id, "
                    "prerequisite_work_item_id, state, version, created_by, "
                    "removed_by, created_at, updated_at, removed_at "
                    "FROM work_item_landing_dependencies "
                    "WHERE dependent_work_item_id = ? "
                    "AND prerequisite_work_item_id = ?",
                    (dependent_work_item_id, prerequisite_work_item_id),
                ).fetchone()
                if existing is not None and existing["state"] == "active":
                    connection.commit()
                    return self._dependency_record(connection, existing)
                self._dependency_mutable(connection, dependent_work_item_id)
                if self._dependency_would_cycle(
                    connection,
                    dependent_work_item_id,
                    prerequisite_work_item_id,
                ):
                    raise ConflictError("landing dependency would create a cycle")
                if existing is None:
                    connection.execute(
                        "INSERT INTO work_item_landing_dependencies "
                        "(id, project_id, dependent_work_item_id, "
                        "prerequisite_work_item_id, state, version, created_by, "
                        "created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, 'active', 1, ?, ?, ?)",
                        (
                            dependency_id,
                            projects[dependent_work_item_id],
                            dependent_work_item_id,
                            prerequisite_work_item_id,
                            created_by,
                            now,
                            now,
                        ),
                    )
                    sequence = 1
                else:
                    dependency_id = existing["id"]
                    sequence = existing["version"] + 1
                    connection.execute(
                        "UPDATE work_item_landing_dependencies SET state = 'active', "
                        "version = ?, removed_by = NULL, removed_at = NULL, "
                        "updated_at = ? WHERE id = ?",
                        (sequence, now, dependency_id),
                    )
                connection.execute(
                    "INSERT INTO work_item_landing_dependency_events "
                    "(dependency_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, ?, 'dependency.added', ?, ?)",
                    (dependency_id, sequence, created_by, now),
                )
                row = self._dependency_row(connection, dependency_id)
                assert row is not None
                result = self._dependency_record(connection, row)
                connection.commit()
            except (ConflictError, NotFoundError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("landing dependency could not be recorded") from exc
        return result

    def remove_landing_dependency(
        self,
        dependency_id: str,
        *,
        expected_version: int,
        removed_by: str,
    ) -> dict[str, Any]:
        dependency_id = require_string(
            dependency_id, field="dependency_id", max_bytes=64
        )
        expected_version = self._expected_version(expected_version)
        removed_by = require_string(removed_by, field="removed_by", max_bytes=64)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, removed_by, role="dependency remover")
                row = self._dependency_row(connection, dependency_id)
                if row is None:
                    raise NotFoundError(
                        f"landing dependency not found: {dependency_id}"
                    )
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError("landing dependency version changed")
                if row["state"] == "removed":
                    connection.commit()
                    return self._dependency_record(connection, row)
                self._dependency_mutable(connection, row["dependent_work_item_id"])
                version = expected_version + 1
                connection.execute(
                    "UPDATE work_item_landing_dependencies SET state = 'removed', "
                    "version = ?, removed_by = ?, removed_at = ?, updated_at = ? "
                    "WHERE id = ?",
                    (version, removed_by, now, now, dependency_id),
                )
                connection.execute(
                    "INSERT INTO work_item_landing_dependency_events "
                    "(dependency_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, ?, 'dependency.removed', ?, ?)",
                    (dependency_id, version, removed_by, now),
                )
                updated = self._dependency_row(connection, dependency_id)
                assert updated is not None
                result = self._dependency_record(connection, updated)
                connection.commit()
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                connection.rollback()
                raise
        return result

    def list_landing_dependencies(
        self,
        *,
        project_id: str | None = None,
        active_only: bool = False,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 500
        ):
            raise ValueError("limit must be between 1 and 500")
        if not isinstance(active_only, bool):
            raise TypeError("active_only must be a boolean")
        if project_id is not None:
            project_id = require_string(project_id, field="project_id", max_bytes=64)
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT id, project_id, dependent_work_item_id, "
                "prerequisite_work_item_id, state, version, created_by, "
                "removed_by, created_at, updated_at, removed_at "
                "FROM work_item_landing_dependencies "
                "WHERE (? IS NULL OR project_id = ?) "
                "AND (? = 0 OR state = 'active') "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                (project_id, project_id, int(active_only), limit),
            ).fetchall()
            return [self._dependency_record(connection, row) for row in rows]

    @staticmethod
    def _integration_ref_record(row: sqlite3.Row) -> dict[str, Any]:
        return dict(row)

    @staticmethod
    def _integration_ref_row(
        connection: sqlite3.Connection, integration_ref_id: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT id, project_id, repository_identity, integration_ref, "
            "object_format, current_revision, version, created_by, "
            "created_at, updated_at FROM integration_refs WHERE id = ?",
            (integration_ref_id,),
        ).fetchone()

    @staticmethod
    def _queue_row(
        connection: sqlite3.Connection, queue_item_id: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT q.id, q.integration_ref_id, q.project_id, q.work_item_id, "
            "q.work_thread_id, q.candidate_id, q.current_policy_id, "
            "q.current_fold_id, q.state, q.version, q.event_sequence, "
            "q.owner_id, q.lease_expires_at, q.landed_receipt_id, "
            "q.created_by, q.created_at, q.updated_at, q.landed_at, "
            "q.cancelled_at, r.integration_ref, r.object_format, "
            "r.current_revision, r.version AS integration_ref_version "
            "FROM integration_queue_items q JOIN integration_refs r "
            "ON r.id = q.integration_ref_id WHERE q.id = ?",
            (queue_item_id,),
        ).fetchone()

    @staticmethod
    def _queue_record(
        connection: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        events = connection.execute(
            "SELECT sequence, kind, from_state, to_state, actor, metadata_json, "
            "created_at FROM integration_queue_events "
            "WHERE queue_item_id = ? ORDER BY sequence",
            (row["id"],),
        ).fetchall()
        result = dict(row)
        result["events"] = []
        for event in events:
            item = dict(event)
            try:
                item["metadata"] = json.loads(item.pop("metadata_json"))
            except (json.JSONDecodeError, TypeError) as exc:
                raise StorageError("stored integration queue event is invalid") from exc
            result["events"].append(item)
        return result

    @staticmethod
    def _queue_event(
        connection: sqlite3.Connection,
        *,
        queue_item_id: str,
        sequence: int,
        kind: str,
        from_state: str | None,
        to_state: str,
        actor: str,
        metadata: dict[str, Any],
        created_at: str,
    ) -> None:
        connection.execute(
            "INSERT INTO integration_queue_events "
            "(queue_item_id, sequence, kind, from_state, to_state, actor, "
            "metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                queue_item_id,
                sequence,
                kind,
                from_state,
                to_state,
                actor,
                canonical_json(metadata),
                created_at,
            ),
        )

    def enqueue_integration(self, policy_id: str, *, created_by: str) -> dict[str, Any]:
        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        created_by = require_string(created_by, field="created_by", max_bytes=64)
        queue_item_id = self.new_id("iqm")
        integration_ref_id = self.new_id("irf")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="queue creator")
                policy_row = connection.execute(
                    f"SELECT {self._policy_columns()} "  # noqa: S608
                    "FROM integration_gate_policies WHERE id = ?",
                    (policy_id,),
                ).fetchone()
                if policy_row is None:
                    raise NotFoundError(f"gate policy not found: {policy_id}")
                policy_record = self._policy_record(policy_row)
                policy = GatePolicyManifest.from_mapping(policy_record["manifest"])
                candidate = connection.execute(
                    "SELECT ic.id, ic.project_id, ic.work_thread_id, "
                    "wt.work_item_id FROM integration_candidates ic "
                    "JOIN work_threads wt ON wt.id = ic.work_thread_id "
                    "WHERE ic.id = ?",
                    (policy.candidate_id,),
                ).fetchone()
                if candidate is None:
                    raise StorageError("gate policy candidate is unavailable")
                ref = connection.execute(
                    "SELECT id, project_id, repository_identity, integration_ref, "
                    "object_format, current_revision, version, created_by, "
                    "created_at, updated_at FROM integration_refs "
                    "WHERE project_id = ? AND integration_ref = ?",
                    (policy.project_id, policy.integration_ref),
                ).fetchone()
                if ref is None:
                    connection.execute(
                        "INSERT INTO integration_refs "
                        "(id, project_id, repository_identity, integration_ref, "
                        "object_format, current_revision, version, created_by, "
                        "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)",
                        (
                            integration_ref_id,
                            policy.project_id,
                            policy.repository_identity,
                            policy.integration_ref,
                            policy.object_format,
                            policy.expected_base_revision,
                            created_by,
                            now,
                            now,
                        ),
                    )
                    current_revision = policy.expected_base_revision
                else:
                    integration_ref_id = ref["id"]
                    current_revision = ref["current_revision"]
                    if ref["object_format"] != policy.object_format:
                        raise ConflictError("integration ref object format changed")
                existing = connection.execute(
                    "SELECT id, current_policy_id, state FROM integration_queue_items "
                    "WHERE integration_ref_id = ? AND candidate_id = ?",
                    (integration_ref_id, policy.candidate_id),
                ).fetchone()
                if existing is not None:
                    if existing["current_policy_id"] != policy_id:
                        raise ConflictError(
                            "integration candidate is already queued with another "
                            "policy"
                        )
                    if existing["state"] == "cancelled":
                        raise ConflictError(
                            "cancelled integration candidate cannot be re-enqueued"
                        )
                    row = self._queue_row(connection, existing["id"])
                    connection.commit()
                    assert row is not None
                    return self._queue_record(connection, row)
                state = (
                    "queued"
                    if current_revision == policy.expected_base_revision
                    else "refold_required"
                )
                connection.execute(
                    "INSERT INTO integration_queue_items "
                    "(id, integration_ref_id, project_id, work_item_id, "
                    "work_thread_id, candidate_id, current_policy_id, state, "
                    "version, event_sequence, created_by, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?)",
                    (
                        queue_item_id,
                        integration_ref_id,
                        policy.project_id,
                        candidate["work_item_id"],
                        policy.work_thread_id,
                        policy.candidate_id,
                        policy_id,
                        state,
                        created_by,
                        now,
                        now,
                    ),
                )
                self._queue_event(
                    connection,
                    queue_item_id=queue_item_id,
                    sequence=1,
                    kind="queue.created",
                    from_state=None,
                    to_state=state,
                    actor=created_by,
                    metadata={"policy_id": policy_id},
                    created_at=now,
                )
                row = self._queue_row(connection, queue_item_id)
                assert row is not None
                result = self._queue_record(connection, row)
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except (IntegrationError, sqlite3.IntegrityError) as exc:
                connection.rollback()
                raise ConflictError("integration item could not be queued") from exc
        return result

    def get_integration_queue_item(self, queue_item_id: str) -> dict[str, Any]:
        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        with self._read_snapshot() as connection:
            row = self._queue_row(connection, queue_item_id)
            if row is None:
                raise NotFoundError(
                    f"integration queue item not found: {queue_item_id}"
                )
            return self._queue_record(connection, row)

    def list_integration_queue(
        self, *, project_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 500
        ):
            raise ValueError("limit must be between 1 and 500")
        if project_id is not None:
            project_id = require_string(project_id, field="project_id", max_bytes=64)
        with self._read_snapshot() as connection:
            if project_id is None:
                rows = connection.execute(
                    "SELECT id FROM integration_queue_items "
                    "ORDER BY created_at DESC, id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT id FROM integration_queue_items WHERE project_id = ? "
                    "ORDER BY created_at DESC, id DESC LIMIT ?",
                    (project_id, limit),
                ).fetchall()
            result = []
            for item in rows:
                row = self._queue_row(connection, item["id"])
                assert row is not None
                result.append(self._queue_record(connection, row))
            return result

    def _assert_queue_authority_ready(
        self, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        if row["state"] == "refold_required":
            raise ConflictError("integration item requires a fresh refold")
        if row["state"] not in {"queued", "claimed"}:
            raise ConflictError("integration item is not available for authority")
        policy_row = connection.execute(
            f"SELECT {self._policy_columns()} "  # noqa: S608
            "FROM integration_gate_policies WHERE id = ?",
            (row["current_policy_id"],),
        ).fetchone()
        if policy_row is None:
            raise StorageError("integration item policy is unavailable")
        policy_record = self._policy_record(policy_row)
        policy = GatePolicyManifest.from_mapping(policy_record["manifest"])
        expected = {
            "candidate_id": row["candidate_id"],
            "project_id": row["project_id"],
            "work_thread_id": row["work_thread_id"],
            "integration_ref": row["integration_ref"],
            "expected_base_revision": row["current_revision"],
            "object_format": row["object_format"],
        }
        if any(getattr(policy, key) != value for key, value in expected.items()):
            raise ConflictError("integration policy does not match the current ref")
        blocked = connection.execute(
            "SELECT 1 FROM work_item_landing_dependencies d "
            "WHERE d.dependent_work_item_id = ? AND d.state = 'active' "
            "AND NOT EXISTS (SELECT 1 FROM integration_queue_items prerequisite "
            "WHERE prerequisite.integration_ref_id = ? "
            "AND prerequisite.work_item_id = d.prerequisite_work_item_id "
            "AND prerequisite.state = 'landed') LIMIT 1",
            (row["work_item_id"], row["integration_ref_id"]),
        ).fetchone()
        if blocked is not None:
            raise ConflictError("integration item landing dependencies are not met")
        readiness = self._gate_readiness(connection, row["current_policy_id"])
        if not readiness["ready"]:
            raise ConflictError("integration item gates are not ready")
        return readiness

    def _expire_ref_claims(
        self,
        connection: sqlite3.Connection,
        integration_ref_id: str,
        *,
        now: str,
    ) -> None:
        rows = connection.execute(
            "SELECT id FROM integration_queue_items WHERE integration_ref_id = ? "
            "AND state = 'claimed' AND lease_expires_at <= ?",
            (integration_ref_id, now),
        ).fetchall()
        for item in rows:
            row = self._queue_row(connection, item["id"])
            assert row is not None
            version = row["version"] + 1
            sequence = row["event_sequence"] + 1
            connection.execute(
                "UPDATE integration_queue_items SET state = 'queued', version = ?, "
                "event_sequence = ?, owner_id = NULL, lease_expires_at = NULL, "
                "updated_at = ? WHERE id = ?",
                (version, sequence, now, row["id"]),
            )
            self._queue_event(
                connection,
                queue_item_id=row["id"],
                sequence=sequence,
                kind="queue.claim_expired",
                from_state="claimed",
                to_state="queued",
                actor="system",
                metadata={"expired_owner_id": row["owner_id"]},
                created_at=now,
            )

    def claim_integration_item(
        self,
        queue_item_id: str,
        *,
        owner_id: str,
        expected_version: int,
        lease_seconds: int,
    ) -> dict[str, Any]:
        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        owner_id = require_string(owner_id, field="owner_id", max_bytes=128)
        expected_version = self._expected_version(expected_version)
        lease_seconds = self._lease_seconds(lease_seconds)
        now = self.now()
        expires = (
            datetime.fromisoformat(now) + timedelta(seconds=lease_seconds)
        ).isoformat(timespec="microseconds")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = self._queue_row(connection, queue_item_id)
                if row is None:
                    raise NotFoundError(
                        f"integration queue item not found: {queue_item_id}"
                    )
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError("integration queue version changed")
                self._expire_ref_claims(connection, row["integration_ref_id"], now=now)
                row = self._queue_row(connection, queue_item_id)
                assert row is not None
                if row["state"] == "claimed":
                    raise ConflictError("integration item is already claimed")
                self._assert_queue_authority_ready(connection, row)
                version = row["version"] + 1
                sequence = row["event_sequence"] + 1
                connection.execute(
                    "UPDATE integration_queue_items SET state = 'claimed', "
                    "version = ?, event_sequence = ?, owner_id = ?, "
                    "lease_expires_at = ?, updated_at = ? WHERE id = ?",
                    (version, sequence, owner_id, expires, now, queue_item_id),
                )
                self._queue_event(
                    connection,
                    queue_item_id=queue_item_id,
                    sequence=sequence,
                    kind="queue.claimed",
                    from_state="queued",
                    to_state="claimed",
                    actor=owner_id,
                    metadata={"lease_expires_at": expires},
                    created_at=now,
                )
                updated = self._queue_row(connection, queue_item_id)
                assert updated is not None
                result = self._queue_record(connection, updated)
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("integration ref is already claimed") from exc
        return result

    def release_integration_claim(
        self,
        queue_item_id: str,
        *,
        owner_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        owner_id = require_string(owner_id, field="owner_id", max_bytes=128)
        expected_version = self._expected_version(expected_version)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = self._queue_row(connection, queue_item_id)
                if row is None:
                    raise NotFoundError(
                        f"integration queue item not found: {queue_item_id}"
                    )
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError("integration queue version changed")
                if row["state"] != "claimed" or row["owner_id"] != owner_id:
                    raise ConflictError("integration claim is not owned by caller")
                version = expected_version + 1
                sequence = row["event_sequence"] + 1
                connection.execute(
                    "UPDATE integration_queue_items SET state = 'queued', "
                    "version = ?, event_sequence = ?, owner_id = NULL, "
                    "lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                    (version, sequence, now, queue_item_id),
                )
                self._queue_event(
                    connection,
                    queue_item_id=queue_item_id,
                    sequence=sequence,
                    kind="queue.claim_released",
                    from_state="claimed",
                    to_state="queued",
                    actor=owner_id,
                    metadata={},
                    created_at=now,
                )
                updated = self._queue_row(connection, queue_item_id)
                assert updated is not None
                result = self._queue_record(connection, updated)
                connection.commit()
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                connection.rollback()
                raise
        return result

    def cancel_integration_item(
        self,
        queue_item_id: str,
        *,
        expected_version: int,
        cancelled_by: str,
    ) -> dict[str, Any]:
        """Cancel queued integration work before manual authority is issued."""

        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        expected_version = self._expected_version(expected_version)
        cancelled_by = require_string(cancelled_by, field="cancelled_by", max_bytes=64)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, cancelled_by, role="integration canceller"
                )
                row = self._queue_row(connection, queue_item_id)
                if row is None:
                    raise NotFoundError(
                        f"integration queue item not found: {queue_item_id}"
                    )
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError("integration queue version changed")
                if row["state"] == "awaiting_receipt":
                    raise ConflictError(
                        "integration item cannot be cancelled after landing "
                        "authority has been issued"
                    )
                if row["state"] not in {"queued", "claimed", "refold_required"}:
                    raise ConflictError(
                        f"integration item cannot be cancelled from {row['state']}"
                    )
                version = expected_version + 1
                sequence = row["event_sequence"] + 1
                metadata = (
                    {"cancelled_owner_id": row["owner_id"]}
                    if row["owner_id"] is not None
                    else {}
                )
                connection.execute(
                    "UPDATE integration_queue_items SET state = 'cancelled', "
                    "version = ?, event_sequence = ?, owner_id = NULL, "
                    "lease_expires_at = NULL, cancelled_at = ?, updated_at = ? "
                    "WHERE id = ?",
                    (version, sequence, now, now, queue_item_id),
                )
                self._queue_event(
                    connection,
                    queue_item_id=queue_item_id,
                    sequence=sequence,
                    kind="queue.cancelled",
                    from_state=row["state"],
                    to_state="cancelled",
                    actor=cancelled_by,
                    metadata=metadata,
                    created_at=now,
                )
                updated = self._queue_row(connection, queue_item_id)
                assert updated is not None
                result = self._queue_record(connection, updated)
                connection.commit()
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                connection.rollback()
                raise
        return result

    @staticmethod
    def _fold_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            manifest = FoldManifest.from_mapping(json.loads(row["manifest_json"]))
        except (IntegrationError, TypeError, json.JSONDecodeError) as exc:
            raise StorageError("stored integration fold is invalid") from exc
        if manifest.digest != row["content_sha256"]:
            raise StorageError("stored integration fold digest does not match")
        return {
            "id": row["id"],
            "queue_item_id": row["queue_item_id"],
            "candidate_id": row["candidate_id"],
            "generation": row["generation"],
            "manifest": manifest.to_dict(),
            "content_sha256": row["content_sha256"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _fold_row(connection: sqlite3.Connection, fold_id: str) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT id, queue_item_id, candidate_id, generation, schema_version, "
            "manifest_json, content_sha256, project_id, context_version_id, "
            "work_thread_id, integration_ref, base_revision, head_revision, "
            "tree_revision, object_format, result_content_sha256, created_by, "
            "created_at FROM integration_folds WHERE id = ?",
            (fold_id,),
        ).fetchone()

    def record_integration_fold(
        self,
        queue_item_id: str,
        manifest: FoldManifest,
        *,
        expected_version: int,
        created_by: str,
    ) -> dict[str, Any]:
        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        expected_version = self._expected_version(expected_version)
        if not isinstance(manifest, FoldManifest):
            raise IntegrationError("integration fold manifest is invalid")
        created_by = require_string(created_by, field="created_by", max_bytes=64)
        fold_id = self.new_id("ifd")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="fold creator")
                queue = self._queue_row(connection, queue_item_id)
                if queue is None:
                    raise NotFoundError(
                        f"integration queue item not found: {queue_item_id}"
                    )
                if queue["version"] != expected_version:
                    raise ConcurrentUpdateError("integration queue version changed")
                if queue["state"] != "refold_required":
                    raise ConflictError("integration item does not require a refold")
                candidate = connection.execute(
                    "SELECT context_version_id, result_content_sha256 "
                    "FROM integration_candidates "
                    "WHERE id = ?",
                    (queue["candidate_id"],),
                ).fetchone()
                if candidate is None:
                    raise StorageError("integration candidate is unavailable")
                expected = {
                    "candidate_id": queue["candidate_id"],
                    "project_id": queue["project_id"],
                    "context_version_id": candidate["context_version_id"],
                    "work_thread_id": queue["work_thread_id"],
                    "integration_ref": queue["integration_ref"],
                    "base_revision": queue["current_revision"],
                    "object_format": queue["object_format"],
                    "result_content_sha256": candidate["result_content_sha256"],
                }
                if any(
                    getattr(manifest, key) != value for key, value in expected.items()
                ):
                    raise ConflictError(
                        "integration fold does not match the current ref"
                    )
                equivalent = connection.execute(
                    "SELECT id FROM integration_folds WHERE candidate_id = ? "
                    "AND integration_ref = ? AND base_revision = ? "
                    "AND head_revision = ?",
                    (
                        manifest.candidate_id,
                        manifest.integration_ref,
                        manifest.base_revision,
                        manifest.head_revision,
                    ),
                ).fetchone()
                if equivalent is not None:
                    row = self._fold_row(connection, equivalent["id"])
                    assert row is not None
                    record = self._fold_record(row)
                    recorded_manifest = dict(record["manifest"])
                    proposed_manifest = manifest.to_dict()
                    recorded_manifest.pop("generation")
                    proposed_manifest.pop("generation")
                    if recorded_manifest != proposed_manifest:
                        raise ConflictError("integration fold identity conflicts")
                    connection.commit()
                    return record
                existing = connection.execute(
                    "SELECT id FROM integration_folds WHERE queue_item_id = ? "
                    "AND content_sha256 = ?",
                    (queue_item_id, manifest.digest),
                ).fetchone()
                if existing is not None:
                    row = self._fold_row(connection, existing["id"])
                    connection.commit()
                    assert row is not None
                    return self._fold_record(row)
                generation = connection.execute(
                    "SELECT COALESCE(MAX(generation), 0) + 1 AS generation "
                    "FROM integration_folds WHERE queue_item_id = ?",
                    (queue_item_id,),
                ).fetchone()["generation"]
                if manifest.generation != generation:
                    raise ConflictError("integration fold generation changed")
                connection.execute(
                    "INSERT INTO integration_folds "
                    "(id, queue_item_id, candidate_id, generation, schema_version, "
                    "manifest_json, content_sha256, project_id, context_version_id, "
                    "work_thread_id, integration_ref, base_revision, head_revision, "
                    "tree_revision, object_format, result_content_sha256, "
                    "created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        fold_id,
                        queue_item_id,
                        manifest.candidate_id,
                        manifest.generation,
                        manifest.schema_version,
                        manifest.canonical,
                        manifest.digest,
                        manifest.project_id,
                        manifest.context_version_id,
                        manifest.work_thread_id,
                        manifest.integration_ref,
                        manifest.base_revision,
                        manifest.head_revision,
                        manifest.tree_revision,
                        manifest.object_format,
                        manifest.result_content_sha256,
                        created_by,
                        now,
                    ),
                )
                row = self._fold_row(connection, fold_id)
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("integration fold could not be recorded") from exc
        assert row is not None
        return self._fold_record(row)

    def get_integration_fold(self, fold_id: str) -> dict[str, Any]:
        fold_id = require_string(fold_id, field="fold_id", max_bytes=64)
        with self._read_snapshot() as connection:
            row = self._fold_row(connection, fold_id)
            if row is None:
                raise NotFoundError(f"integration fold not found: {fold_id}")
            return self._fold_record(row)

    def next_integration_fold_generation(self, queue_item_id: str) -> int:
        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        with self._read_snapshot() as connection:
            if self._queue_row(connection, queue_item_id) is None:
                raise NotFoundError(
                    f"integration queue item not found: {queue_item_id}"
                )
            return connection.execute(
                "SELECT COALESCE(MAX(generation), 0) + 1 AS generation "
                "FROM integration_folds WHERE queue_item_id = ?",
                (queue_item_id,),
            ).fetchone()["generation"]

    def activate_integration_fold(
        self,
        queue_item_id: str,
        *,
        fold_id: str,
        policy_id: str,
        expected_version: int,
        actor: str,
    ) -> dict[str, Any]:
        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        fold_id = require_string(fold_id, field="fold_id", max_bytes=64)
        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        actor = require_string(actor, field="actor", max_bytes=64)
        expected_version = self._expected_version(expected_version)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, actor, role="fold activator")
                queue = self._queue_row(connection, queue_item_id)
                if queue is None:
                    raise NotFoundError(
                        f"integration queue item not found: {queue_item_id}"
                    )
                if queue["version"] != expected_version:
                    raise ConcurrentUpdateError("integration queue version changed")
                if queue["state"] != "refold_required":
                    raise ConflictError("integration item does not require a refold")
                fold_row = self._fold_row(connection, fold_id)
                if fold_row is None:
                    raise NotFoundError(f"integration fold not found: {fold_id}")
                fold = FoldManifest.from_mapping(json.loads(fold_row["manifest_json"]))
                if (
                    fold_row["queue_item_id"] != queue_item_id
                    or fold.base_revision != queue["current_revision"]
                ):
                    raise ConflictError(
                        "integration fold is stale or belongs elsewhere"
                    )
                policy_row = connection.execute(
                    f"SELECT {self._policy_columns()} "  # noqa: S608
                    "FROM integration_gate_policies WHERE id = ?",
                    (policy_id,),
                ).fetchone()
                if policy_row is None:
                    raise NotFoundError(f"gate policy not found: {policy_id}")
                policy = GatePolicyManifest.from_mapping(
                    self._policy_record(policy_row)["manifest"]
                )
                exact = {
                    "candidate_id": fold.candidate_id,
                    "project_id": fold.project_id,
                    "context_version_id": fold.context_version_id,
                    "work_thread_id": fold.work_thread_id,
                    "integration_ref": fold.integration_ref,
                    "expected_base_revision": fold.base_revision,
                    "head_revision": fold.head_revision,
                    "tree_revision": fold.tree_revision,
                    "object_format": fold.object_format,
                }
                if any(getattr(policy, key) != value for key, value in exact.items()):
                    raise ConflictError("fresh gate policy does not match the refold")
                version = expected_version + 1
                sequence = queue["event_sequence"] + 1
                connection.execute(
                    "UPDATE integration_queue_items SET current_policy_id = ?, "
                    "current_fold_id = ?, state = 'queued', version = ?, "
                    "event_sequence = ?, updated_at = ? WHERE id = ?",
                    (
                        policy_id,
                        fold_id,
                        version,
                        sequence,
                        now,
                        queue_item_id,
                    ),
                )
                self._queue_event(
                    connection,
                    queue_item_id=queue_item_id,
                    sequence=sequence,
                    kind="queue.refolded",
                    from_state="refold_required",
                    to_state="queued",
                    actor=actor,
                    metadata={"fold_id": fold_id, "policy_id": policy_id},
                    created_at=now,
                )
                updated = self._queue_row(connection, queue_item_id)
                assert updated is not None
                result = self._queue_record(connection, updated)
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                IntegrationError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
        return result

    @staticmethod
    def _plan_nonce_rows(
        connection: sqlite3.Connection, plan_id: str
    ) -> list[sqlite3.Row]:
        newest_first = connection.execute(
            "SELECT generation, nonce_sha256, issued_by, created_at "
            "FROM integration_landing_plan_nonces WHERE plan_id = ? "
            "ORDER BY generation DESC LIMIT ?",
            (plan_id, MAX_VISIBLE_LANDING_NONCE_GENERATIONS),
        ).fetchall()
        return list(reversed(newest_first))

    @staticmethod
    def _plan_record(row: sqlite3.Row, nonce_rows: list[sqlite3.Row]) -> dict[str, Any]:
        try:
            plan = LandingPlan.from_mapping(json.loads(row["plan_json"]))
            evidence = json.loads(row["evidence_snapshot_json"])
        except (IntegrationError, TypeError, json.JSONDecodeError) as exc:
            raise StorageError("stored landing plan is invalid") from exc
        if (
            plan.digest != row["content_sha256"]
            or content_sha256(canonical_json(evidence))
            != row["evidence_snapshot_sha256"]
        ):
            raise StorageError("stored landing plan digest does not match")
        if not nonce_rows:
            raise StorageError("stored landing plan nonce history is invalid")
        current_nonce = nonce_rows[-1]
        return {
            "id": row["id"],
            "queue_item_id": row["queue_item_id"],
            "sequence": row["sequence"],
            "plan": plan.to_dict(),
            "content_sha256": row["content_sha256"],
            "nonce_generation": current_nonce["generation"],
            "nonce_sha256": current_nonce["nonce_sha256"],
            "nonce_generations": [dict(item) for item in nonce_rows],
            "evidence_snapshot": evidence,
            "evidence_snapshot_sha256": row["evidence_snapshot_sha256"],
            "actuator": row["actuator"],
            "assurance": row["assurance"],
            "state": row["state"],
            "issued_by": row["issued_by"],
            "created_at": row["created_at"],
            "resolved_at": row["resolved_at"],
        }

    @staticmethod
    def _plan_row(connection: sqlite3.Connection, plan_id: str) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT id, queue_item_id, sequence, schema_version, plan_json, "
            "content_sha256, nonce_sha256, evidence_snapshot_json, "
            "evidence_snapshot_sha256, actuator, assurance, state, issued_by, "
            "created_at, resolved_at FROM integration_landing_plans WHERE id = ?",
            (plan_id,),
        ).fetchone()

    def issue_landing_plan(
        self,
        queue_item_id: str,
        *,
        owner_id: str,
        expected_version: int,
        candidate_repository: Path,
        remote: str,
        issued_by: str,
        workflow_execution_id: str | None = None,
        workflow_execution_version: int | None = None,
    ) -> dict[str, Any]:
        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        owner_id = require_string(owner_id, field="owner_id", max_bytes=128)
        issued_by = require_string(issued_by, field="issued_by", max_bytes=64)
        expected_version = self._expected_version(expected_version)
        if (workflow_execution_id is None) != (workflow_execution_version is None):
            raise ValidationError(
                "workflow landing authority must be supplied together"
            )
        if workflow_execution_id is not None:
            workflow_execution_id = require_string(
                workflow_execution_id,
                field="workflow_execution_id",
                max_bytes=64,
            )
            workflow_execution_version = self._expected_version(
                workflow_execution_version
            )
        plan_id = self.new_id("lpl")
        nonce = secrets.token_hex(32)
        nonce_sha256 = hashlib.sha256(nonce.encode("ascii")).hexdigest()
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, issued_by, role="landing plan issuer")
                queue = self._queue_row(connection, queue_item_id)
                if queue is None:
                    raise NotFoundError(
                        f"integration queue item not found: {queue_item_id}"
                    )
                if queue["version"] != expected_version:
                    raise ConcurrentUpdateError("integration queue version changed")
                if workflow_execution_id is not None:
                    workflow = connection.execute(
                        "SELECT e.state, e.stage, e.version, cv.spec_json "
                        "FROM workflow_executions e "
                        "JOIN workflow_configuration_versions cv "
                        "ON cv.id = e.configuration_version_id "
                        "JOIN workflow_landing_bindings b ON b.execution_id = e.id "
                        "WHERE e.id = ? AND b.queue_item_id = ?",
                        (workflow_execution_id, queue_item_id),
                    ).fetchone()
                    if workflow is None:
                        raise ConflictError(
                            "manual landing is not bound to this workflow"
                        )
                    if workflow["version"] != workflow_execution_version:
                        raise ConcurrentUpdateError(
                            "workflow execution version changed"
                        )
                    if workflow["state"] != "active" or workflow["stage"] != "landing":
                        raise ConflictError("workflow is not awaiting landing")
                    try:
                        landing_mode = json.loads(workflow["spec_json"])["landing_mode"]
                    except (json.JSONDecodeError, KeyError, TypeError) as exc:
                        raise StorageError(
                            "stored workflow configuration is invalid"
                        ) from exc
                    if landing_mode != "manual":
                        raise ConflictError(
                            "workflow is configured for provider landing"
                        )
                if queue["state"] != "claimed" or queue["owner_id"] != owner_id:
                    raise ConflictError("integration claim is not owned by caller")
                if queue["lease_expires_at"] <= now:
                    raise ConflictError("integration claim lease expired")
                readiness = self._assert_queue_authority_ready(connection, queue)
                policy_row = connection.execute(
                    f"SELECT {self._policy_columns()} "  # noqa: S608
                    "FROM integration_gate_policies WHERE id = ?",
                    (queue["current_policy_id"],),
                ).fetchone()
                assert policy_row is not None
                policy_record = self._policy_record(policy_row)
                policy = GatePolicyManifest.from_mapping(policy_record["manifest"])
                evidence_json = canonical_json(readiness)
                evidence_sha256 = content_sha256(evidence_json)
                awaiting_version = expected_version + 1
                plan = LandingPlan.create(
                    queue_item_id=queue_item_id,
                    queue_version=awaiting_version,
                    policy_id=queue["current_policy_id"],
                    policy_revision=policy.policy_revision,
                    candidate_id=queue["candidate_id"],
                    fold_id=queue["current_fold_id"],
                    repository_identity=queue["project_id"],
                    integration_ref=queue["integration_ref"],
                    expected_base_revision=policy.expected_base_revision,
                    head_revision=policy.head_revision,
                    tree_revision=policy.tree_revision,
                    object_format=policy.object_format,
                    evidence_snapshot_sha256=evidence_sha256,
                    candidate_repository=candidate_repository,
                    remote=remote,
                )
                plan_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM integration_landing_plans WHERE queue_item_id = ?",
                    (queue_item_id,),
                ).fetchone()["sequence"]
                connection.execute(
                    "INSERT INTO integration_landing_plans "
                    "(id, queue_item_id, sequence, schema_version, plan_json, "
                    "content_sha256, nonce_sha256, evidence_snapshot_json, "
                    "evidence_snapshot_sha256, actuator, assurance, state, "
                    "issued_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, 'pending', ?, ?)",
                    (
                        plan_id,
                        queue_item_id,
                        plan_sequence,
                        plan.schema_version,
                        plan.canonical,
                        plan.digest,
                        nonce_sha256,
                        evidence_json,
                        evidence_sha256,
                        plan.actuator,
                        plan.assurance.value,
                        issued_by,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO integration_landing_plan_nonces "
                    "(plan_id, generation, nonce_sha256, issued_by, created_at) "
                    "VALUES (?, 1, ?, ?, ?)",
                    (plan_id, nonce_sha256, issued_by, now),
                )
                event_sequence = queue["event_sequence"] + 1
                connection.execute(
                    "UPDATE integration_queue_items SET state = 'awaiting_receipt', "
                    "version = ?, event_sequence = ?, owner_id = NULL, "
                    "lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                    (awaiting_version, event_sequence, now, queue_item_id),
                )
                self._queue_event(
                    connection,
                    queue_item_id=queue_item_id,
                    sequence=event_sequence,
                    kind="queue.plan_issued",
                    from_state="claimed",
                    to_state="awaiting_receipt",
                    actor=issued_by,
                    metadata={
                        "plan_id": plan_id,
                        "request_sha256": plan.digest,
                        "evidence_snapshot_sha256": evidence_sha256,
                    },
                    created_at=now,
                )
                plan_row = self._plan_row(connection, plan_id)
                nonce_rows = self._plan_nonce_rows(connection, plan_id)
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                IntegrationError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("landing plan could not be issued") from exc
        assert plan_row is not None
        return {**self._plan_record(plan_row, nonce_rows), "nonce": nonce}

    def get_landing_plan(self, plan_id: str) -> dict[str, Any]:
        plan_id = require_string(plan_id, field="plan_id", max_bytes=64)
        with self._read_snapshot() as connection:
            row = self._plan_row(connection, plan_id)
            if row is None:
                raise NotFoundError(f"landing plan not found: {plan_id}")
            return self._plan_record(row, self._plan_nonce_rows(connection, plan_id))

    def reissue_landing_plan_nonce(
        self,
        plan_id: str,
        *,
        expected_generation: int,
        reissued_by: str,
    ) -> dict[str, Any]:
        plan_id = require_string(plan_id, field="plan_id", max_bytes=64)
        if (
            isinstance(expected_generation, bool)
            or not isinstance(expected_generation, int)
            or expected_generation < 1
        ):
            raise ValueError("expected_generation must be a positive integer")
        reissued_by = require_string(reissued_by, field="reissued_by", max_bytes=64)
        nonce = secrets.token_hex(32)
        nonce_sha256 = hashlib.sha256(nonce.encode("ascii")).hexdigest()
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, reissued_by, role="plan nonce issuer")
                plan_row = self._plan_row(connection, plan_id)
                if plan_row is None:
                    raise NotFoundError(f"landing plan not found: {plan_id}")
                if plan_row["state"] != "pending":
                    raise ConflictError("landing plan is already resolved")
                nonce_rows = self._plan_nonce_rows(connection, plan_id)
                if not nonce_rows:
                    raise StorageError("landing plan nonce history is unavailable")
                current_generation = nonce_rows[-1]["generation"]
                if current_generation != expected_generation:
                    raise ConcurrentUpdateError("landing plan nonce generation changed")
                connection.execute(
                    "INSERT INTO integration_landing_plan_nonces "
                    "(plan_id, generation, nonce_sha256, issued_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        plan_id,
                        current_generation + 1,
                        nonce_sha256,
                        reissued_by,
                        now,
                    ),
                )
                nonce_rows = self._plan_nonce_rows(connection, plan_id)
                result = {
                    **self._plan_record(plan_row, nonce_rows),
                    "nonce": nonce,
                }
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("landing plan nonce could not be reissued") from exc
        return result

    @staticmethod
    def _provider_landing_row(
        connection: sqlite3.Connection, provider_landing_id: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT id, queue_item_id, sequence, schema_version, request_json, "
            "content_sha256, evidence_snapshot_json, evidence_snapshot_sha256, "
            "service_operation_id, capability_binding_id, assurance, state, "
            "issued_by, created_at, resolved_at "
            "FROM integration_provider_landings WHERE id = ?",
            (provider_landing_id,),
        ).fetchone()

    @staticmethod
    def _provider_landing_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            request = ProviderLandingRequest.from_mapping(
                json.loads(row["request_json"])
            )
            evidence = json.loads(row["evidence_snapshot_json"])
        except (IntegrationError, TypeError, json.JSONDecodeError) as exc:
            raise StorageError("stored provider landing is invalid") from exc
        if (
            request.digest != row["content_sha256"]
            or request.service_operation_id != row["service_operation_id"]
            or request.capability_binding_id != row["capability_binding_id"]
            or request.assurance.value != row["assurance"]
            or content_sha256(canonical_json(evidence))
            != row["evidence_snapshot_sha256"]
            or request.evidence_snapshot_sha256 != row["evidence_snapshot_sha256"]
        ):
            raise StorageError("stored provider landing digest does not match")
        return {
            "id": row["id"],
            "queue_item_id": row["queue_item_id"],
            "sequence": row["sequence"],
            "request": request.to_dict(),
            "content_sha256": row["content_sha256"],
            "evidence_snapshot": evidence,
            "evidence_snapshot_sha256": row["evidence_snapshot_sha256"],
            "service_operation_id": row["service_operation_id"],
            "capability_binding_id": row["capability_binding_id"],
            "assurance": row["assurance"],
            "state": row["state"],
            "issued_by": row["issued_by"],
            "created_at": row["created_at"],
            "resolved_at": row["resolved_at"],
        }

    def _provider_landing_binding(
        self, connection: sqlite3.Connection, project_id: str
    ) -> tuple[sqlite3.Row, ServiceSidecarSpec, ServiceMapping, ServiceCapability]:
        capability = core_service_capability("landing.invoke", 1)
        row = connection.execute(
            "SELECT b.id, b.mapping_name, b.state, b.service_sidecar_id, "
            "b.sidecar_content_sha256, b.deployment_sha256, "
            "b.capability_name, b.capability_version, b.capability_mode, "
            "s.name AS sidecar_name, s.state AS sidecar_state, "
            "s.spec_json AS sidecar_spec_json, "
            "s.content_sha256 AS sidecar_content_sha256, "
            "p.active AS project_active "
            "FROM project_capability_bindings b "
            "JOIN service_sidecars s ON s.id = b.service_sidecar_id "
            "JOIN projects p ON p.id = b.project_id "
            "WHERE b.project_id = ? AND b.capability_name = ? "
            "AND b.capability_version = ? AND b.state = 'active'",
            (project_id, capability.name, capability.version),
        ).fetchone()
        if row is None or not row["project_active"] or row["sidecar_state"] != "active":
            raise ConflictError("project landing capability is unavailable")
        sidecar_spec = self._parse_stored_sidecar(row)
        if (
            row["sidecar_content_sha256"] != sidecar_spec.digest
            or row["deployment_sha256"] != sidecar_spec.deployment_sha256
            or row["capability_name"] != capability.name
            or row["capability_version"] != capability.version
            or row["capability_mode"] != capability.mode
        ):
            raise StorageError("stored landing capability binding is invalid")
        mapping = next(
            (
                item
                for item in sidecar_spec.mappings
                if item.name == row["mapping_name"]
            ),
            None,
        )
        advertised = next(
            (
                item
                for item in sidecar_spec.capabilities
                if (item.name, item.version) == (capability.name, capability.version)
            ),
            None,
        )
        if mapping is None or advertised != capability:
            raise StorageError("stored landing capability binding is invalid")
        return row, sidecar_spec, mapping, capability

    def issue_provider_landing(
        self,
        queue_item_id: str,
        *,
        owner_id: str,
        expected_version: int,
        resource_id: str,
        parameters: dict[str, Any],
        issued_by: str,
        workflow_execution_id: str | None = None,
        workflow_execution_version: int | None = None,
    ) -> dict[str, Any]:
        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        owner_id = require_string(owner_id, field="owner_id", max_bytes=128)
        resource_id = require_string(
            resource_id, field="provider landing resource_id", max_bytes=512
        )
        issued_by = require_string(issued_by, field="issued_by", max_bytes=64)
        expected_version = self._expected_version(expected_version)
        if (workflow_execution_id is None) != (workflow_execution_version is None):
            raise ValidationError(
                "workflow landing authority must be supplied together"
            )
        if workflow_execution_id is not None:
            workflow_execution_id = require_string(
                workflow_execution_id,
                field="workflow_execution_id",
                max_bytes=64,
            )
            workflow_execution_version = self._expected_version(
                workflow_execution_version
            )
        if not isinstance(parameters, dict) or set(parameters) != {
            "assurance",
            "integration_ref",
            "merge_method",
        }:
            raise IntegrationError("provider landing parameters are invalid")
        if parameters[
            "assurance"
        ] != LandingAssurance.HEAD_ENFORCED.value or parameters["merge_method"] not in {
            "merge",
            "rebase",
            "squash",
        }:
            raise IntegrationError("provider landing parameters are invalid")
        landing_id = self.new_id("plr")
        operation_id = self.new_id("sop")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, issued_by, role="provider landing issuer")
                queue = self._queue_row(connection, queue_item_id)
                if queue is None:
                    raise NotFoundError(
                        f"integration queue item not found: {queue_item_id}"
                    )
                if queue["version"] != expected_version:
                    raise ConcurrentUpdateError("integration queue version changed")
                if workflow_execution_id is not None:
                    workflow = connection.execute(
                        "SELECT e.state, e.stage, e.version, cv.spec_json, "
                        "t.resource_id, t.merge_method "
                        "FROM workflow_executions e "
                        "JOIN workflow_configuration_versions cv "
                        "ON cv.id = e.configuration_version_id "
                        "JOIN workflow_landing_bindings b ON b.execution_id = e.id "
                        "JOIN workflow_provider_targets t ON t.execution_id = e.id "
                        "WHERE e.id = ? AND b.queue_item_id = ?",
                        (workflow_execution_id, queue_item_id),
                    ).fetchone()
                    if workflow is None:
                        raise ConflictError(
                            "provider landing is not bound to this workflow"
                        )
                    if workflow["version"] != workflow_execution_version:
                        raise ConcurrentUpdateError(
                            "workflow execution version changed"
                        )
                    if workflow["state"] != "active" or workflow["stage"] != "landing":
                        raise ConflictError("workflow is not awaiting landing")
                    try:
                        landing_mode = json.loads(workflow["spec_json"])["landing_mode"]
                    except (json.JSONDecodeError, KeyError, TypeError) as exc:
                        raise StorageError(
                            "stored workflow configuration is invalid"
                        ) from exc
                    if landing_mode != "provider":
                        raise ConflictError("workflow is configured for manual landing")
                    if (
                        workflow["resource_id"] != resource_id
                        or workflow["merge_method"] != parameters["merge_method"]
                    ):
                        raise ConflictError(
                            "provider landing does not match workflow target"
                        )
                if queue["state"] != "claimed" or queue["owner_id"] != owner_id:
                    raise ConflictError("integration claim is not owned by caller")
                if queue["lease_expires_at"] <= now:
                    raise ConflictError("integration claim lease expired")
                if parameters["integration_ref"] != queue["integration_ref"]:
                    raise ConflictError("provider landing ref does not match queue")
                readiness = self._assert_queue_authority_ready(connection, queue)
                policy_row = connection.execute(
                    f"SELECT {self._policy_columns()} "  # noqa: S608
                    "FROM integration_gate_policies WHERE id = ?",
                    (queue["current_policy_id"],),
                ).fetchone()
                assert policy_row is not None
                policy = GatePolicyManifest.from_mapping(
                    self._policy_record(policy_row)["manifest"]
                )
                binding, sidecar_spec, mapping, capability = (
                    self._provider_landing_binding(connection, queue["project_id"])
                )
                operation_spec = ServiceOperationSpec.from_mapping(
                    {
                        "schema_version": 1,
                        "connector_name": binding["sidecar_name"],
                        "deployment_sha256": sidecar_spec.deployment_sha256,
                        "mapping": mapping.to_dict(),
                        "capability": capability.to_dict(),
                        "idempotency_key": (
                            f"integration:{queue_item_id}:{expected_version}:landing"
                        ),
                        "target": {
                            "repository": mapping.repository,
                            "resource_id": resource_id,
                            "object_format": policy.object_format,
                            "expected_revision": policy.expected_base_revision,
                            "desired_revision": policy.head_revision,
                        },
                        "parameters": dict(parameters),
                        "request_bytes": sidecar_spec.request_bytes,
                        "response_bytes": sidecar_spec.response_bytes,
                        "timeout_ms": sidecar_spec.timeout_ms,
                    }
                )
                selected_operation_id = self._create_service_operation_in_transaction(
                    connection,
                    spec=operation_spec,
                    created_by=issued_by,
                    project_capability_binding_id=binding["id"],
                    operation_id=operation_id,
                    now=now,
                )
                if selected_operation_id != operation_id:
                    raise ConflictError("provider landing operation already exists")
                evidence_json = canonical_json(readiness)
                evidence_sha256 = content_sha256(evidence_json)
                awaiting_version = expected_version + 1
                request = ProviderLandingRequest(
                    schema_version=1,
                    queue_item_id=queue_item_id,
                    queue_version=awaiting_version,
                    policy_id=queue["current_policy_id"],
                    policy_revision=policy.policy_revision,
                    candidate_id=queue["candidate_id"],
                    fold_id=queue["current_fold_id"],
                    repository_identity=queue["project_id"],
                    integration_ref=queue["integration_ref"],
                    expected_base_revision=policy.expected_base_revision,
                    head_revision=policy.head_revision,
                    tree_revision=policy.tree_revision,
                    object_format=policy.object_format,
                    evidence_snapshot_sha256=evidence_sha256,
                    service_operation_id=operation_id,
                    service_operation_sha256=operation_spec.digest,
                    capability_binding_id=binding["id"],
                    assurance=LandingAssurance.HEAD_ENFORCED,
                )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM integration_provider_landings WHERE queue_item_id = ?",
                    (queue_item_id,),
                ).fetchone()["sequence"]
                connection.execute(
                    "INSERT INTO integration_provider_landings "
                    "(id, queue_item_id, sequence, schema_version, request_json, "
                    "content_sha256, evidence_snapshot_json, "
                    "evidence_snapshot_sha256, service_operation_id, "
                    "capability_binding_id, assurance, state, issued_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                    (
                        landing_id,
                        queue_item_id,
                        sequence,
                        request.schema_version,
                        request.canonical,
                        request.digest,
                        evidence_json,
                        evidence_sha256,
                        operation_id,
                        binding["id"],
                        request.assurance.value,
                        issued_by,
                        now,
                    ),
                )
                event_sequence = queue["event_sequence"] + 1
                connection.execute(
                    "UPDATE integration_queue_items SET state = 'awaiting_receipt', "
                    "version = ?, event_sequence = ?, owner_id = NULL, "
                    "lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                    (awaiting_version, event_sequence, now, queue_item_id),
                )
                self._queue_event(
                    connection,
                    queue_item_id=queue_item_id,
                    sequence=event_sequence,
                    kind="queue.provider_landing_issued",
                    from_state="claimed",
                    to_state="awaiting_receipt",
                    actor=issued_by,
                    metadata={
                        "provider_landing_id": landing_id,
                        "service_operation_id": operation_id,
                        "request_sha256": request.digest,
                        "evidence_snapshot_sha256": evidence_sha256,
                    },
                    created_at=now,
                )
                landing_row = self._provider_landing_row(connection, landing_id)
                updated_queue = self._queue_row(connection, queue_item_id)
                assert landing_row is not None and updated_queue is not None
                result = {
                    "provider_landing": self._provider_landing_record(landing_row),
                    "operation": self._service_operation_record(
                        connection, operation_id
                    ),
                    "queue_item": self._queue_record(connection, updated_queue),
                }
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                IntegrationError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("provider landing could not be issued") from exc
        return result

    def get_provider_landing(self, provider_landing_id: str) -> dict[str, Any]:
        provider_landing_id = require_string(
            provider_landing_id, field="provider_landing_id", max_bytes=64
        )
        with self._read_snapshot() as connection:
            row = self._provider_landing_row(connection, provider_landing_id)
            if row is None:
                raise NotFoundError(
                    f"provider landing not found: {provider_landing_id}"
                )
            return self._provider_landing_record(row)

    @staticmethod
    def _provider_service_receipt(
        connection: sqlite3.Connection, operation_id: str
    ) -> ServiceOperationReceipt:
        row = connection.execute(
            "SELECT receipt_json, receipt_sha256 "
            "FROM service_operation_attempts WHERE operation_id = ? "
            "AND outcome = 'succeeded' ORDER BY sequence DESC LIMIT 1",
            (operation_id,),
        ).fetchone()
        if row is None or row["receipt_json"] is None:
            raise StorageError("provider landing service receipt is unavailable")
        try:
            receipt = ServiceOperationReceipt.from_mapping(
                json.loads(row["receipt_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored provider service receipt is invalid") from exc
        if content_sha256(canonical_json(receipt.to_dict())) != row["receipt_sha256"]:
            raise StorageError("stored provider service receipt digest does not match")
        return receipt

    def settle_provider_landing(
        self, provider_landing_id: str, *, settled_by: str
    ) -> dict[str, Any]:
        provider_landing_id = require_string(
            provider_landing_id, field="provider_landing_id", max_bytes=64
        )
        settled_by = require_string(settled_by, field="settled_by", max_bytes=64)
        receipt_id = self.new_id("lrc")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, settled_by, role="provider landing settler"
                )
                landing_row = self._provider_landing_row(
                    connection, provider_landing_id
                )
                if landing_row is None:
                    raise NotFoundError(
                        f"provider landing not found: {provider_landing_id}"
                    )
                if landing_row["state"] != "pending":
                    raise ConflictError("provider landing is already resolved")
                landing_record = self._provider_landing_record(landing_row)
                request = ProviderLandingRequest.from_mapping(landing_record["request"])
                queue = self._queue_row(connection, request.queue_item_id)
                if queue is None:
                    raise StorageError("provider landing queue item is unavailable")
                if (
                    queue["state"] != "awaiting_receipt"
                    or queue["version"] != request.queue_version
                    or queue["current_policy_id"] != request.policy_id
                    or queue["current_fold_id"] != request.fold_id
                    or queue["current_revision"] != request.expected_base_revision
                    or queue["candidate_id"] != request.candidate_id
                ):
                    raise ConflictError("provider landing authority is stale")
                operation = self._service_operation_record(
                    connection, request.service_operation_id
                )
                if (
                    operation["content_sha256"] != request.service_operation_sha256
                    or operation["project_capability_binding_id"]
                    != request.capability_binding_id
                ):
                    raise StorageError("provider landing operation does not match")
                if operation["state"] in {"denied", "conflict"}:
                    connection.execute(
                        "UPDATE integration_provider_landings SET state = 'failed', "
                        "resolved_at = ? WHERE id = ? AND state = 'pending'",
                        (now, provider_landing_id),
                    )
                    version = queue["version"] + 1
                    sequence = queue["event_sequence"] + 1
                    connection.execute(
                        "UPDATE integration_queue_items SET state = 'queued', "
                        "version = ?, event_sequence = ?, updated_at = ? WHERE id = ?",
                        (version, sequence, now, queue["id"]),
                    )
                    self._queue_event(
                        connection,
                        queue_item_id=queue["id"],
                        sequence=sequence,
                        kind="queue.provider_landing_failed",
                        from_state="awaiting_receipt",
                        to_state="queued",
                        actor=settled_by,
                        metadata={
                            "provider_landing_id": provider_landing_id,
                            "service_operation_id": request.service_operation_id,
                            "operation_state": operation["state"],
                        },
                        created_at=now,
                    )
                    updated_landing = self._provider_landing_row(
                        connection, provider_landing_id
                    )
                    updated_queue = self._queue_row(connection, queue["id"])
                    updated_ref = self._integration_ref_row(
                        connection, queue["integration_ref_id"]
                    )
                    assert (
                        updated_landing is not None
                        and updated_queue is not None
                        and updated_ref is not None
                    )
                    result = {
                        "provider_landing": self._provider_landing_record(
                            updated_landing
                        ),
                        "operation": operation,
                        "receipt": None,
                        "queue_item": self._queue_record(connection, updated_queue),
                        "integration_ref": self._integration_ref_record(updated_ref),
                    }
                    connection.commit()
                    return result
                if operation["state"] != "succeeded":
                    raise ConflictError("provider landing operation is not settled")
                service_receipt = self._provider_service_receipt(
                    connection, request.service_operation_id
                )
                receipt = ProviderLandingReceipt.from_service_receipt(
                    provider_landing_id, request, service_receipt
                )
                connection.execute(
                    "INSERT INTO integration_landing_receipts "
                    "(id, plan_id, provider_landing_id, schema_version, receipt_json, "
                    "content_sha256, outcome, observed_revision, assurance, "
                    "recorded_by, created_at) "
                    "VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        receipt_id,
                        provider_landing_id,
                        receipt.schema_version,
                        receipt.canonical,
                        receipt.digest,
                        receipt.outcome.value,
                        receipt.observed_revision,
                        receipt.assurance.value,
                        settled_by,
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE integration_provider_landings SET state = ?, "
                    "resolved_at = ? WHERE id = ? AND state = 'pending'",
                    (receipt.outcome.value, now, provider_landing_id),
                )
                version = queue["version"] + 1
                sequence = queue["event_sequence"] + 1
                if receipt.outcome is LandingOutcome.NOT_LANDED:
                    if receipt.observed_revision == request.expected_base_revision:
                        connection.execute(
                            "UPDATE integration_queue_items SET state = 'queued', "
                            "version = ?, event_sequence = ?, updated_at = ? "
                            "WHERE id = ?",
                            (version, sequence, now, queue["id"]),
                        )
                        self._queue_event(
                            connection,
                            queue_item_id=queue["id"],
                            sequence=sequence,
                            kind="queue.not_landed",
                            from_state="awaiting_receipt",
                            to_state="queued",
                            actor=settled_by,
                            metadata={
                                "provider_landing_id": provider_landing_id,
                                "receipt_id": receipt_id,
                            },
                            created_at=now,
                        )
                    else:
                        self._advance_provider_ref(
                            connection,
                            queue=queue,
                            request=request,
                            receipt=receipt,
                            now=now,
                        )
                        connection.execute(
                            "UPDATE integration_queue_items SET "
                            "state = 'refold_required', current_fold_id = NULL, "
                            "version = ?, event_sequence = ?, updated_at = ? "
                            "WHERE id = ?",
                            (version, sequence, now, queue["id"]),
                        )
                        self._queue_event(
                            connection,
                            queue_item_id=queue["id"],
                            sequence=sequence,
                            kind="queue.refold_required",
                            from_state="awaiting_receipt",
                            to_state="refold_required",
                            actor=settled_by,
                            metadata={
                                "new_base_revision": receipt.observed_revision,
                                "provider_landing_id": provider_landing_id,
                                "receipt_id": receipt_id,
                            },
                            created_at=now,
                        )
                        self._provider_refold_siblings(
                            connection,
                            queue=queue,
                            new_revision=receipt.observed_revision,
                            cause_key="drifted_queue_item_id",
                            now=now,
                        )
                else:
                    self._advance_provider_ref(
                        connection,
                        queue=queue,
                        request=request,
                        receipt=receipt,
                        now=now,
                    )
                    connection.execute(
                        "UPDATE integration_queue_items SET state = 'landed', "
                        "version = ?, event_sequence = ?, landed_receipt_id = ?, "
                        "landed_at = ?, updated_at = ? WHERE id = ?",
                        (version, sequence, receipt_id, now, now, queue["id"]),
                    )
                    self._queue_event(
                        connection,
                        queue_item_id=queue["id"],
                        sequence=sequence,
                        kind="queue.landed",
                        from_state="awaiting_receipt",
                        to_state="landed",
                        actor=settled_by,
                        metadata={
                            "provider_landing_id": provider_landing_id,
                            "receipt_id": receipt_id,
                        },
                        created_at=now,
                    )
                    self._provider_refold_siblings(
                        connection,
                        queue=queue,
                        new_revision=receipt.observed_revision,
                        cause_key="landed_queue_item_id",
                        now=now,
                    )
                receipt_row = connection.execute(
                    "SELECT id, plan_id, provider_landing_id, schema_version, "
                    "receipt_json, content_sha256, outcome, observed_revision, "
                    "assurance, recorded_by, created_at "
                    "FROM integration_landing_receipts WHERE id = ?",
                    (receipt_id,),
                ).fetchone()
                updated_landing = self._provider_landing_row(
                    connection, provider_landing_id
                )
                updated_queue = self._queue_row(connection, queue["id"])
                updated_ref = self._integration_ref_row(
                    connection, queue["integration_ref_id"]
                )
                assert (
                    receipt_row is not None
                    and updated_landing is not None
                    and updated_queue is not None
                    and updated_ref is not None
                )
                result = {
                    "provider_landing": self._provider_landing_record(updated_landing),
                    "operation": operation,
                    "receipt": self._receipt_record(receipt_row),
                    "queue_item": self._queue_record(connection, updated_queue),
                    "integration_ref": self._integration_ref_record(updated_ref),
                }
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                IntegrationError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
            except (json.JSONDecodeError, sqlite3.IntegrityError) as exc:
                connection.rollback()
                raise ConflictError("provider landing could not be settled") from exc
        return result

    @staticmethod
    def _advance_provider_ref(
        connection: sqlite3.Connection,
        *,
        queue: sqlite3.Row,
        request: ProviderLandingRequest,
        receipt: ProviderLandingReceipt,
        now: str,
    ) -> None:
        updated = connection.execute(
            "UPDATE integration_refs SET current_revision = ?, "
            "version = version + 1, updated_at = ? WHERE id = ? "
            "AND current_revision = ? AND version = ?",
            (
                receipt.observed_revision,
                now,
                queue["integration_ref_id"],
                request.expected_base_revision,
                queue["integration_ref_version"],
            ),
        )
        if updated.rowcount != 1:
            raise ConcurrentUpdateError("integration ref changed")

    def _provider_refold_siblings(
        self,
        connection: sqlite3.Connection,
        *,
        queue: sqlite3.Row,
        new_revision: str,
        cause_key: str,
        now: str,
    ) -> None:
        siblings = connection.execute(
            "SELECT id FROM integration_queue_items "
            "WHERE integration_ref_id = ? AND id != ? "
            "AND state IN ('queued', 'refold_required')",
            (queue["integration_ref_id"], queue["id"]),
        ).fetchall()
        for sibling_id in siblings:
            sibling = self._queue_row(connection, sibling_id["id"])
            assert sibling is not None
            sibling_version = sibling["version"] + 1
            sibling_sequence = sibling["event_sequence"] + 1
            connection.execute(
                "UPDATE integration_queue_items SET "
                "state = 'refold_required', current_fold_id = NULL, "
                "version = ?, event_sequence = ?, updated_at = ? WHERE id = ?",
                (sibling_version, sibling_sequence, now, sibling["id"]),
            )
            self._queue_event(
                connection,
                queue_item_id=sibling["id"],
                sequence=sibling_sequence,
                kind="queue.refold_required",
                from_state=sibling["state"],
                to_state="refold_required",
                actor="system",
                metadata={
                    cause_key: queue["id"],
                    "new_base_revision": new_revision,
                },
                created_at=now,
            )

    @staticmethod
    def _receipt_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            raw_receipt = json.loads(row["receipt_json"])
            receipt = (
                LandingReceipt.from_mapping(raw_receipt)
                if row["plan_id"] is not None
                else ProviderLandingReceipt.from_mapping(raw_receipt)
            )
        except (IntegrationError, TypeError, json.JSONDecodeError) as exc:
            raise StorageError("stored landing receipt is invalid") from exc
        if receipt.digest != row["content_sha256"]:
            raise StorageError("stored landing receipt digest does not match")
        return {
            "id": row["id"],
            "plan_id": row["plan_id"],
            "provider_landing_id": row["provider_landing_id"],
            "receipt": receipt.to_dict(),
            "content_sha256": row["content_sha256"],
            "outcome": row["outcome"],
            "observed_revision": row["observed_revision"],
            "assurance": row["assurance"],
            "recorded_by": row["recorded_by"],
            "created_at": row["created_at"],
        }

    def record_landing_receipt(
        self, request: LandingReceiptRequest, *, recorded_by: str
    ) -> dict[str, Any]:
        if not isinstance(request, LandingReceiptRequest):
            raise IntegrationError("landing receipt request is invalid")
        recorded_by = require_string(recorded_by, field="recorded_by", max_bytes=64)
        receipt_id = self.new_id("lrc")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, recorded_by, role="receipt recorder")
                plan_row = self._plan_row(connection, request.plan_id)
                if plan_row is None:
                    raise NotFoundError(f"landing plan not found: {request.plan_id}")
                if plan_row["state"] != "pending":
                    raise ConflictError("landing plan is already resolved")
                nonce_rows = self._plan_nonce_rows(connection, request.plan_id)
                if not nonce_rows:
                    raise StorageError("landing plan nonce history is unavailable")
                if not secrets.compare_digest(
                    hashlib.sha256(request.nonce.encode("ascii")).hexdigest(),
                    nonce_rows[-1]["nonce_sha256"],
                ):
                    raise ConflictError("landing receipt nonce does not match")
                plan = LandingPlan.from_mapping(json.loads(plan_row["plan_json"]))
                try:
                    receipt = LandingReceipt.from_request(
                        request, plan_id=plan_row["id"], plan=plan
                    )
                except IntegrationError as exc:
                    raise ConflictError(str(exc)) from exc
                queue = self._queue_row(connection, plan.queue_item_id)
                if queue is None:
                    raise StorageError("landing plan queue item is unavailable")
                if (
                    queue["state"] != "awaiting_receipt"
                    or queue["version"] != plan.queue_version
                    or queue["current_policy_id"] != plan.policy_id
                    or queue["current_fold_id"] != plan.fold_id
                    or queue["current_revision"] != plan.expected_base_revision
                    or queue["candidate_id"] != plan.candidate_id
                ):
                    raise ConflictError("landing plan authority is stale")
                connection.execute(
                    "INSERT INTO integration_landing_receipts "
                    "(id, plan_id, schema_version, receipt_json, content_sha256, "
                    "outcome, observed_revision, assurance, recorded_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        receipt_id,
                        plan_row["id"],
                        receipt.schema_version,
                        receipt.canonical,
                        receipt.digest,
                        receipt.outcome.value,
                        receipt.observed_revision,
                        receipt.assurance.value,
                        recorded_by,
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE integration_landing_plans SET state = ?, resolved_at = ? "
                    "WHERE id = ?",
                    (receipt.outcome.value, now, plan_row["id"]),
                )
                version = queue["version"] + 1
                sequence = queue["event_sequence"] + 1
                if receipt.outcome is LandingOutcome.NOT_LANDED:
                    if receipt.observed_revision == plan.expected_base_revision:
                        connection.execute(
                            "UPDATE integration_queue_items SET state = 'queued', "
                            "version = ?, event_sequence = ?, updated_at = ? "
                            "WHERE id = ?",
                            (version, sequence, now, queue["id"]),
                        )
                        self._queue_event(
                            connection,
                            queue_item_id=queue["id"],
                            sequence=sequence,
                            kind="queue.not_landed",
                            from_state="awaiting_receipt",
                            to_state="queued",
                            actor=recorded_by,
                            metadata={
                                "plan_id": plan_row["id"],
                                "receipt_id": receipt_id,
                            },
                            created_at=now,
                        )
                    else:
                        updated_ref = connection.execute(
                            "UPDATE integration_refs SET current_revision = ?, "
                            "version = version + 1, updated_at = ? WHERE id = ? "
                            "AND current_revision = ? AND version = ?",
                            (
                                receipt.observed_revision,
                                now,
                                queue["integration_ref_id"],
                                plan.expected_base_revision,
                                queue["integration_ref_version"],
                            ),
                        )
                        if updated_ref.rowcount != 1:
                            raise ConcurrentUpdateError("integration ref changed")
                        connection.execute(
                            "UPDATE integration_queue_items SET "
                            "state = 'refold_required', current_fold_id = NULL, "
                            "version = ?, event_sequence = ?, updated_at = ? "
                            "WHERE id = ?",
                            (version, sequence, now, queue["id"]),
                        )
                        self._queue_event(
                            connection,
                            queue_item_id=queue["id"],
                            sequence=sequence,
                            kind="queue.refold_required",
                            from_state="awaiting_receipt",
                            to_state="refold_required",
                            actor=recorded_by,
                            metadata={
                                "new_base_revision": receipt.observed_revision,
                                "plan_id": plan_row["id"],
                                "receipt_id": receipt_id,
                            },
                            created_at=now,
                        )
                        siblings = connection.execute(
                            "SELECT id FROM integration_queue_items "
                            "WHERE integration_ref_id = ? AND id != ? "
                            "AND state IN ('queued', 'refold_required')",
                            (queue["integration_ref_id"], queue["id"]),
                        ).fetchall()
                        for sibling_id in siblings:
                            sibling = self._queue_row(connection, sibling_id["id"])
                            assert sibling is not None
                            sibling_version = sibling["version"] + 1
                            sibling_sequence = sibling["event_sequence"] + 1
                            connection.execute(
                                "UPDATE integration_queue_items SET "
                                "state = 'refold_required', current_fold_id = NULL, "
                                "version = ?, event_sequence = ?, updated_at = ? "
                                "WHERE id = ?",
                                (
                                    sibling_version,
                                    sibling_sequence,
                                    now,
                                    sibling["id"],
                                ),
                            )
                            self._queue_event(
                                connection,
                                queue_item_id=sibling["id"],
                                sequence=sibling_sequence,
                                kind="queue.refold_required",
                                from_state=sibling["state"],
                                to_state="refold_required",
                                actor="system",
                                metadata={
                                    "drifted_queue_item_id": queue["id"],
                                    "new_base_revision": receipt.observed_revision,
                                },
                                created_at=now,
                            )
                else:
                    updated_ref = connection.execute(
                        "UPDATE integration_refs SET current_revision = ?, "
                        "version = version + 1, updated_at = ? WHERE id = ? "
                        "AND current_revision = ? AND version = ?",
                        (
                            plan.head_revision,
                            now,
                            queue["integration_ref_id"],
                            plan.expected_base_revision,
                            queue["integration_ref_version"],
                        ),
                    )
                    if updated_ref.rowcount != 1:
                        raise ConcurrentUpdateError("integration ref changed")
                    connection.execute(
                        "UPDATE integration_queue_items SET state = 'landed', "
                        "version = ?, event_sequence = ?, landed_receipt_id = ?, "
                        "landed_at = ?, updated_at = ? WHERE id = ?",
                        (version, sequence, receipt_id, now, now, queue["id"]),
                    )
                    self._queue_event(
                        connection,
                        queue_item_id=queue["id"],
                        sequence=sequence,
                        kind="queue.landed",
                        from_state="awaiting_receipt",
                        to_state="landed",
                        actor=recorded_by,
                        metadata={"plan_id": plan_row["id"], "receipt_id": receipt_id},
                        created_at=now,
                    )
                    siblings = connection.execute(
                        "SELECT id FROM integration_queue_items "
                        "WHERE integration_ref_id = ? AND id != ? "
                        "AND state IN ('queued', 'refold_required')",
                        (queue["integration_ref_id"], queue["id"]),
                    ).fetchall()
                    for sibling_id in siblings:
                        sibling = self._queue_row(connection, sibling_id["id"])
                        assert sibling is not None
                        sibling_version = sibling["version"] + 1
                        sibling_sequence = sibling["event_sequence"] + 1
                        connection.execute(
                            "UPDATE integration_queue_items SET "
                            "state = 'refold_required', current_fold_id = NULL, "
                            "version = ?, event_sequence = ?, updated_at = ? "
                            "WHERE id = ?",
                            (
                                sibling_version,
                                sibling_sequence,
                                now,
                                sibling["id"],
                            ),
                        )
                        self._queue_event(
                            connection,
                            queue_item_id=sibling["id"],
                            sequence=sibling_sequence,
                            kind="queue.refold_required",
                            from_state=sibling["state"],
                            to_state="refold_required",
                            actor="system",
                            metadata={
                                "landed_queue_item_id": queue["id"],
                                "new_base_revision": plan.head_revision,
                            },
                            created_at=now,
                        )
                receipt_row = connection.execute(
                    "SELECT id, plan_id, provider_landing_id, schema_version, "
                    "receipt_json, "
                    "content_sha256, outcome, observed_revision, assurance, "
                    "recorded_by, created_at FROM integration_landing_receipts "
                    "WHERE id = ?",
                    (receipt_id,),
                ).fetchone()
                updated_queue = self._queue_row(connection, queue["id"])
                updated_ref = self._integration_ref_row(
                    connection, queue["integration_ref_id"]
                )
                assert (
                    receipt_row is not None
                    and updated_queue is not None
                    and updated_ref is not None
                )
                result = {
                    "receipt": self._receipt_record(receipt_row),
                    "queue_item": self._queue_record(connection, updated_queue),
                    "integration_ref": self._integration_ref_record(updated_ref),
                }
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
            except (
                IntegrationError,
                json.JSONDecodeError,
                sqlite3.IntegrityError,
            ) as exc:
                connection.rollback()
                raise ConflictError("landing receipt could not be recorded") from exc
        return result
