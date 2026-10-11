from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Any

from ..domain import (
    AttemptState,
    RunState,
    ValidationError,
    canonical_json,
    require_string,
    validate_provider_name,
)
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)
from .schema import (
    MAX_ATTEMPT_LEASE_SECONDS,
    MIN_ATTEMPT_LEASE_SECONDS,
)


class AttemptStorage:
    def begin_run_attempt(
        self,
        run_id: str,
        *,
        runtime_adapter: str,
        owner_id: str,
        expected_run_version: int,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        """Atomically claim a queued durable run for one disposable worker."""

        runtime_adapter = validate_provider_name(
            runtime_adapter, field="runtime_adapter"
        )
        owner_id = require_string(owner_id, field="owner_id", max_bytes=128)
        self._attempt_lease_seconds(lease_seconds)
        if (
            isinstance(expected_run_version, bool)
            or not isinstance(expected_run_version, int)
            or expected_run_version < 1
        ):
            raise ValidationError("expected_run_version must be a positive integer")
        attempt_id = self.new_id("att")
        now = self.now()
        lease_expires_at = self._lease_expiry(now, lease_seconds)
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                run = connection.execute(
                    "SELECT state, version, profile_version_id FROM runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
                if run is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if run["version"] != expected_run_version:
                    raise ConcurrentUpdateError(
                        f"run version is {run['version']}, "
                        f"expected {expected_run_version}"
                    )
                if run["state"] != RunState.QUEUED.value:
                    raise ConflictError(f"run is not queued: {run['state']}")
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 FROM run_attempts "
                    "WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO run_attempts "
                    "(id, run_id, sequence, profile_version_id, runtime_adapter, "
                    "state, version, owner_id, heartbeat_at, lease_expires_at, "
                    "created_at) VALUES (?, ?, ?, ?, ?, 'preparing', 1, ?, ?, ?, ?)",
                    (
                        attempt_id,
                        run_id,
                        sequence,
                        run["profile_version_id"],
                        runtime_adapter,
                        owner_id,
                        now,
                        lease_expires_at,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO run_attempt_events "
                    "(attempt_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) "
                    "VALUES (?, 1, 'attempt.created', NULL, 'preparing', '{}', ?)",
                    (attempt_id, now),
                )
                changed = connection.execute(
                    "UPDATE runs SET state = 'preparing', version = version + 1, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (now, run_id, expected_run_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                metadata = canonical_json(
                    {
                        "attempt_id": attempt_id,
                        "attempt_sequence": sequence,
                        "owner_id": owner_id,
                        "runtime_adapter": runtime_adapter,
                    }
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, metadata_json, "
                    "created_at) VALUES (?, ?, 'run.transitioned', 'queued', "
                    "'preparing', ?, ?)",
                    (run_id, expected_run_version + 1, metadata, now),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("run attempt could not be created") from exc
        return self.get_run_attempt(attempt_id)

    @staticmethod
    def _attempt_lease_seconds(value: object) -> int:
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
    def _lease_expiry(now: str, lease_seconds: int) -> str:
        try:
            current = datetime.fromisoformat(now)
        except ValueError as exc:
            raise StorageError("database clock returned an invalid timestamp") from exc
        return (current + timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds"
        )

    def renew_run_attempt_lease(
        self,
        attempt_id: str,
        *,
        owner_id: str,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        """Renew one live ownership lease and return its control signals."""

        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        owner_id = require_string(owner_id, field="owner_id", max_bytes=128)
        self._attempt_lease_seconds(lease_seconds)
        now = self.now()
        expires_at = self._lease_expiry(now, lease_seconds)
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt = connection.execute(
                    "SELECT state, owner_id, cancel_requested_at, cancel_reason "
                    "FROM run_attempts WHERE id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise NotFoundError(f"run attempt not found: {attempt_id}")
                if attempt["state"] not in {
                    AttemptState.PREPARING.value,
                    AttemptState.RUNNING.value,
                }:
                    raise ConflictError(
                        f"run attempt is not active: {attempt['state']}"
                    )
                if attempt["owner_id"] != owner_id:
                    raise ConflictError("run attempt is owned by another orchestrator")
                changed = connection.execute(
                    "UPDATE run_attempts SET heartbeat_at = ?, lease_expires_at = ? "
                    "WHERE id = ? AND owner_id = ? "
                    "AND state IN ('preparing', 'running')",
                    (now, expires_at, attempt_id, owner_id),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run attempt ownership changed")
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("run attempt lease could not be renewed") from exc
        return {
            "attempt_id": attempt_id,
            "cancel_reason": attempt["cancel_reason"],
            "cancel_requested": attempt["cancel_requested_at"] is not None,
            "heartbeat_at": now,
            "lease_expires_at": expires_at,
            "owner_id": owner_id,
        }

    def claim_expired_run_attempt(
        self,
        attempt_id: str,
        *,
        owner_id: str,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        """Take ownership only after the previous orchestrator lease expires."""

        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        owner_id = require_string(owner_id, field="owner_id", max_bytes=128)
        self._attempt_lease_seconds(lease_seconds)
        now = self.now()
        expires_at = self._lease_expiry(now, lease_seconds)
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt = connection.execute(
                    "SELECT state, lease_expires_at FROM run_attempts WHERE id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise NotFoundError(f"run attempt not found: {attempt_id}")
                if attempt["state"] not in {
                    AttemptState.PREPARING.value,
                    AttemptState.RUNNING.value,
                }:
                    raise ConflictError(
                        f"run attempt is not active: {attempt['state']}"
                    )
                if (
                    attempt["lease_expires_at"] is not None
                    and attempt["lease_expires_at"] > now
                ):
                    raise ConflictError("run attempt lease has not expired")
                changed = connection.execute(
                    "UPDATE run_attempts SET owner_id = ?, heartbeat_at = ?, "
                    "lease_expires_at = ? WHERE id = ? "
                    "AND state IN ('preparing', 'running') "
                    "AND (lease_expires_at IS NULL OR lease_expires_at <= ?)",
                    (owner_id, now, expires_at, attempt_id, now),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "run attempt lease changed concurrently"
                    )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("run attempt lease could not be claimed") from exc
        return self.get_run_attempt(attempt_id)

    def list_expired_run_attempts(self) -> list[dict[str, Any]]:
        now = self.now()
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT id FROM run_attempts WHERE state IN ('preparing', 'running') "
                "AND (lease_expires_at IS NULL OR lease_expires_at <= ?) "
                "ORDER BY created_at, id",
                (now,),
            ).fetchall()
        return [self.get_run_attempt(row["id"]) for row in rows]

    def start_run_attempt(
        self,
        attempt_id: str,
        *,
        expected_attempt_version: int,
        expected_run_version: int,
        source_revision: str,
        container_id: str,
        container_name: str,
        image_id: str,
    ) -> dict[str, Any]:
        """Record a worker start and the resolved source revision atomically."""

        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        for value, field, maximum in (
            (source_revision, "source_revision", 512),
            (container_id, "container_id", 512),
            (container_name, "container_name", 512),
            (image_id, "image_id", 512),
        ):
            require_string(value, field=field, max_bytes=maximum)
        for value, field in (
            (expected_attempt_version, "expected_attempt_version"),
            (expected_run_version, "expected_run_version"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValidationError(f"{field} must be a positive integer")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt = connection.execute(
                    "SELECT ra.run_id, ra.state, ra.version, r.state AS run_state, "
                    "r.version AS run_version, r.started_at "
                    "FROM run_attempts ra JOIN runs r ON r.id = ra.run_id "
                    "WHERE ra.id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise NotFoundError(f"run attempt not found: {attempt_id}")
                if attempt["version"] != expected_attempt_version:
                    raise ConcurrentUpdateError(
                        f"attempt version is {attempt['version']}, "
                        f"expected {expected_attempt_version}"
                    )
                if attempt["state"] != AttemptState.PREPARING.value:
                    raise ConflictError(
                        f"run attempt is not preparing: {attempt['state']}"
                    )
                if attempt["run_version"] != expected_run_version:
                    raise ConcurrentUpdateError(
                        f"run version is {attempt['run_version']}, "
                        f"expected {expected_run_version}"
                    )
                if attempt["run_state"] != RunState.PREPARING.value:
                    raise ConflictError(f"run is not preparing: {attempt['run_state']}")
                attempt_changed = connection.execute(
                    "UPDATE run_attempts SET state = 'running', version = version + 1, "
                    "source_revision = ?, container_id = ?, container_name = ?, "
                    "image_id = ?, started_at = ? WHERE id = ? AND version = ?",
                    (
                        source_revision,
                        container_id,
                        container_name,
                        image_id,
                        now,
                        attempt_id,
                        expected_attempt_version,
                    ),
                ).rowcount
                if attempt_changed != 1:
                    raise ConcurrentUpdateError("run attempt changed concurrently")
                started_at = attempt["started_at"] or now
                run_changed = connection.execute(
                    "UPDATE runs SET state = 'running', version = version + 1, "
                    "source_revision = ?, updated_at = ?, started_at = ? "
                    "WHERE id = ? AND version = ?",
                    (
                        source_revision,
                        now,
                        started_at,
                        attempt["run_id"],
                        expected_run_version,
                    ),
                ).rowcount
                if run_changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                metadata = canonical_json(
                    {
                        "container_id": container_id,
                        "container_name": container_name,
                        "image_id": image_id,
                        "source_revision": source_revision,
                    }
                )
                connection.execute(
                    "INSERT INTO run_attempt_events "
                    "(attempt_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, 2, 'attempt.started', "
                    "'preparing', 'running', ?, ?)",
                    (attempt_id, metadata, now),
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, metadata_json, "
                    "created_at) VALUES (?, ?, 'run.transitioned', 'preparing', "
                    "'running', ?, ?)",
                    (
                        attempt["run_id"],
                        expected_run_version + 1,
                        metadata,
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("run attempt start could not be recorded") from exc
        return self.get_run_attempt(attempt_id)

    def complete_run_attempt(
        self,
        attempt_id: str,
        *,
        expected_attempt_version: int,
        exit_code: int,
        image_id: str,
    ) -> dict[str, Any]:
        """Close one worker attempt and, when appropriate, its durable run."""

        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        image_id = require_string(image_id, field="image_id", max_bytes=512)
        if (
            isinstance(expected_attempt_version, bool)
            or not isinstance(expected_attempt_version, int)
            or expected_attempt_version < 1
        ):
            raise ValidationError("expected_attempt_version must be a positive integer")
        if (
            isinstance(exit_code, bool)
            or not isinstance(exit_code, int)
            or not -(1 << 31) <= exit_code < (1 << 31)
        ):
            raise ValidationError("exit_code must be a signed 32-bit integer")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt = connection.execute(
                    "SELECT ra.run_id, ra.state, ra.version, ra.image_id, "
                    "ra.cancel_requested_at, ra.cancel_requested_by, "
                    "ra.cancel_reason, "
                    "r.state AS run_state, r.version AS run_version "
                    "FROM run_attempts ra JOIN runs r ON r.id = ra.run_id "
                    "WHERE ra.id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise NotFoundError(f"run attempt not found: {attempt_id}")
                if attempt["version"] != expected_attempt_version:
                    raise ConcurrentUpdateError(
                        f"attempt version is {attempt['version']}, "
                        f"expected {expected_attempt_version}"
                    )
                if attempt["state"] != AttemptState.RUNNING.value:
                    raise ConflictError(
                        f"run attempt is not running: {attempt['state']}"
                    )
                if attempt["image_id"] != image_id:
                    raise ConflictError("run attempt image identity changed")

                run_state = RunState(attempt["run_state"])
                handoff = connection.execute(
                    "SELECT id FROM attempt_handoffs WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                result = connection.execute(
                    "SELECT id, content_sha256 FROM attempt_results "
                    "WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                if result is None:
                    raise ConflictError("run attempt result has not been captured")
                reason: str | None = None
                if attempt["cancel_requested_at"] is not None:
                    outcome = AttemptState.CANCELLED
                    reason = attempt["cancel_reason"]
                    self._cancel_open_interactions(connection, attempt["run_id"], now)
                    run_metadata = {
                        "exit_code": exit_code,
                        "image_id": image_id,
                        "reason": reason,
                        "requested_by": attempt["cancel_requested_by"],
                        "result_id": result["id"],
                        "result_sha256": result["content_sha256"],
                    }
                    run_changed = connection.execute(
                        "UPDATE runs SET state = 'cancelled', "
                        "version = version + 1, updated_at = ?, finished_at = ? "
                        "WHERE id = ? AND version = ?",
                        (now, now, attempt["run_id"], attempt["run_version"]),
                    ).rowcount
                    if run_changed != 1:
                        raise ConcurrentUpdateError("run changed concurrently")
                    connection.execute(
                        "INSERT INTO run_events "
                        "(run_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) VALUES (?, ?, "
                        "'run.transitioned', ?, 'cancelled', ?, ?)",
                        (
                            attempt["run_id"],
                            attempt["run_version"] + 1,
                            run_state.value,
                            canonical_json(run_metadata),
                            now,
                        ),
                    )
                elif run_state is RunState.RUNNING:
                    outcome = (
                        AttemptState.SUCCEEDED
                        if exit_code == 0
                        else AttemptState.FAILED
                    )
                    target = RunState.SUCCEEDED if exit_code == 0 else RunState.FAILED
                    if exit_code != 0:
                        reason = f"agent exited with code {exit_code}"
                    run_metadata: dict[str, Any] = {
                        "exit_code": exit_code,
                        "image_id": image_id,
                        "result_id": result["id"],
                        "result_sha256": result["content_sha256"],
                    }
                    if reason is not None:
                        run_metadata["reason"] = reason
                    run_changed = connection.execute(
                        "UPDATE runs SET state = ?, version = version + 1, "
                        "updated_at = ?, finished_at = ? "
                        "WHERE id = ? AND version = ?",
                        (
                            target.value,
                            now,
                            now,
                            attempt["run_id"],
                            attempt["run_version"],
                        ),
                    ).rowcount
                    if run_changed != 1:
                        raise ConcurrentUpdateError("run changed concurrently")
                    connection.execute(
                        "INSERT INTO run_events "
                        "(run_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) VALUES (?, ?, "
                        "'run.transitioned', 'running', ?, ?, ?)",
                        (
                            attempt["run_id"],
                            attempt["run_version"] + 1,
                            target.value,
                            canonical_json(run_metadata),
                            now,
                        ),
                    )
                elif run_state in {
                    RunState.WAITING_FOR_INPUT,
                    RunState.QUEUED,
                }:
                    if exit_code != 0:
                        outcome = AttemptState.FAILED
                        reason = f"agent exited with code {exit_code}"
                    elif handoff is None:
                        outcome = AttemptState.INTERRUPTED
                        reason = "worker exited while paused without a handoff"
                    else:
                        outcome = AttemptState.HANDED_OFF
                    open_interaction = connection.execute(
                        "SELECT 1 FROM interactions WHERE run_id = ? "
                        "AND state = 'open' LIMIT 1",
                        (attempt["run_id"],),
                    ).fetchone()
                    if (
                        run_state is RunState.WAITING_FOR_INPUT
                        and open_interaction is None
                    ):
                        run_metadata = {
                            "attempt_id": attempt_id,
                            "result_id": result["id"],
                            "result_sha256": result["content_sha256"],
                        }
                        run_changed = connection.execute(
                            "UPDATE runs SET state = 'queued', "
                            "version = version + 1, updated_at = ?, "
                            "finished_at = NULL WHERE id = ? AND version = ?",
                            (now, attempt["run_id"], attempt["run_version"]),
                        ).rowcount
                        if run_changed != 1:
                            raise ConcurrentUpdateError("run changed concurrently")
                        connection.execute(
                            "INSERT INTO run_events "
                            "(run_id, sequence, kind, from_state, to_state, "
                            "metadata_json, created_at) VALUES (?, ?, "
                            "'run.transitioned', 'waiting_for_input', 'queued', ?, ?)",
                            (
                                attempt["run_id"],
                                attempt["run_version"] + 1,
                                canonical_json(run_metadata),
                                now,
                            ),
                        )
                elif run_state is RunState.CANCELLED:
                    outcome = AttemptState.CANCELLED
                    reason = "run was cancelled"
                else:
                    raise ConflictError(
                        f"run finished before its attempt: {run_state.value}"
                    )

                changed = connection.execute(
                    "UPDATE run_attempts SET state = ?, version = version + 1, "
                    "exit_code = ?, reason = ?, finished_at = ? "
                    "WHERE id = ? AND version = ?",
                    (
                        outcome.value,
                        exit_code,
                        reason,
                        now,
                        attempt_id,
                        expected_attempt_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run attempt changed concurrently")
                event_metadata: dict[str, Any] = {
                    "exit_code": exit_code,
                    "image_id": image_id,
                    "result_id": result["id"],
                    "result_sha256": result["content_sha256"],
                }
                if handoff is not None:
                    event_metadata["handoff_id"] = handoff["id"]
                if reason is not None:
                    event_metadata["reason"] = reason
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM run_attempt_events WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO run_attempt_events "
                    "(attempt_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, ?, "
                    "'attempt.completed', 'running', ?, ?, ?)",
                    (
                        attempt_id,
                        event_sequence,
                        outcome.value,
                        canonical_json(event_metadata),
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "run attempt outcome could not be recorded"
                ) from exc
        return self.get_run_attempt(attempt_id)

    def fail_run_attempt(
        self,
        attempt_id: str,
        *,
        expected_attempt_version: int,
        reason: str,
        interrupted: bool = False,
    ) -> dict[str, Any]:
        """Close an active attempt after cleanup, preserving durable waits."""

        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        reason = require_string(reason, field="reason", max_bytes=4096)
        if (
            isinstance(expected_attempt_version, bool)
            or not isinstance(expected_attempt_version, int)
            or expected_attempt_version < 1
        ):
            raise ValidationError("expected_attempt_version must be a positive integer")
        if not isinstance(interrupted, bool):
            raise ValidationError("interrupted must be boolean")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt = connection.execute(
                    "SELECT ra.run_id, ra.state, ra.version, r.state AS run_state, "
                    "ra.cancel_requested_at, ra.cancel_requested_by, "
                    "ra.cancel_reason, "
                    "r.version AS run_version FROM run_attempts ra "
                    "JOIN runs r ON r.id = ra.run_id WHERE ra.id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise NotFoundError(f"run attempt not found: {attempt_id}")
                if attempt["version"] != expected_attempt_version:
                    raise ConcurrentUpdateError(
                        f"attempt version is {attempt['version']}, "
                        f"expected {expected_attempt_version}"
                    )
                if attempt["state"] not in {
                    AttemptState.PREPARING.value,
                    AttemptState.RUNNING.value,
                }:
                    raise ConflictError(
                        f"run attempt is not active: {attempt['state']}"
                    )
                source_state = attempt["state"]
                run_state = RunState(attempt["run_state"])
                cancellation_requested = attempt["cancel_requested_at"] is not None
                if run_state is RunState.CANCELLED or cancellation_requested:
                    outcome = AttemptState.CANCELLED
                else:
                    outcome = (
                        AttemptState.INTERRUPTED if interrupted else AttemptState.FAILED
                    )
                if cancellation_requested:
                    cancellation_reason = attempt["cancel_reason"]
                    run_metadata = {
                        "reason": cancellation_reason,
                        "requested_by": attempt["cancel_requested_by"],
                    }
                    self._cancel_open_interactions(connection, attempt["run_id"], now)
                    changed = connection.execute(
                        "UPDATE runs SET state = 'cancelled', "
                        "version = version + 1, updated_at = ?, finished_at = ? "
                        "WHERE id = ? AND version = ?",
                        (now, now, attempt["run_id"], attempt["run_version"]),
                    ).rowcount
                    if changed != 1:
                        raise ConcurrentUpdateError("run changed concurrently")
                    connection.execute(
                        "INSERT INTO run_events "
                        "(run_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) VALUES (?, ?, "
                        "'run.transitioned', ?, 'cancelled', ?, ?)",
                        (
                            attempt["run_id"],
                            attempt["run_version"] + 1,
                            run_state.value,
                            canonical_json(run_metadata),
                            now,
                        ),
                    )
                    reason = cancellation_reason
                elif run_state in {RunState.PREPARING, RunState.RUNNING}:
                    run_metadata = {"reason": reason}
                    if interrupted:
                        run_metadata["reconciled"] = True
                    changed = connection.execute(
                        "UPDATE runs SET state = 'failed', version = version + 1, "
                        "updated_at = ?, finished_at = ? "
                        "WHERE id = ? AND version = ?",
                        (
                            now,
                            now,
                            attempt["run_id"],
                            attempt["run_version"],
                        ),
                    ).rowcount
                    if changed != 1:
                        raise ConcurrentUpdateError("run changed concurrently")
                    connection.execute(
                        "INSERT INTO run_events "
                        "(run_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) VALUES (?, ?, "
                        "'run.transitioned', ?, 'failed', ?, ?)",
                        (
                            attempt["run_id"],
                            attempt["run_version"] + 1,
                            run_state.value,
                            canonical_json(run_metadata),
                            now,
                        ),
                    )
                elif run_state not in {
                    RunState.QUEUED,
                    RunState.WAITING_FOR_INPUT,
                    RunState.CANCELLED,
                }:
                    raise ConflictError(
                        f"run finished before its attempt: {run_state.value}"
                    )
                changed = connection.execute(
                    "UPDATE run_attempts SET state = ?, version = version + 1, "
                    "reason = ?, finished_at = ? WHERE id = ? AND version = ?",
                    (
                        outcome.value,
                        reason,
                        now,
                        attempt_id,
                        expected_attempt_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run attempt changed concurrently")
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM run_attempt_events WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()[0]
                kind = "attempt.interrupted" if interrupted else "attempt.failed"
                connection.execute(
                    "INSERT INTO run_attempt_events "
                    "(attempt_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        attempt_id,
                        event_sequence,
                        kind,
                        source_state,
                        outcome.value,
                        canonical_json({"reason": reason}),
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "run attempt failure could not be recorded"
                ) from exc
        return self.get_run_attempt(attempt_id)
