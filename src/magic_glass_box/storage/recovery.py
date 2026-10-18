from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Any

from ..domain import ValidationError, canonical_json, require_string
from ..inference import MAX_PROVIDER_RETRY_AFTER_SECONDS
from .errors import ConflictError, NotFoundError, StorageError

PROVIDER_RECOVERY_CATEGORIES = frozenset({"quota_exhausted", "rate_limited"})


class RecoveryStorage:
    """Durable provider backoff attached to one run and failed invocation."""

    @staticmethod
    def _provider_recovery_deadline(now: str, retry_after_seconds: int) -> str:
        try:
            current = datetime.fromisoformat(now)
        except ValueError as exc:
            raise StorageError("database clock returned an invalid timestamp") from exc
        if current.tzinfo is None or current.utcoffset() is None:
            raise StorageError("database clock returned a naive timestamp")
        return (current + timedelta(seconds=retry_after_seconds)).isoformat(
            timespec="microseconds"
        )

    @staticmethod
    def _validate_provider_recovery(
        *, category: object, retry_after_seconds: object
    ) -> tuple[str, int]:
        if category not in PROVIDER_RECOVERY_CATEGORIES:
            raise ValidationError("provider recovery category is invalid")
        if (
            isinstance(retry_after_seconds, bool)
            or not isinstance(retry_after_seconds, int)
            or not 1 <= retry_after_seconds <= MAX_PROVIDER_RETRY_AFTER_SECONDS
        ):
            raise ValidationError(
                "retry_after_seconds must be between 1 and "
                f"{MAX_PROVIDER_RETRY_AFTER_SECONDS}"
            )
        assert isinstance(category, str)
        return category, retry_after_seconds

    def _pause_run_for_provider_recovery(
        self,
        connection: sqlite3.Connection,
        *,
        invocation_id: str,
        category: str,
        retry_after_seconds: int,
        now: str,
    ) -> str:
        """Atomically pause the active run after its invocation is final."""

        category, retry_after_seconds = self._validate_provider_recovery(
            category=category,
            retry_after_seconds=retry_after_seconds,
        )
        invocation = connection.execute(
            "SELECT run_id, outcome, error_code, http_status "
            "FROM provider_invocations WHERE id = ?",
            (invocation_id,),
        ).fetchone()
        if invocation is None:
            raise NotFoundError(f"provider invocation not found: {invocation_id}")
        if invocation["outcome"] != "failed" or invocation["error_code"] is None:
            raise ConflictError(
                "provider recovery requires an explicitly failed invocation"
            )
        run = connection.execute(
            "SELECT state, version FROM runs WHERE id = ?",
            (invocation["run_id"],),
        ).fetchone()
        if run is None:
            raise NotFoundError(f"run not found: {invocation['run_id']}")
        if run["state"] not in {"running", "waiting_for_input"}:
            raise ConflictError(f"run is not running: {run['state']}")
        source_state = run["state"]
        attempt = connection.execute(
            "SELECT id FROM run_attempts WHERE run_id = ? AND state = 'running'",
            (invocation["run_id"],),
        ).fetchone()
        if attempt is None:
            raise ConflictError("provider recovery requires an active run attempt")
        recovery_id = self.new_id("prc")
        eligible_at = self._provider_recovery_deadline(now, retry_after_seconds)
        connection.execute(
            "INSERT INTO provider_recoveries "
            "(id, run_id, attempt_id, invocation_id, category, state, version, "
            "retry_after_seconds, eligible_at, error_code, provider_status, "
            "created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 'active', 1, ?, ?, ?, ?, ?, ?)",
            (
                recovery_id,
                invocation["run_id"],
                attempt["id"],
                invocation_id,
                category,
                retry_after_seconds,
                eligible_at,
                invocation["error_code"],
                invocation["http_status"],
                now,
                now,
            ),
        )
        changed = connection.execute(
            "UPDATE runs SET state = 'paused', version = version + 1, "
            "updated_at = ?, finished_at = NULL WHERE id = ? AND version = ? "
            "AND state = ?",
            (now, invocation["run_id"], run["version"], source_state),
        ).rowcount
        if changed != 1:
            raise ConflictError("run changed while recording provider recovery")
        metadata = canonical_json(
            {
                "attempt_id": attempt["id"],
                "category": category,
                "eligible_at": eligible_at,
                "invocation_id": invocation_id,
                "recovery_id": recovery_id,
                "retry_after_seconds": retry_after_seconds,
            }
        )
        connection.execute(
            "INSERT INTO run_events "
            "(run_id, sequence, kind, from_state, to_state, metadata_json, "
            "created_at) VALUES (?, ?, 'run.provider_paused', ?, "
            "'paused', ?, ?)",
            (
                invocation["run_id"],
                run["version"] + 1,
                source_state,
                metadata,
                now,
            ),
        )
        return recovery_id

    @staticmethod
    def _cancel_active_provider_recoveries(
        connection: sqlite3.Connection, run_id: str, now: str
    ) -> None:
        connection.execute(
            "UPDATE provider_recoveries SET state = 'cancelled', "
            "version = version + 1, updated_at = ?, resolved_at = ? "
            "WHERE run_id = ? AND state = 'active'",
            (now, now, run_id),
        )

    def list_provider_recoveries(self, run_id: str) -> list[dict[str, Any]]:
        run_id = require_string(run_id, field="run_id", max_bytes=64)
        with self._read_snapshot() as connection:
            known = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if known is None:
                raise NotFoundError(f"run not found: {run_id}")
            return self._list_provider_recoveries(connection, run_id)

    @staticmethod
    def _list_provider_recoveries(
        connection: sqlite3.Connection, run_id: str
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            "SELECT id, run_id, attempt_id, invocation_id, category, state, "
            "version, retry_after_seconds, eligible_at, error_code, "
            "provider_status, created_at, updated_at, resolved_at "
            "FROM provider_recoveries WHERE run_id = ? ORDER BY created_at, id",
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]
