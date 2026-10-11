from __future__ import annotations

import json
import sqlite3
from typing import Any

from ..domain import (
    AttemptState,
    HandoffEnvelope,
    HandoffWorkState,
    RunState,
    ValidationError,
    canonical_json,
    require_string,
)
from ..results import ResultError, ResultManifest
from .errors import (
    ConflictError,
    NotFoundError,
    StorageError,
)


class ArtifactStorage:
    @staticmethod
    def _handoff_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            envelope = HandoffEnvelope.from_mapping(json.loads(row["content_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored attempt handoff is invalid") from exc
        if envelope.digest != row["content_sha256"]:
            raise StorageError("stored attempt handoff digest does not match")
        return {
            "id": row["id"],
            "attempt_id": row["attempt_id"],
            "content_sha256": row["content_sha256"],
            "created_at": row["created_at"],
            **envelope.to_dict(),
        }

    @staticmethod
    def _attempt_result_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            manifest = ResultManifest.from_mapping(json.loads(row["manifest_json"]))
        except (json.JSONDecodeError, TypeError, ResultError) as exc:
            raise StorageError("stored attempt result is invalid") from exc
        if manifest.attempt_id != row["attempt_id"]:
            raise StorageError("stored attempt result belongs to another attempt")
        if manifest.digest != row["content_sha256"]:
            raise StorageError("stored attempt result digest does not match")
        if (
            manifest.changed_paths != row["changed_paths"]
            or manifest.changed_bytes != row["changed_bytes"]
        ):
            raise StorageError("stored attempt result summary does not match")
        return {
            "id": row["id"],
            "attempt_id": row["attempt_id"],
            "schema_version": row["schema_version"],
            "manifest": manifest.to_dict(),
            "content_sha256": row["content_sha256"],
            "changed_paths": row["changed_paths"],
            "changed_bytes": row["changed_bytes"],
            "created_at": row["created_at"],
        }

    def record_attempt_result(
        self, attempt_id: str, manifest: ResultManifest
    ) -> dict[str, Any]:
        """Bind one immutable host-captured result to its running attempt."""

        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        if not isinstance(manifest, ResultManifest):
            raise ValidationError("result must be a validated manifest")
        if manifest.attempt_id != attempt_id:
            raise ValidationError("result belongs to another attempt")
        result_id = self.new_id("rsl")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt = connection.execute(
                    "SELECT state, source_revision FROM run_attempts WHERE id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise NotFoundError(f"run attempt not found: {attempt_id}")
                if attempt["state"] != AttemptState.RUNNING.value:
                    raise ConflictError(
                        f"run attempt is not running: {attempt['state']}"
                    )
                if attempt["source_revision"] != manifest.source_revision:
                    raise ConflictError("result source revision does not match")
                existing = connection.execute(
                    "SELECT id, attempt_id, schema_version, manifest_json, "
                    "content_sha256, changed_paths, changed_bytes, created_at "
                    "FROM attempt_results WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != manifest.digest:
                        raise ConflictError(
                            "run attempt already has a different result"
                        )
                    connection.commit()
                    return self._attempt_result_record(existing)
                connection.execute(
                    "INSERT INTO attempt_results "
                    "(id, attempt_id, schema_version, manifest_json, "
                    "content_sha256, changed_paths, changed_bytes, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        result_id,
                        attempt_id,
                        manifest.schema_version,
                        manifest.canonical,
                        manifest.digest,
                        manifest.changed_paths,
                        manifest.changed_bytes,
                        now,
                    ),
                )
                stored = connection.execute(
                    "SELECT id, attempt_id, schema_version, manifest_json, "
                    "content_sha256, changed_paths, changed_bytes, created_at "
                    "FROM attempt_results WHERE id = ?",
                    (result_id,),
                ).fetchone()
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("attempt result could not be recorded") from exc
        if stored is None:
            raise StorageError("attempt result was not recorded")
        return self._attempt_result_record(stored)

    def get_attempt_result(self, attempt_id: str) -> dict[str, Any]:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT id, attempt_id, schema_version, manifest_json, "
                "content_sha256, changed_paths, changed_bytes, created_at "
                "FROM attempt_results WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"attempt result not found: {attempt_id}")
        return self._attempt_result_record(row)

    def get_run_attempt(self, attempt_id: str) -> dict[str, Any]:
        with self._read_snapshot() as connection:
            return self._get_run_attempt(connection, attempt_id)

    def _get_run_attempt(
        self, connection: sqlite3.Connection, attempt_id: str
    ) -> dict[str, Any]:
        with self._connection(connection) as connection:
            row = connection.execute(
                "SELECT ra.id, ra.run_id, ra.sequence, ra.runtime_adapter, "
                "ra.state, ra.version, ra.source_revision, ra.container_id, "
                "ra.container_name, ra.image_id, ra.exit_code, ra.reason, "
                "ra.owner_id, ra.heartbeat_at, ra.lease_expires_at, "
                "ra.cancel_requested_at, ra.cancel_requested_by, ra.cancel_reason, "
                "ra.created_at, ra.started_at, ra.finished_at, "
                "r.state AS run_state, r.version AS run_version, "
                "ap.name AS profile_name, av.id AS profile_version_id, "
                "av.version AS profile_version "
                "FROM run_attempts ra JOIN runs r ON r.id = ra.run_id "
                "JOIN agent_profile_versions av ON av.id = ra.profile_version_id "
                "JOIN agent_profiles ap ON ap.id = av.profile_id WHERE ra.id = ?",
                (attempt_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"run attempt not found: {attempt_id}")
            events = connection.execute(
                "SELECT sequence, kind, from_state, to_state, metadata_json, "
                "created_at FROM run_attempt_events WHERE attempt_id = ? "
                "ORDER BY sequence",
                (attempt_id,),
            ).fetchall()
            handoff = connection.execute(
                "SELECT id, attempt_id, content_json, content_sha256, created_at "
                "FROM attempt_handoffs WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            result_row = connection.execute(
                "SELECT id, attempt_id, schema_version, manifest_json, "
                "content_sha256, changed_paths, changed_bytes, created_at "
                "FROM attempt_results WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
        result = dict(row)
        try:
            result["events"] = [
                {
                    **dict(event),
                    "metadata": json.loads(event["metadata_json"]),
                }
                for event in events
            ]
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored run attempt event is invalid") from exc
        for event in result["events"]:
            event.pop("metadata_json")
        result["handoff"] = None if handoff is None else self._handoff_record(handoff)
        result["result"] = (
            None if result_row is None else self._attempt_result_record(result_row)
        )
        return result

    def list_run_attempts(self, run_id: str) -> list[dict[str, Any]]:
        with self._read_snapshot() as connection:
            return self._list_run_attempts(connection, run_id)

    def _list_run_attempts(
        self, connection: sqlite3.Connection, run_id: str
    ) -> list[dict[str, Any]]:
        with self._connection(connection) as connection:
            exists = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                raise NotFoundError(f"run not found: {run_id}")
            rows = connection.execute(
                "SELECT id FROM run_attempts WHERE run_id = ? ORDER BY sequence",
                (run_id,),
            ).fetchall()
        return [self._get_run_attempt(connection, row["id"]) for row in rows]

    def create_attempt_handoff(
        self, run_id: str, handoff: HandoffEnvelope
    ) -> dict[str, Any]:
        if not isinstance(handoff, HandoffEnvelope):
            raise ValidationError("handoff must be a validated envelope")
        if handoff.work_state is not HandoffWorkState.WAITING_FOR_INPUT:
            raise ValidationError("handoff work_state must be waiting_for_input")
        handoff_id = self.new_id("hof")
        now = self.now()
        stored_attempt_id: str | None = None
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                run = connection.execute(
                    "SELECT state FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                if run is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if run["state"] not in {
                    RunState.WAITING_FOR_INPUT.value,
                    RunState.QUEUED.value,
                }:
                    raise ConflictError(f"run is not waiting for input: {run['state']}")
                attempt = connection.execute(
                    "SELECT id, state FROM run_attempts WHERE run_id = ? "
                    "AND state IN ('preparing', 'running')",
                    (run_id,),
                ).fetchone()
                if attempt is None or attempt["state"] != AttemptState.RUNNING.value:
                    raise ConflictError("run has no active running attempt")
                attempt_interactions = connection.execute(
                    "SELECT id FROM interactions WHERE run_id = ? AND attempt_id = ? "
                    "AND state IN ('open', 'answered') ORDER BY id",
                    (run_id, attempt["id"]),
                ).fetchall()
                expected_interactions = tuple(row["id"] for row in attempt_interactions)
                if (
                    not expected_interactions
                    or not handoff.pending_interaction_ids
                    or len(expected_interactions)
                    != len(handoff.pending_interaction_ids)
                    or set(expected_interactions)
                    != set(handoff.pending_interaction_ids)
                ):
                    raise ConflictError(
                        "handoff pending interactions do not match the run"
                    )
                existing = connection.execute(
                    "SELECT id, attempt_id, content_json, content_sha256, created_at "
                    "FROM attempt_handoffs WHERE attempt_id = ?",
                    (attempt["id"],),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != handoff.digest:
                        raise ConflictError(
                            "run attempt already has a different handoff"
                        )
                    stored_attempt_id = attempt["id"]
                else:
                    connection.execute(
                        "INSERT INTO attempt_handoffs "
                        "(id, attempt_id, content_json, content_sha256, created_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            handoff_id,
                            attempt["id"],
                            handoff.canonical,
                            handoff.digest,
                            now,
                        ),
                    )
                    sequence = connection.execute(
                        "SELECT COALESCE(MAX(sequence), 0) + 1 "
                        "FROM run_attempt_events WHERE attempt_id = ?",
                        (attempt["id"],),
                    ).fetchone()[0]
                    connection.execute(
                        "INSERT INTO run_attempt_events "
                        "(attempt_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) VALUES (?, ?, "
                        "'attempt.handoff_submitted', 'running', 'running', ?, ?)",
                        (
                            attempt["id"],
                            sequence,
                            canonical_json(
                                {
                                    "handoff_id": handoff_id,
                                    "content_sha256": handoff.digest,
                                }
                            ),
                            now,
                        ),
                    )
                    stored_attempt_id = attempt["id"]
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("attempt handoff could not be recorded") from exc
        if stored_attempt_id is None:
            raise StorageError("attempt handoff was not recorded")
        result = self.get_run_attempt(stored_attempt_id)["handoff"]
        if result is None:
            raise StorageError("attempt handoff was not recorded")
        return result
