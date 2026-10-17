from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any

from ..domain import (
    ValidationError,
    WorkAssignmentSpec,
    WorkLogEntry,
    WorkLogKind,
    WorkMemoryCheckpoint,
    canonical_json,
    require_string,
    validate_name,
)
from ..krab import IDEMPOTENCY_KEY_RE
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)
from .schema import (
    MAX_VISIBLE_ASSIGNMENT_EVENTS,
    MAX_WORK_ASSIGNMENTS_PER_THREAD,
)


class WorkStorage:
    @staticmethod
    def _work_idempotency_key(value: object) -> str:
        if not isinstance(value, str) or IDEMPOTENCY_KEY_RE.fullmatch(value) is None:
            raise ValidationError("idempotency_key is invalid")
        return value

    @staticmethod
    def _positive_integer(value: object, *, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError(f"{field} must be a positive integer")
        return value

    @staticmethod
    def _work_author(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT r.work_thread_id, r.profile_version_id, r.state AS run_state, "
            "wt.state AS thread_state, ra.id AS attempt_id, "
            "ra.runtime_adapter, ap.name AS profile_name, "
            "av.version AS profile_version "
            "FROM runs r LEFT JOIN work_threads wt ON wt.id = r.work_thread_id "
            "LEFT JOIN run_attempts ra ON ra.run_id = r.id AND ra.state = 'running' "
            "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
            "JOIN agent_profiles ap ON ap.id = av.profile_id WHERE r.id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"run not found: {run_id}")
        if row["work_thread_id"] is None:
            raise ConflictError("run has no durable work thread")
        if row["thread_state"] != "active":
            raise ConflictError(f"work thread is not active: {row['thread_state']}")
        if row["attempt_id"] is None:
            raise ConflictError("run has no active running attempt")
        return row

    @staticmethod
    def _work_log_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            entry = WorkLogEntry.from_mapping(json.loads(row["content_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored work-log entry is invalid") from exc
        if entry.digest != row["content_sha256"] or entry.kind.value != row["kind"]:
            raise StorageError("stored work-log entry digest does not match")
        return {
            "id": row["id"],
            "thread_id": row["work_thread_id"],
            "sequence": row["sequence"],
            "content_sha256": row["content_sha256"],
            "assignment_id": row["assignment_id"],
            "author": {
                "run_id": row["author_run_id"],
                "attempt_id": row["author_attempt_id"],
                "profile_name": row["profile_name"],
                "profile_version_id": row["profile_version_id"],
                "profile_version": row["profile_version"],
                "runtime_adapter": row["runtime_adapter"],
            },
            "created_at": row["created_at"],
            **entry.to_dict(),
        }

    @staticmethod
    def _work_log_row(connection: sqlite3.Connection, entry_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT e.id, e.work_thread_id, e.sequence, e.kind, e.content_json, "
            "e.content_sha256, e.author_run_id, e.author_attempt_id, "
            "e.assignment_id, e.created_at, ap.name AS profile_name, "
            "av.id AS profile_version_id, av.version AS profile_version, "
            "ra.runtime_adapter FROM work_log_entries e "
            "JOIN run_attempts ra ON ra.id = e.author_attempt_id "
            "JOIN agent_profile_versions av ON av.id = ra.profile_version_id "
            "JOIN agent_profiles ap ON ap.id = av.profile_id WHERE e.id = ?",
            (entry_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work-log entry not found: {entry_id}")
        return row

    @staticmethod
    def _checkpoint_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            checkpoint = WorkMemoryCheckpoint.from_mapping(
                json.loads(row["content_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored work-memory checkpoint is invalid") from exc
        if checkpoint.digest != row["content_sha256"]:
            raise StorageError("stored work-memory checkpoint digest does not match")
        return {
            "id": row["id"],
            "thread_id": row["work_thread_id"],
            "version": row["version"],
            "through_sequence": row["through_sequence"],
            "content_sha256": row["content_sha256"],
            "producer": {
                "run_id": row["producer_run_id"],
                "attempt_id": row["producer_attempt_id"],
                "profile_name": row["profile_name"],
                "profile_version_id": row["profile_version_id"],
                "profile_version": row["profile_version"],
                "runtime_adapter": row["runtime_adapter"],
            },
            "created_at": row["created_at"],
            **checkpoint.to_dict(),
        }

    @staticmethod
    def _checkpoint_row(
        connection: sqlite3.Connection, checkpoint_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT c.id, c.work_thread_id, c.version, c.through_sequence, "
            "c.content_json, c.content_sha256, c.producer_run_id, "
            "c.producer_attempt_id, c.created_at, ap.name AS profile_name, "
            "av.id AS profile_version_id, av.version AS profile_version, "
            "ra.runtime_adapter FROM work_memory_checkpoints c "
            "JOIN run_attempts ra ON ra.id = c.producer_attempt_id "
            "JOIN agent_profile_versions av ON av.id = ra.profile_version_id "
            "JOIN agent_profiles ap ON ap.id = av.profile_id WHERE c.id = ?",
            (checkpoint_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work-memory checkpoint not found: {checkpoint_id}")
        return row

    @staticmethod
    def _assignment_record(
        connection: sqlite3.Connection, assignment_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT a.id, a.work_thread_id, a.sequence, a.spec_json, "
            "a.content_sha256, a.state, a.version, a.target_profile_version_id, "
            "target.name AS target_profile_name, "
            "target_version.version AS target_profile_version, "
            "a.created_by_run_id, a.created_by_attempt_id, "
            "creator.name AS creator_profile_name, a.claimed_by_run_id, "
            "a.claimed_by_attempt_id, claimant.name AS claimant_profile_name, "
            "a.lease_expires_at, a.result_entry_id, a.created_at, a.updated_at, "
            "a.completed_at FROM work_assignments a "
            "JOIN agent_profile_versions target_version "
            "ON target_version.id = a.target_profile_version_id "
            "JOIN agent_profiles target ON target.id = target_version.profile_id "
            "JOIN runs creator_run ON creator_run.id = a.created_by_run_id "
            "JOIN agent_profile_versions creator_version "
            "ON creator_version.id = creator_run.profile_version_id "
            "JOIN agent_profiles creator ON creator.id = creator_version.profile_id "
            "LEFT JOIN runs claimant_run "
            "ON claimant_run.id = a.claimed_by_run_id "
            "LEFT JOIN agent_profile_versions claimant_version "
            "ON claimant_version.id = claimant_run.profile_version_id "
            "LEFT JOIN agent_profiles claimant "
            "ON claimant.id = claimant_version.profile_id WHERE a.id = ?",
            (assignment_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work assignment not found: {assignment_id}")
        try:
            spec = WorkAssignmentSpec.from_mapping(json.loads(row["spec_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored work assignment is invalid") from exc
        if spec.digest != row["content_sha256"]:
            raise StorageError("stored work assignment digest does not match")
        event_count = connection.execute(
            "SELECT COUNT(*) FROM work_assignment_events WHERE assignment_id = ?",
            (assignment_id,),
        ).fetchone()[0]
        events = connection.execute(
            "SELECT sequence, kind, from_state, to_state, actor_run_id, "
            "actor_attempt_id, metadata_json, created_at "
            "FROM work_assignment_events WHERE assignment_id = ? "
            "ORDER BY sequence DESC LIMIT ?",
            (assignment_id, MAX_VISIBLE_ASSIGNMENT_EVENTS),
        ).fetchall()
        try:
            parsed_events = [
                {**dict(event), "metadata": json.loads(event["metadata_json"])}
                for event in reversed(events)
            ]
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored work assignment event is invalid") from exc
        for event in parsed_events:
            event.pop("metadata_json")
        return {
            "id": row["id"],
            "thread_id": row["work_thread_id"],
            "sequence": row["sequence"],
            "state": row["state"],
            "version": row["version"],
            "content_sha256": row["content_sha256"],
            "spec": spec.to_dict(),
            "target": {
                "profile_name": row["target_profile_name"],
                "profile_version_id": row["target_profile_version_id"],
                "profile_version": row["target_profile_version"],
            },
            "created_by": {
                "run_id": row["created_by_run_id"],
                "attempt_id": row["created_by_attempt_id"],
                "profile_name": row["creator_profile_name"],
            },
            "claimed_by_run_id": row["claimed_by_run_id"],
            "claimed_by_attempt_id": row["claimed_by_attempt_id"],
            "claimant_profile_name": row["claimant_profile_name"],
            "lease_expires_at": row["lease_expires_at"],
            "result_entry_id": row["result_entry_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
            "event_count": event_count,
            "events_truncated": event_count > len(parsed_events),
            "events": parsed_events,
        }

    @staticmethod
    def _validate_related_interactions(
        connection: sqlite3.Connection,
        *,
        thread_id: str,
        entry: WorkLogEntry,
    ) -> None:
        if not entry.related_interaction_ids:
            return
        for interaction_id in entry.related_interaction_ids:
            exists = connection.execute(
                "SELECT 1 FROM interactions i JOIN runs r ON r.id = i.run_id "
                "WHERE r.work_thread_id = ? AND i.id = ?",
                (thread_id, interaction_id),
            ).fetchone()
            if exists is None:
                raise ConflictError(
                    "related interaction does not belong to work thread"
                )

    def get_work_memory_for_run(
        self,
        run_id: str,
        *,
        after_sequence: int | None = None,
        limit: int = 16,
    ) -> dict[str, Any]:
        if after_sequence is not None and (
            isinstance(after_sequence, bool)
            or not isinstance(after_sequence, int)
            or after_sequence < 0
        ):
            raise ValidationError("after_sequence must be a non-negative integer")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 64
        ):
            raise ValidationError("limit must be between 1 and 64")
        with self._read_snapshot() as connection:
            return self._get_work_memory_for_run(
                connection,
                run_id,
                after_sequence=after_sequence,
                limit=limit,
            )

    def _get_work_memory_for_run(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        after_sequence: int | None,
        limit: int,
    ) -> dict[str, Any]:
        with self._connection(connection) as connection:
            thread = connection.execute(
                "SELECT wt.id, wt.work_item_id, wt.generation, wt.state, "
                "wt.version, wt.log_sequence, wt.created_at, wt.updated_at, "
                "wt.completed_at FROM runs r JOIN work_threads wt "
                "ON wt.id = r.work_thread_id WHERE r.id = ?",
                (run_id,),
            ).fetchone()
            if thread is None:
                run_exists = connection.execute(
                    "SELECT 1 FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                if run_exists is None:
                    raise NotFoundError(f"run not found: {run_id}")
                raise ConflictError("run has no durable work thread")
            checkpoint_row = connection.execute(
                "SELECT c.id, c.work_thread_id, c.version, c.through_sequence, "
                "c.content_json, c.content_sha256, c.producer_run_id, "
                "c.producer_attempt_id, c.created_at, ap.name AS profile_name, "
                "av.id AS profile_version_id, av.version AS profile_version, "
                "ra.runtime_adapter FROM work_memory_checkpoints c "
                "JOIN run_attempts ra ON ra.id = c.producer_attempt_id "
                "JOIN agent_profile_versions av ON av.id = ra.profile_version_id "
                "JOIN agent_profiles ap ON ap.id = av.profile_id "
                "WHERE c.work_thread_id = ? ORDER BY c.version DESC LIMIT 1",
                (thread["id"],),
            ).fetchone()
            checkpoint_value = (
                None
                if checkpoint_row is None
                else self._checkpoint_record(checkpoint_row)
            )
            effective_after = (
                after_sequence
                if after_sequence is not None
                else (
                    0
                    if checkpoint_value is None
                    else checkpoint_value["through_sequence"]
                )
            )
            rows = connection.execute(
                "SELECT e.id FROM work_log_entries e WHERE e.work_thread_id = ? "
                "AND e.sequence > ? ORDER BY e.sequence LIMIT ?",
                (thread["id"], effective_after, limit + 1),
            ).fetchall()
            page_rows = rows[:limit]
            entries = [
                self._work_log_record(self._work_log_row(connection, row["id"]))
                for row in page_rows
            ]
            assignment_rows = connection.execute(
                "SELECT id FROM work_assignments WHERE work_thread_id = ? "
                "ORDER BY sequence",
                (thread["id"],),
            ).fetchall()
            assignments = [
                self._assignment_record(connection, row["id"])
                for row in assignment_rows
            ]
        result = dict(thread)
        result["thread_id"] = result.pop("id")
        result["checkpoint"] = checkpoint_value
        result["entries"] = entries
        result["entries_truncated"] = len(rows) > limit
        result["next_after_sequence"] = (
            entries[-1]["sequence"] if len(rows) > limit and entries else None
        )
        result["assignments"] = assignments
        return result

    def append_work_log(
        self,
        run_id: str,
        entry: WorkLogEntry,
        *,
        idempotency_key: str,
        assignment_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(entry, WorkLogEntry):
            raise ValidationError("entry must be a validated WorkLogEntry")
        idempotency_key = self._work_idempotency_key(idempotency_key)
        if assignment_id is not None:
            assignment_id = require_string(
                assignment_id, field="assignment_id", max_bytes=64
            )
        entry_id = self.new_id("wle")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                existing = connection.execute(
                    "SELECT id, content_sha256, assignment_id FROM work_log_entries "
                    "WHERE work_thread_id = ? AND author_run_id = ? "
                    "AND idempotency_key = ?",
                    (author["work_thread_id"], run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["content_sha256"] != entry.digest
                        or existing["assignment_id"] != assignment_id
                    ):
                        raise ConflictError(
                            "idempotency key already names a different work-log entry"
                        )
                    stored_id = existing["id"]
                else:
                    if assignment_id is not None:
                        assignment_row = connection.execute(
                            "SELECT state, claimed_by_run_id, claimed_by_attempt_id, "
                            "lease_expires_at, work_thread_id FROM work_assignments "
                            "WHERE id = ?",
                            (assignment_id,),
                        ).fetchone()
                        if assignment_row is None:
                            raise NotFoundError(
                                f"work assignment not found: {assignment_id}"
                            )
                        if assignment_row["work_thread_id"] != author["work_thread_id"]:
                            raise ConflictError(
                                "work assignment belongs to another thread"
                            )
                        if (
                            assignment_row["state"] != "claimed"
                            or assignment_row["claimed_by_run_id"] != run_id
                            or assignment_row["claimed_by_attempt_id"]
                            != author["attempt_id"]
                        ):
                            raise ConflictError(
                                "work assignment is claimed by another run"
                            )
                        if assignment_row["lease_expires_at"] <= now:
                            raise ConflictError("work assignment lease has expired")
                    self._validate_related_interactions(
                        connection,
                        thread_id=author["work_thread_id"],
                        entry=entry,
                    )
                    thread = connection.execute(
                        "SELECT log_sequence FROM work_threads WHERE id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()
                    sequence = thread["log_sequence"] + 1
                    connection.execute(
                        "INSERT INTO work_log_entries "
                        "(id, work_thread_id, sequence, kind, content_json, "
                        "content_sha256, author_run_id, author_attempt_id, "
                        "assignment_id, idempotency_key, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            entry_id,
                            author["work_thread_id"],
                            sequence,
                            entry.kind.value,
                            entry.canonical,
                            entry.digest,
                            run_id,
                            author["attempt_id"],
                            assignment_id,
                            idempotency_key,
                            now,
                        ),
                    )
                    connection.execute(
                        "UPDATE work_threads SET log_sequence = ?, "
                        "version = version + 1, updated_at = ? WHERE id = ?",
                        (sequence, now, author["work_thread_id"]),
                    )
                    stored_id = entry_id
                result = self._work_log_record(
                    self._work_log_row(connection, stored_id)
                )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work-log entry could not be recorded") from exc
        return result

    def create_work_memory_checkpoint(
        self,
        run_id: str,
        checkpoint: WorkMemoryCheckpoint,
        *,
        through_sequence: int,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if not isinstance(checkpoint, WorkMemoryCheckpoint):
            raise ValidationError("checkpoint must be a validated WorkMemoryCheckpoint")
        if (
            isinstance(through_sequence, bool)
            or not isinstance(through_sequence, int)
            or through_sequence < 0
        ):
            raise ValidationError("through_sequence must be a non-negative integer")
        self._positive_integer(expected_thread_version, field="expected_thread_version")
        idempotency_key = self._work_idempotency_key(idempotency_key)
        checkpoint_id = self.new_id("wmc")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                existing = connection.execute(
                    "SELECT id, content_sha256, through_sequence "
                    "FROM work_memory_checkpoints WHERE work_thread_id = ? "
                    "AND producer_run_id = ? AND idempotency_key = ?",
                    (author["work_thread_id"], run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["content_sha256"] != checkpoint.digest
                        or existing["through_sequence"] != through_sequence
                    ):
                        raise ConflictError(
                            "idempotency key already names a different checkpoint"
                        )
                    stored_id = existing["id"]
                else:
                    thread = connection.execute(
                        "SELECT version, log_sequence FROM work_threads WHERE id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()
                    if thread["version"] != expected_thread_version:
                        raise ConcurrentUpdateError(
                            f"work thread version is {thread['version']}, "
                            f"expected {expected_thread_version}"
                        )
                    if through_sequence != thread["log_sequence"]:
                        raise ConflictError(
                            "checkpoint must cover the latest log sequence"
                        )
                    version = connection.execute(
                        "SELECT COALESCE(MAX(version), 0) + 1 "
                        "FROM work_memory_checkpoints WHERE work_thread_id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()[0]
                    connection.execute(
                        "INSERT INTO work_memory_checkpoints "
                        "(id, work_thread_id, version, through_sequence, "
                        "content_json, content_sha256, producer_run_id, "
                        "producer_attempt_id, idempotency_key, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            checkpoint_id,
                            author["work_thread_id"],
                            version,
                            through_sequence,
                            checkpoint.canonical,
                            checkpoint.digest,
                            run_id,
                            author["attempt_id"],
                            idempotency_key,
                            now,
                        ),
                    )
                    changed = connection.execute(
                        "UPDATE work_threads SET version = version + 1, "
                        "updated_at = ? WHERE id = ? AND version = ?",
                        (
                            now,
                            author["work_thread_id"],
                            expected_thread_version,
                        ),
                    ).rowcount
                    if changed != 1:
                        raise ConcurrentUpdateError("work thread changed concurrently")
                    stored_id = checkpoint_id
                result = self._checkpoint_record(
                    self._checkpoint_row(connection, stored_id)
                )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "work-memory checkpoint could not be recorded"
                ) from exc
        return result

    def create_work_assignment(
        self,
        run_id: str,
        *,
        target_profile_name: str,
        spec: WorkAssignmentSpec,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        target_profile_name = validate_name(
            target_profile_name, field="target profile name"
        )
        if not isinstance(spec, WorkAssignmentSpec):
            raise ValidationError("spec must be a validated WorkAssignmentSpec")
        self._positive_integer(expected_thread_version, field="expected_thread_version")
        idempotency_key = self._work_idempotency_key(idempotency_key)
        assignment_id = self.new_id("was")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                existing = connection.execute(
                    "SELECT a.id, a.target_profile_version_id, a.content_sha256, "
                    "ap.name AS target_profile_name FROM work_assignments a "
                    "JOIN agent_profile_versions av "
                    "ON av.id = a.target_profile_version_id "
                    "JOIN agent_profiles ap ON ap.id = av.profile_id "
                    "WHERE a.work_thread_id = ? "
                    "AND created_by_run_id = ? AND idempotency_key = ?",
                    (author["work_thread_id"], run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["target_profile_name"] != target_profile_name
                        or existing["content_sha256"] != spec.digest
                    ):
                        raise ConflictError(
                            "idempotency key already names a different work assignment"
                        )
                    stored_id = existing["id"]
                else:
                    target = connection.execute(
                        "SELECT av.id, av.version, ki.krab_instance_id "
                        "FROM agent_profiles ap JOIN agent_profile_versions av "
                        "ON av.profile_id = ap.id JOIN krab_identities ki "
                        "ON ki.id = av.krab_identity_id WHERE ap.name = ? "
                        "AND ap.active = 1 ORDER BY av.version DESC LIMIT 1",
                        (target_profile_name,),
                    ).fetchone()
                    if target is None:
                        raise NotFoundError(
                            f"active agent profile not found: {target_profile_name}"
                        )
                    project_instance = connection.execute(
                        "SELECT p.krab_instance_id FROM work_threads wt "
                        "JOIN work_items wi ON wi.id = wt.work_item_id "
                        "JOIN projects p ON p.id = wi.project_id WHERE wt.id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()["krab_instance_id"]
                    if target["krab_instance_id"] != project_instance:
                        raise ConflictError(
                            "target profile and work item belong to different "
                            "Krab instances"
                        )
                    thread = connection.execute(
                        "SELECT version FROM work_threads WHERE id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()
                    if thread["version"] != expected_thread_version:
                        raise ConcurrentUpdateError(
                            f"work thread version is {thread['version']}, "
                            f"expected {expected_thread_version}"
                        )
                    assignment_count = connection.execute(
                        "SELECT COUNT(*) FROM work_assignments "
                        "WHERE work_thread_id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()[0]
                    if assignment_count >= MAX_WORK_ASSIGNMENTS_PER_THREAD:
                        raise ConflictError(
                            "work thread has reached its assignment limit"
                        )
                    sequence = connection.execute(
                        "SELECT COALESCE(MAX(sequence), 0) + 1 "
                        "FROM work_assignments WHERE work_thread_id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()[0]
                    connection.execute(
                        "INSERT INTO work_assignments "
                        "(id, work_thread_id, sequence, target_profile_version_id, "
                        "spec_json, content_sha256, state, version, "
                        "created_by_run_id, created_by_attempt_id, idempotency_key, "
                        "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, "
                        "'open', 1, ?, ?, ?, ?, ?)",
                        (
                            assignment_id,
                            author["work_thread_id"],
                            sequence,
                            target["id"],
                            spec.canonical,
                            spec.digest,
                            run_id,
                            author["attempt_id"],
                            idempotency_key,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO work_assignment_events "
                        "(assignment_id, sequence, kind, from_state, to_state, "
                        "actor_run_id, actor_attempt_id, metadata_json, created_at) "
                        "VALUES (?, 1, 'assignment.created', NULL, 'open', ?, ?, ?, ?)",
                        (
                            assignment_id,
                            run_id,
                            author["attempt_id"],
                            canonical_json({"target_profile_version_id": target["id"]}),
                            now,
                        ),
                    )
                    changed = connection.execute(
                        "UPDATE work_threads SET version = version + 1, "
                        "updated_at = ? WHERE id = ? AND version = ?",
                        (
                            now,
                            author["work_thread_id"],
                            expected_thread_version,
                        ),
                    ).rowcount
                    if changed != 1:
                        raise ConcurrentUpdateError("work thread changed concurrently")
                    stored_id = assignment_id
                result = self._assignment_record(connection, stored_id)
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work assignment could not be created") from exc
        return result

    def claim_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        lease_seconds: int,
    ) -> dict[str, Any]:
        assignment_id = require_string(
            assignment_id, field="assignment_id", max_bytes=64
        )
        self._positive_integer(
            expected_assignment_version, field="expected_assignment_version"
        )
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or not 30 <= lease_seconds <= 86_400
        ):
            raise ValidationError("lease_seconds must be between 30 and 86400")
        now = self.now()
        lease_expires_at = (
            datetime.fromisoformat(now) + timedelta(seconds=lease_seconds)
        ).isoformat(timespec="microseconds")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                current = connection.execute(
                    "SELECT work_thread_id, target_profile_version_id, state, "
                    "version, claimed_by_run_id, claimed_by_attempt_id, "
                    "lease_expires_at FROM work_assignments WHERE id = ?",
                    (assignment_id,),
                ).fetchone()
                if current is None:
                    raise NotFoundError(f"work assignment not found: {assignment_id}")
                if current["work_thread_id"] != author["work_thread_id"]:
                    raise ConflictError("work assignment belongs to another thread")
                if current["target_profile_version_id"] != author["profile_version_id"]:
                    raise ConflictError(
                        "run does not use the assignment target profile version"
                    )
                if (
                    current["state"] == "claimed"
                    and current["claimed_by_run_id"] == run_id
                    and current["claimed_by_attempt_id"] == author["attempt_id"]
                    and current["lease_expires_at"] > now
                ):
                    result = self._assignment_record(connection, assignment_id)
                    connection.commit()
                    return result
                if current["version"] != expected_assignment_version:
                    raise ConcurrentUpdateError(
                        f"work assignment version is {current['version']}, "
                        f"expected {expected_assignment_version}"
                    )
                if current["state"] == "claimed" and current["lease_expires_at"] > now:
                    raise ConflictError("work assignment is claimed by another run")
                if current["state"] not in {"open", "claimed"}:
                    raise ConflictError(
                        f"work assignment cannot be claimed: {current['state']}"
                    )
                reclaimed = current["state"] == "claimed"
                changed = connection.execute(
                    "UPDATE work_assignments SET state = 'claimed', "
                    "version = version + 1, claimed_by_run_id = ?, "
                    "claimed_by_attempt_id = ?, lease_expires_at = ?, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (
                        run_id,
                        author["attempt_id"],
                        lease_expires_at,
                        now,
                        assignment_id,
                        expected_assignment_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("work assignment changed concurrently")
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM work_assignment_events WHERE assignment_id = ?",
                    (assignment_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO work_assignment_events "
                    "(assignment_id, sequence, kind, from_state, to_state, "
                    "actor_run_id, actor_attempt_id, metadata_json, created_at) "
                    "VALUES (?, ?, ?, ?, 'claimed', ?, ?, ?, ?)",
                    (
                        assignment_id,
                        event_sequence,
                        "assignment.reclaimed" if reclaimed else "assignment.claimed",
                        current["state"],
                        run_id,
                        author["attempt_id"],
                        canonical_json({"lease_expires_at": lease_expires_at}),
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE work_threads SET version = version + 1, "
                    "updated_at = ? WHERE id = ?",
                    (now, author["work_thread_id"]),
                )
                result = self._assignment_record(connection, assignment_id)
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work assignment could not be claimed") from exc
        return result

    def complete_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        result: WorkLogEntry,
        idempotency_key: str,
    ) -> dict[str, Any]:
        assignment_id = require_string(
            assignment_id, field="assignment_id", max_bytes=64
        )
        self._positive_integer(
            expected_assignment_version, field="expected_assignment_version"
        )
        if not isinstance(result, WorkLogEntry):
            raise ValidationError("result must be a validated WorkLogEntry")
        if result.kind is not WorkLogKind.ASSIGNMENT_RESULT:
            raise ValidationError("result kind must be assignment_result")
        idempotency_key = self._work_idempotency_key(idempotency_key)
        entry_id = self.new_id("wle")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                current = connection.execute(
                    "SELECT work_thread_id, state, version, claimed_by_run_id, "
                    "claimed_by_attempt_id, lease_expires_at, result_entry_id "
                    "FROM work_assignments WHERE id = ?",
                    (assignment_id,),
                ).fetchone()
                if current is None:
                    raise NotFoundError(f"work assignment not found: {assignment_id}")
                if current["work_thread_id"] != author["work_thread_id"]:
                    raise ConflictError("work assignment belongs to another thread")
                existing = connection.execute(
                    "SELECT id, content_sha256, assignment_id FROM work_log_entries "
                    "WHERE work_thread_id = ? AND author_run_id = ? "
                    "AND idempotency_key = ?",
                    (author["work_thread_id"], run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["content_sha256"] != result.digest
                        or existing["assignment_id"] != assignment_id
                        or current["result_entry_id"] != existing["id"]
                    ):
                        raise ConflictError(
                            "idempotency key already names a different "
                            "assignment result"
                        )
                    stored = self._assignment_record(connection, assignment_id)
                    connection.commit()
                    return stored
                if current["version"] != expected_assignment_version:
                    raise ConcurrentUpdateError(
                        f"work assignment version is {current['version']}, "
                        f"expected {expected_assignment_version}"
                    )
                if (
                    current["state"] != "claimed"
                    or current["claimed_by_run_id"] != run_id
                    or current["claimed_by_attempt_id"] != author["attempt_id"]
                ):
                    raise ConflictError("work assignment is claimed by another run")
                if current["lease_expires_at"] <= now:
                    raise ConflictError("work assignment lease has expired")
                self._validate_related_interactions(
                    connection,
                    thread_id=author["work_thread_id"],
                    entry=result,
                )
                thread = connection.execute(
                    "SELECT log_sequence FROM work_threads WHERE id = ?",
                    (author["work_thread_id"],),
                ).fetchone()
                log_sequence = thread["log_sequence"] + 1
                connection.execute(
                    "INSERT INTO work_log_entries "
                    "(id, work_thread_id, sequence, kind, content_json, "
                    "content_sha256, author_run_id, author_attempt_id, "
                    "assignment_id, idempotency_key, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        entry_id,
                        author["work_thread_id"],
                        log_sequence,
                        result.kind.value,
                        result.canonical,
                        result.digest,
                        run_id,
                        author["attempt_id"],
                        assignment_id,
                        idempotency_key,
                        now,
                    ),
                )
                changed = connection.execute(
                    "UPDATE work_assignments SET state = 'completed', "
                    "version = version + 1, result_entry_id = ?, "
                    "updated_at = ?, completed_at = ? WHERE id = ? AND version = ?",
                    (
                        entry_id,
                        now,
                        now,
                        assignment_id,
                        expected_assignment_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("work assignment changed concurrently")
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM work_assignment_events WHERE assignment_id = ?",
                    (assignment_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO work_assignment_events "
                    "(assignment_id, sequence, kind, from_state, to_state, "
                    "actor_run_id, actor_attempt_id, metadata_json, created_at) "
                    "VALUES (?, ?, 'assignment.completed', 'claimed', "
                    "'completed', ?, ?, ?, ?)",
                    (
                        assignment_id,
                        event_sequence,
                        run_id,
                        author["attempt_id"],
                        canonical_json({"result_entry_id": entry_id}),
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE work_threads SET log_sequence = ?, "
                    "version = version + 1, updated_at = ? WHERE id = ?",
                    (log_sequence, now, author["work_thread_id"]),
                )
                stored = self._assignment_record(connection, assignment_id)
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work assignment could not be completed") from exc
        return stored
