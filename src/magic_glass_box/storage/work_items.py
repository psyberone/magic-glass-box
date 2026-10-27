from __future__ import annotations

import json
import sqlite3
from typing import Any

from ..domain import (
    ValidationError,
    WorkItemEnvelope,
    WorkLogKind,
    canonical_json,
    content_sha256,
    require_string,
    validate_project_key,
)
from ..service_sidecar import SERVICE_OPERATION_ID_RE, ServiceOperationReceipt
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)


class WorkItemStorage:
    @staticmethod
    def _work_thread_record(
        connection: sqlite3.Connection, thread_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT id, work_item_id, generation, state, version, log_sequence, "
            "created_at, updated_at, completed_at FROM work_threads WHERE id = ?",
            (thread_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work thread not found: {thread_id}")
        events = connection.execute(
            "SELECT e.sequence, e.kind, e.from_state, e.to_state, "
            "u.handle AS actor, e.metadata_json, e.created_at "
            "FROM work_thread_events e JOIN users u ON u.id = e.actor_id "
            "WHERE e.work_thread_id = ? ORDER BY e.sequence",
            (thread_id,),
        ).fetchall()
        try:
            parsed_events = [
                {**dict(event), "metadata": json.loads(event["metadata_json"])}
                for event in events
            ]
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored work-thread event is invalid") from exc
        for event in parsed_events:
            event.pop("metadata_json")
        result = dict(row)
        result["thread_id"] = result.pop("id")
        result["events"] = parsed_events
        return result

    @staticmethod
    def _work_item_record(
        connection: sqlite3.Connection, work_item_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT wi.id, p.krab_project_key AS project_key, wi.source, "
            "wi.external_id, wi.source_updated_at, "
            "wiv.id AS work_item_version_id, wiv.version, wiv.content_json, "
            "wiv.content_sha256, "
            "wiv.source_updated_at AS version_source_updated_at, "
            "u.handle AS imported_by, wi.created_at, wi.updated_at "
            "FROM work_items wi JOIN projects p ON p.id = wi.project_id "
            "JOIN work_item_versions wiv ON wiv.work_item_id = wi.id "
            "JOIN users u ON u.id = wi.last_imported_by "
            "WHERE wi.id = ? ORDER BY wiv.version DESC LIMIT 1",
            (work_item_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work item not found: {work_item_id}")
        result = dict(row)
        try:
            content = json.loads(result.pop("content_json"))
            envelope = WorkItemEnvelope.from_mapping(
                {
                    **content,
                    "source": result["source"],
                    "external_id": result["external_id"],
                    "source_updated_at": result.pop("version_source_updated_at"),
                }
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored work item is invalid") from exc
        if envelope.digest != result["content_sha256"]:
            raise StorageError("stored work item content digest does not match")
        result["content"] = envelope.content
        thread = connection.execute(
            "SELECT id FROM work_threads WHERE work_item_id = ? "
            "ORDER BY generation DESC LIMIT 1",
            (work_item_id,),
        ).fetchone()
        if thread is None:
            raise StorageError("work item has no durable work thread")
        result["work_thread"] = WorkItemStorage._work_thread_record(
            connection, thread["id"]
        )
        result["state"] = result["work_thread"]["state"]
        import_table = connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' "
            "AND name = 'work_item_service_imports'"
        ).fetchone()
        imports = (
            []
            if import_table is None
            else connection.execute(
                "SELECT si.id, si.work_item_version_id, "
                "si.service_operation_attempt_id, "
                "a.operation_id AS service_operation_id, a.receipt_sha256, "
                "o.content_sha256 AS operation_sha256, "
                "ob.project_capability_binding_id AS capability_binding_id, "
                "u.handle AS imported_by, si.created_at "
                "FROM work_item_service_imports si "
                "JOIN work_item_versions version "
                "ON version.id = si.work_item_version_id "
                "JOIN service_operation_attempts a "
                "ON a.id = si.service_operation_attempt_id "
                "JOIN service_operations o ON o.id = a.operation_id "
                "JOIN service_operation_capability_bindings ob "
                "ON ob.operation_id = o.id "
                "JOIN users u ON u.id = si.imported_by "
                "WHERE version.work_item_id = ? ORDER BY si.created_at, si.id",
                (work_item_id,),
            ).fetchall()
        )
        result["service_imports"] = [dict(item) for item in imports]
        return result

    def get_work_item(self, work_item_id: str) -> dict[str, Any]:
        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
        with self._connection() as connection:
            return self._work_item_record(connection, work_item_id)

    def complete_work_item(
        self,
        work_item_id: str,
        *,
        completed_by: str,
        expected_thread_version: int,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """Seal a quiescent work thread while preserving its audit history."""

        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
        completed_by = require_string(completed_by, field="completed_by", max_bytes=64)
        self._positive_integer(expected_thread_version, field="expected_thread_version")
        if reason is not None:
            reason = require_string(reason, field="reason", max_bytes=4096)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                actor = connection.execute(
                    "SELECT id FROM users WHERE id = ? AND active = 1",
                    (completed_by,),
                ).fetchone()
                if actor is None:
                    raise NotFoundError(f"active local user not found: {completed_by}")
                exists = connection.execute(
                    "SELECT 1 FROM work_items WHERE id = ?", (work_item_id,)
                ).fetchone()
                if exists is None:
                    raise NotFoundError(f"work item not found: {work_item_id}")
                thread = connection.execute(
                    "SELECT id, state, version FROM work_threads "
                    "WHERE work_item_id = ? ORDER BY generation DESC LIMIT 1",
                    (work_item_id,),
                ).fetchone()
                if thread is None:
                    raise StorageError("work item has no durable work thread")
                if thread["version"] != expected_thread_version:
                    raise ConcurrentUpdateError(
                        f"work thread version is {thread['version']}, "
                        f"expected {expected_thread_version}"
                    )
                if thread["state"] != "active":
                    raise ConflictError("work item is already completed")
                active_run = connection.execute(
                    "SELECT id FROM runs WHERE work_thread_id = ? AND state IN "
                    "('queued', 'preparing', 'running', 'waiting_for_input', "
                    "'paused') "
                    "LIMIT 1",
                    (thread["id"],),
                ).fetchone()
                if active_run is not None:
                    raise ConflictError("work item has active runs")
                active_workflow = connection.execute(
                    "SELECT id FROM workflow_executions "
                    "WHERE work_thread_id = ? AND state = 'active' LIMIT 1",
                    (thread["id"],),
                ).fetchone()
                if active_workflow is not None:
                    raise ConflictError("work item has an active workflow")
                active_discovery = connection.execute(
                    "SELECT id FROM discovery_executions "
                    "WHERE work_thread_id = ? AND state = 'active' LIMIT 1",
                    (thread["id"],),
                ).fetchone()
                if active_discovery is not None:
                    raise ConflictError(
                        "work item has an active Discovery prescription"
                    )
                active_advisory = connection.execute(
                    "SELECT id FROM advisory_executions "
                    "WHERE work_thread_id = ? AND state = 'active' LIMIT 1",
                    (thread["id"],),
                ).fetchone()
                if active_advisory is not None:
                    raise ConflictError("work item has an active advisory prescription")
                unfinished = connection.execute(
                    "SELECT id FROM work_assignments WHERE work_thread_id = ? "
                    "AND state IN ('open', 'claimed') LIMIT 1",
                    (thread["id"],),
                ).fetchone()
                if unfinished is not None:
                    raise ConflictError("work item has unfinished assignments")
                outcome = connection.execute(
                    "SELECT id, kind FROM work_log_entries "
                    "WHERE work_thread_id = ? ORDER BY sequence DESC LIMIT 1",
                    (thread["id"],),
                ).fetchone()
                if outcome is None or outcome["kind"] != WorkLogKind.OUTCOME.value:
                    raise ConflictError("work item requires a final outcome entry")
                metadata: dict[str, Any] = {"outcome_entry_id": outcome["id"]}
                if reason is not None:
                    metadata["reason"] = reason
                changed = connection.execute(
                    "UPDATE work_threads SET state = 'completed', "
                    "version = version + 1, updated_at = ?, completed_at = ? "
                    "WHERE id = ? AND version = ? AND state = 'active'",
                    (
                        now,
                        now,
                        thread["id"],
                        expected_thread_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("work thread changed concurrently")
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM work_thread_events WHERE work_thread_id = ?",
                    (thread["id"],),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO work_thread_events "
                    "(work_thread_id, sequence, kind, from_state, to_state, "
                    "actor_id, metadata_json, created_at) VALUES (?, ?, "
                    "'work_thread.completed', 'active', 'completed', ?, ?, ?)",
                    (
                        thread["id"],
                        event_sequence,
                        completed_by,
                        canonical_json(metadata),
                        now,
                    ),
                )
                result = self._work_thread_record(connection, thread["id"])
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work item could not be completed") from exc
        return result

    def reopen_work_item(
        self,
        work_item_id: str,
        *,
        reopened_by: str,
        expected_thread_version: int,
    ) -> dict[str, Any]:
        """Begin a blank work-memory generation after a completed thread."""

        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
        reopened_by = require_string(reopened_by, field="reopened_by", max_bytes=64)
        self._positive_integer(expected_thread_version, field="expected_thread_version")
        thread_id = self.new_id("wth")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                actor = connection.execute(
                    "SELECT id FROM users WHERE id = ? AND active = 1",
                    (reopened_by,),
                ).fetchone()
                if actor is None:
                    raise NotFoundError(f"active local user not found: {reopened_by}")
                exists = connection.execute(
                    "SELECT 1 FROM work_items WHERE id = ?", (work_item_id,)
                ).fetchone()
                if exists is None:
                    raise NotFoundError(f"work item not found: {work_item_id}")
                previous = connection.execute(
                    "SELECT id, generation, state, version FROM work_threads "
                    "WHERE work_item_id = ? ORDER BY generation DESC LIMIT 1",
                    (work_item_id,),
                ).fetchone()
                if previous is None:
                    raise StorageError("work item has no durable work thread")
                if previous["version"] != expected_thread_version:
                    raise ConcurrentUpdateError(
                        f"work thread version is {previous['version']}, "
                        f"expected {expected_thread_version}"
                    )
                if previous["state"] == "active":
                    raise ConflictError("work item is already active")
                connection.execute(
                    "INSERT INTO work_threads "
                    "(id, work_item_id, generation, state, version, log_sequence, "
                    "created_at, updated_at) VALUES (?, ?, ?, 'active', 1, 0, ?, ?)",
                    (
                        thread_id,
                        work_item_id,
                        previous["generation"] + 1,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO work_thread_events "
                    "(work_thread_id, sequence, kind, from_state, to_state, "
                    "actor_id, metadata_json, created_at) VALUES (?, 1, "
                    "'work_thread.created', NULL, 'active', ?, ?, ?)",
                    (
                        thread_id,
                        reopened_by,
                        canonical_json({"previous_thread_id": previous["id"]}),
                        now,
                    ),
                )
                result = self._work_thread_record(connection, thread_id)
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work item could not be reopened") from exc
        return result

    def import_work_item(
        self,
        *,
        project_key: str,
        envelope: WorkItemEnvelope,
        imported_by: str,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        if not isinstance(envelope, WorkItemEnvelope):
            raise ValidationError("envelope must be a WorkItemEnvelope")
        imported_by = require_string(imported_by, field="imported_by", max_bytes=64)
        work_item_id = self.new_id("wki")
        version_id = self.new_id("wiv")
        thread_id = self.new_id("wth")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, imported_by, role="work-item importer")
                project = connection.execute(
                    "SELECT id FROM projects WHERE krab_project_key = ? AND active = 1",
                    (project_key,),
                ).fetchone()
                if project is None:
                    raise NotFoundError(f"active project not found: {project_key}")
                work_item_id, import_status = self._import_work_item_in_transaction(
                    connection,
                    project_id=project["id"],
                    envelope=envelope,
                    imported_by=imported_by,
                    work_item_id=work_item_id,
                    version_id=version_id,
                    thread_id=thread_id,
                    now=now,
                )
                result = self._work_item_record(connection, work_item_id)
                connection.commit()
            except (ConflictError, NotFoundError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work item could not be imported") from exc
        result["import_status"] = import_status
        return result

    @staticmethod
    def _import_work_item_in_transaction(
        connection: sqlite3.Connection,
        *,
        project_id: str,
        envelope: WorkItemEnvelope,
        imported_by: str,
        work_item_id: str,
        version_id: str,
        thread_id: str,
        now: str,
    ) -> tuple[str, str]:
        existing = connection.execute(
            "SELECT id, source_updated_at FROM work_items "
            "WHERE project_id = ? AND source = ? AND external_id = ?",
            (project_id, envelope.source, envelope.external_id),
        ).fetchone()
        if existing is None:
            connection.execute(
                "INSERT INTO work_items "
                "(id, project_id, source, external_id, source_updated_at, "
                "last_imported_by, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    work_item_id,
                    project_id,
                    envelope.source,
                    envelope.external_id,
                    envelope.source_updated_at,
                    imported_by,
                    now,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO work_item_versions "
                "(id, work_item_id, version, content_json, content_sha256, "
                "source_updated_at, imported_by, created_at) "
                "VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
                (
                    version_id,
                    work_item_id,
                    envelope.canonical,
                    envelope.digest,
                    envelope.source_updated_at,
                    imported_by,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO work_threads "
                "(id, work_item_id, generation, state, version, "
                "log_sequence, created_at, updated_at) "
                "VALUES (?, ?, 1, 'active', 1, 0, ?, ?)",
                (thread_id, work_item_id, now, now),
            )
            connection.execute(
                "INSERT INTO work_thread_events "
                "(work_thread_id, sequence, kind, from_state, to_state, "
                "actor_id, metadata_json, created_at) VALUES (?, 1, "
                "'work_thread.created', NULL, 'active', ?, '{}', ?)",
                (thread_id, imported_by, now),
            )
            return work_item_id, "created"

        work_item_id = existing["id"]
        current = connection.execute(
            "SELECT version, content_sha256 FROM work_item_versions "
            "WHERE work_item_id = ? ORDER BY version DESC LIMIT 1",
            (work_item_id,),
        ).fetchone()
        if current is None:
            raise StorageError("work item has no content version")
        if envelope.source_updated_at < existing["source_updated_at"]:
            raise ConflictError("work item import is stale")
        if (
            envelope.source_updated_at == existing["source_updated_at"]
            and envelope.digest != current["content_sha256"]
        ):
            raise ConflictError("work item import changed at the same source revision")
        if envelope.digest == current["content_sha256"]:
            import_status = "unchanged"
        else:
            connection.execute(
                "INSERT INTO work_item_versions "
                "(id, work_item_id, version, content_json, content_sha256, "
                "source_updated_at, imported_by, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    version_id,
                    work_item_id,
                    current["version"] + 1,
                    envelope.canonical,
                    envelope.digest,
                    envelope.source_updated_at,
                    imported_by,
                    now,
                ),
            )
            import_status = "updated"
        connection.execute(
            "UPDATE work_items SET source_updated_at = ?, "
            "last_imported_by = ?, updated_at = ? WHERE id = ?",
            (envelope.source_updated_at, imported_by, now, work_item_id),
        )
        return work_item_id, import_status

    def import_work_item_from_service_operation(
        self,
        *,
        project_key: str,
        operation_id: str,
        imported_by: str,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        if (
            not isinstance(operation_id, str)
            or SERVICE_OPERATION_ID_RE.fullmatch(operation_id) is None
        ):
            raise ValidationError("service operation id is invalid")
        imported_by = require_string(imported_by, field="imported_by", max_bytes=64)
        import_id = self.new_id("wsi")
        work_item_id = self.new_id("wki")
        version_id = self.new_id("wiv")
        thread_id = self.new_id("wth")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, imported_by, role="work-item importer")
                project = connection.execute(
                    "SELECT id FROM projects WHERE krab_project_key = ? AND active = 1",
                    (project_key,),
                ).fetchone()
                if project is None:
                    raise NotFoundError(f"active project not found: {project_key}")
                operation = connection.execute(
                    "SELECT o.state, o.capability_name, o.capability_version, "
                    "o.capability_mode, o.content_sha256 AS operation_sha256, "
                    "ob.project_capability_binding_id, b.project_id, "
                    "a.id AS attempt_id, a.receipt_json, a.receipt_sha256 "
                    "FROM service_operations o "
                    "LEFT JOIN service_operation_capability_bindings ob "
                    "ON ob.operation_id = o.id "
                    "LEFT JOIN project_capability_bindings b "
                    "ON b.id = ob.project_capability_binding_id "
                    "LEFT JOIN service_operation_attempts a "
                    "ON a.operation_id = o.id AND a.outcome = 'succeeded' "
                    "WHERE o.id = ? ORDER BY a.sequence DESC LIMIT 1",
                    (operation_id,),
                ).fetchone()
                if operation is None:
                    raise NotFoundError(f"service operation not found: {operation_id}")
                if operation["state"] != "succeeded":
                    raise ConflictError("service operation must be succeeded")
                if operation["project_capability_binding_id"] is None:
                    raise ConflictError("service operation must be project-bound")
                if operation["project_id"] != project["id"]:
                    raise ConflictError("service operation belongs to another project")
                if (
                    operation["capability_name"] != "work.get"
                    or operation["capability_version"] != 1
                    or operation["capability_mode"] != "read"
                ):
                    raise ConflictError("service operation is not a work.get v1 read")
                if (
                    operation["attempt_id"] is None
                    or operation["receipt_json"] is None
                    or operation["receipt_sha256"] is None
                ):
                    raise StorageError("service operation receipt is unavailable")
                try:
                    receipt = ServiceOperationReceipt.from_mapping(
                        json.loads(operation["receipt_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored service operation receipt is invalid"
                    ) from exc
                if (
                    content_sha256(canonical_json(receipt.to_dict()))
                    != operation["receipt_sha256"]
                ):
                    raise StorageError(
                        "stored service operation receipt digest does not match"
                    )
                raw_result = receipt.result
                if not isinstance(raw_result, dict):
                    raise ValidationError(
                        "service operation work.get result must be an object"
                    )
                envelope = WorkItemEnvelope.from_mapping(raw_result)
                if envelope.to_dict() != raw_result:
                    raise ValidationError(
                        "service operation work.get result must be canonical"
                    )

                existing_import = connection.execute(
                    "SELECT version.work_item_id "
                    "FROM work_item_service_imports imported "
                    "JOIN work_item_versions version "
                    "ON version.id = imported.work_item_version_id "
                    "WHERE imported.service_operation_attempt_id = ?",
                    (operation["attempt_id"],),
                ).fetchone()
                if existing_import is not None:
                    result = self._work_item_record(
                        connection, existing_import["work_item_id"]
                    )
                    import_status = "unchanged"
                    connection.commit()
                else:
                    work_item_id, import_status = self._import_work_item_in_transaction(
                        connection,
                        project_id=project["id"],
                        envelope=envelope,
                        imported_by=imported_by,
                        work_item_id=work_item_id,
                        version_id=version_id,
                        thread_id=thread_id,
                        now=now,
                    )
                    current_version = connection.execute(
                        "SELECT id FROM work_item_versions WHERE work_item_id = ? "
                        "ORDER BY version DESC LIMIT 1",
                        (work_item_id,),
                    ).fetchone()
                    if current_version is None:
                        raise StorageError("work item has no content version")
                    connection.execute(
                        "INSERT INTO work_item_service_imports "
                        "(id, work_item_version_id, service_operation_attempt_id, "
                        "imported_by, created_at) VALUES (?, ?, ?, ?, ?)",
                        (
                            import_id,
                            current_version["id"],
                            operation["attempt_id"],
                            imported_by,
                            now,
                        ),
                    )
                    result = self._work_item_record(connection, work_item_id)
                    connection.commit()
            except (ConflictError, NotFoundError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("service work item could not be imported") from exc
        result["import_status"] = import_status
        return result
