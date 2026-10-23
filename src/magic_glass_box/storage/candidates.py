from __future__ import annotations

import json
import sqlite3
from typing import Any

from ..candidates import CandidateError, CandidateManifest
from ..domain import AttemptState, ProjectContext, ValidationError, require_string
from ..results import ResultError, ResultManifest
from .errors import ConflictError, NotFoundError, StorageError


class CandidateStorage:
    @staticmethod
    def _candidate_columns(alias: str | None = None) -> str:
        prefix = "" if alias is None else f"{alias}."
        return ", ".join(
            prefix + column
            for column in (
                "id",
                "attempt_result_id",
                "schema_version",
                "manifest_json",
                "content_sha256",
                "project_id",
                "context_version_id",
                "work_thread_id",
                "producer_run_id",
                "producer_attempt_id",
                "base_revision",
                "head_revision",
                "tree_revision",
                "object_format",
                "result_content_sha256",
                "created_by",
                "created_at",
            )
        )

    @staticmethod
    def _candidate_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            manifest = CandidateManifest.from_mapping(json.loads(row["manifest_json"]))
        except (CandidateError, TypeError, json.JSONDecodeError) as exc:
            raise StorageError("stored candidate manifest is invalid") from exc
        return {
            "id": row["id"],
            "attempt_result_id": row["attempt_result_id"],
            "schema_version": row["schema_version"],
            "manifest": manifest.to_dict(),
            "content_sha256": row["content_sha256"],
            "project_id": row["project_id"],
            "context_version_id": row["context_version_id"],
            "work_thread_id": row["work_thread_id"],
            "producer_run_id": row["producer_run_id"],
            "attempt_id": row["producer_attempt_id"],
            "base_revision": row["base_revision"],
            "head_revision": row["head_revision"],
            "tree_revision": row["tree_revision"],
            "object_format": row["object_format"],
            "result_content_sha256": row["result_content_sha256"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _source_record(row: sqlite3.Row) -> dict[str, Any]:
        if (
            row["attempt_state"] != AttemptState.SUCCEEDED.value
            or row["exit_code"] != 0
        ):
            raise ConflictError(
                "candidate requires a successful attempt with exit code zero"
            )
        if row["work_thread_id"] is None:
            raise ConflictError("candidate result is not pinned to a work item")
        try:
            result = ResultManifest.from_mapping(json.loads(row["manifest_json"]))
            context = ProjectContext.from_mapping(json.loads(row["context_json"]))
        except (
            ResultError,
            ValidationError,
            TypeError,
            json.JSONDecodeError,
        ) as exc:
            raise StorageError("candidate source data is invalid") from exc
        if (
            result.attempt_id != row["attempt_id"]
            or result.digest != row["result_content_sha256"]
            or result.changed_paths != row["changed_paths"]
            or result.changed_bytes != row["changed_bytes"]
        ):
            raise StorageError("candidate result evidence is invalid")
        return {
            "attempt_id": row["attempt_id"],
            "attempt_result_id": row["attempt_result_id"],
            "attempt_state": row["attempt_state"],
            "producer_run_id": row["producer_run_id"],
            "project_id": row["project_id"],
            "context_version_id": row["context_version_id"],
            "work_thread_id": row["work_thread_id"],
            "repository_path": context.repository_path,
            "result": result,
            "result_content_sha256": row["result_content_sha256"],
            "change_candidate_id": row["change_candidate_id"],
            "change_policy_id": row["change_policy_id"],
        }

    @staticmethod
    def _source_query() -> str:
        return (
            "SELECT ar.id AS attempt_result_id, ar.attempt_id, "
            "ar.schema_version, ar.manifest_json, "
            "ar.content_sha256 AS result_content_sha256, "
            "ar.changed_paths, ar.changed_bytes, "
            "ra.run_id AS producer_run_id, ra.state AS attempt_state, "
            "ra.exit_code, r.project_id, r.context_version_id, "
            "r.work_thread_id, pc.context_json, "
            "COALESCE(gcr.candidate_id, gmcr.candidate_id) "
            "AS change_candidate_id, "
            "COALESCE(gcr.policy_id, gmcr.policy_id) AS change_policy_id "
            "FROM attempt_results ar "
            "JOIN run_attempts ra ON ra.id = ar.attempt_id "
            "JOIN runs r ON r.id = ra.run_id "
            "JOIN project_context_versions pc ON pc.id = r.context_version_id "
            "LEFT JOIN integration_gate_change_runs gcr ON gcr.run_id = r.id "
            "LEFT JOIN integration_gate_maintainer_change_runs gmcr "
            "ON gmcr.run_id = r.id "
        )

    def get_candidate_source(self, attempt_id: str) -> dict[str, Any]:
        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        with self._read_snapshot() as connection:
            row = connection.execute(
                self._source_query() + "WHERE ar.attempt_id = ?",
                (attempt_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"attempt result not found: {attempt_id}")
        return self._source_record(row)

    def find_candidate_for_result(
        self, attempt_result_id: str
    ) -> dict[str, Any] | None:
        attempt_result_id = require_string(
            attempt_result_id, field="attempt_result_id", max_bytes=64
        )
        with self._read_snapshot() as connection:
            row = connection.execute(
                f"SELECT {self._candidate_columns()} "  # noqa: S608
                "FROM integration_candidates WHERE attempt_result_id = ?",
                (attempt_result_id,),
            ).fetchone()
        return None if row is None else self._candidate_record(row)

    def list_candidates(
        self,
        *,
        project_id: str | None = None,
        producer_run_id: str | None = None,
        work_thread_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 500
        ):
            raise ValueError("limit must be between 1 and 500")
        filters = [project_id, producer_run_id, work_thread_id]
        if sum(value is not None for value in filters) > 1:
            raise ValueError("candidate list accepts at most one scope")
        field: str | None = None
        value: str | None = None
        if project_id is not None:
            field, value = (
                "project_id",
                require_string(project_id, field="project_id", max_bytes=64),
            )
        elif producer_run_id is not None:
            field, value = (
                "producer_run_id",
                require_string(producer_run_id, field="producer_run_id", max_bytes=64),
            )
        elif work_thread_id is not None:
            field, value = (
                "work_thread_id",
                require_string(work_thread_id, field="work_thread_id", max_bytes=64),
            )
        where = "" if field is None else f"WHERE {field} = ?"
        parameters: tuple[object, ...] = (limit,) if value is None else (value, limit)
        with self._read_snapshot() as connection:
            rows = connection.execute(
                f"SELECT {self._candidate_columns()} "  # noqa: S608
                f"FROM integration_candidates {where} "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [self._candidate_record(row) for row in rows]

    def get_candidate_repository_path(self, candidate_id: str) -> str | None:
        candidate_id = require_string(candidate_id, field="candidate_id", max_bytes=64)
        with self._read_snapshot() as connection:
            row = connection.execute(
                "SELECT pc.context_json FROM integration_candidates ic "
                "JOIN project_context_versions pc "
                "ON pc.id = ic.context_version_id WHERE ic.id = ?",
                (candidate_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"candidate not found: {candidate_id}")
        try:
            context = ProjectContext.from_mapping(json.loads(row["context_json"]))
            return context.repository_path
        except (ValidationError, TypeError, json.JSONDecodeError) as exc:
            raise StorageError("candidate project context is invalid") from exc

    def create_candidate(
        self, manifest: CandidateManifest, *, created_by: str
    ) -> dict[str, Any]:
        if not isinstance(manifest, CandidateManifest):
            raise CandidateError("candidate manifest is invalid")
        created_by = require_string(created_by, field="created_by", max_bytes=64)
        candidate_id = self.new_id("cnd")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                source_row = connection.execute(
                    self._source_query() + "WHERE ar.id = ?",
                    (manifest.attempt_result_id,),
                ).fetchone()
                if source_row is None:
                    raise NotFoundError(
                        f"attempt result not found: {manifest.attempt_result_id}"
                    )
                source = self._source_record(source_row)
                expected = {
                    "attempt_id": source["attempt_id"],
                    "result_content_sha256": source["result_content_sha256"],
                    "producer_run_id": source["producer_run_id"],
                    "project_id": source["project_id"],
                    "context_version_id": source["context_version_id"],
                    "work_thread_id": source["work_thread_id"],
                    "base_revision": source["result"].source_revision,
                }
                values = manifest.to_dict()
                if any(values[key] != value for key, value in expected.items()):
                    raise ConflictError(
                        "candidate provenance does not match its result"
                    )
                creator = connection.execute(
                    "SELECT active FROM users WHERE id = ?", (created_by,)
                ).fetchone()
                if creator is None:
                    raise NotFoundError(f"user not found: {created_by}")
                if creator["active"] != 1:
                    raise ConflictError("candidate creator is inactive")
                existing = connection.execute(
                    f"SELECT {self._candidate_columns()} "  # noqa: S608
                    "FROM integration_candidates WHERE attempt_result_id = ?",
                    (manifest.attempt_result_id,),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != manifest.digest:
                        raise ConflictError(
                            "attempt result already has a different candidate"
                        )
                    connection.commit()
                    return self._candidate_record(existing)
                connection.execute(
                    "INSERT INTO integration_candidates "
                    "(id, attempt_result_id, schema_version, manifest_json, "
                    "content_sha256, project_id, context_version_id, "
                    "work_thread_id, producer_run_id, producer_attempt_id, "
                    "base_revision, head_revision, tree_revision, object_format, "
                    "result_content_sha256, created_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        candidate_id,
                        manifest.attempt_result_id,
                        manifest.schema_version,
                        manifest.canonical,
                        manifest.digest,
                        manifest.project_id,
                        manifest.context_version_id,
                        manifest.work_thread_id,
                        manifest.producer_run_id,
                        manifest.attempt_id,
                        manifest.base_revision,
                        manifest.head_revision,
                        manifest.tree_revision,
                        manifest.object_format,
                        manifest.result_content_sha256,
                        created_by,
                        now,
                    ),
                )
                row = connection.execute(
                    f"SELECT {self._candidate_columns()} "  # noqa: S608
                    "FROM integration_candidates WHERE id = ?",
                    (candidate_id,),
                ).fetchone()
                connection.commit()
            except (CandidateError, ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("candidate could not be recorded") from exc
        if row is None:
            raise StorageError("candidate was not recorded")
        return self._candidate_record(row)

    def get_candidate(self, candidate_id: str) -> dict[str, Any]:
        candidate_id = require_string(candidate_id, field="candidate_id", max_bytes=64)
        with self._read_snapshot() as connection:
            row = connection.execute(
                f"SELECT {self._candidate_columns()} "  # noqa: S608
                "FROM integration_candidates WHERE id = ?",
                (candidate_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"candidate not found: {candidate_id}")
        return self._candidate_record(row)
