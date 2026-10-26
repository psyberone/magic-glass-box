from __future__ import annotations

import json
import sqlite3
from typing import Any

from ..advisory import (
    AdvisoryBlindReview,
    AdvisoryChallengeReview,
    AdvisoryConfigurationSpec,
    AdvisoryReport,
)
from ..domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    require_string,
    validate_name,
    validate_project_key,
)
from ..krab import IDEMPOTENCY_KEY_RE
from ..workflows import ADVISORY_REVIEW_TEMPLATE_ID
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)

ADVISORY_REVIEWER_ROLES = ("reviewer_one", "reviewer_two", "reviewer_three")
ADVISORY_ACTIVE_STAGES = (
    "blind_review",
    "challenge",
    "triangulate",
    "completion",
)


class AdvisoryStorage:
    """Persist one bounded advisory review without creating gate authority."""

    @staticmethod
    def _advisory_idempotency_key(value: object) -> str:
        if not isinstance(value, str) or IDEMPOTENCY_KEY_RE.fullmatch(value) is None:
            raise ValidationError("idempotency_key is invalid")
        return value

    @staticmethod
    def _advisory_positive_integer(value: object, *, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError(f"{field} must be a positive integer")
        return value

    @staticmethod
    def _advisory_start_request(
        *,
        project_key: str,
        configuration_name: str,
        candidate_id: str,
        expected_configuration_version: int,
        expected_context_version_id: str,
        expected_work_item_version_id: str,
        expected_thread_generation: int,
        expected_thread_version: int,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "project_key": project_key,
            "configuration_name": configuration_name,
            "candidate_id": candidate_id,
            "expected_configuration_version": expected_configuration_version,
            "expected_context_version_id": expected_context_version_id,
            "expected_work_item_version_id": expected_work_item_version_id,
            "expected_thread_generation": expected_thread_generation,
            "expected_thread_version": expected_thread_version,
        }

    def start_advisory_execution(
        self,
        *,
        project_key: str,
        configuration_name: str,
        candidate_id: str,
        expected_configuration_version: int,
        expected_context_version_id: str,
        expected_work_item_version_id: str,
        expected_thread_generation: int,
        expected_thread_version: int,
        idempotency_key: str,
        requested_by: str,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        configuration_name = validate_name(
            configuration_name, field="advisory configuration name"
        )
        candidate_id = require_string(candidate_id, field="candidate_id", max_bytes=64)
        self._advisory_positive_integer(
            expected_configuration_version, field="expected_configuration_version"
        )
        expected_context_version_id = require_string(
            expected_context_version_id,
            field="expected_context_version_id",
            max_bytes=64,
        )
        expected_work_item_version_id = require_string(
            expected_work_item_version_id,
            field="expected_work_item_version_id",
            max_bytes=64,
        )
        self._advisory_positive_integer(
            expected_thread_generation, field="expected_thread_generation"
        )
        self._advisory_positive_integer(
            expected_thread_version, field="expected_thread_version"
        )
        idempotency_key = self._advisory_idempotency_key(idempotency_key)
        requested_by = require_string(requested_by, field="requested_by", max_bytes=64)
        request = self._advisory_start_request(
            project_key=project_key,
            configuration_name=configuration_name,
            candidate_id=candidate_id,
            expected_configuration_version=expected_configuration_version,
            expected_context_version_id=expected_context_version_id,
            expected_work_item_version_id=expected_work_item_version_id,
            expected_thread_generation=expected_thread_generation,
            expected_thread_version=expected_thread_version,
        )
        request_json = canonical_json(request)
        request_sha256 = content_sha256(request_json)
        execution_id = self.new_id("arx")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, requested_by, role="advisory requester")
                existing = connection.execute(
                    "SELECT id, content_sha256 FROM advisory_executions "
                    "WHERE requested_by = ? AND idempotency_key = ?",
                    (requested_by, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != request_sha256:
                        raise ConflictError(
                            "idempotency key already names another advisory execution"
                        )
                    stored_id = existing["id"]
                    connection.commit()
                else:
                    project = connection.execute(
                        "SELECT project.id, project.active, "
                        "context.id AS context_version_id "
                        "FROM projects project JOIN project_context_versions context "
                        "ON context.project_id = project.id "
                        "WHERE project.krab_project_key = ? "
                        "ORDER BY context.version DESC LIMIT 1",
                        (project_key,),
                    ).fetchone()
                    if project is None or project["active"] != 1:
                        raise NotFoundError(f"active project not found: {project_key}")
                    if project["context_version_id"] != expected_context_version_id:
                        raise ConcurrentUpdateError("project context version changed")
                    configuration = connection.execute(
                        "SELECT configuration.id, version.id AS version_id, "
                        "version.version, version.template_definition_id, "
                        "(SELECT COUNT(*) FROM "
                        "workflow_configuration_role_bindings binding "
                        "WHERE binding.configuration_version_id = version.id) "
                        "AS role_count, "
                        "(SELECT COUNT(*) FROM "
                        "workflow_configuration_role_bindings binding "
                        "JOIN agent_profile_versions profile_version "
                        "ON profile_version.id = binding.profile_version_id "
                        "JOIN agent_profiles profile "
                        "ON profile.id = profile_version.profile_id "
                        "WHERE binding.configuration_version_id = version.id "
                        "AND profile.active = 1) AS active_role_count "
                        "FROM workflow_configurations configuration "
                        "JOIN workflow_configuration_versions version "
                        "ON version.configuration_id = configuration.id "
                        "WHERE configuration.project_id = ? "
                        "AND configuration.name = ? "
                        "ORDER BY version.version DESC LIMIT 1",
                        (project["id"], configuration_name),
                    ).fetchone()
                    if configuration is None:
                        raise NotFoundError(
                            "advisory configuration not found: "
                            f"{project_key}/{configuration_name}"
                        )
                    if configuration["version"] != expected_configuration_version:
                        raise ConflictError("advisory configuration version changed")
                    if (
                        configuration["template_definition_id"]
                        != ADVISORY_REVIEW_TEMPLATE_ID
                    ):
                        raise ConflictError(
                            "workflow prescription is not executable by the advisory "
                            "director"
                        )
                    if configuration["role_count"] != 4:
                        raise StorageError(
                            "stored advisory configuration roles do not match"
                        )
                    if configuration["active_role_count"] != 4:
                        raise ConflictError("advisory role profile is inactive")
                    candidate = connection.execute(
                        "SELECT candidate.id, candidate.project_id, "
                        "candidate.context_version_id, candidate.work_thread_id, "
                        "candidate.base_revision, candidate.head_revision, "
                        "candidate.tree_revision, candidate.object_format, "
                        "candidate.content_sha256, thread.work_item_id, "
                        "thread.generation, thread.state, "
                        "thread.version AS thread_version, "
                        "(SELECT version.id FROM work_item_versions version "
                        "WHERE version.work_item_id = thread.work_item_id "
                        "ORDER BY version.version DESC LIMIT 1) "
                        "AS work_item_version_id "
                        "FROM integration_candidates candidate "
                        "JOIN work_threads thread "
                        "ON thread.id = candidate.work_thread_id "
                        "WHERE candidate.id = ?",
                        (candidate_id,),
                    ).fetchone()
                    if candidate is None:
                        raise NotFoundError(f"candidate not found: {candidate_id}")
                    if candidate["project_id"] != project["id"]:
                        raise ConflictError("candidate belongs to another project")
                    if candidate["context_version_id"] != expected_context_version_id:
                        raise ConflictError(
                            "candidate belongs to another project context"
                        )
                    if (
                        candidate["work_item_version_id"]
                        != expected_work_item_version_id
                    ):
                        raise ConcurrentUpdateError("work item version changed")
                    if candidate["state"] != "active":
                        raise ConflictError(
                            f"work thread is not active: {candidate['state']}"
                        )
                    if candidate["generation"] != expected_thread_generation:
                        raise ConcurrentUpdateError("work thread generation changed")
                    if candidate["thread_version"] != expected_thread_version:
                        raise ConcurrentUpdateError("work thread version changed")
                    connection.execute(
                        "INSERT INTO advisory_executions "
                        "(id, project_id, template_definition_id, "
                        "configuration_version_id, context_version_id, work_item_id, "
                        "work_item_version_id, work_thread_id, "
                        "work_thread_generation, candidate_id, state, stage, version, "
                        "request_json, content_sha256, idempotency_key, requested_by, "
                        "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                        "'active', 'blind_review', 1, ?, ?, ?, ?, ?, ?)",
                        (
                            execution_id,
                            project["id"],
                            ADVISORY_REVIEW_TEMPLATE_ID,
                            configuration["version_id"],
                            project["context_version_id"],
                            candidate["work_item_id"],
                            candidate["work_item_version_id"],
                            candidate["work_thread_id"],
                            candidate["generation"],
                            candidate_id,
                            request_json,
                            request_sha256,
                            idempotency_key,
                            requested_by,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO advisory_execution_events "
                        "(execution_id, sequence, kind, from_state, to_state, actor, "
                        "metadata_json, created_at) VALUES (?, 1, "
                        "'advisory.started', NULL, 'active', ?, '{}', ?)",
                        (execution_id, requested_by, now),
                    )
                    stored_id = execution_id
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
                raise ConflictError("advisory execution could not be started") from exc
        return self.get_advisory_execution(stored_id)

    @staticmethod
    def _advisory_blind_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            review = AdvisoryBlindReview.from_mapping(json.loads(row["review_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored advisory blind review is invalid") from exc
        if review.digest != row["content_sha256"]:
            raise StorageError("stored advisory blind review digest does not match")
        return {
            "id": row["id"],
            "execution_id": row["execution_id"],
            "reviewer_role": row["reviewer_role"],
            "run_id": row["run_id"],
            "attempt_id": row["attempt_id"],
            "review": review.to_dict(),
            "content_sha256": row["content_sha256"],
            "idempotency_key": row["idempotency_key"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _advisory_challenge_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            review = AdvisoryChallengeReview.from_mapping(
                json.loads(row["review_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored advisory challenge review is invalid") from exc
        if review.digest != row["content_sha256"]:
            raise StorageError("stored advisory challenge review digest does not match")
        return {
            "id": row["id"],
            "execution_id": row["execution_id"],
            "reviewer_role": row["reviewer_role"],
            "run_id": row["run_id"],
            "attempt_id": row["attempt_id"],
            "review": review.to_dict(),
            "content_sha256": row["content_sha256"],
            "idempotency_key": row["idempotency_key"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _advisory_report_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            report = AdvisoryReport.from_mapping(json.loads(row["report_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored advisory report is invalid") from exc
        if (
            report.digest != row["content_sha256"]
            or report.authority.value != row["authority"]
        ):
            raise StorageError("stored advisory report does not match")
        return {
            "id": row["id"],
            "execution_id": row["execution_id"],
            "run_id": row["run_id"],
            "attempt_id": row["attempt_id"],
            "authority": row["authority"],
            "report": report.to_dict(),
            "content_sha256": row["content_sha256"],
            "idempotency_key": row["idempotency_key"],
            "created_at": row["created_at"],
        }

    def _advisory_execution_record(
        self, connection: sqlite3.Connection, execution_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT execution.id, execution.project_id, "
            "project.krab_project_key AS project_key, "
            "execution.template_definition_id, "
            "execution.configuration_version_id, "
            "configuration.configuration_id, "
            "configuration.version AS configuration_version, "
            "named.name AS configuration_name, execution.context_version_id, "
            "context.version AS context_version, execution.work_item_id, "
            "execution.work_item_version_id, "
            "item_version.version AS work_item_version, "
            "execution.work_thread_id, execution.work_thread_generation, "
            "execution.candidate_id, candidate.base_revision, "
            "candidate.head_revision, candidate.tree_revision, "
            "candidate.object_format, "
            "candidate.content_sha256 AS candidate_content_sha256, "
            "execution.state, execution.stage, execution.version, "
            "execution.content_sha256, execution.cancel_reason, "
            "execution.failure_reason, requester.handle AS requested_by, "
            "execution.created_at, execution.updated_at, execution.finished_at "
            "FROM advisory_executions execution "
            "JOIN projects project ON project.id = execution.project_id "
            "JOIN workflow_configuration_versions configuration "
            "ON configuration.id = execution.configuration_version_id "
            "JOIN workflow_configurations named "
            "ON named.id = configuration.configuration_id "
            "JOIN project_context_versions context "
            "ON context.id = execution.context_version_id "
            "JOIN work_item_versions item_version "
            "ON item_version.id = execution.work_item_version_id "
            "JOIN integration_candidates candidate "
            "ON candidate.id = execution.candidate_id "
            "JOIN users requester ON requester.id = execution.requested_by "
            "WHERE execution.id = ?",
            (execution_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"advisory execution not found: {execution_id}")
        configuration = self._configuration_record(
            connection,
            self._configuration_row(
                connection, version_id=row["configuration_version_id"]
            ),
        )
        if configuration["template"]["id"] != ADVISORY_REVIEW_TEMPLATE_ID:
            raise StorageError("stored advisory execution prescription is invalid")
        event_rows = connection.execute(
            "SELECT event.sequence, event.kind, event.from_state, event.to_state, "
            "actor.handle AS actor, event.metadata_json, event.created_at "
            "FROM advisory_execution_events event JOIN users actor "
            "ON actor.id = event.actor WHERE event.execution_id = ? "
            "ORDER BY event.sequence",
            (execution_id,),
        ).fetchall()
        try:
            events = [
                {**dict(event), "metadata": json.loads(event["metadata_json"])}
                for event in event_rows
            ]
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored advisory event is invalid") from exc
        for event in events:
            event.pop("metadata_json")
        run_rows = connection.execute(
            "SELECT binding.stage, binding.role, binding.run_id, run.state, "
            "run.version, run.created_at, run.finished_at "
            "FROM advisory_run_bindings binding JOIN runs run "
            "ON run.id = binding.run_id WHERE binding.execution_id = ? "
            "ORDER BY binding.stage, binding.role",
            (execution_id,),
        ).fetchall()
        runs: dict[str, dict[str, Any]] = {}
        for run in run_rows:
            stage = run["stage"]
            record = dict(run)
            record.pop("stage")
            record.pop("role")
            runs.setdefault(stage, {})[run["role"]] = record
        blind_rows = connection.execute(
            "SELECT * FROM advisory_blind_reviews WHERE execution_id = ? "
            "ORDER BY reviewer_role",
            (execution_id,),
        ).fetchall()
        challenge_rows = connection.execute(
            "SELECT * FROM advisory_challenge_reviews WHERE execution_id = ? "
            "ORDER BY reviewer_role",
            (execution_id,),
        ).fetchall()
        report_row = connection.execute(
            "SELECT * FROM advisory_reports WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
        result = {
            **dict(row),
            "template": configuration["template"],
            "configuration_id": configuration["id"],
            "roles": configuration["roles"],
            "spec": configuration["spec"],
            "candidate": {
                "id": row["candidate_id"],
                "base_revision": row["base_revision"],
                "head_revision": row["head_revision"],
                "tree_revision": row["tree_revision"],
                "object_format": row["object_format"],
                "content_sha256": row["candidate_content_sha256"],
            },
            "events": events,
            "runs": runs,
            "blind_reviews": {
                item["reviewer_role"]: self._advisory_blind_record(item)
                for item in blind_rows
            },
            "challenge_reviews": {
                item["reviewer_role"]: self._advisory_challenge_record(item)
                for item in challenge_rows
            },
            "report": (
                None if report_row is None else self._advisory_report_record(report_row)
            ),
        }
        for field in (
            "candidate_id",
            "base_revision",
            "head_revision",
            "tree_revision",
            "object_format",
            "candidate_content_sha256",
        ):
            result.pop(field)
        return result

    def get_advisory_execution(self, execution_id: str) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        with self._read_snapshot() as connection:
            return self._advisory_execution_record(connection, execution_id)

    def list_advisory_executions(
        self,
        *,
        project_key: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if project_key is not None:
            project_key = validate_project_key(project_key)
        if state is not None and state not in {
            "active",
            "cancelled",
            "completed",
            "failed",
        }:
            raise ValidationError("advisory execution state is invalid")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 500
        ):
            raise ValidationError("limit must be between 1 and 500")
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT execution.id FROM advisory_executions execution "
                "JOIN projects project ON project.id = execution.project_id "
                "WHERE (? IS NULL OR project.krab_project_key = ?) "
                "AND (? IS NULL OR execution.state = ?) "
                "ORDER BY execution.created_at DESC, execution.id DESC LIMIT ?",
                (project_key, project_key, state, state, limit),
            ).fetchall()
            return [
                self._advisory_execution_record(connection, row["id"]) for row in rows
            ]

    @staticmethod
    def _advisory_submission_context(
        connection: sqlite3.Connection,
        run_id: str,
        *,
        expected_stage: str,
        expected_roles: tuple[str, ...],
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT binding.execution_id, binding.stage AS binding_stage, "
            "binding.role, execution.state, execution.stage, "
            "execution.configuration_version_id, candidate.head_revision, "
            "run.state AS run_state, run.source_revision AS run_source_revision, "
            "attempt.id AS attempt_id, attempt.source_revision AS attempt_source "
            "FROM advisory_run_bindings binding "
            "JOIN advisory_executions execution "
            "ON execution.id = binding.execution_id "
            "JOIN integration_candidates candidate "
            "ON candidate.id = execution.candidate_id "
            "JOIN runs run ON run.id = binding.run_id "
            "LEFT JOIN run_attempts attempt ON attempt.run_id = run.id "
            "AND attempt.state = 'running' WHERE binding.run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"advisory-bound run not found: {run_id}")
        if row["role"] not in expected_roles:
            raise ConflictError("advisory submission requires another role")
        if row["binding_stage"] != expected_stage:
            raise ConflictError("advisory run belongs to another stage")
        if row["state"] != "active" or row["stage"] != expected_stage:
            raise ConflictError("advisory execution is not accepting this submission")
        if row["run_state"] != "running" or row["attempt_id"] is None:
            raise ConflictError("advisory run has no active worker attempt")
        if (
            row["run_source_revision"] != row["head_revision"]
            or row["attempt_source"] != row["head_revision"]
        ):
            raise ConflictError("advisory run candidate revision changed")
        return row

    @staticmethod
    def _advisory_spec(
        connection: sqlite3.Connection, version_id: str
    ) -> AdvisoryConfigurationSpec:
        row = connection.execute(
            "SELECT template_definition_id, spec_json "
            "FROM workflow_configuration_versions WHERE id = ?",
            (version_id,),
        ).fetchone()
        if row is None or row["template_definition_id"] != ADVISORY_REVIEW_TEMPLATE_ID:
            raise StorageError("stored advisory configuration is unavailable")
        try:
            return AdvisoryConfigurationSpec.from_mapping(json.loads(row["spec_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored advisory configuration is invalid") from exc

    def submit_advisory_blind_review(
        self,
        run_id: str,
        *,
        review: AdvisoryBlindReview,
        idempotency_key: str,
    ) -> dict[str, Any]:
        run_id = require_string(run_id, field="run_id", max_bytes=64)
        if not isinstance(review, AdvisoryBlindReview):
            raise ValidationError("review must be an AdvisoryBlindReview")
        idempotency_key = self._advisory_idempotency_key(idempotency_key)
        review_id = self.new_id("abr")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                context = self._advisory_submission_context(
                    connection,
                    run_id,
                    expected_stage="blind_review",
                    expected_roles=ADVISORY_REVIEWER_ROLES,
                )
                existing = connection.execute(
                    "SELECT * FROM advisory_blind_reviews "
                    "WHERE run_id = ? AND idempotency_key = ?",
                    (run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != review.digest:
                        raise ConflictError(
                            "idempotency key already names another blind review"
                        )
                    connection.commit()
                    return self._advisory_blind_record(existing)
                if (
                    connection.execute(
                        "SELECT 1 FROM advisory_blind_reviews "
                        "WHERE execution_id = ? AND reviewer_role = ?",
                        (context["execution_id"], context["role"]),
                    ).fetchone()
                    is not None
                ):
                    raise ConflictError(
                        "advisory reviewer already submitted blind review"
                    )
                spec = self._advisory_spec(
                    connection, context["configuration_version_id"]
                )
                if len(review.findings) > spec.max_findings_per_reviewer:
                    raise ConflictError(
                        "advisory blind review exceeds its configured finding limit"
                    )
                connection.execute(
                    "INSERT INTO advisory_blind_reviews "
                    "(id, execution_id, reviewer_role, run_id, attempt_id, "
                    "schema_version, review_json, content_sha256, idempotency_key, "
                    "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        review_id,
                        context["execution_id"],
                        context["role"],
                        run_id,
                        context["attempt_id"],
                        review.schema_version,
                        review.canonical,
                        review.digest,
                        idempotency_key,
                        now,
                    ),
                )
                stored = connection.execute(
                    "SELECT * FROM advisory_blind_reviews WHERE id = ?",
                    (review_id,),
                ).fetchone()
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "advisory blind review could not be recorded"
                ) from exc
        return self._advisory_blind_record(stored)

    def submit_advisory_challenge_review(
        self,
        run_id: str,
        *,
        review: AdvisoryChallengeReview,
        idempotency_key: str,
    ) -> dict[str, Any]:
        run_id = require_string(run_id, field="run_id", max_bytes=64)
        if not isinstance(review, AdvisoryChallengeReview):
            raise ValidationError("review must be an AdvisoryChallengeReview")
        idempotency_key = self._advisory_idempotency_key(idempotency_key)
        review_id = self.new_id("acr")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                context = self._advisory_submission_context(
                    connection,
                    run_id,
                    expected_stage="challenge",
                    expected_roles=ADVISORY_REVIEWER_ROLES,
                )
                existing = connection.execute(
                    "SELECT * FROM advisory_challenge_reviews "
                    "WHERE run_id = ? AND idempotency_key = ?",
                    (run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != review.digest:
                        raise ConflictError(
                            "idempotency key already names another challenge review"
                        )
                    connection.commit()
                    return self._advisory_challenge_record(existing)
                if (
                    connection.execute(
                        "SELECT 1 FROM advisory_challenge_reviews "
                        "WHERE execution_id = ? AND reviewer_role = ?",
                        (context["execution_id"], context["role"]),
                    ).fetchone()
                    is not None
                ):
                    raise ConflictError(
                        "advisory reviewer already submitted challenge review"
                    )
                spec = self._advisory_spec(
                    connection, context["configuration_version_id"]
                )
                if len(review.challenges) > spec.max_challenges_per_reviewer:
                    raise ConflictError(
                        "advisory challenge review exceeds its configured limit"
                    )
                blind_rows = connection.execute(
                    "SELECT * FROM advisory_blind_reviews WHERE execution_id = ?",
                    (context["execution_id"],),
                ).fetchall()
                finding_refs: set[tuple[str, str]] = set()
                for blind_row in blind_rows:
                    blind = AdvisoryBlindReview.from_mapping(
                        self._advisory_blind_record(blind_row)["review"]
                    )
                    finding_refs.update(
                        (blind_row["reviewer_role"], finding.key)
                        for finding in blind.findings
                    )
                for challenge in review.challenges:
                    reference = (
                        challenge.target_role.value,
                        challenge.target_finding_key,
                    )
                    if challenge.target_role.value == context["role"]:
                        raise ConflictError(
                            "advisory challenge must target another reviewer"
                        )
                    if reference not in finding_refs:
                        raise ConflictError(
                            "advisory challenge references an unknown blind finding"
                        )
                connection.execute(
                    "INSERT INTO advisory_challenge_reviews "
                    "(id, execution_id, reviewer_role, run_id, attempt_id, "
                    "schema_version, review_json, content_sha256, idempotency_key, "
                    "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        review_id,
                        context["execution_id"],
                        context["role"],
                        run_id,
                        context["attempt_id"],
                        review.schema_version,
                        review.canonical,
                        review.digest,
                        idempotency_key,
                        now,
                    ),
                )
                stored = connection.execute(
                    "SELECT * FROM advisory_challenge_reviews WHERE id = ?",
                    (review_id,),
                ).fetchone()
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                connection.rollback()
                raise StorageError("stored advisory blind review is invalid") from exc
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "advisory challenge review could not be recorded"
                ) from exc
        return self._advisory_challenge_record(stored)

    def submit_advisory_report(
        self,
        run_id: str,
        *,
        report: AdvisoryReport,
        idempotency_key: str,
    ) -> dict[str, Any]:
        run_id = require_string(run_id, field="run_id", max_bytes=64)
        if not isinstance(report, AdvisoryReport):
            raise ValidationError("report must be an AdvisoryReport")
        idempotency_key = self._advisory_idempotency_key(idempotency_key)
        report_id = self.new_id("arp")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                context = self._advisory_submission_context(
                    connection,
                    run_id,
                    expected_stage="triangulate",
                    expected_roles=("coordinator",),
                )
                existing = connection.execute(
                    "SELECT * FROM advisory_reports "
                    "WHERE run_id = ? AND idempotency_key = ?",
                    (run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != report.digest:
                        raise ConflictError(
                            "idempotency key already names another advisory report"
                        )
                    connection.commit()
                    return self._advisory_report_record(existing)
                if (
                    connection.execute(
                        "SELECT 1 FROM advisory_reports WHERE execution_id = ?",
                        (context["execution_id"],),
                    ).fetchone()
                    is not None
                ):
                    raise ConflictError("advisory execution already has a report")
                spec = self._advisory_spec(
                    connection, context["configuration_version_id"]
                )
                if len(report.items) > spec.max_report_items:
                    raise ConflictError(
                        "advisory report exceeds its configured item limit"
                    )
                blind_rows = connection.execute(
                    "SELECT * FROM advisory_blind_reviews WHERE execution_id = ?",
                    (context["execution_id"],),
                ).fetchall()
                challenge_rows = connection.execute(
                    "SELECT * FROM advisory_challenge_reviews WHERE execution_id = ?",
                    (context["execution_id"],),
                ).fetchall()
                finding_refs: set[tuple[str, str]] = set()
                for blind_row in blind_rows:
                    blind = AdvisoryBlindReview.from_mapping(
                        self._advisory_blind_record(blind_row)["review"]
                    )
                    finding_refs.update(
                        (blind_row["reviewer_role"], finding.key)
                        for finding in blind.findings
                    )
                challenge_refs: set[tuple[str, str]] = set()
                for challenge_row in challenge_rows:
                    challenge_review = AdvisoryChallengeReview.from_mapping(
                        self._advisory_challenge_record(challenge_row)["review"]
                    )
                    challenge_refs.update(
                        (challenge_row["reviewer_role"], challenge.key)
                        for challenge in challenge_review.challenges
                    )
                for item in report.items:
                    if any(
                        (reference.reviewer_role.value, reference.finding_key)
                        not in finding_refs
                        for reference in item.finding_refs
                    ):
                        raise ConflictError(
                            "advisory report references an unknown blind finding"
                        )
                    if any(
                        (reference.reviewer_role.value, reference.challenge_key)
                        not in challenge_refs
                        for reference in item.challenge_refs
                    ):
                        raise ConflictError(
                            "advisory report references an unknown challenge"
                        )
                connection.execute(
                    "INSERT INTO advisory_reports "
                    "(id, execution_id, run_id, attempt_id, authority, "
                    "schema_version, report_json, content_sha256, idempotency_key, "
                    "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        report_id,
                        context["execution_id"],
                        run_id,
                        context["attempt_id"],
                        report.authority.value,
                        report.schema_version,
                        report.canonical,
                        report.digest,
                        idempotency_key,
                        now,
                    ),
                )
                stored = connection.execute(
                    "SELECT * FROM advisory_reports WHERE id = ?", (report_id,)
                ).fetchone()
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                connection.rollback()
                raise StorageError("stored advisory evidence is invalid") from exc
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("advisory report could not be recorded") from exc
        return self._advisory_report_record(stored)

    @staticmethod
    def _advisory_phase_evidence(
        connection: sqlite3.Connection,
        execution_id: str,
        *,
        stage: str,
    ) -> list[dict[str, Any]]:
        if stage == "blind_review":
            rows = connection.execute(
                "SELECT binding.role, binding.run_id, run.state, "
                "artifact.id AS artifact_id, artifact.attempt_id, "
                "artifact.content_sha256 "
                "FROM advisory_run_bindings binding "
                "JOIN runs run ON run.id = binding.run_id "
                "LEFT JOIN advisory_blind_reviews artifact "
                "ON artifact.execution_id = binding.execution_id "
                "AND artifact.reviewer_role = binding.role "
                "AND artifact.run_id = binding.run_id "
                "WHERE binding.execution_id = ? AND binding.stage = ? "
                "ORDER BY binding.role",
                (execution_id, stage),
            ).fetchall()
        elif stage == "challenge":
            rows = connection.execute(
                "SELECT binding.role, binding.run_id, run.state, "
                "artifact.id AS artifact_id, artifact.attempt_id, "
                "artifact.content_sha256 "
                "FROM advisory_run_bindings binding "
                "JOIN runs run ON run.id = binding.run_id "
                "LEFT JOIN advisory_challenge_reviews artifact "
                "ON artifact.execution_id = binding.execution_id "
                "AND artifact.reviewer_role = binding.role "
                "AND artifact.run_id = binding.run_id "
                "WHERE binding.execution_id = ? AND binding.stage = ? "
                "ORDER BY binding.role",
                (execution_id, stage),
            ).fetchall()
        else:
            rows = connection.execute(
                "SELECT binding.role, binding.run_id, run.state, "
                "artifact.id AS artifact_id, artifact.attempt_id, "
                "artifact.content_sha256 "
                "FROM advisory_run_bindings binding "
                "JOIN runs run ON run.id = binding.run_id "
                "LEFT JOIN advisory_reports artifact "
                "ON artifact.execution_id = binding.execution_id "
                "AND artifact.run_id = binding.run_id "
                "WHERE binding.execution_id = ? AND binding.stage = 'triangulate' "
                "AND binding.role = 'coordinator'",
                (execution_id,),
            ).fetchall()
            if len(rows) != 1:
                raise ConflictError("advisory triangulation run is incomplete")
        if stage in {"blind_review", "challenge"} and {
            row["role"] for row in rows
        } != set(ADVISORY_REVIEWER_ROLES):
            raise ConflictError(f"advisory {stage} runs are incomplete")
        if any(row["state"] != "succeeded" for row in rows):
            raise ConflictError(f"advisory {stage} runs have not succeeded")
        if any(row["artifact_id"] is None for row in rows):
            raise ConflictError(f"advisory {stage} evidence is incomplete")
        return [
            {
                "role": row["role"],
                "run_id": row["run_id"],
                "attempt_id": row["attempt_id"],
                "artifact_id": row["artifact_id"],
                "content_sha256": row["content_sha256"],
            }
            for row in rows
        ]

    def advance_advisory_execution(
        self, execution_id: str, *, expected_version: int
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._advisory_positive_integer(expected_version, field="expected_version")
        transitions = {
            "blind_review": ("challenge", "advisory.blind_completed"),
            "challenge": ("triangulate", "advisory.challenge_completed"),
            "triangulate": ("completion", "advisory.triangulation_completed"),
        }
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, stage, version, requested_by "
                    "FROM advisory_executions WHERE id = ?",
                    (execution_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"advisory execution not found: {execution_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"advisory execution version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["state"] != "active" or row["stage"] not in transitions:
                    raise ConflictError("advisory execution cannot advance")
                stage = row["stage"]
                target_stage, event_kind = transitions[stage]
                evidence = self._advisory_phase_evidence(
                    connection, execution_id, stage=stage
                )
                changed = connection.execute(
                    "UPDATE advisory_executions SET stage = ?, "
                    "version = version + 1, updated_at = ? "
                    "WHERE id = ? AND version = ? AND state = 'active' AND stage = ?",
                    (target_stage, now, execution_id, expected_version, stage),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "advisory execution changed concurrently"
                    )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM advisory_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO advisory_execution_events "
                    "(execution_id, sequence, kind, from_state, to_state, actor, "
                    "metadata_json, created_at) VALUES (?, ?, ?, 'active', "
                    "'active', ?, ?, ?)",
                    (
                        execution_id,
                        sequence,
                        event_kind,
                        row["requested_by"],
                        canonical_json(
                            {
                                "artifacts": evidence,
                                "from_stage": stage,
                                "to_stage": target_stage,
                            }
                        ),
                        now,
                    ),
                )
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
                raise ConflictError("advisory execution could not advance") from exc
        return self.get_advisory_execution(execution_id)

    def complete_advisory_execution(
        self, execution_id: str, *, expected_version: int
    ) -> dict[str, Any]:
        return self._finish_advisory_execution(
            execution_id,
            expected_version=expected_version,
            target_state="completed",
            event_kind="advisory.completed",
            reason=None,
        )

    def cancel_advisory_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        cancelled_by: str,
        reason: str,
    ) -> dict[str, Any]:
        cancelled_by = require_string(cancelled_by, field="cancelled_by", max_bytes=64)
        reason = require_string(reason, field="reason", max_bytes=4096)
        return self._finish_advisory_execution(
            execution_id,
            expected_version=expected_version,
            target_state="cancelled",
            event_kind="advisory.cancelled",
            reason=reason,
            actor=cancelled_by,
        )

    def fail_advisory_execution(
        self, execution_id: str, *, expected_version: int, reason: str
    ) -> dict[str, Any]:
        reason = require_string(reason, field="reason", max_bytes=4096)
        return self._finish_advisory_execution(
            execution_id,
            expected_version=expected_version,
            target_state="failed",
            event_kind="advisory.failed",
            reason=reason,
        )

    def _finish_advisory_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        target_state: str,
        event_kind: str,
        reason: str | None,
        actor: str | None = None,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._advisory_positive_integer(expected_version, field="expected_version")
        if target_state not in {"completed", "cancelled", "failed"}:
            raise ValidationError("advisory terminal state is invalid")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, stage, version, requested_by "
                    "FROM advisory_executions WHERE id = ?",
                    (execution_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"advisory execution not found: {execution_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"advisory execution version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["state"] != "active":
                    raise ConflictError(
                        f"advisory execution is not active: {row['state']}"
                    )
                event_actor = row["requested_by"] if actor is None else actor
                self._active_user(connection, event_actor, role="advisory actor")
                if actor is not None and actor != row["requested_by"]:
                    raise ConflictError("only the pinned advisory requester may cancel")
                if target_state == "completed":
                    if row["stage"] != "completion":
                        raise ConflictError("advisory execution is not completing")
                    if (
                        connection.execute(
                            "SELECT 1 FROM advisory_reports WHERE execution_id = ?",
                            (execution_id,),
                        ).fetchone()
                        is None
                    ):
                        raise ConflictError("advisory execution has no report")
                cancel_reason = reason if target_state == "cancelled" else None
                failure_reason = reason if target_state == "failed" else None
                changed = connection.execute(
                    "UPDATE advisory_executions SET state = ?, "
                    "version = version + 1, cancel_reason = ?, failure_reason = ?, "
                    "updated_at = ?, finished_at = ? "
                    "WHERE id = ? AND version = ? AND state = 'active'",
                    (
                        target_state,
                        cancel_reason,
                        failure_reason,
                        now,
                        now,
                        execution_id,
                        expected_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "advisory execution changed concurrently"
                    )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM advisory_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO advisory_execution_events "
                    "(execution_id, sequence, kind, from_state, to_state, actor, "
                    "metadata_json, created_at) VALUES (?, ?, ?, 'active', ?, ?, ?, ?)",
                    (
                        execution_id,
                        sequence,
                        event_kind,
                        target_state,
                        event_actor,
                        canonical_json({} if reason is None else {"reason": reason}),
                        now,
                    ),
                )
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
                raise ConflictError(
                    f"advisory execution could not become {target_state}"
                ) from exc
        return self.get_advisory_execution(execution_id)
