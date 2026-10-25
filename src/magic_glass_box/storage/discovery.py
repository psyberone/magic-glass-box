from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from typing import Any

from ..discovery import (
    DiscoveryConfigurationSpec,
    DiscoveryDisposition,
    DiscoveryRuling,
    DiscoverySourceKind,
    DiscoveryTrace,
)
from ..domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    require_string,
    validate_krab_id,
    validate_name,
    validate_project_key,
)
from ..krab import IDEMPOTENCY_KEY_RE
from ..workflows import DISCOVERY_TEMPLATE_ID
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)

OBJECT_REVISION_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
MAX_DISCOVERY_ASK_RULINGS = 8


class DiscoveryStorage:
    """Persist the bounded Discovery prescription and its typed evidence."""

    @staticmethod
    def _discovery_idempotency_key(value: object) -> str:
        if not isinstance(value, str) or IDEMPOTENCY_KEY_RE.fullmatch(value) is None:
            raise ValidationError("idempotency_key is invalid")
        return value

    @staticmethod
    def _discovery_positive_integer(value: object, *, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError(f"{field} must be a positive integer")
        return value

    @staticmethod
    def _discovery_source_revision(value: object) -> str:
        if not isinstance(value, str) or OBJECT_REVISION_RE.fullmatch(value) is None:
            raise ValidationError(
                "source_revision must be a 40- or 64-character object ID"
            )
        return value

    @staticmethod
    def _discovery_start_request(
        *,
        project_key: str,
        configuration_name: str,
        work_item_id: str,
        source_revision: str,
        expected_configuration_version: int,
        expected_context_version_id: str,
        expected_work_item_version_id: str,
        expected_thread_generation: int,
        expected_thread_version: int,
        krab_session_id: str,
        krab_claim_id: str | None,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "project_key": project_key,
            "configuration_name": configuration_name,
            "work_item_id": work_item_id,
            "source_revision": source_revision,
            "expected_configuration_version": expected_configuration_version,
            "expected_context_version_id": expected_context_version_id,
            "expected_work_item_version_id": expected_work_item_version_id,
            "expected_thread_generation": expected_thread_generation,
            "expected_thread_version": expected_thread_version,
            "krab_session_id": krab_session_id,
            "krab_claim_id": krab_claim_id,
        }

    def start_discovery_execution(
        self,
        *,
        project_key: str,
        configuration_name: str,
        work_item_id: str,
        source_revision: str,
        expected_configuration_version: int,
        expected_context_version_id: str,
        expected_work_item_version_id: str,
        expected_thread_generation: int,
        expected_thread_version: int,
        krab_session_id: str,
        krab_claim_id: str | None,
        idempotency_key: str,
        requested_by: str,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        configuration_name = validate_name(
            configuration_name, field="discovery configuration name"
        )
        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
        source_revision = self._discovery_source_revision(source_revision)
        self._discovery_positive_integer(
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
        self._discovery_positive_integer(
            expected_thread_generation, field="expected_thread_generation"
        )
        self._discovery_positive_integer(
            expected_thread_version, field="expected_thread_version"
        )
        krab_session_id = validate_krab_id(krab_session_id, "ses")
        if krab_claim_id is not None:
            krab_claim_id = validate_krab_id(krab_claim_id, "clm")
        idempotency_key = self._discovery_idempotency_key(idempotency_key)
        requested_by = require_string(requested_by, field="requested_by", max_bytes=64)
        request = self._discovery_start_request(
            project_key=project_key,
            configuration_name=configuration_name,
            work_item_id=work_item_id,
            source_revision=source_revision,
            expected_configuration_version=expected_configuration_version,
            expected_context_version_id=expected_context_version_id,
            expected_work_item_version_id=expected_work_item_version_id,
            expected_thread_generation=expected_thread_generation,
            expected_thread_version=expected_thread_version,
            krab_session_id=krab_session_id,
            krab_claim_id=krab_claim_id,
        )
        request_json = canonical_json(request)
        request_sha256 = content_sha256(request_json)
        execution_id = self.new_id("dxe")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, requested_by, role="discovery requester")
                existing = connection.execute(
                    "SELECT id, content_sha256 FROM discovery_executions "
                    "WHERE requested_by = ? AND idempotency_key = ?",
                    (requested_by, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != request_sha256:
                        raise ConflictError(
                            "idempotency key already names another discovery execution"
                        )
                    stored_id = existing["id"]
                    connection.commit()
                else:
                    project = connection.execute(
                        "SELECT p.id, p.active, p.krab_instance_id, "
                        "context.id AS context_version_id FROM projects p "
                        "JOIN project_context_versions context "
                        "ON context.project_id = p.id "
                        "WHERE p.krab_project_key = ? "
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
                            "discovery configuration not found: "
                            f"{project_key}/{configuration_name}"
                        )
                    if configuration["version"] != expected_configuration_version:
                        raise ConflictError("discovery configuration version changed")
                    if configuration["template_definition_id"] != DISCOVERY_TEMPLATE_ID:
                        raise ConflictError(
                            "workflow prescription is not executable by the "
                            "Discovery director"
                        )
                    if configuration["role_count"] != 2:
                        raise StorageError(
                            "stored discovery configuration roles do not match"
                        )
                    if configuration["active_role_count"] != 2:
                        raise ConflictError("discovery role profile is inactive")
                    work_item = connection.execute(
                        "SELECT item.project_id, version.id AS work_item_version_id, "
                        "thread.id AS work_thread_id, thread.generation, "
                        "thread.state, thread.version AS thread_version "
                        "FROM work_items item JOIN work_item_versions version "
                        "ON version.work_item_id = item.id JOIN work_threads thread "
                        "ON thread.work_item_id = item.id WHERE item.id = ? "
                        "ORDER BY version.version DESC, thread.generation DESC LIMIT 1",
                        (work_item_id,),
                    ).fetchone()
                    if work_item is None:
                        raise NotFoundError(f"work item not found: {work_item_id}")
                    if work_item["project_id"] != project["id"]:
                        raise ConflictError("work item belongs to another project")
                    if (
                        work_item["work_item_version_id"]
                        != expected_work_item_version_id
                    ):
                        raise ConcurrentUpdateError("work item version changed")
                    if work_item["state"] != "active":
                        raise ConflictError(
                            f"work thread is not active: {work_item['state']}"
                        )
                    if work_item["generation"] != expected_thread_generation:
                        raise ConcurrentUpdateError("work thread generation changed")
                    if work_item["thread_version"] != expected_thread_version:
                        raise ConcurrentUpdateError("work thread version changed")
                    active_discovery = connection.execute(
                        "SELECT id FROM discovery_executions "
                        "WHERE work_thread_id = ? AND state = 'active'",
                        (work_item["work_thread_id"],),
                    ).fetchone()
                    active_workflow = connection.execute(
                        "SELECT id FROM workflow_executions "
                        "WHERE work_thread_id = ? AND state = 'active'",
                        (work_item["work_thread_id"],),
                    ).fetchone()
                    if active_discovery is not None or active_workflow is not None:
                        raise ConflictError(
                            "work thread already has an active workflow or active "
                            "prescription"
                        )
                    decision_owners = connection.execute(
                        "SELECT identity.id, identity.handle "
                        "FROM user_krab_bindings binding "
                        "JOIN krab_identities identity "
                        "ON identity.id = binding.krab_identity_id "
                        "WHERE binding.user_id = ? "
                        "AND identity.krab_instance_id = ? ORDER BY identity.id",
                        (requested_by, project["krab_instance_id"]),
                    ).fetchall()
                    if len(decision_owners) != 1:
                        raise ConflictError(
                            "discovery requester requires exactly one Krab identity "
                            "on the project instance"
                        )
                    decision_owner = decision_owners[0]
                    connection.execute(
                        "INSERT INTO discovery_executions "
                        "(id, project_id, template_definition_id, "
                        "configuration_version_id, context_version_id, work_item_id, "
                        "work_item_version_id, work_thread_id, "
                        "work_thread_generation, source_revision, "
                        "decision_owner_krab_identity_id, krab_session_id, "
                        "krab_claim_id, state, stage, version, request_json, "
                        "content_sha256, idempotency_key, requested_by, created_at, "
                        "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                        "'active', 'investigate', 1, ?, ?, ?, ?, ?, ?)",
                        (
                            execution_id,
                            project["id"],
                            DISCOVERY_TEMPLATE_ID,
                            configuration["version_id"],
                            project["context_version_id"],
                            work_item_id,
                            work_item["work_item_version_id"],
                            work_item["work_thread_id"],
                            work_item["generation"],
                            source_revision,
                            decision_owner["id"],
                            krab_session_id,
                            krab_claim_id,
                            request_json,
                            request_sha256,
                            idempotency_key,
                            requested_by,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO discovery_execution_events "
                        "(execution_id, sequence, kind, from_state, to_state, actor, "
                        "metadata_json, created_at) VALUES (?, 1, "
                        "'discovery.started', NULL, 'active', ?, '{}', ?)",
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
                raise ConflictError("discovery execution could not be started") from exc
        return self.get_discovery_execution(stored_id)

    def _discovery_trace_record(self, row: sqlite3.Row) -> dict[str, Any]:
        try:
            trace = DiscoveryTrace.from_mapping(json.loads(row["trace_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored discovery trace is invalid") from exc
        if trace.digest != row["content_sha256"]:
            raise StorageError("stored discovery trace digest does not match")
        return {
            "id": row["id"],
            "execution_id": row["execution_id"],
            "run_id": row["run_id"],
            "attempt_id": row["attempt_id"],
            "trace": trace.to_dict(),
            "content_sha256": row["content_sha256"],
            "idempotency_key": row["idempotency_key"],
            "created_at": row["created_at"],
        }

    def _discovery_ruling_record(self, row: sqlite3.Row) -> dict[str, Any]:
        try:
            ruling = DiscoveryRuling.from_mapping(json.loads(row["ruling_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored discovery ruling is invalid") from exc
        if (
            ruling.digest != row["content_sha256"]
            or ruling.disposition.value != row["disposition"]
            or ruling.interaction_id != row["interaction_id"]
        ):
            raise StorageError("stored discovery ruling does not match")
        return {
            "id": row["id"],
            "execution_id": row["execution_id"],
            "sequence": row["sequence"],
            "run_id": row["run_id"],
            "attempt_id": row["attempt_id"],
            "disposition": row["disposition"],
            "interaction_id": row["interaction_id"],
            "ruling": ruling.to_dict(),
            "content_sha256": row["content_sha256"],
            "idempotency_key": row["idempotency_key"],
            "created_at": row["created_at"],
        }

    def _discovery_execution_record(
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
            "execution.source_revision, execution.krab_session_id, "
            "execution.krab_claim_id, execution.state, execution.stage, "
            "execution.version, execution.content_sha256, "
            "execution.cancel_reason, execution.failure_reason, "
            "requester.handle AS requested_by, "
            "owner.id AS decision_owner_krab_identity_id, "
            "owner.krab_user_id AS decision_owner_krab_user_id, "
            "owner.handle AS decision_owner_krab_handle, "
            "execution.created_at, execution.updated_at, execution.finished_at "
            "FROM discovery_executions execution "
            "JOIN projects project ON project.id = execution.project_id "
            "JOIN workflow_configuration_versions configuration "
            "ON configuration.id = execution.configuration_version_id "
            "JOIN workflow_configurations named "
            "ON named.id = configuration.configuration_id "
            "JOIN project_context_versions context "
            "ON context.id = execution.context_version_id "
            "JOIN work_item_versions item_version "
            "ON item_version.id = execution.work_item_version_id "
            "JOIN users requester ON requester.id = execution.requested_by "
            "JOIN krab_identities owner "
            "ON owner.id = execution.decision_owner_krab_identity_id "
            "WHERE execution.id = ?",
            (execution_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"discovery execution not found: {execution_id}")
        configuration = self._configuration_record(
            connection,
            self._configuration_row(
                connection, version_id=row["configuration_version_id"]
            ),
        )
        if configuration["template"]["id"] != DISCOVERY_TEMPLATE_ID:
            raise StorageError("stored Discovery execution prescription is invalid")
        events = connection.execute(
            "SELECT event.sequence, event.kind, event.from_state, event.to_state, "
            "actor.handle AS actor, event.metadata_json, event.created_at "
            "FROM discovery_execution_events event JOIN users actor "
            "ON actor.id = event.actor WHERE event.execution_id = ? "
            "ORDER BY event.sequence",
            (execution_id,),
        ).fetchall()
        try:
            parsed_events = [
                {**dict(event), "metadata": json.loads(event["metadata_json"])}
                for event in events
            ]
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored Discovery event is invalid") from exc
        for event in parsed_events:
            event.pop("metadata_json")
        trace_row = connection.execute(
            "SELECT * FROM discovery_traces WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
        ruling_rows = connection.execute(
            "SELECT * FROM discovery_rulings WHERE execution_id = ? ORDER BY sequence",
            (execution_id,),
        ).fetchall()
        rulings = [self._discovery_ruling_record(item) for item in ruling_rows]
        run_rows = connection.execute(
            "SELECT binding.role, binding.run_id, run.state, run.version, "
            "run.created_at, run.finished_at FROM discovery_run_bindings binding "
            "JOIN runs run ON run.id = binding.run_id "
            "WHERE binding.execution_id = ? ORDER BY binding.role",
            (execution_id,),
        ).fetchall()
        result = {
            **dict(row),
            "template": configuration["template"],
            "configuration_id": configuration["id"],
            "roles": configuration["roles"],
            "spec": configuration["spec"],
            "decision_owner": {
                "local_user_handle": row["requested_by"],
                "krab_identity_id": row["decision_owner_krab_identity_id"],
                "krab_user_id": row["decision_owner_krab_user_id"],
                "krab_handle": row["decision_owner_krab_handle"],
            },
            "events": parsed_events,
            "runs": {run["role"]: dict(run) for run in run_rows},
            "trace": (
                None if trace_row is None else self._discovery_trace_record(trace_row)
            ),
            "rulings": rulings,
            "final_ruling": next(
                (
                    ruling
                    for ruling in reversed(rulings)
                    if ruling["disposition"] in {"GO", "NO_GO"}
                ),
                None,
            ),
        }
        result.pop("decision_owner_krab_identity_id")
        result.pop("decision_owner_krab_user_id")
        result.pop("decision_owner_krab_handle")
        return result

    def get_discovery_execution(self, execution_id: str) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        with self._read_snapshot() as connection:
            return self._discovery_execution_record(connection, execution_id)

    def list_discovery_executions(
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
            raise ValidationError("discovery execution state is invalid")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 500
        ):
            raise ValidationError("limit must be between 1 and 500")
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT execution.id FROM discovery_executions execution "
                "JOIN projects project ON project.id = execution.project_id "
                "WHERE (? IS NULL OR project.krab_project_key = ?) "
                "AND (? IS NULL OR execution.state = ?) "
                "ORDER BY execution.created_at DESC, execution.id DESC LIMIT ?",
                (project_key, project_key, state, state, limit),
            ).fetchall()
            return [
                self._discovery_execution_record(connection, row["id"]) for row in rows
            ]

    @staticmethod
    def _submission_context(
        connection: sqlite3.Connection,
        run_id: str,
        *,
        expected_role: str,
        expected_stage: str,
        allow_waiting: bool,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT binding.execution_id, binding.role, execution.state, "
            "execution.stage, execution.configuration_version_id, "
            "execution.work_item_version_id, execution.source_revision, "
            "execution.decision_owner_krab_identity_id, "
            "run.state AS run_state, run.source_revision AS run_source_revision, "
            "attempt.id AS attempt_id, attempt.source_revision AS attempt_source "
            "FROM discovery_run_bindings binding "
            "JOIN discovery_executions execution "
            "ON execution.id = binding.execution_id "
            "JOIN runs run ON run.id = binding.run_id "
            "LEFT JOIN run_attempts attempt ON attempt.run_id = run.id "
            "AND attempt.state = 'running' WHERE binding.run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"Discovery-bound run not found: {run_id}")
        if row["role"] != expected_role:
            raise ConflictError(
                f"Discovery submission requires the {expected_role} role"
            )
        if row["state"] != "active" or row["stage"] != expected_stage:
            raise ConflictError("Discovery execution is not accepting this submission")
        allowed_states = (
            {"running", "waiting_for_input"} if allow_waiting else {"running"}
        )
        if row["run_state"] not in allowed_states or row["attempt_id"] is None:
            raise ConflictError("Discovery run has no active worker attempt")
        if (
            row["run_source_revision"] != row["source_revision"]
            or row["attempt_source"] != row["source_revision"]
        ):
            raise ConflictError("Discovery run source revision changed")
        return row

    @staticmethod
    def _discovery_spec(
        connection: sqlite3.Connection, version_id: str
    ) -> DiscoveryConfigurationSpec:
        row = connection.execute(
            "SELECT template_definition_id, spec_json "
            "FROM workflow_configuration_versions WHERE id = ?",
            (version_id,),
        ).fetchone()
        if row is None or row["template_definition_id"] != DISCOVERY_TEMPLATE_ID:
            raise StorageError("stored Discovery configuration is unavailable")
        try:
            return DiscoveryConfigurationSpec.from_mapping(json.loads(row["spec_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored Discovery configuration is invalid") from exc

    @staticmethod
    def _acceptance_criterion_count(
        connection: sqlite3.Connection, work_item_version_id: str
    ) -> int:
        row = connection.execute(
            "SELECT content_json FROM work_item_versions WHERE id = ?",
            (work_item_version_id,),
        ).fetchone()
        if row is None:
            raise StorageError("pinned Discovery work item version is unavailable")
        try:
            content = json.loads(row["content_json"])
            criteria = content["acceptance_criteria"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise StorageError("pinned Discovery work item is invalid") from exc
        if not isinstance(criteria, list):
            raise StorageError("pinned Discovery work item is invalid")
        return len(criteria)

    @staticmethod
    def _validate_source_refs(
        records: list[Any], *, acceptance_criterion_count: int
    ) -> None:
        for record in records:
            for source in record.source_refs:
                if (
                    source.kind is DiscoverySourceKind.ACCEPTANCE_CRITERION
                    and source.index is not None
                    and source.index >= acceptance_criterion_count
                ):
                    raise ConflictError(
                        "Discovery source reference is outside the pinned work item"
                    )

    def submit_discovery_trace(
        self,
        run_id: str,
        *,
        trace: DiscoveryTrace,
        idempotency_key: str,
    ) -> dict[str, Any]:
        run_id = require_string(run_id, field="run_id", max_bytes=64)
        if not isinstance(trace, DiscoveryTrace):
            raise ValidationError("trace must be a DiscoveryTrace")
        idempotency_key = self._discovery_idempotency_key(idempotency_key)
        trace_id = self.new_id("dtr")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                context = self._submission_context(
                    connection,
                    run_id,
                    expected_role="investigator",
                    expected_stage="investigate",
                    allow_waiting=False,
                )
                existing = connection.execute(
                    "SELECT * FROM discovery_traces "
                    "WHERE run_id = ? AND idempotency_key = ?",
                    (run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != trace.digest:
                        raise ConflictError(
                            "idempotency key already names another Discovery trace"
                        )
                    connection.commit()
                    return self._discovery_trace_record(existing)
                if (
                    connection.execute(
                        "SELECT 1 FROM discovery_traces WHERE execution_id = ?",
                        (context["execution_id"],),
                    ).fetchone()
                    is not None
                ):
                    raise ConflictError("Discovery execution already has a trace")
                spec = self._discovery_spec(
                    connection, context["configuration_version_id"]
                )
                if len(trace.findings) > spec.max_findings:
                    raise ConflictError(
                        "Discovery trace exceeds its configured finding limit"
                    )
                criterion_count = self._acceptance_criterion_count(
                    connection, context["work_item_version_id"]
                )
                self._validate_source_refs(
                    list(trace.findings),
                    acceptance_criterion_count=criterion_count,
                )
                connection.execute(
                    "INSERT INTO discovery_traces "
                    "(id, execution_id, run_id, attempt_id, schema_version, "
                    "trace_json, content_sha256, idempotency_key, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        trace_id,
                        context["execution_id"],
                        run_id,
                        context["attempt_id"],
                        trace.schema_version,
                        trace.canonical,
                        trace.digest,
                        idempotency_key,
                        now,
                    ),
                )
                stored = connection.execute(
                    "SELECT * FROM discovery_traces WHERE id = ?", (trace_id,)
                ).fetchone()
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("Discovery trace could not be recorded") from exc
        return self._discovery_trace_record(stored)

    def submit_discovery_ruling(
        self,
        run_id: str,
        *,
        ruling: DiscoveryRuling,
        idempotency_key: str,
    ) -> dict[str, Any]:
        run_id = require_string(run_id, field="run_id", max_bytes=64)
        if not isinstance(ruling, DiscoveryRuling):
            raise ValidationError("ruling must be a DiscoveryRuling")
        idempotency_key = self._discovery_idempotency_key(idempotency_key)
        ruling_id = self.new_id("drl")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                context = self._submission_context(
                    connection,
                    run_id,
                    expected_role="coordinator",
                    expected_stage="coordinate",
                    allow_waiting=True,
                )
                existing = connection.execute(
                    "SELECT * FROM discovery_rulings "
                    "WHERE run_id = ? AND idempotency_key = ?",
                    (run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != ruling.digest:
                        raise ConflictError(
                            "idempotency key already names another Discovery ruling"
                        )
                    connection.commit()
                    return self._discovery_ruling_record(existing)
                trace_row = connection.execute(
                    "SELECT * FROM discovery_traces WHERE execution_id = ?",
                    (context["execution_id"],),
                ).fetchone()
                if trace_row is None:
                    raise ConflictError("Discovery ruling requires an exact trace")
                trace_record = self._discovery_trace_record(trace_row)
                trace = DiscoveryTrace.from_mapping(trace_record["trace"])
                finding_keys = {finding.key for finding in trace.findings}
                if not set(ruling.trace_finding_keys).issubset(finding_keys):
                    raise ConflictError(
                        "Discovery ruling references an unknown trace finding"
                    )
                spec = self._discovery_spec(
                    connection, context["configuration_version_id"]
                )
                criterion_count = self._acceptance_criterion_count(
                    connection, context["work_item_version_id"]
                )
                if ruling.work_ready_brief is not None:
                    brief = ruling.work_ready_brief
                    if len(brief.must_items) > spec.max_must_items:
                        raise ConflictError(
                            "Discovery ruling exceeds its configured MUST limit"
                        )
                    if len(brief.follow_up_proposals) > spec.max_follow_up_proposals:
                        raise ConflictError(
                            "Discovery ruling exceeds its configured follow-up limit"
                        )
                    self._validate_source_refs(
                        list(brief.must_items),
                        acceptance_criterion_count=criterion_count,
                    )
                    for must in brief.must_items:
                        if not set(must.finding_keys).issubset(finding_keys):
                            raise ConflictError(
                                "Discovery MUST item references an unknown trace "
                                "finding"
                            )
                previous = connection.execute(
                    "SELECT * FROM discovery_rulings WHERE execution_id = ? "
                    "ORDER BY sequence",
                    (context["execution_id"],),
                ).fetchall()
                asks = [item for item in previous if item["disposition"] == "ASK_HUMAN"]
                if ruling.disposition is DiscoveryDisposition.ASK_HUMAN:
                    if len(asks) >= MAX_DISCOVERY_ASK_RULINGS:
                        raise ConflictError("Discovery human-question limit is reached")
                    interaction = connection.execute(
                        "SELECT run_id, attempt_id, kind, state, "
                        "recipient_krab_identity_id FROM interactions WHERE id = ?",
                        (ruling.interaction_id,),
                    ).fetchone()
                    if (
                        interaction is None
                        or interaction["run_id"] != run_id
                        or interaction["attempt_id"] != context["attempt_id"]
                        or interaction["state"] != "open"
                        or interaction["kind"] not in {"question", "choice"}
                        or interaction["recipient_krab_identity_id"]
                        != context["decision_owner_krab_identity_id"]
                    ):
                        raise ConflictError(
                            "ASK_HUMAN requires the current decision-owner interaction"
                        )
                else:
                    if any(item["disposition"] in {"GO", "NO_GO"} for item in previous):
                        raise ConflictError(
                            "Discovery execution already has a final ruling"
                        )
                    if asks:
                        latest_ask = asks[-1]
                        interaction = connection.execute(
                            "SELECT state FROM interactions WHERE id = ?",
                            (latest_ask["interaction_id"],),
                        ).fetchone()
                        if (
                            ruling.interaction_id != latest_ask["interaction_id"]
                            or interaction is None
                            or interaction["state"] != "answered"
                        ):
                            raise ConflictError(
                                "final Discovery ruling requires the latest "
                                "accepted answer"
                            )
                    elif ruling.interaction_id is not None:
                        raise ConflictError(
                            "final Discovery ruling names no preceding human question"
                        )
                sequence = len(previous) + 1
                connection.execute(
                    "INSERT INTO discovery_rulings "
                    "(id, execution_id, sequence, run_id, attempt_id, disposition, "
                    "interaction_id, schema_version, ruling_json, content_sha256, "
                    "idempotency_key, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        ruling_id,
                        context["execution_id"],
                        sequence,
                        run_id,
                        context["attempt_id"],
                        ruling.disposition.value,
                        ruling.interaction_id,
                        ruling.schema_version,
                        ruling.canonical,
                        ruling.digest,
                        idempotency_key,
                        now,
                    ),
                )
                stored = connection.execute(
                    "SELECT * FROM discovery_rulings WHERE id = ?", (ruling_id,)
                ).fetchone()
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                connection.rollback()
                raise StorageError("stored Discovery trace is invalid") from exc
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("Discovery ruling could not be recorded") from exc
        return self._discovery_ruling_record(stored)

    def advance_discovery_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        expected_stage: str,
        target_stage: str,
        event_kind: str,
        metadata: Mapping[str, Any],
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._discovery_positive_integer(expected_version, field="expected_version")
        expected = {
            (
                "investigate",
                "coordinate",
            ): "discovery.investigation_completed",
            (
                "coordinate",
                "completion",
            ): "discovery.coordination_completed",
        }
        if expected.get((expected_stage, target_stage)) != event_kind:
            raise ValidationError("Discovery stage transition is invalid")
        if not isinstance(metadata, Mapping):
            raise ValidationError("metadata must be an object")
        metadata_value = dict(metadata)
        canonical_json(metadata_value)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, stage, version, requested_by "
                    "FROM discovery_executions WHERE id = ?",
                    (execution_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(
                        f"discovery execution not found: {execution_id}"
                    )
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"discovery execution version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["state"] != "active" or row["stage"] != expected_stage:
                    raise ConflictError("Discovery execution stage changed")
                role = (
                    "investigator" if expected_stage == "investigate" else "coordinator"
                )
                run = connection.execute(
                    "SELECT run.id, run.state FROM discovery_run_bindings binding "
                    "JOIN runs run ON run.id = binding.run_id "
                    "WHERE binding.execution_id = ? AND binding.role = ?",
                    (execution_id, role),
                ).fetchone()
                if run is None or run["state"] != "succeeded":
                    raise ConflictError(f"Discovery {role} run has not succeeded")
                if expected_stage == "investigate":
                    evidence = connection.execute(
                        "SELECT 1 FROM discovery_traces WHERE execution_id = ? "
                        "AND run_id = ?",
                        (execution_id, run["id"]),
                    ).fetchone()
                    if evidence is None:
                        raise ConflictError("Discovery investigation has no trace")
                else:
                    evidence = connection.execute(
                        "SELECT 1 FROM discovery_rulings WHERE execution_id = ? "
                        "AND run_id = ? AND disposition IN ('GO', 'NO_GO')",
                        (execution_id, run["id"]),
                    ).fetchone()
                    if evidence is None:
                        raise ConflictError(
                            "Discovery coordination has no final ruling"
                        )
                changed = connection.execute(
                    "UPDATE discovery_executions SET stage = ?, "
                    "version = version + 1, updated_at = ? "
                    "WHERE id = ? AND version = ? AND state = 'active' "
                    "AND stage = ?",
                    (
                        target_stage,
                        now,
                        execution_id,
                        expected_version,
                        expected_stage,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "Discovery execution changed concurrently"
                    )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM discovery_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO discovery_execution_events "
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
                                **metadata_value,
                                "from_stage": expected_stage,
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
                raise ConflictError("Discovery execution could not advance") from exc
        return self.get_discovery_execution(execution_id)

    def complete_discovery_execution(
        self, execution_id: str, *, expected_version: int
    ) -> dict[str, Any]:
        return self._finish_discovery_execution(
            execution_id,
            expected_version=expected_version,
            target_state="completed",
            event_kind="discovery.completed",
            reason=None,
        )

    def cancel_discovery_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        cancelled_by: str,
        reason: str,
    ) -> dict[str, Any]:
        cancelled_by = require_string(cancelled_by, field="cancelled_by", max_bytes=64)
        reason = require_string(reason, field="reason", max_bytes=4096)
        return self._finish_discovery_execution(
            execution_id,
            expected_version=expected_version,
            target_state="cancelled",
            event_kind="discovery.cancelled",
            reason=reason,
            actor=cancelled_by,
        )

    def fail_discovery_execution(
        self, execution_id: str, *, expected_version: int, reason: str
    ) -> dict[str, Any]:
        reason = require_string(reason, field="reason", max_bytes=4096)
        return self._finish_discovery_execution(
            execution_id,
            expected_version=expected_version,
            target_state="failed",
            event_kind="discovery.failed",
            reason=reason,
        )

    def _finish_discovery_execution(
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
        self._discovery_positive_integer(expected_version, field="expected_version")
        if target_state not in {"completed", "cancelled", "failed"}:
            raise ValidationError("Discovery terminal state is invalid")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, stage, version, requested_by "
                    "FROM discovery_executions WHERE id = ?",
                    (execution_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(
                        f"discovery execution not found: {execution_id}"
                    )
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"discovery execution version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["state"] != "active":
                    raise ConflictError(
                        f"Discovery execution is not active: {row['state']}"
                    )
                event_actor = row["requested_by"] if actor is None else actor
                self._active_user(connection, event_actor, role="Discovery actor")
                if actor is not None and actor != row["requested_by"]:
                    raise ConflictError(
                        "only the pinned Discovery decision owner may cancel"
                    )
                if target_state == "completed":
                    if row["stage"] != "completion":
                        raise ConflictError("Discovery execution is not completing")
                    final = connection.execute(
                        "SELECT 1 FROM discovery_rulings WHERE execution_id = ? "
                        "AND disposition IN ('GO', 'NO_GO')",
                        (execution_id,),
                    ).fetchone()
                    if final is None:
                        raise ConflictError("Discovery execution has no final ruling")
                cancel_reason = reason if target_state == "cancelled" else None
                failure_reason = reason if target_state == "failed" else None
                changed = connection.execute(
                    "UPDATE discovery_executions SET state = ?, "
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
                        "Discovery execution changed concurrently"
                    )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM discovery_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO discovery_execution_events "
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
                    "Discovery execution could not become terminal"
                ) from exc
        return self.get_discovery_execution(execution_id)
