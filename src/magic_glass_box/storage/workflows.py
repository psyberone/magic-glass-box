from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from ..domain import (
    AgentProfileSpec,
    ValidationError,
    WorkspaceMode,
    canonical_json,
    content_sha256,
    require_string,
    validate_name,
    validate_project_key,
)
from ..gates import ReviewSeparation
from ..krab import IDEMPOTENCY_KEY_RE
from ..workflows import (
    IMPLEMENTATION_TEMPLATE,
    IMPLEMENTATION_TEMPLATE_CANONICAL,
    IMPLEMENTATION_TEMPLATE_ID,
    IMPLEMENTATION_TEMPLATE_KEY,
    IMPLEMENTATION_TEMPLATE_REVISION,
    WorkflowConfigurationSpec,
)
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)

OBJECT_REVISION_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class WorkflowStorage:
    @staticmethod
    def _workflow_idempotency_key(value: object) -> str:
        if not isinstance(value, str) or IDEMPOTENCY_KEY_RE.fullmatch(value) is None:
            raise ValidationError("idempotency_key is invalid")
        return value

    @staticmethod
    def _positive_integer(value: object, *, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError(f"{field} must be a positive integer")
        return value

    @staticmethod
    def _source_revision(value: object) -> str:
        if not isinstance(value, str) or OBJECT_REVISION_RE.fullmatch(value) is None:
            raise ValidationError(
                "source_revision must be a 40- or 64-character object ID"
            )
        return value

    @staticmethod
    def _configuration_manifest(
        spec: WorkflowConfigurationSpec,
        *,
        producer_profile_version_id: str,
        reviewer_profile_version_id: str,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "template_definition_id": IMPLEMENTATION_TEMPLATE_ID,
            "spec": spec.to_dict(),
            "roles": {
                "producer": producer_profile_version_id,
                "reviewer": reviewer_profile_version_id,
            },
        }

    @staticmethod
    def _definition_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            definition = json.loads(row["definition_json"])
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError(
                "stored workflow template definition is invalid"
            ) from exc
        canonical = canonical_json(definition)
        if (
            row["id"] != IMPLEMENTATION_TEMPLATE_ID
            or row["template_key"] != IMPLEMENTATION_TEMPLATE_KEY
            or row["version"] != 1
            or canonical != IMPLEMENTATION_TEMPLATE_CANONICAL
            or content_sha256(canonical) != row["content_sha256"]
            or row["content_sha256"] != IMPLEMENTATION_TEMPLATE_REVISION
            or definition != IMPLEMENTATION_TEMPLATE
        ):
            raise StorageError("stored workflow template definition does not match")
        return {
            "id": row["id"],
            "key": row["template_key"],
            "version": row["version"],
            "revision": row["content_sha256"],
            "built_in": bool(row["built_in"]),
            "definition": definition,
            "created_at": row["template_created_at"],
        }

    @staticmethod
    def _profile_pin(
        connection: sqlite3.Connection, profile_name: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT p.id AS profile_id, p.name, p.active, "
            "v.id AS profile_version_id, v.version AS profile_version, "
            "v.spec_json, v.content_sha256, v.krab_identity_id, "
            "ki.handle AS krab_handle FROM agent_profiles p "
            "JOIN agent_profile_versions v ON v.profile_id = p.id "
            "JOIN krab_identities ki ON ki.id = v.krab_identity_id "
            "WHERE p.name = ? ORDER BY v.version DESC LIMIT 1",
            (profile_name,),
        ).fetchone()
        if row is None or row["active"] != 1:
            raise NotFoundError(f"active agent profile not found: {profile_name}")
        try:
            spec = AgentProfileSpec.from_mapping(json.loads(row["spec_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored agent profile is invalid") from exc
        if spec.digest != row["content_sha256"]:
            raise StorageError("stored agent profile digest does not match")
        return {**dict(row), "spec": spec}

    @staticmethod
    def _validate_roles(
        spec: WorkflowConfigurationSpec,
        producer: dict[str, Any],
        reviewer: dict[str, Any],
    ) -> None:
        if producer["profile_id"] == reviewer["profile_id"]:
            raise ConflictError(
                "workflow producer and reviewer require distinct profiles"
            )
        if producer["spec"].workspace_mode is not WorkspaceMode.COPY_ON_WRITE:
            raise ConflictError(
                "workflow producer profile must use copy-on-write workspace"
            )
        if reviewer["spec"].workspace_mode is not WorkspaceMode.READ_ONLY:
            raise ConflictError(
                "workflow reviewer profile must use a read-only workspace"
            )
        if (
            spec.review_separation
            in {
                ReviewSeparation.DIFFERENT_IDENTITY,
                ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
            }
            and producer["krab_identity_id"] == reviewer["krab_identity_id"]
        ):
            raise ConflictError(
                "workflow producer and reviewer require distinct Krab identities"
            )

    def list_workflow_template_definitions(self) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT id, template_key, version, definition_json, "
                "content_sha256, built_in, created_at AS template_created_at "
                "FROM workflow_template_definitions ORDER BY template_key, version"
            ).fetchall()
        return [self._definition_record(row) for row in rows]

    def _configuration_row(
        self,
        connection: sqlite3.Connection,
        *,
        project_key: str | None = None,
        name: str | None = None,
        version: int | None = None,
        version_id: str | None = None,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT c.id, c.name, c.project_id, c.created_at AS config_created_at, "
            "p.krab_project_key AS project_key, cv.id AS version_id, "
            "cv.version, cv.spec_json, cv.content_sha256, cv.created_at, "
            "creator.handle AS created_by, t.id AS template_id, "
            "t.template_key, t.version AS template_version, "
            "t.definition_json, t.content_sha256 AS template_revision, "
            "t.built_in, t.created_at AS template_created_at, "
            "producer.id AS producer_profile_id, "
            "producer.name AS producer_profile_name, "
            "producer.active AS producer_active, "
            "producer_v.id AS producer_profile_version_id, "
            "producer_v.version AS producer_profile_version, "
            "producer_v.spec_json AS producer_spec_json, "
            "producer_v.content_sha256 AS producer_spec_sha256, "
            "producer_v.krab_identity_id AS producer_identity_id, "
            "producer_ki.handle AS producer_krab_handle, "
            "reviewer.id AS reviewer_profile_id, "
            "reviewer.name AS reviewer_profile_name, "
            "reviewer.active AS reviewer_active, "
            "reviewer_v.id AS reviewer_profile_version_id, "
            "reviewer_v.version AS reviewer_profile_version, "
            "reviewer_v.spec_json AS reviewer_spec_json, "
            "reviewer_v.content_sha256 AS reviewer_spec_sha256, "
            "reviewer_v.krab_identity_id AS reviewer_identity_id, "
            "reviewer_ki.handle AS reviewer_krab_handle "
            "FROM workflow_configurations c "
            "JOIN projects p ON p.id = c.project_id "
            "JOIN workflow_configuration_versions cv "
            "ON cv.configuration_id = c.id "
            "JOIN workflow_template_definitions t "
            "ON t.id = cv.template_definition_id "
            "JOIN users creator ON creator.id = cv.created_by "
            "JOIN agent_profile_versions producer_v "
            "ON producer_v.id = cv.producer_profile_version_id "
            "JOIN agent_profiles producer ON producer.id = producer_v.profile_id "
            "JOIN krab_identities producer_ki "
            "ON producer_ki.id = producer_v.krab_identity_id "
            "JOIN agent_profile_versions reviewer_v "
            "ON reviewer_v.id = cv.reviewer_profile_version_id "
            "JOIN agent_profiles reviewer ON reviewer.id = reviewer_v.profile_id "
            "JOIN krab_identities reviewer_ki "
            "ON reviewer_ki.id = reviewer_v.krab_identity_id WHERE "
            "((? IS NOT NULL AND cv.id = ?) OR "
            "(? IS NULL AND p.krab_project_key = ? AND c.name = ? "
            "AND (? IS NULL OR cv.version = ?))) "
            "ORDER BY cv.version DESC LIMIT 1",
            (
                version_id,
                version_id,
                version_id,
                project_key,
                name,
                version,
                version,
            ),
        ).fetchone()
        if row is None:
            label = version_id if version_id is not None else f"{project_key}/{name}"
            raise NotFoundError(f"workflow configuration not found: {label}")
        return row

    def _configuration_record(
        self, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        try:
            spec = WorkflowConfigurationSpec.from_mapping(json.loads(row["spec_json"]))
            producer_spec = AgentProfileSpec.from_mapping(
                json.loads(row["producer_spec_json"])
            )
            reviewer_spec = AgentProfileSpec.from_mapping(
                json.loads(row["reviewer_spec_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored workflow configuration is invalid") from exc
        if (
            producer_spec.digest != row["producer_spec_sha256"]
            or reviewer_spec.digest != row["reviewer_spec_sha256"]
        ):
            raise StorageError("stored workflow profile digest does not match")
        manifest = self._configuration_manifest(
            spec,
            producer_profile_version_id=row["producer_profile_version_id"],
            reviewer_profile_version_id=row["reviewer_profile_version_id"],
        )
        if content_sha256(canonical_json(manifest)) != row["content_sha256"]:
            raise StorageError("stored workflow configuration digest does not match")
        definition = self._definition_record(
            {
                "id": row["template_id"],
                "template_key": row["template_key"],
                "version": row["template_version"],
                "definition_json": row["definition_json"],
                "content_sha256": row["template_revision"],
                "built_in": row["built_in"],
                "template_created_at": row["template_created_at"],
            }
        )
        definition.pop("definition")
        return {
            "id": row["id"],
            "project_id": row["project_id"],
            "project_key": row["project_key"],
            "name": row["name"],
            "workflow_configuration_version_id": row["version_id"],
            "version": row["version"],
            "content_sha256": row["content_sha256"],
            "template": definition,
            "spec": spec.to_dict(),
            "roles": {
                "producer": {
                    "profile_id": row["producer_profile_id"],
                    "profile_name": row["producer_profile_name"],
                    "profile_version_id": row["producer_profile_version_id"],
                    "profile_version": row["producer_profile_version"],
                    "workspace_mode": producer_spec.workspace_mode.value,
                    "krab_identity_id": row["producer_identity_id"],
                    "krab_handle": row["producer_krab_handle"],
                },
                "reviewer": {
                    "profile_id": row["reviewer_profile_id"],
                    "profile_name": row["reviewer_profile_name"],
                    "profile_version_id": row["reviewer_profile_version_id"],
                    "profile_version": row["reviewer_profile_version"],
                    "workspace_mode": reviewer_spec.workspace_mode.value,
                    "krab_identity_id": row["reviewer_identity_id"],
                    "krab_handle": row["reviewer_krab_handle"],
                },
            },
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "configuration_created_at": row["config_created_at"],
        }

    def create_workflow_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: WorkflowConfigurationSpec,
        producer_profile_name: str,
        reviewer_profile_name: str,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_workflow_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            producer_profile_name=producer_profile_name,
            reviewer_profile_name=reviewer_profile_name,
            created_by=created_by,
            create=True,
        )

    def revise_workflow_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: WorkflowConfigurationSpec,
        producer_profile_name: str,
        reviewer_profile_name: str,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_workflow_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            producer_profile_name=producer_profile_name,
            reviewer_profile_name=reviewer_profile_name,
            created_by=created_by,
            create=False,
        )

    def _write_workflow_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: WorkflowConfigurationSpec,
        producer_profile_name: str,
        reviewer_profile_name: str,
        created_by: str,
        create: bool,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        name = validate_name(name, field="workflow configuration name")
        producer_profile_name = validate_name(
            producer_profile_name, field="producer profile name"
        )
        reviewer_profile_name = validate_name(
            reviewer_profile_name, field="reviewer profile name"
        )
        if not isinstance(spec, WorkflowConfigurationSpec):
            raise ValidationError("spec must be a WorkflowConfigurationSpec")
        configuration_id = self.new_id("wfc")
        version_id = self.new_id("wfv")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="workflow creator")
                project = connection.execute(
                    "SELECT id, active FROM projects WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None or project["active"] != 1:
                    raise NotFoundError(f"active project not found: {project_key}")
                definition = connection.execute(
                    "SELECT id, content_sha256 FROM workflow_template_definitions "
                    "WHERE id = ?",
                    (IMPLEMENTATION_TEMPLATE_ID,),
                ).fetchone()
                if (
                    definition is None
                    or definition["content_sha256"] != IMPLEMENTATION_TEMPLATE_REVISION
                ):
                    raise StorageError("built-in workflow definition is unavailable")
                producer = self._profile_pin(connection, producer_profile_name)
                reviewer = self._profile_pin(connection, reviewer_profile_name)
                self._validate_roles(spec, producer, reviewer)
                if create:
                    version = 1
                    connection.execute(
                        "INSERT INTO workflow_configurations "
                        "(id, project_id, name, created_by, created_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (configuration_id, project["id"], name, created_by, now),
                    )
                else:
                    configuration = connection.execute(
                        "SELECT id FROM workflow_configurations "
                        "WHERE project_id = ? AND name = ?",
                        (project["id"], name),
                    ).fetchone()
                    if configuration is None:
                        raise NotFoundError(
                            f"workflow configuration not found: {project_key}/{name}"
                        )
                    configuration_id = configuration["id"]
                    version = connection.execute(
                        "SELECT COALESCE(MAX(version), 0) + 1 AS version "
                        "FROM workflow_configuration_versions "
                        "WHERE configuration_id = ?",
                        (configuration_id,),
                    ).fetchone()["version"]
                manifest = self._configuration_manifest(
                    spec,
                    producer_profile_version_id=producer["profile_version_id"],
                    reviewer_profile_version_id=reviewer["profile_version_id"],
                )
                connection.execute(
                    "INSERT INTO workflow_configuration_versions "
                    "(id, configuration_id, version, template_definition_id, "
                    "schema_version, spec_json, producer_profile_version_id, "
                    "reviewer_profile_version_id, content_sha256, created_by, "
                    "created_at) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)",
                    (
                        version_id,
                        configuration_id,
                        version,
                        IMPLEMENTATION_TEMPLATE_ID,
                        spec.canonical,
                        producer["profile_version_id"],
                        reviewer["profile_version_id"],
                        content_sha256(canonical_json(manifest)),
                        created_by,
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                message = (
                    "workflow configuration already exists or is invalid"
                    if create
                    else "identical workflow configuration version already exists"
                )
                raise ConflictError(message) from exc
        return self.get_workflow_configuration(project_key, name, version=version)

    def get_workflow_configuration(
        self, project_key: str, name: str, *, version: int | None = None
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        name = validate_name(name, field="workflow configuration name")
        if version is not None:
            self._positive_integer(version, field="version")
        with self._read_snapshot() as connection:
            row = self._configuration_row(
                connection, project_key=project_key, name=name, version=version
            )
            return self._configuration_record(connection, row)

    def list_workflow_configurations(
        self, *, project_key: str | None = None
    ) -> list[dict[str, Any]]:
        if project_key is not None:
            project_key = validate_project_key(project_key)
        with self._read_snapshot() as connection:
            if project_key is None:
                rows = connection.execute(
                    "SELECT c.id, p.krab_project_key AS project_key, c.name "
                    "FROM workflow_configurations c JOIN projects p "
                    "ON p.id = c.project_id ORDER BY p.krab_project_key, c.name"
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT c.id, p.krab_project_key AS project_key, c.name "
                    "FROM workflow_configurations c JOIN projects p "
                    "ON p.id = c.project_id WHERE p.krab_project_key = ? "
                    "ORDER BY c.name",
                    (project_key,),
                ).fetchall()
            return [
                self._configuration_record(
                    connection,
                    self._configuration_row(
                        connection,
                        project_key=row["project_key"],
                        name=row["name"],
                    ),
                )
                for row in rows
            ]

    @staticmethod
    def _start_request(
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
        }

    def start_workflow_execution(
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
        idempotency_key: str,
        requested_by: str,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        configuration_name = validate_name(
            configuration_name, field="workflow configuration name"
        )
        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
        source_revision = self._source_revision(source_revision)
        self._positive_integer(
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
        self._positive_integer(
            expected_thread_generation, field="expected_thread_generation"
        )
        self._positive_integer(expected_thread_version, field="expected_thread_version")
        idempotency_key = self._workflow_idempotency_key(idempotency_key)
        request = self._start_request(
            project_key=project_key,
            configuration_name=configuration_name,
            work_item_id=work_item_id,
            source_revision=source_revision,
            expected_configuration_version=expected_configuration_version,
            expected_context_version_id=expected_context_version_id,
            expected_work_item_version_id=expected_work_item_version_id,
            expected_thread_generation=expected_thread_generation,
            expected_thread_version=expected_thread_version,
        )
        request_json = canonical_json(request)
        request_sha256 = content_sha256(request_json)
        execution_id = self.new_id("wfx")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, requested_by, role="workflow requester")
                existing = connection.execute(
                    "SELECT id, content_sha256 FROM workflow_executions "
                    "WHERE requested_by = ? AND idempotency_key = ?",
                    (requested_by, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != request_sha256:
                        raise ConflictError(
                            "idempotency key already names another workflow execution"
                        )
                    stored_id = existing["id"]
                    connection.commit()
                else:
                    project = connection.execute(
                        "SELECT p.id, p.active, c.id AS context_version_id "
                        "FROM projects p JOIN project_context_versions c "
                        "ON c.project_id = p.id WHERE p.krab_project_key = ? "
                        "ORDER BY c.version DESC LIMIT 1",
                        (project_key,),
                    ).fetchone()
                    if project is None or project["active"] != 1:
                        raise NotFoundError(f"active project not found: {project_key}")
                    if project["context_version_id"] != expected_context_version_id:
                        raise ConcurrentUpdateError("project context version changed")
                    configuration = connection.execute(
                        "SELECT c.id, cv.id AS version_id, cv.version, "
                        "cv.template_definition_id, "
                        "producer.active AS producer_active, "
                        "reviewer.active AS reviewer_active "
                        "FROM workflow_configurations c "
                        "JOIN workflow_configuration_versions cv "
                        "ON cv.configuration_id = c.id "
                        "JOIN agent_profile_versions producer_v "
                        "ON producer_v.id = cv.producer_profile_version_id "
                        "JOIN agent_profiles producer "
                        "ON producer.id = producer_v.profile_id "
                        "JOIN agent_profile_versions reviewer_v "
                        "ON reviewer_v.id = cv.reviewer_profile_version_id "
                        "JOIN agent_profiles reviewer "
                        "ON reviewer.id = reviewer_v.profile_id "
                        "WHERE c.project_id = ? AND c.name = ? "
                        "ORDER BY cv.version DESC LIMIT 1",
                        (project["id"], configuration_name),
                    ).fetchone()
                    if configuration is None:
                        raise NotFoundError(
                            "workflow configuration not found: "
                            f"{project_key}/{configuration_name}"
                        )
                    if configuration["version"] != expected_configuration_version:
                        raise ConflictError("workflow configuration version changed")
                    if (
                        configuration["producer_active"] != 1
                        or configuration["reviewer_active"] != 1
                    ):
                        raise ConflictError("workflow role profile is inactive")
                    work_item = connection.execute(
                        "SELECT wi.project_id, wiv.id AS work_item_version_id, "
                        "wt.id AS work_thread_id, wt.generation, wt.state, "
                        "wt.version AS thread_version FROM work_items wi "
                        "JOIN work_item_versions wiv ON wiv.work_item_id = wi.id "
                        "JOIN work_threads wt ON wt.work_item_id = wi.id "
                        "WHERE wi.id = ? ORDER BY wiv.version DESC, "
                        "wt.generation DESC LIMIT 1",
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
                    active = connection.execute(
                        "SELECT id FROM workflow_executions "
                        "WHERE work_thread_id = ? AND state = 'active'",
                        (work_item["work_thread_id"],),
                    ).fetchone()
                    if active is not None:
                        raise ConflictError(
                            "work thread already has an active workflow"
                        )
                    connection.execute(
                        "INSERT INTO workflow_executions "
                        "(id, project_id, template_definition_id, "
                        "configuration_version_id, context_version_id, work_item_id, "
                        "work_item_version_id, work_thread_id, work_thread_generation, "
                        "source_revision, state, stage, version, request_json, "
                        "content_sha256, idempotency_key, requested_by, created_at, "
                        "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', "
                        "'build', 1, ?, ?, ?, ?, ?, ?)",
                        (
                            execution_id,
                            project["id"],
                            configuration["template_definition_id"],
                            configuration["version_id"],
                            project["context_version_id"],
                            work_item_id,
                            work_item["work_item_version_id"],
                            work_item["work_thread_id"],
                            work_item["generation"],
                            source_revision,
                            request_json,
                            request_sha256,
                            idempotency_key,
                            requested_by,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO workflow_execution_events "
                        "(execution_id, sequence, kind, from_state, to_state, actor, "
                        "metadata_json, created_at) VALUES (?, 1, "
                        "'workflow.started', NULL, 'active', ?, '{}', ?)",
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
                raise ConflictError("workflow execution could not be started") from exc
        return self.get_workflow_execution(stored_id)

    def _execution_record(
        self, connection: sqlite3.Connection, execution_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT e.id, e.project_id, p.krab_project_key AS project_key, "
            "e.template_definition_id, e.configuration_version_id, "
            "cv.configuration_id, cv.version AS configuration_version, "
            "c.name AS configuration_name, e.context_version_id, "
            "pc.version AS context_version, e.work_item_id, "
            "e.work_item_version_id, wiv.version AS work_item_version, "
            "e.work_thread_id, e.work_thread_generation, e.source_revision, "
            "e.state, e.stage, e.version, e.content_sha256, e.cancel_reason, "
            "u.handle AS requested_by, e.created_at, e.updated_at, e.finished_at "
            "FROM workflow_executions e "
            "JOIN projects p ON p.id = e.project_id "
            "JOIN workflow_configuration_versions cv "
            "ON cv.id = e.configuration_version_id "
            "JOIN workflow_configurations c ON c.id = cv.configuration_id "
            "JOIN project_context_versions pc ON pc.id = e.context_version_id "
            "JOIN work_item_versions wiv ON wiv.id = e.work_item_version_id "
            "JOIN users u ON u.id = e.requested_by WHERE e.id = ?",
            (execution_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"workflow execution not found: {execution_id}")
        configuration = self._configuration_record(
            connection,
            self._configuration_row(
                connection, version_id=row["configuration_version_id"]
            ),
        )
        events = connection.execute(
            "SELECT sequence, kind, from_state, to_state, actor.handle AS actor, "
            "metadata_json, event.created_at FROM workflow_execution_events event "
            "JOIN users actor ON actor.id = event.actor "
            "WHERE event.execution_id = ? ORDER BY event.sequence",
            (execution_id,),
        ).fetchall()
        try:
            parsed_events = [
                {**dict(event), "metadata": json.loads(event["metadata_json"])}
                for event in events
            ]
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored workflow event is invalid") from exc
        for event in parsed_events:
            event.pop("metadata_json")
        return {
            **dict(row),
            "template": configuration["template"],
            "configuration_id": configuration["id"],
            "roles": configuration["roles"],
            "spec": configuration["spec"],
            "events": parsed_events,
        }

    def get_workflow_execution(self, execution_id: str) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        with self._read_snapshot() as connection:
            return self._execution_record(connection, execution_id)

    def list_workflow_executions(
        self,
        *,
        project_key: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if project_key is not None:
            project_key = validate_project_key(project_key)
        if state is not None and state not in {"active", "cancelled", "completed"}:
            raise ValidationError("workflow state is invalid")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 500
        ):
            raise ValidationError("limit must be between 1 and 500")
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT e.id FROM workflow_executions e JOIN projects p "
                "ON p.id = e.project_id "
                "WHERE (? IS NULL OR p.krab_project_key = ?) "
                "AND (? IS NULL OR e.state = ?) "
                "ORDER BY e.created_at DESC, e.id DESC LIMIT ?",
                (project_key, project_key, state, state, limit),
            ).fetchall()
            return [self._execution_record(connection, row["id"]) for row in rows]

    def cancel_workflow_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        cancelled_by: str,
        reason: str,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        reason = require_string(reason, field="reason", max_bytes=4096)
        if not reason.strip():
            raise ValidationError("reason must not be blank")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, cancelled_by, role="workflow canceller")
                row = connection.execute(
                    "SELECT state, version FROM workflow_executions WHERE id = ?",
                    (execution_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"workflow execution not found: {execution_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if row["state"] != "active":
                    raise ConflictError(
                        f"workflow execution is not active: {row['state']}"
                    )
                connection.execute(
                    "UPDATE workflow_executions SET state = 'cancelled', "
                    "version = version + 1, cancel_reason = ?, updated_at = ?, "
                    "finished_at = ? WHERE id = ?",
                    (reason, now, now, execution_id),
                )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM workflow_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()["sequence"]
                connection.execute(
                    "INSERT INTO workflow_execution_events "
                    "(execution_id, sequence, kind, from_state, to_state, actor, "
                    "metadata_json, created_at) VALUES (?, ?, "
                    "'workflow.cancelled', 'active', 'cancelled', ?, ?, ?)",
                    (
                        execution_id,
                        sequence,
                        cancelled_by,
                        canonical_json({"reason": reason}),
                        now,
                    ),
                )
                connection.commit()
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow execution could not be cancelled"
                ) from exc
        return self.get_workflow_execution(execution_id)
