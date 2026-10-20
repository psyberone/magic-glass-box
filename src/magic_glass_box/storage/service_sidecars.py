from __future__ import annotations

import json
import math
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from ..domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    require_string,
    validate_project_key,
    validate_provider_name,
)
from ..service_sidecar import (
    CAPABILITY_BINDING_ID_RE,
    SERVICE_OPERATION_ID_RE,
    ServiceOperationReceipt,
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceOperationSpec,
    ServiceSidecarSpec,
    core_service_capability,
)
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)

MIN_SERVICE_OPERATION_LEASE_SECONDS = 5
MAX_SERVICE_OPERATION_LEASE_SECONDS = 10 * 60
DEFAULT_SERVICE_OPERATION_RETRY_SECONDS = 5
MAX_SERVICE_OPERATION_HISTORY = 1000


def _positive_version(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValidationError(f"{field} must be a positive integer")
    return value


def _operation_id(value: object) -> str:
    if not isinstance(value, str) or SERVICE_OPERATION_ID_RE.fullmatch(value) is None:
        raise ValidationError("service operation id is invalid")
    return value


def _capability_binding_id(value: object) -> str:
    if not isinstance(value, str) or CAPABILITY_BINDING_ID_RE.fullmatch(value) is None:
        raise ValidationError("project capability binding id is invalid")
    return value


def _owner_id(value: object) -> str:
    text = require_string(value, field="service operation owner", max_bytes=128)
    if not text.strip():
        raise ValidationError("service operation owner must not be blank")
    return text


class ServiceSidecarStorage:
    def _service_sidecar_record(
        self,
        connection: sqlite3.Connection,
        *,
        name: str | None = None,
        sidecar_id: str | None = None,
    ) -> dict[str, Any]:
        if (name is None) == (sidecar_id is None):
            raise TypeError("select a service sidecar by exactly one identifier")
        row = connection.execute(
            "SELECT s.id, s.name, s.state, s.version, s.schema_version, "
            "s.connector_protocol_version, s.spec_json, s.content_sha256, "
            "s.deployment_sha256, s.revoked_at, "
            "creator.handle AS created_by, revoker.handle AS revoked_by, "
            "s.created_at, s.updated_at FROM service_sidecars s "
            "JOIN users creator ON creator.id = s.created_by "
            "LEFT JOIN users revoker ON revoker.id = s.revoked_by "
            "WHERE (? IS NOT NULL AND s.name = ?) "
            "OR (? IS NOT NULL AND s.id = ?)",
            (name, name, sidecar_id, sidecar_id),
        ).fetchone()
        if row is None:
            identifier = name if name is not None else sidecar_id
            raise NotFoundError(f"service sidecar not found: {identifier}")
        result = dict(row)
        try:
            spec = ServiceSidecarSpec.from_mapping(json.loads(result.pop("spec_json")))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored service sidecar spec is invalid") from exc
        if spec.digest != result["content_sha256"]:
            raise StorageError("stored service sidecar digest does not match")
        result["spec"] = spec.to_dict()
        events = connection.execute(
            "SELECT e.sequence, e.kind, u.handle AS actor, e.created_at "
            "FROM service_sidecar_events e "
            "JOIN users u ON u.id = e.actor_id "
            "WHERE e.service_sidecar_id = ? ORDER BY e.sequence",
            (result["id"],),
        ).fetchall()
        result["events"] = [dict(event) for event in events]
        return result

    def register_service_sidecar(
        self,
        *,
        name: str,
        spec: ServiceSidecarSpec,
        created_by: str,
    ) -> dict[str, Any]:
        from ..domain import validate_provider_name

        name = validate_provider_name(name, field="service sidecar name")
        if not isinstance(spec, ServiceSidecarSpec):
            raise ValidationError("spec must be a ServiceSidecarSpec")
        sidecar_id = self.new_id("ssc")
        now = self.now()
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="sidecar creator")
                connection.execute(
                    "INSERT INTO service_sidecars "
                    "(id, name, state, version, schema_version, "
                    "connector_protocol_version, spec_json, content_sha256, "
                    "deployment_sha256, transport_kind, socket_path, "
                    "request_bytes, response_bytes, timeout_ms, max_concurrency, "
                    "max_requests_per_minute, created_by, created_at, updated_at) "
                    "VALUES (?, ?, 'active', 1, ?, ?, ?, ?, ?, 'unix', ?, "
                    "?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        sidecar_id,
                        name,
                        spec.schema_version,
                        spec.connector_protocol_version,
                        spec.canonical,
                        spec.digest,
                        spec.deployment_sha256,
                        spec.transport.socket_path,
                        spec.request_bytes,
                        spec.response_bytes,
                        spec.timeout_ms,
                        spec.max_concurrency,
                        spec.max_requests_per_minute,
                        created_by,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO service_sidecar_events "
                    "(service_sidecar_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, 1, 'service_sidecar.created', ?, ?)",
                    (sidecar_id, created_by, now),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError("service sidecar name or spec already exists") from exc
        return self.get_service_sidecar(name)

    def get_service_sidecar(self, name: str) -> dict[str, Any]:
        from ..domain import validate_provider_name

        name = validate_provider_name(name, field="service sidecar name")
        with self._connection() as connection:
            return self._service_sidecar_record(connection, name=name)

    def list_service_sidecars(self) -> list[dict[str, Any]]:
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT id FROM service_sidecars ORDER BY created_at, id"
            ).fetchall()
            return [
                self._service_sidecar_record(connection, sidecar_id=row["id"])
                for row in rows
            ]

    def revoke_service_sidecar(
        self,
        name: str,
        *,
        expected_version: int,
        revoked_by: str,
    ) -> dict[str, Any]:
        from ..domain import validate_provider_name

        name = validate_provider_name(name, field="service sidecar name")
        expected_version = _positive_version(expected_version, field="expected_version")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, revoked_by, role="sidecar revoker")
                current = connection.execute(
                    "SELECT id, state, version FROM service_sidecars WHERE name = ?",
                    (name,),
                ).fetchone()
                if current is None:
                    raise NotFoundError(f"service sidecar not found: {name}")
                if current["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"service sidecar version is {current['version']}, "
                        f"expected {expected_version}"
                    )
                if current["state"] != "active":
                    raise ConflictError("service sidecar is already revoked")
                changed = connection.execute(
                    "UPDATE service_sidecars SET state = 'revoked', "
                    "version = version + 1, revoked_at = ?, revoked_by = ?, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (now, revoked_by, now, current["id"], expected_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("service sidecar changed concurrently")
                connection.execute(
                    "INSERT INTO service_sidecar_events "
                    "(service_sidecar_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, ?, 'service_sidecar.revoked', ?, ?)",
                    (current["id"], expected_version + 1, revoked_by, now),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ConcurrentUpdateError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("service sidecar could not be revoked") from exc
        return self.get_service_sidecar(name)

    def _project_capability_binding_record(
        self,
        connection: sqlite3.Connection,
        binding_id: str,
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT b.id, b.state, b.version, b.capability_name, "
            "b.capability_version, b.capability_mode, "
            "b.sidecar_content_sha256 AS pinned_sidecar_content_sha256, "
            "b.deployment_sha256 AS pinned_deployment_sha256, b.mapping_name, "
            "b.created_at, b.updated_at, b.unbound_at, "
            "p.id AS project_id, p.krab_project_key AS project_key, "
            "p.active AS project_active, s.id AS sidecar_id, "
            "s.name AS sidecar_name, s.state AS sidecar_state, "
            "s.version AS sidecar_version, s.connector_protocol_version, "
            "s.spec_json AS sidecar_spec_json, "
            "s.content_sha256 AS sidecar_content_sha256, "
            "creator.handle AS bound_by, unbinder.handle AS unbound_by "
            "FROM project_capability_bindings b "
            "JOIN projects p ON p.id = b.project_id "
            "JOIN service_sidecars s ON s.id = b.service_sidecar_id "
            "JOIN users creator ON creator.id = b.bound_by "
            "LEFT JOIN users unbinder ON unbinder.id = b.unbound_by "
            "WHERE b.id = ?",
            (binding_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"project capability binding not found: {binding_id}")
        sidecar_spec = self._parse_stored_sidecar(row)
        try:
            capability = core_service_capability(
                row["capability_name"], row["capability_version"]
            )
        except ValidationError as exc:
            raise StorageError(
                "stored project capability binding is not core-defined"
            ) from exc
        if capability.mode != row["capability_mode"]:
            raise StorageError("stored project capability mode does not match")
        advertised = next(
            (
                item
                for item in sidecar_spec.capabilities
                if (item.name, item.version) == (capability.name, capability.version)
            ),
            None,
        )
        if advertised != capability:
            raise StorageError(
                "stored project capability is not advertised by its sidecar"
            )
        mapping = next(
            (
                item
                for item in sidecar_spec.mappings
                if item.name == row["mapping_name"]
            ),
            None,
        )
        if mapping is None:
            raise StorageError("stored project capability mapping is unavailable")
        if (
            row["pinned_sidecar_content_sha256"] != row["sidecar_content_sha256"]
            or row["pinned_sidecar_content_sha256"] != sidecar_spec.digest
            or row["pinned_deployment_sha256"] != sidecar_spec.deployment_sha256
        ):
            raise StorageError("stored project capability controller does not match")
        events = connection.execute(
            "SELECT e.sequence, e.kind, u.handle AS actor, e.created_at "
            "FROM project_capability_binding_events e "
            "JOIN users u ON u.id = e.actor_id "
            "WHERE e.binding_id = ? ORDER BY e.sequence",
            (binding_id,),
        ).fetchall()
        return {
            "id": row["id"],
            "state": row["state"],
            "version": row["version"],
            "project": {
                "id": row["project_id"],
                "key": row["project_key"],
                "active": row["project_active"],
            },
            "capability": capability.to_dict(),
            "controller": {
                "sidecar_id": row["sidecar_id"],
                "sidecar_name": row["sidecar_name"],
                "sidecar_state": row["sidecar_state"],
                "sidecar_version": row["sidecar_version"],
                "connector_protocol_version": row["connector_protocol_version"],
                "sidecar_content_sha256": row["pinned_sidecar_content_sha256"],
                "deployment_sha256": row["pinned_deployment_sha256"],
                "mapping": mapping.to_dict(),
            },
            "bound_by": row["bound_by"],
            "unbound_by": row["unbound_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "unbound_at": row["unbound_at"],
            "events": [dict(event) for event in events],
        }

    def bind_project_capability(
        self,
        *,
        project_key: str,
        capability_name: str,
        capability_version: int,
        sidecar_name: str,
        mapping_name: str,
        bound_by: str,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        capability = core_service_capability(capability_name, capability_version)
        sidecar_name = validate_provider_name(
            sidecar_name, field="service sidecar name"
        )
        mapping_name = validate_provider_name(
            mapping_name, field="service mapping name"
        )
        binding_id = self.new_id("cpb")
        selected_id: str | None = None
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, bound_by, role="capability binder")
                project = connection.execute(
                    "SELECT id, active FROM projects WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None:
                    raise NotFoundError(f"project not found: {project_key}")
                if not project["active"]:
                    raise ConflictError("project capability is unavailable")
                sidecar = connection.execute(
                    "SELECT id, state, spec_json AS sidecar_spec_json, "
                    "content_sha256 AS sidecar_content_sha256 "
                    "FROM service_sidecars WHERE name = ?",
                    (sidecar_name,),
                ).fetchone()
                if sidecar is None or sidecar["state"] != "active":
                    raise ConflictError("service sidecar is unavailable")
                sidecar_spec = self._parse_stored_sidecar(sidecar)
                advertised = next(
                    (
                        item
                        for item in sidecar_spec.capabilities
                        if (item.name, item.version)
                        == (capability.name, capability.version)
                    ),
                    None,
                )
                if advertised != capability:
                    raise ConflictError(
                        "service sidecar does not advertise the core capability"
                    )
                mapping = next(
                    (
                        item
                        for item in sidecar_spec.mappings
                        if item.name == mapping_name
                    ),
                    None,
                )
                if mapping is None:
                    raise ConflictError("service sidecar mapping is unavailable")
                existing = connection.execute(
                    "SELECT id, service_sidecar_id, sidecar_content_sha256, "
                    "deployment_sha256, mapping_name, capability_mode "
                    "FROM project_capability_bindings WHERE project_id = ? "
                    "AND capability_name = ? AND capability_version = ? "
                    "AND state = 'active'",
                    (project["id"], capability.name, capability.version),
                ).fetchone()
                if existing is not None:
                    exact = (
                        existing["service_sidecar_id"] == sidecar["id"]
                        and existing["sidecar_content_sha256"] == sidecar_spec.digest
                        and existing["deployment_sha256"]
                        == sidecar_spec.deployment_sha256
                        and existing["mapping_name"] == mapping.name
                        and existing["capability_mode"] == capability.mode
                    )
                    if not exact:
                        raise ConflictError(
                            "project capability is already bound to another controller"
                        )
                    selected_id = existing["id"]
                    connection.commit()
                else:
                    connection.execute(
                        "INSERT INTO project_capability_bindings "
                        "(id, project_id, capability_name, capability_version, "
                        "capability_mode, service_sidecar_id, "
                        "sidecar_content_sha256, deployment_sha256, mapping_name, "
                        "state, version, bound_by, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, ?, ?)",
                        (
                            binding_id,
                            project["id"],
                            capability.name,
                            capability.version,
                            capability.mode,
                            sidecar["id"],
                            sidecar_spec.digest,
                            sidecar_spec.deployment_sha256,
                            mapping.name,
                            bound_by,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO project_capability_binding_events "
                        "(binding_id, sequence, kind, actor_id, created_at) "
                        "VALUES (?, 1, 'capability.bound', ?, ?)",
                        (binding_id, bound_by, now),
                    )
                    selected_id = binding_id
                    connection.commit()
            except (
                NotFoundError,
                ConflictError,
                ConcurrentUpdateError,
                StorageError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("project capability could not be bound") from exc
        assert selected_id is not None
        return self.get_project_capability_binding(selected_id)

    def get_project_capability_binding(self, binding_id: str) -> dict[str, Any]:
        binding_id = _capability_binding_id(binding_id)
        with self._connection() as connection:
            return self._project_capability_binding_record(connection, binding_id)

    def list_project_capability_bindings(
        self,
        *,
        project_key: str | None = None,
        include_unbound: bool = False,
    ) -> list[dict[str, Any]]:
        if not isinstance(include_unbound, bool):
            raise ValidationError("include_unbound must be a boolean")
        if project_key is not None:
            project_key = validate_project_key(project_key)
        with self._read_snapshot() as connection:
            project_id: str | None = None
            if project_key is not None:
                project = connection.execute(
                    "SELECT id FROM projects WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None:
                    raise NotFoundError(f"project not found: {project_key}")
                project_id = project["id"]
            rows = connection.execute(
                "SELECT id FROM project_capability_bindings "
                "WHERE (? IS NULL OR project_id = ?) "
                "AND (? = 1 OR state = 'active') "
                "ORDER BY created_at, id",
                (project_id, project_id, int(include_unbound)),
            ).fetchall()
            return [
                self._project_capability_binding_record(connection, row["id"])
                for row in rows
            ]

    def resolve_project_capability(
        self,
        project_key: str,
        capability_name: str,
        capability_version: int,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        capability = core_service_capability(capability_name, capability_version)
        with self._read_snapshot() as connection:
            row = connection.execute(
                "SELECT b.id, p.active AS project_active, "
                "s.state AS sidecar_state "
                "FROM projects p LEFT JOIN project_capability_bindings b "
                "ON b.project_id = p.id AND b.capability_name = ? "
                "AND b.capability_version = ? AND b.state = 'active' "
                "LEFT JOIN service_sidecars s ON s.id = b.service_sidecar_id "
                "WHERE p.krab_project_key = ?",
                (capability.name, capability.version, project_key),
            ).fetchone()
            if (
                row is None
                or not row["project_active"]
                or row["id"] is None
                or row["sidecar_state"] != "active"
            ):
                raise ConflictError("project capability is unavailable")
            return self._project_capability_binding_record(connection, row["id"])

    def unbind_project_capability(
        self,
        binding_id: str,
        *,
        expected_version: int,
        unbound_by: str,
    ) -> dict[str, Any]:
        binding_id = _capability_binding_id(binding_id)
        expected_version = _positive_version(expected_version, field="expected_version")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, unbound_by, role="capability unbinder")
                current = connection.execute(
                    "SELECT state, version FROM project_capability_bindings "
                    "WHERE id = ?",
                    (binding_id,),
                ).fetchone()
                if current is None:
                    raise NotFoundError(
                        f"project capability binding not found: {binding_id}"
                    )
                if current["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"project capability binding version is "
                        f"{current['version']}, expected {expected_version}"
                    )
                if current["state"] != "active":
                    raise ConflictError("project capability is already unbound")
                changed = connection.execute(
                    "UPDATE project_capability_bindings SET state = 'unbound', "
                    "version = version + 1, unbound_by = ?, unbound_at = ?, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (unbound_by, now, now, binding_id, expected_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "project capability binding changed concurrently"
                    )
                connection.execute(
                    "INSERT INTO project_capability_binding_events "
                    "(binding_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, 2, 'capability.unbound', ?, ?)",
                    (binding_id, unbound_by, now),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ConcurrentUpdateError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("project capability could not be unbound") from exc
        return self.get_project_capability_binding(binding_id)

    @staticmethod
    def _parse_stored_sidecar(row: sqlite3.Row) -> ServiceSidecarSpec:
        try:
            spec = ServiceSidecarSpec.from_mapping(json.loads(row["sidecar_spec_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored service sidecar spec is invalid") from exc
        if spec.digest != row["sidecar_content_sha256"]:
            raise StorageError("stored service sidecar digest does not match")
        return spec

    @staticmethod
    def _parse_stored_operation(row: sqlite3.Row) -> ServiceOperationSpec:
        try:
            spec = ServiceOperationSpec.from_mapping(
                json.loads(row["operation_spec_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored service operation spec is invalid") from exc
        if spec.digest != row["operation_content_sha256"]:
            raise StorageError("stored service operation digest does not match")
        if spec.input_sha256 != row["input_sha256"]:
            raise StorageError("stored service operation input digest does not match")
        return spec

    def _service_operation_record(
        self, connection: sqlite3.Connection, operation_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT o.id, o.state, o.version, o.schema_version, "
            "o.spec_json AS operation_spec_json, "
            "o.content_sha256 AS operation_content_sha256, o.input_sha256, "
            "o.created_at, o.updated_at, o.eligible_at, o.completed_at, "
            "u.handle AS created_by, s.id AS sidecar_id, s.name AS sidecar_name, "
            "s.state AS sidecar_state, s.version AS sidecar_version, "
            "s.connector_protocol_version, s.spec_json AS sidecar_spec_json, "
            "s.content_sha256 AS sidecar_content_sha256, "
            "ob.project_capability_binding_id "
            "FROM service_operations o "
            "JOIN service_sidecars s ON s.id = o.service_sidecar_id "
            "JOIN users u ON u.id = o.created_by "
            "LEFT JOIN service_operation_capability_bindings ob "
            "ON ob.operation_id = o.id WHERE o.id = ?",
            (operation_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"service operation not found: {operation_id}")
        sidecar_spec = self._parse_stored_sidecar(row)
        operation_spec = self._parse_stored_operation(row)
        result = {
            "id": row["id"],
            "state": row["state"],
            "version": row["version"],
            "schema_version": row["schema_version"],
            "content_sha256": row["operation_content_sha256"],
            "input_sha256": row["input_sha256"],
            "project_capability_binding_id": row["project_capability_binding_id"],
            "spec": operation_spec.to_dict(),
            "sidecar": {
                "id": row["sidecar_id"],
                "name": row["sidecar_name"],
                "state": row["sidecar_state"],
                "version": row["sidecar_version"],
                "connector_protocol_version": row["connector_protocol_version"],
                "content_sha256": row["sidecar_content_sha256"],
                "spec": sidecar_spec.to_dict(),
            },
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "eligible_at": row["eligible_at"],
            "completed_at": row["completed_at"],
        }
        attempts = connection.execute(
            "SELECT id, sequence, phase, outcome, owner_id, deadline, "
            "lease_expires_at, request_bytes, response_bytes, provider_status, "
            "error_code, retry_after_seconds, receipt_json, receipt_sha256, "
            "started_at, finished_at FROM service_operation_attempts "
            "WHERE operation_id = ? ORDER BY sequence",
            (operation_id,),
        ).fetchall()
        result["attempts"] = []
        for attempt_row in attempts:
            attempt = dict(attempt_row)
            raw_receipt = attempt.pop("receipt_json")
            receipt_sha256 = attempt.pop("receipt_sha256")
            if raw_receipt is None:
                if receipt_sha256 is not None:
                    raise StorageError("stored service operation receipt is incomplete")
                attempt["receipt"] = None
            else:
                try:
                    receipt = ServiceOperationReceipt.from_mapping(
                        json.loads(raw_receipt)
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored service operation receipt is invalid"
                    ) from exc
                if content_sha256(canonical_json(receipt.to_dict())) != receipt_sha256:
                    raise StorageError(
                        "stored service operation receipt digest does not match"
                    )
                attempt["receipt"] = receipt.to_dict()
            result["attempts"].append(attempt)
        return result

    def _create_service_operation_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        spec: ServiceOperationSpec,
        created_by: str,
        project_capability_binding_id: str | None,
        operation_id: str,
        now: str,
    ) -> str:
        """Create and pin an operation inside its caller's authority transaction."""

        sidecar_row = connection.execute(
            "SELECT id, state, spec_json AS sidecar_spec_json, "
            "content_sha256 AS sidecar_content_sha256 "
            "FROM service_sidecars WHERE name = ?",
            (spec.connector_name,),
        ).fetchone()
        if sidecar_row is None or sidecar_row["state"] != "active":
            raise ConflictError("service sidecar is unavailable")
        sidecar_spec = self._parse_stored_sidecar(sidecar_row)
        try:
            sidecar_spec.validate_operation(spec.connector_name, spec)
        except ValidationError as exc:
            raise ConflictError(str(exc)) from exc
        if project_capability_binding_id is not None:
            binding = connection.execute(
                "SELECT b.state, b.capability_name, "
                "b.capability_version, b.capability_mode, "
                "b.service_sidecar_id, b.sidecar_content_sha256, "
                "b.deployment_sha256, b.mapping_name, "
                "p.active AS project_active "
                "FROM project_capability_bindings b "
                "JOIN projects p ON p.id = b.project_id WHERE b.id = ?",
                (project_capability_binding_id,),
            ).fetchone()
            if (
                binding is None
                or binding["state"] != "active"
                or not binding["project_active"]
            ):
                raise ConflictError("project capability is unavailable")
            binding_authorizes = (
                binding["service_sidecar_id"] == sidecar_row["id"]
                and binding["sidecar_content_sha256"] == sidecar_spec.digest
                and binding["deployment_sha256"] == sidecar_spec.deployment_sha256
                and binding["mapping_name"] == spec.mapping.name
                and binding["capability_name"] == spec.capability.name
                and binding["capability_version"] == spec.capability.version
                and binding["capability_mode"] == spec.capability.mode
            )
            if not binding_authorizes:
                raise ConflictError(
                    "project capability binding does not authorize operation"
                )
        existing = connection.execute(
            "SELECT o.id, o.content_sha256, ob.project_capability_binding_id "
            "FROM service_operations o "
            "LEFT JOIN service_operation_capability_bindings ob "
            "ON ob.operation_id = o.id "
            "WHERE o.service_sidecar_id = ? AND o.idempotency_key = ?",
            (sidecar_row["id"], spec.idempotency_key),
        ).fetchone()
        if existing is not None:
            if (
                existing["content_sha256"] != spec.digest
                or existing["project_capability_binding_id"]
                != project_capability_binding_id
            ):
                raise ConflictError(
                    "service operation idempotency key was reused with changes"
                )
            return existing["id"]
        connection.execute(
            "INSERT INTO service_operations "
            "(id, service_sidecar_id, state, version, schema_version, "
            "spec_json, content_sha256, sidecar_content_sha256, "
            "deployment_sha256, mapping_name, service, host, repository, "
            "account, credential_scope_sha256, capability_name, "
            "capability_version, capability_mode, idempotency_key, "
            "input_sha256, request_bytes, response_bytes, timeout_ms, "
            "created_by, created_at, updated_at) "
            "VALUES (?, ?, 'pending', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                operation_id,
                sidecar_row["id"],
                spec.schema_version,
                spec.canonical,
                spec.digest,
                sidecar_spec.digest,
                spec.deployment_sha256,
                spec.mapping.name,
                spec.mapping.service,
                spec.mapping.host,
                spec.mapping.repository,
                spec.mapping.account,
                spec.mapping.credential_scope_sha256,
                spec.capability.name,
                spec.capability.version,
                spec.capability.mode,
                spec.idempotency_key,
                spec.input_sha256,
                spec.request_bytes,
                spec.response_bytes,
                spec.timeout_ms,
                created_by,
                now,
                now,
            ),
        )
        if project_capability_binding_id is not None:
            connection.execute(
                "INSERT INTO service_operation_capability_bindings "
                "(operation_id, project_capability_binding_id, created_at) "
                "VALUES (?, ?, ?)",
                (operation_id, project_capability_binding_id, now),
            )
        return operation_id

    def create_service_operation(
        self,
        *,
        spec: ServiceOperationSpec,
        created_by: str,
        project_capability_binding_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(spec, ServiceOperationSpec):
            raise ValidationError("spec must be a ServiceOperationSpec")
        if project_capability_binding_id is not None:
            project_capability_binding_id = _capability_binding_id(
                project_capability_binding_id
            )
        operation_id = self.new_id("sop")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="operation creator")
                selected_id = self._create_service_operation_in_transaction(
                    connection,
                    spec=spec,
                    created_by=created_by,
                    project_capability_binding_id=project_capability_binding_id,
                    operation_id=operation_id,
                    now=now,
                )
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("service operation could not be created") from exc
        return self.get_service_operation(selected_id)

    def get_service_operation(self, operation_id: str) -> dict[str, Any]:
        operation_id = _operation_id(operation_id)
        with self._connection() as connection:
            return self._service_operation_record(connection, operation_id)

    def list_service_operations(self, *, limit: int = 100) -> list[dict[str, Any]]:
        limit = _bounded_limit(limit)
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT id FROM service_operations "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [
                self._service_operation_record(connection, row["id"]) for row in rows
            ]

    @staticmethod
    def _expire_service_operation_attempts(
        connection: sqlite3.Connection,
        *,
        sidecar_id: str,
        now: str,
    ) -> None:
        expired = connection.execute(
            "SELECT a.id, a.operation_id, o.capability_mode "
            "FROM service_operation_attempts a "
            "JOIN service_operations o ON o.id = a.operation_id "
            "WHERE o.service_sidecar_id = ? AND a.outcome = 'dispatching' "
            "AND a.lease_expires_at <= ?",
            (sidecar_id, now),
        ).fetchall()
        for attempt in expired:
            retryable = attempt["capability_mode"] == "read"
            retry_after = DEFAULT_SERVICE_OPERATION_RETRY_SECONDS if retryable else None
            eligible_at = (
                (
                    datetime.fromisoformat(now)
                    + timedelta(seconds=DEFAULT_SERVICE_OPERATION_RETRY_SECONDS)
                ).isoformat(timespec="microseconds")
                if retryable
                else None
            )
            connection.execute(
                "UPDATE service_operation_attempts SET outcome = 'abandoned', "
                "error_code = 'lease_expired', retry_after_seconds = ?, "
                "finished_at = ? WHERE id = ? "
                "AND outcome = 'dispatching'",
                (retry_after, now, attempt["id"]),
            )
            connection.execute(
                "UPDATE service_operations SET state = ?, "
                "version = version + 1, eligible_at = ?, updated_at = ? "
                "WHERE id = ? AND state = 'dispatching'",
                (
                    "retryable" if retryable else "uncertain",
                    eligible_at,
                    now,
                    attempt["operation_id"],
                ),
            )

    def begin_service_operation_attempt(
        self,
        operation_id: str,
        *,
        phase: str,
        owner_id: str,
        expected_version: int,
        lease_seconds: int,
    ) -> dict[str, Any]:
        operation_id = _operation_id(operation_id)
        owner_id = _owner_id(owner_id)
        expected_version = _positive_version(expected_version, field="expected_version")
        if phase not in {"invoke", "reconcile"}:
            raise ValidationError("service operation phase is invalid")
        lease_seconds = _lease_seconds(lease_seconds)
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat(timespec="microseconds")
        attempt_id = self.new_id("soa")
        request: ServiceOperationRequest | None = None
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT o.id, o.state, o.version, o.eligible_at, "
                    "o.capability_mode, "
                    "o.spec_json AS operation_spec_json, "
                    "o.content_sha256 AS operation_content_sha256, o.input_sha256, "
                    "s.id AS sidecar_id, s.state AS sidecar_state, "
                    "s.connector_protocol_version, "
                    "s.spec_json AS sidecar_spec_json, "
                    "s.content_sha256 AS sidecar_content_sha256 "
                    "FROM service_operations o "
                    "JOIN service_sidecars s ON s.id = o.service_sidecar_id "
                    "WHERE o.id = ?",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"service operation not found: {operation_id}")
                self._expire_service_operation_attempts(
                    connection, sidecar_id=row["sidecar_id"], now=now
                )
                row = connection.execute(
                    "SELECT o.id, o.state, o.version, o.eligible_at, "
                    "o.capability_mode, "
                    "o.spec_json AS operation_spec_json, "
                    "o.content_sha256 AS operation_content_sha256, o.input_sha256, "
                    "s.id AS sidecar_id, s.state AS sidecar_state, "
                    "s.connector_protocol_version, "
                    "s.spec_json AS sidecar_spec_json, "
                    "s.content_sha256 AS sidecar_content_sha256 "
                    "FROM service_operations o "
                    "JOIN service_sidecars s ON s.id = o.service_sidecar_id "
                    "WHERE o.id = ?",
                    (operation_id,),
                ).fetchone()
                assert row is not None
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"service operation version is {row['version']}, "
                        f"expected {expected_version}"
                    )
                if row["sidecar_state"] != "active":
                    raise ConflictError("service sidecar is unavailable")
                sidecar_spec = self._parse_stored_sidecar(row)
                operation_spec = self._parse_stored_operation(row)
                if phase == "invoke":
                    if row["state"] not in {"pending", "retryable", "throttled"}:
                        raise ConflictError(
                            f"service operation cannot be invoked from {row['state']}"
                        )
                    if row["eligible_at"] is not None and row["eligible_at"] > now:
                        raise ConflictError("service operation is not eligible yet")
                else:
                    if operation_spec.capability.mode == "read":
                        raise ConflictError(
                            "a read operation cannot enter reconciliation"
                        )
                    if row["state"] != "uncertain":
                        raise ConflictError(
                            "only an uncertain service operation can be reconciled"
                        )
                try:
                    sidecar_spec.validate_operation(
                        operation_spec.connector_name, operation_spec
                    )
                except ValidationError as exc:
                    raise StorageError(
                        "stored service operation authority is invalid"
                    ) from exc
                minimum_lease = math.ceil(operation_spec.timeout_ms / 1000) + 1
                if lease_seconds < minimum_lease:
                    raise ValidationError(
                        "service operation lease must outlive its transport timeout"
                    )
                active = connection.execute(
                    "SELECT COUNT(*) FROM service_operation_attempts a "
                    "JOIN service_operations o ON o.id = a.operation_id "
                    "WHERE o.service_sidecar_id = ? AND a.outcome = 'dispatching' "
                    "AND a.lease_expires_at > ?",
                    (row["sidecar_id"], now),
                ).fetchone()[0]
                if active >= sidecar_spec.max_concurrency:
                    raise ConflictError("service sidecar concurrency is exhausted")
                cutoff = (now_dt - timedelta(minutes=1)).isoformat(
                    timespec="microseconds"
                )
                recent = connection.execute(
                    "SELECT COUNT(*) FROM service_operation_attempts a "
                    "JOIN service_operations o ON o.id = a.operation_id "
                    "WHERE o.service_sidecar_id = ? AND a.started_at > ?",
                    (row["sidecar_id"], cutoff),
                ).fetchone()[0]
                if recent >= sidecar_spec.max_requests_per_minute:
                    raise ConflictError("service sidecar rate limit is exhausted")
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM service_operation_attempts WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()[0]
                deadline = now_dt + timedelta(milliseconds=operation_spec.timeout_ms)
                deadline_text = deadline.isoformat(timespec="microseconds")
                lease_expires_at = (
                    now_dt + timedelta(seconds=lease_seconds)
                ).isoformat(timespec="microseconds")
                changed = connection.execute(
                    "UPDATE service_operations SET state = 'dispatching', "
                    "version = version + 1, eligible_at = NULL, updated_at = ? "
                    "WHERE id = ? AND version = ?",
                    (now, operation_id, expected_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "service operation changed concurrently"
                    )
                connection.execute(
                    "INSERT INTO service_operation_attempts "
                    "(id, operation_id, sequence, phase, outcome, owner_id, "
                    "deadline, lease_expires_at, request_bytes, response_bytes, "
                    "started_at) VALUES (?, ?, ?, ?, 'dispatching', ?, ?, ?, 0, 0, ?)",
                    (
                        attempt_id,
                        operation_id,
                        sequence,
                        phase,
                        owner_id,
                        deadline_text,
                        lease_expires_at,
                        now,
                    ),
                )
                request = ServiceOperationRequest.create(
                    operation_id=operation_id,
                    connector_id=row["sidecar_id"],
                    connector_protocol_version=row["connector_protocol_version"],
                    operation=operation_spec,
                    phase=phase,
                    deadline=deadline_text,
                )
                connection.commit()
            except (
                NotFoundError,
                ConflictError,
                ConcurrentUpdateError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "service operation attempt could not begin"
                ) from exc
        assert request is not None
        operation = self.get_service_operation(operation_id)
        attempt = next(
            item for item in operation["attempts"] if item["id"] == attempt_id
        )
        return {
            "attempt": attempt,
            "operation": operation,
            "request": request.to_dict(),
        }

    def _active_attempt(
        self,
        connection: sqlite3.Connection,
        *,
        attempt_id: str,
        owner_id: str,
        expected_operation_version: int,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT a.id, a.operation_id, a.phase, a.outcome, a.owner_id, "
            "a.deadline, o.state AS operation_state, "
            "o.version AS operation_version, o.capability_mode, "
            "o.spec_json AS operation_spec_json, "
            "o.content_sha256 AS operation_content_sha256, o.input_sha256, "
            "s.id AS sidecar_id, s.connector_protocol_version, "
            "s.spec_json AS sidecar_spec_json, "
            "s.content_sha256 AS sidecar_content_sha256 "
            "FROM service_operation_attempts a "
            "JOIN service_operations o ON o.id = a.operation_id "
            "JOIN service_sidecars s ON s.id = o.service_sidecar_id "
            "WHERE a.id = ?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"service operation attempt not found: {attempt_id}")
        if row["operation_version"] != expected_operation_version:
            raise ConcurrentUpdateError(
                f"service operation version is {row['operation_version']}, "
                f"expected {expected_operation_version}"
            )
        if row["owner_id"] != owner_id:
            raise ConflictError("service operation attempt belongs to another owner")
        if row["outcome"] != "dispatching" or row["operation_state"] != "dispatching":
            raise ConflictError("service operation attempt is not dispatching")
        return row

    def complete_service_operation_attempt(
        self,
        attempt_id: str,
        *,
        owner_id: str,
        expected_operation_version: int,
        response: ServiceOperationResponse,
    ) -> dict[str, Any]:
        if not isinstance(response, ServiceOperationResponse):
            raise ValidationError("response must be a ServiceOperationResponse")
        owner_id = _owner_id(owner_id)
        expected_operation_version = _positive_version(
            expected_operation_version, field="expected_operation_version"
        )
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat(timespec="microseconds")
        operation_id: str | None = None
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = self._active_attempt(
                    connection,
                    attempt_id=attempt_id,
                    owner_id=owner_id,
                    expected_operation_version=expected_operation_version,
                )
                operation_id = row["operation_id"]
                operation_spec = self._parse_stored_operation(row)
                request = ServiceOperationRequest.create(
                    operation_id=operation_id,
                    connector_id=row["sidecar_id"],
                    connector_protocol_version=row["connector_protocol_version"],
                    operation=operation_spec,
                    phase=row["phase"],
                    deadline=row["deadline"],
                )
                response.validate_correlation(request)
                state = response.outcome
                eligible_at = None
                completed_at = None
                if response.outcome == "not_found":
                    state = "pending"
                elif response.outcome in {"retryable", "throttled"}:
                    delay = (
                        response.retry_after_seconds
                        or DEFAULT_SERVICE_OPERATION_RETRY_SECONDS
                    )
                    eligible_at = (now_dt + timedelta(seconds=delay)).isoformat(
                        timespec="microseconds"
                    )
                elif response.outcome in {"succeeded", "denied", "conflict"}:
                    completed_at = now
                receipt_json = None
                receipt_sha256 = None
                if response.receipt is not None:
                    receipt_json = canonical_json(response.receipt.to_dict())
                    receipt_sha256 = content_sha256(receipt_json)
                connection.execute(
                    "UPDATE service_operation_attempts SET outcome = ?, "
                    "request_bytes = ?, response_bytes = ?, provider_status = ?, "
                    "error_code = ?, retry_after_seconds = ?, receipt_json = ?, "
                    "receipt_sha256 = ?, finished_at = ? WHERE id = ? "
                    "AND outcome = 'dispatching'",
                    (
                        response.outcome,
                        response.request_bytes,
                        response.response_bytes,
                        response.provider_status,
                        response.error_code,
                        response.retry_after_seconds,
                        receipt_json,
                        receipt_sha256,
                        now,
                        attempt_id,
                    ),
                )
                changed = connection.execute(
                    "UPDATE service_operations SET state = ?, "
                    "version = version + 1, eligible_at = ?, completed_at = ?, "
                    "updated_at = ? WHERE id = ? AND version = ? "
                    "AND state = 'dispatching'",
                    (
                        state,
                        eligible_at,
                        completed_at,
                        now,
                        operation_id,
                        expected_operation_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "service operation changed concurrently"
                    )
                connection.commit()
            except (
                NotFoundError,
                ConflictError,
                ConcurrentUpdateError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "service operation attempt could not complete"
                ) from exc
        assert operation_id is not None
        return self.get_service_operation(operation_id)

    def fail_service_operation_attempt(
        self,
        attempt_id: str,
        *,
        owner_id: str,
        expected_operation_version: int,
        outcome: str,
        error_code: str,
    ) -> dict[str, Any]:
        if outcome not in {"unavailable", "ambiguous"}:
            raise ValidationError("service operation transport outcome is invalid")
        from ..domain import validate_provider_name

        error_code = validate_provider_name(error_code, field="error_code")
        owner_id = _owner_id(owner_id)
        expected_operation_version = _positive_version(
            expected_operation_version, field="expected_operation_version"
        )
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat(timespec="microseconds")
        operation_id: str | None = None
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = self._active_attempt(
                    connection,
                    attempt_id=attempt_id,
                    owner_id=owner_id,
                    expected_operation_version=expected_operation_version,
                )
                operation_id = row["operation_id"]
                retryable = outcome == "unavailable" or (
                    outcome == "ambiguous" and row["capability_mode"] == "read"
                )
                state = "retryable" if retryable else "uncertain"
                retry_after = (
                    DEFAULT_SERVICE_OPERATION_RETRY_SECONDS if retryable else None
                )
                eligible_at = (
                    (now_dt + timedelta(seconds=retry_after)).isoformat(
                        timespec="microseconds"
                    )
                    if retry_after is not None
                    else None
                )
                connection.execute(
                    "UPDATE service_operation_attempts SET outcome = ?, "
                    "error_code = ?, retry_after_seconds = ?, finished_at = ? "
                    "WHERE id = ? AND outcome = 'dispatching'",
                    (outcome, error_code, retry_after, now, attempt_id),
                )
                changed = connection.execute(
                    "UPDATE service_operations SET state = ?, "
                    "version = version + 1, eligible_at = ?, updated_at = ? "
                    "WHERE id = ? AND version = ? AND state = 'dispatching'",
                    (
                        state,
                        eligible_at,
                        now,
                        operation_id,
                        expected_operation_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "service operation changed concurrently"
                    )
                connection.commit()
            except (NotFoundError, ConflictError, ConcurrentUpdateError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "service operation transport failure could not be recorded"
                ) from exc
        assert operation_id is not None
        return self.get_service_operation(operation_id)


def _lease_seconds(value: object) -> int:
    return _bounded_integer(
        value,
        field="lease_seconds",
        minimum=MIN_SERVICE_OPERATION_LEASE_SECONDS,
        maximum=MAX_SERVICE_OPERATION_LEASE_SECONDS,
    )


def _bounded_limit(value: object) -> int:
    return _bounded_integer(
        value,
        field="limit",
        minimum=1,
        maximum=MAX_SERVICE_OPERATION_HISTORY,
    )


def _bounded_integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return value
