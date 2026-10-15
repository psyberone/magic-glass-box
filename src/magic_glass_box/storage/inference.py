from __future__ import annotations

import json
import sqlite3
from typing import Any

from ..domain import ValidationError, validate_provider_name
from ..inference import InferenceConnectorSpec, InferenceGrantSpec
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)


class InferenceStorage:
    def _inference_connector_record(
        self,
        connection: sqlite3.Connection,
        *,
        name: str | None = None,
        connector_id: str | None = None,
    ) -> dict[str, Any]:
        if (name is None) == (connector_id is None):
            raise TypeError("select an inference connector by exactly one identifier")
        row = connection.execute(
            "SELECT c.id, c.name, c.state, c.version, c.schema_version, "
            "c.connector_protocol_version, c.spec_json, c.content_sha256, "
            "c.deployment_sha256, c.revoked_at, "
            "creator.handle AS created_by, revoker.handle AS revoked_by, "
            "c.created_at, c.updated_at FROM inference_connectors c "
            "JOIN users creator ON creator.id = c.created_by "
            "LEFT JOIN users revoker ON revoker.id = c.revoked_by "
            "WHERE (? IS NOT NULL AND c.name = ?) "
            "OR (? IS NOT NULL AND c.id = ?)",
            (name, name, connector_id, connector_id),
        ).fetchone()
        if row is None:
            identifier = name if name is not None else connector_id
            raise NotFoundError(f"inference connector not found: {identifier}")
        result = dict(row)
        try:
            spec = InferenceConnectorSpec.from_mapping(
                json.loads(result.pop("spec_json"))
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored inference connector spec is invalid") from exc
        if spec.digest != result["content_sha256"]:
            raise StorageError("stored inference connector digest does not match")
        result["spec"] = spec.to_dict()
        events = connection.execute(
            "SELECT e.sequence, e.kind, u.handle AS actor, e.created_at "
            "FROM inference_connector_events e "
            "JOIN users u ON u.id = e.actor_id "
            "WHERE e.inference_connector_id = ? ORDER BY e.sequence",
            (result["id"],),
        ).fetchall()
        result["events"] = [dict(event) for event in events]
        return result

    def register_inference_connector(
        self,
        *,
        name: str,
        spec: InferenceConnectorSpec,
        created_by: str,
    ) -> dict[str, Any]:
        name = validate_provider_name(name, field="inference connector name")
        if not isinstance(spec, InferenceConnectorSpec):
            raise ValidationError("spec must be an InferenceConnectorSpec")
        connector_id = self.new_id("ifc")
        now = self.now()
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO inference_connectors "
                    "(id, name, state, version, schema_version, "
                    "connector_protocol_version, spec_json, content_sha256, "
                    "deployment_sha256, transport_kind, socket_path, "
                    "request_bytes, response_bytes, timeout_ms, max_concurrency, "
                    "created_by, created_at, updated_at) "
                    "VALUES (?, ?, 'active', 1, ?, ?, ?, ?, ?, 'unix', ?, "
                    "?, ?, ?, ?, ?, ?, ?)",
                    (
                        connector_id,
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
                        created_by,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO inference_connector_events "
                    "(inference_connector_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, 1, 'inference_connector.created', ?, ?)",
                    (connector_id, created_by, now),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError(
                "inference connector name or spec already exists"
            ) from exc
        return self.get_inference_connector(name)

    def get_inference_connector(self, name: str) -> dict[str, Any]:
        name = validate_provider_name(name, field="inference connector name")
        with self._connection() as connection:
            return self._inference_connector_record(connection, name=name)

    def list_inference_connectors(self) -> list[dict[str, Any]]:
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT id FROM inference_connectors ORDER BY created_at, id"
            ).fetchall()
            return [
                self._inference_connector_record(connection, connector_id=row["id"])
                for row in rows
            ]

    def revoke_inference_connector(
        self,
        name: str,
        *,
        expected_version: int,
        revoked_by: str,
    ) -> dict[str, Any]:
        name = validate_provider_name(name, field="inference connector name")
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 1
        ):
            raise ValidationError("expected_version must be a positive integer")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                current = connection.execute(
                    "SELECT id, state, version FROM inference_connectors "
                    "WHERE name = ?",
                    (name,),
                ).fetchone()
                if current is None:
                    raise NotFoundError(f"inference connector not found: {name}")
                if current["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"inference connector version is {current['version']}, "
                        f"expected {expected_version}"
                    )
                if current["state"] != "active":
                    raise ConflictError("inference connector is already revoked")
                cursor = connection.execute(
                    "UPDATE inference_connectors SET state = 'revoked', "
                    "version = version + 1, revoked_at = ?, revoked_by = ?, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (now, revoked_by, now, current["id"], expected_version),
                )
                if cursor.rowcount != 1:
                    raise ConcurrentUpdateError(
                        "inference connector changed concurrently"
                    )
                connection.execute(
                    "INSERT INTO inference_connector_events "
                    "(inference_connector_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, ?, 'inference_connector.revoked', ?, ?)",
                    (current["id"], expected_version + 1, revoked_by, now),
                )
                connection.commit()
            except ConflictError:
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("inference connector could not be revoked") from exc
        return self.get_inference_connector(name)

    def create_connector_provider_grant(
        self,
        *,
        name: str,
        spec: InferenceGrantSpec,
        expires_at: str | None,
        created_by: str,
    ) -> dict[str, Any]:
        name = validate_provider_name(name, field="provider grant name")
        if not isinstance(spec, InferenceGrantSpec):
            raise ValidationError("spec must be an InferenceGrantSpec")
        if expires_at is not None:
            expires_at = self._timestamp(expires_at, field="expires_at")
        grant_id = self.new_id("pgr")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                connector_row = connection.execute(
                    "SELECT id, state, spec_json, content_sha256 "
                    "FROM inference_connectors WHERE name = ?",
                    (spec.connector_name,),
                ).fetchone()
                if connector_row is None or connector_row["state"] != "active":
                    raise ConflictError("inference connector is unavailable")
                try:
                    connector_spec = InferenceConnectorSpec.from_mapping(
                        json.loads(connector_row["spec_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored inference connector spec is invalid"
                    ) from exc
                if connector_spec.digest != connector_row["content_sha256"]:
                    raise StorageError(
                        "stored inference connector digest does not match"
                    )
                try:
                    connector_spec.validate_grant(spec.connector_name, spec)
                except ValidationError as exc:
                    raise ConflictError(str(exc)) from exc
                connection.execute(
                    "INSERT INTO provider_grants "
                    "(id, name, state, version, policy_json, content_sha256, "
                    "credential_ref, expires_at, created_by, created_at, "
                    "updated_at, grant_kind) "
                    "VALUES (?, ?, 'active', 1, ?, ?, '', ?, ?, ?, ?, 'connector')",
                    (
                        grant_id,
                        name,
                        spec.canonical,
                        spec.digest,
                        expires_at,
                        created_by,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO provider_grant_events "
                    "(provider_grant_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, 1, 'provider_grant.created', ?, ?)",
                    (grant_id, created_by, now),
                )
                connection.execute(
                    "INSERT INTO provider_connector_bindings "
                    "(provider_grant_id, inference_connector_id, "
                    "deployment_sha256, route, credential_scope_sha256, "
                    "provider, protocol, model, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        grant_id,
                        connector_row["id"],
                        spec.deployment_sha256,
                        spec.route,
                        spec.credential_scope_sha256,
                        spec.provider,
                        spec.protocol,
                        spec.model,
                        now,
                    ),
                )
                connection.commit()
            except (ConflictError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "connector provider grant already exists or is invalid"
                ) from exc
        return self.get_provider_grant(name)
