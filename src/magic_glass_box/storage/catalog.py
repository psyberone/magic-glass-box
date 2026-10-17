from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from ..domain import (
    AgentProfileSpec,
    GatewayCapability,
    ProjectContext,
    ProviderGrantSpec,
    ValidationError,
    require_string,
    validate_handle,
    validate_krab_id,
    validate_name,
    validate_project_key,
    validate_provider_name,
)
from ..inference import InferenceGrantSpec
from ..krab import KrabIdentity, validate_base_url
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)


class CatalogStorage:
    def create_user(self, handle: str, display_name: str) -> dict[str, Any]:
        handle = validate_handle(handle)
        display_name = require_string(display_name, field="display_name", max_bytes=256)
        user_id = self.new_id("boxu")
        now = self.now()
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO users "
                    "(id, handle, display_name, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (user_id, handle, display_name, now, now),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError(f"local user already exists: {handle}") from exc
        return self.get_user(handle)

    def get_user(self, handle: str) -> dict[str, Any]:
        handle = validate_handle(handle)
        with self._connection() as connection:
            row = connection.execute(
                "SELECT id, handle, display_name, active, created_at, updated_at "
                "FROM users WHERE handle = ?",
                (handle,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"local user not found: {handle}")
        return dict(row)

    @staticmethod
    def _timestamp(value: object, *, field: str) -> str:
        text = require_string(value, field=field, max_bytes=64)
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValidationError(
                f"{field} must be a valid RFC 3339 timestamp"
            ) from exc
        if parsed.tzinfo is None:
            raise ValidationError(f"{field} must include a timezone")
        return parsed.astimezone(UTC).isoformat(timespec="microseconds")

    @staticmethod
    def _provider_grant_record(
        connection: sqlite3.Connection,
        *,
        name: str | None = None,
        grant_id: str | None = None,
        include_reference: bool = False,
    ) -> dict[str, Any]:
        if (name is None) == (grant_id is None):
            raise TypeError("select a provider grant by exactly one identifier")
        row = connection.execute(
            "SELECT pg.id, pg.name, pg.state, pg.version, pg.policy_json, "
            "pg.content_sha256, pg.credential_ref, pg.grant_kind, pg.expires_at, "
            "pg.revoked_at, creator.handle AS created_by, "
            "revoker.handle AS revoked_by, pg.created_at, pg.updated_at "
            "FROM provider_grants pg "
            "JOIN users creator ON creator.id = pg.created_by "
            "LEFT JOIN users revoker ON revoker.id = pg.revoked_by "
            "WHERE (? IS NOT NULL AND pg.name = ?) "
            "OR (? IS NOT NULL AND pg.id = ?)",
            (name, name, grant_id, grant_id),
        ).fetchone()
        if row is None:
            identifier = name if name is not None else grant_id
            raise NotFoundError(f"provider grant not found: {identifier}")
        result = dict(row)
        try:
            raw_policy = json.loads(result.pop("policy_json"))
            spec = (
                ProviderGrantSpec.from_mapping(raw_policy)
                if result["grant_kind"] == "direct"
                else InferenceGrantSpec.from_mapping(raw_policy)
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored provider grant policy is invalid") from exc
        if spec.digest != result["content_sha256"]:
            raise StorageError("stored provider grant policy digest does not match")
        result["policy"] = spec.to_dict()
        if result["grant_kind"] == "connector":
            result.pop("credential_ref")
            binding = connection.execute(
                "SELECT c.id, c.name, c.state, c.version, "
                "c.connector_protocol_version, "
                "b.deployment_sha256, b.route, b.credential_scope_sha256, "
                "b.provider, b.protocol, b.model "
                "FROM provider_connector_bindings b "
                "JOIN inference_connectors c ON c.id = b.inference_connector_id "
                "WHERE b.provider_grant_id = ?",
                (result["id"],),
            ).fetchone()
            if binding is None:
                raise StorageError("connector provider grant has no binding")
            result["connector"] = dict(binding)
        elif not include_reference:
            result.pop("credential_ref")
        events = connection.execute(
            "SELECT e.sequence, e.kind, u.handle AS actor, e.created_at "
            "FROM provider_grant_events e "
            "JOIN users u ON u.id = e.actor_id "
            "WHERE e.provider_grant_id = ? ORDER BY e.sequence",
            (result["id"],),
        ).fetchall()
        result["events"] = [dict(event) for event in events]
        return result

    def create_provider_grant(
        self,
        *,
        name: str,
        spec: ProviderGrantSpec,
        credential_ref: str,
        expires_at: str | None,
        created_by: str,
    ) -> dict[str, Any]:
        name = validate_provider_name(name, field="provider grant name")
        if not isinstance(spec, ProviderGrantSpec):
            raise ValidationError("spec must be a ProviderGrantSpec")
        credential_ref = require_string(
            credential_ref, field="credential_ref", max_bytes=4096
        )
        if expires_at is not None:
            expires_at = self._timestamp(expires_at, field="expires_at")
        grant_id = self.new_id("pgr")
        now = self.now()
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="provider grant creator")
                connection.execute(
                    "INSERT INTO provider_grants "
                    "(id, name, state, version, policy_json, content_sha256, "
                    "credential_ref, expires_at, created_by, created_at, "
                    "updated_at) VALUES (?, ?, 'active', 1, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        grant_id,
                        name,
                        spec.canonical,
                        spec.digest,
                        credential_ref,
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
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError("provider grant already exists or is invalid") from exc
        return self.get_provider_grant(name)

    def get_provider_grant(self, name: str) -> dict[str, Any]:
        name = validate_provider_name(name, field="provider grant name")
        with self._connection() as connection:
            return self._provider_grant_record(connection, name=name)

    def revoke_provider_grant(
        self,
        name: str,
        *,
        expected_version: int,
        revoked_by: str,
    ) -> dict[str, Any]:
        name = validate_provider_name(name, field="provider grant name")
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
                self._active_user(connection, revoked_by, role="provider grant revoker")
                current = connection.execute(
                    "SELECT id, state, version FROM provider_grants WHERE name = ?",
                    (name,),
                ).fetchone()
                if current is None:
                    raise NotFoundError(f"provider grant not found: {name}")
                if current["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"provider grant version is {current['version']}, "
                        f"expected {expected_version}"
                    )
                if current["state"] != "active":
                    raise ConflictError("provider grant is already revoked")
                cursor = connection.execute(
                    "UPDATE provider_grants SET state = 'revoked', "
                    "version = version + 1, revoked_at = ?, revoked_by = ?, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (now, revoked_by, now, current["id"], expected_version),
                )
                if cursor.rowcount != 1:
                    raise ConcurrentUpdateError("provider grant changed concurrently")
                connection.execute(
                    "INSERT INTO provider_grant_events "
                    "(provider_grant_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, ?, 'provider_grant.revoked', ?, ?)",
                    (current["id"], expected_version + 1, revoked_by, now),
                )
                connection.commit()
            except (NotFoundError, ConflictError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("provider grant could not be revoked") from exc
        return self.get_provider_grant(name)

    def create_krab_instance(self, name: str, base_url: str) -> dict[str, Any]:
        name = validate_name(name, field="instance name")
        _, _, normalized_url = validate_base_url(base_url)
        instance_id = self.new_id("ksi")
        now = self.now()
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO krab_instances "
                    "(id, name, base_url, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (instance_id, name, normalized_url, now, now),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError("Krab instance name or URL already exists") from exc
        return self.get_krab_instance(name)

    def get_krab_instance(self, name: str) -> dict[str, Any]:
        name = validate_name(name, field="instance name")
        with self._connection() as connection:
            row = connection.execute(
                "SELECT id, name, base_url, created_at, updated_at "
                "FROM krab_instances WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"Krab instance not found: {name}")
        return dict(row)

    def store_krab_identity(
        self,
        instance_id: str,
        identity: KrabIdentity,
        credential_ref: str,
        *,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        validate_krab_id(identity.user_id, "usr")
        validate_handle(identity.handle)
        credential_ref = require_string(
            credential_ref, field="credential_ref", max_bytes=4096
        )
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                if user_id is not None:
                    self._active_user(connection, user_id, role="identity owner")
                existing = connection.execute(
                    "SELECT id FROM krab_identities "
                    "WHERE krab_instance_id = ? AND krab_user_id = ?",
                    (instance_id, identity.user_id),
                ).fetchone()
                identity_id = existing["id"] if existing else self.new_id("kid")
                if existing:
                    connection.execute(
                        "UPDATE krab_identities SET handle = ?, token_version = ?, "
                        "credential_ref = ?, verified_at = ?, updated_at = ? "
                        "WHERE id = ?",
                        (
                            identity.handle,
                            identity.token_version,
                            credential_ref,
                            now,
                            now,
                            identity_id,
                        ),
                    )
                else:
                    connection.execute(
                        "INSERT INTO krab_identities "
                        "(id, krab_instance_id, krab_user_id, handle, token_version, "
                        "credential_ref, verified_at, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            identity_id,
                            instance_id,
                            identity.user_id,
                            identity.handle,
                            identity.token_version,
                            credential_ref,
                            now,
                            now,
                            now,
                        ),
                    )
                if user_id is not None:
                    connection.execute(
                        "INSERT OR IGNORE INTO user_krab_bindings "
                        "(user_id, krab_identity_id, created_at) VALUES (?, ?, ?)",
                        (user_id, identity_id, now),
                    )
                    binding = connection.execute(
                        "SELECT user_id FROM user_krab_bindings "
                        "WHERE krab_identity_id = ?",
                        (identity_id,),
                    ).fetchone()
                    if binding is None or binding["user_id"] != user_id:
                        raise ConflictError(
                            "Krab identity is already linked to another local user"
                        )
                connection.commit()
            except ConflictError:
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "Krab identity conflicts with an existing handle or binding"
                ) from exc
        return self.get_krab_identity(instance_id, identity.handle)

    def get_krab_identity(self, instance_id: str, handle: str) -> dict[str, Any]:
        handle = validate_handle(handle)
        with self._connection() as connection:
            row = connection.execute(
                "SELECT ki.id, ki.krab_instance_id, ki.krab_user_id, ki.handle, "
                "ki.token_version, ki.credential_ref, ki.verified_at, "
                "ki.created_at, ki.updated_at, ub.user_id AS local_user_id, "
                "u.active AS local_user_active "
                "FROM krab_identities ki "
                "LEFT JOIN user_krab_bindings ub ON ub.krab_identity_id = ki.id "
                "LEFT JOIN users u ON u.id = ub.user_id "
                "WHERE ki.krab_instance_id = ? AND ki.handle = ?",
                (instance_id, handle),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"Krab identity not found: {handle}")
        return dict(row)

    def create_project(
        self,
        *,
        instance_id: str,
        krab_project_id: str,
        krab_project_key: str,
        display_name: str,
        context: ProjectContext,
        created_by: str,
    ) -> dict[str, Any]:
        krab_project_id = validate_krab_id(krab_project_id, "prj")
        krab_project_key = validate_project_key(krab_project_key)
        display_name = require_string(display_name, field="display_name", max_bytes=256)
        project_id = self.new_id("boxp")
        context_id = self.new_id("ctx")
        now = self.now()
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="project creator")
                connection.execute(
                    "INSERT INTO projects "
                    "(id, krab_instance_id, krab_project_id, krab_project_key, "
                    "display_name, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        project_id,
                        instance_id,
                        krab_project_id,
                        krab_project_key,
                        display_name,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO project_context_versions "
                    "(id, project_id, version, context_json, content_sha256, "
                    "created_by, created_at) VALUES (?, ?, 1, ?, ?, ?, ?)",
                    (
                        context_id,
                        project_id,
                        context.canonical,
                        context.digest,
                        created_by,
                        now,
                    ),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError("project mapping already exists or is invalid") from exc
        return self.get_project(krab_project_key)

    def get_project(self, project_key: str) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        with self._connection() as connection:
            row = connection.execute(
                "SELECT p.id, p.krab_instance_id, p.krab_project_id, "
                "p.krab_project_key, p.display_name, p.active, p.created_at, "
                "p.updated_at, c.id AS context_version_id, "
                "c.version AS context_version, c.context_json, c.content_sha256 "
                "FROM projects p JOIN project_context_versions c "
                "ON c.project_id = p.id "
                "WHERE p.krab_project_key = ? "
                "ORDER BY c.version DESC LIMIT 1",
                (project_key,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"project not found: {project_key}")
        result = dict(row)
        result["context"] = json.loads(result.pop("context_json"))
        return result

    def add_project_context(
        self, project_key: str, context: ProjectContext, created_by: str
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        now = self.now()
        context_id = self.new_id("ctx")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="context creator")
                project = connection.execute(
                    "SELECT id FROM projects WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None:
                    raise NotFoundError(f"project not found: {project_key}")
                latest = connection.execute(
                    "SELECT COALESCE(MAX(version), 0) AS version "
                    "FROM project_context_versions WHERE project_id = ?",
                    (project["id"],),
                ).fetchone()["version"]
                connection.execute(
                    "INSERT INTO project_context_versions "
                    "(id, project_id, version, context_json, content_sha256, "
                    "created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        context_id,
                        project["id"],
                        latest + 1,
                        context.canonical,
                        context.digest,
                        created_by,
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE projects SET updated_at = ? WHERE id = ?",
                    (now, project["id"]),
                )
                connection.commit()
            except NotFoundError:
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("identical project context already exists") from exc
        return self.get_project(project_key)

    def _available_provider_grants(
        self,
        connection: sqlite3.Connection,
        spec: AgentProfileSpec,
    ) -> list[sqlite3.Row]:
        if GatewayCapability.PROVIDER_INVOKE not in spec.gateway_capabilities:
            return []
        result: list[sqlite3.Row] = []
        now = self.now()
        for name in spec.credential_names:
            if name == "krab":
                continue
            row = connection.execute(
                "SELECT pg.id, pg.name, pg.state, pg.grant_kind, "
                "pg.expires_at, ic.state AS connector_state "
                "FROM provider_grants pg "
                "LEFT JOIN provider_connector_bindings pcb "
                "ON pcb.provider_grant_id = pg.id "
                "LEFT JOIN inference_connectors ic "
                "ON ic.id = pcb.inference_connector_id "
                "WHERE pg.name = ?",
                (name,),
            ).fetchone()
            if (
                row is None
                or row["state"] != "active"
                or row["grant_kind"] != "connector"
                or row["connector_state"] != "active"
                or (row["expires_at"] is not None and row["expires_at"] <= now)
            ):
                raise ConflictError(f"provider grant is unavailable: {name}")
            result.append(row)
        return result

    def create_agent_profile(
        self,
        *,
        name: str,
        spec: AgentProfileSpec,
        krab_identity_id: str,
        created_by: str,
    ) -> dict[str, Any]:
        name = validate_name(name, field="profile name")
        profile_id = self.new_id("agp")
        version_id = self.new_id("apv")
        now = self.now()
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="profile creator")
                self._available_provider_grants(connection, spec)
                connection.execute(
                    "INSERT INTO agent_profiles "
                    "(id, name, created_by, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (profile_id, name, created_by, now, now),
                )
                connection.execute(
                    "INSERT INTO agent_profile_versions "
                    "(id, profile_id, version, spec_json, content_sha256, "
                    "krab_identity_id, created_by, created_at) "
                    "VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
                    (
                        version_id,
                        profile_id,
                        spec.canonical,
                        spec.digest,
                        krab_identity_id,
                        created_by,
                        now,
                    ),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError("agent profile already exists or is invalid") from exc
        return self.get_agent_profile(name)

    def get_agent_profile(self, name: str) -> dict[str, Any]:
        name = validate_name(name, field="profile name")
        with self._connection() as connection:
            row = connection.execute(
                "SELECT p.id, p.name, p.active, p.created_at, p.updated_at, "
                "v.id AS profile_version_id, v.version AS profile_version, "
                "v.spec_json, v.content_sha256, v.krab_identity_id, "
                "ki.handle AS krab_handle, ki.krab_user_id "
                "FROM agent_profiles p JOIN agent_profile_versions v "
                "ON v.profile_id = p.id JOIN krab_identities ki "
                "ON ki.id = v.krab_identity_id "
                "WHERE p.name = ? ORDER BY v.version DESC LIMIT 1",
                (name,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"agent profile not found: {name}")
        result = dict(row)
        result["spec"] = json.loads(result.pop("spec_json"))
        return result

    def add_agent_profile_version(
        self,
        *,
        name: str,
        spec: AgentProfileSpec,
        krab_identity_id: str,
        created_by: str,
    ) -> dict[str, Any]:
        name = validate_name(name, field="profile name")
        now = self.now()
        version_id = self.new_id("apv")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="profile creator")
                profile = connection.execute(
                    "SELECT id FROM agent_profiles WHERE name = ?", (name,)
                ).fetchone()
                if profile is None:
                    raise NotFoundError(f"agent profile not found: {name}")
                latest = connection.execute(
                    "SELECT COALESCE(MAX(version), 0) AS version "
                    "FROM agent_profile_versions WHERE profile_id = ?",
                    (profile["id"],),
                ).fetchone()["version"]
                self._available_provider_grants(connection, spec)
                connection.execute(
                    "INSERT INTO agent_profile_versions "
                    "(id, profile_id, version, spec_json, content_sha256, "
                    "krab_identity_id, created_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        version_id,
                        profile["id"],
                        latest + 1,
                        spec.canonical,
                        spec.digest,
                        krab_identity_id,
                        created_by,
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE agent_profiles SET updated_at = ? WHERE id = ?",
                    (now, profile["id"]),
                )
                connection.commit()
            except NotFoundError:
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "identical agent profile version already exists"
                ) from exc
        return self.get_agent_profile(name)
