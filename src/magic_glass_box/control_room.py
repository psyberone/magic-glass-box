from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from collections.abc import Callable
from contextlib import closing
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from .credentials import (
    CredentialError,
    CredentialResolver,
    PrivateFileCredentialResolver,
)
from .db import SCHEMA_VERSION, Database, StorageError
from .domain import (
    AgentProfileSpec,
    ProviderGrantSpec,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
)
from .krab import KrabClient, KrabError, WorkClaim
from .results import ResultError, ResultManifest

ClientFactory = Callable[[str, str], KrabClient]
ACTIVE_RUN_STATES = {"queued", "preparing", "running", "waiting_for_input"}
ACTIVE_ATTEMPT_STATES = {"preparing", "running"}


def _timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise StorageError(f"stored {label} timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise StorageError(f"stored {label} timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise StorageError(f"stored {label} timestamp is invalid")
    return parsed.astimezone(UTC)


def _work_item(row: sqlite3.Row, *, prefix: str = "") -> WorkItemEnvelope:
    try:
        content = json.loads(row[prefix + "content_json"])
        envelope = WorkItemEnvelope.from_mapping(
            {
                **content,
                "source": row[prefix + "source"],
                "external_id": row[prefix + "external_id"],
                "source_updated_at": row[prefix + "version_source_updated_at"],
            }
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValidationError) as exc:
        raise StorageError("stored work item is invalid") from exc
    if envelope.digest != row[prefix + "content_sha256"]:
        raise StorageError("stored work item content digest does not match")
    return envelope


class ControlRoom:
    """Build a redacted operational view without retaining private content."""

    def __init__(
        self,
        database: Database,
        *,
        credential_resolver: CredentialResolver | None = None,
        client_factory: ClientFactory | None = None,
    ) -> None:
        self.database = database
        self.credential_resolver = (
            credential_resolver or PrivateFileCredentialResolver()
        )
        self.client_factory = client_factory or KrabClient

    def snapshot(
        self,
        *,
        history_limit: int = 25,
        refresh_krab: bool = False,
    ) -> dict[str, Any]:
        if (
            isinstance(history_limit, bool)
            or not isinstance(history_limit, int)
            or not 0 <= history_limit <= 500
        ):
            raise ValueError("history_limit must be an integer between 0 and 500")
        if not isinstance(refresh_krab, bool):
            raise ValueError("refresh_krab must be boolean")
        generated_at = self.database.now()
        with closing(self.database.connect()) as connection:
            try:
                connection.execute("BEGIN")
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                if version != SCHEMA_VERSION:
                    raise StorageError(
                        f"database schema {version} is not supported; "
                        f"expected {SCHEMA_VERSION}"
                    )
                local, routes = self._local_snapshot(
                    connection,
                    generated_at=generated_at,
                    history_limit=history_limit,
                )
                connection.commit()
            except StorageError:
                connection.rollback()
                raise
            except sqlite3.Error as exc:
                connection.rollback()
                raise StorageError(f"cannot read control-room snapshot: {exc}") from exc

        local["krab"] = (
            self._refresh_krab(local, routes)
            if refresh_krab
            else {
                "status": "not_refreshed",
                "refreshed_at": None,
                "roles": [],
                "work_claims": [],
                "errors": [],
            }
        )
        local["summary"]["attention"] = len(local["attention"])
        return local

    def _local_snapshot(
        self,
        connection: sqlite3.Connection,
        *,
        generated_at: str,
        history_limit: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        users, instances, routes = self._identities(connection)
        projects = self._projects(connection)
        profiles = self._profiles(connection)
        provider_grants = self._provider_grants(connection, generated_at)
        work_items = self._work_items(connection)
        assignments = self._assignments(connection, generated_at)
        runs, terminal_total = self._runs(connection, history_limit)
        run_ids = [run["id"] for run in runs]
        attempts = self._attempts(connection, run_ids, generated_at)
        interactions = self._interactions(connection, run_ids)
        retained_results = self._results(connection, run_ids)
        authorities = self._run_authorities(connection, run_ids, generated_at)
        self._attach_run_counts(
            runs, attempts, interactions, retained_results, authorities
        )
        summary = self._summary(connection, provider_grants, attempts)
        attention = self._attention(
            work_items,
            assignments,
            runs,
            attempts,
            interactions,
            authorities,
        )
        return (
            {
                "snapshot": {
                    "database": str(self.database.path),
                    "schema_version": SCHEMA_VERSION,
                    "generated_at": generated_at,
                    "consistency": "single_sqlite_read_transaction",
                    "history_limit": history_limit,
                    "terminal_history_truncated": terminal_total > history_limit,
                },
                "summary": summary,
                "attention": attention,
                "users": users,
                "krab_instances": instances,
                "projects": projects,
                "profiles": profiles,
                "provider_grants": provider_grants,
                "work_items": work_items,
                "assignments": assignments,
                "runs": runs,
                "attempts": attempts,
                "interactions": interactions,
                "retained_results": retained_results,
                "run_authorities": authorities,
            },
            routes,
        )

    @staticmethod
    def _identities(
        connection: sqlite3.Connection,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        user_rows = connection.execute(
            "SELECT id, handle, display_name, active, created_at, updated_at "
            "FROM users ORDER BY handle"
        ).fetchall()
        identity_rows = connection.execute(
            "SELECT ki.id, ki.krab_instance_id, ksi.name AS instance_name, "
            "ksi.base_url, ki.krab_user_id, ki.handle, ki.token_version, "
            "ki.credential_ref, ki.verified_at, ki.created_at, ki.updated_at, "
            "ub.user_id AS local_user_id "
            "FROM krab_identities ki JOIN krab_instances ksi "
            "ON ksi.id = ki.krab_instance_id LEFT JOIN user_krab_bindings ub "
            "ON ub.krab_identity_id = ki.id ORDER BY ksi.name, ki.handle"
        ).fetchall()
        by_user: dict[str, list[str]] = defaultdict(list)
        identities_by_instance: dict[str, list[dict[str, Any]]] = defaultdict(list)
        routes: list[dict[str, Any]] = []
        for row in identity_rows:
            if row["local_user_id"] is not None:
                by_user[row["local_user_id"]].append(row["id"])
            public = {
                "id": row["id"],
                "krab_user_id": row["krab_user_id"],
                "handle": row["handle"],
                "token_version": row["token_version"],
                "local_user_id": row["local_user_id"],
                "verified_at": row["verified_at"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            identities_by_instance[row["krab_instance_id"]].append(public)
            routes.append(
                {
                    **public,
                    "krab_instance_id": row["krab_instance_id"],
                    "instance_name": row["instance_name"],
                    "base_url": row["base_url"],
                    "credential_ref": row["credential_ref"],
                }
            )
        users = [
            {
                **dict(row),
                "krab_identity_ids": by_user.get(row["id"], []),
            }
            for row in user_rows
        ]
        instance_rows = connection.execute(
            "SELECT id, name, base_url, created_at, updated_at "
            "FROM krab_instances ORDER BY name"
        ).fetchall()
        instances = [
            {
                **dict(row),
                "identities": identities_by_instance.get(row["id"], []),
            }
            for row in instance_rows
        ]
        return users, instances, routes

    @staticmethod
    def _projects(connection: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = connection.execute(
            "SELECT p.id, p.krab_project_id, p.krab_project_key AS project_key, "
            "p.display_name, p.active, p.created_at, p.updated_at, "
            "ksi.id AS krab_instance_id, ksi.name AS krab_instance_name, "
            "ksi.base_url AS krab_base_url, c.id AS context_version_id, "
            "c.version AS context_version, c.content_sha256 AS context_sha256 "
            "FROM projects p JOIN krab_instances ksi "
            "ON ksi.id = p.krab_instance_id JOIN project_context_versions c "
            "ON c.project_id = p.id WHERE c.version = "
            "(SELECT MAX(latest.version) FROM project_context_versions latest "
            "WHERE latest.project_id = p.id) ORDER BY p.krab_project_key"
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _profiles(connection: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = connection.execute(
            "SELECT ap.id, ap.name, ap.active, ap.created_at, ap.updated_at, "
            "av.id AS profile_version_id, av.version AS profile_version, "
            "av.spec_json, av.content_sha256, ki.id AS krab_identity_id, "
            "ki.krab_user_id, ki.handle AS krab_handle, "
            "ksi.name AS krab_instance_name "
            "FROM agent_profiles ap JOIN agent_profile_versions av "
            "ON av.profile_id = ap.id JOIN krab_identities ki "
            "ON ki.id = av.krab_identity_id JOIN krab_instances ksi "
            "ON ksi.id = ki.krab_instance_id WHERE av.version = "
            "(SELECT MAX(latest.version) FROM agent_profile_versions latest "
            "WHERE latest.profile_id = ap.id) ORDER BY ap.name"
        ).fetchall()
        profiles: list[dict[str, Any]] = []
        for row in rows:
            try:
                spec = AgentProfileSpec.from_mapping(json.loads(row["spec_json"]))
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError("stored agent profile is invalid") from exc
            if spec.digest != row["content_sha256"]:
                raise StorageError("stored agent profile digest does not match")
            profiles.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "active": row["active"],
                    "profile_version_id": row["profile_version_id"],
                    "profile_version": row["profile_version"],
                    "content_sha256": row["content_sha256"],
                    "image": spec.image,
                    "command_argv_count": len(spec.command),
                    "runtime_adapter": spec.runtime_adapter,
                    "runtime_provider_grant": spec.runtime_provider_grant,
                    "workspace_mode": spec.workspace_mode.value,
                    "network_mode": spec.network_mode.value,
                    "credential_names": list(spec.credential_names),
                    "gateway_capabilities": [
                        capability.value for capability in spec.gateway_capabilities
                    ],
                    "krab_identity_id": row["krab_identity_id"],
                    "krab_user_id": row["krab_user_id"],
                    "krab_handle": row["krab_handle"],
                    "krab_instance_name": row["krab_instance_name"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )
        return profiles

    @staticmethod
    def _provider_grants(
        connection: sqlite3.Connection, generated_at: str
    ) -> list[dict[str, Any]]:
        now = _timestamp(generated_at, label="snapshot")
        rows = connection.execute(
            "SELECT pg.id, pg.name, pg.state, pg.version, pg.policy_json, "
            "pg.content_sha256, pg.expires_at, pg.revoked_at, "
            "creator.handle AS created_by, revoker.handle AS revoked_by, "
            "pg.created_at, pg.updated_at FROM provider_grants pg "
            "JOIN users creator ON creator.id = pg.created_by "
            "LEFT JOIN users revoker ON revoker.id = pg.revoked_by "
            "ORDER BY pg.name"
        ).fetchall()
        grants: list[dict[str, Any]] = []
        for row in rows:
            try:
                spec = ProviderGrantSpec.from_mapping(json.loads(row["policy_json"]))
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError("stored provider grant policy is invalid") from exc
            if spec.digest != row["content_sha256"]:
                raise StorageError("stored provider grant policy digest does not match")
            expired = (
                row["expires_at"] is not None
                and _timestamp(row["expires_at"], label="provider expiry") <= now
            )
            grants.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "state": row["state"],
                    "version": row["version"],
                    "available": row["state"] == "active" and not expired,
                    "expired": expired,
                    "expires_at": row["expires_at"],
                    "revoked_at": row["revoked_at"],
                    "revoked_by": row["revoked_by"],
                    "created_by": row["created_by"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "content_sha256": row["content_sha256"],
                    "policy": {
                        "provider": spec.provider,
                        "adapter": spec.adapter,
                        "destination": urlunsplit(
                            (*urlsplit(spec.destination)[:3], "", "")
                        ),
                        "destination_query_redacted": bool(
                            urlsplit(spec.destination).query
                        ),
                        "method": spec.method,
                        "model": spec.model,
                        "credential_header": spec.credential_header,
                        "static_header_names": sorted(
                            name for name, _value in spec.static_headers
                        ),
                        "request_bytes": spec.request_bytes,
                        "response_bytes": spec.response_bytes,
                        "timeout_ms": spec.timeout_ms,
                        "max_concurrency": spec.max_concurrency,
                    },
                }
            )
        return grants

    @staticmethod
    def _work_items(connection: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = connection.execute(
            "SELECT wi.id, p.krab_project_key AS project_key, wi.source, "
            "wi.external_id, wi.source_updated_at, wi.created_at, wi.updated_at, "
            "wiv.id AS work_item_version_id, wiv.version, wiv.content_json, "
            "wiv.content_sha256, wiv.source_updated_at AS version_source_updated_at, "
            "wt.id AS work_thread_id, wt.generation, wt.state, "
            "wt.version AS thread_version, wt.log_sequence, wt.completed_at, "
            "(SELECT COUNT(*) FROM work_log_entries wle "
            "WHERE wle.work_thread_id = wt.id AND wle.kind = 'blocker') "
            "AS blocker_entries, "
            "(SELECT MAX(wle.created_at) FROM work_log_entries wle "
            "WHERE wle.work_thread_id = wt.id AND wle.kind = 'blocker') "
            "AS last_blocker_at, "
            "(SELECT COUNT(*) FROM runs r WHERE r.work_thread_id = wt.id) "
            "AS run_count, "
            "(SELECT COUNT(*) FROM runs r WHERE r.work_thread_id = wt.id "
            "AND r.state IN ('queued','preparing','running','waiting_for_input')) "
            "AS active_run_count "
            "FROM work_items wi JOIN projects p ON p.id = wi.project_id "
            "JOIN work_item_versions wiv ON wiv.work_item_id = wi.id "
            "JOIN work_threads wt ON wt.work_item_id = wi.id "
            "WHERE wiv.version = (SELECT MAX(v.version) FROM work_item_versions v "
            "WHERE v.work_item_id = wi.id) AND wt.generation = "
            "(SELECT MAX(t.generation) FROM work_threads t "
            "WHERE t.work_item_id = wi.id) "
            "ORDER BY CASE wt.state WHEN 'active' THEN 0 ELSE 1 END, "
            "wi.updated_at DESC, wi.id"
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            envelope = _work_item(row)
            items.append(
                {
                    "id": row["id"],
                    "project_key": row["project_key"],
                    "source": row["source"],
                    "external_id": row["external_id"],
                    "source_updated_at": row["source_updated_at"],
                    "work_item_version_id": row["work_item_version_id"],
                    "version": row["version"],
                    "content_sha256": row["content_sha256"],
                    "untrusted_title": envelope.title,
                    "work_thread_id": row["work_thread_id"],
                    "generation": row["generation"],
                    "state": row["state"],
                    "thread_version": row["thread_version"],
                    "log_sequence": row["log_sequence"],
                    "blocker_entries": row["blocker_entries"],
                    "last_blocker_at": row["last_blocker_at"],
                    "run_count": row["run_count"],
                    "active_run_count": row["active_run_count"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "completed_at": row["completed_at"],
                }
            )
        return items

    @staticmethod
    def _assignments(
        connection: sqlite3.Connection, generated_at: str
    ) -> list[dict[str, Any]]:
        now = _timestamp(generated_at, label="snapshot")
        rows = connection.execute(
            "SELECT wa.id, wa.sequence, wa.spec_json, wa.content_sha256, "
            "wa.state, wa.version, wa.created_by_run_id, "
            "wa.created_by_attempt_id, wa.claimed_by_run_id, "
            "wa.claimed_by_attempt_id, wa.lease_expires_at, wa.result_entry_id, "
            "wa.created_at, wa.updated_at, wa.completed_at, "
            "wt.id AS work_thread_id, wt.generation, wt.state AS thread_state, "
            "wi.id AS work_item_id, p.krab_project_key AS project_key, "
            "ap.name AS target_profile_name, "
            "av.id AS target_profile_version_id, av.version AS target_profile_version "
            "FROM work_assignments wa JOIN work_threads wt "
            "ON wt.id = wa.work_thread_id JOIN work_items wi "
            "ON wi.id = wt.work_item_id JOIN projects p ON p.id = wi.project_id "
            "JOIN agent_profile_versions av "
            "ON av.id = wa.target_profile_version_id JOIN agent_profiles ap "
            "ON ap.id = av.profile_id ORDER BY CASE wa.state "
            "WHEN 'claimed' THEN 0 WHEN 'open' THEN 1 ELSE 2 END, "
            "wa.updated_at DESC, wa.id"
        ).fetchall()
        assignments: list[dict[str, Any]] = []
        for row in rows:
            try:
                spec = WorkAssignmentSpec.from_mapping(json.loads(row["spec_json"]))
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError("stored work assignment is invalid") from exc
            if spec.digest != row["content_sha256"]:
                raise StorageError("stored work assignment digest does not match")
            lease_expired = (
                row["lease_expires_at"] is not None
                and _timestamp(row["lease_expires_at"], label="assignment lease") <= now
            )
            assignments.append(
                {
                    "id": row["id"],
                    "sequence": row["sequence"],
                    "work_thread_id": row["work_thread_id"],
                    "work_item_id": row["work_item_id"],
                    "project_key": row["project_key"],
                    "generation": row["generation"],
                    "thread_state": row["thread_state"],
                    "untrusted_title": spec.title,
                    "scope_path_count": len(spec.scope_paths),
                    "acceptance_check_count": len(spec.acceptance_checks),
                    "base_revision": spec.base_revision,
                    "content_sha256": row["content_sha256"],
                    "target_profile_name": row["target_profile_name"],
                    "target_profile_version_id": row["target_profile_version_id"],
                    "target_profile_version": row["target_profile_version"],
                    "state": row["state"],
                    "version": row["version"],
                    "created_by_run_id": row["created_by_run_id"],
                    "created_by_attempt_id": row["created_by_attempt_id"],
                    "claimed_by_run_id": row["claimed_by_run_id"],
                    "claimed_by_attempt_id": row["claimed_by_attempt_id"],
                    "lease_expires_at": row["lease_expires_at"],
                    "lease_expired": lease_expired,
                    "result_entry_id": row["result_entry_id"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "completed_at": row["completed_at"],
                }
            )
        return assignments

    @staticmethod
    def _runs(
        connection: sqlite3.Connection, history_limit: int
    ) -> tuple[list[dict[str, Any]], int]:
        terminal_total = connection.execute(
            "SELECT COUNT(*) FROM runs WHERE state IN "
            "('succeeded','failed','cancelled')"
        ).fetchone()[0]
        rows = connection.execute(
            "WITH terminal AS (SELECT id FROM runs WHERE state IN "
            "('succeeded','failed','cancelled') "
            "ORDER BY updated_at DESC, id LIMIT ?), visible AS ("
            "SELECT id, 0 AS group_order FROM runs WHERE state IN "
            "('queued','preparing','running','waiting_for_input') UNION ALL "
            "SELECT id, 1 AS group_order FROM terminal"
            ") SELECT r.id, r.state, r.version, r.source_revision, "
            "r.predecessor_run_id, r.replacement_kind, "
            "r.krab_session_id, r.krab_claim_id, r.created_at, r.updated_at, "
            "r.started_at, r.finished_at, visible.group_order, "
            "p.krab_project_key AS project_key, c.id AS context_version_id, "
            "c.version AS context_version, ap.name AS profile_name, "
            "av.id AS profile_version_id, av.version AS profile_version, "
            "u.handle AS requested_by, wi.id AS work_item_id, "
            "wiv.id AS work_item_version_id, wiv.version AS work_item_version, "
            "wi.source AS pinned_source, wi.external_id AS pinned_external_id, "
            "wiv.source_updated_at AS pinned_version_source_updated_at, "
            "wiv.content_json AS pinned_content_json, "
            "wiv.content_sha256 AS pinned_content_sha256, r.work_thread_id "
            "FROM visible JOIN runs r ON r.id = visible.id "
            "JOIN projects p ON p.id = r.project_id "
            "JOIN project_context_versions c ON c.id = r.context_version_id "
            "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
            "JOIN agent_profiles ap ON ap.id = av.profile_id "
            "JOIN users u ON u.id = r.requested_by "
            "LEFT JOIN work_item_versions wiv ON wiv.id = r.work_item_version_id "
            "LEFT JOIN work_items wi ON wi.id = wiv.work_item_id "
            "ORDER BY visible.group_order, r.updated_at DESC, r.id",
            (history_limit,),
        ).fetchall()
        runs: list[dict[str, Any]] = []
        for row in rows:
            title = None
            if row["pinned_content_json"] is not None:
                envelope = _work_item(row, prefix="pinned_")
                title = envelope.title
            runs.append(
                {
                    "id": row["id"],
                    "state": row["state"],
                    "version": row["version"],
                    "project_key": row["project_key"],
                    "context_version_id": row["context_version_id"],
                    "context_version": row["context_version"],
                    "profile_name": row["profile_name"],
                    "profile_version_id": row["profile_version_id"],
                    "profile_version": row["profile_version"],
                    "requested_by": row["requested_by"],
                    "work_item_id": row["work_item_id"],
                    "work_item_version_id": row["work_item_version_id"],
                    "work_item_version": row["work_item_version"],
                    "work_thread_id": row["work_thread_id"],
                    "untrusted_work_item_title": title,
                    "source_revision": row["source_revision"],
                    "predecessor_run_id": row["predecessor_run_id"],
                    "replacement_kind": row["replacement_kind"],
                    "krab_session_id": row["krab_session_id"],
                    "krab_claim_id": row["krab_claim_id"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                }
            )
        return runs, terminal_total

    @staticmethod
    def _for_runs(
        connection: sqlite3.Connection,
        run_ids: list[str],
        query: str,
    ) -> list[sqlite3.Row]:
        if not run_ids:
            return []
        placeholders = ",".join("?" for _ in run_ids)
        return connection.execute(
            query.format(run_ids=placeholders), run_ids
        ).fetchall()

    @classmethod
    def _attempts(
        cls,
        connection: sqlite3.Connection,
        run_ids: list[str],
        generated_at: str,
    ) -> list[dict[str, Any]]:
        now = _timestamp(generated_at, label="snapshot")
        rows = cls._for_runs(
            connection,
            run_ids,
            "SELECT ra.id, ra.run_id, ra.sequence, ra.runtime_adapter, "
            "ra.state, ra.version, ra.source_revision, ra.container_name, "
            "ra.image_id, ra.exit_code, ra.reason, ra.owner_id, "
            "ra.heartbeat_at, ra.lease_expires_at, ra.cancel_requested_at, "
            "u.handle AS cancel_requested_by, ra.cancel_reason, "
            "ra.created_at, ra.started_at, ra.finished_at, "
            "ap.name AS profile_name, av.id AS profile_version_id, "
            "av.version AS profile_version FROM run_attempts ra "
            "JOIN agent_profile_versions av ON av.id = ra.profile_version_id "
            "JOIN agent_profiles ap ON ap.id = av.profile_id "
            "LEFT JOIN users u ON u.id = ra.cancel_requested_by "
            "WHERE ra.run_id IN ({run_ids}) ORDER BY ra.created_at DESC, ra.id",
        )
        attempts: list[dict[str, Any]] = []
        for row in rows:
            lease_expired = (
                row["state"] in ACTIVE_ATTEMPT_STATES
                and row["lease_expires_at"] is not None
                and _timestamp(row["lease_expires_at"], label="attempt lease") <= now
            )
            attempts.append({**dict(row), "lease_expired": lease_expired})
        return attempts

    @classmethod
    def _interactions(
        cls, connection: sqlite3.Connection, run_ids: list[str]
    ) -> list[dict[str, Any]]:
        rows = cls._for_runs(
            connection,
            run_ids,
            "SELECT i.id, i.run_id, i.attempt_id, i.kind, i.state, i.version, "
            "ki.id AS recipient_krab_identity_id, "
            "ki.krab_user_id AS recipient_krab_user_id, "
            "ki.handle AS recipient_krab_handle, i.request_item_id, "
            "i.response_item_id, i.created_at, i.updated_at, i.resolved_at "
            "FROM interactions i JOIN krab_identities ki "
            "ON ki.id = i.recipient_krab_identity_id "
            "WHERE i.run_id IN ({run_ids}) ORDER BY CASE i.state "
            "WHEN 'open' THEN 0 ELSE 1 END, i.created_at DESC, i.id",
        )
        return [dict(row) for row in rows]

    @classmethod
    def _results(
        cls, connection: sqlite3.Connection, run_ids: list[str]
    ) -> list[dict[str, Any]]:
        rows = cls._for_runs(
            connection,
            run_ids,
            "SELECT ar.id, ar.attempt_id, ra.run_id, ar.schema_version, "
            "ar.manifest_json, ar.content_sha256, ar.changed_paths, "
            "ar.changed_bytes, ar.created_at "
            "FROM attempt_results ar JOIN run_attempts ra "
            "ON ra.id = ar.attempt_id WHERE ra.run_id IN ({run_ids}) "
            "ORDER BY ar.created_at DESC, ar.id",
        )
        results: list[dict[str, Any]] = []
        for row in rows:
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
            result = dict(row)
            result.pop("manifest_json")
            results.append(result)
        return results

    @classmethod
    def _run_authorities(
        cls,
        connection: sqlite3.Connection,
        run_ids: list[str],
        generated_at: str,
    ) -> list[dict[str, Any]]:
        now = _timestamp(generated_at, label="snapshot")
        rows = cls._for_runs(
            connection,
            run_ids,
            "SELECT rpg.run_id, rpg.authority_name, pg.id AS grant_id, "
            "pg.name, pg.state, pg.expires_at, pg.revoked_at "
            "FROM run_provider_grants rpg JOIN provider_grants pg "
            "ON pg.id = rpg.provider_grant_id "
            "WHERE rpg.run_id IN ({run_ids}) "
            "ORDER BY rpg.run_id, rpg.authority_name",
        )
        authorities: list[dict[str, Any]] = []
        for row in rows:
            expired = (
                row["expires_at"] is not None
                and _timestamp(row["expires_at"], label="provider expiry") <= now
            )
            authorities.append(
                {
                    **dict(row),
                    "expired": expired,
                    "available": row["state"] == "active" and not expired,
                }
            )
        return authorities

    @staticmethod
    def _attach_run_counts(
        runs: list[dict[str, Any]],
        attempts: list[dict[str, Any]],
        interactions: list[dict[str, Any]],
        results: list[dict[str, Any]],
        authorities: list[dict[str, Any]],
    ) -> None:
        indexes: tuple[tuple[str, list[dict[str, Any]]], ...] = (
            ("attempt_count", attempts),
            ("interaction_count", interactions),
            ("retained_result_count", results),
            ("authority_count", authorities),
        )
        counts: dict[str, dict[str, int]] = {}
        for label, records in indexes:
            current: dict[str, int] = defaultdict(int)
            for record in records:
                current[record["run_id"]] += 1
            counts[label] = current
        for run in runs:
            for label, _ in indexes:
                run[label] = counts[label].get(run["id"], 0)

    @staticmethod
    def _summary(
        connection: sqlite3.Connection,
        provider_grants: list[dict[str, Any]],
        attempts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        def states(query: str) -> dict[str, int]:
            return {
                row["state"]: row["count"]
                for row in connection.execute(query).fetchall()
            }

        run_states = states("SELECT state, COUNT(*) AS count FROM runs GROUP BY state")
        attempt_states = states(
            "SELECT state, COUNT(*) AS count FROM run_attempts GROUP BY state"
        )
        interaction_states = states(
            "SELECT state, COUNT(*) AS count FROM interactions GROUP BY state"
        )
        assignment_states = states(
            "SELECT state, COUNT(*) AS count FROM work_assignments GROUP BY state"
        )
        run_total = sum(run_states.values())
        attempt_total = sum(attempt_states.values())
        return {
            "users": connection.execute("SELECT COUNT(*) FROM users").fetchone()[0],
            "krab_identities": connection.execute(
                "SELECT COUNT(*) FROM krab_identities"
            ).fetchone()[0],
            "projects": connection.execute("SELECT COUNT(*) FROM projects").fetchone()[
                0
            ],
            "profiles": connection.execute(
                "SELECT COUNT(*) FROM agent_profiles"
            ).fetchone()[0],
            "provider_grants": {
                "total": len(provider_grants),
                "available": sum(1 for grant in provider_grants if grant["available"]),
            },
            "work_items": connection.execute(
                "SELECT COUNT(*) FROM work_items"
            ).fetchone()[0],
            "runs": {
                "total": run_total,
                "active": sum(run_states.get(state, 0) for state in ACTIVE_RUN_STATES),
                "terminal": run_total
                - sum(run_states.get(state, 0) for state in ACTIVE_RUN_STATES),
                **{state: run_states.get(state, 0) for state in sorted(run_states)},
            },
            "attempts": {
                "total": attempt_total,
                "active": sum(
                    attempt_states.get(state, 0) for state in ACTIVE_ATTEMPT_STATES
                ),
                "expired_leases": sum(
                    1 for attempt in attempts if attempt["lease_expired"]
                ),
                **{
                    state: attempt_states.get(state, 0)
                    for state in sorted(attempt_states)
                },
            },
            "interactions": {
                "total": sum(interaction_states.values()),
                **{
                    state: interaction_states.get(state, 0)
                    for state in ("open", "answered", "cancelled")
                },
            },
            "assignments": {
                "total": sum(assignment_states.values()),
                **{
                    state: assignment_states.get(state, 0)
                    for state in ("open", "claimed", "completed", "cancelled")
                },
            },
            "retained_results": connection.execute(
                "SELECT COUNT(*) FROM attempt_results"
            ).fetchone()[0],
        }

    @staticmethod
    def _attention(
        work_items: list[dict[str, Any]],
        assignments: list[dict[str, Any]],
        runs: list[dict[str, Any]],
        attempts: list[dict[str, Any]],
        interactions: list[dict[str, Any]],
        authorities: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        attention: list[dict[str, Any]] = []
        active_runs = {
            run["id"]: run for run in runs if run["state"] in ACTIVE_RUN_STATES
        }
        active_attempt_runs = {
            attempt["run_id"]
            for attempt in attempts
            if attempt["state"] in ACTIVE_ATTEMPT_STATES
        }
        open_interaction_runs = {
            interaction["run_id"]
            for interaction in interactions
            if interaction["state"] == "open"
        }
        for interaction in interactions:
            if interaction["state"] == "open":
                attention.append(
                    {
                        "kind": "interaction.pending",
                        "severity": "action",
                        "subject_type": "interaction",
                        "subject_id": interaction["id"],
                        "run_id": interaction["run_id"],
                        "interaction_kind": interaction["kind"],
                        "recipient_krab_handle": interaction["recipient_krab_handle"],
                        "created_at": interaction["created_at"],
                    }
                )
        for assignment in assignments:
            if assignment["thread_state"] != "active":
                continue
            if assignment["state"] == "open":
                attention.append(
                    {
                        "kind": "assignment.open",
                        "severity": "action",
                        "subject_type": "assignment",
                        "subject_id": assignment["id"],
                        "work_item_id": assignment["work_item_id"],
                        "target_profile_name": assignment["target_profile_name"],
                        "created_at": assignment["created_at"],
                    }
                )
            elif assignment["state"] == "claimed" and assignment["lease_expired"]:
                attention.append(
                    {
                        "kind": "assignment.lease_expired",
                        "severity": "warning",
                        "subject_type": "assignment",
                        "subject_id": assignment["id"],
                        "work_item_id": assignment["work_item_id"],
                        "claimed_by_run_id": assignment["claimed_by_run_id"],
                        "lease_expires_at": assignment["lease_expires_at"],
                    }
                )
        for item in work_items:
            if item["state"] == "active" and item["blocker_entries"]:
                attention.append(
                    {
                        "kind": "work.blocker",
                        "severity": "warning",
                        "subject_type": "work_item",
                        "subject_id": item["id"],
                        "blocker_entries": item["blocker_entries"],
                        "last_blocker_at": item["last_blocker_at"],
                    }
                )
        for attempt in attempts:
            if attempt["cancel_requested_at"] is not None:
                attention.append(
                    {
                        "kind": "run.cancellation_requested",
                        "severity": "action",
                        "subject_type": "attempt",
                        "subject_id": attempt["id"],
                        "run_id": attempt["run_id"],
                        "owner_id": attempt["owner_id"],
                        "requested_at": attempt["cancel_requested_at"],
                    }
                )
            if attempt["lease_expired"]:
                attention.append(
                    {
                        "kind": "attempt.lease_expired",
                        "severity": "warning",
                        "subject_type": "attempt",
                        "subject_id": attempt["id"],
                        "run_id": attempt["run_id"],
                        "owner_id": attempt["owner_id"],
                        "lease_expires_at": attempt["lease_expires_at"],
                    }
                )
        for run_id, run in active_runs.items():
            if (
                run["state"] in {"preparing", "running"}
                and run_id not in active_attempt_runs
            ):
                attention.append(
                    {
                        "kind": "run.missing_active_attempt",
                        "severity": "warning",
                        "subject_type": "run",
                        "subject_id": run_id,
                        "state": run["state"],
                        "updated_at": run["updated_at"],
                    }
                )
            if (
                run["state"] == "waiting_for_input"
                and run_id not in open_interaction_runs
                and run_id not in active_attempt_runs
            ):
                attention.append(
                    {
                        "kind": "run.waiting_without_interaction",
                        "severity": "warning",
                        "subject_type": "run",
                        "subject_id": run_id,
                        "updated_at": run["updated_at"],
                    }
                )
        for authority in authorities:
            if authority["run_id"] in active_runs and not authority["available"]:
                attention.append(
                    {
                        "kind": "authority.unavailable",
                        "severity": "warning",
                        "subject_type": "provider_grant",
                        "subject_id": authority["grant_id"],
                        "run_id": authority["run_id"],
                        "authority_name": authority["authority_name"],
                        "state": authority["state"],
                        "expires_at": authority["expires_at"],
                    }
                )
        attention.sort(
            key=lambda item: (
                0 if item["severity"] == "action" else 1,
                item["kind"],
                item["subject_id"],
            )
        )
        return attention

    def _refresh_krab(
        self,
        local: dict[str, Any],
        routes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        roles: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        project_clients: dict[str, tuple[dict[str, Any], KrabClient]] = {}
        known_projects = {
            project["project_key"]: project for project in local["projects"]
        }
        for route in routes:
            try:
                token = self.credential_resolver.resolve(route["credential_ref"])
                client = self.client_factory(route["base_url"], token)
                actual = client.whoami()
                if actual.user_id != route["krab_user_id"]:
                    raise KrabError(
                        "credential now belongs to a different Krab identity"
                    )
                visible = client.projects()
            except (CredentialError, KrabError) as exc:
                errors.append(
                    {
                        "scope": "identity",
                        "identity_id": route["id"],
                        "krab_handle": route["handle"],
                        "error_type": type(exc).__name__,
                        "message": (
                            "configured credential is unavailable"
                            if isinstance(exc, CredentialError)
                            else str(exc)
                        ),
                    }
                )
                continue
            for project in visible:
                if project.project_key not in known_projects:
                    continue
                registered = known_projects[project.project_key]
                if project.project_id != registered["krab_project_id"]:
                    errors.append(
                        {
                            "scope": "role",
                            "identity_id": route["id"],
                            "project_key": project.project_key,
                            "error_type": "KrabError",
                            "message": "Krab project ID no longer matches local state",
                        }
                    )
                    continue
                roles.append(
                    {
                        "identity_id": route["id"],
                        "krab_user_id": route["krab_user_id"],
                        "krab_handle": route["handle"],
                        "local_user_id": route["local_user_id"],
                        "project_key": project.project_key,
                        "role": project.role,
                    }
                )
                current = project_clients.get(project.project_key)
                rank = {"member": 0, "coordinator": 1, "admin": 2}
                if current is None or rank[project.role] > rank[current[0]["role"]]:
                    project_clients[project.project_key] = (
                        {"role": project.role, "identity_id": route["id"]},
                        client,
                    )

        work_item_by_reference = {
            (item["project_key"], item["external_id"]): item
            for item in local["work_items"]
        }
        work_item_by_claim: dict[tuple[str, str], dict[str, Any]] = {}
        run_by_id = {run["id"]: run for run in local["runs"]}
        for run in run_by_id.values():
            if run["krab_claim_id"] is not None and run["work_item_id"] is not None:
                item = next(
                    (
                        candidate
                        for candidate in local["work_items"]
                        if candidate["id"] == run["work_item_id"]
                    ),
                    None,
                )
                if item is not None:
                    work_item_by_claim[(run["project_key"], run["krab_claim_id"])] = (
                        item
                    )
        work_claims: list[dict[str, Any]] = []
        for project_key, (identity, client) in sorted(project_clients.items()):
            for kind in ("issue", "pull_request"):
                try:
                    page = client.work_claims(project_key, kind=kind)
                except KrabError as exc:
                    errors.append(
                        {
                            "scope": "work_claims",
                            "identity_id": identity["identity_id"],
                            "project_key": project_key,
                            "kind": kind,
                            "error_type": type(exc).__name__,
                            "message": str(exc),
                        }
                    )
                    continue
                for claim in page.claims:
                    item = work_item_by_claim.get((project_key, claim.claim_id))
                    if item is None:
                        item = work_item_by_reference.get(
                            (project_key, claim.external_id)
                        )
                    work_claims.append(
                        {
                            **self._claim_metadata(claim),
                            "read_via_identity_id": identity["identity_id"],
                            "local_work_item": (
                                None
                                if item is None
                                else {
                                    "id": item["id"],
                                    "untrusted_title": item["untrusted_title"],
                                }
                            ),
                        }
                    )
        roles.sort(key=lambda item: (item["project_key"], item["krab_handle"]))
        work_claims.sort(
            key=lambda item: (
                item["project_key"],
                item["kind"],
                item["status"],
                item["claim_id"],
            )
        )
        return {
            "status": "degraded" if errors else "refreshed",
            "refreshed_at": self.database.now(),
            "roles": roles,
            "work_claims": work_claims,
            "errors": errors,
        }

    @staticmethod
    def _claim_metadata(claim: WorkClaim) -> dict[str, Any]:
        result = asdict(claim)
        return result
