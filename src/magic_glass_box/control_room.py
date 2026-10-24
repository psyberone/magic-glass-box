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
    SupervisionPolicy,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
)
from .gates import GateError, GatePolicyManifest
from .inference import InferenceGrantSpec
from .krab import KrabClient, KrabError, WorkClaim
from .results import ResultError, ResultManifest

ClientFactory = Callable[[str, str], KrabClient]
ACTIVE_RUN_STATES = {
    "queued",
    "preparing",
    "running",
    "waiting_for_input",
    "paused",
}
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
        workflows = self._workflows(connection, history_limit)
        run_ids = [run["id"] for run in runs]
        workflow_ids = [workflow["id"] for workflow in workflows]
        attempts = self._attempts(connection, run_ids, generated_at)
        interactions = self._interactions(connection, run_ids)
        retained_results = self._results(connection, run_ids)
        candidates = self._candidates(connection, run_ids)
        gate_policies = self._gate_policies(connection, run_ids)
        landing_dependencies = self._landing_dependencies(connection, history_limit)
        integration_queue = self._integration_queue(
            connection, history_limit, generated_at
        )
        authorities = self._run_authorities(connection, run_ids, generated_at)
        provider_recoveries = self._provider_recoveries(connection, run_ids)
        provider_budgets = self._provider_budgets(connection, run_ids)
        supervisor_actions = self._supervisor_actions(connection, run_ids, workflow_ids)
        escalations = self._escalations(connection, run_ids)
        self._attach_run_counts(
            runs,
            attempts,
            interactions,
            retained_results,
            candidates,
            authorities,
            provider_recoveries,
        )
        summary = self._summary(connection, provider_grants, attempts)
        summary["provider_budgets"] = {
            "authorities": len(provider_budgets),
            "policy_consumed_microusd": sum(
                item["policy_consumed_microusd"] for item in provider_budgets
            ),
            "accounted_microusd": sum(
                item["accounted_microusd"] for item in provider_budgets
            ),
            "reserved_microusd": sum(
                item["reserved_microusd"] for item in provider_budgets
            ),
            "estimated_microusd": sum(
                item["estimated_microusd"] for item in provider_budgets
            ),
            "active_reservations": sum(
                item["active_reservations"] for item in provider_budgets
            ),
            "conservative_invocations": sum(
                item["conservative_invocations"] for item in provider_budgets
            ),
        }
        summary["provider_recoveries"] = {
            "total": len(provider_recoveries),
            **{
                state: sum(
                    1 for recovery in provider_recoveries if recovery["state"] == state
                )
                for state in ("active", "resumed", "cancelled")
            },
        }
        attention = self._attention(
            work_items,
            assignments,
            runs,
            attempts,
            interactions,
            authorities,
            provider_recoveries,
            supervisor_actions,
            escalations,
            gate_policies,
            integration_queue,
            workflows=workflows,
            generated_at=generated_at,
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
                "workflows": workflows,
                "attempts": attempts,
                "interactions": interactions,
                "retained_results": retained_results,
                "candidates": candidates,
                "gate_policies": gate_policies,
                "landing_dependencies": landing_dependencies,
                "integration_queue": integration_queue,
                "run_authorities": authorities,
                "provider_recoveries": provider_recoveries,
                "provider_budgets": provider_budgets,
                "supervisor_actions": supervisor_actions,
                "escalations": escalations,
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
            "SELECT pg.id, pg.name, pg.state, pg.version, pg.grant_kind, "
            "pg.policy_json, "
            "pg.content_sha256, pg.expires_at, pg.revoked_at, "
            "creator.handle AS created_by, revoker.handle AS revoked_by, "
            "pg.created_at, pg.updated_at, c.id AS connector_id, "
            "c.name AS connector_name, c.state AS connector_state, "
            "c.version AS connector_version FROM provider_grants pg "
            "JOIN users creator ON creator.id = pg.created_by "
            "LEFT JOIN users revoker ON revoker.id = pg.revoked_by "
            "LEFT JOIN provider_connector_bindings pcb "
            "ON pcb.provider_grant_id = pg.id "
            "LEFT JOIN inference_connectors c "
            "ON c.id = pcb.inference_connector_id "
            "ORDER BY pg.name"
        ).fetchall()
        grants: list[dict[str, Any]] = []
        for row in rows:
            try:
                raw_policy = json.loads(row["policy_json"])
                spec = (
                    ProviderGrantSpec.from_mapping(raw_policy)
                    if row["grant_kind"] == "direct"
                    else InferenceGrantSpec.from_mapping(raw_policy)
                )
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError("stored provider grant policy is invalid") from exc
            if spec.digest != row["content_sha256"]:
                raise StorageError("stored provider grant policy digest does not match")
            if row["grant_kind"] == "connector" and row["connector_id"] is None:
                raise StorageError("stored connector provider grant has no binding")
            expired = (
                row["expires_at"] is not None
                and _timestamp(row["expires_at"], label="provider expiry") <= now
            )
            dispatch_enabled = (
                row["grant_kind"] == "connector" and row["connector_state"] == "active"
            )
            available = row["state"] == "active" and not expired and dispatch_enabled
            if row["grant_kind"] == "direct":
                assert isinstance(spec, ProviderGrantSpec)
                policy = {
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
                }
            else:
                assert isinstance(spec, InferenceGrantSpec)
                policy = spec.to_dict()
            grant = {
                "id": row["id"],
                "name": row["name"],
                "state": row["state"],
                "version": row["version"],
                "grant_kind": row["grant_kind"],
                "available": available,
                "expired": expired,
                "expires_at": row["expires_at"],
                "revoked_at": row["revoked_at"],
                "revoked_by": row["revoked_by"],
                "created_by": row["created_by"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "content_sha256": row["content_sha256"],
                "policy": policy,
            }
            if row["grant_kind"] == "connector":
                grant["connector"] = {
                    "id": row["connector_id"],
                    "name": row["connector_name"],
                    "state": row["connector_state"],
                    "version": row["connector_version"],
                }
            grants.append(grant)
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
            "AND r.state IN ("
            "'queued','preparing','running','waiting_for_input','paused')) "
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
            "('queued','preparing','running','waiting_for_input','paused') "
            "UNION ALL "
            "SELECT id, 1 AS group_order FROM terminal"
            ") SELECT r.id, r.state, r.version, r.source_revision, "
            "r.predecessor_run_id, r.replacement_kind, "
            "r.krab_session_id, r.krab_claim_id, r.supervision_policy_json, "
            "r.created_at, r.updated_at, "
            "r.started_at, r.finished_at, visible.group_order, "
            "p.krab_project_key AS project_key, c.id AS context_version_id, "
            "c.version AS context_version, ap.name AS profile_name, "
            "av.id AS profile_version_id, av.version AS profile_version, "
            "u.handle AS requested_by, wi.id AS work_item_id, "
            "wiv.id AS work_item_version_id, wiv.version AS work_item_version, "
            "wi.source AS pinned_source, wi.external_id AS pinned_external_id, "
            "wiv.source_updated_at AS pinned_version_source_updated_at, "
            "wiv.content_json AS pinned_content_json, "
            "wiv.content_sha256 AS pinned_content_sha256, r.work_thread_id, "
            "grr.policy_id AS gate_policy_id, "
            "grr.candidate_id AS review_candidate_id, "
            "gcr.policy_id AS change_policy_id, "
            "gcr.candidate_id AS change_candidate_id, "
            "gcr.review_evidence_id AS change_review_evidence_id, "
            "gcr.review_entry_id AS change_review_entry_id, "
            "gmcr.policy_id AS maintainer_change_policy_id, "
            "gmcr.candidate_id AS maintainer_change_candidate_id, "
            "gmcr.maintainer_review_id AS change_maintainer_review_id "
            "FROM visible JOIN runs r ON r.id = visible.id "
            "JOIN projects p ON p.id = r.project_id "
            "JOIN project_context_versions c ON c.id = r.context_version_id "
            "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
            "JOIN agent_profiles ap ON ap.id = av.profile_id "
            "JOIN users u ON u.id = r.requested_by "
            "LEFT JOIN work_item_versions wiv ON wiv.id = r.work_item_version_id "
            "LEFT JOIN work_items wi ON wi.id = wiv.work_item_id "
            "LEFT JOIN integration_gate_review_runs grr ON grr.run_id = r.id "
            "LEFT JOIN integration_gate_change_runs gcr ON gcr.run_id = r.id "
            "LEFT JOIN integration_gate_maintainer_change_runs gmcr "
            "ON gmcr.run_id = r.id "
            "ORDER BY visible.group_order, r.updated_at DESC, r.id",
            (history_limit,),
        ).fetchall()
        runs: list[dict[str, Any]] = []
        for row in rows:
            title = None
            if row["pinned_content_json"] is not None:
                envelope = _work_item(row, prefix="pinned_")
                title = envelope.title
            try:
                supervision_policy = SupervisionPolicy.from_mapping(
                    json.loads(row["supervision_policy_json"])
                )
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError("stored supervision policy is invalid") from exc
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
                    "gate_policy_id": row["gate_policy_id"],
                    "review_candidate_id": row["review_candidate_id"],
                    "change_policy_id": row["change_policy_id"],
                    "change_candidate_id": row["change_candidate_id"],
                    "change_review_evidence_id": row["change_review_evidence_id"],
                    "change_review_entry_id": row["change_review_entry_id"],
                    "maintainer_change_policy_id": row["maintainer_change_policy_id"],
                    "maintainer_change_candidate_id": row[
                        "maintainer_change_candidate_id"
                    ],
                    "change_maintainer_review_id": row["change_maintainer_review_id"],
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
                    "supervision_policy": supervision_policy.to_dict(),
                }
            )
        return runs, terminal_total

    @staticmethod
    def _workflows(
        connection: sqlite3.Connection, history_limit: int
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            "WITH terminal AS (SELECT id FROM workflow_executions "
            "WHERE state IN ('cancelled','completed') "
            "ORDER BY updated_at DESC, id LIMIT ?), visible AS ("
            "SELECT id, 0 AS group_order FROM workflow_executions "
            "WHERE state = 'active' UNION ALL "
            "SELECT id, 1 AS group_order FROM terminal"
            ") SELECT e.id, p.krab_project_key AS project_key, "
            "t.template_key, t.version AS template_version, "
            "t.content_sha256 AS template_revision, c.name AS configuration_name, "
            "cv.version AS configuration_version, "
            "cv.id AS configuration_version_id, e.work_item_id, "
            "e.work_item_version_id, e.work_thread_id, "
            "e.work_thread_generation, e.context_version_id, e.source_revision, "
            "e.state, e.stage, e.correction_round, e.version, "
            "producer.name AS producer_profile_name, "
            "producer_v.version AS producer_profile_version, "
            "reviewer.name AS reviewer_profile_name, "
            "reviewer_v.version AS reviewer_profile_version, "
            "requester.handle AS requested_by, action.id AS action_id, "
            "action.state AS action_state, "
            "action.attempt_count AS action_attempt_count, "
            "action.max_attempts AS action_max_attempts, "
            "action.last_error_code AS action_last_error_code, "
            "ruling.id AS open_ruling_id, ruling.kind AS open_ruling_kind, "
            "ruling.version AS open_ruling_version, "
            "ruling.created_at AS open_ruling_created_at, "
            "json_extract(cv.spec_json, '$.landing_mode') AS landing_mode, "
            "landing.candidate_id AS landing_candidate_id, "
            "queue.current_policy_id AS landing_policy_id, "
            "landing.queue_item_id AS landing_queue_item_id, "
            "queue.state AS landing_queue_state, "
            "queue.version AS landing_queue_version, "
            "queue.updated_at AS landing_queue_updated_at, "
            "target.resource_id AS landing_provider_resource_id, "
            "target.merge_method AS landing_merge_method, "
            "CASE WHEN landing.execution_id IS NOT NULL "
            "AND json_extract(cv.spec_json, '$.landing_mode') = 'provider' "
            "THEN EXISTS(SELECT 1 FROM project_capability_bindings pcb "
            "JOIN service_sidecars sidecar ON sidecar.id = pcb.service_sidecar_id "
            "WHERE pcb.project_id = e.project_id "
            "AND pcb.capability_name = 'landing.invoke' "
            "AND pcb.capability_version = 1 AND pcb.state = 'active' "
            "AND sidecar.state = 'active') ELSE NULL END "
            "AS landing_provider_available, "
            "plan.id AS landing_plan_id, plan.state AS landing_plan_state, "
            "provider_landing.id AS landing_provider_landing_id, "
            "provider_landing.state AS landing_provider_landing_state, "
            "queue.landed_receipt_id AS landing_receipt_id, "
            "completion.outcome_entry_id AS completion_outcome_entry_id, "
            "e.created_at, e.updated_at, "
            "e.finished_at FROM visible "
            "JOIN workflow_executions e ON e.id = visible.id "
            "JOIN projects p ON p.id = e.project_id "
            "JOIN workflow_template_definitions t "
            "ON t.id = e.template_definition_id "
            "JOIN workflow_configuration_versions cv "
            "ON cv.id = e.configuration_version_id "
            "JOIN workflow_configurations c ON c.id = cv.configuration_id "
            "JOIN agent_profile_versions producer_v "
            "ON producer_v.id = cv.producer_profile_version_id "
            "JOIN agent_profiles producer ON producer.id = producer_v.profile_id "
            "JOIN agent_profile_versions reviewer_v "
            "ON reviewer_v.id = cv.reviewer_profile_version_id "
            "JOIN agent_profiles reviewer ON reviewer.id = reviewer_v.profile_id "
            "JOIN users requester ON requester.id = e.requested_by "
            "LEFT JOIN supervisor_actions action ON action.id = ("
            "SELECT sa.id FROM supervisor_actions sa "
            "WHERE sa.workflow_execution_id = e.id "
            "AND sa.subject_version = e.version "
            "ORDER BY sa.created_at DESC, sa.id DESC LIMIT 1) "
            "LEFT JOIN workflow_rulings ruling ON ruling.execution_id = e.id "
            "AND ruling.state = 'open' "
            "LEFT JOIN workflow_landing_bindings landing "
            "ON landing.execution_id = e.id "
            "LEFT JOIN integration_queue_items queue "
            "ON queue.id = landing.queue_item_id "
            "LEFT JOIN workflow_provider_targets target "
            "ON target.execution_id = e.id "
            "LEFT JOIN integration_landing_plans plan ON plan.id = ("
            "SELECT lp.id FROM integration_landing_plans lp "
            "WHERE lp.queue_item_id = landing.queue_item_id "
            "ORDER BY lp.sequence DESC LIMIT 1) "
            "LEFT JOIN integration_provider_landings provider_landing "
            "ON provider_landing.id = ("
            "SELECT pl.id FROM integration_provider_landings pl "
            "WHERE pl.queue_item_id = landing.queue_item_id "
            "ORDER BY pl.sequence DESC LIMIT 1) "
            "LEFT JOIN workflow_completions completion "
            "ON completion.execution_id = e.id "
            "ORDER BY visible.group_order, e.updated_at DESC, e.id",
            (history_limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    @classmethod
    def _supervisor_actions(
        cls,
        connection: sqlite3.Connection,
        run_ids: list[str],
        workflow_ids: list[str],
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[str] = []
        if run_ids:
            clauses.append("run_id IN (" + ",".join("?" for _ in run_ids) + ")")
            parameters.extend(run_ids)
        if workflow_ids:
            clauses.append(
                "workflow_execution_id IN (" + ",".join("?" for _ in workflow_ids) + ")"
            )
            parameters.extend(workflow_ids)
        if not clauses:
            return []
        rows = connection.execute(
            "SELECT id, kind, subject_id, subject_version, run_id, "
            "workflow_execution_id, state, version, attempt_count, "
            "max_attempts, available_at, owner_id, lease_expires_at, "
            "last_error_code, created_at, updated_at, finished_at "
            "FROM supervisor_actions WHERE "
            + " OR ".join(f"({clause})" for clause in clauses)
            + " ORDER BY created_at DESC, id",
            parameters,
        ).fetchall()
        return [dict(row) for row in rows]

    @classmethod
    def _escalations(
        cls, connection: sqlite3.Connection, run_ids: list[str]
    ) -> list[dict[str, Any]]:
        rows = cls._for_runs(
            connection,
            run_ids,
            "SELECT id, run_id, kind, subject_type, subject_id, "
            "threshold_seconds, age_started_at, channel, state, version, "
            "attempt_count, max_attempts, available_at, owner_id, "
            "lease_expires_at, last_error_code, krab_event_seq, created_at, "
            "updated_at, delivered_at FROM supervisor_escalations "
            "WHERE run_id IN ({run_ids}) "
            "ORDER BY created_at DESC, threshold_seconds DESC, id",
        )
        return [dict(row) for row in rows]

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
    def _candidates(
        cls, connection: sqlite3.Connection, run_ids: list[str]
    ) -> list[dict[str, Any]]:
        rows = cls._for_runs(
            connection,
            run_ids,
            "SELECT ic.id, ic.attempt_result_id, ic.schema_version, "
            "ic.project_id, ic.context_version_id, ic.work_thread_id, "
            "ic.producer_run_id, ic.producer_attempt_id, ic.base_revision, "
            "ic.head_revision, ic.tree_revision, ic.object_format, "
            "ic.result_content_sha256, ic.content_sha256, "
            "u.handle AS created_by_handle, ic.created_at "
            "FROM integration_candidates ic JOIN users u "
            "ON u.id = ic.created_by "
            "WHERE ic.producer_run_id IN ({run_ids}) "
            "ORDER BY ic.created_at DESC, ic.id",
        )
        return [dict(row) for row in rows]

    @classmethod
    def _gate_policies(
        cls, connection: sqlite3.Connection, run_ids: list[str]
    ) -> list[dict[str, Any]]:
        if not run_ids:
            return []
        placeholders = ",".join("?" for _ in run_ids)
        query = (
            "SELECT gp.id, gp.candidate_id, gp.manifest_json, "  # noqa: S608
            "gp.content_sha256, gp.repository_identity, gp.project_id, "
            "gp.work_thread_id, gp.integration_ref, "
            "gp.expected_base_revision, gp.head_revision, gp.tree_revision, "
            "gp.object_format, gp.policy_revision, gp.review_separation, "
            "gp.created_at, ic.producer_run_id, u.handle AS created_by_handle "
            "FROM integration_gate_policies gp "
            "JOIN integration_candidates ic ON ic.id = gp.candidate_id "
            "JOIN users u ON u.id = gp.created_by "
            f"WHERE ic.producer_run_id IN ({placeholders}) OR EXISTS ("
            "SELECT 1 FROM integration_gate_review_runs grr "
            "WHERE grr.policy_id = gp.id AND grr.run_id IN ("
            f"{placeholders})) OR EXISTS ("
            "SELECT 1 FROM integration_gate_change_runs gcr "
            "WHERE gcr.policy_id = gp.id AND gcr.run_id IN ("
            f"{placeholders})) OR EXISTS ("
            "SELECT 1 FROM integration_gate_maintainer_change_runs gmcr "
            "WHERE gmcr.policy_id = gp.id AND gmcr.run_id IN ("
            f"{placeholders})) ORDER BY gp.created_at DESC, gp.id"
        )
        rows = connection.execute(
            query, (*run_ids, *run_ids, *run_ids, *run_ids)
        ).fetchall()
        if not rows:
            return []
        manifests: dict[str, GatePolicyManifest] = {}
        for row in rows:
            try:
                manifests[row["id"]] = GatePolicyManifest.from_mapping(
                    json.loads(row["manifest_json"])
                )
            except (GateError, TypeError, json.JSONDecodeError) as exc:
                raise StorageError("stored gate policy manifest is invalid") from exc

        policy_ids = list(manifests)
        placeholders = ",".join("?" for _ in policy_ids)
        evidence_rows = connection.execute(
            "SELECT e.policy_id, e.id, e.gate_name, e.outcome, e.sequence, "  # noqa: S608
            "e.created_at FROM integration_gate_evidence e JOIN ("
            "SELECT policy_id, gate_name, MAX(sequence) AS sequence "
            "FROM integration_gate_evidence WHERE policy_id IN ("
            + placeholders
            + ") GROUP BY policy_id, gate_name) latest "
            "ON latest.policy_id = e.policy_id "
            "AND latest.gate_name = e.gate_name "
            "AND latest.sequence = e.sequence",
            policy_ids,
        ).fetchall()
        latest: dict[str, dict[str, sqlite3.Row]] = defaultdict(dict)
        for evidence in evidence_rows:
            latest[evidence["policy_id"]][evidence["gate_name"]] = evidence
        maintainer_rows = connection.execute(
            "SELECT r.policy_id, r.id, 'maintainer-approval' AS gate_name, "
            "CASE r.decision WHEN 'approved' THEN 'passed' ELSE 'failed' END "
            "AS outcome, r.sequence, r.created_at "
            "FROM integration_maintainer_reviews r JOIN ("
            "SELECT policy_id, MAX(sequence) AS sequence "
            "FROM integration_maintainer_reviews WHERE policy_id IN ("
            + placeholders
            + ") GROUP BY policy_id) latest "
            "ON latest.policy_id = r.policy_id AND latest.sequence = r.sequence",
            policy_ids,
        ).fetchall()
        for evidence in maintainer_rows:
            latest[evidence["policy_id"]][evidence["gate_name"]] = evidence

        policies: list[dict[str, Any]] = []
        for row in rows:
            manifest = manifests[row["id"]]
            current = latest[row["id"]]
            required = manifest.policy.required_gates
            states = [
                "pending" if name not in current else current[name]["outcome"]
                for name in required
            ]
            evidence_times = [record["created_at"] for record in current.values()]
            passed = states.count("passed")
            failed = states.count("failed")
            pending = states.count("pending")
            policies.append(
                {
                    "id": row["id"],
                    "candidate_id": row["candidate_id"],
                    "project_id": row["project_id"],
                    "work_thread_id": row["work_thread_id"],
                    "producer_run_id": row["producer_run_id"],
                    "repository_identity": row["repository_identity"],
                    "integration_ref": row["integration_ref"],
                    "expected_base_revision": row["expected_base_revision"],
                    "head_revision": row["head_revision"],
                    "tree_revision": row["tree_revision"],
                    "object_format": row["object_format"],
                    "policy_revision": row["policy_revision"],
                    "content_sha256": row["content_sha256"],
                    "review_separation": row["review_separation"],
                    "required_count": len(required),
                    "passed_count": passed,
                    "failed_count": failed,
                    "pending_count": pending,
                    "ready": passed == len(required),
                    "latest_evidence_at": (
                        None if not evidence_times else max(evidence_times)
                    ),
                    "created_by_handle": row["created_by_handle"],
                    "created_at": row["created_at"],
                }
            )
        return policies

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
            "pg.name, pg.state, pg.grant_kind, pg.expires_at, pg.revoked_at, "
            "rpg.created_at AS authority_created_at, "
            "c.state AS connector_state, c.revoked_at AS connector_revoked_at "
            "FROM run_provider_grants rpg JOIN provider_grants pg "
            "ON pg.id = rpg.provider_grant_id "
            "LEFT JOIN provider_connector_bindings pcb "
            "ON pcb.provider_grant_id = pg.id "
            "LEFT JOIN inference_connectors c "
            "ON c.id = pcb.inference_connector_id "
            "WHERE rpg.run_id IN ({run_ids}) "
            "ORDER BY rpg.run_id, rpg.authority_name",
        )
        authorities: list[dict[str, Any]] = []
        for row in rows:
            expired = (
                row["expires_at"] is not None
                and _timestamp(row["expires_at"], label="provider expiry") <= now
            )
            authority = dict(row)
            authority.pop("grant_kind")
            authority.pop("connector_state")
            connector_revoked_at = authority.pop("connector_revoked_at")
            authority_created_at = authority.pop("authority_created_at")
            unavailable_times: list[str] = []
            if row["state"] != "active" and row["revoked_at"] is not None:
                unavailable_times.append(row["revoked_at"])
            if expired:
                unavailable_times.append(row["expires_at"])
            if row["grant_kind"] != "connector":
                authority["state"] = "unsupported"
                unavailable_times.append(authority_created_at)
            elif row["connector_state"] != "active":
                if row["state"] == "active":
                    authority["state"] = row["connector_state"] or "unavailable"
                    authority["revoked_at"] = connector_revoked_at
                unavailable_times.append(connector_revoked_at or authority_created_at)
            authority.update(
                {
                    "expired": expired,
                    "available": (
                        row["state"] == "active"
                        and row["grant_kind"] == "connector"
                        and row["connector_state"] == "active"
                        and not expired
                    ),
                    "unavailable_since": (
                        min(unavailable_times) if unavailable_times else None
                    ),
                }
            )
            authorities.append(authority)
        return authorities

    @classmethod
    def _provider_recoveries(
        cls,
        connection: sqlite3.Connection,
        run_ids: list[str],
    ) -> list[dict[str, Any]]:
        rows = cls._for_runs(
            connection,
            run_ids,
            "SELECT id, run_id, attempt_id, invocation_id, category, state, "
            "version, retry_after_seconds, eligible_at, error_code, "
            "provider_status, created_at, updated_at, resolved_at "
            "FROM provider_recoveries WHERE run_id IN ({run_ids}) "
            "ORDER BY created_at, id",
        )
        return [dict(row) for row in rows]

    @classmethod
    def _provider_budgets(
        cls,
        connection: sqlite3.Connection,
        run_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Project bounded accounting metadata for displayed run authorities."""

        authority_rows = cls._for_runs(
            connection,
            run_ids,
            "SELECT rpg.run_id, rpg.authority_name, rpg.provider_grant_id, "
            "r.project_id, p.krab_project_key AS project_key, pg.name AS grant_name, "
            "pg.policy_json, pg.content_sha256 FROM run_provider_grants rpg "
            "JOIN runs r ON r.id = rpg.run_id "
            "JOIN projects p ON p.id = r.project_id "
            "JOIN provider_grants pg ON pg.id = rpg.provider_grant_id "
            "WHERE rpg.run_id IN ({run_ids}) AND pg.grant_kind = 'connector' "
            "ORDER BY rpg.run_id, rpg.authority_name",
        )
        parsed_authorities: list[tuple[sqlite3.Row, Any]] = []
        for row in authority_rows:
            try:
                spec = InferenceGrantSpec.from_mapping(json.loads(row["policy_json"]))
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError("stored provider grant policy is invalid") from exc
            if spec.digest != row["content_sha256"]:
                raise StorageError("stored provider grant policy digest does not match")
            if spec.budget is not None:
                parsed_authorities.append((row, spec.budget))
        if not parsed_authorities:
            return []

        placeholders = ", ".join("?" for _ in run_ids)
        amount = (
            "CASE WHEN pi.outcome = 'dispatching' THEN pi.reserved_microusd "
            "ELSE pi.accounted_microusd END"
        )
        exact_rows = connection.execute(
            "SELECT pi.run_id, pi.provider_grant_id, pi.budget_account, "  # noqa: S608 - only generated placeholders and controller-owned SQL fragments
            "COUNT(*) AS invocations, "
            "SUM(pi.accounting_status = 'reported') AS reported, "
            "SUM(pi.accounting_status = 'conservative') AS conservative, "
            "SUM(pi.accounting_status = 'reserved') AS active, "
            f"SUM({amount}) AS consumed, "
            "SUM(COALESCE(pi.accounted_microusd, 0)) AS accounted, "
            "SUM(CASE WHEN pi.accounting_status = 'reserved' "
            "THEN pi.reserved_microusd ELSE 0 END) AS reserved, "
            "SUM(COALESCE(pi.estimated_microusd, 0)) AS estimated, "
            "SUM(COALESCE(pi.input_tokens, 0)) AS input, "
            "SUM(COALESCE(pi.cached_input_tokens, 0)) AS cached, "
            "SUM(COALESCE(pi.output_tokens, 0)) AS output, "
            "SUM(COALESCE(pi.reasoning_output_tokens, 0)) AS reasoning "
            "FROM provider_invocations pi "
            f"WHERE pi.run_id IN ({placeholders}) "
            "AND pi.budget_account IS NOT NULL "
            "GROUP BY pi.run_id, pi.provider_grant_id, pi.budget_account",
            run_ids,
        ).fetchall()
        exact = {
            (row["run_id"], row["provider_grant_id"], row["budget_account"]): {
                field: int(row[field])
                for field in (
                    "invocations",
                    "reported",
                    "conservative",
                    "active",
                    "consumed",
                    "accounted",
                    "reserved",
                    "estimated",
                    "input",
                    "cached",
                    "output",
                    "reasoning",
                )
            }
            for row in exact_rows
        }
        scope_rows = connection.execute(
            "WITH selected_runs AS ("  # noqa: S608 - only generated placeholders and controller-owned SQL fragments
            "SELECT id, project_id FROM runs "
            f"WHERE id IN ({placeholders})"
            "), selected_grants AS ("
            "SELECT DISTINCT rpg.provider_grant_id AS id "
            "FROM run_provider_grants rpg "
            "JOIN selected_runs sr ON sr.id = rpg.run_id"
            "), selected_projects AS ("
            "SELECT DISTINCT project_id AS id FROM selected_runs"
            ") "
            "SELECT 'run' AS scope, pi.run_id AS scope_id, "
            f"pi.budget_account, SUM({amount}) AS consumed "
            "FROM provider_invocations pi "
            "JOIN selected_runs sr ON sr.id = pi.run_id "
            "WHERE pi.budget_account IS NOT NULL "
            "GROUP BY pi.run_id, pi.budget_account "
            "UNION ALL "
            "SELECT 'grant' AS scope, pi.provider_grant_id AS scope_id, "
            f"pi.budget_account, SUM({amount}) AS consumed "
            "FROM provider_invocations pi "
            "JOIN selected_grants sg ON sg.id = pi.provider_grant_id "
            "WHERE pi.budget_account IS NOT NULL "
            "GROUP BY pi.provider_grant_id, pi.budget_account "
            "UNION ALL "
            "SELECT 'project' AS scope, r.project_id AS scope_id, "
            f"pi.budget_account, SUM({amount}) AS consumed "
            "FROM provider_invocations pi "
            "JOIN runs r ON r.id = pi.run_id "
            "JOIN selected_projects sp ON sp.id = r.project_id "
            "WHERE pi.budget_account IS NOT NULL "
            "GROUP BY r.project_id, pi.budget_account",
            run_ids,
        ).fetchall()
        scope_totals = {
            (row["scope"], row["scope_id"], row["budget_account"]): int(row["consumed"])
            for row in scope_rows
        }

        budgets: list[dict[str, Any]] = []
        empty = {
            "invocations": 0,
            "reported": 0,
            "conservative": 0,
            "active": 0,
            "consumed": 0,
            "accounted": 0,
            "reserved": 0,
            "estimated": 0,
            "input": 0,
            "cached": 0,
            "output": 0,
            "reasoning": 0,
        }
        for row, policy in parsed_authorities:
            metrics = exact.get(
                (row["run_id"], row["provider_grant_id"], policy.account), empty
            )
            scope_consumed = {
                "run": scope_totals.get(("run", row["run_id"], policy.account), 0),
                "grant": scope_totals.get(
                    ("grant", row["provider_grant_id"], policy.account), 0
                ),
                "project": scope_totals.get(
                    ("project", row["project_id"], policy.account), 0
                ),
            }
            ceilings = {
                "run": policy.run_microusd,
                "grant": policy.grant_microusd,
                "project": policy.project_microusd,
            }
            budgets.append(
                {
                    "run_id": row["run_id"],
                    "project_key": row["project_key"],
                    "authority_name": row["authority_name"],
                    "grant_id": row["provider_grant_id"],
                    "grant_name": row["grant_name"],
                    "account": policy.account,
                    "revision": policy.revision,
                    "policy_sha256": policy.digest,
                    "currency": policy.currency,
                    "invocation_microusd": policy.invocation_microusd,
                    "ceilings_microusd": ceilings,
                    "scope_consumed_microusd": scope_consumed,
                    "remaining_microusd": {
                        scope: max(0, ceilings[scope] - scope_consumed[scope])
                        for scope in ("run", "grant", "project")
                    },
                    "invocations": metrics["invocations"],
                    "reported_invocations": metrics["reported"],
                    "conservative_invocations": metrics["conservative"],
                    "active_reservations": metrics["active"],
                    "policy_consumed_microusd": metrics["consumed"],
                    "accounted_microusd": metrics["accounted"],
                    "reserved_microusd": metrics["reserved"],
                    "estimated_microusd": metrics["estimated"],
                    "usage": {
                        "input_tokens": metrics["input"],
                        "cached_input_tokens": metrics["cached"],
                        "output_tokens": metrics["output"],
                        "reasoning_output_tokens": metrics["reasoning"],
                        "total_tokens": metrics["input"] + metrics["output"],
                    },
                    "reconciled": (
                        metrics["active"] == 0 and metrics["conservative"] == 0
                    ),
                }
            )
        return budgets

    @staticmethod
    def _landing_dependencies(
        connection: sqlite3.Connection, history_limit: int
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            "WITH removed AS (SELECT id FROM work_item_landing_dependencies "
            "WHERE state = 'removed' ORDER BY updated_at DESC, id DESC LIMIT ?), "
            "visible AS (SELECT id FROM work_item_landing_dependencies "
            "WHERE state = 'active' UNION ALL SELECT id FROM removed) "
            "SELECT d.id, d.project_id, d.dependent_work_item_id, "
            "d.prerequisite_work_item_id, d.state, d.version, "
            "creator.handle AS created_by_handle, "
            "remover.handle AS removed_by_handle, d.created_at, d.updated_at, "
            "d.removed_at FROM visible v "
            "JOIN work_item_landing_dependencies d ON d.id = v.id "
            "JOIN users creator ON creator.id = d.created_by "
            "LEFT JOIN users remover ON remover.id = d.removed_by "
            "ORDER BY CASE d.state WHEN 'active' THEN 0 ELSE 1 END, "
            "d.updated_at DESC, d.id",
            (history_limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _integration_queue(
        self,
        connection: sqlite3.Connection,
        history_limit: int,
        generated_at: str,
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            "WITH terminal AS (SELECT id FROM integration_queue_items "
            "WHERE state IN ('landed', 'cancelled') "
            "ORDER BY updated_at DESC, id DESC LIMIT ?), visible AS ("
            "SELECT id, 0 AS group_order FROM integration_queue_items "
            "WHERE state NOT IN ('landed', 'cancelled') UNION ALL "
            "SELECT id, 1 AS group_order FROM terminal) "
            "SELECT q.id, q.integration_ref_id, q.project_id, "
            "p.krab_project_key AS project_key, q.work_item_id, q.work_thread_id, "
            "q.candidate_id, q.current_policy_id, q.current_fold_id, q.state, "
            "q.version, q.owner_id, q.lease_expires_at, q.landed_receipt_id, "
            "q.created_at, q.updated_at, q.landed_at, q.cancelled_at, "
            "r.integration_ref, r.current_revision, r.object_format, "
            "r.version AS integration_ref_version, "
            "u.handle AS created_by_handle, wiv.content_json, "
            "lp.id AS pending_plan_id, lp.created_at AS plan_issued_at, "
            "pl.id AS pending_provider_landing_id, "
            "pl.created_at AS provider_landing_issued_at, "
            "pl.capability_binding_id AS provider_capability_binding_id, "
            "pl.service_operation_id AS provider_service_operation_id, "
            "so.state AS provider_operation_state "
            "FROM visible v JOIN integration_queue_items q ON q.id = v.id "
            "JOIN integration_refs r ON r.id = q.integration_ref_id "
            "JOIN projects p ON p.id = q.project_id "
            "JOIN users u ON u.id = q.created_by "
            "LEFT JOIN work_item_versions wiv ON wiv.id = ("
            "SELECT latest.id FROM work_item_versions latest "
            "WHERE latest.work_item_id = q.work_item_id "
            "ORDER BY latest.version DESC LIMIT 1) "
            "LEFT JOIN integration_landing_plans lp "
            "ON lp.queue_item_id = q.id AND lp.state = 'pending' "
            "LEFT JOIN integration_provider_landings pl "
            "ON pl.queue_item_id = q.id AND pl.state = 'pending' "
            "LEFT JOIN service_operations so ON so.id = pl.service_operation_id "
            "ORDER BY v.group_order, q.updated_at DESC, q.id",
            (history_limit,),
        ).fetchall()
        now = _timestamp(generated_at, label="snapshot")
        queue: list[dict[str, Any]] = []
        for row in rows:
            try:
                title = (
                    None
                    if row["content_json"] is None
                    else json.loads(row["content_json"])["title"]
                )
            except (json.JSONDecodeError, KeyError, TypeError) as exc:
                raise StorageError("stored integration work item is invalid") from exc
            readiness = self.database._gate_readiness(  # noqa: SLF001
                connection, row["current_policy_id"]
            )
            blocked_dependencies = connection.execute(
                "SELECT COUNT(*) FROM work_item_landing_dependencies d "
                "WHERE d.dependent_work_item_id = ? AND d.state = 'active' "
                "AND NOT EXISTS (SELECT 1 FROM integration_queue_items prerequisite "
                "WHERE prerequisite.integration_ref_id = ? "
                "AND prerequisite.work_item_id = d.prerequisite_work_item_id "
                "AND prerequisite.state = 'landed')",
                (row["work_item_id"], row["integration_ref_id"]),
            ).fetchone()[0]
            lease_expired = (
                row["state"] == "claimed"
                and row["lease_expires_at"] is not None
                and _timestamp(row["lease_expires_at"], label="integration lease")
                <= now
            )
            item = dict(row)
            item.pop("content_json")
            item.update(
                {
                    "untrusted_work_item_title": title,
                    "blocked_dependency_count": blocked_dependencies,
                    "lease_expired": lease_expired,
                    "gates_ready": readiness["ready"],
                    "gate_passed_count": readiness["passed_count"],
                    "gate_failed_count": readiness["failed_count"],
                    "gate_pending_count": readiness["pending_count"],
                }
            )
            queue.append(item)
        return queue

    @staticmethod
    def _attach_run_counts(
        runs: list[dict[str, Any]],
        attempts: list[dict[str, Any]],
        interactions: list[dict[str, Any]],
        results: list[dict[str, Any]],
        candidates: list[dict[str, Any]],
        authorities: list[dict[str, Any]],
        provider_recoveries: list[dict[str, Any]],
    ) -> None:
        indexes: tuple[tuple[str, list[dict[str, Any]], str], ...] = (
            ("attempt_count", attempts, "run_id"),
            ("interaction_count", interactions, "run_id"),
            ("retained_result_count", results, "run_id"),
            ("candidate_count", candidates, "producer_run_id"),
            ("authority_count", authorities, "run_id"),
            ("provider_recovery_count", provider_recoveries, "run_id"),
        )
        counts: dict[str, dict[str, int]] = {}
        for label, records, run_key in indexes:
            current: dict[str, int] = defaultdict(int)
            for record in records:
                current[record[run_key]] += 1
            counts[label] = current
        for run in runs:
            for label, _, _ in indexes:
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
        action_states = states(
            "SELECT state, COUNT(*) AS count FROM supervisor_actions GROUP BY state"
        )
        escalation_states = states(
            "SELECT state, COUNT(*) AS count FROM supervisor_escalations GROUP BY state"
        )
        integration_states = states(
            "SELECT state, COUNT(*) AS count FROM integration_queue_items "
            "GROUP BY state"
        )
        dependency_states = states(
            "SELECT state, COUNT(*) AS count "
            "FROM work_item_landing_dependencies GROUP BY state"
        )
        workflow_states = states(
            "SELECT state, COUNT(*) AS count FROM workflow_executions GROUP BY state"
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
            "workflows": {
                "total": sum(workflow_states.values()),
                **{
                    state: workflow_states.get(state, 0)
                    for state in ("active", "cancelled", "completed")
                },
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
            "supervisor_actions": {
                "total": sum(action_states.values()),
                **{
                    state: action_states.get(state, 0)
                    for state in ("pending", "claimed", "succeeded", "exhausted")
                },
            },
            "escalations": {
                "total": sum(escalation_states.values()),
                **{
                    state: escalation_states.get(state, 0)
                    for state in (
                        "pending",
                        "claimed",
                        "delivered",
                        "dismissed",
                        "exhausted",
                    )
                },
            },
            "retained_results": connection.execute(
                "SELECT COUNT(*) FROM attempt_results"
            ).fetchone()[0],
            "candidates": connection.execute(
                "SELECT COUNT(*) FROM integration_candidates"
            ).fetchone()[0],
            "integration_gate_policies": connection.execute(
                "SELECT COUNT(*) FROM integration_gate_policies"
            ).fetchone()[0],
            "integration_gate_review_runs": connection.execute(
                "SELECT COUNT(*) FROM integration_gate_review_runs"
            ).fetchone()[0],
            "integration_gate_change_runs": connection.execute(
                "SELECT COUNT(*) FROM integration_gate_change_runs"
            ).fetchone()[0],
            "integration_maintainer_reviews": connection.execute(
                "SELECT COUNT(*) FROM integration_maintainer_reviews"
            ).fetchone()[0],
            "integration_gate_maintainer_change_runs": connection.execute(
                "SELECT COUNT(*) FROM integration_gate_maintainer_change_runs"
            ).fetchone()[0],
            "integration_gate_evidence": connection.execute(
                "SELECT COUNT(*) FROM integration_gate_evidence"
            ).fetchone()[0],
            "integration_queue": {
                "total": sum(integration_states.values()),
                **{
                    state: integration_states.get(state, 0)
                    for state in (
                        "queued",
                        "claimed",
                        "awaiting_receipt",
                        "refold_required",
                        "landed",
                        "cancelled",
                    )
                },
            },
            "landing_dependencies": {
                "total": sum(dependency_states.values()),
                "active": dependency_states.get("active", 0),
                "removed": dependency_states.get("removed", 0),
            },
            "pending_landing_plans": connection.execute(
                "SELECT COUNT(*) FROM integration_landing_plans WHERE state = 'pending'"
            ).fetchone()[0],
            "pending_provider_landings": connection.execute(
                "SELECT COUNT(*) FROM integration_provider_landings "
                "WHERE state = 'pending'"
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
        provider_recoveries: list[dict[str, Any]] | None = None,
        supervisor_actions: list[dict[str, Any]] | None = None,
        escalations: list[dict[str, Any]] | None = None,
        gate_policies: list[dict[str, Any]] | None = None,
        integration_queue: list[dict[str, Any]] | None = None,
        *,
        workflows: list[dict[str, Any]] | None = None,
        generated_at: str | None = None,
    ) -> list[dict[str, Any]]:
        attention: list[dict[str, Any]] = []
        now = (
            _timestamp(generated_at, label="snapshot")
            if generated_at is not None
            else None
        )

        def add(entry: dict[str, Any], age_started_at: object) -> None:
            if now is not None:
                started = _timestamp(age_started_at, label="attention age")
                entry["age_seconds"] = max(0, int((now - started).total_seconds()))
            attention.append(entry)

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
                add(
                    {
                        "kind": "interaction.pending",
                        "severity": "action",
                        "subject_type": "interaction",
                        "subject_id": interaction["id"],
                        "run_id": interaction["run_id"],
                        "interaction_kind": interaction["kind"],
                        "recipient_krab_handle": interaction["recipient_krab_handle"],
                        "created_at": interaction["created_at"],
                    },
                    interaction["created_at"],
                )
        for assignment in assignments:
            if assignment["thread_state"] != "active":
                continue
            if assignment["state"] == "open":
                add(
                    {
                        "kind": "assignment.open",
                        "severity": "action",
                        "subject_type": "assignment",
                        "subject_id": assignment["id"],
                        "work_item_id": assignment["work_item_id"],
                        "target_profile_name": assignment["target_profile_name"],
                        "created_at": assignment["created_at"],
                    },
                    assignment["created_at"],
                )
            elif assignment["state"] == "claimed" and assignment["lease_expired"]:
                add(
                    {
                        "kind": "assignment.lease_expired",
                        "severity": "warning",
                        "subject_type": "assignment",
                        "subject_id": assignment["id"],
                        "work_item_id": assignment["work_item_id"],
                        "claimed_by_run_id": assignment["claimed_by_run_id"],
                        "lease_expires_at": assignment["lease_expires_at"],
                    },
                    assignment["lease_expires_at"],
                )
        for item in work_items:
            if item["state"] == "active" and item["blocker_entries"]:
                add(
                    {
                        "kind": "work.blocker",
                        "severity": "warning",
                        "subject_type": "work_item",
                        "subject_id": item["id"],
                        "blocker_entries": item["blocker_entries"],
                        "last_blocker_at": item["last_blocker_at"],
                    },
                    item["last_blocker_at"],
                )
        for attempt in attempts:
            if attempt["cancel_requested_at"] is not None:
                add(
                    {
                        "kind": "run.cancellation_requested",
                        "severity": "action",
                        "subject_type": "attempt",
                        "subject_id": attempt["id"],
                        "run_id": attempt["run_id"],
                        "owner_id": attempt["owner_id"],
                        "requested_at": attempt["cancel_requested_at"],
                    },
                    attempt["cancel_requested_at"],
                )
            if attempt["lease_expired"]:
                add(
                    {
                        "kind": "attempt.lease_expired",
                        "severity": "warning",
                        "subject_type": "attempt",
                        "subject_id": attempt["id"],
                        "run_id": attempt["run_id"],
                        "owner_id": attempt["owner_id"],
                        "lease_expires_at": attempt["lease_expires_at"],
                    },
                    attempt["lease_expires_at"],
                )
        for recovery in provider_recoveries or []:
            if recovery["state"] != "active":
                continue
            eligible_at = _timestamp(
                recovery["eligible_at"], label="provider recovery eligibility"
            )
            entry = {
                "kind": "provider.paused",
                "severity": "warning",
                "subject_type": "provider_recovery",
                "subject_id": recovery["id"],
                "run_id": recovery["run_id"],
                "attempt_id": recovery["attempt_id"],
                "category": recovery["category"],
                "retry_after_seconds": recovery["retry_after_seconds"],
                "eligible_at": recovery["eligible_at"],
                "eligible": now is not None and eligible_at <= now,
                "created_at": recovery["created_at"],
            }
            add(entry, recovery["created_at"])
        for run_id, run in active_runs.items():
            if (
                run["state"] in {"preparing", "running"}
                and run_id not in active_attempt_runs
            ):
                add(
                    {
                        "kind": "run.missing_active_attempt",
                        "severity": "warning",
                        "subject_type": "run",
                        "subject_id": run_id,
                        "state": run["state"],
                        "updated_at": run["updated_at"],
                    },
                    run["updated_at"],
                )
            if (
                run["state"] == "waiting_for_input"
                and run_id not in open_interaction_runs
                and run_id not in active_attempt_runs
            ):
                add(
                    {
                        "kind": "run.waiting_without_interaction",
                        "severity": "warning",
                        "subject_type": "run",
                        "subject_id": run_id,
                        "updated_at": run["updated_at"],
                    },
                    run["updated_at"],
                )
        for authority in authorities:
            if authority["run_id"] in active_runs and not authority["available"]:
                age_started_at = authority.get("unavailable_since")
                if age_started_at is None:
                    age_started_at = generated_at
                add(
                    {
                        "kind": "authority.unavailable",
                        "severity": "warning",
                        "subject_type": "provider_grant",
                        "subject_id": authority["grant_id"],
                        "run_id": authority["run_id"],
                        "authority_name": authority["authority_name"],
                        "state": authority["state"],
                        "expires_at": authority["expires_at"],
                    },
                    age_started_at,
                )
        for action in supervisor_actions or []:
            if action["state"] == "exhausted":
                add(
                    {
                        "kind": "supervisor.action_exhausted",
                        "severity": "warning",
                        "subject_type": "supervisor_action",
                        "subject_id": action["id"],
                        "run_id": action["run_id"],
                        "workflow_execution_id": action["workflow_execution_id"],
                        "action_kind": action["kind"],
                        "last_error_code": action["last_error_code"],
                        "updated_at": action["updated_at"],
                    },
                    action["updated_at"],
                )
        for workflow in workflows or []:
            if workflow["open_ruling_id"] is not None:
                add(
                    {
                        "kind": "workflow.ruling_open",
                        "severity": "action",
                        "subject_type": "workflow_ruling",
                        "subject_id": workflow["open_ruling_id"],
                        "workflow_execution_id": workflow["id"],
                        "ruling_kind": workflow["open_ruling_kind"],
                        "ruling_version": workflow["open_ruling_version"],
                    },
                    workflow["open_ruling_created_at"],
                )
            if workflow["state"] != "active" or workflow["stage"] != "landing":
                continue
            queue_item_id = workflow["landing_queue_item_id"]
            queue_state = workflow["landing_queue_state"]
            if queue_item_id is None:
                continue
            if queue_state == "refold_required":
                add(
                    {
                        "kind": "workflow.refold_required",
                        "severity": "action",
                        "subject_type": "integration_queue_item",
                        "subject_id": queue_item_id,
                        "workflow_execution_id": workflow["id"],
                    },
                    workflow["landing_queue_updated_at"],
                )
            elif (
                workflow["landing_mode"] == "provider"
                and workflow["landing_provider_resource_id"] is None
            ):
                add(
                    {
                        "kind": "workflow.provider_target_required",
                        "severity": "action",
                        "subject_type": "workflow_execution",
                        "subject_id": workflow["id"],
                        "workflow_execution_id": workflow["id"],
                        "queue_item_id": queue_item_id,
                    },
                    workflow["landing_queue_updated_at"],
                )
            elif (
                workflow["landing_mode"] == "provider"
                and not workflow["landing_provider_available"]
            ):
                add(
                    {
                        "kind": "workflow.provider_capability_required",
                        "severity": "action",
                        "subject_type": "workflow_execution",
                        "subject_id": workflow["id"],
                        "workflow_execution_id": workflow["id"],
                        "queue_item_id": queue_item_id,
                        "capability": "landing.invoke@1",
                    },
                    workflow["landing_queue_updated_at"],
                )
            elif workflow["landing_mode"] == "manual":
                if workflow["landing_plan_id"] is None and queue_state in {
                    "queued",
                    "claimed",
                }:
                    add(
                        {
                            "kind": "workflow.manual_plan_required",
                            "severity": "action",
                            "subject_type": "workflow_execution",
                            "subject_id": workflow["id"],
                            "workflow_execution_id": workflow["id"],
                            "queue_item_id": queue_item_id,
                        },
                        workflow["landing_queue_updated_at"],
                    )
                elif (
                    workflow["landing_plan_state"] == "pending"
                    and queue_state == "awaiting_receipt"
                ):
                    add(
                        {
                            "kind": "workflow.manual_receipt_pending",
                            "severity": "action",
                            "subject_type": "landing_plan",
                            "subject_id": workflow["landing_plan_id"],
                            "workflow_execution_id": workflow["id"],
                            "queue_item_id": queue_item_id,
                        },
                        workflow["landing_queue_updated_at"],
                    )
                elif (
                    workflow["landing_plan_state"] == "not_landed"
                    and queue_state == "queued"
                ):
                    add(
                        {
                            "kind": "workflow.manual_retry_required",
                            "severity": "action",
                            "subject_type": "workflow_execution",
                            "subject_id": workflow["id"],
                            "workflow_execution_id": workflow["id"],
                            "queue_item_id": queue_item_id,
                            "prior_plan_id": workflow["landing_plan_id"],
                        },
                        workflow["landing_queue_updated_at"],
                    )
        for escalation in escalations or []:
            if escalation["state"] == "exhausted":
                add(
                    {
                        "kind": "supervisor.escalation_exhausted",
                        "severity": "warning",
                        "subject_type": "supervisor_escalation",
                        "subject_id": escalation["id"],
                        "run_id": escalation["run_id"],
                        "escalation_kind": escalation["kind"],
                        "last_error_code": escalation["last_error_code"],
                        "updated_at": escalation["updated_at"],
                    },
                    escalation["updated_at"],
                )
        for policy in gate_policies or []:
            if policy["ready"]:
                continue
            failed = policy["failed_count"]
            add(
                {
                    "kind": "gate.failed" if failed else "gate.pending",
                    "severity": "warning" if failed else "action",
                    "subject_type": "gate_policy",
                    "subject_id": policy["id"],
                    "candidate_id": policy["candidate_id"],
                    "passed_count": policy["passed_count"],
                    "failed_count": failed,
                    "pending_count": policy["pending_count"],
                    "created_at": policy["created_at"],
                },
                policy["latest_evidence_at"] or policy["created_at"],
            )
        for item in integration_queue or []:
            common = {
                "subject_type": "integration_queue_item",
                "subject_id": item["id"],
                "work_item_id": item["work_item_id"],
                "candidate_id": item["candidate_id"],
                "integration_ref": item["integration_ref"],
            }
            if item["state"] == "refold_required":
                add(
                    {
                        **common,
                        "kind": "integration.refold_required",
                        "severity": "action",
                        "updated_at": item["updated_at"],
                    },
                    item["updated_at"],
                )
            elif item["state"] == "awaiting_receipt":
                if item["pending_provider_landing_id"] is not None:
                    add(
                        {
                            **common,
                            "kind": "integration.provider_landing_pending",
                            "severity": "action",
                            "provider_landing_id": item["pending_provider_landing_id"],
                            "service_operation_id": item[
                                "provider_service_operation_id"
                            ],
                            "service_operation_state": item["provider_operation_state"],
                            "provider_landing_issued_at": item[
                                "provider_landing_issued_at"
                            ],
                        },
                        item["provider_landing_issued_at"] or item["updated_at"],
                    )
                else:
                    add(
                        {
                            **common,
                            "kind": "integration.receipt_pending",
                            "severity": "action",
                            "plan_id": item["pending_plan_id"],
                            "plan_issued_at": item["plan_issued_at"],
                        },
                        item["plan_issued_at"] or item["updated_at"],
                    )
            elif item["state"] == "claimed" and item["lease_expired"]:
                add(
                    {
                        **common,
                        "kind": "integration.claim_expired",
                        "severity": "warning",
                        "owner_id": item["owner_id"],
                        "lease_expires_at": item["lease_expires_at"],
                    },
                    item["lease_expires_at"],
                )
            elif item["state"] == "queued" and not item["gates_ready"]:
                add(
                    {
                        **common,
                        "kind": "integration.gates_pending",
                        "severity": (
                            "warning" if item["gate_failed_count"] else "action"
                        ),
                        "policy_id": item["current_policy_id"],
                        "failed_count": item["gate_failed_count"],
                        "pending_count": item["gate_pending_count"],
                    },
                    item["updated_at"],
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
                            else "Krab identity refresh failed"
                        ),
                    }
                )
                continue
            if actual.user_id != route["krab_user_id"]:
                errors.append(
                    {
                        "scope": "identity",
                        "identity_id": route["id"],
                        "krab_handle": route["handle"],
                        "error_type": "KrabError",
                        "message": (
                            "credential now belongs to a different Krab identity"
                        ),
                    }
                )
                continue
            try:
                visible = client.projects()
            except KrabError as exc:
                errors.append(
                    {
                        "scope": "identity",
                        "identity_id": route["id"],
                        "krab_handle": route["handle"],
                        "error_type": type(exc).__name__,
                        "message": "Krab identity refresh failed",
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
                            "message": "Krab work-claim refresh failed",
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
