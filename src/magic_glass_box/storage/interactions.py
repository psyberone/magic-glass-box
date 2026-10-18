from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any

from ..domain import (
    InteractionKind,
    InteractionState,
    ProviderGrantSpec,
    RunState,
    ValidationError,
    canonical_json,
    require_string,
    validate_krab_id,
    validate_provider_name,
)
from ..inference import (
    CONNECTOR_ID_RE,
    PROVIDER_INVOCATION_ID_RE,
    SHA256_RE,
    SUPPORTED_INFERENCE_CONNECTOR_PROTOCOL_VERSIONS,
    InferenceBudgetPolicy,
    InferenceConnectorSpec,
    InferenceGrantSpec,
    InferenceUsage,
)
from .errors import (
    BudgetExceededError,
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)

RUN_ID_RE = re.compile(r"^run_[0-9a-f]{32}$")
PROVIDER_GRANT_ID_RE = re.compile(r"^pgr_[0-9a-f]{32}$")
PROVIDER_DISPATCH_ID_RE = re.compile(r"^pdr_[0-9a-f]{32}$")
MAX_PROVIDER_INVOCATION_HISTORY = 1000


class InteractionStorage:
    def open_interaction(
        self,
        run_id: str,
        *,
        kind: InteractionKind,
        recipient_krab_identity_id: str,
        request_item_id: str,
        expected_run_version: int,
    ) -> dict[str, Any]:
        if not isinstance(kind, InteractionKind):
            raise ValidationError("interaction kind is invalid")
        recipient_krab_identity_id = require_string(
            recipient_krab_identity_id,
            field="recipient_krab_identity_id",
            max_bytes=64,
        )
        request_item_id = validate_krab_id(request_item_id, "itm")
        if (
            isinstance(expected_run_version, bool)
            or not isinstance(expected_run_version, int)
            or expected_run_version < 1
        ):
            raise ValidationError("expected_run_version must be a positive integer")

        interaction_id = self.new_id("int")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                run = connection.execute(
                    "SELECT r.state, r.version, r.krab_session_id, "
                    "p.krab_instance_id FROM runs r "
                    "JOIN projects p ON p.id = r.project_id WHERE r.id = ?",
                    (run_id,),
                ).fetchone()
                if run is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if run["version"] != expected_run_version:
                    raise ConcurrentUpdateError(
                        f"run version is {run['version']}, "
                        f"expected {expected_run_version}"
                    )
                if run["state"] != RunState.RUNNING.value:
                    raise ConflictError(f"run is not running: {run['state']}")
                if run["krab_session_id"] is None:
                    raise ConflictError("run has no Krab session")
                active_attempt = connection.execute(
                    "SELECT id FROM run_attempts WHERE run_id = ? "
                    "AND state = 'running'",
                    (run_id,),
                ).fetchone()

                recipient = connection.execute(
                    "SELECT ki.krab_instance_id, ki.krab_user_id, "
                    "ub.user_id, u.handle AS local_user_handle, u.active "
                    "FROM krab_identities ki "
                    "LEFT JOIN user_krab_bindings ub "
                    "ON ub.krab_identity_id = ki.id "
                    "LEFT JOIN users u ON u.id = ub.user_id WHERE ki.id = ?",
                    (recipient_krab_identity_id,),
                ).fetchone()
                if recipient is None:
                    raise NotFoundError("recipient Krab identity not found")
                if recipient["krab_instance_id"] != run["krab_instance_id"]:
                    raise ConflictError(
                        "recipient belongs to a different Krab instance"
                    )
                if recipient["user_id"] is None or not recipient["active"]:
                    raise NotFoundError(
                        "recipient Krab identity has no active bound local user"
                    )

                connection.execute(
                    "INSERT INTO interactions "
                    "(id, run_id, kind, state, version, "
                    "recipient_krab_identity_id, request_item_id, response_item_id, "
                    "created_at, updated_at, resolved_at, attempt_id) "
                    "VALUES (?, ?, ?, 'open', 1, ?, ?, NULL, ?, ?, NULL, ?)",
                    (
                        interaction_id,
                        run_id,
                        kind.value,
                        recipient_krab_identity_id,
                        request_item_id,
                        now,
                        now,
                        None if active_attempt is None else active_attempt["id"],
                    ),
                )
                changed = connection.execute(
                    "UPDATE runs SET state = 'waiting_for_input', "
                    "version = version + 1, updated_at = ? "
                    "WHERE id = ? AND version = ?",
                    (now, run_id, expected_run_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                connection.execute(
                    "INSERT INTO interaction_events "
                    "(interaction_id, sequence, kind, from_state, to_state, "
                    "krab_item_id, created_at) "
                    "VALUES (?, 1, 'interaction.opened', NULL, 'open', ?, ?)",
                    (interaction_id, request_item_id, now),
                )
                metadata = canonical_json(
                    {
                        "interaction_id": interaction_id,
                        "interaction_kind": kind.value,
                        "recipient_krab_user_id": recipient["krab_user_id"],
                        "request_item_id": request_item_id,
                    }
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) "
                    "VALUES (?, ?, 'run.transitioned', 'running', "
                    "'waiting_for_input', ?, ?)",
                    (run_id, expected_run_version + 1, metadata, now),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("interaction could not be opened") from exc
        return self.get_interaction(interaction_id)

    def answer_interaction(
        self,
        interaction_id: str,
        *,
        response_item_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        response_item_id = validate_krab_id(response_item_id, "itm")
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
                interaction = connection.execute(
                    "SELECT i.state, i.version, i.request_item_id, i.run_id, "
                    "r.state AS run_state, r.version AS run_version, "
                    "u.active AS recipient_active, "
                    "EXISTS (SELECT 1 FROM run_attempts ra "
                    "WHERE ra.run_id = r.id "
                    "AND ra.state IN ('preparing', 'running')) AS active_attempt, "
                    "EXISTS (SELECT 1 FROM provider_recoveries pr "
                    "WHERE pr.run_id = r.id AND pr.state = 'active') "
                    "AS active_provider_recovery "
                    "FROM interactions i JOIN runs r ON r.id = i.run_id "
                    "JOIN krab_identities ki "
                    "ON ki.id = i.recipient_krab_identity_id "
                    "JOIN user_krab_bindings ub "
                    "ON ub.krab_identity_id = ki.id "
                    "JOIN users u ON u.id = ub.user_id "
                    "WHERE i.id = ?",
                    (interaction_id,),
                ).fetchone()
                if interaction is None:
                    raise NotFoundError(f"interaction not found: {interaction_id}")
                if interaction["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"interaction version is {interaction['version']}, "
                        f"expected {expected_version}"
                    )
                if interaction["state"] != InteractionState.OPEN.value:
                    raise ConflictError(
                        f"interaction is not open: {interaction['state']}"
                    )
                if interaction["recipient_active"] != 1:
                    raise ConflictError("interaction recipient is inactive")
                if response_item_id == interaction["request_item_id"]:
                    raise ValidationError(
                        "response item must differ from the request item"
                    )
                waiting = interaction["run_state"] == RunState.WAITING_FOR_INPUT.value
                provider_paused = (
                    interaction["run_state"] == RunState.PAUSED.value
                    and interaction["active_provider_recovery"] == 1
                )
                if not waiting and not provider_paused:
                    raise ConflictError(
                        "interaction run is not waiting for input: "
                        + interaction["run_state"]
                    )

                changed = connection.execute(
                    "UPDATE interactions SET state = 'answered', "
                    "version = version + 1, response_item_id = ?, updated_at = ?, "
                    "resolved_at = ? WHERE id = ? AND version = ?",
                    (
                        response_item_id,
                        now,
                        now,
                        interaction_id,
                        expected_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("interaction changed concurrently")
                connection.execute(
                    "INSERT INTO interaction_events "
                    "(interaction_id, sequence, kind, from_state, to_state, "
                    "krab_item_id, created_at) "
                    "VALUES (?, ?, 'interaction.answered', 'open', 'answered', ?, ?)",
                    (
                        interaction_id,
                        expected_version + 1,
                        response_item_id,
                        now,
                    ),
                )
                if not interaction["active_attempt"] and waiting:
                    run_changed = connection.execute(
                        "UPDATE runs SET state = 'queued', version = version + 1, "
                        "updated_at = ? WHERE id = ? AND version = ?",
                        (
                            now,
                            interaction["run_id"],
                            interaction["run_version"],
                        ),
                    ).rowcount
                    if run_changed != 1:
                        raise ConcurrentUpdateError("run changed concurrently")
                    metadata = canonical_json(
                        {
                            "interaction_id": interaction_id,
                            "response_item_id": response_item_id,
                        }
                    )
                    connection.execute(
                        "INSERT INTO run_events "
                        "(run_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) "
                        "VALUES (?, ?, 'run.transitioned', 'waiting_for_input', "
                        "'queued', ?, ?)",
                        (
                            interaction["run_id"],
                            interaction["run_version"] + 1,
                            metadata,
                            now,
                        ),
                    )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("interaction could not be answered") from exc
        return self.get_interaction(interaction_id)

    def get_interaction(self, interaction_id: str) -> dict[str, Any]:
        with self._read_snapshot() as connection:
            return self._get_interaction(connection, interaction_id)

    def _get_interaction(
        self, connection: sqlite3.Connection, interaction_id: str
    ) -> dict[str, Any]:
        with self._connection(connection) as connection:
            row = connection.execute(
                "SELECT i.id, i.run_id, i.kind, i.state, i.version, "
                "i.attempt_id, i.request_item_id, i.response_item_id, i.created_at, "
                "i.updated_at, i.resolved_at, r.state AS run_state, "
                "r.version AS run_version, r.krab_session_id, "
                "p.krab_project_key AS project_key, "
                "ki.id AS recipient_krab_identity_id, "
                "ki.krab_user_id AS recipient_krab_user_id, "
                "ki.handle AS recipient_krab_handle, "
                "u.id AS recipient_local_user_id, "
                "u.handle AS recipient_local_user_handle "
                "FROM interactions i JOIN runs r ON r.id = i.run_id "
                "JOIN projects p ON p.id = r.project_id "
                "JOIN krab_identities ki "
                "ON ki.id = i.recipient_krab_identity_id "
                "JOIN user_krab_bindings ub "
                "ON ub.krab_identity_id = ki.id "
                "JOIN users u ON u.id = ub.user_id WHERE i.id = ?",
                (interaction_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"interaction not found: {interaction_id}")
            events = connection.execute(
                "SELECT sequence, kind, from_state, to_state, krab_item_id, "
                "created_at FROM interaction_events "
                "WHERE interaction_id = ? ORDER BY sequence",
                (interaction_id,),
            ).fetchall()
        result = dict(row)
        result["recipient"] = {
            "local_user_id": result.pop("recipient_local_user_id"),
            "local_user_handle": result.pop("recipient_local_user_handle"),
            "krab_identity_id": result.pop("recipient_krab_identity_id"),
            "krab_user_id": result.pop("recipient_krab_user_id"),
            "krab_handle": result.pop("recipient_krab_handle"),
        }
        result["events"] = [dict(event) for event in events]
        return result

    def list_run_interactions(self, run_id: str) -> list[dict[str, Any]]:
        with self._read_snapshot() as connection:
            return self._list_run_interactions(connection, run_id)

    def _list_run_interactions(
        self, connection: sqlite3.Connection, run_id: str
    ) -> list[dict[str, Any]]:
        with self._connection(connection) as connection:
            exists = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                raise NotFoundError(f"run not found: {run_id}")
            rows = connection.execute(
                "SELECT id FROM interactions WHERE run_id = ? ORDER BY created_at, id",
                (run_id,),
            ).fetchall()
        return [self._get_interaction(connection, row["id"]) for row in rows]

    def get_run_provider_grant(
        self, run_id: str, authority_name: str
    ) -> dict[str, Any]:
        authority_name = validate_provider_name(
            authority_name, field="provider authority name"
        )
        with self._connection() as connection:
            mapping = connection.execute(
                "SELECT provider_grant_id FROM run_provider_grants "
                "WHERE run_id = ? AND authority_name = ?",
                (run_id, authority_name),
            ).fetchone()
            if mapping is None:
                raise NotFoundError(
                    f"provider grant is not authorized for run: {authority_name}"
                )
            result = self._provider_grant_record(
                connection,
                grant_id=mapping["provider_grant_id"],
                include_reference=True,
            )
        if result["state"] != "active" or (
            result["expires_at"] is not None and result["expires_at"] <= self.now()
        ):
            raise ConflictError(f"provider grant is unavailable: {authority_name}")
        if (
            result["grant_kind"] == "connector"
            and result["connector"]["state"] != "active"
        ):
            raise ConflictError(f"provider grant is unavailable: {authority_name}")
        result["authority_name"] = authority_name
        return result

    def reserve_provider_dispatch(
        self,
        run_id: str,
        authority_name: str,
        *,
        expected_grant_id: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        """Atomically recheck authority and claim one grant concurrency slot."""

        if not isinstance(run_id, str) or RUN_ID_RE.fullmatch(run_id) is None:
            raise ValidationError("invalid run identifier")
        authority_name = validate_provider_name(
            authority_name, field="provider authority name"
        )
        if (
            not isinstance(expected_grant_id, str)
            or PROVIDER_GRANT_ID_RE.fullmatch(expected_grant_id) is None
        ):
            raise ValidationError("invalid provider grant identifier")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or not 1 <= lease_seconds <= 300
        ):
            raise ValidationError(
                "provider dispatch lease_seconds must be between 1 and 300"
            )
        reservation_id = self.new_id("pdr")
        now = self.now()
        expires_at = (
            datetime.fromisoformat(now) + timedelta(seconds=lease_seconds)
        ).isoformat(timespec="microseconds")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "DELETE FROM provider_dispatch_reservations WHERE expires_at <= ?",
                    (now,),
                )
                row = connection.execute(
                    "SELECT r.state AS run_state, pg.id AS grant_id, "
                    "pg.state AS grant_state, pg.grant_kind, "
                    "pg.expires_at AS grant_expires_at, pg.policy_json, "
                    "pg.content_sha256, ra.id AS attempt_id, "
                    "ra.state AS attempt_state, pcb.inference_connector_id, "
                    "pcb.deployment_sha256 AS binding_deployment_sha256, "
                    "pcb.route AS binding_route, "
                    "pcb.credential_scope_sha256 AS binding_scope_sha256, "
                    "pcb.provider AS binding_provider, "
                    "pcb.protocol AS binding_protocol, pcb.model AS binding_model, "
                    "ic.name AS connector_name, ic.state AS connector_state, "
                    "ic.connector_protocol_version, "
                    "ic.spec_json AS connector_spec_json, "
                    "ic.content_sha256 AS connector_content_sha256, "
                    "ic.socket_path "
                    "FROM runs r JOIN run_provider_grants rpg "
                    "ON rpg.run_id = r.id JOIN provider_grants pg "
                    "ON pg.id = rpg.provider_grant_id "
                    "LEFT JOIN provider_connector_bindings pcb "
                    "ON pcb.provider_grant_id = pg.id "
                    "LEFT JOIN inference_connectors ic "
                    "ON ic.id = pcb.inference_connector_id "
                    "LEFT JOIN run_attempts ra ON ra.run_id = r.id "
                    "AND ra.state IN ('preparing', 'running') "
                    "WHERE r.id = ? AND rpg.authority_name = ?",
                    (run_id, authority_name),
                ).fetchone()
                if row is None:
                    raise NotFoundError(
                        f"provider grant is not authorized for run: {authority_name}"
                    )
                if row["grant_id"] != expected_grant_id:
                    raise ConflictError(
                        f"provider grant is unavailable: {authority_name}"
                    )
                if (
                    row["run_state"]
                    not in (RunState.RUNNING.value, RunState.WAITING_FOR_INPUT.value)
                    or row["attempt_state"] != "running"
                ):
                    raise ConflictError(f"run is not running: {row['run_state']}")
                if row["grant_state"] != "active" or (
                    row["grant_expires_at"] is not None
                    and row["grant_expires_at"] <= now
                ):
                    raise ConflictError(
                        f"provider grant is unavailable: {authority_name}"
                    )
                if row["grant_kind"] != "connector":
                    raise ConflictError(
                        f"provider grant is unavailable: {authority_name}"
                    )
                if row["connector_state"] != "active":
                    raise ConflictError(
                        f"provider grant is unavailable: {authority_name}"
                    )
                try:
                    spec = InferenceGrantSpec.from_mapping(
                        json.loads(row["policy_json"])
                    )
                    connector_spec = InferenceConnectorSpec.from_mapping(
                        json.loads(row["connector_spec_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError("stored inference authority is invalid") from exc
                if spec.digest != row["content_sha256"]:
                    raise StorageError(
                        "stored provider grant policy digest does not match"
                    )
                if connector_spec.digest != row["connector_content_sha256"]:
                    raise StorageError(
                        "stored inference connector digest does not match"
                    )
                try:
                    connector_spec.validate_grant(row["connector_name"], spec)
                except ValidationError as exc:
                    raise StorageError(
                        "stored inference grant does not match its connector"
                    ) from exc
                binding = (
                    row["binding_deployment_sha256"],
                    row["binding_route"],
                    row["binding_scope_sha256"],
                    row["binding_provider"],
                    row["binding_protocol"],
                    row["binding_model"],
                )
                if binding != (
                    spec.deployment_sha256,
                    spec.route,
                    spec.credential_scope_sha256,
                    spec.provider,
                    spec.protocol,
                    spec.model,
                ):
                    raise StorageError(
                        "stored inference grant binding does not match its policy"
                    )
                active = connection.execute(
                    "SELECT COUNT(*) FROM provider_dispatch_reservations "
                    "WHERE provider_grant_id = ? AND expires_at > ?",
                    (expected_grant_id, now),
                ).fetchone()[0]
                if active >= spec.max_concurrency:
                    connection.commit()
                    return None
                connector_active = connection.execute(
                    "SELECT COUNT(*) FROM provider_dispatch_reservations pdr "
                    "JOIN provider_connector_bindings pcb "
                    "ON pcb.provider_grant_id = pdr.provider_grant_id "
                    "WHERE pcb.inference_connector_id = ? AND pdr.expires_at > ?",
                    (row["inference_connector_id"], now),
                ).fetchone()[0]
                if connector_active >= connector_spec.max_concurrency:
                    connection.commit()
                    return None
                connection.execute(
                    "INSERT INTO provider_dispatch_reservations "
                    "(id, run_id, provider_grant_id, attempt_id, reserved_at, "
                    "expires_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        reservation_id,
                        run_id,
                        expected_grant_id,
                        row["attempt_id"],
                        now,
                        expires_at,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("provider dispatch could not be reserved") from exc
        return {
            "id": reservation_id,
            "run_id": run_id,
            "provider_grant_id": expected_grant_id,
            "attempt_id": row["attempt_id"],
            "reserved_at": now,
            "expires_at": expires_at,
            "inference": {
                "connector_id": row["inference_connector_id"],
                "connector_protocol_version": row["connector_protocol_version"],
                "deployment_sha256": spec.deployment_sha256,
                "route": spec.route,
                "policy_sha256": spec.digest,
                "credential_scope_sha256": spec.credential_scope_sha256,
                "provider": spec.provider,
                "protocol": spec.protocol,
                "model": spec.model,
                "request_bytes": spec.request_bytes,
                "response_bytes": spec.response_bytes,
                "timeout_ms": spec.timeout_ms,
                "socket_path": row["socket_path"],
            },
        }

    def renew_provider_dispatch(
        self,
        reservation_id: str,
        *,
        lease_seconds: int,
    ) -> dict[str, Any]:
        """Extend one still-live dispatch claim without resurrecting it."""

        if (
            not isinstance(reservation_id, str)
            or PROVIDER_DISPATCH_ID_RE.fullmatch(reservation_id) is None
        ):
            raise ValidationError("invalid provider dispatch identifier")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or not 1 <= lease_seconds <= 300
        ):
            raise ValidationError(
                "provider dispatch lease_seconds must be between 1 and 300"
            )
        now = self.now()
        expires_at = (
            datetime.fromisoformat(now) + timedelta(seconds=lease_seconds)
        ).isoformat(timespec="microseconds")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                renewed = connection.execute(
                    "UPDATE provider_dispatch_reservations SET expires_at = ? "
                    "WHERE id = ? AND expires_at > ?",
                    (expires_at, reservation_id, now),
                ).rowcount
                if renewed != 1:
                    raise ConflictError("provider dispatch reservation is unavailable")
                row = connection.execute(
                    "SELECT id, run_id, provider_grant_id, attempt_id, "
                    "reserved_at, expires_at FROM provider_dispatch_reservations "
                    "WHERE id = ?",
                    (reservation_id,),
                ).fetchone()
                if row is None:
                    raise StorageError("provider dispatch renewal was not retained")
                connection.commit()
            except (ConflictError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("provider dispatch could not be renewed") from exc
        return dict(row)

    def release_provider_dispatch(self, reservation_id: str) -> bool:
        if (
            not isinstance(reservation_id, str)
            or PROVIDER_DISPATCH_ID_RE.fullmatch(reservation_id) is None
        ):
            raise ValidationError("invalid provider dispatch identifier")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                removed = connection.execute(
                    "DELETE FROM provider_dispatch_reservations WHERE id = ?",
                    (reservation_id,),
                ).rowcount
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("provider dispatch could not be released") from exc
        return removed == 1

    def list_run_provider_grants(self, run_id: str) -> list[dict[str, Any]]:
        with self._read_snapshot() as connection:
            return self._list_run_provider_grants(connection, run_id)

    def _list_run_provider_grants(
        self, connection: sqlite3.Connection, run_id: str
    ) -> list[dict[str, Any]]:
        with self._connection(connection) as connection:
            exists = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                raise NotFoundError(f"run not found: {run_id}")
            rows = connection.execute(
                "SELECT rpg.authority_name, pg.id AS grant_id, pg.name, "
                "pg.state, pg.grant_kind, pg.expires_at, pg.policy_json, "
                "pg.content_sha256, ic.state AS connector_state "
                "FROM run_provider_grants rpg JOIN provider_grants pg "
                "ON pg.id = rpg.provider_grant_id "
                "LEFT JOIN provider_connector_bindings pcb "
                "ON pcb.provider_grant_id = pg.id "
                "LEFT JOIN inference_connectors ic "
                "ON ic.id = pcb.inference_connector_id "
                "WHERE rpg.run_id = ? "
                "ORDER BY rpg.authority_name",
                (run_id,),
            ).fetchall()
        now = self.now()
        result: list[dict[str, Any]] = []
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
            state = row["state"]
            if (
                state == "active"
                and row["expires_at"] is not None
                and row["expires_at"] <= now
            ):
                state = "expired"
            elif (
                state == "active"
                and row["grant_kind"] == "connector"
                and row["connector_state"] != "active"
            ):
                state = "unavailable"
            result.append(
                {
                    "authority_name": row["authority_name"],
                    "grant_id": row["grant_id"],
                    "name": row["name"],
                    "provider": spec.provider,
                    "model": spec.model,
                    "grant_kind": row["grant_kind"],
                    "state": state,
                    "expires_at": row["expires_at"],
                }
            )
        return result

    def record_provider_invocation(
        self,
        *,
        run_id: str,
        provider_grant_id: str,
        invocation_id: str | None = None,
        dispatch_kind: str = "direct",
        inference_connector_id: str | None = None,
        connector_protocol_version: int | None = None,
        deployment_sha256: str | None = None,
        route: str | None = None,
        policy_sha256: str | None = None,
        credential_scope_sha256: str | None = None,
        provider: str | None = None,
        protocol: str | None = None,
        model: str | None = None,
        deadline: str | None = None,
        outcome: str,
        request_bytes: int,
        response_bytes: int,
        http_status: int | None,
        error_code: str | None,
        started_at: str,
        budget: InferenceBudgetPolicy | None = None,
    ) -> dict[str, Any]:
        if outcome not in {
            "dispatching",
            "succeeded",
            "failed",
            "denied",
            "busy",
            "ambiguous",
        }:
            raise ValidationError("provider invocation outcome is invalid")
        if dispatch_kind not in {"direct", "connector"}:
            raise ValidationError("provider invocation dispatch_kind is invalid")
        for field, value in (
            ("request_bytes", request_bytes),
            ("response_bytes", response_bytes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValidationError(f"{field} must be a non-negative integer")
        if http_status is not None and (
            isinstance(http_status, bool)
            or not isinstance(http_status, int)
            or not 100 <= http_status <= 599
        ):
            raise ValidationError("http_status must be between 100 and 599")
        if error_code is not None:
            error_code = require_string(error_code, field="error_code", max_bytes=64)
        if outcome == "dispatching":
            if (
                dispatch_kind != "connector"
                or request_bytes != 0
                or response_bytes != 0
                or http_status is not None
                or error_code is not None
            ):
                raise ValidationError("dispatching provider invocation is inconsistent")
        elif outcome == "succeeded":
            if http_status is None or not 200 <= http_status < 300 or error_code:
                raise ValidationError("successful provider invocation is inconsistent")
        elif error_code is None:
            raise ValidationError("failed provider invocation requires an error_code")
        connector_values = (
            inference_connector_id,
            connector_protocol_version,
            deployment_sha256,
            route,
            policy_sha256,
            credential_scope_sha256,
            provider,
            protocol,
            model,
            deadline,
        )
        if dispatch_kind == "direct":
            if any(value is not None for value in connector_values):
                raise ValidationError(
                    "direct provider invocation cannot contain connector authority"
                )
        else:
            if (
                not isinstance(inference_connector_id, str)
                or CONNECTOR_ID_RE.fullmatch(inference_connector_id) is None
            ):
                raise ValidationError("invalid inference connector identifier")
            if (
                connector_protocol_version
                not in SUPPORTED_INFERENCE_CONNECTOR_PROTOCOL_VERSIONS
            ):
                raise ValidationError("invalid inference connector protocol version")
            for field, value in (
                ("deployment_sha256", deployment_sha256),
                ("policy_sha256", policy_sha256),
                ("credential_scope_sha256", credential_scope_sha256),
            ):
                if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
                    raise ValidationError(f"{field} must be a lowercase SHA-256 digest")
            route = validate_provider_name(route, field="provider invocation route")
            provider = validate_provider_name(
                provider, field="provider invocation provider"
            )
            protocol = validate_provider_name(
                protocol, field="provider invocation protocol"
            )
            model = require_string(
                model, field="provider invocation model", max_bytes=256
            )
            if not model.strip():
                raise ValidationError("provider invocation model must not be blank")
            deadline = self._timestamp(deadline, field="deadline")
        if budget is not None and not isinstance(budget, InferenceBudgetPolicy):
            raise ValidationError("budget must be an InferenceBudgetPolicy")
        if dispatch_kind == "direct" and budget is not None:
            raise ValidationError("direct provider invocation cannot contain a budget")
        if outcome == "dispatching":
            if connector_protocol_version == 2 and budget is None:
                raise ValidationError("protocol version 2 dispatch requires a budget")
            if connector_protocol_version == 1 and budget is not None:
                raise ValidationError("protocol version 1 cannot contain a budget")
        started_at = self._timestamp(started_at, field="started_at")
        if invocation_id is None:
            invocation_id = self.new_id("pinv")
        elif (
            not isinstance(invocation_id, str)
            or PROVIDER_INVOCATION_ID_RE.fullmatch(invocation_id) is None
        ):
            raise ValidationError("invalid provider invocation identifier")
        finished_at = None if outcome == "dispatching" else self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                authority = connection.execute(
                    "SELECT pg.grant_kind, pg.content_sha256, pg.policy_json, "
                    "r.project_id, "
                    "pcb.inference_connector_id, "
                    "pcb.deployment_sha256, pcb.route, "
                    "pcb.credential_scope_sha256, pcb.provider, pcb.protocol, "
                    "pcb.model, ic.connector_protocol_version "
                    "FROM run_provider_grants rpg "
                    "JOIN runs r ON r.id = rpg.run_id "
                    "JOIN provider_grants pg ON pg.id = rpg.provider_grant_id "
                    "LEFT JOIN provider_connector_bindings pcb "
                    "ON pcb.provider_grant_id = pg.id "
                    "LEFT JOIN inference_connectors ic "
                    "ON ic.id = pcb.inference_connector_id "
                    "WHERE rpg.run_id = ? AND rpg.provider_grant_id = ?",
                    (run_id, provider_grant_id),
                ).fetchone()
                if authority is None:
                    raise ConflictError(
                        "provider grant is not pinned to the requested run"
                    )
                if authority["grant_kind"] != dispatch_kind:
                    raise ConflictError(
                        "provider invocation dispatch kind does not match its grant"
                    )
                if dispatch_kind == "connector" and (
                    authority["content_sha256"],
                    authority["inference_connector_id"],
                    authority["connector_protocol_version"],
                    authority["deployment_sha256"],
                    authority["route"],
                    authority["credential_scope_sha256"],
                    authority["provider"],
                    authority["protocol"],
                    authority["model"],
                ) != (
                    policy_sha256,
                    inference_connector_id,
                    connector_protocol_version,
                    deployment_sha256,
                    route,
                    credential_scope_sha256,
                    provider,
                    protocol,
                    model,
                ):
                    raise ConflictError(
                        "provider invocation authority does not match its grant"
                    )
                budget_values: tuple[Any, ...] = (None,) * 9
                if budget is not None:
                    try:
                        stored_spec = InferenceGrantSpec.from_mapping(
                            json.loads(authority["policy_json"])
                        )
                    except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                        raise StorageError(
                            "stored inference grant budget is invalid"
                        ) from exc
                    if (
                        stored_spec.digest != authority["content_sha256"]
                        or stored_spec.budget != budget
                    ):
                        raise ConflictError(
                            "provider invocation budget does not match its grant"
                        )
                    mismatched_currency = connection.execute(
                        "SELECT 1 FROM provider_invocations WHERE budget_account = ? "
                        "AND budget_currency != ? LIMIT 1",
                        (budget.account, budget.currency),
                    ).fetchone()
                    if mismatched_currency is not None:
                        raise StorageError(
                            "provider budget account currency is inconsistent"
                        )
                    scope_queries = (
                        (
                            "run",
                            budget.run_microusd,
                            "SELECT COALESCE(SUM(CASE "
                            "WHEN outcome = 'dispatching' THEN reserved_microusd "
                            "ELSE accounted_microusd END), 0) "
                            "FROM provider_invocations "
                            "WHERE run_id = ? AND budget_account = ?",
                            (run_id, budget.account),
                        ),
                        (
                            "grant",
                            budget.grant_microusd,
                            "SELECT COALESCE(SUM(CASE "
                            "WHEN outcome = 'dispatching' THEN reserved_microusd "
                            "ELSE accounted_microusd END), 0) "
                            "FROM provider_invocations "
                            "WHERE provider_grant_id = ? AND budget_account = ?",
                            (provider_grant_id, budget.account),
                        ),
                        (
                            "project",
                            budget.project_microusd,
                            "SELECT COALESCE(SUM(CASE "
                            "WHEN outcome = 'dispatching' THEN reserved_microusd "
                            "ELSE accounted_microusd END), 0) "
                            "FROM provider_invocations pi "
                            "JOIN runs r ON r.id = pi.run_id "
                            "WHERE r.project_id = ? AND pi.budget_account = ?",
                            (authority["project_id"], budget.account),
                        ),
                    )
                    for scope, ceiling, query, parameters in scope_queries:
                        consumed = int(
                            connection.execute(query, parameters).fetchone()[0]
                        )
                        if consumed + budget.invocation_microusd > ceiling:
                            raise BudgetExceededError(
                                f"{scope} provider budget is exhausted"
                            )
                    budget_values = (
                        budget.account,
                        budget.revision,
                        budget.digest,
                        budget.currency,
                        budget.canonical,
                        budget.invocation_microusd,
                        None,
                        None,
                        "reserved",
                    )
                connection.execute(
                    "INSERT INTO provider_invocations "
                    "(id, run_id, provider_grant_id, dispatch_kind, "
                    "inference_connector_id, connector_protocol_version, "
                    "deployment_sha256, route, policy_sha256, "
                    "credential_scope_sha256, provider, protocol, model, deadline, "
                    "outcome, request_bytes, response_bytes, http_status, "
                    "error_code, budget_account, budget_revision, "
                    "budget_policy_sha256, budget_currency, budget_policy_json, "
                    "reserved_microusd, accounted_microusd, estimated_microusd, "
                    "accounting_status, started_at, finished_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        invocation_id,
                        run_id,
                        provider_grant_id,
                        dispatch_kind,
                        inference_connector_id,
                        connector_protocol_version,
                        deployment_sha256,
                        route,
                        policy_sha256,
                        credential_scope_sha256,
                        provider,
                        protocol,
                        model,
                        deadline,
                        outcome,
                        request_bytes,
                        response_bytes,
                        http_status,
                        error_code,
                        *budget_values,
                        started_at,
                        finished_at,
                    ),
                )
                connection.commit()
            except (ConflictError, BudgetExceededError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "provider invocation could not be recorded"
                ) from exc
        with self._connection() as connection:
            row = connection.execute(
                "SELECT id, run_id, provider_grant_id, dispatch_kind, "
                "inference_connector_id, connector_protocol_version, "
                "deployment_sha256, route, policy_sha256, "
                "credential_scope_sha256, provider, protocol, model, deadline, "
                "outcome, request_bytes, response_bytes, http_status, error_code, "
                "budget_account, budget_revision, budget_policy_sha256, "
                "budget_currency, budget_policy_json, reserved_microusd, "
                "accounted_microusd, estimated_microusd, accounting_status, "
                "input_tokens, cached_input_tokens, output_tokens, "
                "reasoning_output_tokens, started_at, finished_at "
                "FROM provider_invocations WHERE id = ?",
                (invocation_id,),
            ).fetchone()
        if row is None:
            raise StorageError("provider invocation was not recorded")
        return dict(row)

    def finalize_provider_invocation(
        self,
        invocation_id: str,
        *,
        outcome: str,
        request_bytes: int,
        response_bytes: int,
        http_status: int | None,
        error_code: str | None,
        usage: InferenceUsage | None = None,
        release_budget: bool = False,
        recovery_category: str | None = None,
        retry_after_seconds: int | None = None,
    ) -> dict[str, Any]:
        if (
            not isinstance(invocation_id, str)
            or PROVIDER_INVOCATION_ID_RE.fullmatch(invocation_id) is None
        ):
            raise ValidationError("invalid provider invocation identifier")
        if outcome not in {"succeeded", "failed", "denied", "busy", "ambiguous"}:
            raise ValidationError("provider invocation outcome is invalid")
        for field, value in (
            ("request_bytes", request_bytes),
            ("response_bytes", response_bytes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValidationError(f"{field} must be a non-negative integer")
        if http_status is not None and (
            isinstance(http_status, bool)
            or not isinstance(http_status, int)
            or not 100 <= http_status <= 599
        ):
            raise ValidationError("http_status must be between 100 and 599")
        if error_code is not None:
            error_code = require_string(error_code, field="error_code", max_bytes=64)
        if usage is not None and not isinstance(usage, InferenceUsage):
            raise ValidationError("usage must be InferenceUsage")
        if not isinstance(release_budget, bool):
            raise ValidationError("release_budget must be boolean")
        if usage is not None and release_budget:
            raise ValidationError("reported usage cannot release its budget")
        if (recovery_category is None) != (retry_after_seconds is None):
            raise ValidationError("provider recovery metadata is incomplete")
        if recovery_category is not None:
            recovery_category, retry_after_seconds = self._validate_provider_recovery(
                category=recovery_category,
                retry_after_seconds=retry_after_seconds,
            )
            if outcome != "failed":
                raise ValidationError(
                    "provider recovery requires an explicitly failed invocation"
                )
        if outcome == "succeeded":
            if http_status is None or not 200 <= http_status < 300 or error_code:
                raise ValidationError("successful provider invocation is inconsistent")
        elif error_code is None:
            raise ValidationError("failed provider invocation requires an error_code")
        finished_at = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                pending = connection.execute(
                    "SELECT budget_policy_json, budget_policy_sha256, "
                    "reserved_microusd FROM provider_invocations "
                    "WHERE id = ? AND outcome = 'dispatching'",
                    (invocation_id,),
                ).fetchone()
                if pending is None:
                    exists = connection.execute(
                        "SELECT 1 FROM provider_invocations WHERE id = ?",
                        (invocation_id,),
                    ).fetchone()
                    if exists is None:
                        raise NotFoundError(
                            f"provider invocation not found: {invocation_id}"
                        )
                    raise ConflictError("provider invocation is already final")
                accounting_values: tuple[Any, ...] = (None,) * 7
                if pending["budget_policy_json"] is None:
                    if usage is not None:
                        raise ValidationError(
                            "unbudgeted provider invocation cannot contain accounting"
                        )
                else:
                    try:
                        budget = InferenceBudgetPolicy.from_mapping(
                            json.loads(pending["budget_policy_json"])
                        )
                    except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                        raise StorageError(
                            "stored provider invocation budget is invalid"
                        ) from exc
                    if budget.digest != pending["budget_policy_sha256"]:
                        raise StorageError(
                            "stored provider invocation budget digest does not match"
                        )
                    reserved = int(pending["reserved_microusd"])
                    if usage is not None:
                        estimate = budget.estimate_microusd(usage)
                        if estimate > reserved:
                            raise ConflictError(
                                "provider usage exceeds its reserved budget"
                            )
                        accounting_values = (
                            estimate,
                            estimate,
                            "reported",
                            usage.input_tokens,
                            usage.cached_input_tokens,
                            usage.output_tokens,
                            usage.reasoning_output_tokens,
                        )
                    elif release_budget:
                        accounting_values = (
                            0,
                            None,
                            "released",
                            None,
                            None,
                            None,
                            None,
                        )
                    else:
                        accounting_values = (
                            reserved,
                            None,
                            "conservative",
                            None,
                            None,
                            None,
                            None,
                        )
                    if outcome == "succeeded" and usage is None:
                        raise ValidationError(
                            "successful budgeted invocation requires usage"
                        )
                changed = connection.execute(
                    "UPDATE provider_invocations SET outcome = ?, request_bytes = ?, "
                    "response_bytes = ?, http_status = ?, error_code = ?, "
                    "accounted_microusd = ?, estimated_microusd = ?, "
                    "accounting_status = ?, input_tokens = ?, "
                    "cached_input_tokens = ?, output_tokens = ?, "
                    "reasoning_output_tokens = ?, finished_at = ? "
                    "WHERE id = ? AND outcome = 'dispatching'",
                    (
                        outcome,
                        request_bytes,
                        response_bytes,
                        http_status,
                        error_code,
                        *accounting_values,
                        finished_at,
                        invocation_id,
                    ),
                ).rowcount
                if changed != 1:
                    raise StorageError(
                        "provider invocation finalization was not retained"
                    )
                if recovery_category is not None:
                    assert retry_after_seconds is not None
                    self._pause_run_for_provider_recovery(
                        connection,
                        invocation_id=invocation_id,
                        category=recovery_category,
                        retry_after_seconds=retry_after_seconds,
                        now=finished_at,
                    )
                row = connection.execute(
                    "SELECT id, run_id, provider_grant_id, dispatch_kind, "
                    "inference_connector_id, connector_protocol_version, "
                    "deployment_sha256, route, policy_sha256, "
                    "credential_scope_sha256, provider, protocol, model, deadline, "
                    "outcome, request_bytes, response_bytes, http_status, error_code, "
                    "budget_account, budget_revision, budget_policy_sha256, "
                    "budget_currency, budget_policy_json, reserved_microusd, "
                    "accounted_microusd, estimated_microusd, accounting_status, "
                    "input_tokens, cached_input_tokens, output_tokens, "
                    "reasoning_output_tokens, started_at, finished_at "
                    "FROM provider_invocations WHERE id = ?",
                    (invocation_id,),
                ).fetchone()
                assert row is not None
                result = dict(row)
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "provider invocation could not be finalized"
                ) from exc
        return result

    def list_provider_invocations(
        self,
        run_id: str,
        *,
        limit: int = MAX_PROVIDER_INVOCATION_HISTORY,
    ) -> list[dict[str, Any]]:
        with self._read_snapshot() as connection:
            return self._list_provider_invocations(connection, run_id, limit=limit)

    def _list_provider_invocations(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        limit: int = MAX_PROVIDER_INVOCATION_HISTORY,
    ) -> list[dict[str, Any]]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_PROVIDER_INVOCATION_HISTORY
        ):
            raise ValidationError(
                "provider invocation history limit must be between 1 and "
                f"{MAX_PROVIDER_INVOCATION_HISTORY}"
            )
        with self._connection(connection) as connection:
            exists = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                raise NotFoundError(f"run not found: {run_id}")
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            budget_columns = (
                "budget_account, budget_revision, budget_policy_sha256, "
                "budget_currency, budget_policy_json, reserved_microusd, "
                "accounted_microusd, estimated_microusd, accounting_status, "
                "input_tokens, cached_input_tokens, output_tokens, "
                "reasoning_output_tokens, "
                if version >= 19
                else ""
            )
            columns = (
                "id, run_id, provider_grant_id, dispatch_kind, "
                "inference_connector_id, connector_protocol_version, "
                "deployment_sha256, route, policy_sha256, "
                "credential_scope_sha256, provider, protocol, model, deadline, "
                "outcome, request_bytes, response_bytes, http_status, error_code, "
                + budget_columns
                + "started_at, finished_at"
            )
            rows = connection.execute(
                "WITH latest AS (SELECT "  # noqa: S608 - columns are fixed by the local schema version
                + columns
                + " FROM provider_invocations WHERE run_id = ? "
                "ORDER BY started_at DESC, id DESC LIMIT ?) "
                "SELECT * FROM latest ORDER BY started_at, id",
                (run_id, limit),
            ).fetchall()
        results = [dict(row) for row in rows]
        if version < 19:
            for result in results:
                for field in (
                    "budget_account",
                    "budget_revision",
                    "budget_policy_sha256",
                    "budget_currency",
                    "budget_policy_json",
                    "reserved_microusd",
                    "accounted_microusd",
                    "estimated_microusd",
                    "accounting_status",
                    "input_tokens",
                    "cached_input_tokens",
                    "output_tokens",
                    "reasoning_output_tokens",
                ):
                    result[field] = None
        return results
