from __future__ import annotations

import json
import sqlite3
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
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)


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
                    "EXISTS (SELECT 1 FROM run_attempts ra "
                    "WHERE ra.run_id = r.id "
                    "AND ra.state IN ('preparing', 'running')) AS active_attempt "
                    "FROM interactions i JOIN runs r ON r.id = i.run_id "
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
                if response_item_id == interaction["request_item_id"]:
                    raise ValidationError(
                        "response item must differ from the request item"
                    )
                if interaction["run_state"] != RunState.WAITING_FOR_INPUT.value:
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
                if not interaction["active_attempt"]:
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
        result["authority_name"] = authority_name
        return result

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
                "pg.state, pg.expires_at, pg.policy_json, pg.content_sha256 "
                "FROM run_provider_grants rpg JOIN provider_grants pg "
                "ON pg.id = rpg.provider_grant_id WHERE rpg.run_id = ? "
                "ORDER BY rpg.authority_name",
                (run_id,),
            ).fetchall()
        now = self.now()
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                spec = ProviderGrantSpec.from_mapping(json.loads(row["policy_json"]))
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
            result.append(
                {
                    "authority_name": row["authority_name"],
                    "grant_id": row["grant_id"],
                    "name": row["name"],
                    "provider": spec.provider,
                    "model": spec.model,
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
        outcome: str,
        request_bytes: int,
        response_bytes: int,
        http_status: int | None,
        error_code: str | None,
        started_at: str,
    ) -> dict[str, Any]:
        if outcome not in {"succeeded", "failed", "denied", "busy"}:
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
        if outcome == "succeeded":
            if http_status is None or not 200 <= http_status < 300 or error_code:
                raise ValidationError("successful provider invocation is inconsistent")
        elif error_code is None:
            raise ValidationError("failed provider invocation requires an error_code")
        started_at = self._timestamp(started_at, field="started_at")
        invocation_id = self.new_id("pinv")
        finished_at = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                authority = connection.execute(
                    "SELECT 1 FROM run_provider_grants "
                    "WHERE run_id = ? AND provider_grant_id = ?",
                    (run_id, provider_grant_id),
                ).fetchone()
                if authority is None:
                    raise ConflictError(
                        "provider grant is not pinned to the requested run"
                    )
                connection.execute(
                    "INSERT INTO provider_invocations "
                    "(id, run_id, provider_grant_id, outcome, request_bytes, "
                    "response_bytes, http_status, error_code, started_at, "
                    "finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        invocation_id,
                        run_id,
                        provider_grant_id,
                        outcome,
                        request_bytes,
                        response_bytes,
                        http_status,
                        error_code,
                        started_at,
                        finished_at,
                    ),
                )
                connection.commit()
            except ConflictError:
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "provider invocation could not be recorded"
                ) from exc
        with self._connection() as connection:
            row = connection.execute(
                "SELECT id, run_id, provider_grant_id, outcome, request_bytes, "
                "response_bytes, http_status, error_code, started_at, finished_at "
                "FROM provider_invocations WHERE id = ?",
                (invocation_id,),
            ).fetchone()
        if row is None:
            raise StorageError("provider invocation was not recorded")
        return dict(row)

    def list_provider_invocations(self, run_id: str) -> list[dict[str, Any]]:
        with self._read_snapshot() as connection:
            return self._list_provider_invocations(connection, run_id)

    def _list_provider_invocations(
        self, connection: sqlite3.Connection, run_id: str
    ) -> list[dict[str, Any]]:
        with self._connection(connection) as connection:
            exists = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                raise NotFoundError(f"run not found: {run_id}")
            rows = connection.execute(
                "SELECT id, run_id, provider_grant_id, outcome, request_bytes, "
                "response_bytes, http_status, error_code, started_at, finished_at "
                "FROM provider_invocations WHERE run_id = ? "
                "ORDER BY started_at, id",
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]
