from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from ..domain import (
    AgentProfileSpec,
    RunState,
    ValidationError,
    canonical_json,
    require_string,
    validate_krab_id,
    validate_name,
    validate_project_key,
    validate_transition,
)
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)


class RunStorage:
    def create_run(
        self,
        *,
        project_key: str,
        profile_name: str,
        work_item_id: str,
        requested_by: str,
        source_revision: str | None = None,
        krab_session_id: str | None = None,
        krab_claim_id: str | None = None,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        profile_name = validate_name(profile_name, field="profile name")
        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
        if source_revision is not None:
            source_revision = require_string(
                source_revision, field="source_revision", max_bytes=512
            )
        if krab_session_id is not None:
            krab_session_id = validate_krab_id(krab_session_id, "ses")
        if krab_claim_id is not None:
            krab_claim_id = validate_krab_id(krab_claim_id, "clm")
        run_id = self.new_id("run")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                project = connection.execute(
                    "SELECT id, active, krab_instance_id FROM projects "
                    "WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None or not project["active"]:
                    raise NotFoundError(f"active project not found: {project_key}")
                context = connection.execute(
                    "SELECT id FROM project_context_versions WHERE project_id = ? "
                    "ORDER BY version DESC LIMIT 1",
                    (project["id"],),
                ).fetchone()
                work_item_version = connection.execute(
                    "SELECT wiv.id, wt.id AS work_thread_id FROM work_items wi "
                    "JOIN work_item_versions wiv ON wiv.work_item_id = wi.id "
                    "JOIN work_threads wt ON wt.work_item_id = wi.id "
                    "WHERE wi.id = ? AND wi.project_id = ? "
                    "AND wt.state = 'active' "
                    "ORDER BY wiv.version DESC LIMIT 1",
                    (work_item_id, project["id"]),
                ).fetchone()
                if work_item_version is None:
                    known_work_item = connection.execute(
                        "SELECT 1 FROM work_items WHERE id = ? AND project_id = ?",
                        (work_item_id, project["id"]),
                    ).fetchone()
                    if known_work_item is not None:
                        raise ConflictError(
                            "work item is completed; reopen it before creating a run"
                        )
                    raise NotFoundError(
                        "work item not found for project: " + work_item_id
                    )
                work_item_version_id = work_item_version["id"]
                work_thread_id = work_item_version["work_thread_id"]
                profile = connection.execute(
                    "SELECT id, active FROM agent_profiles WHERE name = ?",
                    (profile_name,),
                ).fetchone()
                if profile is None or not profile["active"]:
                    raise NotFoundError(
                        f"active agent profile not found: {profile_name}"
                    )
                profile_version = connection.execute(
                    "SELECT av.id, av.spec_json, ki.krab_instance_id "
                    "FROM agent_profile_versions av "
                    "JOIN krab_identities ki ON ki.id = av.krab_identity_id "
                    "WHERE av.profile_id = ? ORDER BY av.version DESC LIMIT 1",
                    (profile["id"],),
                ).fetchone()
                if profile_version["krab_instance_id"] != project["krab_instance_id"]:
                    raise ConflictError(
                        "agent profile and project belong to different Krab instances"
                    )
                try:
                    profile_spec = AgentProfileSpec.from_mapping(
                        json.loads(profile_version["spec_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError("stored agent profile is invalid") from exc
                provider_grants = self._available_provider_grants(
                    connection, profile_spec
                )
                connection.execute(
                    "INSERT INTO runs "
                    "(id, project_id, context_version_id, profile_version_id, "
                    "work_item_version_id, work_thread_id, requested_by, "
                    "state, version, "
                    "source_revision, krab_session_id, krab_claim_id, created_at, "
                    "updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        project["id"],
                        context["id"],
                        profile_version["id"],
                        work_item_version_id,
                        work_thread_id,
                        requested_by,
                        source_revision,
                        krab_session_id,
                        krab_claim_id,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, metadata_json, "
                    "created_at) VALUES (?, 1, 'run.created', NULL, 'queued', '{}', ?)",
                    (run_id, now),
                )
                for grant in provider_grants:
                    connection.execute(
                        "INSERT INTO run_provider_grants "
                        "(run_id, authority_name, provider_grant_id, created_at) "
                        "VALUES (?, ?, ?, ?)",
                        (run_id, grant["name"], grant["id"], now),
                    )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError):
                connection.rollback()
                raise
            except (sqlite3.IntegrityError, TypeError) as exc:
                connection.rollback()
                raise ConflictError("run could not be created") from exc
        return self.get_run(run_id)

    def retry_run(
        self,
        run_id: str,
        *,
        requested_by: str,
        expected_run_version: int,
    ) -> dict[str, Any]:
        """Create a successor with the terminal run's exact immutable pins."""

        return self._create_replacement_run(
            run_id,
            requested_by=requested_by,
            expected_run_version=expected_run_version,
            replacement_kind="retry",
            profile_name=None,
        )

    def reassign_run(
        self,
        run_id: str,
        *,
        profile_name: str,
        requested_by: str,
        expected_run_version: int,
    ) -> dict[str, Any]:
        """Create a successor pinned to the named profile's latest revision."""

        return self._create_replacement_run(
            run_id,
            requested_by=requested_by,
            expected_run_version=expected_run_version,
            replacement_kind="reassign",
            profile_name=validate_name(profile_name, field="profile name"),
        )

    def _create_replacement_run(
        self,
        run_id: str,
        *,
        requested_by: str,
        expected_run_version: int,
        replacement_kind: str,
        profile_name: str | None,
    ) -> dict[str, Any]:
        run_id = require_string(run_id, field="run_id", max_bytes=64)
        requested_by = require_string(requested_by, field="requested_by", max_bytes=64)
        if (
            isinstance(expected_run_version, bool)
            or not isinstance(expected_run_version, int)
            or expected_run_version < 1
        ):
            raise ValidationError("expected_run_version must be a positive integer")
        if replacement_kind not in {"retry", "reassign"}:
            raise ValidationError("replacement_kind is invalid")
        if (replacement_kind == "retry") != (profile_name is None):
            raise ValidationError("replacement profile does not match the action")

        replacement_run_id = self.new_id("run")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                actor = connection.execute(
                    "SELECT active FROM users WHERE id = ?", (requested_by,)
                ).fetchone()
                if actor is None or not actor["active"]:
                    raise NotFoundError("replacement operator is not an active user")
                source = connection.execute(
                    "SELECT r.project_id, r.context_version_id, "
                    "r.profile_version_id, r.work_item_version_id, "
                    "r.work_thread_id, r.state, r.version, r.source_revision, "
                    "r.krab_session_id, r.krab_claim_id, p.active AS project_active, "
                    "p.krab_instance_id, wt.state AS thread_state, "
                    "ap.active AS profile_active, ap.name AS profile_name "
                    "FROM runs r JOIN projects p ON p.id = r.project_id "
                    "JOIN work_threads wt ON wt.id = r.work_thread_id "
                    "JOIN agent_profile_versions av "
                    "ON av.id = r.profile_version_id JOIN agent_profiles ap "
                    "ON ap.id = av.profile_id WHERE r.id = ?",
                    (run_id,),
                ).fetchone()
                if source is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if source["version"] != expected_run_version:
                    raise ConcurrentUpdateError(
                        f"run version is {source['version']}, "
                        f"expected {expected_run_version}"
                    )
                if source["state"] not in {"succeeded", "failed", "cancelled"}:
                    raise ConflictError(f"run is not terminal: {source['state']}")
                if source["thread_state"] != "active":
                    raise ConflictError("run work thread is not active")
                if not source["project_active"]:
                    raise ConflictError("run project is not active")
                active_successor = connection.execute(
                    "SELECT id FROM runs WHERE predecessor_run_id = ? "
                    "AND state IN ('queued','preparing','running','waiting_for_input')",
                    (run_id,),
                ).fetchone()
                if active_successor is not None:
                    raise ConflictError(
                        "run already has an active successor: " + active_successor["id"]
                    )

                if replacement_kind == "retry":
                    if not source["profile_active"]:
                        raise ConflictError("run profile is not active")
                    profile_version_id = source["profile_version_id"]
                    authority_rows = connection.execute(
                        "SELECT rpg.authority_name, pg.id, pg.state, pg.expires_at "
                        "FROM run_provider_grants rpg JOIN provider_grants pg "
                        "ON pg.id = rpg.provider_grant_id WHERE rpg.run_id = ? "
                        "ORDER BY rpg.authority_name",
                        (run_id,),
                    ).fetchall()
                    unavailable = next(
                        (
                            row["authority_name"]
                            for row in authority_rows
                            if row["state"] != "active"
                            or (
                                row["expires_at"] is not None
                                and row["expires_at"] <= now
                            )
                        ),
                        None,
                    )
                    if unavailable is not None:
                        raise ConflictError(
                            f"pinned provider grant is unavailable: {unavailable}"
                        )
                    authorities = [
                        (row["authority_name"], row["id"]) for row in authority_rows
                    ]
                else:
                    if profile_name == source["profile_name"]:
                        raise ConflictError(
                            "target profile is already assigned; use retry"
                        )
                    target = connection.execute(
                        "SELECT av.id AS profile_version_id, av.spec_json, "
                        "av.content_sha256, ki.krab_instance_id "
                        "FROM agent_profiles ap JOIN agent_profile_versions av "
                        "ON av.profile_id = ap.id JOIN krab_identities ki "
                        "ON ki.id = av.krab_identity_id WHERE ap.name = ? "
                        "AND ap.active = 1 ORDER BY av.version DESC LIMIT 1",
                        (profile_name,),
                    ).fetchone()
                    if target is None:
                        raise NotFoundError(
                            f"active agent profile not found: {profile_name}"
                        )
                    if target["krab_instance_id"] != source["krab_instance_id"]:
                        raise ConflictError(
                            "agent profile and project belong to different "
                            "Krab instances"
                        )
                    try:
                        target_spec = AgentProfileSpec.from_mapping(
                            json.loads(target["spec_json"])
                        )
                    except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                        raise StorageError("stored agent profile is invalid") from exc
                    if target_spec.digest != target["content_sha256"]:
                        raise StorageError("stored agent profile digest does not match")
                    profile_version_id = target["profile_version_id"]
                    authority_rows = self._available_provider_grants(
                        connection, target_spec
                    )
                    authorities = [(row["name"], row["id"]) for row in authority_rows]

                connection.execute(
                    "INSERT INTO runs "
                    "(id, project_id, context_version_id, profile_version_id, "
                    "work_item_version_id, work_thread_id, requested_by, state, "
                    "version, source_revision, krab_session_id, krab_claim_id, "
                    "created_at, updated_at, predecessor_run_id, replacement_kind) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        replacement_run_id,
                        source["project_id"],
                        source["context_version_id"],
                        profile_version_id,
                        source["work_item_version_id"],
                        source["work_thread_id"],
                        requested_by,
                        source["source_revision"],
                        source["krab_session_id"],
                        source["krab_claim_id"],
                        now,
                        now,
                        run_id,
                        replacement_kind,
                    ),
                )
                created_metadata = canonical_json(
                    {
                        "predecessor_run_id": run_id,
                        "replacement_kind": replacement_kind,
                    }
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) "
                    "VALUES (?, 1, 'run.created', NULL, 'queued', ?, ?)",
                    (replacement_run_id, created_metadata, now),
                )
                for authority_name, grant_id in authorities:
                    connection.execute(
                        "INSERT INTO run_provider_grants "
                        "(run_id, authority_name, provider_grant_id, created_at) "
                        "VALUES (?, ?, ?, ?)",
                        (
                            replacement_run_id,
                            authority_name,
                            grant_id,
                            now,
                        ),
                    )

                changed = connection.execute(
                    "UPDATE runs SET version = version + 1, updated_at = ? "
                    "WHERE id = ? AND version = ?",
                    (now, run_id, expected_run_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                event_kind = (
                    "run.retry_created"
                    if replacement_kind == "retry"
                    else "run.reassigned"
                )
                event_metadata: dict[str, Any] = {
                    "replacement_run_id": replacement_run_id,
                    "requested_by": requested_by,
                }
                if replacement_kind == "reassign":
                    event_metadata["profile_version_id"] = profile_version_id
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        expected_run_version + 1,
                        event_kind,
                        source["state"],
                        source["state"],
                        canonical_json(event_metadata),
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("replacement run could not be created") from exc
        return self.get_run(replacement_run_id)

    @staticmethod
    def _cancel_open_interactions(
        connection: sqlite3.Connection, run_id: str, now: str
    ) -> None:
        interactions = connection.execute(
            "SELECT id, version FROM interactions WHERE run_id = ? AND state = 'open'",
            (run_id,),
        ).fetchall()
        for interaction in interactions:
            version = interaction["version"]
            connection.execute(
                "UPDATE interactions SET state = 'cancelled', "
                "version = version + 1, updated_at = ?, resolved_at = ? "
                "WHERE id = ? AND version = ?",
                (now, now, interaction["id"], version),
            )
            connection.execute(
                "INSERT INTO interaction_events "
                "(interaction_id, sequence, kind, from_state, to_state, "
                "krab_item_id, created_at) VALUES (?, ?, "
                "'interaction.cancelled', 'open', 'cancelled', NULL, ?)",
                (interaction["id"], version + 1, now),
            )

    def request_run_cancellation(
        self,
        run_id: str,
        *,
        requested_by: str,
        expected_run_version: int,
        reason: str,
    ) -> dict[str, Any]:
        """Request stop for an active attempt without claiming it has stopped."""

        requested_by = require_string(requested_by, field="requested_by", max_bytes=64)
        reason = require_string(reason, field="reason", max_bytes=4096)
        if (
            isinstance(expected_run_version, bool)
            or not isinstance(expected_run_version, int)
            or expected_run_version < 1
        ):
            raise ValidationError("expected_run_version must be a positive integer")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                actor = connection.execute(
                    "SELECT active FROM users WHERE id = ?", (requested_by,)
                ).fetchone()
                if actor is None or not actor["active"]:
                    raise NotFoundError("cancellation operator is not an active user")
                run = connection.execute(
                    "SELECT state, version FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                if run is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if run["version"] != expected_run_version:
                    raise ConcurrentUpdateError(
                        f"run version is {run['version']}, "
                        f"expected {expected_run_version}"
                    )
                state = RunState(run["state"])
                if state in {
                    RunState.SUCCEEDED,
                    RunState.FAILED,
                    RunState.CANCELLED,
                }:
                    raise ConflictError(f"run is already terminal: {state.value}")
                attempt = connection.execute(
                    "SELECT id, cancel_requested_at FROM run_attempts "
                    "WHERE run_id = ? AND state IN ('preparing', 'running')",
                    (run_id,),
                ).fetchone()
                metadata = canonical_json(
                    {"reason": reason, "requested_by": requested_by}
                )
                if attempt is None:
                    if state not in {
                        RunState.QUEUED,
                        RunState.WAITING_FOR_INPUT,
                    }:
                        raise ConflictError("active run has no active attempt")
                    self._cancel_open_interactions(connection, run_id, now)
                    changed = connection.execute(
                        "UPDATE runs SET state = 'cancelled', "
                        "version = version + 1, updated_at = ?, finished_at = ? "
                        "WHERE id = ? AND version = ?",
                        (now, now, run_id, expected_run_version),
                    ).rowcount
                    kind = "run.transitioned"
                    target = RunState.CANCELLED.value
                else:
                    if attempt["cancel_requested_at"] is not None:
                        raise ConflictError("run cancellation is already requested")
                    connection.execute(
                        "UPDATE run_attempts SET cancel_requested_at = ?, "
                        "cancel_requested_by = ?, cancel_reason = ? WHERE id = ?",
                        (now, requested_by, reason, attempt["id"]),
                    )
                    changed = connection.execute(
                        "UPDATE runs SET version = version + 1, updated_at = ? "
                        "WHERE id = ? AND version = ?",
                        (now, run_id, expected_run_version),
                    ).rowcount
                    kind = "run.cancel_requested"
                    target = state.value
                if changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        expected_run_version + 1,
                        kind,
                        state.value,
                        target,
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
                raise ConflictError("run cancellation could not be requested") from exc
        return self.get_run(run_id)

    def transition_run(
        self,
        run_id: str,
        *,
        target: RunState,
        expected_version: int,
        reason: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        source_revision: str | None = None,
    ) -> dict[str, Any]:
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 1
        ):
            raise ValidationError("expected_version must be a positive integer")
        if reason is not None:
            reason = require_string(reason, field="reason", max_bytes=4096)
        if metadata is None:
            event_metadata: dict[str, Any] = {}
        elif not isinstance(metadata, Mapping) or not all(
            isinstance(key, str) for key in metadata
        ):
            raise ValidationError("transition metadata must be an object")
        else:
            event_metadata = dict(metadata)
        if "reason" in event_metadata:
            raise ValidationError("transition metadata uses reserved field: reason")
        if reason is not None:
            event_metadata["reason"] = reason
        metadata_json = canonical_json(event_metadata)
        if len(metadata_json.encode("utf-8")) > 16 * 1024:
            raise ValidationError("transition metadata exceeds 16384 UTF-8 bytes")
        if source_revision is not None:
            if target is not RunState.RUNNING:
                raise ValidationError(
                    "source revision can only be pinned when entering running"
                )
            source_revision = require_string(
                source_revision, field="source_revision", max_bytes=512
            )
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, version, source_revision, started_at "
                    "FROM runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"run version is {row['version']}, expected {expected_version}"
                    )
                source = RunState(row["state"])
                validate_transition(source, target)
                open_interactions = connection.execute(
                    "SELECT id, version FROM interactions "
                    "WHERE run_id = ? AND state = 'open'",
                    (run_id,),
                ).fetchall()
                if open_interactions and target in {
                    RunState.QUEUED,
                    RunState.RUNNING,
                }:
                    raise ConflictError(
                        "run cannot resume while it has an open interaction"
                    )
                if open_interactions and target in {
                    RunState.FAILED,
                    RunState.CANCELLED,
                }:
                    for interaction in open_interactions:
                        interaction_version = interaction["version"]
                        connection.execute(
                            "UPDATE interactions SET state = 'cancelled', "
                            "version = version + 1, updated_at = ?, resolved_at = ? "
                            "WHERE id = ? AND version = ?",
                            (
                                now,
                                now,
                                interaction["id"],
                                interaction_version,
                            ),
                        )
                        connection.execute(
                            "INSERT INTO interaction_events "
                            "(interaction_id, sequence, kind, from_state, to_state, "
                            "krab_item_id, created_at) "
                            "VALUES (?, ?, 'interaction.cancelled', 'open', "
                            "'cancelled', NULL, ?)",
                            (
                                interaction["id"],
                                interaction_version + 1,
                                now,
                            ),
                        )
                terminal = target in {
                    RunState.SUCCEEDED,
                    RunState.FAILED,
                    RunState.CANCELLED,
                }
                started_at = row["started_at"]
                if target is RunState.RUNNING and started_at is None:
                    started_at = now
                finished_at = now if terminal else None
                pinned_revision = (
                    source_revision
                    if source_revision is not None
                    else row["source_revision"]
                )
                changed = connection.execute(
                    "UPDATE runs SET state = ?, version = version + 1, "
                    "updated_at = ?, started_at = ?, finished_at = ?, "
                    "source_revision = ? "
                    "WHERE id = ? AND version = ?",
                    (
                        target.value,
                        now,
                        started_at,
                        finished_at,
                        pinned_revision,
                        run_id,
                        expected_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, metadata_json, "
                    "created_at) VALUES (?, ?, 'run.transitioned', ?, ?, ?, ?)",
                    (
                        run_id,
                        expected_version + 1,
                        source.value,
                        target.value,
                        metadata_json,
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("run transition could not be recorded") from exc
        return self.get_run(run_id)
