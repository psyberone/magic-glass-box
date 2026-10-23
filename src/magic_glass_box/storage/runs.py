from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from ..domain import (
    AgentProfileSpec,
    RunState,
    SupervisionPolicy,
    ValidationError,
    WorkLogEntry,
    WorkLogKind,
    WorkspaceMode,
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
        supervision_policy: SupervisionPolicy | None = None,
        gate_policy_id: str | None = None,
        change_policy_id: str | None = None,
        change_review_evidence_id: str | None = None,
        change_review_entry_id: str | None = None,
        change_maintainer_review_id: str | None = None,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        profile_name = validate_name(profile_name, field="profile name")
        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
        if source_revision is not None:
            source_revision = require_string(
                source_revision, field="source_revision", max_bytes=512
            )
        if gate_policy_id is not None:
            gate_policy_id = require_string(
                gate_policy_id, field="gate_policy_id", max_bytes=64
            )
        agent_feedback = (
            change_review_evidence_id is not None or change_review_entry_id is not None
        )
        maintainer_feedback = change_maintainer_review_id is not None
        if change_policy_id is not None or agent_feedback or maintainer_feedback:
            if change_policy_id is None:
                raise ValidationError("change-request binding is incomplete")
            if agent_feedback == maintainer_feedback:
                raise ValidationError(
                    "change request must select exactly one feedback source"
                )
            if agent_feedback and (
                change_review_evidence_id is None or change_review_entry_id is None
            ):
                raise ValidationError("change-request binding is incomplete")
            if gate_policy_id is not None:
                raise ValidationError(
                    "a run cannot be both a review and changes-requested run"
                )
            change_policy_id = require_string(
                change_policy_id, field="change_policy_id", max_bytes=64
            )
            if agent_feedback:
                change_review_evidence_id = require_string(
                    change_review_evidence_id,
                    field="change_review_evidence_id",
                    max_bytes=64,
                )
                change_review_entry_id = require_string(
                    change_review_entry_id,
                    field="change_review_entry_id",
                    max_bytes=64,
                )
            else:
                change_maintainer_review_id = require_string(
                    change_maintainer_review_id,
                    field="change_maintainer_review_id",
                    max_bytes=64,
                )
        if krab_session_id is not None:
            krab_session_id = validate_krab_id(krab_session_id, "ses")
        if krab_claim_id is not None:
            krab_claim_id = validate_krab_id(krab_claim_id, "clm")
        if supervision_policy is None:
            supervision_policy = SupervisionPolicy.disabled()
        elif not isinstance(supervision_policy, SupervisionPolicy):
            raise ValidationError("supervision_policy must be a SupervisionPolicy")
        run_id = self.new_id("run")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, requested_by, role="run requester")
                project = connection.execute(
                    "SELECT id, active, krab_instance_id FROM projects "
                    "WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None or not project["active"]:
                    raise NotFoundError(f"active project not found: {project_key}")
                gate_policy = None
                change_target = None
                maintainer_change_target = None
                bound_policy_id = gate_policy_id or change_policy_id
                if bound_policy_id is not None:
                    gate_policy = connection.execute(
                        "SELECT gp.id, gp.candidate_id, gp.project_id, "
                        "gp.context_version_id, gp.work_thread_id, "
                        "gp.head_revision, gp.review_separation, "
                        "producer_profile.profile_id AS producer_profile_id, "
                        "producer_profile.krab_identity_id "
                        "AS producer_identity_id "
                        "FROM integration_gate_policies gp "
                        "JOIN integration_candidates ic "
                        "ON ic.id = gp.candidate_id "
                        "JOIN run_attempts producer_attempt "
                        "ON producer_attempt.id = ic.producer_attempt_id "
                        "JOIN agent_profile_versions producer_profile "
                        "ON producer_profile.id = producer_attempt.profile_version_id "
                        "WHERE gp.id = ?",
                        (bound_policy_id,),
                    ).fetchone()
                    if gate_policy is None:
                        raise NotFoundError(f"gate policy not found: {bound_policy_id}")
                    if gate_policy["project_id"] != project["id"]:
                        raise ConflictError("gate policy belongs to another project")
                    if (
                        gate_policy_id is not None
                        and gate_policy["review_separation"] is None
                    ):
                        raise ConflictError(
                            "gate policy does not require independent review"
                        )
                    if (
                        source_revision is not None
                        and source_revision != gate_policy["head_revision"]
                    ):
                        raise ConflictError("run source revision must match exact head")
                    source_revision = gate_policy["head_revision"]
                    context = {"id": gate_policy["context_version_id"]}
                    if change_review_evidence_id is not None:
                        evidence = connection.execute(
                            "SELECT id, policy_id, candidate_id, kind, gate_name, "
                            "outcome, reviewer_run_id, reviewer_attempt_id "
                            "FROM integration_gate_evidence WHERE id = ?",
                            (change_review_evidence_id,),
                        ).fetchone()
                        if evidence is None:
                            raise NotFoundError(
                                "gate review evidence not found: "
                                + change_review_evidence_id
                            )
                        if (
                            evidence["policy_id"] != gate_policy["id"]
                            or evidence["candidate_id"] != gate_policy["candidate_id"]
                            or evidence["kind"] != "independent_review"
                            or evidence["gate_name"] != "independent-review"
                        ):
                            raise ConflictError(
                                "review evidence does not match the policy candidate"
                            )
                        if evidence["outcome"] != "failed":
                            raise ConflictError(
                                "changes requested require failed review evidence"
                            )
                        latest = connection.execute(
                            "SELECT id FROM integration_gate_evidence "
                            "WHERE policy_id = ? AND gate_name = "
                            "'independent-review' ORDER BY sequence DESC LIMIT 1",
                            (gate_policy["id"],),
                        ).fetchone()
                        if latest is None or latest["id"] != evidence["id"]:
                            raise ConflictError(
                                "changes requested require the current review evidence"
                            )
                        entry_row = connection.execute(
                            "SELECT id, work_thread_id, kind, content_json, "
                            "content_sha256, author_run_id, author_attempt_id "
                            "FROM work_log_entries WHERE id = ?",
                            (change_review_entry_id,),
                        ).fetchone()
                        if entry_row is None:
                            raise NotFoundError(
                                "work-log entry not found: " + change_review_entry_id
                            )
                        try:
                            review_entry = WorkLogEntry.from_mapping(
                                json.loads(entry_row["content_json"])
                            )
                        except (
                            json.JSONDecodeError,
                            TypeError,
                            ValidationError,
                        ) as exc:
                            raise StorageError(
                                "stored review work-log entry is invalid"
                            ) from exc
                        if (
                            review_entry.digest != entry_row["content_sha256"]
                            or entry_row["kind"] != WorkLogKind.REVIEW.value
                            or review_entry.kind is not WorkLogKind.REVIEW
                        ):
                            raise ConflictError(
                                "changes requested require a review work-log entry"
                            )
                        if (
                            not review_entry.summary.strip()
                            .upper()
                            .startswith("CHANGES REQUESTED")
                            or not review_entry.details
                        ):
                            raise ConflictError(
                                "changes requested require an actionable "
                                "changes-requested note"
                            )
                        if (
                            entry_row["work_thread_id"] != gate_policy["work_thread_id"]
                            or entry_row["author_run_id"] != evidence["reviewer_run_id"]
                            or entry_row["author_attempt_id"]
                            != evidence["reviewer_attempt_id"]
                        ):
                            raise ConflictError(
                                "review work-log entry does not match review evidence"
                            )
                        if (
                            review_entry.candidate_revision
                            != gate_policy["head_revision"]
                        ):
                            raise ConflictError(
                                "review work-log entry is not pinned to the exact "
                                "candidate"
                            )
                        change_target = evidence
                    elif change_maintainer_review_id is not None:
                        maintainer_review = connection.execute(
                            "SELECT id, policy_id, candidate_id, decision "
                            "FROM integration_maintainer_reviews WHERE id = ?",
                            (change_maintainer_review_id,),
                        ).fetchone()
                        if maintainer_review is None:
                            raise NotFoundError(
                                "maintainer review not found: "
                                + change_maintainer_review_id
                            )
                        if (
                            maintainer_review["policy_id"] != gate_policy["id"]
                            or maintainer_review["candidate_id"]
                            != gate_policy["candidate_id"]
                        ):
                            raise ConflictError(
                                "maintainer review does not match the policy candidate"
                            )
                        if maintainer_review["decision"] != "changes_requested":
                            raise ConflictError(
                                "changes requested require a maintainer review "
                                "with that decision"
                            )
                        latest = connection.execute(
                            "SELECT id FROM integration_maintainer_reviews "
                            "WHERE policy_id = ? ORDER BY sequence DESC LIMIT 1",
                            (gate_policy["id"],),
                        ).fetchone()
                        if latest is None or latest["id"] != maintainer_review["id"]:
                            raise ConflictError(
                                "changes requested require the current maintainer "
                                "review"
                            )
                        maintainer_change_target = maintainer_review
                else:
                    context = connection.execute(
                        "SELECT id FROM project_context_versions "
                        "WHERE project_id = ? ORDER BY version DESC LIMIT 1",
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
                if (
                    gate_policy is not None
                    and work_thread_id != gate_policy["work_thread_id"]
                ):
                    raise ConflictError(
                        "gate review policy belongs to another work thread"
                    )
                profile = connection.execute(
                    "SELECT id, active FROM agent_profiles WHERE name = ?",
                    (profile_name,),
                ).fetchone()
                if profile is None or not profile["active"]:
                    raise NotFoundError(
                        f"active agent profile not found: {profile_name}"
                    )
                profile_version = connection.execute(
                    "SELECT av.id, av.spec_json, av.krab_identity_id, "
                    "ki.krab_instance_id "
                    "FROM agent_profile_versions av "
                    "JOIN krab_identities ki ON ki.id = av.krab_identity_id "
                    "WHERE av.profile_id = ? ORDER BY av.version DESC LIMIT 1",
                    (profile["id"],),
                ).fetchone()
                if profile_version["krab_instance_id"] != project["krab_instance_id"]:
                    raise ConflictError(
                        "agent profile and project belong to different Krab instances"
                    )
                if gate_policy_id is not None:
                    separation = gate_policy["review_separation"]
                    if (
                        separation
                        in {
                            "different_profile",
                            "different_profile_and_identity",
                        }
                        and profile["id"] == gate_policy["producer_profile_id"]
                    ):
                        raise ConflictError(
                            "reviewer profile must differ from the producer profile"
                        )
                    if separation in {
                        "different_identity",
                        "different_profile_and_identity",
                    } and (
                        profile_version["krab_identity_id"]
                        == gate_policy["producer_identity_id"]
                    ):
                        raise ConflictError(
                            "reviewer identity must differ from the producer identity"
                        )
                try:
                    profile_spec = AgentProfileSpec.from_mapping(
                        json.loads(profile_version["spec_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError("stored agent profile is invalid") from exc
                if (
                    gate_policy_id is not None
                    and profile_spec.workspace_mode is not WorkspaceMode.READ_ONLY
                ):
                    raise ConflictError(
                        "independent reviewer profile must use a read-only workspace"
                    )
                provider_grants = self._available_provider_grants(
                    connection, profile_spec
                )
                connection.execute(
                    "INSERT INTO runs "
                    "(id, project_id, context_version_id, profile_version_id, "
                    "work_item_version_id, work_thread_id, requested_by, "
                    "state, version, supervision_policy_json, "
                    "source_revision, krab_session_id, krab_claim_id, created_at, "
                    "updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        project["id"],
                        context["id"],
                        profile_version["id"],
                        work_item_version_id,
                        work_thread_id,
                        requested_by,
                        supervision_policy.canonical,
                        source_revision,
                        krab_session_id,
                        krab_claim_id,
                        now,
                        now,
                    ),
                )
                if gate_policy is not None:
                    if gate_policy_id is not None:
                        connection.execute(
                            "INSERT INTO integration_gate_review_runs "
                            "(run_id, policy_id, candidate_id, created_at) "
                            "VALUES (?, ?, ?, ?)",
                            (
                                run_id,
                                gate_policy["id"],
                                gate_policy["candidate_id"],
                                now,
                            ),
                        )
                    elif change_target is not None:
                        assert change_target is not None
                        connection.execute(
                            "INSERT INTO integration_gate_change_runs "
                            "(run_id, policy_id, candidate_id, "
                            "review_evidence_id, review_entry_id, created_at) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                run_id,
                                gate_policy["id"],
                                gate_policy["candidate_id"],
                                change_target["id"],
                                change_review_entry_id,
                                now,
                            ),
                        )
                    else:
                        assert maintainer_change_target is not None
                        connection.execute(
                            "INSERT INTO integration_gate_maintainer_change_runs "
                            "(run_id, policy_id, candidate_id, "
                            "maintainer_review_id, created_at) "
                            "VALUES (?, ?, ?, ?, ?)",
                            (
                                run_id,
                                gate_policy["id"],
                                gate_policy["candidate_id"],
                                maintainer_change_target["id"],
                                now,
                            ),
                        )
                event_metadata: dict[str, Any] = {}
                if supervision_policy != SupervisionPolicy.disabled():
                    event_metadata["supervision_policy_sha256"] = (
                        supervision_policy.digest
                    )
                if gate_policy is not None:
                    event_metadata["gate_policy_id"] = gate_policy["id"]
                    event_metadata["candidate_id"] = gate_policy["candidate_id"]
                if change_target is not None:
                    event_metadata["changes_requested"] = True
                    event_metadata["feedback_source"] = "independent_review"
                    event_metadata["review_evidence_id"] = change_target["id"]
                    event_metadata["review_entry_id"] = change_review_entry_id
                if maintainer_change_target is not None:
                    event_metadata["changes_requested"] = True
                    event_metadata["feedback_source"] = "maintainer_review"
                    event_metadata["maintainer_review_id"] = maintainer_change_target[
                        "id"
                    ]
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, metadata_json, "
                    "created_at) VALUES (?, 1, 'run.created', NULL, 'queued', ?, ?)",
                    (
                        run_id,
                        canonical_json(event_metadata),
                        now,
                    ),
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
                    "r.krab_session_id, r.krab_claim_id, "
                    "r.supervision_policy_json, p.active AS project_active, "
                    "p.krab_instance_id, wt.state AS thread_state, "
                    "ap.id AS profile_id, ap.active AS profile_active, "
                    "ap.name AS profile_name, "
                    "av.krab_identity_id AS profile_identity_id, "
                    "av.spec_json AS profile_spec_json, "
                    "av.content_sha256 AS profile_content_sha256 "
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
                    "AND state IN ("
                    "'queued','preparing','running','waiting_for_input','paused')",
                    (run_id,),
                ).fetchone()
                if active_successor is not None:
                    raise ConflictError(
                        "run already has an active successor: " + active_successor["id"]
                    )

                review_target = connection.execute(
                    "SELECT grr.policy_id, grr.candidate_id, "
                    "gp.review_separation, producer_profile.profile_id "
                    "AS producer_profile_id, producer_profile.krab_identity_id "
                    "AS producer_identity_id "
                    "FROM integration_gate_review_runs grr "
                    "JOIN integration_gate_policies gp ON gp.id = grr.policy_id "
                    "JOIN integration_candidates ic ON ic.id = grr.candidate_id "
                    "JOIN run_attempts producer_attempt "
                    "ON producer_attempt.id = ic.producer_attempt_id "
                    "JOIN agent_profile_versions producer_profile "
                    "ON producer_profile.id = producer_attempt.profile_version_id "
                    "WHERE grr.run_id = ?",
                    (run_id,),
                ).fetchone()
                change_target = connection.execute(
                    "SELECT policy_id, candidate_id, review_evidence_id, "
                    "review_entry_id FROM integration_gate_change_runs "
                    "WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                maintainer_change_target = connection.execute(
                    "SELECT policy_id, candidate_id, maintainer_review_id "
                    "FROM integration_gate_maintainer_change_runs "
                    "WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                binding_count = sum(
                    target is not None
                    for target in (
                        review_target,
                        change_target,
                        maintainer_change_target,
                    )
                )
                if binding_count > 1:
                    raise StorageError(
                        "run has conflicting review and changes-requested bindings"
                    )

                if replacement_kind == "retry":
                    if not source["profile_active"]:
                        raise ConflictError("run profile is not active")
                    try:
                        selected_profile_spec = AgentProfileSpec.from_mapping(
                            json.loads(source["profile_spec_json"])
                        )
                    except (
                        json.JSONDecodeError,
                        TypeError,
                        ValidationError,
                    ) as exc:
                        raise StorageError("stored agent profile is invalid") from exc
                    if selected_profile_spec.digest != source["profile_content_sha256"]:
                        raise StorageError("stored agent profile digest does not match")
                    profile_version_id = source["profile_version_id"]
                    authority_rows = connection.execute(
                        "SELECT rpg.authority_name, pg.id, pg.state, "
                        "pg.grant_kind, pg.expires_at, "
                        "pcb.inference_connector_id, "
                        "ic.state AS connector_state "
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
                    unavailable = next(
                        (
                            row["authority_name"]
                            for row in authority_rows
                            if row["state"] != "active"
                            or row["grant_kind"] != "connector"
                            or row["inference_connector_id"] is None
                            or row["connector_state"] != "active"
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
                    profile_id = source["profile_id"]
                    profile_identity_id = source["profile_identity_id"]
                else:
                    if profile_name == source["profile_name"]:
                        raise ConflictError(
                            "target profile is already assigned; use retry"
                        )
                    target = connection.execute(
                        "SELECT ap.id AS profile_id, "
                        "av.id AS profile_version_id, av.spec_json, "
                        "av.content_sha256, av.krab_identity_id, "
                        "ki.krab_instance_id "
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
                    selected_profile_spec = target_spec
                    profile_version_id = target["profile_version_id"]
                    profile_id = target["profile_id"]
                    profile_identity_id = target["krab_identity_id"]
                    authority_rows = self._available_provider_grants(
                        connection, target_spec
                    )
                    authorities = [(row["name"], row["id"]) for row in authority_rows]

                if review_target is not None:
                    if (
                        selected_profile_spec.workspace_mode
                        is not WorkspaceMode.READ_ONLY
                    ):
                        raise ConflictError(
                            "independent reviewer profile must use a read-only "
                            "workspace"
                        )
                    separation = review_target["review_separation"]
                    if (
                        separation
                        in {
                            "different_profile",
                            "different_profile_and_identity",
                        }
                        and profile_id == review_target["producer_profile_id"]
                    ):
                        raise ConflictError(
                            "reviewer profile must differ from the producer profile"
                        )
                    if (
                        separation
                        in {
                            "different_identity",
                            "different_profile_and_identity",
                        }
                        and profile_identity_id == review_target["producer_identity_id"]
                    ):
                        raise ConflictError(
                            "reviewer identity must differ from the producer identity"
                        )

                connection.execute(
                    "INSERT INTO runs "
                    "(id, project_id, context_version_id, profile_version_id, "
                    "work_item_version_id, work_thread_id, requested_by, state, "
                    "version, source_revision, krab_session_id, krab_claim_id, "
                    "supervision_policy_json, created_at, updated_at, "
                    "predecessor_run_id, replacement_kind) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                        source["supervision_policy_json"],
                        now,
                        now,
                        run_id,
                        replacement_kind,
                    ),
                )
                if review_target is not None:
                    connection.execute(
                        "INSERT INTO integration_gate_review_runs "
                        "(run_id, policy_id, candidate_id, created_at) "
                        "VALUES (?, ?, ?, ?)",
                        (
                            replacement_run_id,
                            review_target["policy_id"],
                            review_target["candidate_id"],
                            now,
                        ),
                    )
                if change_target is not None:
                    connection.execute(
                        "INSERT INTO integration_gate_change_runs "
                        "(run_id, policy_id, candidate_id, review_evidence_id, "
                        "review_entry_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            replacement_run_id,
                            change_target["policy_id"],
                            change_target["candidate_id"],
                            change_target["review_evidence_id"],
                            change_target["review_entry_id"],
                            now,
                        ),
                    )
                if maintainer_change_target is not None:
                    connection.execute(
                        "INSERT INTO integration_gate_maintainer_change_runs "
                        "(run_id, policy_id, candidate_id, maintainer_review_id, "
                        "created_at) VALUES (?, ?, ?, ?, ?)",
                        (
                            replacement_run_id,
                            maintainer_change_target["policy_id"],
                            maintainer_change_target["candidate_id"],
                            maintainer_change_target["maintainer_review_id"],
                            now,
                        ),
                    )
                try:
                    source_policy = SupervisionPolicy.from_mapping(
                        json.loads(source["supervision_policy_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError("stored supervision policy is invalid") from exc
                created_event = {
                    "predecessor_run_id": run_id,
                    "replacement_kind": replacement_kind,
                }
                if review_target is not None:
                    created_event["gate_policy_id"] = review_target["policy_id"]
                    created_event["candidate_id"] = review_target["candidate_id"]
                if change_target is not None:
                    created_event["gate_policy_id"] = change_target["policy_id"]
                    created_event["candidate_id"] = change_target["candidate_id"]
                    created_event["changes_requested"] = True
                    created_event["review_evidence_id"] = change_target[
                        "review_evidence_id"
                    ]
                    created_event["review_entry_id"] = change_target["review_entry_id"]
                    created_event["feedback_source"] = "independent_review"
                if maintainer_change_target is not None:
                    created_event["gate_policy_id"] = maintainer_change_target[
                        "policy_id"
                    ]
                    created_event["candidate_id"] = maintainer_change_target[
                        "candidate_id"
                    ]
                    created_event["changes_requested"] = True
                    created_event["feedback_source"] = "maintainer_review"
                    created_event["maintainer_review_id"] = maintainer_change_target[
                        "maintainer_review_id"
                    ]
                if source_policy != SupervisionPolicy.disabled():
                    created_event["supervision_policy_sha256"] = source_policy.digest
                created_metadata = canonical_json(created_event)
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
                        RunState.PAUSED,
                    }:
                        raise ConflictError("active run has no active attempt")
                    self._cancel_open_interactions(connection, run_id, now)
                    self._cancel_active_provider_recoveries(connection, run_id, now)
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
                active_attempt = connection.execute(
                    "SELECT 1 FROM run_attempts WHERE run_id = ? "
                    "AND state IN ('preparing', 'running') LIMIT 1",
                    (run_id,),
                ).fetchone()
                if active_attempt is not None:
                    raise ConflictError(
                        "run transition is controlled by its active attempt"
                    )
                if source is RunState.PAUSED or target is RunState.PAUSED:
                    raise ConflictError(
                        "paused runs are controlled by provider recovery"
                    )
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
