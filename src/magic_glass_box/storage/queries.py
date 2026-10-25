from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from typing import Any

from ..domain import (
    SupervisionPolicy,
    ValidationError,
    WorkItemEnvelope,
)
from .errors import (
    NotFoundError,
    StorageError,
)


class QueryStorage:
    def get_run_execution(self, run_id: str) -> dict[str, Any]:
        """Load every immutable input and identity pinned by a run."""

        with self._read_snapshot() as connection:
            return self._get_run_execution(connection, run_id)

    def _get_run_execution(
        self, connection: sqlite3.Connection, run_id: str
    ) -> dict[str, Any]:
        """Load a run execution while preserving the caller's read snapshot."""

        with self._connection(connection) as connection:
            row = connection.execute(
                "SELECT r.id, r.state, r.version, r.source_revision, "
                "r.work_thread_id, r.predecessor_run_id, r.replacement_kind, "
                "r.supervision_policy_json, "
                "p.krab_project_key AS project_key, c.context_json, "
                "av.spec_json, ki.credential_ref, wi.id AS work_item_id, "
                "wi.source AS work_item_source, "
                "wi.external_id AS work_item_external_id, "
                "wiv.id AS work_item_version_id, "
                "wiv.version AS work_item_version, "
                "wiv.source_updated_at AS work_item_source_updated_at, "
                "wiv.content_json AS work_item_json, "
                "wiv.content_sha256 AS work_item_sha256, "
                "grr.policy_id AS gate_policy_id, "
                "grr.candidate_id AS review_candidate_id, "
                "review_policy.head_revision AS review_head_revision, "
                "review_policy.policy_revision AS review_policy_revision, "
                "review_policy.review_separation AS review_separation, "
                "gcr.policy_id AS change_policy_id, "
                "gcr.candidate_id AS change_candidate_id, "
                "gcr.review_evidence_id AS change_review_evidence_id, "
                "gcr.review_entry_id AS change_review_entry_id, "
                "gmcr.policy_id AS maintainer_change_policy_id, "
                "gmcr.candidate_id AS maintainer_change_candidate_id, "
                "gmcr.maintainer_review_id AS change_maintainer_review_id, "
                "drb.execution_id AS discovery_execution_id, "
                "drb.role AS discovery_role, "
                "discovery.stage AS discovery_stage, "
                "decision_owner.handle AS discovery_decision_owner_handle "
                "FROM runs r JOIN projects p ON p.id = r.project_id "
                "JOIN project_context_versions c ON c.id = r.context_version_id "
                "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
                "JOIN krab_identities ki ON ki.id = av.krab_identity_id "
                "LEFT JOIN work_item_versions wiv "
                "ON wiv.id = r.work_item_version_id "
                "LEFT JOIN work_items wi ON wi.id = wiv.work_item_id "
                "LEFT JOIN integration_gate_review_runs grr ON grr.run_id = r.id "
                "LEFT JOIN integration_gate_policies review_policy "
                "ON review_policy.id = grr.policy_id "
                "LEFT JOIN integration_gate_change_runs gcr ON gcr.run_id = r.id "
                "LEFT JOIN integration_gate_maintainer_change_runs gmcr "
                "ON gmcr.run_id = r.id "
                "LEFT JOIN discovery_run_bindings drb ON drb.run_id = r.id "
                "LEFT JOIN discovery_executions discovery "
                "ON discovery.id = drb.execution_id "
                "LEFT JOIN krab_identities decision_owner "
                "ON decision_owner.id = "
                "discovery.decision_owner_krab_identity_id "
                "WHERE r.id = ?",
                (run_id,),
            ).fetchone()
            provider_authorities = connection.execute(
                "SELECT authority_name FROM run_provider_grants "
                "WHERE run_id = ? ORDER BY authority_name",
                (run_id,),
            ).fetchall()
            handoff_row = connection.execute(
                "SELECT h.id, h.attempt_id, h.content_json, h.content_sha256, "
                "h.created_at FROM attempt_handoffs h JOIN run_attempts a "
                "ON a.id = h.attempt_id WHERE a.run_id = ? "
                "ORDER BY a.sequence DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            checkpoint_row = connection.execute(
                "SELECT rc.id AS checkpoint_id, rc.attempt_id, "
                "source_attempt.run_id AS source_run_id, rc.source_attempt_id, "
                "rc.result_id, rc.source_revision, rc.content_sha256, "
                "rc.changed_paths, rc.changed_bytes, "
                "rc.created_at AS checkpoint_created_at, app.applied_at "
                "FROM attempt_recovery_checkpoints rc "
                "JOIN run_attempts target_attempt ON target_attempt.id = rc.attempt_id "
                "JOIN run_attempts source_attempt "
                "ON source_attempt.id = rc.source_attempt_id "
                "LEFT JOIN attempt_recovery_applications app "
                "ON app.checkpoint_id = rc.id "
                "WHERE rc.attempt_id = ("
                "SELECT latest.id FROM run_attempts latest "
                "WHERE latest.run_id = ? ORDER BY latest.sequence DESC LIMIT 1"
                ")",
                (run_id,),
            ).fetchone()
            discovery_trace_row = None
            discovery_ruling_rows: list[sqlite3.Row] = []
            if row is not None and row["discovery_execution_id"] is not None:
                discovery_trace_row = connection.execute(
                    "SELECT * FROM discovery_traces WHERE execution_id = ?",
                    (row["discovery_execution_id"],),
                ).fetchone()
                discovery_ruling_rows = connection.execute(
                    "SELECT * FROM discovery_rulings WHERE execution_id = ? "
                    "ORDER BY sequence",
                    (row["discovery_execution_id"],),
                ).fetchall()
        if row is None:
            raise NotFoundError(f"run not found: {run_id}")
        result = dict(row)
        try:
            result["context"] = json.loads(result.pop("context_json"))
            result["profile"] = json.loads(result.pop("spec_json"))
            result["supervision_policy"] = SupervisionPolicy.from_mapping(
                json.loads(result.pop("supervision_policy_json"))
            ).to_dict()
            work_item_json = result.pop("work_item_json")
            if work_item_json is None:
                result["work_item"] = None
            else:
                content = json.loads(work_item_json)
                work_item = WorkItemEnvelope.from_mapping(
                    {
                        **content,
                        "source": result.pop("work_item_source"),
                        "external_id": result.pop("work_item_external_id"),
                        "source_updated_at": result.pop("work_item_source_updated_at"),
                    }
                )
                if work_item.digest != result.pop("work_item_sha256"):
                    raise ValidationError("work item digest does not match")
                result["work_item"] = {
                    "id": result["work_item_id"],
                    "version": result["work_item_version"],
                    "source": work_item.source,
                    "external_id": work_item.external_id,
                    "source_updated_at": work_item.source_updated_at,
                    **work_item.content,
                }
            if work_item_json is None:
                result.pop("work_item_source")
                result.pop("work_item_external_id")
                result.pop("work_item_source_updated_at")
                result.pop("work_item_sha256")
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("pinned run data is invalid") from exc
        result["credential_references"] = {
            "krab": result.pop("credential_ref"),
        }
        result["provider_authority_names"] = [
            authority["authority_name"] for authority in provider_authorities
        ]
        result["handoff"] = (
            None if handoff_row is None else self._handoff_record(handoff_row)
        )
        result["recovery_checkpoint"] = (
            None
            if checkpoint_row is None
            else self._recovery_checkpoint_record(checkpoint_row)
        )
        discovery_execution_id = result.pop("discovery_execution_id")
        discovery_role = result.pop("discovery_role")
        discovery_stage = result.pop("discovery_stage")
        discovery_decision_owner_handle = result.pop("discovery_decision_owner_handle")
        if discovery_execution_id is None:
            result["discovery_assignment"] = None
        else:
            trace = None
            if discovery_trace_row is not None:
                trace = self._discovery_trace_record(discovery_trace_row)
            rulings = [
                self._discovery_ruling_record(item) for item in discovery_ruling_rows
            ]
            result["discovery_assignment"] = {
                "execution_id": discovery_execution_id,
                "role": discovery_role,
                "stage": discovery_stage,
                "decision_owner_handle": discovery_decision_owner_handle,
                "trace": trace,
                "rulings": rulings,
            }
        result["work_memory"] = self._get_work_memory_for_run(
            connection,
            run_id,
            after_sequence=None,
            limit=16,
        )
        result["review_assignment"] = (
            None
            if result["review_candidate_id"] is None
            else {
                "policy_id": result["gate_policy_id"],
                "candidate_id": result["review_candidate_id"],
                "head_revision": result["review_head_revision"],
                "policy_revision": result["review_policy_revision"],
                "review_separation": result["review_separation"],
            }
        )
        change_entry_id = result["change_review_entry_id"]
        maintainer_review_id = result["change_maintainer_review_id"]
        if change_entry_id is not None and maintainer_review_id is not None:
            raise StorageError(
                "run has conflicting independent and maintainer feedback bindings"
            )
        if maintainer_review_id is not None:
            result["change_policy_id"] = result["maintainer_change_policy_id"]
            result["change_candidate_id"] = result["maintainer_change_candidate_id"]
        if change_entry_id is not None:
            result["changes_requested"] = {
                "policy_id": result["change_policy_id"],
                "candidate_id": result["change_candidate_id"],
                "source": "independent_review",
                "review_evidence_id": result["change_review_evidence_id"],
                "review_entry": self._work_log_record(
                    self._work_log_row(connection, change_entry_id)
                ),
            }
        elif maintainer_review_id is not None:
            result["changes_requested"] = {
                "policy_id": result["maintainer_change_policy_id"],
                "candidate_id": result["maintainer_change_candidate_id"],
                "source": "maintainer_review",
                "maintainer_review": self._maintainer_review_record(
                    self._maintainer_review_row(connection, maintainer_review_id)
                ),
            }
        else:
            result["changes_requested"] = None
        return result

    def get_run_transport(self, run_id: str) -> dict[str, Any]:
        """Load trusted Krab routing metadata pinned by a run."""

        with self._connection() as connection:
            row = connection.execute(
                "SELECT r.id, r.state, r.version, r.krab_session_id, "
                "r.krab_claim_id, p.krab_project_key AS project_key, "
                "p.krab_instance_id, ksi.name AS krab_instance_name, "
                "ki.id AS agent_krab_identity_id, "
                "ki.krab_user_id AS agent_krab_user_id, "
                "ki.handle AS agent_krab_handle, "
                "(SELECT ra.state FROM run_attempts ra WHERE ra.run_id = r.id "
                "AND ra.state IN ('preparing', 'running') LIMIT 1) "
                "AS active_attempt_state "
                "FROM runs r JOIN projects p ON p.id = r.project_id "
                "JOIN krab_instances ksi ON ksi.id = p.krab_instance_id "
                "JOIN agent_profile_versions av "
                "ON av.id = r.profile_version_id "
                "JOIN krab_identities ki ON ki.id = av.krab_identity_id "
                "WHERE r.id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"run not found: {run_id}")
        return dict(row)

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self._read_snapshot() as connection:
            return self._get_run(connection, run_id)

    def _get_run(self, connection: sqlite3.Connection, run_id: str) -> dict[str, Any]:
        with self._connection(connection) as connection:
            row = connection.execute(
                "SELECT r.id, r.state, r.version, r.source_revision, "
                "r.work_thread_id, r.predecessor_run_id, r.replacement_kind, "
                "r.supervision_policy_json, "
                "r.krab_session_id, r.krab_claim_id, r.created_at, r.updated_at, "
                "r.started_at, r.finished_at, p.krab_project_key AS project_key, "
                "grr.policy_id AS gate_policy_id, "
                "grr.candidate_id AS review_candidate_id, "
                "gcr.policy_id AS change_policy_id, "
                "gcr.candidate_id AS change_candidate_id, "
                "gcr.review_evidence_id AS change_review_evidence_id, "
                "gcr.review_entry_id AS change_review_entry_id, "
                "gmcr.policy_id AS maintainer_change_policy_id, "
                "gmcr.candidate_id AS maintainer_change_candidate_id, "
                "gmcr.maintainer_review_id AS change_maintainer_review_id, "
                "wrb.execution_id AS workflow_execution_id, "
                "wrb.correction_round AS workflow_round, "
                "wrb.role AS workflow_role, "
                "drb.execution_id AS discovery_execution_id, "
                "drb.role AS discovery_role, "
                "c.id AS context_version_id, c.version AS context_version, "
                "ap.name AS profile_name, av.id AS profile_version_id, "
                "av.version AS profile_version, u.handle AS requested_by, "
                "wi.id AS work_item_id, wiv.id AS work_item_version_id, "
                "wiv.version AS work_item_version, "
                "wiv.content_json AS work_item_json "
                "FROM runs r JOIN projects p ON p.id = r.project_id "
                "JOIN project_context_versions c ON c.id = r.context_version_id "
                "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
                "JOIN agent_profiles ap ON ap.id = av.profile_id "
                "JOIN users u ON u.id = r.requested_by "
                "LEFT JOIN work_item_versions wiv "
                "ON wiv.id = r.work_item_version_id "
                "LEFT JOIN work_items wi ON wi.id = wiv.work_item_id "
                "LEFT JOIN integration_gate_review_runs grr ON grr.run_id = r.id "
                "LEFT JOIN integration_gate_change_runs gcr ON gcr.run_id = r.id "
                "LEFT JOIN integration_gate_maintainer_change_runs gmcr "
                "ON gmcr.run_id = r.id "
                "LEFT JOIN workflow_run_bindings wrb ON wrb.run_id = r.id "
                "LEFT JOIN discovery_run_bindings drb ON drb.run_id = r.id "
                "WHERE r.id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"run not found: {run_id}")
            events = connection.execute(
                "SELECT sequence, kind, from_state, to_state, metadata_json, "
                "created_at FROM run_events WHERE run_id = ? ORDER BY sequence",
                (run_id,),
            ).fetchall()
        result = dict(row)
        try:
            result["supervision_policy"] = SupervisionPolicy.from_mapping(
                json.loads(result.pop("supervision_policy_json"))
            ).to_dict()
            work_item_json = result.pop("work_item_json")
            result["work_item_title"] = (
                None if work_item_json is None else json.loads(work_item_json)["title"]
            )
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise StorageError("pinned work item data is invalid") from exc
        result["events"] = [
            {**dict(event), "metadata": json.loads(event["metadata_json"])}
            for event in events
        ]
        for event in result["events"]:
            event.pop("metadata_json")
        result["interactions"] = self._list_run_interactions(connection, run_id)
        result["attempts"] = self._list_run_attempts(connection, run_id)
        result["provider_grants"] = self._list_run_provider_grants(connection, run_id)
        result["provider_invocations"] = self._list_provider_invocations(
            connection, run_id
        )
        result["provider_recoveries"] = self._list_provider_recoveries(
            connection, run_id
        )
        return result

    def iter_active_runs(self) -> Iterator[dict[str, Any]]:
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT id FROM runs WHERE state IN "
                "('queued', 'preparing', 'running', 'waiting_for_input', 'paused') "
                "ORDER BY created_at"
            ).fetchall()
            runs = [self._get_run(connection, row["id"]) for row in rows]
        yield from runs
