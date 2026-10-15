from __future__ import annotations

import json
import sqlite3
from typing import Any

from ..candidates import CandidateError, CandidateManifest
from ..domain import (
    AttemptState,
    ProjectContext,
    RunState,
    ValidationError,
    require_string,
)
from ..gates import (
    GateError,
    GateEvidence,
    GatePolicyManifest,
    ReviewEvidence,
    RunProvenance,
    ValidationEvidence,
    normalize_integration_ref,
    parse_gate_evidence,
)
from .errors import ConflictError, NotFoundError, StorageError


class GateStorage:
    @staticmethod
    def _policy_columns(alias: str | None = None) -> str:
        prefix = "" if alias is None else f"{alias}."
        return ", ".join(
            prefix + column
            for column in (
                "id",
                "candidate_id",
                "schema_version",
                "manifest_json",
                "content_sha256",
                "repository_identity",
                "project_id",
                "context_version_id",
                "work_thread_id",
                "integration_ref",
                "expected_base_revision",
                "head_revision",
                "tree_revision",
                "object_format",
                "policy_revision",
                "review_separation",
                "created_by",
                "created_at",
            )
        )

    @staticmethod
    def _policy_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            manifest = GatePolicyManifest.from_mapping(json.loads(row["manifest_json"]))
        except (GateError, TypeError, json.JSONDecodeError) as exc:
            raise StorageError("stored gate policy manifest is invalid") from exc
        expected = {
            "candidate_id": manifest.candidate_id,
            "schema_version": manifest.schema_version,
            "content_sha256": manifest.digest,
            "repository_identity": manifest.repository_identity,
            "project_id": manifest.project_id,
            "context_version_id": manifest.context_version_id,
            "work_thread_id": manifest.work_thread_id,
            "integration_ref": manifest.integration_ref,
            "expected_base_revision": manifest.expected_base_revision,
            "head_revision": manifest.head_revision,
            "tree_revision": manifest.tree_revision,
            "object_format": manifest.object_format,
            "policy_revision": manifest.policy_revision,
            "review_separation": (
                None
                if manifest.policy.review_separation is None
                else manifest.policy.review_separation.value
            ),
        }
        if any(row[field] != value for field, value in expected.items()):
            raise StorageError("stored gate policy columns do not match its manifest")
        return {
            "id": row["id"],
            **expected,
            "manifest": manifest.to_dict(),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _evidence_columns(alias: str | None = None) -> str:
        prefix = "" if alias is None else f"{alias}."
        return ", ".join(
            prefix + column
            for column in (
                "id",
                "policy_id",
                "sequence",
                "schema_version",
                "kind",
                "gate_name",
                "outcome",
                "evidence_json",
                "content_sha256",
                "candidate_id",
                "repository_identity",
                "integration_ref",
                "expected_base_revision",
                "head_revision",
                "tree_revision",
                "object_format",
                "policy_revision",
                "reviewer_run_id",
                "reviewer_attempt_id",
                "recorded_by",
                "created_at",
            )
        )

    @staticmethod
    def _evidence_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            evidence = parse_gate_evidence(json.loads(row["evidence_json"]))
        except (GateError, TypeError, json.JSONDecodeError) as exc:
            raise StorageError("stored gate evidence is invalid") from exc
        inputs = evidence.inputs
        expected = {
            "policy_id": inputs.policy_id,
            "schema_version": evidence.schema_version,
            "kind": (
                "independent_review"
                if isinstance(evidence, ReviewEvidence)
                else "local_validation"
            ),
            "gate_name": evidence.gate_name,
            "outcome": evidence.outcome.value,
            "content_sha256": evidence.digest,
            "candidate_id": inputs.candidate_id,
            "repository_identity": inputs.repository_identity,
            "integration_ref": inputs.integration_ref,
            "expected_base_revision": inputs.expected_base_revision,
            "head_revision": inputs.head_revision,
            "tree_revision": inputs.tree_revision,
            "object_format": inputs.object_format,
            "policy_revision": inputs.policy_revision,
            "reviewer_run_id": (
                evidence.reviewer.run_id
                if isinstance(evidence, ReviewEvidence)
                else None
            ),
            "reviewer_attempt_id": (
                evidence.reviewer.attempt_id
                if isinstance(evidence, ReviewEvidence)
                else None
            ),
        }
        if any(row[field] != value for field, value in expected.items()):
            raise StorageError("stored gate evidence columns do not match its manifest")
        return {
            "id": row["id"],
            **expected,
            "sequence": row["sequence"],
            "evidence": evidence.to_dict(),
            "recorded_by": row["recorded_by"],
            "created_at": row["created_at"],
        }

    def create_gate_policy(
        self, manifest: GatePolicyManifest, *, created_by: str
    ) -> dict[str, Any]:
        if not isinstance(manifest, GatePolicyManifest):
            raise GateError("gate policy manifest is invalid")
        created_by = require_string(created_by, field="created_by", max_bytes=64)
        policy_id = self.new_id("gpl")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                source = connection.execute(
                    "SELECT ic.id, ic.project_id, ic.context_version_id, "
                    "ic.work_thread_id, ic.base_revision, ic.head_revision, "
                    "ic.tree_revision, ic.object_format, "
                    "ic.result_content_sha256, pc.context_json "
                    "FROM integration_candidates ic "
                    "JOIN project_context_versions pc "
                    "ON pc.id = ic.context_version_id WHERE ic.id = ?",
                    (manifest.candidate_id,),
                ).fetchone()
                if source is None:
                    raise NotFoundError(f"candidate not found: {manifest.candidate_id}")
                try:
                    context = ProjectContext.from_mapping(
                        json.loads(source["context_json"])
                    )
                except (ValidationError, TypeError, json.JSONDecodeError) as exc:
                    raise StorageError("candidate project context is invalid") from exc
                expected_identity = {
                    "project_id": source["project_id"],
                    "repository_identity": source["project_id"],
                    "context_version_id": source["context_version_id"],
                    "work_thread_id": source["work_thread_id"],
                    "integration_ref": normalize_integration_ref(
                        context.default_branch
                    ),
                }
                if any(
                    getattr(manifest, key) != value
                    for key, value in expected_identity.items()
                ):
                    raise ConflictError(
                        "gate policy does not match its candidate exact inputs"
                    )
                expected_revision = {
                    "expected_base_revision": source["base_revision"],
                    "head_revision": source["head_revision"],
                    "tree_revision": source["tree_revision"],
                    "object_format": source["object_format"],
                }
                if any(
                    getattr(manifest, key) != value
                    for key, value in expected_revision.items()
                ):
                    fold = connection.execute(
                        "SELECT 1 FROM integration_folds WHERE candidate_id = ? "
                        "AND project_id = ? AND context_version_id = ? "
                        "AND work_thread_id = ? AND integration_ref = ? "
                        "AND base_revision = ? AND head_revision = ? "
                        "AND tree_revision = ? AND object_format = ? "
                        "AND result_content_sha256 = ?",
                        (
                            manifest.candidate_id,
                            manifest.project_id,
                            manifest.context_version_id,
                            manifest.work_thread_id,
                            manifest.integration_ref,
                            manifest.expected_base_revision,
                            manifest.head_revision,
                            manifest.tree_revision,
                            manifest.object_format,
                            source["result_content_sha256"],
                        ),
                    ).fetchone()
                    if fold is None:
                        raise ConflictError(
                            "gate policy does not match its candidate exact inputs "
                            "or a recorded fold"
                        )
                creator = connection.execute(
                    "SELECT active FROM users WHERE id = ?", (created_by,)
                ).fetchone()
                if creator is None:
                    raise NotFoundError(f"user not found: {created_by}")
                if creator["active"] != 1:
                    raise ConflictError("gate policy creator is inactive")
                existing = connection.execute(
                    f"SELECT {self._policy_columns()} "  # noqa: S608
                    "FROM integration_gate_policies WHERE content_sha256 = ?",
                    (manifest.digest,),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return self._policy_record(existing)
                separation = manifest.policy.review_separation
                connection.execute(
                    "INSERT INTO integration_gate_policies "
                    "(id, candidate_id, schema_version, manifest_json, "
                    "content_sha256, repository_identity, project_id, "
                    "context_version_id, work_thread_id, integration_ref, "
                    "expected_base_revision, head_revision, tree_revision, "
                    "object_format, policy_revision, review_separation, "
                    "created_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        policy_id,
                        manifest.candidate_id,
                        manifest.schema_version,
                        manifest.canonical,
                        manifest.digest,
                        manifest.repository_identity,
                        manifest.project_id,
                        manifest.context_version_id,
                        manifest.work_thread_id,
                        manifest.integration_ref,
                        manifest.expected_base_revision,
                        manifest.head_revision,
                        manifest.tree_revision,
                        manifest.object_format,
                        manifest.policy_revision,
                        None if separation is None else separation.value,
                        created_by,
                        now,
                    ),
                )
                row = connection.execute(
                    f"SELECT {self._policy_columns()} "  # noqa: S608
                    "FROM integration_gate_policies WHERE id = ?",
                    (policy_id,),
                ).fetchone()
                connection.commit()
            except (ConflictError, GateError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("gate policy could not be recorded") from exc
        if row is None:
            raise StorageError("gate policy was not recorded")
        return self._policy_record(row)

    def get_gate_policy(self, policy_id: str) -> dict[str, Any]:
        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        with self._read_snapshot() as connection:
            row = connection.execute(
                f"SELECT {self._policy_columns()} "  # noqa: S608
                "FROM integration_gate_policies WHERE id = ?",
                (policy_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"gate policy not found: {policy_id}")
        return self._policy_record(row)

    def get_gate_review_target(self, policy_id: str) -> dict[str, Any]:
        """Resolve the durable run target for one candidate-backed review."""

        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        with self._read_snapshot() as connection:
            row = connection.execute(
                "SELECT gp.policy_revision, gp.candidate_id, gp.head_revision, "
                "gp.review_separation, p.krab_project_key AS project_key, "
                "wt.work_item_id FROM integration_gate_policies gp "
                "JOIN projects p ON p.id = gp.project_id "
                "JOIN work_threads wt ON wt.id = gp.work_thread_id "
                "WHERE gp.id = ?",
                (policy_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"gate policy not found: {policy_id}")
        return dict(row)

    def get_gate_candidate_context(self, candidate_id: str) -> dict[str, Any]:
        candidate_id = require_string(candidate_id, field="candidate_id", max_bytes=64)
        with self._read_snapshot() as connection:
            row = connection.execute(
                "SELECT pc.context_json FROM integration_candidates ic "
                "JOIN project_context_versions pc "
                "ON pc.id = ic.context_version_id WHERE ic.id = ?",
                (candidate_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"candidate not found: {candidate_id}")
        try:
            return ProjectContext.from_mapping(
                json.loads(row["context_json"])
            ).to_dict()
        except (ValidationError, TypeError, json.JSONDecodeError) as exc:
            raise StorageError("candidate project context is invalid") from exc

    def get_gate_revision(self, policy_id: str) -> dict[str, Any]:
        """Resolve one policy to its exact original or folded candidate tree."""

        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        with self._read_snapshot() as connection:
            policy_row = connection.execute(
                f"SELECT {self._policy_columns()} "  # noqa: S608
                "FROM integration_gate_policies WHERE id = ?",
                (policy_id,),
            ).fetchone()
            if policy_row is None:
                raise NotFoundError(f"gate policy not found: {policy_id}")
            policy_record = self._policy_record(policy_row)
            policy = GatePolicyManifest.from_mapping(policy_record["manifest"])
            source = connection.execute(
                "SELECT ic.manifest_json, pc.context_json "
                "FROM integration_candidates ic "
                "JOIN project_context_versions pc "
                "ON pc.id = ic.context_version_id WHERE ic.id = ?",
                (policy.candidate_id,),
            ).fetchone()
            if source is None:
                raise StorageError("gate policy candidate is unavailable")
            try:
                origin = CandidateManifest.from_mapping(
                    json.loads(source["manifest_json"])
                )
                context = ProjectContext.from_mapping(
                    json.loads(source["context_json"])
                )
            except (
                CandidateError,
                ValidationError,
                TypeError,
                json.JSONDecodeError,
            ) as exc:
                raise StorageError("gate policy candidate identity is invalid") from exc
            exact = CandidateManifest(
                schema_version=origin.schema_version,
                attempt_id=origin.attempt_id,
                attempt_result_id=origin.attempt_result_id,
                producer_run_id=origin.producer_run_id,
                project_id=origin.project_id,
                context_version_id=origin.context_version_id,
                work_thread_id=origin.work_thread_id,
                base_revision=policy.expected_base_revision,
                result_content_sha256=origin.result_content_sha256,
                head_revision=policy.head_revision,
                tree_revision=policy.tree_revision,
                object_format=policy.object_format,
            )
            if exact != origin:
                fold = connection.execute(
                    "SELECT 1 FROM integration_folds WHERE candidate_id = ? "
                    "AND project_id = ? AND context_version_id = ? "
                    "AND work_thread_id = ? AND integration_ref = ? "
                    "AND base_revision = ? AND head_revision = ? "
                    "AND tree_revision = ? AND object_format = ? "
                    "AND result_content_sha256 = ?",
                    (
                        policy.candidate_id,
                        policy.project_id,
                        policy.context_version_id,
                        policy.work_thread_id,
                        policy.integration_ref,
                        policy.expected_base_revision,
                        policy.head_revision,
                        policy.tree_revision,
                        policy.object_format,
                        origin.result_content_sha256,
                    ),
                ).fetchone()
                if fold is None:
                    raise StorageError("gate policy exact revision is unavailable")
        return {
            "policy": policy_record,
            "manifest": exact.to_dict(),
            "repository_path": context.repository_path,
        }

    def list_gate_policies(
        self,
        *,
        candidate_id: str | None = None,
        project_id: str | None = None,
        work_thread_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 500
        ):
            raise ValueError("limit must be between 1 and 500")
        scopes = (candidate_id, project_id, work_thread_id)
        if sum(value is not None for value in scopes) > 1:
            raise ValueError("gate policy list accepts at most one scope")
        field = None
        value = None
        for candidate_field, candidate_value in (
            ("candidate_id", candidate_id),
            ("project_id", project_id),
            ("work_thread_id", work_thread_id),
        ):
            if candidate_value is not None:
                field = candidate_field
                value = require_string(
                    candidate_value, field=candidate_field, max_bytes=64
                )
                break
        where = "" if field is None else f"WHERE {field} = ?"
        parameters: tuple[object, ...] = (limit,) if value is None else (value, limit)
        with self._read_snapshot() as connection:
            rows = connection.execute(
                f"SELECT {self._policy_columns()} "  # noqa: S608
                f"FROM integration_gate_policies {where} "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [self._policy_record(row) for row in rows]

    @staticmethod
    def _attempt_provenance(
        connection: sqlite3.Connection, attempt_id: str
    ) -> tuple[RunProvenance, sqlite3.Row]:
        row = connection.execute(
            "SELECT ra.id AS attempt_id, ra.run_id, ra.profile_version_id, "
            "ra.state AS attempt_state, ra.exit_code, "
            "ra.source_revision AS attempt_source_revision, "
            "r.state AS run_state, r.source_revision AS run_source_revision, "
            "r.project_id, r.context_version_id, r.work_thread_id, "
            "r.requested_by, apv.profile_id, apv.krab_identity_id "
            "FROM run_attempts ra JOIN runs r ON r.id = ra.run_id "
            "JOIN agent_profile_versions apv ON apv.id = ra.profile_version_id "
            "WHERE ra.id = ?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"run attempt not found: {attempt_id}")
        return (
            RunProvenance(
                run_id=row["run_id"],
                attempt_id=row["attempt_id"],
                profile_id=row["profile_id"],
                profile_version_id=row["profile_version_id"],
                identity_id=row["krab_identity_id"],
                requested_by=row["requested_by"],
            ),
            row,
        )

    def record_gate_evidence(
        self, evidence: GateEvidence, *, recorded_by: str
    ) -> dict[str, Any]:
        if not isinstance(evidence, (ReviewEvidence, ValidationEvidence)):
            raise GateError("gate evidence is invalid")
        recorded_by = require_string(recorded_by, field="recorded_by", max_bytes=64)
        evidence_id = self.new_id("gev")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                policy_row = connection.execute(
                    f"SELECT {self._policy_columns()} "  # noqa: S608
                    "FROM integration_gate_policies WHERE id = ?",
                    (evidence.inputs.policy_id,),
                ).fetchone()
                if policy_row is None:
                    raise NotFoundError(
                        f"gate policy not found: {evidence.inputs.policy_id}"
                    )
                policy_record = self._policy_record(policy_row)
                policy = GatePolicyManifest.from_mapping(policy_record["manifest"])
                try:
                    evidence.inputs.assert_matches(policy)
                except GateError as exc:
                    raise ConflictError(
                        "gate evidence does not match the exact policy inputs"
                    ) from exc
                if evidence.gate_name not in policy.policy.required_gates:
                    raise ConflictError("gate evidence is not required by the policy")
                producer_row = connection.execute(
                    "SELECT producer_attempt_id FROM integration_candidates "
                    "WHERE id = ?",
                    (policy.candidate_id,),
                ).fetchone()
                if producer_row is None:
                    raise StorageError("gate policy candidate is unavailable")
                reviewer_run_id: str | None = None
                reviewer_attempt_id: str | None = None
                if isinstance(evidence, ReviewEvidence):
                    if evidence.separation is not policy.policy.review_separation:
                        raise ConflictError(
                            "review evidence separation does not match the policy"
                        )
                    producer, _ = self._attempt_provenance(
                        connection, producer_row["producer_attempt_id"]
                    )
                    reviewer, reviewer_row = self._attempt_provenance(
                        connection, evidence.reviewer.attempt_id
                    )
                    review_target = connection.execute(
                        "SELECT policy_id, candidate_id "
                        "FROM integration_gate_review_runs WHERE run_id = ?",
                        (reviewer.run_id,),
                    ).fetchone()
                    if (
                        review_target is None
                        or review_target["policy_id"] != evidence.inputs.policy_id
                        or review_target["candidate_id"] != policy.candidate_id
                    ):
                        raise ConflictError(
                            "review evidence was not produced by a run bound to "
                            "this policy candidate"
                        )
                    if evidence.producer != producer or evidence.reviewer != reviewer:
                        raise ConflictError("review evidence provenance is invalid")
                    if (
                        reviewer_row["attempt_state"] != AttemptState.SUCCEEDED.value
                        or reviewer_row["exit_code"] != 0
                        or reviewer_row["run_state"] != RunState.SUCCEEDED.value
                    ):
                        raise ConflictError(
                            "review evidence requires a successful reviewer attempt"
                        )
                    if (
                        reviewer_row["project_id"] != policy.project_id
                        or reviewer_row["context_version_id"]
                        != policy.context_version_id
                        or reviewer_row["work_thread_id"] != policy.work_thread_id
                    ):
                        raise ConflictError(
                            "review evidence belongs to another project context"
                        )
                    if (
                        reviewer_row["run_source_revision"] != policy.head_revision
                        or reviewer_row["attempt_source_revision"]
                        != policy.head_revision
                    ):
                        raise ConflictError(
                            "review evidence was not produced from the exact head"
                        )
                    reviewer_run_id = reviewer.run_id
                    reviewer_attempt_id = reviewer.attempt_id
                else:
                    validation = policy.policy.validation(evidence.validation_name)
                    if (
                        evidence.image_id != validation.image_id
                        or evidence.command_sha256 != validation.command_sha256
                    ):
                        raise ConflictError(
                            "validation evidence does not match its configured gate"
                        )
                recorder = connection.execute(
                    "SELECT active FROM users WHERE id = ?", (recorded_by,)
                ).fetchone()
                if recorder is None:
                    raise NotFoundError(f"user not found: {recorded_by}")
                if recorder["active"] != 1:
                    raise ConflictError("gate evidence recorder is inactive")
                existing = connection.execute(
                    f"SELECT {self._evidence_columns()} "  # noqa: S608
                    "FROM integration_gate_evidence "
                    "WHERE policy_id = ? AND content_sha256 = ?",
                    (evidence.inputs.policy_id, evidence.digest),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return self._evidence_record(existing)
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM integration_gate_evidence WHERE policy_id = ?",
                    (evidence.inputs.policy_id,),
                ).fetchone()["sequence"]
                inputs = evidence.inputs
                kind = (
                    "independent_review"
                    if isinstance(evidence, ReviewEvidence)
                    else "local_validation"
                )
                connection.execute(
                    "INSERT INTO integration_gate_evidence "
                    "(id, policy_id, sequence, schema_version, kind, gate_name, "
                    "outcome, evidence_json, content_sha256, candidate_id, "
                    "repository_identity, integration_ref, "
                    "expected_base_revision, head_revision, tree_revision, "
                    "object_format, policy_revision, reviewer_run_id, "
                    "reviewer_attempt_id, recorded_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?, ?)",
                    (
                        evidence_id,
                        inputs.policy_id,
                        sequence,
                        evidence.schema_version,
                        kind,
                        evidence.gate_name,
                        evidence.outcome.value,
                        evidence.canonical,
                        evidence.digest,
                        inputs.candidate_id,
                        inputs.repository_identity,
                        inputs.integration_ref,
                        inputs.expected_base_revision,
                        inputs.head_revision,
                        inputs.tree_revision,
                        inputs.object_format,
                        inputs.policy_revision,
                        reviewer_run_id,
                        reviewer_attempt_id,
                        recorded_by,
                        now,
                    ),
                )
                row = connection.execute(
                    f"SELECT {self._evidence_columns()} "  # noqa: S608
                    "FROM integration_gate_evidence WHERE id = ?",
                    (evidence_id,),
                ).fetchone()
                connection.commit()
            except (ConflictError, GateError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("gate evidence could not be recorded") from exc
        if row is None:
            raise StorageError("gate evidence was not recorded")
        return self._evidence_record(row)

    def get_gate_review_provenance(
        self, policy_id: str, reviewer_attempt_id: str
    ) -> dict[str, Any]:
        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        reviewer_attempt_id = require_string(
            reviewer_attempt_id, field="reviewer_attempt_id", max_bytes=64
        )
        with self._read_snapshot() as connection:
            policy_row = connection.execute(
                f"SELECT {self._policy_columns()} "  # noqa: S608
                "FROM integration_gate_policies WHERE id = ?",
                (policy_id,),
            ).fetchone()
            if policy_row is None:
                raise NotFoundError(f"gate policy not found: {policy_id}")
            producer_row = connection.execute(
                "SELECT ic.producer_attempt_id "
                "FROM integration_gate_policies igp "
                "JOIN integration_candidates ic ON ic.id = igp.candidate_id "
                "WHERE igp.id = ?",
                (policy_id,),
            ).fetchone()
            if producer_row is None:
                raise StorageError("gate policy candidate is unavailable")
            producer, _ = self._attempt_provenance(
                connection, producer_row["producer_attempt_id"]
            )
            reviewer, _ = self._attempt_provenance(connection, reviewer_attempt_id)
        return {
            "policy": self._policy_record(policy_row),
            "producer": producer,
            "reviewer": reviewer,
        }

    def list_gate_evidence(
        self, policy_id: str, *, limit: int = 500
    ) -> list[dict[str, Any]]:
        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 1000
        ):
            raise ValueError("limit must be between 1 and 1000")
        with self._read_snapshot() as connection:
            known = connection.execute(
                "SELECT 1 FROM integration_gate_policies WHERE id = ?",
                (policy_id,),
            ).fetchone()
            if known is None:
                raise NotFoundError(f"gate policy not found: {policy_id}")
            rows = connection.execute(
                f"SELECT {self._evidence_columns()} "  # noqa: S608
                "FROM integration_gate_evidence WHERE policy_id = ? "
                "ORDER BY sequence DESC LIMIT ?",
                (policy_id, limit),
            ).fetchall()
        return [self._evidence_record(row) for row in rows]

    def _gate_readiness(
        self, connection: sqlite3.Connection, policy_id: str
    ) -> dict[str, Any]:
        policy_row = connection.execute(
            f"SELECT {self._policy_columns()} "  # noqa: S608
            "FROM integration_gate_policies WHERE id = ?",
            (policy_id,),
        ).fetchone()
        if policy_row is None:
            raise NotFoundError(f"gate policy not found: {policy_id}")
        evidence_rows = connection.execute(
            f"SELECT {self._evidence_columns('e')} "  # noqa: S608
            "FROM integration_gate_evidence e JOIN ("
            "SELECT gate_name, MAX(sequence) AS sequence "
            "FROM integration_gate_evidence WHERE policy_id = ? "
            "GROUP BY gate_name) latest ON latest.gate_name = e.gate_name "
            "AND latest.sequence = e.sequence WHERE e.policy_id = ?",
            (policy_id, policy_id),
        ).fetchall()
        policy_record = self._policy_record(policy_row)
        manifest = GatePolicyManifest.from_mapping(policy_record["manifest"])
        latest = {
            record["gate_name"]: record
            for record in (self._evidence_record(row) for row in evidence_rows)
        }
        gates: list[dict[str, Any]] = []
        for name in manifest.policy.required_gates:
            record = latest.get(name)
            state = "pending" if record is None else record["outcome"]
            gates.append(
                {
                    "name": name,
                    "kind": (
                        "independent_review"
                        if name == "independent-review"
                        else "local_validation"
                    ),
                    "state": state,
                    "evidence_id": None if record is None else record["id"],
                    "sequence": None if record is None else record["sequence"],
                    "recorded_at": None if record is None else record["created_at"],
                }
            )
        passed = sum(gate["state"] == "passed" for gate in gates)
        failed = sum(gate["state"] == "failed" for gate in gates)
        pending = sum(gate["state"] == "pending" for gate in gates)
        return {
            "policy_id": policy_id,
            "candidate_id": manifest.candidate_id,
            "repository_identity": manifest.repository_identity,
            "integration_ref": manifest.integration_ref,
            "expected_base_revision": manifest.expected_base_revision,
            "head_revision": manifest.head_revision,
            "tree_revision": manifest.tree_revision,
            "object_format": manifest.object_format,
            "policy_revision": manifest.policy_revision,
            "ready": passed == len(gates),
            "required_count": len(gates),
            "passed_count": passed,
            "failed_count": failed,
            "pending_count": pending,
            "gates": gates,
        }

    def gate_readiness(self, policy_id: str) -> dict[str, Any]:
        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        with self._read_snapshot() as connection:
            return self._gate_readiness(connection, policy_id)
