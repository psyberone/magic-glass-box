from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from ..domain import (
    ValidationError,
    canonical_json,
    parse_rfc3339_timestamp,
    require_string,
    validate_name,
    validate_project_key,
)
from ..workflow_policy import WorkflowActivationSpec, WorkflowPolicySpec
from ..workflows import IMPLEMENTATION_TEMPLATE_ID
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)


class WorkflowPolicyStorage:
    _WAKE_IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")

    @staticmethod
    def _workflow_policy_positive_version(value: object, *, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError(f"{field} must be a positive integer")
        return value

    @staticmethod
    def _workflow_policy_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            spec = WorkflowPolicySpec.from_mapping(json.loads(row["spec_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored workflow policy is invalid") from exc
        if spec.digest != row["content_sha256"]:
            raise StorageError("stored workflow policy digest does not match")
        return {
            "id": row["id"],
            "name": row["name"],
            "version": row["version"],
            "spec": spec.to_dict(),
            "content_sha256": row["content_sha256"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _workflow_policy_row(
        connection: sqlite3.Connection, name: str, version: int | None = None
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT policy.id, policy.name, policy.version, policy.spec_json, "
            "policy.content_sha256, creator.handle AS created_by, "
            "policy.created_at FROM workflow_policies policy "
            "JOIN users creator ON creator.id = policy.created_by "
            "WHERE policy.name = ? AND (? IS NULL OR policy.version = ?) "
            "ORDER BY policy.version DESC LIMIT 1",
            (name, version, version),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"workflow policy not found: {name}")
        return row

    def create_workflow_policy(
        self,
        *,
        name: str,
        spec: WorkflowPolicySpec,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_workflow_policy(
            name=name, spec=spec, created_by=created_by, create=True
        )

    def revise_workflow_policy(
        self,
        *,
        name: str,
        spec: WorkflowPolicySpec,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_workflow_policy(
            name=name, spec=spec, created_by=created_by, create=False
        )

    def _write_workflow_policy(
        self,
        *,
        name: str,
        spec: WorkflowPolicySpec,
        created_by: str,
        create: bool,
    ) -> dict[str, Any]:
        name = validate_name(name, field="workflow policy name")
        if not isinstance(spec, WorkflowPolicySpec):
            raise ValidationError("spec must be a WorkflowPolicySpec")
        policy_id = self.new_id("wfp")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="workflow policy owner")
                latest = connection.execute(
                    "SELECT id, version, content_sha256 FROM workflow_policies "
                    "WHERE name = ? ORDER BY version DESC LIMIT 1",
                    (name,),
                ).fetchone()
                if create:
                    if latest is not None:
                        raise ConflictError("workflow policy already exists")
                    version = 1
                else:
                    if latest is None:
                        raise NotFoundError(f"workflow policy not found: {name}")
                    if latest["content_sha256"] == spec.digest:
                        raise ConflictError("workflow policy revision is identical")
                    version = latest["version"] + 1
                connection.execute(
                    "INSERT INTO workflow_policies "
                    "(id, name, version, schema_version, spec_json, "
                    "content_sha256, created_by, created_at) "
                    "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                    (
                        policy_id,
                        name,
                        version,
                        canonical_json(spec.to_dict()),
                        spec.digest,
                        created_by,
                        now,
                    ),
                )
                connection.commit()
            except (ConflictError, NotFoundError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("workflow policy could not be stored") from exc
        return self.get_workflow_policy(name, version=version)

    def get_workflow_policy(
        self, name: str, *, version: int | None = None
    ) -> dict[str, Any]:
        name = validate_name(name, field="workflow policy name")
        if version is not None:
            version = self._workflow_policy_positive_version(
                version, field="workflow policy version"
            )
        with self._connection() as connection:
            return self._workflow_policy_record(
                self._workflow_policy_row(connection, name, version)
            )

    def list_workflow_policies(self) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT policy.id, policy.name, policy.version, policy.spec_json, "
                "policy.content_sha256, creator.handle AS created_by, "
                "policy.created_at FROM workflow_policies policy "
                "JOIN users creator ON creator.id = policy.created_by "
                "WHERE policy.version = (SELECT MAX(latest.version) "
                "FROM workflow_policies latest WHERE latest.name = policy.name) "
                "ORDER BY policy.name"
            ).fetchall()
        return [self._workflow_policy_record(row) for row in rows]

    def create_workflow_activation(
        self,
        *,
        project_key: str,
        policy_name: str,
        expected_policy_version: int,
        spec: WorkflowActivationSpec,
        acknowledged_by: str,
    ) -> dict[str, Any]:
        return self._write_workflow_activation(
            project_key=project_key,
            policy_name=policy_name,
            expected_policy_version=expected_policy_version,
            spec=spec,
            acknowledged_by=acknowledged_by,
            create=True,
        )

    def revise_workflow_activation(
        self,
        *,
        project_key: str,
        policy_name: str,
        expected_policy_version: int,
        spec: WorkflowActivationSpec,
        acknowledged_by: str,
    ) -> dict[str, Any]:
        return self._write_workflow_activation(
            project_key=project_key,
            policy_name=policy_name,
            expected_policy_version=expected_policy_version,
            spec=spec,
            acknowledged_by=acknowledged_by,
            create=False,
        )

    def _write_workflow_activation(
        self,
        *,
        project_key: str,
        policy_name: str,
        expected_policy_version: int,
        spec: WorkflowActivationSpec,
        acknowledged_by: str,
        create: bool,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        policy_name = validate_name(policy_name, field="workflow policy name")
        expected_policy_version = self._workflow_policy_positive_version(
            expected_policy_version, field="expected policy version"
        )
        if not isinstance(spec, WorkflowActivationSpec):
            raise ValidationError("spec must be a WorkflowActivationSpec")
        activation_id = self.new_id("wpa")
        version_id = self.new_id("wav")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, acknowledged_by, role="workflow activation owner"
                )
                project = connection.execute(
                    "SELECT id, active FROM projects WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None:
                    raise NotFoundError(f"project not found: {project_key}")
                if project["active"] != 1:
                    raise ConflictError("workflow activation project is inactive")
                policy_row = self._workflow_policy_row(connection, policy_name)
                policy = self._workflow_policy_record(policy_row)
                if policy["version"] != expected_policy_version:
                    raise ConflictError("workflow policy version changed")
                policy_spec = WorkflowPolicySpec.from_mapping(policy["spec"])
                self._validate_activation(
                    connection,
                    project_id=project["id"],
                    project_key=project_key,
                    policy=policy_spec,
                    spec=spec,
                )
                existing = connection.execute(
                    "SELECT activation.id, latest.version, "
                    "latest.content_sha256 FROM workflow_activations activation "
                    "JOIN workflow_activation_versions latest "
                    "ON latest.activation_id = activation.id "
                    "WHERE activation.project_id = ? "
                    "ORDER BY latest.version DESC LIMIT 1",
                    (project["id"],),
                ).fetchone()
                if create:
                    if existing is not None:
                        raise ConflictError("workflow activation already exists")
                    version = 1
                    selected_activation_id = activation_id
                    connection.execute(
                        "INSERT INTO workflow_activations "
                        "(id, project_id, created_at) VALUES (?, ?, ?)",
                        (selected_activation_id, project["id"], now),
                    )
                else:
                    if existing is None:
                        raise NotFoundError(
                            f"workflow activation not found: {project_key}"
                        )
                    if existing["content_sha256"] == spec.digest:
                        raise ConflictError("workflow activation revision is identical")
                    version = existing["version"] + 1
                    selected_activation_id = existing["id"]
                connection.execute(
                    "INSERT INTO workflow_activation_versions "
                    "(id, activation_id, version, policy_id, "
                    "workflow_configuration_version_id, schema_version, "
                    "spec_json, content_sha256, acknowledged_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)",
                    (
                        version_id,
                        selected_activation_id,
                        version,
                        policy["id"],
                        spec.workflow_configuration_version_id,
                        canonical_json(spec.to_dict()),
                        spec.digest,
                        acknowledged_by,
                        now,
                    ),
                )
                for binding in spec.capability_bindings:
                    connection.execute(
                        "INSERT INTO workflow_activation_capability_bindings "
                        "(activation_version_id, capability_name, "
                        "capability_version, project_capability_binding_id, "
                        "created_at) VALUES (?, ?, ?, ?, ?)",
                        (
                            version_id,
                            binding.name,
                            binding.version,
                            binding.binding_id,
                            now,
                        ),
                    )
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("workflow activation could not be stored") from exc
        return self.get_workflow_activation(project_key, version=version)

    def _validate_activation(
        self,
        connection: sqlite3.Connection,
        *,
        project_id: str,
        project_key: str,
        policy: WorkflowPolicySpec,
        spec: WorkflowActivationSpec,
    ) -> None:
        if spec.policy_sha256 != policy.digest:
            raise ConflictError("workflow activation policy digest does not match")
        configuration = self._configuration_record(
            connection,
            self._configuration_row(
                connection,
                version_id=spec.workflow_configuration_version_id,
            ),
        )
        if configuration["project_id"] != project_id:
            raise ConflictError("workflow activation configuration project differs")
        if configuration["template"]["id"] != IMPLEMENTATION_TEMPLATE_ID:
            raise ConflictError("workflow activation requires implementation-v1")
        if not configuration["spec"]["maintainer_approval"]:
            raise ConflictError("workflow activation requires maintainer approval")
        if configuration["spec"]["landing_mode"] != "provider":
            raise ConflictError("workflow activation requires provider landing")
        obligation_bindings = {
            binding.obligation: binding.validation
            for binding in spec.validation_bindings
        }
        if set(obligation_bindings) != set(policy.validation_obligations):
            raise ConflictError("workflow activation validation scope is incomplete")
        configured_validations = {
            validation["name"] for validation in configuration["spec"]["validations"]
        }
        if set(obligation_bindings.values()) != configured_validations:
            raise ConflictError("workflow activation validation binding differs")
        maxima = policy.resource_maxima
        resources = spec.resources
        if (
            resources.review_rounds > maxima.review_rounds
            or resources.seat_count > maxima.seat_count
            or resources.shared_host_concurrency > maxima.shared_host_concurrency
            or resources.wall_time_seconds > maxima.wall_time_seconds
        ):
            raise ConflictError("workflow activation exceeds policy resources")
        if resources.review_rounds != configuration["spec"]["max_correction_rounds"]:
            raise ConflictError("workflow activation review-round ceiling differs")
        if resources.seat_count < len(configuration["roles"]):
            raise ConflictError("workflow activation seat ceiling is too small")
        if spec.merge_method not in policy.landing_methods:
            raise ConflictError("workflow activation merge method is not permitted")
        required_capabilities = {
            (name, version, mode)
            for name, version, mode in policy.required_capabilities
        }
        bound_capabilities = {
            (binding.name, binding.version, binding.mode)
            for binding in spec.capability_bindings
        }
        if bound_capabilities != required_capabilities:
            raise ConflictError("workflow activation capability scope is incomplete")
        for binding in spec.capability_bindings:
            record = self._project_capability_binding_record(
                connection, binding.binding_id
            )
            if (
                record["project"]["id"] != project_id
                or record["project"]["key"] != project_key
            ):
                raise ConflictError("workflow activation capability project differs")
            if record["state"] != "active" or not record["project"]["active"]:
                raise ConflictError("workflow activation capability is unavailable")
            if record["controller"]["sidecar_state"] != "active":
                raise ConflictError("workflow activation controller is unavailable")
            capability = record["capability"]
            if capability != {
                "name": binding.name,
                "version": binding.version,
                "mode": binding.mode,
            }:
                raise ConflictError("workflow activation capability binding differs")

    @staticmethod
    def _activation_row(
        connection: sqlite3.Connection,
        project_key: str,
        version: int | None,
        *,
        version_id: str | None = None,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT activation.id, activation.project_id, "
            "project.krab_project_key AS project_key, "
            "activation.created_at AS activation_created_at, "
            "av.id AS version_id, av.version, av.policy_id, "
            "av.workflow_configuration_version_id, av.spec_json, "
            "av.content_sha256, owner.handle AS acknowledged_by, av.created_at, "
            "policy.name AS policy_name, policy.version AS policy_version, "
            "policy.spec_json AS policy_spec_json, "
            "policy.content_sha256 AS policy_content_sha256, "
            "creator.handle AS policy_created_by, "
            "policy.created_at AS policy_created_at "
            "FROM workflow_activations activation "
            "JOIN projects project ON project.id = activation.project_id "
            "JOIN workflow_activation_versions av "
            "ON av.activation_id = activation.id "
            "JOIN workflow_policies policy ON policy.id = av.policy_id "
            "JOIN users owner ON owner.id = av.acknowledged_by "
            "JOIN users creator ON creator.id = policy.created_by "
            "WHERE project.krab_project_key = ? AND (? IS NULL OR av.version = ?) "
            "AND (? IS NULL OR av.id = ?) "
            "ORDER BY av.version DESC LIMIT 1",
            (project_key, version, version, version_id, version_id),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"workflow activation not found: {project_key}")
        return row

    def _activation_record(
        self, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        try:
            spec = WorkflowActivationSpec.from_mapping(json.loads(row["spec_json"]))
            policy = WorkflowPolicySpec.from_mapping(
                json.loads(row["policy_spec_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored workflow activation is invalid") from exc
        if spec.digest != row["content_sha256"]:
            raise StorageError("stored workflow activation digest does not match")
        if (
            policy.digest != row["policy_content_sha256"]
            or spec.policy_sha256 != policy.digest
        ):
            raise StorageError("stored workflow activation policy does not match")
        binding_rows = connection.execute(
            "SELECT capability_name, capability_version, "
            "project_capability_binding_id FROM "
            "workflow_activation_capability_bindings "
            "WHERE activation_version_id = ? "
            "ORDER BY capability_name, capability_version",
            (row["version_id"],),
        ).fetchall()
        stored_bindings = {
            (
                binding["capability_name"],
                binding["capability_version"],
                binding["project_capability_binding_id"],
            )
            for binding in binding_rows
        }
        expected_bindings = {
            (binding.name, binding.version, binding.binding_id)
            for binding in spec.capability_bindings
        }
        if stored_bindings != expected_bindings:
            raise StorageError(
                "stored workflow activation capability bindings do not match"
            )
        availability = self._workflow_activation_availability(
            connection,
            row=row,
            policy=policy,
            spec=spec,
        )
        return {
            "id": row["id"],
            "project_id": row["project_id"],
            "project_key": row["project_key"],
            "workflow_activation_version_id": row["version_id"],
            "version": row["version"],
            "policy": {
                "id": row["policy_id"],
                "name": row["policy_name"],
                "version": row["policy_version"],
                "content_sha256": row["policy_content_sha256"],
                "created_by": row["policy_created_by"],
                "created_at": row["policy_created_at"],
            },
            "workflow_configuration_version_id": row[
                "workflow_configuration_version_id"
            ],
            "spec": spec.to_dict(),
            "content_sha256": row["content_sha256"],
            "acknowledged_by": row["acknowledged_by"],
            "availability": availability,
            "created_at": row["created_at"],
            "activation_created_at": row["activation_created_at"],
        }

    def _workflow_activation_availability(
        self,
        connection: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        policy: WorkflowPolicySpec,
        spec: WorkflowActivationSpec,
    ) -> dict[str, str | None]:
        try:
            self._validate_activation(
                connection,
                project_id=row["project_id"],
                project_key=row["project_key"],
                policy=policy,
                spec=spec,
            )
            roles = connection.execute(
                "SELECT COUNT(*) AS role_count, "
                "SUM(CASE WHEN profile.active = 1 THEN 1 ELSE 0 END) "
                "AS active_role_count FROM workflow_configuration_role_bindings "
                "binding JOIN agent_profile_versions profile_version "
                "ON profile_version.id = binding.profile_version_id "
                "JOIN agent_profiles profile "
                "ON profile.id = profile_version.profile_id "
                "WHERE binding.configuration_version_id = ?",
                (row["workflow_configuration_version_id"],),
            ).fetchone()
            if (
                roles is None
                or roles["role_count"] != 2
                or roles["active_role_count"] != roles["role_count"]
            ):
                raise ConflictError("workflow activation role profile is unavailable")
        except (ConflictError, NotFoundError):
            return {
                "status": "unavailable",
                "reason": "activation_requirements_unavailable",
            }
        return {"status": "available", "reason": None}

    def _resolve_workflow_activation_for_start(
        self,
        connection: sqlite3.Connection,
        *,
        project_key: str,
        configuration_version_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        row = self._activation_row(connection, project_key, None)
        if row["version"] != expected_version:
            raise ConflictError("workflow activation version changed")
        activation = self._activation_record(connection, row)
        if activation["workflow_configuration_version_id"] != configuration_version_id:
            raise ConflictError("workflow activation configuration changed")
        try:
            policy = WorkflowPolicySpec.from_mapping(
                json.loads(row["policy_spec_json"])
            )
            spec = WorkflowActivationSpec.from_mapping(json.loads(row["spec_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored workflow activation is invalid") from exc
        self._validate_activation(
            connection,
            project_id=activation["project_id"],
            project_key=project_key,
            policy=policy,
            spec=spec,
        )
        return activation

    def get_workflow_activation(
        self, project_key: str, *, version: int | None = None
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        if version is not None:
            version = self._workflow_policy_positive_version(
                version, field="workflow activation version"
            )
        with self._connection() as connection:
            return self._activation_record(
                connection, self._activation_row(connection, project_key, version)
            )

    def list_workflow_activations(
        self, *, project_key: str | None = None
    ) -> list[dict[str, Any]]:
        if project_key is not None:
            return [self.get_workflow_activation(project_key)]
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT project.krab_project_key AS project_key "
                "FROM workflow_activations activation "
                "JOIN projects project ON project.id = activation.project_id "
                "ORDER BY project.krab_project_key"
            ).fetchall()
            return [
                self._activation_record(
                    connection,
                    self._activation_row(connection, row["project_key"], None),
                )
                for row in rows
            ]

    def _create_workflow_manual_wake(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        execution_id: str,
        correction_round: int,
        role: str,
        now: str,
    ) -> str | None:
        """Create the exact manual wake for an activated workflow role run."""

        execution = connection.execute(
            "SELECT execution.workflow_activation_version_id, "
            "execution.version, execution.correction_round, "
            "activation.acknowledged_by AS activation_owner, "
            "activation.spec_json "
            "FROM workflow_executions execution "
            "LEFT JOIN workflow_activation_versions activation "
            "ON activation.id = execution.workflow_activation_version_id "
            "WHERE execution.id = ?",
            (execution_id,),
        ).fetchone()
        if execution is None:
            raise NotFoundError(f"workflow execution not found: {execution_id}")
        activation_version_id = execution["workflow_activation_version_id"]
        if activation_version_id is None:
            return None
        if execution["correction_round"] != correction_round:
            raise ConflictError("workflow correction round changed")
        try:
            activation = WorkflowActivationSpec.from_mapping(
                json.loads(execution["spec_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored workflow activation is invalid") from exc
        wake_classes = {
            binding.role: binding.wake_class for binding in activation.wake_bindings
        }
        wake_class = wake_classes.get(role)
        if wake_class != "human_turn_only":
            raise StorageError("workflow activation role wake binding is invalid")
        wake_id = self.new_id("wwk")
        connection.execute(
            "INSERT INTO workflow_manual_wakes "
            "(id, run_id, execution_id, workflow_activation_version_id, "
            "execution_version, correction_round, role, wake_class, state, "
            "version, recipient_id, requested_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'requested', 1, ?, ?, ?)",
            (
                wake_id,
                run_id,
                execution_id,
                activation_version_id,
                execution["version"],
                correction_round,
                role,
                wake_class,
                execution["activation_owner"],
                now,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO workflow_manual_wake_events "
            "(wake_id, sequence, kind, from_state, to_state, actor_id, "
            "created_at) VALUES (?, 1, 'wake.requested', NULL, 'requested', "
            "NULL, ?)",
            (wake_id, now),
        )
        return wake_id

    @staticmethod
    def _workflow_manual_wake_row(
        connection: sqlite3.Connection, wake_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT wake.id, wake.run_id, wake.execution_id, "
            "wake.workflow_activation_version_id, wake.execution_version, "
            "wake.correction_round, wake.role, wake.wake_class, wake.state, "
            "wake.version, recipient.handle AS recipient, "
            "acknowledger.handle AS acknowledged_by, wake.requested_at, "
            "wake.updated_at, wake.acknowledged_at "
            "FROM workflow_manual_wakes wake "
            "JOIN users recipient ON recipient.id = wake.recipient_id "
            "LEFT JOIN users acknowledger ON acknowledger.id = wake.acknowledged_by "
            "WHERE wake.id = ?",
            (wake_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"workflow manual wake not found: {wake_id}")
        return row

    @staticmethod
    def _workflow_manual_wake_record(
        connection: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        events = connection.execute(
            "SELECT event.sequence, event.kind, event.from_state, "
            "event.to_state, actor.handle AS actor, event.created_at "
            "FROM workflow_manual_wake_events event "
            "LEFT JOIN users actor ON actor.id = event.actor_id "
            "WHERE event.wake_id = ? ORDER BY event.sequence",
            (row["id"],),
        ).fetchall()
        availability = connection.execute(
            "SELECT observation.id, observation.sequence, "
            "observation.wake_version, observation.state, "
            "observation.unavailable_until, observation.blocked_on, "
            "observer.handle AS observed_by, observation.idempotency_key, "
            "observation.created_at FROM "
            "workflow_wake_availability_observations observation "
            "JOIN users observer ON observer.id = observation.observed_by "
            "WHERE observation.wake_id = ? "
            "ORDER BY observation.sequence DESC LIMIT 1",
            (row["id"],),
        ).fetchone()
        observed = row["acknowledged_at"] is not None
        return {
            **dict(row),
            "events": [dict(event) for event in events],
            "availability": None if availability is None else dict(availability),
            "binding_observation": {
                "status": "observed" if observed else "unobserved",
                "wake_class": row["wake_class"],
                "observed_at": row["acknowledged_at"],
            },
        }

    def get_workflow_manual_wake(self, wake_id: str) -> dict[str, Any]:
        wake_id = require_string(wake_id, field="wake_id", max_bytes=64)
        with self._connection() as connection:
            return self._workflow_manual_wake_record(
                connection,
                self._workflow_manual_wake_row(connection, wake_id),
            )

    def list_workflow_manual_wakes(
        self,
        *,
        execution_id: str | None = None,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if execution_id is not None:
            execution_id = require_string(
                execution_id, field="execution_id", max_bytes=64
            )
        if run_id is not None:
            run_id = require_string(run_id, field="run_id", max_bytes=64)
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT id FROM workflow_manual_wakes "
                "WHERE (? IS NULL OR execution_id = ?) "
                "AND (? IS NULL OR run_id = ?) "
                "ORDER BY requested_at, id",
                (execution_id, execution_id, run_id, run_id),
            ).fetchall()
            return [
                self._workflow_manual_wake_record(
                    connection,
                    self._workflow_manual_wake_row(connection, row["id"]),
                )
                for row in rows
            ]

    def acknowledge_workflow_manual_wake(
        self,
        wake_id: str,
        *,
        expected_version: int,
        acknowledged_by: str,
    ) -> dict[str, Any]:
        wake_id = require_string(wake_id, field="wake_id", max_bytes=64)
        expected_version = self._workflow_policy_positive_version(
            expected_version, field="expected wake version"
        )
        acknowledged_by = require_string(
            acknowledged_by, field="acknowledged_by", max_bytes=64
        )
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, acknowledged_by, role="workflow wake recipient"
                )
                wake = connection.execute(
                    "SELECT state, version, recipient_id "
                    "FROM workflow_manual_wakes WHERE id = ?",
                    (wake_id,),
                ).fetchone()
                if wake is None:
                    raise NotFoundError(f"workflow manual wake not found: {wake_id}")
                if wake["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"workflow manual wake version is {wake['version']}, "
                        f"expected {expected_version}"
                    )
                if wake["recipient_id"] != acknowledged_by:
                    raise ConflictError(
                        "workflow manual wake may only be acknowledged by its recipient"
                    )
                if wake["state"] not in {"requested", "delivered"}:
                    raise ConflictError(
                        f"workflow manual wake cannot be acknowledged: {wake['state']}"
                    )
                from_state = wake["state"]
                changed = connection.execute(
                    "UPDATE workflow_manual_wakes SET state = 'acknowledged', "
                    "version = version + 1, acknowledged_by = ?, updated_at = ?, "
                    "acknowledged_at = ? WHERE id = ? AND version = ?",
                    (acknowledged_by, now, now, wake_id, expected_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "workflow manual wake changed concurrently"
                    )
                connection.execute(
                    "INSERT INTO workflow_manual_wake_events "
                    "(wake_id, sequence, kind, from_state, to_state, actor_id, "
                    "created_at) VALUES (?, ?, 'wake.acknowledged', ?, "
                    "'acknowledged', ?, ?)",
                    (
                        wake_id,
                        expected_version + 1,
                        from_state,
                        acknowledged_by,
                        now,
                    ),
                )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow manual wake could not be acknowledged"
                ) from exc
        return self.get_workflow_manual_wake(wake_id)

    def transition_workflow_manual_wake(
        self,
        wake_id: str,
        *,
        expected_version: int,
        target_state: str,
        actor_id: str,
    ) -> dict[str, Any]:
        """Record one explicit manual wake delivery, expiry, or escalation."""

        wake_id = require_string(wake_id, field="wake_id", max_bytes=64)
        expected_version = self._workflow_policy_positive_version(
            expected_version, field="expected wake version"
        )
        if target_state not in {"delivered", "expired", "escalated"}:
            raise ValidationError("workflow manual wake target state is invalid")
        actor_id = require_string(actor_id, field="actor_id", max_bytes=64)
        transitions = {
            "requested": {"delivered", "expired", "escalated"},
            "delivered": {"expired", "escalated"},
            "expired": {"escalated"},
        }
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, actor_id, role="workflow wake observer")
                wake = connection.execute(
                    "SELECT wake.state, wake.version, wake.recipient_id, "
                    "execution.requested_by FROM workflow_manual_wakes wake "
                    "JOIN workflow_executions execution "
                    "ON execution.id = wake.execution_id WHERE wake.id = ?",
                    (wake_id,),
                ).fetchone()
                if wake is None:
                    raise NotFoundError(f"workflow manual wake not found: {wake_id}")
                if wake["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"workflow manual wake version is {wake['version']}, "
                        f"expected {expected_version}"
                    )
                if actor_id not in {wake["recipient_id"], wake["requested_by"]}:
                    raise ConflictError(
                        "workflow manual wake transition requires its recipient "
                        "or workflow requester"
                    )
                if target_state not in transitions.get(wake["state"], set()):
                    raise ConflictError(
                        "workflow manual wake transition is not allowed: "
                        f"{wake['state']} -> {target_state}"
                    )
                changed = connection.execute(
                    "UPDATE workflow_manual_wakes SET state = ?, "
                    "version = version + 1, updated_at = ? "
                    "WHERE id = ? AND version = ?",
                    (target_state, now, wake_id, expected_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "workflow manual wake changed concurrently"
                    )
                connection.execute(
                    "INSERT INTO workflow_manual_wake_events "
                    "(wake_id, sequence, kind, from_state, to_state, actor_id, "
                    "created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        wake_id,
                        expected_version + 1,
                        f"wake.{target_state}",
                        wake["state"],
                        target_state,
                        actor_id,
                        now,
                    ),
                )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow manual wake transition could not be recorded"
                ) from exc
        return self.get_workflow_manual_wake(wake_id)

    def record_workflow_wake_availability(
        self,
        wake_id: str,
        *,
        expected_wake_version: int,
        state: str,
        unavailable_until: str | None,
        blocked_on: str | None,
        observed_by: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Append one recipient-authored availability fact for an exact wake."""

        wake_id = require_string(wake_id, field="wake_id", max_bytes=64)
        expected_wake_version = self._workflow_policy_positive_version(
            expected_wake_version, field="expected wake version"
        )
        if state not in {"available", "unavailable_until", "blocked_on"}:
            raise ValidationError("workflow wake availability state is invalid")
        if state == "available":
            if unavailable_until is not None or blocked_on is not None:
                raise ValidationError("available wake has no delay or blocker")
        elif state == "unavailable_until":
            if unavailable_until is None or blocked_on is not None:
                raise ValidationError(
                    "unavailable-until wake requires only one timestamp"
                )
            parse_rfc3339_timestamp(
                unavailable_until, field="workflow wake unavailable_until"
            )
        else:
            if unavailable_until is not None or blocked_on is None:
                raise ValidationError("blocked wake requires only one blocker")
            blocked_on = require_string(
                blocked_on, field="workflow wake blocked_on", max_bytes=256
            )
        observed_by = require_string(observed_by, field="observed_by", max_bytes=64)
        idempotency_key = require_string(
            idempotency_key, field="idempotency_key", max_bytes=128
        )
        if self._WAKE_IDEMPOTENCY_KEY_RE.fullmatch(idempotency_key) is None:
            raise ValidationError("workflow wake idempotency key is invalid")
        now = self.now()
        observation_id = self.new_id("wao")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, observed_by, role="workflow wake recipient"
                )
                wake = connection.execute(
                    "SELECT version, recipient_id FROM workflow_manual_wakes "
                    "WHERE id = ?",
                    (wake_id,),
                ).fetchone()
                if wake is None:
                    raise NotFoundError(f"workflow manual wake not found: {wake_id}")
                existing = connection.execute(
                    "SELECT id, wake_version, state, unavailable_until, blocked_on, "
                    "observed_by FROM workflow_wake_availability_observations "
                    "WHERE wake_id = ? AND idempotency_key = ?",
                    (wake_id, idempotency_key),
                ).fetchone()
                expected = (
                    expected_wake_version,
                    state,
                    unavailable_until,
                    blocked_on,
                    observed_by,
                )
                if existing is not None:
                    actual = tuple(
                        existing[field]
                        for field in (
                            "wake_version",
                            "state",
                            "unavailable_until",
                            "blocked_on",
                            "observed_by",
                        )
                    )
                    if actual != expected:
                        raise ConflictError("workflow wake idempotency key was reused")
                    connection.commit()
                    return self.get_workflow_manual_wake(wake_id)
                if wake["version"] != expected_wake_version:
                    raise ConcurrentUpdateError(
                        f"workflow manual wake version is {wake['version']}, "
                        f"expected {expected_wake_version}"
                    )
                if wake["recipient_id"] != observed_by:
                    raise ConflictError(
                        "workflow wake availability may only be recorded by "
                        "its recipient"
                    )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM "
                    "workflow_wake_availability_observations WHERE wake_id = ?",
                    (wake_id,),
                ).fetchone()["sequence"]
                connection.execute(
                    "INSERT INTO workflow_wake_availability_observations "
                    "(id, wake_id, sequence, wake_version, state, "
                    "unavailable_until, blocked_on, observed_by, idempotency_key, "
                    "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        observation_id,
                        wake_id,
                        sequence,
                        expected_wake_version,
                        state,
                        unavailable_until,
                        blocked_on,
                        observed_by,
                        idempotency_key,
                        now,
                    ),
                )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow wake availability could not be recorded"
                ) from exc
        return self.get_workflow_manual_wake(wake_id)
