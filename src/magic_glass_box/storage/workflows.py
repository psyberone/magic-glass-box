from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime
from typing import Any

from ..advisory import AdvisoryConfigurationSpec
from ..discovery import (
    DiscoveryConfigurationSpec,
    DiscoveryDisposition,
    DiscoveryRuling,
)
from ..domain import (
    AgentProfileSpec,
    GatewayCapability,
    ValidationError,
    WorkItemEnvelope,
    WorkLogEntry,
    WorkLogKind,
    WorkspaceMode,
    canonical_json,
    content_sha256,
    require_string,
    validate_name,
    validate_project_key,
)
from ..gates import ReviewSeparation
from ..krab import IDEMPOTENCY_KEY_RE
from ..provider_effects import (
    OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION,
    ChangeOpenReadback,
    LandingReadback,
    RefPublicationReadback,
    WorkflowProviderIntent,
)
from ..service_sidecar import (
    ServiceOperationReceipt,
    ServiceOperationSpec,
    core_service_capability,
)
from ..workflow_closeout import (
    WorkflowCloseoutDeclaration,
    validate_delivery_closeout,
)
from ..workflow_policy import WorkflowActivationSpec, WorkflowPolicySpec
from ..workflow_rounds import WorkflowRoundCause
from ..workflow_scope import WorkflowScopeReview
from ..workflows import (
    ADVISORY_REVIEW_TEMPLATE_ID,
    ADVISORY_REVIEW_TEMPLATE_REVISION,
    BUILT_IN_WORKFLOW_TEMPLATE_DEFINITIONS,
    DISCOVERY_TEMPLATE_ID,
    DISCOVERY_TEMPLATE_REVISION,
    IMPLEMENTATION_TEMPLATE_ID,
    IMPLEMENTATION_TEMPLATE_REVISION,
    WorkflowConfigurationSpec,
    WorkflowLandingMode,
    WorkflowTemplateDefinition,
)
from .errors import (
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)

OBJECT_REVISION_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class WorkflowStorage:
    @staticmethod
    def _workflow_idempotency_key(value: object) -> str:
        if not isinstance(value, str) or IDEMPOTENCY_KEY_RE.fullmatch(value) is None:
            raise ValidationError("idempotency_key is invalid")
        return value

    @staticmethod
    def _positive_integer(value: object, *, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError(f"{field} must be a positive integer")
        return value

    @staticmethod
    def _source_revision(value: object) -> str:
        if not isinstance(value, str) or OBJECT_REVISION_RE.fullmatch(value) is None:
            raise ValidationError(
                "source_revision must be a 40- or 64-character object ID"
            )
        return value

    @staticmethod
    def _configuration_manifest(
        spec: (
            WorkflowConfigurationSpec
            | DiscoveryConfigurationSpec
            | AdvisoryConfigurationSpec
        ),
        *,
        role_profile_version_ids: dict[str, str],
        template_definition_id: str = IMPLEMENTATION_TEMPLATE_ID,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "template_definition_id": template_definition_id,
            "spec": spec.to_dict(),
            "roles": dict(role_profile_version_ids),
        }

    @staticmethod
    def _definition_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            definition = WorkflowTemplateDefinition.from_mapping(
                json.loads(row["definition_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError(
                "stored workflow template definition is invalid"
            ) from exc
        expected = BUILT_IN_WORKFLOW_TEMPLATE_DEFINITIONS.get(row["id"])
        if (
            expected is None
            or row["template_key"] != definition.key
            or row["version"] != definition.version
            or row["content_sha256"] != definition.digest
            or definition != expected
            or row["built_in"] != 1
        ):
            raise StorageError("stored workflow template definition does not match")
        return {
            "id": row["id"],
            "key": row["template_key"],
            "version": row["version"],
            "revision": row["content_sha256"],
            "built_in": bool(row["built_in"]),
            "definition": definition.to_dict(),
            "created_at": row["template_created_at"],
        }

    @staticmethod
    def _profile_pin(
        connection: sqlite3.Connection, profile_name: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT p.id AS profile_id, p.name, p.active, "
            "v.id AS profile_version_id, v.version AS profile_version, "
            "v.spec_json, v.content_sha256, v.krab_identity_id, "
            "ki.handle AS krab_handle FROM agent_profiles p "
            "JOIN agent_profile_versions v ON v.profile_id = p.id "
            "JOIN krab_identities ki ON ki.id = v.krab_identity_id "
            "WHERE p.name = ? ORDER BY v.version DESC LIMIT 1",
            (profile_name,),
        ).fetchone()
        if row is None or row["active"] != 1:
            raise NotFoundError(f"active agent profile not found: {profile_name}")
        try:
            spec = AgentProfileSpec.from_mapping(json.loads(row["spec_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored agent profile is invalid") from exc
        if spec.digest != row["content_sha256"]:
            raise StorageError("stored agent profile digest does not match")
        return {**dict(row), "spec": spec}

    @staticmethod
    def _validate_roles(
        spec: WorkflowConfigurationSpec,
        producer: dict[str, Any],
        reviewer: dict[str, Any],
    ) -> None:
        if producer["profile_id"] == reviewer["profile_id"]:
            raise ConflictError(
                "workflow producer and reviewer require distinct profiles"
            )
        if producer["spec"].workspace_mode is not WorkspaceMode.COPY_ON_WRITE:
            raise ConflictError(
                "workflow producer profile must use copy-on-write workspace"
            )
        if reviewer["spec"].workspace_mode is not WorkspaceMode.READ_ONLY:
            raise ConflictError(
                "workflow reviewer profile must use a read-only workspace"
            )
        if (
            spec.review_separation
            in {
                ReviewSeparation.DIFFERENT_IDENTITY,
                ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
            }
            and producer["krab_identity_id"] == reviewer["krab_identity_id"]
        ):
            raise ConflictError(
                "workflow producer and reviewer require distinct Krab identities"
            )

    @staticmethod
    def _validate_scoped_workflow_profiles(
        connection: sqlite3.Connection, configuration_version_id: str
    ) -> None:
        rows = connection.execute(
            "SELECT binding.role, version.spec_json "
            "FROM workflow_configuration_role_bindings binding "
            "JOIN agent_profile_versions version "
            "ON version.id = binding.profile_version_id "
            "WHERE binding.configuration_version_id = ?",
            (configuration_version_id,),
        ).fetchall()
        required = {
            "producer": {GatewayCapability.RUN_DESCRIBE},
            "reviewer": {
                GatewayCapability.RUN_DESCRIBE,
                GatewayCapability.WORK_APPEND,
                GatewayCapability.WORKFLOW_SCOPE_REVIEW_SUBMIT,
            },
        }
        if {row["role"] for row in rows} != set(required):
            raise StorageError("stored workflow configuration roles do not match")
        for row in rows:
            try:
                profile = AgentProfileSpec.from_mapping(json.loads(row["spec_json"]))
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError("stored workflow profile is invalid") from exc
            missing = required[row["role"]].difference(profile.gateway_capabilities)
            if missing:
                raise ConflictError(
                    f"scoped workflow {row['role']} lacks gateway capability: "
                    + ", ".join(sorted(capability.value for capability in missing))
                )

    @staticmethod
    def _validate_discovery_roles(
        _spec: DiscoveryConfigurationSpec,
        investigator: dict[str, Any],
        coordinator: dict[str, Any],
    ) -> None:
        if investigator["profile_id"] == coordinator["profile_id"]:
            raise ConflictError(
                "discovery investigator and coordinator require distinct profiles"
            )
        if investigator["krab_identity_id"] == coordinator["krab_identity_id"]:
            raise ConflictError(
                "discovery investigator and coordinator require distinct "
                "Krab identities"
            )
        required = {
            "investigator": {
                GatewayCapability.RUN_DESCRIBE,
                GatewayCapability.DISCOVERY_TRACE_SUBMIT,
            },
            "coordinator": {
                GatewayCapability.RUN_DESCRIBE,
                GatewayCapability.INTERACTION_OPEN,
                GatewayCapability.INTERACTION_READ,
                GatewayCapability.HANDOFF_SUBMIT,
                GatewayCapability.DISCOVERY_RULING_SUBMIT,
            },
        }
        for role, pin in (
            ("investigator", investigator),
            ("coordinator", coordinator),
        ):
            spec = pin["spec"]
            if spec.workspace_mode is not WorkspaceMode.READ_ONLY:
                raise ConflictError(f"discovery {role} must use a read-only workspace")
            missing = required[role].difference(spec.gateway_capabilities)
            if missing:
                raise ConflictError(
                    f"discovery {role} lacks gateway capability: "
                    + ", ".join(sorted(capability.value for capability in missing))
                )
        if "krab" not in coordinator["spec"].credential_names:
            raise ConflictError("discovery coordinator requires the krab authority")

    @staticmethod
    def _validate_advisory_roles(
        _spec: AdvisoryConfigurationSpec,
        role_pins: dict[str, dict[str, Any]],
    ) -> None:
        if len({pin["profile_id"] for pin in role_pins.values()}) != len(role_pins):
            raise ConflictError("advisory roles require distinct profiles")
        if len({pin["krab_identity_id"] for pin in role_pins.values()}) != len(
            role_pins
        ):
            raise ConflictError("advisory roles require distinct Krab identities")
        reviewer_capabilities = {
            GatewayCapability.RUN_DESCRIBE,
            GatewayCapability.ADVISORY_BLIND_SUBMIT,
            GatewayCapability.ADVISORY_CHALLENGE_SUBMIT,
        }
        coordinator_capabilities = {
            GatewayCapability.RUN_DESCRIBE,
            GatewayCapability.ADVISORY_REPORT_SUBMIT,
        }
        for role, pin in role_pins.items():
            profile_spec = pin["spec"]
            if profile_spec.workspace_mode is not WorkspaceMode.READ_ONLY:
                raise ConflictError(f"advisory {role} must use a read-only workspace")
            required = (
                coordinator_capabilities
                if role == "coordinator"
                else reviewer_capabilities
            )
            missing = required.difference(profile_spec.gateway_capabilities)
            if missing:
                raise ConflictError(
                    f"advisory {role} lacks gateway capability: "
                    + ", ".join(sorted(capability.value for capability in missing))
                )

    def list_workflow_template_definitions(self) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT id, template_key, version, definition_json, "
                "content_sha256, built_in, created_at AS template_created_at "
                "FROM workflow_template_definitions ORDER BY template_key, version"
            ).fetchall()
        return [self._definition_record(row) for row in rows]

    def _configuration_row(
        self,
        connection: sqlite3.Connection,
        *,
        project_key: str | None = None,
        name: str | None = None,
        version: int | None = None,
        version_id: str | None = None,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT c.id, c.name, c.project_id, c.created_at AS config_created_at, "
            "p.krab_project_key AS project_key, cv.id AS version_id, "
            "cv.version, cv.spec_json, cv.content_sha256, cv.created_at, "
            "creator.handle AS created_by, t.id AS template_id, "
            "t.template_key, t.version AS template_version, "
            "t.definition_json, t.content_sha256 AS template_revision, "
            "t.built_in, t.created_at AS template_created_at "
            "FROM workflow_configurations c "
            "JOIN projects p ON p.id = c.project_id "
            "JOIN workflow_configuration_versions cv "
            "ON cv.configuration_id = c.id "
            "JOIN workflow_template_definitions t "
            "ON t.id = cv.template_definition_id "
            "JOIN users creator ON creator.id = cv.created_by "
            "WHERE "
            "((? IS NOT NULL AND cv.id = ?) OR "
            "(? IS NULL AND p.krab_project_key = ? AND c.name = ? "
            "AND (? IS NULL OR cv.version = ?))) "
            "ORDER BY cv.version DESC LIMIT 1",
            (
                version_id,
                version_id,
                version_id,
                project_key,
                name,
                version,
                version,
            ),
        ).fetchone()
        if row is None:
            label = version_id if version_id is not None else f"{project_key}/{name}"
            raise NotFoundError(f"workflow configuration not found: {label}")
        return row

    def _configuration_record(
        self, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        definition = self._definition_record(
            {
                "id": row["template_id"],
                "template_key": row["template_key"],
                "version": row["template_version"],
                "definition_json": row["definition_json"],
                "content_sha256": row["template_revision"],
                "built_in": row["built_in"],
                "template_created_at": row["template_created_at"],
            }
        )
        try:
            raw_spec = json.loads(row["spec_json"])
            if row["template_id"] == IMPLEMENTATION_TEMPLATE_ID:
                spec: (
                    WorkflowConfigurationSpec
                    | DiscoveryConfigurationSpec
                    | AdvisoryConfigurationSpec
                ) = WorkflowConfigurationSpec.from_mapping(raw_spec)
            elif row["template_id"] == DISCOVERY_TEMPLATE_ID:
                spec = DiscoveryConfigurationSpec.from_mapping(raw_spec)
            elif row["template_id"] == ADVISORY_REVIEW_TEMPLATE_ID:
                spec = AdvisoryConfigurationSpec.from_mapping(raw_spec)
            else:
                raise ValidationError("workflow configuration template is unsupported")
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored workflow configuration is invalid") from exc
        parsed_definition = WorkflowTemplateDefinition.from_mapping(
            definition["definition"]
        )
        role_rows = connection.execute(
            "SELECT binding.role, profile.id AS profile_id, "
            "profile.name AS profile_name, profile.active AS profile_active, "
            "version.id AS profile_version_id, "
            "version.version AS profile_version, version.spec_json, "
            "version.content_sha256 AS spec_sha256, "
            "version.krab_identity_id, identity.handle AS krab_handle "
            "FROM workflow_configuration_role_bindings binding "
            "JOIN agent_profile_versions version "
            "ON version.id = binding.profile_version_id "
            "JOIN agent_profiles profile ON profile.id = version.profile_id "
            "JOIN krab_identities identity "
            "ON identity.id = version.krab_identity_id "
            "WHERE binding.configuration_version_id = ? ORDER BY binding.role",
            (row["version_id"],),
        ).fetchall()
        expected_modes = parsed_definition.role_workspace_modes
        if {role["role"] for role in role_rows} != set(expected_modes):
            raise StorageError("stored workflow configuration roles do not match")
        roles: dict[str, dict[str, Any]] = {}
        role_pins: dict[str, dict[str, Any]] = {}
        role_profile_version_ids: dict[str, str] = {}
        for role in role_rows:
            try:
                profile_spec = AgentProfileSpec.from_mapping(
                    json.loads(role["spec_json"])
                )
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError("stored workflow profile is invalid") from exc
            if profile_spec.digest != role["spec_sha256"]:
                raise StorageError("stored workflow profile digest does not match")
            if profile_spec.workspace_mode.value != expected_modes[role["role"]]:
                raise StorageError("stored workflow role workspace does not match")
            role_pins[role["role"]] = {**dict(role), "spec": profile_spec}
            role_profile_version_ids[role["role"]] = role["profile_version_id"]
            roles[role["role"]] = {
                "profile_id": role["profile_id"],
                "profile_name": role["profile_name"],
                "profile_version_id": role["profile_version_id"],
                "profile_version": role["profile_version"],
                "workspace_mode": profile_spec.workspace_mode.value,
                "krab_identity_id": role["krab_identity_id"],
                "krab_handle": role["krab_handle"],
            }
        if row["template_id"] == IMPLEMENTATION_TEMPLATE_ID:
            assert isinstance(spec, WorkflowConfigurationSpec)
            self._validate_roles(spec, role_pins["producer"], role_pins["reviewer"])
        elif row["template_id"] == DISCOVERY_TEMPLATE_ID:
            assert isinstance(spec, DiscoveryConfigurationSpec)
            self._validate_discovery_roles(
                spec,
                role_pins["investigator"],
                role_pins["coordinator"],
            )
        elif row["template_id"] == ADVISORY_REVIEW_TEMPLATE_ID:
            assert isinstance(spec, AdvisoryConfigurationSpec)
            self._validate_advisory_roles(spec, role_pins)
        manifest = self._configuration_manifest(
            spec,
            role_profile_version_ids=role_profile_version_ids,
            template_definition_id=row["template_id"],
        )
        if content_sha256(canonical_json(manifest)) != row["content_sha256"]:
            raise StorageError("stored workflow configuration digest does not match")
        definition.pop("definition")
        return {
            "id": row["id"],
            "project_id": row["project_id"],
            "project_key": row["project_key"],
            "name": row["name"],
            "workflow_configuration_version_id": row["version_id"],
            "version": row["version"],
            "content_sha256": row["content_sha256"],
            "template": definition,
            "spec": spec.to_dict(),
            "roles": roles,
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "configuration_created_at": row["config_created_at"],
        }

    def create_workflow_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: WorkflowConfigurationSpec,
        producer_profile_name: str,
        reviewer_profile_name: str,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_workflow_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            producer_profile_name=producer_profile_name,
            reviewer_profile_name=reviewer_profile_name,
            created_by=created_by,
            create=True,
        )

    def revise_workflow_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: WorkflowConfigurationSpec,
        producer_profile_name: str,
        reviewer_profile_name: str,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_workflow_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            producer_profile_name=producer_profile_name,
            reviewer_profile_name=reviewer_profile_name,
            created_by=created_by,
            create=False,
        )

    def _write_workflow_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: WorkflowConfigurationSpec,
        producer_profile_name: str,
        reviewer_profile_name: str,
        created_by: str,
        create: bool,
    ) -> dict[str, Any]:
        if not isinstance(spec, WorkflowConfigurationSpec):
            raise ValidationError("spec must be a WorkflowConfigurationSpec")
        return self._write_prescription_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            role_profile_names={
                "producer": producer_profile_name,
                "reviewer": reviewer_profile_name,
            },
            template_definition_id=IMPLEMENTATION_TEMPLATE_ID,
            template_revision=IMPLEMENTATION_TEMPLATE_REVISION,
            created_by=created_by,
            create=create,
        )

    def create_discovery_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: DiscoveryConfigurationSpec,
        investigator_profile_name: str,
        coordinator_profile_name: str,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_discovery_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            investigator_profile_name=investigator_profile_name,
            coordinator_profile_name=coordinator_profile_name,
            created_by=created_by,
            create=True,
        )

    def revise_discovery_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: DiscoveryConfigurationSpec,
        investigator_profile_name: str,
        coordinator_profile_name: str,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_discovery_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            investigator_profile_name=investigator_profile_name,
            coordinator_profile_name=coordinator_profile_name,
            created_by=created_by,
            create=False,
        )

    def _write_discovery_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: DiscoveryConfigurationSpec,
        investigator_profile_name: str,
        coordinator_profile_name: str,
        created_by: str,
        create: bool,
    ) -> dict[str, Any]:
        if not isinstance(spec, DiscoveryConfigurationSpec):
            raise ValidationError("spec must be a DiscoveryConfigurationSpec")
        return self._write_prescription_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            role_profile_names={
                "investigator": investigator_profile_name,
                "coordinator": coordinator_profile_name,
            },
            template_definition_id=DISCOVERY_TEMPLATE_ID,
            template_revision=DISCOVERY_TEMPLATE_REVISION,
            created_by=created_by,
            create=create,
        )

    def create_advisory_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: AdvisoryConfigurationSpec,
        reviewer_one_profile_name: str,
        reviewer_two_profile_name: str,
        reviewer_three_profile_name: str,
        coordinator_profile_name: str,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_advisory_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            reviewer_one_profile_name=reviewer_one_profile_name,
            reviewer_two_profile_name=reviewer_two_profile_name,
            reviewer_three_profile_name=reviewer_three_profile_name,
            coordinator_profile_name=coordinator_profile_name,
            created_by=created_by,
            create=True,
        )

    def revise_advisory_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: AdvisoryConfigurationSpec,
        reviewer_one_profile_name: str,
        reviewer_two_profile_name: str,
        reviewer_three_profile_name: str,
        coordinator_profile_name: str,
        created_by: str,
    ) -> dict[str, Any]:
        return self._write_advisory_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            reviewer_one_profile_name=reviewer_one_profile_name,
            reviewer_two_profile_name=reviewer_two_profile_name,
            reviewer_three_profile_name=reviewer_three_profile_name,
            coordinator_profile_name=coordinator_profile_name,
            created_by=created_by,
            create=False,
        )

    def _write_advisory_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: AdvisoryConfigurationSpec,
        reviewer_one_profile_name: str,
        reviewer_two_profile_name: str,
        reviewer_three_profile_name: str,
        coordinator_profile_name: str,
        created_by: str,
        create: bool,
    ) -> dict[str, Any]:
        if not isinstance(spec, AdvisoryConfigurationSpec):
            raise ValidationError("spec must be an AdvisoryConfigurationSpec")
        return self._write_prescription_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            role_profile_names={
                "reviewer_one": reviewer_one_profile_name,
                "reviewer_two": reviewer_two_profile_name,
                "reviewer_three": reviewer_three_profile_name,
                "coordinator": coordinator_profile_name,
            },
            template_definition_id=ADVISORY_REVIEW_TEMPLATE_ID,
            template_revision=ADVISORY_REVIEW_TEMPLATE_REVISION,
            created_by=created_by,
            create=create,
        )

    def _write_prescription_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: (
            WorkflowConfigurationSpec
            | DiscoveryConfigurationSpec
            | AdvisoryConfigurationSpec
        ),
        role_profile_names: dict[str, str],
        template_definition_id: str,
        template_revision: str,
        created_by: str,
        create: bool,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        name = validate_name(name, field="workflow configuration name")
        definition_spec = BUILT_IN_WORKFLOW_TEMPLATE_DEFINITIONS.get(
            template_definition_id
        )
        if (
            definition_spec is None
            or definition_spec.digest != template_revision
            or set(role_profile_names) != set(definition_spec.role_workspace_modes)
        ):
            raise ValidationError("prescription role bindings are invalid")
        role_profile_names = {
            role: validate_name(profile_name, field=f"{role} profile name")
            for role, profile_name in role_profile_names.items()
        }
        configuration_id = self.new_id("wfc")
        version_id = self.new_id("wfv")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, created_by, role="workflow creator")
                project = connection.execute(
                    "SELECT id, active FROM projects WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None or project["active"] != 1:
                    raise NotFoundError(f"active project not found: {project_key}")
                definition = connection.execute(
                    "SELECT id, content_sha256 FROM workflow_template_definitions "
                    "WHERE id = ?",
                    (template_definition_id,),
                ).fetchone()
                if (
                    definition is None
                    or definition["content_sha256"] != template_revision
                ):
                    raise StorageError("built-in workflow definition is unavailable")
                role_pins = {
                    role: self._profile_pin(connection, profile_name)
                    for role, profile_name in role_profile_names.items()
                }
                if template_definition_id == IMPLEMENTATION_TEMPLATE_ID:
                    assert isinstance(spec, WorkflowConfigurationSpec)
                    self._validate_roles(
                        spec, role_pins["producer"], role_pins["reviewer"]
                    )
                elif template_definition_id == DISCOVERY_TEMPLATE_ID:
                    assert isinstance(spec, DiscoveryConfigurationSpec)
                    self._validate_discovery_roles(
                        spec,
                        role_pins["investigator"],
                        role_pins["coordinator"],
                    )
                elif template_definition_id == ADVISORY_REVIEW_TEMPLATE_ID:
                    assert isinstance(spec, AdvisoryConfigurationSpec)
                    self._validate_advisory_roles(spec, role_pins)
                if create:
                    version = 1
                    connection.execute(
                        "INSERT INTO workflow_configurations "
                        "(id, project_id, name, created_by, created_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (configuration_id, project["id"], name, created_by, now),
                    )
                else:
                    configuration = connection.execute(
                        "SELECT c.id, latest.template_definition_id "
                        "FROM workflow_configurations c "
                        "JOIN workflow_configuration_versions latest "
                        "ON latest.id = (SELECT cv.id "
                        "FROM workflow_configuration_versions cv "
                        "WHERE cv.configuration_id = c.id "
                        "ORDER BY cv.version DESC LIMIT 1) "
                        "WHERE c.project_id = ? AND c.name = ?",
                        (project["id"], name),
                    ).fetchone()
                    if configuration is None:
                        raise NotFoundError(
                            f"workflow configuration not found: {project_key}/{name}"
                        )
                    if (
                        configuration["template_definition_id"]
                        != template_definition_id
                    ):
                        raise ConflictError(
                            "workflow configuration belongs to another prescription"
                        )
                    configuration_id = configuration["id"]
                    version = connection.execute(
                        "SELECT COALESCE(MAX(version), 0) + 1 AS version "
                        "FROM workflow_configuration_versions "
                        "WHERE configuration_id = ?",
                        (configuration_id,),
                    ).fetchone()["version"]
                manifest = self._configuration_manifest(
                    spec,
                    role_profile_version_ids={
                        role: pin["profile_version_id"]
                        for role, pin in role_pins.items()
                    },
                    template_definition_id=template_definition_id,
                )
                connection.execute(
                    "INSERT INTO workflow_configuration_versions "
                    "(id, configuration_id, version, template_definition_id, "
                    "schema_version, spec_json, content_sha256, created_by, "
                    "created_at) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)",
                    (
                        version_id,
                        configuration_id,
                        version,
                        template_definition_id,
                        spec.canonical,
                        content_sha256(canonical_json(manifest)),
                        created_by,
                        now,
                    ),
                )
                connection.executemany(
                    "INSERT INTO workflow_configuration_role_bindings "
                    "(configuration_version_id, role, profile_version_id) "
                    "VALUES (?, ?, ?)",
                    (
                        (version_id, role, pin["profile_version_id"])
                        for role, pin in role_pins.items()
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                message = (
                    "workflow configuration already exists or is invalid"
                    if create
                    else "identical workflow configuration version already exists"
                )
                raise ConflictError(message) from exc
        return self.get_workflow_configuration(project_key, name, version=version)

    def get_workflow_configuration(
        self, project_key: str, name: str, *, version: int | None = None
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        name = validate_name(name, field="workflow configuration name")
        if version is not None:
            self._positive_integer(version, field="version")
        with self._read_snapshot() as connection:
            row = self._configuration_row(
                connection, project_key=project_key, name=name, version=version
            )
            return self._configuration_record(connection, row)

    def list_workflow_configurations(
        self, *, project_key: str | None = None
    ) -> list[dict[str, Any]]:
        if project_key is not None:
            project_key = validate_project_key(project_key)
        with self._read_snapshot() as connection:
            if project_key is None:
                rows = connection.execute(
                    "SELECT c.id, p.krab_project_key AS project_key, c.name "
                    "FROM workflow_configurations c JOIN projects p "
                    "ON p.id = c.project_id ORDER BY p.krab_project_key, c.name"
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT c.id, p.krab_project_key AS project_key, c.name "
                    "FROM workflow_configurations c JOIN projects p "
                    "ON p.id = c.project_id WHERE p.krab_project_key = ? "
                    "ORDER BY c.name",
                    (project_key,),
                ).fetchall()
            return [
                self._configuration_record(
                    connection,
                    self._configuration_row(
                        connection,
                        project_key=row["project_key"],
                        name=row["name"],
                    ),
                )
                for row in rows
            ]

    def get_discovery_configuration(
        self, project_key: str, name: str, *, version: int | None = None
    ) -> dict[str, Any]:
        configuration = self.get_workflow_configuration(
            project_key, name, version=version
        )
        if configuration["template"]["id"] != DISCOVERY_TEMPLATE_ID:
            raise NotFoundError(
                f"discovery configuration not found: {project_key}/{name}"
            )
        return configuration

    def list_discovery_configurations(
        self, *, project_key: str | None = None
    ) -> list[dict[str, Any]]:
        return [
            configuration
            for configuration in self.list_workflow_configurations(
                project_key=project_key
            )
            if configuration["template"]["id"] == DISCOVERY_TEMPLATE_ID
        ]

    def get_advisory_configuration(
        self, project_key: str, name: str, *, version: int | None = None
    ) -> dict[str, Any]:
        configuration = self.get_workflow_configuration(
            project_key, name, version=version
        )
        if configuration["template"]["id"] != ADVISORY_REVIEW_TEMPLATE_ID:
            raise NotFoundError(
                f"advisory configuration not found: {project_key}/{name}"
            )
        return configuration

    def list_advisory_configurations(
        self, *, project_key: str | None = None
    ) -> list[dict[str, Any]]:
        return [
            configuration
            for configuration in self.list_workflow_configurations(
                project_key=project_key
            )
            if configuration["template"]["id"] == ADVISORY_REVIEW_TEMPLATE_ID
        ]

    @staticmethod
    def _start_request(
        *,
        project_key: str,
        configuration_name: str,
        work_item_id: str,
        source_revision: str,
        expected_configuration_version: int,
        expected_context_version_id: str,
        expected_work_item_version_id: str,
        expected_thread_generation: int,
        expected_thread_version: int,
        discovery_execution_id: str | None,
        expected_activation_version: int | None,
    ) -> dict[str, Any]:
        request = {
            "schema_version": 1,
            "project_key": project_key,
            "configuration_name": configuration_name,
            "work_item_id": work_item_id,
            "source_revision": source_revision,
            "expected_configuration_version": expected_configuration_version,
            "expected_context_version_id": expected_context_version_id,
            "expected_work_item_version_id": expected_work_item_version_id,
            "expected_thread_generation": expected_thread_generation,
            "expected_thread_version": expected_thread_version,
        }
        if discovery_execution_id is not None:
            request["discovery_execution_id"] = discovery_execution_id
        if expected_activation_version is not None:
            request["expected_activation_version"] = expected_activation_version
        return request

    def start_workflow_execution(
        self,
        *,
        project_key: str,
        configuration_name: str,
        work_item_id: str,
        source_revision: str,
        expected_configuration_version: int,
        expected_context_version_id: str,
        expected_work_item_version_id: str,
        expected_thread_generation: int,
        expected_thread_version: int,
        idempotency_key: str,
        requested_by: str,
        discovery_execution_id: str | None = None,
        expected_activation_version: int | None = None,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        configuration_name = validate_name(
            configuration_name, field="workflow configuration name"
        )
        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
        source_revision = self._source_revision(source_revision)
        self._positive_integer(
            expected_configuration_version, field="expected_configuration_version"
        )
        expected_context_version_id = require_string(
            expected_context_version_id,
            field="expected_context_version_id",
            max_bytes=64,
        )
        expected_work_item_version_id = require_string(
            expected_work_item_version_id,
            field="expected_work_item_version_id",
            max_bytes=64,
        )
        self._positive_integer(
            expected_thread_generation, field="expected_thread_generation"
        )
        self._positive_integer(expected_thread_version, field="expected_thread_version")
        if discovery_execution_id is not None:
            discovery_execution_id = require_string(
                discovery_execution_id,
                field="discovery_execution_id",
                max_bytes=64,
            )
        if expected_activation_version is not None:
            self._positive_integer(
                expected_activation_version,
                field="expected_activation_version",
            )
        idempotency_key = self._workflow_idempotency_key(idempotency_key)
        request = self._start_request(
            project_key=project_key,
            configuration_name=configuration_name,
            work_item_id=work_item_id,
            source_revision=source_revision,
            expected_configuration_version=expected_configuration_version,
            expected_context_version_id=expected_context_version_id,
            expected_work_item_version_id=expected_work_item_version_id,
            expected_thread_generation=expected_thread_generation,
            expected_thread_version=expected_thread_version,
            discovery_execution_id=discovery_execution_id,
            expected_activation_version=expected_activation_version,
        )
        request_json = canonical_json(request)
        request_sha256 = content_sha256(request_json)
        execution_id = self.new_id("wfx")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, requested_by, role="workflow requester")
                existing = connection.execute(
                    "SELECT id, content_sha256 FROM workflow_executions "
                    "WHERE requested_by = ? AND idempotency_key = ?",
                    (requested_by, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != request_sha256:
                        raise ConflictError(
                            "idempotency key already names another workflow execution"
                        )
                    stored_id = existing["id"]
                    connection.commit()
                else:
                    project = connection.execute(
                        "SELECT p.id, p.active, c.id AS context_version_id "
                        "FROM projects p JOIN project_context_versions c "
                        "ON c.project_id = p.id WHERE p.krab_project_key = ? "
                        "ORDER BY c.version DESC LIMIT 1",
                        (project_key,),
                    ).fetchone()
                    if project is None or project["active"] != 1:
                        raise NotFoundError(f"active project not found: {project_key}")
                    if project["context_version_id"] != expected_context_version_id:
                        raise ConcurrentUpdateError("project context version changed")
                    configuration = connection.execute(
                        "SELECT c.id, cv.id AS version_id, cv.version, "
                        "cv.template_definition_id, "
                        "(SELECT COUNT(*) "
                        "FROM workflow_configuration_role_bindings binding "
                        "WHERE binding.configuration_version_id = cv.id) "
                        "AS role_count, "
                        "(SELECT COUNT(*) "
                        "FROM workflow_configuration_role_bindings binding "
                        "JOIN agent_profile_versions profile_version "
                        "ON profile_version.id = binding.profile_version_id "
                        "JOIN agent_profiles profile "
                        "ON profile.id = profile_version.profile_id "
                        "WHERE binding.configuration_version_id = cv.id "
                        "AND profile.active = 1) AS active_role_count "
                        "FROM workflow_configurations c "
                        "JOIN workflow_configuration_versions cv "
                        "ON cv.configuration_id = c.id "
                        "WHERE c.project_id = ? AND c.name = ? "
                        "ORDER BY cv.version DESC LIMIT 1",
                        (project["id"], configuration_name),
                    ).fetchone()
                    if configuration is None:
                        raise NotFoundError(
                            "workflow configuration not found: "
                            f"{project_key}/{configuration_name}"
                        )
                    if configuration["version"] != expected_configuration_version:
                        raise ConflictError("workflow configuration version changed")
                    if (
                        configuration["template_definition_id"]
                        != IMPLEMENTATION_TEMPLATE_ID
                    ):
                        raise ConflictError(
                            "workflow prescription is not executable by the "
                            "implementation director"
                        )
                    if configuration["role_count"] != 2:
                        raise StorageError(
                            "stored workflow configuration roles do not match"
                        )
                    if configuration["active_role_count"] != 2:
                        raise ConflictError("workflow role profile is inactive")
                    activation = None
                    if expected_activation_version is not None:
                        activation = self._resolve_workflow_activation_for_start(
                            connection,
                            project_key=project_key,
                            configuration_version_id=configuration["version_id"],
                            expected_version=expected_activation_version,
                        )
                    elif (
                        connection.execute(
                            "SELECT 1 FROM workflow_activations WHERE project_id = ?",
                            (project["id"],),
                        ).fetchone()
                        is not None
                    ):
                        raise ConflictError(
                            "project requires its acknowledged workflow activation"
                        )
                    work_item = connection.execute(
                        "SELECT wi.project_id, wiv.id AS work_item_version_id, "
                        "wt.id AS work_thread_id, wt.generation, wt.state, "
                        "wt.version AS thread_version FROM work_items wi "
                        "JOIN work_item_versions wiv ON wiv.work_item_id = wi.id "
                        "JOIN work_threads wt ON wt.work_item_id = wi.id "
                        "WHERE wi.id = ? ORDER BY wiv.version DESC, "
                        "wt.generation DESC LIMIT 1",
                        (work_item_id,),
                    ).fetchone()
                    if work_item is None:
                        raise NotFoundError(f"work item not found: {work_item_id}")
                    if work_item["project_id"] != project["id"]:
                        raise ConflictError("work item belongs to another project")
                    if (
                        work_item["work_item_version_id"]
                        != expected_work_item_version_id
                    ):
                        raise ConcurrentUpdateError("work item version changed")
                    if work_item["state"] != "active":
                        raise ConflictError(
                            f"work thread is not active: {work_item['state']}"
                        )
                    if work_item["generation"] != expected_thread_generation:
                        raise ConcurrentUpdateError("work thread generation changed")
                    if work_item["thread_version"] != expected_thread_version:
                        raise ConcurrentUpdateError("work thread version changed")
                    active_workflow = connection.execute(
                        "SELECT id FROM workflow_executions "
                        "WHERE work_thread_id = ? AND state = 'active'",
                        (work_item["work_thread_id"],),
                    ).fetchone()
                    active_discovery = connection.execute(
                        "SELECT id FROM discovery_executions "
                        "WHERE work_thread_id = ? AND state = 'active'",
                        (work_item["work_thread_id"],),
                    ).fetchone()
                    if active_workflow is not None or active_discovery is not None:
                        raise ConflictError(
                            "work thread already has an active workflow or active "
                            "prescription"
                        )
                    scope_ruling = None
                    if discovery_execution_id is not None:
                        self._validate_scoped_workflow_profiles(
                            connection, configuration["version_id"]
                        )
                        scope_ruling = self._workflow_scope_source(
                            connection,
                            discovery_execution_id=discovery_execution_id,
                            project_id=project["id"],
                            context_version_id=project["context_version_id"],
                            work_item_id=work_item_id,
                            work_item_version_id=work_item["work_item_version_id"],
                            work_thread_id=work_item["work_thread_id"],
                            work_thread_generation=work_item["generation"],
                            source_revision=source_revision,
                        )
                    connection.execute(
                        "INSERT INTO workflow_executions "
                        "(id, project_id, template_definition_id, "
                        "configuration_version_id, workflow_activation_version_id, "
                        "context_version_id, work_item_id, work_item_version_id, "
                        "work_thread_id, work_thread_generation, source_revision, "
                        "state, stage, version, request_json, content_sha256, "
                        "idempotency_key, requested_by, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 'build', "
                        "1, ?, ?, ?, ?, ?, ?)",
                        (
                            execution_id,
                            project["id"],
                            configuration["template_definition_id"],
                            configuration["version_id"],
                            (
                                None
                                if activation is None
                                else activation["workflow_activation_version_id"]
                            ),
                            project["context_version_id"],
                            work_item_id,
                            work_item["work_item_version_id"],
                            work_item["work_thread_id"],
                            work_item["generation"],
                            source_revision,
                            request_json,
                            request_sha256,
                            idempotency_key,
                            requested_by,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO workflow_execution_events "
                        "(execution_id, sequence, kind, from_state, to_state, actor, "
                        "metadata_json, created_at) VALUES (?, 1, "
                        "'workflow.started', NULL, 'active', ?, ?, ?)",
                        (
                            execution_id,
                            requested_by,
                            canonical_json(
                                {}
                                if activation is None
                                else {
                                    "workflow_activation_version_id": activation[
                                        "workflow_activation_version_id"
                                    ],
                                    "workflow_policy_id": activation["policy"]["id"],
                                    "workflow_policy_version": activation["policy"][
                                        "version"
                                    ],
                                    "workflow_policy_sha256": activation["policy"][
                                        "content_sha256"
                                    ],
                                }
                            ),
                            now,
                        ),
                    )
                    if scope_ruling is not None:
                        connection.execute(
                            "INSERT INTO workflow_scope_bindings "
                            "(workflow_execution_id, discovery_execution_id, "
                            "discovery_ruling_id, created_at) VALUES (?, ?, ?, ?)",
                            (
                                execution_id,
                                discovery_execution_id,
                                scope_ruling["id"],
                                now,
                            ),
                        )
                    stored_id = execution_id
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
                raise ConflictError("workflow execution could not be started") from exc
        return self.get_workflow_execution(stored_id)

    @staticmethod
    def _workflow_scope_source(
        connection: sqlite3.Connection,
        *,
        discovery_execution_id: str,
        project_id: str,
        context_version_id: str,
        work_item_id: str,
        work_item_version_id: str,
        work_thread_id: str,
        work_thread_generation: int,
        source_revision: str,
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT execution.project_id, execution.context_version_id, "
            "execution.work_item_id, execution.work_item_version_id, "
            "execution.work_thread_id, execution.work_thread_generation, "
            "execution.source_revision, execution.state, execution.stage, "
            "ruling.id, ruling.ruling_json, ruling.content_sha256 "
            "FROM discovery_executions execution "
            "LEFT JOIN discovery_rulings ruling "
            "ON ruling.execution_id = execution.id "
            "AND ruling.disposition = 'GO' WHERE execution.id = ?",
            (discovery_execution_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(
                f"Discovery execution not found: {discovery_execution_id}"
            )
        if (
            row["state"] != "completed"
            or row["stage"] != "completion"
            or row["id"] is None
        ):
            raise ConflictError(
                "workflow scope requires a completed GO ruling from Discovery"
            )
        expected = {
            "project_id": project_id,
            "context_version_id": context_version_id,
            "work_item_id": work_item_id,
            "work_item_version_id": work_item_version_id,
            "work_thread_id": work_thread_id,
            "work_thread_generation": work_thread_generation,
            "source_revision": source_revision,
        }
        for field, value in expected.items():
            if row[field] != value:
                raise ConflictError(
                    "Discovery scope " + field.replace("_", " ") + " changed"
                )
        try:
            ruling = DiscoveryRuling.from_mapping(json.loads(row["ruling_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored Discovery GO ruling is invalid") from exc
        if (
            ruling.disposition is not DiscoveryDisposition.GO
            or ruling.work_ready_brief is None
        ):
            raise StorageError("stored Discovery GO ruling has no work-ready brief")
        return {
            "id": row["id"],
            "content_sha256": row["content_sha256"],
            "work_ready_brief": ruling.work_ready_brief.to_dict(),
        }

    def _execution_record(
        self, connection: sqlite3.Connection, execution_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT e.id, e.project_id, p.krab_project_key AS project_key, "
            "e.template_definition_id, e.configuration_version_id, "
            "e.workflow_activation_version_id, "
            "cv.configuration_id, cv.version AS configuration_version, "
            "c.name AS configuration_name, e.context_version_id, "
            "pc.version AS context_version, e.work_item_id, "
            "e.work_item_version_id, wiv.version AS work_item_version, "
            "e.work_thread_id, e.work_thread_generation, e.source_revision, "
            "e.state, e.stage, e.correction_round, e.version, "
            "e.content_sha256, e.cancel_reason, "
            "u.handle AS requested_by, e.created_at, e.updated_at, e.finished_at "
            "FROM workflow_executions e "
            "JOIN projects p ON p.id = e.project_id "
            "JOIN workflow_configuration_versions cv "
            "ON cv.id = e.configuration_version_id "
            "JOIN workflow_configurations c ON c.id = cv.configuration_id "
            "JOIN project_context_versions pc ON pc.id = e.context_version_id "
            "JOIN work_item_versions wiv ON wiv.id = e.work_item_version_id "
            "JOIN users u ON u.id = e.requested_by WHERE e.id = ?",
            (execution_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"workflow execution not found: {execution_id}")
        configuration = self._configuration_record(
            connection,
            self._configuration_row(
                connection, version_id=row["configuration_version_id"]
            ),
        )
        events = connection.execute(
            "SELECT sequence, kind, from_state, to_state, actor.handle AS actor, "
            "metadata_json, event.created_at FROM workflow_execution_events event "
            "JOIN users actor ON actor.id = event.actor "
            "WHERE event.execution_id = ? ORDER BY event.sequence",
            (execution_id,),
        ).fetchall()
        try:
            parsed_events = [
                {**dict(event), "metadata": json.loads(event["metadata_json"])}
                for event in events
            ]
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored workflow event is invalid") from exc
        for event in parsed_events:
            event.pop("metadata_json")
        rulings = connection.execute(
            "SELECT wr.id, wr.execution_version, wr.correction_round, wr.kind, "
            "wr.state, wr.version, wr.policy_id, wr.run_id, wr.gate_name, "
            "wr.provider_landing_id, wr.landing_plan_id, "
            "resolver.handle AS resolved_by, wr.decision, wr.created_at, "
            "wr.updated_at, wr.resolved_at FROM workflow_rulings wr "
            "LEFT JOIN users resolver ON resolver.id = wr.resolved_by "
            "WHERE wr.execution_id = ? ORDER BY wr.created_at, wr.id",
            (execution_id,),
        ).fetchall()
        ruling_records = []
        for ruling in rulings:
            ruling_record = dict(ruling)
            ruling_record["ceiling_extension"] = (
                self._workflow_correction_ceiling_extension_record(
                    connection, ruling["id"]
                )
            )
            ruling_records.append(ruling_record)
        landing = self._workflow_landing_record(
            connection,
            execution_id,
            mode=configuration["spec"]["landing_mode"],
        )
        completion = self._workflow_completion_record(connection, execution_id)
        closeout = self._workflow_closeout_record(connection, execution_id)
        scope_binding = self._workflow_scope_binding_record(connection, execution_id)
        portable_policy: dict[str, Any] = {"status": "pre_policy"}
        if row["workflow_activation_version_id"] is not None:
            activation = self._activation_record(
                connection,
                self._activation_row(
                    connection,
                    row["project_key"],
                    None,
                    version_id=row["workflow_activation_version_id"],
                ),
            )
            portable_policy = {
                "status": "activated",
                "activation_id": activation["id"],
                "activation_version_id": activation["workflow_activation_version_id"],
                "activation_version": activation["version"],
                "activation_sha256": activation["content_sha256"],
                "policy_id": activation["policy"]["id"],
                "policy_name": activation["policy"]["name"],
                "policy_version": activation["policy"]["version"],
                "policy_sha256": activation["policy"]["content_sha256"],
                "resources": activation["spec"]["resources"],
                "wake_bindings": activation["spec"]["wake_bindings"],
                "merge_method": activation["spec"]["merge_method"],
            }
        return {
            **dict(row),
            "template": configuration["template"],
            "configuration_id": configuration["id"],
            "roles": configuration["roles"],
            "spec": configuration["spec"],
            "events": parsed_events,
            "rulings": ruling_records,
            "landing": landing,
            "completion": completion,
            "closeout": closeout,
            "scope_binding": scope_binding,
            "portable_policy": portable_policy,
            "responsibility": self._workflow_responsibility(connection, execution_id),
        }

    @staticmethod
    def _workflow_responsibility(
        connection: sqlite3.Connection, execution_id: str
    ) -> dict[str, Any]:
        """Derive one total next-actor fact from durable workflow state."""

        execution = connection.execute(
            "SELECT state, stage, correction_round, version, "
            "workflow_activation_version_id FROM workflow_executions "
            "WHERE id = ?",
            (execution_id,),
        ).fetchone()
        if execution is None:
            raise NotFoundError(f"workflow execution not found: {execution_id}")
        if execution["workflow_activation_version_id"] is None:
            return {"status": "pre_policy"}

        def fact(
            status: str,
            holder: str,
            reason: str,
            basis: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return {
                "status": status,
                "holder": holder,
                "reason": reason,
                "execution_version": execution["version"],
                "stage": execution["stage"],
                "correction_round": execution["correction_round"],
                "basis": {} if basis is None else basis,
            }

        if execution["state"] == "completed":
            obligation = connection.execute(
                "SELECT id, version, deadline FROM workflow_closeout_obligations "
                "WHERE workflow_execution_id = ? AND state = 'open' "
                "ORDER BY deadline, id LIMIT 1",
                (execution_id,),
            ).fetchone()
            if obligation is not None:
                return fact(
                    "action_required",
                    "owner",
                    "closeout_obligation",
                    {
                        "obligation_id": obligation["id"],
                        "obligation_version": obligation["version"],
                        "deadline": obligation["deadline"],
                    },
                )
        if execution["state"] != "active":
            hold = connection.execute(
                "SELECT id, version, placement_kind, reason, integration_ref_id, "
                "landing_proof_id "
                "FROM integration_target_holds WHERE workflow_execution_id = ? "
                "AND state = 'open' ORDER BY created_at, id LIMIT 1",
                (execution_id,),
            ).fetchone()
            if hold is not None:
                return fact(
                    "action_required",
                    "owner",
                    "landing_target_hold",
                    {
                        "hold_id": hold["id"],
                        "hold_version": hold["version"],
                        "hold_placement_kind": hold["placement_kind"],
                        "hold_reason": hold["reason"],
                        "integration_ref_id": hold["integration_ref_id"],
                        "landing_proof_id": hold["landing_proof_id"],
                    },
                )
            return fact(
                "terminal",
                "none",
                "execution_terminal",
                {"execution_state": execution["state"]},
            )
        ruling = connection.execute(
            "SELECT id, version, kind FROM workflow_rulings "
            "WHERE execution_id = ? AND state = 'open' "
            "ORDER BY created_at, id LIMIT 1",
            (execution_id,),
        ).fetchone()
        if ruling is not None:
            return fact(
                "action_required",
                "owner",
                "ruling_resolution",
                {
                    "ruling_id": ruling["id"],
                    "ruling_kind": ruling["kind"],
                    "ruling_version": ruling["version"],
                },
            )
        hold = connection.execute(
            "SELECT id, version, placement_kind, reason, integration_ref_id, "
            "landing_proof_id "
            "FROM integration_target_holds WHERE workflow_execution_id = ? "
            "AND state = 'open' ORDER BY created_at, id LIMIT 1",
            (execution_id,),
        ).fetchone()
        if hold is not None:
            return fact(
                "action_required",
                "owner",
                "landing_target_hold",
                {
                    "hold_id": hold["id"],
                    "hold_version": hold["version"],
                    "hold_placement_kind": hold["placement_kind"],
                    "hold_reason": hold["reason"],
                    "integration_ref_id": hold["integration_ref_id"],
                    "landing_proof_id": hold["landing_proof_id"],
                },
            )
        exhausted_action = connection.execute(
            "SELECT id, kind, version, last_error_code FROM supervisor_actions "
            "WHERE workflow_execution_id = ? AND subject_version = ? "
            "AND state = 'exhausted' ORDER BY updated_at DESC, id DESC LIMIT 1",
            (execution_id, execution["version"]),
        ).fetchone()
        if exhausted_action is not None:
            return fact(
                "action_required",
                "owner",
                "supervisor_action_exhausted",
                {
                    "action_id": exhausted_action["id"],
                    "action_kind": exhausted_action["kind"],
                    "action_version": exhausted_action["version"],
                    "last_error_code": exhausted_action["last_error_code"],
                },
            )

        stage = execution["stage"]
        if stage in {"build", "review"}:
            role = "producer" if stage == "build" else "reviewer"
            run = connection.execute(
                "SELECT binding.run_id, binding.generation, run.state, "
                "run.version, wake.id AS wake_id, wake.state AS wake_state, "
                "wake.version AS wake_version, wake.updated_at AS wake_updated_at "
                "FROM workflow_run_bindings binding "
                "JOIN runs run ON run.id = binding.run_id "
                "LEFT JOIN workflow_manual_wakes wake ON wake.run_id = run.id "
                "WHERE binding.execution_id = ? "
                "AND binding.correction_round = ? AND binding.role = ? "
                "ORDER BY binding.generation DESC LIMIT 1",
                (execution_id, execution["correction_round"], role),
            ).fetchone()
            if run is None:
                return fact(
                    "action_required",
                    "control_plane",
                    "role_admission",
                )
            run_basis = {
                "run_id": run["run_id"],
                "run_version": run["version"],
                "run_generation": run["generation"],
            }
            if run["wake_id"] is None:
                return fact(
                    "attention",
                    "control_room",
                    "manual_wake_missing",
                    run_basis,
                )
            wake_basis = {
                "run_id": run["run_id"],
                "wake_id": run["wake_id"],
                "wake_version": run["wake_version"],
                "wake_state": run["wake_state"],
                "wake_updated_at": run["wake_updated_at"],
            }
            if run["wake_state"] != "acknowledged":
                reason = {
                    "expired": "manual_wake_expired",
                    "escalated": "manual_wake_escalated",
                }.get(run["wake_state"], "manual_wake_acknowledgement")
                return fact(
                    "action_required",
                    "owner",
                    reason,
                    wake_basis,
                )
            if run["state"] == "queued":
                return fact(
                    "action_required",
                    "control_plane",
                    "run_launch",
                    wake_basis,
                )
            if run["state"] in {"preparing", "running"}:
                return fact(
                    "working",
                    role,
                    "run_execution",
                    run_basis,
                )
            if run["state"] == "waiting_for_input":
                return fact(
                    "action_required",
                    "owner",
                    "run_interaction",
                    run_basis,
                )
            if run["state"] == "paused":
                return fact(
                    "action_required",
                    "control_plane",
                    "run_resume",
                    run_basis,
                )
            if run["state"] in {"succeeded", "failed", "cancelled"}:
                return fact(
                    "action_required",
                    "control_plane",
                    "stage_transition",
                    run_basis,
                )
            return fact(
                "attention",
                "control_room",
                "run_state_unresolved",
                run_basis,
            )

        if stage == "maintainer":
            return fact(
                "action_required",
                "owner",
                "maintainer_disposition",
            )
        if stage == "landing":
            landing = connection.execute(
                "SELECT binding.queue_item_id, queue.state AS queue_state, "
                "queue.version AS queue_version, "
                "target.execution_id AS target_id, intent.id AS intent_id "
                "FROM workflow_landing_bindings binding "
                "JOIN integration_queue_items queue "
                "ON queue.id = binding.queue_item_id "
                "LEFT JOIN workflow_provider_targets target "
                "ON target.execution_id = binding.execution_id "
                "LEFT JOIN workflow_provider_intents intent "
                "ON intent.workflow_execution_id = binding.execution_id "
                "WHERE binding.execution_id = ?",
                (execution_id,),
            ).fetchone()
            if landing is None:
                return fact(
                    "action_required",
                    "control_plane",
                    "landing_admission",
                )
            basis = {
                "queue_item_id": landing["queue_item_id"],
                "queue_version": landing["queue_version"],
            }
            if landing["intent_id"] is None:
                return fact(
                    "action_required",
                    "owner",
                    "provider_intent",
                    basis,
                )
            for capability_name in ("refs.publish", "change.open"):
                effect = connection.execute(
                    "SELECT id, state, conforms_intent, service_operation_id "
                    "FROM workflow_provider_effects WHERE intent_id = ? "
                    "AND capability_name = ?",
                    (landing["intent_id"], capability_name),
                ).fetchone()
                if effect is None:
                    return fact(
                        "action_required",
                        "control_plane",
                        "provider_effect_dispatch",
                        {**basis, "capability_name": capability_name},
                    )
                effect_basis = {
                    **basis,
                    "capability_name": capability_name,
                    "effect_id": effect["id"],
                    "service_operation_id": effect["service_operation_id"],
                }
                if effect["state"] == "pending":
                    return fact(
                        "waiting",
                        "landing_actuator",
                        "provider_effect_receipt",
                        effect_basis,
                    )
                if effect["state"] in {"failed", "uncertain"}:
                    return fact(
                        "attention",
                        "control_room",
                        "provider_effect_unresolved",
                        {**effect_basis, "effect_state": effect["state"]},
                    )
                if effect["state"] == "confirmed" and effect["conforms_intent"] != 1:
                    return fact(
                        "action_required",
                        "owner",
                        "provider_effect_mismatch",
                        effect_basis,
                    )
            if landing["target_id"] is None:
                return fact(
                    "action_required",
                    "control_plane",
                    "provider_target_derivation",
                    basis,
                )
            if landing["queue_state"] in {"queued", "claimed"}:
                return fact(
                    "action_required",
                    "control_plane",
                    "landing_dispatch",
                    basis,
                )
            if landing["queue_state"] == "awaiting_receipt":
                return fact(
                    "waiting",
                    "landing_actuator",
                    "provider_receipt",
                    basis,
                )
            if landing["queue_state"] == "landed":
                return fact(
                    "action_required",
                    "control_plane",
                    "stage_transition",
                    basis,
                )
            if landing["queue_state"] == "refold_required":
                return fact(
                    "action_required",
                    "control_plane",
                    "landing_refold_resolution",
                    basis,
                )
            return fact(
                "attention",
                "control_room",
                "landing_state_unresolved",
                {**basis, "queue_state": landing["queue_state"]},
            )
        if stage == "completion":
            declaration = connection.execute(
                "SELECT id FROM workflow_closeout_declarations "
                "WHERE workflow_execution_id = ?",
                (execution_id,),
            ).fetchone()
            if declaration is None:
                return fact(
                    "action_required",
                    "owner",
                    "closeout_declaration",
                )
            return fact(
                "action_required",
                "control_plane",
                "workflow_completion",
            )
        return fact(
            "attention",
            "control_room",
            "workflow_stage_unresolved",
        )

    @staticmethod
    def _workflow_scope_binding_record(
        connection: sqlite3.Connection, execution_id: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT binding.discovery_execution_id, "
            "binding.discovery_ruling_id, ruling.content_sha256, "
            "ruling.ruling_json, binding.created_at "
            "FROM workflow_scope_bindings binding "
            "JOIN discovery_rulings ruling "
            "ON ruling.id = binding.discovery_ruling_id "
            "WHERE binding.workflow_execution_id = ?",
            (execution_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            ruling = DiscoveryRuling.from_mapping(json.loads(row["ruling_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored workflow scope ruling is invalid") from exc
        if ruling.work_ready_brief is None:
            raise StorageError("stored workflow scope ruling has no work-ready brief")
        return {
            "discovery_execution_id": row["discovery_execution_id"],
            "discovery_ruling_id": row["discovery_ruling_id"],
            "discovery_ruling_sha256": row["content_sha256"],
            "work_ready_brief": ruling.work_ready_brief.to_dict(),
            "created_at": row["created_at"],
        }

    @staticmethod
    def _workflow_scope_review_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            review = WorkflowScopeReview.from_mapping(json.loads(row["review_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored workflow scope review is invalid") from exc
        if (
            review.digest != row["content_sha256"]
            or review.expected_review_outcome.value != row["expected_review_outcome"]
        ):
            raise StorageError("stored workflow scope review does not match")
        return {
            "id": row["id"],
            "workflow_execution_id": row["workflow_execution_id"],
            "correction_round": row["correction_round"],
            "candidate_id": row["candidate_id"],
            "policy_id": row["policy_id"],
            "reviewer_run_id": row["reviewer_run_id"],
            "reviewer_attempt_id": row["reviewer_attempt_id"],
            "review": review.to_dict(),
            "content_sha256": row["content_sha256"],
            "expected_review_outcome": row["expected_review_outcome"],
            "failed_must_keys": list(review.failed_must_keys),
            "regression_probe_keys": list(review.regression_probe_keys),
            "follow_up_probe_keys": list(review.follow_up_probe_keys),
            "probe_classifications": [
                {
                    "key": probe.key,
                    "classification": probe.classification.value,
                }
                for probe in review.probes
            ],
            "idempotency_key": row["idempotency_key"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _workflow_scope_review_context(
        connection: sqlite3.Connection, run_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT binding.execution_id, binding.correction_round, binding.role, "
            "execution.state, execution.stage, "
            "execution.correction_round AS execution_round, "
            "gate.candidate_id, gate.policy_id, policy.head_revision, "
            "review_target.candidate_id AS review_candidate_id, "
            "review_target.policy_id AS review_policy_id, "
            "run.state AS run_state, run.source_revision AS run_source_revision, "
            "attempt.id AS attempt_id, "
            "attempt.source_revision AS attempt_source_revision, "
            "ruling.ruling_json "
            "FROM workflow_run_bindings binding "
            "JOIN workflow_executions execution "
            "ON execution.id = binding.execution_id "
            "JOIN workflow_scope_bindings scope "
            "ON scope.workflow_execution_id = execution.id "
            "JOIN discovery_rulings ruling ON ruling.id = scope.discovery_ruling_id "
            "JOIN workflow_gate_bindings gate "
            "ON gate.execution_id = binding.execution_id "
            "AND gate.correction_round = binding.correction_round "
            "JOIN integration_gate_policies policy ON policy.id = gate.policy_id "
            "JOIN integration_gate_review_runs review_target "
            "ON review_target.run_id = binding.run_id "
            "JOIN runs run ON run.id = binding.run_id "
            "LEFT JOIN run_attempts attempt ON attempt.run_id = run.id "
            "AND attempt.state = 'running' WHERE binding.run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"scoped workflow reviewer run not found: {run_id}")
        if row["role"] != "reviewer":
            raise ConflictError("workflow scope review requires the reviewer role")
        if (
            row["state"] != "active"
            or row["stage"] != "review"
            or row["execution_round"] != row["correction_round"]
        ):
            raise ConflictError("workflow is not accepting a scope review")
        if row["run_state"] != "running" or row["attempt_id"] is None:
            raise ConflictError("workflow reviewer has no active worker attempt")
        if (
            row["policy_id"] != row["review_policy_id"]
            or row["candidate_id"] != row["review_candidate_id"]
        ):
            raise ConflictError("workflow reviewer gate authority changed")
        if (
            row["run_source_revision"] != row["head_revision"]
            or row["attempt_source_revision"] != row["head_revision"]
        ):
            raise ConflictError("workflow reviewer candidate revision changed")
        return row

    def submit_workflow_scope_review(
        self,
        run_id: str,
        *,
        review: WorkflowScopeReview,
        idempotency_key: str,
    ) -> dict[str, Any]:
        run_id = require_string(run_id, field="run_id", max_bytes=64)
        if not isinstance(review, WorkflowScopeReview):
            raise ValidationError("review must be a WorkflowScopeReview")
        idempotency_key = self._workflow_idempotency_key(idempotency_key)
        review_id = self.new_id("wsr")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                context = self._workflow_scope_review_context(connection, run_id)
                existing = connection.execute(
                    "SELECT * FROM workflow_scope_reviews "
                    "WHERE reviewer_attempt_id = ?",
                    (context["attempt_id"],),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["idempotency_key"] != idempotency_key
                        or existing["content_sha256"] != review.digest
                    ):
                        raise ConflictError(
                            "reviewer attempt already submitted another scope review"
                        )
                    connection.commit()
                    return self._workflow_scope_review_record(existing)
                try:
                    ruling = DiscoveryRuling.from_mapping(
                        json.loads(context["ruling_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored workflow scope ruling is invalid"
                    ) from exc
                if ruling.work_ready_brief is None:
                    raise StorageError("stored workflow scope ruling has no brief")
                expected_keys = {
                    item.key for item in ruling.work_ready_brief.must_items
                }
                submitted_keys = {item.key for item in review.must_results}
                if submitted_keys != expected_keys:
                    raise ConflictError(
                        "workflow scope review MUST keys do not match the pinned brief"
                    )
                connection.execute(
                    "INSERT INTO workflow_scope_reviews "
                    "(id, workflow_execution_id, correction_round, candidate_id, "
                    "policy_id, reviewer_run_id, reviewer_attempt_id, "
                    "schema_version, review_json, content_sha256, "
                    "expected_review_outcome, idempotency_key, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        review_id,
                        context["execution_id"],
                        context["correction_round"],
                        context["candidate_id"],
                        context["policy_id"],
                        run_id,
                        context["attempt_id"],
                        review.schema_version,
                        review.canonical,
                        review.digest,
                        review.expected_review_outcome.value,
                        idempotency_key,
                        now,
                    ),
                )
                stored = connection.execute(
                    "SELECT * FROM workflow_scope_reviews WHERE id = ?",
                    (review_id,),
                ).fetchone()
                connection.commit()
            except (ConflictError, NotFoundError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow scope review could not be recorded"
                ) from exc
        return self._workflow_scope_review_record(stored)

    def get_workflow_scope_review(
        self, policy_id: str, reviewer_run_id: str, reviewer_attempt_id: str
    ) -> dict[str, Any] | None:
        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        reviewer_run_id = require_string(
            reviewer_run_id, field="reviewer_run_id", max_bytes=64
        )
        reviewer_attempt_id = require_string(
            reviewer_attempt_id, field="reviewer_attempt_id", max_bytes=64
        )
        with self._read_snapshot() as connection:
            row = connection.execute(
                "SELECT * FROM workflow_scope_reviews WHERE policy_id = ? "
                "AND reviewer_run_id = ? AND reviewer_attempt_id = ?",
                (policy_id, reviewer_run_id, reviewer_attempt_id),
            ).fetchone()
        return None if row is None else self._workflow_scope_review_record(row)

    @staticmethod
    def _workflow_provider_effect_record(row: sqlite3.Row) -> dict[str, Any]:
        raw_fact = row["fact_json"]
        fact = None
        if raw_fact is not None:
            try:
                value = json.loads(raw_fact)
                if row["capability_name"] == "refs.publish":
                    parsed = RefPublicationReadback.from_mapping(value)
                elif row["capability_name"] == "change.open":
                    parsed = ChangeOpenReadback.from_mapping(value)
                else:
                    raise StorageError(
                        "stored workflow provider effect capability is invalid"
                    )
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError(
                    "stored workflow provider effect fact is invalid"
                ) from exc
            fact = parsed.to_dict()
            if content_sha256(canonical_json(fact)) != row["fact_sha256"]:
                raise StorageError(
                    "stored workflow provider effect digest does not match"
                )
        conforms_value = row["conforms_intent"]
        if row["state"] == "confirmed":
            if conforms_value not in {0, 1}:
                raise StorageError(
                    "stored workflow provider effect conformance is invalid"
                )
            conforms_intent: bool | None = bool(conforms_value)
        else:
            if conforms_value is not None:
                raise StorageError(
                    "stored workflow provider effect conformance is invalid"
                )
            conforms_intent = None
        return {
            "id": row["id"],
            "intent_id": row["intent_id"],
            "capability_name": row["capability_name"],
            "capability_binding_id": row["capability_binding_id"],
            "service_operation_id": row["service_operation_id"],
            "operation_state": row["operation_state"],
            "state": row["state"],
            "conforms_intent": conforms_intent,
            "fact": fact,
            "fact_sha256": row["fact_sha256"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "resolved_at": row["resolved_at"],
        }

    @classmethod
    def _workflow_provider_intent_record(
        cls, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        try:
            intent = WorkflowProviderIntent.from_mapping(json.loads(row["intent_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored workflow provider intent is invalid") from exc
        if intent.digest != row["content_sha256"]:
            raise StorageError("stored workflow provider intent digest does not match")
        effect_rows = connection.execute(
            "SELECT effect.*, operation.state AS operation_state "
            "FROM workflow_provider_effects effect "
            "JOIN service_operations operation "
            "ON operation.id = effect.service_operation_id "
            "WHERE effect.intent_id = ? ORDER BY effect.capability_name",
            (row["id"],),
        ).fetchall()
        effects = {
            effect["capability_name"]: cls._workflow_provider_effect_record(effect)
            for effect in effect_rows
        }
        return {
            "id": row["id"],
            "workflow_execution_id": row["workflow_execution_id"],
            "workflow_activation_version_id": row["workflow_activation_version_id"],
            "workflow_policy_id": row["workflow_policy_id"],
            "candidate_id": row["candidate_id"],
            "gate_policy_id": row["gate_policy_id"],
            "work_item_version_id": row["work_item_version_id"],
            "intent": intent.to_dict(),
            "content_sha256": row["content_sha256"],
            "created_by": row["created_by_handle"],
            "created_at": row["created_at"],
            "effects": effects,
        }

    @classmethod
    def _workflow_provider_intent_for_execution(
        cls, connection: sqlite3.Connection, execution_id: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT intent.*, creator.handle AS created_by_handle "
            "FROM workflow_provider_intents intent "
            "JOIN users creator ON creator.id = intent.created_by "
            "WHERE intent.workflow_execution_id = ?",
            (execution_id,),
        ).fetchone()
        return (
            None
            if row is None
            else cls._workflow_provider_intent_record(connection, row)
        )

    def _workflow_landing_record(
        self,
        connection: sqlite3.Connection,
        execution_id: str,
        *,
        mode: str,
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT b.execution_id, b.correction_round, b.candidate_id, "
            "b.policy_id AS initial_policy_id, b.queue_item_id, b.created_at, "
            "q.current_policy_id AS policy_id, q.current_fold_id AS fold_id, "
            "q.state AS queue_state, q.version AS queue_version, q.owner_id, "
            "q.lease_expires_at, q.landed_receipt_id AS receipt_id "
            "FROM workflow_landing_bindings b "
            "JOIN integration_queue_items q ON q.id = b.queue_item_id "
            "WHERE b.execution_id = ?",
            (execution_id,),
        ).fetchone()
        if row is None:
            return None
        authority = connection.execute(
            "SELECT COUNT(d.id) AS blocked_dependency_count, q.work_item_id, "
            "q.integration_ref_id FROM integration_queue_items q "
            "LEFT JOIN work_item_landing_dependencies d "
            "ON d.dependent_work_item_id = q.work_item_id AND d.state = 'active' "
            "AND NOT EXISTS (SELECT 1 FROM integration_queue_items prerequisite "
            "WHERE prerequisite.integration_ref_id = q.integration_ref_id "
            "AND prerequisite.work_item_id = d.prerequisite_work_item_id "
            "AND prerequisite.state = 'landed') WHERE q.id = ? GROUP BY q.id",
            (row["queue_item_id"],),
        ).fetchone()
        if authority is None:
            raise StorageError("workflow landing queue is unavailable")
        gates_ready = self._gate_readiness(connection, row["policy_id"])["ready"]
        provider_available = None
        if mode == WorkflowLandingMode.PROVIDER.value:
            provider_available = bool(
                connection.execute(
                    "SELECT CASE WHEN e.workflow_activation_version_id IS NULL "
                    "THEN EXISTS(SELECT 1 FROM project_capability_bindings b "
                    "JOIN service_sidecars s ON s.id = b.service_sidecar_id "
                    "WHERE b.project_id = e.project_id "
                    "AND b.capability_name = 'landing.invoke' "
                    "AND b.capability_version = 1 AND b.state = 'active' "
                    "AND s.state = 'active') ELSE ("
                    "(SELECT COUNT(*) FROM workflow_activation_capability_bindings ab "
                    "WHERE ab.activation_version_id = "
                    "e.workflow_activation_version_id AND ab.capability_version = 1 "
                    "AND ab.capability_name IN "
                    "('refs.publish', 'change.open', 'landing.invoke')) = 3 "
                    "AND NOT EXISTS(SELECT 1 "
                    "FROM workflow_activation_capability_bindings ab "
                    "LEFT JOIN project_capability_bindings b "
                    "ON b.id = ab.project_capability_binding_id "
                    "LEFT JOIN service_sidecars s ON s.id = b.service_sidecar_id "
                    "WHERE ab.activation_version_id = "
                    "e.workflow_activation_version_id "
                    "AND ab.capability_name IN "
                    "('refs.publish', 'change.open', 'landing.invoke') "
                    "AND (b.id IS NULL OR b.project_id != e.project_id "
                    "OR b.state != 'active' OR s.state != 'active'))) END "
                    "FROM workflow_executions e WHERE e.id = ?",
                    (execution_id,),
                ).fetchone()[0]
            )
        target = connection.execute(
            "SELECT t.resource_id, t.merge_method, u.handle AS created_by, "
            "t.created_at, origin.intent_id, origin.change_effect_id "
            "FROM workflow_provider_targets t "
            "JOIN users u ON u.id = t.created_by "
            "LEFT JOIN workflow_provider_target_origins origin "
            "ON origin.workflow_execution_id = t.execution_id "
            "WHERE t.execution_id = ?",
            (execution_id,),
        ).fetchone()
        plan = connection.execute(
            "SELECT id, state, sequence, created_at, resolved_at "
            "FROM integration_landing_plans WHERE queue_item_id = ? "
            "ORDER BY sequence DESC LIMIT 1",
            (row["queue_item_id"],),
        ).fetchone()
        provider = connection.execute(
            "SELECT id, state, sequence, service_operation_id, created_at, "
            "resolved_at FROM integration_provider_landings "
            "WHERE queue_item_id = ? ORDER BY sequence DESC LIMIT 1",
            (row["queue_item_id"],),
        ).fetchone()
        receipt = None
        if row["receipt_id"] is not None:
            receipt = connection.execute(
                "SELECT id, plan_id, provider_landing_id, outcome, "
                "observed_revision, assurance, created_at "
                "FROM integration_landing_receipts WHERE id = ?",
                (row["receipt_id"],),
            ).fetchone()
            if receipt is None:
                raise StorageError("workflow landing receipt is unavailable")
        proof_row = self._workflow_provider_landing_proof_row(
            connection, execution_id=execution_id
        )
        proof = (
            None
            if proof_row is None
            else self._workflow_provider_landing_proof_record(proof_row)
        )
        hold_row = self._integration_target_hold_row(
            connection,
            integration_ref_id=authority["integration_ref_id"],
            open_only=True,
        )
        provider_intent = self._workflow_provider_intent_for_execution(
            connection, execution_id
        )
        target_record = None if target is None else dict(target)
        if target_record is not None and target_record["intent_id"] is None:
            target_record.pop("intent_id")
            target_record.pop("change_effect_id")
        return {
            **dict(row),
            "mode": mode,
            "gates_ready": gates_ready,
            "blocked_dependency_count": authority["blocked_dependency_count"],
            "provider_available": provider_available,
            "provider_intent": provider_intent,
            "provider_target": target_record,
            "plan_id": None if plan is None else plan["id"],
            "plan_state": None if plan is None else plan["state"],
            "provider_landing_id": None if provider is None else provider["id"],
            "provider_landing_state": (None if provider is None else provider["state"]),
            "provider_operation_id": (
                None if provider is None else provider["service_operation_id"]
            ),
            "receipt_kind": (
                None
                if receipt is None
                else ("manual" if receipt["plan_id"] is not None else "provider")
            ),
            "receipt_outcome": None if receipt is None else receipt["outcome"],
            "receipt_observed_revision": (
                None if receipt is None else receipt["observed_revision"]
            ),
            "receipt_assurance": None if receipt is None else receipt["assurance"],
            "proof": proof,
            "target_hold": (
                None
                if hold_row is None
                else self._integration_target_hold_record(hold_row)
            ),
        }

    @staticmethod
    def _workflow_completion_record(
        connection: sqlite3.Connection, execution_id: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT c.execution_id, c.queue_item_id, c.receipt_id, "
            "c.outcome_entry_id, c.candidate_id, c.policy_id, "
            "c.observed_revision, u.handle AS completed_by, c.created_at "
            "FROM workflow_completions c JOIN users u ON u.id = c.completed_by "
            "WHERE c.execution_id = ?",
            (execution_id,),
        ).fetchone()
        return None if row is None else dict(row)

    @staticmethod
    def _workflow_closeout_declaration_row(
        connection: sqlite3.Connection, execution_id: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT declaration.id, declaration.workflow_execution_id, "
            "declaration.workflow_activation_version_id, "
            "declaration.workflow_policy_id, declaration.landing_proof_id, "
            "declaration.schema_version, declaration.declaration_json, "
            "declaration.content_sha256, declaration.declared_by, "
            "declarer.handle AS declared_by_handle, declaration.created_at "
            "FROM workflow_closeout_declarations declaration "
            "JOIN users declarer ON declarer.id = declaration.declared_by "
            "WHERE declaration.workflow_execution_id = ?",
            (execution_id,),
        ).fetchone()

    @staticmethod
    def _workflow_closeout_obligation_record(
        row: sqlite3.Row, *, observed_at: str
    ) -> dict[str, Any]:
        if row["state"] == "resolved":
            deadline_state = "resolved"
        else:
            try:
                deadline = datetime.fromisoformat(
                    row["deadline"].replace("Z", "+00:00")
                )
                observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
            except (AttributeError, TypeError, ValueError) as exc:
                raise StorageError(
                    "stored workflow closeout obligation deadline is invalid"
                ) from exc
            if deadline.tzinfo is None or observed.tzinfo is None:
                raise StorageError(
                    "stored workflow closeout obligation deadline is invalid"
                )
            deadline_state = "overdue" if deadline <= observed else "pending"
        return {
            "id": row["id"],
            "declaration_id": row["declaration_id"],
            "workflow_execution_id": row["workflow_execution_id"],
            "kind": row["kind"],
            "obligation": row["policy_obligation"],
            "validation": row["validation_name"],
            "baseline_gate": "validation:" + row["validation_name"],
            "baseline_evidence_id": row["baseline_evidence_id"],
            "deadline": row["deadline"],
            "deadline_state": deadline_state,
            "reason": row["reason"],
            "state": row["state"],
            "version": row["version"],
            "resolved_by": row["resolved_by_handle"],
            "resolution": row["resolution"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "resolved_at": row["resolved_at"],
        }

    @classmethod
    def _workflow_closeout_declaration_record(
        cls,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        observed_at: str,
    ) -> dict[str, Any]:
        try:
            declaration = WorkflowCloseoutDeclaration.from_mapping(
                json.loads(row["declaration_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError(
                "stored workflow closeout declaration is invalid"
            ) from exc
        if (
            declaration.digest != row["content_sha256"]
            or declaration.schema_version != row["schema_version"]
        ):
            raise StorageError(
                "stored workflow closeout declaration digest does not match"
            )
        obligation_rows = connection.execute(
            "SELECT obligation.id, obligation.declaration_id, "
            "obligation.workflow_execution_id, obligation.kind, "
            "obligation.policy_obligation, obligation.validation_name, "
            "obligation.baseline_evidence_id, obligation.deadline, "
            "obligation.reason, obligation.state, obligation.version, "
            "resolver.handle AS resolved_by_handle, obligation.resolution, "
            "obligation.created_at, obligation.updated_at, obligation.resolved_at "
            "FROM workflow_closeout_obligations obligation "
            "LEFT JOIN users resolver ON resolver.id = obligation.resolved_by "
            "WHERE obligation.declaration_id = ? "
            "ORDER BY obligation.policy_obligation",
            (row["id"],),
        ).fetchall()
        obligations = [
            cls._workflow_closeout_obligation_record(
                obligation, observed_at=observed_at
            )
            for obligation in obligation_rows
        ]
        declared = {item.obligation: item for item in declaration.obligations}
        if len(obligations) != len(declared) or any(
            obligation["obligation"] not in declared
            or obligation["kind"] != declared[obligation["obligation"]].kind
            or obligation["deadline"] != declared[obligation["obligation"]].deadline
            or obligation["reason"] != declared[obligation["obligation"]].reason
            or obligation["workflow_execution_id"] != row["workflow_execution_id"]
            for obligation in obligations
        ):
            raise StorageError(
                "stored workflow closeout obligations differ from their declaration"
            )
        return {
            "id": row["id"],
            "workflow_execution_id": row["workflow_execution_id"],
            "workflow_activation_version_id": row["workflow_activation_version_id"],
            "workflow_policy_id": row["workflow_policy_id"],
            "landing_proof_id": row["landing_proof_id"],
            "declaration": declaration.to_dict(),
            "content_sha256": row["content_sha256"],
            "declared_by": row["declared_by_handle"],
            "created_at": row["created_at"],
            "obligations": obligations,
        }

    def _workflow_closeout_record(
        self, connection: sqlite3.Connection, execution_id: str
    ) -> dict[str, Any] | None:
        observed_at = self.now()
        declaration_row = self._workflow_closeout_declaration_row(
            connection, execution_id
        )
        if declaration_row is None:
            return None
        declaration = self._workflow_closeout_declaration_record(
            connection, declaration_row, observed_at=observed_at
        )
        proof_row = connection.execute(
            "SELECT id, workflow_execution_id, declaration_id, "
            "workflow_activation_version_id, workflow_policy_id, "
            "landing_proof_id, schema_version, proof_json, content_sha256, "
            "status, created_at FROM workflow_closeout_proofs "
            "WHERE workflow_execution_id = ?",
            (execution_id,),
        ).fetchone()
        obligations = declaration["obligations"]
        open_count = sum(item["state"] == "open" for item in obligations)
        overdue_count = sum(item["deadline_state"] == "overdue" for item in obligations)
        if proof_row is None:
            return {
                **declaration,
                "status": "declared",
                "facts": None,
                "summary": "pending closeout",
                "open_obligation_count": open_count,
                "overdue_obligation_count": overdue_count,
            }
        try:
            proof = json.loads(proof_row["proof_json"])
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored workflow closeout proof is invalid") from exc
        if (
            not isinstance(proof, dict)
            or content_sha256(canonical_json(proof)) != proof_row["content_sha256"]
            or proof.get("schema_version") != proof_row["schema_version"]
            or proof.get("workflow_execution_id") != proof_row["workflow_execution_id"]
            or proof.get("declaration_id") != proof_row["declaration_id"]
            or proof.get("workflow_activation_version_id")
            != proof_row["workflow_activation_version_id"]
            or proof.get("workflow_policy_id") != proof_row["workflow_policy_id"]
            or proof.get("landing_proof_id") != proof_row["landing_proof_id"]
            or proof.get("status") != proof_row["status"]
            or proof_row["declaration_id"] != declaration["id"]
        ):
            raise StorageError("stored workflow closeout proof digest does not match")
        return {
            **declaration,
            "proof_id": proof_row["id"],
            "proof_sha256": proof_row["content_sha256"],
            "status": proof["status"],
            "facts": proof["facts"],
            "closed_at": proof_row["created_at"],
            "summary": "closed" if open_count == 0 else f"closed, {open_count} open",
            "open_obligation_count": open_count,
            "overdue_obligation_count": overdue_count,
        }

    def find_workflow_run(
        self, execution_id: str, correction_round: int, role: str
    ) -> dict[str, Any] | None:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        if (
            isinstance(correction_round, bool)
            or not isinstance(correction_round, int)
            or not 0 <= correction_round <= 8
        ):
            raise ValidationError("correction_round must be between 0 and 8")
        if role not in {"producer", "reviewer"}:
            raise ValidationError("workflow role is invalid")
        with self._read_snapshot() as connection:
            row = connection.execute(
                "SELECT run_id FROM workflow_run_bindings "
                "WHERE execution_id = ? AND correction_round = ? AND role = ? "
                "ORDER BY generation DESC LIMIT 1",
                (execution_id, correction_round, role),
            ).fetchone()
        return None if row is None else self.get_run(row["run_id"])

    def bind_workflow_gate(
        self,
        execution_id: str,
        *,
        correction_round: int,
        candidate_id: str,
        policy_id: str,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._nonnegative_correction_round(correction_round)
        candidate_id = require_string(candidate_id, field="candidate_id", max_bytes=64)
        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT candidate_id, policy_id, created_at "
                    "FROM workflow_gate_bindings WHERE execution_id = ? "
                    "AND correction_round = ?",
                    (execution_id, correction_round),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["candidate_id"] != candidate_id
                        or existing["policy_id"] != policy_id
                    ):
                        raise ConflictError(
                            "workflow round already names another gate policy"
                        )
                    connection.commit()
                    return {
                        "execution_id": execution_id,
                        "correction_round": correction_round,
                        **dict(existing),
                    }
                source = connection.execute(
                    "SELECT e.state, e.correction_round, e.project_id, "
                    "e.context_version_id, e.work_thread_id, "
                    "ic.producer_run_id, ic.project_id AS candidate_project_id, "
                    "ic.context_version_id AS candidate_context_version_id, "
                    "ic.work_thread_id AS candidate_work_thread_id, "
                    "gp.candidate_id AS policy_candidate_id "
                    "FROM workflow_executions e "
                    "JOIN integration_candidates ic ON ic.id = ? "
                    "JOIN integration_gate_policies gp ON gp.id = ? "
                    "WHERE e.id = ?",
                    (candidate_id, policy_id, execution_id),
                ).fetchone()
                if source is None:
                    raise NotFoundError("workflow candidate or policy not found")
                if source["state"] != "active":
                    raise ConflictError("workflow execution is not active")
                if source["correction_round"] != correction_round:
                    raise ConflictError("workflow correction round changed")
                producer = connection.execute(
                    "SELECT run_id FROM workflow_run_bindings "
                    "WHERE execution_id = ? AND correction_round = ? "
                    "AND role = 'producer' ORDER BY generation DESC LIMIT 1",
                    (execution_id, correction_round),
                ).fetchone()
                if producer is None or producer["run_id"] != source["producer_run_id"]:
                    raise ConflictError(
                        "workflow candidate was not produced by this round"
                    )
                if source["policy_candidate_id"] != candidate_id:
                    raise ConflictError("workflow gate policy names another candidate")
                for candidate_field, execution_field in (
                    ("candidate_project_id", "project_id"),
                    ("candidate_context_version_id", "context_version_id"),
                    ("candidate_work_thread_id", "work_thread_id"),
                ):
                    if source[candidate_field] != source[execution_field]:
                        raise ConflictError(
                            "workflow candidate belongs to another authority scope"
                        )
                connection.execute(
                    "INSERT INTO workflow_gate_bindings "
                    "(execution_id, correction_round, candidate_id, policy_id, "
                    "created_at) VALUES (?, ?, ?, ?, ?)",
                    (execution_id, correction_round, candidate_id, policy_id, now),
                )
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("workflow gate could not be bound") from exc
        return {
            "execution_id": execution_id,
            "correction_round": correction_round,
            "candidate_id": candidate_id,
            "policy_id": policy_id,
            "created_at": now,
        }

    @staticmethod
    def _nonnegative_correction_round(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 8:
            raise ValidationError("correction_round must be between 0 and 8")
        return value

    def get_workflow_gate(
        self, execution_id: str, correction_round: int
    ) -> dict[str, Any] | None:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._nonnegative_correction_round(correction_round)
        with self._read_snapshot() as connection:
            row = connection.execute(
                "SELECT execution_id, correction_round, candidate_id, policy_id, "
                "created_at FROM workflow_gate_bindings WHERE execution_id = ? "
                "AND correction_round = ?",
                (execution_id, correction_round),
            ).fetchone()
        return None if row is None else dict(row)

    def bind_workflow_landing(
        self,
        execution_id: str,
        *,
        expected_version: int,
        correction_round: int,
        candidate_id: str,
        policy_id: str,
        queue_item_id: str,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        self._nonnegative_correction_round(correction_round)
        candidate_id = require_string(candidate_id, field="candidate_id", max_bytes=64)
        policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
        queue_item_id = require_string(
            queue_item_id, field="queue_item_id", max_bytes=64
        )
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT execution_id, correction_round, candidate_id, policy_id, "
                    "queue_item_id, created_at FROM workflow_landing_bindings "
                    "WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()
                if existing is not None:
                    expected = {
                        "correction_round": correction_round,
                        "candidate_id": candidate_id,
                        "policy_id": policy_id,
                        "queue_item_id": queue_item_id,
                    }
                    if any(
                        existing[field] != value for field, value in expected.items()
                    ):
                        raise ConflictError(
                            "workflow already names another landing authority"
                        )
                    current = connection.execute(
                        "SELECT version FROM workflow_executions WHERE id = ?",
                        (execution_id,),
                    ).fetchone()
                    if current is None:
                        raise StorageError("workflow landing execution is unavailable")
                    if current["version"] != expected_version + 1:
                        raise ConcurrentUpdateError(
                            "workflow execution version changed"
                        )
                    connection.commit()
                    return dict(existing)
                source = connection.execute(
                    "SELECT e.state, e.stage, e.correction_round, e.version, "
                    "e.requested_by, e.project_id, e.work_thread_id, "
                    "g.candidate_id AS gate_candidate_id, "
                    "g.policy_id AS gate_policy_id, q.project_id AS queue_project_id, "
                    "q.work_thread_id AS queue_work_thread_id, "
                    "q.candidate_id AS queue_candidate_id, "
                    "q.current_policy_id AS queue_policy_id, q.state AS queue_state "
                    "FROM workflow_executions e "
                    "JOIN workflow_gate_bindings g ON g.execution_id = e.id "
                    "AND g.correction_round = e.correction_round "
                    "JOIN integration_queue_items q ON q.id = ? "
                    "WHERE e.id = ?",
                    (queue_item_id, execution_id),
                ).fetchone()
                if source is None:
                    raise NotFoundError("workflow landing authority not found")
                if source["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if source["state"] != "active" or source["stage"] != "landing":
                    raise ConflictError("workflow is not ready for landing")
                if source["correction_round"] != correction_round:
                    raise ConflictError("workflow correction round changed")
                expected = {
                    "gate_candidate_id": candidate_id,
                    "gate_policy_id": policy_id,
                    "queue_candidate_id": candidate_id,
                    "queue_policy_id": policy_id,
                    "queue_project_id": source["project_id"],
                    "queue_work_thread_id": source["work_thread_id"],
                }
                if any(source[field] != value for field, value in expected.items()):
                    raise ConflictError(
                        "workflow landing authority does not match its exact gate"
                    )
                if source["queue_state"] == "cancelled":
                    raise ConflictError("workflow landing queue is cancelled")
                if not self._gate_readiness(connection, policy_id)["ready"]:
                    raise ConflictError("workflow landing gate is not ready")
                connection.execute(
                    "INSERT INTO workflow_landing_bindings "
                    "(execution_id, correction_round, candidate_id, policy_id, "
                    "queue_item_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        execution_id,
                        correction_round,
                        candidate_id,
                        policy_id,
                        queue_item_id,
                        now,
                    ),
                )
                changed = connection.execute(
                    "UPDATE workflow_executions SET version = version + 1, "
                    "updated_at = ? WHERE id = ? AND version = ? "
                    "AND state = 'active' AND stage = 'landing'",
                    (now, execution_id, expected_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "workflow execution changed concurrently"
                    )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM workflow_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()["sequence"]
                connection.execute(
                    "INSERT INTO workflow_execution_events "
                    "(execution_id, sequence, kind, from_state, to_state, actor, "
                    "metadata_json, created_at) VALUES (?, ?, "
                    "'workflow.landing_enqueued', 'active', 'active', ?, ?, ?)",
                    (
                        execution_id,
                        sequence,
                        source["requested_by"],
                        canonical_json(
                            {
                                "from_stage": "landing",
                                "to_stage": "landing",
                                "from_correction_round": correction_round,
                                "to_correction_round": correction_round,
                                "candidate_id": candidate_id,
                                "policy_id": policy_id,
                                "queue_item_id": queue_item_id,
                            }
                        ),
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
                raise ConflictError("workflow landing could not be bound") from exc
        return {
            "execution_id": execution_id,
            "correction_round": correction_round,
            "candidate_id": candidate_id,
            "policy_id": policy_id,
            "queue_item_id": queue_item_id,
            "created_at": now,
        }

    def create_workflow_provider_intent(
        self,
        execution_id: str,
        *,
        expected_version: int,
        head_ref: str,
        title: str,
        body: str,
        draft: bool,
        created_by: str,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        created_by = require_string(created_by, field="created_by", max_bytes=64)
        intent_id = self.new_id("wpi")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, created_by, role="workflow provider intent owner"
                )
                source = connection.execute(
                    "SELECT execution.project_id, project.krab_project_key, "
                    "execution.work_item_id, execution.work_item_version_id, "
                    "execution.state, execution.stage, execution.version, "
                    "execution.workflow_activation_version_id, "
                    "configuration.spec_json AS configuration_spec_json, "
                    "activation.policy_id AS workflow_policy_id, "
                    "activation.spec_json AS activation_spec_json, "
                    "activation.acknowledged_by, item.source, item.external_id, "
                    "version.content_json, version.source_updated_at, "
                    "landing.candidate_id, landing.policy_id AS gate_policy_id, "
                    "queue.current_policy_id, ref.integration_ref, queue.state "
                    "AS queue_state FROM workflow_executions execution "
                    "JOIN projects project ON project.id = execution.project_id "
                    "JOIN workflow_configuration_versions configuration "
                    "ON configuration.id = execution.configuration_version_id "
                    "JOIN workflow_activation_versions activation "
                    "ON activation.id = execution.workflow_activation_version_id "
                    "JOIN work_items item ON item.id = execution.work_item_id "
                    "JOIN work_item_versions version "
                    "ON version.id = execution.work_item_version_id "
                    "JOIN workflow_landing_bindings landing "
                    "ON landing.execution_id = execution.id "
                    "JOIN integration_queue_items queue "
                    "ON queue.id = landing.queue_item_id "
                    "JOIN integration_refs ref ON ref.id = queue.integration_ref_id "
                    "WHERE execution.id = ?",
                    (execution_id,),
                ).fetchone()
                if source is None:
                    raise ConflictError(
                        "activated workflow provider landing is unavailable"
                    )
                if source["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if source["state"] != "active" or source["stage"] != "landing":
                    raise ConflictError("workflow is not awaiting landing")
                if source["acknowledged_by"] != created_by:
                    raise ConflictError(
                        "workflow provider intent requires the activation owner"
                    )
                try:
                    configuration = WorkflowConfigurationSpec.from_mapping(
                        json.loads(source["configuration_spec_json"])
                    )
                    activation = WorkflowActivationSpec.from_mapping(
                        json.loads(source["activation_spec_json"])
                    )
                    content = json.loads(source["content_json"])
                    envelope = WorkItemEnvelope.from_mapping(
                        {
                            **content,
                            "source": source["source"],
                            "external_id": source["external_id"],
                            "source_updated_at": source["source_updated_at"],
                        }
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored workflow provider intent inputs are invalid"
                    ) from exc
                if configuration.landing_mode is not WorkflowLandingMode.PROVIDER:
                    raise ConflictError("workflow is configured for manual landing")
                if source["queue_state"] not in {"queued", "claimed"}:
                    raise ConflictError(
                        "workflow landing queue cannot accept provider intent"
                    )
                if source["current_policy_id"] != source["gate_policy_id"]:
                    raise ConflictError("workflow landing policy changed")
                if title != envelope.title:
                    raise ConflictError(
                        "workflow provider title differs from the pinned work item"
                    )
                intent = WorkflowProviderIntent.from_mapping(
                    {
                        "schema_version": 1,
                        "project_id": source["project_id"],
                        "work_item_id": source["work_item_id"],
                        "work_item_version_id": source["work_item_version_id"],
                        "candidate_id": source["candidate_id"],
                        "gate_policy_id": source["gate_policy_id"],
                        "base_ref": source["integration_ref"],
                        "head_ref": head_ref,
                        "title": title,
                        "body": body,
                        "draft": draft,
                        "merge_method": activation.merge_method,
                        "secondary_effects": [],
                    }
                )
                existing = connection.execute(
                    "SELECT intent.*, creator.handle AS created_by_handle "
                    "FROM workflow_provider_intents intent "
                    "JOIN users creator ON creator.id = intent.created_by "
                    "WHERE intent.workflow_execution_id = ?",
                    (execution_id,),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["content_sha256"] != intent.digest
                        or existing["created_by"] != created_by
                    ):
                        raise ConflictError(
                            "workflow already has another provider intent"
                        )
                    connection.commit()
                    return self._workflow_provider_intent_record(connection, existing)
                connection.execute(
                    "INSERT INTO workflow_provider_intents "
                    "(id, workflow_execution_id, workflow_activation_version_id, "
                    "workflow_policy_id, candidate_id, gate_policy_id, "
                    "work_item_version_id, schema_version, intent_json, "
                    "content_sha256, created_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        intent_id,
                        execution_id,
                        source["workflow_activation_version_id"],
                        source["workflow_policy_id"],
                        source["candidate_id"],
                        source["gate_policy_id"],
                        source["work_item_version_id"],
                        intent.schema_version,
                        intent.canonical,
                        intent.digest,
                        created_by,
                        now,
                    ),
                )
                stored = connection.execute(
                    "SELECT intent.*, creator.handle AS created_by_handle "
                    "FROM workflow_provider_intents intent "
                    "JOIN users creator ON creator.id = intent.created_by "
                    "WHERE intent.id = ?",
                    (intent_id,),
                ).fetchone()
                assert stored is not None
                result = self._workflow_provider_intent_record(connection, stored)
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow provider intent could not be stored"
                ) from exc
        return result

    def _workflow_provider_effect_authority(
        self,
        connection: sqlite3.Connection,
        *,
        activation_version_id: str,
        capability_name: str,
    ) -> tuple[sqlite3.Row, Any, Any, Any]:
        capability = core_service_capability(capability_name, 1)
        row = connection.execute(
            "SELECT binding.id, binding.state, binding.project_id, "
            "binding.capability_name, binding.capability_version, "
            "binding.capability_mode, binding.mapping_name, "
            "binding.sidecar_content_sha256, binding.deployment_sha256, "
            "sidecar.name AS sidecar_name, sidecar.state AS sidecar_state, "
            "sidecar.spec_json AS sidecar_spec_json, project.active "
            "AS project_active FROM workflow_activation_capability_bindings pin "
            "JOIN project_capability_bindings binding "
            "ON binding.id = pin.project_capability_binding_id "
            "JOIN service_sidecars sidecar "
            "ON sidecar.id = binding.service_sidecar_id "
            "JOIN projects project ON project.id = binding.project_id "
            "WHERE pin.activation_version_id = ? AND pin.capability_name = ? "
            "AND pin.capability_version = 1",
            (activation_version_id, capability_name),
        ).fetchone()
        if (
            row is None
            or row["state"] != "active"
            or row["sidecar_state"] != "active"
            or not row["project_active"]
        ):
            raise ConflictError(
                f"activated workflow capability is unavailable: {capability_name}"
            )
        sidecar = self._parse_stored_sidecar(row)
        mapping = next(
            (item for item in sidecar.mappings if item.name == row["mapping_name"]),
            None,
        )
        advertised = next(
            (
                item
                for item in sidecar.capabilities
                if (item.name, item.version) == (capability.name, capability.version)
            ),
            None,
        )
        if (
            mapping is None
            or advertised != capability
            or row["sidecar_content_sha256"] != sidecar.digest
            or row["deployment_sha256"] != sidecar.deployment_sha256
            or row["capability_mode"] != capability.mode
        ):
            raise StorageError(
                "stored activated workflow capability binding is invalid"
            )
        return row, sidecar, mapping, capability

    def issue_workflow_provider_effect(
        self,
        execution_id: str,
        *,
        expected_version: int,
        capability_name: str,
        issued_by: str,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        if capability_name not in {"refs.publish", "change.open"}:
            raise ValidationError("workflow provider effect capability is invalid")
        issued_by = require_string(issued_by, field="issued_by", max_bytes=64)
        effect_id = self.new_id("wpe")
        operation_id = self.new_id("sop")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, issued_by, role="workflow provider effect issuer"
                )
                source = connection.execute(
                    "SELECT execution.state, execution.stage, execution.version, "
                    "execution.workflow_activation_version_id, "
                    "intent.id AS intent_id, intent.intent_json, "
                    "intent.created_by, gate.expected_base_revision, "
                    "gate.head_revision, gate.object_format, "
                    "queue.state AS queue_state FROM workflow_executions execution "
                    "JOIN workflow_provider_intents intent "
                    "ON intent.workflow_execution_id = execution.id "
                    "JOIN integration_gate_policies gate "
                    "ON gate.id = intent.gate_policy_id "
                    "JOIN workflow_landing_bindings landing "
                    "ON landing.execution_id = execution.id "
                    "JOIN integration_queue_items queue "
                    "ON queue.id = landing.queue_item_id WHERE execution.id = ?",
                    (execution_id,),
                ).fetchone()
                if source is None:
                    raise ConflictError("workflow provider intent is unavailable")
                if source["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if source["state"] != "active" or source["stage"] != "landing":
                    raise ConflictError("workflow is not awaiting landing")
                if source["created_by"] != issued_by:
                    raise ConflictError(
                        "workflow provider effect requires the intent owner"
                    )
                if source["queue_state"] not in {"queued", "claimed"}:
                    raise ConflictError(
                        "workflow landing queue cannot issue provider effects"
                    )
                try:
                    intent = WorkflowProviderIntent.from_mapping(
                        json.loads(source["intent_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored workflow provider intent is invalid"
                    ) from exc
                existing = connection.execute(
                    "SELECT effect.*, operation.state AS operation_state "
                    "FROM workflow_provider_effects effect "
                    "JOIN service_operations operation "
                    "ON operation.id = effect.service_operation_id "
                    "WHERE effect.intent_id = ? AND effect.capability_name = ?",
                    (source["intent_id"], capability_name),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return self._workflow_provider_effect_record(existing)
                if capability_name == "change.open":
                    publication = connection.execute(
                        "SELECT state, conforms_intent FROM workflow_provider_effects "
                        "WHERE intent_id = ? AND capability_name = 'refs.publish'",
                        (source["intent_id"],),
                    ).fetchone()
                    if (
                        publication is None
                        or publication["state"] != "confirmed"
                        or publication["conforms_intent"] != 1
                    ):
                        raise ConflictError(
                            "workflow change cannot open before ref publication"
                        )
                authorities = {
                    required_name: self._workflow_provider_effect_authority(
                        connection,
                        activation_version_id=source["workflow_activation_version_id"],
                        capability_name=required_name,
                    )
                    for required_name in (
                        "refs.publish",
                        "change.open",
                        "landing.invoke",
                    )
                }
                binding, sidecar, mapping, capability = authorities[capability_name]
                if binding["project_id"] != intent.project_id:
                    raise ConflictError(
                        "workflow provider effect belongs to another project"
                    )
                if capability_name == "refs.publish":
                    resource_id = intent.candidate_id
                    expected_revision = None
                    parameters: dict[str, Any] = {"target_ref": intent.head_ref}
                else:
                    resource_id = intent.head_ref
                    expected_revision = source["expected_base_revision"]
                    parameters = {
                        "base_ref": intent.base_ref,
                        "head_ref": intent.head_ref,
                        "title": intent.title,
                        "body": intent.body,
                        "draft": intent.draft,
                        "expected_secondary_effects": list(intent.secondary_effects),
                    }
                operation = ServiceOperationSpec.from_mapping(
                    {
                        "schema_version": 1,
                        "connector_name": binding["sidecar_name"],
                        "deployment_sha256": sidecar.deployment_sha256,
                        "mapping": mapping.to_dict(),
                        "capability": capability.to_dict(),
                        "idempotency_key": (
                            f"workflow:{execution_id}:{capability_name}"
                        ),
                        "target": {
                            "repository": mapping.repository,
                            "resource_id": resource_id,
                            "object_format": source["object_format"],
                            "expected_revision": expected_revision,
                            "desired_revision": source["head_revision"],
                        },
                        "parameters": parameters,
                        "request_bytes": sidecar.request_bytes,
                        "response_bytes": sidecar.response_bytes,
                        "timeout_ms": sidecar.timeout_ms,
                    }
                )
                selected_operation_id = self._create_service_operation_in_transaction(
                    connection,
                    spec=operation,
                    created_by=issued_by,
                    project_capability_binding_id=binding["id"],
                    operation_id=operation_id,
                    now=now,
                )
                if selected_operation_id != operation_id:
                    raise ConflictError("workflow provider effect already exists")
                connection.execute(
                    "INSERT INTO workflow_provider_effects "
                    "(id, intent_id, capability_name, capability_binding_id, "
                    "service_operation_id, state, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)",
                    (
                        effect_id,
                        source["intent_id"],
                        capability_name,
                        binding["id"],
                        operation_id,
                        now,
                        now,
                    ),
                )
                stored = connection.execute(
                    "SELECT effect.*, operation.state AS operation_state "
                    "FROM workflow_provider_effects effect "
                    "JOIN service_operations operation "
                    "ON operation.id = effect.service_operation_id "
                    "WHERE effect.id = ?",
                    (effect_id,),
                ).fetchone()
                assert stored is not None
                result = self._workflow_provider_effect_record(stored)
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow provider effect could not be issued"
                ) from exc
        return result

    def settle_workflow_provider_effect(self, effect_id: str) -> dict[str, Any]:
        effect_id = require_string(effect_id, field="effect_id", max_bytes=64)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT effect.*, operation.state AS operation_state, "
                    "intent.intent_json FROM workflow_provider_effects effect "
                    "JOIN service_operations operation "
                    "ON operation.id = effect.service_operation_id "
                    "JOIN workflow_provider_intents intent "
                    "ON intent.id = effect.intent_id WHERE effect.id = ?",
                    (effect_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(
                        f"workflow provider effect not found: {effect_id}"
                    )
                if row["state"] != "pending":
                    connection.commit()
                    return self._workflow_provider_effect_record(row)
                operation_state = row["operation_state"]
                if operation_state not in {
                    "succeeded",
                    "denied",
                    "conflict",
                    "cancelled",
                }:
                    connection.commit()
                    return self._workflow_provider_effect_record(row)
                fact_json = None
                fact_sha256 = None
                conforms_intent = None
                state = "failed"
                if operation_state == "succeeded":
                    attempt = connection.execute(
                        "SELECT receipt_json FROM service_operation_attempts "
                        "WHERE operation_id = ? AND outcome = 'succeeded' "
                        "ORDER BY sequence DESC LIMIT 1",
                        (row["service_operation_id"],),
                    ).fetchone()
                    receipt = None
                    if attempt is not None and attempt["receipt_json"] is not None:
                        try:
                            receipt = ServiceOperationReceipt.from_mapping(
                                json.loads(attempt["receipt_json"])
                            )
                        except (
                            json.JSONDecodeError,
                            TypeError,
                            ValidationError,
                        ) as exc:
                            raise StorageError(
                                "stored workflow provider receipt is invalid"
                            ) from exc
                    if receipt is None or receipt.result is None:
                        state = "uncertain"
                    else:
                        try:
                            intent = WorkflowProviderIntent.from_mapping(
                                json.loads(row["intent_json"])
                            )
                            if row["capability_name"] == "refs.publish":
                                readback = RefPublicationReadback.from_mapping(
                                    receipt.result
                                )
                                matches = (
                                    readback.candidate_id == intent.candidate_id
                                    and readback.target_ref == intent.head_ref
                                    and not readback.secondary_effects
                                )
                            else:
                                readback = ChangeOpenReadback.from_mapping(
                                    receipt.result
                                )
                                matches = (
                                    readback.schema_version
                                    == OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION
                                    and readback.base_ref == intent.base_ref
                                    and readback.head_ref == intent.head_ref
                                    and readback.title == intent.title
                                    and readback.body == intent.body
                                    and readback.draft is intent.draft
                                    and readback.state == "open"
                                    and readback.secondary_effects
                                    == intent.secondary_effects
                                )
                        except (
                            json.JSONDecodeError,
                            TypeError,
                            ValidationError,
                        ) as exc:
                            raise StorageError(
                                "stored workflow provider readback is invalid"
                            ) from exc
                        state = "confirmed"
                        conforms_intent = 1 if matches else 0
                        fact_json = canonical_json(readback.to_dict())
                        fact_sha256 = content_sha256(fact_json)
                connection.execute(
                    "UPDATE workflow_provider_effects SET state = ?, "
                    "fact_json = ?, fact_sha256 = ?, conforms_intent = ?, "
                    "updated_at = ?, "
                    "resolved_at = ? WHERE id = ? AND state = 'pending'",
                    (
                        state,
                        fact_json,
                        fact_sha256,
                        conforms_intent,
                        now,
                        now,
                        effect_id,
                    ),
                )
                stored = connection.execute(
                    "SELECT effect.*, operation.state AS operation_state "
                    "FROM workflow_provider_effects effect "
                    "JOIN service_operations operation "
                    "ON operation.id = effect.service_operation_id "
                    "WHERE effect.id = ?",
                    (effect_id,),
                ).fetchone()
                assert stored is not None
                result = self._workflow_provider_effect_record(stored)
                connection.commit()
            except (NotFoundError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow provider effect could not be settled"
                ) from exc
        return result

    def derive_workflow_provider_target(
        self, execution_id: str, *, expected_version: int
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                source = connection.execute(
                    "SELECT execution.state, execution.stage, execution.version, "
                    "landing.queue_item_id, intent.id AS intent_id, "
                    "intent.intent_json, intent.created_by, effect.id AS effect_id, "
                    "effect.state AS effect_state, effect.conforms_intent, "
                    "effect.fact_json "
                    "FROM workflow_executions execution "
                    "JOIN workflow_landing_bindings landing "
                    "ON landing.execution_id = execution.id "
                    "JOIN workflow_provider_intents intent "
                    "ON intent.workflow_execution_id = execution.id "
                    "JOIN workflow_provider_effects effect "
                    "ON effect.intent_id = intent.id "
                    "AND effect.capability_name = 'change.open' "
                    "WHERE execution.id = ?",
                    (execution_id,),
                ).fetchone()
                if source is None or source["effect_state"] != "confirmed":
                    raise ConflictError("workflow change-open effect is not confirmed")
                if source["conforms_intent"] != 1:
                    raise ConflictError(
                        "workflow change-open effect does not conform to its intent"
                    )
                if source["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if source["state"] != "active" or source["stage"] != "landing":
                    raise ConflictError("workflow is not awaiting landing")
                try:
                    intent = WorkflowProviderIntent.from_mapping(
                        json.loads(source["intent_json"])
                    )
                    readback = ChangeOpenReadback.from_mapping(
                        json.loads(source["fact_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored workflow provider target source is invalid"
                    ) from exc
                existing = connection.execute(
                    "SELECT target.execution_id, target.queue_item_id, "
                    "target.resource_id, target.merge_method, "
                    "creator.handle AS created_by, target.created_at, "
                    "origin.intent_id, origin.change_effect_id "
                    "FROM workflow_provider_targets target "
                    "JOIN users creator ON creator.id = target.created_by "
                    "LEFT JOIN workflow_provider_target_origins origin "
                    "ON origin.workflow_execution_id = target.execution_id "
                    "WHERE target.execution_id = ?",
                    (execution_id,),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["resource_id"] != readback.change_id
                        or existing["merge_method"] != intent.merge_method
                    ):
                        raise ConflictError(
                            "workflow provider target differs from its readback"
                        )
                    if (
                        existing["intent_id"] is None
                        and existing["change_effect_id"] is None
                    ):
                        connection.execute(
                            "INSERT INTO workflow_provider_target_origins "
                            "(workflow_execution_id, intent_id, change_effect_id, "
                            "created_at) VALUES (?, ?, ?, ?)",
                            (
                                execution_id,
                                source["intent_id"],
                                source["effect_id"],
                                now,
                            ),
                        )
                        adopted = dict(existing)
                        adopted["intent_id"] = source["intent_id"]
                        adopted["change_effect_id"] = source["effect_id"]
                        connection.commit()
                        return adopted
                    if (
                        existing["intent_id"] != source["intent_id"]
                        or existing["change_effect_id"] != source["effect_id"]
                    ):
                        raise ConflictError(
                            "workflow provider target differs from its readback"
                        )
                    connection.commit()
                    return dict(existing)
                connection.execute(
                    "INSERT INTO workflow_provider_targets "
                    "(execution_id, queue_item_id, resource_id, merge_method, "
                    "created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        execution_id,
                        source["queue_item_id"],
                        readback.change_id,
                        intent.merge_method,
                        source["created_by"],
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO workflow_provider_target_origins "
                    "(workflow_execution_id, intent_id, change_effect_id, "
                    "created_at) VALUES (?, ?, ?, ?)",
                    (
                        execution_id,
                        source["intent_id"],
                        source["effect_id"],
                        now,
                    ),
                )
                creator = connection.execute(
                    "SELECT handle FROM users WHERE id = ?",
                    (source["created_by"],),
                ).fetchone()
                assert creator is not None
                result = {
                    "execution_id": execution_id,
                    "queue_item_id": source["queue_item_id"],
                    "resource_id": readback.change_id,
                    "merge_method": intent.merge_method,
                    "created_by": creator["handle"],
                    "created_at": now,
                    "intent_id": source["intent_id"],
                    "change_effect_id": source["effect_id"],
                }
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow provider target could not be derived"
                ) from exc
        return result

    def configure_workflow_provider_target(
        self,
        execution_id: str,
        *,
        expected_version: int,
        resource_id: str,
        merge_method: str,
        created_by: str,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        resource_id = require_string(resource_id, field="resource_id", max_bytes=512)
        if not resource_id.strip():
            raise ValidationError("resource_id must not be blank")
        if merge_method not in {"merge", "rebase", "squash"}:
            raise ValidationError("merge_method is invalid")
        created_by = require_string(created_by, field="created_by", max_bytes=64)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, created_by, role="workflow landing authorizer"
                )
                creator = connection.execute(
                    "SELECT handle FROM users WHERE id = ?", (created_by,)
                ).fetchone()
                existing = connection.execute(
                    "SELECT t.execution_id, t.queue_item_id, t.resource_id, "
                    "t.merge_method, u.handle AS created_by, t.created_at, "
                    "origin.intent_id, origin.change_effect_id "
                    "FROM workflow_provider_targets t JOIN users u "
                    "ON u.id = t.created_by "
                    "LEFT JOIN workflow_provider_target_origins origin "
                    "ON origin.workflow_execution_id = t.execution_id "
                    "WHERE t.execution_id = ?",
                    (execution_id,),
                ).fetchone()
                if existing is not None:
                    current = connection.execute(
                        "SELECT e.state, e.stage, e.version, cv.spec_json, "
                        "av.spec_json AS activation_spec_json "
                        "FROM workflow_executions e "
                        "JOIN workflow_configuration_versions cv "
                        "ON cv.id = e.configuration_version_id "
                        "LEFT JOIN workflow_activation_versions av "
                        "ON av.id = e.workflow_activation_version_id WHERE e.id = ?",
                        (execution_id,),
                    ).fetchone()
                    if current is None:
                        raise StorageError("workflow execution is unavailable")
                    if current["version"] != expected_version:
                        raise ConcurrentUpdateError(
                            "workflow execution version changed"
                        )
                    if current["state"] != "active" or current["stage"] != "landing":
                        raise ConflictError("workflow is not awaiting landing")
                    try:
                        current_spec = WorkflowConfigurationSpec.from_mapping(
                            json.loads(current["spec_json"])
                        )
                    except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                        raise StorageError(
                            "stored workflow configuration is invalid"
                        ) from exc
                    if current_spec.landing_mode is not WorkflowLandingMode.PROVIDER:
                        raise ConflictError("workflow is configured for manual landing")
                    if current["activation_spec_json"] is not None:
                        try:
                            activation = WorkflowActivationSpec.from_mapping(
                                json.loads(current["activation_spec_json"])
                            )
                        except (
                            json.JSONDecodeError,
                            TypeError,
                            ValidationError,
                        ) as exc:
                            raise StorageError(
                                "stored workflow activation is invalid"
                            ) from exc
                        if activation.merge_method != merge_method:
                            raise ConflictError(
                                "workflow merge method differs from its activation"
                            )
                        if (
                            existing["intent_id"] is None
                            or existing["change_effect_id"] is None
                        ):
                            raise StorageError(
                                "activated workflow provider target has no origin"
                            )
                    if (
                        existing["resource_id"] != resource_id
                        or existing["merge_method"] != merge_method
                    ):
                        raise ConflictError(
                            "workflow already names another provider target"
                        )
                    result = dict(existing)
                    if result["intent_id"] is None:
                        result.pop("intent_id")
                        result.pop("change_effect_id")
                    connection.commit()
                    return result
                source = connection.execute(
                    "SELECT e.state, e.stage, e.version, cv.spec_json, "
                    "av.spec_json AS activation_spec_json, "
                    "b.queue_item_id, q.state AS queue_state "
                    "FROM workflow_executions e "
                    "JOIN workflow_configuration_versions cv "
                    "ON cv.id = e.configuration_version_id "
                    "JOIN workflow_landing_bindings b ON b.execution_id = e.id "
                    "JOIN integration_queue_items q ON q.id = b.queue_item_id "
                    "LEFT JOIN workflow_activation_versions av "
                    "ON av.id = e.workflow_activation_version_id "
                    "WHERE e.id = ?",
                    (execution_id,),
                ).fetchone()
                if source is None:
                    raise NotFoundError("workflow landing binding not found")
                if source["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if source["state"] != "active" or source["stage"] != "landing":
                    raise ConflictError("workflow is not awaiting landing")
                try:
                    spec = WorkflowConfigurationSpec.from_mapping(
                        json.loads(source["spec_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored workflow configuration is invalid"
                    ) from exc
                if spec.landing_mode is not WorkflowLandingMode.PROVIDER:
                    raise ConflictError("workflow is configured for manual landing")
                if source["activation_spec_json"] is not None:
                    try:
                        activation = WorkflowActivationSpec.from_mapping(
                            json.loads(source["activation_spec_json"])
                        )
                    except (
                        json.JSONDecodeError,
                        TypeError,
                        ValidationError,
                    ) as exc:
                        raise StorageError(
                            "stored workflow activation is invalid"
                        ) from exc
                    if activation.merge_method != merge_method:
                        raise ConflictError(
                            "workflow merge method differs from its activation"
                        )
                    raise ConflictError(
                        "activated workflow provider target is derived from "
                        "provider readback"
                    )
                if source["queue_state"] not in {"queued", "refold_required"}:
                    raise ConflictError("workflow landing authority is already in use")
                connection.execute(
                    "INSERT INTO workflow_provider_targets "
                    "(execution_id, queue_item_id, resource_id, merge_method, "
                    "created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        execution_id,
                        source["queue_item_id"],
                        resource_id,
                        merge_method,
                        created_by,
                        now,
                    ),
                )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow provider target could not be stored"
                ) from exc
        return {
            "execution_id": execution_id,
            "queue_item_id": source["queue_item_id"],
            "resource_id": resource_id,
            "merge_method": merge_method,
            "created_by": creator["handle"],
            "created_at": now,
        }

    def advance_workflow_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        expected_stage: str,
        expected_correction_round: int,
        target_stage: str,
        target_correction_round: int,
        event_kind: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        self._nonnegative_correction_round(expected_correction_round)
        self._nonnegative_correction_round(target_correction_round)
        allowed_stages = {"build", "review", "maintainer", "landing", "completion"}
        if expected_stage not in allowed_stages or target_stage not in allowed_stages:
            raise ValidationError("workflow stage is invalid")
        allowed_events = {
            "workflow.review_started",
            "workflow.correction_started",
            "workflow.maintainer_pending",
            "workflow.landing_ready",
            "workflow.landing_enqueued",
            "workflow.landing_recorded",
            "workflow.ruling_resolved",
        }
        if event_kind not in allowed_events:
            raise ValidationError("workflow transition event is invalid")
        if not isinstance(metadata, dict):
            raise ValidationError("workflow transition metadata must be an object")
        if event_kind == "workflow.correction_started":
            raw_cause = metadata.get("round_cause")
            if not isinstance(raw_cause, dict):
                raise ValidationError(
                    "workflow correction metadata requires round_cause"
                )
            round_cause = WorkflowRoundCause.from_mapping(raw_cause)
            if (
                target_stage != "build"
                or target_correction_round != expected_correction_round + 1
            ):
                raise ValidationError(
                    "workflow correction must advance exactly one build round"
                )
            if (
                metadata.get("policy_id", round_cause.policy_id)
                != round_cause.policy_id
            ):
                raise ValidationError(
                    "workflow correction policy_id contradicts round_cause"
                )
            if round_cause.kind == "reviewer_finding":
                expected_provenance = {
                    "feedback_source": "independent_review",
                    "review_evidence_id": round_cause.review_evidence_id,
                    "review_entry_id": round_cause.review_entry_id,
                    "scope_review_id": round_cause.scope_review_id,
                    "maintainer_review_id": None,
                }
            elif round_cause.kind == "maintainer_request":
                expected_provenance = {
                    "feedback_source": "maintainer_review",
                    "review_evidence_id": None,
                    "review_entry_id": None,
                    "scope_review_id": None,
                    "maintainer_review_id": round_cause.maintainer_review_id,
                }
            else:
                expected_provenance = {}
            if any(
                metadata.get(field) != expected
                for field, expected in expected_provenance.items()
            ):
                raise ValidationError(
                    "workflow correction provenance contradicts round_cause"
                )
            metadata = {**metadata, "round_cause": round_cause.to_dict()}
        elif "round_cause" in metadata:
            raise ValidationError(
                "workflow transition only accepts round_cause for a correction"
            )
        metadata_json = canonical_json(metadata)
        if len(metadata_json.encode("utf-8")) > 16 * 1024:
            raise ValidationError("workflow transition metadata is too large")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, stage, correction_round, version, requested_by "
                    "FROM workflow_executions WHERE id = ?",
                    (execution_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"workflow execution not found: {execution_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if row["state"] != "active":
                    raise ConflictError("workflow execution is not active")
                if (
                    row["stage"] != expected_stage
                    or row["correction_round"] != expected_correction_round
                ):
                    raise ConcurrentUpdateError("workflow execution stage changed")
                changed = connection.execute(
                    "UPDATE workflow_executions SET stage = ?, "
                    "correction_round = ?, version = version + 1, updated_at = ? "
                    "WHERE id = ? AND version = ? AND state = 'active'",
                    (
                        target_stage,
                        target_correction_round,
                        now,
                        execution_id,
                        expected_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "workflow execution changed concurrently"
                    )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM workflow_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()["sequence"]
                event_metadata = {
                    "from_stage": expected_stage,
                    "to_stage": target_stage,
                    "from_correction_round": expected_correction_round,
                    "to_correction_round": target_correction_round,
                    **metadata,
                }
                connection.execute(
                    "INSERT INTO workflow_execution_events "
                    "(execution_id, sequence, kind, from_state, to_state, actor, "
                    "metadata_json, created_at) VALUES (?, ?, ?, 'active', "
                    "'active', ?, ?, ?)",
                    (
                        execution_id,
                        sequence,
                        event_kind,
                        row["requested_by"],
                        canonical_json(event_metadata),
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
                raise ConflictError("workflow execution could not advance") from exc
        return self.get_workflow_execution(execution_id)

    def declare_workflow_closeout(
        self,
        execution_id: str,
        *,
        expected_version: int,
        declaration: WorkflowCloseoutDeclaration,
        declared_by: str,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        if not isinstance(declaration, WorkflowCloseoutDeclaration):
            raise ValidationError("declaration must be a WorkflowCloseoutDeclaration")
        declared_by = require_string(declared_by, field="declared_by", max_bytes=64)
        declaration_id = self.new_id("wcd")
        now = self.now()
        try:
            observed = datetime.fromisoformat(now.replace("Z", "+00:00"))
        except ValueError as exc:
            raise StorageError("control-plane closeout time is invalid") from exc
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, declared_by, role="workflow closeout declarer"
                )
                source = connection.execute(
                    "SELECT execution.state, execution.stage, execution.version, "
                    "execution.workflow_activation_version_id, "
                    "activation.acknowledged_by, activation.policy_id, "
                    "activation.spec_json AS activation_spec_json, "
                    "policy.spec_json AS policy_spec_json, "
                    "landing.policy_id AS gate_policy_id, "
                    "proof.id AS landing_proof_id, "
                    "proof.status AS landing_proof_status "
                    "FROM workflow_executions execution "
                    "JOIN workflow_activation_versions activation "
                    "ON activation.id = execution.workflow_activation_version_id "
                    "JOIN workflow_policies policy ON policy.id = activation.policy_id "
                    "JOIN workflow_landing_bindings landing "
                    "ON landing.execution_id = execution.id "
                    "JOIN workflow_provider_landing_proofs proof "
                    "ON proof.workflow_execution_id = execution.id "
                    "WHERE execution.id = ?",
                    (execution_id,),
                ).fetchone()
                if source is None:
                    raise ConflictError(
                        "activated workflow closeout authority is unavailable"
                    )
                if source["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if source["state"] != "active" or source["stage"] != "completion":
                    raise ConflictError("workflow is not awaiting closeout")
                if source["acknowledged_by"] != declared_by:
                    raise ConflictError(
                        "workflow closeout declaration requires the activation owner"
                    )
                if source["landing_proof_status"] != "conformant":
                    raise ConflictError(
                        "workflow closeout requires a conformant landing proof"
                    )
                try:
                    activation = WorkflowActivationSpec.from_mapping(
                        json.loads(source["activation_spec_json"])
                    )
                    policy = WorkflowPolicySpec.from_mapping(
                        json.loads(source["policy_spec_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored workflow closeout authority is invalid"
                    ) from exc
                if declaration.obligations and not policy.allow_deferred_validation:
                    raise ConflictError(
                        "workflow policy does not permit deferred validation"
                    )
                validation_bindings = {
                    binding.obligation: binding.validation
                    for binding in activation.validation_bindings
                }
                evidence: dict[str, tuple[str, str]] = {}
                for obligation in declaration.obligations:
                    if obligation.obligation not in policy.validation_obligations:
                        raise ConflictError(
                            "workflow closeout names an unknown policy obligation"
                        )
                    validation_name = validation_bindings.get(obligation.obligation)
                    if validation_name is None:
                        raise ConflictError(
                            "workflow closeout policy obligation has no activation "
                            "binding"
                        )
                    deadline = datetime.fromisoformat(
                        obligation.deadline.replace("Z", "+00:00")
                    )
                    if deadline <= observed:
                        raise ConflictError(
                            "workflow closeout obligation deadline must be future"
                        )
                    gate_name = "validation:" + validation_name
                    baseline = connection.execute(
                        "SELECT id, outcome FROM integration_gate_evidence "
                        "WHERE policy_id = ? AND gate_name = ? "
                        "ORDER BY sequence DESC LIMIT 1",
                        (source["gate_policy_id"], gate_name),
                    ).fetchone()
                    if baseline is None or baseline["outcome"] != "passed":
                        raise ConflictError(
                            "workflow closeout cannot defer missing immutable "
                            "validation"
                        )
                    evidence[obligation.obligation] = (
                        validation_name,
                        baseline["id"],
                    )
                existing = self._workflow_closeout_declaration_row(
                    connection, execution_id
                )
                if existing is not None:
                    if (
                        existing["content_sha256"] != declaration.digest
                        or existing["declared_by"] != declared_by
                    ):
                        raise ConflictError(
                            "workflow already has another closeout declaration"
                        )
                    result = self._workflow_closeout_declaration_record(
                        connection, existing, observed_at=now
                    )
                    connection.commit()
                    return result
                connection.execute(
                    "INSERT INTO workflow_closeout_declarations "
                    "(id, workflow_execution_id, workflow_activation_version_id, "
                    "workflow_policy_id, landing_proof_id, schema_version, "
                    "declaration_json, content_sha256, declared_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        declaration_id,
                        execution_id,
                        source["workflow_activation_version_id"],
                        source["policy_id"],
                        source["landing_proof_id"],
                        declaration.schema_version,
                        declaration.canonical,
                        declaration.digest,
                        declared_by,
                        now,
                    ),
                )
                for obligation in declaration.obligations:
                    validation_name, baseline_evidence_id = evidence[
                        obligation.obligation
                    ]
                    connection.execute(
                        "INSERT INTO workflow_closeout_obligations "
                        "(id, declaration_id, workflow_execution_id, kind, "
                        "policy_obligation, validation_name, baseline_evidence_id, "
                        "deadline, reason, state, version, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', 1, ?, ?)",
                        (
                            self.new_id("wco"),
                            declaration_id,
                            execution_id,
                            obligation.kind,
                            obligation.obligation,
                            validation_name,
                            baseline_evidence_id,
                            obligation.deadline,
                            obligation.reason,
                            now,
                            now,
                        ),
                    )
                stored = self._workflow_closeout_declaration_row(
                    connection, execution_id
                )
                assert stored is not None
                result = self._workflow_closeout_declaration_record(
                    connection, stored, observed_at=now
                )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except (ValueError, sqlite3.IntegrityError) as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow closeout declaration could not be stored"
                ) from exc
        return result

    def get_workflow_closeout_obligation(self, obligation_id: str) -> dict[str, Any]:
        obligation_id = require_string(
            obligation_id, field="obligation_id", max_bytes=64
        )
        with self._read_snapshot() as connection:
            row = connection.execute(
                "SELECT obligation.id, obligation.declaration_id, "
                "obligation.workflow_execution_id, obligation.kind, "
                "obligation.policy_obligation, obligation.validation_name, "
                "obligation.baseline_evidence_id, obligation.deadline, "
                "obligation.reason, obligation.state, obligation.version, "
                "resolver.handle AS resolved_by_handle, obligation.resolution, "
                "obligation.created_at, obligation.updated_at, "
                "obligation.resolved_at FROM workflow_closeout_obligations "
                "obligation LEFT JOIN users resolver "
                "ON resolver.id = obligation.resolved_by WHERE obligation.id = ?",
                (obligation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"workflow closeout obligation not found: {obligation_id}"
                )
            return self._workflow_closeout_obligation_record(
                row, observed_at=self.now()
            )

    def list_workflow_closeout_obligations(
        self,
        *,
        execution_id: str | None = None,
        open_only: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if execution_id is not None:
            execution_id = require_string(
                execution_id, field="execution_id", max_bytes=64
            )
        if not isinstance(open_only, bool):
            raise ValidationError("open_only must be a boolean")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 500
        ):
            raise ValidationError("limit must be between 1 and 500")
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT obligation.id, obligation.declaration_id, "
                "obligation.workflow_execution_id, obligation.kind, "
                "obligation.policy_obligation, obligation.validation_name, "
                "obligation.baseline_evidence_id, obligation.deadline, "
                "obligation.reason, obligation.state, obligation.version, "
                "resolver.handle AS resolved_by_handle, obligation.resolution, "
                "obligation.created_at, obligation.updated_at, "
                "obligation.resolved_at FROM workflow_closeout_obligations "
                "obligation LEFT JOIN users resolver "
                "ON resolver.id = obligation.resolved_by "
                "WHERE (? IS NULL OR obligation.workflow_execution_id = ?) "
                "AND (? = 0 OR obligation.state = 'open') "
                "ORDER BY obligation.deadline, obligation.id LIMIT ?",
                (execution_id, execution_id, 1 if open_only else 0, limit),
            ).fetchall()
            observed_at = self.now()
            return [
                self._workflow_closeout_obligation_record(row, observed_at=observed_at)
                for row in rows
            ]

    def resolve_workflow_closeout_obligation(
        self,
        obligation_id: str,
        *,
        expected_version: int,
        resolution: str,
        resolved_by: str,
    ) -> dict[str, Any]:
        obligation_id = require_string(
            obligation_id, field="obligation_id", max_bytes=64
        )
        self._positive_integer(expected_version, field="expected_version")
        resolution = require_string(
            resolution, field="workflow closeout resolution", max_bytes=4096
        )
        if not resolution.strip():
            raise ValidationError("workflow closeout resolution must not be blank")
        resolved_by = require_string(resolved_by, field="resolved_by", max_bytes=64)
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(
                    connection, resolved_by, role="workflow closeout resolver"
                )
                source = connection.execute(
                    "SELECT obligation.state, obligation.version, "
                    "activation.acknowledged_by FROM workflow_closeout_obligations "
                    "obligation JOIN workflow_closeout_declarations declaration "
                    "ON declaration.id = obligation.declaration_id "
                    "JOIN workflow_activation_versions activation "
                    "ON activation.id = declaration.workflow_activation_version_id "
                    "WHERE obligation.id = ?",
                    (obligation_id,),
                ).fetchone()
                if source is None:
                    raise NotFoundError(
                        f"workflow closeout obligation not found: {obligation_id}"
                    )
                if source["acknowledged_by"] != resolved_by:
                    raise ConflictError(
                        "workflow closeout resolution requires the activation owner"
                    )
                if source["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        "workflow closeout obligation version changed"
                    )
                if source["state"] != "open":
                    raise ConflictError(
                        "workflow closeout obligation is already resolved"
                    )
                changed = connection.execute(
                    "UPDATE workflow_closeout_obligations SET state = 'resolved', "
                    "version = version + 1, resolved_by = ?, resolution = ?, "
                    "updated_at = ?, resolved_at = ? WHERE id = ? "
                    "AND version = ? AND state = 'open'",
                    (
                        resolved_by,
                        resolution,
                        now,
                        now,
                        obligation_id,
                        expected_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "workflow closeout obligation changed concurrently"
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
                    "workflow closeout obligation could not be resolved"
                ) from exc
        return self.get_workflow_closeout_obligation(obligation_id)

    def _record_workflow_closeout_proof(
        self,
        connection: sqlite3.Connection,
        *,
        execution_id: str,
        provider_landing_id: str,
        landing_receipt_id: str,
        now: str,
    ) -> dict[str, Any]:
        declaration_row = self._workflow_closeout_declaration_row(
            connection, execution_id
        )
        if declaration_row is None:
            raise ConflictError(
                "activated workflow requires an owner closeout declaration"
            )
        declaration = self._workflow_closeout_declaration_record(
            connection, declaration_row, observed_at=now
        )
        landing_proof_row = self._workflow_provider_landing_proof_row(
            connection, execution_id=execution_id
        )
        if landing_proof_row is None:
            raise ConflictError("workflow closeout landing proof is unavailable")
        landing_proof = self._workflow_provider_landing_proof_record(landing_proof_row)
        if (
            landing_proof["status"] != "conformant"
            or landing_proof["provider_landing_id"] != provider_landing_id
            or landing_proof["landing_receipt_id"] != landing_receipt_id
            or landing_proof["id"] != declaration["landing_proof_id"]
        ):
            raise ConflictError(
                "workflow closeout does not match its conformant landing proof"
            )
        readback_value = landing_proof.get("readback")
        if readback_value is None:
            raise ConflictError("workflow closeout landing readback is unavailable")
        try:
            landing_readback = LandingReadback.from_mapping(readback_value)
        except ValidationError as exc:
            raise StorageError("stored workflow closeout readback is invalid") from exc
        intent = self._workflow_provider_intent_for_execution(connection, execution_id)
        if intent is None:
            raise ConflictError("workflow closeout provider intent is unavailable")
        change_effect = intent["effects"].get("change.open")
        if (
            change_effect is None
            or change_effect["state"] != "confirmed"
            or change_effect["fact"] is None
        ):
            raise ConflictError("workflow closeout change-open readback is unavailable")
        try:
            change_readback = ChangeOpenReadback.from_mapping(change_effect["fact"])
            intent_value = WorkflowProviderIntent.from_mapping(intent["intent"])
        except ValidationError as exc:
            raise StorageError("stored workflow closeout metadata is invalid") from exc
        landing_operation = connection.execute(
            "SELECT service_operation_id FROM integration_provider_landings "
            "WHERE id = ?",
            (provider_landing_id,),
        ).fetchone()
        if landing_operation is None:
            raise StorageError("workflow closeout provider landing is unavailable")
        service_receipt = self._provider_service_receipt(
            connection, landing_operation["service_operation_id"]
        )
        try:
            facts = validate_delivery_closeout(
                intent=intent_value,
                change_readback=change_readback,
                landing_readback=landing_readback,
                remote_identity=service_receipt.remote_identity,
                closed_references=service_receipt.closed_references,
            )
        except ValidationError as exc:
            raise ConflictError(str(exc)) from exc
        existing = connection.execute(
            "SELECT id FROM workflow_closeout_proofs WHERE workflow_execution_id = ?",
            (execution_id,),
        ).fetchone()
        if existing is not None:
            raise ConflictError("workflow closeout proof already exists")
        proof_id = self.new_id("wcp")
        proof = {
            "schema_version": 1,
            "workflow_execution_id": execution_id,
            "declaration_id": declaration["id"],
            "workflow_activation_version_id": declaration[
                "workflow_activation_version_id"
            ],
            "workflow_policy_id": declaration["workflow_policy_id"],
            "landing_proof_id": landing_proof["id"],
            "provider_intent_id": intent["id"],
            "change_effect_id": change_effect["id"],
            "provider_landing_id": provider_landing_id,
            "landing_receipt_id": landing_receipt_id,
            "status": "closed",
            "facts": facts,
        }
        proof_json = canonical_json(proof)
        proof_sha256 = content_sha256(proof_json)
        connection.execute(
            "INSERT INTO workflow_closeout_proofs "
            "(id, workflow_execution_id, declaration_id, "
            "workflow_activation_version_id, workflow_policy_id, "
            "landing_proof_id, schema_version, proof_json, content_sha256, "
            "status, created_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, "
            "'closed', ?)",
            (
                proof_id,
                execution_id,
                declaration["id"],
                declaration["workflow_activation_version_id"],
                declaration["workflow_policy_id"],
                landing_proof["id"],
                proof_json,
                proof_sha256,
                now,
            ),
        )
        open_count = sum(
            obligation["state"] == "open" for obligation in declaration["obligations"]
        )
        return {
            "id": proof_id,
            "content_sha256": proof_sha256,
            "facts": facts,
            "summary": "closed" if open_count == 0 else f"closed, {open_count} open",
        }

    def complete_workflow_execution(
        self, execution_id: str, *, expected_version: int
    ) -> dict[str, Any]:
        """Atomically bind a landed receipt, final outcome, and sealed thread."""

        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        outcome_entry_id = self.new_id("wle")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                source = connection.execute(
                    "SELECT e.state, e.stage, e.correction_round, e.version, "
                    "e.work_thread_id, e.requested_by, "
                    "e.workflow_activation_version_id, cv.spec_json, "
                    "b.queue_item_id, b.candidate_id, "
                    "q.current_policy_id AS policy_id, q.state AS queue_state, "
                    "q.landed_receipt_id AS receipt_id, gp.head_revision, "
                    "r.plan_id, r.provider_landing_id, r.outcome, "
                    "r.observed_revision, r.assurance "
                    "FROM workflow_executions e "
                    "JOIN workflow_configuration_versions cv "
                    "ON cv.id = e.configuration_version_id "
                    "JOIN workflow_landing_bindings b ON b.execution_id = e.id "
                    "JOIN integration_queue_items q ON q.id = b.queue_item_id "
                    "JOIN integration_gate_policies gp "
                    "ON gp.id = q.current_policy_id "
                    "LEFT JOIN integration_landing_receipts r "
                    "ON r.id = q.landed_receipt_id WHERE e.id = ?",
                    (execution_id,),
                ).fetchone()
                if source is None:
                    raise NotFoundError("workflow landing completion not found")
                if source["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if source["state"] != "active" or source["stage"] != "completion":
                    raise ConflictError("workflow is not ready for completion")
                if (
                    source["queue_state"] != "landed"
                    or source["receipt_id"] is None
                    or source["outcome"] != "landed"
                ):
                    raise ConflictError("workflow has no matching landed receipt")
                try:
                    spec = WorkflowConfigurationSpec.from_mapping(
                        json.loads(source["spec_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError(
                        "stored workflow configuration is invalid"
                    ) from exc
                closeout_proof: dict[str, Any] | None = None
                if spec.landing_mode is WorkflowLandingMode.MANUAL:
                    if (
                        source["plan_id"] is None
                        or source["provider_landing_id"] is not None
                    ):
                        raise ConflictError(
                            "workflow receipt does not match manual landing mode"
                        )
                else:
                    if (
                        source["provider_landing_id"] is None
                        or source["plan_id"] is not None
                    ):
                        raise ConflictError(
                            "workflow receipt does not match provider landing mode"
                        )
                    target = connection.execute(
                        "SELECT target.resource_id, target.merge_method, "
                        "origin.intent_id, origin.change_effect_id "
                        "FROM workflow_provider_targets target "
                        "LEFT JOIN workflow_provider_target_origins origin "
                        "ON origin.workflow_execution_id = target.execution_id "
                        "WHERE target.execution_id = ?",
                        (execution_id,),
                    ).fetchone()
                    operation = connection.execute(
                        "SELECT o.spec_json FROM integration_provider_landings p "
                        "JOIN service_operations o ON o.id = p.service_operation_id "
                        "WHERE p.id = ? AND p.queue_item_id = ?",
                        (source["provider_landing_id"], source["queue_item_id"]),
                    ).fetchone()
                    if target is None or operation is None:
                        raise ConflictError(
                            "workflow provider landing target is unavailable"
                        )
                    try:
                        operation_spec = json.loads(operation["spec_json"])
                        operation_resource = operation_spec["target"]["resource_id"]
                        operation_method = operation_spec["parameters"]["merge_method"]
                    except (json.JSONDecodeError, KeyError, TypeError) as exc:
                        raise StorageError(
                            "stored provider landing operation is invalid"
                        ) from exc
                    if (
                        operation_resource != target["resource_id"]
                        or operation_method != target["merge_method"]
                    ):
                        raise ConflictError(
                            "workflow provider landing does not match its target"
                        )
                    if source["workflow_activation_version_id"] is not None:
                        if (
                            target["intent_id"] is None
                            or target["change_effect_id"] is None
                        ):
                            raise ConflictError(
                                "activated workflow provider target has no "
                                "confirmed origin"
                            )
                        proof = connection.execute(
                            "SELECT status FROM workflow_provider_landing_proofs "
                            "WHERE workflow_execution_id = ? "
                            "AND provider_landing_id = ? "
                            "AND landing_receipt_id = ?",
                            (
                                execution_id,
                                source["provider_landing_id"],
                                source["receipt_id"],
                            ),
                        ).fetchone()
                        if proof is None or proof["status"] != "conformant":
                            raise ConflictError(
                                "workflow provider landing proof is not conformant"
                            )
                        closeout_proof = self._record_workflow_closeout_proof(
                            connection,
                            execution_id=execution_id,
                            provider_landing_id=source["provider_landing_id"],
                            landing_receipt_id=source["receipt_id"],
                            now=now,
                        )
                if not self._gate_readiness(connection, source["policy_id"])["ready"]:
                    raise ConflictError("workflow landing gate is no longer ready")
                active_run = connection.execute(
                    "SELECT id FROM runs WHERE work_thread_id = ? AND state IN "
                    "('queued', 'preparing', 'running', 'waiting_for_input', "
                    "'paused') LIMIT 1",
                    (source["work_thread_id"],),
                ).fetchone()
                if active_run is not None:
                    raise ConflictError("workflow work item has active runs")
                unfinished = connection.execute(
                    "SELECT id FROM work_assignments WHERE work_thread_id = ? "
                    "AND state IN ('open', 'claimed') LIMIT 1",
                    (source["work_thread_id"],),
                ).fetchone()
                if unfinished is not None:
                    raise ConflictError("workflow work item has unfinished assignments")
                thread = connection.execute(
                    "SELECT state, version, log_sequence FROM work_threads "
                    "WHERE id = ?",
                    (source["work_thread_id"],),
                ).fetchone()
                if thread is None:
                    raise StorageError("workflow work thread is unavailable")
                if thread["state"] != "active":
                    raise ConflictError("workflow work thread is not active")
                outcome = WorkLogEntry.from_mapping(
                    {
                        "schema_version": 1,
                        "kind": WorkLogKind.OUTCOME.value,
                        "summary": (
                            "The exact workflow candidate landed and its durable "
                            "work thread was completed."
                        ),
                        "details": [
                            f"candidate_id={source['candidate_id']}",
                            f"landing_receipt_id={source['receipt_id']}",
                            f"landing_assurance={source['assurance']}",
                            f"observed_revision={source['observed_revision']}",
                        ]
                        + (
                            []
                            if closeout_proof is None
                            else [f"delivery_status={closeout_proof['summary']}"]
                        ),
                        "artifacts": [],
                        "related_interaction_ids": [],
                        "candidate_revision": source["head_revision"],
                    }
                )
                log_sequence = thread["log_sequence"] + 1
                idempotency_key = f"workflow-outcome-{execution_id}"
                connection.execute(
                    "INSERT INTO work_log_entries "
                    "(id, work_thread_id, sequence, kind, content_json, "
                    "content_sha256, author_run_id, author_attempt_id, actor_id, "
                    "workflow_execution_id, assignment_id, idempotency_key, "
                    "created_at) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, "
                    "NULL, ?, ?)",
                    (
                        outcome_entry_id,
                        source["work_thread_id"],
                        log_sequence,
                        outcome.kind.value,
                        outcome.canonical,
                        outcome.digest,
                        source["requested_by"],
                        execution_id,
                        idempotency_key,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO workflow_completions "
                    "(execution_id, queue_item_id, receipt_id, outcome_entry_id, "
                    "candidate_id, policy_id, observed_revision, completed_by, "
                    "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        execution_id,
                        source["queue_item_id"],
                        source["receipt_id"],
                        outcome_entry_id,
                        source["candidate_id"],
                        source["policy_id"],
                        source["observed_revision"],
                        source["requested_by"],
                        now,
                    ),
                )
                changed = connection.execute(
                    "UPDATE work_threads SET state = 'completed', "
                    "version = version + 2, log_sequence = ?, updated_at = ?, "
                    "completed_at = ? WHERE id = ? AND version = ? "
                    "AND state = 'active'",
                    (
                        log_sequence,
                        now,
                        now,
                        source["work_thread_id"],
                        thread["version"],
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("workflow work thread changed")
                thread_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM work_thread_events WHERE work_thread_id = ?",
                    (source["work_thread_id"],),
                ).fetchone()["sequence"]
                connection.execute(
                    "INSERT INTO work_thread_events "
                    "(work_thread_id, sequence, kind, from_state, to_state, "
                    "actor_id, metadata_json, created_at) VALUES (?, ?, "
                    "'work_thread.completed', 'active', 'completed', ?, ?, ?)",
                    (
                        source["work_thread_id"],
                        thread_sequence,
                        source["requested_by"],
                        canonical_json(
                            {
                                "outcome_entry_id": outcome_entry_id,
                                "landing_receipt_id": source["receipt_id"],
                                "workflow_execution_id": execution_id,
                            }
                        ),
                        now,
                    ),
                )
                changed = connection.execute(
                    "UPDATE workflow_executions SET state = 'completed', "
                    "version = version + 1, updated_at = ?, finished_at = ? "
                    "WHERE id = ? AND version = ? AND state = 'active' "
                    "AND stage = 'completion'",
                    (now, now, execution_id, expected_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "workflow execution changed concurrently"
                    )
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM workflow_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()["sequence"]
                connection.execute(
                    "INSERT INTO workflow_execution_events "
                    "(execution_id, sequence, kind, from_state, to_state, actor, "
                    "metadata_json, created_at) VALUES (?, ?, "
                    "'workflow.completed', 'active', 'completed', ?, ?, ?)",
                    (
                        execution_id,
                        event_sequence,
                        source["requested_by"],
                        canonical_json(
                            {
                                "from_stage": "completion",
                                "to_stage": "completion",
                                "from_correction_round": source["correction_round"],
                                "to_correction_round": source["correction_round"],
                                "candidate_id": source["candidate_id"],
                                "policy_id": source["policy_id"],
                                "queue_item_id": source["queue_item_id"],
                                "receipt_id": source["receipt_id"],
                                "outcome_entry_id": outcome_entry_id,
                                **(
                                    {}
                                    if closeout_proof is None
                                    else {
                                        "closeout_proof_id": closeout_proof["id"],
                                        "closeout_proof_sha256": closeout_proof[
                                            "content_sha256"
                                        ],
                                    }
                                ),
                            }
                        ),
                        now,
                    ),
                )
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow completion could not be recorded"
                ) from exc
        return self.get_workflow_execution(execution_id)

    @staticmethod
    def _workflow_ruling_round_cause(
        connection: sqlite3.Connection,
        *,
        execution_id: str,
        ruling_id: str,
    ) -> WorkflowRoundCause | None:
        row = connection.execute(
            "SELECT metadata_json FROM workflow_execution_events "
            "WHERE execution_id = ? AND kind = 'workflow.ruling_opened' "
            "AND json_extract(metadata_json, '$.ruling_id') = ? "
            "ORDER BY sequence DESC LIMIT 1",
            (execution_id, ruling_id),
        ).fetchone()
        if row is None:
            return None
        try:
            metadata = json.loads(row["metadata_json"])
            raw_cause = metadata.get("round_cause")
            if raw_cause is None:
                return None
            return WorkflowRoundCause.from_mapping(raw_cause)
        except (
            AttributeError,
            json.JSONDecodeError,
            TypeError,
            ValidationError,
        ) as exc:
            raise StorageError("stored workflow ruling round cause is invalid") from exc

    def open_workflow_ruling(
        self,
        execution_id: str,
        *,
        expected_version: int,
        kind: str,
        policy_id: str | None = None,
        run_id: str | None = None,
        gate_name: str | None = None,
        provider_landing_id: str | None = None,
        landing_plan_id: str | None = None,
        round_cause: WorkflowRoundCause | None = None,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        allowed = {
            "correction_limit",
            "producer_failed",
            "reviewer_failed",
            "review_verdict_missing",
            "validation_failed",
            "landing_failed",
            "landing_authority_mismatch",
            "landing_cancelled",
            "landing_refold_required",
            "resource_exhausted",
        }
        if kind not in allowed:
            raise ValidationError("workflow ruling kind is invalid")
        if kind == "correction_limit":
            if not isinstance(round_cause, WorkflowRoundCause):
                raise ValidationError(
                    "correction-limit ruling requires one typed round cause"
                )
        elif round_cause is not None:
            raise ValidationError(
                "workflow ruling only accepts round_cause at the correction limit"
            )
        if policy_id is not None:
            policy_id = require_string(policy_id, field="policy_id", max_bytes=64)
            if round_cause is not None and round_cause.policy_id != policy_id:
                raise ValidationError(
                    "workflow ruling policy_id contradicts round_cause"
                )
        if run_id is not None:
            run_id = require_string(run_id, field="run_id", max_bytes=64)
        if gate_name is not None:
            gate_name = require_string(gate_name, field="gate_name", max_bytes=128)
        if provider_landing_id is not None:
            provider_landing_id = require_string(
                provider_landing_id, field="provider_landing_id", max_bytes=64
            )
        if landing_plan_id is not None:
            landing_plan_id = require_string(
                landing_plan_id, field="landing_plan_id", max_bytes=64
            )
        if kind == "landing_failed":
            if provider_landing_id is None or landing_plan_id is not None:
                raise ValidationError(
                    "landing-failed ruling requires one provider landing"
                )
        elif kind == "landing_authority_mismatch":
            if (provider_landing_id is None) == (landing_plan_id is None):
                raise ValidationError(
                    "landing-authority ruling requires exactly one authority"
                )
        elif provider_landing_id is not None or landing_plan_id is not None:
            raise ValidationError("workflow ruling does not accept landing authority")
        ruling_id = self.new_id("wri")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                execution = connection.execute(
                    "SELECT state, stage, version, correction_round, requested_by "
                    "FROM workflow_executions WHERE id = ?",
                    (execution_id,),
                ).fetchone()
                if execution is None:
                    raise NotFoundError(f"workflow execution not found: {execution_id}")
                existing = connection.execute(
                    "SELECT * FROM workflow_rulings WHERE execution_id = ? "
                    "AND execution_version = ? AND kind = ?",
                    (execution_id, expected_version, kind),
                ).fetchone()
                if existing is not None:
                    if round_cause is not None:
                        existing_cause = self._workflow_ruling_round_cause(
                            connection,
                            execution_id=execution_id,
                            ruling_id=existing["id"],
                        )
                        if existing_cause != round_cause:
                            raise ConflictError(
                                "workflow ruling round cause changed on replay"
                            )
                    connection.commit()
                    return self._workflow_ruling_record(connection, existing)
                if execution["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if execution["state"] != "active":
                    raise ConflictError("workflow execution is not active")
                if kind == "landing_refold_required":
                    refold = connection.execute(
                        "SELECT 1 FROM workflow_landing_bindings b "
                        "JOIN integration_queue_items q ON q.id = b.queue_item_id "
                        "WHERE b.execution_id = ? AND q.state = 'refold_required'",
                        (execution_id,),
                    ).fetchone()
                    if execution["stage"] != "landing" or refold is None:
                        raise ConflictError(
                            "workflow landing does not require a refold"
                        )
                if (
                    connection.execute(
                        "SELECT 1 FROM workflow_rulings WHERE execution_id = ? "
                        "AND state = 'open'",
                        (execution_id,),
                    ).fetchone()
                    is not None
                ):
                    raise ConflictError("workflow execution already has an open ruling")
                if policy_id is not None:
                    gate = connection.execute(
                        "SELECT 1 FROM workflow_gate_bindings WHERE execution_id = ? "
                        "AND correction_round = ? AND policy_id = ?",
                        (execution_id, execution["correction_round"], policy_id),
                    ).fetchone()
                    if gate is None:
                        raise ConflictError("workflow ruling policy is not current")
                if run_id is not None:
                    bound_run = connection.execute(
                        "SELECT 1 FROM workflow_run_bindings WHERE execution_id = ? "
                        "AND correction_round = ? AND run_id = ? "
                        "AND generation = (SELECT MAX(latest.generation) "
                        "FROM workflow_run_bindings latest "
                        "WHERE latest.execution_id = ? "
                        "AND latest.correction_round = ? "
                        "AND latest.role = workflow_run_bindings.role)",
                        (
                            execution_id,
                            execution["correction_round"],
                            run_id,
                            execution_id,
                            execution["correction_round"],
                        ),
                    ).fetchone()
                    if bound_run is None:
                        raise ConflictError("workflow ruling run is not current")
                if provider_landing_id is not None:
                    provider_landing = connection.execute(
                        "SELECT pl.state FROM integration_provider_landings pl "
                        "JOIN workflow_landing_bindings b "
                        "ON b.queue_item_id = pl.queue_item_id "
                        "WHERE b.execution_id = ? AND pl.id = ?",
                        (execution_id, provider_landing_id),
                    ).fetchone()
                    if provider_landing is None:
                        raise ConflictError(
                            "workflow ruling provider landing is not current"
                        )
                    if kind == "landing_failed" and provider_landing["state"] not in {
                        "failed",
                        "not_landed",
                    }:
                        raise ConflictError(
                            "workflow ruling provider landing has not failed"
                        )
                if landing_plan_id is not None:
                    landing_plan = connection.execute(
                        "SELECT 1 FROM integration_landing_plans lp "
                        "JOIN workflow_landing_bindings b "
                        "ON b.queue_item_id = lp.queue_item_id "
                        "WHERE b.execution_id = ? AND lp.id = ?",
                        (execution_id, landing_plan_id),
                    ).fetchone()
                    if landing_plan is None:
                        raise ConflictError(
                            "workflow ruling landing plan is not current"
                        )
                connection.execute(
                    "INSERT INTO workflow_rulings "
                    "(id, execution_id, execution_version, correction_round, kind, "
                    "state, version, policy_id, run_id, gate_name, "
                    "provider_landing_id, landing_plan_id, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, 'open', 1, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        ruling_id,
                        execution_id,
                        expected_version,
                        execution["correction_round"],
                        kind,
                        policy_id,
                        run_id,
                        gate_name,
                        provider_landing_id,
                        landing_plan_id,
                        now,
                        now,
                    ),
                )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM workflow_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()["sequence"]
                event_metadata = {
                    "ruling_id": ruling_id,
                    "kind": kind,
                    "correction_round": execution["correction_round"],
                    "policy_id": policy_id,
                    "run_id": run_id,
                    "gate_name": gate_name,
                }
                if round_cause is not None:
                    finding_class_counts: dict[str, int] = {}
                    prior_causes = connection.execute(
                        "SELECT metadata_json FROM workflow_execution_events "
                        "WHERE execution_id = ? AND ("
                        "kind = 'workflow.correction_started' OR ("
                        "kind = 'workflow.ruling_resolved' "
                        "AND json_extract(metadata_json, '$.decision') = 'continue' "
                        "AND json_type(metadata_json, '$.round_cause') = 'object')) "
                        "ORDER BY sequence",
                        (execution_id,),
                    ).fetchall()
                    for prior in prior_causes:
                        prior_metadata = json.loads(prior["metadata_json"])
                        prior_cause = WorkflowRoundCause.from_mapping(
                            prior_metadata["round_cause"]
                        )
                        for finding_class in prior_cause.finding_classes:
                            finding_class_counts[finding_class] = (
                                finding_class_counts.get(finding_class, 0) + 1
                            )
                    for finding_class in round_cause.finding_classes:
                        finding_class_counts[finding_class] = (
                            finding_class_counts.get(finding_class, 0) + 1
                        )
                    event_metadata["round_cause"] = round_cause.to_dict()
                    event_metadata["finding_class_summary"] = [
                        {
                            "finding_class": finding_class,
                            "round_count": finding_class_counts[finding_class],
                        }
                        for finding_class in sorted(finding_class_counts)
                    ]
                if provider_landing_id is not None:
                    event_metadata["provider_landing_id"] = provider_landing_id
                if landing_plan_id is not None:
                    event_metadata["landing_plan_id"] = landing_plan_id
                connection.execute(
                    "INSERT INTO workflow_execution_events "
                    "(execution_id, sequence, kind, from_state, to_state, actor, "
                    "metadata_json, created_at) VALUES (?, ?, "
                    "'workflow.ruling_opened', 'active', 'active', ?, ?, ?)",
                    (
                        execution_id,
                        sequence,
                        execution["requested_by"],
                        canonical_json(event_metadata),
                        now,
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM workflow_rulings WHERE id = ?", (ruling_id,)
                ).fetchone()
                connection.commit()
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
                ValidationError,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("workflow ruling could not be opened") from exc
        return self._workflow_ruling_record(None, row)

    @staticmethod
    def _workflow_correction_ceiling_extension_record(
        connection: sqlite3.Connection, ruling_id: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT extension.ruling_id, extension.workflow_execution_id, "
            "extension.workflow_activation_version_id, "
            "extension.configured_bound, extension.exceeded_bound, "
            "extension.authorized_bound, owner.handle AS authorized_by, "
            "extension.created_at "
            "FROM workflow_correction_ceiling_extensions extension "
            "JOIN users owner ON owner.id = extension.authorized_by "
            "WHERE extension.ruling_id = ?",
            (ruling_id,),
        ).fetchone()
        if row is None:
            return None
        return {**dict(row), "single_use": True}

    @staticmethod
    def _workflow_ruling_record(
        connection: sqlite3.Connection | None, row: sqlite3.Row
    ) -> dict[str, Any]:
        record = dict(row)
        if connection is not None and row["resolved_by"] is not None:
            resolver = connection.execute(
                "SELECT handle FROM users WHERE id = ?", (row["resolved_by"],)
            ).fetchone()
            if resolver is None:
                raise StorageError("workflow ruling resolver is unavailable")
            record["resolved_by"] = resolver["handle"]
        record["ceiling_extension"] = (
            None
            if connection is None
            else WorkflowStorage._workflow_correction_ceiling_extension_record(
                connection, row["id"]
            )
        )
        return record

    def get_workflow_ruling(self, ruling_id: str) -> dict[str, Any]:
        ruling_id = require_string(ruling_id, field="ruling_id", max_bytes=64)
        with self._read_snapshot() as connection:
            row = connection.execute(
                "SELECT * FROM workflow_rulings WHERE id = ?", (ruling_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"workflow ruling not found: {ruling_id}")
            return self._workflow_ruling_record(connection, row)

    def resolve_workflow_ruling(
        self,
        ruling_id: str,
        *,
        expected_version: int,
        decision: str,
        resolved_by: str,
    ) -> dict[str, Any]:
        ruling_id = require_string(ruling_id, field="ruling_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        if decision not in {"continue", "cancel"}:
            raise ValidationError("workflow ruling decision is invalid")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, resolved_by, role="workflow resolver")
                ruling = connection.execute(
                    "SELECT * FROM workflow_rulings WHERE id = ?", (ruling_id,)
                ).fetchone()
                if ruling is None:
                    raise NotFoundError(f"workflow ruling not found: {ruling_id}")
                if ruling["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow ruling version changed")
                if ruling["state"] != "open":
                    raise ConflictError("workflow ruling is not open")
                execution = connection.execute(
                    "SELECT execution.state, execution.stage, "
                    "execution.correction_round, execution.version, "
                    "execution.requested_by, "
                    "execution.workflow_activation_version_id, "
                    "configuration.spec_json AS configuration_spec_json, "
                    "activation.acknowledged_by AS activation_owner "
                    "FROM workflow_executions execution "
                    "JOIN workflow_configuration_versions configuration "
                    "ON configuration.id = execution.configuration_version_id "
                    "LEFT JOIN workflow_activation_versions activation "
                    "ON activation.id = execution.workflow_activation_version_id "
                    "WHERE execution.id = ?",
                    (ruling["execution_id"],),
                ).fetchone()
                if execution is None:
                    raise StorageError("workflow ruling execution is unavailable")
                if (
                    execution["workflow_activation_version_id"] is not None
                    and execution["activation_owner"] != resolved_by
                ):
                    raise ConflictError(
                        "workflow ruling resolution requires the activation owner"
                    )
                if (
                    execution["state"] != "active"
                    or execution["version"] != ruling["execution_version"]
                    or execution["correction_round"] != ruling["correction_round"]
                ):
                    raise ConflictError("workflow execution changed after the ruling")
                resolution_metadata: dict[str, Any] = {}
                ceiling_extension: dict[str, Any] | None = None
                if decision == "continue":
                    if ruling["kind"] == "resource_exhausted":
                        raise ConflictError(
                            "an exhausted workflow resource ceiling cannot be "
                            "increased or continued"
                        )
                    if ruling["kind"] == "correction_limit":
                        if execution["correction_round"] >= 8:
                            raise ConflictError(
                                "workflow reached the absolute correction-round limit"
                            )
                        if (
                            connection.execute(
                                "SELECT 1 FROM "
                                "workflow_correction_ceiling_extensions "
                                "WHERE workflow_execution_id = ?",
                                (ruling["execution_id"],),
                            ).fetchone()
                            is not None
                        ):
                            raise ConflictError(
                                "workflow correction ceiling exception has already "
                                "been used"
                            )
                        round_cause = self._workflow_ruling_round_cause(
                            connection,
                            execution_id=ruling["execution_id"],
                            ruling_id=ruling_id,
                        )
                        if round_cause is None:
                            raise ConflictError(
                                "correction-limit continuation requires its typed "
                                "round cause"
                            )
                        if (
                            ruling["policy_id"] is not None
                            and ruling["policy_id"] != round_cause.policy_id
                        ):
                            raise StorageError(
                                "stored correction-limit cause is for another policy"
                            )
                        target_stage = "build"
                        target_round = execution["correction_round"] + 1
                        resolution_metadata["round_cause"] = round_cause.to_dict()
                        try:
                            configuration = WorkflowConfigurationSpec.from_mapping(
                                json.loads(execution["configuration_spec_json"])
                            )
                        except (
                            json.JSONDecodeError,
                            TypeError,
                            ValidationError,
                        ) as exc:
                            raise StorageError(
                                "stored workflow configuration is invalid"
                            ) from exc
                        if (
                            execution["correction_round"]
                            < configuration.max_correction_rounds
                        ):
                            raise ConflictError(
                                "workflow correction ceiling has not been reached"
                            )
                        ceiling_extension = {
                            "ruling_id": ruling_id,
                            "configured_bound": configuration.max_correction_rounds,
                            "exceeded_bound": execution["correction_round"],
                            "authorized_bound": target_round,
                            "single_use": True,
                        }
                    elif ruling["kind"] == "validation_failed":
                        if execution["stage"] != "review":
                            raise ConflictError(
                                "validation retry ruling is outside the review stage"
                            )
                        failed_evidence = connection.execute(
                            "SELECT id, outcome FROM integration_gate_evidence "
                            "WHERE policy_id = ? AND gate_name = ? "
                            "ORDER BY sequence DESC LIMIT 1",
                            (ruling["policy_id"], ruling["gate_name"]),
                        ).fetchone()
                        if (
                            failed_evidence is None
                            or failed_evidence["outcome"] != "failed"
                        ):
                            raise ConflictError(
                                "validation retry authority is no longer failed"
                            )
                        target_stage = "review"
                        target_round = execution["correction_round"]
                        resolution_metadata["failed_evidence_id"] = failed_evidence[
                            "id"
                        ]
                    elif ruling["kind"] == "landing_failed":
                        if execution["stage"] != "landing":
                            raise ConflictError(
                                "landing retry ruling is outside the landing stage"
                            )
                        provider_landing = connection.execute(
                            "SELECT state FROM integration_provider_landings "
                            "WHERE id = ?",
                            (ruling["provider_landing_id"],),
                        ).fetchone()
                        if provider_landing is None or provider_landing[
                            "state"
                        ] not in {"failed", "not_landed"}:
                            raise ConflictError(
                                "landing retry authority is no longer failed"
                            )
                        target_stage = "landing"
                        target_round = execution["correction_round"]
                    elif ruling["kind"] == "landing_refold_required":
                        if execution["stage"] != "landing":
                            raise ConflictError(
                                "refold ruling is outside the landing stage"
                            )
                        landing = connection.execute(
                            "SELECT q.state, q.current_policy_id "
                            "FROM workflow_landing_bindings b "
                            "JOIN integration_queue_items q "
                            "ON q.id = b.queue_item_id "
                            "WHERE b.execution_id = ?",
                            (ruling["execution_id"],),
                        ).fetchone()
                        if landing is None:
                            raise StorageError("workflow landing queue is unavailable")
                        if landing["state"] == "refold_required":
                            raise ConflictError(
                                "workflow landing still requires a refold"
                            )
                        if landing["state"] == "cancelled":
                            raise ConflictError("workflow landing queue was cancelled")
                        if not self._gate_readiness(
                            connection, landing["current_policy_id"]
                        )["ready"]:
                            raise ConflictError("workflow refold gate is not ready")
                        target_stage = "landing"
                        target_round = execution["correction_round"]
                    else:
                        raise ConflictError(
                            "only a correction-limit, validation-failed, "
                            "landing-failed, or refold-required ruling can "
                            "continue the workflow"
                        )
                    target_state = "active"
                    cancel_reason = None
                    finished_at = None
                else:
                    target_state = "cancelled"
                    target_stage = execution["stage"]
                    target_round = execution["correction_round"]
                    cancel_reason = "workflow ruling cancelled by maintainer"
                    finished_at = now
                changed = connection.execute(
                    "UPDATE workflow_executions SET state = ?, stage = ?, "
                    "correction_round = ?, version = version + 1, "
                    "cancel_reason = ?, updated_at = ?, finished_at = ? "
                    "WHERE id = ? AND version = ? AND state = 'active'",
                    (
                        target_state,
                        target_stage,
                        target_round,
                        cancel_reason,
                        now,
                        finished_at,
                        ruling["execution_id"],
                        execution["version"],
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError(
                        "workflow execution changed concurrently"
                    )
                if ceiling_extension is not None:
                    connection.execute(
                        "INSERT INTO workflow_correction_ceiling_extensions "
                        "(ruling_id, workflow_execution_id, "
                        "workflow_activation_version_id, configured_bound, "
                        "exceeded_bound, authorized_bound, authorized_by, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            ruling_id,
                            ruling["execution_id"],
                            execution["workflow_activation_version_id"],
                            ceiling_extension["configured_bound"],
                            ceiling_extension["exceeded_bound"],
                            ceiling_extension["authorized_bound"],
                            resolved_by,
                            now,
                        ),
                    )
                connection.execute(
                    "UPDATE workflow_rulings SET state = 'resolved', "
                    "version = version + 1, resolved_by = ?, decision = ?, "
                    "updated_at = ?, resolved_at = ? WHERE id = ? AND version = ?",
                    (
                        resolved_by,
                        decision,
                        now,
                        now,
                        ruling_id,
                        expected_version,
                    ),
                )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM workflow_execution_events WHERE execution_id = ?",
                    (ruling["execution_id"],),
                ).fetchone()["sequence"]
                connection.execute(
                    "INSERT INTO workflow_execution_events "
                    "(execution_id, sequence, kind, from_state, to_state, actor, "
                    "metadata_json, created_at) VALUES (?, ?, "
                    "'workflow.ruling_resolved', 'active', ?, ?, ?, ?)",
                    (
                        ruling["execution_id"],
                        sequence,
                        target_state,
                        resolved_by,
                        canonical_json(
                            {
                                "ruling_id": ruling_id,
                                "decision": decision,
                                "from_stage": execution["stage"],
                                "to_stage": target_stage,
                                "from_correction_round": execution["correction_round"],
                                "to_correction_round": target_round,
                                **resolution_metadata,
                            }
                        ),
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
                raise ConflictError("workflow ruling could not be resolved") from exc
        return self.get_workflow_ruling(ruling_id)

    def get_workflow_execution(self, execution_id: str) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        with self._read_snapshot() as connection:
            return self._execution_record(connection, execution_id)

    def list_workflow_executions(
        self,
        *,
        project_key: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if project_key is not None:
            project_key = validate_project_key(project_key)
        if state is not None and state not in {"active", "cancelled", "completed"}:
            raise ValidationError("workflow state is invalid")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 500
        ):
            raise ValidationError("limit must be between 1 and 500")
        with self._read_snapshot() as connection:
            rows = connection.execute(
                "SELECT e.id FROM workflow_executions e JOIN projects p "
                "ON p.id = e.project_id "
                "WHERE (? IS NULL OR p.krab_project_key = ?) "
                "AND (? IS NULL OR e.state = ?) "
                "ORDER BY e.created_at DESC, e.id DESC LIMIT ?",
                (project_key, project_key, state, state, limit),
            ).fetchall()
            return [self._execution_record(connection, row["id"]) for row in rows]

    def cancel_workflow_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        cancelled_by: str,
        reason: str,
    ) -> dict[str, Any]:
        execution_id = require_string(execution_id, field="execution_id", max_bytes=64)
        self._positive_integer(expected_version, field="expected_version")
        reason = require_string(reason, field="reason", max_bytes=4096)
        if not reason.strip():
            raise ValidationError("reason must not be blank")
        now = self.now()
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._active_user(connection, cancelled_by, role="workflow canceller")
                row = connection.execute(
                    "SELECT state, version FROM workflow_executions WHERE id = ?",
                    (execution_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"workflow execution not found: {execution_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError("workflow execution version changed")
                if row["state"] != "active":
                    raise ConflictError(
                        f"workflow execution is not active: {row['state']}"
                    )
                connection.execute(
                    "UPDATE workflow_executions SET state = 'cancelled', "
                    "version = version + 1, cancel_reason = ?, updated_at = ?, "
                    "finished_at = ? WHERE id = ?",
                    (reason, now, now, execution_id),
                )
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence "
                    "FROM workflow_execution_events WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()["sequence"]
                connection.execute(
                    "INSERT INTO workflow_execution_events "
                    "(execution_id, sequence, kind, from_state, to_state, actor, "
                    "metadata_json, created_at) VALUES (?, ?, "
                    "'workflow.cancelled', 'active', 'cancelled', ?, ?, ?)",
                    (
                        execution_id,
                        sequence,
                        cancelled_by,
                        canonical_json({"reason": reason}),
                        now,
                    ),
                )
                connection.commit()
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "workflow execution could not be cancelled"
                ) from exc
        return self.get_workflow_execution(execution_id)
