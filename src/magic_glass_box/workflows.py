from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .domain import ValidationError, WorkspaceMode, canonical_json, content_sha256
from .gates import GateError, ReviewSeparation, ValidationGate

WORKFLOW_CONFIGURATION_SCHEMA_VERSION = 1
WORKFLOW_TEMPLATE_SCHEMA_VERSION = 1
MAX_CORRECTION_ROUNDS = 8
IMPLEMENTATION_TEMPLATE_KEY = "implementation-v1"
WORKFLOW_TEMPLATE_KEY_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
WORKFLOW_TEMPLATE_MEMBER_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
MAX_WORKFLOW_TEMPLATE_ROLES = 8
MAX_WORKFLOW_TEMPLATE_STAGES = 16


class WorkflowLandingMode(StrEnum):
    MANUAL = "manual"
    PROVIDER = "provider"


@dataclass(frozen=True)
class WorkflowTemplateRole:
    name: str
    workspace_mode: WorkspaceMode

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowTemplateRole:
        if not isinstance(value, dict):
            raise ValidationError("workflow template role must be an object")
        if set(value) != {"name", "workspace_mode"}:
            raise ValidationError(
                "workflow template role requires name and workspace_mode"
            )
        name = value["name"]
        if (
            not isinstance(name, str)
            or WORKFLOW_TEMPLATE_MEMBER_RE.fullmatch(name) is None
        ):
            raise ValidationError("workflow template role name is invalid")
        try:
            workspace_mode = WorkspaceMode(value["workspace_mode"])
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                "workflow template role workspace_mode is invalid"
            ) from exc
        return cls(name=name, workspace_mode=workspace_mode)

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "workspace_mode": self.workspace_mode.value}


@dataclass(frozen=True)
class WorkflowTemplateDefinition:
    schema_version: int
    key: str
    version: int
    roles: tuple[WorkflowTemplateRole, ...]
    stages: tuple[str, ...]
    correction_loop: bool

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowTemplateDefinition:
        if not isinstance(value, dict):
            raise ValidationError("workflow template definition must be an object")
        fields = {
            "schema_version",
            "key",
            "version",
            "roles",
            "stages",
            "correction_loop",
        }
        if set(value) != fields:
            raise ValidationError(
                "workflow template definition has missing or unknown fields"
            )
        schema_version = value["schema_version"]
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version != WORKFLOW_TEMPLATE_SCHEMA_VERSION
        ):
            raise ValidationError(
                f"schema_version must be {WORKFLOW_TEMPLATE_SCHEMA_VERSION}"
            )
        key = value["key"]
        if not isinstance(key, str) or WORKFLOW_TEMPLATE_KEY_RE.fullmatch(key) is None:
            raise ValidationError("workflow template key is invalid")
        version = value["version"]
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ValidationError("workflow template version must be positive")
        raw_roles = value["roles"]
        if (
            not isinstance(raw_roles, list)
            or not 1 <= len(raw_roles) <= MAX_WORKFLOW_TEMPLATE_ROLES
        ):
            raise ValidationError(
                "workflow template roles must contain between 1 and "
                f"{MAX_WORKFLOW_TEMPLATE_ROLES} entries"
            )
        roles = tuple(WorkflowTemplateRole.from_mapping(role) for role in raw_roles)
        if len({role.name for role in roles}) != len(roles):
            raise ValidationError("workflow template role names must be unique")
        raw_stages = value["stages"]
        if (
            not isinstance(raw_stages, list)
            or not 1 <= len(raw_stages) <= MAX_WORKFLOW_TEMPLATE_STAGES
        ):
            raise ValidationError(
                "workflow template stages must contain between 1 and "
                f"{MAX_WORKFLOW_TEMPLATE_STAGES} entries"
            )
        stages = tuple(raw_stages)
        if any(
            not isinstance(stage, str)
            or WORKFLOW_TEMPLATE_MEMBER_RE.fullmatch(stage) is None
            for stage in stages
        ):
            raise ValidationError("workflow template stage is invalid")
        if len(set(stages)) != len(stages):
            raise ValidationError("workflow template stages must be unique")
        correction_loop = value["correction_loop"]
        if not isinstance(correction_loop, bool):
            raise ValidationError("workflow template correction_loop must be boolean")
        return cls(
            schema_version=schema_version,
            key=key,
            version=version,
            roles=roles,
            stages=stages,
            correction_loop=correction_loop,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "key": self.key,
            "version": self.version,
            "roles": [role.to_dict() for role in self.roles],
            "stages": list(self.stages),
            "correction_loop": self.correction_loop,
        }

    @property
    def role_workspace_modes(self) -> dict[str, str]:
        return {role.name: role.workspace_mode.value for role in self.roles}

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class WorkflowConfigurationSpec:
    schema_version: int
    review_separation: ReviewSeparation
    maintainer_approval: bool
    validations: tuple[ValidationGate, ...]
    max_correction_rounds: int
    landing_mode: WorkflowLandingMode

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowConfigurationSpec:
        if not isinstance(value, dict):
            raise ValidationError("workflow configuration must be an object")
        fields = {
            "schema_version",
            "review_separation",
            "maintainer_approval",
            "validations",
            "max_correction_rounds",
            "landing_mode",
        }
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown:
            raise ValidationError(
                "workflow configuration contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        if missing:
            raise ValidationError(
                "workflow configuration is missing fields: "
                + ", ".join(sorted(missing))
            )
        schema_version = value["schema_version"]
        if (
            isinstance(schema_version, bool)
            or schema_version != WORKFLOW_CONFIGURATION_SCHEMA_VERSION
        ):
            raise ValidationError(
                f"schema_version must be {WORKFLOW_CONFIGURATION_SCHEMA_VERSION}"
            )
        try:
            review_separation = ReviewSeparation(value["review_separation"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("review_separation is invalid") from exc
        maintainer_approval = value["maintainer_approval"]
        if not isinstance(maintainer_approval, bool):
            raise ValidationError("maintainer_approval must be boolean")
        raw_validations = value["validations"]
        if not isinstance(raw_validations, list) or len(raw_validations) > 32:
            raise ValidationError("validations must be a list of at most 32 entries")
        try:
            validations = tuple(
                ValidationGate.from_mapping(validation)
                for validation in raw_validations
            )
        except GateError as exc:
            raise ValidationError(str(exc)) from exc
        names = [validation.name for validation in validations]
        if len(set(names)) != len(names):
            raise ValidationError("validation names must be unique")
        max_correction_rounds = value["max_correction_rounds"]
        if (
            isinstance(max_correction_rounds, bool)
            or not isinstance(max_correction_rounds, int)
            or not 1 <= max_correction_rounds <= MAX_CORRECTION_ROUNDS
        ):
            raise ValidationError(
                f"max_correction_rounds must be between 1 and {MAX_CORRECTION_ROUNDS}"
            )
        try:
            landing_mode = WorkflowLandingMode(value["landing_mode"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("landing_mode is invalid") from exc
        return cls(
            schema_version=schema_version,
            review_separation=review_separation,
            maintainer_approval=maintainer_approval,
            validations=validations,
            max_correction_rounds=max_correction_rounds,
            landing_mode=landing_mode,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "review_separation": self.review_separation.value,
            "maintainer_approval": self.maintainer_approval,
            "validations": [validation.to_dict() for validation in self.validations],
            "max_correction_rounds": self.max_correction_rounds,
            "landing_mode": self.landing_mode.value,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


IMPLEMENTATION_TEMPLATE = {
    "schema_version": WORKFLOW_TEMPLATE_SCHEMA_VERSION,
    "key": IMPLEMENTATION_TEMPLATE_KEY,
    "version": 1,
    "roles": [
        {"name": "producer", "workspace_mode": "copy_on_write"},
        {"name": "reviewer", "workspace_mode": "read_only"},
    ],
    "stages": ["build", "review", "maintainer", "landing", "completion"],
    "correction_loop": True,
}
IMPLEMENTATION_TEMPLATE_DEFINITION = WorkflowTemplateDefinition.from_mapping(
    IMPLEMENTATION_TEMPLATE
)
IMPLEMENTATION_TEMPLATE_CANONICAL = IMPLEMENTATION_TEMPLATE_DEFINITION.canonical
IMPLEMENTATION_TEMPLATE_REVISION = IMPLEMENTATION_TEMPLATE_DEFINITION.digest
IMPLEMENTATION_TEMPLATE_ID = f"wtd_{IMPLEMENTATION_TEMPLATE_REVISION[:32]}"
BUILT_IN_WORKFLOW_TEMPLATE_DEFINITIONS = {
    IMPLEMENTATION_TEMPLATE_ID: IMPLEMENTATION_TEMPLATE_DEFINITION
}
