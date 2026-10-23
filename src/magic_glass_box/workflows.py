from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .domain import ValidationError, canonical_json, content_sha256
from .gates import GateError, ReviewSeparation, ValidationGate

WORKFLOW_CONFIGURATION_SCHEMA_VERSION = 1
WORKFLOW_TEMPLATE_SCHEMA_VERSION = 1
MAX_CORRECTION_ROUNDS = 8
IMPLEMENTATION_TEMPLATE_KEY = "implementation-v1"


class WorkflowLandingMode(StrEnum):
    MANUAL = "manual"
    PROVIDER = "provider"


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
IMPLEMENTATION_TEMPLATE_CANONICAL = canonical_json(IMPLEMENTATION_TEMPLATE)
IMPLEMENTATION_TEMPLATE_REVISION = content_sha256(IMPLEMENTATION_TEMPLATE_CANONICAL)
IMPLEMENTATION_TEMPLATE_ID = f"wtd_{IMPLEMENTATION_TEMPLATE_REVISION[:32]}"
