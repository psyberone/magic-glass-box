from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
from typing import Any

from .domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    parse_rfc3339_timestamp,
    require_string,
)
from .provider_effects import (
    OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION,
    ChangeOpenReadback,
    LandingReadback,
    WorkflowProviderIntent,
)

WORKFLOW_CLOSEOUT_SCHEMA_VERSION = 1
MAX_CLOSEOUT_OBLIGATIONS = 32

_OBLIGATION_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")

DELIVERY_CLOSEOUT_FACTS = {
    "primary_landing": "conformant",
    "proposal_metadata": "conformant",
    "proposal_closure": "closed",
    "secondary_effects": "none",
    "replication": "not_required",
    "tracker": "not_required",
}


def _exact_fields(
    value: Mapping[str, Any], *, expected: set[str], subject: str
) -> None:
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown:
        raise ValidationError(
            f"{subject} contains unknown fields: " + ", ".join(sorted(unknown))
        )
    if missing:
        raise ValidationError(
            f"{subject} is missing fields: " + ", ".join(sorted(missing))
        )


def _schema_version(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value != WORKFLOW_CLOSEOUT_SCHEMA_VERSION
    ):
        raise ValidationError("workflow closeout schema_version must be 1")
    return value


def validate_delivery_closeout(
    *,
    intent: WorkflowProviderIntent,
    change_readback: ChangeOpenReadback,
    landing_readback: LandingReadback,
    remote_identity: str,
    closed_references: tuple[str, ...],
) -> dict[str, str]:
    """Prove proposal metadata and closure separately from landing mechanics."""

    expected_metadata = (
        intent.base_ref,
        intent.head_ref,
        intent.title,
        intent.body,
        intent.draft,
    )
    if (
        change_readback.state != "open"
        or (
            change_readback.base_ref,
            change_readback.head_ref,
            change_readback.title,
            change_readback.body,
            change_readback.draft,
        )
        != expected_metadata
    ):
        raise ValidationError(
            "workflow closeout proposal metadata does not match its intent"
        )
    if (
        change_readback.schema_version
        != OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION
        or landing_readback.schema_version
        != OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION
        or change_readback.head_revision != landing_readback.candidate_revision
        or change_readback.change_id != landing_readback.change_id
        or landing_readback.base_ref != intent.base_ref
        or landing_readback.head_ref != intent.head_ref
        or landing_readback.title != intent.title
        or landing_readback.body != intent.body
        or landing_readback.draft is not intent.draft
        or landing_readback.state != "closed"
        or landing_readback.first_parent_status != "contains"
    ):
        raise ValidationError(
            "workflow closeout provider readback differs from its intent"
        )
    if (
        intent.secondary_effects
        or change_readback.secondary_effects
        or change_readback.closure_references
        or landing_readback.secondary_effects
        or landing_readback.closure_references
    ):
        raise ValidationError("workflow closeout has undeclared secondary effects")
    if closed_references != (remote_identity,):
        raise ValidationError(
            "workflow closeout provider closure does not match its landing"
        )
    return dict(DELIVERY_CLOSEOUT_FACTS)


@dataclass(frozen=True)
class WorkflowCloseoutObligation:
    kind: str
    obligation: str
    deadline: str
    reason: str

    @classmethod
    def from_mapping(cls, value: object) -> WorkflowCloseoutObligation:
        if not isinstance(value, dict):
            raise ValidationError("workflow closeout obligation must be an object")
        _exact_fields(
            value,
            expected={"kind", "obligation", "deadline", "reason"},
            subject="workflow closeout obligation",
        )
        if value["kind"] != "deferred_validation":
            raise ValidationError(
                "workflow closeout obligation kind must be deferred_validation"
            )
        obligation = value["obligation"]
        if (
            not isinstance(obligation, str)
            or _OBLIGATION_RE.fullmatch(obligation) is None
        ):
            raise ValidationError("workflow closeout obligation name is invalid")
        parsed_deadline = parse_rfc3339_timestamp(
            value["deadline"], field="workflow closeout obligation deadline"
        )
        deadline = (
            parsed_deadline.astimezone(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )
        reason = require_string(
            value["reason"],
            field="workflow closeout obligation reason",
            max_bytes=4096,
        )
        if not reason.strip():
            raise ValidationError(
                "workflow closeout obligation reason must not be blank"
            )
        return cls(
            kind="deferred_validation",
            obligation=obligation,
            deadline=deadline,
            reason=reason,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "obligation": self.obligation,
            "deadline": self.deadline,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class WorkflowCloseoutDeclaration:
    schema_version: int
    obligations: tuple[WorkflowCloseoutObligation, ...]

    @classmethod
    def from_mapping(cls, value: object) -> WorkflowCloseoutDeclaration:
        if not isinstance(value, dict):
            raise ValidationError("workflow closeout declaration must be an object")
        _exact_fields(
            value,
            expected={"schema_version", "obligations"},
            subject="workflow closeout declaration",
        )
        raw_obligations = value["obligations"]
        if (
            not isinstance(raw_obligations, list)
            or len(raw_obligations) > MAX_CLOSEOUT_OBLIGATIONS
        ):
            raise ValidationError(
                "workflow closeout obligations must contain at most 32 entries"
            )
        obligations = tuple(
            sorted(
                (
                    WorkflowCloseoutObligation.from_mapping(item)
                    for item in raw_obligations
                ),
                key=lambda item: item.obligation,
            )
        )
        if len({item.obligation for item in obligations}) != len(obligations):
            raise ValidationError("workflow closeout obligations must be unique")
        return cls(
            schema_version=_schema_version(value["schema_version"]),
            obligations=obligations,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "obligations": [item.to_dict() for item in self.obligations],
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)
