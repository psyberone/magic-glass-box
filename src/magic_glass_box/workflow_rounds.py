from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .domain import ValidationError

WORKFLOW_ROUND_CAUSE_SCHEMA_VERSION = 1
MAX_FINDING_CLASSES = 16

FINDING_CLASS_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
ID_PATTERNS = {
    "policy_id": re.compile(r"^gpl_[0-9a-f]{32}$"),
    "reviewer_run_id": re.compile(r"^run_[0-9a-f]{32}$"),
    "reviewer_attempt_id": re.compile(r"^att_[0-9a-f]{32}$"),
    "review_evidence_id": re.compile(r"^gev_[0-9a-f]{32}$"),
    "review_entry_id": re.compile(r"^wle_[0-9a-f]{32}$"),
    "scope_review_id": re.compile(r"^wsr_[0-9a-f]{32}$"),
    "maintainer_review_id": re.compile(r"^mrv_[0-9a-f]{32}$"),
}
CAUSE_KINDS = frozenset({"reviewer_finding", "maintainer_request", "gate_error"})


def _nullable_id(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or ID_PATTERNS[field].fullmatch(value) is None:
        raise ValidationError(f"workflow round cause {field} is invalid")
    return value


@dataclass(frozen=True)
class WorkflowRoundCause:
    """The immutable, structured reason for requesting one correction round."""

    schema_version: int
    kind: str
    finding_classes: tuple[str, ...]
    policy_id: str
    reviewer_run_id: str | None
    reviewer_attempt_id: str | None
    review_evidence_id: str | None
    review_entry_id: str | None
    scope_review_id: str | None
    maintainer_review_id: str | None
    gate_name: str | None

    def __post_init__(self) -> None:
        if (
            isinstance(self.schema_version, bool)
            or self.schema_version != WORKFLOW_ROUND_CAUSE_SCHEMA_VERSION
        ):
            raise ValidationError(
                "workflow round cause schema_version must be "
                f"{WORKFLOW_ROUND_CAUSE_SCHEMA_VERSION}"
            )
        if not isinstance(self.kind, str) or self.kind not in CAUSE_KINDS:
            raise ValidationError("workflow round cause kind is invalid")
        if (
            not isinstance(self.finding_classes, tuple)
            or not 1 <= len(self.finding_classes) <= MAX_FINDING_CLASSES
            or any(
                not isinstance(item, str) or FINDING_CLASS_RE.fullmatch(item) is None
                for item in self.finding_classes
            )
            or tuple(sorted(set(self.finding_classes))) != self.finding_classes
        ):
            raise ValidationError(
                "workflow round cause finding_classes must be a canonical "
                "non-empty bounded set"
            )
        if (
            not isinstance(self.policy_id, str)
            or ID_PATTERNS["policy_id"].fullmatch(self.policy_id) is None
        ):
            raise ValidationError("workflow round cause policy_id is invalid")
        for field in (
            "reviewer_run_id",
            "reviewer_attempt_id",
            "review_evidence_id",
            "review_entry_id",
            "scope_review_id",
            "maintainer_review_id",
        ):
            _nullable_id(getattr(self, field), field=field)
        if self.gate_name is not None and (
            not isinstance(self.gate_name, str)
            or FINDING_CLASS_RE.fullmatch(self.gate_name) is None
        ):
            raise ValidationError("workflow round cause gate_name is invalid")

        reviewer_fields = (
            self.reviewer_run_id,
            self.reviewer_attempt_id,
            self.review_evidence_id,
            self.review_entry_id,
        )
        if self.kind == "reviewer_finding":
            if any(value is None for value in reviewer_fields):
                raise ValidationError(
                    "reviewer finding cause requires exact reviewer provenance"
                )
            if self.maintainer_review_id is not None or self.gate_name is not None:
                raise ValidationError("reviewer finding cause mixes source authority")
        elif self.kind == "maintainer_request":
            if (
                self.maintainer_review_id is None
                or any(value is not None for value in reviewer_fields)
                or self.scope_review_id is not None
                or self.gate_name is not None
            ):
                raise ValidationError(
                    "maintainer request cause requires exact maintainer provenance"
                )
        elif (
            self.gate_name is None
            or any(value is not None for value in reviewer_fields)
            or self.scope_review_id is not None
            or self.maintainer_review_id is not None
        ):
            raise ValidationError("gate error cause requires one exact gate name")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowRoundCause:
        if not isinstance(value, dict):
            raise ValidationError("workflow round cause must be an object")
        fields = {
            "schema_version",
            "kind",
            "finding_classes",
            "policy_id",
            "reviewer_run_id",
            "reviewer_attempt_id",
            "review_evidence_id",
            "review_entry_id",
            "scope_review_id",
            "maintainer_review_id",
            "gate_name",
        }
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown:
            raise ValidationError(
                "workflow round cause contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        if missing:
            raise ValidationError(
                "workflow round cause is missing fields: " + ", ".join(sorted(missing))
            )
        raw_classes = value["finding_classes"]
        if not isinstance(raw_classes, list):
            raise ValidationError("workflow round cause finding_classes must be a list")
        return cls(
            schema_version=value["schema_version"],
            kind=value["kind"],
            finding_classes=tuple(raw_classes),
            policy_id=value["policy_id"],
            reviewer_run_id=_nullable_id(
                value["reviewer_run_id"], field="reviewer_run_id"
            ),
            reviewer_attempt_id=_nullable_id(
                value["reviewer_attempt_id"], field="reviewer_attempt_id"
            ),
            review_evidence_id=_nullable_id(
                value["review_evidence_id"], field="review_evidence_id"
            ),
            review_entry_id=_nullable_id(
                value["review_entry_id"], field="review_entry_id"
            ),
            scope_review_id=_nullable_id(
                value["scope_review_id"], field="scope_review_id"
            ),
            maintainer_review_id=_nullable_id(
                value["maintainer_review_id"], field="maintainer_review_id"
            ),
            gate_name=value["gate_name"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind,
            "finding_classes": list(self.finding_classes),
            "policy_id": self.policy_id,
            "reviewer_run_id": self.reviewer_run_id,
            "reviewer_attempt_id": self.reviewer_attempt_id,
            "review_evidence_id": self.review_evidence_id,
            "review_entry_id": self.review_entry_id,
            "scope_review_id": self.scope_review_id,
            "maintainer_review_id": self.maintainer_review_id,
            "gate_name": self.gate_name,
        }
