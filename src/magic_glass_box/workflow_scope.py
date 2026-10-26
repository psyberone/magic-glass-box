from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .domain import (
    ValidationError,
    WorkLogReviewOutcome,
    canonical_json,
    content_sha256,
    require_string,
)

WORKFLOW_SCOPE_REVIEW_SCHEMA_VERSION = 1
MAX_SCOPE_MUST_RESULTS = 5
MAX_SCOPE_PROBES = 16
MAX_SCOPE_EVIDENCE = 8
MAX_SCOPE_REVIEW_BYTES = 64 * 1024
SCOPE_REVIEW_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")


def _exact_fields(
    value: object, fields: set[str], *, subject: str
) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValidationError(f"{subject} has missing or unknown fields")
    return value


def _text(value: object, *, field: str, max_bytes: int) -> str:
    result = require_string(value, field=field, max_bytes=max_bytes)
    if not result.strip():
        raise ValidationError(f"{field} must not be blank")
    return result


def _key(value: object, *, field: str) -> str:
    if not isinstance(value, str) or SCOPE_REVIEW_KEY_RE.fullmatch(value) is None:
        raise ValidationError(f"{field} is invalid")
    return value


def _evidence(value: object, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_SCOPE_EVIDENCE:
        raise ValidationError(
            f"{field} must contain between 1 and {MAX_SCOPE_EVIDENCE} entries"
        )
    result = tuple(
        _text(item, field=f"{field}[{index}]", max_bytes=2048)
        for index, item in enumerate(value)
    )
    if len(set(result)) != len(result):
        raise ValidationError(f"{field} entries must be unique")
    return result


class ScopeMustOutcome(StrEnum):
    PASSED = "passed"
    FAILED = "failed"


class ScopeProbeFinding(StrEnum):
    ABSENT = "absent"
    PRESENT = "present"


class ScopeProbeClassification(StrEnum):
    CLEAN = "clean"
    RESOLVED = "resolved"
    FOLLOW_UP = "follow_up"
    REGRESSION = "regression"


@dataclass(frozen=True)
class WorkflowMustResult:
    key: str
    outcome: ScopeMustOutcome
    summary: str
    evidence: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowMustResult:
        item = _exact_fields(
            value,
            {"key", "outcome", "summary", "evidence"},
            subject="workflow MUST result",
        )
        try:
            outcome = ScopeMustOutcome(item["outcome"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("workflow MUST outcome is invalid") from exc
        return cls(
            key=_key(item["key"], field="workflow MUST key"),
            outcome=outcome,
            summary=_text(
                item["summary"], field="workflow MUST summary", max_bytes=4096
            ),
            evidence=_evidence(item["evidence"], field="workflow MUST evidence"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "outcome": self.outcome.value,
            "summary": self.summary,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class WorkflowProbeResult:
    key: str
    summary: str
    base_finding: ScopeProbeFinding
    head_finding: ScopeProbeFinding
    evidence: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowProbeResult:
        item = _exact_fields(
            value,
            {"key", "summary", "base_finding", "head_finding", "evidence"},
            subject="workflow PROBE result",
        )
        try:
            base_finding = ScopeProbeFinding(item["base_finding"])
            head_finding = ScopeProbeFinding(item["head_finding"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("workflow PROBE finding state is invalid") from exc
        return cls(
            key=_key(item["key"], field="workflow PROBE key"),
            summary=_text(
                item["summary"], field="workflow PROBE summary", max_bytes=4096
            ),
            base_finding=base_finding,
            head_finding=head_finding,
            evidence=_evidence(item["evidence"], field="workflow PROBE evidence"),
        )

    @property
    def classification(self) -> ScopeProbeClassification:
        if self.base_finding is ScopeProbeFinding.ABSENT:
            if self.head_finding is ScopeProbeFinding.PRESENT:
                return ScopeProbeClassification.REGRESSION
            return ScopeProbeClassification.CLEAN
        if self.head_finding is ScopeProbeFinding.PRESENT:
            return ScopeProbeClassification.FOLLOW_UP
        return ScopeProbeClassification.RESOLVED

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "summary": self.summary,
            "base_finding": self.base_finding.value,
            "head_finding": self.head_finding.value,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class WorkflowScopeReview:
    schema_version: int
    must_results: tuple[WorkflowMustResult, ...]
    probes: tuple[WorkflowProbeResult, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowScopeReview:
        item = _exact_fields(
            value,
            {"schema_version", "must_results", "probes"},
            subject="workflow scope review",
        )
        schema_version = item["schema_version"]
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version != WORKFLOW_SCOPE_REVIEW_SCHEMA_VERSION
        ):
            raise ValidationError(
                f"schema_version must be {WORKFLOW_SCOPE_REVIEW_SCHEMA_VERSION}"
            )
        raw_must_results = item["must_results"]
        if (
            not isinstance(raw_must_results, list)
            or not 1 <= len(raw_must_results) <= MAX_SCOPE_MUST_RESULTS
        ):
            raise ValidationError(
                "must_results must contain between 1 and "
                f"{MAX_SCOPE_MUST_RESULTS} entries"
            )
        raw_probes = item["probes"]
        if not isinstance(raw_probes, list) or len(raw_probes) > MAX_SCOPE_PROBES:
            raise ValidationError(
                f"probes must contain at most {MAX_SCOPE_PROBES} entries"
            )
        must_results = tuple(
            WorkflowMustResult.from_mapping(result) for result in raw_must_results
        )
        probes = tuple(WorkflowProbeResult.from_mapping(probe) for probe in raw_probes)
        keys = [result.key for result in must_results] + [probe.key for probe in probes]
        if len(set(keys)) != len(keys):
            raise ValidationError("workflow scope review keys must be unique")
        result = cls(
            schema_version=schema_version,
            must_results=must_results,
            probes=probes,
        )
        if len(result.canonical.encode("utf-8")) > MAX_SCOPE_REVIEW_BYTES:
            raise ValidationError(
                f"workflow scope review exceeds {MAX_SCOPE_REVIEW_BYTES} UTF-8 bytes"
            )
        return result

    @property
    def failed_must_keys(self) -> tuple[str, ...]:
        return tuple(
            result.key
            for result in self.must_results
            if result.outcome is ScopeMustOutcome.FAILED
        )

    @property
    def regression_probe_keys(self) -> tuple[str, ...]:
        return tuple(
            probe.key
            for probe in self.probes
            if probe.classification is ScopeProbeClassification.REGRESSION
        )

    @property
    def follow_up_probe_keys(self) -> tuple[str, ...]:
        return tuple(
            probe.key
            for probe in self.probes
            if probe.classification is ScopeProbeClassification.FOLLOW_UP
        )

    @property
    def blocking(self) -> bool:
        return bool(self.failed_must_keys or self.regression_probe_keys)

    @property
    def expected_review_outcome(self) -> WorkLogReviewOutcome:
        return (
            WorkLogReviewOutcome.CHANGES_REQUESTED
            if self.blocking
            else WorkLogReviewOutcome.APPROVED
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "must_results": [result.to_dict() for result in self.must_results],
            "probes": [probe.to_dict() for probe in self.probes],
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)
