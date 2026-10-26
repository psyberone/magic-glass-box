from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any

from .domain import ValidationError, canonical_json, content_sha256, require_string

ADVISORY_SCHEMA_VERSION = 1
MAX_ADVISORY_FINDINGS = 32
MAX_ADVISORY_CHALLENGES = 32
MAX_ADVISORY_REPORT_ITEMS = 64
MAX_ADVISORY_ARTIFACT_BYTES = 64 * 1024
MAX_ADVISORY_EVIDENCE = 8
ADVISORY_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")


def _exact_fields(
    value: object, fields: set[str], *, subject: str
) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValidationError(f"{subject} has missing or unknown fields")
    return value


def _schema_version(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value != ADVISORY_SCHEMA_VERSION
    ):
        raise ValidationError(f"schema_version must be {ADVISORY_SCHEMA_VERSION}")
    return ADVISORY_SCHEMA_VERSION


def _integer(value: object, *, field: str, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= maximum
    ):
        raise ValidationError(f"{field} must be between 1 and {maximum}")
    return value


def _text(value: object, *, field: str, max_bytes: int) -> str:
    result = require_string(value, field=field, max_bytes=max_bytes)
    if not result.strip():
        raise ValidationError(f"{field} must not be blank")
    return result


def _strings(
    value: object,
    *,
    field: str,
    maximum: int,
    item_bytes: int,
) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > maximum:
        raise ValidationError(f"{field} must contain at most {maximum} entries")
    result = tuple(
        _text(item, field=f"{field}[{index}]", max_bytes=item_bytes)
        for index, item in enumerate(value)
    )
    if len(set(result)) != len(result):
        raise ValidationError(f"{field} entries must be unique")
    return result


def _key(value: object, *, field: str) -> str:
    if not isinstance(value, str) or ADVISORY_KEY_RE.fullmatch(value) is None:
        raise ValidationError(f"{field} is invalid")
    return value


def _path(value: object, *, field: str) -> str:
    result = _text(value, field=field, max_bytes=1024)
    parsed = PurePosixPath(result)
    if (
        result.startswith("/")
        or "\\" in result
        or parsed.as_posix() != result
        or parsed.as_posix() == "."
        or any(part in {".", ".."} for part in parsed.parts)
    ):
        raise ValidationError(f"{field} must be a safe relative POSIX path")
    return result


def _bounded_artifact(record: object, *, subject: str) -> None:
    canonical = record.canonical  # type: ignore[attr-defined]
    if len(canonical.encode("utf-8")) > MAX_ADVISORY_ARTIFACT_BYTES:
        raise ValidationError(
            f"{subject} exceeds {MAX_ADVISORY_ARTIFACT_BYTES} UTF-8 bytes"
        )


@dataclass(frozen=True)
class AdvisoryConfigurationSpec:
    schema_version: int
    max_findings_per_reviewer: int
    max_challenges_per_reviewer: int
    max_report_items: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryConfigurationSpec:
        item = _exact_fields(
            value,
            {
                "schema_version",
                "max_findings_per_reviewer",
                "max_challenges_per_reviewer",
                "max_report_items",
            },
            subject="advisory configuration",
        )
        return cls(
            schema_version=_schema_version(item["schema_version"]),
            max_findings_per_reviewer=_integer(
                item["max_findings_per_reviewer"],
                field="max_findings_per_reviewer",
                maximum=MAX_ADVISORY_FINDINGS,
            ),
            max_challenges_per_reviewer=_integer(
                item["max_challenges_per_reviewer"],
                field="max_challenges_per_reviewer",
                maximum=MAX_ADVISORY_CHALLENGES,
            ),
            max_report_items=_integer(
                item["max_report_items"],
                field="max_report_items",
                maximum=MAX_ADVISORY_REPORT_ITEMS,
            ),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "schema_version": self.schema_version,
            "max_findings_per_reviewer": self.max_findings_per_reviewer,
            "max_challenges_per_reviewer": self.max_challenges_per_reviewer,
            "max_report_items": self.max_report_items,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class AdvisoryEvidence:
    path: str
    line_start: int
    line_end: int
    summary: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryEvidence:
        item = _exact_fields(
            value,
            {"path", "line_start", "line_end", "summary"},
            subject="advisory evidence",
        )
        line_start = _integer(
            item["line_start"], field="line_start", maximum=10_000_000
        )
        line_end = _integer(item["line_end"], field="line_end", maximum=10_000_000)
        if line_end < line_start:
            raise ValidationError("line_end must not precede line_start")
        return cls(
            path=_path(item["path"], field="evidence path"),
            line_start=line_start,
            line_end=line_end,
            summary=_text(item["summary"], field="evidence summary", max_bytes=2048),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "line_start": self.line_start,
            "line_end": self.line_end,
            "summary": self.summary,
        }


class AdvisoryRiskLevel(StrEnum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class AdvisoryFindingCategory(StrEnum):
    CORRECTNESS = "correctness"
    SECURITY = "security"
    RELIABILITY = "reliability"
    PERFORMANCE = "performance"
    MAINTAINABILITY = "maintainability"
    TESTING = "testing"
    DOCUMENTATION = "documentation"
    PRODUCT_FIT = "product_fit"


class AdvisorySeverity(StrEnum):
    CRITICAL = "critical"
    MAJOR = "major"
    MINOR = "minor"
    NOTE = "note"


@dataclass(frozen=True)
class AdvisoryFinding:
    key: str
    category: AdvisoryFindingCategory
    severity: AdvisorySeverity
    summary: str
    rationale: str
    evidence: tuple[AdvisoryEvidence, ...]
    suggested_check: str | None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryFinding:
        item = _exact_fields(
            value,
            {
                "key",
                "category",
                "severity",
                "summary",
                "rationale",
                "evidence",
                "suggested_check",
            },
            subject="advisory finding",
        )
        try:
            category = AdvisoryFindingCategory(item["category"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("advisory finding category is invalid") from exc
        try:
            severity = AdvisorySeverity(item["severity"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("advisory finding severity is invalid") from exc
        raw_evidence = item["evidence"]
        if (
            not isinstance(raw_evidence, list)
            or len(raw_evidence) > MAX_ADVISORY_EVIDENCE
        ):
            raise ValidationError(
                f"finding evidence must contain at most {MAX_ADVISORY_EVIDENCE} entries"
            )
        raw_check = item["suggested_check"]
        suggested_check = (
            None
            if raw_check is None
            else _text(raw_check, field="suggested_check", max_bytes=4096)
        )
        return cls(
            key=_key(item["key"], field="finding key"),
            category=category,
            severity=severity,
            summary=_text(item["summary"], field="finding summary", max_bytes=4096),
            rationale=_text(
                item["rationale"], field="finding rationale", max_bytes=8192
            ),
            evidence=tuple(
                AdvisoryEvidence.from_mapping(entry) for entry in raw_evidence
            ),
            suggested_check=suggested_check,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "category": self.category.value,
            "severity": self.severity.value,
            "summary": self.summary,
            "rationale": self.rationale,
            "evidence": [entry.to_dict() for entry in self.evidence],
            "suggested_check": self.suggested_check,
        }


@dataclass(frozen=True)
class AdvisoryBlindReview:
    schema_version: int
    summary: str
    risk_level: AdvisoryRiskLevel
    findings: tuple[AdvisoryFinding, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryBlindReview:
        item = _exact_fields(
            value,
            {"schema_version", "summary", "risk_level", "findings"},
            subject="advisory blind review",
        )
        try:
            risk_level = AdvisoryRiskLevel(item["risk_level"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("advisory risk_level is invalid") from exc
        raw_findings = item["findings"]
        if (
            not isinstance(raw_findings, list)
            or len(raw_findings) > MAX_ADVISORY_FINDINGS
        ):
            raise ValidationError(
                f"findings must contain at most {MAX_ADVISORY_FINDINGS} entries"
            )
        findings = tuple(
            AdvisoryFinding.from_mapping(finding) for finding in raw_findings
        )
        if len({finding.key for finding in findings}) != len(findings):
            raise ValidationError("finding keys must be unique")
        if (risk_level is AdvisoryRiskLevel.NONE) != (not findings):
            raise ValidationError("risk_level none must correspond to no findings")
        result = cls(
            schema_version=_schema_version(item["schema_version"]),
            summary=_text(item["summary"], field="review summary", max_bytes=4096),
            risk_level=risk_level,
            findings=findings,
        )
        _bounded_artifact(result, subject="advisory blind review")
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "summary": self.summary,
            "risk_level": self.risk_level.value,
            "findings": [finding.to_dict() for finding in self.findings],
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


class AdvisoryReviewerRole(StrEnum):
    REVIEWER_ONE = "reviewer_one"
    REVIEWER_TWO = "reviewer_two"
    REVIEWER_THREE = "reviewer_three"


class AdvisoryChallengeStance(StrEnum):
    SUPPORT = "support"
    REFINE = "refine"
    DISPUTE = "dispute"


@dataclass(frozen=True)
class AdvisoryChallenge:
    key: str
    target_role: AdvisoryReviewerRole
    target_finding_key: str
    stance: AdvisoryChallengeStance
    summary: str
    evidence: tuple[AdvisoryEvidence, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryChallenge:
        item = _exact_fields(
            value,
            {
                "key",
                "target_role",
                "target_finding_key",
                "stance",
                "summary",
                "evidence",
            },
            subject="advisory challenge",
        )
        try:
            target_role = AdvisoryReviewerRole(item["target_role"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("challenge target_role is invalid") from exc
        try:
            stance = AdvisoryChallengeStance(item["stance"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("challenge stance is invalid") from exc
        raw_evidence = item["evidence"]
        if (
            not isinstance(raw_evidence, list)
            or len(raw_evidence) > MAX_ADVISORY_EVIDENCE
        ):
            raise ValidationError(
                "challenge evidence must contain at most "
                f"{MAX_ADVISORY_EVIDENCE} entries"
            )
        return cls(
            key=_key(item["key"], field="challenge key"),
            target_role=target_role,
            target_finding_key=_key(
                item["target_finding_key"], field="target finding key"
            ),
            stance=stance,
            summary=_text(item["summary"], field="challenge summary", max_bytes=4096),
            evidence=tuple(
                AdvisoryEvidence.from_mapping(entry) for entry in raw_evidence
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "target_role": self.target_role.value,
            "target_finding_key": self.target_finding_key,
            "stance": self.stance.value,
            "summary": self.summary,
            "evidence": [entry.to_dict() for entry in self.evidence],
        }


@dataclass(frozen=True)
class AdvisoryChallengeReview:
    schema_version: int
    summary: str
    challenges: tuple[AdvisoryChallenge, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryChallengeReview:
        item = _exact_fields(
            value,
            {"schema_version", "summary", "challenges"},
            subject="advisory challenge review",
        )
        raw_challenges = item["challenges"]
        if (
            not isinstance(raw_challenges, list)
            or len(raw_challenges) > MAX_ADVISORY_CHALLENGES
        ):
            raise ValidationError(
                f"challenges must contain at most {MAX_ADVISORY_CHALLENGES} entries"
            )
        challenges = tuple(
            AdvisoryChallenge.from_mapping(challenge) for challenge in raw_challenges
        )
        if len({challenge.key for challenge in challenges}) != len(challenges):
            raise ValidationError("challenge keys must be unique")
        targets = {
            (challenge.target_role.value, challenge.target_finding_key)
            for challenge in challenges
        }
        if len(targets) != len(challenges):
            raise ValidationError("challenge targets must be unique")
        result = cls(
            schema_version=_schema_version(item["schema_version"]),
            summary=_text(
                item["summary"], field="challenge review summary", max_bytes=4096
            ),
            challenges=challenges,
        )
        _bounded_artifact(result, subject="advisory challenge review")
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "summary": self.summary,
            "challenges": [challenge.to_dict() for challenge in self.challenges],
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class AdvisoryFindingRef:
    reviewer_role: AdvisoryReviewerRole
    finding_key: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryFindingRef:
        item = _exact_fields(
            value,
            {"reviewer_role", "finding_key"},
            subject="advisory finding ref",
        )
        try:
            role = AdvisoryReviewerRole(item["reviewer_role"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("finding ref reviewer_role is invalid") from exc
        return cls(
            reviewer_role=role,
            finding_key=_key(item["finding_key"], field="finding ref key"),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "reviewer_role": self.reviewer_role.value,
            "finding_key": self.finding_key,
        }


@dataclass(frozen=True)
class AdvisoryChallengeRef:
    reviewer_role: AdvisoryReviewerRole
    challenge_key: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryChallengeRef:
        item = _exact_fields(
            value,
            {"reviewer_role", "challenge_key"},
            subject="advisory challenge ref",
        )
        try:
            role = AdvisoryReviewerRole(item["reviewer_role"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("challenge ref reviewer_role is invalid") from exc
        return cls(
            reviewer_role=role,
            challenge_key=_key(item["challenge_key"], field="challenge ref key"),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "reviewer_role": self.reviewer_role.value,
            "challenge_key": self.challenge_key,
        }


class AdvisoryItemClassification(StrEnum):
    CORROBORATED = "corroborated"
    CONTESTED = "contested"
    SINGLETON = "singleton"
    DISMISSED = "dismissed"


@dataclass(frozen=True)
class AdvisoryReportItem:
    key: str
    classification: AdvisoryItemClassification
    severity: AdvisorySeverity
    summary: str
    finding_refs: tuple[AdvisoryFindingRef, ...]
    challenge_refs: tuple[AdvisoryChallengeRef, ...]
    recommended_action: str | None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryReportItem:
        item = _exact_fields(
            value,
            {
                "key",
                "classification",
                "severity",
                "summary",
                "finding_refs",
                "challenge_refs",
                "recommended_action",
            },
            subject="advisory report item",
        )
        try:
            classification = AdvisoryItemClassification(item["classification"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("report item classification is invalid") from exc
        try:
            severity = AdvisorySeverity(item["severity"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("report item severity is invalid") from exc
        raw_findings = item["finding_refs"]
        if not isinstance(raw_findings, list) or not 1 <= len(raw_findings) <= 3:
            raise ValidationError("finding_refs must contain between 1 and 3 entries")
        finding_refs = tuple(
            AdvisoryFindingRef.from_mapping(reference) for reference in raw_findings
        )
        finding_identities = {
            (reference.reviewer_role.value, reference.finding_key)
            for reference in finding_refs
        }
        if len(finding_identities) != len(finding_refs):
            raise ValidationError("finding_refs entries must be unique")
        raw_challenges = item["challenge_refs"]
        if not isinstance(raw_challenges, list) or len(raw_challenges) > 6:
            raise ValidationError("challenge_refs must contain at most 6 entries")
        challenge_refs = tuple(
            AdvisoryChallengeRef.from_mapping(reference) for reference in raw_challenges
        )
        challenge_identities = {
            (reference.reviewer_role.value, reference.challenge_key)
            for reference in challenge_refs
        }
        if len(challenge_identities) != len(challenge_refs):
            raise ValidationError("challenge_refs entries must be unique")
        raw_action = item["recommended_action"]
        recommended_action = (
            None
            if raw_action is None
            else _text(raw_action, field="recommended_action", max_bytes=4096)
        )
        return cls(
            key=_key(item["key"], field="report item key"),
            classification=classification,
            severity=severity,
            summary=_text(item["summary"], field="report item summary", max_bytes=4096),
            finding_refs=finding_refs,
            challenge_refs=challenge_refs,
            recommended_action=recommended_action,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "classification": self.classification.value,
            "severity": self.severity.value,
            "summary": self.summary,
            "finding_refs": [reference.to_dict() for reference in self.finding_refs],
            "challenge_refs": [
                reference.to_dict() for reference in self.challenge_refs
            ],
            "recommended_action": self.recommended_action,
        }


class AdvisoryAuthority(StrEnum):
    ADVISORY_ONLY = "advisory_only"


class AdvisoryAttention(StrEnum):
    NONE = "none"
    ROUTINE = "routine"
    URGENT = "urgent"


@dataclass(frozen=True)
class AdvisoryReport:
    schema_version: int
    authority: AdvisoryAuthority
    attention: AdvisoryAttention
    summary: str
    items: tuple[AdvisoryReportItem, ...]
    follow_up_checks: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AdvisoryReport:
        item = _exact_fields(
            value,
            {
                "schema_version",
                "authority",
                "attention",
                "summary",
                "items",
                "follow_up_checks",
            },
            subject="advisory report",
        )
        try:
            authority = AdvisoryAuthority(item["authority"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("advisory report authority is invalid") from exc
        try:
            attention = AdvisoryAttention(item["attention"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("advisory report attention is invalid") from exc
        raw_items = item["items"]
        if (
            not isinstance(raw_items, list)
            or len(raw_items) > MAX_ADVISORY_REPORT_ITEMS
        ):
            raise ValidationError(
                f"items must contain at most {MAX_ADVISORY_REPORT_ITEMS} entries"
            )
        items = tuple(AdvisoryReportItem.from_mapping(entry) for entry in raw_items)
        if len({entry.key for entry in items}) != len(items):
            raise ValidationError("report item keys must be unique")
        if (attention is AdvisoryAttention.NONE) != (not items):
            raise ValidationError("attention none must correspond to no report items")
        result = cls(
            schema_version=_schema_version(item["schema_version"]),
            authority=authority,
            attention=attention,
            summary=_text(item["summary"], field="report summary", max_bytes=4096),
            items=items,
            follow_up_checks=_strings(
                item["follow_up_checks"],
                field="follow_up_checks",
                maximum=32,
                item_bytes=2048,
            ),
        )
        _bounded_artifact(result, subject="advisory report")
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "authority": self.authority.value,
            "attention": self.attention.value,
            "summary": self.summary,
            "items": [item.to_dict() for item in self.items],
            "follow_up_checks": list(self.follow_up_checks),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)
