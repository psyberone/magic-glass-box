from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any

from .domain import ValidationError, canonical_json, content_sha256, require_string

DISCOVERY_SCHEMA_VERSION = 1
MAX_DISCOVERY_FINDINGS = 32
MAX_DISCOVERY_MUST_ITEMS = 5
MAX_DISCOVERY_FOLLOW_UP_PROPOSALS = 16
MAX_DISCOVERY_TRACE_BYTES = 64 * 1024
MAX_DISCOVERY_RULING_BYTES = 64 * 1024
DISCOVERY_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
INTERACTION_ID_RE = re.compile(r"^int_[0-9a-f]{32}$")


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
        or value != DISCOVERY_SCHEMA_VERSION
    ):
        raise ValidationError(f"schema_version must be {DISCOVERY_SCHEMA_VERSION}")
    return DISCOVERY_SCHEMA_VERSION


def _integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return value


def _text(value: object, *, field: str, max_bytes: int) -> str:
    text = require_string(value, field=field, max_bytes=max_bytes)
    if not text.strip():
        raise ValidationError(f"{field} must not be blank")
    return text


def _strings(
    value: object,
    *,
    field: str,
    maximum: int,
    item_bytes: int,
    minimum: int = 0,
) -> tuple[str, ...]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise ValidationError(
            f"{field} must contain between {minimum} and {maximum} entries"
        )
    result = tuple(
        _text(item, field=f"{field}[{index}]", max_bytes=item_bytes)
        for index, item in enumerate(value)
    )
    if len(set(result)) != len(result):
        raise ValidationError(f"{field} entries must be unique")
    return result


def _key(value: object, *, field: str) -> str:
    if not isinstance(value, str) or DISCOVERY_KEY_RE.fullmatch(value) is None:
        raise ValidationError(f"{field} is invalid")
    return value


def _path(value: object, *, field: str) -> str:
    text = _text(value, field=field, max_bytes=1024)
    parsed = PurePosixPath(text)
    if (
        text.startswith("/")
        or "\\" in text
        or parsed.as_posix() != text
        or parsed.as_posix() == "."
        or any(part in {".", ".."} for part in parsed.parts)
    ):
        raise ValidationError(f"{field} must be a safe relative POSIX path")
    return text


@dataclass(frozen=True)
class DiscoveryConfigurationSpec:
    schema_version: int
    max_findings: int
    max_must_items: int
    max_follow_up_proposals: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DiscoveryConfigurationSpec:
        item = _exact_fields(
            value,
            {
                "schema_version",
                "max_findings",
                "max_must_items",
                "max_follow_up_proposals",
            },
            subject="discovery configuration",
        )
        return cls(
            schema_version=_schema_version(item["schema_version"]),
            max_findings=_integer(
                item["max_findings"],
                field="max_findings",
                minimum=1,
                maximum=MAX_DISCOVERY_FINDINGS,
            ),
            max_must_items=_integer(
                item["max_must_items"],
                field="max_must_items",
                minimum=1,
                maximum=MAX_DISCOVERY_MUST_ITEMS,
            ),
            max_follow_up_proposals=_integer(
                item["max_follow_up_proposals"],
                field="max_follow_up_proposals",
                minimum=0,
                maximum=MAX_DISCOVERY_FOLLOW_UP_PROPOSALS,
            ),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "schema_version": self.schema_version,
            "max_findings": self.max_findings,
            "max_must_items": self.max_must_items,
            "max_follow_up_proposals": self.max_follow_up_proposals,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


class DiscoverySourceKind(StrEnum):
    BRIEF = "brief"
    ACCEPTANCE_CRITERION = "acceptance_criterion"


@dataclass(frozen=True)
class DiscoverySourceRef:
    kind: DiscoverySourceKind
    index: int | None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DiscoverySourceRef:
        item = _exact_fields(value, {"kind", "index"}, subject="discovery source ref")
        try:
            kind = DiscoverySourceKind(item["kind"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("discovery source ref kind is invalid") from exc
        index = item["index"]
        if kind is DiscoverySourceKind.BRIEF:
            if index is not None:
                raise ValidationError("brief source ref index must be null")
        else:
            index = _integer(
                index,
                field="acceptance criterion index",
                minimum=0,
                maximum=127,
            )
        return cls(kind=kind, index=index)

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind.value, "index": self.index}


def _source_refs(value: object, *, field: str) -> tuple[DiscoverySourceRef, ...]:
    if not isinstance(value, list) or not 1 <= len(value) <= 8:
        raise ValidationError(f"{field} must contain between 1 and 8 entries")
    refs = tuple(DiscoverySourceRef.from_mapping(item) for item in value)
    identities = {(ref.kind.value, ref.index) for ref in refs}
    if len(identities) != len(refs):
        raise ValidationError(f"{field} entries must be unique")
    return refs


@dataclass(frozen=True)
class DiscoveryEvidence:
    path: str
    line_start: int
    line_end: int
    summary: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DiscoveryEvidence:
        item = _exact_fields(
            value,
            {"path", "line_start", "line_end", "summary"},
            subject="discovery evidence",
        )
        line_start = _integer(
            item["line_start"], field="line_start", minimum=1, maximum=10_000_000
        )
        line_end = _integer(
            item["line_end"], field="line_end", minimum=1, maximum=10_000_000
        )
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


class DiscoveryFindingClassification(StrEnum):
    REQUIREMENT = "requirement"
    CONSTRAINT = "constraint"
    RISK = "risk"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class DiscoveryFinding:
    key: str
    classification: DiscoveryFindingClassification
    summary: str
    source_refs: tuple[DiscoverySourceRef, ...]
    evidence: tuple[DiscoveryEvidence, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DiscoveryFinding:
        item = _exact_fields(
            value,
            {"key", "classification", "summary", "source_refs", "evidence"},
            subject="discovery finding",
        )
        try:
            classification = DiscoveryFindingClassification(item["classification"])
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                "discovery finding classification is invalid"
            ) from exc
        raw_evidence = item["evidence"]
        if not isinstance(raw_evidence, list) or len(raw_evidence) > 8:
            raise ValidationError("finding evidence must contain at most 8 entries")
        return cls(
            key=_key(item["key"], field="finding key"),
            classification=classification,
            summary=_text(item["summary"], field="finding summary", max_bytes=4096),
            source_refs=_source_refs(item["source_refs"], field="finding source_refs"),
            evidence=tuple(
                DiscoveryEvidence.from_mapping(entry) for entry in raw_evidence
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "classification": self.classification.value,
            "summary": self.summary,
            "source_refs": [ref.to_dict() for ref in self.source_refs],
            "evidence": [item.to_dict() for item in self.evidence],
        }


@dataclass(frozen=True)
class DiscoveryTrace:
    schema_version: int
    summary: str
    findings: tuple[DiscoveryFinding, ...]
    recommended_checks: tuple[str, ...]
    open_questions: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DiscoveryTrace:
        item = _exact_fields(
            value,
            {
                "schema_version",
                "summary",
                "findings",
                "recommended_checks",
                "open_questions",
            },
            subject="discovery trace",
        )
        raw_findings = item["findings"]
        if (
            not isinstance(raw_findings, list)
            or not 1 <= len(raw_findings) <= MAX_DISCOVERY_FINDINGS
        ):
            raise ValidationError(
                f"findings must contain between 1 and {MAX_DISCOVERY_FINDINGS} entries"
            )
        findings = tuple(
            DiscoveryFinding.from_mapping(finding) for finding in raw_findings
        )
        if len({finding.key for finding in findings}) != len(findings):
            raise ValidationError("finding keys must be unique")
        result = cls(
            schema_version=_schema_version(item["schema_version"]),
            summary=_text(item["summary"], field="trace summary", max_bytes=4096),
            findings=findings,
            recommended_checks=_strings(
                item["recommended_checks"],
                field="recommended_checks",
                maximum=16,
                item_bytes=2048,
            ),
            open_questions=_strings(
                item["open_questions"],
                field="open_questions",
                maximum=8,
                item_bytes=2048,
            ),
        )
        if len(result.canonical.encode("utf-8")) > MAX_DISCOVERY_TRACE_BYTES:
            raise ValidationError(
                f"discovery trace exceeds {MAX_DISCOVERY_TRACE_BYTES} UTF-8 bytes"
            )
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "summary": self.summary,
            "findings": [finding.to_dict() for finding in self.findings],
            "recommended_checks": list(self.recommended_checks),
            "open_questions": list(self.open_questions),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class DiscoveryMustItem:
    key: str
    requirement: str
    source_refs: tuple[DiscoverySourceRef, ...]
    finding_keys: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DiscoveryMustItem:
        item = _exact_fields(
            value,
            {"key", "requirement", "source_refs", "finding_keys"},
            subject="discovery MUST item",
        )
        return cls(
            key=_key(item["key"], field="MUST item key"),
            requirement=_text(
                item["requirement"], field="MUST item requirement", max_bytes=4096
            ),
            source_refs=_source_refs(item["source_refs"], field="MUST source_refs"),
            finding_keys=tuple(
                _key(key, field="MUST finding key")
                for key in _strings(
                    item["finding_keys"],
                    field="finding_keys",
                    minimum=1,
                    maximum=8,
                    item_bytes=32,
                )
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "requirement": self.requirement,
            "source_refs": [ref.to_dict() for ref in self.source_refs],
            "finding_keys": list(self.finding_keys),
        }


@dataclass(frozen=True)
class WorkReadyBrief:
    objective: str
    must_items: tuple[DiscoveryMustItem, ...]
    scope_paths: tuple[str, ...]
    verification_checks: tuple[str, ...]
    non_goals: tuple[str, ...]
    follow_up_proposals: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkReadyBrief:
        item = _exact_fields(
            value,
            {
                "objective",
                "must_items",
                "scope_paths",
                "verification_checks",
                "non_goals",
                "follow_up_proposals",
            },
            subject="work-ready brief",
        )
        raw_musts = item["must_items"]
        if (
            not isinstance(raw_musts, list)
            or not 1 <= len(raw_musts) <= MAX_DISCOVERY_MUST_ITEMS
        ):
            raise ValidationError(
                "must_items must contain between 1 and "
                f"{MAX_DISCOVERY_MUST_ITEMS} entries"
            )
        musts = tuple(DiscoveryMustItem.from_mapping(entry) for entry in raw_musts)
        if len({entry.key for entry in musts}) != len(musts):
            raise ValidationError("MUST item keys must be unique")
        raw_paths = item["scope_paths"]
        if not isinstance(raw_paths, list) or len(raw_paths) > 64:
            raise ValidationError("scope_paths must contain at most 64 entries")
        paths = tuple(
            _path(path, field=f"scope_paths[{index}]")
            for index, path in enumerate(raw_paths)
        )
        if len(set(paths)) != len(paths):
            raise ValidationError("scope_paths entries must be unique")
        return cls(
            objective=_text(item["objective"], field="objective", max_bytes=8192),
            must_items=musts,
            scope_paths=paths,
            verification_checks=_strings(
                item["verification_checks"],
                field="verification_checks",
                minimum=1,
                maximum=16,
                item_bytes=2048,
            ),
            non_goals=_strings(
                item["non_goals"],
                field="non_goals",
                maximum=16,
                item_bytes=2048,
            ),
            follow_up_proposals=_strings(
                item["follow_up_proposals"],
                field="follow_up_proposals",
                maximum=MAX_DISCOVERY_FOLLOW_UP_PROPOSALS,
                item_bytes=4096,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "objective": self.objective,
            "must_items": [item.to_dict() for item in self.must_items],
            "scope_paths": list(self.scope_paths),
            "verification_checks": list(self.verification_checks),
            "non_goals": list(self.non_goals),
            "follow_up_proposals": list(self.follow_up_proposals),
        }


class DiscoveryDisposition(StrEnum):
    GO = "GO"
    NO_GO = "NO_GO"
    ASK_HUMAN = "ASK_HUMAN"


@dataclass(frozen=True)
class DiscoveryRuling:
    schema_version: int
    disposition: DiscoveryDisposition
    summary: str
    trace_finding_keys: tuple[str, ...]
    interaction_id: str | None
    work_ready_brief: WorkReadyBrief | None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DiscoveryRuling:
        item = _exact_fields(
            value,
            {
                "schema_version",
                "disposition",
                "summary",
                "trace_finding_keys",
                "interaction_id",
                "work_ready_brief",
            },
            subject="discovery ruling",
        )
        try:
            disposition = DiscoveryDisposition(item["disposition"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("discovery disposition is invalid") from exc
        interaction_id = item["interaction_id"]
        if interaction_id is not None and (
            not isinstance(interaction_id, str)
            or INTERACTION_ID_RE.fullmatch(interaction_id) is None
        ):
            raise ValidationError("discovery interaction_id is invalid")
        raw_brief = item["work_ready_brief"]
        brief = None
        if raw_brief is not None:
            brief = WorkReadyBrief.from_mapping(raw_brief)
        if disposition is DiscoveryDisposition.GO:
            if brief is None:
                raise ValidationError("GO requires a work-ready brief")
        elif brief is not None:
            raise ValidationError("only GO may include a work-ready brief")
        if disposition is DiscoveryDisposition.ASK_HUMAN and interaction_id is None:
            raise ValidationError("ASK_HUMAN requires an interaction_id")
        result = cls(
            schema_version=_schema_version(item["schema_version"]),
            disposition=disposition,
            summary=_text(item["summary"], field="ruling summary", max_bytes=4096),
            trace_finding_keys=tuple(
                _key(key, field="trace finding key")
                for key in _strings(
                    item["trace_finding_keys"],
                    field="trace_finding_keys",
                    minimum=1,
                    maximum=MAX_DISCOVERY_FINDINGS,
                    item_bytes=32,
                )
            ),
            interaction_id=interaction_id,
            work_ready_brief=brief,
        )
        if len(result.canonical.encode("utf-8")) > MAX_DISCOVERY_RULING_BYTES:
            raise ValidationError(
                f"discovery ruling exceeds {MAX_DISCOVERY_RULING_BYTES} UTF-8 bytes"
            )
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "disposition": self.disposition.value,
            "summary": self.summary,
            "trace_finding_keys": list(self.trace_finding_keys),
            "interaction_id": self.interaction_id,
            "work_ready_brief": (
                None
                if self.work_ready_brief is None
                else self.work_ready_brief.to_dict()
            ),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)
