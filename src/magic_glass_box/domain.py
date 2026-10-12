from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit


class ValidationError(ValueError):
    """A control-plane value failed deterministic validation."""


HANDLE_RE = re.compile(r"^[a-z][a-z0-9_-]{2,31}$")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,63}$")
KRAB_PROJECT_KEY_RE = re.compile(r"^[a-z][a-z0-9-]{2,31}$")
KRAB_OBJECT_ID_RE = re.compile(r"^(?:usr|prj|ses|itm|clm)_[0-9a-f]{32}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
WORK_ITEM_SOURCE_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
WORK_ITEM_SCHEMA_VERSION = 1
MAX_WORK_ITEM_CONTENT_BYTES = 256 * 1024
HANDOFF_SCHEMA_VERSION = 1
MAX_HANDOFF_CONTENT_BYTES = 24 * 1024
WORK_MEMORY_SCHEMA_VERSION = 1
MAX_WORK_LOG_CONTENT_BYTES = 24 * 1024
MAX_WORK_CHECKPOINT_CONTENT_BYTES = 24 * 1024
MAX_WORK_ASSIGNMENT_CONTENT_BYTES = 16 * 1024
INTERACTION_ID_RE = re.compile(r"^int_[0-9a-f]{32}$")
SUPERVISION_POLICY_SCHEMA_VERSION = 1
MAX_SUPERVISION_SCHEDULE_ENTRIES = 8
MAX_SUPERVISION_DELAY_SECONDS = 30 * 24 * 60 * 60


def require_string(
    value: object,
    *,
    field: str,
    max_bytes: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{field} must be text")
    if not allow_empty and not value:
        raise ValidationError(f"{field} must not be empty")
    if "\x00" in value:
        raise ValidationError(f"{field} must not contain NUL")
    if len(value.encode("utf-8")) > max_bytes:
        raise ValidationError(f"{field} exceeds {max_bytes} UTF-8 bytes")
    return value


def validate_handle(value: object) -> str:
    if not isinstance(value, str) or HANDLE_RE.fullmatch(value) is None:
        raise ValidationError("handle must match [a-z][a-z0-9_-]{2,31}")
    return value


def validate_name(value: object, *, field: str = "name") -> str:
    if not isinstance(value, str) or NAME_RE.fullmatch(value) is None:
        raise ValidationError(f"{field} must be 1 to 64 restricted display characters")
    return value


def validate_project_key(value: object) -> str:
    if not isinstance(value, str) or KRAB_PROJECT_KEY_RE.fullmatch(value) is None:
        raise ValidationError("project key must match [a-z][a-z0-9-]{2,31}")
    return value


def validate_krab_id(value: object, prefix: str) -> str:
    if (
        not isinstance(value, str)
        or KRAB_OBJECT_ID_RE.fullmatch(value) is None
        or not value.startswith(prefix + "_")
    ):
        raise ValidationError(f"invalid Krab {prefix} identifier")
    return value


def canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValidationError("value must be finite JSON data") from exc


def content_sha256(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _string_tuple(
    value: object, *, field: str, maximum: int, item_bytes: int
) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > maximum:
        raise ValidationError(f"{field} must be a list of at most {maximum} entries")
    result = tuple(
        require_string(entry, field=f"{field} entry", max_bytes=item_bytes)
        for entry in value
    )
    if len(set(result)) != len(result):
        raise ValidationError(f"{field} entries must be unique")
    return result


def _commands(value: object, *, field: str) -> tuple[tuple[str, ...], ...]:
    if not isinstance(value, list) or len(value) > 32:
        raise ValidationError(f"{field} must be a list of at most 32 argv arrays")
    commands: list[tuple[str, ...]] = []
    for index, command in enumerate(value):
        if not isinstance(command, list) or not 1 <= len(command) <= 64:
            raise ValidationError(f"{field}[{index}] must be a non-empty argv array")
        commands.append(
            tuple(
                require_string(
                    argument,
                    field=f"{field}[{index}] argument",
                    max_bytes=1024,
                )
                for argument in command
            )
        )
    return tuple(commands)


def _nonblank_string(value: object, *, field: str, max_bytes: int) -> str:
    result = require_string(value, field=field, max_bytes=max_bytes)
    if not result.strip():
        raise ValidationError(f"{field} must not be blank")
    return result


def _nonblank_tuple(
    value: object, *, field: str, maximum: int, item_bytes: int
) -> tuple[str, ...]:
    result = _string_tuple(
        value,
        field=field,
        maximum=maximum,
        item_bytes=item_bytes,
    )
    if any(not entry.strip() for entry in result):
        raise ValidationError(f"{field} entries must not be blank")
    return result


class WorkItemPriority(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


@dataclass(frozen=True)
class WorkItemContext:
    kind: str
    value: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], *, index: int) -> WorkItemContext:
        field = f"context[{index}]"
        if not isinstance(value, dict):
            raise ValidationError(f"{field} must be an object")
        unknown = set(value) - {"type", "value"}
        if unknown:
            raise ValidationError(
                f"{field} contains unknown fields: " + ", ".join(sorted(unknown))
            )
        kind = value.get("type")
        if kind not in {"note", "workspace_path"}:
            raise ValidationError(f"{field} type is invalid")
        maximum = 8192 if kind == "note" else 1024
        content = _nonblank_string(
            value.get("value"), field=f"{field} value", max_bytes=maximum
        )
        if kind == "workspace_path":
            path = PurePosixPath(content)
            if (
                content.startswith("/")
                or "\\" in content
                or content != path.as_posix()
                or path.as_posix() == "."
                or any(part in {".", ".."} for part in path.parts)
            ):
                raise ValidationError(
                    f"{field} value must be a safe relative POSIX path"
                )
        return cls(kind=kind, value=content)

    def to_dict(self) -> dict[str, str]:
        return {"type": self.kind, "value": self.value}


@dataclass(frozen=True)
class WorkItemEnvelope:
    schema_version: int
    source: str
    external_id: str
    source_updated_at: str
    title: str
    brief: str
    acceptance_criteria: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    context: tuple[WorkItemContext, ...] = ()
    labels: tuple[str, ...] = ()
    priority: WorkItemPriority = WorkItemPriority.NORMAL

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkItemEnvelope:
        if not isinstance(value, dict):
            raise ValidationError("work item must be an object")
        allowed = {
            "schema_version",
            "source",
            "external_id",
            "source_updated_at",
            "title",
            "brief",
            "acceptance_criteria",
            "constraints",
            "context",
            "labels",
            "priority",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValidationError(
                "work item contains unknown fields: " + ", ".join(sorted(unknown))
            )
        schema_version = value.get("schema_version")
        if (
            isinstance(schema_version, bool)
            or schema_version != WORK_ITEM_SCHEMA_VERSION
        ):
            raise ValidationError(f"schema_version must be {WORK_ITEM_SCHEMA_VERSION}")
        source = value.get("source")
        if not isinstance(source, str) or WORK_ITEM_SOURCE_RE.fullmatch(source) is None:
            raise ValidationError("source must match [a-z][a-z0-9_.-]{0,63}")
        external_id = _nonblank_string(
            value.get("external_id"), field="external_id", max_bytes=512
        )
        source_updated_at = cls._timestamp(value.get("source_updated_at"))
        title = _nonblank_string(value.get("title"), field="title", max_bytes=512)
        brief = _nonblank_string(value.get("brief"), field="brief", max_bytes=65536)
        raw_context = value.get("context", [])
        if not isinstance(raw_context, list) or len(raw_context) > 128:
            raise ValidationError("context must be a list of at most 128 entries")
        context = tuple(
            WorkItemContext.from_mapping(entry, index=index)
            for index, entry in enumerate(raw_context)
        )
        if len(set(context)) != len(context):
            raise ValidationError("context entries must be unique")
        try:
            priority = WorkItemPriority(value.get("priority", "normal"))
        except (TypeError, ValueError) as exc:
            raise ValidationError("priority is invalid") from exc
        result = cls(
            schema_version=schema_version,
            source=source,
            external_id=external_id,
            source_updated_at=source_updated_at,
            title=title,
            brief=brief,
            acceptance_criteria=_nonblank_tuple(
                value.get("acceptance_criteria", []),
                field="acceptance_criteria",
                maximum=128,
                item_bytes=4096,
            ),
            constraints=_nonblank_tuple(
                value.get("constraints", []),
                field="constraints",
                maximum=128,
                item_bytes=4096,
            ),
            context=context,
            labels=_nonblank_tuple(
                value.get("labels", []),
                field="labels",
                maximum=64,
                item_bytes=128,
            ),
            priority=priority,
        )
        if len(result.canonical.encode("utf-8")) > MAX_WORK_ITEM_CONTENT_BYTES:
            raise ValidationError(
                f"work item content exceeds {MAX_WORK_ITEM_CONTENT_BYTES} UTF-8 bytes"
            )
        return result

    @staticmethod
    def _timestamp(value: object) -> str:
        text = require_string(value, field="source_updated_at", max_bytes=64)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (ValueError, OverflowError) as exc:
            raise ValidationError(
                "source_updated_at must be a valid RFC 3339 timestamp"
            ) from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValidationError("source_updated_at must include a timezone")
        return (
            parsed.astimezone(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )

    @property
    def content(self) -> dict[str, Any]:
        return {
            "acceptance_criteria": list(self.acceptance_criteria),
            "brief": self.brief,
            "constraints": list(self.constraints),
            "context": [entry.to_dict() for entry in self.context],
            "labels": list(self.labels),
            "priority": self.priority.value,
            "schema_version": self.schema_version,
            "title": self.title,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.content,
            "external_id": self.external_id,
            "source": self.source,
            "source_updated_at": self.source_updated_at,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.content)

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


class WorkLogKind(StrEnum):
    PLAN = "plan"
    PROGRESS = "progress"
    FINDING = "finding"
    DECISION = "decision"
    BLOCKER = "blocker"
    VERIFICATION = "verification"
    REVIEW = "review"
    ASSIGNMENT_RESULT = "assignment_result"
    OUTCOME = "outcome"


class HandoffWorkState(StrEnum):
    IN_PROGRESS = "in_progress"
    WAITING_FOR_INPUT = "waiting_for_input"
    BLOCKED = "blocked"
    READY_FOR_REVIEW = "ready_for_review"


@dataclass(frozen=True)
class HandoffArtifact:
    path: str
    description: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], *, index: int) -> HandoffArtifact:
        field = f"artifacts[{index}]"
        if not isinstance(value, dict):
            raise ValidationError(f"{field} must be an object")
        unknown = set(value) - {"path", "description"}
        if unknown:
            raise ValidationError(
                f"{field} contains unknown fields: " + ", ".join(sorted(unknown))
            )
        missing = {"path", "description"} - set(value)
        if missing:
            raise ValidationError(
                f"{field} is missing fields: " + ", ".join(sorted(missing))
            )
        path_text = _nonblank_string(
            value["path"], field=f"{field} path", max_bytes=1024
        )
        path = PurePosixPath(path_text)
        if (
            path_text.startswith("/")
            or "\\" in path_text
            or path_text != path.as_posix()
            or path.as_posix() == "."
            or any(part in {".", ".."} for part in path.parts)
        ):
            raise ValidationError(f"{field} path must be a safe relative POSIX path")
        return cls(
            path=path_text,
            description=_nonblank_string(
                value["description"],
                field=f"{field} description",
                max_bytes=2048,
            ),
        )

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "description": self.description}


@dataclass(frozen=True)
class HandoffEnvelope:
    """Bounded durable state for a replacement worker, never a transcript."""

    schema_version: int
    work_state: HandoffWorkState
    summary: str
    decisions: tuple[str, ...] = ()
    artifacts: tuple[HandoffArtifact, ...] = ()
    next_steps: tuple[str, ...] = ()
    pending_interaction_ids: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> HandoffEnvelope:
        if not isinstance(value, dict):
            raise ValidationError("handoff must be an object")
        fields = {
            "schema_version",
            "work_state",
            "summary",
            "decisions",
            "artifacts",
            "next_steps",
            "pending_interaction_ids",
        }
        unknown = set(value) - fields
        if unknown:
            raise ValidationError(
                "handoff contains unknown fields: " + ", ".join(sorted(unknown))
            )
        missing = fields - set(value)
        if missing:
            raise ValidationError(
                "handoff is missing fields: " + ", ".join(sorted(missing))
            )
        schema_version = value["schema_version"]
        if isinstance(schema_version, bool) or schema_version != HANDOFF_SCHEMA_VERSION:
            raise ValidationError(f"schema_version must be {HANDOFF_SCHEMA_VERSION}")
        try:
            work_state = HandoffWorkState(value["work_state"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("work_state is invalid") from exc
        summary = _nonblank_string(value["summary"], field="summary", max_bytes=4096)
        decisions = _nonblank_tuple(
            value["decisions"],
            field="decisions",
            maximum=32,
            item_bytes=2048,
        )
        raw_artifacts = value["artifacts"]
        if not isinstance(raw_artifacts, list) or len(raw_artifacts) > 64:
            raise ValidationError("artifacts must be a list of at most 64 entries")
        artifacts = tuple(
            HandoffArtifact.from_mapping(item, index=index)
            for index, item in enumerate(raw_artifacts)
        )
        if len({artifact.path for artifact in artifacts}) != len(artifacts):
            raise ValidationError("artifacts must use unique paths")
        next_steps = _nonblank_tuple(
            value["next_steps"],
            field="next_steps",
            maximum=32,
            item_bytes=2048,
        )
        raw_pending = value["pending_interaction_ids"]
        if not isinstance(raw_pending, list) or len(raw_pending) > 16:
            raise ValidationError(
                "pending_interaction_ids must be a list of at most 16 entries"
            )
        pending = tuple(raw_pending)
        if any(
            not isinstance(interaction_id, str)
            or INTERACTION_ID_RE.fullmatch(interaction_id) is None
            for interaction_id in pending
        ):
            raise ValidationError("pending interaction identifier is invalid")
        if len(set(pending)) != len(pending):
            raise ValidationError("pending_interaction_ids must be unique")
        result = cls(
            schema_version=schema_version,
            work_state=work_state,
            summary=summary,
            decisions=decisions,
            artifacts=artifacts,
            next_steps=next_steps,
            pending_interaction_ids=pending,
        )
        if len(result.canonical.encode("utf-8")) > MAX_HANDOFF_CONTENT_BYTES:
            raise ValidationError(
                f"handoff content exceeds {MAX_HANDOFF_CONTENT_BYTES} UTF-8 bytes"
            )
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "work_state": self.work_state.value,
            "summary": self.summary,
            "decisions": list(self.decisions),
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "next_steps": list(self.next_steps),
            "pending_interaction_ids": list(self.pending_interaction_ids),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class WorkLogEntry:
    """Bounded evidence appended by a run; provenance is supplied by storage."""

    schema_version: int
    kind: WorkLogKind
    summary: str
    details: tuple[str, ...] = ()
    artifacts: tuple[HandoffArtifact, ...] = ()
    related_interaction_ids: tuple[str, ...] = ()
    candidate_revision: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkLogEntry:
        if not isinstance(value, dict):
            raise ValidationError("work-log entry must be an object")
        fields = {
            "schema_version",
            "kind",
            "summary",
            "details",
            "artifacts",
            "related_interaction_ids",
            "candidate_revision",
        }
        unknown = set(value) - fields
        if unknown:
            raise ValidationError(
                "work-log entry contains unknown fields: " + ", ".join(sorted(unknown))
            )
        missing = fields - set(value)
        if missing:
            raise ValidationError(
                "work-log entry is missing fields: " + ", ".join(sorted(missing))
            )
        schema_version = value["schema_version"]
        if (
            isinstance(schema_version, bool)
            or schema_version != WORK_MEMORY_SCHEMA_VERSION
        ):
            raise ValidationError(
                f"schema_version must be {WORK_MEMORY_SCHEMA_VERSION}"
            )
        try:
            kind = WorkLogKind(value["kind"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("work-log kind is invalid") from exc
        raw_artifacts = value["artifacts"]
        if not isinstance(raw_artifacts, list) or len(raw_artifacts) > 32:
            raise ValidationError("artifacts must be a list of at most 32 entries")
        artifacts = tuple(
            HandoffArtifact.from_mapping(item, index=index)
            for index, item in enumerate(raw_artifacts)
        )
        if len({artifact.path for artifact in artifacts}) != len(artifacts):
            raise ValidationError("artifacts must use unique paths")
        raw_interactions = value["related_interaction_ids"]
        if not isinstance(raw_interactions, list) or len(raw_interactions) > 32:
            raise ValidationError(
                "related_interaction_ids must be a list of at most 32 entries"
            )
        interactions = tuple(raw_interactions)
        if any(
            not isinstance(interaction_id, str)
            or INTERACTION_ID_RE.fullmatch(interaction_id) is None
            for interaction_id in interactions
        ):
            raise ValidationError("related interaction identifier is invalid")
        if len(set(interactions)) != len(interactions):
            raise ValidationError("related_interaction_ids must be unique")
        candidate_revision = value["candidate_revision"]
        if candidate_revision is not None:
            candidate_revision = _nonblank_string(
                candidate_revision,
                field="candidate_revision",
                max_bytes=512,
            )
        result = cls(
            schema_version=schema_version,
            kind=kind,
            summary=_nonblank_string(value["summary"], field="summary", max_bytes=4096),
            details=_nonblank_tuple(
                value["details"],
                field="details",
                maximum=32,
                item_bytes=4096,
            ),
            artifacts=artifacts,
            related_interaction_ids=interactions,
            candidate_revision=candidate_revision,
        )
        if len(result.canonical.encode("utf-8")) > MAX_WORK_LOG_CONTENT_BYTES:
            raise ValidationError(
                f"work-log content exceeds {MAX_WORK_LOG_CONTENT_BYTES} UTF-8 bytes"
            )
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "summary": self.summary,
            "details": list(self.details),
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "related_interaction_ids": list(self.related_interaction_ids),
            "candidate_revision": self.candidate_revision,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class WorkMemoryCheckpoint:
    """A compact coordinator view through one exact work-log sequence."""

    schema_version: int
    summary: str
    decisions: tuple[str, ...] = ()
    artifacts: tuple[HandoffArtifact, ...] = ()
    next_steps: tuple[str, ...] = ()
    open_blockers: tuple[str, ...] = ()
    candidate_revision: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkMemoryCheckpoint:
        if not isinstance(value, dict):
            raise ValidationError("work-memory checkpoint must be an object")
        fields = {
            "schema_version",
            "summary",
            "decisions",
            "artifacts",
            "next_steps",
            "open_blockers",
            "candidate_revision",
        }
        unknown = set(value) - fields
        if unknown:
            raise ValidationError(
                "work-memory checkpoint contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        missing = fields - set(value)
        if missing:
            raise ValidationError(
                "work-memory checkpoint is missing fields: "
                + ", ".join(sorted(missing))
            )
        schema_version = value["schema_version"]
        if (
            isinstance(schema_version, bool)
            or schema_version != WORK_MEMORY_SCHEMA_VERSION
        ):
            raise ValidationError(
                f"schema_version must be {WORK_MEMORY_SCHEMA_VERSION}"
            )
        raw_artifacts = value["artifacts"]
        if not isinstance(raw_artifacts, list) or len(raw_artifacts) > 32:
            raise ValidationError("artifacts must be a list of at most 32 entries")
        artifacts = tuple(
            HandoffArtifact.from_mapping(item, index=index)
            for index, item in enumerate(raw_artifacts)
        )
        if len({artifact.path for artifact in artifacts}) != len(artifacts):
            raise ValidationError("artifacts must use unique paths")
        candidate_revision = value["candidate_revision"]
        if candidate_revision is not None:
            candidate_revision = _nonblank_string(
                candidate_revision,
                field="candidate_revision",
                max_bytes=512,
            )
        result = cls(
            schema_version=schema_version,
            summary=_nonblank_string(value["summary"], field="summary", max_bytes=4096),
            decisions=_nonblank_tuple(
                value["decisions"],
                field="decisions",
                maximum=32,
                item_bytes=2048,
            ),
            artifacts=artifacts,
            next_steps=_nonblank_tuple(
                value["next_steps"],
                field="next_steps",
                maximum=32,
                item_bytes=2048,
            ),
            open_blockers=_nonblank_tuple(
                value["open_blockers"],
                field="open_blockers",
                maximum=32,
                item_bytes=2048,
            ),
            candidate_revision=candidate_revision,
        )
        if len(result.canonical.encode("utf-8")) > MAX_WORK_CHECKPOINT_CONTENT_BYTES:
            raise ValidationError(
                "work-memory checkpoint content exceeds "
                f"{MAX_WORK_CHECKPOINT_CONTENT_BYTES} UTF-8 bytes"
            )
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "summary": self.summary,
            "decisions": list(self.decisions),
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "next_steps": list(self.next_steps),
            "open_blockers": list(self.open_blockers),
            "candidate_revision": self.candidate_revision,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class WorkAssignmentSpec:
    """A bounded task delegated to one exact agent-profile revision."""

    schema_version: int
    title: str
    objective: str
    scope_paths: tuple[str, ...] = ()
    acceptance_checks: tuple[str, ...] = ()
    base_revision: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkAssignmentSpec:
        if not isinstance(value, dict):
            raise ValidationError("work assignment must be an object")
        fields = {
            "schema_version",
            "title",
            "objective",
            "scope_paths",
            "acceptance_checks",
            "base_revision",
        }
        unknown = set(value) - fields
        if unknown:
            raise ValidationError(
                "work assignment contains unknown fields: " + ", ".join(sorted(unknown))
            )
        missing = fields - set(value)
        if missing:
            raise ValidationError(
                "work assignment is missing fields: " + ", ".join(sorted(missing))
            )
        schema_version = value["schema_version"]
        if (
            isinstance(schema_version, bool)
            or schema_version != WORK_MEMORY_SCHEMA_VERSION
        ):
            raise ValidationError(
                f"schema_version must be {WORK_MEMORY_SCHEMA_VERSION}"
            )
        scope_paths = _nonblank_tuple(
            value["scope_paths"],
            field="scope_paths",
            maximum=64,
            item_bytes=1024,
        )
        for path_text in scope_paths:
            path = PurePosixPath(path_text)
            if (
                path_text.startswith("/")
                or "\\" in path_text
                or path_text != path.as_posix()
                or path.as_posix() == "."
                or any(part in {".", ".."} for part in path.parts)
            ):
                raise ValidationError(
                    "scope_paths entries must be safe relative POSIX paths"
                )
        base_revision = value["base_revision"]
        if base_revision is not None:
            base_revision = _nonblank_string(
                base_revision, field="base_revision", max_bytes=512
            )
        result = cls(
            schema_version=schema_version,
            title=_nonblank_string(value["title"], field="title", max_bytes=512),
            objective=_nonblank_string(
                value["objective"], field="objective", max_bytes=8192
            ),
            scope_paths=scope_paths,
            acceptance_checks=_nonblank_tuple(
                value["acceptance_checks"],
                field="acceptance_checks",
                maximum=32,
                item_bytes=2048,
            ),
            base_revision=base_revision,
        )
        if len(result.canonical.encode("utf-8")) > MAX_WORK_ASSIGNMENT_CONTENT_BYTES:
            raise ValidationError(
                "work assignment content exceeds "
                f"{MAX_WORK_ASSIGNMENT_CONTENT_BYTES} UTF-8 bytes"
            )
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "title": self.title,
            "objective": self.objective,
            "scope_paths": list(self.scope_paths),
            "acceptance_checks": list(self.acceptance_checks),
            "base_revision": self.base_revision,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class ProjectContext:
    purpose: str
    instructions: tuple[str, ...] = ()
    repository_path: str | None = None
    default_branch: str = "main"
    build_commands: tuple[tuple[str, ...], ...] = ()
    test_commands: tuple[tuple[str, ...], ...] = ()
    allowed_tools: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ProjectContext:
        if not isinstance(value, dict):
            raise ValidationError("project context must be an object")
        allowed = {
            "purpose",
            "instructions",
            "repository_path",
            "default_branch",
            "build_commands",
            "test_commands",
            "allowed_tools",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValidationError(
                "project context contains unknown fields: " + ", ".join(sorted(unknown))
            )
        purpose = require_string(value.get("purpose"), field="purpose", max_bytes=4096)
        repository_path = value.get("repository_path")
        if repository_path is not None:
            repository_path = require_string(
                repository_path, field="repository_path", max_bytes=4096
            )
        return cls(
            purpose=purpose,
            instructions=_string_tuple(
                value.get("instructions", []),
                field="instructions",
                maximum=128,
                item_bytes=4096,
            ),
            repository_path=repository_path,
            default_branch=require_string(
                value.get("default_branch", "main"),
                field="default_branch",
                max_bytes=255,
            ),
            build_commands=_commands(
                value.get("build_commands", []), field="build_commands"
            ),
            test_commands=_commands(
                value.get("test_commands", []), field="test_commands"
            ),
            allowed_tools=_string_tuple(
                value.get("allowed_tools", []),
                field="allowed_tools",
                maximum=128,
                item_bytes=128,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed_tools": list(self.allowed_tools),
            "build_commands": [list(command) for command in self.build_commands],
            "default_branch": self.default_branch,
            "instructions": list(self.instructions),
            "purpose": self.purpose,
            "repository_path": self.repository_path,
            "test_commands": [list(command) for command in self.test_commands],
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


class WorkspaceMode(StrEnum):
    READ_ONLY = "read_only"
    COPY_ON_WRITE = "copy_on_write"


class NetworkMode(StrEnum):
    NONE = "none"
    GATEWAY_ONLY = "gateway_only"
    KRAB_ONLY = "krab_only"


class GatewayCapability(StrEnum):
    RUN_DESCRIBE = "run.describe"
    INTERACTION_OPEN = "interaction.open"
    INTERACTION_READ = "interaction.read"
    HANDOFF_SUBMIT = "handoff.submit"
    PROVIDER_INVOKE = "provider.invoke"
    WORK_READ = "work.read"
    WORK_APPEND = "work.append"
    WORK_CHECKPOINT = "work.checkpoint"
    WORK_ASSIGN = "work.assign"
    WORK_ASSIGNMENT_CLAIM = "work.assignment.claim"
    WORK_ASSIGNMENT_COMPLETE = "work.assignment.complete"


PROVIDER_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
HTTP_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]{1,64}$")
PROVIDER_RESERVED_HEADERS = {
    "accept",
    "authorization",
    "connection",
    "content-length",
    "content-type",
    "host",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "user-agent",
}


def validate_provider_name(value: object, *, field: str = "provider name") -> str:
    if not isinstance(value, str) or PROVIDER_NAME_RE.fullmatch(value) is None:
        raise ValidationError(f"{field} must match [a-z][a-z0-9_-]{{0,63}}")
    return value


def _bounded_integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True)
class ProviderGrantSpec:
    """Immutable outbound policy selected by a named provider grant."""

    provider: str
    adapter: str
    destination: str
    method: str
    model: str
    credential_header: str
    credential_prefix: str
    static_headers: tuple[tuple[str, str], ...]
    request_bytes: int
    response_bytes: int
    timeout_ms: int
    max_concurrency: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ProviderGrantSpec:
        if not isinstance(value, dict):
            raise ValidationError("provider grant spec must be an object")
        fields = {
            "provider",
            "adapter",
            "destination",
            "method",
            "model",
            "credential_header",
            "credential_prefix",
            "static_headers",
            "request_bytes",
            "response_bytes",
            "timeout_ms",
            "max_concurrency",
        }
        unknown = set(value) - fields
        if unknown:
            raise ValidationError(
                "provider grant spec contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        missing = fields - set(value)
        if missing:
            raise ValidationError(
                "provider grant spec is missing fields: " + ", ".join(sorted(missing))
            )

        provider = validate_provider_name(value["provider"], field="provider")
        adapter = require_string(value["adapter"], field="adapter", max_bytes=32)
        if adapter not in {
            "json_http",
            "openai_responses",
            "anthropic_messages",
        }:
            raise ValidationError(
                "provider adapter must be json_http, openai_responses, "
                "or anthropic_messages"
            )

        destination = require_string(
            value["destination"], field="destination", max_bytes=2048
        )
        parsed = urlsplit(destination)
        if parsed.username is not None or parsed.password is not None:
            raise ValidationError("provider destination must not contain credentials")
        if parsed.fragment:
            raise ValidationError("provider destination must not contain a fragment")
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValidationError("provider destination has an invalid port") from exc
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname is None
            or not parsed.hostname.isascii()
            or any(character.isspace() for character in destination)
            or not parsed.path.startswith("/")
            or port == 0
        ):
            raise ValidationError("provider destination must be an absolute HTTP URL")
        if parsed.scheme != "https" and parsed.hostname not in {
            "127.0.0.1",
            "::1",
            "localhost",
        }:
            raise ValidationError(
                "provider destination must use HTTPS unless it is loopback"
            )

        method = require_string(value["method"], field="method", max_bytes=16)
        if method != "POST":
            raise ValidationError("provider method must be POST")
        model = _nonblank_string(value["model"], field="model", max_bytes=256)

        header = require_string(
            value["credential_header"],
            field="credential_header",
            max_bytes=64,
        ).lower()
        if HTTP_HEADER_NAME_RE.fullmatch(
            header
        ) is None or header in PROVIDER_RESERVED_HEADERS - {"authorization"}:
            raise ValidationError("provider credential header is invalid")
        prefix = require_string(
            value["credential_prefix"],
            field="credential_prefix",
            max_bytes=64,
            allow_empty=True,
        )
        if any(not 0x20 <= ord(character) <= 0x7E for character in prefix):
            raise ValidationError("provider credential prefix must be printable ASCII")

        header_values = value["static_headers"]
        if not isinstance(header_values, dict) or len(header_values) > 16:
            raise ValidationError(
                "provider static_headers must be an object of at most 16 entries"
            )
        normalized_headers: dict[str, str] = {}
        for raw_name, raw_value in header_values.items():
            if not isinstance(raw_name, str):
                raise ValidationError("provider static header name is invalid")
            name = raw_name.lower()
            if (
                HTTP_HEADER_NAME_RE.fullmatch(name) is None
                or name in PROVIDER_RESERVED_HEADERS
                or name == header
                or name in normalized_headers
            ):
                raise ValidationError(
                    "provider static header is reserved or duplicated"
                )
            content = require_string(
                raw_value,
                field=f"provider static header {name}",
                max_bytes=512,
                allow_empty=True,
            )
            if any(not 0x20 <= ord(character) <= 0x7E for character in content):
                raise ValidationError(
                    "provider static header value must be printable ASCII"
                )
            normalized_headers[name] = content

        return cls(
            provider=provider,
            adapter=adapter,
            destination=destination,
            method=method,
            model=model,
            credential_header=header,
            credential_prefix=prefix,
            static_headers=tuple(sorted(normalized_headers.items())),
            request_bytes=_bounded_integer(
                value["request_bytes"],
                field="request_bytes",
                minimum=1,
                maximum=2 * 1024 * 1024,
            ),
            response_bytes=_bounded_integer(
                value["response_bytes"],
                field="response_bytes",
                minimum=1,
                maximum=4 * 1024 * 1024,
            ),
            timeout_ms=_bounded_integer(
                value["timeout_ms"],
                field="timeout_ms",
                minimum=100,
                maximum=120_000,
            ),
            max_concurrency=_bounded_integer(
                value["max_concurrency"],
                field="max_concurrency",
                minimum=1,
                maximum=16,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "adapter": self.adapter,
            "credential_header": self.credential_header,
            "credential_prefix": self.credential_prefix,
            "destination": self.destination,
            "max_concurrency": self.max_concurrency,
            "method": self.method,
            "model": self.model,
            "provider": self.provider,
            "request_bytes": self.request_bytes,
            "response_bytes": self.response_bytes,
            "static_headers": dict(self.static_headers),
            "timeout_ms": self.timeout_ms,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class AgentProfileSpec:
    image: str
    command: tuple[str, ...]
    runtime_adapter: str = "command"
    runtime_provider_grant: str | None = None
    workspace_mode: WorkspaceMode = WorkspaceMode.COPY_ON_WRITE
    network_mode: NetworkMode = NetworkMode.GATEWAY_ONLY
    credential_names: tuple[str, ...] = ()
    gateway_capabilities: tuple[GatewayCapability, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AgentProfileSpec:
        if not isinstance(value, dict):
            raise ValidationError("agent profile spec must be an object")
        allowed = {
            "image",
            "command",
            "runtime_adapter",
            "runtime_provider_grant",
            "workspace_mode",
            "network_mode",
            "credential_names",
            "gateway_capabilities",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValidationError(
                "agent profile spec contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        if "image" not in value or "command" not in value:
            raise ValidationError("agent profile spec requires image and command")
        return cls.create(
            image=value["image"],
            command=value["command"],
            runtime_adapter=value.get("runtime_adapter", "command"),
            runtime_provider_grant=value.get("runtime_provider_grant"),
            workspace_mode=value.get(
                "workspace_mode", WorkspaceMode.COPY_ON_WRITE.value
            ),
            network_mode=value.get("network_mode", NetworkMode.GATEWAY_ONLY.value),
            credential_names=value.get("credential_names", ()),
            gateway_capabilities=value.get("gateway_capabilities", ()),
        )

    @classmethod
    def create(
        cls,
        *,
        image: object,
        command: Sequence[object],
        runtime_adapter: object = "command",
        runtime_provider_grant: object = None,
        workspace_mode: str = WorkspaceMode.COPY_ON_WRITE,
        network_mode: str = NetworkMode.GATEWAY_ONLY,
        credential_names: Sequence[object] = (),
        gateway_capabilities: Sequence[object] = (),
    ) -> AgentProfileSpec:
        image_text = require_string(image, field="image", max_bytes=512)
        if not isinstance(command, (list, tuple)) or not 1 <= len(command) <= 64:
            raise ValidationError("command must be a non-empty argv sequence")
        command_tuple = tuple(
            require_string(value, field="command argument", max_bytes=1024)
            for value in command
        )
        adapter = validate_provider_name(runtime_adapter, field="runtime_adapter")
        provider_grant = None
        if runtime_provider_grant is not None:
            provider_grant = validate_provider_name(
                runtime_provider_grant, field="runtime provider grant"
            )
            if provider_grant == "krab":
                raise ValidationError("runtime provider grant cannot be krab")
        credential_tuple = tuple(
            require_string(value, field="credential name", max_bytes=128)
            for value in credential_names
        )
        if any(
            re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", value) is None
            for value in credential_tuple
        ):
            raise ValidationError("credential names must match [a-z][a-z0-9_-]{0,63}")
        if len(credential_tuple) > 32 or len(set(credential_tuple)) != len(
            credential_tuple
        ):
            raise ValidationError(
                "credential names must contain at most 32 unique handles"
            )
        if not isinstance(gateway_capabilities, (list, tuple)) or len(
            gateway_capabilities
        ) > len(GatewayCapability):
            raise ValidationError("gateway capabilities must be a bounded sequence")
        try:
            capability_tuple = tuple(
                GatewayCapability(capability) for capability in gateway_capabilities
            )
        except (TypeError, ValueError) as exc:
            raise ValidationError("gateway capability is invalid") from exc
        if len(set(capability_tuple)) != len(capability_tuple):
            raise ValidationError("gateway capabilities must be unique")
        try:
            workspace = WorkspaceMode(workspace_mode)
            network = NetworkMode(network_mode)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        if capability_tuple and network is not NetworkMode.GATEWAY_ONLY:
            raise ValidationError(
                "gateway capabilities require gateway_only networking"
            )
        interaction_capabilities = {
            GatewayCapability.INTERACTION_OPEN,
            GatewayCapability.INTERACTION_READ,
        }
        if interaction_capabilities.intersection(capability_tuple) and (
            "krab" not in credential_tuple
        ):
            raise ValidationError(
                "interaction gateway capabilities require the krab credential"
            )
        if GatewayCapability.PROVIDER_INVOKE in capability_tuple and not set(
            credential_tuple
        ).difference({"krab"}):
            raise ValidationError(
                "provider.invoke requires at least one named provider grant"
            )
        if provider_grant is not None:
            if provider_grant not in credential_tuple:
                raise ValidationError(
                    "runtime provider grant must appear in credential_names"
                )
            required = {
                GatewayCapability.RUN_DESCRIBE,
                GatewayCapability.PROVIDER_INVOKE,
            }
            missing = required.difference(capability_tuple)
            if missing:
                raise ValidationError(
                    "runtime provider grant requires gateway capability: "
                    + ", ".join(sorted(capability.value for capability in missing))
                )
        return cls(
            image=image_text,
            command=command_tuple,
            runtime_adapter=adapter,
            runtime_provider_grant=provider_grant,
            workspace_mode=workspace,
            network_mode=network,
            credential_names=credential_tuple,
            gateway_capabilities=capability_tuple,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "command": list(self.command),
            "credential_names": list(self.credential_names),
            "image": self.image,
            "network_mode": self.network_mode.value,
            "workspace_mode": self.workspace_mode.value,
        }
        if self.runtime_adapter != "command":
            result["runtime_adapter"] = self.runtime_adapter
        if self.runtime_provider_grant is not None:
            result["runtime_provider_grant"] = self.runtime_provider_grant
        if self.gateway_capabilities:
            result["gateway_capabilities"] = [
                capability.value for capability in self.gateway_capabilities
            ]
        return result

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


def _seconds_schedule(
    value: object,
    *,
    field: str,
    minimum: int,
) -> tuple[int, ...]:
    if not isinstance(value, list) or len(value) > MAX_SUPERVISION_SCHEDULE_ENTRIES:
        raise ValidationError(
            f"{field} must be a list of at most "
            f"{MAX_SUPERVISION_SCHEDULE_ENTRIES} entries"
        )
    schedule: list[int] = []
    for entry in value:
        if (
            isinstance(entry, bool)
            or not isinstance(entry, int)
            or not minimum <= entry <= MAX_SUPERVISION_DELAY_SECONDS
        ):
            raise ValidationError(
                f"{field} entries must be between {minimum} and "
                f"{MAX_SUPERVISION_DELAY_SECONDS} seconds"
            )
        schedule.append(entry)
    if any(
        current <= previous
        for previous, current in zip(schedule, schedule[1:], strict=False)
    ):
        raise ValidationError(f"{field} entries must be strictly increasing")
    return tuple(schedule)


@dataclass(frozen=True)
class SupervisionPolicy:
    """Immutable run policy for automatic advancement and aged attention."""

    schema_version: int
    auto_execute: bool
    retry_backoff_seconds: tuple[int, ...]
    interaction_escalation_seconds: tuple[int, ...]
    attempt_idle_escalation_seconds: tuple[int, ...]
    attempt_lease_expired_escalation_seconds: tuple[int, ...]
    other_attention_escalation_seconds: tuple[int, ...]

    @classmethod
    def disabled(cls) -> SupervisionPolicy:
        return cls(
            schema_version=SUPERVISION_POLICY_SCHEMA_VERSION,
            auto_execute=False,
            retry_backoff_seconds=(5, 30, 300),
            interaction_escalation_seconds=(),
            attempt_idle_escalation_seconds=(),
            attempt_lease_expired_escalation_seconds=(),
            other_attention_escalation_seconds=(),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SupervisionPolicy:
        if not isinstance(value, dict):
            raise ValidationError("supervision policy must be an object")
        fields = {
            "schema_version",
            "auto_execute",
            "retry_backoff_seconds",
            "interaction_escalation_seconds",
            "attempt_idle_escalation_seconds",
            "attempt_lease_expired_escalation_seconds",
            "other_attention_escalation_seconds",
        }
        unknown = set(value) - fields
        if unknown:
            raise ValidationError(
                "supervision policy contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        missing = fields - set(value)
        if missing:
            raise ValidationError(
                "supervision policy requires fields: " + ", ".join(sorted(missing))
            )
        schema_version = value["schema_version"]
        if (
            isinstance(schema_version, bool)
            or schema_version != SUPERVISION_POLICY_SCHEMA_VERSION
        ):
            raise ValidationError(
                "supervision policy schema_version must be "
                f"{SUPERVISION_POLICY_SCHEMA_VERSION}"
            )
        auto_execute = value["auto_execute"]
        if not isinstance(auto_execute, bool):
            raise ValidationError("auto_execute must be boolean")
        return cls(
            schema_version=schema_version,
            auto_execute=auto_execute,
            retry_backoff_seconds=_seconds_schedule(
                value["retry_backoff_seconds"],
                field="retry_backoff_seconds",
                minimum=1,
            ),
            interaction_escalation_seconds=_seconds_schedule(
                value["interaction_escalation_seconds"],
                field="interaction_escalation_seconds",
                minimum=0,
            ),
            attempt_idle_escalation_seconds=_seconds_schedule(
                value["attempt_idle_escalation_seconds"],
                field="attempt_idle_escalation_seconds",
                minimum=0,
            ),
            attempt_lease_expired_escalation_seconds=_seconds_schedule(
                value["attempt_lease_expired_escalation_seconds"],
                field="attempt_lease_expired_escalation_seconds",
                minimum=0,
            ),
            other_attention_escalation_seconds=_seconds_schedule(
                value["other_attention_escalation_seconds"],
                field="other_attention_escalation_seconds",
                minimum=0,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "auto_execute": self.auto_execute,
            "retry_backoff_seconds": list(self.retry_backoff_seconds),
            "interaction_escalation_seconds": list(self.interaction_escalation_seconds),
            "attempt_idle_escalation_seconds": list(
                self.attempt_idle_escalation_seconds
            ),
            "attempt_lease_expired_escalation_seconds": list(
                self.attempt_lease_expired_escalation_seconds
            ),
            "other_attention_escalation_seconds": list(
                self.other_attention_escalation_seconds
            ),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


class RunState(StrEnum):
    QUEUED = "queued"
    PREPARING = "preparing"
    RUNNING = "running"
    WAITING_FOR_INPUT = "waiting_for_input"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AttemptState(StrEnum):
    PREPARING = "preparing"
    RUNNING = "running"
    HANDED_OFF = "handed_off"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class InteractionKind(StrEnum):
    QUESTION = "question"
    APPROVAL = "approval"
    CHOICE = "choice"


class InteractionState(StrEnum):
    OPEN = "open"
    ANSWERED = "answered"
    CANCELLED = "cancelled"


RUN_TRANSITIONS: dict[RunState, frozenset[RunState]] = {
    RunState.QUEUED: frozenset({RunState.PREPARING, RunState.CANCELLED}),
    RunState.PREPARING: frozenset(
        {RunState.RUNNING, RunState.FAILED, RunState.CANCELLED}
    ),
    RunState.RUNNING: frozenset(
        {
            RunState.WAITING_FOR_INPUT,
            RunState.SUCCEEDED,
            RunState.FAILED,
            RunState.CANCELLED,
        }
    ),
    RunState.WAITING_FOR_INPUT: frozenset(
        {
            RunState.QUEUED,
            RunState.FAILED,
            RunState.CANCELLED,
        }
    ),
    RunState.SUCCEEDED: frozenset(),
    RunState.FAILED: frozenset(),
    RunState.CANCELLED: frozenset(),
}


def validate_transition(source: RunState, target: RunState) -> None:
    if target not in RUN_TRANSITIONS[source]:
        raise ValidationError(
            f"invalid run transition: {source.value} -> {target.value}"
        )
