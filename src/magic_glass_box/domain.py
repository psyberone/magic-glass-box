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


@dataclass(frozen=True)
class AgentProfileSpec:
    image: str
    command: tuple[str, ...]
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
        return cls(
            image=image_text,
            command=command_tuple,
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


class RunState(StrEnum):
    QUEUED = "queued"
    PREPARING = "preparing"
    RUNNING = "running"
    WAITING_FOR_INPUT = "waiting_for_input"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


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
            RunState.RUNNING,
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
