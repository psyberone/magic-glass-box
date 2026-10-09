from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class ValidationError(ValueError):
    """A control-plane value failed deterministic validation."""


HANDLE_RE = re.compile(r"^[a-z][a-z0-9_-]{2,31}$")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,63}$")
KRAB_PROJECT_KEY_RE = re.compile(r"^[a-z][a-z0-9-]{2,31}$")
KRAB_OBJECT_ID_RE = re.compile(r"^(?:usr|prj|ses|itm|clm)_[0-9a-f]{32}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


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
    KRAB_ONLY = "krab_only"


@dataclass(frozen=True)
class AgentProfileSpec:
    image: str
    command: tuple[str, ...]
    workspace_mode: WorkspaceMode = WorkspaceMode.COPY_ON_WRITE
    network_mode: NetworkMode = NetworkMode.KRAB_ONLY
    credential_names: tuple[str, ...] = ()

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
            network_mode=value.get("network_mode", NetworkMode.KRAB_ONLY.value),
            credential_names=value.get("credential_names", ()),
        )

    @classmethod
    def create(
        cls,
        *,
        image: object,
        command: Sequence[object],
        workspace_mode: str = WorkspaceMode.COPY_ON_WRITE,
        network_mode: str = NetworkMode.KRAB_ONLY,
        credential_names: Sequence[object] = (),
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
        try:
            workspace = WorkspaceMode(workspace_mode)
            network = NetworkMode(network_mode)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        return cls(
            image=image_text,
            command=command_tuple,
            workspace_mode=workspace,
            network_mode=network,
            credential_names=credential_tuple,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": list(self.command),
            "credential_names": list(self.credential_names),
            "image": self.image,
            "network_mode": self.network_mode.value,
            "workspace_mode": self.workspace_mode.value,
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
