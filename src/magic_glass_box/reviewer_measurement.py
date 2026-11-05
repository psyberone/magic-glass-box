from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .domain import ValidationError, canonical_json, content_sha256, require_string
from .gates import ExactGateInputs, GateError

REVIEWER_MEASUREMENT_SCHEMA_VERSION = 1
MAX_MEASUREMENT_ARGUMENTS = 64
MAX_MEASUREMENT_ARGUMENT_BYTES = 1024
MAX_MEASUREMENT_COMPARISON_RULES = 16
MAX_MEASUREMENT_TOOL_OBSERVATIONS = 32
MAX_MEASUREMENT_COUNT = 10_000_000
MAX_MEASUREMENT_DURATION_MS = 24 * 60 * 60 * 1000
MAX_MEASUREMENT_OUTPUT_BYTES = 24 * 1024 * 1024

NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
ATTEMPT_ID_RE = re.compile(r"^att_[0-9a-f]{32}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ARCHITECTURE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")


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


def _bounded_integer(
    value: object, *, field: str, maximum: int, minimum: int = 0
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return value


def _command(value: object) -> tuple[str, ...]:
    if (
        not isinstance(value, (list, tuple))
        or not 1 <= len(value) <= MAX_MEASUREMENT_ARGUMENTS
    ):
        raise ValidationError(
            "reviewer measurement command must be a non-empty bounded argv sequence"
        )
    command = tuple(
        require_string(
            argument,
            field="reviewer measurement command argument",
            max_bytes=MAX_MEASUREMENT_ARGUMENT_BYTES,
        )
        for argument in value
    )
    if any("\0" in argument for argument in command):
        raise ValidationError("reviewer measurement command must not contain NUL")
    return command


def _comparison_rules(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) > (
        MAX_MEASUREMENT_COMPARISON_RULES
    ):
        raise ValidationError(
            "reviewer measurement comparison_rules must be a bounded sequence"
        )
    rules = tuple(
        require_string(
            rule,
            field="reviewer measurement comparison rule",
            max_bytes=MAX_MEASUREMENT_ARGUMENT_BYTES,
        )
        for rule in value
    )
    if len(set(rules)) != len(rules):
        raise ValidationError("reviewer measurement comparison_rules must be unique")
    return rules


@dataclass(frozen=True)
class ReviewerToolObservation:
    """One relevant tool's directly observed presence and version."""

    name: str
    status: str
    version: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or TOOL_NAME_RE.fullmatch(self.name) is None:
            raise ValidationError("reviewer measurement tool name is invalid")
        if not isinstance(self.status, str) or self.status not in {"present", "absent"}:
            raise ValidationError("reviewer measurement tool status is invalid")
        if self.status == "present":
            require_string(
                self.version,
                field="reviewer measurement tool version",
                max_bytes=256,
            )
        elif self.version is not None:
            raise ValidationError("an absent reviewer measurement tool has no version")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ReviewerToolObservation:
        if not isinstance(value, dict):
            raise ValidationError("reviewer measurement tool must be an object")
        _exact_fields(
            value,
            expected={"name", "status", "version"},
            subject="reviewer measurement tool",
        )
        return cls(
            name=value["name"],
            status=value["status"],
            version=value["version"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "status": self.status, "version": self.version}


@dataclass(frozen=True)
class ReviewerResultFieldSemantics:
    """Reviewer-declared meanings for the otherwise ambiguous result fields."""

    exit_code: str
    collected_count: str
    executed_count: str
    output_sha256: str

    def __post_init__(self) -> None:
        for field in (
            "exit_code",
            "collected_count",
            "executed_count",
            "output_sha256",
        ):
            require_string(
                getattr(self, field),
                field=f"reviewer measurement {field} semantics",
                max_bytes=1024,
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ReviewerResultFieldSemantics:
        if not isinstance(value, dict):
            raise ValidationError(
                "reviewer measurement result_field_semantics must be an object"
            )
        fields = {
            "exit_code",
            "collected_count",
            "executed_count",
            "output_sha256",
        }
        _exact_fields(
            value,
            expected=fields,
            subject="reviewer measurement result_field_semantics",
        )
        return cls(**{field: value[field] for field in fields})

    def to_dict(self) -> dict[str, str]:
        return {
            "exit_code": self.exit_code,
            "collected_count": self.collected_count,
            "executed_count": self.executed_count,
            "output_sha256": self.output_sha256,
        }


@dataclass(frozen=True)
class ReviewerMeasurement:
    """One reviewer-authored, exact-input observation from its bounded sandbox.

    This artifact is supplemental review evidence. It deliberately has no gate
    outcome and therefore cannot satisfy an immutable validation obligation.
    """

    schema_version: int
    inputs: ExactGateInputs
    attempt_id: str
    name: str
    command: tuple[str, ...]
    architecture: str
    code_origin: str
    code_origin_sha256: str
    interpreter: str
    tool_observations: tuple[ReviewerToolObservation, ...]
    exit_code: int
    duration_ms: int
    collected_count: int
    executed_count: int
    output_sha256: str
    output_bytes: int
    result_field_semantics: ReviewerResultFieldSemantics
    comparison_rules: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            isinstance(self.schema_version, bool)
            or self.schema_version != REVIEWER_MEASUREMENT_SCHEMA_VERSION
        ):
            raise ValidationError(
                "reviewer measurement schema_version must be "
                f"{REVIEWER_MEASUREMENT_SCHEMA_VERSION}"
            )
        if not isinstance(self.inputs, ExactGateInputs):
            raise ValidationError("reviewer measurement inputs are invalid")
        if (
            not isinstance(self.attempt_id, str)
            or ATTEMPT_ID_RE.fullmatch(self.attempt_id) is None
        ):
            raise ValidationError("reviewer measurement attempt_id is invalid")
        if not isinstance(self.name, str) or NAME_RE.fullmatch(self.name) is None:
            raise ValidationError("reviewer measurement name is invalid")
        _command(self.command)
        if (
            not isinstance(self.architecture, str)
            or ARCHITECTURE_RE.fullmatch(self.architecture) is None
        ):
            raise ValidationError("reviewer measurement architecture is invalid")
        require_string(
            self.code_origin,
            field="reviewer measurement code_origin",
            max_bytes=1024,
        )
        if (
            not isinstance(self.code_origin_sha256, str)
            or SHA256_RE.fullmatch(self.code_origin_sha256) is None
        ):
            raise ValidationError("reviewer measurement code_origin_sha256 is invalid")
        require_string(
            self.interpreter,
            field="reviewer measurement interpreter",
            max_bytes=256,
        )
        if (
            not isinstance(self.tool_observations, tuple)
            or not 1 <= len(self.tool_observations) <= MAX_MEASUREMENT_TOOL_OBSERVATIONS
            or any(
                not isinstance(item, ReviewerToolObservation)
                for item in self.tool_observations
            )
            or tuple(sorted(item.name for item in self.tool_observations))
            != tuple(item.name for item in self.tool_observations)
            or len({item.name for item in self.tool_observations})
            != len(self.tool_observations)
            or not any(item.status == "present" for item in self.tool_observations)
        ):
            raise ValidationError(
                "reviewer measurement tool_observations must be a canonical "
                "non-empty bounded set with at least one present tool"
            )
        if (
            isinstance(self.exit_code, bool)
            or not isinstance(self.exit_code, int)
            or not 0 <= self.exit_code <= 255
        ):
            raise ValidationError("reviewer measurement exit_code must be 0 to 255")
        _bounded_integer(
            self.duration_ms,
            field="reviewer measurement duration_ms",
            maximum=MAX_MEASUREMENT_DURATION_MS,
        )
        _bounded_integer(
            self.collected_count,
            field="reviewer measurement collected_count",
            maximum=MAX_MEASUREMENT_COUNT,
            minimum=1,
        )
        _bounded_integer(
            self.executed_count,
            field="reviewer measurement executed_count",
            maximum=MAX_MEASUREMENT_COUNT,
            minimum=1,
        )
        if self.executed_count > self.collected_count:
            raise ValidationError(
                "reviewer measurement executed_count exceeds collected_count"
            )
        if (
            not isinstance(self.output_sha256, str)
            or SHA256_RE.fullmatch(self.output_sha256) is None
        ):
            raise ValidationError("reviewer measurement output_sha256 is invalid")
        _bounded_integer(
            self.output_bytes,
            field="reviewer measurement output_bytes",
            maximum=MAX_MEASUREMENT_OUTPUT_BYTES,
        )
        if not isinstance(self.result_field_semantics, ReviewerResultFieldSemantics):
            raise ValidationError(
                "reviewer measurement result_field_semantics is invalid"
            )
        _comparison_rules(self.comparison_rules)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ReviewerMeasurement:
        if not isinstance(value, dict):
            raise ValidationError("reviewer measurement must be an object")
        fields = {
            "schema_version",
            "inputs",
            "attempt_id",
            "name",
            "command",
            "architecture",
            "code_origin",
            "code_origin_sha256",
            "interpreter",
            "tool_observations",
            "exit_code",
            "duration_ms",
            "collected_count",
            "executed_count",
            "output_sha256",
            "output_bytes",
            "result_field_semantics",
            "comparison_rules",
        }
        _exact_fields(value, expected=fields, subject="reviewer measurement")
        try:
            inputs = ExactGateInputs.from_mapping(value["inputs"])
        except GateError as exc:
            raise ValidationError(str(exc)) from exc
        command = _command(value["command"])
        raw_tools = value["tool_observations"]
        if not isinstance(raw_tools, list):
            raise ValidationError(
                "reviewer measurement tool_observations must be a list"
            )
        tool_observations = tuple(
            ReviewerToolObservation.from_mapping(item) for item in raw_tools
        )
        result_field_semantics = ReviewerResultFieldSemantics.from_mapping(
            value["result_field_semantics"]
        )
        comparison_rules = _comparison_rules(value["comparison_rules"])
        return cls(
            schema_version=value["schema_version"],
            inputs=inputs,
            attempt_id=value["attempt_id"],
            name=value["name"],
            command=command,
            architecture=value["architecture"],
            code_origin=value["code_origin"],
            code_origin_sha256=value["code_origin_sha256"],
            interpreter=value["interpreter"],
            tool_observations=tool_observations,
            exit_code=value["exit_code"],
            duration_ms=value["duration_ms"],
            collected_count=value["collected_count"],
            executed_count=value["executed_count"],
            output_sha256=value["output_sha256"],
            output_bytes=value["output_bytes"],
            result_field_semantics=result_field_semantics,
            comparison_rules=comparison_rules,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "inputs": self.inputs.to_dict(),
            "attempt_id": self.attempt_id,
            "name": self.name,
            "command": list(self.command),
            "architecture": self.architecture,
            "code_origin": self.code_origin,
            "code_origin_sha256": self.code_origin_sha256,
            "interpreter": self.interpreter,
            "tool_observations": [item.to_dict() for item in self.tool_observations],
            "exit_code": self.exit_code,
            "duration_ms": self.duration_ms,
            "collected_count": self.collected_count,
            "executed_count": self.executed_count,
            "output_sha256": self.output_sha256,
            "output_bytes": self.output_bytes,
            "result_field_semantics": self.result_field_semantics.to_dict(),
            "comparison_rules": list(self.comparison_rules),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)
