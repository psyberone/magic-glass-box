from __future__ import annotations

import base64
import binascii
import hashlib
import re
from dataclasses import dataclass
from datetime import UTC
from typing import Any

from .domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    parse_rfc3339_timestamp,
    validate_provider_name,
)

EXECUTION_SCHEMA_VERSION = 1
EXECUTION_ID_RE = re.compile(r"^rex_[0-9a-f]{32}$")
SERVICE_OPERATION_ID_RE = re.compile(r"^sop_[0-9a-f]{32}$")
CANDIDATE_ID_RE = re.compile(r"^cnd_[0-9a-f]{32}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
OBJECT_ID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
OBJECT_FORMAT_LENGTHS = {"sha1": 40, "sha256": 64}
EXECUTION_STATES = frozenset({"queued", "running", "succeeded", "failed", "cancelled"})
TERMINAL_EXECUTION_STATES = frozenset({"succeeded", "failed", "cancelled"})
MAX_EXECUTION_DURATION_MS = 24 * 60 * 60 * 1000
MAX_EXECUTION_OUTPUT_BYTES = 24 * 1024 * 1024
MAX_EXECUTION_SOURCE_BYTES = 64 * 1024 * 1024
MAX_EXECUTION_ARTIFACT_BYTES = 2 * 1024 * 1024
MAX_EXECUTION_ARTIFACTS = 10_000


def _exact(value: object, *, fields: set[str], subject: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValidationError(f"{subject} fields are invalid")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise ValidationError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _image_id(value: object) -> str:
    if not isinstance(value, str) or IMAGE_ID_RE.fullmatch(value) is None:
        raise ValidationError("execution image identity is invalid")
    return value


def _bounded_integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return value


def _timestamp(value: object, *, field: str) -> str:
    return (
        parse_rfc3339_timestamp(value, field=field)
        .astimezone(UTC)
        .isoformat(timespec="microseconds")
    )


def _optional_timestamp(value: object, *, field: str) -> str | None:
    return None if value is None else _timestamp(value, field=field)


def _object_revision(value: object, *, object_format: str, field: str) -> str:
    length = OBJECT_FORMAT_LENGTHS.get(object_format)
    if (
        length is None
        or not isinstance(value, str)
        or OBJECT_ID_RE.fullmatch(value) is None
        or len(value) != length
    ):
        raise ValidationError(f"execution {field} is invalid")
    return value


@dataclass(frozen=True)
class ExecutionStatusResult:
    schema_version: int
    execution_id: str
    state: str
    candidate_revision: str
    object_format: str
    recipe_id: str
    recipe_sha256: str
    image_id: str
    source_bytes: int
    source_sha256: str
    submitted_at: str
    started_at: str | None
    finished_at: str | None
    exit_code: int | None

    @classmethod
    def from_mapping(cls, value: object) -> ExecutionStatusResult:
        item = _exact(
            value,
            fields={
                "schema_version",
                "execution_id",
                "state",
                "candidate_revision",
                "object_format",
                "recipe_id",
                "recipe_sha256",
                "image_id",
                "source_bytes",
                "source_sha256",
                "submitted_at",
                "started_at",
                "finished_at",
                "exit_code",
            },
            subject="execution status result",
        )
        if (
            isinstance(item["schema_version"], bool)
            or item["schema_version"] != EXECUTION_SCHEMA_VERSION
        ):
            raise ValidationError(
                f"execution status schema_version must be {EXECUTION_SCHEMA_VERSION}"
            )
        execution_id = item["execution_id"]
        if (
            not isinstance(execution_id, str)
            or EXECUTION_ID_RE.fullmatch(execution_id) is None
        ):
            raise ValidationError("execution result identity is invalid")
        state = item["state"]
        if state not in EXECUTION_STATES:
            raise ValidationError("execution result state is invalid")
        object_format = item["object_format"]
        if object_format not in OBJECT_FORMAT_LENGTHS:
            raise ValidationError("execution object format is invalid")
        candidate_revision = _object_revision(
            item["candidate_revision"],
            object_format=object_format,
            field="candidate revision",
        )
        try:
            recipe_id = validate_provider_name(
                item["recipe_id"], field="execution recipe ID"
            )
        except ValidationError:
            raise
        submitted_at = _timestamp(
            item["submitted_at"], field="execution submission timestamp"
        )
        started_at = _optional_timestamp(
            item["started_at"], field="execution start timestamp"
        )
        finished_at = _optional_timestamp(
            item["finished_at"], field="execution finish timestamp"
        )
        exit_code = item["exit_code"]
        if exit_code is not None:
            exit_code = _bounded_integer(
                exit_code, field="execution exit code", minimum=0, maximum=255
            )
        lifecycle_valid = (
            (
                state == "queued"
                and started_at is None
                and finished_at is None
                and exit_code is None
            )
            or (
                state == "running"
                and started_at is not None
                and finished_at is None
                and exit_code is None
            )
            or (
                state == "succeeded"
                and started_at is not None
                and finished_at is not None
                and exit_code == 0
            )
            or (
                state == "failed"
                and started_at is not None
                and finished_at is not None
                and exit_code is not None
                and exit_code != 0
            )
            or (state == "cancelled" and finished_at is not None and exit_code is None)
        )
        if not lifecycle_valid:
            if state in {"succeeded", "failed"}:
                raise ValidationError("execution exit code contradicts its state")
            raise ValidationError("execution lifecycle fields are inconsistent")
        submitted = parse_rfc3339_timestamp(
            submitted_at, field="execution submission timestamp"
        )
        started = (
            None
            if started_at is None
            else parse_rfc3339_timestamp(started_at, field="execution start timestamp")
        )
        finished = (
            None
            if finished_at is None
            else parse_rfc3339_timestamp(
                finished_at, field="execution finish timestamp"
            )
        )
        if (started is not None and started < submitted) or (
            finished is not None
            and (finished < submitted or (started is not None and finished < started))
        ):
            raise ValidationError("execution lifecycle timestamps are inconsistent")
        return cls(
            schema_version=EXECUTION_SCHEMA_VERSION,
            execution_id=execution_id,
            state=state,
            candidate_revision=candidate_revision,
            object_format=object_format,
            recipe_id=recipe_id,
            recipe_sha256=_sha256(
                item["recipe_sha256"], field="execution recipe digest"
            ),
            image_id=_image_id(item["image_id"]),
            source_bytes=_bounded_integer(
                item["source_bytes"],
                field="execution source size",
                minimum=1,
                maximum=MAX_EXECUTION_SOURCE_BYTES,
            ),
            source_sha256=_sha256(
                item["source_sha256"], field="execution source digest"
            ),
            submitted_at=submitted_at,
            started_at=started_at,
            finished_at=finished_at,
            exit_code=exit_code,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_revision": self.candidate_revision,
            "execution_id": self.execution_id,
            "exit_code": self.exit_code,
            "finished_at": self.finished_at,
            "image_id": self.image_id,
            "object_format": self.object_format,
            "recipe_id": self.recipe_id,
            "recipe_sha256": self.recipe_sha256,
            "schema_version": self.schema_version,
            "source_bytes": self.source_bytes,
            "source_sha256": self.source_sha256,
            "started_at": self.started_at,
            "state": self.state,
            "submitted_at": self.submitted_at,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())


@dataclass(frozen=True)
class ExecutionEvidenceResult:
    schema_version: int
    execution: ExecutionStatusResult
    duration_ms: int
    output_bytes: int
    output_sha256: str
    artifact_bytes: int
    artifact_count: int
    artifact_sha256: str
    artifact_encoding: str
    artifact_bundle: bytes

    @classmethod
    def from_mapping(cls, value: object) -> ExecutionEvidenceResult:
        item = _exact(
            value,
            fields={
                "schema_version",
                "execution",
                "duration_ms",
                "output_bytes",
                "output_sha256",
                "artifact_bytes",
                "artifact_count",
                "artifact_sha256",
                "artifact_encoding",
                "artifact_bundle",
            },
            subject="execution evidence result",
        )
        if (
            isinstance(item["schema_version"], bool)
            or item["schema_version"] != EXECUTION_SCHEMA_VERSION
        ):
            raise ValidationError(
                f"execution evidence schema_version must be {EXECUTION_SCHEMA_VERSION}"
            )
        execution = ExecutionStatusResult.from_mapping(item["execution"])
        if execution.state not in TERMINAL_EXECUTION_STATES:
            raise ValidationError("execution evidence requires a terminal execution")
        duration_ms = _bounded_integer(
            item["duration_ms"],
            field="execution duration",
            minimum=0,
            maximum=MAX_EXECUTION_DURATION_MS,
        )
        if execution.started_at is None:
            expected_duration = 0
        else:
            assert execution.finished_at is not None
            started = parse_rfc3339_timestamp(
                execution.started_at, field="execution start timestamp"
            )
            finished = parse_rfc3339_timestamp(
                execution.finished_at, field="execution finish timestamp"
            )
            expected_duration = int((finished - started).total_seconds() * 1000)
        if duration_ms != expected_duration:
            raise ValidationError("execution duration does not match its timestamps")
        artifact_encoding = item["artifact_encoding"]
        if artifact_encoding != "tar+gzip+base64":
            raise ValidationError("execution artifact encoding is invalid")
        encoded = item["artifact_bundle"]
        if not isinstance(encoded, str):
            raise ValidationError("execution artifact bundle must be base64 text")
        try:
            bundle = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValidationError(
                "execution artifact bundle is invalid base64"
            ) from exc
        artifact_bytes = _bounded_integer(
            item["artifact_bytes"],
            field="execution artifact size",
            minimum=0,
            maximum=MAX_EXECUTION_ARTIFACT_BYTES,
        )
        if len(bundle) != artifact_bytes:
            raise ValidationError("execution artifact size does not match its bundle")
        artifact_sha256 = _sha256(
            item["artifact_sha256"], field="execution artifact digest"
        )
        if hashlib.sha256(bundle).hexdigest() != artifact_sha256:
            raise ValidationError("execution artifact digest does not match its bundle")
        artifact_count = _bounded_integer(
            item["artifact_count"],
            field="execution artifact count",
            minimum=0,
            maximum=MAX_EXECUTION_ARTIFACTS,
        )
        return cls(
            schema_version=EXECUTION_SCHEMA_VERSION,
            execution=execution,
            duration_ms=duration_ms,
            output_bytes=_bounded_integer(
                item["output_bytes"],
                field="execution output size",
                minimum=0,
                maximum=MAX_EXECUTION_OUTPUT_BYTES,
            ),
            output_sha256=_sha256(
                item["output_sha256"], field="execution output digest"
            ),
            artifact_bytes=artifact_bytes,
            artifact_count=artifact_count,
            artifact_sha256=artifact_sha256,
            artifact_encoding=artifact_encoding,
            artifact_bundle=bundle,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_bundle": base64.b64encode(self.artifact_bundle).decode("ascii"),
            "artifact_bytes": self.artifact_bytes,
            "artifact_count": self.artifact_count,
            "artifact_encoding": self.artifact_encoding,
            "artifact_sha256": self.artifact_sha256,
            "duration_ms": self.duration_ms,
            "execution": self.execution.to_dict(),
            "output_bytes": self.output_bytes,
            "output_sha256": self.output_sha256,
            "schema_version": self.schema_version,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class ExecutionJobSpec:
    operation_id: str
    mapping_name: str
    repository: str
    candidate_id: str
    object_format: str
    base_revision: str
    candidate_revision: str
    recipe_id: str
    recipe_sha256: str
    image_id: str

    def __post_init__(self) -> None:
        if SERVICE_OPERATION_ID_RE.fullmatch(self.operation_id) is None:
            raise ValidationError("execution operation ID is invalid")
        validate_provider_name(self.mapping_name, field="execution mapping name")
        if (
            not isinstance(self.repository, str)
            or not self.repository
            or len(self.repository.encode("utf-8")) > 512
        ):
            raise ValidationError("execution repository is invalid")
        if CANDIDATE_ID_RE.fullmatch(self.candidate_id) is None:
            raise ValidationError("execution candidate ID is invalid")
        _object_revision(
            self.base_revision,
            object_format=self.object_format,
            field="base revision",
        )
        _object_revision(
            self.candidate_revision,
            object_format=self.object_format,
            field="candidate revision",
        )
        validate_provider_name(self.recipe_id, field="execution recipe ID")
        _sha256(self.recipe_sha256, field="execution recipe digest")
        _image_id(self.image_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_revision": self.base_revision,
            "candidate_id": self.candidate_id,
            "candidate_revision": self.candidate_revision,
            "image_id": self.image_id,
            "mapping_name": self.mapping_name,
            "object_format": self.object_format,
            "operation_id": self.operation_id,
            "recipe_id": self.recipe_id,
            "recipe_sha256": self.recipe_sha256,
            "repository": self.repository,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)
