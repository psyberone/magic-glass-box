from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .domain import canonical_json, content_sha256, require_string

GATE_POLICY_SCHEMA_VERSION = 1
GATE_EVIDENCE_SCHEMA_VERSION = 1
MAX_VALIDATION_GATES = 32
MAX_VALIDATION_ARGUMENTS = 64
# Keep the durable gate contract no wider than AgentProfileSpec, which is the
# sandbox request boundary used to execute these commands.
MAX_VALIDATION_ARGUMENT_BYTES = 1024
MAX_VALIDATION_DURATION_MS = 24 * 60 * 60 * 1000
MAX_VALIDATION_OUTPUT_BYTES = 24 * 1024 * 1024

VALIDATION_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
OBJECT_ID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
GATE_ID_PATTERNS = {
    "candidate_id": re.compile(r"^cnd_[0-9a-f]{32}$"),
    "project_id": re.compile(r"^boxp_[0-9a-f]{32}$"),
    "context_version_id": re.compile(r"^ctx_[0-9a-f]{32}$"),
    "work_thread_id": re.compile(r"^wth_[0-9a-f]{32}$"),
}
PROVENANCE_ID_PATTERNS = {
    "run_id": re.compile(r"^run_[0-9a-f]{32}$"),
    "attempt_id": re.compile(r"^att_[0-9a-f]{32}$"),
    "profile_id": re.compile(r"^agp_[0-9a-f]{32}$"),
    "profile_version_id": re.compile(r"^apv_[0-9a-f]{32}$"),
    "identity_id": re.compile(r"^kid_[0-9a-f]{32}$"),
    "requested_by": re.compile(r"^boxu_[0-9a-f]{32}$"),
}
POLICY_ID_RE = re.compile(r"^gpl_[0-9a-f]{32}$")
SANDBOX_ID_RE = re.compile(r"^run_[0-9a-f]{32}$")
OBJECT_FORMAT_LENGTHS = {"sha1": 40, "sha256": 64}


class GateError(RuntimeError):
    """An integration gate policy or evidence record is invalid."""


class ReviewSeparation(StrEnum):
    DIFFERENT_PROFILE = "different_profile"
    DIFFERENT_IDENTITY = "different_identity"
    DIFFERENT_PROFILE_AND_IDENTITY = "different_profile_and_identity"


class GateOutcome(StrEnum):
    PASSED = "passed"
    FAILED = "failed"


def normalize_integration_ref(value: object) -> str:
    branch = _bounded_text(value, field="integration ref", max_bytes=512)
    full = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
    name = full.removeprefix("refs/heads/")
    forbidden = set(" ~^:?*[\\")
    if (
        not name
        or name == "@"
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in name)
        or any(character in forbidden for character in name)
        or ".." in name
        or "@{" in name
        or "//" in name
        or name.startswith("/")
        or name.endswith(("/", "."))
    ):
        raise GateError("integration ref is invalid")
    components = name.split("/")
    if any(
        not component or component.startswith(".") or component.endswith(".lock")
        for component in components
    ):
        raise GateError("integration ref is invalid")
    return full


def _bounded_text(value: object, *, field: str, max_bytes: int) -> str:
    try:
        return require_string(value, field=field, max_bytes=max_bytes)
    except (TypeError, ValueError) as exc:
        raise GateError(str(exc)) from exc


def _command(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not 1 <= len(value) <= (
        MAX_VALIDATION_ARGUMENTS
    ):
        raise GateError("validation command must be a non-empty bounded argv sequence")
    command: list[str] = []
    for argument in value:
        text = _bounded_text(
            argument,
            field="validation command argument",
            max_bytes=MAX_VALIDATION_ARGUMENT_BYTES,
        )
        if "\0" in text:
            raise GateError("validation command argument must not contain NUL")
        command.append(text)
    return tuple(command)


@dataclass(frozen=True)
class ValidationGate:
    name: str
    image_id: str
    command: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or VALIDATION_NAME_RE.fullmatch(self.name) is None
        ):
            raise GateError("validation name must match [a-z][a-z0-9_-]{0,63}")
        if (
            not isinstance(self.image_id, str)
            or IMAGE_ID_RE.fullmatch(self.image_id) is None
        ):
            raise GateError("validation image must be an immutable sha256 image ID")
        if not isinstance(self.command, tuple):
            raise GateError("validation command must be an argv tuple")
        _command(self.command)

    @classmethod
    def from_mapping(cls, value: object) -> ValidationGate:
        if not isinstance(value, dict):
            raise GateError("validation gate must be an object")
        fields = {"name", "image_id", "command"}
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown:
            raise GateError(
                "validation gate contains unknown fields: " + ", ".join(sorted(unknown))
            )
        if missing:
            raise GateError(
                "validation gate is missing fields: " + ", ".join(sorted(missing))
            )
        return cls(
            name=value["name"],  # type: ignore[arg-type]
            image_id=value["image_id"],  # type: ignore[arg-type]
            command=_command(value["command"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": list(self.command),
            "image_id": self.image_id,
            "name": self.name,
        }

    @property
    def command_sha256(self) -> str:
        return content_sha256(canonical_json(list(self.command)))


@dataclass(frozen=True)
class GatePolicySpec:
    schema_version: int
    review_separation: ReviewSeparation | None
    validations: tuple[ValidationGate, ...]

    def __post_init__(self) -> None:
        if (
            isinstance(self.schema_version, bool)
            or self.schema_version != GATE_POLICY_SCHEMA_VERSION
        ):
            raise GateError(
                f"gate policy schema_version must be {GATE_POLICY_SCHEMA_VERSION}"
            )
        if self.review_separation is not None and not isinstance(
            self.review_separation, ReviewSeparation
        ):
            raise GateError("gate policy review separation is invalid")
        if not isinstance(self.validations, tuple) or len(self.validations) > (
            MAX_VALIDATION_GATES
        ):
            raise GateError("gate policy validations must be a bounded tuple")
        if any(not isinstance(gate, ValidationGate) for gate in self.validations):
            raise GateError("gate policy validation is invalid")
        names = [gate.name for gate in self.validations]
        if len(set(names)) != len(names):
            raise GateError("gate policy validation names must be unique")
        if self.review_separation is None and not self.validations:
            raise GateError("gate policy must require at least one gate")
        if tuple(sorted(self.validations, key=lambda gate: gate.name)) != (
            self.validations
        ):
            raise GateError("gate policy validations must use canonical name order")

    @classmethod
    def from_mapping(cls, value: object) -> GatePolicySpec:
        if not isinstance(value, dict):
            raise GateError("gate policy must be an object")
        fields = {"schema_version", "review_separation", "validations"}
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown:
            raise GateError(
                "gate policy contains unknown fields: " + ", ".join(sorted(unknown))
            )
        if missing:
            raise GateError(
                "gate policy is missing fields: " + ", ".join(sorted(missing))
            )
        raw_review = value["review_separation"]
        try:
            review = (
                None if raw_review is None else ReviewSeparation(raw_review)  # type: ignore[arg-type]
            )
        except (TypeError, ValueError) as exc:
            raise GateError("gate policy review separation is invalid") from exc
        raw_validations = value["validations"]
        if not isinstance(raw_validations, list) or len(raw_validations) > (
            MAX_VALIDATION_GATES
        ):
            raise GateError("gate policy validations must be a bounded list")
        validations = tuple(
            sorted(
                (ValidationGate.from_mapping(item) for item in raw_validations),
                key=lambda gate: gate.name,
            )
        )
        return cls(
            schema_version=value["schema_version"],  # type: ignore[arg-type]
            review_separation=review,
            validations=validations,
        )

    @classmethod
    def create(
        cls,
        *,
        review_separation: ReviewSeparation | str | None,
        validations: Sequence[ValidationGate] = (),
    ) -> GatePolicySpec:
        try:
            review = (
                None
                if review_separation is None
                else ReviewSeparation(review_separation)
            )
        except (TypeError, ValueError) as exc:
            raise GateError("gate policy review separation is invalid") from exc
        return cls(
            schema_version=GATE_POLICY_SCHEMA_VERSION,
            review_separation=review,
            validations=tuple(sorted(validations, key=lambda gate: gate.name)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "review_separation": (
                None if self.review_separation is None else self.review_separation.value
            ),
            "schema_version": self.schema_version,
            "validations": [gate.to_dict() for gate in self.validations],
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def revision(self) -> str:
        return content_sha256(self.canonical)

    @property
    def required_gates(self) -> tuple[str, ...]:
        required: list[str] = []
        if self.review_separation is not None:
            required.append("independent-review")
        required.extend(f"validation:{gate.name}" for gate in self.validations)
        return tuple(required)

    def validation(self, name: str) -> ValidationGate:
        for gate in self.validations:
            if gate.name == name:
                return gate
        raise GateError(f"validation gate is not required: {name}")


@dataclass(frozen=True)
class GatePolicyManifest:
    schema_version: int
    candidate_id: str
    repository_identity: str
    project_id: str
    context_version_id: str
    work_thread_id: str
    integration_ref: str
    expected_base_revision: str
    head_revision: str
    tree_revision: str
    object_format: str
    policy_revision: str
    policy: GatePolicySpec

    def __post_init__(self) -> None:
        if (
            isinstance(self.schema_version, bool)
            or self.schema_version != GATE_POLICY_SCHEMA_VERSION
        ):
            raise GateError(
                f"gate policy schema_version must be {GATE_POLICY_SCHEMA_VERSION}"
            )
        for field in (
            "candidate_id",
            "project_id",
            "context_version_id",
            "work_thread_id",
        ):
            value = getattr(self, field)
            if (
                not isinstance(value, str)
                or GATE_ID_PATTERNS[field].fullmatch(value) is None
            ):
                raise GateError(f"gate policy {field} is invalid")
        if self.repository_identity != self.project_id:
            raise GateError("gate policy repository identity is invalid")
        if normalize_integration_ref(self.integration_ref) != self.integration_ref:
            raise GateError("gate policy integration ref is not canonical")
        object_length = OBJECT_FORMAT_LENGTHS.get(self.object_format)
        if object_length is None:
            raise GateError("gate policy object format is invalid")
        for field in (
            "expected_base_revision",
            "head_revision",
            "tree_revision",
        ):
            value = getattr(self, field)
            if (
                not isinstance(value, str)
                or OBJECT_ID_RE.fullmatch(value) is None
                or len(value) != object_length
            ):
                raise GateError(f"gate policy {field} is invalid")
        if not isinstance(self.policy, GatePolicySpec):
            raise GateError("gate policy specification is invalid")
        if (
            not isinstance(self.policy_revision, str)
            or SHA256_RE.fullmatch(self.policy_revision) is None
            or self.policy_revision != self.policy.revision
        ):
            raise GateError("gate policy revision does not match its specification")

    @classmethod
    def bind(
        cls,
        candidate: Mapping[str, object],
        *,
        default_branch: object,
        policy: GatePolicySpec,
    ) -> GatePolicyManifest:
        if not isinstance(candidate, dict):
            raise GateError("candidate record must be an object")
        required = {
            "id",
            "project_id",
            "context_version_id",
            "work_thread_id",
            "base_revision",
            "head_revision",
            "tree_revision",
            "object_format",
        }
        missing = required - set(candidate)
        if missing:
            raise GateError(
                "candidate record is missing fields: " + ", ".join(sorted(missing))
            )
        if not isinstance(policy, GatePolicySpec):
            raise GateError("gate policy specification is invalid")
        project_id = candidate["project_id"]
        return cls(
            schema_version=GATE_POLICY_SCHEMA_VERSION,
            candidate_id=candidate["id"],  # type: ignore[arg-type]
            repository_identity=project_id,  # type: ignore[arg-type]
            project_id=project_id,  # type: ignore[arg-type]
            context_version_id=candidate["context_version_id"],  # type: ignore[arg-type]
            work_thread_id=candidate["work_thread_id"],  # type: ignore[arg-type]
            integration_ref=normalize_integration_ref(default_branch),
            expected_base_revision=candidate["base_revision"],  # type: ignore[arg-type]
            head_revision=candidate["head_revision"],  # type: ignore[arg-type]
            tree_revision=candidate["tree_revision"],  # type: ignore[arg-type]
            object_format=candidate["object_format"],  # type: ignore[arg-type]
            policy_revision=policy.revision,
            policy=policy,
        )

    @classmethod
    def from_mapping(cls, value: object) -> GatePolicyManifest:
        if not isinstance(value, dict):
            raise GateError("gate policy manifest must be an object")
        fields = {
            "schema_version",
            "candidate_id",
            "repository_identity",
            "project_id",
            "context_version_id",
            "work_thread_id",
            "integration_ref",
            "expected_base_revision",
            "head_revision",
            "tree_revision",
            "object_format",
            "policy_revision",
            "policy",
        }
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown:
            raise GateError(
                "gate policy manifest contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        if missing:
            raise GateError(
                "gate policy manifest is missing fields: " + ", ".join(sorted(missing))
            )
        return cls(
            schema_version=value["schema_version"],  # type: ignore[arg-type]
            candidate_id=value["candidate_id"],  # type: ignore[arg-type]
            repository_identity=value["repository_identity"],  # type: ignore[arg-type]
            project_id=value["project_id"],  # type: ignore[arg-type]
            context_version_id=value["context_version_id"],  # type: ignore[arg-type]
            work_thread_id=value["work_thread_id"],  # type: ignore[arg-type]
            integration_ref=value["integration_ref"],  # type: ignore[arg-type]
            expected_base_revision=value["expected_base_revision"],  # type: ignore[arg-type]
            head_revision=value["head_revision"],  # type: ignore[arg-type]
            tree_revision=value["tree_revision"],  # type: ignore[arg-type]
            object_format=value["object_format"],  # type: ignore[arg-type]
            policy_revision=value["policy_revision"],  # type: ignore[arg-type]
            policy=GatePolicySpec.from_mapping(value["policy"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "context_version_id": self.context_version_id,
            "expected_base_revision": self.expected_base_revision,
            "head_revision": self.head_revision,
            "integration_ref": self.integration_ref,
            "object_format": self.object_format,
            "policy": self.policy.to_dict(),
            "policy_revision": self.policy_revision,
            "project_id": self.project_id,
            "repository_identity": self.repository_identity,
            "schema_version": self.schema_version,
            "tree_revision": self.tree_revision,
            "work_thread_id": self.work_thread_id,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class ExactGateInputs:
    policy_id: str
    candidate_id: str
    repository_identity: str
    integration_ref: str
    expected_base_revision: str
    head_revision: str
    tree_revision: str
    object_format: str
    policy_revision: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.policy_id, str)
            or POLICY_ID_RE.fullmatch(self.policy_id) is None
        ):
            raise GateError("gate evidence policy_id is invalid")
        if (
            not isinstance(self.candidate_id, str)
            or GATE_ID_PATTERNS["candidate_id"].fullmatch(self.candidate_id) is None
        ):
            raise GateError("gate evidence candidate_id is invalid")
        if (
            not isinstance(self.repository_identity, str)
            or GATE_ID_PATTERNS["project_id"].fullmatch(self.repository_identity)
            is None
        ):
            raise GateError("gate evidence repository identity is invalid")
        if normalize_integration_ref(self.integration_ref) != self.integration_ref:
            raise GateError("gate evidence integration ref is not canonical")
        length = OBJECT_FORMAT_LENGTHS.get(self.object_format)
        if length is None:
            raise GateError("gate evidence object format is invalid")
        for field in (
            "expected_base_revision",
            "head_revision",
            "tree_revision",
        ):
            value = getattr(self, field)
            if (
                not isinstance(value, str)
                or OBJECT_ID_RE.fullmatch(value) is None
                or len(value) != length
            ):
                raise GateError(f"gate evidence {field} is invalid")
        if (
            not isinstance(self.policy_revision, str)
            or SHA256_RE.fullmatch(self.policy_revision) is None
        ):
            raise GateError("gate evidence policy revision is invalid")

    @classmethod
    def from_policy(
        cls, policy_id: str, manifest: GatePolicyManifest
    ) -> ExactGateInputs:
        if not isinstance(manifest, GatePolicyManifest):
            raise GateError("gate policy manifest is invalid")
        return cls(
            policy_id=policy_id,
            candidate_id=manifest.candidate_id,
            repository_identity=manifest.repository_identity,
            integration_ref=manifest.integration_ref,
            expected_base_revision=manifest.expected_base_revision,
            head_revision=manifest.head_revision,
            tree_revision=manifest.tree_revision,
            object_format=manifest.object_format,
            policy_revision=manifest.policy_revision,
        )

    @classmethod
    def from_mapping(cls, value: object) -> ExactGateInputs:
        if not isinstance(value, dict):
            raise GateError("gate evidence inputs must be an object")
        fields = {
            "policy_id",
            "candidate_id",
            "repository_identity",
            "integration_ref",
            "expected_base_revision",
            "head_revision",
            "tree_revision",
            "object_format",
            "policy_revision",
        }
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown or missing:
            detail = unknown if unknown else missing
            label = "unknown" if unknown else "missing"
            raise GateError(
                f"gate evidence inputs contain {label} fields: "
                + ", ".join(sorted(detail))
            )
        return cls(**{field: value[field] for field in fields})  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, str]:
        return {
            "candidate_id": self.candidate_id,
            "expected_base_revision": self.expected_base_revision,
            "head_revision": self.head_revision,
            "integration_ref": self.integration_ref,
            "object_format": self.object_format,
            "policy_id": self.policy_id,
            "policy_revision": self.policy_revision,
            "repository_identity": self.repository_identity,
            "tree_revision": self.tree_revision,
        }

    def assert_matches(self, manifest: GatePolicyManifest) -> None:
        if not isinstance(manifest, GatePolicyManifest):
            raise GateError("gate policy manifest is invalid")
        expected = ExactGateInputs.from_policy(self.policy_id, manifest)
        if self != expected:
            raise GateError("gate evidence does not match the exact policy inputs")


@dataclass(frozen=True)
class RunProvenance:
    run_id: str
    attempt_id: str
    profile_id: str
    profile_version_id: str
    identity_id: str
    requested_by: str

    def __post_init__(self) -> None:
        for field, pattern in PROVENANCE_ID_PATTERNS.items():
            value = getattr(self, field)
            if not isinstance(value, str) or pattern.fullmatch(value) is None:
                raise GateError(f"review {field} is invalid")

    @classmethod
    def from_mapping(cls, value: object) -> RunProvenance:
        if not isinstance(value, dict):
            raise GateError("review provenance must be an object")
        fields = set(PROVENANCE_ID_PATTERNS)
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown or missing:
            detail = unknown if unknown else missing
            label = "unknown" if unknown else "missing"
            raise GateError(
                f"review provenance contains {label} fields: "
                + ", ".join(sorted(detail))
            )
        return cls(**{field: value[field] for field in fields})  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, str]:
        return {
            "attempt_id": self.attempt_id,
            "identity_id": self.identity_id,
            "profile_id": self.profile_id,
            "profile_version_id": self.profile_version_id,
            "requested_by": self.requested_by,
            "run_id": self.run_id,
        }


def _schema_version(value: object) -> None:
    if isinstance(value, bool) or value != GATE_EVIDENCE_SCHEMA_VERSION:
        raise GateError(
            f"gate evidence schema_version must be {GATE_EVIDENCE_SCHEMA_VERSION}"
        )


def _outcome(value: object) -> GateOutcome:
    try:
        return GateOutcome(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise GateError("gate evidence outcome is invalid") from exc


@dataclass(frozen=True)
class ReviewEvidence:
    schema_version: int
    inputs: ExactGateInputs
    outcome: GateOutcome
    separation: ReviewSeparation
    producer: RunProvenance
    reviewer: RunProvenance

    def __post_init__(self) -> None:
        _schema_version(self.schema_version)
        if not isinstance(self.inputs, ExactGateInputs):
            raise GateError("review evidence inputs are invalid")
        if not isinstance(self.outcome, GateOutcome):
            raise GateError("review evidence outcome is invalid")
        if not isinstance(self.separation, ReviewSeparation):
            raise GateError("review evidence separation is invalid")
        if not isinstance(self.producer, RunProvenance) or not isinstance(
            self.reviewer, RunProvenance
        ):
            raise GateError("review evidence provenance is invalid")
        if self.reviewer.run_id == self.producer.run_id:
            raise GateError("reviewer run must differ from the producer run")
        if self.reviewer.attempt_id == self.producer.attempt_id:
            raise GateError("reviewer attempt must differ from the producer attempt")
        if (
            self.separation
            in {
                ReviewSeparation.DIFFERENT_PROFILE,
                ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
            }
            and self.reviewer.profile_id == self.producer.profile_id
        ):
            raise GateError("reviewer profile must differ from the producer profile")
        if (
            self.separation
            in {
                ReviewSeparation.DIFFERENT_IDENTITY,
                ReviewSeparation.DIFFERENT_PROFILE_AND_IDENTITY,
            }
            and self.reviewer.identity_id == self.producer.identity_id
        ):
            raise GateError("reviewer identity must differ from the producer identity")

    @classmethod
    def from_mapping(cls, value: object) -> ReviewEvidence:
        if not isinstance(value, dict):
            raise GateError("review evidence must be an object")
        fields = {
            "schema_version",
            "kind",
            "inputs",
            "outcome",
            "separation",
            "producer",
            "reviewer",
        }
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown or missing:
            detail = unknown if unknown else missing
            label = "unknown" if unknown else "missing"
            raise GateError(
                f"review evidence contains {label} fields: " + ", ".join(sorted(detail))
            )
        if value["kind"] != "independent_review":
            raise GateError("review evidence kind is invalid")
        try:
            separation = ReviewSeparation(value["separation"])
        except (TypeError, ValueError) as exc:
            raise GateError("review evidence separation is invalid") from exc
        return cls(
            schema_version=value["schema_version"],  # type: ignore[arg-type]
            inputs=ExactGateInputs.from_mapping(value["inputs"]),
            outcome=_outcome(value["outcome"]),
            separation=separation,
            producer=RunProvenance.from_mapping(value["producer"]),
            reviewer=RunProvenance.from_mapping(value["reviewer"]),
        )

    @property
    def gate_name(self) -> str:
        return "independent-review"

    def to_dict(self) -> dict[str, Any]:
        return {
            "inputs": self.inputs.to_dict(),
            "kind": "independent_review",
            "outcome": self.outcome.value,
            "producer": self.producer.to_dict(),
            "reviewer": self.reviewer.to_dict(),
            "schema_version": self.schema_version,
            "separation": self.separation.value,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class ValidationEvidence:
    schema_version: int
    inputs: ExactGateInputs
    outcome: GateOutcome
    validation_name: str
    image_id: str
    command_sha256: str
    sandbox_id: str
    exit_code: int
    duration_ms: int
    output_sha256: str
    output_bytes: int

    def __post_init__(self) -> None:
        _schema_version(self.schema_version)
        if not isinstance(self.inputs, ExactGateInputs):
            raise GateError("validation evidence inputs are invalid")
        if not isinstance(self.outcome, GateOutcome):
            raise GateError("validation evidence outcome is invalid")
        if (
            not isinstance(self.validation_name, str)
            or VALIDATION_NAME_RE.fullmatch(self.validation_name) is None
        ):
            raise GateError("validation evidence name is invalid")
        if (
            not isinstance(self.image_id, str)
            or IMAGE_ID_RE.fullmatch(self.image_id) is None
        ):
            raise GateError("validation evidence image is invalid")
        if (
            not isinstance(self.command_sha256, str)
            or SHA256_RE.fullmatch(self.command_sha256) is None
        ):
            raise GateError("validation evidence command digest is invalid")
        if (
            not isinstance(self.sandbox_id, str)
            or SANDBOX_ID_RE.fullmatch(self.sandbox_id) is None
        ):
            raise GateError("validation evidence sandbox ID is invalid")
        if (
            isinstance(self.exit_code, bool)
            or not isinstance(self.exit_code, int)
            or not 0 <= self.exit_code <= 255
        ):
            raise GateError("validation evidence exit code is invalid")
        expected = GateOutcome.PASSED if self.exit_code == 0 else GateOutcome.FAILED
        if self.outcome is not expected:
            raise GateError("validation outcome does not match its exit code")
        if (
            isinstance(self.duration_ms, bool)
            or not isinstance(self.duration_ms, int)
            or not 0 <= self.duration_ms <= MAX_VALIDATION_DURATION_MS
        ):
            raise GateError("validation evidence duration is invalid")
        if (
            not isinstance(self.output_sha256, str)
            or SHA256_RE.fullmatch(self.output_sha256) is None
        ):
            raise GateError("validation evidence output digest is invalid")
        if (
            isinstance(self.output_bytes, bool)
            or not isinstance(self.output_bytes, int)
            or not 0 <= self.output_bytes <= MAX_VALIDATION_OUTPUT_BYTES
        ):
            raise GateError("validation evidence output size is invalid")

    @classmethod
    def from_mapping(cls, value: object) -> ValidationEvidence:
        if not isinstance(value, dict):
            raise GateError("validation evidence must be an object")
        fields = {
            "schema_version",
            "kind",
            "inputs",
            "outcome",
            "validation_name",
            "image_id",
            "command_sha256",
            "sandbox_id",
            "exit_code",
            "duration_ms",
            "output_sha256",
            "output_bytes",
        }
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown or missing:
            detail = unknown if unknown else missing
            label = "unknown" if unknown else "missing"
            raise GateError(
                f"validation evidence contains {label} fields: "
                + ", ".join(sorted(detail))
            )
        if value["kind"] != "local_validation":
            raise GateError("validation evidence kind is invalid")
        return cls(
            schema_version=value["schema_version"],  # type: ignore[arg-type]
            inputs=ExactGateInputs.from_mapping(value["inputs"]),
            outcome=_outcome(value["outcome"]),
            validation_name=value["validation_name"],  # type: ignore[arg-type]
            image_id=value["image_id"],  # type: ignore[arg-type]
            command_sha256=value["command_sha256"],  # type: ignore[arg-type]
            sandbox_id=value["sandbox_id"],  # type: ignore[arg-type]
            exit_code=value["exit_code"],  # type: ignore[arg-type]
            duration_ms=value["duration_ms"],  # type: ignore[arg-type]
            output_sha256=value["output_sha256"],  # type: ignore[arg-type]
            output_bytes=value["output_bytes"],  # type: ignore[arg-type]
        )

    @property
    def gate_name(self) -> str:
        return f"validation:{self.validation_name}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "command_sha256": self.command_sha256,
            "duration_ms": self.duration_ms,
            "exit_code": self.exit_code,
            "image_id": self.image_id,
            "inputs": self.inputs.to_dict(),
            "kind": "local_validation",
            "outcome": self.outcome.value,
            "output_bytes": self.output_bytes,
            "output_sha256": self.output_sha256,
            "sandbox_id": self.sandbox_id,
            "schema_version": self.schema_version,
            "validation_name": self.validation_name,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


GateEvidence = ReviewEvidence | ValidationEvidence


def parse_gate_evidence(value: object) -> GateEvidence:
    if not isinstance(value, dict):
        raise GateError("gate evidence must be an object")
    kind = value.get("kind")
    if kind == "independent_review":
        return ReviewEvidence.from_mapping(value)
    if kind == "local_validation":
        return ValidationEvidence.from_mapping(value)
    raise GateError("gate evidence kind is invalid")


def resolve_gate_policy_request(
    value: object,
    *,
    image_resolver: Callable[[str], object],
) -> GatePolicySpec:
    if not isinstance(value, dict):
        raise GateError("gate policy request must be an object")
    fields = {"schema_version", "review_separation", "validations"}
    unknown = set(value) - fields
    missing = fields - set(value)
    if unknown:
        raise GateError(
            "gate policy request contains unknown fields: " + ", ".join(sorted(unknown))
        )
    if missing:
        raise GateError(
            "gate policy request is missing fields: " + ", ".join(sorted(missing))
        )
    schema_version = value["schema_version"]
    if isinstance(schema_version, bool) or schema_version != GATE_POLICY_SCHEMA_VERSION:
        raise GateError(
            f"gate policy schema_version must be {GATE_POLICY_SCHEMA_VERSION}"
        )
    raw_validations = value["validations"]
    if not isinstance(raw_validations, list) or len(raw_validations) > (
        MAX_VALIDATION_GATES
    ):
        raise GateError("gate policy validations must be a bounded list")
    validations: list[ValidationGate] = []
    for raw in raw_validations:
        if not isinstance(raw, dict):
            raise GateError("validation gate request must be an object")
        validation_fields = {"name", "image", "command"}
        validation_unknown = set(raw) - validation_fields
        validation_missing = validation_fields - set(raw)
        if validation_unknown:
            raise GateError(
                "validation gate request contains unknown fields: "
                + ", ".join(sorted(validation_unknown))
            )
        if validation_missing:
            raise GateError(
                "validation gate request is missing fields: "
                + ", ".join(sorted(validation_missing))
            )
        image_reference = _bounded_text(
            raw["image"], field="validation image", max_bytes=512
        )
        resolved = image_resolver(image_reference)
        image_id = getattr(resolved, "image_id", None)
        declared_volumes = getattr(resolved, "declared_volumes", None)
        if not isinstance(declared_volumes, tuple):
            raise GateError("validation image resolver returned an invalid result")
        if declared_volumes:
            raise GateError("validation image declares writable volumes")
        validations.append(
            ValidationGate(
                name=raw["name"],  # type: ignore[arg-type]
                image_id=image_id,
                command=_command(raw["command"]),
            )
        )
    return GatePolicySpec.from_mapping(
        {
            "schema_version": schema_version,
            "review_separation": value["review_separation"],
            "validations": [validation.to_dict() for validation in validations],
        }
    )
