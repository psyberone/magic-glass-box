from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from .candidates import CandidateManifest, MaterializedCandidate
from .domain import ValidationError, canonical_json, content_sha256, require_string
from .gates import GateError, normalize_integration_ref

INTEGRATION_FOLD_SCHEMA_VERSION = 1
LANDING_PLAN_SCHEMA_VERSION = 1
LANDING_RECEIPT_SCHEMA_VERSION = 1

OBJECT_FORMAT_LENGTHS = {"sha1": 40, "sha256": 64}
OBJECT_ID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
NONCE_RE = re.compile(r"^[0-9a-f]{64}$")
REMOTE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
ID_PATTERNS = {
    "candidate_id": re.compile(r"^cnd_[0-9a-f]{32}$"),
    "context_version_id": re.compile(r"^ctx_[0-9a-f]{32}$"),
    "fold_id": re.compile(r"^ifd_[0-9a-f]{32}$"),
    "plan_id": re.compile(r"^lpl_[0-9a-f]{32}$"),
    "policy_id": re.compile(r"^gpl_[0-9a-f]{32}$"),
    "project_id": re.compile(r"^boxp_[0-9a-f]{32}$"),
    "queue_item_id": re.compile(r"^iqm_[0-9a-f]{32}$"),
    "work_thread_id": re.compile(r"^wth_[0-9a-f]{32}$"),
}


class IntegrationError(RuntimeError):
    """An integration dependency, fold, plan, or receipt is invalid."""


class LandingAssurance(StrEnum):
    BASE_AND_HEAD_ENFORCED = "base_and_head_enforced"
    OPERATOR_ATTESTED = "operator_attested"


class LandingOutcome(StrEnum):
    LANDED = "landed"
    NOT_LANDED = "not_landed"


def _schema_version(value: object, *, expected: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value != expected:
        raise IntegrationError(f"{label} schema_version must be {expected}")
    return value


def _identifier(value: object, *, field: str) -> str:
    pattern = ID_PATTERNS[field]
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise IntegrationError(f"{field.replace('_', ' ')} is invalid")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise IntegrationError(f"{field} is invalid")
    return value


def _object_format(value: object) -> str:
    if not isinstance(value, str) or value not in OBJECT_FORMAT_LENGTHS:
        raise IntegrationError("object format is invalid")
    return value


def _object_id(value: object, *, field: str, object_format: str) -> str:
    length = OBJECT_FORMAT_LENGTHS[object_format]
    if (
        not isinstance(value, str)
        or OBJECT_ID_RE.fullmatch(value) is None
        or len(value) != length
    ):
        raise IntegrationError(f"{field} is invalid")
    return value


def _integration_ref(value: object) -> str:
    try:
        normalized = normalize_integration_ref(value)
    except (GateError, TypeError, ValueError) as exc:
        raise IntegrationError(str(exc)) from exc
    if normalized != value:
        raise IntegrationError("integration ref is not canonical")
    return normalized


def _strict_fields(value: object, *, label: str, fields: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise IntegrationError(f"{label} must be an object")
    unknown = set(value) - fields
    missing = fields - set(value)
    if unknown:
        raise IntegrationError(
            f"{label} contains unknown fields: " + ", ".join(sorted(unknown))
        )
    if missing:
        raise IntegrationError(
            f"{label} is missing fields: " + ", ".join(sorted(missing))
        )
    return value


@dataclass(frozen=True)
class FoldManifest:
    schema_version: int
    candidate_id: str
    project_id: str
    context_version_id: str
    work_thread_id: str
    integration_ref: str
    generation: int
    base_revision: str
    head_revision: str
    tree_revision: str
    object_format: str
    result_content_sha256: str

    def __post_init__(self) -> None:
        _schema_version(
            self.schema_version,
            expected=INTEGRATION_FOLD_SCHEMA_VERSION,
            label="integration fold",
        )
        for field in (
            "candidate_id",
            "project_id",
            "context_version_id",
            "work_thread_id",
        ):
            _identifier(getattr(self, field), field=field)
        _integration_ref(self.integration_ref)
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 1
        ):
            raise IntegrationError("fold generation must be a positive integer")
        _object_format(self.object_format)
        for field in ("base_revision", "head_revision", "tree_revision"):
            _object_id(
                getattr(self, field), field=field, object_format=self.object_format
            )
        _sha256(self.result_content_sha256, field="result content digest")

    @classmethod
    def create(
        cls,
        candidate: object,
        *,
        integration_ref: object,
        generation: int,
        materialized: MaterializedCandidate,
    ) -> FoldManifest:
        if not isinstance(candidate, dict):
            raise IntegrationError("candidate record must be an object")
        required = {
            "id",
            "project_id",
            "context_version_id",
            "work_thread_id",
            "result_content_sha256",
        }
        missing = required - set(candidate)
        if missing:
            raise IntegrationError(
                "candidate record is missing fields: " + ", ".join(sorted(missing))
            )
        if not isinstance(materialized, MaterializedCandidate):
            raise IntegrationError("fold materialization is invalid")
        return cls(
            schema_version=INTEGRATION_FOLD_SCHEMA_VERSION,
            candidate_id=candidate["id"],  # type: ignore[arg-type]
            project_id=candidate["project_id"],  # type: ignore[arg-type]
            context_version_id=candidate["context_version_id"],  # type: ignore[arg-type]
            work_thread_id=candidate["work_thread_id"],  # type: ignore[arg-type]
            integration_ref=_integration_ref(integration_ref),
            generation=generation,
            base_revision=materialized.base_revision,
            head_revision=materialized.head_revision,
            tree_revision=materialized.tree_revision,
            object_format=materialized.object_format,
            result_content_sha256=candidate["result_content_sha256"],  # type: ignore[arg-type]
        )

    @classmethod
    def from_mapping(cls, value: object) -> FoldManifest:
        fields = {
            "schema_version",
            "candidate_id",
            "project_id",
            "context_version_id",
            "work_thread_id",
            "integration_ref",
            "generation",
            "base_revision",
            "head_revision",
            "tree_revision",
            "object_format",
            "result_content_sha256",
        }
        document = _strict_fields(value, label="integration fold", fields=fields)
        return cls(**{field: document[field] for field in fields})  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_revision": self.base_revision,
            "candidate_id": self.candidate_id,
            "context_version_id": self.context_version_id,
            "generation": self.generation,
            "head_revision": self.head_revision,
            "integration_ref": self.integration_ref,
            "object_format": self.object_format,
            "project_id": self.project_id,
            "result_content_sha256": self.result_content_sha256,
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

    def to_candidate_manifest(self, origin: CandidateManifest) -> CandidateManifest:
        if not isinstance(origin, CandidateManifest):
            raise IntegrationError("origin candidate manifest is invalid")
        expected = {
            "project_id": self.project_id,
            "context_version_id": self.context_version_id,
            "work_thread_id": self.work_thread_id,
            "object_format": self.object_format,
            "result_content_sha256": self.result_content_sha256,
        }
        if any(getattr(origin, field) != value for field, value in expected.items()):
            raise IntegrationError("fold does not match its origin candidate")
        return CandidateManifest(
            schema_version=origin.schema_version,
            attempt_id=origin.attempt_id,
            attempt_result_id=origin.attempt_result_id,
            producer_run_id=origin.producer_run_id,
            project_id=origin.project_id,
            context_version_id=origin.context_version_id,
            work_thread_id=origin.work_thread_id,
            base_revision=self.base_revision,
            result_content_sha256=origin.result_content_sha256,
            head_revision=self.head_revision,
            tree_revision=self.tree_revision,
            object_format=self.object_format,
        )


@dataclass(frozen=True)
class LandingPlan:
    schema_version: int
    queue_item_id: str
    queue_version: int
    policy_id: str
    policy_revision: str
    candidate_id: str
    fold_id: str | None
    repository_identity: str
    integration_ref: str
    expected_base_revision: str
    head_revision: str
    tree_revision: str
    object_format: str
    evidence_snapshot_sha256: str
    actuator: str
    assurance: LandingAssurance
    candidate_repository: str
    remote: str
    commands: tuple[tuple[str, ...], ...]

    def __post_init__(self) -> None:
        _schema_version(
            self.schema_version,
            expected=LANDING_PLAN_SCHEMA_VERSION,
            label="landing plan",
        )
        _identifier(self.queue_item_id, field="queue_item_id")
        if (
            isinstance(self.queue_version, bool)
            or not isinstance(self.queue_version, int)
            or self.queue_version < 1
        ):
            raise IntegrationError("landing plan queue version is invalid")
        _identifier(self.policy_id, field="policy_id")
        _sha256(self.policy_revision, field="policy revision")
        _identifier(self.candidate_id, field="candidate_id")
        if self.fold_id is not None:
            _identifier(self.fold_id, field="fold_id")
        _identifier(self.repository_identity, field="project_id")
        _integration_ref(self.integration_ref)
        _object_format(self.object_format)
        for field in (
            "expected_base_revision",
            "head_revision",
            "tree_revision",
        ):
            _object_id(
                getattr(self, field), field=field, object_format=self.object_format
            )
        _sha256(self.evidence_snapshot_sha256, field="evidence snapshot digest")
        if self.actuator != "git-force-with-lease-v1":
            raise IntegrationError("landing actuator is invalid")
        if self.assurance is not LandingAssurance.BASE_AND_HEAD_ENFORCED:
            raise IntegrationError("landing plan assurance is invalid")
        try:
            repository = require_string(
                self.candidate_repository,
                field="candidate repository",
                max_bytes=4096,
            )
            remote = require_string(self.remote, field="remote", max_bytes=128)
        except ValidationError as exc:
            raise IntegrationError(str(exc)) from exc
        if not Path(repository).is_absolute():
            raise IntegrationError("candidate repository must be an absolute path")
        if REMOTE_NAME_RE.fullmatch(remote) is None:
            raise IntegrationError("remote must be a configured remote name")
        expected_commands = self._commands(
            repository=repository,
            remote=remote,
            integration_ref=self.integration_ref,
            expected_base_revision=self.expected_base_revision,
            head_revision=self.head_revision,
        )
        if self.commands != expected_commands:
            raise IntegrationError("landing plan commands do not match exact inputs")

    @staticmethod
    def _commands(
        *,
        repository: str,
        remote: str,
        integration_ref: str,
        expected_base_revision: str,
        head_revision: str,
    ) -> tuple[tuple[str, ...], ...]:
        return (
            ("git", "fetch", "--no-tags", repository, head_revision),
            (
                "git",
                "push",
                f"--force-with-lease={integration_ref}:{expected_base_revision}",
                remote,
                f"{head_revision}:{integration_ref}",
            ),
        )

    @classmethod
    def create(
        cls,
        *,
        queue_item_id: str,
        queue_version: int,
        policy_id: str,
        policy_revision: str,
        candidate_id: str,
        fold_id: str | None,
        repository_identity: str,
        integration_ref: str,
        expected_base_revision: str,
        head_revision: str,
        tree_revision: str,
        object_format: str,
        evidence_snapshot_sha256: str,
        candidate_repository: Path,
        remote: str,
    ) -> LandingPlan:
        repository = str(Path(candidate_repository).expanduser().absolute())
        normalized_ref = _integration_ref(integration_ref)
        return cls(
            schema_version=LANDING_PLAN_SCHEMA_VERSION,
            queue_item_id=queue_item_id,
            queue_version=queue_version,
            policy_id=policy_id,
            policy_revision=policy_revision,
            candidate_id=candidate_id,
            fold_id=fold_id,
            repository_identity=repository_identity,
            integration_ref=normalized_ref,
            expected_base_revision=expected_base_revision,
            head_revision=head_revision,
            tree_revision=tree_revision,
            object_format=object_format,
            evidence_snapshot_sha256=evidence_snapshot_sha256,
            actuator="git-force-with-lease-v1",
            assurance=LandingAssurance.BASE_AND_HEAD_ENFORCED,
            candidate_repository=repository,
            remote=remote,
            commands=cls._commands(
                repository=repository,
                remote=remote,
                integration_ref=normalized_ref,
                expected_base_revision=expected_base_revision,
                head_revision=head_revision,
            ),
        )

    @classmethod
    def from_mapping(cls, value: object) -> LandingPlan:
        fields = {
            "schema_version",
            "queue_item_id",
            "queue_version",
            "policy_id",
            "policy_revision",
            "candidate_id",
            "fold_id",
            "repository_identity",
            "integration_ref",
            "expected_base_revision",
            "head_revision",
            "tree_revision",
            "object_format",
            "evidence_snapshot_sha256",
            "actuator",
            "assurance",
            "candidate_repository",
            "remote",
            "commands",
        }
        document = _strict_fields(value, label="landing plan", fields=fields)
        raw_commands = document["commands"]
        if not isinstance(raw_commands, list) or not all(
            isinstance(command, list)
            and command
            and all(isinstance(argument, str) for argument in command)
            for command in raw_commands
        ):
            raise IntegrationError("landing plan commands must be argv arrays")
        try:
            assurance = LandingAssurance(document["assurance"])
        except (TypeError, ValueError) as exc:
            raise IntegrationError("landing plan assurance is invalid") from exc
        return cls(
            schema_version=document["schema_version"],
            queue_item_id=document["queue_item_id"],
            queue_version=document["queue_version"],
            policy_id=document["policy_id"],
            policy_revision=document["policy_revision"],
            candidate_id=document["candidate_id"],
            fold_id=document["fold_id"],
            repository_identity=document["repository_identity"],
            integration_ref=document["integration_ref"],
            expected_base_revision=document["expected_base_revision"],
            head_revision=document["head_revision"],
            tree_revision=document["tree_revision"],
            object_format=document["object_format"],
            evidence_snapshot_sha256=document["evidence_snapshot_sha256"],
            actuator=document["actuator"],
            assurance=assurance,
            candidate_repository=document["candidate_repository"],
            remote=document["remote"],
            commands=tuple(tuple(command) for command in raw_commands),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "actuator": self.actuator,
            "assurance": self.assurance.value,
            "candidate_id": self.candidate_id,
            "candidate_repository": self.candidate_repository,
            "commands": [list(command) for command in self.commands],
            "evidence_snapshot_sha256": self.evidence_snapshot_sha256,
            "expected_base_revision": self.expected_base_revision,
            "fold_id": self.fold_id,
            "head_revision": self.head_revision,
            "integration_ref": self.integration_ref,
            "object_format": self.object_format,
            "policy_id": self.policy_id,
            "policy_revision": self.policy_revision,
            "queue_item_id": self.queue_item_id,
            "queue_version": self.queue_version,
            "remote": self.remote,
            "repository_identity": self.repository_identity,
            "schema_version": self.schema_version,
            "tree_revision": self.tree_revision,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class LandingReceiptRequest:
    schema_version: int
    plan_id: str
    request_sha256: str
    nonce: str
    outcome: LandingOutcome
    observed_revision: str

    def __post_init__(self) -> None:
        _schema_version(
            self.schema_version,
            expected=LANDING_RECEIPT_SCHEMA_VERSION,
            label="landing receipt request",
        )
        _identifier(self.plan_id, field="plan_id")
        _sha256(self.request_sha256, field="landing request digest")
        if not isinstance(self.nonce, str) or NONCE_RE.fullmatch(self.nonce) is None:
            raise IntegrationError("landing receipt nonce is invalid")
        if not isinstance(self.outcome, LandingOutcome):
            raise IntegrationError("landing receipt outcome is invalid")
        if (
            not isinstance(self.observed_revision, str)
            or OBJECT_ID_RE.fullmatch(self.observed_revision) is None
        ):
            raise IntegrationError("landing receipt observed revision is invalid")

    @classmethod
    def from_mapping(cls, value: object) -> LandingReceiptRequest:
        fields = {
            "schema_version",
            "plan_id",
            "request_sha256",
            "nonce",
            "outcome",
            "observed_revision",
        }
        document = _strict_fields(value, label="landing receipt request", fields=fields)
        try:
            outcome = LandingOutcome(document["outcome"])
        except (TypeError, ValueError) as exc:
            raise IntegrationError("landing receipt outcome is invalid") from exc
        return cls(
            schema_version=document["schema_version"],
            plan_id=document["plan_id"],
            request_sha256=document["request_sha256"],
            nonce=document["nonce"],
            outcome=outcome,
            observed_revision=document["observed_revision"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "nonce": self.nonce,
            "observed_revision": self.observed_revision,
            "outcome": self.outcome.value,
            "plan_id": self.plan_id,
            "request_sha256": self.request_sha256,
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True)
class LandingReceipt:
    schema_version: int
    plan_id: str
    request_sha256: str
    nonce_sha256: str
    outcome: LandingOutcome
    observed_revision: str
    actuator: str
    assurance: LandingAssurance

    def __post_init__(self) -> None:
        _schema_version(
            self.schema_version,
            expected=LANDING_RECEIPT_SCHEMA_VERSION,
            label="landing receipt",
        )
        _identifier(self.plan_id, field="plan_id")
        _sha256(self.request_sha256, field="landing request digest")
        _sha256(self.nonce_sha256, field="landing nonce digest")
        if not isinstance(self.outcome, LandingOutcome):
            raise IntegrationError("landing receipt outcome is invalid")
        if (
            not isinstance(self.observed_revision, str)
            or OBJECT_ID_RE.fullmatch(self.observed_revision) is None
        ):
            raise IntegrationError("landing receipt observed revision is invalid")
        if self.actuator != "git-force-with-lease-v1":
            raise IntegrationError("landing receipt actuator is invalid")
        if not isinstance(self.assurance, LandingAssurance):
            raise IntegrationError("landing receipt assurance is invalid")

    @classmethod
    def from_request(
        cls,
        request: LandingReceiptRequest,
        *,
        plan_id: str,
        plan: LandingPlan,
    ) -> LandingReceipt:
        if not isinstance(request, LandingReceiptRequest):
            raise IntegrationError("landing receipt request is invalid")
        _identifier(plan_id, field="plan_id")
        if not isinstance(plan, LandingPlan):
            raise IntegrationError("landing plan is invalid")
        if request.plan_id != plan_id:
            raise IntegrationError("landing receipt belongs to another plan")
        if request.request_sha256 != plan.digest:
            raise IntegrationError("landing receipt request digest does not match")
        _object_id(
            request.observed_revision,
            field="landing receipt observed revision",
            object_format=plan.object_format,
        )
        if (
            request.outcome is LandingOutcome.LANDED
            and request.observed_revision != plan.head_revision
        ):
            raise IntegrationError("landing receipt observed revision is invalid")
        if (
            request.outcome is LandingOutcome.NOT_LANDED
            and request.observed_revision == plan.head_revision
        ):
            raise IntegrationError("landing receipt observed revision is invalid")
        return cls(
            schema_version=LANDING_RECEIPT_SCHEMA_VERSION,
            plan_id=plan_id,
            request_sha256=request.request_sha256,
            nonce_sha256=hashlib.sha256(request.nonce.encode("ascii")).hexdigest(),
            outcome=request.outcome,
            observed_revision=request.observed_revision,
            actuator=plan.actuator,
            assurance=LandingAssurance.OPERATOR_ATTESTED,
        )

    @classmethod
    def from_mapping(cls, value: object) -> LandingReceipt:
        fields = {
            "schema_version",
            "plan_id",
            "request_sha256",
            "nonce_sha256",
            "outcome",
            "observed_revision",
            "actuator",
            "assurance",
        }
        document = _strict_fields(value, label="landing receipt", fields=fields)
        try:
            outcome = LandingOutcome(document["outcome"])
            assurance = LandingAssurance(document["assurance"])
        except (TypeError, ValueError) as exc:
            raise IntegrationError("landing receipt enum value is invalid") from exc
        return cls(
            schema_version=document["schema_version"],
            plan_id=document["plan_id"],
            request_sha256=document["request_sha256"],
            nonce_sha256=document["nonce_sha256"],
            outcome=outcome,
            observed_revision=document["observed_revision"],
            actuator=document["actuator"],
            assurance=assurance,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "actuator": self.actuator,
            "assurance": self.assurance.value,
            "nonce_sha256": self.nonce_sha256,
            "observed_revision": self.observed_revision,
            "outcome": self.outcome.value,
            "plan_id": self.plan_id,
            "request_sha256": self.request_sha256,
            "schema_version": self.schema_version,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)
