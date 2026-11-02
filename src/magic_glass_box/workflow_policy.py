from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .domain import ValidationError, canonical_json, content_sha256
from .service_sidecar import core_service_capability
from .workflows import IMPLEMENTATION_TEMPLATE_KEY, IMPLEMENTATION_TEMPLATE_REVISION

WORKFLOW_POLICY_SCHEMA_VERSION = 1
WORKFLOW_ACTIVATION_SCHEMA_VERSION = 1
MAX_POLICY_ITEMS = 32
MAX_REVIEW_ROUNDS = 8
MAX_SEAT_COUNT = 16
MAX_SHARED_HOST_CONCURRENCY = 64
MIN_WALL_TIME_SECONDS = 60
MAX_WALL_TIME_SECONDS = 7 * 24 * 60 * 60

NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
WORKFLOW_CONFIGURATION_VERSION_ID_RE = re.compile(r"^wfv_[0-9a-f]{32}$")
CAPABILITY_BINDING_ID_RE = re.compile(r"^cpb_[0-9a-f]{32}$")
ROLE_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
RULE_RE = re.compile(r"^[a-z][a-z0-9_-]{0,127}$")

EVIDENCE_FLOOR = frozenset(
    {"maintainer_disposition", "reviewed_content", "validated_execution"}
)
LANDING_METHODS = frozenset({"merge", "rebase", "squash"})


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


def _schema_version(value: object, *, expected: int, subject: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value != expected:
        raise ValidationError(f"{subject} schema_version must be {expected}")
    return value


def _bounded_integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return value


def _identifier(value: object, *, field: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValidationError(f"{field} is invalid")
    return value


def _bounded_unique_strings(
    value: object,
    *,
    field: str,
    pattern: re.Pattern[str],
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or (not allow_empty and not value)
        or len(value) > MAX_POLICY_ITEMS
    ):
        minimum = "zero" if allow_empty else "one"
        raise ValidationError(
            f"{field} must contain between {minimum} and {MAX_POLICY_ITEMS} entries"
        )
    parsed = tuple(
        _identifier(item, field=f"{field} entry", pattern=pattern) for item in value
    )
    if len(set(parsed)) != len(parsed):
        raise ValidationError(f"{field} must be unique")
    return tuple(sorted(parsed))


@dataclass(frozen=True)
class WorkflowResourceBounds:
    review_rounds: int
    seat_count: int
    shared_host_concurrency: int
    wall_time_seconds: int

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any], *, subject: str
    ) -> WorkflowResourceBounds:
        if not isinstance(value, dict):
            raise ValidationError(f"{subject} must be an object")
        _exact_fields(
            value,
            expected={
                "review_rounds",
                "seat_count",
                "shared_host_concurrency",
                "wall_time_seconds",
            },
            subject=subject,
        )
        return cls(
            review_rounds=_bounded_integer(
                value["review_rounds"],
                field=f"{subject} review_rounds",
                minimum=1,
                maximum=MAX_REVIEW_ROUNDS,
            ),
            seat_count=_bounded_integer(
                value["seat_count"],
                field=f"{subject} seat_count",
                minimum=1,
                maximum=MAX_SEAT_COUNT,
            ),
            shared_host_concurrency=_bounded_integer(
                value["shared_host_concurrency"],
                field=f"{subject} shared_host_concurrency",
                minimum=1,
                maximum=MAX_SHARED_HOST_CONCURRENCY,
            ),
            wall_time_seconds=_bounded_integer(
                value["wall_time_seconds"],
                field=f"{subject} wall_time_seconds",
                minimum=MIN_WALL_TIME_SECONDS,
                maximum=MAX_WALL_TIME_SECONDS,
            ),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "review_rounds": self.review_rounds,
            "seat_count": self.seat_count,
            "shared_host_concurrency": self.shared_host_concurrency,
            "wall_time_seconds": self.wall_time_seconds,
        }


@dataclass(frozen=True)
class WorkflowPrescriptionRef:
    key: str
    revision: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowPrescriptionRef:
        if not isinstance(value, dict):
            raise ValidationError("workflow policy prescription must be an object")
        _exact_fields(
            value,
            expected={"key", "revision"},
            subject="workflow policy prescription",
        )
        if value["key"] != IMPLEMENTATION_TEMPLATE_KEY:
            raise ValidationError(
                "workflow policy prescription must be implementation-v1"
            )
        revision = _identifier(
            value["revision"],
            field="workflow policy prescription revision",
            pattern=SHA256_RE,
        )
        if revision != IMPLEMENTATION_TEMPLATE_REVISION:
            raise ValidationError("workflow policy prescription revision is unknown")
        return cls(key=IMPLEMENTATION_TEMPLATE_KEY, revision=revision)

    def to_dict(self) -> dict[str, str]:
        return {"key": self.key, "revision": self.revision}


@dataclass(frozen=True)
class WorkflowPolicySpec:
    schema_version: int
    prescription: WorkflowPrescriptionRef
    evidence_floor: tuple[str, ...]
    validation_obligations: tuple[str, ...]
    resource_maxima: WorkflowResourceBounds
    wake_mode: str
    required_capabilities: tuple[tuple[str, int, str], ...]
    landing_methods: tuple[str, ...]
    allow_deferred_validation: bool

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowPolicySpec:
        if not isinstance(value, dict):
            raise ValidationError("workflow policy must be an object")
        fields = {
            "schema_version",
            "prescription",
            "evidence_floor",
            "validation_obligations",
            "resource_maxima",
            "wake_mode",
            "required_capabilities",
            "landing_methods",
            "allow_deferred_validation",
        }
        _exact_fields(value, expected=fields, subject="workflow policy")
        evidence_floor = _bounded_unique_strings(
            value["evidence_floor"],
            field="workflow policy evidence_floor",
            pattern=NAME_RE,
        )
        if set(evidence_floor) != EVIDENCE_FLOOR:
            raise ValidationError(
                "workflow policy evidence_floor must require reviewed content, "
                "validated execution, and maintainer disposition"
            )
        validation_obligations = _bounded_unique_strings(
            value["validation_obligations"],
            field="workflow policy validation_obligations",
            pattern=NAME_RE,
        )
        raw_capabilities = value["required_capabilities"]
        if (
            not isinstance(raw_capabilities, list)
            or not raw_capabilities
            or len(raw_capabilities) > MAX_POLICY_ITEMS
        ):
            raise ValidationError(
                "workflow policy required_capabilities must contain between one "
                f"and {MAX_POLICY_ITEMS} entries"
            )
        capabilities: list[tuple[str, int, str]] = []
        for raw in raw_capabilities:
            if not isinstance(raw, dict):
                raise ValidationError(
                    "workflow policy required capability must be an object"
                )
            _exact_fields(
                raw,
                expected={"mode", "name", "version"},
                subject="workflow policy required capability",
            )
            capability = core_service_capability(raw["name"], raw["version"])
            if raw["mode"] != capability.mode:
                raise ValidationError(
                    "workflow policy required capability mode does not match"
                )
            capabilities.append((capability.name, capability.version, capability.mode))
        if len(set(capabilities)) != len(capabilities):
            raise ValidationError(
                "workflow policy required_capabilities must be unique"
            )
        landing_methods = _bounded_unique_strings(
            value["landing_methods"],
            field="workflow policy landing_methods",
            pattern=NAME_RE,
        )
        if not set(landing_methods).issubset(LANDING_METHODS):
            raise ValidationError("workflow policy landing method is unsupported")
        if value["wake_mode"] != "manual":
            raise ValidationError("workflow policy wake_mode must be manual")
        allow_deferred_validation = value["allow_deferred_validation"]
        if not isinstance(allow_deferred_validation, bool):
            raise ValidationError(
                "workflow policy allow_deferred_validation must be boolean"
            )
        return cls(
            schema_version=_schema_version(
                value["schema_version"],
                expected=WORKFLOW_POLICY_SCHEMA_VERSION,
                subject="workflow policy",
            ),
            prescription=WorkflowPrescriptionRef.from_mapping(value["prescription"]),
            evidence_floor=evidence_floor,
            validation_obligations=validation_obligations,
            resource_maxima=WorkflowResourceBounds.from_mapping(
                value["resource_maxima"], subject="workflow policy resource_maxima"
            ),
            wake_mode="manual",
            required_capabilities=tuple(sorted(capabilities)),
            landing_methods=landing_methods,
            allow_deferred_validation=allow_deferred_validation,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "prescription": self.prescription.to_dict(),
            "evidence_floor": list(self.evidence_floor),
            "validation_obligations": list(self.validation_obligations),
            "resource_maxima": self.resource_maxima.to_dict(),
            "wake_mode": self.wake_mode,
            "required_capabilities": [
                {"mode": mode, "name": name, "version": version}
                for name, version, mode in self.required_capabilities
            ],
            "landing_methods": list(self.landing_methods),
            "allow_deferred_validation": self.allow_deferred_validation,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class WorkflowValidationBinding:
    obligation: str
    validation: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowValidationBinding:
        if not isinstance(value, dict):
            raise ValidationError("workflow validation binding must be an object")
        _exact_fields(
            value,
            expected={"obligation", "validation"},
            subject="workflow validation binding",
        )
        return cls(
            obligation=_identifier(
                value["obligation"],
                field="workflow validation binding obligation",
                pattern=NAME_RE,
            ),
            validation=_identifier(
                value["validation"],
                field="workflow validation binding validation",
                pattern=NAME_RE,
            ),
        )

    def to_dict(self) -> dict[str, str]:
        return {"obligation": self.obligation, "validation": self.validation}


@dataclass(frozen=True)
class WorkflowCapabilityBinding:
    binding_id: str
    name: str
    version: int
    mode: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowCapabilityBinding:
        if not isinstance(value, dict):
            raise ValidationError("workflow capability binding must be an object")
        _exact_fields(
            value,
            expected={"binding_id", "mode", "name", "version"},
            subject="workflow capability binding",
        )
        capability = core_service_capability(value["name"], value["version"])
        if value["mode"] != capability.mode:
            raise ValidationError("workflow capability binding mode does not match")
        return cls(
            binding_id=_identifier(
                value["binding_id"],
                field="workflow capability binding ID",
                pattern=CAPABILITY_BINDING_ID_RE,
            ),
            name=capability.name,
            version=capability.version,
            mode=capability.mode,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "binding_id": self.binding_id,
            "mode": self.mode,
            "name": self.name,
            "version": self.version,
        }


@dataclass(frozen=True)
class WorkflowWakeBinding:
    role: str
    wake_class: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowWakeBinding:
        if not isinstance(value, dict):
            raise ValidationError("workflow wake binding must be an object")
        _exact_fields(
            value,
            expected={"role", "wake_class"},
            subject="workflow wake binding",
        )
        if value["wake_class"] != "human_turn_only":
            raise ValidationError(
                "workflow activation wake_class must be human_turn_only"
            )
        return cls(
            role=_identifier(
                value["role"], field="workflow wake binding role", pattern=ROLE_RE
            ),
            wake_class="human_turn_only",
        )

    def to_dict(self) -> dict[str, str]:
        return {"role": self.role, "wake_class": self.wake_class}


@dataclass(frozen=True)
class WorkflowActivationSpec:
    schema_version: int
    policy_sha256: str
    workflow_configuration_version_id: str
    validation_bindings: tuple[WorkflowValidationBinding, ...]
    capability_bindings: tuple[WorkflowCapabilityBinding, ...]
    resources: WorkflowResourceBounds
    wake_bindings: tuple[WorkflowWakeBinding, ...]
    merge_method: str
    retired_rules: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkflowActivationSpec:
        if not isinstance(value, dict):
            raise ValidationError("workflow activation must be an object")
        fields = {
            "schema_version",
            "policy_sha256",
            "workflow_configuration_version_id",
            "validation_bindings",
            "capability_bindings",
            "resources",
            "wake_bindings",
            "merge_method",
            "retired_rules",
        }
        _exact_fields(value, expected=fields, subject="workflow activation")
        validation_bindings = cls._validation_bindings(value["validation_bindings"])
        capability_bindings = cls._capability_bindings(value["capability_bindings"])
        wake_bindings = cls._wake_bindings(value["wake_bindings"])
        merge_method = value["merge_method"]
        if merge_method not in LANDING_METHODS:
            raise ValidationError("workflow activation merge_method is unsupported")
        return cls(
            schema_version=_schema_version(
                value["schema_version"],
                expected=WORKFLOW_ACTIVATION_SCHEMA_VERSION,
                subject="workflow activation",
            ),
            policy_sha256=_identifier(
                value["policy_sha256"],
                field="workflow activation policy_sha256",
                pattern=SHA256_RE,
            ),
            workflow_configuration_version_id=_identifier(
                value["workflow_configuration_version_id"],
                field="workflow activation configuration version ID",
                pattern=WORKFLOW_CONFIGURATION_VERSION_ID_RE,
            ),
            validation_bindings=validation_bindings,
            capability_bindings=capability_bindings,
            resources=WorkflowResourceBounds.from_mapping(
                value["resources"], subject="workflow activation resources"
            ),
            wake_bindings=wake_bindings,
            merge_method=merge_method,
            retired_rules=_bounded_unique_strings(
                value["retired_rules"],
                field="workflow activation retired_rules",
                pattern=RULE_RE,
                allow_empty=True,
            ),
        )

    @staticmethod
    def _validation_bindings(value: object) -> tuple[WorkflowValidationBinding, ...]:
        if not isinstance(value, list) or not value or len(value) > MAX_POLICY_ITEMS:
            raise ValidationError(
                "workflow activation validation_bindings must contain between "
                f"one and {MAX_POLICY_ITEMS} entries"
            )
        parsed = tuple(WorkflowValidationBinding.from_mapping(item) for item in value)
        if len({item.obligation for item in parsed}) != len(parsed):
            raise ValidationError(
                "workflow activation validation obligations must be unique"
            )
        if len({item.validation for item in parsed}) != len(parsed):
            raise ValidationError(
                "workflow activation validation targets must be unique"
            )
        return tuple(sorted(parsed, key=lambda item: item.obligation))

    @staticmethod
    def _capability_bindings(value: object) -> tuple[WorkflowCapabilityBinding, ...]:
        if not isinstance(value, list) or not value or len(value) > MAX_POLICY_ITEMS:
            raise ValidationError(
                "workflow activation capability_bindings must contain between "
                f"one and {MAX_POLICY_ITEMS} entries"
            )
        parsed = tuple(WorkflowCapabilityBinding.from_mapping(item) for item in value)
        keys = {(item.name, item.version) for item in parsed}
        if len(keys) != len(parsed) or len({item.binding_id for item in parsed}) != len(
            parsed
        ):
            raise ValidationError(
                "workflow activation capability bindings must be unique"
            )
        return tuple(sorted(parsed, key=lambda item: (item.name, item.version)))

    @staticmethod
    def _wake_bindings(value: object) -> tuple[WorkflowWakeBinding, ...]:
        if not isinstance(value, list) or not value or len(value) > MAX_POLICY_ITEMS:
            raise ValidationError(
                "workflow activation wake_bindings must contain between one and "
                f"{MAX_POLICY_ITEMS} entries"
            )
        parsed = tuple(WorkflowWakeBinding.from_mapping(item) for item in value)
        if len({item.role for item in parsed}) != len(parsed):
            raise ValidationError("workflow activation wake roles must be unique")
        if {item.role for item in parsed} != {"producer", "reviewer"}:
            raise ValidationError(
                "workflow activation wake bindings require producer and reviewer"
            )
        return tuple(sorted(parsed, key=lambda item: item.role))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "policy_sha256": self.policy_sha256,
            "workflow_configuration_version_id": (
                self.workflow_configuration_version_id
            ),
            "validation_bindings": [
                binding.to_dict() for binding in self.validation_bindings
            ],
            "capability_bindings": [
                binding.to_dict() for binding in self.capability_bindings
            ],
            "resources": self.resources.to_dict(),
            "wake_bindings": [binding.to_dict() for binding in self.wake_bindings],
            "merge_method": self.merge_method,
            "retired_rules": list(self.retired_rules),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)
