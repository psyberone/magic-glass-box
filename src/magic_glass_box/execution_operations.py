from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .db import ConflictError
from .domain import ValidationError, validate_provider_name
from .execution import (
    CANDIDATE_ID_RE,
    IMAGE_ID_RE,
    SERVICE_OPERATION_ID_RE,
    SHA256_RE,
    ExecutionStatusResult,
)
from .service_sidecar import (
    ServiceCapability,
    ServiceMapping,
    ServiceOperationReceipt,
    ServiceOperationSpec,
    ServiceSidecarSpec,
)

FOLLOWUP_ACTIONS = frozenset({"inspect", "cancel", "collect"})


class ExecutionOperationError(ValueError):
    """Controlled-execution authority cannot be derived safely."""


def _exact(value: object, *, fields: set[str], subject: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ExecutionOperationError(f"{subject} is invalid")
    return value


@dataclass(frozen=True)
class ExecutionRecipeAuthority:
    recipe_id: str
    recipe_sha256: str
    image_id: str

    @classmethod
    def from_mapping(cls, value: object) -> ExecutionRecipeAuthority:
        item = _exact(
            value,
            fields={"recipe_id", "recipe_sha256", "image_id"},
            subject="execution recipe authority",
        )
        try:
            recipe_id = validate_provider_name(
                item["recipe_id"], field="execution recipe ID"
            )
        except ValidationError as exc:
            raise ExecutionOperationError(
                "execution recipe authority is invalid"
            ) from exc
        recipe_sha256 = item["recipe_sha256"]
        image_id = item["image_id"]
        if (
            not isinstance(recipe_sha256, str)
            or SHA256_RE.fullmatch(recipe_sha256) is None
            or not isinstance(image_id, str)
            or IMAGE_ID_RE.fullmatch(image_id) is None
        ):
            raise ExecutionOperationError("execution recipe authority is invalid")
        return cls(recipe_id, recipe_sha256, image_id)

    def to_dict(self) -> dict[str, str]:
        return {
            "image_id": self.image_id,
            "recipe_id": self.recipe_id,
            "recipe_sha256": self.recipe_sha256,
        }


def _sidecar_authority(
    binding: Mapping[str, Any],
    sidecar: Mapping[str, Any],
    capability_name: str,
    *,
    require_active_binding: bool,
) -> tuple[ServiceSidecarSpec, ServiceMapping, ServiceCapability]:
    if not isinstance(binding, dict) or not isinstance(sidecar, dict):
        raise ExecutionOperationError("execution controller authority is invalid")
    if binding.get("state") != "active" and require_active_binding:
        raise ConflictError("execution capability is unavailable")
    project = binding.get("project")
    controller = binding.get("controller")
    if (
        not isinstance(project, dict)
        or not project.get("active")
        or not isinstance(controller, dict)
        or controller.get("sidecar_state") != "active"
        or sidecar.get("state") != "active"
    ):
        raise ConflictError("execution controller is unavailable")
    try:
        spec = ServiceSidecarSpec.from_mapping(sidecar["spec"])
        capability = ServiceCapability.from_mapping(binding["capability"])
        mapping = ServiceMapping.from_mapping(controller["mapping"])
    except (KeyError, TypeError, ValidationError) as exc:
        raise ExecutionOperationError(
            "execution controller authority is invalid"
        ) from exc
    advertised_mapping = next(
        (candidate for candidate in spec.mappings if candidate.name == mapping.name),
        None,
    )
    advertised_capability = next(
        (
            candidate
            for candidate in spec.capabilities
            if (candidate.name, candidate.version)
            == (capability.name, capability.version)
        ),
        None,
    )
    if capability.name != capability_name or capability.version != 1:
        raise ExecutionOperationError("execution capability is invalid")
    if (
        advertised_capability != capability
        or advertised_mapping != mapping
        or mapping.service != "ssh-runner"
        or mapping.resource.kind != "repository"
        or sidecar.get("id") != controller.get("sidecar_id")
        or sidecar.get("name") != controller.get("sidecar_name")
        or sidecar.get("content_sha256") != spec.digest
        or controller.get("sidecar_content_sha256") != spec.digest
        or sidecar.get("deployment_sha256") != spec.deployment_sha256
        or controller.get("deployment_sha256") != spec.deployment_sha256
        or controller.get("connector_protocol_version")
        != spec.connector_protocol_version
    ):
        raise ConflictError("execution controller authority does not match")
    return spec, mapping, capability


def _target(
    schema_version: int,
    mapping: ServiceMapping,
    *,
    candidate_id: object,
    object_format: object,
    base_revision: object,
    head_revision: object,
) -> dict[str, object]:
    common = {
        "object_format": object_format,
        "expected_revision": base_revision,
        "desired_revision": head_revision,
    }
    if schema_version == 1:
        return {
            **common,
            "repository": mapping.repository,
            "resource_id": candidate_id,
        }
    return {
        **common,
        "resource": mapping.resource.to_dict(),
        "object_id": candidate_id,
    }


def _operation(
    *,
    spec: ServiceSidecarSpec,
    sidecar_name: object,
    mapping: ServiceMapping,
    capability: ServiceCapability,
    idempotency_key: str,
    target: dict[str, object],
    parameters: dict[str, object],
) -> ServiceOperationSpec:
    try:
        operation = ServiceOperationSpec.from_mapping(
            {
                "schema_version": spec.schema_version,
                "connector_name": sidecar_name,
                "deployment_sha256": spec.deployment_sha256,
                "mapping": mapping.to_dict(),
                "capability": capability.to_dict(),
                "idempotency_key": idempotency_key,
                "target": target,
                "parameters": parameters,
                "request_bytes": spec.request_bytes,
                "response_bytes": spec.response_bytes,
                "timeout_ms": spec.timeout_ms,
            }
        )
        spec.validate_operation(operation.connector_name, operation)
    except (TypeError, ValidationError) as exc:
        raise ExecutionOperationError("execution operation is invalid") from exc
    return operation


def build_execution_submission(
    *,
    candidate: Mapping[str, Any],
    binding: Mapping[str, Any],
    sidecar: Mapping[str, Any],
    recipe: ExecutionRecipeAuthority,
    request_key: str,
) -> ServiceOperationSpec:
    """Derive submission authority from a durable candidate and project binding."""

    if not isinstance(candidate, dict) or not isinstance(
        recipe, ExecutionRecipeAuthority
    ):
        raise ExecutionOperationError("execution submission authority is invalid")
    spec, mapping, capability = _sidecar_authority(
        binding,
        sidecar,
        "execution.submit",
        require_active_binding=True,
    )
    project = binding["project"]
    if candidate.get("project_id") != project.get("id"):
        raise ConflictError("execution candidate belongs to another project")
    candidate_id = candidate.get("id")
    if (
        not isinstance(candidate_id, str)
        or CANDIDATE_ID_RE.fullmatch(candidate_id) is None
    ):
        raise ExecutionOperationError("execution candidate identity is invalid")
    try:
        key = validate_provider_name(request_key, field="execution request key")
    except ValidationError as exc:
        raise ExecutionOperationError("execution request key is invalid") from exc
    return _operation(
        spec=spec,
        sidecar_name=sidecar.get("name"),
        mapping=mapping,
        capability=capability,
        idempotency_key=f"execution-submit:{candidate_id}:{key}",
        target=_target(
            spec.schema_version,
            mapping,
            candidate_id=candidate_id,
            object_format=candidate.get("object_format"),
            base_revision=candidate.get("base_revision"),
            head_revision=candidate.get("head_revision"),
        ),
        parameters=recipe.to_dict(),
    )


def _successful_submission_status(
    submission: Mapping[str, Any],
) -> tuple[ServiceOperationSpec, ExecutionStatusResult]:
    if submission.get("state") != "succeeded":
        raise ConflictError("execution submission is not durably successful")
    try:
        spec = ServiceOperationSpec.from_mapping(submission["spec"])
    except (KeyError, TypeError, ValidationError) as exc:
        raise ExecutionOperationError(
            "execution submission authority is invalid"
        ) from exc
    if (spec.capability.name, spec.capability.version) != ("execution.submit", 1):
        raise ExecutionOperationError("execution source is not a submission")
    attempts = submission.get("attempts")
    if not isinstance(attempts, list):
        raise ExecutionOperationError("execution submission receipt is invalid")
    successful = [
        attempt
        for attempt in attempts
        if isinstance(attempt, dict) and attempt.get("outcome") == "succeeded"
    ]
    if not successful:
        raise ConflictError("execution submission has no successful receipt")
    try:
        receipt = ServiceOperationReceipt.from_mapping(successful[-1]["receipt"])
        status = ExecutionStatusResult.from_mapping(receipt.result)
    except (KeyError, TypeError, ValidationError) as exc:
        raise ExecutionOperationError(
            "execution submission receipt is invalid"
        ) from exc
    if (
        receipt.remote_identity != f"execution:{status.execution_id}"
        or receipt.observed_revision != status.candidate_revision
        or status.candidate_revision != spec.target.desired_revision
        or status.object_format != spec.target.object_format
        or spec.parameters
        != {
            "image_id": status.image_id,
            "recipe_id": status.recipe_id,
            "recipe_sha256": status.recipe_sha256,
        }
    ):
        raise ConflictError("execution submission receipt does not match its authority")
    return spec, status


def build_execution_followup(
    *,
    submission: Mapping[str, Any],
    submission_binding: Mapping[str, Any],
    binding: Mapping[str, Any],
    sidecar: Mapping[str, Any],
    action: str,
    request_key: str,
) -> ServiceOperationSpec:
    """Derive inspect/cancel/collect authority from one successful submission."""

    if action not in FOLLOWUP_ACTIONS:
        raise ExecutionOperationError("execution follow-up action is invalid")
    source_id = submission.get("id")
    if (
        not isinstance(source_id, str)
        or SERVICE_OPERATION_ID_RE.fullmatch(source_id) is None
    ):
        raise ExecutionOperationError("execution submission identity is invalid")
    if submission.get("project_capability_binding_id") != submission_binding.get("id"):
        raise ConflictError("execution submission binding does not match")
    source_spec, status = _successful_submission_status(submission)
    source_sidecar = submission.get("sidecar")
    if not isinstance(source_sidecar, dict):
        raise ExecutionOperationError("execution submission controller is invalid")
    _, source_mapping, _ = _sidecar_authority(
        submission_binding,
        sidecar,
        "execution.submit",
        require_active_binding=False,
    )
    spec, mapping, capability = _sidecar_authority(
        binding,
        sidecar,
        f"execution.{action}",
        require_active_binding=True,
    )
    if (
        submission_binding.get("project", {}).get("id")
        != binding.get("project", {}).get("id")
        or source_mapping != mapping
        or source_spec.mapping != mapping
        or source_sidecar.get("id") != sidecar.get("id")
        or source_sidecar.get("content_sha256") != sidecar.get("content_sha256")
    ):
        raise ConflictError("execution follow-up controller does not match submission")
    try:
        key = validate_provider_name(request_key, field="execution request key")
    except ValidationError as exc:
        raise ExecutionOperationError("execution request key is invalid") from exc
    return _operation(
        spec=spec,
        sidecar_name=sidecar.get("name"),
        mapping=mapping,
        capability=capability,
        idempotency_key=f"execution-{action}:{source_id}:{key}",
        target=source_spec.target.to_dict(),
        parameters={
            "execution_id": status.execution_id,
            "image_id": status.image_id,
            "recipe_id": status.recipe_id,
            "recipe_sha256": status.recipe_sha256,
            "submission_operation_id": source_id,
        },
    )


__all__ = [
    "ExecutionOperationError",
    "ExecutionRecipeAuthority",
    "build_execution_followup",
    "build_execution_submission",
]
