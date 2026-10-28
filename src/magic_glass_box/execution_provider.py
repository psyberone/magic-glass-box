from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .domain import ValidationError, canonical_json, content_sha256
from .execution import (
    CANDIDATE_ID_RE,
    EXECUTION_ID_RE,
    SERVICE_OPERATION_ID_RE,
    ExecutionEvidenceResult,
    ExecutionJobSpec,
    ExecutionStatusResult,
)
from .service_sidecar import (
    ServiceOperationReceipt,
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceSidecarSpec,
)

SUPPORTED_CAPABILITIES = frozenset(
    {
        ("execution.submit", 1),
        ("execution.inspect", 1),
        ("execution.cancel", 1),
        ("execution.collect", 1),
    }
)
CAPABILITY_MODES = {
    "execution.submit": "effect",
    "execution.inspect": "read",
    "execution.cancel": "effect",
    "execution.collect": "read",
}
DEFAULT_RETRY_SECONDS = 15


class ExecutionProviderError(ValueError):
    """A controlled-execution request or normalized result is invalid."""


class ExecutionBackendUnavailable(RuntimeError):
    """The remote execution backend was unavailable before a known effect."""


class ExecutionBackendAmbiguous(RuntimeError):
    """The remote execution backend may have performed an effect."""


class ExecutionBackend(Protocol):
    def submit(
        self, job: ExecutionJobSpec, *, deadline: str
    ) -> ExecutionStatusResult: ...

    def reconcile_submit(
        self, job: ExecutionJobSpec, *, deadline: str
    ) -> ExecutionStatusResult | None: ...

    def inspect(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionStatusResult | None: ...

    def cancel(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionStatusResult | None: ...

    def reconcile_cancel(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionStatusResult | None: ...

    def collect(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionEvidenceResult | None: ...


@dataclass
class _Invocation:
    request: ServiceOperationRequest
    request_bytes: int = 0
    response_bytes: int = 0
    effect_dispatched: bool = False


def _exact_parameters(
    value: object, *, fields: set[str], capability: str
) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ExecutionProviderError(f"{capability} parameters are invalid")
    return dict(value)


class ControlledExecutionProvider:
    """Expose pinned remote validation without exposing a remote shell."""

    def __init__(self, spec: ServiceSidecarSpec, backend: ExecutionBackend) -> None:
        if not isinstance(spec, ServiceSidecarSpec) or spec.schema_version not in {
            1,
            2,
        }:
            raise ExecutionProviderError(
                "controlled execution requires a supported service-sidecar schema"
            )
        if not spec.mappings or any(
            mapping.service != "ssh-runner" or mapping.resource.kind != "repository"
            for mapping in spec.mappings
        ):
            raise ExecutionProviderError(
                "controlled execution requires repository runner mappings"
            )
        capability_keys = {
            (capability.name, capability.version) for capability in spec.capabilities
        }
        if capability_keys - SUPPORTED_CAPABILITIES:
            raise ExecutionProviderError(
                "controlled execution capability is unsupported"
            )
        if any(
            capability.mode != CAPABILITY_MODES.get(capability.name)
            for capability in spec.capabilities
        ):
            raise ExecutionProviderError("controlled execution mode is invalid")
        required = {
            "submit",
            "reconcile_submit",
            "inspect",
            "cancel",
            "reconcile_cancel",
            "collect",
        }
        if any(not hasattr(backend, method) for method in required):
            raise ExecutionProviderError("controlled execution backend is invalid")
        self.spec = spec
        self.backend = backend

    def invoke(self, request: ServiceOperationRequest) -> ServiceOperationResponse:
        if not isinstance(request, ServiceOperationRequest):
            raise ExecutionProviderError("execution operation request is invalid")
        invocation = _Invocation(request)
        try:
            self._validate_authority(request)
            job, execution_id = self._job(request)
            invocation.request_bytes = len(job.canonical.encode("utf-8")) + (
                0 if execution_id is None else len(execution_id.encode("utf-8"))
            )
            if invocation.request_bytes > request.request_limit_bytes:
                raise ExecutionProviderError("execution request budget is exhausted")
            if request.capability.name == "execution.submit":
                return self._submit(invocation, job)
            if request.capability.name == "execution.inspect":
                assert execution_id is not None
                return self._inspect(invocation, job, execution_id)
            if request.capability.name == "execution.cancel":
                assert execution_id is not None
                return self._cancel(invocation, job, execution_id)
            if request.capability.name == "execution.collect":
                assert execution_id is not None
                return self._collect(invocation, job, execution_id)
            raise ExecutionProviderError("execution capability is unsupported")
        except ExecutionBackendUnavailable:
            return self._failure(
                invocation,
                outcome="retryable",
                error_code="execution-transport-unavailable",
                retry_after_seconds=DEFAULT_RETRY_SECONDS,
            )
        except ExecutionBackendAmbiguous:
            if request.capability.mode == "effect" and invocation.effect_dispatched:
                return self._failure(
                    invocation,
                    outcome="uncertain",
                    error_code="execution-transport-ambiguous",
                )
            return self._failure(
                invocation,
                outcome="retryable",
                error_code="execution-transport-unavailable",
                retry_after_seconds=DEFAULT_RETRY_SECONDS,
            )
        except (ExecutionProviderError, ValidationError):
            if invocation.effect_dispatched:
                return self._failure(
                    invocation,
                    outcome="uncertain",
                    error_code="execution-result-ambiguous",
                )
            return self._failure(
                invocation,
                outcome="denied",
                error_code="execution-request-invalid",
            )

    def _validate_authority(self, request: ServiceOperationRequest) -> None:
        if request.schema_version != self.spec.schema_version:
            raise ExecutionProviderError("execution operation schema does not match")
        if request.connector_protocol_version != self.spec.connector_protocol_version:
            raise ExecutionProviderError("execution connector protocol does not match")
        if request.deployment_sha256 != self.spec.deployment_sha256:
            raise ExecutionProviderError("execution deployment does not match")
        if request.request_limit_bytes > self.spec.request_bytes:
            raise ExecutionProviderError("execution request limit exceeds authority")
        if request.response_limit_bytes > self.spec.response_bytes:
            raise ExecutionProviderError("execution response limit exceeds authority")
        mapping = next(
            (item for item in self.spec.mappings if item.name == request.mapping.name),
            None,
        )
        if mapping is None or mapping != request.mapping:
            raise ExecutionProviderError("execution mapping does not match authority")
        capability = next(
            (
                item
                for item in self.spec.capabilities
                if (item.name, item.version)
                == (request.capability.name, request.capability.version)
            ),
            None,
        )
        if capability is None or capability != request.capability:
            raise ExecutionProviderError(
                "execution capability does not match authority"
            )
        if request.target.resource != request.mapping.resource:
            raise ExecutionProviderError("execution target repository does not match")
        if (
            request.target.object_format not in {"sha1", "sha256"}
            or request.target.expected_revision is None
            or request.target.desired_revision is None
            or CANDIDATE_ID_RE.fullmatch(request.target.object_id) is None
        ):
            raise ExecutionProviderError("execution target is not an exact candidate")
        if request.capability.mode == "read" and request.phase != "invoke":
            raise ExecutionProviderError("execution reads cannot be reconciled")

    def _job(
        self, request: ServiceOperationRequest
    ) -> tuple[ExecutionJobSpec, str | None]:
        common = {"recipe_id", "recipe_sha256", "image_id"}
        fields = common
        if request.capability.name != "execution.submit":
            fields = common | {"execution_id", "submission_operation_id"}
        parameters = _exact_parameters(
            request.parameters,
            fields=fields,
            capability=request.capability.name,
        )
        execution_id = parameters.get("execution_id")
        if execution_id is not None and (
            not isinstance(execution_id, str)
            or EXECUTION_ID_RE.fullmatch(execution_id) is None
        ):
            raise ExecutionProviderError("execution result identity is invalid")
        operation_id = request.operation_id
        if request.capability.name != "execution.submit":
            submission_operation_id = parameters.get("submission_operation_id")
            if (
                not isinstance(submission_operation_id, str)
                or SERVICE_OPERATION_ID_RE.fullmatch(submission_operation_id) is None
            ):
                raise ExecutionProviderError(
                    "execution submission operation identity is invalid"
                )
            operation_id = submission_operation_id
        target = request.target
        assert target.object_format is not None
        assert target.expected_revision is not None
        assert target.desired_revision is not None
        job = ExecutionJobSpec(
            operation_id=operation_id,
            mapping_name=request.mapping.name,
            repository=request.mapping.repository,
            candidate_id=target.object_id,
            object_format=target.object_format,
            base_revision=target.expected_revision,
            candidate_revision=target.desired_revision,
            recipe_id=parameters["recipe_id"],  # type: ignore[arg-type]
            recipe_sha256=parameters["recipe_sha256"],  # type: ignore[arg-type]
            image_id=parameters["image_id"],  # type: ignore[arg-type]
        )
        return job, execution_id if isinstance(execution_id, str) else None

    @staticmethod
    def _validate_status(
        job: ExecutionJobSpec,
        result: ExecutionStatusResult,
        *,
        execution_id: str | None = None,
    ) -> ExecutionStatusResult:
        parsed = ExecutionStatusResult.from_mapping(result.to_dict())
        if (
            parsed.candidate_revision != job.candidate_revision
            or parsed.object_format != job.object_format
            or parsed.recipe_id != job.recipe_id
            or parsed.recipe_sha256 != job.recipe_sha256
            or parsed.image_id != job.image_id
            or (execution_id is not None and parsed.execution_id != execution_id)
        ):
            raise ExecutionProviderError("execution result does not match its request")
        return parsed

    def _submit(
        self, invocation: _Invocation, job: ExecutionJobSpec
    ) -> ServiceOperationResponse:
        request = invocation.request
        if request.phase == "reconcile":
            result = self.backend.reconcile_submit(job, deadline=request.deadline)
            if result is None:
                return self._failure(
                    invocation,
                    outcome="not_found",
                    error_code="execution-submission-not-observed",
                )
            parsed = self._validate_status(job, result)
            return self._success(invocation, parsed, provider_status=200)
        invocation.effect_dispatched = True
        result = self.backend.submit(job, deadline=request.deadline)
        parsed = self._validate_status(job, result)
        return self._success(invocation, parsed, provider_status=202)

    def _inspect(
        self, invocation: _Invocation, job: ExecutionJobSpec, execution_id: str
    ) -> ServiceOperationResponse:
        result = self.backend.inspect(
            job, execution_id, deadline=invocation.request.deadline
        )
        if result is None:
            return self._failure(
                invocation,
                outcome="conflict",
                error_code="execution-not-found",
            )
        parsed = self._validate_status(job, result, execution_id=execution_id)
        return self._success(invocation, parsed, provider_status=200)

    def _cancel(
        self, invocation: _Invocation, job: ExecutionJobSpec, execution_id: str
    ) -> ServiceOperationResponse:
        request = invocation.request
        if request.phase == "reconcile":
            result = self.backend.reconcile_cancel(
                job, execution_id, deadline=request.deadline
            )
            if result is None:
                return self._failure(
                    invocation,
                    outcome="not_found",
                    error_code="execution-cancellation-not-observed",
                )
            parsed = self._validate_status(job, result, execution_id=execution_id)
            if parsed.state == "cancelled":
                return self._success(invocation, parsed, provider_status=200)
            return self._failure(
                invocation,
                outcome="conflict",
                error_code="execution-finished-before-cancel",
            )
        invocation.effect_dispatched = True
        result = self.backend.cancel(job, execution_id, deadline=request.deadline)
        if result is None:
            return self._failure(
                invocation,
                outcome="conflict",
                error_code="execution-not-found",
            )
        parsed = self._validate_status(job, result, execution_id=execution_id)
        return self._success(invocation, parsed, provider_status=200)

    def _collect(
        self, invocation: _Invocation, job: ExecutionJobSpec, execution_id: str
    ) -> ServiceOperationResponse:
        result = self.backend.collect(
            job, execution_id, deadline=invocation.request.deadline
        )
        if result is None:
            return self._failure(
                invocation,
                outcome="conflict",
                error_code="execution-evidence-unavailable",
            )
        parsed = ExecutionEvidenceResult.from_mapping(result.to_dict())
        self._validate_status(job, parsed.execution, execution_id=execution_id)
        return self._success(invocation, parsed, provider_status=200)

    def _success(
        self,
        invocation: _Invocation,
        result: ExecutionStatusResult | ExecutionEvidenceResult,
        *,
        provider_status: int,
    ) -> ServiceOperationResponse:
        result_dict = result.to_dict()
        raw = canonical_json(result_dict).encode("utf-8")
        invocation.response_bytes = len(raw)
        if invocation.response_bytes > invocation.request.response_limit_bytes:
            raise ExecutionProviderError("execution response budget is exhausted")
        status = (
            result if isinstance(result, ExecutionStatusResult) else result.execution
        )
        receipt = ServiceOperationReceipt.from_mapping(
            {
                "remote_identity": f"execution:{status.execution_id}",
                "observed_revision": status.candidate_revision,
                "evidence_sha256": content_sha256(canonical_json(result_dict)),
                "closed_references": [],
                "result": result_dict,
            }
        )
        return ServiceOperationResponse.from_mapping(
            {
                **invocation.request.correlation_dict(),
                "outcome": "succeeded",
                "provider_status": provider_status,
                "request_bytes": invocation.request_bytes,
                "response_bytes": invocation.response_bytes,
                "error_code": None,
                "retry_after_seconds": None,
                "receipt": receipt.to_dict(),
            }
        )

    @staticmethod
    def _failure(
        invocation: _Invocation,
        *,
        outcome: str,
        error_code: str,
        retry_after_seconds: int | None = None,
    ) -> ServiceOperationResponse:
        return ServiceOperationResponse.from_mapping(
            {
                **invocation.request.correlation_dict(),
                "outcome": outcome,
                "provider_status": None,
                "request_bytes": min(
                    invocation.request_bytes,
                    invocation.request.request_limit_bytes,
                ),
                "response_bytes": min(
                    invocation.response_bytes,
                    invocation.request.response_limit_bytes,
                ),
                "error_code": error_code,
                "retry_after_seconds": retry_after_seconds,
                "receipt": None,
            }
        )


__all__ = [
    "ControlledExecutionProvider",
    "ExecutionBackend",
    "ExecutionBackendAmbiguous",
    "ExecutionBackendUnavailable",
    "ExecutionEvidenceResult",
    "ExecutionJobSpec",
    "ExecutionProviderError",
    "ExecutionStatusResult",
]
