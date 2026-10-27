from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from .advisory import AdvisoryBlindReview, AdvisoryChallengeReview, AdvisoryReport
from .db import ConcurrentUpdateError, ConflictError, NotFoundError, StorageError
from .discovery import DiscoveryRuling, DiscoveryTrace
from .domain import (
    PROVIDER_NAME_RE,
    GatewayCapability,
    HandoffEnvelope,
    InteractionKind,
    ValidationError,
    WorkAssignmentSpec,
    WorkLogEntry,
    WorkMemoryCheckpoint,
    canonical_json,
    validate_name,
)
from .krab import (
    IDEMPOTENCY_KEY_RE,
    KRAB_HANDLE_RE,
    MAX_BODY_BYTES,
    MAX_TITLE_BYTES,
    KrabError,
)
from .provider import (
    ProviderAmbiguousError,
    ProviderBudgetError,
    ProviderBusyError,
    ProviderError,
    ProviderQuotaError,
    ProviderRateLimitError,
    ProviderUpstreamError,
)
from .service import AcceptedInteractionResponse, AcceptedProviderResponse
from .workflow_scope import WorkflowScopeReview

GATEWAY_PROTOCOL_VERSION = 1
MAX_GATEWAY_REQUEST_BYTES = 3 * 1024 * 1024
MAX_GATEWAY_RESPONSE_BYTES = 9 * 1024 * 1024
MAX_GATEWAY_MARKER_BYTES = 512
MAX_GATEWAY_SEQUENCE = (1 << 53) - 1
MAX_GATEWAY_INTERACTIONS = 128
GATEWAY_PROGRESS_INTERVAL_SECONDS = 1.0
GATEWAY_REQUEST_NAME = "request.json"
GATEWAY_REQUEST_READY_NAME = "request.ready"
GATEWAY_RESPONSE_NAME = "response.json"
GATEWAY_RESPONSE_READY_NAME = "response.ready"
GATEWAY_FILE_NAMES = frozenset(
    {
        GATEWAY_REQUEST_NAME,
        GATEWAY_REQUEST_READY_NAME,
        GATEWAY_RESPONSE_NAME,
        GATEWAY_RESPONSE_READY_NAME,
    }
)
GATEWAY_WRITABLE_NAMES = (GATEWAY_REQUEST_NAME, GATEWAY_REQUEST_READY_NAME)
CONTAINER_GATEWAY_ROOT = "/run/magic-glass-box"
RUN_ID_RE = re.compile(r"^run_[0-9a-f]{32}$")
INTERACTION_ID_RE = re.compile(r"^int_[0-9a-f]{32}$")
ASSIGNMENT_ID_RE = re.compile(r"^was_[0-9a-f]{32}$")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")


class GatewayError(RuntimeError):
    """A run-scoped gateway could not be created or operated safely."""


class GatewayControlPlane(Protocol):
    database: Any

    def open_interaction(self, run_id: str, **kwargs: Any) -> dict[str, Any]: ...

    def read_interaction_response(
        self, run_id: str, interaction_id: str
    ) -> AcceptedInteractionResponse: ...

    def invoke_provider(
        self, run_id: str, *, grant_name: str, request: dict[str, Any]
    ) -> AcceptedProviderResponse: ...

    def submit_handoff(
        self, run_id: str, handoff: HandoffEnvelope
    ) -> dict[str, Any]: ...

    def submit_discovery_trace(
        self,
        run_id: str,
        *,
        trace: DiscoveryTrace,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    def submit_discovery_ruling(
        self,
        run_id: str,
        *,
        ruling: DiscoveryRuling,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    def submit_advisory_blind_review(
        self,
        run_id: str,
        *,
        review: AdvisoryBlindReview,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    def submit_advisory_challenge_review(
        self,
        run_id: str,
        *,
        review: AdvisoryChallengeReview,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    def submit_advisory_report(
        self,
        run_id: str,
        *,
        report: AdvisoryReport,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    def submit_workflow_scope_review(
        self,
        run_id: str,
        *,
        review: WorkflowScopeReview,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    def append_work_log(
        self,
        run_id: str,
        entry: WorkLogEntry,
        *,
        idempotency_key: str,
        assignment_id: str | None = None,
    ) -> dict[str, Any]: ...

    def checkpoint_work_memory(
        self,
        run_id: str,
        checkpoint: WorkMemoryCheckpoint,
        *,
        through_sequence: int,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    def create_work_assignment(
        self,
        run_id: str,
        *,
        target_profile_name: str,
        spec: WorkAssignmentSpec,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    def claim_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        lease_seconds: int,
    ) -> dict[str, Any]: ...

    def complete_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        result: WorkLogEntry,
        idempotency_key: str,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class GatewayEndpoint:
    directory: Path


@dataclass(frozen=True)
class _MailboxMarker:
    sequence: int
    size: int
    sha256: str

    def encode(self) -> bytes:
        return (
            canonical_json(
                {
                    "sequence": self.sequence,
                    "sha256": self.sha256,
                    "size": self.size,
                }
            )
            + "\n"
        ).encode("ascii")


def default_gateway_root(database_path: Path) -> Path:
    """Return a stable private runtime root without exposing the database path."""

    override = os.environ.get("MAGIC_GLASS_BOX_RUNTIME_DIR") or os.environ.get(
        "XDG_RUNTIME_DIR"
    )
    base = Path(override).expanduser() if override else Path("/tmp")  # noqa: S108
    identity = str(Path(database_path).expanduser().absolute()).encode("utf-8")
    digest = hashlib.sha256(identity).hexdigest()[:12]
    uid = os.getuid() if hasattr(os, "getuid") else os.getpid()
    return base / f"mgb-{uid}-{digest}"


class _RequestFailure(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _text(value: object, *, field: str, max_bytes: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\x00" in value
        or len(value.encode("utf-8")) > max_bytes
    ):
        raise _RequestFailure("invalid_request", f"invalid {field}")
    return value


def _exact_payload(value: object, fields: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise _RequestFailure("invalid_request", "invalid operation payload")
    return value


def _integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise _RequestFailure("invalid_request", f"invalid {field}")
    return value


class RunGateway:
    """Dispatch a narrow protocol whose run identity is fixed out of band."""

    def __init__(
        self,
        control: GatewayControlPlane,
        run_id: str,
        *,
        capabilities: Sequence[GatewayCapability | str],
        credential_names: Sequence[str],
    ):
        if not isinstance(run_id, str) or RUN_ID_RE.fullmatch(run_id) is None:
            raise GatewayError("invalid gateway run identifier")
        try:
            normalized = frozenset(
                GatewayCapability(capability) for capability in capabilities
            )
        except (TypeError, ValueError) as exc:
            raise GatewayError("invalid gateway capability policy") from exc
        if len(normalized) != len(capabilities):
            raise GatewayError("gateway capability policy contains duplicates")
        if (
            len(credential_names) > 32
            or any(
                not isinstance(name, str)
                or re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", name) is None
                for name in credential_names
            )
            or len(set(credential_names)) != len(credential_names)
        ):
            raise GatewayError("invalid gateway credential policy")
        self.control = control
        self.run_id = run_id
        self.capabilities = normalized
        self.credential_names = frozenset(credential_names)

    @staticmethod
    def _success(request_id: str, result: object) -> dict[str, Any]:
        return {
            "version": GATEWAY_PROTOCOL_VERSION,
            "request_id": request_id,
            "ok": True,
            "result": result,
        }

    @staticmethod
    def _failure(
        request_id: str | None,
        code: str,
        message: str,
        *,
        retry_after_seconds: int | None = None,
    ) -> dict[str, Any]:
        error: dict[str, Any] = {"code": code, "message": message}
        if retry_after_seconds is not None:
            error["retry_after_seconds"] = retry_after_seconds
        return {
            "version": GATEWAY_PROTOCOL_VERSION,
            "request_id": request_id,
            "ok": False,
            "error": error,
        }

    def handle(self, value: object) -> dict[str, Any]:
        request_id: str | None = None
        try:
            if not isinstance(value, dict) or set(value) != {
                "version",
                "request_id",
                "operation",
                "payload",
            }:
                raise _RequestFailure("invalid_request", "invalid gateway request")
            if (
                isinstance(value["version"], bool)
                or value["version"] != GATEWAY_PROTOCOL_VERSION
            ):
                raise _RequestFailure(
                    "invalid_request", "unsupported gateway protocol version"
                )
            request_id = value["request_id"]
            if (
                not isinstance(request_id, str)
                or REQUEST_ID_RE.fullmatch(request_id) is None
            ):
                request_id = None
                raise _RequestFailure("invalid_request", "invalid request_id")
            operation_text = _text(value["operation"], field="operation", max_bytes=64)
            try:
                operation = GatewayCapability(operation_text)
            except ValueError as exc:
                raise _RequestFailure(
                    "invalid_request", "unknown gateway operation"
                ) from exc
            if operation not in self.capabilities:
                raise _RequestFailure(
                    "forbidden", "operation is not authorized for this run"
                )
            if (
                operation
                in {
                    GatewayCapability.INTERACTION_OPEN,
                    GatewayCapability.INTERACTION_READ,
                }
                and "krab" not in self.credential_names
            ):
                raise _RequestFailure(
                    "forbidden", "operation has no authorized credential"
                )
            if operation is GatewayCapability.RUN_DESCRIBE:
                result = self._describe(value["payload"])
            elif operation is GatewayCapability.DISCOVERY_TRACE_SUBMIT:
                result = self._submit_discovery_trace(value["payload"])
            elif operation is GatewayCapability.DISCOVERY_RULING_SUBMIT:
                result = self._submit_discovery_ruling(value["payload"])
            elif operation is GatewayCapability.ADVISORY_BLIND_SUBMIT:
                result = self._submit_advisory_blind_review(value["payload"])
            elif operation is GatewayCapability.ADVISORY_CHALLENGE_SUBMIT:
                result = self._submit_advisory_challenge_review(value["payload"])
            elif operation is GatewayCapability.ADVISORY_REPORT_SUBMIT:
                result = self._submit_advisory_report(value["payload"])
            elif operation is GatewayCapability.WORKFLOW_SCOPE_REVIEW_SUBMIT:
                result = self._submit_workflow_scope_review(value["payload"])
            elif operation is GatewayCapability.INTERACTION_OPEN:
                result = self._open_interaction(value["payload"])
            elif operation is GatewayCapability.INTERACTION_READ:
                result = self._read_interaction(value["payload"])
            elif operation is GatewayCapability.HANDOFF_SUBMIT:
                result = self._submit_handoff(value["payload"])
            elif operation is GatewayCapability.PROVIDER_INVOKE:
                result = self._invoke_provider(value["payload"])
            elif operation is GatewayCapability.WORK_READ:
                result = self._read_work(value["payload"])
            elif operation is GatewayCapability.WORK_APPEND:
                result = self._append_work(value["payload"])
            elif operation is GatewayCapability.WORK_CHECKPOINT:
                result = self._checkpoint_work(value["payload"])
            elif operation is GatewayCapability.WORK_ASSIGN:
                result = self._assign_work(value["payload"])
            elif operation is GatewayCapability.WORK_ASSIGNMENT_CLAIM:
                result = self._claim_assignment(value["payload"])
            elif operation is GatewayCapability.WORK_ASSIGNMENT_COMPLETE:
                result = self._complete_assignment(value["payload"])
            else:
                raise _RequestFailure(
                    "invalid_request", "unsupported gateway operation"
                )
            return self._success(request_id, result)
        except _RequestFailure as exc:
            return self._failure(request_id, exc.code, str(exc))
        except ValidationError as exc:
            return self._failure(request_id, "invalid_request", str(exc))
        except NotFoundError as exc:
            return self._failure(request_id, "not_found", str(exc))
        except (ConcurrentUpdateError, ConflictError) as exc:
            return self._failure(request_id, "conflict", str(exc))
        except KrabError:
            return self._failure(
                request_id, "upstream_error", "gateway upstream request failed"
            )
        except ProviderBusyError:
            return self._failure(
                request_id, "busy", "provider concurrency limit is reached"
            )
        except ProviderBudgetError:
            return self._failure(
                request_id, "budget_exhausted", "provider budget is exhausted"
            )
        except ProviderQuotaError as exc:
            return self._failure(
                request_id,
                "quota_exhausted",
                "provider quota is temporarily exhausted",
                retry_after_seconds=exc.retry_after_seconds,
            )
        except ProviderRateLimitError as exc:
            return self._failure(
                request_id,
                "rate_limited",
                "provider rate limit is temporarily exhausted",
                retry_after_seconds=exc.retry_after_seconds,
            )
        except ProviderAmbiguousError:
            return self._failure(
                request_id,
                "ambiguous",
                "provider invocation outcome is ambiguous",
            )
        except ProviderUpstreamError:
            return self._failure(
                request_id, "upstream_error", "provider request failed"
            )
        except ProviderError:
            return self._failure(request_id, "forbidden", "provider request was denied")
        except (StorageError, KeyError, TypeError, ValueError):
            return self._failure(
                request_id, "internal_error", "gateway operation failed"
            )
        except Exception:
            return self._failure(
                request_id, "internal_error", "gateway operation failed"
            )

    def _describe(self, payload: object) -> dict[str, Any]:
        _exact_payload(payload, set())
        execution = self.control.database.get_run_execution(self.run_id)
        visible = self.control.database.get_run(self.run_id)
        context = dict(execution["context"])
        context.pop("repository_path", None)
        profile = execution["profile"]
        all_interactions = visible["interactions"]
        interactions = []
        for interaction in all_interactions[-MAX_GATEWAY_INTERACTIONS:]:
            interactions.append(
                {
                    "id": interaction["id"],
                    "kind": interaction["kind"],
                    "recipient_handle": interaction["recipient"]["krab_handle"],
                    "request_item_id": interaction["request_item_id"],
                    "response_item_id": interaction["response_item_id"],
                    "state": interaction["state"],
                    "version": interaction["version"],
                }
            )
        return {
            "run_id": self.run_id,
            "state": visible["state"],
            "version": visible["version"],
            "project_key": execution["project_key"],
            "source_revision": execution["source_revision"],
            "context": context,
            "work_item": execution["work_item"],
            "handoff": execution["handoff"],
            "recovery_checkpoint": execution["recovery_checkpoint"],
            "review_assignment": execution["review_assignment"],
            "changes_requested": execution["changes_requested"],
            "workflow_assignment": execution.get("workflow_assignment"),
            "discovery_assignment": execution.get("discovery_assignment"),
            "advisory_assignment": execution.get("advisory_assignment"),
            "work_memory": execution["work_memory"],
            "authority": {
                "workspace_mode": profile["workspace_mode"],
                "network_mode": profile["network_mode"],
                "credential_names": sorted(self.credential_names),
                "gateway_capabilities": sorted(
                    capability.value for capability in self.capabilities
                ),
            },
            "interaction_count": len(all_interactions),
            "interactions": interactions,
            "interactions_truncated": len(all_interactions) > len(interactions),
        }

    def _open_interaction(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(
            payload,
            {
                "kind",
                "recipient_handle",
                "title",
                "body",
                "idempotency_key",
            },
        )
        try:
            kind = InteractionKind(item["kind"])
        except (TypeError, ValueError) as exc:
            raise _RequestFailure(
                "invalid_request", "invalid interaction kind"
            ) from exc
        recipient = _text(
            item["recipient_handle"], field="recipient_handle", max_bytes=32
        )
        if KRAB_HANDLE_RE.fullmatch(recipient) is None:
            raise _RequestFailure("invalid_request", "invalid recipient_handle")
        title = _text(item["title"], field="title", max_bytes=MAX_TITLE_BYTES)
        body = _text(item["body"], field="body", max_bytes=MAX_BODY_BYTES)
        idempotency_key = _text(
            item["idempotency_key"], field="idempotency_key", max_bytes=128
        )
        if IDEMPOTENCY_KEY_RE.fullmatch(idempotency_key) is None:
            raise _RequestFailure("invalid_request", "invalid idempotency_key")
        route = self.control.database.get_run_transport(self.run_id)
        opened = self.control.open_interaction(
            self.run_id,
            kind=kind,
            recipient_krab_handle=recipient,
            title=title,
            body=body,
            idempotency_key=idempotency_key,
            expected_run_version=route["version"],
        )
        return {
            "interaction_id": opened["id"],
            "interaction_state": opened["state"],
            "interaction_version": opened["version"],
            "request_item_id": opened["request_item_id"],
            "run_state": opened["run_state"],
            "run_version": opened["run_version"],
            "agent_action": (
                "submit_handoff_then_exit"
                if GatewayCapability.HANDOFF_SUBMIT in self.capabilities
                else "exit"
            ),
        }

    def _read_interaction(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(payload, {"interaction_id"})
        interaction_id = _text(
            item["interaction_id"], field="interaction_id", max_bytes=64
        )
        if INTERACTION_ID_RE.fullmatch(interaction_id) is None:
            raise _RequestFailure("invalid_request", "invalid interaction_id")
        delivery = self.control.read_interaction_response(self.run_id, interaction_id)
        return asdict(delivery)

    def _submit_handoff(self, payload: object) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise _RequestFailure("invalid_request", "invalid operation payload")
        handoff = HandoffEnvelope.from_mapping(payload)
        stored = self.control.submit_handoff(self.run_id, handoff)
        return {
            "handoff_id": stored["id"],
            "attempt_id": stored["attempt_id"],
            "content_sha256": stored["content_sha256"],
            "agent_action": "exit",
        }

    def _submit_discovery_trace(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(payload, {"trace", "idempotency_key"})
        trace_value = item["trace"]
        if not isinstance(trace_value, Mapping):
            raise _RequestFailure("invalid_request", "trace must be an object")
        trace = DiscoveryTrace.from_mapping(trace_value)
        stored = self.control.submit_discovery_trace(
            self.run_id,
            trace=trace,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
        )
        return {
            "trace_id": stored["id"],
            "content_sha256": stored.get("content_sha256", trace.digest),
            "agent_action": "exit",
        }

    def _submit_discovery_ruling(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(payload, {"ruling", "idempotency_key"})
        ruling_value = item["ruling"]
        if not isinstance(ruling_value, Mapping):
            raise _RequestFailure("invalid_request", "ruling must be an object")
        ruling = DiscoveryRuling.from_mapping(ruling_value)
        stored = self.control.submit_discovery_ruling(
            self.run_id,
            ruling=ruling,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
        )
        return {
            "ruling_id": stored["id"],
            "sequence": stored.get("sequence"),
            "disposition": ruling.disposition.value,
            "content_sha256": stored.get("content_sha256", ruling.digest),
            "agent_action": (
                "submit_handoff_then_exit"
                if ruling.disposition.value == "ASK_HUMAN"
                else "exit"
            ),
        }

    def _submit_advisory_blind_review(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(payload, {"review", "idempotency_key"})
        review_value = item["review"]
        if not isinstance(review_value, Mapping):
            raise _RequestFailure("invalid_request", "review must be an object")
        review = AdvisoryBlindReview.from_mapping(review_value)
        stored = self.control.submit_advisory_blind_review(
            self.run_id,
            review=review,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
        )
        return {
            "blind_review_id": stored["id"],
            "content_sha256": stored.get("content_sha256", review.digest),
            "agent_action": "exit",
        }

    def _submit_advisory_challenge_review(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(payload, {"review", "idempotency_key"})
        review_value = item["review"]
        if not isinstance(review_value, Mapping):
            raise _RequestFailure("invalid_request", "review must be an object")
        review = AdvisoryChallengeReview.from_mapping(review_value)
        stored = self.control.submit_advisory_challenge_review(
            self.run_id,
            review=review,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
        )
        return {
            "challenge_review_id": stored["id"],
            "content_sha256": stored.get("content_sha256", review.digest),
            "agent_action": "exit",
        }

    def _submit_advisory_report(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(payload, {"report", "idempotency_key"})
        report_value = item["report"]
        if not isinstance(report_value, Mapping):
            raise _RequestFailure("invalid_request", "report must be an object")
        report = AdvisoryReport.from_mapping(report_value)
        stored = self.control.submit_advisory_report(
            self.run_id,
            report=report,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
        )
        return {
            "report_id": stored["id"],
            "content_sha256": stored.get("content_sha256", report.digest),
            "agent_action": "exit",
        }

    def _submit_workflow_scope_review(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(payload, {"review", "idempotency_key"})
        review_value = item["review"]
        if not isinstance(review_value, Mapping):
            raise _RequestFailure("invalid_request", "review must be an object")
        review = WorkflowScopeReview.from_mapping(review_value)
        stored = self.control.submit_workflow_scope_review(
            self.run_id,
            review=review,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
        )
        return {
            "scope_review_id": stored["id"],
            "content_sha256": stored.get("content_sha256", review.digest),
            "expected_review_outcome": stored["expected_review_outcome"],
            "failed_must_keys": stored["failed_must_keys"],
            "regression_probe_keys": stored["regression_probe_keys"],
            "follow_up_probe_keys": stored["follow_up_probe_keys"],
        }

    def _invoke_provider(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(payload, {"grant", "request"})
        grant = _text(item["grant"], field="grant", max_bytes=64)
        if PROVIDER_NAME_RE.fullmatch(grant) is None or grant == "krab":
            raise _RequestFailure("invalid_request", "invalid provider grant")
        if grant not in self.credential_names:
            raise _RequestFailure(
                "forbidden", "provider grant is not authorized for this run"
            )
        provider_request = item["request"]
        if not isinstance(provider_request, dict):
            raise _RequestFailure(
                "invalid_request", "provider request must be an object"
            )
        if "model" in provider_request:
            raise _RequestFailure(
                "invalid_request", "provider request cannot select a model"
            )
        try:
            canonical_json(provider_request)
        except ValidationError as exc:
            raise _RequestFailure("invalid_request", str(exc)) from exc
        accepted = self.control.invoke_provider(
            self.run_id,
            grant_name=grant,
            request=provider_request,
        )
        result = asdict(accepted)
        # Accounting is operator metadata; keep the worker-facing carrier stable.
        result.pop("usage", None)
        result.pop("estimated_microusd", None)
        return result

    def _read_work(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(payload, {"after_sequence", "limit"})
        after_sequence = item["after_sequence"]
        if after_sequence is not None:
            after_sequence = _integer(
                after_sequence,
                field="after_sequence",
                minimum=0,
                maximum=MAX_GATEWAY_SEQUENCE,
            )
        limit = _integer(item["limit"], field="limit", minimum=1, maximum=16)
        return self.control.database.get_work_memory_for_run(
            self.run_id,
            after_sequence=after_sequence,
            limit=limit,
        )

    @staticmethod
    def _idempotency_key(value: object) -> str:
        key = _text(value, field="idempotency_key", max_bytes=128)
        if IDEMPOTENCY_KEY_RE.fullmatch(key) is None:
            raise _RequestFailure("invalid_request", "invalid idempotency_key")
        return key

    @staticmethod
    def _assignment_id(value: object) -> str:
        assignment_id = _text(value, field="assignment_id", max_bytes=64)
        if ASSIGNMENT_ID_RE.fullmatch(assignment_id) is None:
            raise _RequestFailure("invalid_request", "invalid assignment_id")
        return assignment_id

    def _append_work(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(
            payload,
            {"assignment_id", "idempotency_key", "entry"},
        )
        assignment_id = item["assignment_id"]
        if assignment_id is not None:
            assignment_id = self._assignment_id(assignment_id)
        entry = WorkLogEntry.from_mapping(item["entry"])
        stored = self.control.append_work_log(
            self.run_id,
            entry,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
            assignment_id=assignment_id,
        )
        return {
            "entry_id": stored["id"],
            "sequence": stored["sequence"],
            "content_sha256": stored.get("content_sha256", entry.digest),
        }

    def _checkpoint_work(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(
            payload,
            {
                "expected_thread_version",
                "through_sequence",
                "idempotency_key",
                "checkpoint",
            },
        )
        expected_version = _integer(
            item["expected_thread_version"],
            field="expected_thread_version",
            minimum=1,
            maximum=MAX_GATEWAY_SEQUENCE,
        )
        through_sequence = _integer(
            item["through_sequence"],
            field="through_sequence",
            minimum=0,
            maximum=MAX_GATEWAY_SEQUENCE,
        )
        checkpoint = WorkMemoryCheckpoint.from_mapping(item["checkpoint"])
        stored = self.control.checkpoint_work_memory(
            self.run_id,
            checkpoint,
            through_sequence=through_sequence,
            expected_thread_version=expected_version,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
        )
        return {
            "checkpoint_id": stored["id"],
            "version": stored["version"],
            "through_sequence": stored["through_sequence"],
            "content_sha256": stored.get("content_sha256", checkpoint.digest),
        }

    def _assign_work(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(
            payload,
            {
                "expected_thread_version",
                "target_profile_name",
                "idempotency_key",
                "spec",
            },
        )
        expected_version = _integer(
            item["expected_thread_version"],
            field="expected_thread_version",
            minimum=1,
            maximum=MAX_GATEWAY_SEQUENCE,
        )
        target_profile_name = validate_name(
            item["target_profile_name"], field="target profile name"
        )
        spec = WorkAssignmentSpec.from_mapping(item["spec"])
        stored = self.control.create_work_assignment(
            self.run_id,
            target_profile_name=target_profile_name,
            spec=spec,
            expected_thread_version=expected_version,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
        )
        return {
            "assignment_id": stored["id"],
            "state": stored["state"],
            "version": stored["version"],
        }

    def _claim_assignment(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(
            payload,
            {"assignment_id", "expected_assignment_version", "lease_seconds"},
        )
        stored = self.control.claim_work_assignment(
            self.run_id,
            self._assignment_id(item["assignment_id"]),
            expected_assignment_version=_integer(
                item["expected_assignment_version"],
                field="expected_assignment_version",
                minimum=1,
                maximum=MAX_GATEWAY_SEQUENCE,
            ),
            lease_seconds=_integer(
                item["lease_seconds"],
                field="lease_seconds",
                minimum=30,
                maximum=86_400,
            ),
        )
        return {
            "assignment_id": stored["id"],
            "state": stored["state"],
            "version": stored["version"],
            "lease_expires_at": stored.get("lease_expires_at"),
        }

    def _complete_assignment(self, payload: object) -> dict[str, Any]:
        item = _exact_payload(
            payload,
            {
                "assignment_id",
                "expected_assignment_version",
                "idempotency_key",
                "result",
            },
        )
        result = WorkLogEntry.from_mapping(item["result"])
        stored = self.control.complete_work_assignment(
            self.run_id,
            self._assignment_id(item["assignment_id"]),
            expected_assignment_version=_integer(
                item["expected_assignment_version"],
                field="expected_assignment_version",
                minimum=1,
                maximum=MAX_GATEWAY_SEQUENCE,
            ),
            result=result,
            idempotency_key=self._idempotency_key(item["idempotency_key"]),
        )
        return {
            "assignment_id": stored["id"],
            "state": stored["state"],
            "version": stored["version"],
            "result_entry_id": stored["result_entry_id"],
        }


def _strict_json_document(raw: bytes, *, max_bytes: int, subject: str) -> object:
    if not raw.endswith(b"\n"):
        raise _RequestFailure("invalid_request", f"{subject} is not framed")
    if not 1 <= len(raw) - 1 <= max_bytes:
        raise _RequestFailure("invalid_request", f"{subject} size is invalid")
    try:
        text = raw[:-1].decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise _RequestFailure("invalid_request", f"{subject} is not UTF-8") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise _RequestFailure(
                    "invalid_request", f"{subject} has duplicate keys"
                )
            value[key] = item
        return value

    def reject_constant(_value: str) -> None:
        raise _RequestFailure("invalid_request", f"{subject} has invalid numbers")

    try:
        return json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except _RequestFailure:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise _RequestFailure("invalid_request", f"{subject} is malformed") from exc


def _strict_request(raw: bytes) -> object:
    return _strict_json_document(
        raw,
        max_bytes=MAX_GATEWAY_REQUEST_BYTES,
        subject="gateway request",
    )


def _strict_marker(raw: bytes) -> _MailboxMarker:
    value = _strict_json_document(
        raw,
        max_bytes=MAX_GATEWAY_MARKER_BYTES,
        subject="gateway marker",
    )
    if not isinstance(value, dict) or set(value) != {"sequence", "sha256", "size"}:
        raise _RequestFailure("invalid_request", "gateway marker shape is invalid")
    sequence = value["sequence"]
    size = value["size"]
    digest = value["sha256"]
    if (
        isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or not 1 <= sequence <= MAX_GATEWAY_SEQUENCE
    ):
        raise _RequestFailure("invalid_request", "gateway marker sequence is invalid")
    if (
        isinstance(size, bool)
        or not isinstance(size, int)
        or not 1 <= size <= (1 << 31) - 1
    ):
        raise _RequestFailure("invalid_request", "gateway marker size is invalid")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise _RequestFailure("invalid_request", "gateway marker digest is invalid")
    return _MailboxMarker(sequence, size, digest)


def _marker_sequence_hint(raw: bytes, last_sequence: int) -> int:
    """Recover only the response correlation field from an invalid marker."""

    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
        value = None
    if isinstance(value, dict):
        sequence = value.get("sequence")
        if (
            not isinstance(sequence, bool)
            and isinstance(sequence, int)
            and last_sequence < sequence <= MAX_GATEWAY_SEQUENCE
        ):
            return sequence
    if last_sequence < MAX_GATEWAY_SEQUENCE:
        return last_sequence + 1
    return last_sequence


def _read_bounded(path: Path, limit: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise GatewayError("gateway mailbox file is unsafe")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            value = stream.read(limit + 1)
    finally:
        os.close(descriptor)
    if len(value) > limit:
        raise _RequestFailure("invalid_request", "gateway mailbox file is too large")
    return value


def _write_all(path: Path, value: bytes) -> None:
    flags = os.O_WRONLY | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise GatewayError("gateway mailbox file is unsafe")
        offset = 0
        while offset < len(value):
            written = os.write(descriptor, value[offset:])
            if written <= 0:
                raise GatewayError("gateway mailbox write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class _FileGatewayChannel:
    def __init__(
        self,
        directory: Path,
        gateway: RunGateway,
        *,
        poll_interval: float,
        request_timeout: float,
        progress_interval: float,
    ):
        self.directory = directory
        self.gateway = gateway
        self.poll_interval = poll_interval
        self.request_timeout = request_timeout
        self.progress_interval = progress_interval
        self.stop_event = threading.Event()
        self.failure: BaseException | None = None

    def _publish(self, sequence: int, response: dict[str, Any]) -> None:
        encoded = (canonical_json(response) + "\n").encode("utf-8")
        if len(encoded) > MAX_GATEWAY_RESPONSE_BYTES:
            encoded = (
                canonical_json(
                    self.gateway._failure(
                        response.get("request_id"),
                        "internal_error",
                        "gateway response exceeds its size limit",
                    )
                )
                + "\n"
            ).encode("utf-8")
        marker = _MailboxMarker(
            sequence=sequence,
            size=len(encoded),
            sha256=hashlib.sha256(encoded).hexdigest(),
        )
        _write_all(self.directory / GATEWAY_RESPONSE_NAME, encoded)
        _write_all(self.directory / GATEWAY_RESPONSE_READY_NAME, marker.encode())

    def _publish_progress(
        self,
        sequence: int,
        request_id: str,
        heartbeat: int,
    ) -> None:
        self._publish(
            sequence,
            {
                "version": GATEWAY_PROTOCOL_VERSION,
                "request_id": request_id,
                "pending": True,
                "heartbeat": heartbeat,
            },
        )

    def _handle_request(
        self,
        request: dict[str, Any],
        responses: list[dict[str, Any]],
        failures: list[BaseException],
        finished: threading.Event,
    ) -> None:
        try:
            responses.append(self.gateway.handle(request))
        except BaseException as exc:
            failures.append(exc)
        finally:
            finished.set()

    def serve(self) -> None:
        last_marker = b""
        last_sequence = 0
        pending_marker = b""
        pending_since = 0.0
        try:
            while not self.stop_event.wait(self.poll_interval):
                try:
                    marker_raw = _read_bounded(
                        self.directory / GATEWAY_REQUEST_READY_NAME,
                        MAX_GATEWAY_MARKER_BYTES,
                    )
                except FileNotFoundError:
                    pending_marker = b""
                    pending_since = 0.0
                    continue
                except _RequestFailure as exc:
                    marker_raw = b"<oversized-marker>"
                    marker_failure = exc
                else:
                    marker_failure = None
                if not marker_raw or marker_raw == last_marker:
                    continue
                try:
                    if marker_failure is not None:
                        raise marker_failure
                    marker = _strict_marker(marker_raw)
                except _RequestFailure as exc:
                    now = time.monotonic()
                    if pending_marker != marker_raw:
                        pending_marker = marker_raw
                        pending_since = now
                    elif now - pending_since >= self.request_timeout:
                        sequence = _marker_sequence_hint(marker_raw, last_sequence)
                        self._publish(
                            sequence,
                            self.gateway._failure(None, exc.code, str(exc)),
                        )
                        last_sequence = sequence
                        last_marker = marker_raw
                        pending_marker = b""
                    continue
                if marker.sequence <= last_sequence:
                    self._publish(
                        marker.sequence,
                        self.gateway._failure(
                            None,
                            "invalid_request",
                            "gateway marker sequence is not increasing",
                        ),
                    )
                    last_marker = marker_raw
                    continue
                try:
                    request_raw = _read_bounded(
                        self.directory / GATEWAY_REQUEST_NAME,
                        MAX_GATEWAY_REQUEST_BYTES,
                    )
                except FileNotFoundError:
                    request_raw = b""
                except _RequestFailure as exc:
                    self._publish(
                        marker.sequence,
                        self.gateway._failure(None, exc.code, str(exc)),
                    )
                    last_sequence = marker.sequence
                    last_marker = marker_raw
                    continue
                committed = (
                    len(request_raw) == marker.size
                    and hashlib.sha256(request_raw).hexdigest() == marker.sha256
                )
                if not committed:
                    now = time.monotonic()
                    if pending_marker != marker_raw:
                        pending_marker = marker_raw
                        pending_since = now
                    elif now - pending_since >= self.request_timeout:
                        self._publish(
                            marker.sequence,
                            self.gateway._failure(
                                None,
                                "invalid_request",
                                "gateway request commit is invalid",
                            ),
                        )
                        last_sequence = marker.sequence
                        last_marker = marker_raw
                    continue
                pending_marker = b""
                try:
                    request = _strict_request(request_raw)
                except _RequestFailure as exc:
                    response = self.gateway._failure(None, exc.code, str(exc))
                else:
                    responses: list[dict[str, Any]] = []
                    failures: list[BaseException] = []
                    finished = threading.Event()

                    operation = threading.Thread(
                        target=self._handle_request,
                        args=(request, responses, failures, finished),
                        name="magic-glass-box-gateway-operation",
                        daemon=True,
                    )
                    operation.start()
                    progress_request_id = None
                    if (
                        isinstance(request, dict)
                        and request.get("operation") == "provider.invoke"
                        and isinstance(request.get("request_id"), str)
                        and REQUEST_ID_RE.fullmatch(request["request_id"]) is not None
                    ):
                        progress_request_id = request["request_id"]
                    heartbeat = 0
                    next_progress = time.monotonic() + self.progress_interval
                    while not finished.wait(self.poll_interval):
                        if self.stop_event.is_set():
                            return
                        now = time.monotonic()
                        if (
                            progress_request_id is not None
                            and now >= next_progress
                            and heartbeat < MAX_GATEWAY_SEQUENCE
                        ):
                            heartbeat += 1
                            self._publish_progress(
                                marker.sequence,
                                progress_request_id,
                                heartbeat,
                            )
                            next_progress = now + self.progress_interval
                    if self.stop_event.is_set():
                        return
                    if failures:
                        raise failures[0]
                    if len(responses) != 1:
                        raise GatewayError("gateway operation returned no response")
                    response = responses[0]
                self._publish(marker.sequence, response)
                last_sequence = marker.sequence
                last_marker = marker_raw
        except BaseException as exc:
            self.failure = exc

    def stop(self) -> None:
        self.stop_event.set()


class FileGatewayManager:
    """Own fixed per-run mailboxes served by the orchestrator process."""

    def __init__(
        self,
        root: Path,
        control: GatewayControlPlane,
        *,
        poll_interval: float = 0.025,
        request_timeout: float = 5.0,
    ):
        self.root = Path(root).expanduser().absolute()
        self.control = control
        if (
            isinstance(poll_interval, bool)
            or not isinstance(poll_interval, (int, float))
            or not 0.005 <= poll_interval <= 1
        ):
            raise GatewayError("gateway poll interval must be between 0.005 and 1")
        if (
            isinstance(request_timeout, bool)
            or not isinstance(request_timeout, (int, float))
            or not 0.1 <= request_timeout <= 60
        ):
            raise GatewayError("gateway request timeout must be between 0.1 and 60")
        self.poll_interval = float(poll_interval)
        self.request_timeout = float(request_timeout)
        self._active: dict[str, tuple[_FileGatewayChannel, threading.Thread]] = {}
        self._lock = threading.Lock()

    def _ensure_root(self) -> None:
        try:
            self.root.lstat()
            existed = True
        except FileNotFoundError:
            existed = False
        except OSError as exc:
            raise GatewayError(f"gateway root is unavailable: {self.root}") from exc
        try:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            info = self.root.lstat()
        except OSError as exc:
            raise GatewayError(f"gateway root is unavailable: {self.root}") from exc
        if not existed:
            os.chmod(self.root, 0o700)
            info = self.root.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise GatewayError("gateway root must be a private directory")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise GatewayError("gateway root is not owned by the current user")
        if info.st_mode & 0o077:
            raise GatewayError("gateway root must use mode 0700")

    def open(
        self,
        run_id: str,
        *,
        capabilities: Sequence[GatewayCapability | str],
        credential_names: Sequence[str],
    ) -> GatewayEndpoint:
        if not isinstance(run_id, str) or RUN_ID_RE.fullmatch(run_id) is None:
            raise GatewayError("invalid gateway run identifier")
        self._ensure_root()
        with self._lock:
            if run_id in self._active:
                raise GatewayError(f"gateway is already active for run: {run_id}")
            run_root = self.root / run_id
            if "," in str(run_root):
                raise GatewayError("gateway path must not contain a comma")
            try:
                run_root.mkdir(mode=0o700)
            except FileExistsError as exc:
                raise GatewayError(
                    f"gateway endpoint state already exists for run: {run_id}"
                ) from exc
            except OSError as exc:
                raise GatewayError("cannot create gateway endpoint") from exc
            channel: _FileGatewayChannel | None = None
            created: list[Path] = []
            try:
                os.chmod(run_root, 0o700)
                for name in sorted(GATEWAY_FILE_NAMES):
                    path = run_root / name
                    descriptor = os.open(
                        path,
                        os.O_CREAT
                        | os.O_EXCL
                        | os.O_WRONLY
                        | getattr(os, "O_NOFOLLOW", 0),
                        0o600,
                    )
                    os.close(descriptor)
                    os.chmod(path, 0o600)
                    created.append(path)
                gateway = RunGateway(
                    self.control,
                    run_id,
                    capabilities=capabilities,
                    credential_names=credential_names,
                )
                channel = _FileGatewayChannel(
                    run_root,
                    gateway,
                    poll_interval=self.poll_interval,
                    request_timeout=self.request_timeout,
                    progress_interval=GATEWAY_PROGRESS_INTERVAL_SECONDS,
                )
                thread = threading.Thread(
                    target=channel.serve,
                    name="magic-glass-box-gateway-" + run_id.removeprefix("run_"),
                    daemon=True,
                )
                thread.start()
                self._active[run_id] = (channel, thread)
            except Exception:
                if channel is not None:
                    channel.stop()
                for path in reversed(created):
                    path.unlink(missing_ok=True)
                run_root.rmdir()
                raise
        return GatewayEndpoint(directory=run_root)

    def cleanup(self, run_id: str) -> bool:
        if not isinstance(run_id, str) or RUN_ID_RE.fullmatch(run_id) is None:
            raise GatewayError("invalid gateway run identifier")
        with self._lock:
            active = self._active.pop(run_id, None)
        if active is not None:
            channel, thread = active
            channel.stop()
            thread.join(timeout=5)
            if thread.is_alive():
                raise GatewayError("gateway server did not stop")
            if channel.failure is not None:
                raise GatewayError("gateway server failed") from channel.failure
        run_root = self.root / run_id
        try:
            info = run_root.lstat()
        except FileNotFoundError:
            return active is not None
        except OSError as exc:
            raise GatewayError("gateway endpoint is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise GatewayError("gateway endpoint must be a private directory")
        entries = list(run_root.iterdir())
        if any(entry.name not in GATEWAY_FILE_NAMES for entry in entries):
            raise GatewayError("gateway endpoint contains unexpected files")
        for entry in entries:
            entry_info = entry.lstat()
            if not stat.S_ISREG(entry_info.st_mode) or entry_info.st_nlink != 1:
                raise GatewayError("gateway endpoint contains an unsafe file")
        for entry in entries:
            entry.unlink()
        run_root.rmdir()
        return True

    def close(self) -> None:
        with self._lock:
            run_ids = tuple(self._active)
        for run_id in run_ids:
            self.cleanup(run_id)
