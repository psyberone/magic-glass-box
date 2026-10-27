from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any

from .domain import (
    NAME_RE,
    PROVIDER_NAME_RE,
    GatewayCapability,
    InteractionKind,
    canonical_json,
)
from .krab import (
    IDEMPOTENCY_KEY_RE,
    KRAB_HANDLE_RE,
    MAX_BODY_BYTES,
    MAX_TITLE_BYTES,
)

GATEWAY_CATALOG_VERSION = 1
GATEWAY_CATALOG_OPERATION = "gateway.catalog"
MAX_GATEWAY_CATALOG_BYTES = 512 * 1024
MAX_GATEWAY_SCHEMA_BYTES = 128 * 1024
SCHEMA_REFERENCE_RE = re.compile(r"^schemas/[a-z0-9][a-z0-9-]*\.schema\.json$")


class GatewayContractError(ValueError):
    """The published worker-gateway contract is invalid or unsupported."""


class TrustClass(StrEnum):
    AUTHORITY = "authority"
    CONTROL_METADATA = "control_metadata"
    UNTRUSTED_CONTENT = "untrusted_content"


@dataclass(frozen=True)
class FieldProvenance:
    path: str
    trust_class: TrustClass
    source: str

    def to_dict(self) -> dict[str, str]:
        return {
            "path": self.path,
            "source": self.source,
            "trust_class": self.trust_class.value,
        }


@dataclass(frozen=True)
class OperationContract:
    capability: GatewayCapability
    handler_name: str
    request_fields: frozenset[str]
    request_schema: dict[str, Any]
    result_schema: dict[str, Any]
    follow_up_actions: tuple[str, ...]
    response_provenance: tuple[FieldProvenance, ...]

    def to_catalog_entry(self) -> dict[str, Any]:
        return {
            "follow_up_actions": list(self.follow_up_actions),
            "operation": self.capability.value,
            "request_schema": self.request_schema,
            "response_provenance": [
                item.to_dict() for item in self.response_provenance
            ],
            "result_schema": self.result_schema,
        }


def _text(
    max_bytes: int,
    *,
    pattern: str | None = None,
    values: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "minLength": 1,
        "type": "string",
        "x-max-utf8-bytes": max_bytes,
    }
    if pattern is not None:
        result["pattern"] = pattern
    if values is not None:
        result["enum"] = list(values)
    return result


def _integer(minimum: int, maximum: int) -> dict[str, Any]:
    return {"maximum": maximum, "minimum": minimum, "type": "integer"}


def _nullable(schema: dict[str, Any]) -> dict[str, Any]:
    return {"anyOf": [schema, {"type": "null"}]}


def _object(
    properties: dict[str, Any],
    *,
    required: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    required_fields = tuple(properties) if required is None else required
    return {
        "additionalProperties": False,
        "properties": properties,
        "required": sorted(required_fields),
        "type": "object",
    }


def _ref(name: str) -> dict[str, str]:
    return {"$ref": f"schemas/{name}"}


def _result(*fields: str) -> dict[str, Any]:
    return _object({field: {} for field in fields})


CONTROL_ROOT = FieldProvenance(
    "/result", TrustClass.CONTROL_METADATA, "gateway_control_plane"
)
_IDEMPOTENCY_KEY = _text(128, pattern=IDEMPOTENCY_KEY_RE.pattern)
_INTERACTION_ID = _text(64, pattern=r"^int_[0-9a-f]{32}$")
_ASSIGNMENT_ID = _text(64, pattern=r"^was_[0-9a-f]{32}$")
_SEQUENCE = _integer(0, (1 << 53) - 1)
_VERSION = _integer(1, (1 << 53) - 1)


def _contract(
    capability: GatewayCapability,
    handler_name: str,
    request_fields: tuple[str, ...],
    request_schema: dict[str, Any],
    result_schema: dict[str, Any],
    *,
    follow_up_actions: tuple[str, ...] = (),
    provenance: tuple[FieldProvenance, ...] = (CONTROL_ROOT,),
) -> OperationContract:
    return OperationContract(
        capability=capability,
        handler_name=handler_name,
        request_fields=frozenset(request_fields),
        request_schema=request_schema,
        result_schema=result_schema,
        follow_up_actions=follow_up_actions,
        response_provenance=provenance,
    )


_CONTRACTS = {
    GatewayCapability.RUN_DESCRIBE: _contract(
        GatewayCapability.RUN_DESCRIBE,
        "_describe",
        (),
        _object({}),
        _result(
            "run_id",
            "state",
            "version",
            "project_key",
            "source_revision",
            "context",
            "work_item",
            "handoff",
            "recovery_checkpoint",
            "review_assignment",
            "changes_requested",
            "workflow_assignment",
            "discovery_assignment",
            "advisory_assignment",
            "work_memory",
            "authority",
            "interaction_count",
            "interactions",
            "interactions_truncated",
            "gateway_catalog",
        ),
        provenance=(
            CONTROL_ROOT,
            FieldProvenance(
                "/result/authority", TrustClass.AUTHORITY, "gateway_enforcement"
            ),
            FieldProvenance(
                "/result/gateway_catalog",
                TrustClass.AUTHORITY,
                "gateway_enforcement",
            ),
            FieldProvenance(
                "/result/context", TrustClass.UNTRUSTED_CONTENT, "project_context"
            ),
            FieldProvenance(
                "/result/work_item", TrustClass.UNTRUSTED_CONTENT, "work_item"
            ),
            FieldProvenance(
                "/result/handoff", TrustClass.UNTRUSTED_CONTENT, "work_handoff"
            ),
            FieldProvenance(
                "/result/work_memory", TrustClass.UNTRUSTED_CONTENT, "work_memory"
            ),
            FieldProvenance(
                "/result/changes_requested",
                TrustClass.UNTRUSTED_CONTENT,
                "review_feedback",
            ),
            FieldProvenance(
                "/result/review_assignment",
                TrustClass.UNTRUSTED_CONTENT,
                "review_assignment",
            ),
            FieldProvenance(
                "/result/workflow_assignment",
                TrustClass.UNTRUSTED_CONTENT,
                "workflow_assignment",
            ),
            FieldProvenance(
                "/result/discovery_assignment",
                TrustClass.UNTRUSTED_CONTENT,
                "discovery_assignment",
            ),
            FieldProvenance(
                "/result/advisory_assignment",
                TrustClass.UNTRUSTED_CONTENT,
                "advisory_assignment",
            ),
        ),
    ),
    GatewayCapability.DISCOVERY_TRACE_SUBMIT: _contract(
        GatewayCapability.DISCOVERY_TRACE_SUBMIT,
        "_submit_discovery_trace",
        ("trace", "idempotency_key"),
        _object(
            {
                "trace": _ref("discovery-trace-v1.schema.json"),
                "idempotency_key": _IDEMPOTENCY_KEY,
            }
        ),
        _result("trace_id", "content_sha256", "agent_action"),
        follow_up_actions=("exit",),
    ),
    GatewayCapability.DISCOVERY_RULING_SUBMIT: _contract(
        GatewayCapability.DISCOVERY_RULING_SUBMIT,
        "_submit_discovery_ruling",
        ("ruling", "idempotency_key"),
        _object(
            {
                "ruling": _ref("discovery-ruling-v1.schema.json"),
                "idempotency_key": _IDEMPOTENCY_KEY,
            }
        ),
        _result(
            "ruling_id",
            "sequence",
            "disposition",
            "content_sha256",
            "agent_action",
        ),
        follow_up_actions=("exit", "submit_handoff_then_exit"),
    ),
    GatewayCapability.ADVISORY_BLIND_SUBMIT: _contract(
        GatewayCapability.ADVISORY_BLIND_SUBMIT,
        "_submit_advisory_blind_review",
        ("review", "idempotency_key"),
        _object(
            {
                "review": _ref("advisory-blind-review-v1.schema.json"),
                "idempotency_key": _IDEMPOTENCY_KEY,
            }
        ),
        _result("blind_review_id", "content_sha256", "agent_action"),
        follow_up_actions=("exit",),
    ),
    GatewayCapability.ADVISORY_CHALLENGE_SUBMIT: _contract(
        GatewayCapability.ADVISORY_CHALLENGE_SUBMIT,
        "_submit_advisory_challenge_review",
        ("review", "idempotency_key"),
        _object(
            {
                "review": _ref("advisory-challenge-review-v1.schema.json"),
                "idempotency_key": _IDEMPOTENCY_KEY,
            }
        ),
        _result("challenge_review_id", "content_sha256", "agent_action"),
        follow_up_actions=("exit",),
    ),
    GatewayCapability.ADVISORY_REPORT_SUBMIT: _contract(
        GatewayCapability.ADVISORY_REPORT_SUBMIT,
        "_submit_advisory_report",
        ("report", "idempotency_key"),
        _object(
            {
                "report": _ref("advisory-report-v1.schema.json"),
                "idempotency_key": _IDEMPOTENCY_KEY,
            }
        ),
        _result("report_id", "content_sha256", "agent_action"),
        follow_up_actions=("exit",),
    ),
    GatewayCapability.WORKFLOW_SCOPE_REVIEW_SUBMIT: _contract(
        GatewayCapability.WORKFLOW_SCOPE_REVIEW_SUBMIT,
        "_submit_workflow_scope_review",
        ("review", "idempotency_key"),
        _object(
            {
                "review": _ref("workflow-scope-review-v1.schema.json"),
                "idempotency_key": _IDEMPOTENCY_KEY,
            }
        ),
        _result(
            "scope_review_id",
            "content_sha256",
            "expected_review_outcome",
            "failed_must_keys",
            "regression_probe_keys",
            "follow_up_probe_keys",
        ),
        follow_up_actions=("submit_exact_head_review",),
    ),
    GatewayCapability.INTERACTION_OPEN: _contract(
        GatewayCapability.INTERACTION_OPEN,
        "_open_interaction",
        ("kind", "recipient_handle", "title", "body", "idempotency_key"),
        _object(
            {
                "kind": _text(
                    32,
                    values=tuple(kind.value for kind in InteractionKind),
                ),
                "recipient_handle": _text(32, pattern=KRAB_HANDLE_RE.pattern),
                "title": _text(MAX_TITLE_BYTES),
                "body": _text(MAX_BODY_BYTES),
                "idempotency_key": _IDEMPOTENCY_KEY,
            }
        ),
        _result(
            "interaction_id",
            "interaction_state",
            "interaction_version",
            "request_item_id",
            "run_state",
            "run_version",
            "agent_action",
        ),
        follow_up_actions=("submit_handoff_then_exit", "exit"),
    ),
    GatewayCapability.INTERACTION_READ: _contract(
        GatewayCapability.INTERACTION_READ,
        "_read_interaction",
        ("interaction_id",),
        _object({"interaction_id": _INTERACTION_ID}),
        _result(
            "interaction_id",
            "run_id",
            "response_item_id",
            "interaction_state",
            "interaction_version",
            "run_state",
            "run_version",
            "untrusted_title",
            "untrusted_body",
            "content_risk_flags",
        ),
        provenance=(
            CONTROL_ROOT,
            FieldProvenance(
                "/result/untrusted_title",
                TrustClass.UNTRUSTED_CONTENT,
                "human_response",
            ),
            FieldProvenance(
                "/result/untrusted_body",
                TrustClass.UNTRUSTED_CONTENT,
                "human_response",
            ),
        ),
    ),
    GatewayCapability.HANDOFF_SUBMIT: _contract(
        GatewayCapability.HANDOFF_SUBMIT,
        "_submit_handoff",
        (
            "schema_version",
            "work_state",
            "summary",
            "decisions",
            "artifacts",
            "next_steps",
            "pending_interaction_ids",
        ),
        _ref("handoff-v1.schema.json"),
        _result("handoff_id", "attempt_id", "content_sha256", "agent_action"),
        follow_up_actions=("exit",),
    ),
    GatewayCapability.PROVIDER_INVOKE: _contract(
        GatewayCapability.PROVIDER_INVOKE,
        "_invoke_provider",
        ("grant", "request"),
        _object(
            {
                "grant": _text(64, pattern=PROVIDER_NAME_RE.pattern),
                "request": {
                    "type": "object",
                    "x-runtime-validator": "pinned provider grant protocol",
                },
            }
        ),
        _result(
            "invocation_id",
            "grant_name",
            "provider",
            "model",
            "status_code",
            "request_bytes",
            "response_bytes",
            "untrusted_response",
        ),
        provenance=(
            CONTROL_ROOT,
            FieldProvenance(
                "/result/untrusted_response",
                TrustClass.UNTRUSTED_CONTENT,
                "provider_response",
            ),
        ),
    ),
    GatewayCapability.WORK_READ: _contract(
        GatewayCapability.WORK_READ,
        "_read_work",
        ("after_sequence", "limit"),
        _object(
            {
                "after_sequence": _nullable(_SEQUENCE),
                "limit": _integer(1, 16),
            }
        ),
        _result(
            "thread_id",
            "work_item_id",
            "generation",
            "state",
            "version",
            "log_sequence",
            "created_at",
            "updated_at",
            "completed_at",
            "checkpoint",
            "entries",
            "entries_truncated",
            "next_after_sequence",
            "assignments",
        ),
        provenance=(
            CONTROL_ROOT,
            FieldProvenance(
                "/result/checkpoint", TrustClass.UNTRUSTED_CONTENT, "work_memory"
            ),
            FieldProvenance(
                "/result/entries", TrustClass.UNTRUSTED_CONTENT, "work_memory"
            ),
            FieldProvenance(
                "/result/assignments", TrustClass.UNTRUSTED_CONTENT, "work_memory"
            ),
        ),
    ),
    GatewayCapability.WORK_APPEND: _contract(
        GatewayCapability.WORK_APPEND,
        "_append_work",
        ("assignment_id", "idempotency_key", "entry"),
        _object(
            {
                "assignment_id": _nullable(_ASSIGNMENT_ID),
                "idempotency_key": _IDEMPOTENCY_KEY,
                "entry": {
                    "oneOf": [
                        _ref("work-log-entry-v1.schema.json"),
                        _ref("work-log-entry-v2.schema.json"),
                    ]
                },
            }
        ),
        _result("entry_id", "sequence", "content_sha256"),
    ),
    GatewayCapability.WORK_CHECKPOINT: _contract(
        GatewayCapability.WORK_CHECKPOINT,
        "_checkpoint_work",
        (
            "expected_thread_version",
            "through_sequence",
            "idempotency_key",
            "checkpoint",
        ),
        _object(
            {
                "expected_thread_version": _VERSION,
                "through_sequence": _SEQUENCE,
                "idempotency_key": _IDEMPOTENCY_KEY,
                "checkpoint": _ref("work-memory-checkpoint-v1.schema.json"),
            }
        ),
        _result("checkpoint_id", "version", "through_sequence", "content_sha256"),
    ),
    GatewayCapability.WORK_ASSIGN: _contract(
        GatewayCapability.WORK_ASSIGN,
        "_assign_work",
        (
            "expected_thread_version",
            "target_profile_name",
            "idempotency_key",
            "spec",
        ),
        _object(
            {
                "expected_thread_version": _VERSION,
                "target_profile_name": _text(64, pattern=NAME_RE.pattern),
                "idempotency_key": _IDEMPOTENCY_KEY,
                "spec": _ref("work-assignment-v1.schema.json"),
            }
        ),
        _result("assignment_id", "state", "version"),
    ),
    GatewayCapability.WORK_ASSIGNMENT_CLAIM: _contract(
        GatewayCapability.WORK_ASSIGNMENT_CLAIM,
        "_claim_assignment",
        ("assignment_id", "expected_assignment_version", "lease_seconds"),
        _object(
            {
                "assignment_id": _ASSIGNMENT_ID,
                "expected_assignment_version": _VERSION,
                "lease_seconds": _integer(30, 86_400),
            }
        ),
        _result("assignment_id", "state", "version", "lease_expires_at"),
    ),
    GatewayCapability.WORK_ASSIGNMENT_COMPLETE: _contract(
        GatewayCapability.WORK_ASSIGNMENT_COMPLETE,
        "_complete_assignment",
        (
            "assignment_id",
            "expected_assignment_version",
            "idempotency_key",
            "result",
        ),
        _object(
            {
                "assignment_id": _ASSIGNMENT_ID,
                "expected_assignment_version": _VERSION,
                "idempotency_key": _IDEMPOTENCY_KEY,
                "result": {
                    "oneOf": [
                        _ref("work-log-entry-v1.schema.json"),
                        _ref("work-log-entry-v2.schema.json"),
                    ]
                },
            }
        ),
        _result("assignment_id", "state", "version", "result_entry_id"),
    ),
}

if set(_CONTRACTS) != set(GatewayCapability):
    missing = set(GatewayCapability).difference(_CONTRACTS)
    extra = set(_CONTRACTS).difference(GatewayCapability)
    raise RuntimeError(
        "gateway operation contract registry is incomplete: "
        f"missing={sorted(item.value for item in missing)} "
        f"extra={sorted(str(item) for item in extra)}"
    )

GATEWAY_OPERATION_CONTRACTS = MappingProxyType(_CONTRACTS)


def operation_contract(capability: GatewayCapability) -> OperationContract:
    try:
        return GATEWAY_OPERATION_CONTRACTS[capability]
    except KeyError as exc:
        raise GatewayContractError("gateway operation has no contract") from exc


def _schema_references(value: object) -> set[str]:
    references: set[str] = set()
    if isinstance(value, dict):
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.startswith("schemas/"):
            references.add(reference)
        for item in value.values():
            references.update(_schema_references(item))
    elif isinstance(value, list):
        for item in value:
            references.update(_schema_references(item))
    return references


def _load_schema_document(reference: str) -> dict[str, Any]:
    if SCHEMA_REFERENCE_RE.fullmatch(reference) is None:
        raise GatewayContractError("gateway catalog schema reference is invalid")
    source_root = Path(__file__).resolve().parents[2]
    candidates = (
        source_root / reference,
        Path(sys.prefix) / "share" / "magic-glass-box" / reference,
        Path(sys.base_prefix) / "share" / "magic-glass-box" / reference,
    )
    raw: bytes | None = None
    for candidate in candidates:
        try:
            raw = candidate.read_bytes()
        except OSError:
            continue
        break
    if raw is None:
        raise GatewayContractError("gateway catalog schema is unavailable")
    if not 1 <= len(raw) <= MAX_GATEWAY_SCHEMA_BYTES:
        raise GatewayContractError("gateway catalog schema exceeds its size limit")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise GatewayContractError("gateway catalog schema is invalid")
            result[key] = item
        return result

    def reject_constant(_value: str) -> None:
        raise GatewayContractError("gateway catalog schema is invalid")

    try:
        document = json.loads(
            raw.decode("utf-8", "strict"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except GatewayContractError:
        raise
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        ValueError,
    ) as exc:
        raise GatewayContractError("gateway catalog schema is invalid") from exc
    if not isinstance(document, dict):
        raise GatewayContractError("gateway catalog schema is invalid")
    return document


def build_gateway_catalog(
    capabilities: tuple[GatewayCapability, ...] | frozenset[GatewayCapability],
    *,
    requested_version: int,
) -> dict[str, Any]:
    if (
        isinstance(requested_version, bool)
        or requested_version != GATEWAY_CATALOG_VERSION
    ):
        raise GatewayContractError("unsupported gateway catalog version")
    try:
        normalized = tuple(GatewayCapability(item) for item in capabilities)
    except (TypeError, ValueError) as exc:
        raise GatewayContractError("gateway catalog capability is invalid") from exc
    if len(set(normalized)) != len(normalized):
        raise GatewayContractError("gateway catalog capabilities must be unique")
    entries = [
        operation_contract(capability).to_catalog_entry()
        for capability in sorted(normalized, key=lambda item: item.value)
    ]
    references: set[str] = set()
    for entry in entries:
        references.update(_schema_references(entry["request_schema"]))
        references.update(_schema_references(entry["result_schema"]))
    catalog = {
        "catalog_version": GATEWAY_CATALOG_VERSION,
        "max_catalog_bytes": MAX_GATEWAY_CATALOG_BYTES,
        "meta_operation": GATEWAY_CATALOG_OPERATION,
        "operations": entries,
        "schema_documents": {
            reference: _load_schema_document(reference)
            for reference in sorted(references)
        },
    }
    encoded = canonical_json(catalog).encode()
    if len(encoded) > MAX_GATEWAY_CATALOG_BYTES:
        raise GatewayContractError("gateway catalog exceeds its size limit")
    return catalog


def _pointer_exists(result: object, path: str) -> bool:
    if path == "/result":
        return True
    if not path.startswith("/result/"):
        return False
    current = result
    for part in path.removeprefix("/result/").split("/"):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return True


def response_field_provenance(
    capability: GatewayCapability,
    result: object,
) -> list[dict[str, str]]:
    return [
        item.to_dict()
        for item in operation_contract(capability).response_provenance
        if _pointer_exists(result, item.path)
    ]


def validate_result_shape(
    capability: GatewayCapability,
    result: object,
) -> None:
    schema = operation_contract(capability).result_schema
    properties = schema.get("properties")
    if (
        not isinstance(result, dict)
        or not isinstance(properties, dict)
        or set(result) != set(properties)
    ):
        raise GatewayContractError("gateway operation returned an invalid result shape")
