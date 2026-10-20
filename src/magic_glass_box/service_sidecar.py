from __future__ import annotations

import json
import os
import re
import socket
import stat
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

from .domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    parse_rfc3339_timestamp,
    require_string,
    validate_provider_name,
)

SERVICE_SIDECAR_SCHEMA_VERSION = 1
SERVICE_SIDECAR_PROTOCOL_VERSION = 1
SUPPORTED_SERVICE_SIDECAR_PROTOCOL_VERSIONS = frozenset({1})
MAX_SERVICE_CAPABILITIES = 64
MAX_SERVICE_MAPPINGS = 64
MAX_OPERATION_PARAMETERS_BYTES = 256 * 1024
MAX_SERVICE_FRAME_OVERHEAD = 64 * 1024
MAX_UNIX_SOCKET_PATH_BYTES = 100
MAX_RETRY_AFTER_SECONDS = 30 * 24 * 60 * 60

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SERVICE_SIDECAR_ID_RE = re.compile(r"^ssc_[0-9a-f]{32}$")
SERVICE_OPERATION_ID_RE = re.compile(r"^sop_[0-9a-f]{32}$")
CAPABILITY_RE = re.compile(
    r"^(?:work|change|checks|refs|landing)\.[a-z][a-z0-9_-]{0,63}$"
)
IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
OBJECT_ID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
OBJECT_FORMAT_LENGTHS = {"sha1": 40, "sha256": 64}
HOST_RE = re.compile(
    r"^(?:[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?|"
    r"\[[0-9a-f:.]+\])(?::[1-9][0-9]{0,4})?$"
)
FORBIDDEN_PARAMETER_FIELDS = frozenset(
    {
        "accesstoken",
        "apikey",
        "apitoken",
        "authorization",
        "bearertoken",
        "credential",
        "credentialref",
        "credentials",
        "password",
        "privatekey",
        "refreshtoken",
        "secret",
        "token",
    }
)


class ServiceSidecarError(RuntimeError):
    """A service sidecar operation failed without exposing provider data."""


class ServiceSidecarUnavailableError(ServiceSidecarError):
    """No operation bytes were accepted by the sidecar."""


class ServiceSidecarAmbiguousError(ServiceSidecarError):
    """The sidecar may have performed an effect before transport failed."""


class ServiceSidecarClient(Protocol):
    def invoke(self, request: ServiceOperationRequest) -> ServiceOperationResponse: ...


def _exact_fields(
    value: Mapping[str, Any],
    *,
    expected: set[str],
    subject: str,
) -> None:
    unknown = set(value) - expected
    if unknown:
        raise ValidationError(
            f"{subject} contains unknown fields: " + ", ".join(sorted(unknown))
        )
    missing = expected - set(value)
    if missing:
        raise ValidationError(
            f"{subject} is missing fields: " + ", ".join(sorted(missing))
        )


def _schema_version(value: object) -> int:
    if isinstance(value, bool) or value != SERVICE_SIDECAR_SCHEMA_VERSION:
        raise ValidationError(
            f"schema_version must be {SERVICE_SIDECAR_SCHEMA_VERSION}"
        )
    return SERVICE_SIDECAR_SCHEMA_VERSION


def _protocol_version(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value not in SUPPORTED_SERVICE_SIDECAR_PROTOCOL_VERSIONS
    ):
        raise ValidationError(
            "connector_protocol_version must be one of "
            + ", ".join(
                str(item)
                for item in sorted(SUPPORTED_SERVICE_SIDECAR_PROTOCOL_VERSIONS)
            )
        )
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise ValidationError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _identifier(value: object, *, field: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValidationError(f"{field} is invalid")
    return value


def _nonblank(value: object, *, field: str, max_bytes: int) -> str:
    result = require_string(value, field=field, max_bytes=max_bytes)
    if not result.strip():
        raise ValidationError(f"{field} must not be blank")
    return result


def _bounded_integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValidationError(f"{field} must be between {minimum} and {maximum}")
    return value


def _request_bytes(value: object, *, field: str = "request_bytes") -> int:
    return _bounded_integer(value, field=field, minimum=1, maximum=2 * 1024 * 1024)


def _response_bytes(value: object, *, field: str = "response_bytes") -> int:
    return _bounded_integer(value, field=field, minimum=1, maximum=4 * 1024 * 1024)


def _timeout_ms(value: object) -> int:
    return _bounded_integer(value, field="timeout_ms", minimum=100, maximum=120_000)


def _timestamp(value: object, *, field: str) -> str:
    parsed = parse_rfc3339_timestamp(value, field=field)
    return parsed.astimezone(UTC).isoformat(timespec="microseconds")


def _unix_socket_path(value: object) -> str:
    text = require_string(
        value,
        field="service sidecar Unix socket path",
        max_bytes=MAX_UNIX_SOCKET_PATH_BYTES,
    )
    if not Path(text).is_absolute():
        raise ValidationError("service sidecar Unix socket path must be absolute")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in text):
        raise ValidationError(
            "service sidecar Unix socket path contains control characters"
        )
    return text


def _host(value: object) -> str:
    text = _nonblank(value, field="service mapping host", max_bytes=255).lower()
    if HOST_RE.fullmatch(text) is None or any(mark in text for mark in "/@?#"):
        raise ValidationError("service mapping host is invalid")
    if text.endswith((".", "-")) or ".." in text:
        raise ValidationError("service mapping host is invalid")
    if text.startswith("["):
        suffix = text[text.rfind("]") + 1 :]
        port = suffix[1:] if suffix else None
    else:
        port = text.rsplit(":", 1)[1] if ":" in text else None
    if port is not None and int(port) > 65535:
        raise ValidationError("service mapping host is invalid")
    return text


def _repository(value: object) -> str:
    text = _nonblank(value, field="service mapping repository", max_bytes=512)
    if any(
        character in ":@?#" or ord(character) < 0x20 or ord(character) == 0x7F
        for character in text
    ):
        raise ValidationError("service mapping repository is invalid")
    return text


def _capability_name(value: object) -> str:
    if not isinstance(value, str) or CAPABILITY_RE.fullmatch(value) is None:
        raise ValidationError("service capability name is invalid")
    return value


def _validate_no_secret_fields(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValidationError("operation parameter field names must be text")
            normalized = "".join(
                character for character in key.casefold() if character.isalnum()
            )
            if normalized in FORBIDDEN_PARAMETER_FIELDS:
                raise ValidationError(
                    f"operation parameters cannot contain credential field: {key}"
                )
            _validate_no_secret_fields(item)
    elif isinstance(value, list):
        for item in value:
            _validate_no_secret_fields(item)


def _detached_json_object(canonical: str) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(canonical))


@dataclass(frozen=True)
class ServiceSidecarTransport:
    kind: str
    socket_path: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServiceSidecarTransport:
        if not isinstance(value, dict):
            raise ValidationError("service sidecar transport must be an object")
        _exact_fields(
            value,
            expected={"kind", "socket_path"},
            subject="service sidecar transport",
        )
        if value["kind"] != "unix":
            raise ValidationError("service sidecar transport kind must be unix")
        return cls(kind="unix", socket_path=_unix_socket_path(value["socket_path"]))

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "socket_path": self.socket_path}


@dataclass(frozen=True)
class ServiceCapability:
    name: str
    version: int
    mode: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServiceCapability:
        if not isinstance(value, dict):
            raise ValidationError("service capability must be an object")
        _exact_fields(
            value,
            expected={"name", "version", "mode"},
            subject="service capability",
        )
        mode = value["mode"]
        if mode not in {"read", "effect"}:
            raise ValidationError("service capability mode must be read or effect")
        return cls(
            name=_capability_name(value["name"]),
            version=_bounded_integer(
                value["version"],
                field="service capability version",
                minimum=1,
                maximum=1_000_000,
            ),
            mode=mode,
        )

    def to_dict(self) -> dict[str, Any]:
        return {"mode": self.mode, "name": self.name, "version": self.version}


@dataclass(frozen=True)
class ServiceMapping:
    name: str
    service: str
    host: str
    repository: str
    account: str
    credential_scope_sha256: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServiceMapping:
        if not isinstance(value, dict):
            raise ValidationError("service mapping must be an object")
        _exact_fields(
            value,
            expected={
                "name",
                "service",
                "host",
                "repository",
                "account",
                "credential_scope_sha256",
            },
            subject="service mapping",
        )
        return cls(
            name=validate_provider_name(value["name"], field="service mapping name"),
            service=validate_provider_name(
                value["service"], field="service mapping service"
            ),
            host=_host(value["host"]),
            repository=_repository(value["repository"]),
            account=_nonblank(
                value["account"], field="service mapping account", max_bytes=256
            ),
            credential_scope_sha256=_sha256(
                value["credential_scope_sha256"],
                field="service mapping credential scope",
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "credential_scope_sha256": self.credential_scope_sha256,
            "host": self.host,
            "name": self.name,
            "repository": self.repository,
            "service": self.service,
        }


@dataclass(frozen=True)
class ServiceSidecarSpec:
    schema_version: int
    connector_protocol_version: int
    deployment_sha256: str
    transport: ServiceSidecarTransport
    capabilities: tuple[ServiceCapability, ...]
    mappings: tuple[ServiceMapping, ...]
    request_bytes: int
    response_bytes: int
    timeout_ms: int
    max_concurrency: int
    max_requests_per_minute: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServiceSidecarSpec:
        if not isinstance(value, dict):
            raise ValidationError("service sidecar spec must be an object")
        _exact_fields(
            value,
            expected={
                "schema_version",
                "connector_protocol_version",
                "deployment_sha256",
                "transport",
                "capabilities",
                "mappings",
                "request_bytes",
                "response_bytes",
                "timeout_ms",
                "max_concurrency",
                "max_requests_per_minute",
            },
            subject="service sidecar spec",
        )
        raw_capabilities = value["capabilities"]
        if (
            not isinstance(raw_capabilities, list)
            or not 1 <= len(raw_capabilities) <= MAX_SERVICE_CAPABILITIES
        ):
            raise ValidationError(
                "service sidecar capabilities must contain 1 to "
                f"{MAX_SERVICE_CAPABILITIES} entries"
            )
        capabilities = tuple(
            ServiceCapability.from_mapping(item) for item in raw_capabilities
        )
        capability_keys = {(item.name, item.version) for item in capabilities}
        if len(capability_keys) != len(capabilities):
            raise ValidationError("service sidecar capabilities must be unique")
        raw_mappings = value["mappings"]
        if (
            not isinstance(raw_mappings, list)
            or not 1 <= len(raw_mappings) <= MAX_SERVICE_MAPPINGS
        ):
            raise ValidationError(
                "service sidecar mappings must contain 1 to "
                f"{MAX_SERVICE_MAPPINGS} entries"
            )
        mappings = tuple(ServiceMapping.from_mapping(item) for item in raw_mappings)
        if len({item.name for item in mappings}) != len(mappings):
            raise ValidationError("service sidecar mapping names must be unique")
        result = cls(
            schema_version=_schema_version(value["schema_version"]),
            connector_protocol_version=_protocol_version(
                value["connector_protocol_version"]
            ),
            deployment_sha256=_sha256(
                value["deployment_sha256"], field="service sidecar deployment"
            ),
            transport=ServiceSidecarTransport.from_mapping(value["transport"]),
            capabilities=capabilities,
            mappings=mappings,
            request_bytes=_request_bytes(value["request_bytes"]),
            response_bytes=_response_bytes(value["response_bytes"]),
            timeout_ms=_timeout_ms(value["timeout_ms"]),
            max_concurrency=_bounded_integer(
                value["max_concurrency"],
                field="max_concurrency",
                minimum=1,
                maximum=16,
            ),
            max_requests_per_minute=_bounded_integer(
                value["max_requests_per_minute"],
                field="max_requests_per_minute",
                minimum=1,
                maximum=10_000,
            ),
        )
        if len(result.canonical.encode("utf-8")) > 128 * 1024:
            raise ValidationError("service sidecar spec exceeds 131072 UTF-8 bytes")
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "capabilities": [item.to_dict() for item in self.capabilities],
            "connector_protocol_version": self.connector_protocol_version,
            "deployment_sha256": self.deployment_sha256,
            "mappings": [item.to_dict() for item in self.mappings],
            "max_concurrency": self.max_concurrency,
            "max_requests_per_minute": self.max_requests_per_minute,
            "request_bytes": self.request_bytes,
            "response_bytes": self.response_bytes,
            "schema_version": self.schema_version,
            "timeout_ms": self.timeout_ms,
            "transport": self.transport.to_dict(),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)

    def validate_operation(
        self, connector_name: str, operation: ServiceOperationSpec
    ) -> None:
        connector_name = validate_provider_name(
            connector_name, field="service sidecar name"
        )
        if not isinstance(operation, ServiceOperationSpec):
            raise ValidationError("operation must be a ServiceOperationSpec")
        if operation.connector_name != connector_name:
            raise ValidationError("operation connector does not match sidecar")
        if operation.deployment_sha256 != self.deployment_sha256:
            raise ValidationError("operation deployment does not match sidecar")
        mapping = next(
            (item for item in self.mappings if item.name == operation.mapping.name),
            None,
        )
        if mapping is None:
            raise ValidationError("operation mapping is not registered by sidecar")
        if mapping != operation.mapping:
            raise ValidationError("operation mapping does not match sidecar authority")
        capability = next(
            (
                item
                for item in self.capabilities
                if (item.name, item.version)
                == (operation.capability.name, operation.capability.version)
            ),
            None,
        )
        if capability is None:
            raise ValidationError("operation capability is not registered by sidecar")
        if capability != operation.capability:
            raise ValidationError(
                "operation capability does not match sidecar authority"
            )
        if operation.request_bytes > self.request_bytes:
            raise ValidationError("operation request limit exceeds sidecar authority")
        if operation.response_bytes > self.response_bytes:
            raise ValidationError("operation response limit exceeds sidecar authority")
        if operation.timeout_ms > self.timeout_ms:
            raise ValidationError("operation timeout exceeds sidecar authority")


@dataclass(frozen=True)
class ServiceOperationTarget:
    repository: str
    resource_id: str
    object_format: str | None
    expected_revision: str | None
    desired_revision: str | None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServiceOperationTarget:
        if not isinstance(value, dict):
            raise ValidationError("service operation target must be an object")
        _exact_fields(
            value,
            expected={
                "repository",
                "resource_id",
                "object_format",
                "expected_revision",
                "desired_revision",
            },
            subject="service operation target",
        )
        object_format = value["object_format"]
        if object_format is not None and object_format not in OBJECT_FORMAT_LENGTHS:
            raise ValidationError("service operation object_format is invalid")
        revisions: dict[str, str | None] = {}
        for field in ("expected_revision", "desired_revision"):
            revision = value[field]
            if revision is not None:
                if object_format is None:
                    raise ValidationError(
                        "service operation revisions require an object format"
                    )
                if (
                    not isinstance(revision, str)
                    or OBJECT_ID_RE.fullmatch(revision) is None
                    or len(revision) != OBJECT_FORMAT_LENGTHS[object_format]
                ):
                    raise ValidationError(f"service operation {field} is invalid")
            revisions[field] = revision
        return cls(
            repository=_repository(value["repository"]),
            resource_id=_nonblank(
                value["resource_id"],
                field="service operation resource_id",
                max_bytes=512,
            ),
            object_format=object_format,
            expected_revision=revisions["expected_revision"],
            desired_revision=revisions["desired_revision"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "desired_revision": self.desired_revision,
            "expected_revision": self.expected_revision,
            "object_format": self.object_format,
            "repository": self.repository,
            "resource_id": self.resource_id,
        }


@dataclass(frozen=True)
class ServiceOperationSpec:
    schema_version: int
    connector_name: str
    deployment_sha256: str
    mapping: ServiceMapping
    capability: ServiceCapability
    idempotency_key: str
    target: ServiceOperationTarget
    _parameters_json: str
    request_bytes: int
    response_bytes: int
    timeout_ms: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServiceOperationSpec:
        if not isinstance(value, dict):
            raise ValidationError("service operation spec must be an object")
        _exact_fields(
            value,
            expected={
                "schema_version",
                "connector_name",
                "deployment_sha256",
                "mapping",
                "capability",
                "idempotency_key",
                "target",
                "parameters",
                "request_bytes",
                "response_bytes",
                "timeout_ms",
            },
            subject="service operation spec",
        )
        parameters = value["parameters"]
        if not isinstance(parameters, dict):
            raise ValidationError("service operation parameters must be an object")
        _validate_no_secret_fields(parameters)
        parameters_json = canonical_json(parameters)
        if len(parameters_json.encode("utf-8")) > MAX_OPERATION_PARAMETERS_BYTES:
            raise ValidationError("service operation parameters are oversized")
        mapping = ServiceMapping.from_mapping(value["mapping"])
        target = ServiceOperationTarget.from_mapping(value["target"])
        if target.repository != mapping.repository:
            raise ValidationError(
                "service operation repository does not match its mapping"
            )
        idempotency_key = value["idempotency_key"]
        if (
            not isinstance(idempotency_key, str)
            or IDEMPOTENCY_KEY_RE.fullmatch(idempotency_key) is None
        ):
            raise ValidationError("service operation idempotency_key is invalid")
        request_bytes = _request_bytes(value["request_bytes"])
        input_json = canonical_json(
            {"parameters": json.loads(parameters_json), "target": target.to_dict()}
        )
        if len(input_json.encode("utf-8")) > request_bytes:
            raise ValidationError("service operation input exceeds request limit")
        return cls(
            schema_version=_schema_version(value["schema_version"]),
            connector_name=validate_provider_name(
                value["connector_name"], field="service operation connector_name"
            ),
            deployment_sha256=_sha256(
                value["deployment_sha256"], field="service operation deployment"
            ),
            mapping=mapping,
            capability=ServiceCapability.from_mapping(value["capability"]),
            idempotency_key=idempotency_key,
            target=target,
            _parameters_json=parameters_json,
            request_bytes=request_bytes,
            response_bytes=_response_bytes(value["response_bytes"]),
            timeout_ms=_timeout_ms(value["timeout_ms"]),
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return _detached_json_object(self._parameters_json)

    @property
    def input_dict(self) -> dict[str, Any]:
        return {"parameters": self.parameters, "target": self.target.to_dict()}

    @property
    def input_sha256(self) -> str:
        return content_sha256(canonical_json(self.input_dict))

    def to_dict(self) -> dict[str, Any]:
        return {
            "capability": self.capability.to_dict(),
            "connector_name": self.connector_name,
            "deployment_sha256": self.deployment_sha256,
            "idempotency_key": self.idempotency_key,
            "mapping": self.mapping.to_dict(),
            "parameters": self.parameters,
            "request_bytes": self.request_bytes,
            "response_bytes": self.response_bytes,
            "schema_version": self.schema_version,
            "target": self.target.to_dict(),
            "timeout_ms": self.timeout_ms,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


_CORRELATION_FIELDS = (
    "schema_version",
    "connector_protocol_version",
    "operation_id",
    "connector_id",
    "deployment_sha256",
    "mapping",
    "capability",
    "phase",
    "input_sha256",
    "deadline",
    "request_limit_bytes",
    "response_limit_bytes",
)


def _operation_authority(value: Mapping[str, Any]) -> dict[str, Any]:
    phase = value["phase"]
    if phase not in {"invoke", "reconcile"}:
        raise ValidationError("service operation phase is invalid")
    return {
        "schema_version": _schema_version(value["schema_version"]),
        "connector_protocol_version": _protocol_version(
            value["connector_protocol_version"]
        ),
        "operation_id": _identifier(
            value["operation_id"],
            field="operation_id",
            pattern=SERVICE_OPERATION_ID_RE,
        ),
        "connector_id": _identifier(
            value["connector_id"],
            field="connector_id",
            pattern=SERVICE_SIDECAR_ID_RE,
        ),
        "deployment_sha256": _sha256(
            value["deployment_sha256"], field="service operation deployment"
        ),
        "mapping": ServiceMapping.from_mapping(value["mapping"]),
        "capability": ServiceCapability.from_mapping(value["capability"]),
        "phase": phase,
        "input_sha256": _sha256(value["input_sha256"], field="service operation input"),
        "deadline": _timestamp(value["deadline"], field="service operation deadline"),
        "request_limit_bytes": _request_bytes(
            value["request_limit_bytes"], field="request_limit_bytes"
        ),
        "response_limit_bytes": _response_bytes(
            value["response_limit_bytes"], field="response_limit_bytes"
        ),
    }


@dataclass(frozen=True)
class ServiceOperationRequest:
    schema_version: int
    connector_protocol_version: int
    operation_id: str
    connector_id: str
    deployment_sha256: str
    mapping: ServiceMapping
    capability: ServiceCapability
    phase: str
    input_sha256: str
    deadline: str
    request_limit_bytes: int
    response_limit_bytes: int
    target: ServiceOperationTarget
    _parameters_json: str

    @classmethod
    def create(
        cls,
        *,
        operation_id: str,
        connector_id: str,
        connector_protocol_version: int,
        operation: ServiceOperationSpec,
        phase: str,
        deadline: str,
    ) -> ServiceOperationRequest:
        if not isinstance(operation, ServiceOperationSpec):
            raise ValidationError("operation must be a ServiceOperationSpec")
        return cls.from_mapping(
            {
                "schema_version": SERVICE_SIDECAR_SCHEMA_VERSION,
                "connector_protocol_version": connector_protocol_version,
                "operation_id": operation_id,
                "connector_id": connector_id,
                "deployment_sha256": operation.deployment_sha256,
                "mapping": operation.mapping.to_dict(),
                "capability": operation.capability.to_dict(),
                "phase": phase,
                "input_sha256": operation.input_sha256,
                "deadline": deadline,
                "request_limit_bytes": operation.request_bytes,
                "response_limit_bytes": operation.response_bytes,
                "target": operation.target.to_dict(),
                "parameters": operation.parameters,
            }
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServiceOperationRequest:
        if not isinstance(value, dict):
            raise ValidationError("service operation request must be an object")
        _exact_fields(
            value,
            expected=set(_CORRELATION_FIELDS) | {"target", "parameters"},
            subject="service operation request",
        )
        authority = _operation_authority(value)
        target = ServiceOperationTarget.from_mapping(value["target"])
        if target.repository != authority["mapping"].repository:
            raise ValidationError(
                "service operation repository does not match its mapping"
            )
        parameters = value["parameters"]
        if not isinstance(parameters, dict):
            raise ValidationError("service operation parameters must be an object")
        _validate_no_secret_fields(parameters)
        parameters_json = canonical_json(parameters)
        input_json = canonical_json(
            {"parameters": json.loads(parameters_json), "target": target.to_dict()}
        )
        if len(input_json.encode("utf-8")) > authority["request_limit_bytes"]:
            raise ValidationError("service operation input exceeds request limit")
        if content_sha256(input_json) != authority["input_sha256"]:
            raise ValidationError("service operation input digest does not match")
        return cls(**authority, target=target, _parameters_json=parameters_json)

    @property
    def parameters(self) -> dict[str, Any]:
        return _detached_json_object(self._parameters_json)

    def correlation_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for field in _CORRELATION_FIELDS:
            value = getattr(self, field)
            if isinstance(value, (ServiceMapping, ServiceCapability)):
                value = value.to_dict()
            result[field] = value
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.correlation_dict(),
            "parameters": self.parameters,
            "target": self.target.to_dict(),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def correlation(self) -> str:
        return canonical_json(self.correlation_dict())


@dataclass(frozen=True)
class ServiceOperationReceipt:
    remote_identity: str
    observed_revision: str | None
    evidence_sha256: str
    closed_references: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServiceOperationReceipt:
        if not isinstance(value, dict):
            raise ValidationError("service operation receipt must be an object")
        _exact_fields(
            value,
            expected={
                "remote_identity",
                "observed_revision",
                "evidence_sha256",
                "closed_references",
            },
            subject="service operation receipt",
        )
        observed_revision = value["observed_revision"]
        if observed_revision is not None and (
            not isinstance(observed_revision, str)
            or OBJECT_ID_RE.fullmatch(observed_revision) is None
        ):
            raise ValidationError("service operation observed_revision is invalid")
        raw_closed = value["closed_references"]
        if not isinstance(raw_closed, list) or len(raw_closed) > 64:
            raise ValidationError(
                "service operation closed_references must contain at most 64 entries"
            )
        closed = tuple(
            _nonblank(item, field="closed reference", max_bytes=256)
            for item in raw_closed
        )
        if len(set(closed)) != len(closed):
            raise ValidationError("service operation closed references must be unique")
        return cls(
            remote_identity=_nonblank(
                value["remote_identity"],
                field="service operation remote identity",
                max_bytes=512,
            ),
            observed_revision=observed_revision,
            evidence_sha256=_sha256(
                value["evidence_sha256"], field="service operation evidence"
            ),
            closed_references=closed,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "closed_references": list(self.closed_references),
            "evidence_sha256": self.evidence_sha256,
            "observed_revision": self.observed_revision,
            "remote_identity": self.remote_identity,
        }


@dataclass(frozen=True)
class ServiceOperationResponse:
    schema_version: int
    connector_protocol_version: int
    operation_id: str
    connector_id: str
    deployment_sha256: str
    mapping: ServiceMapping
    capability: ServiceCapability
    phase: str
    input_sha256: str
    deadline: str
    request_limit_bytes: int
    response_limit_bytes: int
    outcome: str
    provider_status: int | None
    request_bytes: int
    response_bytes: int
    error_code: str | None
    retry_after_seconds: int | None
    receipt: ServiceOperationReceipt | None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServiceOperationResponse:
        if not isinstance(value, dict):
            raise ValidationError("service operation response must be an object")
        _exact_fields(
            value,
            expected=set(_CORRELATION_FIELDS)
            | {
                "outcome",
                "provider_status",
                "request_bytes",
                "response_bytes",
                "error_code",
                "retry_after_seconds",
                "receipt",
            },
            subject="service operation response",
        )
        authority = _operation_authority(value)
        outcome = value["outcome"]
        allowed = {
            "succeeded",
            "denied",
            "conflict",
            "retryable",
            "throttled",
            "uncertain",
            "not_found",
        }
        if outcome not in allowed:
            raise ValidationError("service operation outcome is invalid")
        if outcome == "not_found" and authority["phase"] != "reconcile":
            raise ValidationError("not_found is valid only while reconciling")
        provider_status = value["provider_status"]
        if provider_status is not None and (
            isinstance(provider_status, bool)
            or not isinstance(provider_status, int)
            or not 100 <= provider_status <= 599
        ):
            raise ValidationError("provider_status must be between 100 and 599")
        request_bytes = _bounded_integer(
            value["request_bytes"],
            field="request_bytes",
            minimum=0,
            maximum=authority["request_limit_bytes"],
        )
        response_bytes = _bounded_integer(
            value["response_bytes"],
            field="response_bytes",
            minimum=0,
            maximum=authority["response_limit_bytes"],
        )
        error_code = value["error_code"]
        if error_code is not None:
            error_code = validate_provider_name(error_code, field="error_code")
        retry_after = value["retry_after_seconds"]
        if retry_after is not None:
            retry_after = _bounded_integer(
                retry_after,
                field="retry_after_seconds",
                minimum=1,
                maximum=MAX_RETRY_AFTER_SECONDS,
            )
            if outcome not in {"retryable", "throttled"}:
                raise ValidationError(
                    "retry_after_seconds requires a retryable or throttled outcome"
                )
        receipt_value = value["receipt"]
        receipt = (
            None
            if receipt_value is None
            else ServiceOperationReceipt.from_mapping(receipt_value)
        )
        if outcome == "succeeded":
            if (
                provider_status is None
                or not 200 <= provider_status < 300
                or error_code is not None
                or retry_after is not None
                or receipt is None
            ):
                raise ValidationError("successful service operation is inconsistent")
        elif error_code is None or receipt is not None:
            raise ValidationError(
                f"{outcome} service operation must have an error and no receipt"
            )
        return cls(
            **authority,
            outcome=outcome,
            provider_status=provider_status,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
            error_code=error_code,
            retry_after_seconds=retry_after,
            receipt=receipt,
        )

    def correlation_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for field in _CORRELATION_FIELDS:
            value = getattr(self, field)
            if isinstance(value, (ServiceMapping, ServiceCapability)):
                value = value.to_dict()
            result[field] = value
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.correlation_dict(),
            "error_code": self.error_code,
            "outcome": self.outcome,
            "provider_status": self.provider_status,
            "receipt": None if self.receipt is None else self.receipt.to_dict(),
            "request_bytes": self.request_bytes,
            "response_bytes": self.response_bytes,
            "retry_after_seconds": self.retry_after_seconds,
        }

    @property
    def correlation(self) -> str:
        return canonical_json(self.correlation_dict())

    def validate_correlation(self, request: ServiceOperationRequest) -> None:
        if not isinstance(request, ServiceOperationRequest):
            raise ValidationError("request must be a ServiceOperationRequest")
        if self.correlation != request.correlation:
            raise ValidationError("service sidecar response correlation does not match")
        if self.receipt is not None and self.receipt.observed_revision is not None:
            object_format = request.target.object_format
            if object_format is None or (
                len(self.receipt.observed_revision)
                != OBJECT_FORMAT_LENGTHS[object_format]
            ):
                raise ValidationError(
                    "service sidecar receipt revision does not match the target"
                )


def _strict_json_object(payload: bytes) -> dict[str, Any]:
    try:
        text = payload.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise ValidationError(
            "service sidecar response must be valid UTF-8 JSON"
        ) from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError(
                    "service sidecar response contains duplicate fields"
                )
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ValidationError("service sidecar response must contain finite JSON")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ValidationError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ValidationError("service sidecar response must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValidationError("service sidecar response must be an object")
    return value


def _remaining_timeout(deadline: datetime) -> float:
    remaining = (deadline - datetime.now(UTC)).total_seconds()
    if remaining <= 0:
        raise TimeoutError("service sidecar deadline has expired")
    return remaining


def _recv_exact(connection: socket.socket, length: int, *, deadline: datetime) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        connection.settimeout(_remaining_timeout(deadline))
        chunk = connection.recv(remaining)
        if not chunk:
            raise OSError(
                "service sidecar response ended before its frame was complete"
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _validate_unix_socket(path: str) -> None:
    socket_path = Path(path)
    try:
        info = socket_path.lstat()
        parent = socket_path.parent.lstat()
    except OSError as exc:
        raise ServiceSidecarUnavailableError("service sidecar is unavailable") from exc
    unsafe_parent = (
        stat.S_ISLNK(parent.st_mode)
        or not stat.S_ISDIR(parent.st_mode)
        or bool(parent.st_mode & 0o077)
    )
    unsafe_socket = (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISSOCK(info.st_mode)
        or bool(info.st_mode & 0o077)
    )
    if hasattr(os, "getuid"):
        unsafe_parent = unsafe_parent or parent.st_uid != os.getuid()
        unsafe_socket = unsafe_socket or info.st_uid != os.getuid()
    if unsafe_parent or unsafe_socket:
        raise ServiceSidecarUnavailableError(
            "service sidecar path is not a private Unix socket"
        )


class UnixServiceSidecarClient:
    """Send one bounded semantic operation over one registered Unix socket."""

    def __init__(self, socket_path: Path):
        try:
            self.socket_path = _unix_socket_path(str(socket_path))
        except ValidationError as exc:
            raise ServiceSidecarError(str(exc)) from exc

    def invoke(self, request: ServiceOperationRequest) -> ServiceOperationResponse:
        if not isinstance(request, ServiceOperationRequest):
            raise ServiceSidecarError("request must be a ServiceOperationRequest")
        deadline = datetime.fromisoformat(request.deadline)
        try:
            remaining = _remaining_timeout(deadline)
        except TimeoutError as exc:
            raise ServiceSidecarUnavailableError(
                "service sidecar deadline has expired"
            ) from exc
        payload = request.canonical.encode("utf-8")
        if len(payload) > request.request_limit_bytes + MAX_SERVICE_FRAME_OVERHEAD:
            raise ServiceSidecarError("service sidecar request frame is oversized")
        frame = struct.pack("!I", len(payload)) + payload
        try:
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        except (OSError, TimeoutError) as exc:
            raise ServiceSidecarUnavailableError(
                "service sidecar is unavailable"
            ) from exc
        try:
            _validate_unix_socket(self.socket_path)
            try:
                connection.settimeout(remaining)
                connection.connect(self.socket_path)
            except (OSError, TimeoutError) as exc:
                raise ServiceSidecarUnavailableError(
                    "service sidecar is unavailable"
                ) from exc
            try:
                connection.settimeout(_remaining_timeout(deadline))
            except (OSError, TimeoutError) as exc:
                raise ServiceSidecarUnavailableError(
                    "service sidecar is unavailable before dispatch"
                ) from exc
            try:
                connection.sendall(frame)
                raw_length = _recv_exact(connection, 4, deadline=deadline)
                length = struct.unpack("!I", raw_length)[0]
                maximum = request.response_limit_bytes + MAX_SERVICE_FRAME_OVERHEAD
                if not 1 <= length <= maximum:
                    raise ValidationError("service sidecar response frame is oversized")
                raw_response = _recv_exact(connection, length, deadline=deadline)
                response = ServiceOperationResponse.from_mapping(
                    _strict_json_object(raw_response)
                )
                response.validate_correlation(request)
                return response
            except (OSError, TimeoutError, ValidationError) as exc:
                raise ServiceSidecarAmbiguousError(
                    "service sidecar outcome is ambiguous"
                ) from exc
        finally:
            connection.close()
