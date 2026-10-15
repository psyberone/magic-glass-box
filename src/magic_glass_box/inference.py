from __future__ import annotations

import json
import re
import socket
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from .domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    require_string,
    validate_provider_name,
)

INFERENCE_SCHEMA_VERSION = 1
INFERENCE_CONNECTOR_PROTOCOL_VERSION = 1
MAX_CONNECTOR_ROUTES = 32
MAX_ROUTE_MODELS = 64
MAX_CONNECTOR_FRAME_OVERHEAD = 64 * 1024
MAX_UNIX_SOCKET_PATH_BYTES = 100
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CONNECTOR_ID_RE = re.compile(r"^ifc_[0-9a-f]{32}$")
PROVIDER_GRANT_ID_RE = re.compile(r"^pgr_[0-9a-f]{32}$")
PROVIDER_INVOCATION_ID_RE = re.compile(r"^pinv_[0-9a-f]{32}$")


class InferenceConnectorError(RuntimeError):
    """A connector operation failed without exposing private response data."""


class InferenceConnectorUnavailableError(InferenceConnectorError):
    """No invocation bytes were accepted by the connector."""


class InferenceConnectorAmbiguousError(InferenceConnectorError):
    """The connector may have consumed the invocation before transport failed."""


def _exact_fields(
    value: Mapping[str, Any], *, expected: set[str], subject: str
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


def _schema_version(value: object, *, field: str = "schema_version") -> int:
    if isinstance(value, bool) or value != INFERENCE_SCHEMA_VERSION:
        raise ValidationError(f"{field} must be {INFERENCE_SCHEMA_VERSION}")
    return INFERENCE_SCHEMA_VERSION


def _connector_protocol_version(value: object) -> int:
    if isinstance(value, bool) or value != INFERENCE_CONNECTOR_PROTOCOL_VERSION:
        raise ValidationError(
            f"connector_protocol_version must be {INFERENCE_CONNECTOR_PROTOCOL_VERSION}"
        )
    return INFERENCE_CONNECTOR_PROTOCOL_VERSION


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
    return _bounded_integer(
        value,
        field=field,
        minimum=1,
        maximum=2 * 1024 * 1024,
    )


def _response_bytes(value: object, *, field: str = "response_bytes") -> int:
    return _bounded_integer(
        value,
        field=field,
        minimum=1,
        maximum=4 * 1024 * 1024,
    )


def _timeout_ms(value: object) -> int:
    return _bounded_integer(
        value,
        field="timeout_ms",
        minimum=100,
        maximum=120_000,
    )


def _max_concurrency(value: object) -> int:
    return _bounded_integer(
        value,
        field="max_concurrency",
        minimum=1,
        maximum=16,
    )


def _timestamp(value: object, *, field: str) -> str:
    text = require_string(value, field=field, max_bytes=64)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(f"{field} must be a valid RFC 3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValidationError(f"{field} must include a timezone")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds")


def _detached_json_object(canonical: str) -> dict[str, Any]:
    """Return a fresh object from already validated canonical JSON."""

    return cast(dict[str, Any], json.loads(canonical))


def _unix_socket_path(value: object) -> str:
    text = require_string(
        value,
        field="connector Unix socket path",
        max_bytes=MAX_UNIX_SOCKET_PATH_BYTES,
    )
    if not Path(text).is_absolute():
        raise ValidationError("connector Unix socket path must be absolute")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in text):
        raise ValidationError("connector Unix socket path contains control characters")
    return text


@dataclass(frozen=True)
class InferenceConnectorTransport:
    kind: str
    socket_path: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> InferenceConnectorTransport:
        if not isinstance(value, dict):
            raise ValidationError("connector transport must be an object")
        _exact_fields(
            value,
            expected={"kind", "socket_path"},
            subject="connector transport",
        )
        if value["kind"] != "unix":
            raise ValidationError("connector transport kind must be unix")
        return cls(kind="unix", socket_path=_unix_socket_path(value["socket_path"]))

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "socket_path": self.socket_path}


@dataclass(frozen=True)
class InferenceConnectorRoute:
    name: str
    provider: str
    protocol: str
    models: tuple[str, ...]
    credential_scope_sha256: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> InferenceConnectorRoute:
        if not isinstance(value, dict):
            raise ValidationError("connector route must be an object")
        _exact_fields(
            value,
            expected={
                "name",
                "provider",
                "protocol",
                "models",
                "credential_scope_sha256",
            },
            subject="connector route",
        )
        raw_models = value["models"]
        if (
            not isinstance(raw_models, list)
            or not 1 <= len(raw_models) <= MAX_ROUTE_MODELS
        ):
            raise ValidationError(
                f"connector route models must contain 1 to {MAX_ROUTE_MODELS} entries"
            )
        models = tuple(
            _nonblank(model, field="connector route model", max_bytes=256)
            for model in raw_models
        )
        if len(set(models)) != len(models):
            raise ValidationError("connector route models must be unique")
        return cls(
            name=validate_provider_name(value["name"], field="connector route name"),
            provider=validate_provider_name(
                value["provider"], field="connector route provider"
            ),
            protocol=validate_provider_name(
                value["protocol"], field="connector route protocol"
            ),
            models=models,
            credential_scope_sha256=_sha256(
                value["credential_scope_sha256"],
                field="connector route credential scope",
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "credential_scope_sha256": self.credential_scope_sha256,
            "models": list(self.models),
            "name": self.name,
            "protocol": self.protocol,
            "provider": self.provider,
        }


@dataclass(frozen=True)
class InferenceConnectorSpec:
    schema_version: int
    connector_protocol_version: int
    deployment_sha256: str
    transport: InferenceConnectorTransport
    routes: tuple[InferenceConnectorRoute, ...]
    request_bytes: int
    response_bytes: int
    timeout_ms: int
    max_concurrency: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> InferenceConnectorSpec:
        if not isinstance(value, dict):
            raise ValidationError("inference connector spec must be an object")
        _exact_fields(
            value,
            expected={
                "schema_version",
                "connector_protocol_version",
                "deployment_sha256",
                "transport",
                "routes",
                "request_bytes",
                "response_bytes",
                "timeout_ms",
                "max_concurrency",
            },
            subject="inference connector spec",
        )
        raw_routes = value["routes"]
        if (
            not isinstance(raw_routes, list)
            or not 1 <= len(raw_routes) <= MAX_CONNECTOR_ROUTES
        ):
            raise ValidationError(
                f"connector routes must contain 1 to {MAX_CONNECTOR_ROUTES} entries"
            )
        routes = tuple(
            InferenceConnectorRoute.from_mapping(item) for item in raw_routes
        )
        if len({route.name for route in routes}) != len(routes):
            raise ValidationError("connector route names must be unique")
        result = cls(
            schema_version=_schema_version(value["schema_version"]),
            connector_protocol_version=_connector_protocol_version(
                value["connector_protocol_version"]
            ),
            deployment_sha256=_sha256(
                value["deployment_sha256"], field="connector deployment"
            ),
            transport=InferenceConnectorTransport.from_mapping(value["transport"]),
            routes=routes,
            request_bytes=_request_bytes(value["request_bytes"]),
            response_bytes=_response_bytes(value["response_bytes"]),
            timeout_ms=_timeout_ms(value["timeout_ms"]),
            max_concurrency=_max_concurrency(value["max_concurrency"]),
        )
        if len(result.canonical.encode("utf-8")) > 64 * 1024:
            raise ValidationError("inference connector spec exceeds 65536 UTF-8 bytes")
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "connector_protocol_version": self.connector_protocol_version,
            "deployment_sha256": self.deployment_sha256,
            "max_concurrency": self.max_concurrency,
            "request_bytes": self.request_bytes,
            "response_bytes": self.response_bytes,
            "routes": [route.to_dict() for route in self.routes],
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

    def validate_grant(self, connector_name: str, grant: InferenceGrantSpec) -> None:
        connector_name = validate_provider_name(connector_name, field="connector_name")
        if not isinstance(grant, InferenceGrantSpec):
            raise ValidationError("grant must be an InferenceGrantSpec")
        if grant.connector_name != connector_name:
            raise ValidationError("grant connector_name does not match connector")
        if grant.deployment_sha256 != self.deployment_sha256:
            raise ValidationError("grant deployment does not match connector")
        route = next((item for item in self.routes if item.name == grant.route), None)
        if route is None:
            raise ValidationError("grant route is not registered by connector")
        if route.provider != grant.provider:
            raise ValidationError("grant provider does not match connector route")
        if route.protocol != grant.protocol:
            raise ValidationError("grant protocol does not match connector route")
        if route.credential_scope_sha256 != grant.credential_scope_sha256:
            raise ValidationError(
                "grant credential scope does not match connector route"
            )
        if grant.model not in route.models:
            raise ValidationError("grant model is not approved by connector route")
        for field in (
            "request_bytes",
            "response_bytes",
            "timeout_ms",
            "max_concurrency",
        ):
            if getattr(grant, field) > getattr(self, field):
                raise ValidationError(f"grant {field} exceeds connector limit")


@dataclass(frozen=True)
class InferenceGrantSpec:
    schema_version: int
    connector_name: str
    deployment_sha256: str
    route: str
    credential_scope_sha256: str
    provider: str
    protocol: str
    model: str
    request_bytes: int
    response_bytes: int
    timeout_ms: int
    max_concurrency: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> InferenceGrantSpec:
        if not isinstance(value, dict):
            raise ValidationError("inference grant spec must be an object")
        _exact_fields(
            value,
            expected={
                "schema_version",
                "connector_name",
                "deployment_sha256",
                "route",
                "credential_scope_sha256",
                "provider",
                "protocol",
                "model",
                "request_bytes",
                "response_bytes",
                "timeout_ms",
                "max_concurrency",
            },
            subject="inference grant spec",
        )
        return cls(
            schema_version=_schema_version(value["schema_version"]),
            connector_name=validate_provider_name(
                value["connector_name"], field="connector_name"
            ),
            deployment_sha256=_sha256(
                value["deployment_sha256"], field="grant deployment"
            ),
            route=validate_provider_name(value["route"], field="grant route"),
            credential_scope_sha256=_sha256(
                value["credential_scope_sha256"], field="grant credential scope"
            ),
            provider=validate_provider_name(value["provider"], field="grant provider"),
            protocol=validate_provider_name(value["protocol"], field="grant protocol"),
            model=_nonblank(value["model"], field="grant model", max_bytes=256),
            request_bytes=_request_bytes(value["request_bytes"]),
            response_bytes=_response_bytes(value["response_bytes"]),
            timeout_ms=_timeout_ms(value["timeout_ms"]),
            max_concurrency=_max_concurrency(value["max_concurrency"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "connector_name": self.connector_name,
            "credential_scope_sha256": self.credential_scope_sha256,
            "deployment_sha256": self.deployment_sha256,
            "max_concurrency": self.max_concurrency,
            "model": self.model,
            "protocol": self.protocol,
            "provider": self.provider,
            "request_bytes": self.request_bytes,
            "response_bytes": self.response_bytes,
            "route": self.route,
            "schema_version": self.schema_version,
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
    "invocation_id",
    "connector_id",
    "deployment_sha256",
    "route",
    "grant_id",
    "policy_sha256",
    "credential_scope_sha256",
    "provider",
    "protocol",
    "model",
    "deadline",
    "request_limit_bytes",
    "response_limit_bytes",
)


def _invocation_authority(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": _schema_version(value["schema_version"]),
        "connector_protocol_version": _connector_protocol_version(
            value["connector_protocol_version"]
        ),
        "invocation_id": _identifier(
            value["invocation_id"],
            field="invocation_id",
            pattern=PROVIDER_INVOCATION_ID_RE,
        ),
        "connector_id": _identifier(
            value["connector_id"], field="connector_id", pattern=CONNECTOR_ID_RE
        ),
        "deployment_sha256": _sha256(
            value["deployment_sha256"], field="invocation deployment"
        ),
        "route": validate_provider_name(value["route"], field="invocation route"),
        "grant_id": _identifier(
            value["grant_id"], field="grant_id", pattern=PROVIDER_GRANT_ID_RE
        ),
        "policy_sha256": _sha256(value["policy_sha256"], field="invocation policy"),
        "credential_scope_sha256": _sha256(
            value["credential_scope_sha256"], field="invocation credential scope"
        ),
        "provider": validate_provider_name(
            value["provider"], field="invocation provider"
        ),
        "protocol": validate_provider_name(
            value["protocol"], field="invocation protocol"
        ),
        "model": _nonblank(value["model"], field="invocation model", max_bytes=256),
        "deadline": _timestamp(value["deadline"], field="invocation deadline"),
        "request_limit_bytes": _request_bytes(
            value["request_limit_bytes"], field="request_limit_bytes"
        ),
        "response_limit_bytes": _response_bytes(
            value["response_limit_bytes"], field="response_limit_bytes"
        ),
    }


@dataclass(frozen=True)
class InferenceInvocationRequest:
    schema_version: int
    connector_protocol_version: int
    invocation_id: str
    connector_id: str
    deployment_sha256: str
    route: str
    grant_id: str
    policy_sha256: str
    credential_scope_sha256: str
    provider: str
    protocol: str
    model: str
    deadline: str
    request_limit_bytes: int
    response_limit_bytes: int
    _request_json: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> InferenceInvocationRequest:
        if not isinstance(value, dict):
            raise ValidationError("inference invocation request must be an object")
        _exact_fields(
            value,
            expected=set(_CORRELATION_FIELDS) | {"request"},
            subject="inference invocation request",
        )
        authority = _invocation_authority(value)
        request = value["request"]
        if not isinstance(request, dict):
            raise ValidationError("inference invocation request body must be an object")
        request_canonical = canonical_json(request)
        if len(request_canonical.encode("utf-8")) > authority["request_limit_bytes"]:
            raise ValidationError("inference invocation request exceeds request limit")
        return cls(**authority, _request_json=request_canonical)

    @property
    def request(self) -> dict[str, Any]:
        return _detached_json_object(self._request_json)

    def to_dict(self) -> dict[str, Any]:
        result = {field: getattr(self, field) for field in _CORRELATION_FIELDS}
        result["request"] = self.request
        return result

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def correlation(self) -> tuple[Any, ...]:
        return tuple(getattr(self, field) for field in _CORRELATION_FIELDS)


@dataclass(frozen=True)
class InferenceInvocationResponse:
    schema_version: int
    connector_protocol_version: int
    invocation_id: str
    connector_id: str
    deployment_sha256: str
    route: str
    grant_id: str
    policy_sha256: str
    credential_scope_sha256: str
    provider: str
    protocol: str
    model: str
    deadline: str
    request_limit_bytes: int
    response_limit_bytes: int
    outcome: str
    provider_status: int | None
    request_bytes: int
    response_bytes: int
    error_code: str | None
    _response_json: str | None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> InferenceInvocationResponse:
        if not isinstance(value, dict):
            raise ValidationError("inference invocation response must be an object")
        _exact_fields(
            value,
            expected=set(_CORRELATION_FIELDS)
            | {
                "outcome",
                "provider_status",
                "request_bytes",
                "response_bytes",
                "error_code",
                "response",
            },
            subject="inference invocation response",
        )
        authority = _invocation_authority(value)
        outcome = value["outcome"]
        if outcome not in {"succeeded", "failed", "ambiguous"}:
            raise ValidationError("inference invocation outcome is invalid")
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
        response = value["response"]
        if response is not None and not isinstance(response, dict):
            raise ValidationError(
                "inference invocation response body must be an object"
            )
        response_canonical = None
        if response is not None:
            response_canonical = canonical_json(response)
            if (
                len(response_canonical.encode("utf-8"))
                > authority["response_limit_bytes"]
            ):
                raise ValidationError(
                    "inference invocation response exceeds response limit"
                )
        if outcome == "succeeded":
            if (
                provider_status is None
                or not 200 <= provider_status < 300
                or error_code is not None
                or response is None
            ):
                raise ValidationError("successful inference invocation is inconsistent")
        elif error_code is None or response is not None:
            raise ValidationError(
                f"{outcome} inference invocation must have an error and no response"
            )
        return cls(
            **authority,
            outcome=outcome,
            provider_status=provider_status,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
            error_code=error_code,
            _response_json=response_canonical,
        )

    @property
    def untrusted_response(self) -> dict[str, Any] | None:
        if self._response_json is None:
            return None
        return _detached_json_object(self._response_json)

    def to_dict(self) -> dict[str, Any]:
        result = {field: getattr(self, field) for field in _CORRELATION_FIELDS}
        result.update(
            {
                "error_code": self.error_code,
                "outcome": self.outcome,
                "provider_status": self.provider_status,
                "request_bytes": self.request_bytes,
                "response": self.untrusted_response,
                "response_bytes": self.response_bytes,
            }
        )
        return result

    @property
    def correlation(self) -> tuple[Any, ...]:
        return tuple(getattr(self, field) for field in _CORRELATION_FIELDS)

    def validate_correlation(self, request: InferenceInvocationRequest) -> None:
        if not isinstance(request, InferenceInvocationRequest):
            raise ValidationError("request must be an InferenceInvocationRequest")
        if self.correlation != request.correlation:
            raise ValidationError(
                "inference connector response correlation does not match"
            )


def _strict_json_object(payload: bytes) -> dict[str, Any]:
    try:
        text = payload.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise ValidationError("connector response must be valid UTF-8 JSON") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError("connector response contains duplicate fields")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ValidationError("connector response must contain finite JSON")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ValidationError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ValidationError("connector response must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValidationError("connector response must be an object")
    return value


def _remaining_timeout(deadline: datetime) -> float:
    remaining = (deadline - datetime.now(UTC)).total_seconds()
    if remaining <= 0:
        raise TimeoutError("inference connector deadline has expired")
    return remaining


def _recv_exact(connection: socket.socket, length: int, *, deadline: datetime) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        connection.settimeout(_remaining_timeout(deadline))
        chunk = connection.recv(remaining)
        if not chunk:
            raise OSError("connector response ended before its frame was complete")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class UnixInferenceConnectorClient:
    """Send one bounded invocation over one operator-selected Unix socket."""

    def __init__(self, socket_path: Path):
        try:
            self.socket_path = _unix_socket_path(str(socket_path))
        except ValidationError as exc:
            raise InferenceConnectorError(str(exc)) from exc

    def invoke(
        self, request: InferenceInvocationRequest
    ) -> InferenceInvocationResponse:
        if not isinstance(request, InferenceInvocationRequest):
            raise InferenceConnectorError(
                "request must be an InferenceInvocationRequest"
            )
        deadline = datetime.fromisoformat(request.deadline)
        try:
            remaining = _remaining_timeout(deadline)
        except TimeoutError as exc:
            raise InferenceConnectorUnavailableError(
                "inference connector deadline has expired"
            ) from exc
        payload = request.canonical.encode("utf-8")
        if len(payload) > request.request_limit_bytes + MAX_CONNECTOR_FRAME_OVERHEAD:
            raise InferenceConnectorError(
                "inference connector request frame is oversized"
            )
        frame = struct.pack("!I", len(payload)) + payload
        try:
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        except (OSError, TimeoutError) as exc:
            raise InferenceConnectorUnavailableError(
                "inference connector is unavailable"
            ) from exc
        try:
            try:
                connection.settimeout(remaining)
                connection.connect(self.socket_path)
            except (OSError, TimeoutError) as exc:
                raise InferenceConnectorUnavailableError(
                    "inference connector is unavailable"
                ) from exc
            try:
                connection.settimeout(_remaining_timeout(deadline))
            except (OSError, TimeoutError) as exc:
                raise InferenceConnectorUnavailableError(
                    "inference connector is unavailable before dispatch"
                ) from exc
            try:
                connection.sendall(frame)
                raw_length = _recv_exact(connection, 4, deadline=deadline)
                length = struct.unpack("!I", raw_length)[0]
                maximum = request.response_limit_bytes + MAX_CONNECTOR_FRAME_OVERHEAD
                if not 1 <= length <= maximum:
                    raise ValidationError("connector response frame is oversized")
                raw_response = _recv_exact(connection, length, deadline=deadline)
                response = InferenceInvocationResponse.from_mapping(
                    _strict_json_object(raw_response)
                )
                response.validate_correlation(request)
                return response
            except (OSError, TimeoutError, ValidationError) as exc:
                raise InferenceConnectorAmbiguousError(
                    "inference connector outcome is ambiguous"
                ) from exc
        finally:
            connection.close()
