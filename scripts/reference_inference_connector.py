#!/usr/bin/env python3
"""Serve pinned provider adapters outside the Magic Glass Box process."""

from __future__ import annotations

import argparse
import json
import socket
import struct
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from magic_glass_box.credentials import PrivateFileCredentialResolver
from magic_glass_box.domain import ProviderGrantSpec, ValidationError, canonical_json
from magic_glass_box.inference import (
    CONNECTOR_ID_RE,
    SHA256_RE,
    InferenceInvocationRequest,
)
from magic_glass_box.provider import (
    ProviderAmbiguousError,
    ProviderBroker,
    ProviderError,
    ProviderUpstreamError,
)
from magic_glass_box.sidecar import UnixSidecarError, serve_unix_socket

MAX_CONFIG_BYTES = 1024 * 1024
MAX_REQUEST_FRAME_BYTES = 2 * 1024 * 1024 + 64 * 1024


@dataclass(frozen=True)
class _Route:
    connector_id: str
    deployment_sha256: str
    name: str
    credential_scope_sha256: str
    policy: ProviderGrantSpec
    credential: str


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _strict_object(payload: bytes, *, subject: str) -> dict[str, Any]:
    try:
        text = payload.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise ValidationError(f"{subject} must be UTF-8 JSON") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError(f"{subject} contains duplicate fields")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ValidationError(f"{subject} must contain finite JSON")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ValidationError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ValidationError(f"{subject} must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValidationError(f"{subject} must be an object")
    return value


def _load_routes(path: Path) -> dict[str, _Route]:
    if not path.is_absolute():
        raise SystemExit("--config must be absolute")
    try:
        with path.open(mode="rb") as stream:
            raw = stream.read(MAX_CONFIG_BYTES + 1)
    except OSError as exc:
        raise SystemExit("connector configuration is unavailable") from exc
    if not raw or len(raw) > MAX_CONFIG_BYTES:
        raise SystemExit("connector configuration is empty or oversized")
    try:
        value = _strict_object(raw, subject="connector configuration")
        if set(value) != {
            "connector_id",
            "deployment_sha256",
            "routes",
        } or not isinstance(value["routes"], list):
            raise ValidationError("connector configuration has an invalid shape")
        connector_id = value["connector_id"]
        deployment_sha256 = value["deployment_sha256"]
        if (
            not isinstance(connector_id, str)
            or CONNECTOR_ID_RE.fullmatch(connector_id) is None
            or not isinstance(deployment_sha256, str)
            or SHA256_RE.fullmatch(deployment_sha256) is None
        ):
            raise ValidationError("connector configuration authority is invalid")
        resolver = PrivateFileCredentialResolver()
        routes: dict[str, _Route] = {}
        for raw_route in value["routes"]:
            if not isinstance(raw_route, dict) or set(raw_route) != {
                "credential_file",
                "credential_scope_sha256",
                "name",
                "policy",
            }:
                raise ValidationError("connector route has an invalid shape")
            name = raw_route["name"]
            scope = raw_route["credential_scope_sha256"]
            credential_file = raw_route["credential_file"]
            if (
                not isinstance(name, str)
                or not name
                or name in routes
                or not isinstance(scope, str)
                or len(scope) != 64
                or any(character not in "0123456789abcdef" for character in scope)
                or not isinstance(credential_file, str)
            ):
                raise ValidationError("connector route authority is invalid")
            credential_path = Path(credential_file)
            reference = resolver.secret_reference(credential_path)
            routes[name] = _Route(
                connector_id=connector_id,
                deployment_sha256=deployment_sha256,
                name=name,
                credential_scope_sha256=scope,
                policy=ProviderGrantSpec.from_mapping(raw_route["policy"]),
                credential=resolver.resolve_secret(reference),
            )
    except (TypeError, ValidationError) as exc:
        raise SystemExit(str(exc)) from exc
    if not routes:
        raise SystemExit("connector configuration requires at least one route")
    return routes


def _recv_exact(connection: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise OSError("request frame ended early")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_request(connection: socket.socket) -> InferenceInvocationRequest:
    length = struct.unpack("!I", _recv_exact(connection, 4))[0]
    if not 1 <= length <= MAX_REQUEST_FRAME_BYTES:
        raise ValidationError("request frame is oversized")
    return InferenceInvocationRequest.from_mapping(
        _strict_object(
            _recv_exact(connection, length),
            subject="request frame",
        )
    )


def _response_authority(request: InferenceInvocationRequest) -> dict[str, Any]:
    value = request.to_dict()
    value.pop("request")
    return value


def _invoke(
    request: InferenceInvocationRequest,
    *,
    routes: dict[str, _Route],
    broker: ProviderBroker,
) -> dict[str, Any]:
    route = routes.get(request.route)
    authority = _response_authority(request)
    if (
        route is None
        or route.connector_id != request.connector_id
        or route.deployment_sha256 != request.deployment_sha256
        or route.name != request.route
        or route.credential_scope_sha256 != request.credential_scope_sha256
        or route.policy.provider != request.provider
        or route.policy.model != request.model
        or route.policy.request_bytes < request.request_limit_bytes
        or route.policy.response_bytes < request.response_limit_bytes
    ):
        return {
            **authority,
            "outcome": "failed",
            "provider_status": None,
            "request_bytes": 0,
            "response_bytes": 0,
            "error_code": "authority_mismatch",
            "response": None,
        }
    deadline = datetime.fromisoformat(request.deadline)
    remaining_ms = int((deadline - datetime.now(UTC)).total_seconds() * 1000)
    if remaining_ms < 100:
        return {
            **authority,
            "outcome": "failed",
            "provider_status": None,
            "request_bytes": 0,
            "response_bytes": 0,
            "error_code": "deadline_expired",
            "response": None,
        }
    expected_adapter = {
        "openai-responses": "openai_responses",
        "anthropic-messages": "anthropic_messages",
    }.get(request.protocol, request.protocol)
    if route.policy.adapter != expected_adapter:
        return {
            **authority,
            "outcome": "failed",
            "provider_status": None,
            "request_bytes": 0,
            "response_bytes": 0,
            "error_code": "protocol_mismatch",
            "response": None,
        }
    effective_policy = replace(
        route.policy,
        request_bytes=request.request_limit_bytes,
        response_bytes=request.response_limit_bytes,
        timeout_ms=min(route.policy.timeout_ms, remaining_ms),
    )
    try:
        result = broker.invoke(
            grant_id=request.grant_id,
            spec=effective_policy,
            credential=route.credential,
            request=request.request,
        )
    except ProviderAmbiguousError as exc:
        return {
            **authority,
            "outcome": "ambiguous",
            "provider_status": exc.status_code,
            "request_bytes": min(exc.request_bytes, request.request_limit_bytes),
            "response_bytes": min(exc.response_bytes, request.response_limit_bytes),
            "error_code": "upstream_ambiguous",
            "response": None,
        }
    except ProviderUpstreamError as exc:
        return {
            **authority,
            "outcome": "failed",
            "provider_status": exc.status_code,
            "request_bytes": min(exc.request_bytes, request.request_limit_bytes),
            "response_bytes": min(exc.response_bytes, request.response_limit_bytes),
            "error_code": "upstream_error",
            "response": None,
        }
    except ProviderError:
        return {
            **authority,
            "outcome": "failed",
            "provider_status": None,
            "request_bytes": 0,
            "response_bytes": 0,
            "error_code": "request_denied",
            "response": None,
        }
    return {
        **authority,
        "outcome": "succeeded",
        "provider_status": result.status_code,
        "request_bytes": result.request_bytes,
        "response_bytes": result.response_bytes,
        "error_code": None,
        "response": result.untrusted_response,
    }


def _write_response(connection: socket.socket, response: dict[str, Any]) -> None:
    payload = canonical_json(response).encode("utf-8")
    connection.sendall(struct.pack("!I", len(payload)) + payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket-path", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--max-invocations", type=_positive_integer, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    routes = _load_routes(args.config)
    broker = ProviderBroker()

    def handle(connection: socket.socket) -> None:
        request = _read_request(connection)
        _write_response(
            connection,
            _invoke(request, routes=routes, broker=broker),
        )

    try:
        completed = serve_unix_socket(
            args.socket_path,
            args.max_invocations,
            handle,
            recoverable_errors=(OSError, ValidationError),
        )
    except UnixSidecarError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps({"invocations": completed, "status": "stopped"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
