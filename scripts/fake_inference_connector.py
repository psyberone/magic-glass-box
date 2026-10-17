#!/usr/bin/env python3
"""Serve deterministic credential-free inference responses over the v1/v2 carrier."""

from __future__ import annotations

import argparse
import json
import socket
import stat
import struct
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from magic_glass_box.domain import ValidationError, canonical_json
from magic_glass_box.inference import InferenceInvocationRequest
from magic_glass_box.sidecar import UnixSidecarError, serve_unix_socket

MAX_REQUEST_FRAME_BYTES = 2 * 1024 * 1024 + 64 * 1024
MAX_FAKE_CREDENTIAL_BYTES = 16 * 1024


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


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


def _strict_object(payload: bytes) -> dict[str, Any]:
    try:
        text = payload.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise ValidationError("request frame must be UTF-8 JSON") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError("request frame contains duplicate fields")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ValidationError("request frame must contain finite JSON")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ValidationError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ValidationError("request frame must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValidationError("request frame must be an object")
    return value


def _read_request(connection: socket.socket) -> InferenceInvocationRequest:
    raw_length = _recv_exact(connection, 4)
    length = struct.unpack("!I", raw_length)[0]
    if not 1 <= length <= MAX_REQUEST_FRAME_BYTES:
        raise ValidationError("request frame is oversized")
    return InferenceInvocationRequest.from_mapping(
        _strict_object(_recv_exact(connection, length))
    )


def _response_for(
    request: InferenceInvocationRequest, *, credential_loaded: bool = True
) -> dict[str, Any]:
    if not credential_loaded:
        raise ValidationError("fake connector credential is unavailable")
    request_value = request.to_dict()
    request_value.pop("request")
    response = {"output_text": "deterministic fake response"}
    request_value.update(
        {
            "outcome": "succeeded",
            "provider_status": 200,
            "request_bytes": len(canonical_json(request.request).encode("utf-8")),
            "response_bytes": len(canonical_json(response).encode("utf-8")),
            "error_code": None,
            "response": response,
        }
    )
    if request.connector_protocol_version == 2:
        assert request.budget is not None
        request_value["usage"] = {
            "input_tokens": 8,
            "cached_input_tokens": 0,
            "output_tokens": min(4, request.budget.max_output_tokens),
            "reasoning_output_tokens": 0,
        }
    return request_value


def _write_response(connection: socket.socket, response: dict[str, Any]) -> None:
    payload = canonical_json(response).encode("utf-8")
    connection.sendall(struct.pack("!I", len(payload)) + payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="serve deterministic credential-free inference responses"
    )
    parser.add_argument("--socket-path", required=True, type=Path)
    parser.add_argument("--max-invocations", type=_positive_integer, default=1)
    parser.add_argument(
        "--credential-file",
        type=Path,
        help="optional connector-private canary credential used by containment proofs",
    )
    return parser


def _load_private_credential(path: Path | None) -> bool:
    if path is None:
        return True
    if not path.is_absolute():
        raise SystemExit("--credential-file must be absolute")
    try:
        info = path.stat()
        value = path.read_bytes()
    except OSError as exc:
        raise SystemExit("connector credential is unavailable") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise SystemExit("connector credential must be a private regular file")
    if not value or len(value) > MAX_FAKE_CREDENTIAL_BYTES:
        raise SystemExit("connector credential is empty or oversized")
    return True


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    credential_loaded = _load_private_credential(args.credential_file)

    def handle(connection: socket.socket) -> None:
        request = _read_request(connection)
        _write_response(
            connection,
            _response_for(request, credential_loaded=credential_loaded),
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
