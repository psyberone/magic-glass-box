#!/usr/bin/env python3
"""Serve deterministic credential-free inference responses over the v1 carrier."""

from __future__ import annotations

import argparse
import json
import os
import socket
import struct
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from magic_glass_box.domain import ValidationError, canonical_json
from magic_glass_box.inference import InferenceInvocationRequest

MAX_REQUEST_FRAME_BYTES = 2 * 1024 * 1024 + 64 * 1024


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


def _response_for(request: InferenceInvocationRequest) -> dict[str, Any]:
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    socket_path = args.socket_path
    if not socket_path.is_absolute():
        raise SystemExit("--socket-path must be absolute")
    socket_path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    if socket_path.exists() or socket_path.is_symlink():
        raise SystemExit("--socket-path already exists")
    completed = 0
    bound = False
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(socket_path))
            bound = True
            os.chmod(socket_path, 0o600)
            listener.listen(1)
            for _ in range(args.max_invocations):
                connection, _ = listener.accept()
                with connection:
                    request = _read_request(connection)
                    _write_response(connection, _response_for(request))
                    completed += 1
    finally:
        if bound:
            socket_path.unlink(missing_ok=True)
    print(json.dumps({"invocations": completed, "status": "stopped"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
