from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

from .domain import canonical_json
from .gateway import (
    GATEWAY_FILE_NAMES,
    GATEWAY_PROTOCOL_VERSION,
    GATEWAY_REQUEST_NAME,
    GATEWAY_REQUEST_READY_NAME,
    GATEWAY_RESPONSE_NAME,
    GATEWAY_RESPONSE_READY_NAME,
    MAX_GATEWAY_MARKER_BYTES,
    MAX_GATEWAY_REQUEST_BYTES,
    MAX_GATEWAY_RESPONSE_BYTES,
    MAX_GATEWAY_SEQUENCE,
)

TRANSPORT_HOST = "127.0.0.1"
TRANSPORT_PORT = 8766
MAX_VENDOR_REQUEST_BYTES = 2 * 1024 * 1024
PROVIDER_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
OPERATION_RE = re.compile(r"^[a-z][a-z0-9_.]{0,63}$")
_DEFAULT_GATEWAY_TIMEOUT = object()


class WorkerTransportError(RuntimeError):
    """The worker transport could not safely use its run-scoped gateway."""


class GatewayClient(Protocol):
    def exchange(
        self,
        operation: str,
        payload: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]: ...


def _strict_json(raw: bytes, *, subject: str, limit: int) -> Any:
    if not raw or len(raw) > limit:
        raise WorkerTransportError(f"{subject} exceeds its size limit")
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise WorkerTransportError(f"{subject} is invalid JSON") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise WorkerTransportError(f"{subject} is invalid JSON")
            value[key] = item
        return value

    def reject_constant(_value: str) -> None:
        raise WorkerTransportError(f"{subject} is invalid JSON")

    try:
        return json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except WorkerTransportError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise WorkerTransportError(f"{subject} is invalid JSON") from exc


def _safe_file(path: Path, flags: int) -> int:
    try:
        descriptor = os.open(path, flags | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise WorkerTransportError("gateway mailbox is unavailable or unsafe") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise WorkerTransportError("gateway mailbox contains an unsafe file")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _read_file(path: Path, limit: int) -> bytes:
    descriptor = _safe_file(path, os.O_RDONLY)
    try:
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            value = stream.read(limit + 1)
    finally:
        os.close(descriptor)
    if len(value) > limit:
        raise WorkerTransportError("gateway mailbox response exceeds its size limit")
    return value


def _write_descriptor(descriptor: int, value: bytes) -> None:
    os.ftruncate(descriptor, 0)
    os.lseek(descriptor, 0, os.SEEK_SET)
    offset = 0
    while offset < len(value):
        written = os.write(descriptor, value[offset:])
        if written <= 0:
            raise WorkerTransportError("gateway mailbox write failed")
        offset += written
    os.fsync(descriptor)


def _write_file(path: Path, value: bytes) -> None:
    descriptor = _safe_file(path, os.O_WRONLY)
    try:
        _write_descriptor(descriptor, value)
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class _Marker:
    sequence: int
    size: int
    sha256: str

    @classmethod
    def decode(cls, raw: bytes, *, allow_empty: bool = False) -> _Marker | None:
        if allow_empty and not raw:
            return None
        value = _strict_json(
            raw,
            subject="gateway mailbox marker",
            limit=MAX_GATEWAY_MARKER_BYTES,
        )
        if not isinstance(value, dict) or set(value) != {
            "sequence",
            "sha256",
            "size",
        }:
            raise WorkerTransportError("gateway mailbox marker is invalid")
        sequence = value["sequence"]
        size = value["size"]
        digest = value["sha256"]
        if (
            isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or not 1 <= sequence <= MAX_GATEWAY_SEQUENCE
            or isinstance(size, bool)
            or not isinstance(size, int)
            or not 1 <= size <= MAX_GATEWAY_RESPONSE_BYTES
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        ):
            raise WorkerTransportError("gateway mailbox marker is invalid")
        return cls(sequence, size, digest)

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


class FileGatewayClient:
    """Serialize committed-file exchanges across worker processes."""

    def __init__(self, directory: Path, *, timeout: float = 130.0):
        self.directory = Path(directory)
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0.1 <= timeout <= 300
        ):
            raise WorkerTransportError("gateway timeout is invalid")
        self.timeout = float(timeout)
        try:
            info = self.directory.lstat()
            names = {path.name for path in self.directory.iterdir()}
        except OSError as exc:
            raise WorkerTransportError("gateway mailbox is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise WorkerTransportError("gateway mailbox is unsafe")
        if names != GATEWAY_FILE_NAMES:
            raise WorkerTransportError("gateway mailbox has an invalid shape")
        for name in GATEWAY_FILE_NAMES:
            descriptor = _safe_file(self.directory / name, os.O_RDONLY)
            os.close(descriptor)

    def _next_sequence(self) -> int:
        markers = []
        for name in (GATEWAY_REQUEST_READY_NAME, GATEWAY_RESPONSE_READY_NAME):
            marker = _Marker.decode(
                _read_file(self.directory / name, MAX_GATEWAY_MARKER_BYTES),
                allow_empty=True,
            )
            if marker is not None:
                markers.append(marker.sequence)
        sequence = max(markers, default=0) + 1
        if sequence > MAX_GATEWAY_SEQUENCE:
            raise WorkerTransportError("gateway mailbox sequence is exhausted")
        return sequence

    def exchange(
        self,
        operation: str,
        payload: dict[str, Any],
        *,
        timeout: float | None | object = _DEFAULT_GATEWAY_TIMEOUT,
    ) -> dict[str, Any]:
        if not isinstance(operation, str) or OPERATION_RE.fullmatch(operation) is None:
            raise WorkerTransportError("gateway operation is invalid")
        if not isinstance(payload, dict):
            raise WorkerTransportError("gateway payload must be an object")
        effective_timeout = (
            self.timeout if timeout is _DEFAULT_GATEWAY_TIMEOUT else timeout
        )
        if effective_timeout is not None and (
            isinstance(effective_timeout, bool)
            or not isinstance(effective_timeout, (int, float))
            or not 0.1 <= effective_timeout <= 300
        ):
            raise WorkerTransportError("gateway timeout is invalid")
        request_path = self.directory / GATEWAY_REQUEST_NAME
        descriptor = _safe_file(request_path, os.O_RDWR)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            sequence = self._next_sequence()
            request_id = "worker-" + uuid.uuid4().hex
            raw = (
                canonical_json(
                    {
                        "version": GATEWAY_PROTOCOL_VERSION,
                        "request_id": request_id,
                        "operation": operation,
                        "payload": payload,
                    }
                )
                + "\n"
            ).encode("utf-8")
            if len(raw) > MAX_GATEWAY_REQUEST_BYTES:
                raise WorkerTransportError("gateway request exceeds its size limit")
            _write_descriptor(descriptor, raw)
            marker = _Marker(
                sequence,
                len(raw),
                hashlib.sha256(raw).hexdigest(),
            )
            _write_file(
                self.directory / GATEWAY_REQUEST_READY_NAME,
                marker.encode(),
            )
            response = self._wait_response(
                sequence,
                request_id,
                None if effective_timeout is None else float(effective_timeout),
            )
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        return response

    def _wait_response(
        self, sequence: int, request_id: str, timeout: float | None
    ) -> dict[str, Any]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while deadline is None or time.monotonic() < deadline:
            ready_raw = _read_file(
                self.directory / GATEWAY_RESPONSE_READY_NAME,
                MAX_GATEWAY_MARKER_BYTES,
            )
            try:
                marker = _Marker.decode(ready_raw, allow_empty=True)
            except WorkerTransportError:
                marker = None
            if marker is None or marker.sequence != sequence:
                time.sleep(0.01)
                continue
            raw = _read_file(self.directory / GATEWAY_RESPONSE_NAME, marker.size)
            if (
                len(raw) != marker.size
                or hashlib.sha256(raw).hexdigest() != marker.sha256
            ):
                time.sleep(0.01)
                continue
            value = _strict_json(
                raw,
                subject="gateway response",
                limit=MAX_GATEWAY_RESPONSE_BYTES,
            )
            if (
                not isinstance(value, dict)
                or value.get("version") != GATEWAY_PROTOCOL_VERSION
                or value.get("request_id") != request_id
                or not isinstance(value.get("ok"), bool)
            ):
                raise WorkerTransportError("gateway returned an invalid response")
            return value
        raise WorkerTransportError("gateway response timed out")


@dataclass(frozen=True)
class ProviderHTTPResponse:
    status_code: int
    content_type: str
    body: bytes


class ProviderHTTPTransport:
    ROUTES = {
        "openai-responses": "/v1/responses",
        "anthropic-messages": "/v1/messages",
    }
    HEADERS = {
        "openai-responses": ("openai-beta", "x-codex-beta-features"),
        "anthropic-messages": ("anthropic-beta", "anthropic-version"),
    }

    def __init__(self, gateway: GatewayClient, *, dialect: str, grant: str):
        if dialect not in self.ROUTES:
            raise WorkerTransportError("provider dialect is unavailable")
        if not isinstance(grant, str) or PROVIDER_NAME_RE.fullmatch(grant) is None:
            raise WorkerTransportError("provider grant is invalid")
        if grant == "krab":
            raise WorkerTransportError("provider grant is invalid")
        self.gateway = gateway
        self.dialect = dialect
        self.grant = grant

    def invoke(
        self,
        target: str,
        raw: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> ProviderHTTPResponse:
        parsed = urlsplit(target)
        if (
            parsed.scheme
            or parsed.netloc
            or parsed.path != self.ROUTES[self.dialect]
            or parsed.fragment
            or (self.dialect == "openai-responses" and parsed.query)
            or (
                self.dialect == "anthropic-messages"
                and parsed.query not in {"", "beta=true"}
            )
        ):
            raise WorkerTransportError("provider route is not allowed")
        value = _strict_json(
            raw,
            subject="provider request JSON",
            limit=MAX_VENDOR_REQUEST_BYTES,
        )
        if not isinstance(value, dict):
            raise WorkerTransportError("provider request must be an object")
        model = value.pop("model", None)
        if not isinstance(model, str) or not model or len(model.encode()) > 256:
            raise WorkerTransportError("provider request model is invalid")
        if value.get("stream") is not True:
            raise WorkerTransportError("provider request must use streaming")
        forwarded_headers: dict[str, str] = {}
        if headers is not None:
            for name in self.HEADERS[self.dialect]:
                header = headers.get(name)
                if header is None:
                    continue
                if (
                    not isinstance(header, str)
                    or not header
                    or len(header) > 8192
                    or any(not 0x20 <= ord(character) <= 0x7E for character in header)
                ):
                    raise WorkerTransportError("provider protocol header is invalid")
                forwarded_headers[name] = header
        response = self.gateway.exchange(
            "provider.invoke",
            {
                "grant": self.grant,
                "request": {
                    "protocol": self.dialect,
                    "body": value,
                    "headers": forwarded_headers,
                },
            },
            timeout=None,
        )
        if response.get("ok") is not True:
            raise WorkerTransportError("provider gateway denied the request")
        result = response.get("result")
        if not isinstance(result, dict):
            raise WorkerTransportError("provider gateway returned an invalid response")
        accepted = {
            "grant_name",
            "invocation_id",
            "model",
            "provider",
            "request_bytes",
            "response_bytes",
            "status_code",
            "untrusted_response",
        }
        status_code = result.get("status_code")
        upstream = result.get("untrusted_response")
        if (
            set(result) != accepted
            or result.get("grant_name") != self.grant
            or isinstance(status_code, bool)
            or not isinstance(status_code, int)
            or not 200 <= status_code < 300
            or not isinstance(upstream, dict)
            or set(upstream) != {"body", "content_type", "protocol"}
            or upstream.get("protocol") != self.dialect
        ):
            raise WorkerTransportError("provider gateway returned an invalid response")
        content_type = upstream.get("content_type")
        body = upstream.get("body")
        if (
            not isinstance(content_type, str)
            or content_type.split(";", 1)[0].strip().lower() != "text/event-stream"
            or not isinstance(body, str)
            or not body
        ):
            raise WorkerTransportError("provider gateway returned an invalid response")
        return ProviderHTTPResponse(status_code, content_type, body.encode("utf-8"))


class _TransportServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        transport: ProviderHTTPTransport,
    ):
        self.transport = transport
        super().__init__(address, _TransportHandler)


class _TransportHandler(BaseHTTPRequestHandler):
    server: _TransportServer

    def do_POST(self) -> None:  # noqa: N802
        declared = self.headers.get("Content-Length")
        content_type = self.headers.get("Content-Type", "")
        media_type = content_type.split(";", 1)[0].strip().lower()
        if (
            self.headers.get("Transfer-Encoding") is not None
            or not declared
            or not declared.isascii()
            or not declared.isdecimal()
            or media_type != "application/json"
        ):
            self._failure(400)
            return
        size = int(declared)
        if not 1 <= size <= MAX_VENDOR_REQUEST_BYTES:
            self._failure(413)
            return
        raw = self.rfile.read(size)
        if len(raw) != size:
            self._failure(400)
            return
        try:
            response = self.server.transport.invoke(
                self.path,
                raw,
                headers=self.headers,
            )
        except WorkerTransportError:
            self._failure(502)
            return
        self.send_response(response.status_code)
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(response.body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(response.body)

    def _failure(self, status_code: int) -> None:
        message = "Magic Glass Box provider transport rejected the request"
        if self.server.transport.dialect == "anthropic-messages":
            value = {
                "type": "error",
                "error": {"type": "api_error", "message": message},
            }
        else:
            value = {"error": {"type": "invalid_request_error", "message": message}}
        raw = canonical_json(value).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, _format: str, *args: object) -> None:
        return


def _gateway_from_environment() -> FileGatewayClient:
    value = os.environ.get("MAGIC_GLASS_BOX_GATEWAY_DIR")
    if not value:
        raise WorkerTransportError("gateway mailbox environment is unavailable")
    return FileGatewayClient(Path(value))


def _serve(dialect: str, grant: str, command: list[str]) -> int:
    if not command:
        raise WorkerTransportError("worker command is missing")
    transport = ProviderHTTPTransport(
        _gateway_from_environment(),
        dialect=dialect,
        grant=grant,
    )
    try:
        server = _TransportServer((TRANSPORT_HOST, TRANSPORT_PORT), transport)
    except OSError as exc:
        raise WorkerTransportError(
            "provider loopback transport is unavailable"
        ) from exc
    thread = threading.Thread(
        target=server.serve_forever,
        name="magic-glass-box-provider-transport",
        daemon=True,
    )
    thread.start()
    try:
        completed = subprocess.run(command, check=False)  # noqa: S603
        return completed.returncode
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def _call(operation: str, payload_text: str) -> int:
    raw = (
        sys.stdin.buffer.read(MAX_GATEWAY_REQUEST_BYTES + 1)
        if payload_text == "-"
        else payload_text.encode("utf-8")
    )
    payload = _strict_json(
        raw,
        subject="gateway payload JSON",
        limit=MAX_GATEWAY_REQUEST_BYTES,
    )
    if not isinstance(payload, dict):
        raise WorkerTransportError("gateway payload must be an object")
    response = _gateway_from_environment().exchange(operation, payload)
    sys.stdout.write(canonical_json(response) + "\n")
    return 0 if response.get("ok") is True else 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="magic-glass-box-worker",
        description="Use a run-scoped Magic Glass Box worker gateway.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve")
    serve.add_argument(
        "--dialect", choices=tuple(ProviderHTTPTransport.ROUTES), required=True
    )
    serve.add_argument("--grant", required=True)
    serve.add_argument("worker_command", nargs=argparse.REMAINDER)
    call = commands.add_parser("call")
    call.add_argument("operation")
    call.add_argument("payload", nargs="?", default="{}")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "serve":
            command = list(args.worker_command)
            if command[:1] == ["--"]:
                command.pop(0)
            return _serve(args.dialect, args.grant, command)
        return _call(args.operation, args.payload)
    except WorkerTransportError as exc:
        print(f"magic-glass-box-worker: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
