from __future__ import annotations

import argparse
import json
import os
import socket
import stat
import struct
import sys
import time
import webbrowser
from collections.abc import Sequence
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, urlsplit

from .domain import ValidationError, canonical_json
from .inference import InferenceInvocationRequest, InferenceInvocationResponse
from .openai_chatgpt import (
    ChatGPTAuthenticationError,
    ChatGPTConnectorError,
    ChatGPTOAuth,
    ChatGPTProfileStore,
    OpenAIChatGPTConnector,
    OpenAIConnectorConfig,
    OpenAIResponsesClient,
    _strict_json_object,
)
from .sidecar import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    UnixSidecarError,
    serve_unix_socket,
)

MAX_CONFIG_BYTES = 1024 * 1024
MAX_REQUEST_FRAME_BYTES = 2 * 1024 * 1024 + 64 * 1024
MAX_CALLBACK_SECONDS = 15 * 60


class Connector(Protocol):
    def invoke(
        self, request: InferenceInvocationRequest
    ) -> InferenceInvocationResponse: ...


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _callback_seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if not 1 <= parsed <= MAX_CALLBACK_SECONDS:
        raise argparse.ArgumentTypeError(
            f"must be between 1 and {MAX_CALLBACK_SECONDS} seconds"
        )
    return parsed


def _profile_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile-dir", required=True, type=Path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="magic-glass-box-openai-connector",
        description=(
            "Run a credential-isolated OpenAI ChatGPT-plan inference connector."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    login = commands.add_parser("login", help="Continue with ChatGPT")
    _profile_argument(login)
    login.add_argument("--no-browser", action="store_true")
    login.add_argument(
        "--callback-timeout",
        type=_callback_seconds,
        default=300.0,
    )

    status = commands.add_parser("status", help="Show non-secret account metadata")
    _profile_argument(status)

    models = commands.add_parser("models", help="List account-visible models")
    _profile_argument(models)

    serve = commands.add_parser("serve", help="Serve the bounded MGB carrier")
    _profile_argument(serve)
    serve.add_argument("--config", required=True, type=Path)
    serve.add_argument("--socket-path", required=True, type=Path)
    serve.add_argument("--max-invocations", required=True, type=_positive_integer)
    return parser


def _normalize_callback_target(target: str) -> dict[str, str]:
    parsed = urlsplit(target)
    if parsed.path != "/auth/callback" or parsed.fragment:
        raise ChatGPTAuthenticationError("OAuth callback path is invalid")
    try:
        query = parse_qs(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
        )
    except ValueError as exc:
        raise ChatGPTAuthenticationError("OAuth callback is invalid") from exc
    allowed = {
        "client_id",
        "code",
        "error",
        "error_description",
        "error_uri",
        "scope",
        "state",
    }
    if set(query) - allowed or any(len(values) != 1 for values in query.values()):
        raise ChatGPTAuthenticationError("OAuth callback is invalid")
    state = query.get("state", [""])[0]
    code = query.get("code", [""])[0]
    error = query.get("error", [""])[0]
    if not state or bool(code) == bool(error):
        raise ChatGPTAuthenticationError("OAuth callback is invalid")
    callback = {"state": state}
    if code:
        callback["code"] = code
        client_id = query.get("client_id", [None])[0]
        if client_id is not None:
            if not client_id:
                raise ChatGPTAuthenticationError("OAuth callback is invalid")
            callback["client_id"] = client_id
    else:
        callback["error"] = error
    return dict(sorted(callback.items()))


class _CallbackServer(HTTPServer):
    result: dict[str, str] | None

    def __init__(self) -> None:
        self.result = None
        super().__init__(("127.0.0.1", 0), _CallbackHandler)


class _CallbackHandler(BaseHTTPRequestHandler):
    server: _CallbackServer

    def do_GET(self) -> None:  # noqa: N802
        try:
            callback = _normalize_callback_target(self.path)
        except ChatGPTAuthenticationError:
            self._reply(400, "The authorization callback was invalid.")
            return
        self.server.result = callback
        self._reply(200, "Authorization returned. You may close this window.")

    def _reply(self, status: int, message: str) -> None:
        raw = (
            "<!doctype html><meta charset=utf-8><title>Magic Glass Box</title>"
            f"<p>{message}</p>"
        ).encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, _format: str, *args: object) -> None:
        return


def _login(
    store: ChatGPTProfileStore,
    *,
    no_browser: bool,
    callback_timeout: float,
) -> dict[str, Any]:
    oauth = ChatGPTOAuth(store)
    with _CallbackServer() as callback_server:
        host, port = callback_server.server_address
        redirect_uri = f"http://{host}:{port}/auth/callback"
        attempt = oauth.begin(redirect_uri)
        opened = False if no_browser else webbrowser.open(attempt.authorization_url)
        if not opened:
            print(
                json.dumps(
                    {"authorization_url": attempt.authorization_url},
                    sort_keys=True,
                )
            )
        deadline = time.monotonic() + callback_timeout
        while callback_server.result is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ChatGPTAuthenticationError(
                    "ChatGPT authorization callback timed out"
                )
            callback_server.timeout = min(0.5, remaining)
            callback_server.handle_request()
        oauth.complete(attempt, callback_server.result)
    return _status_payload(store)


def _status_payload(store: ChatGPTProfileStore) -> dict[str, Any]:
    registration = store.registration()
    session = store.session()
    if session is not None:
        return {
            "client_id": session.client_id,
            "credential_scope_sha256": session.credential_scope_sha256,
            "email": session.email,
            "expires_at": session.expires_at,
            "sharing": session.sharing,
            "status": "connected",
        }
    if registration is not None:
        return {
            "client_id": registration,
            "credential_scope_sha256": None,
            "email": None,
            "expires_at": None,
            "sharing": False,
            "status": "registration-only",
        }
    return {
        "client_id": None,
        "credential_scope_sha256": None,
        "email": None,
        "expires_at": None,
        "sharing": False,
        "status": "not-connected",
    }


def load_config(path: Path) -> OpenAIConnectorConfig:
    path = Path(path)
    if not path.is_absolute():
        raise ChatGPTAuthenticationError("connector config path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ChatGPTAuthenticationError("connector config is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_mode & 0o077
            or (hasattr(os, "getuid") and info.st_uid != os.getuid())
        ):
            raise ChatGPTAuthenticationError(
                "connector config must be one private regular file"
            )
        raw = os.read(descriptor, MAX_CONFIG_BYTES + 1)
    finally:
        os.close(descriptor)
    if not raw or len(raw) > MAX_CONFIG_BYTES:
        raise ChatGPTAuthenticationError("connector config is empty or oversized")
    try:
        return OpenAIConnectorConfig.from_mapping(
            _strict_json_object(raw, subject="connector config")
        )
    except ChatGPTConnectorError as exc:
        raise ChatGPTAuthenticationError("connector config is invalid") from exc


def _recv_exact(connection: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise ChatGPTConnectorError("connector request frame ended early")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_request(connection: socket.socket) -> InferenceInvocationRequest:
    length = struct.unpack("!I", _recv_exact(connection, 4))[0]
    if not 1 <= length <= MAX_REQUEST_FRAME_BYTES:
        raise ChatGPTConnectorError("connector request frame is oversized")
    raw = _recv_exact(connection, length)
    try:
        return InferenceInvocationRequest.from_mapping(
            _strict_json_object(raw, subject="connector request")
        )
    except (ChatGPTConnectorError, ValidationError) as exc:
        raise ChatGPTConnectorError("connector request is invalid") from exc


def _write_response(
    connection: socket.socket,
    response: InferenceInvocationResponse,
) -> None:
    raw = canonical_json(response.to_dict()).encode("utf-8")
    connection.sendall(struct.pack("!I", len(raw)) + raw)


def serve_unix(
    socket_path: Path,
    max_invocations: int,
    connector: Connector,
    *,
    client_timeout: float = DEFAULT_CLIENT_TIMEOUT_SECONDS,
) -> int:
    def handle(connection: socket.socket) -> None:
        request = _read_request(connection)
        response = connector.invoke(request)
        _write_response(connection, response)

    try:
        return serve_unix_socket(
            socket_path,
            max_invocations,
            handle,
            recoverable_errors=(ChatGPTConnectorError, ValidationError, OSError),
            client_timeout=client_timeout,
        )
    except UnixSidecarError as exc:
        raise ChatGPTAuthenticationError(str(exc)) from exc


def _serve(args: argparse.Namespace) -> dict[str, Any]:
    store = ChatGPTProfileStore(args.profile_dir)
    oauth = ChatGPTOAuth(store)
    session = oauth.session()
    config = load_config(args.config)
    if any(
        route.credential_scope_sha256 != session.credential_scope_sha256
        for route in config.routes
    ):
        raise ChatGPTAuthenticationError(
            "connector config does not match the selected ChatGPT account"
        )
    connector = OpenAIChatGPTConnector(
        config,
        credentials=oauth,
        responses=OpenAIResponsesClient(),
    )
    completed = serve_unix(args.socket_path, args.max_invocations, connector)
    return {"invocations": completed, "status": "stopped"}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        store = ChatGPTProfileStore(args.profile_dir)
        if args.command == "login":
            result = _login(
                store,
                no_browser=args.no_browser,
                callback_timeout=args.callback_timeout,
            )
        elif args.command == "status":
            result = _status_payload(store)
        elif args.command == "models":
            result = {
                "models": [
                    {"display_name": model.display_name, "slug": model.slug}
                    for model in ChatGPTOAuth(store).list_models()
                ],
                "status": "connected",
            }
        else:
            result = _serve(args)
    except (ChatGPTConnectorError, OSError) as exc:
        print(f"magic-glass-box-openai-connector: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
