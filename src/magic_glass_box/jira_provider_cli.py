from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import socket
import stat
import struct
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    parse_rfc3339_timestamp,
    require_string,
)
from .jira_provider import (
    ISSUE_FIELDS,
    JIRA_ID_RE,
    PROJECT_KEY_RE,
    JiraAPIResponse,
    JiraProvider,
    JiraProviderError,
    JiraTransportError,
)
from .service_sidecar import (
    MAX_SERVICE_FRAME_OVERHEAD,
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceSidecarSpec,
)
from .sidecar import UnixSidecarError, serve_unix_socket

MAX_CREDENTIAL_BYTES = 128 * 1024
MAX_SPEC_BYTES = 1024 * 1024
MAX_HTTP_SECONDS = 120.0
MAX_TOKEN_BYTES = 64 * 1024
ISSUE_FIELD_ALLOWLIST = frozenset((*ISSUE_FIELDS, "status"))
ISSUE_PATH_RE = re.compile(r"^/rest/api/3/issue/[A-Z][A-Z0-9_]{1,31}-[1-9][0-9]{0,9}$")
TRANSITIONS_PATH_RE = re.compile(
    r"^/rest/api/3/issue/[A-Z][A-Z0-9_]{1,31}-[1-9][0-9]{0,9}/transitions$"
)
HOST_RE = re.compile(
    r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?::[0-9]{1,5})?$"
)


class JiraProviderCLIError(ValueError):
    """The local Jira provider is misconfigured or received invalid data."""


@dataclass(frozen=True)
class JiraCredentialIdentity:
    host: str
    account: str
    credential_scope_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "account": self.account,
            "credential_scope_sha256": self.credential_scope_sha256,
            "host": self.host,
        }


@dataclass(frozen=True)
class JiraCredential:
    base_url: str
    host: str
    account: str
    api_token: str = field(repr=False)
    credential_scope_sha256: str
    state_targets: dict[str, dict[str, str]] = field(repr=False)

    @property
    def identity(self) -> JiraCredentialIdentity:
        return JiraCredentialIdentity(
            host=self.host,
            account=self.account,
            credential_scope_sha256=self.credential_scope_sha256,
        )


class HTTPOpener(Protocol):
    def open(self, request: Request, *, timeout: float) -> object: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        return None


def _strict_json_value(raw: bytes, *, subject: str) -> object:
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise JiraProviderCLIError(f"{subject} is invalid") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise JiraProviderCLIError(f"{subject} is invalid")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise JiraProviderCLIError(f"{subject} is invalid")

    try:
        return json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except JiraProviderCLIError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise JiraProviderCLIError(f"{subject} is invalid") from exc


def _strict_json_object(raw: bytes, *, subject: str) -> dict[str, Any]:
    value = _strict_json_value(raw, subject=subject)
    if not isinstance(value, dict):
        raise JiraProviderCLIError(f"{subject} is invalid")
    return value


def _read_private_file(path: Path, *, maximum: int, subject: str) -> bytes:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise JiraProviderCLIError(f"{subject} path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise JiraProviderCLIError(f"{subject} is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise JiraProviderCLIError(f"{subject} must be a regular file")
        if info.st_mode & 0o077:
            raise JiraProviderCLIError(f"{subject} must not be group/world accessible")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise JiraProviderCLIError(f"{subject} must be owned by this user")
        raw = os.read(descriptor, maximum + 1)
    finally:
        os.close(descriptor)
    if not raw or len(raw) > maximum:
        raise JiraProviderCLIError(f"{subject} is empty or oversized")
    return raw


def _credential_base_url(value: object) -> tuple[str, str]:
    try:
        text = require_string(value, field="Jira base_url", max_bytes=2048)
    except ValidationError as exc:
        raise JiraProviderCLIError("Jira credential file is invalid") from exc
    try:
        parsed = urlsplit(text)
        port = parsed.port
    except ValueError as exc:
        raise JiraProviderCLIError("Jira credential file is invalid") from exc
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.hostname is None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise JiraProviderCLIError("Jira credential file is invalid")
    hostname = parsed.hostname.lower()
    host = hostname if port in {None, 443} else f"{hostname}:{port}"
    if (
        HOST_RE.fullmatch(host) is None
        or host.endswith((".", "-"))
        or ".." in host
        or (port is not None and port > 65535)
    ):
        raise JiraProviderCLIError("Jira credential file is invalid")
    return f"https://{host}", host


def load_credential(path: Path) -> JiraCredential:
    raw = _read_private_file(
        path, maximum=MAX_CREDENTIAL_BYTES, subject="Jira credential file"
    )
    document = _strict_json_object(raw, subject="Jira credential file")
    required = {"schema_version", "base_url", "account", "api_token"}
    if set(document) - required - {"state_targets"} or not required.issubset(document):
        raise JiraProviderCLIError("Jira credential file is invalid")
    schema_version = document["schema_version"]
    if isinstance(schema_version, bool) or schema_version != 1:
        raise JiraProviderCLIError("Jira credential file is invalid")
    base_url, host = _credential_base_url(document["base_url"])
    try:
        account = require_string(
            document["account"], field="Jira account", max_bytes=256
        )
        token = require_string(
            document["api_token"], field="Jira API token", max_bytes=MAX_TOKEN_BYTES
        )
    except ValidationError as exc:
        raise JiraProviderCLIError("Jira credential file is invalid") from exc
    if (
        not account.strip()
        or ":" in account
        or any(character in "\r\n" for character in account + token)
    ):
        raise JiraProviderCLIError("Jira credential file is invalid")
    token_sha256 = hashlib.sha256(token.encode("utf-8")).hexdigest()
    scope = content_sha256(
        canonical_json({"account": account, "host": host, "token_sha256": token_sha256})
    )
    raw_targets = document.get("state_targets", {})
    if not isinstance(raw_targets, dict):
        raise JiraProviderCLIError("Jira credential file is invalid")
    state_targets: dict[str, dict[str, str]] = {}
    for project, states in raw_targets.items():
        if (
            not isinstance(project, str)
            or PROJECT_KEY_RE.fullmatch(project) is None
            or not isinstance(states, dict)
            or set(states) != {"done"}
            or not isinstance(states["done"], str)
            or JIRA_ID_RE.fullmatch(states["done"]) is None
        ):
            raise JiraProviderCLIError("Jira credential file is invalid")
        state_targets[project] = {"done": states["done"]}
    return JiraCredential(base_url, host, account, token, scope, state_targets)


def _remaining_seconds(deadline: str) -> float:
    try:
        parsed = parse_rfc3339_timestamp(deadline, field="Jira operation deadline")
    except ValidationError as exc:
        raise JiraTransportError("Jira operation deadline is invalid") from exc
    remaining = (parsed.astimezone(UTC) - datetime.now(UTC)).total_seconds()
    if remaining <= 0:
        raise JiraTransportError("Jira operation deadline has expired")
    return min(remaining, MAX_HTTP_SECONDS)


def _validate_endpoint(
    method: str, endpoint: str, body: Mapping[str, Any] | None
) -> None:
    if method == "POST" and endpoint == "/rest/api/3/search/jql":
        if not isinstance(body, dict):
            raise JiraTransportError("Jira API request is invalid")
        return
    if method == "POST" and isinstance(endpoint, str):
        transition_id = None
        if isinstance(body, dict) and set(body) == {"transition"}:
            transition = body["transition"]
            if isinstance(transition, dict) and set(transition) == {"id"}:
                transition_id = transition["id"]
        if (
            TRANSITIONS_PATH_RE.fullmatch(endpoint) is not None
            and isinstance(transition_id, str)
            and JIRA_ID_RE.fullmatch(transition_id) is not None
        ):
            return
        raise JiraTransportError("Jira API request is invalid")
    if method != "GET" or body is not None or not isinstance(endpoint, str):
        raise JiraTransportError("Jira API request is invalid")
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme
        or parsed.netloc
        or parsed.fragment
        or (
            ISSUE_PATH_RE.fullmatch(parsed.path) is None
            and TRANSITIONS_PATH_RE.fullmatch(parsed.path) is None
        )
    ):
        raise JiraTransportError("Jira API request is invalid")
    if TRANSITIONS_PATH_RE.fullmatch(parsed.path) is not None:
        if parsed.query:
            raise JiraTransportError("Jira API request is invalid")
        return
    if ISSUE_PATH_RE.fullmatch(parsed.path) is None:
        raise JiraTransportError("Jira API request is invalid")
    try:
        parameters = parse_qsl(
            parsed.query, keep_blank_values=True, strict_parsing=True
        )
    except ValueError as exc:
        raise JiraTransportError("Jira API request is invalid") from exc
    if len(parameters) != 1 or parameters[0][0] != "fields":
        raise JiraTransportError("Jira API request is invalid")
    fields = parameters[0][1].split(",")
    if (
        not fields
        or len(set(fields)) != len(fields)
        or any(item not in ISSUE_FIELD_ALLOWLIST for item in fields)
    ):
        raise JiraTransportError("Jira API request is invalid")


def _header(headers: object, name: str) -> str | None:
    getter = getattr(headers, "get", None)
    if getter is None:
        raise JiraTransportError("Jira HTTP response is invalid")
    value = getter(name)
    if value is None:
        value = getter(name.lower())
    if value is None:
        return None
    if not isinstance(value, str):
        raise JiraTransportError("Jira HTTP response is invalid")
    return value


def _read_http_response(response: object, *, limit: int) -> JiraAPIResponse:
    status = getattr(response, "status", None)
    if status is None:
        status = getattr(response, "code", None)
    headers = getattr(response, "headers", None)
    reader = getattr(response, "read", None)
    if headers is None or reader is None:
        raise JiraTransportError("Jira HTTP response is invalid")
    content_length = _header(headers, "Content-Length")
    if content_length is not None and (
        not content_length.isdigit() or int(content_length) > limit
    ):
        raise JiraTransportError("Jira HTTP response exceeds its limit")
    try:
        raw = reader(limit + 1)
    except (OSError, TimeoutError) as exc:
        raise JiraTransportError("Jira HTTP response failed") from exc
    if not isinstance(raw, bytes) or len(raw) > limit:
        raise JiraTransportError("Jira HTTP response exceeds its limit")
    retry_header = _header(headers, "Retry-After")
    retry_after: int | None = None
    if retry_header is not None:
        if not retry_header.isdigit():
            raise JiraTransportError("Jira retry header is invalid")
        retry_after = int(retry_header)
        if not 1 <= retry_after <= 30 * 24 * 60 * 60:
            raise JiraTransportError("Jira retry header is invalid")
    try:
        body = (
            None
            if not raw.strip()
            else _strict_json_value(raw, subject="Jira HTTP response JSON")
        )
        return JiraAPIResponse(status, body, len(raw), retry_after)
    except (JiraProviderCLIError, JiraProviderError) as exc:
        raise JiraTransportError("Jira HTTP response is invalid") from exc


class JiraHTTP:
    """Credential-owning HTTPS transport for the narrow Jira provider."""

    def __init__(
        self, credential: JiraCredential, *, opener: HTTPOpener | None = None
    ) -> None:
        if not isinstance(credential, JiraCredential):
            raise JiraProviderCLIError("Jira credential is invalid")
        self.credential = credential
        self.opener = opener or build_opener(_NoRedirect())

    def request(
        self,
        method: str,
        *,
        host: str,
        endpoint: str,
        body: Mapping[str, Any] | None,
        deadline: str,
        response_limit_bytes: int,
    ) -> JiraAPIResponse:
        if host != self.credential.host:
            raise JiraTransportError("Jira API host is outside credential authority")
        _validate_endpoint(method, endpoint, body)
        if (
            isinstance(response_limit_bytes, bool)
            or not isinstance(response_limit_bytes, int)
            or response_limit_bytes < 1
        ):
            raise JiraTransportError("Jira API response limit is invalid")
        timeout = _remaining_seconds(deadline)
        raw_body = None if body is None else canonical_json(dict(body)).encode("utf-8")
        encoded = base64.b64encode(
            f"{self.credential.account}:{self.credential.api_token}".encode()
        ).decode("ascii")
        headers = {
            "Accept": "application/json",
            "Authorization": f"Basic {encoded}",
        }
        if raw_body is not None:
            headers["Content-Type"] = "application/json"
        outbound = Request(  # noqa: S310 - base URL is validated HTTPS authority.
            self.credential.base_url + endpoint,
            data=raw_body,
            headers=headers,
            method=method,
        )
        response: object
        try:
            response = self.opener.open(outbound, timeout=timeout)
        except HTTPError as exc:
            response = exc
        except (OSError, TimeoutError, URLError) as exc:
            raise JiraTransportError("Jira HTTPS transport failed") from exc
        try:
            return _read_http_response(response, limit=response_limit_bytes)
        finally:
            closer = getattr(response, "close", None)
            if closer is not None:
                closer()


def load_spec(path: Path) -> ServiceSidecarSpec:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise JiraProviderCLIError("Jira provider spec path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise JiraProviderCLIError("Jira provider spec is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise JiraProviderCLIError("Jira provider spec must be a regular file")
        raw = os.read(descriptor, MAX_SPEC_BYTES + 1)
    finally:
        os.close(descriptor)
    if not raw or len(raw) > MAX_SPEC_BYTES:
        raise JiraProviderCLIError("Jira provider spec is empty or oversized")
    try:
        return ServiceSidecarSpec.from_mapping(
            _strict_json_object(raw, subject="Jira provider spec")
        )
    except (ValidationError, JiraProviderCLIError) as exc:
        raise JiraProviderCLIError("Jira provider spec is invalid") from exc


def validate_spec_credentials(
    spec: ServiceSidecarSpec, credential: JiraCredential
) -> JiraCredentialIdentity:
    if not isinstance(spec, ServiceSidecarSpec) or not isinstance(
        credential, JiraCredential
    ):
        raise JiraProviderCLIError("Jira credential authority is invalid")
    identity = credential.identity
    for mapping in spec.mappings:
        if mapping.host != identity.host:
            raise JiraProviderCLIError(
                "Jira mapping host does not match the selected credential"
            )
        if mapping.account.casefold() != identity.account.casefold():
            raise JiraProviderCLIError(
                "Jira mapping account does not match the selected credential"
            )
        if not hmac.compare_digest(
            mapping.credential_scope_sha256, identity.credential_scope_sha256
        ):
            raise JiraProviderCLIError("Jira mapping credential scope does not match")
    return identity


def _recv_exact(connection: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise JiraProviderCLIError("Jira provider request ended early")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_request(
    connection: socket.socket, spec: ServiceSidecarSpec
) -> ServiceOperationRequest:
    raw_length = _recv_exact(connection, 4)
    length = struct.unpack("!I", raw_length)[0]
    maximum = spec.request_bytes + MAX_SERVICE_FRAME_OVERHEAD
    if not 1 <= length <= maximum:
        raise JiraProviderCLIError("Jira provider request frame is oversized")
    raw = _recv_exact(connection, length)
    try:
        return ServiceOperationRequest.from_mapping(
            _strict_json_object(raw, subject="Jira provider request")
        )
    except (ValidationError, JiraProviderCLIError) as exc:
        raise JiraProviderCLIError("Jira provider request is invalid") from exc


def _write_response(
    connection: socket.socket,
    request: ServiceOperationRequest,
    response: ServiceOperationResponse,
) -> None:
    raw = canonical_json(response.to_dict()).encode("utf-8")
    if len(raw) > request.response_limit_bytes + MAX_SERVICE_FRAME_OVERHEAD:
        raise JiraProviderCLIError("Jira provider response frame is oversized")
    connection.sendall(struct.pack("!I", len(raw)) + raw)


def serve_unix(socket_path: Path, max_invocations: int, provider: JiraProvider) -> int:
    if Path(socket_path) != Path(provider.spec.transport.socket_path):
        raise JiraProviderCLIError("Jira provider socket does not match its authority")

    def handle(connection: socket.socket) -> None:
        request = _read_request(connection, provider.spec)
        response = provider.invoke(request)
        _write_response(connection, request, response)

    try:
        return serve_unix_socket(
            socket_path,
            max_invocations,
            handle,
            recoverable_errors=(
                JiraProviderCLIError,
                JiraProviderError,
                ValidationError,
                OSError,
            ),
            client_timeout=min(300.0, provider.spec.timeout_ms / 1000 + 1.0),
        )
    except UnixSidecarError as exc:
        raise JiraProviderCLIError(str(exc)) from exc


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="magic-glass-box-jira-provider",
        description="Run the credential-isolated Jira reference provider.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="Show non-secret credential identity")
    inspect.add_argument("--credential-file", required=True, type=Path)

    serve = commands.add_parser("serve", help="Serve the bounded provider carrier")
    serve.add_argument("--credential-file", required=True, type=Path)
    serve.add_argument("--spec", required=True, type=Path)
    serve.add_argument("--socket-path", required=True, type=Path)
    serve.add_argument("--max-invocations", required=True, type=_positive_integer)
    return parser


def _serve(args: argparse.Namespace) -> dict[str, object]:
    credential = load_credential(args.credential_file)
    spec = load_spec(args.spec)
    validate_spec_credentials(spec, credential)
    completed = serve_unix(
        args.socket_path,
        args.max_invocations,
        JiraProvider(
            spec,
            JiraHTTP(credential),
            state_targets=credential.state_targets or None,
        ),
    )
    return {"invocations": completed, "status": "stopped"}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "inspect":
            credential = load_credential(args.credential_file)
            result: object = {
                "credential": credential.identity.to_dict(),
                "status": "configured",
            }
        else:
            result = _serve(args)
    except (JiraProviderCLIError, JiraProviderError, JiraTransportError) as exc:
        print(f"magic-glass-box-jira-provider: {exc}", file=sys.stderr)
        return 2
    except OSError:
        print("magic-glass-box-jira-provider: operation failed", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
