from __future__ import annotations

import http.client
import json
import re
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit

from . import __version__
from .domain import ProviderGrantSpec, ValidationError, canonical_json

PROVIDER_GRANT_ID_RE = re.compile(r"^pgr_[0-9a-f]{32}$")


class ProviderError(RuntimeError):
    """A provider request was denied or failed without exposing credentials."""


class ProviderBusyError(ProviderError):
    pass


class ProviderBudgetError(ProviderError):
    """A pinned local budget ceiling denied dispatch before provider access."""


class ProviderInvocationLimitError(ProviderError):
    """A pinned per-run invocation ceiling denied dispatch before provider access."""


class ProviderUpstreamError(ProviderError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        request_bytes: int = 0,
        response_bytes: int = 0,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.request_bytes = request_bytes
        self.response_bytes = response_bytes


class ProviderAmbiguousError(ProviderUpstreamError):
    """An invocation may have consumed provider quota and must not be replayed."""


class ProviderRecoverableError(ProviderUpstreamError):
    """An explicit provider refusal can be retried after a bounded delay."""

    category: str

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: int,
        status_code: int | None = None,
        request_bytes: int = 0,
        response_bytes: int = 0,
    ):
        super().__init__(
            message,
            status_code=status_code,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
        )
        self.retry_after_seconds = retry_after_seconds


class ProviderQuotaError(ProviderRecoverableError):
    category = "quota_exhausted"


class ProviderRateLimitError(ProviderRecoverableError):
    category = "rate_limited"


DEFAULT_QUOTA_RETRY_AFTER_SECONDS = 15 * 60
DEFAULT_RATE_LIMIT_RETRY_AFTER_SECONDS = 60
_QUOTA_ERROR_MARKERS = ("quota", "usage_limit", "billing", "credit_balance")
_RATE_LIMIT_ERROR_MARKERS = ("rate_limit", "too_many_requests", "overload")


def provider_recovery_error(
    *,
    error_code: str,
    status_code: int | None,
    retry_after_seconds: int | None,
    request_bytes: int,
    response_bytes: int,
) -> ProviderRecoverableError | None:
    """Classify only explicit provider failures whose replay timing is known."""

    normalized = error_code.lower()
    error_type: type[ProviderRecoverableError] | None = None
    fallback = 0
    if any(marker in normalized for marker in _QUOTA_ERROR_MARKERS):
        error_type = ProviderQuotaError
        fallback = DEFAULT_QUOTA_RETRY_AFTER_SECONDS
    elif status_code in {429, 529} or any(
        marker in normalized for marker in _RATE_LIMIT_ERROR_MARKERS
    ):
        error_type = ProviderRateLimitError
        fallback = DEFAULT_RATE_LIMIT_RETRY_AFTER_SECONDS
    if error_type is None:
        return None
    delay = fallback if retry_after_seconds is None else retry_after_seconds
    return error_type(
        "provider request is temporarily ineligible",
        retry_after_seconds=delay,
        status_code=status_code,
        request_bytes=request_bytes,
        response_bytes=response_bytes,
    )


@dataclass(frozen=True)
class ProviderInvocationResult:
    status_code: int
    request_bytes: int
    response_bytes: int
    untrusted_response: dict[str, Any]


@dataclass(frozen=True)
class _HTTPResult:
    status_code: int
    body: bytes
    content_type: str | None


class ProviderAdapter(Protocol):
    def invoke(
        self,
        spec: ProviderGrantSpec,
        credential: str,
        request: dict[str, Any],
    ) -> ProviderInvocationResult: ...


ConnectionFactory = Callable[[str, str, int | None, float], http.client.HTTPConnection]


def _remaining_http_time(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("provider request deadline expired")
    return remaining


def _apply_http_deadline(connection: object, deadline: float) -> None:
    remaining = _remaining_http_time(deadline)
    sock = getattr(connection, "sock", None)
    if sock is not None:
        sock.settimeout(remaining)


def _read_http_response(
    response: object,
    connection: object,
    *,
    deadline: float,
    limit: int,
) -> bytes:
    read_one = getattr(response, "read1", None)
    if not callable(read_one):
        _apply_http_deadline(connection, deadline)
        raw = response.read(limit + 1)
        _remaining_http_time(deadline)
        return raw
    chunks: list[bytes] = []
    total = 0
    while total <= limit:
        _apply_http_deadline(connection, deadline)
        chunk = read_one(min(64 * 1024, limit + 1 - total))
        _remaining_http_time(deadline)
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    return b"".join(chunks)


def _invoke_http(
    spec: ProviderGrantSpec,
    credential: str,
    body: bytes,
    *,
    accept: str,
    extra_headers: Mapping[str, str] | None = None,
    connection_factory: ConnectionFactory,
) -> _HTTPResult:
    if not body or len(body) > spec.request_bytes:
        raise ProviderError("provider request exceeds its size limit")
    parsed = urlsplit(spec.destination)
    target = parsed.path
    if parsed.query:
        target += "?" + parsed.query
    headers = {
        "accept": accept,
        "accept-encoding": "identity",
        "content-type": "application/json",
        "content-length": str(len(body)),
        "user-agent": f"magic-glass-box/{__version__}",
        **dict(extra_headers or {}),
        **dict(spec.static_headers),
        spec.credential_header: spec.credential_prefix + credential,
    }
    timeout_seconds = spec.timeout_ms / 1000
    deadline = time.monotonic() + timeout_seconds
    connection = connection_factory(
        parsed.scheme,
        parsed.hostname or "",
        parsed.port,
        timeout_seconds,
    )
    status: int | None = None
    raw = b""
    content_type: str | None = None
    dispatched = False

    def response_error(message: str) -> ProviderUpstreamError:
        error_type: type[ProviderUpstreamError] = (
            ProviderAmbiguousError
            if status is not None and 200 <= status < 300
            else ProviderUpstreamError
        )
        return error_type(
            message,
            status_code=status,
            request_bytes=len(body),
            response_bytes=len(raw),
        )

    try:
        _apply_http_deadline(connection, deadline)
        dispatched = True
        connection.request(spec.method, target, body=body, headers=headers)
        _apply_http_deadline(connection, deadline)
        response = connection.getresponse()
        status = response.status
        declared = response.getheader("Content-Length")
        content_type = response.getheader("Content-Type")
        if declared is not None:
            try:
                declared_size = int(declared)
            except ValueError as exc:
                raise response_error("provider returned an invalid response") from exc
            if declared_size < 0 or declared_size > spec.response_bytes:
                raise response_error("provider response exceeds its size limit")
        raw = _read_http_response(
            response,
            connection,
            deadline=deadline,
            limit=spec.response_bytes,
        )
        if len(raw) > spec.response_bytes:
            raise response_error("provider response exceeds its size limit")
    except ProviderError:
        raise
    except (OSError, TimeoutError, http.client.HTTPException) as exc:
        error_type: type[ProviderUpstreamError] = (
            ProviderAmbiguousError
            if dispatched and (status is None or 200 <= status < 300)
            else ProviderUpstreamError
        )
        raise error_type(
            "provider is unavailable",
            status_code=status,
            request_bytes=len(body),
            response_bytes=len(raw),
        ) from exc
    finally:
        connection.close()
    if status is None or not 200 <= status < 300:
        raise ProviderUpstreamError(
            "provider request failed",
            status_code=status,
            request_bytes=len(body),
            response_bytes=len(raw),
        )
    return _HTTPResult(status, raw, content_type)


def _strict_response(raw: bytes) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise ProviderUpstreamError("provider returned an invalid response") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ProviderUpstreamError("provider returned an invalid response")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ProviderUpstreamError("provider returned an invalid response")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ProviderUpstreamError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ProviderUpstreamError("provider returned an invalid response") from exc
    if not isinstance(value, dict):
        raise ProviderUpstreamError("provider returned an invalid response")
    return value


class JSONHTTPProviderAdapter:
    """POST finite JSON to one policy-pinned provider destination."""

    def __init__(self, connection_factory: ConnectionFactory | None = None):
        self.connection_factory = connection_factory or self._new_connection

    @staticmethod
    def _new_connection(
        scheme: str,
        host: str,
        port: int | None,
        timeout: float,
    ) -> http.client.HTTPConnection:
        connection_type = (
            http.client.HTTPSConnection
            if scheme == "https"
            else http.client.HTTPConnection
        )
        return connection_type(host, port=port, timeout=timeout)

    def invoke(
        self,
        spec: ProviderGrantSpec,
        credential: str,
        request: dict[str, Any],
    ) -> ProviderInvocationResult:
        body = (canonical_json({**request, "model": spec.model})).encode("utf-8")
        response = _invoke_http(
            spec,
            credential,
            body,
            accept="application/json",
            extra_headers=None,
            connection_factory=self.connection_factory,
        )
        try:
            value = _strict_response(response.body)
        except ProviderUpstreamError as exc:
            raise ProviderAmbiguousError(
                str(exc),
                status_code=response.status_code,
                request_bytes=len(body),
                response_bytes=len(response.body),
            ) from exc
        return ProviderInvocationResult(
            status_code=response.status_code,
            request_bytes=len(body),
            response_bytes=len(response.body),
            untrusted_response=value,
        )


class BufferedHTTPProviderAdapter:
    """Buffer one pinned vendor SSE response for a networkless worker."""

    ADAPTERS = {
        "openai-responses": "openai_responses",
        "anthropic-messages": "anthropic_messages",
    }
    HEADERS = {
        "openai-responses": frozenset({"openai-beta", "x-codex-beta-features"}),
        "anthropic-messages": frozenset({"anthropic-beta", "anthropic-version"}),
    }

    def __init__(
        self,
        protocol: str,
        connection_factory: ConnectionFactory | None = None,
    ):
        if protocol not in self.ADAPTERS:
            raise ProviderError("provider protocol is unavailable")
        self.protocol = protocol
        self.connection_factory = (
            connection_factory or JSONHTTPProviderAdapter._new_connection
        )

    def invoke(
        self,
        spec: ProviderGrantSpec,
        credential: str,
        request: dict[str, Any],
    ) -> ProviderInvocationResult:
        if spec.adapter != self.ADAPTERS[self.protocol]:
            raise ProviderError("provider protocol does not match its pinned adapter")
        if not isinstance(request, dict) or set(request) != {
            "protocol",
            "body",
            "headers",
        }:
            raise ProviderError("provider protocol request has an invalid shape")
        if request["protocol"] != self.protocol:
            raise ProviderError("provider protocol does not match its pinned adapter")
        provider_body = request["body"]
        if not isinstance(provider_body, dict):
            raise ProviderError("provider protocol body must be an object")
        if "model" in provider_body:
            raise ProviderError("provider protocol body cannot select a model")
        if provider_body.get("stream") is not True:
            raise ProviderError("provider protocol request must use streaming")
        raw_headers = request["headers"]
        if not isinstance(raw_headers, dict) or len(raw_headers) > len(
            self.HEADERS[self.protocol]
        ):
            raise ProviderError("provider protocol headers are invalid")
        headers: dict[str, str] = {}
        for raw_name, raw_value in raw_headers.items():
            if not isinstance(raw_name, str):
                raise ProviderError("provider protocol header is invalid")
            name = raw_name.lower()
            if (
                name not in self.HEADERS[self.protocol]
                or name in headers
                or not isinstance(raw_value, str)
                or not raw_value
                or not raw_value.isascii()
                or len(raw_value) > 8192
                or any(not 0x20 <= ord(character) <= 0x7E for character in raw_value)
            ):
                raise ProviderError("provider protocol header is invalid")
            headers[name] = raw_value
        try:
            body = canonical_json({**provider_body, "model": spec.model}).encode(
                "utf-8"
            )
        except ValidationError as exc:
            raise ProviderError(str(exc)) from exc
        response = _invoke_http(
            spec,
            credential,
            body,
            accept="text/event-stream",
            extra_headers=headers,
            connection_factory=self.connection_factory,
        )
        content_type = response.content_type
        if (
            not isinstance(content_type, str)
            or content_type.split(";", 1)[0].strip().lower() != "text/event-stream"
        ):
            raise ProviderAmbiguousError(
                "provider returned an invalid response",
                status_code=response.status_code,
                request_bytes=len(body),
                response_bytes=len(response.body),
            )
        try:
            text = response.body.decode("utf-8", "strict")
        except UnicodeDecodeError as exc:
            raise ProviderAmbiguousError(
                "provider returned an invalid response",
                status_code=response.status_code,
                request_bytes=len(body),
                response_bytes=len(response.body),
            ) from exc
        if not text:
            raise ProviderAmbiguousError(
                "provider returned an invalid response",
                status_code=response.status_code,
                request_bytes=len(body),
            )
        return ProviderInvocationResult(
            status_code=response.status_code,
            request_bytes=len(body),
            response_bytes=len(response.body),
            untrusted_response={
                "body": text,
                "content_type": content_type,
                "protocol": self.protocol,
            },
        )


class ProviderBroker:
    """Apply process-local concurrency defense and dispatch registered adapters."""

    def __init__(self, adapters: Mapping[str, ProviderAdapter] | None = None):
        self.adapters = dict(
            {
                "json_http": JSONHTTPProviderAdapter(),
                "openai_responses": BufferedHTTPProviderAdapter("openai-responses"),
                "anthropic_messages": BufferedHTTPProviderAdapter("anthropic-messages"),
            }
            if adapters is None
            else adapters
        )
        self._limits: dict[str, tuple[int, threading.BoundedSemaphore]] = {}
        self._lock = threading.Lock()

    def _semaphore(self, grant_id: str, maximum: int) -> threading.BoundedSemaphore:
        with self._lock:
            current = self._limits.get(grant_id)
            if current is None:
                current = (maximum, threading.BoundedSemaphore(maximum))
                self._limits[grant_id] = current
            elif current[0] != maximum:
                raise ProviderError("provider grant policy changed unexpectedly")
            return current[1]

    def invoke(
        self,
        *,
        grant_id: str,
        spec: ProviderGrantSpec,
        credential: str,
        request: dict[str, Any],
    ) -> ProviderInvocationResult:
        if (
            not isinstance(grant_id, str)
            or PROVIDER_GRANT_ID_RE.fullmatch(grant_id) is None
        ):
            raise ProviderError("provider grant is invalid")
        if not isinstance(spec, ProviderGrantSpec):
            raise ProviderError("provider grant policy is invalid")
        if not isinstance(credential, str) or not credential:
            raise ProviderError("provider credential is unavailable")
        if not isinstance(request, dict):
            raise ProviderError("provider request must be an object")
        if "model" in request:
            raise ProviderError("provider request cannot override the pinned model")
        try:
            encoded = canonical_json({**request, "model": spec.model}).encode("utf-8")
        except ValidationError as exc:
            raise ProviderError(str(exc)) from exc
        if not encoded or len(encoded) > spec.request_bytes:
            raise ProviderError("provider request exceeds its size limit")
        adapter = self.adapters.get(spec.adapter)
        if adapter is None:
            raise ProviderError("provider adapter is unavailable")
        semaphore = self._semaphore(grant_id, spec.max_concurrency)
        if not semaphore.acquire(blocking=False):
            raise ProviderBusyError("provider grant concurrency limit is reached")
        try:
            result = adapter.invoke(spec, credential, request)
        finally:
            semaphore.release()
        if not isinstance(result, ProviderInvocationResult):
            raise ProviderError("provider adapter returned an invalid response")
        if (
            isinstance(result.status_code, bool)
            or not isinstance(result.status_code, int)
            or not 200 <= result.status_code < 300
            or isinstance(result.request_bytes, bool)
            or not isinstance(result.request_bytes, int)
            or not 1 <= result.request_bytes <= spec.request_bytes
            or isinstance(result.response_bytes, bool)
            or not isinstance(result.response_bytes, int)
            or not 1 <= result.response_bytes <= spec.response_bytes
            or not isinstance(result.untrusted_response, dict)
        ):
            raise ProviderError("provider adapter returned an invalid response")
        try:
            canonical_json(result.untrusted_response)
        except ValidationError as exc:
            raise ProviderError(
                "provider adapter returned an invalid response"
            ) from exc
        return result
