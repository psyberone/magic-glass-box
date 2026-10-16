from __future__ import annotations

import base64
import contextlib
import fcntl
import hashlib
import hmac
import http.client
import json
import os
import re
import secrets
import stat
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlencode, urlsplit

from . import __version__
from .domain import ValidationError, canonical_json, validate_provider_name
from .inference import (
    CONNECTOR_ID_RE,
    InferenceInvocationRequest,
    InferenceInvocationResponse,
)

CHATGPT_ISSUER = "https://auth.openai.com"
CHATGPT_AUTHORIZE_URL = CHATGPT_ISSUER + "/api/accounts/authorize"
CHATGPT_TOKEN_URL = CHATGPT_ISSUER + "/api/accounts/oauth/token"
CHATGPT_JWKS_URL = CHATGPT_ISSUER + "/.well-known/jwks.json"
CHATGPT_RESOURCE = "https://api.openai.com/v1"
CHATGPT_MODELS_URL = CHATGPT_RESOURCE + "/models"
CHATGPT_RESPONSES_URL = CHATGPT_RESOURCE + "/responses"
CHATGPT_DIRECT_SCOPE = "chatgpt.tokens.use.direct"
CHATGPT_SCOPES = (
    "openid",
    "profile",
    "email",
    "offline_access",
    "resource.invoke",
    CHATGPT_DIRECT_SCOPE,
)
MAX_AUTH_RESPONSE_BYTES = 1024 * 1024
MAX_PROFILE_BYTES = 512 * 1024
CLIENT_ID_RE = re.compile(r"^oaiapp_[A-Za-z0-9_-]{8,240}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ChatGPTConnectorError(RuntimeError):
    """A connector-owned ChatGPT operation failed without exposing tokens."""


class ChatGPTAuthenticationError(ChatGPTConnectorError):
    pass


class ChatGPTResponseFailure(ChatGPTConnectorError):
    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        status_code: int | None = None,
        request_bytes: int = 0,
        response_bytes: int = 0,
    ):
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code
        self.request_bytes = request_bytes
        self.response_bytes = response_bytes


class ChatGPTResponseAmbiguousError(ChatGPTResponseFailure):
    """An inference request may have consumed quota and must not be replayed."""


class HTTPTransportError(RuntimeError):
    def __init__(self, message: str, *, ambiguous: bool):
        super().__init__(message)
        self.ambiguous = ambiguous


@dataclass(frozen=True)
class HTTPResponse:
    status_code: int
    content_type: str | None
    body: bytes
    request_id: str | None


class HTTPClient(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
        max_response_bytes: int,
    ) -> HTTPResponse: ...


HTTPSConnectionFactory = Callable[[str, int | None, float], http.client.HTTPSConnection]


class BoundedHTTPSClient:
    """Issue one bounded HTTPS request and classify pre/post-dispatch failures."""

    def __init__(
        self,
        connection_factory: HTTPSConnectionFactory | None = None,
    ):
        self.connection_factory = connection_factory or self._new_connection

    @staticmethod
    def _new_connection(
        host: str,
        port: int | None,
        timeout: float,
    ) -> http.client.HTTPSConnection:
        return http.client.HTTPSConnection(host, port=port, timeout=timeout)

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
        max_response_bytes: int,
    ) -> HTTPResponse:
        parsed = urlsplit(url)
        try:
            port = parsed.port
        except ValueError as exc:
            raise HTTPTransportError(
                "HTTPS destination is invalid", ambiguous=False
            ) from exc
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or not parsed.path.startswith("/")
        ):
            raise HTTPTransportError("HTTPS destination is invalid", ambiguous=False)
        if (
            method not in {"GET", "POST"}
            or isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or timeout <= 0
            or isinstance(max_response_bytes, bool)
            or not isinstance(max_response_bytes, int)
            or max_response_bytes < 1
        ):
            raise HTTPTransportError("HTTPS request is invalid", ambiguous=False)
        target = parsed.path
        if parsed.query:
            target += "?" + parsed.query
        try:
            connection = self.connection_factory(parsed.hostname, port, float(timeout))
        except (OSError, TimeoutError, http.client.HTTPException) as exc:
            raise HTTPTransportError(
                "HTTPS endpoint is unavailable", ambiguous=False
            ) from exc
        dispatched = False
        raw = b""
        try:
            try:
                connection.connect()
            except (OSError, TimeoutError, http.client.HTTPException) as exc:
                raise HTTPTransportError(
                    "HTTPS endpoint is unavailable", ambiguous=False
                ) from exc
            try:
                dispatched = True
                connection.request(method, target, body=body, headers=headers)
                response = connection.getresponse()
                declared = response.getheader("Content-Length")
                if declared is not None:
                    try:
                        declared_size = int(declared)
                    except ValueError as exc:
                        raise HTTPTransportError(
                            "HTTPS response is invalid", ambiguous=True
                        ) from exc
                    if not 0 <= declared_size <= max_response_bytes:
                        raise HTTPTransportError(
                            "HTTPS response is oversized", ambiguous=True
                        )
                raw = response.read(max_response_bytes + 1)
                if len(raw) > max_response_bytes:
                    raise HTTPTransportError(
                        "HTTPS response is oversized", ambiguous=True
                    )
                return HTTPResponse(
                    status_code=response.status,
                    content_type=response.getheader("Content-Type"),
                    body=raw,
                    request_id=(
                        response.getheader("x-request-id")
                        or response.getheader("request-id")
                    ),
                )
            except HTTPTransportError:
                raise
            except (OSError, TimeoutError, http.client.HTTPException) as exc:
                raise HTTPTransportError(
                    "HTTPS request outcome is unavailable",
                    ambiguous=dispatched,
                ) from exc
        finally:
            connection.close()


def _strict_json_object(raw: bytes, *, subject: str) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise ChatGPTConnectorError(f"{subject} is invalid") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ChatGPTConnectorError(f"{subject} is invalid")
            value[key] = item
        return value

    def reject_constant(_value: str) -> None:
        raise ChatGPTConnectorError(f"{subject} is invalid")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ChatGPTConnectorError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ChatGPTConnectorError(f"{subject} is invalid") from exc
    if not isinstance(value, dict):
        raise ChatGPTConnectorError(f"{subject} is invalid")
    return value


def _exact_fields(
    value: Mapping[str, Any],
    *,
    expected: set[str],
    subject: str,
) -> None:
    if set(value) != expected:
        raise ChatGPTConnectorError(f"{subject} has an invalid shape")


def _text(
    value: object,
    *,
    field: str,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str) or (not value and not allow_empty):
        raise ChatGPTConnectorError(f"{field} is invalid")
    if len(value.encode("utf-8")) > maximum or "\x00" in value:
        raise ChatGPTConnectorError(f"{field} is invalid")
    return value


def _authentication_text(
    value: object,
    *,
    field: str,
    maximum: int,
) -> str:
    try:
        return _text(value, field=field, maximum=maximum)
    except ChatGPTConnectorError as exc:
        raise ChatGPTAuthenticationError(str(exc)) from exc


def _client_id(value: object) -> str:
    if not isinstance(value, str) or CLIENT_ID_RE.fullmatch(value) is None:
        raise ChatGPTAuthenticationError("client_id is invalid")
    return value


def _host_id(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("urn:uuid:"):
        raise ChatGPTAuthenticationError("host identifier is invalid")
    try:
        parsed = uuid.UUID(value.removeprefix("urn:uuid:"))
    except (ValueError, AttributeError) as exc:
        raise ChatGPTAuthenticationError("host identifier is invalid") from exc
    if parsed.version != 4:
        raise ChatGPTAuthenticationError("host identifier is invalid")
    return value


def _timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise ChatGPTAuthenticationError(f"{field} is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ChatGPTAuthenticationError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise ChatGPTAuthenticationError(f"{field} is invalid")
    return parsed.astimezone(UTC)


@dataclass(frozen=True, repr=False)
class ChatGPTSession:
    schema_version: int
    client_id: str
    ext_agent_host_id: str
    issuer: str
    subject: str
    email: str | None
    id_token: str
    access_token: str
    refresh_token: str
    token_type: str
    expires_at: str
    scopes: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ChatGPTSession:
        if not isinstance(value, dict):
            raise ChatGPTAuthenticationError("ChatGPT session is invalid")
        expected = {
            "schema_version",
            "client_id",
            "ext_agent_host_id",
            "issuer",
            "subject",
            "email",
            "id_token",
            "access_token",
            "refresh_token",
            "token_type",
            "expires_at",
            "scopes",
        }
        try:
            _exact_fields(value, expected=expected, subject="ChatGPT session")
        except ChatGPTConnectorError as exc:
            raise ChatGPTAuthenticationError(str(exc)) from exc
        if value["schema_version"] != 1:
            raise ChatGPTAuthenticationError("ChatGPT session version is invalid")
        client_id = _client_id(value["client_id"])
        host_id = _host_id(value["ext_agent_host_id"])
        issuer = _authentication_text(
            value["issuer"], field="session issuer", maximum=256
        )
        if issuer != CHATGPT_ISSUER:
            raise ChatGPTAuthenticationError("session issuer is invalid")
        subject = _authentication_text(
            value["subject"], field="session subject", maximum=1024
        )
        email_value = value["email"]
        email = (
            None
            if email_value is None
            else _authentication_text(email_value, field="session email", maximum=1024)
        )
        id_token = _authentication_text(
            value["id_token"], field="ID token", maximum=262144
        )
        access_token = _authentication_text(
            value["access_token"], field="access token", maximum=262144
        )
        refresh_token = _authentication_text(
            value["refresh_token"], field="refresh token", maximum=262144
        )
        token_type = value["token_type"]
        if not isinstance(token_type, str) or token_type.lower() != "bearer":  # noqa: S105
            raise ChatGPTAuthenticationError("token type is invalid")
        expires_at = _timestamp(value["expires_at"], field="token expiry")
        raw_scopes = value["scopes"]
        if not isinstance(raw_scopes, list) or not raw_scopes or len(raw_scopes) > 32:
            raise ChatGPTAuthenticationError("granted scopes are invalid")
        scopes = tuple(
            sorted(
                _authentication_text(item, field="granted scope", maximum=128)
                for item in raw_scopes
            )
        )
        if len(set(scopes)) != len(scopes):
            raise ChatGPTAuthenticationError("granted scopes are invalid")
        return cls(
            schema_version=1,
            client_id=client_id,
            ext_agent_host_id=host_id,
            issuer=issuer,
            subject=subject,
            email=email,
            id_token=id_token,
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="Bearer",  # noqa: S106 - OAuth token type
            expires_at=expires_at.isoformat(),
            scopes=scopes,
        )

    def __repr__(self) -> str:
        return (
            "ChatGPTSession("
            f"client_id={self.client_id!r}, subject={self.subject!r}, "
            f"email={self.email!r}, expires_at={self.expires_at!r}, "
            f"scopes={self.scopes!r})"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "access_token": self.access_token,
            "client_id": self.client_id,
            "email": self.email,
            "expires_at": self.expires_at,
            "ext_agent_host_id": self.ext_agent_host_id,
            "id_token": self.id_token,
            "issuer": self.issuer,
            "refresh_token": self.refresh_token,
            "schema_version": self.schema_version,
            "scopes": list(self.scopes),
            "subject": self.subject,
            "token_type": self.token_type,
        }

    @property
    def sharing(self) -> bool:
        return {
            CHATGPT_DIRECT_SCOPE,
            "resource.invoke",
        }.issubset(self.scopes)

    @property
    def credential_scope_sha256(self) -> str:
        authority = canonical_json(
            {
                "client_id": self.client_id,
                "issuer": self.issuer,
                "subject": self.subject,
            }
        )
        return hashlib.sha256(authority.encode("utf-8")).hexdigest()

    def is_fresh(
        self,
        *,
        now: datetime | None = None,
        leeway_seconds: int = 60,
    ) -> bool:
        current = now or datetime.now(UTC)
        expiry = datetime.fromisoformat(self.expires_at)
        return expiry > current + timedelta(seconds=leeway_seconds)


class ChatGPTProfileStore:
    """Persist one connector-owned registration and renewable session."""

    def __init__(self, directory: Path):
        self.directory = Path(directory)
        if not self.directory.is_absolute():
            raise ChatGPTAuthenticationError("profile directory must be absolute")

    def _ensure_directory(self) -> None:
        try:
            self.directory.mkdir(parents=True, mode=0o700, exist_ok=True)
            info = self.directory.lstat()
        except OSError as exc:
            raise ChatGPTAuthenticationError(
                "profile directory is unavailable"
            ) from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ChatGPTAuthenticationError("profile directory is unsafe")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise ChatGPTAuthenticationError(
                "profile directory is not owned by the current user"
            )
        if info.st_mode & 0o077:
            raise ChatGPTAuthenticationError("profile directory must use mode 0700")

    def _read(self, name: str) -> dict[str, Any] | None:
        self._ensure_directory()
        path = self.directory / name
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ChatGPTAuthenticationError("profile record is unsafe") from exc
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_mode & 0o077
                or (hasattr(os, "getuid") and info.st_uid != os.getuid())
            ):
                raise ChatGPTAuthenticationError("profile record is unsafe")
            raw = os.read(descriptor, MAX_PROFILE_BYTES + 1)
            if len(raw) > MAX_PROFILE_BYTES:
                raise ChatGPTAuthenticationError("profile record is oversized")
        finally:
            os.close(descriptor)
        try:
            return _strict_json_object(raw, subject="profile record")
        except ChatGPTConnectorError as exc:
            raise ChatGPTAuthenticationError(str(exc)) from exc

    def _write(self, name: str, value: Mapping[str, Any]) -> None:
        self._ensure_directory()
        raw = canonical_json(dict(value)).encode("utf-8")
        if not raw or len(raw) > MAX_PROFILE_BYTES:
            raise ChatGPTAuthenticationError("profile record is oversized")
        temporary = self.directory / f".{name}.{uuid.uuid4().hex}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor: int | None = None
        try:
            descriptor = os.open(temporary, flags, 0o600)
            offset = 0
            while offset < len(raw):
                written = os.write(descriptor, raw[offset:])
                if written <= 0:
                    raise OSError("short profile write")
                offset += written
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.replace(temporary, self.directory / name)
        except (OSError, ValidationError) as exc:
            raise ChatGPTAuthenticationError(
                "profile record could not be saved"
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            with contextlib.suppress(OSError):
                temporary.unlink(missing_ok=True)

    def host_id(self) -> str:
        value = self._read("host.json")
        if value is None:
            host_id = "urn:uuid:" + str(uuid.uuid4())
            self._write(
                "host.json",
                {"ext_agent_host_id": host_id, "schema_version": 1},
            )
            return host_id
        if (
            set(value) != {"ext_agent_host_id", "schema_version"}
            or value.get("schema_version") != 1
        ):
            raise ChatGPTAuthenticationError("host profile is invalid")
        return _host_id(value["ext_agent_host_id"])

    def registration(self) -> str | None:
        value = self._read("registration.json")
        if value is None:
            return None
        if (
            set(value) != {"client_id", "schema_version"}
            or value.get("schema_version") != 1
        ):
            raise ChatGPTAuthenticationError("registration profile is invalid")
        return _client_id(value["client_id"])

    def save_registration(self, client_id: str) -> None:
        self._write(
            "registration.json",
            {"client_id": _client_id(client_id), "schema_version": 1},
        )

    def session(self) -> ChatGPTSession | None:
        value = self._read("session.json")
        if value is None:
            return None
        return ChatGPTSession.from_mapping(value)

    def save_session(self, session: ChatGPTSession) -> None:
        if not isinstance(session, ChatGPTSession):
            raise ChatGPTAuthenticationError("ChatGPT session is invalid")
        if session.ext_agent_host_id != self.host_id():
            raise ChatGPTAuthenticationError("ChatGPT session host does not match")
        if self.registration() != session.client_id:
            raise ChatGPTAuthenticationError(
                "ChatGPT session registration does not match"
            )
        self._write("session.json", session.to_dict())

    def clear_session(self) -> None:
        self._ensure_directory()
        try:
            (self.directory / "session.json").unlink(missing_ok=True)
        except OSError as exc:
            raise ChatGPTAuthenticationError(
                "ChatGPT session could not be cleared"
            ) from exc

    @contextlib.contextmanager
    def refresh_lock(self) -> Iterator[None]:
        self._ensure_directory()
        path = self.directory / ".refresh.lock"
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags, 0o600)
        except OSError as exc:
            raise ChatGPTAuthenticationError("refresh lock is unavailable") from exc
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                raise ChatGPTAuthenticationError("refresh lock is unsafe")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


@dataclass(frozen=True, repr=False)
class AuthorizationAttempt:
    authorization_url: str
    state: str
    nonce: str
    code_verifier: str
    redirect_uri: str
    requested_client_id: str
    new_registration: bool


@dataclass(frozen=True)
class ChatGPTModel:
    slug: str
    display_name: str


IDTokenValidator = Callable[[str, str, str | None], Mapping[str, Any]]


class ChatGPTOAuth:
    """Own dynamic registration, token renewal, and account model discovery."""

    def __init__(
        self,
        store: ChatGPTProfileStore,
        *,
        http_client: HTTPClient | None = None,
        id_token_validator: IDTokenValidator | None = None,
    ):
        self.store = store
        self.http = http_client or BoundedHTTPSClient()
        self.id_token_validator = id_token_validator or self._validate_id_token

    @staticmethod
    def _redirect_uri(value: str) -> str:
        parsed = urlsplit(value)
        try:
            port = parsed.port
        except ValueError as exc:
            raise ChatGPTAuthenticationError("redirect URI is invalid") from exc
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or port is None
            or parsed.path != "/auth/callback"
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ChatGPTAuthenticationError("redirect URI is invalid")
        return value

    def begin(self, redirect_uri: str) -> AuthorizationAttempt:
        redirect_uri = self._redirect_uri(redirect_uri)
        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(32)
        verifier = secrets.token_urlsafe(64)
        challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        registration = self.store.registration()
        new_registration = registration is None
        requested_client_id = registration or "dynamic_agent_client"
        parameters = {
            "client_id": requested_client_id,
            "ext_agent_host_id": self.store.host_id(),
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": " ".join(CHATGPT_SCOPES),
            "resource": CHATGPT_RESOURCE,
            "state": state,
            "nonce": nonce,
            "code_challenge_method": "S256",
            "code_challenge": challenge,
        }
        if new_registration:
            parameters["agent_name_hint"] = "Magic Glass Box"
        else:
            session = self.store.session()
            if session is not None and session.email is not None:
                parameters["login_hint"] = session.email
        return AuthorizationAttempt(
            authorization_url=CHATGPT_AUTHORIZE_URL + "?" + urlencode(parameters),
            state=state,
            nonce=nonce,
            code_verifier=verifier,
            redirect_uri=redirect_uri,
            requested_client_id=requested_client_id,
            new_registration=new_registration,
        )

    @staticmethod
    def _callback_value(callback: Mapping[str, str], name: str) -> str | None:
        value = callback.get(name)
        if value is None:
            return None
        if not isinstance(value, str) or not value or len(value.encode()) > 8192:
            raise ChatGPTAuthenticationError("OAuth callback is invalid")
        return value

    def complete(
        self,
        attempt: AuthorizationAttempt,
        callback: Mapping[str, str],
    ) -> ChatGPTSession:
        if not isinstance(attempt, AuthorizationAttempt) or not isinstance(
            callback, Mapping
        ):
            raise ChatGPTAuthenticationError("OAuth callback is invalid")
        state = self._callback_value(callback, "state")
        if state is None or not hmac.compare_digest(state, attempt.state):
            raise ChatGPTAuthenticationError("OAuth callback state is invalid")
        error = self._callback_value(callback, "error")
        if error is not None:
            raise ChatGPTAuthenticationError("ChatGPT authorization was denied")
        code = self._callback_value(callback, "code")
        if code is None:
            raise ChatGPTAuthenticationError("OAuth callback code is missing")
        callback_client = self._callback_value(callback, "client_id")
        if attempt.new_registration:
            if callback_client is None:
                raise ChatGPTAuthenticationError("OAuth callback client_id is missing")
            client_id = _client_id(callback_client)
        else:
            client_id = _client_id(attempt.requested_client_id)
            if callback_client is not None and not hmac.compare_digest(
                callback_client, client_id
            ):
                raise ChatGPTAuthenticationError("OAuth callback client_id changed")
        self.store.save_registration(client_id)
        body = urlencode(
            {
                "grant_type": "authorization_code",
                "client_id": client_id,
                "code": code,
                "code_verifier": attempt.code_verifier,
                "redirect_uri": attempt.redirect_uri,
                "resource": CHATGPT_RESOURCE,
            }
        ).encode("ascii")
        try:
            response = self.http.request(
                "POST",
                CHATGPT_TOKEN_URL,
                headers={
                    "accept": "application/json",
                    "content-type": "application/x-www-form-urlencoded",
                    "content-length": str(len(body)),
                    "user-agent": f"magic-glass-box-openai-connector/{__version__}",
                },
                body=body,
                timeout=30,
                max_response_bytes=MAX_AUTH_RESPONSE_BYTES,
            )
        except HTTPTransportError as exc:
            raise ChatGPTAuthenticationError(
                "authorization code exchange failed"
            ) from exc
        if not 200 <= response.status_code < 300:
            raise ChatGPTAuthenticationError("authorization code exchange failed")
        try:
            token = _strict_json_object(response.body, subject="token response")
        except ChatGPTConnectorError as exc:
            raise ChatGPTAuthenticationError(str(exc)) from exc
        session = self._session_from_token(
            token,
            client_id=client_id,
            host_id=self.store.host_id(),
            nonce=attempt.nonce,
            previous=self.store.session(),
        )
        if not session.sharing:
            raise ChatGPTAuthenticationError("ChatGPT plan usage was not granted")
        self.store.save_session(session)
        return session

    def _session_from_token(
        self,
        token: Mapping[str, Any],
        *,
        client_id: str,
        host_id: str,
        nonce: str | None,
        previous: ChatGPTSession | None,
    ) -> ChatGPTSession:
        access_token = _authentication_text(
            token.get("access_token"), field="access token", maximum=262144
        )
        id_token_value = token.get("id_token")
        id_token = (
            previous.id_token
            if id_token_value is None and previous is not None
            else _authentication_text(id_token_value, field="ID token", maximum=262144)
        )
        refresh_value = token.get("refresh_token")
        refresh_token = (
            previous.refresh_token
            if refresh_value is None and previous is not None
            else _authentication_text(
                refresh_value, field="refresh token", maximum=262144
            )
        )
        token_type = token.get("token_type")
        if not isinstance(token_type, str) or token_type.lower() != "bearer":
            raise ChatGPTAuthenticationError("token response type is invalid")
        expires_in = token.get("expires_in")
        if (
            isinstance(expires_in, bool)
            or not isinstance(expires_in, int)
            or not 1 <= expires_in <= 7 * 24 * 60 * 60
        ):
            raise ChatGPTAuthenticationError("token response expiry is invalid")
        raw_scope = token.get("scope")
        if raw_scope is None and previous is not None:
            scopes = previous.scopes
        elif isinstance(raw_scope, str):
            scopes = tuple(sorted(raw_scope.split()))
        else:
            raise ChatGPTAuthenticationError("token response scopes are invalid")
        if not scopes or len(set(scopes)) != len(scopes):
            raise ChatGPTAuthenticationError("token response scopes are invalid")
        try:
            claims = self.id_token_validator(id_token, client_id, nonce)
        except ChatGPTAuthenticationError:
            raise
        except Exception as exc:
            raise ChatGPTAuthenticationError("ID token validation failed") from exc
        issuer = claims.get("iss")
        subject = claims.get("sub")
        email = claims.get("email")
        if issuer != CHATGPT_ISSUER or not isinstance(subject, str) or not subject:
            raise ChatGPTAuthenticationError("ID token identity is invalid")
        if email is not None and not isinstance(email, str):
            raise ChatGPTAuthenticationError("ID token identity is invalid")
        if previous is not None and (
            previous.client_id != client_id
            or previous.issuer != issuer
            or previous.subject != subject
        ):
            raise ChatGPTAuthenticationError("ChatGPT account identity changed")
        return ChatGPTSession.from_mapping(
            {
                "schema_version": 1,
                "client_id": client_id,
                "ext_agent_host_id": host_id,
                "issuer": issuer,
                "subject": subject,
                "email": email,
                "id_token": id_token,
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "Bearer",
                "expires_at": (
                    datetime.now(UTC) + timedelta(seconds=expires_in)
                ).isoformat(),
                "scopes": list(scopes),
            }
        )

    def session(self) -> ChatGPTSession:
        session = self.store.session()
        if session is None or not session.sharing:
            raise ChatGPTAuthenticationError("ChatGPT sign-in is required")
        return session

    @staticmethod
    def _oauth_error(response: HTTPResponse) -> str | None:
        try:
            value = _strict_json_object(response.body, subject="OAuth error")
        except ChatGPTConnectorError:
            return None
        error = value.get("error")
        return error if isinstance(error, str) else None

    def access_token(self) -> str:
        with self.store.refresh_lock():
            session = self.session()
            if session.is_fresh():
                return session.access_token
            body = urlencode(
                {
                    "grant_type": "refresh_token",
                    "client_id": session.client_id,
                    "refresh_token": session.refresh_token,
                    "resource": CHATGPT_RESOURCE,
                }
            ).encode("ascii")
            try:
                response = self.http.request(
                    "POST",
                    CHATGPT_TOKEN_URL,
                    headers={
                        "accept": "application/json",
                        "content-type": "application/x-www-form-urlencoded",
                        "content-length": str(len(body)),
                        "user-agent": (
                            f"magic-glass-box-openai-connector/{__version__}"
                        ),
                    },
                    body=body,
                    timeout=30,
                    max_response_bytes=MAX_AUTH_RESPONSE_BYTES,
                )
            except HTTPTransportError as exc:
                raise ChatGPTAuthenticationError(
                    "ChatGPT token renewal is temporarily unavailable"
                ) from exc
            if not 200 <= response.status_code < 300:
                error = self._oauth_error(response)
                terminal = {
                    "invalid_grant",
                    "invalid_refresh_token",
                    "token_expired",
                    "refresh_token_expired",
                    "refresh_token_invalidated",
                    "refresh_token_reused",
                }
                if error in terminal:
                    self.store.clear_session()
                    raise ChatGPTAuthenticationError(
                        "ChatGPT session expired; sign in again"
                    )
                if response.status_code >= 500:
                    raise ChatGPTAuthenticationError(
                        "ChatGPT token renewal is temporarily unavailable"
                    )
                raise ChatGPTAuthenticationError("ChatGPT token renewal failed")
            try:
                token = _strict_json_object(response.body, subject="token response")
            except ChatGPTConnectorError as exc:
                raise ChatGPTAuthenticationError(str(exc)) from exc
            refreshed = self._session_from_token(
                token,
                client_id=session.client_id,
                host_id=session.ext_agent_host_id,
                nonce=None,
                previous=session,
            )
            if not refreshed.sharing:
                self.store.clear_session()
                raise ChatGPTAuthenticationError(
                    "ChatGPT plan usage is no longer authorized; sign in again"
                )
            self.store.save_session(refreshed)
            return refreshed.access_token

    def list_models(self) -> tuple[ChatGPTModel, ...]:
        access_token = self.access_token()
        try:
            response = self.http.request(
                "GET",
                CHATGPT_MODELS_URL,
                headers={
                    "accept": "application/json",
                    "authorization": "Bearer " + access_token,
                    "user-agent": f"magic-glass-box-openai-connector/{__version__}",
                },
                body=None,
                timeout=30,
                max_response_bytes=MAX_AUTH_RESPONSE_BYTES,
            )
        except HTTPTransportError as exc:
            raise ChatGPTAuthenticationError(
                "ChatGPT model discovery is unavailable"
            ) from exc
        if not 200 <= response.status_code < 300:
            raise ChatGPTAuthenticationError("ChatGPT model discovery failed")
        try:
            value = _strict_json_object(response.body, subject="model response")
        except ChatGPTConnectorError as exc:
            raise ChatGPTAuthenticationError(str(exc)) from exc
        raw_models = value.get("models")
        if not isinstance(raw_models, list) or len(raw_models) > 1024:
            raise ChatGPTAuthenticationError("model response is invalid")
        models: list[ChatGPTModel] = []
        for item in raw_models:
            if not isinstance(item, dict) or item.get("visibility") != "list":
                continue
            try:
                slug = _text(item.get("slug"), field="model slug", maximum=256)
                display_name = _text(
                    item.get("display_name"),
                    field="model display name",
                    maximum=512,
                )
            except ChatGPTConnectorError as exc:
                raise ChatGPTAuthenticationError(str(exc)) from exc
            models.append(ChatGPTModel(slug, display_name))
        return tuple(models)

    def _validate_id_token(
        self,
        token: str,
        client_id: str,
        nonce: str | None,
    ) -> Mapping[str, Any]:
        try:
            import jwt
        except ImportError as exc:
            raise ChatGPTAuthenticationError(
                "install the openai-connector extra for ID token validation"
            ) from exc
        try:
            response = self.http.request(
                "GET",
                CHATGPT_JWKS_URL,
                headers={
                    "accept": "application/json",
                    "user-agent": f"magic-glass-box-openai-connector/{__version__}",
                },
                body=None,
                timeout=30,
                max_response_bytes=MAX_AUTH_RESPONSE_BYTES,
            )
            if not 200 <= response.status_code < 300:
                raise ChatGPTAuthenticationError("ID token validation failed")
            jwks = _strict_json_object(response.body, subject="JWKS response")
            header = jwt.get_unverified_header(token)
            if header.get("alg") != "RS256" or not isinstance(header.get("kid"), str):
                raise ChatGPTAuthenticationError("ID token validation failed")
            keys = jwks.get("keys")
            if not isinstance(keys, list):
                raise ChatGPTAuthenticationError("ID token validation failed")
            selected = next(
                (
                    key
                    for key in keys
                    if isinstance(key, dict) and key.get("kid") == header["kid"]
                ),
                None,
            )
            if selected is None:
                raise ChatGPTAuthenticationError("ID token validation failed")
            signing_key = jwt.PyJWK.from_dict(selected).key
            # Construct a decoder beside the selected key instead of using
            # PyJWT's module-global instance.  Besides avoiding shared mutable
            # decoder state, this keeps the cryptography algorithm classes
            # coherent in instrumented/reloaded processes such as Mutmut.
            claims = jwt.PyJWT().decode(
                token,
                signing_key,
                algorithms=["RS256"],
                audience=client_id,
                issuer=CHATGPT_ISSUER,
                options={"require": ["aud", "exp", "iss", "sub"]},
            )
        except ChatGPTAuthenticationError:
            raise
        except Exception as exc:
            raise ChatGPTAuthenticationError("ID token validation failed") from exc
        if not isinstance(claims, dict):
            raise ChatGPTAuthenticationError("ID token validation failed")
        if nonce is not None and (
            not isinstance(claims.get("nonce"), str)
            or not hmac.compare_digest(claims["nonce"], nonce)
        ):
            raise ChatGPTAuthenticationError("ID token nonce is invalid")
        return claims


@dataclass(frozen=True)
class OpenAIResponseResult:
    status_code: int
    request_bytes: int
    response_bytes: int
    untrusted_response: dict[str, Any]


def _safe_error_code(value: object, *, fallback: str) -> str:
    try:
        return validate_provider_name(value, field="provider error code")
    except ValidationError:
        return fallback


def _http_error_code(raw: bytes) -> str:
    try:
        value = _strict_json_object(raw, subject="provider error")
    except ChatGPTConnectorError:
        return "upstream_error"
    error = value.get("error")
    if isinstance(error, dict):
        return _safe_error_code(error.get("code"), fallback="upstream_error")
    return _safe_error_code(error, fallback="upstream_error")


def _sse_terminal(raw: bytes) -> tuple[str, str | None]:
    try:
        text = raw.decode("utf-8", "strict").replace("\r\n", "\n")
    except UnicodeDecodeError as exc:
        raise ChatGPTResponseAmbiguousError(
            "provider invocation outcome is ambiguous",
            error_code="upstream_ambiguous",
        ) from exc
    terminals: list[tuple[str, str | None]] = []
    saw_json_event = False
    for frame in text.split("\n\n"):
        data_lines = [
            line.removeprefix("data:").lstrip()
            for line in frame.splitlines()
            if line.startswith("data:")
        ]
        if not data_lines:
            continue
        payload = "\n".join(data_lines)
        if payload == "[DONE]":
            continue
        try:
            event = _strict_json_object(
                payload.encode("utf-8"), subject="provider stream event"
            )
        except ChatGPTConnectorError as exc:
            raise ChatGPTResponseAmbiguousError(
                "provider invocation outcome is ambiguous",
                error_code="upstream_ambiguous",
            ) from exc
        saw_json_event = True
        event_type = event.get("type")
        if event_type == "response.completed":
            response = event.get("response")
            if not isinstance(response, dict) or response.get("status") != "completed":
                raise ChatGPTResponseAmbiguousError(
                    "provider invocation outcome is ambiguous",
                    error_code="upstream_ambiguous",
                )
            terminals.append(("completed", None))
        elif event_type == "response.failed":
            response = event.get("response")
            error = response.get("error") if isinstance(response, dict) else None
            code = error.get("code") if isinstance(error, dict) else None
            terminals.append(
                ("failed", _safe_error_code(code, fallback="response_failed"))
            )
        elif event_type == "response.incomplete":
            terminals.append(("failed", "response_incomplete"))
        elif event_type == "error":
            code = event.get("code")
            if code is None:
                error = event.get("error")
                code = error.get("code") if isinstance(error, dict) else None
            terminals.append(
                ("failed", _safe_error_code(code, fallback="response_failed"))
            )
    if not saw_json_event or len(terminals) != 1:
        raise ChatGPTResponseAmbiguousError(
            "provider invocation outcome is ambiguous",
            error_code="upstream_ambiguous",
        )
    return terminals[0]


class OpenAIResponsesClient:
    """Send one ChatGPT-plan-compatible streaming Responses request."""

    ALLOWED_HEADERS = frozenset({"openai-beta", "x-codex-beta-features"})
    ALLOWED_FIELDS = frozenset(
        {
            "client_metadata",
            "include",
            "input",
            "instructions",
            "parallel_tool_calls",
            "prompt_cache_key",
            "reasoning",
            "store",
            "stream",
            "text",
            "tool_choice",
            "tools",
        }
    )
    FUNCTION_TOOL_FIELDS = frozenset(
        {"description", "name", "parameters", "strict", "type"}
    )
    NAMESPACE_TOOL_FIELDS = frozenset({"description", "name", "tools", "type"})

    def __init__(self, http_client: HTTPClient | None = None):
        self.http = http_client or BoundedHTTPSClient()

    @classmethod
    def _body_and_headers(
        cls,
        request: dict[str, Any],
        *,
        model: str,
        request_limit_bytes: int,
    ) -> tuple[bytes, dict[str, str]]:
        if not isinstance(request, dict) or set(request) != {
            "protocol",
            "body",
            "headers",
        }:
            raise ChatGPTResponseFailure(
                "provider protocol request has an invalid shape",
                error_code="invalid_request",
            )
        if request["protocol"] != "openai-responses":
            raise ChatGPTResponseFailure(
                "provider protocol is invalid", error_code="protocol_mismatch"
            )
        provider_body = request["body"]
        if not isinstance(provider_body, dict):
            raise ChatGPTResponseFailure(
                "provider request body is invalid", error_code="invalid_request"
            )
        if "model" in provider_body:
            raise ChatGPTResponseFailure(
                "provider request cannot select a model",
                error_code="model_override",
            )
        if provider_body.get("store") is not False:
            raise ChatGPTResponseFailure(
                "provider request must set store to false",
                error_code="invalid_request",
            )
        if provider_body.get("stream") is not True:
            raise ChatGPTResponseFailure(
                "provider request must use streaming",
                error_code="invalid_request",
            )
        inputs = provider_body.get("input")
        if not isinstance(inputs, list):
            raise ChatGPTResponseFailure(
                "provider request input must be an array",
                error_code="invalid_request",
            )
        if not set(provider_body).issubset(cls.ALLOWED_FIELDS):
            raise ChatGPTResponseFailure(
                "provider request contains an unsupported field",
                error_code="unsupported_capability",
            )
        for item in inputs:
            if (
                isinstance(item, dict)
                and item.get("type") == "message"
                and item.get("role") == "system"
            ):
                raise ChatGPTResponseFailure(
                    "provider request contains an unsupported system message",
                    error_code="unsupported_capability",
                )
        tools = provider_body.get("tools", [])
        if not isinstance(tools, list) or len(tools) > 256:
            raise ChatGPTResponseFailure(
                "provider request tools are invalid", error_code="invalid_request"
            )
        local_tools_only = True
        for tool in tools:
            if not isinstance(tool, dict):
                local_tools_only = False
                break
            tool_type = tool.get("type")
            if tool_type == "function":
                if set(tool) != cls.FUNCTION_TOOL_FIELDS:
                    local_tools_only = False
                    break
                continue
            if tool_type == "namespace":
                nested = tool.get("tools")
                if (
                    set(tool) != cls.NAMESPACE_TOOL_FIELDS
                    or not isinstance(nested, list)
                    or not 1 <= len(nested) <= 128
                    or any(
                        not isinstance(item, dict)
                        or item.get("type") != "function"
                        or set(item) != cls.FUNCTION_TOOL_FIELDS
                        for item in nested
                    )
                ):
                    local_tools_only = False
                    break
                continue
            local_tools_only = False
            break
        if not local_tools_only:
            raise ChatGPTResponseFailure(
                "provider request contains an unsupported tool",
                error_code="unsupported_capability",
            )
        raw_headers = request["headers"]
        if not isinstance(raw_headers, dict) or len(raw_headers) > len(
            cls.ALLOWED_HEADERS
        ):
            raise ChatGPTResponseFailure(
                "provider protocol header is invalid",
                error_code="invalid_request",
            )
        forwarded: dict[str, str] = {}
        for raw_name, raw_value in raw_headers.items():
            if not isinstance(raw_name, str):
                raise ChatGPTResponseFailure(
                    "provider protocol header is invalid",
                    error_code="invalid_request",
                )
            name = raw_name.lower()
            if (
                name not in cls.ALLOWED_HEADERS
                or name in forwarded
                or not isinstance(raw_value, str)
                or not raw_value
                or not raw_value.isascii()
                or len(raw_value) > 8192
                or any(not 0x20 <= ord(character) <= 0x7E for character in raw_value)
            ):
                raise ChatGPTResponseFailure(
                    "provider protocol header is invalid",
                    error_code="invalid_request",
                )
            forwarded[name] = raw_value
        try:
            body = canonical_json({**provider_body, "model": model}).encode("utf-8")
        except ValidationError as exc:
            raise ChatGPTResponseFailure(
                "provider request JSON is invalid", error_code="invalid_request"
            ) from exc
        if not body or len(body) > request_limit_bytes:
            raise ChatGPTResponseFailure(
                "provider request exceeds its size limit",
                error_code="request_too_large",
            )
        return body, forwarded

    def invoke(
        self,
        *,
        access_token: str,
        model: str,
        request: dict[str, Any],
        request_limit_bytes: int,
        response_limit_bytes: int,
        deadline: datetime,
    ) -> OpenAIResponseResult:
        if not isinstance(access_token, str) or not access_token:
            raise ChatGPTResponseFailure(
                "provider authentication is unavailable",
                error_code="authentication_required",
            )
        model = _text(model, field="provider model", maximum=256)
        body, forwarded = self._body_and_headers(
            request,
            model=model,
            request_limit_bytes=request_limit_bytes,
        )
        remaining = (deadline.astimezone(UTC) - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise ChatGPTResponseFailure(
                "provider request deadline has expired",
                error_code="deadline_expired",
                request_bytes=len(body),
            )
        headers = {
            "accept": "text/event-stream",
            "accept-encoding": "identity",
            "authorization": "Bearer " + access_token,
            "content-type": "application/json",
            "content-length": str(len(body)),
            "user-agent": f"magic-glass-box-openai-connector/{__version__}",
            **forwarded,
        }
        try:
            response = self.http.request(
                "POST",
                CHATGPT_RESPONSES_URL,
                headers=headers,
                body=body,
                timeout=remaining,
                max_response_bytes=response_limit_bytes,
            )
        except HTTPTransportError as exc:
            if exc.ambiguous:
                raise ChatGPTResponseAmbiguousError(
                    "provider invocation outcome is ambiguous",
                    error_code="upstream_ambiguous",
                    request_bytes=len(body),
                ) from exc
            raise ChatGPTResponseFailure(
                "provider is unavailable",
                error_code="upstream_unavailable",
                request_bytes=len(body),
            ) from exc
        if not 200 <= response.status_code < 300:
            raise ChatGPTResponseFailure(
                "provider request failed",
                error_code=_http_error_code(response.body),
                status_code=response.status_code,
                request_bytes=len(body),
                response_bytes=len(response.body),
            )
        content_type = response.content_type
        if content_type is not None and (
            not isinstance(content_type, str)
            or content_type.split(";", 1)[0].strip().lower() != "text/event-stream"
        ):
            raise ChatGPTResponseAmbiguousError(
                "provider invocation outcome is ambiguous",
                error_code="upstream_ambiguous",
                status_code=response.status_code,
                request_bytes=len(body),
                response_bytes=len(response.body),
            )
        try:
            outcome, error_code = _sse_terminal(response.body)
        except ChatGPTResponseAmbiguousError as exc:
            raise ChatGPTResponseAmbiguousError(
                "provider invocation outcome is ambiguous",
                error_code="upstream_ambiguous",
                status_code=response.status_code,
                request_bytes=len(body),
                response_bytes=len(response.body),
            ) from exc
        if outcome == "failed":
            raise ChatGPTResponseFailure(
                "provider response failed",
                error_code=error_code or "response_failed",
                status_code=response.status_code,
                request_bytes=len(body),
                response_bytes=len(response.body),
            )
        try:
            response_text = response.body.decode("utf-8", "strict")
            untrusted = {
                "body": response_text,
                "content_type": content_type or "text/event-stream",
                "protocol": "openai-responses",
            }
            encoded_response = canonical_json(untrusted).encode("utf-8")
        except (UnicodeDecodeError, ValidationError) as exc:
            raise ChatGPTResponseAmbiguousError(
                "provider invocation outcome is ambiguous",
                error_code="upstream_ambiguous",
                status_code=response.status_code,
                request_bytes=len(body),
                response_bytes=len(response.body),
            ) from exc
        if len(encoded_response) > response_limit_bytes:
            raise ChatGPTResponseAmbiguousError(
                "provider invocation outcome is ambiguous",
                error_code="upstream_ambiguous",
                status_code=response.status_code,
                request_bytes=len(body),
                response_bytes=len(response.body),
            )
        return OpenAIResponseResult(
            status_code=response.status_code,
            request_bytes=len(body),
            response_bytes=len(response.body),
            untrusted_response=untrusted,
        )


@dataclass(frozen=True)
class OpenAIConnectorRoute:
    name: str
    credential_scope_sha256: str
    models: tuple[str, ...]
    request_bytes: int
    response_bytes: int
    timeout_ms: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> OpenAIConnectorRoute:
        if not isinstance(value, dict):
            raise ChatGPTConnectorError("connector route is invalid")
        _exact_fields(
            value,
            expected={
                "name",
                "credential_scope_sha256",
                "models",
                "request_bytes",
                "response_bytes",
                "timeout_ms",
            },
            subject="connector route",
        )
        try:
            name = validate_provider_name(value["name"], field="connector route")
        except ValidationError as exc:
            raise ChatGPTConnectorError(str(exc)) from exc
        scope = value["credential_scope_sha256"]
        if not isinstance(scope, str) or SHA256_RE.fullmatch(scope) is None:
            raise ChatGPTConnectorError("connector route credential scope is invalid")
        raw_models = value["models"]
        if not isinstance(raw_models, list) or not 1 <= len(raw_models) <= 64:
            raise ChatGPTConnectorError("connector route models are invalid")
        models = tuple(
            _text(model, field="connector route model", maximum=256)
            for model in raw_models
        )
        if len(set(models)) != len(models):
            raise ChatGPTConnectorError("connector route models are invalid")

        def bounded(
            field: str,
            *,
            minimum: int,
            maximum: int,
        ) -> int:
            item = value[field]
            if (
                isinstance(item, bool)
                or not isinstance(item, int)
                or not minimum <= item <= maximum
            ):
                raise ChatGPTConnectorError(f"connector route {field} is invalid")
            return item

        return cls(
            name=name,
            credential_scope_sha256=scope,
            models=models,
            request_bytes=bounded("request_bytes", minimum=1, maximum=2 * 1024 * 1024),
            response_bytes=bounded(
                "response_bytes", minimum=1, maximum=4 * 1024 * 1024
            ),
            timeout_ms=bounded("timeout_ms", minimum=100, maximum=120_000),
        )


@dataclass(frozen=True)
class OpenAIConnectorConfig:
    schema_version: int
    connector_id: str
    deployment_sha256: str
    routes: tuple[OpenAIConnectorRoute, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> OpenAIConnectorConfig:
        if not isinstance(value, dict):
            raise ChatGPTConnectorError("connector configuration is invalid")
        _exact_fields(
            value,
            expected={
                "schema_version",
                "connector_id",
                "deployment_sha256",
                "routes",
            },
            subject="connector configuration",
        )
        if value["schema_version"] != 1:
            raise ChatGPTConnectorError("connector configuration version is invalid")
        connector_id = value["connector_id"]
        if (
            not isinstance(connector_id, str)
            or CONNECTOR_ID_RE.fullmatch(connector_id) is None
        ):
            raise ChatGPTConnectorError("connector configuration identity is invalid")
        deployment = value["deployment_sha256"]
        if not isinstance(deployment, str) or SHA256_RE.fullmatch(deployment) is None:
            raise ChatGPTConnectorError("connector configuration deployment is invalid")
        raw_routes = value["routes"]
        if not isinstance(raw_routes, list) or not 1 <= len(raw_routes) <= 32:
            raise ChatGPTConnectorError("connector configuration routes are invalid")
        routes = tuple(OpenAIConnectorRoute.from_mapping(item) for item in raw_routes)
        if len({route.name for route in routes}) != len(routes):
            raise ChatGPTConnectorError("connector route names must be unique")
        return cls(1, connector_id, deployment, routes)

    def route(self, name: str) -> OpenAIConnectorRoute | None:
        return next((route for route in self.routes if route.name == name), None)


class ChatGPTCredentials(Protocol):
    def session(self) -> ChatGPTSession: ...

    def access_token(self) -> str: ...


def _response_authority(request: InferenceInvocationRequest) -> dict[str, Any]:
    value = request.to_dict()
    value.pop("request")
    return value


class OpenAIChatGPTConnector:
    """Bind MGB inference authority to one connector-owned ChatGPT session."""

    def __init__(
        self,
        config: OpenAIConnectorConfig,
        *,
        credentials: ChatGPTCredentials,
        responses: OpenAIResponsesClient,
    ):
        self.config = config
        self.credentials = credentials
        self.responses = responses

    @staticmethod
    def _failed(
        request: InferenceInvocationRequest,
        *,
        error_code: str,
        status_code: int | None = None,
        request_bytes: int = 0,
        response_bytes: int = 0,
        outcome: str = "failed",
    ) -> InferenceInvocationResponse:
        return InferenceInvocationResponse.from_mapping(
            {
                **_response_authority(request),
                "outcome": outcome,
                "provider_status": status_code,
                "request_bytes": min(request_bytes, request.request_limit_bytes),
                "response_bytes": min(response_bytes, request.response_limit_bytes),
                "error_code": _safe_error_code(error_code, fallback="connector_error"),
                "response": None,
            }
        )

    def _authority_error(
        self,
        request: InferenceInvocationRequest,
    ) -> str | None:
        if request.connector_id != self.config.connector_id:
            return "connector_mismatch"
        if request.deployment_sha256 != self.config.deployment_sha256:
            return "deployment_mismatch"
        route = self.config.route(request.route)
        if route is None:
            return "route_mismatch"
        if request.credential_scope_sha256 != route.credential_scope_sha256:
            return "credential_scope_mismatch"
        if request.provider != "openai":
            return "provider_mismatch"
        if request.protocol != "openai-responses":
            return "protocol_mismatch"
        if request.model not in route.models:
            return "model_mismatch"
        if request.request_limit_bytes > route.request_bytes:
            return "request_limit_exceeded"
        if request.response_limit_bytes > route.response_bytes:
            return "response_limit_exceeded"
        deadline = datetime.fromisoformat(request.deadline)
        remaining = (deadline - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            return "deadline_expired"
        if remaining > route.timeout_ms / 1000 + 1:
            return "deadline_exceeds_route"
        return None

    def invoke(
        self,
        request: InferenceInvocationRequest,
    ) -> InferenceInvocationResponse:
        if not isinstance(request, InferenceInvocationRequest):
            raise ChatGPTConnectorError("request must be an InferenceInvocationRequest")
        authority_error = self._authority_error(request)
        if authority_error is not None:
            return self._failed(request, error_code=authority_error)
        try:
            session = self.credentials.session()
            if (
                not session.sharing
                or session.credential_scope_sha256 != request.credential_scope_sha256
            ):
                return self._failed(request, error_code="credential_scope_mismatch")
            access_token = self.credentials.access_token()
        except ChatGPTAuthenticationError:
            return self._failed(request, error_code="authentication_required")
        try:
            result = self.responses.invoke(
                access_token=access_token,
                model=request.model,
                request=request.request,
                request_limit_bytes=request.request_limit_bytes,
                response_limit_bytes=request.response_limit_bytes,
                deadline=datetime.fromisoformat(request.deadline),
            )
        except ChatGPTResponseAmbiguousError as exc:
            return self._failed(
                request,
                error_code=exc.error_code,
                status_code=exc.status_code,
                request_bytes=exc.request_bytes,
                response_bytes=exc.response_bytes,
                outcome="ambiguous",
            )
        except ChatGPTResponseFailure as exc:
            return self._failed(
                request,
                error_code=exc.error_code,
                status_code=exc.status_code,
                request_bytes=exc.request_bytes,
                response_bytes=exc.response_bytes,
            )
        return InferenceInvocationResponse.from_mapping(
            {
                **_response_authority(request),
                "outcome": "succeeded",
                "provider_status": result.status_code,
                "request_bytes": result.request_bytes,
                "response_bytes": result.response_bytes,
                "error_code": None,
                "response": result.untrusted_response,
            }
        )
