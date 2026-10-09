from __future__ import annotations

import http.client
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

from . import __version__
from .domain import validate_krab_id, validate_project_key

DEFAULT_KRAB_PORT = 8765
MAX_RESPONSE_BYTES = 1024 * 1024
KRAB_HANDLE_RE = re.compile(r"^[a-z][a-z0-9_-]{2,31}$")
ITEM_STATUS = {
    "open.pending",
    "open.in_progress",
    "open.under_review",
    "open.needs_changes",
    "open.approved",
    "closed.approved",
    "closed.rejected",
}
EVENT_KINDS = {"item.created", "status.changed", "item.reminded"}


class KrabError(RuntimeError):
    """The Krab API was unavailable or returned an invalid response."""


class KrabHTTPError(KrabError):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"Krab HTTP {status} ({code}): {message}")
        self.status = status
        self.code = code
        self.message = message


@dataclass(frozen=True)
class KrabIdentity:
    user_id: str
    handle: str
    token_version: int


@dataclass(frozen=True)
class KrabProject:
    project_id: str
    project_key: str
    role: str
    label: str
    content_risk_flags: tuple[str, ...]


@dataclass(frozen=True)
class KrabUserRef:
    user_id: str
    handle: str


@dataclass(frozen=True)
class WorkClaimRef:
    claim_id: str
    kind: str
    external_id: str


@dataclass(frozen=True)
class ActivityEvent:
    event_seq: int
    kind: str
    project_key: str
    session_id: str
    item_id: str
    actor: KrabUserRef
    creator: KrabUserRef
    recipient: KrabUserRef
    work_claim: WorkClaimRef | None
    from_status: str | None
    to_status: str | None
    current_status: str
    version: int
    created_at: str
    untrusted_title: str


@dataclass(frozen=True)
class ActivityPage:
    project_key: str
    after: int
    next_cursor: int
    events: tuple[ActivityEvent, ...]


ConnectionFactory = Callable[[str, int, float], http.client.HTTPConnection]


def validate_base_url(value: str) -> tuple[str, int, str]:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "localhost"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise KrabError(
            "Krab URL must be an HTTP loopback origin without credentials or a path"
        )
    try:
        port = parsed.port or DEFAULT_KRAB_PORT
    except ValueError as exc:
        raise KrabError("Krab URL contains an invalid port") from exc
    return parsed.hostname, port, f"http://{parsed.hostname}:{port}"


def _strict_json(raw: bytes) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise KrabError("Krab returned non-UTF-8 JSON") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise KrabError("Krab returned a duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise KrabError(f"Krab returned an invalid JSON number: {value}")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except KrabError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise KrabError("Krab returned malformed JSON") from exc
    if not isinstance(value, dict):
        raise KrabError("Krab response must be a JSON object")
    return value


def _exact_object(value: object, *, fields: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise KrabError(f"invalid {label} object")
    return value


def _integer(value: object, *, minimum: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise KrabError(f"invalid {label}")
    return value


def _text(value: object, *, label: str, max_bytes: int = 16_384) -> str:
    if (
        not isinstance(value, str)
        or "\x00" in value
        or len(value.encode("utf-8")) > max_bytes
    ):
        raise KrabError(f"invalid {label}")
    return value


def _timestamp(value: object, *, label: str) -> str:
    text = _text(value, label=label, max_bytes=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KrabError(f"invalid {label}") from exc
    if parsed.tzinfo is None:
        raise KrabError(f"invalid {label}")
    return text


def _handle(value: object) -> str:
    if not isinstance(value, str) or KRAB_HANDLE_RE.fullmatch(value) is None:
        raise KrabError("invalid Krab handle")
    return value


def _user_ref(value: object, label: str) -> KrabUserRef:
    item = _exact_object(value, fields={"user_id", "handle"}, label=label)
    try:
        user_id = validate_krab_id(item["user_id"], "usr")
    except ValueError as exc:
        raise KrabError(f"invalid {label} user ID") from exc
    return KrabUserRef(user_id=user_id, handle=_handle(item["handle"]))


class KrabClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 40.0,
        connection_factory: ConnectionFactory | None = None,
    ):
        self.host, self.port, self.base_url = validate_base_url(base_url)
        if not isinstance(token, str) or not token or len(token) > 256:
            raise KrabError("invalid Krab bearer token")
        self._token = token
        self.timeout = timeout
        self._connection_factory = connection_factory or self._new_connection

    @staticmethod
    def _new_connection(
        host: str, port: int, timeout: float
    ) -> http.client.HTTPConnection:
        return http.client.HTTPConnection(host, port, timeout=timeout)

    def _request(
        self, path: str, query: Mapping[str, int] | None = None
    ) -> dict[str, Any]:
        target = path
        if query:
            target += "?" + urlencode(query)
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token}",
            "User-Agent": f"magic-glass-box/{__version__}",
        }
        connection = self._connection_factory(self.host, self.port, self.timeout)
        try:
            connection.request("GET", target, headers=headers)
            response = connection.getresponse()
            declared = response.getheader("Content-Length")
            if declared is not None:
                try:
                    declared_size = int(declared)
                except ValueError as exc:
                    raise KrabError("Krab returned invalid Content-Length") from exc
                if declared_size < 0 or declared_size > MAX_RESPONSE_BYTES:
                    raise KrabError("Krab response exceeds the size limit")
            raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                raise KrabError("Krab response exceeds the size limit")
            status = response.status
        except KrabError:
            raise
        except (OSError, TimeoutError, http.client.HTTPException) as exc:
            raise KrabError(f"Krab is unavailable: {exc}") from None
        finally:
            connection.close()
        payload = _strict_json(raw)
        if status >= 400:
            error = payload.get("error")
            if isinstance(error, dict):
                code = error.get("code", "http_error")
                message = error.get("message", f"HTTP {status}")
            else:
                code, message = "http_error", f"HTTP {status}"
            raise KrabHTTPError(status, str(code), str(message))
        if not 200 <= status < 300:
            raise KrabError(f"Krab returned unexpected HTTP status {status}")
        return payload

    def whoami(self) -> KrabIdentity:
        payload = _exact_object(
            self._request("/v1/whoami"),
            fields={"trusted_metadata"},
            label="whoami response",
        )
        metadata = _exact_object(
            payload["trusted_metadata"],
            fields={"user_id", "handle", "token_version"},
            label="whoami metadata",
        )
        try:
            user_id = validate_krab_id(metadata["user_id"], "usr")
        except ValueError as exc:
            raise KrabError("invalid whoami user ID") from exc
        return KrabIdentity(
            user_id=user_id,
            handle=_handle(metadata["handle"]),
            token_version=_integer(
                metadata["token_version"], minimum=1, label="token version"
            ),
        )

    def projects(self) -> tuple[KrabProject, ...]:
        payload = _exact_object(
            self._request("/v1/projects"),
            fields={"projects"},
            label="project-list response",
        )
        entries = payload["projects"]
        if not isinstance(entries, list) or len(entries) > 10_000:
            raise KrabError("invalid project list")
        projects: list[KrabProject] = []
        for entry in entries:
            item = _exact_object(
                entry,
                fields={"trusted_metadata", "untrusted_text", "content_risk_flags"},
                label="project",
            )
            metadata = _exact_object(
                item["trusted_metadata"],
                fields={"project_id", "project_key", "role"},
                label="project metadata",
            )
            untrusted = _exact_object(
                item["untrusted_text"], fields={"label"}, label="project text"
            )
            flags = item["content_risk_flags"]
            if (
                not isinstance(flags, list)
                or len(flags) > 64
                or any(not isinstance(flag, str) for flag in flags)
                or len(set(flags)) != len(flags)
            ):
                raise KrabError("invalid project risk flags")
            try:
                project_id = validate_krab_id(metadata["project_id"], "prj")
                project_key = validate_project_key(metadata["project_key"])
            except ValueError as exc:
                raise KrabError("invalid project identifier") from exc
            role = metadata["role"]
            if role not in {"member", "coordinator", "admin"}:
                raise KrabError("invalid project role")
            projects.append(
                KrabProject(
                    project_id=project_id,
                    project_key=project_key,
                    role=role,
                    label=_text(
                        untrusted["label"], label="project label", max_bytes=256
                    ),
                    content_risk_flags=tuple(flags),
                )
            )
        return tuple(projects)

    def activity(
        self,
        project_key: str,
        *,
        after: int = 0,
        limit: int = 50,
        wait: int = 0,
    ) -> ActivityPage:
        try:
            key = validate_project_key(project_key)
        except ValueError as exc:
            raise KrabError(str(exc)) from exc
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise KrabError("after must be a non-negative integer")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 100
        ):
            raise KrabError("limit must be between 1 and 100")
        if isinstance(wait, bool) or not isinstance(wait, int) or not 0 <= wait <= 30:
            raise KrabError("wait must be between 0 and 30")
        payload = _exact_object(
            self._request(
                "/v1/projects/" + quote(key, safe="") + "/activity",
                {"after": after, "limit": limit, "wait": wait},
            ),
            fields={"trusted_metadata", "events"},
            label="activity response",
        )
        metadata = _exact_object(
            payload["trusted_metadata"],
            fields={"project_key", "after", "next_cursor"},
            label="activity metadata",
        )
        response_key = metadata["project_key"]
        if response_key != key:
            raise KrabError("activity response project does not match the request")
        response_after = _integer(metadata["after"], minimum=0, label="activity after")
        if response_after != after:
            raise KrabError("activity response cursor does not match the request")
        next_cursor = _integer(
            metadata["next_cursor"], minimum=0, label="activity next cursor"
        )
        entries = payload["events"]
        if not isinstance(entries, list) or len(entries) > limit:
            raise KrabError("invalid activity event list")
        events = tuple(self._activity_event(entry, key) for entry in entries)
        if any(
            current.event_seq <= previous.event_seq
            for previous, current in zip(events, events[1:], strict=False)
        ):
            raise KrabError("activity events are not strictly ordered")
        expected_cursor = events[-1].event_seq if events else after
        if next_cursor != expected_cursor:
            raise KrabError("activity next cursor does not match its events")
        return ActivityPage(
            project_key=key,
            after=after,
            next_cursor=next_cursor,
            events=events,
        )

    @staticmethod
    def _activity_event(value: object, project_key: str) -> ActivityEvent:
        event = _exact_object(
            value,
            fields={"trusted_metadata", "untrusted_text"},
            label="activity event",
        )
        metadata = _exact_object(
            event["trusted_metadata"],
            fields={
                "event_seq",
                "kind",
                "project_key",
                "session_id",
                "item_id",
                "actor",
                "creator",
                "recipient",
                "work_claim",
                "from_status",
                "to_status",
                "current_status",
                "version",
                "created_at",
            },
            label="activity event metadata",
        )
        text = _exact_object(
            event["untrusted_text"], fields={"title"}, label="activity event text"
        )
        if metadata["project_key"] != project_key:
            raise KrabError("activity event belongs to another project")
        kind = metadata["kind"]
        if kind not in EVENT_KINDS:
            raise KrabError("invalid activity event kind")

        def status(value: object, *, nullable: bool) -> str | None:
            if value is None and nullable:
                return None
            if value not in ITEM_STATUS:
                raise KrabError("invalid activity item status")
            return str(value)

        claim_value = metadata["work_claim"]
        claim: WorkClaimRef | None = None
        if claim_value is not None:
            claim_item = _exact_object(
                claim_value,
                fields={"claim_id", "kind", "external_id"},
                label="work claim",
            )
            try:
                claim_id = validate_krab_id(claim_item["claim_id"], "clm")
            except ValueError as exc:
                raise KrabError("invalid work-claim ID") from exc
            claim_kind = claim_item["kind"]
            if claim_kind not in {"issue", "pull_request"}:
                raise KrabError("invalid work-claim kind")
            claim = WorkClaimRef(
                claim_id=claim_id,
                kind=claim_kind,
                external_id=_text(
                    claim_item["external_id"],
                    label="work-claim external ID",
                    max_bytes=256,
                ),
            )
        try:
            session_id = validate_krab_id(metadata["session_id"], "ses")
            item_id = validate_krab_id(metadata["item_id"], "itm")
        except ValueError as exc:
            raise KrabError("invalid activity object ID") from exc
        return ActivityEvent(
            event_seq=_integer(
                metadata["event_seq"], minimum=1, label="event sequence"
            ),
            kind=kind,
            project_key=project_key,
            session_id=session_id,
            item_id=item_id,
            actor=_user_ref(metadata["actor"], "event actor"),
            creator=_user_ref(metadata["creator"], "event creator"),
            recipient=_user_ref(metadata["recipient"], "event recipient"),
            work_claim=claim,
            from_status=status(metadata["from_status"], nullable=True),
            to_status=status(metadata["to_status"], nullable=True),
            current_status=status(metadata["current_status"], nullable=False) or "",
            version=_integer(metadata["version"], minimum=1, label="item version"),
            created_at=_timestamp(metadata["created_at"], label="event timestamp"),
            untrusted_title=_text(
                text["title"], label="work-item title", max_bytes=512
            ),
        )
