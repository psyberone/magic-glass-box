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
MAX_REQUEST_BYTES = 32 * 1024
MAX_TITLE_BYTES = 512
MAX_BODY_BYTES = 16 * 1024
MAX_NOTE_BYTES = 4 * 1024
MAX_HISTORY_EVENTS = 128
KRAB_HANDLE_RE = re.compile(r"^[a-z][a-z0-9_-]{2,31}$")
IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
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
WORK_CLAIM_KINDS = {"issue", "pull_request"}
WORK_CLAIM_STATUSES = {
    "available",
    "claimed",
    "active",
    "under_review",
    "done",
}
WORK_CLAIM_PATHS = {"issue": "issues", "pull_request": "pull-requests"}


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
class WorkClaim:
    claim_id: str
    project_key: str
    kind: str
    external_id: str
    status: str
    version: int
    review_round: int
    assignee: KrabUserRef | None
    created_by: KrabUserRef
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class WorkClaimPage:
    project_key: str
    kind: str
    claims: tuple[WorkClaim, ...]


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


@dataclass(frozen=True)
class SessionItemHistoryEvent:
    event_seq: int
    kind: str
    actor: KrabUserRef
    from_status: str | None
    to_status: str | None
    created_at: str
    untrusted_note: str | None
    content_risk_flags: tuple[str, ...]


@dataclass(frozen=True)
class SessionItem:
    item_id: str
    project_key: str
    session_id: str
    creator: KrabUserRef
    recipient: KrabUserRef
    work_claim: WorkClaimRef | None
    status: str
    version: int
    created_at: str
    updated_at: str
    event_seq: int | None
    untrusted_title: str
    untrusted_body: str
    content_risk_flags: tuple[str, ...]
    history: tuple[SessionItemHistoryEvent, ...]


@dataclass(frozen=True)
class SessionItemReminder:
    event_seq: int
    project_key: str
    session_id: str
    item_id: str
    recipient_id: str


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


def _risk_flags(value: object, *, label: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or len(value) > 64
        or any(
            not isinstance(flag, str)
            or not flag
            or "\x00" in flag
            or len(flag.encode("utf-8")) > 64
            for flag in value
        )
        or len(set(value)) != len(value)
    ):
        raise KrabError(f"invalid {label} risk flags")
    return tuple(value)


def _work_claim(value: object) -> WorkClaimRef | None:
    if value is None:
        return None
    item = _exact_object(
        value,
        fields={"claim_id", "kind", "external_id"},
        label="work claim",
    )
    try:
        claim_id = validate_krab_id(item["claim_id"], "clm")
    except ValueError as exc:
        raise KrabError("invalid work-claim ID") from exc
    kind = item["kind"]
    if not isinstance(kind, str) or kind not in {"issue", "pull_request"}:
        raise KrabError("invalid work-claim kind")
    return WorkClaimRef(
        claim_id=claim_id,
        kind=kind,
        external_id=_text(
            item["external_id"], label="work-claim external ID", max_bytes=256
        ),
    )


def _item_status(value: object, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or value not in ITEM_STATUS:
        raise KrabError("invalid session-item status")
    return value


def _session_item_history(value: object) -> SessionItemHistoryEvent:
    event = _exact_object(
        value,
        fields={"trusted_metadata", "untrusted_text", "content_risk_flags"},
        label="session-item history event",
    )
    metadata = _exact_object(
        event["trusted_metadata"],
        fields={
            "event_seq",
            "kind",
            "actor",
            "from_status",
            "to_status",
            "created_at",
        },
        label="session-item history metadata",
    )
    text = _exact_object(
        event["untrusted_text"],
        fields={"note"},
        label="session-item history text",
    )
    kind = metadata["kind"]
    if not isinstance(kind, str) or kind not in EVENT_KINDS:
        raise KrabError("invalid session-item history event kind")
    note_value = text["note"]
    note = (
        None
        if note_value is None
        else _text(
            note_value, label="session-item history note", max_bytes=MAX_NOTE_BYTES
        )
    )
    return SessionItemHistoryEvent(
        event_seq=_integer(
            metadata["event_seq"], minimum=1, label="history event sequence"
        ),
        kind=kind,
        actor=_user_ref(metadata["actor"], "history actor"),
        from_status=_item_status(metadata["from_status"], nullable=True),
        to_status=_item_status(metadata["to_status"], nullable=True),
        created_at=_timestamp(metadata["created_at"], label="history timestamp"),
        untrusted_note=note,
        content_risk_flags=_risk_flags(event["content_risk_flags"], label="history"),
    )


def _session_item(
    value: object,
    *,
    project_key: str,
    session_id: str,
    item_id: str | None,
    created: bool,
) -> SessionItem:
    response_fields = {"trusted_metadata", "untrusted_text", "content_risk_flags"}
    if not created:
        response_fields.add("history")
    response = _exact_object(
        value, fields=response_fields, label="session-item response"
    )
    metadata_fields = {
        "item_id",
        "project_key",
        "session_id",
        "creator",
        "recipient",
        "work_claim",
        "status",
        "version",
        "created_at",
        "updated_at",
    }
    if created:
        metadata_fields.add("event_seq")
    metadata = _exact_object(
        response["trusted_metadata"],
        fields=metadata_fields,
        label="session-item metadata",
    )
    text = _exact_object(
        response["untrusted_text"],
        fields={"title", "body"},
        label="session-item text",
    )
    if metadata["project_key"] != project_key:
        raise KrabError("session-item project does not match the request")
    try:
        response_session_id = validate_krab_id(metadata["session_id"], "ses")
        response_item_id = validate_krab_id(metadata["item_id"], "itm")
    except ValueError as exc:
        raise KrabError("invalid session-item object ID") from exc
    if response_session_id != session_id:
        raise KrabError("session-item session does not match the request")
    if item_id is not None and response_item_id != item_id:
        raise KrabError("session-item item does not match the request")

    history: tuple[SessionItemHistoryEvent, ...] = ()
    if not created:
        history_values = response["history"]
        if (
            not isinstance(history_values, list)
            or not history_values
            or len(history_values) > MAX_HISTORY_EVENTS
        ):
            raise KrabError("invalid session-item history")
        history = tuple(_session_item_history(event) for event in history_values)
        if any(
            current.event_seq <= previous.event_seq
            for previous, current in zip(history, history[1:], strict=False)
        ):
            raise KrabError("session-item history is not strictly ordered")

    return SessionItem(
        item_id=response_item_id,
        project_key=project_key,
        session_id=response_session_id,
        creator=_user_ref(metadata["creator"], "session-item creator"),
        recipient=_user_ref(metadata["recipient"], "session-item recipient"),
        work_claim=_work_claim(metadata["work_claim"]),
        status=_item_status(metadata["status"]) or "",
        version=_integer(metadata["version"], minimum=1, label="session-item version"),
        created_at=_timestamp(metadata["created_at"], label="item creation timestamp"),
        updated_at=_timestamp(metadata["updated_at"], label="item update timestamp"),
        event_seq=(
            _integer(metadata["event_seq"], minimum=1, label="item event sequence")
            if created
            else None
        ),
        untrusted_title=_text(
            text["title"], label="session-item title", max_bytes=MAX_TITLE_BYTES
        ),
        untrusted_body=_text(
            text["body"], label="session-item body", max_bytes=MAX_BODY_BYTES
        ),
        content_risk_flags=_risk_flags(
            response["content_risk_flags"], label="session-item"
        ),
        history=history,
    )


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
        self,
        path: str,
        query: Mapping[str, str | int] | None = None,
        *,
        method: str = "GET",
        payload: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        expected_status: int = 200,
    ) -> dict[str, Any]:
        target = path
        if query:
            target += "?" + urlencode(query)
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token}",
            "User-Agent": f"magic-glass-box/{__version__}",
        }
        body: bytes | None = None
        if payload is not None:
            try:
                body = json.dumps(
                    payload,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            except (TypeError, ValueError, RecursionError) as exc:
                raise KrabError("invalid Krab request body") from exc
            if not body or len(body) > MAX_REQUEST_BYTES:
                raise KrabError("Krab request exceeds the size limit")
            headers.update(
                {
                    "Content-Type": "application/json",
                    "Content-Length": str(len(body)),
                }
            )
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        connection = self._connection_factory(self.host, self.port, self.timeout)
        try:
            connection.request(method, target, body=body, headers=headers)
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
            retry = (
                "; mutation outcome may be unknown; retry with Idempotency-Key "
                + idempotency_key
                if idempotency_key is not None
                else ""
            )
            raise KrabError(f"Krab is unavailable: {exc}{retry}") from None
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
        if status != expected_status:
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
            flags = _risk_flags(item["content_risk_flags"], label="project")
            try:
                project_id = validate_krab_id(metadata["project_id"], "prj")
                project_key = validate_project_key(metadata["project_key"])
            except ValueError as exc:
                raise KrabError("invalid project identifier") from exc
            role = metadata["role"]
            if not isinstance(role, str) or role not in {
                "member",
                "coordinator",
                "admin",
            }:
                raise KrabError("invalid project role")
            projects.append(
                KrabProject(
                    project_id=project_id,
                    project_key=project_key,
                    role=role,
                    label=_text(
                        untrusted["label"], label="project label", max_bytes=256
                    ),
                    content_risk_flags=flags,
                )
            )
        return tuple(projects)

    def work_claims(
        self,
        project_key: str,
        *,
        kind: str,
        status: str | None = None,
        assignee: str | None = None,
    ) -> WorkClaimPage:
        try:
            key = validate_project_key(project_key)
        except ValueError as exc:
            raise KrabError(str(exc)) from exc
        if not isinstance(kind, str) or kind not in WORK_CLAIM_KINDS:
            raise KrabError("invalid work-claim kind")
        if status is not None and (
            not isinstance(status, str) or status not in WORK_CLAIM_STATUSES
        ):
            raise KrabError("invalid work-claim status")
        if assignee is not None:
            assignee = _handle(assignee)
        query: dict[str, str] = {}
        if status is not None:
            query["status"] = status
        if assignee is not None:
            query["assignee"] = assignee
        payload = _exact_object(
            self._request(
                "/v1/projects/" + quote(key, safe="") + "/" + WORK_CLAIM_PATHS[kind],
                query,
            ),
            fields={"trusted_metadata", "claims"},
            label="work-claim list response",
        )
        metadata = _exact_object(
            payload["trusted_metadata"],
            fields={"project_key", "kind", "count"},
            label="work-claim list metadata",
        )
        if metadata["project_key"] != key:
            raise KrabError("work-claim response project does not match the request")
        if metadata["kind"] != kind:
            raise KrabError("work-claim response kind does not match the request")
        entries = payload["claims"]
        if not isinstance(entries, list) or len(entries) > 10_000:
            raise KrabError("invalid work-claim list")
        count = _integer(metadata["count"], minimum=0, label="work-claim count")
        if count != len(entries):
            raise KrabError("work-claim count does not match the response")
        claims = tuple(self._work_claim(entry, key, kind) for entry in entries)
        if len({claim.claim_id for claim in claims}) != len(claims):
            raise KrabError("work-claim response contains duplicate claims")
        return WorkClaimPage(project_key=key, kind=kind, claims=claims)

    @staticmethod
    def _work_claim(value: object, project_key: str, kind: str) -> WorkClaim:
        item = _exact_object(
            value,
            fields={"trusted_metadata"},
            label="work claim",
        )
        metadata = _exact_object(
            item["trusted_metadata"],
            fields={
                "claim_id",
                "project_key",
                "kind",
                "external_id",
                "status",
                "version",
                "review_round",
                "assignee",
                "created_by",
                "created_at",
                "updated_at",
            },
            label="work-claim metadata",
        )
        if metadata["project_key"] != project_key:
            raise KrabError("work claim belongs to another project")
        if metadata["kind"] != kind:
            raise KrabError("work claim has an unexpected kind")
        try:
            claim_id = validate_krab_id(metadata["claim_id"], "clm")
        except ValueError as exc:
            raise KrabError("invalid work-claim claim ID") from exc
        claim_status = metadata["status"]
        if not isinstance(claim_status, str) or claim_status not in WORK_CLAIM_STATUSES:
            raise KrabError("invalid work-claim status")
        assignee_value = metadata["assignee"]
        assignee = (
            None
            if assignee_value is None
            else _user_ref(assignee_value, "work-claim assignee")
        )
        if (claim_status == "available") != (assignee is None):
            raise KrabError("invalid work-claim assignee for status")
        return WorkClaim(
            claim_id=claim_id,
            project_key=project_key,
            kind=kind,
            external_id=_text(
                metadata["external_id"],
                label="work-claim external ID",
                max_bytes=256,
            ),
            status=claim_status,
            version=_integer(
                metadata["version"], minimum=1, label="work-claim version"
            ),
            review_round=_integer(
                metadata["review_round"],
                minimum=0,
                label="work-claim review round",
            ),
            assignee=assignee,
            created_by=_user_ref(metadata["created_by"], "work-claim creator"),
            created_at=_timestamp(
                metadata["created_at"], label="work-claim created timestamp"
            ),
            updated_at=_timestamp(
                metadata["updated_at"], label="work-claim updated timestamp"
            ),
        )

    def create_session_item(
        self,
        project_key: str,
        session_id: str,
        *,
        recipient_handle: str,
        title: str,
        body: str,
        idempotency_key: str,
        claim_id: str | None = None,
    ) -> SessionItem:
        try:
            key = validate_project_key(project_key)
            session = validate_krab_id(session_id, "ses")
        except ValueError as exc:
            raise KrabError(str(exc)) from exc
        recipient = _handle(recipient_handle)
        request_title = _text(
            title, label="session-item title", max_bytes=MAX_TITLE_BYTES
        )
        request_body = _text(body, label="session-item body", max_bytes=MAX_BODY_BYTES)
        if not request_title:
            raise KrabError("invalid session-item title")
        if not request_body:
            raise KrabError("invalid session-item body")
        if (
            not isinstance(idempotency_key, str)
            or IDEMPOTENCY_KEY_RE.fullmatch(idempotency_key) is None
        ):
            raise KrabError("invalid idempotency key")
        request: dict[str, Any] = {
            "to": recipient,
            "title": request_title,
            "body": request_body,
        }
        if claim_id is not None:
            try:
                request["claim_id"] = validate_krab_id(claim_id, "clm")
            except ValueError as exc:
                raise KrabError(str(exc)) from exc
        response = self._request(
            "/v1/projects/"
            + quote(key, safe="")
            + "/sessions/"
            + quote(session, safe="")
            + "/items",
            method="POST",
            payload=request,
            idempotency_key=idempotency_key,
            expected_status=201,
        )
        return _session_item(
            response,
            project_key=key,
            session_id=session,
            item_id=None,
            created=True,
        )

    def get_session_item(
        self, project_key: str, session_id: str, item_id: str
    ) -> SessionItem:
        try:
            key = validate_project_key(project_key)
            session = validate_krab_id(session_id, "ses")
            item = validate_krab_id(item_id, "itm")
        except ValueError as exc:
            raise KrabError(str(exc)) from exc
        response = self._request(
            "/v1/projects/"
            + quote(key, safe="")
            + "/sessions/"
            + quote(session, safe="")
            + "/items/"
            + quote(item, safe="")
        )
        return _session_item(
            response,
            project_key=key,
            session_id=session,
            item_id=item,
            created=False,
        )

    def remind_session_item(
        self,
        project_key: str,
        session_id: str,
        item_id: str,
        *,
        idempotency_key: str,
    ) -> SessionItemReminder:
        try:
            key = validate_project_key(project_key)
            session = validate_krab_id(session_id, "ses")
            item = validate_krab_id(item_id, "itm")
        except ValueError as exc:
            raise KrabError(str(exc)) from exc
        if (
            not isinstance(idempotency_key, str)
            or IDEMPOTENCY_KEY_RE.fullmatch(idempotency_key) is None
        ):
            raise KrabError("invalid idempotency key")
        response = _exact_object(
            self._request(
                "/v1/projects/"
                + quote(key, safe="")
                + "/sessions/"
                + quote(session, safe="")
                + "/items/"
                + quote(item, safe="")
                + "/notify",
                method="POST",
                payload={},
                idempotency_key=idempotency_key,
                expected_status=202,
            ),
            fields={"trusted_metadata"},
            label="session-item reminder response",
        )
        metadata = _exact_object(
            response["trusted_metadata"],
            fields={
                "event_seq",
                "kind",
                "project_key",
                "session_id",
                "item_id",
                "recipient_id",
            },
            label="session-item reminder metadata",
        )
        if metadata["kind"] != "item.reminded":
            raise KrabError("invalid session-item reminder kind")
        if metadata["project_key"] != key:
            raise KrabError("session-item reminder project does not match the request")
        if metadata["session_id"] != session:
            raise KrabError("session-item reminder session does not match the request")
        if metadata["item_id"] != item:
            raise KrabError("session-item reminder item does not match the request")
        try:
            recipient_id = validate_krab_id(metadata["recipient_id"], "usr")
        except ValueError as exc:
            raise KrabError("invalid session-item reminder recipient") from exc
        return SessionItemReminder(
            event_seq=_integer(
                metadata["event_seq"], minimum=1, label="reminder event sequence"
            ),
            project_key=key,
            session_id=session,
            item_id=item,
            recipient_id=recipient_id,
        )

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
        if not isinstance(kind, str) or kind not in EVENT_KINDS:
            raise KrabError("invalid activity event kind")
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
            work_claim=_work_claim(metadata["work_claim"]),
            from_status=_item_status(metadata["from_status"], nullable=True),
            to_status=_item_status(metadata["to_status"], nullable=True),
            current_status=_item_status(metadata["current_status"]) or "",
            version=_integer(metadata["version"], minimum=1, label="item version"),
            created_at=_timestamp(metadata["created_at"], label="event timestamp"),
            untrusted_title=_text(
                text["title"], label="work-item title", max_bytes=512
            ),
        )
