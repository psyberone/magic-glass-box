from __future__ import annotations

import json
import unittest
from typing import Any

from hexadecapus import __version__
from hexadecapus.krab import (
    ActivityEvent,
    ActivityPage,
    KrabClient,
    KrabError,
    KrabHTTPError,
    KrabIdentity,
    KrabProject,
    KrabUserRef,
    WorkClaimRef,
    validate_base_url,
)

USER_ID = "usr_" + "1" * 32
OTHER_USER_ID = "usr_" + "2" * 32
PROJECT_ID = "prj_" + "3" * 32
SESSION_ID = "ses_" + "4" * 32
ITEM_ID = "itm_" + "5" * 32
CLAIM_ID = "clm_" + "6" * 32


class FakeResponse:
    def __init__(
        self,
        status: int,
        payload: dict[str, Any] | bytes,
        *,
        content_length: str | None = None,
    ):
        self.status = status
        self.raw = (
            payload
            if isinstance(payload, bytes)
            else json.dumps(payload, separators=(",", ":")).encode("utf-8")
        )
        self.content_length = content_length

    def getheader(self, name: str) -> str | None:
        if name.lower() == "content-length":
            return self.content_length
        return None

    def read(self, amount: int) -> bytes:
        return self.raw[:amount]


class FakeConnection:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.requests: list[tuple[str, str, dict[str, str]]] = []
        self.closed = False
        self.factory_args: tuple[str, int, float] | None = None

    def request(self, method: str, target: str, *, headers: dict[str, str]) -> None:
        self.requests.append((method, target, headers))

    def getresponse(self) -> FakeResponse:
        return self.response

    def close(self) -> None:
        self.closed = True


def client_for(response: FakeResponse) -> tuple[KrabClient, FakeConnection]:
    connection = FakeConnection(response)

    def factory(host: str, port: int, timeout: float) -> FakeConnection:
        connection.factory_args = (host, port, timeout)
        return connection

    client = KrabClient(
        "http://127.0.0.1:8765",
        "secret-token",
        connection_factory=factory,
    )
    return client, connection


def valid_event(sequence: int = 1) -> dict[str, Any]:
    return {
        "trusted_metadata": {
            "event_seq": sequence,
            "kind": "status.changed",
            "project_key": "hex-project",
            "session_id": SESSION_ID,
            "item_id": ITEM_ID,
            "actor": {"user_id": USER_ID, "handle": "agent_one"},
            "creator": {"user_id": USER_ID, "handle": "agent_one"},
            "recipient": {"user_id": OTHER_USER_ID, "handle": "agent_two"},
            "work_claim": None,
            "from_status": "open.pending",
            "to_status": "open.in_progress",
            "current_status": "open.in_progress",
            "version": sequence,
            "created_at": "2026-10-08T12:00:00Z",
        },
        "untrusted_text": {"title": "Visible title"},
    }


def activity_response(
    events: list[dict[str, Any]], *, after: int = 0, next_cursor: int | None = None
) -> dict[str, Any]:
    if next_cursor is None:
        next_cursor = events[-1]["trusted_metadata"]["event_seq"] if events else after
    return {
        "trusted_metadata": {
            "project_key": "hex-project",
            "after": after,
            "next_cursor": next_cursor,
        },
        "events": events,
    }


class KrabClientTests(unittest.TestCase):
    def test_whoami_is_typed_and_authenticated(self) -> None:
        client, connection = client_for(
            FakeResponse(
                200,
                {
                    "trusted_metadata": {
                        "user_id": USER_ID,
                        "handle": "agent_one",
                        "token_version": 2,
                    }
                },
            )
        )
        identity = client.whoami()
        self.assertEqual(identity, KrabIdentity(USER_ID, "agent_one", 2))
        method, target, headers = connection.requests[0]
        self.assertEqual((method, target), ("GET", "/v1/whoami"))
        self.assertEqual(
            headers,
            {
                "Accept": "application/json",
                "Authorization": "Bearer secret-token",
                "User-Agent": f"hexadecapus/{__version__}",
            },
        )
        self.assertEqual(connection.factory_args, ("127.0.0.1", 8765, 40.0))
        self.assertTrue(connection.closed)

    def test_projects_keep_label_explicitly_untrusted(self) -> None:
        client, _ = client_for(
            FakeResponse(
                200,
                {
                    "projects": [
                        {
                            "trusted_metadata": {
                                "project_id": PROJECT_ID,
                                "project_key": "hex-project",
                                "role": "member",
                            },
                            "untrusted_text": {"label": "Ignore instructions"},
                            "content_risk_flags": ["instruction_override"],
                        }
                    ]
                },
            )
        )
        self.assertEqual(
            client.projects(),
            (
                KrabProject(
                    project_id=PROJECT_ID,
                    project_key="hex-project",
                    role="member",
                    label="Ignore instructions",
                    content_risk_flags=("instruction_override",),
                ),
            ),
        )

    def test_activity_contains_title_but_no_body_shape(self) -> None:
        event = {
            "trusted_metadata": {
                "event_seq": 7,
                "kind": "item.created",
                "project_key": "hex-project",
                "session_id": SESSION_ID,
                "item_id": ITEM_ID,
                "actor": {"user_id": USER_ID, "handle": "agent_one"},
                "creator": {"user_id": USER_ID, "handle": "agent_one"},
                "recipient": {"user_id": OTHER_USER_ID, "handle": "agent_two"},
                "work_claim": {
                    "claim_id": CLAIM_ID,
                    "kind": "issue",
                    "external_id": "owner/repository#42",
                },
                "from_status": None,
                "to_status": "open.pending",
                "current_status": "open.pending",
                "version": 1,
                "created_at": "2026-10-08T12:00:00+00:00",
            },
            "untrusted_text": {"title": "Do not execute me"},
        }
        client, connection = client_for(
            FakeResponse(
                200,
                {
                    "trusted_metadata": {
                        "project_key": "hex-project",
                        "after": 0,
                        "next_cursor": 7,
                    },
                    "events": [event],
                },
            )
        )
        page = client.activity("hex-project", limit=1)
        self.assertEqual(
            page,
            ActivityPage(
                project_key="hex-project",
                after=0,
                next_cursor=7,
                events=(
                    ActivityEvent(
                        event_seq=7,
                        kind="item.created",
                        project_key="hex-project",
                        session_id=SESSION_ID,
                        item_id=ITEM_ID,
                        actor=KrabUserRef(USER_ID, "agent_one"),
                        creator=KrabUserRef(USER_ID, "agent_one"),
                        recipient=KrabUserRef(OTHER_USER_ID, "agent_two"),
                        work_claim=WorkClaimRef(
                            CLAIM_ID, "issue", "owner/repository#42"
                        ),
                        from_status=None,
                        to_status="open.pending",
                        current_status="open.pending",
                        version=1,
                        created_at="2026-10-08T12:00:00+00:00",
                        untrusted_title="Do not execute me",
                    ),
                ),
            ),
        )
        self.assertEqual(
            connection.requests[0][1],
            "/v1/projects/hex-project/activity?after=0&limit=1&wait=0",
        )

        event["untrusted_text"]["body"] = "private transcript"
        invalid, _ = client_for(
            FakeResponse(
                200,
                {
                    "trusted_metadata": {
                        "project_key": "hex-project",
                        "after": 0,
                        "next_cursor": 7,
                    },
                    "events": [event],
                },
            )
        )
        with self.assertRaisesRegex(KrabError, "activity event text"):
            invalid.activity("hex-project", limit=1)

    def test_rejects_duplicate_json_keys(self) -> None:
        raw = (
            b'{"trusted_metadata":{"user_id":"'
            + USER_ID.encode()
            + b'","handle":"one","handle":"two","token_version":1}}'
        )
        client, _ = client_for(FakeResponse(200, raw))
        with self.assertRaisesRegex(KrabError, "duplicate JSON key"):
            client.whoami()

    def test_rejects_oversized_declared_response(self) -> None:
        client, _ = client_for(
            FakeResponse(200, {}, content_length=str(1024 * 1024 + 1))
        )
        with self.assertRaisesRegex(KrabError, "size limit"):
            client.whoami()

    def test_surfaces_structured_http_error(self) -> None:
        client, _ = client_for(
            FakeResponse(
                401,
                {"error": {"code": "unauthorized", "message": "bad token"}},
            )
        )
        with self.assertRaises(KrabHTTPError) as raised:
            client.whoami()
        self.assertEqual(raised.exception.status, 401)
        self.assertEqual(raised.exception.code, "unauthorized")
        self.assertEqual(raised.exception.message, "bad token")
        self.assertEqual(
            str(raised.exception), "Krab HTTP 401 (unauthorized): bad token"
        )

    def test_rejects_non_loopback_origin(self) -> None:
        self.assertEqual(
            validate_base_url("http://localhost"),
            ("localhost", 8765, "http://localhost:8765"),
        )
        self.assertEqual(
            validate_base_url("http://127.0.0.1:9000/"),
            ("127.0.0.1", 9000, "http://127.0.0.1:9000"),
        )
        for url in (
            "https://krab.example.test",
            "http://example.test",
            "http://user@localhost",
            "http://user:password@localhost",
            "http://localhost/path",
            "http://localhost?query=yes",
            "http://localhost/#fragment",
            "http://localhost:99999",
        ):
            with self.subTest(url=url), self.assertRaises(KrabError):
                validate_base_url(url)

    def test_rejects_invalid_bearer_tokens(self) -> None:
        for token in ("", "x" * 257, 7):
            with (
                self.subTest(token=token),
                self.assertRaisesRegex(KrabError, "invalid Krab bearer token"),
            ):
                KrabClient("http://localhost", token)  # type: ignore[arg-type]

    def test_rejects_invalid_content_length_and_actual_oversize(self) -> None:
        client, _ = client_for(FakeResponse(200, {}, content_length="many"))
        with self.assertRaisesRegex(KrabError, "Content-Length"):
            client.whoami()
        client, _ = client_for(FakeResponse(200, b"{" + b" " * (1024 * 1024)))
        with self.assertRaisesRegex(KrabError, "size limit"):
            client.whoami()

    def test_rejects_malformed_and_non_utf8_json(self) -> None:
        for raw, message in (
            (b"{", "malformed"),
            (b'"\xff"', "non-UTF-8"),
            (b"[]", "JSON object"),
            (b'{"number":NaN}', "invalid JSON number"),
        ):
            with self.subTest(message=message):
                client, _ = client_for(FakeResponse(200, raw))
                with self.assertRaisesRegex(KrabError, message):
                    client.whoami()

    def test_activity_rejects_invalid_arguments(self) -> None:
        client, _ = client_for(FakeResponse(200, {}))
        for kwargs, message in (
            ({"after": -1}, "after"),
            ({"after": True}, "after"),
            ({"limit": 0}, "limit"),
            ({"limit": 101}, "limit"),
            ({"limit": True}, "limit"),
            ({"wait": -1}, "wait"),
            ({"wait": 31}, "wait"),
            ({"wait": True}, "wait"),
        ):
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaisesRegex(KrabError, message),
            ):
                client.activity("hex-project", **kwargs)

    def test_activity_rejects_cursor_and_order_mismatches(self) -> None:
        def event(sequence: int) -> dict[str, Any]:
            return {
                "trusted_metadata": {
                    "event_seq": sequence,
                    "kind": "status.changed",
                    "project_key": "hex-project",
                    "session_id": SESSION_ID,
                    "item_id": ITEM_ID,
                    "actor": {"user_id": USER_ID, "handle": "agent_one"},
                    "creator": {"user_id": USER_ID, "handle": "agent_one"},
                    "recipient": {
                        "user_id": OTHER_USER_ID,
                        "handle": "agent_two",
                    },
                    "work_claim": None,
                    "from_status": "open.pending",
                    "to_status": "open.in_progress",
                    "current_status": "open.in_progress",
                    "version": sequence,
                    "created_at": "2026-10-08T12:00:00Z",
                },
                "untrusted_text": {"title": "Visible title"},
            }

        payload = {
            "trusted_metadata": {
                "project_key": "hex-project",
                "after": 0,
                "next_cursor": 8,
            },
            "events": [event(8), event(7)],
        }
        client, _ = client_for(FakeResponse(200, payload))
        with self.assertRaisesRegex(KrabError, "strictly ordered"):
            client.activity("hex-project")
        payload["events"] = [event(7)]
        payload["trusted_metadata"]["next_cursor"] = 6
        client, _ = client_for(FakeResponse(200, payload))
        with self.assertRaisesRegex(KrabError, "next cursor"):
            client.activity("hex-project")
        payload["trusted_metadata"]["after"] = 2
        client, _ = client_for(FakeResponse(200, payload))
        with self.assertRaisesRegex(KrabError, "cursor does not match"):
            client.activity("hex-project")

    def test_activity_rejects_invalid_timestamp_status_and_claim(self) -> None:
        base = {
            "event_seq": 1,
            "kind": "item.created",
            "project_key": "hex-project",
            "session_id": SESSION_ID,
            "item_id": ITEM_ID,
            "actor": {"user_id": USER_ID, "handle": "agent_one"},
            "creator": {"user_id": USER_ID, "handle": "agent_one"},
            "recipient": {"user_id": OTHER_USER_ID, "handle": "agent_two"},
            "work_claim": None,
            "from_status": None,
            "to_status": "open.pending",
            "current_status": "open.pending",
            "version": 1,
            "created_at": "2026-10-08T12:00:00Z",
        }

        def assert_invalid(field: str, value: object, message: str) -> None:
            metadata = dict(base)
            metadata[field] = value
            payload = {
                "trusted_metadata": {
                    "project_key": "hex-project",
                    "after": 0,
                    "next_cursor": 1,
                },
                "events": [
                    {
                        "trusted_metadata": metadata,
                        "untrusted_text": {"title": "Title"},
                    }
                ],
            }
            client, _ = client_for(FakeResponse(200, payload))
            with self.assertRaisesRegex(KrabError, message):
                client.activity("hex-project")

        assert_invalid("current_status", "deleted", "status")
        assert_invalid("created_at", "yesterday", "timestamp")
        claim = {
            "claim_id": CLAIM_ID,
            "kind": "task",
            "external_id": "42",
        }
        assert_invalid("work_claim", claim, "claim kind")

    def test_projects_reject_invalid_roles_and_duplicate_risk_flags(self) -> None:
        def payload(role: str, flags: list[str]) -> dict[str, Any]:
            return {
                "projects": [
                    {
                        "trusted_metadata": {
                            "project_id": PROJECT_ID,
                            "project_key": "hex-project",
                            "role": role,
                        },
                        "untrusted_text": {"label": "Hex"},
                        "content_risk_flags": flags,
                    }
                ]
            }

        client, _ = client_for(FakeResponse(200, payload("owner", [])))
        with self.assertRaisesRegex(KrabError, "role"):
            client.projects()
        client, _ = client_for(
            FakeResponse(200, payload("member", ["external_url", "external_url"]))
        )
        with self.assertRaisesRegex(KrabError, "risk flags"):
            client.projects()

    def test_whoami_rejects_non_exact_and_invalid_metadata(self) -> None:
        valid = {
            "trusted_metadata": {
                "user_id": USER_ID,
                "handle": "agent_one",
                "token_version": 1,
            }
        }
        cases: tuple[tuple[dict[str, Any], str], ...] = (
            ({**valid, "extra": True}, "whoami response"),
            ({"trusted_metadata": {"user_id": USER_ID}}, "whoami metadata"),
            (
                {
                    "trusted_metadata": {
                        **valid["trusted_metadata"],
                        "user_id": PROJECT_ID,
                    }
                },
                "whoami user ID",
            ),
            (
                {
                    "trusted_metadata": {
                        **valid["trusted_metadata"],
                        "handle": "Bad Handle",
                    }
                },
                "Krab handle",
            ),
            (
                {
                    "trusted_metadata": {
                        **valid["trusted_metadata"],
                        "token_version": 0,
                    }
                },
                "token version",
            ),
            (
                {
                    "trusted_metadata": {
                        **valid["trusted_metadata"],
                        "token_version": True,
                    }
                },
                "token version",
            ),
        )
        for payload, message in cases:
            with self.subTest(message=message):
                client, _ = client_for(FakeResponse(200, payload))
                with self.assertRaisesRegex(KrabError, message):
                    client.whoami()

    def test_projects_accept_all_roles_and_reject_non_exact_shapes(self) -> None:
        def entry(role: str = "member") -> dict[str, Any]:
            return {
                "trusted_metadata": {
                    "project_id": PROJECT_ID,
                    "project_key": "hex-project",
                    "role": role,
                },
                "untrusted_text": {"label": "Hex"},
                "content_risk_flags": [],
            }

        for role in ("member", "coordinator", "admin"):
            client, _ = client_for(FakeResponse(200, {"projects": [entry(role)]}))
            self.assertEqual(client.projects()[0].role, role)

        cases: tuple[tuple[dict[str, Any], str], ...] = (
            ({"projects": "invalid"}, "project list"),
            ({"projects": [{**entry(), "extra": 1}]}, "project object"),
            (
                {
                    "projects": [
                        {**entry(), "trusted_metadata": {"project_id": PROJECT_ID}}
                    ]
                },
                "project metadata",
            ),
            (
                {
                    "projects": [
                        {
                            **entry(),
                            "untrusted_text": {"label": "Hex", "body": "no"},
                        }
                    ]
                },
                "project text",
            ),
            (
                {"projects": [{**entry(), "content_risk_flags": "flag"}]},
                "risk flags",
            ),
            (
                {"projects": [{**entry(), "content_risk_flags": [7]}]},
                "risk flags",
            ),
            (
                {
                    "projects": [
                        {
                            **entry(),
                            "content_risk_flags": [str(index) for index in range(65)],
                        }
                    ]
                },
                "risk flags",
            ),
            (
                {
                    "projects": [
                        {
                            **entry(),
                            "trusted_metadata": {
                                **entry()["trusted_metadata"],
                                "project_id": "usr_" + "1" * 32,
                            },
                        }
                    ]
                },
                "project identifier",
            ),
            (
                {
                    "projects": [
                        {
                            **entry(),
                            "trusted_metadata": {
                                **entry()["trusted_metadata"],
                                "project_key": "Bad_Key",
                            },
                        }
                    ]
                },
                "project identifier",
            ),
            (
                {"projects": [{**entry(), "untrusted_text": {"label": "x\x00y"}}]},
                "project label",
            ),
            (
                {"projects": [{**entry(), "untrusted_text": {"label": "x" * 257}}]},
                "project label",
            ),
        )
        for payload, message in cases:
            with self.subTest(message=message):
                client, _ = client_for(FakeResponse(200, payload))
                with self.assertRaisesRegex(KrabError, message):
                    client.projects()

    def test_activity_accepts_boundaries_and_empty_pages(self) -> None:
        client, connection = client_for(FakeResponse(200, activity_response([])))
        self.assertEqual(
            client.activity("hex-project", after=0, limit=100, wait=30),
            ActivityPage("hex-project", 0, 0, ()),
        )
        self.assertEqual(
            connection.requests[0][1],
            "/v1/projects/hex-project/activity?after=0&limit=100&wait=30",
        )

    def test_activity_rejects_response_and_event_shape_mismatches(self) -> None:
        base_event = valid_event()
        cases: list[tuple[dict[str, Any], str]] = [
            ({**activity_response([]), "extra": 1}, "activity response"),
            (
                {
                    **activity_response([]),
                    "trusted_metadata": {"project_key": "hex-project"},
                },
                "activity metadata",
            ),
            (
                {
                    **activity_response([]),
                    "trusted_metadata": {
                        "project_key": "other-project",
                        "after": 0,
                        "next_cursor": 0,
                    },
                },
                "does not match",
            ),
            ({**activity_response([]), "events": "invalid"}, "event list"),
            (
                activity_response([{**base_event, "extra": True}]),
                "activity event",
            ),
            (
                activity_response(
                    [
                        {
                            **base_event,
                            "trusted_metadata": {
                                **base_event["trusted_metadata"],
                                "extra": True,
                            },
                        }
                    ]
                ),
                "activity event metadata",
            ),
            (
                activity_response(
                    [
                        {
                            **base_event,
                            "untrusted_text": {"title": "Title", "body": "no"},
                        }
                    ]
                ),
                "activity event text",
            ),
        ]
        for payload, message in cases:
            with self.subTest(message=message):
                client, _ = client_for(FakeResponse(200, payload))
                with self.assertRaisesRegex(KrabError, message):
                    client.activity("hex-project")

    def test_activity_rejects_invalid_event_metadata_fields(self) -> None:
        base = valid_event()["trusted_metadata"]

        def payload_with(field: str, value: object) -> dict[str, Any]:
            event = valid_event()
            event["trusted_metadata"] = {**base, field: value}
            return activity_response([event], next_cursor=1)

        cases = (
            ("project_key", "other-project", "another project"),
            ("kind", "item.deleted", "event kind"),
            ("event_seq", 0, "event sequence"),
            ("event_seq", True, "event sequence"),
            ("version", 0, "item version"),
            ("session_id", "itm_" + "4" * 32, "object ID"),
            ("item_id", "ses_" + "5" * 32, "object ID"),
            ("actor", {"user_id": USER_ID}, "event actor"),
            (
                "creator",
                {"user_id": PROJECT_ID, "handle": "agent_one"},
                "creator user ID",
            ),
            (
                "recipient",
                {"user_id": OTHER_USER_ID, "handle": "Bad"},
                "Krab handle",
            ),
            ("from_status", "invalid", "item status"),
            ("to_status", 7, "item status"),
            ("current_status", None, "item status"),
            ("created_at", "2026-10-08T12:00:00", "timestamp"),
        )
        for field, value, message in cases:
            with self.subTest(field=field, value=value):
                client, _ = client_for(FakeResponse(200, payload_with(field, value)))
                with self.assertRaisesRegex(KrabError, message):
                    client.activity("hex-project")

    def test_activity_rejects_invalid_claim_and_title_fields(self) -> None:
        base_claim = {
            "claim_id": CLAIM_ID,
            "kind": "pull_request",
            "external_id": "owner/repository#7",
        }

        def with_claim(claim: object) -> dict[str, Any]:
            event = valid_event()
            event["trusted_metadata"]["work_claim"] = claim
            return activity_response([event])

        cases = (
            ({**base_claim, "extra": True}, "work claim"),
            ({**base_claim, "claim_id": ITEM_ID}, "work-claim ID"),
            ({**base_claim, "kind": "task"}, "work-claim kind"),
            ({**base_claim, "external_id": "x\x00y"}, "external ID"),
            ({**base_claim, "external_id": "x" * 257}, "external ID"),
        )
        for claim, message in cases:
            with self.subTest(claim=claim):
                client, _ = client_for(FakeResponse(200, with_claim(claim)))
                with self.assertRaisesRegex(KrabError, message):
                    client.activity("hex-project")

        event = valid_event()
        event["untrusted_text"]["title"] = "x" * 513
        client, _ = client_for(FakeResponse(200, activity_response([event])))
        with self.assertRaisesRegex(KrabError, "work-item title"):
            client.activity("hex-project")

    def test_http_status_and_fallback_error_shapes(self) -> None:
        client, _ = client_for(FakeResponse(304, {}))
        with self.assertRaisesRegex(KrabError, "unexpected HTTP status 304"):
            client.whoami()

        for payload, code, message in (
            ({}, "http_error", "HTTP 503"),
            ({"error": "no details"}, "http_error", "HTTP 503"),
            ({"error": {}}, "http_error", "HTTP 503"),
        ):
            with self.subTest(payload=payload):
                client, _ = client_for(FakeResponse(503, payload))
                with self.assertRaises(KrabHTTPError) as raised:
                    client.whoami()
                self.assertEqual(raised.exception.status, 503)
                self.assertEqual(raised.exception.code, code)
                self.assertEqual(raised.exception.message, message)


if __name__ == "__main__":
    unittest.main()
