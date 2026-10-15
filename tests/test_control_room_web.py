from __future__ import annotations

import http.client
import socket
import threading
import unittest
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import urlencode, urlsplit

from magic_glass_box.control_room_web import (
    ControlRoomHTTPServer,
    _ControlRoomHandler,
    _escape,
    _render_control_room,
    build_control_room_server,
)
from magic_glass_box.db import ConflictError, NotFoundError

ACCESS_TOKEN = "A" * 43
CSRF_TOKEN = "C" * 43


class ControlRoomHTMLParser(HTMLParser):
    """Capture the control room's operator-visible semantic contract."""

    VOID_ELEMENTS = {"input", "meta"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.declarations: list[str] = []
        self.start_tags: Counter[str] = Counter()
        self.errors: list[str] = []
        self.headings: list[tuple[str, str]] = []
        self.tables: dict[str, list[list[str]]] = {}
        self.forms: list[dict[str, object]] = []
        self.links: list[dict[str, str | None]] = []
        self.style_nonces: list[str | None] = []
        self.title = ""
        self._stack: list[str] = []
        self._section = ""
        self._heading_tag: str | None = None
        self._heading_text: list[str] = []
        self._table_rows: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._form: dict[str, object] | None = None
        self._link: dict[str, object] | None = None
        self._in_title = False

    @staticmethod
    def _attributes(attrs: list[tuple[str, str | None]]) -> dict[str, str | None]:
        return dict(attrs)

    def handle_decl(self, decl: str) -> None:
        self.declarations.append(decl)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = self._attributes(attrs)
        self.start_tags[tag] += 1
        if tag not in self.VOID_ELEMENTS:
            self._stack.append(tag)
        if tag in {"h1", "h2"}:
            self._heading_tag = tag
            self._heading_text = []
        elif tag == "title":
            self._in_title = True
        elif tag == "style":
            self.style_nonces.append(attributes.get("nonce"))
        elif tag == "table":
            self._table_rows = []
        elif tag == "tr":
            self._row = []
        elif tag in {"th", "td"}:
            self._cell = []
        elif tag == "form":
            self._form = {
                "attributes": attributes,
                "controls": [],
                "text": [],
            }
        elif tag in {"input", "select", "option", "button"} and self._form:
            controls = self._form["controls"]
            assert isinstance(controls, list)
            controls.append((tag, attributes))
        elif tag == "a":
            self._link = {"href": attributes.get("href"), "text_parts": []}

    def handle_endtag(self, tag: str) -> None:
        if tag in {"th", "td"} and self._cell is not None:
            if self._row is not None:
                self._row.append(" ".join(self._cell))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._table_rows is not None:
                self._table_rows.append(self._row)
            self._row = None
        elif tag == "table" and self._table_rows is not None:
            self.tables[self._section] = self._table_rows
            self._table_rows = None
        elif tag in {"h1", "h2"} and self._heading_tag == tag:
            text = " ".join(self._heading_text)
            self.headings.append((tag, text))
            if tag == "h2":
                self._section = text
            self._heading_tag = None
            self._heading_text = []
        elif tag == "form" and self._form is not None:
            self.forms.append(self._form)
            self._form = None
        elif tag == "a" and self._link is not None:
            parts = self._link.pop("text_parts")
            assert isinstance(parts, list)
            self._link["text"] = " ".join(parts)
            self.links.append(self._link)  # type: ignore[arg-type]
            self._link = None
        elif tag == "title":
            self._in_title = False

        if tag not in self.VOID_ELEMENTS:
            if not self._stack or self._stack[-1] != tag:
                self.errors.append(f"unexpected closing tag: {tag}")
            else:
                self._stack.pop()

    def handle_data(self, data: str) -> None:
        text = " ".join(data.split())
        if not text:
            return
        if self._heading_tag is not None:
            self._heading_text.append(text)
        if self._cell is not None:
            self._cell.append(text)
        if self._form is not None:
            parts = self._form["text"]
            assert isinstance(parts, list)
            parts.append(text)
        if self._link is not None:
            parts = self._link["text_parts"]
            assert isinstance(parts, list)
            parts.append(text)
        if self._in_title:
            self.title += text

    def close(self) -> None:
        super().close()
        if self._stack:
            self.errors.append(f"unclosed tags: {self._stack!r}")


class FakeDatabase:
    def __init__(self) -> None:
        self.path = Path("fake-control-plane.db")
        self.calls: list[tuple[str, object]] = []

    def get_run(self, run_id: str) -> dict[str, object]:
        self.calls.append(("get_run", run_id))
        return {"id": run_id, "state": "cancelled", "version": 4}


class FakeControlPlane:
    def __init__(self) -> None:
        self.database = FakeDatabase()
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.snapshot_calls: list[dict[str, object]] = []

    def control_room_snapshot(self, **kwargs: object) -> dict[str, Any]:
        self.snapshot_calls.append(kwargs)
        return snapshot_fixture()

    def request_run_cancellation(
        self, run_id: str, **kwargs: object
    ) -> dict[str, object]:
        self.calls.append(("cancel", {"run_id": run_id, **kwargs}))
        return {"id": run_id, "state": "running", "version": 4}

    def retry_run(self, run_id: str, **kwargs: object) -> dict[str, object]:
        self.calls.append(("retry", {"run_id": run_id, **kwargs}))
        return {"id": "run_successor", "predecessor_run_id": run_id}

    def reassign_run(self, run_id: str, **kwargs: object) -> dict[str, object]:
        self.calls.append(("reassign", {"run_id": run_id, **kwargs}))
        return {"id": "run_reassigned", "predecessor_run_id": run_id}

    def revoke_provider_grant(self, name: str, **kwargs: object) -> dict[str, object]:
        self.calls.append(("revoke", {"name": name, **kwargs}))
        return {"name": name, "state": "revoked", "version": 2}

    def accept_interaction_response(
        self, interaction_id: str, **kwargs: object
    ) -> dict[str, object]:
        self.calls.append(("answer", {"interaction_id": interaction_id, **kwargs}))
        return {"id": interaction_id, "state": "answered", "version": 2}


class FakeCoordinator:
    def __init__(self) -> None:
        self.calls: list[str | None] = []

    def reconcile(self, run_id: str | None = None) -> list[dict[str, object]]:
        self.calls.append(run_id)
        return []


def snapshot_fixture() -> dict[str, Any]:
    return {
        "snapshot": {
            "generated_at": "2026-10-10T12:00:00+00:00",
            "history_limit": 25,
            "terminal_history_truncated": False,
        },
        "summary": {
            "users": 1,
            "projects": 1,
            "profiles": 2,
            "work_items": 1,
            "runs": {"total": 2, "active": 1, "terminal": 1},
            "attention": 1,
        },
        "attention": [
            {
                "kind": "interaction.pending",
                "severity": "action",
                "subject_id": "int_pending",
                "run_id": "run_active",
                "age_seconds": 7200,
            }
        ],
        "users": [
            {
                "id": "boxu_operator",
                "handle": "operator",
                "display_name": "Local Operator",
                "active": 1,
                "krab_identity_ids": ["kid_worker"],
            }
        ],
        "krab_instances": [
            {
                "id": "ksi_local",
                "name": "Local Krab",
                "base_url": "http://127.0.0.1:8765",
                "identities": [],
            }
        ],
        "projects": [
            {
                "id": "boxp_project",
                "project_key": "box-project",
                "display_name": "Magic Glass Box",
                "context_version": 1,
                "active": 1,
            }
        ],
        "profiles": [
            {
                "id": "agp_worker",
                "name": "Worker",
                "active": 1,
                "profile_version": 1,
                "krab_handle": "worker_one",
                "network_mode": "none",
                "runtime_adapter": "codex",
                "runtime_provider_grant": "openai-main",
            },
            {
                "id": "agp_reviewer",
                "name": "Reviewer",
                "active": 1,
                "profile_version": 2,
                "krab_handle": "reviewer_one",
                "network_mode": "none",
                "runtime_adapter": "claude-code",
                "runtime_provider_grant": "anthropic-main",
            },
        ],
        "provider_grants": [
            {
                "id": "pgr_openai",
                "name": "openai-main",
                "state": "active",
                "version": 1,
                "grant_kind": "direct",
                "available": True,
                "expired": False,
                "policy": {"provider": "openai", "model": "gpt-test"},
            }
        ],
        "work_items": [
            {
                "id": "wki_work",
                "project_key": "box-project",
                "untrusted_title": "<script>alert('title')</script>",
                "state": "active",
                "generation": 1,
                "thread_version": 2,
                "active_run_count": 1,
                "private_brief": "NEVER_RENDER_PRIVATE_BRIEF",
            }
        ],
        "assignments": [
            {
                "id": "was_assignment",
                "untrusted_title": "Review the patch",
                "state": "open",
                "version": 1,
                "target_profile_name": "Reviewer",
                "work_item_id": "wki_work",
                "lease_expired": False,
            }
        ],
        "runs": [
            {
                "id": "run_active",
                "state": "running",
                "version": 3,
                "profile_name": "Worker",
                "profile_version": 1,
                "project_key": "box-project",
                "work_item_id": "wki_work",
                "untrusted_work_item_title": "<script>alert('title')</script>",
                "attempt_count": 1,
                "interaction_count": 1,
                "retained_result_count": 0,
                "predecessor_run_id": None,
                "replacement_kind": None,
                "supervision_policy": {"auto_execute": True},
            },
            {
                "id": "run_terminal",
                "state": "failed",
                "version": 5,
                "profile_name": "Worker",
                "profile_version": 1,
                "project_key": "box-project",
                "work_item_id": "wki_work",
                "untrusted_work_item_title": "Fix the control room",
                "attempt_count": 1,
                "interaction_count": 0,
                "retained_result_count": 1,
                "predecessor_run_id": None,
                "replacement_kind": None,
                "supervision_policy": {"auto_execute": False},
            },
        ],
        "attempts": [
            {
                "id": "att_active",
                "run_id": "run_active",
                "state": "running",
                "runtime_adapter": "codex",
                "owner_id": "orchestrator-one",
                "lease_expired": False,
                "cancel_requested_at": None,
            }
        ],
        "interactions": [
            {
                "id": "int_pending",
                "run_id": "run_active",
                "kind": "question",
                "state": "open",
                "version": 1,
                "recipient_krab_handle": "operator",
            }
        ],
        "retained_results": [
            {
                "id": "res_one",
                "attempt_id": "att_terminal",
                "run_id": "run_terminal",
                "changed_paths": 2,
                "changed_bytes": 120,
                "content_sha256": "a" * 64,
            }
        ],
        "candidates": [
            {
                "id": "cnd_one",
                "attempt_result_id": "rsl_one",
                "producer_attempt_id": "att_terminal",
                "producer_run_id": "run_terminal",
                "head_revision": "b" * 40,
                "base_revision": "a" * 40,
                "object_format": "sha1",
                "created_by_handle": "operator",
            }
        ],
        "gate_policies": [
            {
                "id": "gpl_one",
                "candidate_id": "cnd_one",
                "ready": False,
                "passed_count": 1,
                "failed_count": 0,
                "pending_count": 1,
                "integration_ref": "refs/heads/main",
                "expected_base_revision": "a" * 40,
                "head_revision": "b" * 40,
                "policy_revision": "c" * 64,
            }
        ],
        "run_authorities": [
            {
                "run_id": "run_active",
                "authority_name": "openai-main",
                "state": "active",
                "available": True,
                "expired": False,
                "expires_at": None,
            }
        ],
        "supervisor_actions": [
            {
                "id": "act_execute",
                "run_id": "run_active",
                "kind": "run.execute",
                "state": "claimed",
                "attempt_count": 1,
                "max_attempts": 4,
                "owner_id": "supervisor-one",
                "available_at": "2026-10-10T10:00:00+00:00",
                "last_error_code": None,
            }
        ],
        "escalations": [
            {
                "id": "esc_question",
                "run_id": "run_active",
                "kind": "interaction.pending",
                "threshold_seconds": 7200,
                "channel": "krab_reminder",
                "state": "delivered",
                "attempt_count": 1,
                "max_attempts": 4,
                "delivered_at": "2026-10-10T12:00:00+00:00",
                "last_error_code": None,
            }
        ],
        "krab": {
            "status": "ok",
            "refreshed_at": "2026-10-10T12:00:01+00:00",
            "roles": [
                {
                    "project_key": "box-project",
                    "krab_handle": "worker_one",
                    "role": "member",
                }
            ],
            "work_claims": [
                {
                    "claim_id": "clm_one",
                    "project_key": "box-project",
                    "kind": "issue",
                    "status": "active",
                    "version": 2,
                    "review_round": 1,
                    "assignee": {
                        "user_id": "usr_worker",
                        "handle": "worker_one",
                    },
                    "external_id": "owner/repo#1",
                    "local_work_item": {
                        "id": "wki_work",
                        "untrusted_title": "Imported issue title",
                    },
                }
            ],
            "errors": [
                {
                    "scope": "work_claims",
                    "identity_id": "kid_worker",
                    "project_key": "box-project",
                    "kind": "transport",
                    "error_type": "KrabUnavailable",
                    "message": "Krab metadata unavailable",
                }
            ],
        },
    }


class ControlRoomWebTests(unittest.TestCase):
    def setUp(self) -> None:
        self.control = FakeControlPlane()
        self.coordinator = FakeCoordinator()
        self.server = build_control_room_server(
            self.control,
            self.coordinator,
            host="127.0.0.1",
            port=0,
            access_token=ACCESS_TOKEN,
            csrf_token=CSRF_TOKEN,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop_server)
        parsed = urlsplit(self.server.control_url)
        self.connection = http.client.HTTPConnection(parsed.hostname, parsed.port)
        self.addCleanup(self.connection.close)
        self.path = parsed.path
        self.origin = f"{parsed.scheme}://{parsed.netloc}"

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(
        self,
        method: str,
        path: str,
        *,
        fields: dict[str, str] | None = None,
        origin: str | None = None,
    ) -> tuple[int, dict[str, str], str]:
        body = None if fields is None else urlencode(fields)
        headers = {"Host": urlsplit(self.origin).netloc}
        if fields is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        if origin is not None:
            headers["Origin"] = origin
        return self.raw_request(method, path, body=body, headers=headers)

    def raw_request(
        self,
        method: str,
        path: str,
        *,
        body: str | bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], str]:
        request_headers = {"Host": urlsplit(self.origin).netloc}
        if headers is not None:
            request_headers.update(headers)
        self.connection.request(method, path, body=body, headers=request_headers)
        response = self.connection.getresponse()
        payload = response.read().decode("utf-8")
        return response.status, dict(response.getheaders()), payload

    def raw_socket_request(self, request: bytes) -> bytes:
        with socket.create_connection(self.server.server_address, timeout=1) as client:
            client.settimeout(1)
            client.sendall(request)
            response = bytearray()
            while True:
                chunk = client.recv(65536)
                if not chunk:
                    break
                response.extend(chunk)
        return bytes(response)

    def test_browser_renders_only_redacted_fields_and_escapes_untrusted_titles(
        self,
    ) -> None:
        status, headers, body = self.request("GET", self.path)

        self.assertEqual(status, 200)
        self.assertIn("Magic Glass Box control room", body)
        self.assertIn("&lt;script&gt;alert(&#x27;title&#x27;)&lt;/script&gt;", body)
        self.assertIn("Imported issue title", body)
        self.assertIn("worker_one", body)
        self.assertNotIn("<script>alert('title')</script>", body)
        self.assertNotIn("NEVER_RENDER_PRIVATE_BRIEF", body)
        for label in (
            "Answer",
            "Cancel",
            "Retry",
            "Reassign",
            "Revoke",
            "Reconcile",
        ):
            self.assertIn(label, body)
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
        self.assertEqual(
            self.control.snapshot_calls[-1],
            {"history_limit": 25, "refresh_krab": False},
        )

        status, _headers, _body = self.request("GET", self.path + "?refresh_krab=1")
        self.assertEqual(status, 200)
        self.assertEqual(
            self.control.snapshot_calls[-1],
            {"history_limit": 25, "refresh_krab": True},
        )

    def test_renderer_preserves_semantic_tables_forms_and_document_structure(
        self,
    ) -> None:
        with mock.patch(
            "magic_glass_box.control_room_web.secrets.token_urlsafe",
            return_value="fixed-nonce",
        ):
            body, nonce = _render_control_room(
                snapshot_fixture(),
                root="/private/",
                csrf_token=CSRF_TOKEN,
                receipt="retry",
            )

        parser = ControlRoomHTMLParser()
        parser.feed(body.decode("utf-8"))
        parser.close()

        self.assertEqual(nonce, "fixed-nonce")
        self.assertEqual(parser.declarations, ["doctype html"])
        self.assertEqual(parser.errors, [])
        self.assertEqual(parser.title, "Magic Glass Box control room")
        self.assertEqual(parser.style_nonces, ["fixed-nonce"])
        self.assertEqual(
            parser.links,
            [
                {
                    "href": "/private/?refresh_krab=1",
                    "text": "Refresh Krab metadata",
                }
            ],
        )
        self.assertEqual(
            parser.headings,
            [
                ("h1", "Magic Glass Box control room"),
                ("h2", "Recovery"),
                ("h2", "Attention"),
                ("h2", "Supervisor actions"),
                ("h2", "Escalations"),
                ("h2", "Runs"),
                ("h2", "Attempts"),
                ("h2", "Interactions"),
                ("h2", "Retained results"),
                ("h2", "Candidates"),
                ("h2", "Integration gates"),
                ("h2", "Integration queue"),
                ("h2", "Landing dependencies"),
                ("h2", "Work items"),
                ("h2", "Assignments"),
                ("h2", "Provider authority"),
                ("h2", "Run-bound authority"),
                ("h2", "Projects"),
                ("h2", "Profiles"),
                ("h2", "Users"),
                ("h2", "Krab roles"),
                ("h2", "Krab work claims"),
                ("h2", "Krab refresh errors"),
            ],
        )
        self.assertEqual(
            {
                tag: parser.start_tags[tag]
                for tag in (
                    "html",
                    "head",
                    "meta",
                    "title",
                    "style",
                    "body",
                    "main",
                    "header",
                    "h1",
                    "h2",
                    "section",
                    "table",
                    "tr",
                    "th",
                    "td",
                    "form",
                    "input",
                    "select",
                    "option",
                    "button",
                    "a",
                )
            },
            {
                "html": 1,
                "head": 1,
                "meta": 2,
                "title": 1,
                "style": 1,
                "body": 1,
                "main": 1,
                "header": 1,
                "h1": 1,
                "h2": 22,
                "section": 22,
                "table": 19,
                "tr": 40,
                "th": 129,
                "td": 145,
                "form": 6,
                "input": 19,
                "select": 5,
                "option": 6,
                "button": 6,
                "a": 1,
            },
        )
        self.assertEqual(
            parser.tables,
            {
                "Attention": [
                    ["Severity", "Condition", "Subject", "Run", "Age (seconds)"],
                    [
                        "action",
                        "interaction.pending",
                        "int_pending",
                        "run_active",
                        "7200",
                    ],
                ],
                "Supervisor actions": [
                    [
                        "ID",
                        "Run",
                        "Kind",
                        "State",
                        "Attempts",
                        "Owner",
                        "Available",
                        "Error",
                    ],
                    [
                        "act_execute",
                        "run_active",
                        "run.execute",
                        "claimed",
                        "1 / 4",
                        "supervisor-one",
                        "2026-10-10T10:00:00+00:00",
                        "—",
                    ],
                ],
                "Escalations": [
                    [
                        "ID",
                        "Run",
                        "Kind",
                        "Threshold",
                        "Channel",
                        "State",
                        "Attempts",
                        "Delivered",
                        "Error",
                    ],
                    [
                        "esc_question",
                        "run_active",
                        "interaction.pending",
                        "7200",
                        "krab_reminder",
                        "delivered",
                        "1 / 4",
                        "2026-10-10T12:00:00+00:00",
                        "—",
                    ],
                ],
                "Runs": [
                    [
                        "ID",
                        "State",
                        "Version",
                        "Profile",
                        "Project",
                        "Untrusted work title",
                        "Lineage",
                        "Autonomy",
                        "Actions",
                    ],
                    [
                        "run_active",
                        "running",
                        "3",
                        "Worker",
                        "box-project",
                        "<script>alert('title')</script>",
                        "original",
                        "automatic",
                        "operator Cancel",
                    ],
                    [
                        "run_terminal",
                        "failed",
                        "5",
                        "Worker",
                        "box-project",
                        "Fix the control room",
                        "original",
                        "manual",
                        "operator Retry operator Worker Reviewer Reassign",
                    ],
                ],
                "Attempts": [
                    [
                        "ID",
                        "Run",
                        "State",
                        "Runtime",
                        "Owner",
                        "Lease expired",
                        "Cancel requested",
                    ],
                    [
                        "att_active",
                        "run_active",
                        "running",
                        "codex",
                        "orchestrator-one",
                        "no",
                        "no",
                    ],
                ],
                "Interactions": [
                    ["ID", "Run", "Kind", "State", "Recipient", "Action"],
                    [
                        "int_pending",
                        "run_active",
                        "question",
                        "open",
                        "operator",
                        "Answer",
                    ],
                ],
                "Retained results": [
                    [
                        "ID",
                        "Attempt",
                        "Run",
                        "Changed paths",
                        "Changed bytes",
                        "SHA-256",
                    ],
                    ["res_one", "att_terminal", "run_terminal", "2", "120", "a" * 64],
                ],
                "Candidates": [
                    [
                        "ID",
                        "Attempt",
                        "Run",
                        "Base",
                        "Head",
                        "Format",
                        "Created by",
                    ],
                    [
                        "cnd_one",
                        "att_terminal",
                        "run_terminal",
                        "a" * 40,
                        "b" * 40,
                        "sha1",
                        "operator",
                    ],
                ],
                "Integration gates": [
                    [
                        "ID",
                        "Candidate",
                        "Ready",
                        "Passed",
                        "Failed",
                        "Pending",
                        "Integration ref",
                        "Expected base",
                        "Head",
                        "Policy revision",
                    ],
                    [
                        "gpl_one",
                        "cnd_one",
                        "no",
                        "1",
                        "0",
                        "1",
                        "refs/heads/main",
                        "a" * 40,
                        "b" * 40,
                        "c" * 64,
                    ],
                ],
                "Work items": [
                    [
                        "ID",
                        "Project",
                        "Untrusted title",
                        "State",
                        "Generation",
                        "Thread version",
                        "Active runs",
                    ],
                    [
                        "wki_work",
                        "box-project",
                        "<script>alert('title')</script>",
                        "active",
                        "1",
                        "2",
                        "1",
                    ],
                ],
                "Assignments": [
                    [
                        "ID",
                        "Untrusted title",
                        "State",
                        "Version",
                        "Target profile",
                        "Work item",
                        "Lease expired",
                    ],
                    [
                        "was_assignment",
                        "Review the patch",
                        "open",
                        "1",
                        "Reviewer",
                        "wki_work",
                        "no",
                    ],
                ],
                "Provider authority": [
                    [
                        "Name",
                        "Kind",
                        "Connector",
                        "State",
                        "Version",
                        "Available",
                        "Provider",
                        "Model",
                        "Action",
                    ],
                    [
                        "openai-main",
                        "direct",
                        "—",
                        "active",
                        "1",
                        "yes",
                        "openai",
                        "gpt-test",
                        "operator Revoke",
                    ],
                ],
                "Run-bound authority": [
                    ["Run", "Authority", "State", "Available", "Expired", "Expires"],
                    ["run_active", "openai-main", "active", "yes", "no", "—"],
                ],
                "Projects": [
                    ["Key", "Name", "Active", "Context version"],
                    ["box-project", "Magic Glass Box", "1", "1"],
                ],
                "Profiles": [
                    [
                        "Name",
                        "Active",
                        "Version",
                        "Runtime",
                        "Provider route",
                        "Krab user",
                        "Network",
                    ],
                    [
                        "Worker",
                        "1",
                        "1",
                        "codex",
                        "openai-main",
                        "worker_one",
                        "none",
                    ],
                    [
                        "Reviewer",
                        "1",
                        "2",
                        "claude-code",
                        "anthropic-main",
                        "reviewer_one",
                        "none",
                    ],
                ],
                "Users": [
                    ["Handle", "Display name", "Active", "Krab identities"],
                    ["operator", "Local Operator", "1", "1"],
                ],
                "Krab roles": [
                    ["Project", "Handle", "Role"],
                    ["box-project", "worker_one", "member"],
                ],
                "Krab work claims": [
                    [
                        "Claim",
                        "Project",
                        "Kind",
                        "Status",
                        "Version",
                        "Review round",
                        "Assignee",
                        "External ID",
                        "Untrusted title",
                    ],
                    [
                        "clm_one",
                        "box-project",
                        "issue",
                        "active",
                        "2",
                        "1",
                        "worker_one",
                        "owner/repo#1",
                        "Imported issue title",
                    ],
                ],
                "Krab refresh errors": [
                    ["Scope", "Identity", "Project", "Kind", "Type", "Message"],
                    [
                        "work_claims",
                        "kid_worker",
                        "box-project",
                        "transport",
                        "KrabUnavailable",
                        "Krab metadata unavailable",
                    ],
                ],
            },
        )
        self.assertEqual(
            parser.forms,
            [
                {
                    "attributes": {
                        "method": "post",
                        "action": "/private/actions/reconcile",
                    },
                    "controls": [
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "csrf_token",
                                "value": CSRF_TOKEN,
                            },
                        ),
                        (
                            "input",
                            {
                                "name": "run_id",
                                "placeholder": "Optional run ID",
                                "maxlength": "64",
                            },
                        ),
                        ("button", {"type": "submit"}),
                    ],
                    "text": ["Reconcile"],
                },
                {
                    "attributes": {
                        "method": "post",
                        "action": "/private/actions/cancel",
                    },
                    "controls": [
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "csrf_token",
                                "value": CSRF_TOKEN,
                            },
                        ),
                        (
                            "input",
                            {"type": "hidden", "name": "run_id", "value": "run_active"},
                        ),
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "expected_version",
                                "value": "3",
                            },
                        ),
                        ("select", {"name": "requested_by", "required": None}),
                        ("option", {"value": "operator"}),
                        (
                            "input",
                            {
                                "name": "reason",
                                "required": None,
                                "maxlength": "4096",
                                "placeholder": "Reason for cancellation",
                            },
                        ),
                        ("button", {"type": "submit"}),
                    ],
                    "text": ["operator", "Cancel"],
                },
                {
                    "attributes": {
                        "method": "post",
                        "action": "/private/actions/retry",
                    },
                    "controls": [
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "csrf_token",
                                "value": CSRF_TOKEN,
                            },
                        ),
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "run_id",
                                "value": "run_terminal",
                            },
                        ),
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "expected_version",
                                "value": "5",
                            },
                        ),
                        ("select", {"name": "requested_by", "required": None}),
                        ("option", {"value": "operator"}),
                        ("button", {"type": "submit"}),
                    ],
                    "text": ["operator", "Retry"],
                },
                {
                    "attributes": {
                        "method": "post",
                        "action": "/private/actions/reassign",
                    },
                    "controls": [
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "csrf_token",
                                "value": CSRF_TOKEN,
                            },
                        ),
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "run_id",
                                "value": "run_terminal",
                            },
                        ),
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "expected_version",
                                "value": "5",
                            },
                        ),
                        ("select", {"name": "requested_by", "required": None}),
                        ("option", {"value": "operator"}),
                        ("select", {"name": "profile_name", "required": None}),
                        ("option", {"value": "Worker"}),
                        ("option", {"value": "Reviewer"}),
                        ("button", {"type": "submit"}),
                    ],
                    "text": ["operator", "Worker", "Reviewer", "Reassign"],
                },
                {
                    "attributes": {
                        "method": "post",
                        "action": "/private/actions/answer",
                    },
                    "controls": [
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "csrf_token",
                                "value": CSRF_TOKEN,
                            },
                        ),
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "interaction_id",
                                "value": "int_pending",
                            },
                        ),
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "expected_version",
                                "value": "1",
                            },
                        ),
                        (
                            "input",
                            {
                                "name": "response_item_id",
                                "required": None,
                                "maxlength": "36",
                                "aria-label": "Krab response item ID",
                                "placeholder": "Krab response item ID",
                            },
                        ),
                        ("button", {"type": "submit"}),
                    ],
                    "text": ["Answer"],
                },
                {
                    "attributes": {
                        "method": "post",
                        "action": "/private/actions/revoke",
                    },
                    "controls": [
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "csrf_token",
                                "value": CSRF_TOKEN,
                            },
                        ),
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "grant_name",
                                "value": "openai-main",
                            },
                        ),
                        (
                            "input",
                            {
                                "type": "hidden",
                                "name": "expected_version",
                                "value": "1",
                            },
                        ),
                        ("select", {"name": "requested_by", "required": None}),
                        ("option", {"value": "operator"}),
                        ("button", {"type": "submit"}),
                    ],
                    "text": ["operator", "Revoke"],
                },
            ],
        )

    def test_retry_post_requires_same_origin_csrf_and_typed_version(self) -> None:
        action_path = self.path + "actions/retry"
        fields = {
            "csrf_token": CSRF_TOKEN,
            "run_id": "run_terminal",
            "expected_version": "5",
            "requested_by": "operator",
        }

        status, _headers, _body = self.request(
            "POST", action_path, fields=fields, origin="http://evil.invalid"
        )
        self.assertEqual(status, 403)
        self.assertEqual(self.control.calls, [])

        status, _headers, _body = self.request(
            "POST",
            action_path,
            fields={**fields, "csrf_token": "wrong"},
            origin=self.origin,
        )
        self.assertEqual(status, 403)
        self.assertEqual(self.control.calls, [])

        status, headers, _body = self.request(
            "POST", action_path, fields=fields, origin=self.origin
        )
        self.assertEqual(status, 303)
        self.assertEqual(headers["Location"], self.path + "?receipt=retry")
        self.assertEqual(
            self.control.calls[-1],
            (
                "retry",
                {
                    "run_id": "run_terminal",
                    "expected_run_version": 5,
                    "requested_by_handle": "operator",
                },
            ),
        )

    def test_answer_post_binds_an_explicit_krab_item_without_private_content(
        self,
    ) -> None:
        response_item_id = "itm_" + "2" * 32
        status, headers, body = self.request(
            "POST",
            self.path + "actions/answer",
            fields={
                "csrf_token": CSRF_TOKEN,
                "interaction_id": "int_pending",
                "response_item_id": response_item_id,
                "expected_version": "1",
            },
            origin=self.origin,
        )

        self.assertEqual(status, 303)
        self.assertEqual(headers["Location"], self.path + "?receipt=answer")
        self.assertEqual(body, "")
        self.assertEqual(
            self.control.calls[-1],
            (
                "answer",
                {
                    "interaction_id": "int_pending",
                    "response_item_id": response_item_id,
                    "expected_version": 1,
                },
            ),
        )

    def test_every_operator_action_has_a_bounded_explicit_route(self) -> None:
        cases = (
            (
                "cancel",
                {
                    "run_id": "run_active",
                    "expected_version": "3",
                    "requested_by": "operator",
                    "reason": "Stop this worker",
                },
                "cancel",
            ),
            (
                "reassign",
                {
                    "run_id": "run_terminal",
                    "expected_version": "5",
                    "requested_by": "operator",
                    "profile_name": "Reviewer",
                },
                "reassign",
            ),
            (
                "revoke",
                {
                    "grant_name": "openai-main",
                    "expected_version": "1",
                    "requested_by": "operator",
                },
                "revoke",
            ),
            (
                "reconcile",
                {"run_id": "run_active"},
                None,
            ),
        )
        for action, fields, expected_control_call in cases:
            with self.subTest(action=action):
                status, _headers, _body = self.request(
                    "POST",
                    self.path + "actions/" + action,
                    fields={"csrf_token": CSRF_TOKEN, **fields},
                    origin=self.origin,
                )
                self.assertEqual(status, 303)
                if expected_control_call is not None:
                    self.assertEqual(self.control.calls[-1][0], expected_control_call)
        self.assertEqual(self.coordinator.calls, ["run_active", "run_active"])
        self.assertEqual(self.control.database.calls, [("get_run", "run_active")])

        status, _headers, _body = self.request(
            "POST",
            self.path + "actions/reconcile",
            fields={"csrf_token": CSRF_TOKEN, "run_id": ""},
            origin=self.origin,
        )
        self.assertEqual(status, 303)
        self.assertIsNone(self.coordinator.calls[-1])

    def test_unknown_paths_methods_and_malformed_forms_fail_closed(self) -> None:
        status, _headers, _body = self.request("GET", "/")
        self.assertEqual(status, 404)
        status, _headers, _body = self.request("PUT", self.path)
        self.assertEqual(status, 405)
        status, _headers, _body = self.request(
            "POST",
            self.path + "actions/retry",
            fields={
                "csrf_token": CSRF_TOKEN,
                "run_id": "run_terminal",
                "expected_version": "not-an-integer",
                "requested_by": "operator",
            },
            origin=self.origin,
        )
        self.assertEqual(status, 400)
        for invalid_version in ("0", "01", "-1"):
            with self.subTest(invalid_version=invalid_version):
                status, _headers, _body = self.request(
                    "POST",
                    self.path + "actions/retry",
                    fields={
                        "csrf_token": CSRF_TOKEN,
                        "run_id": "run_terminal",
                        "expected_version": invalid_version,
                        "requested_by": "operator",
                    },
                    origin=self.origin,
                )
                self.assertEqual(status, 400)
        for invalid_operator in ("", "o" * 129):
            with self.subTest(invalid_operator=len(invalid_operator)):
                status, _headers, _body = self.request(
                    "POST",
                    self.path + "actions/retry",
                    fields={
                        "csrf_token": CSRF_TOKEN,
                        "run_id": "run_terminal",
                        "expected_version": "5",
                        "requested_by": invalid_operator,
                    },
                    origin=self.origin,
                )
                self.assertEqual(status, 400)

    def test_query_and_projection_errors_fail_closed(self) -> None:
        cases = (
            "?refresh_krab=1&refresh_krab=0",
            "?unknown=1",
            "?refresh_krab=yes",
            "?receipt=unknown",
        )
        for query in cases:
            with self.subTest(query=query):
                status, _headers, _body = self.request("GET", self.path + query)
                self.assertEqual(status, 400)

        status, _headers, _body = self.raw_request(
            "GET", self.path, headers={"Host": "evil.invalid"}
        )
        self.assertEqual(status, 400)

        with mock.patch.object(
            self.control,
            "control_room_snapshot",
            side_effect=RuntimeError("private implementation detail"),
        ):
            status, _headers, body = self.request("GET", self.path)
        self.assertEqual(status, 503)
        self.assertNotIn("private implementation detail", body)

    def test_non_ascii_http_authority_and_paths_return_bounded_errors(self) -> None:
        authority = self.server.authority.encode("ascii")
        token = self.server.access_token.encode("ascii")
        cases = (
            (
                b"GET /h\xc3\xa9llo/ HTTP/1.1\r\nHost: "
                + authority
                + b"\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 404",
            ),
            (
                b"GET /"
                + token
                + b"/ HTTP/1.1\r\nHost: h\xc3\xa9st\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 400",
            ),
            (
                b"POST /"
                + token
                + b"/actions/retry HTTP/1.1\r\nHost: "
                + authority
                + b"\r\nOrigin: \xc3\xa9\r\n"
                b"Content-Type: application/x-www-form-urlencoded\r\n"
                b"Content-Length: 3\r\nConnection: close\r\n\r\nx=1",
                b"HTTP/1.1 403",
            ),
        )
        for request, status_line in cases:
            with self.subTest(status_line=status_line):
                response = self.raw_socket_request(request)
                self.assertTrue(response.startswith(status_line), response[:100])
                self.assertNotIn(b"Traceback", response)

        status, _headers, _body = self.request("GET", self.path)
        self.assertEqual(status, 200)

    def test_unexpected_handler_failure_is_redacted_and_server_survives(self) -> None:
        with mock.patch.object(
            _ControlRoomHandler,
            "_valid_host",
            side_effect=RuntimeError("private handler detail"),
        ):
            status, _headers, body = self.request("GET", self.path)

        self.assertEqual(status, 500)
        self.assertNotIn("private handler detail", body)
        status, _headers, _body = self.request("GET", self.path)
        self.assertEqual(status, 200)

    def test_malformed_http_actions_and_action_errors_are_redacted(self) -> None:
        origin_headers = {
            "Origin": self.origin,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        status, _headers, _body = self.raw_request(
            "POST",
            self.path + "actions/retry",
            body="x=1",
            headers={**origin_headers, "Host": "evil.invalid"},
        )
        self.assertEqual(status, 400)
        for invalid_path in (
            "/wrong-token/actions/retry",
            self.path + "actions/unknown",
            self.path + "not-actions/retry",
        ):
            with self.subTest(invalid_path=invalid_path):
                status, _headers, _body = self.raw_request(
                    "POST",
                    invalid_path,
                    body="x=1",
                    headers=origin_headers,
                )
                self.assertEqual(status, 404)
        status, _headers, _body = self.raw_request(
            "POST",
            self.path + "actions/retry",
            body="x=1",
            headers={**origin_headers, "Content-Type": "text/plain"},
        )
        self.assertEqual(status, 415)
        status, _headers, _body = self.raw_request(
            "POST",
            self.path + "actions/retry",
            body="x=1",
            headers={**origin_headers, "Content-Length": "invalid"},
        )
        self.assertEqual(status, 400)
        status, _headers, _body = self.raw_request(
            "POST",
            self.path + "actions/retry",
            body=b"",
            headers={**origin_headers, "Content-Length": "16385"},
        )
        self.assertEqual(status, 413)
        status, _headers, _body = self.raw_request(
            "POST",
            self.path + "actions/retry",
            body=b"\xff",
            headers=origin_headers,
        )
        self.assertEqual(status, 400)

        valid = urlencode(
            {
                "csrf_token": CSRF_TOKEN,
                "run_id": "run_terminal",
                "expected_version": "5",
                "requested_by": "operator",
            }
        )
        status, _headers, _body = self.raw_request(
            "POST",
            self.path + "actions/retry",
            body=valid + "&run_id=duplicate",
            headers=origin_headers,
        )
        self.assertEqual(status, 400)
        status, _headers, _body = self.raw_request(
            "POST",
            self.path + "actions/retry",
            body="csrf_token=" + CSRF_TOKEN,
            headers=origin_headers,
        )
        self.assertEqual(status, 400)

        fields = {
            "csrf_token": CSRF_TOKEN,
            "run_id": "run_terminal",
            "expected_version": "5",
            "requested_by": "operator",
        }
        errors = (
            (NotFoundError("private missing target"), 404),
            (ConflictError("private conflicting state"), 409),
            (RuntimeError("private unexpected failure"), 500),
        )
        for error, expected_status in errors:
            with (
                self.subTest(error=type(error).__name__),
                mock.patch.object(self.control, "retry_run", side_effect=error),
            ):
                status, _headers, body = self.request(
                    "POST",
                    self.path + "actions/retry",
                    fields=fields,
                    origin=self.origin,
                )
                self.assertEqual(status, expected_status)
                self.assertNotIn(str(error), body)

    def test_renderer_handles_empty_and_non_record_collections(self) -> None:
        body, nonce = _render_control_room(
            {
                "snapshot": {},
                "summary": {},
                "users": [None],
                "profiles": [None],
                "provider_grants": [None, {"name": "old", "state": "revoked"}],
                "runs": [
                    None,
                    {
                        "id": "run_child",
                        "state": "failed",
                        "version": 2,
                        "replacement_kind": "retry",
                        "predecessor_run_id": "run_parent",
                    },
                ],
                "krab": {},
            },
            root="/private/",
            csrf_token=CSRF_TOKEN,
            receipt="retry",
        )
        rendered = body.decode("utf-8")
        self.assertGreaterEqual(len(nonce), 24)
        self.assertIn("Retry action accepted", rendered)
        self.assertIn("retry of run_parent", rendered)
        self.assertIn("None.", rendered)
        self.assertEqual(_escape(None), "—")
        self.assertEqual(_escape(True), "yes")
        self.assertEqual(_escape(False), "no")

    def test_server_refuses_non_loopback_bindings_and_weak_tokens(self) -> None:
        with self.assertRaisesRegex(ValueError, "loopback"):
            build_control_room_server(
                self.control,
                self.coordinator,
                host="0.0.0.0",  # noqa: S104 - rejection is the behavior under test
                port=0,
            )
        with self.assertRaisesRegex(ValueError, "access token"):
            build_control_room_server(
                self.control,
                self.coordinator,
                host="127.0.0.1",
                port=0,
                access_token="x" * 5,
            )
        invalid_settings = (
            {"host": "localhost"},
            {"port": -1},
            {"port": 65536},
            {"port": True},
            {"history_limit": -1},
            {"history_limit": 501},
            {"history_limit": True},
            {"access_token": "!" * 43},
            {"csrf_token": "!" * 43},
        )
        for settings in invalid_settings:
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                arguments: dict[str, object] = {"host": "127.0.0.1", "port": 0}
                arguments.update(settings)
                build_control_room_server(
                    self.control,
                    self.coordinator,
                    **arguments,
                )
        with self.assertRaisesRegex(ValueError, "distinct"):
            build_control_room_server(
                self.control,
                self.coordinator,
                host="127.0.0.1",
                port=0,
                access_token=ACCESS_TOKEN,
                csrf_token=ACCESS_TOKEN,
            )

    def test_stalled_connections_have_a_bounded_socket_idle_time(self) -> None:
        self.assertFalse(ControlRoomHTTPServer.daemon_threads)
        self.assertTrue(ControlRoomHTTPServer.block_on_close)
        self.assertIsInstance(_ControlRoomHandler.timeout, float)
        self.assertEqual(_ControlRoomHandler.timeout, 15.0)

        with (
            mock.patch.object(_ControlRoomHandler, "timeout", 0.05),
            socket.create_connection(self.server.server_address, timeout=1) as client,
        ):
            client.settimeout(1)
            client.sendall(b"POST /incomplete HTTP/1.1\r\nHost: 127.0.0.1")
            self.assertEqual(client.recv(1), b"")


if __name__ == "__main__":
    unittest.main()
