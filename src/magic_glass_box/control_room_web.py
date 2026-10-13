from __future__ import annotations

import html
import ipaddress
import secrets
import socket
from collections.abc import Mapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol
from urllib.parse import parse_qsl, quote, urlsplit

from .db import ConcurrentUpdateError, ConflictError, NotFoundError
from .domain import ValidationError

MAX_FORM_BYTES = 16 * 1024
MAX_FORM_FIELDS = 8
TOKEN_MIN_LENGTH = 32
TOKEN_MAX_LENGTH = 128
CONTROL_ROOM_SOCKET_TIMEOUT_SECONDS = 15.0
ACTIVE_RUN_STATES = {"queued", "preparing", "running", "waiting_for_input"}
TERMINAL_RUN_STATES = {"succeeded", "failed", "cancelled"}
CONTROL_ROOM_CSS = """
:root {
  --bg: #11131a;
  --panel: #1a1e29;
  --ink: #f4f2ec;
  --muted: #a9b0c2;
  --line: #343b4e;
  --accent: #f0b35a;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font: 15px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace;
}
main { max-width: 1500px; margin: auto; padding: 2rem; }
h1 { font-size: 2rem; margin: .2rem 0; }
h2 { margin-top: 2rem; }
.sub, .empty { color: var(--muted); }
.cards { display: flex; gap: 1rem; flex-wrap: wrap; }
.card, section {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 1rem;
}
.card strong { font-size: 1.5rem; display: block; }
section { margin-top: 1rem; }
.table-wrap { overflow: auto; }
table { width: 100%; border-collapse: collapse; }
th, td {
  padding: .6rem;
  text-align: left;
  border-bottom: 1px solid var(--line);
  vertical-align: top;
}
th { color: var(--accent); }
form { display: flex; gap: .4rem; flex-wrap: wrap; margin: .25rem 0; }
input, select, button {
  font: inherit;
  padding: .35rem;
  background: #0f1117;
  color: var(--ink);
  border: 1px solid var(--line);
  border-radius: 4px;
}
button { border-color: var(--accent); cursor: pointer; }
.receipt {
  border-left: 4px solid var(--accent);
  padding: .7rem;
  background: var(--panel);
}
a { color: var(--accent); }
"""
ACTION_FIELDS = {
    "answer": frozenset(
        {"csrf_token", "interaction_id", "response_item_id", "expected_version"}
    ),
    "cancel": frozenset(
        {"csrf_token", "run_id", "expected_version", "requested_by", "reason"}
    ),
    "retry": frozenset({"csrf_token", "run_id", "expected_version", "requested_by"}),
    "reassign": frozenset(
        {
            "csrf_token",
            "run_id",
            "expected_version",
            "requested_by",
            "profile_name",
        }
    ),
    "revoke": frozenset(
        {"csrf_token", "grant_name", "expected_version", "requested_by"}
    ),
    "reconcile": frozenset({"csrf_token", "run_id"}),
}


class ControlPlaneLike(Protocol):
    database: Any

    def control_room_snapshot(
        self, *, history_limit: int, refresh_krab: bool
    ) -> dict[str, Any]: ...

    def request_run_cancellation(
        self,
        run_id: str,
        *,
        requested_by_handle: str,
        expected_run_version: int,
        reason: str,
    ) -> dict[str, Any]: ...

    def retry_run(
        self,
        run_id: str,
        *,
        requested_by_handle: str,
        expected_run_version: int,
    ) -> dict[str, Any]: ...

    def reassign_run(
        self,
        run_id: str,
        *,
        profile_name: str,
        requested_by_handle: str,
        expected_run_version: int,
    ) -> dict[str, Any]: ...

    def revoke_provider_grant(
        self,
        name: str,
        *,
        expected_version: int,
        revoked_by_handle: str,
    ) -> dict[str, Any]: ...

    def accept_interaction_response(
        self,
        interaction_id: str,
        *,
        response_item_id: str,
        expected_version: int,
    ) -> Any: ...


class CoordinatorLike(Protocol):
    def reconcile(self, run_id: str | None = None) -> list[dict[str, Any]]: ...


class OperatorActions:
    """Route an explicit browser action through existing control-plane APIs."""

    def __init__(self, control: ControlPlaneLike, coordinator: CoordinatorLike) -> None:
        self.control = control
        self.coordinator = coordinator

    def cancel(
        self,
        run_id: str,
        *,
        expected_version: int,
        requested_by: str,
        reason: str,
    ) -> dict[str, Any]:
        self.control.request_run_cancellation(
            run_id,
            requested_by_handle=requested_by,
            expected_run_version=expected_version,
            reason=reason,
        )
        self.coordinator.reconcile(run_id)
        return self.control.database.get_run(run_id)

    def answer(
        self,
        interaction_id: str,
        *,
        response_item_id: str,
        expected_version: int,
    ) -> Any:
        return self.control.accept_interaction_response(
            interaction_id,
            response_item_id=response_item_id,
            expected_version=expected_version,
        )

    def retry(
        self,
        run_id: str,
        *,
        expected_version: int,
        requested_by: str,
    ) -> dict[str, Any]:
        return self.control.retry_run(
            run_id,
            requested_by_handle=requested_by,
            expected_run_version=expected_version,
        )

    def reassign(
        self,
        run_id: str,
        *,
        profile_name: str,
        expected_version: int,
        requested_by: str,
    ) -> dict[str, Any]:
        return self.control.reassign_run(
            run_id,
            profile_name=profile_name,
            requested_by_handle=requested_by,
            expected_run_version=expected_version,
        )

    def revoke(
        self,
        grant_name: str,
        *,
        expected_version: int,
        requested_by: str,
    ) -> dict[str, Any]:
        return self.control.revoke_provider_grant(
            grant_name,
            expected_version=expected_version,
            revoked_by_handle=requested_by,
        )

    def reconcile(self, run_id: str | None) -> list[dict[str, Any]]:
        return self.coordinator.reconcile(run_id)


class _Markup(str):
    pass


def _escape(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    return html.escape(str(value), quote=True)


def _markup(value: str) -> _Markup:
    return _Markup(value)


def _cell(value: object) -> str:
    return str(value) if isinstance(value, _Markup) else _escape(value)


def _table(headers: tuple[str, ...], rows: list[tuple[object, ...]]) -> str:
    if not rows:
        return '<p class="empty">None.</p>'
    heading = "".join(f"<th>{_escape(label)}</th>" for label in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{_cell(value)}</td>" for value in row) + "</tr>"
        for row in rows
    )
    return (
        "<div class=table-wrap><table><thead><tr>"
        f"{heading}</tr></thead><tbody>{body}</tbody></table></div>"
    )


def _section(title: str, table: str) -> str:
    return f"<section><h2>{_escape(title)}</h2>{table}</section>"


def _card(value: object, label: str) -> str:
    return f'<div class="card"><strong>{_escape(value)}</strong>{_escape(label)}</div>'


def _hidden(name: str, value: object) -> str:
    return f'<input type="hidden" name="{_escape(name)}" value="{_escape(value)}">'


def _operator_select(handles: list[str]) -> str:
    options = "".join(
        f'<option value="{_escape(handle)}">{_escape(handle)}</option>'
        for handle in handles
    )
    return f'<select name="requested_by" required>{options}</select>'


def _action_form(
    root: str,
    action: str,
    csrf_token: str,
    hidden: Mapping[str, object],
    controls: str,
    label: str,
) -> _Markup:
    fields = _hidden("csrf_token", csrf_token) + "".join(
        _hidden(name, value) for name, value in hidden.items()
    )
    return _markup(
        f'<form method="post" action="{_escape(root)}actions/{_escape(action)}">'
        f"{fields}{controls}<button type=submit>{_escape(label)}</button></form>"
    )


def _render_control_room(
    snapshot: Mapping[str, Any],
    *,
    root: str,
    csrf_token: str,
    receipt: str | None,
) -> tuple[bytes, str]:
    nonce = secrets.token_urlsafe(18)
    users = [
        row
        for row in snapshot.get("users", [])
        if isinstance(row, Mapping) and bool(row.get("active"))
    ]
    handles = [str(row.get("handle", "")) for row in users if row.get("handle")]
    profiles = [
        row
        for row in snapshot.get("profiles", [])
        if isinstance(row, Mapping) and bool(row.get("active"))
    ]
    profile_names = [str(row.get("name", "")) for row in profiles if row.get("name")]
    operator = _operator_select(handles)
    profile_options = "".join(
        f'<option value="{_escape(name)}">{_escape(name)}</option>'
        for name in profile_names
    )
    profile_select = f'<select name="profile_name" required>{profile_options}</select>'

    run_rows: list[tuple[object, ...]] = []
    for run in snapshot.get("runs", []):
        if not isinstance(run, Mapping):
            continue
        state = run.get("state")
        controls: list[str] = []
        common = {
            "run_id": run.get("id", ""),
            "expected_version": run.get("version", ""),
        }
        if state in ACTIVE_RUN_STATES and handles:
            cancel_controls = (
                operator + '<input name="reason" required maxlength="4096" '
                'placeholder="Reason for cancellation">'
            )
            controls.append(
                _action_form(
                    root,
                    "cancel",
                    csrf_token,
                    common,
                    cancel_controls,
                    "Cancel",
                )
            )
        if state in TERMINAL_RUN_STATES and handles:
            controls.append(
                _action_form(
                    root,
                    "retry",
                    csrf_token,
                    common,
                    operator,
                    "Retry",
                )
            )
            if profile_names:
                controls.append(
                    _action_form(
                        root,
                        "reassign",
                        csrf_token,
                        common,
                        operator + profile_select,
                        "Reassign",
                    )
                )
        lineage = run.get("replacement_kind") or "original"
        if run.get("predecessor_run_id"):
            lineage = f"{lineage} of {run['predecessor_run_id']}"
        supervision_policy = run.get("supervision_policy")
        supervision_policy = (
            supervision_policy if isinstance(supervision_policy, Mapping) else {}
        )
        run_rows.append(
            (
                run.get("id"),
                state,
                run.get("version"),
                run.get("profile_name"),
                run.get("project_key"),
                run.get("untrusted_work_item_title"),
                lineage,
                "automatic" if supervision_policy.get("auto_execute") else "manual",
                _markup("".join(controls)) if controls else "—",
            )
        )

    grant_rows: list[tuple[object, ...]] = []
    for grant in snapshot.get("provider_grants", []):
        if not isinstance(grant, Mapping):
            continue
        action: object = "—"
        if grant.get("state") == "active" and handles:
            action = _action_form(
                root,
                "revoke",
                csrf_token,
                {
                    "grant_name": grant.get("name", ""),
                    "expected_version": grant.get("version", ""),
                },
                operator,
                "Revoke",
            )
        policy = grant.get("policy")
        policy = policy if isinstance(policy, Mapping) else {}
        grant_rows.append(
            (
                grant.get("name"),
                grant.get("state"),
                grant.get("version"),
                grant.get("available"),
                policy.get("provider"),
                policy.get("model"),
                action,
            )
        )

    metadata = snapshot.get("snapshot")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    summary = snapshot.get("summary")
    summary = summary if isinstance(summary, Mapping) else {}
    run_summary = summary.get("runs")
    run_summary = run_summary if isinstance(run_summary, Mapping) else {}
    krab = snapshot.get("krab")
    krab = krab if isinstance(krab, Mapping) else {}

    attention_rows = [
        (
            row.get("severity"),
            row.get("kind"),
            row.get("subject_id"),
            row.get("run_id"),
            row.get("age_seconds"),
        )
        for row in snapshot.get("attention", [])
        if isinstance(row, Mapping)
    ]
    supervisor_action_rows = [
        (
            row.get("id"),
            row.get("run_id"),
            row.get("kind"),
            row.get("state"),
            f"{row.get('attempt_count')} / {row.get('max_attempts')}",
            row.get("owner_id"),
            row.get("available_at"),
            row.get("last_error_code"),
        )
        for row in snapshot.get("supervisor_actions", [])
        if isinstance(row, Mapping)
    ]
    escalation_rows = [
        (
            row.get("id"),
            row.get("run_id"),
            row.get("kind"),
            row.get("threshold_seconds"),
            row.get("channel"),
            row.get("state"),
            f"{row.get('attempt_count')} / {row.get('max_attempts')}",
            row.get("delivered_at"),
            row.get("last_error_code"),
        )
        for row in snapshot.get("escalations", [])
        if isinstance(row, Mapping)
    ]
    user_rows = [
        (
            row.get("handle"),
            row.get("display_name"),
            row.get("active"),
            len(row.get("krab_identity_ids", [])),
        )
        for row in snapshot.get("users", [])
        if isinstance(row, Mapping)
    ]
    project_rows = [
        (
            row.get("project_key"),
            row.get("display_name"),
            row.get("active"),
            row.get("context_version"),
        )
        for row in snapshot.get("projects", [])
        if isinstance(row, Mapping)
    ]
    profile_rows = [
        (
            row.get("name"),
            row.get("active"),
            row.get("profile_version"),
            row.get("runtime_adapter"),
            row.get("runtime_provider_grant"),
            row.get("krab_handle"),
            row.get("network_mode"),
        )
        for row in snapshot.get("profiles", [])
        if isinstance(row, Mapping)
    ]
    work_rows = [
        (
            row.get("id"),
            row.get("project_key"),
            row.get("untrusted_title"),
            row.get("state"),
            row.get("generation"),
            row.get("thread_version"),
            row.get("active_run_count"),
        )
        for row in snapshot.get("work_items", [])
        if isinstance(row, Mapping)
    ]
    assignment_rows = [
        (
            row.get("id"),
            row.get("untrusted_title"),
            row.get("state"),
            row.get("version"),
            row.get("target_profile_name"),
            row.get("work_item_id"),
            row.get("lease_expired"),
        )
        for row in snapshot.get("assignments", [])
        if isinstance(row, Mapping)
    ]
    attempt_rows = [
        (
            row.get("id"),
            row.get("run_id"),
            row.get("state"),
            row.get("runtime_adapter"),
            row.get("owner_id"),
            row.get("lease_expired"),
            bool(row.get("cancel_requested_at")),
        )
        for row in snapshot.get("attempts", [])
        if isinstance(row, Mapping)
    ]
    interaction_rows: list[tuple[object, ...]] = []
    for row in snapshot.get("interactions", []):
        if not isinstance(row, Mapping):
            continue
        action: object = "—"
        if row.get("state") == "open":
            action = _action_form(
                root,
                "answer",
                csrf_token,
                {
                    "interaction_id": row.get("id", ""),
                    "expected_version": row.get("version", ""),
                },
                (
                    '<input name="response_item_id" required maxlength="36" '
                    'aria-label="Krab response item ID" '
                    'placeholder="Krab response item ID">'
                ),
                "Answer",
            )
        interaction_rows.append(
            (
                row.get("id"),
                row.get("run_id"),
                row.get("kind"),
                row.get("state"),
                row.get("recipient_krab_handle"),
                action,
            )
        )
    result_rows = [
        (
            row.get("id"),
            row.get("attempt_id"),
            row.get("run_id"),
            row.get("changed_paths"),
            row.get("changed_bytes"),
            row.get("content_sha256"),
        )
        for row in snapshot.get("retained_results", [])
        if isinstance(row, Mapping)
    ]
    role_rows = [
        (row.get("project_key"), row.get("krab_handle"), row.get("role"))
        for row in krab.get("roles", [])
        if isinstance(row, Mapping)
    ]
    claim_rows: list[tuple[object, ...]] = []
    for row in krab.get("work_claims", []):
        if not isinstance(row, Mapping):
            continue
        assignee = row.get("assignee")
        assignee = assignee if isinstance(assignee, Mapping) else {}
        local_item = row.get("local_work_item")
        local_item = local_item if isinstance(local_item, Mapping) else {}
        claim_rows.append(
            (
                row.get("claim_id"),
                row.get("project_key"),
                row.get("kind"),
                row.get("status"),
                row.get("version"),
                row.get("review_round"),
                assignee.get("handle"),
                row.get("external_id"),
                local_item.get("untrusted_title"),
            )
        )
    authority_rows = [
        (
            row.get("run_id"),
            row.get("authority_name"),
            row.get("state"),
            row.get("available"),
            row.get("expired"),
            row.get("expires_at"),
        )
        for row in snapshot.get("run_authorities", [])
        if isinstance(row, Mapping)
    ]
    krab_error_rows = [
        (
            row.get("scope"),
            row.get("identity_id"),
            row.get("project_key"),
            row.get("kind"),
            row.get("error_type"),
            row.get("message"),
        )
        for row in krab.get("errors", [])
        if isinstance(row, Mapping)
    ]

    reconcile_form = _action_form(
        root,
        "reconcile",
        csrf_token,
        {},
        '<input name="run_id" placeholder="Optional run ID" maxlength="64">',
        "Reconcile",
    )
    receipt_html = ""
    if receipt is not None:
        receipt_html = (
            '<p class="receipt" role="status">'
            f"{_escape(receipt.title())} action accepted.</p>"
        )
    refresh_link = f'<a href="{_escape(root)}?refresh_krab=1">Refresh Krab metadata</a>'
    header = (
        '<header><p class="sub">Local, redacted, explicit</p>'
        "<h1>Magic Glass Box control room</h1>"
        f'<p class="sub">Snapshot {_escape(metadata.get("generated_at"))} · '
        f"Krab {_escape(krab.get('status'))} · {refresh_link}</p></header>"
    )
    cards = (
        '<div class="cards">'
        + "".join(
            (
                _card(summary.get("attention", 0), "attention"),
                _card(run_summary.get("active", 0), "active runs"),
                _card(run_summary.get("terminal", 0), "terminal runs"),
                _card(summary.get("work_items", 0), "work items"),
            )
        )
        + "</div>"
    )
    sections = (
        f"<section><h2>Recovery</h2>{reconcile_form}</section>",
        _section(
            "Attention",
            _table(
                ("Severity", "Condition", "Subject", "Run", "Age (seconds)"),
                attention_rows,
            ),
        ),
        _section(
            "Supervisor actions",
            _table(
                (
                    "ID",
                    "Run",
                    "Kind",
                    "State",
                    "Attempts",
                    "Owner",
                    "Available",
                    "Error",
                ),
                supervisor_action_rows,
            ),
        ),
        _section(
            "Escalations",
            _table(
                (
                    "ID",
                    "Run",
                    "Kind",
                    "Threshold",
                    "Channel",
                    "State",
                    "Attempts",
                    "Delivered",
                    "Error",
                ),
                escalation_rows,
            ),
        ),
        _section(
            "Runs",
            _table(
                (
                    "ID",
                    "State",
                    "Version",
                    "Profile",
                    "Project",
                    "Untrusted work title",
                    "Lineage",
                    "Autonomy",
                    "Actions",
                ),
                run_rows,
            ),
        ),
        _section(
            "Attempts",
            _table(
                (
                    "ID",
                    "Run",
                    "State",
                    "Runtime",
                    "Owner",
                    "Lease expired",
                    "Cancel requested",
                ),
                attempt_rows,
            ),
        ),
        _section(
            "Interactions",
            _table(
                ("ID", "Run", "Kind", "State", "Recipient", "Action"),
                interaction_rows,
            ),
        ),
        _section(
            "Retained results",
            _table(
                ("ID", "Attempt", "Run", "Changed paths", "Changed bytes", "SHA-256"),
                result_rows,
            ),
        ),
        _section(
            "Work items",
            _table(
                (
                    "ID",
                    "Project",
                    "Untrusted title",
                    "State",
                    "Generation",
                    "Thread version",
                    "Active runs",
                ),
                work_rows,
            ),
        ),
        _section(
            "Assignments",
            _table(
                (
                    "ID",
                    "Untrusted title",
                    "State",
                    "Version",
                    "Target profile",
                    "Work item",
                    "Lease expired",
                ),
                assignment_rows,
            ),
        ),
        _section(
            "Provider authority",
            _table(
                (
                    "Name",
                    "State",
                    "Version",
                    "Available",
                    "Provider",
                    "Model",
                    "Action",
                ),
                grant_rows,
            ),
        ),
        _section(
            "Run-bound authority",
            _table(
                ("Run", "Authority", "State", "Available", "Expired", "Expires"),
                authority_rows,
            ),
        ),
        _section(
            "Projects",
            _table(("Key", "Name", "Active", "Context version"), project_rows),
        ),
        _section(
            "Profiles",
            _table(
                (
                    "Name",
                    "Active",
                    "Version",
                    "Runtime",
                    "Provider route",
                    "Krab user",
                    "Network",
                ),
                profile_rows,
            ),
        ),
        _section(
            "Users",
            _table(("Handle", "Display name", "Active", "Krab identities"), user_rows),
        ),
        _section("Krab roles", _table(("Project", "Handle", "Role"), role_rows)),
        _section(
            "Krab work claims",
            _table(
                (
                    "Claim",
                    "Project",
                    "Kind",
                    "Status",
                    "Version",
                    "Review round",
                    "Assignee",
                    "External ID",
                    "Untrusted title",
                ),
                claim_rows,
            ),
        ),
        _section(
            "Krab refresh errors",
            _table(
                ("Scope", "Identity", "Project", "Kind", "Type", "Message"),
                krab_error_rows,
            ),
        ),
    )
    body = "\n".join(
        (
            "<!doctype html>",
            '<html lang="en"><head><meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width,initial-scale=1">',
            "<title>Magic Glass Box control room</title>",
            f'<style nonce="{_escape(nonce)}">{CONTROL_ROOM_CSS}</style>',
            "</head><body><main>",
            header,
            receipt_html,
            cards,
            *sections,
            "</main></body></html>",
        )
    )
    return body.encode("utf-8"), nonce


def _valid_token(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or not TOKEN_MIN_LENGTH <= len(value) <= TOKEN_MAX_LENGTH
        or any(
            character
            not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            for character in value
        )
    ):
        raise ValueError(f"{label} must be a strong URL-safe token")
    return value


def _positive_version(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValidationError("expected_version must be a positive integer") from exc
    if parsed < 1 or str(parsed) != value:
        raise ValidationError("expected_version must be a positive integer")
    return parsed


def _required(fields: Mapping[str, str], name: str, maximum: int) -> str:
    value = fields[name]
    if not value or len(value.encode("utf-8")) > maximum:
        raise ValidationError(f"{name} is invalid")
    return value


class ControlRoomHTTPServer(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = False

    def __init__(
        self,
        server_address: tuple[str, int],
        control: ControlPlaneLike,
        coordinator: CoordinatorLike,
        *,
        access_token: str,
        csrf_token: str,
        history_limit: int,
    ) -> None:
        self.control = control
        self.actions = OperatorActions(control, coordinator)
        self.access_token = access_token
        self.csrf_token = csrf_token
        self.history_limit = history_limit
        super().__init__(server_address, _ControlRoomHandler)
        bound_host, bound_port = self.server_address[:2]
        display_host = f"[{bound_host}]" if ":" in bound_host else bound_host
        self.origin = f"http://{display_host}:{bound_port}"
        self.authority = f"{display_host}:{bound_port}"
        self.root_path = f"/{self.access_token}/"
        self.control_url = self.origin + self.root_path


class _ControlRoomHTTPServerV6(ControlRoomHTTPServer):
    address_family = socket.AF_INET6


class _ControlRoomHandler(BaseHTTPRequestHandler):
    server: ControlRoomHTTPServer
    protocol_version = "HTTP/1.1"
    # StreamRequestHandler applies this to the accepted socket before parsing
    # request headers. It therefore bounds incomplete headers, partial bodies,
    # idle keep-alive connections, and stalled response writes.
    timeout = CONTROL_ROOM_SOCKET_TIMEOUT_SECONDS

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _security_headers(self, *, nonce: str | None = None) -> dict[str, str]:
        style = "'none'" if nonce is None else f"'nonce-{nonce}'"
        return {
            "Cache-Control": "no-store",
            "Content-Security-Policy": (
                "default-src 'none'; base-uri 'none'; form-action 'self'; "
                f"frame-ancestors 'none'; style-src {style}"
            ),
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
        }

    def _send(
        self,
        status: HTTPStatus,
        body: bytes,
        *,
        content_type: str = "text/html; charset=utf-8",
        headers: Mapping[str, str] | None = None,
        nonce: str | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for name, value in self._security_headers(nonce=nonce).items():
            self.send_header(name, value)
        if headers is not None:
            for name, value in headers.items():
                self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _plain_error(self, status: HTTPStatus, message: str) -> None:
        self.close_connection = True
        body = (
            "<!doctype html><html><head><title>Control room error</title></head>"
            f"<body><h1>{status.value} {_escape(status.phrase)}</h1>"
            f"<p>{_escape(message)}</p></body></html>"
        ).encode()
        self._send(status, body, headers={"Connection": "close"})

    def _valid_host(self) -> bool:
        return secrets.compare_digest(
            self.headers.get("Host", ""), self.server.authority
        )

    def _action_from_path(self, path: str) -> str | None:
        parts = path.split("/")
        if len(parts) != 4 or parts[0] or parts[2] != "actions":
            return None
        if not secrets.compare_digest(parts[1], self.server.access_token):
            return None
        return parts[3] if parts[3] in ACTION_FIELDS else None

    def do_GET(self) -> None:  # noqa: N802
        if not self._valid_host():
            self._plain_error(HTTPStatus.BAD_REQUEST, "Invalid host header.")
            return
        parsed = urlsplit(self.path)
        if not secrets.compare_digest(parsed.path, self.server.root_path):
            self._plain_error(HTTPStatus.NOT_FOUND, "Not found.")
            return
        try:
            pairs = parse_qsl(
                parsed.query,
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=2,
            )
            if len({name for name, _value in pairs}) != len(pairs):
                raise ValueError("duplicate query field")
            query = dict(pairs)
            if not set(query).issubset({"refresh_krab", "receipt"}):
                raise ValueError("unknown query field")
            refresh = query.get("refresh_krab", "0")
            if refresh not in {"0", "1"}:
                raise ValueError("invalid refresh flag")
            receipt = query.get("receipt")
            if receipt is not None and receipt not in ACTION_FIELDS:
                raise ValueError("invalid receipt")
            snapshot = self.server.control.control_room_snapshot(
                history_limit=self.server.history_limit,
                refresh_krab=refresh == "1",
            )
            body, nonce = _render_control_room(
                snapshot,
                root=self.server.root_path,
                csrf_token=self.server.csrf_token,
                receipt=receipt,
            )
        except (TypeError, ValueError, ValidationError):
            self._plain_error(HTTPStatus.BAD_REQUEST, "Invalid control-room request.")
            return
        except Exception:
            self._plain_error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "The redacted control-room snapshot is unavailable.",
            )
            return
        self._send(HTTPStatus.OK, body, nonce=nonce)

    def do_POST(self) -> None:  # noqa: N802
        if not self._valid_host():
            self._plain_error(HTTPStatus.BAD_REQUEST, "Invalid host header.")
            return
        parsed = urlsplit(self.path)
        action = self._action_from_path(parsed.path)
        if action is None or parsed.query:
            self._plain_error(HTTPStatus.NOT_FOUND, "Not found.")
            return
        if not secrets.compare_digest(
            self.headers.get("Origin", ""), self.server.origin
        ):
            self._plain_error(HTTPStatus.FORBIDDEN, "Same-origin request required.")
            return
        media_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip()
        if media_type != "application/x-www-form-urlencoded":
            self._plain_error(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "Invalid form type.")
            return
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._plain_error(HTTPStatus.BAD_REQUEST, "Invalid form length.")
            return
        if not 0 < length <= MAX_FORM_BYTES:
            self._plain_error(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Invalid form length."
            )
            return
        try:
            raw = self.rfile.read(length).decode("utf-8", "strict")
            pairs = parse_qsl(
                raw,
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=MAX_FORM_FIELDS,
            )
            if len({name for name, _value in pairs}) != len(pairs):
                raise ValidationError("duplicate form field")
            fields = dict(pairs)
            if frozenset(fields) != ACTION_FIELDS[action]:
                raise ValidationError("form fields do not match the action")
            if not secrets.compare_digest(fields["csrf_token"], self.server.csrf_token):
                self._plain_error(HTTPStatus.FORBIDDEN, "Invalid CSRF token.")
                return
            self._execute(action, fields)
        except UnicodeDecodeError:
            self._plain_error(HTTPStatus.BAD_REQUEST, "Form must be UTF-8.")
            return
        except (KeyError, TypeError, ValueError, ValidationError):
            self._plain_error(HTTPStatus.BAD_REQUEST, "Invalid action request.")
            return
        except NotFoundError:
            self._plain_error(HTTPStatus.NOT_FOUND, "Action target was not found.")
            return
        except (ConcurrentUpdateError, ConflictError):
            self._plain_error(
                HTTPStatus.CONFLICT,
                "State changed or the action is not currently allowed. "
                "Refresh and retry.",
            )
            return
        except Exception:
            self._plain_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "The action could not be completed.",
            )
            return
        location = self.server.root_path + "?receipt=" + quote(action, safe="")
        self._send(
            HTTPStatus.SEE_OTHER,
            b"",
            headers={"Location": location},
        )

    def _execute(self, action: str, fields: Mapping[str, str]) -> None:
        if action == "reconcile":
            run_id = fields["run_id"] or None
            if run_id is not None:
                _required(fields, "run_id", 64)
            self.server.actions.reconcile(run_id)
            return
        expected_version = _positive_version(fields["expected_version"])
        if action == "answer":
            self.server.actions.answer(
                _required(fields, "interaction_id", 64),
                response_item_id=_required(fields, "response_item_id", 36),
                expected_version=expected_version,
            )
            return
        requested_by = _required(fields, "requested_by", 128)
        if action == "cancel":
            self.server.actions.cancel(
                _required(fields, "run_id", 64),
                expected_version=expected_version,
                requested_by=requested_by,
                reason=_required(fields, "reason", 4096),
            )
        elif action == "retry":
            self.server.actions.retry(
                _required(fields, "run_id", 64),
                expected_version=expected_version,
                requested_by=requested_by,
            )
        elif action == "reassign":
            self.server.actions.reassign(
                _required(fields, "run_id", 64),
                profile_name=_required(fields, "profile_name", 128),
                expected_version=expected_version,
                requested_by=requested_by,
            )
        elif action == "revoke":
            self.server.actions.revoke(
                _required(fields, "grant_name", 128),
                expected_version=expected_version,
                requested_by=requested_by,
            )
        else:
            raise ValidationError("unknown action")

    def do_PUT(self) -> None:  # noqa: N802
        self._plain_error(HTTPStatus.METHOD_NOT_ALLOWED, "Method not allowed.")

    do_DELETE = do_PUT
    do_PATCH = do_PUT


def build_control_room_server(
    control: ControlPlaneLike,
    coordinator: CoordinatorLike,
    *,
    host: str = "127.0.0.1",
    port: int = 8766,
    history_limit: int = 25,
    access_token: str | None = None,
    csrf_token: str | None = None,
) -> ControlRoomHTTPServer:
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ValueError("control-room host must be a loopback IP address") from exc
    if not address.is_loopback:
        raise ValueError("control-room host must be a loopback IP address")
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ValueError("control-room port must be between 0 and 65535")
    if (
        isinstance(history_limit, bool)
        or not isinstance(history_limit, int)
        or not 0 <= history_limit <= 500
    ):
        raise ValueError("history_limit must be an integer between 0 and 500")
    access = _valid_token(
        access_token or secrets.token_urlsafe(32), label="access token"
    )
    csrf = _valid_token(csrf_token or secrets.token_urlsafe(32), label="CSRF token")
    if secrets.compare_digest(access, csrf):
        raise ValueError("access and CSRF tokens must be distinct")
    server_type = (
        _ControlRoomHTTPServerV6 if address.version == 6 else ControlRoomHTTPServer
    )
    return server_type(
        (host, port),
        control,
        coordinator,
        access_token=access,
        csrf_token=csrf,
        history_limit=history_limit,
    )
