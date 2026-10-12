from __future__ import annotations

import argparse
import io
import json
import os
import signal
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any
from unittest import mock

from magic_glass_box.cli import (
    InputError,
    _build_run_coordinator,
    _emit,
    _handle_sigterm,
    _json_file,
    _serve_control_room,
    _serve_supervisor,
    _TerminationRequested,
    build_parser,
    main,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    ProviderGrantSpec,
    SupervisionPolicy,
    WorkItemEnvelope,
)
from magic_glass_box.krab import ActivityPage


class FakeDatabase:
    def __init__(self) -> None:
        self.path = Path("fake-control-plane.db")
        self.calls: list[tuple[str, dict[str, object]]] = []

    def status(self) -> dict[str, object]:
        self.calls.append(("status", {}))
        return {"operation": "status"}

    def get_run(self, run_id: str) -> dict[str, object]:
        self.calls.append(("run.show", {"run_id": run_id}))
        return {"operation": "run.show", "run_id": run_id}

    def get_provider_grant(self, name: str) -> dict[str, object]:
        self.calls.append(("provider.show", {"name": name}))
        return {"operation": "provider.show", "name": name}

    def get_attempt_result(self, attempt_id: str) -> dict[str, object]:
        self.calls.append(("result.show", {"attempt_id": attempt_id}))
        return {
            "attempt_id": attempt_id,
            "content_sha256": "a" * 64,
            "operation": "result.show",
        }


class FakeControlPlane:
    def __init__(self) -> None:
        self.database = FakeDatabase()
        self.calls: list[tuple[str, dict[str, object]]] = []

    def _called(self, operation: str, **kwargs: object) -> dict[str, object]:
        self.calls.append((operation, kwargs))
        return {"operation": operation}

    def initialize(self) -> dict[str, object]:
        return self._called("init")

    def control_room_snapshot(self, **kwargs: object) -> dict[str, object]:
        return self._called("control-room.show", **kwargs)

    def create_user(self, handle: str, display_name: str | None) -> dict[str, object]:
        return self._called("user.add", handle=handle, display_name=display_name)

    def add_krab_instance(self, name: str, url: str) -> dict[str, object]:
        return self._called("krab.instance-add", name=name, url=url)

    def add_krab_identity(self, **kwargs: object) -> dict[str, object]:
        return self._called("krab.identity-add", **kwargs)

    def project_activity(self, **kwargs: object) -> ActivityPage:
        self.calls.append(("krab.activity", kwargs))
        return ActivityPage("box-project", 0, 0, ())

    def register_project(self, **kwargs: object) -> dict[str, object]:
        return self._called("project.add", **kwargs)

    def revise_project_context(self, **kwargs: object) -> dict[str, object]:
        return self._called("project.revise", **kwargs)

    def create_agent_profile(self, **kwargs: object) -> dict[str, object]:
        return self._called("profile.add", **kwargs)

    def revise_agent_profile(self, **kwargs: object) -> dict[str, object]:
        return self._called("profile.revise", **kwargs)

    def add_provider_grant(self, **kwargs: object) -> dict[str, object]:
        return self._called("provider.add", **kwargs)

    def revoke_provider_grant(self, name: str, **kwargs: object) -> dict[str, object]:
        return self._called("provider.revoke", name=name, **kwargs)

    def import_work_item(self, **kwargs: object) -> dict[str, object]:
        return self._called("work-item.import", **kwargs)

    def get_work_item(self, work_item_id: str) -> dict[str, object]:
        return self._called("work-item.show", work_item_id=work_item_id)

    def complete_work_item(
        self, work_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("work-item.complete", work_item_id=work_item_id, **kwargs)

    def reopen_work_item(
        self, work_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("work-item.reopen", work_item_id=work_item_id, **kwargs)

    def create_run(self, **kwargs: object) -> dict[str, object]:
        return self._called("run.create", **kwargs)

    def request_run_cancellation(
        self, run_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("run.cancel", run_id=run_id, **kwargs)

    def retry_run(self, run_id: str, **kwargs: object) -> dict[str, object]:
        return self._called("run.retry", run_id=run_id, **kwargs)

    def reassign_run(self, run_id: str, **kwargs: object) -> dict[str, object]:
        return self._called("run.reassign", run_id=run_id, **kwargs)


class FakeRunCoordinator:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def execute(
        self, run_id: str, *, runtime_adapter: str | None = None
    ) -> dict[str, object]:
        self.calls.append(
            (
                "execute",
                run_id
                if runtime_adapter is None
                else {"run_id": run_id, "runtime_adapter": runtime_adapter},
            )
        )
        return {"operation": "run.execute", "run_id": run_id}

    def reconcile(self, run_id: str | None = None) -> list[dict[str, object]]:
        self.calls.append(("reconcile", {} if run_id is None else run_id))
        return [{"operation": "run.reconcile"}]


class FakeSupervisor:
    def __init__(self) -> None:
        self.supervisor_id = "supervisor-test"
        self.run_calls = 0
        self.close_calls: list[bool] = []

    def run_forever(self) -> None:
        self.run_calls += 1
        raise KeyboardInterrupt

    def close(self, *, wait: bool = True) -> bool:
        self.close_calls.append(wait)
        return True


class FakeResultStore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def inspect(self, attempt_id: str, **kwargs: object) -> object:
        self.calls.append(("inspect", {"attempt_id": attempt_id, **kwargs}))
        return object()

    def apply(
        self, attempt_id: str, repository: Path, **kwargs: object
    ) -> dict[str, object]:
        self.calls.append(
            (
                "apply",
                {"attempt_id": attempt_id, "repository": repository, **kwargs},
            )
        )
        return {"operation": "result.apply", "attempt_id": attempt_id}


class CLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "control-plane.db"

    def invoke(self, *arguments: str) -> tuple[int, dict[str, object], str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = main(["--db", str(self.database), *arguments])
        output = stdout.getvalue() or stderr.getvalue()
        return status, json.loads(output), stderr.getvalue()

    def test_json_output_preserves_unicode_for_human_readability(self) -> None:
        stream = io.StringIO()
        _emit({"message": "café"}, stream=stream)
        self.assertEqual(stream.getvalue(), '{\n  "message": "café"\n}\n')

    def test_parser_exposes_the_documented_command_help_contract(self) -> None:
        parser = build_parser()
        self.assertEqual(parser.prog, "magic-glass-box")
        self.assertEqual(
            parser.description,
            "Magic Glass Box control-plane administration",
        )
        database_action = next(
            action for action in parser._actions if action.dest == "db"
        )
        self.assertEqual(database_action.help, "control-plane database path")

        expected = {
            (): (
                "command",
                True,
                (
                    ("init", "initialize the private state database"),
                    ("status", "validate state and show object counts"),
                    ("control-room", "inspect redacted operational state"),
                    ("user", "manage local users"),
                    ("krab", "manage and query Krab connections"),
                    ("project", "manage project context"),
                    ("profile", "manage agent profiles"),
                    ("provider", "manage scoped provider grants"),
                    ("work-item", "manage durable work items"),
                    ("run", "manage agent runs"),
                    ("supervisor", "run the autonomous control loop"),
                    ("result", "inspect and apply retained work"),
                ),
            ),
            ("user",): (
                "user_command",
                True,
                (("add", "add a local user"),),
            ),
            ("control-room",): (
                "control_room_command",
                True,
                (
                    ("show", "show one consistent redacted snapshot"),
                    ("serve", "serve the interactive loopback control room"),
                ),
            ),
            ("krab",): (
                "krab_command",
                True,
                (
                    ("instance-add", "add a Krab origin"),
                    (
                        "identity-add",
                        "verify and retain a Krab credential reference",
                    ),
                    ("activity", "read metadata-only project activity"),
                ),
            ),
            ("project",): (
                "project_command",
                True,
                (
                    ("add", "verify and register a visible Krab project"),
                    ("revise", "append an immutable project-context revision"),
                ),
            ),
            ("profile",): (
                "profile_command",
                True,
                (
                    ("add", "create a versioned agent profile"),
                    ("revise", "append an immutable profile revision"),
                ),
            ),
            ("provider",): (
                "provider_command",
                True,
                (
                    (
                        "add",
                        "add an immutable provider policy and credential reference",
                    ),
                    ("show", "show provider policy and lifecycle metadata"),
                    ("revoke", "revoke a provider grant"),
                ),
            ),
            ("work-item",): (
                "work_item_command",
                True,
                (
                    ("import", "create or update a normalized work item"),
                    ("show", "show work-item lifecycle metadata"),
                    ("complete", "seal completed work memory"),
                    ("reopen", "start a fresh work-memory generation"),
                ),
            ),
            ("run",): (
                "run_command",
                True,
                (
                    ("create", "create a pinned run record"),
                    ("cancel", "request stop for a queued or active run"),
                    (
                        "retry",
                        "create an audited successor with the same pinned profile",
                    ),
                    (
                        "reassign",
                        "create an audited successor for another profile",
                    ),
                    ("show", "show a run and its event stream"),
                    (
                        "execute",
                        "execute one queued run in its pinned sandbox",
                    ),
                    (
                        "reconcile",
                        "clean and fail runs interrupted by an orchestrator restart",
                    ),
                ),
            ),
            ("supervisor",): (
                "supervisor_command",
                True,
                (("run", "advance opted-in work until stopped"),),
            ),
            ("result",): (
                "result_command",
                True,
                (
                    ("show", "verify and show one retained attempt result"),
                    (
                        "apply",
                        "apply one retained result after conflict preflight",
                    ),
                ),
            ),
        }

        actual: dict[
            tuple[str, ...], tuple[str, bool, tuple[tuple[str, str | None], ...]]
        ] = {}
        required_options: dict[tuple[str, ...], tuple[str, ...]] = {}
        choices: dict[tuple[tuple[str, ...], str], tuple[str, ...]] = {}

        def collect(
            current: argparse.ArgumentParser, path: tuple[str, ...] = ()
        ) -> None:
            required = tuple(
                action.option_strings[0]
                for action in current._actions
                if action.required and action.option_strings
            )
            if required:
                required_options[path] = required
            for parser_action in current._actions:
                if (
                    not isinstance(parser_action, argparse._SubParsersAction)
                    and parser_action.choices is not None
                ):
                    choices[(path, parser_action.dest)] = tuple(parser_action.choices)
            subparsers = [
                action
                for action in current._actions
                if isinstance(action, argparse._SubParsersAction)
            ]
            if not subparsers:
                return
            self.assertEqual(len(subparsers), 1)
            action = subparsers[0]
            help_by_name = {
                choice.dest: choice.help for choice in action._choices_actions
            }
            actual[path] = (
                action.dest,
                action.required,
                tuple((name, help_by_name[name]) for name in action.choices),
            )
            for name, child in action.choices.items():
                collect(child, (*path, name))

        collect(parser)
        self.assertEqual(actual, expected)
        self.assertEqual(
            required_options,
            {
                ("krab", "instance-add"): ("--url",),
                ("krab", "identity-add"): ("--token-file",),
                ("krab", "activity"): ("--as-user",),
                ("project", "add"): (
                    "--instance",
                    "--as-user",
                    "--name",
                    "--context",
                    "--created-by",
                ),
                ("project", "revise"): ("--context", "--created-by"),
                ("profile", "add"): (
                    "--instance",
                    "--krab-user",
                    "--spec",
                    "--created-by",
                ),
                ("profile", "revise"): (
                    "--instance",
                    "--krab-user",
                    "--spec",
                    "--created-by",
                ),
                ("provider", "add"): (
                    "--policy",
                    "--credential-file",
                    "--created-by",
                ),
                ("provider", "revoke"): (
                    "--expected-version",
                    "--revoked-by",
                ),
                ("work-item", "import"): ("--project", "--imported-by"),
                ("work-item", "complete"): (
                    "--expected-thread-version",
                    "--completed-by",
                ),
                ("work-item", "reopen"): (
                    "--expected-thread-version",
                    "--reopened-by",
                ),
                ("run", "create"): (
                    "--project",
                    "--profile",
                    "--work-item",
                    "--requested-by",
                ),
                ("run", "cancel"): (
                    "--expected-version",
                    "--requested-by",
                    "--reason",
                ),
                ("run", "retry"): ("--expected-version", "--requested-by"),
                ("run", "reassign"): (
                    "--profile",
                    "--expected-version",
                    "--requested-by",
                ),
                ("result", "apply"): ("--repository",),
            },
        )
        self.assertEqual(choices, {})

    def test_init_user_and_status(self) -> None:
        status, initialized, _ = self.invoke("init")
        self.assertEqual(status, 0)
        self.assertEqual(initialized["schema_version"], 10)
        status, user, _ = self.invoke(
            "user", "add", "operator", "--display-name", "Local Operator"
        )
        self.assertEqual(status, 0)
        self.assertEqual(user["handle"], "operator")
        status, state, _ = self.invoke("status")
        self.assertEqual(status, 0)
        self.assertEqual(state["counts"]["users"], 1)
        status, room, _ = self.invoke("control-room", "show", "--history-limit", "0")
        self.assertEqual(status, 0)
        self.assertEqual(room["summary"]["users"], 1)
        self.assertEqual(room["snapshot"]["history_limit"], 0)

    def test_error_is_structured_and_nonzero(self) -> None:
        self.invoke("init")
        status, error, stderr = self.invoke("user", "add", "x")
        self.assertEqual(status, 2)
        self.assertIn("error", error)
        self.assertIn("ValidationError", stderr)

    def test_all_administrative_routes_dispatch(self) -> None:
        context = Path(self.temporary.name) / "context.json"
        context.write_text('{"purpose":"Build Magic Glass Box"}', encoding="utf-8")
        spec = Path(self.temporary.name) / "profile.json"
        spec.write_text(
            '{"image":"agent@sha256:abc","command":["agent"]}',
            encoding="utf-8",
        )
        work_item = Path(self.temporary.name) / "work-item.json"
        work_item.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "source": "github",
                    "external_id": "psyberone/magic-glass-box#42",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Import work items",
                    "brief": "Add a forge-neutral upsert boundary.",
                }
            ),
            encoding="utf-8",
        )
        provider_policy = Path(self.temporary.name) / "provider.json"
        provider_policy.write_text(
            json.dumps(
                {
                    "provider": "openai",
                    "adapter": "json_http",
                    "destination": "https://api.openai.com/v1/responses",
                    "method": "POST",
                    "model": "gpt-test",
                    "credential_header": "Authorization",
                    "credential_prefix": "Bearer ",
                    "static_headers": {},
                    "request_bytes": 32768,
                    "response_bytes": 1048576,
                    "timeout_ms": 30000,
                    "max_concurrency": 2,
                }
            ),
            encoding="utf-8",
        )
        parsed_provider_policy = ProviderGrantSpec.from_mapping(
            json.loads(provider_policy.read_text(encoding="utf-8"))
        )
        supervision_policy = Path(self.temporary.name) / "supervision.json"
        supervision_policy.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "auto_execute": True,
                    "retry_backoff_seconds": [5, 30, 300],
                    "interaction_escalation_seconds": [1800, 7200],
                    "attempt_idle_escalation_seconds": [1800],
                    "attempt_lease_expired_escalation_seconds": [0],
                    "other_attention_escalation_seconds": [3600],
                }
            ),
            encoding="utf-8",
        )
        parsed_supervision_policy = SupervisionPolicy.from_mapping(
            json.loads(supervision_policy.read_text(encoding="utf-8"))
        )
        fake = FakeControlPlane()
        cases: tuple[
            tuple[
                str,
                list[str],
                tuple[str, dict[str, object]],
                dict[str, object],
            ],
            ...,
        ] = (
            ("status", ["status"], ("status", {}), {"operation": "status"}),
            (
                "control-room.show",
                [
                    "control-room",
                    "show",
                    "--history-limit",
                    "17",
                    "--refresh-krab",
                ],
                (
                    "control-room.show",
                    {"history_limit": 17, "refresh_krab": True},
                ),
                {"operation": "control-room.show"},
            ),
            (
                "krab.instance-add",
                ["krab", "instance-add", "local", "--url", "http://localhost:8765"],
                (
                    "krab.instance-add",
                    {"name": "local", "url": "http://localhost:8765"},
                ),
                {"operation": "krab.instance-add"},
            ),
            (
                "krab.identity-add",
                [
                    "krab",
                    "identity-add",
                    "local",
                    "--token-file",
                    "/private/token",
                    "--local-user",
                    "operator",
                ],
                (
                    "krab.identity-add",
                    {
                        "instance_name": "local",
                        "credential_path": Path("/private/token"),
                        "local_user_handle": "operator",
                    },
                ),
                {"operation": "krab.identity-add"},
            ),
            (
                "krab.activity",
                [
                    "krab",
                    "activity",
                    "local",
                    "box-project",
                    "--as-user",
                    "agent_one",
                    "--after",
                    "7",
                    "--limit",
                    "23",
                    "--wait",
                    "9",
                ],
                (
                    "krab.activity",
                    {
                        "instance_name": "local",
                        "krab_handle": "agent_one",
                        "project_key": "box-project",
                        "after": 7,
                        "limit": 23,
                        "wait": 9,
                    },
                ),
                {
                    "after": 0,
                    "events": [],
                    "next_cursor": 0,
                    "project_key": "box-project",
                },
            ),
            (
                "project.add",
                [
                    "project",
                    "add",
                    "box-project",
                    "--instance",
                    "local",
                    "--as-user",
                    "agent_one",
                    "--name",
                    "Magic Glass Box",
                    "--context",
                    str(context),
                    "--created-by",
                    "operator",
                ],
                (
                    "project.add",
                    {
                        "instance_name": "local",
                        "krab_handle": "agent_one",
                        "project_key": "box-project",
                        "display_name": "Magic Glass Box",
                        "context": ProjectContext.from_mapping(
                            {"purpose": "Build Magic Glass Box"}
                        ),
                        "created_by_handle": "operator",
                    },
                ),
                {"operation": "project.add"},
            ),
            (
                "project.revise",
                [
                    "project",
                    "revise",
                    "box-project",
                    "--context",
                    str(context),
                    "--created-by",
                    "operator",
                ],
                (
                    "project.revise",
                    {
                        "project_key": "box-project",
                        "context": ProjectContext.from_mapping(
                            {"purpose": "Build Magic Glass Box"}
                        ),
                        "created_by_handle": "operator",
                    },
                ),
                {"operation": "project.revise"},
            ),
            (
                "profile.add",
                [
                    "profile",
                    "add",
                    "Worker",
                    "--instance",
                    "local",
                    "--krab-user",
                    "agent_one",
                    "--spec",
                    str(spec),
                    "--created-by",
                    "operator",
                ],
                (
                    "profile.add",
                    {
                        "name": "Worker",
                        "instance_name": "local",
                        "krab_handle": "agent_one",
                        "spec": AgentProfileSpec.from_mapping(
                            {"image": "agent@sha256:abc", "command": ["agent"]}
                        ),
                        "created_by_handle": "operator",
                    },
                ),
                {"operation": "profile.add"},
            ),
            (
                "profile.revise",
                [
                    "profile",
                    "revise",
                    "Worker",
                    "--instance",
                    "local",
                    "--krab-user",
                    "agent_one",
                    "--spec",
                    str(spec),
                    "--created-by",
                    "operator",
                ],
                (
                    "profile.revise",
                    {
                        "name": "Worker",
                        "instance_name": "local",
                        "krab_handle": "agent_one",
                        "spec": AgentProfileSpec.from_mapping(
                            {"image": "agent@sha256:abc", "command": ["agent"]}
                        ),
                        "created_by_handle": "operator",
                    },
                ),
                {"operation": "profile.revise"},
            ),
            (
                "provider.add",
                [
                    "provider",
                    "add",
                    "openai-main",
                    "--policy",
                    str(provider_policy),
                    "--credential-file",
                    "/private/openai.secret",
                    "--expires-at",
                    "2027-01-01T00:00:00Z",
                    "--created-by",
                    "operator",
                ],
                (
                    "provider.add",
                    {
                        "name": "openai-main",
                        "spec": parsed_provider_policy,
                        "credential_path": Path("/private/openai.secret"),
                        "expires_at": "2027-01-01T00:00:00Z",
                        "created_by_handle": "operator",
                    },
                ),
                {"operation": "provider.add"},
            ),
            (
                "provider.show",
                ["provider", "show", "openai-main"],
                ("provider.show", {"name": "openai-main"}),
                {"operation": "provider.show", "name": "openai-main"},
            ),
            (
                "provider.revoke",
                [
                    "provider",
                    "revoke",
                    "openai-main",
                    "--expected-version",
                    "1",
                    "--revoked-by",
                    "operator",
                ],
                (
                    "provider.revoke",
                    {
                        "name": "openai-main",
                        "expected_version": 1,
                        "revoked_by_handle": "operator",
                    },
                ),
                {"operation": "provider.revoke"},
            ),
            (
                "work-item.import",
                [
                    "work-item",
                    "import",
                    str(work_item),
                    "--project",
                    "box-project",
                    "--imported-by",
                    "operator",
                ],
                (
                    "work-item.import",
                    {
                        "project_key": "box-project",
                        "envelope": WorkItemEnvelope.from_mapping(
                            json.loads(work_item.read_text(encoding="utf-8"))
                        ),
                        "imported_by_handle": "operator",
                    },
                ),
                {"operation": "work-item.import"},
            ),
            (
                "work-item.show",
                ["work-item", "show", "wki_" + "6" * 32],
                (
                    "work-item.show",
                    {"work_item_id": "wki_" + "6" * 32},
                ),
                {"operation": "work-item.show"},
            ),
            (
                "work-item.complete",
                [
                    "work-item",
                    "complete",
                    "wki_" + "6" * 32,
                    "--expected-thread-version",
                    "4",
                    "--completed-by",
                    "operator",
                    "--reason",
                    "Acceptance checks passed.",
                ],
                (
                    "work-item.complete",
                    {
                        "work_item_id": "wki_" + "6" * 32,
                        "expected_thread_version": 4,
                        "completed_by_handle": "operator",
                        "reason": "Acceptance checks passed.",
                    },
                ),
                {"operation": "work-item.complete"},
            ),
            (
                "work-item.reopen",
                [
                    "work-item",
                    "reopen",
                    "wki_" + "6" * 32,
                    "--expected-thread-version",
                    "5",
                    "--reopened-by",
                    "operator",
                ],
                (
                    "work-item.reopen",
                    {
                        "work_item_id": "wki_" + "6" * 32,
                        "expected_thread_version": 5,
                        "reopened_by_handle": "operator",
                    },
                ),
                {"operation": "work-item.reopen"},
            ),
            (
                "run.create",
                [
                    "run",
                    "create",
                    "--project",
                    "box-project",
                    "--profile",
                    "Worker",
                    "--work-item",
                    "wki_" + "6" * 32,
                    "--requested-by",
                    "operator",
                    "--source-revision",
                    "abc123",
                    "--krab-session",
                    "ses_" + "4" * 32,
                    "--krab-claim",
                    "clm_" + "5" * 32,
                    "--supervision-policy",
                    str(supervision_policy),
                ],
                (
                    "run.create",
                    {
                        "project_key": "box-project",
                        "profile_name": "Worker",
                        "work_item_id": "wki_" + "6" * 32,
                        "requested_by_handle": "operator",
                        "source_revision": "abc123",
                        "krab_session_id": "ses_" + "4" * 32,
                        "krab_claim_id": "clm_" + "5" * 32,
                        "supervision_policy": parsed_supervision_policy,
                    },
                ),
                {"operation": "run.create"},
            ),
            (
                "run.show",
                ["run", "show", "run_123"],
                ("run.show", {"run_id": "run_123"}),
                {"operation": "run.show", "run_id": "run_123"},
            ),
            (
                "run.retry",
                [
                    "run",
                    "retry",
                    "run_123",
                    "--expected-version",
                    "4",
                    "--requested-by",
                    "operator",
                ],
                (
                    "run.retry",
                    {
                        "run_id": "run_123",
                        "expected_run_version": 4,
                        "requested_by_handle": "operator",
                    },
                ),
                {"operation": "run.retry"},
            ),
            (
                "run.reassign",
                [
                    "run",
                    "reassign",
                    "run_123",
                    "--profile",
                    "Reviewer",
                    "--expected-version",
                    "5",
                    "--requested-by",
                    "operator",
                ],
                (
                    "run.reassign",
                    {
                        "run_id": "run_123",
                        "profile_name": "Reviewer",
                        "expected_run_version": 5,
                        "requested_by_handle": "operator",
                    },
                ),
                {"operation": "run.reassign"},
            ),
        )
        for expected, arguments, expected_call, expected_result in cases:
            with self.subTest(expected=expected):
                stdout = io.StringIO()
                with (
                    mock.patch("magic_glass_box.cli.ControlPlane", return_value=fake),
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(json.loads(stdout.getvalue()), expected_result)
                if expected in {"status", "run.show", "provider.show"}:
                    self.assertEqual(fake.database.calls[-1], expected_call)
                else:
                    self.assertEqual(fake.calls[-1], expected_call)

    def test_parser_contract_and_activity_defaults(self) -> None:
        parser = build_parser()
        self.assertEqual(parser.prog, "magic-glass-box")
        self.assertEqual(
            parser.description, "Magic Glass Box control-plane administration"
        )
        parsed = parser.parse_args(
            [
                "--db",
                str(self.database),
                "krab",
                "activity",
                "local",
                "box-project",
                "--as-user",
                "agent_one",
            ]
        )
        self.assertEqual(parsed.db, self.database)
        self.assertEqual(parsed.after, 0)
        self.assertEqual(parsed.limit, 50)
        self.assertEqual(parsed.wait, 0)
        self.assertEqual(parsed.krab_handle, "agent_one")

        with (
            redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as missing_importer,
        ):
            parser.parse_args(
                [
                    "work-item",
                    "import",
                    "work-item.json",
                    "--project",
                    "box-project",
                ]
            )
        self.assertEqual(missing_importer.exception.code, 2)

        timed = parser.parse_args(
            ["run", "execute", "run_" + "1" * 32, "--run-timeout", "12.5"]
        )
        self.assertEqual(timed.run_timeout, 12.5)
        default_timed = parser.parse_args(["run", "execute", "run_" + "1" * 32])
        self.assertEqual(default_timed.run_timeout, 21_600.0)
        help_output = io.StringIO()
        with redirect_stdout(help_output), self.assertRaises(SystemExit) as help_exit:
            parser.parse_args(["run", "execute", "run_" + "1" * 32, "--help"])
        self.assertEqual(help_exit.exception.code, 0)
        self.assertIn(
            "maximum agent runtime in seconds (default: 21600)",
            help_output.getvalue(),
        )
        room = parser.parse_args(["control-room", "show"])
        self.assertEqual(room.history_limit, 25)
        self.assertFalse(room.refresh_krab)
        served = parser.parse_args(["control-room", "serve"])
        self.assertEqual(served.host, "127.0.0.1")
        self.assertEqual(served.port, 8766)
        self.assertEqual(served.history_limit, 25)
        supervised = parser.parse_args(["supervisor", "run"])
        self.assertEqual(supervised.poll_interval, 2.0)
        self.assertEqual(supervised.max_workers, 4)
        self.assertEqual(supervised.action_lease_seconds, 60)
        self.assertEqual(supervised.run_timeout, 21_600.0)
        for invalid_limit in ("-1", "501", "1.5"):
            with (
                self.subTest(invalid_limit=invalid_limit),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as invalid,
            ):
                parser.parse_args(
                    ["control-room", "show", "--history-limit", invalid_limit]
                )
            self.assertEqual(invalid.exception.code, 2)
        for invalid_port in ("-1", "65536", "1.5"):
            with (
                self.subTest(invalid_port=invalid_port),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as invalid,
            ):
                parser.parse_args(["control-room", "serve", "--port", invalid_port])
            self.assertEqual(invalid.exception.code, 2)
        for invalid_timeout in ("0", "-1", "nan", "inf", "not-a-number"):
            with (
                self.subTest(invalid_timeout=invalid_timeout),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as invalid,
            ):
                parser.parse_args(
                    [
                        "run",
                        "execute",
                        "run_" + "1" * 32,
                        "--run-timeout",
                        invalid_timeout,
                    ]
                )
            self.assertEqual(invalid.exception.code, 2)
        for option, invalid_values in (
            ("--max-workers", ("0", "33", "1.5")),
            ("--action-lease-seconds", ("9", "3601", "1.5")),
        ):
            for invalid_value in invalid_values:
                with (
                    self.subTest(option=option, invalid_value=invalid_value),
                    redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit) as invalid,
                ):
                    parser.parse_args(["supervisor", "run", option, invalid_value])
                self.assertEqual(invalid.exception.code, 2)

        root = Path(self.temporary.name)
        home = root / "home"
        cases = (
            (
                {"MAGIC_GLASS_BOX_DB": str(root / "current.db")},
                root / "current.db",
            ),
            ({"HEX_DB": str(root / "legacy.db")}, root / "legacy.db"),
            (
                {
                    "MAGIC_GLASS_BOX_DB": str(root / "current.db"),
                    "HEX_DB": str(root / "legacy.db"),
                },
                root / "current.db",
            ),
            (
                {"MAGIC_GLASS_BOX_STATE_DIR": str(root / "current-state")},
                root / "current-state" / "control-plane.db",
            ),
            (
                {"HEX_STATE_DIR": str(root / "legacy-state")},
                root / "legacy-state" / "hex.db",
            ),
            (
                {"XDG_STATE_HOME": str(root / "xdg")},
                root / "xdg" / "magic-glass-box" / "control-plane.db",
            ),
            (
                {},
                home / ".local" / "state" / "magic-glass-box" / "control-plane.db",
            ),
        )
        for environment, expected in cases:
            isolated_environment = dict(environment)
            mutant = os.environ.get("MUTANT_UNDER_TEST")
            if mutant is not None:
                isolated_environment["MUTANT_UNDER_TEST"] = mutant
            with (
                self.subTest(environment=environment),
                mock.patch.dict(os.environ, isolated_environment, clear=True),
                mock.patch("magic_glass_box.storage.core.Path.home", return_value=home),
            ):
                parsed = build_parser().parse_args(["status"])
            self.assertEqual(parsed.db, expected)

    def test_complete_parser_surface_is_an_explicit_contract(self) -> None:
        parser = build_parser()

        def command_help(
            current: argparse.ArgumentParser,
        ) -> tuple[dict[str, str | None], dict[str, argparse.ArgumentParser]]:
            subcommands = next(
                (
                    action
                    for action in current._actions
                    if isinstance(action, argparse._SubParsersAction)
                ),
                None,
            )
            if subcommands is None:
                return {}, {}
            helps = {
                choice.dest: choice.help for choice in subcommands._choices_actions
            }
            return helps, dict(subcommands.choices)

        root_help, root_commands = command_help(parser)
        self.assertEqual(
            root_help,
            {
                "init": "initialize the private state database",
                "status": "validate state and show object counts",
                "control-room": "inspect redacted operational state",
                "user": "manage local users",
                "krab": "manage and query Krab connections",
                "project": "manage project context",
                "profile": "manage agent profiles",
                "provider": "manage scoped provider grants",
                "work-item": "manage durable work items",
                "run": "manage agent runs",
                "supervisor": "run the autonomous control loop",
                "result": "inspect and apply retained work",
            },
        )
        expected_nested = {
            "control-room": {
                "show": "show one consistent redacted snapshot",
                "serve": "serve the interactive loopback control room",
            },
            "user": {"add": "add a local user"},
            "krab": {
                "instance-add": "add a Krab origin",
                "identity-add": "verify and retain a Krab credential reference",
                "activity": "read metadata-only project activity",
            },
            "project": {
                "add": "verify and register a visible Krab project",
                "revise": "append an immutable project-context revision",
            },
            "profile": {
                "add": "create a versioned agent profile",
                "revise": "append an immutable profile revision",
            },
            "provider": {
                "add": "add an immutable provider policy and credential reference",
                "show": "show provider policy and lifecycle metadata",
                "revoke": "revoke a provider grant",
            },
            "work-item": {
                "import": "create or update a normalized work item",
                "show": "show work-item lifecycle metadata",
                "complete": "seal completed work memory",
                "reopen": "start a fresh work-memory generation",
            },
            "run": {
                "create": "create a pinned run record",
                "cancel": "request stop for a queued or active run",
                "retry": ("create an audited successor with the same pinned profile"),
                "reassign": "create an audited successor for another profile",
                "show": "show a run and its event stream",
                "execute": "execute one queued run in its pinned sandbox",
                "reconcile": (
                    "clean and fail runs interrupted by an orchestrator restart"
                ),
            },
            "supervisor": {"run": "advance opted-in work until stopped"},
            "result": {
                "show": "verify and show one retained attempt result",
                "apply": "apply one retained result after conflict preflight",
            },
        }
        for command, expected_help in expected_nested.items():
            with self.subTest(command=command):
                nested_help, _ = command_help(root_commands[command])
                self.assertEqual(nested_help, expected_help)

        db_action = next(action for action in parser._actions if action.dest == "db")
        self.assertEqual(db_action.option_strings, ["--db"])
        self.assertIs(db_action.type, Path)
        self.assertIsInstance(db_action.default, Path)
        self.assertEqual(db_action.help, "control-plane database path")

    def test_execution_cancellation_and_reconciliation_use_the_coordinator(
        self,
    ) -> None:
        control = FakeControlPlane()
        coordinator = FakeRunCoordinator()
        result_store = FakeResultStore()
        cases = (
            (
                [
                    "run",
                    "execute",
                    "run_" + "1" * 32,
                    "--runtime-adapter",
                    "codex",
                ],
                (
                    "execute",
                    {
                        "run_id": "run_" + "1" * 32,
                        "runtime_adapter": "codex",
                    },
                ),
                {"operation": "run.execute", "run_id": "run_" + "1" * 32},
            ),
            (
                ["run", "reconcile"],
                ("reconcile", {}),
                [{"operation": "run.reconcile"}],
            ),
            (
                [
                    "run",
                    "cancel",
                    "run_" + "2" * 32,
                    "--expected-version",
                    "3",
                    "--requested-by",
                    "operator",
                    "--reason",
                    "operator requested stop",
                ],
                ("reconcile", "run_" + "2" * 32),
                {"operation": "run.show", "run_id": "run_" + "2" * 32},
            ),
        )
        for arguments, expected_call, expected_output in cases:
            with self.subTest(arguments=arguments):
                stdout = io.StringIO()
                with (
                    mock.patch(
                        "magic_glass_box.cli.ControlPlane", return_value=control
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_run_coordinator",
                        return_value=coordinator,
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_result_store",
                        return_value=result_store,
                    ),
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(coordinator.calls[-1], expected_call)
                self.assertEqual(json.loads(stdout.getvalue()), expected_output)

        self.assertEqual(
            control.calls[-1],
            (
                "run.cancel",
                {
                    "run_id": "run_" + "2" * 32,
                    "requested_by_handle": "operator",
                    "expected_run_version": 3,
                    "reason": "operator requested stop",
                },
            ),
        )

    def test_control_room_server_prints_the_private_url_and_closes_cleanly(
        self,
    ) -> None:
        class FakeServer:
            control_url = "http://127.0.0.1:8766/private-token/"
            origin = "http://127.0.0.1:8766"

            def __init__(self) -> None:
                self.closed = False

            def serve_forever(self) -> None:
                raise KeyboardInterrupt

            def server_close(self) -> None:
                self.closed = True

        server = FakeServer()
        args = argparse.Namespace(host="127.0.0.1", port=8766, history_limit=25)
        stdout = io.StringIO()
        with (
            mock.patch(
                "magic_glass_box.cli.build_control_room_server",
                return_value=server,
            ) as build,
            redirect_stdout(stdout),
        ):
            status = _serve_control_room(args, FakeControlPlane(), FakeRunCoordinator())

        self.assertEqual(status, 0)
        self.assertTrue(server.closed)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "binding": "http://127.0.0.1:8766",
                "control_room_url": "http://127.0.0.1:8766/private-token/",
                "network_scope": "loopback_only",
            },
        )
        self.assertEqual(build.call_args.kwargs["history_limit"], 25)

        with (
            mock.patch(
                "magic_glass_box.cli.build_control_room_server",
                side_effect=OSError("address already in use"),
            ),
            self.assertRaisesRegex(InputError, "address already in use"),
        ):
            _serve_control_room(args, FakeControlPlane(), FakeRunCoordinator())

    def test_control_room_serve_main_route_builds_one_coordinator(self) -> None:
        control = FakeControlPlane()
        coordinator = FakeRunCoordinator()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            mock.patch(
                "magic_glass_box.cli._build_run_coordinator",
                return_value=coordinator,
            ) as build,
            mock.patch("magic_glass_box.cli._build_result_store") as result_store,
            mock.patch(
                "magic_glass_box.cli._serve_control_room", return_value=0
            ) as serve,
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "control-room",
                    "serve",
                    "--port",
                    "0",
                    "--history-limit",
                    "3",
                ]
            )

        self.assertEqual(status, 0)
        result_store.assert_called_once_with(control)
        build.assert_called_once()
        serve.assert_called_once()
        served_args = serve.call_args.args[0]
        self.assertEqual(served_args.port, 0)
        self.assertEqual(served_args.history_limit, 3)

    def test_supervisor_service_announces_policy_and_closes_cleanly(self) -> None:
        supervisor = FakeSupervisor()
        args = argparse.Namespace(
            poll_interval=1.5,
            max_workers=3,
            action_lease_seconds=45,
        )
        control = FakeControlPlane()
        stdout = io.StringIO()
        with (
            mock.patch(
                "magic_glass_box.cli.Supervisor", return_value=supervisor
            ) as supervisor_type,
            redirect_stdout(stdout),
        ):
            status = _serve_supervisor(args, control, FakeRunCoordinator())

        self.assertEqual(status, 0)
        self.assertEqual(supervisor.run_calls, 1)
        self.assertEqual(supervisor.close_calls, [True])
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "action_lease_seconds": 45,
                "authority": "explicit_run_policy",
                "max_workers": 3,
                "poll_interval_seconds": 1.5,
                "supervisor_id": "supervisor-test",
            },
        )
        supervisor_type.assert_called_once_with(
            control.database,
            mock.ANY,
            control,
            poll_interval=1.5,
            max_workers=3,
            action_lease_seconds=45,
        )

    def test_sigterm_unwinds_a_service_and_restores_the_previous_handler(self) -> None:
        previous = mock.sentinel.previous_handler
        installed: list[Any] = []

        def install(_signal: int, handler: object) -> None:
            installed.append(handler)

        with (
            mock.patch(
                "magic_glass_box.cli.signal.getsignal", return_value=previous
            ) as get_handler,
            mock.patch("magic_glass_box.cli.signal.signal", side_effect=install),
            self.assertRaises(_TerminationRequested),
            _handle_sigterm(),
        ):
            installed[0](signal.SIGTERM, None)

        get_handler.assert_called_once_with(signal.SIGTERM)
        self.assertEqual(len(installed), 2)
        self.assertIs(installed[1], previous)

    def test_supervisor_main_route_builds_one_coordinator(self) -> None:
        control = FakeControlPlane()
        coordinator = FakeRunCoordinator()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            mock.patch(
                "magic_glass_box.cli._build_run_coordinator",
                return_value=coordinator,
            ) as build,
            mock.patch("magic_glass_box.cli._build_result_store") as result_store,
            mock.patch(
                "magic_glass_box.cli._serve_supervisor", return_value=0
            ) as serve,
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "supervisor",
                    "run",
                    "--poll-interval",
                    "0.5",
                    "--max-workers",
                    "2",
                    "--action-lease-seconds",
                    "30",
                    "--run-timeout",
                    "90",
                ]
            )

        self.assertEqual(status, 0)
        result_store.assert_called_once_with(control)
        build.assert_called_once_with(
            control,
            run_timeout=90.0,
            result_store=result_store.return_value,
        )
        serve.assert_called_once_with(mock.ANY, control, coordinator)

    def test_result_routes_verify_database_binding_before_show_or_apply(self) -> None:
        control = FakeControlPlane()
        result_store = FakeResultStore()
        attempt_id = "att_" + "3" * 32
        cases = (
            (
                ["result", "show", attempt_id],
                (
                    "inspect",
                    {"attempt_id": attempt_id, "expected_digest": "a" * 64},
                ),
                {
                    "attempt_id": attempt_id,
                    "content_sha256": "a" * 64,
                    "operation": "result.show",
                },
            ),
            (
                [
                    "result",
                    "apply",
                    attempt_id,
                    "--repository",
                    str(self.database.parent),
                ],
                (
                    "apply",
                    {
                        "attempt_id": attempt_id,
                        "repository": self.database.parent,
                        "expected_digest": "a" * 64,
                    },
                ),
                {"operation": "result.apply", "attempt_id": attempt_id},
            ),
        )
        for arguments, expected_call, expected_output in cases:
            with self.subTest(arguments=arguments):
                stdout = io.StringIO()
                with (
                    mock.patch(
                        "magic_glass_box.cli.ControlPlane", return_value=control
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_result_store",
                        return_value=result_store,
                    ),
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(result_store.calls[-1], expected_call)
                self.assertEqual(json.loads(stdout.getvalue()), expected_output)
                self.assertEqual(
                    control.database.calls[-1],
                    ("result.show", {"attempt_id": attempt_id}),
                )

    def test_run_coordinator_builder_wires_the_run_scoped_gateway(self) -> None:
        control = mock.Mock()
        control.database.path = self.database
        credential_resolver = object()
        control.credential_resolver = credential_resolver
        workspace = object()
        executor = object()
        gateway = object()
        result_store = object()
        coordinator = object()
        runtime_root = Path(self.temporary.name) / "runtime"
        with (
            mock.patch("magic_glass_box.cli.GitSnapshotter", return_value="git"),
            mock.patch(
                "magic_glass_box.cli.WorkspaceManager", return_value=workspace
            ) as workspace_type,
            mock.patch(
                "magic_glass_box.cli.DockerCLI", return_value="docker"
            ) as docker_type,
            mock.patch(
                "magic_glass_box.cli.DockerExecutor", return_value=executor
            ) as executor_type,
            mock.patch(
                "magic_glass_box.cli.default_gateway_root",
                return_value=runtime_root,
            ) as gateway_root,
            mock.patch(
                "magic_glass_box.cli.FileGatewayManager", return_value=gateway
            ) as gateway_type,
            mock.patch(
                "magic_glass_box.cli.RunCoordinator", return_value=coordinator
            ) as coordinator_type,
        ):
            result = _build_run_coordinator(
                control, run_timeout=90, result_store=result_store
            )

        self.assertIs(result, coordinator)
        workspace_type.assert_called_once_with(
            self.database.parent / "workspaces", "git"
        )
        docker_type.assert_called_once_with(run_timeout=90)
        executor_type.assert_called_once_with("docker")
        gateway_root.assert_called_once_with(self.database)
        gateway_type.assert_called_once_with(runtime_root, control)
        coordinator_type.assert_called_once_with(
            control.database,
            executor,
            workspace,
            credential_resolver,
            gateway,
            result_store,
        )

    def test_emit_has_stable_readable_json(self) -> None:
        output = io.StringIO()
        _emit({"z": "å", "a": 1}, stream=output)
        self.assertEqual(output.getvalue(), '{\n  "a": 1,\n  "z": "å"\n}\n')

    def test_json_input_rejects_empty_duplicate_and_non_object_documents(self) -> None:
        path = Path(self.temporary.name) / "input.json"
        for raw, message in (
            (b"", "between 1 byte"),
            (b'{"a":1,"a":2}', "duplicate JSON key"),
            (b"[]", "must contain an object"),
            (b"{", "malformed JSON"),
            (b'"\xff"', "UTF-8"),
            (b'{"number":NaN}', "invalid JSON number"),
        ):
            with self.subTest(message=message):
                path.write_bytes(raw)
                with self.assertRaisesRegex(InputError, message):
                    _json_file(path)

    def test_json_input_preserves_unicode_and_accepts_exact_size_limit(self) -> None:
        path = Path(self.temporary.name) / "input.json"
        prefix = '{"value":"å'.encode()
        suffix = b'"}'
        raw = prefix + (b"x" * (1024 * 1024 - len(prefix) - len(suffix))) + suffix
        path.write_bytes(raw)
        value = _json_file(path)
        self.assertTrue(value["value"].startswith("å"))
        self.assertEqual(len(raw), 1024 * 1024)

    def test_json_input_rejects_missing_and_oversized_files(self) -> None:
        missing = Path(self.temporary.name) / "missing.json"
        with self.assertRaisesRegex(InputError, "cannot read"):
            _json_file(missing)
        oversized = Path(self.temporary.name) / "oversized.json"
        oversized.write_bytes(b" " * (1024 * 1024 + 1))
        with self.assertRaisesRegex(InputError, "1 MiB"):
            _json_file(oversized)


if __name__ == "__main__":
    unittest.main()
