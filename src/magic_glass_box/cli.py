from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .control_room_web import build_control_room_server
from .credentials import CredentialError
from .db import Database, StorageError, default_db_path
from .domain import (
    AgentProfileSpec,
    ProjectContext,
    ProviderGrantSpec,
    ValidationError,
    WorkItemEnvelope,
)
from .executor import DockerCLI, DockerExecutor, ExecutionError
from .gateway import FileGatewayManager, default_gateway_root
from .krab import KrabError
from .orchestration import RunCoordinator, RunOrchestrationError
from .provider import ProviderError
from .results import ResultError, WorkspaceResultStore, default_result_root
from .service import ControlPlane, activity_to_dict
from .workspace import GitSnapshotter, WorkspaceError, WorkspaceManager


class InputError(ValueError):
    pass


def _positive_seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return parsed


def _history_limit(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be an integer between 0 and 500"
        ) from exc
    if not 0 <= parsed <= 500:
        raise argparse.ArgumentTypeError("must be an integer between 0 and 500")
    return parsed


def _port(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be an integer between 0 and 65535"
        ) from exc
    if not 0 <= parsed <= 65535:
        raise argparse.ArgumentTypeError("must be an integer between 0 and 65535")
    return parsed


def _json_file(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise InputError(f"cannot read JSON file {path}: {exc}") from exc
    if not raw or len(raw) > 1024 * 1024:
        raise InputError("JSON file must contain between 1 byte and 1 MiB")
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise InputError("JSON file must be UTF-8") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise InputError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise InputError(f"invalid JSON number: {value}")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except InputError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise InputError(f"malformed JSON file: {path}") from exc
    if not isinstance(value, dict):
        raise InputError("JSON file must contain an object")
    return value


def _emit(value: object, *, stream: Any | None = None) -> None:
    if stream is None:
        stream = sys.stdout
    print(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        file=stream,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="magic-glass-box",
        description="Magic Glass Box control-plane administration",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=default_db_path(),
        help="control-plane database path",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("init", help="initialize the private state database")
    commands.add_parser("status", help="validate state and show object counts")

    control_room = commands.add_parser(
        "control-room", help="inspect redacted operational state"
    )
    control_room_commands = control_room.add_subparsers(
        dest="control_room_command", required=True
    )
    control_room_show = control_room_commands.add_parser(
        "show", help="show one consistent redacted snapshot"
    )
    control_room_show.add_argument(
        "--history-limit",
        type=_history_limit,
        default=25,
        help="terminal runs to include in addition to every active run (0-500)",
    )
    control_room_show.add_argument(
        "--refresh-krab",
        action="store_true",
        help="resolve configured identities and read live roles and claim queues",
    )
    control_room_serve = control_room_commands.add_parser(
        "serve", help="serve the interactive loopback control room"
    )
    control_room_serve.add_argument(
        "--host",
        default="127.0.0.1",
        help="loopback IP address to bind (default: 127.0.0.1)",
    )
    control_room_serve.add_argument(
        "--port",
        type=_port,
        default=8766,
        help="loopback TCP port, or 0 for an ephemeral port (default: 8766)",
    )
    control_room_serve.add_argument(
        "--history-limit",
        type=_history_limit,
        default=25,
        help="terminal runs to include in addition to every active run (0-500)",
    )

    user = commands.add_parser("user", help="manage local users")
    user_commands = user.add_subparsers(dest="user_command", required=True)
    user_add = user_commands.add_parser("add", help="add a local user")
    user_add.add_argument("handle")
    user_add.add_argument("--display-name")

    krab = commands.add_parser("krab", help="manage and query Krab connections")
    krab_commands = krab.add_subparsers(dest="krab_command", required=True)
    instance_add = krab_commands.add_parser("instance-add", help="add a Krab origin")
    instance_add.add_argument("name")
    instance_add.add_argument("--url", required=True)
    identity_add = krab_commands.add_parser(
        "identity-add", help="verify and retain a Krab credential reference"
    )
    identity_add.add_argument("instance")
    identity_add.add_argument("--token-file", required=True, type=Path)
    identity_add.add_argument("--local-user")
    activity = krab_commands.add_parser(
        "activity", help="read metadata-only project activity"
    )
    activity.add_argument("instance")
    activity.add_argument("project_key")
    activity.add_argument("--as-user", required=True, dest="krab_handle")
    activity.add_argument("--after", type=int, default=0)
    activity.add_argument("--limit", type=int, default=50)
    activity.add_argument("--wait", type=int, default=0)

    project = commands.add_parser("project", help="manage project context")
    project_commands = project.add_subparsers(dest="project_command", required=True)
    project_add = project_commands.add_parser(
        "add", help="verify and register a visible Krab project"
    )
    project_add.add_argument("project_key")
    project_add.add_argument("--instance", required=True)
    project_add.add_argument("--as-user", required=True, dest="krab_handle")
    project_add.add_argument("--name", required=True)
    project_add.add_argument("--context", required=True, type=Path)
    project_add.add_argument("--created-by", required=True)
    project_revise = project_commands.add_parser(
        "revise", help="append an immutable project-context revision"
    )
    project_revise.add_argument("project_key")
    project_revise.add_argument("--context", required=True, type=Path)
    project_revise.add_argument("--created-by", required=True)

    profile = commands.add_parser("profile", help="manage agent profiles")
    profile_commands = profile.add_subparsers(dest="profile_command", required=True)
    for verb in ("add", "revise"):
        profile_action = profile_commands.add_parser(
            verb,
            help=(
                "create a versioned agent profile"
                if verb == "add"
                else "append an immutable profile revision"
            ),
        )
        profile_action.add_argument("name")
        profile_action.add_argument("--instance", required=True)
        profile_action.add_argument("--krab-user", required=True)
        profile_action.add_argument("--spec", required=True, type=Path)
        profile_action.add_argument("--created-by", required=True)

    provider = commands.add_parser("provider", help="manage scoped provider grants")
    provider_commands = provider.add_subparsers(dest="provider_command", required=True)
    provider_add = provider_commands.add_parser(
        "add", help="add an immutable provider policy and credential reference"
    )
    provider_add.add_argument("name")
    provider_add.add_argument("--policy", required=True, type=Path)
    provider_add.add_argument("--credential-file", required=True, type=Path)
    provider_add.add_argument("--expires-at")
    provider_add.add_argument("--created-by", required=True)
    provider_show = provider_commands.add_parser(
        "show", help="show provider policy and lifecycle metadata"
    )
    provider_show.add_argument("name")
    provider_revoke = provider_commands.add_parser(
        "revoke", help="revoke a provider grant"
    )
    provider_revoke.add_argument("name")
    provider_revoke.add_argument("--expected-version", required=True, type=int)
    provider_revoke.add_argument("--revoked-by", required=True)

    work_item = commands.add_parser("work-item", help="manage durable work items")
    work_item_commands = work_item.add_subparsers(
        dest="work_item_command", required=True
    )
    work_item_import = work_item_commands.add_parser(
        "import", help="create or update a normalized work item"
    )
    work_item_import.add_argument("document", type=Path)
    work_item_import.add_argument("--project", required=True)
    work_item_import.add_argument("--imported-by", required=True)
    work_item_show = work_item_commands.add_parser(
        "show", help="show work-item lifecycle metadata"
    )
    work_item_show.add_argument("work_item_id")
    work_item_complete = work_item_commands.add_parser(
        "complete", help="seal completed work memory"
    )
    work_item_complete.add_argument("work_item_id")
    work_item_complete.add_argument(
        "--expected-thread-version", required=True, type=int
    )
    work_item_complete.add_argument("--completed-by", required=True)
    work_item_complete.add_argument("--reason")
    work_item_reopen = work_item_commands.add_parser(
        "reopen", help="start a fresh work-memory generation"
    )
    work_item_reopen.add_argument("work_item_id")
    work_item_reopen.add_argument("--expected-thread-version", required=True, type=int)
    work_item_reopen.add_argument("--reopened-by", required=True)

    run = commands.add_parser("run", help="manage agent runs")
    run_commands = run.add_subparsers(dest="run_command", required=True)
    run_create = run_commands.add_parser("create", help="create a pinned run record")
    run_create.add_argument("--project", required=True)
    run_create.add_argument("--profile", required=True)
    run_create.add_argument("--work-item", required=True)
    run_create.add_argument("--requested-by", required=True)
    run_create.add_argument("--source-revision")
    run_create.add_argument("--krab-session")
    run_create.add_argument("--krab-claim")
    run_cancel = run_commands.add_parser(
        "cancel", help="request stop for a queued or active run"
    )
    run_cancel.add_argument("run_id")
    run_cancel.add_argument("--expected-version", required=True, type=int)
    run_cancel.add_argument("--requested-by", required=True)
    run_cancel.add_argument("--reason", required=True)
    run_retry = run_commands.add_parser(
        "retry", help="create an audited successor with the same pinned profile"
    )
    run_retry.add_argument("run_id")
    run_retry.add_argument("--expected-version", required=True, type=int)
    run_retry.add_argument("--requested-by", required=True)
    run_reassign = run_commands.add_parser(
        "reassign", help="create an audited successor for another profile"
    )
    run_reassign.add_argument("run_id")
    run_reassign.add_argument("--profile", required=True)
    run_reassign.add_argument("--expected-version", required=True, type=int)
    run_reassign.add_argument("--requested-by", required=True)
    run_show = run_commands.add_parser("show", help="show a run and its event stream")
    run_show.add_argument("run_id")
    run_execute = run_commands.add_parser(
        "execute", help="execute one queued run in its pinned sandbox"
    )
    run_execute.add_argument("run_id")
    run_execute.add_argument(
        "--run-timeout",
        type=_positive_seconds,
        help="optional maximum agent runtime in seconds",
    )
    run_commands.add_parser(
        "reconcile", help="clean and fail runs interrupted by an orchestrator restart"
    )

    result = commands.add_parser("result", help="inspect and apply retained work")
    result_commands = result.add_subparsers(dest="result_command", required=True)
    result_show = result_commands.add_parser(
        "show", help="verify and show one retained attempt result"
    )
    result_show.add_argument("attempt_id")
    result_apply = result_commands.add_parser(
        "apply", help="apply one retained result after conflict preflight"
    )
    result_apply.add_argument("attempt_id")
    result_apply.add_argument("--repository", required=True, type=Path)
    return parser


def _build_result_store(control: ControlPlane) -> WorkspaceResultStore:
    return WorkspaceResultStore(default_result_root(control.database.path))


def _build_run_coordinator(
    control: ControlPlane,
    *,
    run_timeout: float | None = None,
    result_store: WorkspaceResultStore | None = None,
) -> RunCoordinator:
    workspaces = WorkspaceManager(
        control.database.path.parent / "workspaces",
        GitSnapshotter(),
    )
    return RunCoordinator(
        control.database,
        DockerExecutor(DockerCLI(run_timeout=run_timeout)),
        workspaces,
        control.credential_resolver,
        FileGatewayManager(
            default_gateway_root(control.database.path),
            control,
        ),
        result_store or _build_result_store(control),
    )


def _serve_control_room(
    args: argparse.Namespace,
    control: ControlPlane,
    coordinator: RunCoordinator,
) -> int:
    try:
        server = build_control_room_server(
            control,
            coordinator,
            host=args.host,
            port=args.port,
            history_limit=args.history_limit,
        )
    except (OSError, ValueError) as exc:
        raise InputError(str(exc)) from exc
    _emit(
        {
            "control_room_url": server.control_url,
            "binding": server.origin,
            "network_scope": "loopback_only",
        }
    )
    sys.stdout.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _dispatch(
    args: argparse.Namespace,
    control: ControlPlane,
    coordinator: RunCoordinator | None = None,
    result_store: WorkspaceResultStore | None = None,
) -> object:
    if args.command == "init":
        return control.initialize()
    if args.command == "status":
        return control.database.status()
    if args.command == "control-room" and args.control_room_command == "show":
        return control.control_room_snapshot(
            history_limit=args.history_limit,
            refresh_krab=args.refresh_krab,
        )
    if args.command == "user" and args.user_command == "add":
        return control.create_user(args.handle, args.display_name)
    if args.command == "krab":
        if args.krab_command == "instance-add":
            return control.add_krab_instance(args.name, args.url)
        if args.krab_command == "identity-add":
            return control.add_krab_identity(
                instance_name=args.instance,
                credential_path=args.token_file,
                local_user_handle=args.local_user,
            )
        if args.krab_command == "activity":
            return activity_to_dict(
                control.project_activity(
                    instance_name=args.instance,
                    krab_handle=args.krab_handle,
                    project_key=args.project_key,
                    after=args.after,
                    limit=args.limit,
                    wait=args.wait,
                )
            )
    if args.command == "project":
        context = ProjectContext.from_mapping(_json_file(args.context))
        if args.project_command == "add":
            return control.register_project(
                instance_name=args.instance,
                krab_handle=args.krab_handle,
                project_key=args.project_key,
                display_name=args.name,
                context=context,
                created_by_handle=args.created_by,
            )
        if args.project_command == "revise":
            return control.revise_project_context(
                project_key=args.project_key,
                context=context,
                created_by_handle=args.created_by,
            )
    if args.command == "profile":
        spec = AgentProfileSpec.from_mapping(_json_file(args.spec))
        kwargs = {
            "name": args.name,
            "instance_name": args.instance,
            "krab_handle": args.krab_user,
            "spec": spec,
            "created_by_handle": args.created_by,
        }
        if args.profile_command == "add":
            return control.create_agent_profile(**kwargs)
        if args.profile_command == "revise":
            return control.revise_agent_profile(**kwargs)
    if args.command == "provider":
        if args.provider_command == "add":
            return control.add_provider_grant(
                name=args.name,
                spec=ProviderGrantSpec.from_mapping(_json_file(args.policy)),
                credential_path=args.credential_file,
                expires_at=args.expires_at,
                created_by_handle=args.created_by,
            )
        if args.provider_command == "show":
            return control.database.get_provider_grant(args.name)
        if args.provider_command == "revoke":
            return control.revoke_provider_grant(
                args.name,
                expected_version=args.expected_version,
                revoked_by_handle=args.revoked_by,
            )
    if args.command == "work-item":
        if args.work_item_command == "import":
            return control.import_work_item(
                project_key=args.project,
                envelope=WorkItemEnvelope.from_mapping(_json_file(args.document)),
                imported_by_handle=args.imported_by,
            )
        if args.work_item_command == "show":
            return control.get_work_item(args.work_item_id)
        if args.work_item_command == "complete":
            return control.complete_work_item(
                args.work_item_id,
                completed_by_handle=args.completed_by,
                expected_thread_version=args.expected_thread_version,
                reason=args.reason,
            )
        if args.work_item_command == "reopen":
            return control.reopen_work_item(
                args.work_item_id,
                reopened_by_handle=args.reopened_by,
                expected_thread_version=args.expected_thread_version,
            )
    if args.command == "run":
        if args.run_command == "create":
            return control.create_run(
                project_key=args.project,
                profile_name=args.profile,
                work_item_id=args.work_item,
                requested_by_handle=args.requested_by,
                source_revision=args.source_revision,
                krab_session_id=args.krab_session,
                krab_claim_id=args.krab_claim,
            )
        if args.run_command == "cancel":
            requested = control.request_run_cancellation(
                args.run_id,
                requested_by_handle=args.requested_by,
                expected_run_version=args.expected_version,
                reason=args.reason,
            )
            if coordinator is not None:
                coordinator.reconcile(args.run_id)
                return control.database.get_run(args.run_id)
            return requested
        if args.run_command == "retry":
            return control.retry_run(
                args.run_id,
                requested_by_handle=args.requested_by,
                expected_run_version=args.expected_version,
            )
        if args.run_command == "reassign":
            return control.reassign_run(
                args.run_id,
                profile_name=args.profile,
                requested_by_handle=args.requested_by,
                expected_run_version=args.expected_version,
            )
        if args.run_command == "show":
            return control.database.get_run(args.run_id)
        if args.run_command == "execute" and coordinator is not None:
            return coordinator.execute(args.run_id)
        if args.run_command == "reconcile" and coordinator is not None:
            return coordinator.reconcile()
    if args.command == "result" and result_store is not None:
        stored = control.database.get_attempt_result(args.attempt_id)
        if args.result_command == "show":
            result_store.inspect(
                args.attempt_id, expected_digest=stored["content_sha256"]
            )
            return stored
        if args.result_command == "apply":
            return result_store.apply(
                args.attempt_id,
                args.repository,
                expected_digest=stored["content_sha256"],
            )
    raise RuntimeError("unreachable command")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    control = ControlPlane(Database(args.db))
    try:
        coordinator = None
        result_store = None
        if args.command == "run" and args.run_command in {
            "cancel",
            "execute",
            "reconcile",
        }:
            result_store = _build_result_store(control)
            coordinator = _build_run_coordinator(
                control,
                run_timeout=(
                    args.run_timeout if args.run_command == "execute" else None
                ),
                result_store=result_store,
            )
        elif args.command == "control-room" and args.control_room_command == "serve":
            result_store = _build_result_store(control)
            coordinator = _build_run_coordinator(
                control,
                result_store=result_store,
            )
        elif args.command == "result":
            result_store = _build_result_store(control)
        if (
            args.command == "control-room"
            and args.control_room_command == "serve"
            and coordinator is not None
        ):
            return _serve_control_room(args, control, coordinator)
        result = _dispatch(args, control, coordinator, result_store)
    except (
        CredentialError,
        ExecutionError,
        InputError,
        KrabError,
        ProviderError,
        ResultError,
        RunOrchestrationError,
        StorageError,
        ValidationError,
        WorkspaceError,
    ) as exc:
        _emit(
            {"error": {"type": type(exc).__name__, "message": str(exc)}},
            stream=sys.stderr,
        )
        return 2
    _emit(result)
    return 0
