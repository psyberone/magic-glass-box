from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .credentials import CredentialError
from .db import Database, StorageError, default_db_path
from .domain import (
    AgentProfileSpec,
    ProjectContext,
    RunState,
    ValidationError,
    WorkItemEnvelope,
)
from .executor import DockerCLI, DockerExecutor, ExecutionError
from .krab import KrabError
from .orchestration import RunCoordinator, RunOrchestrationError
from .service import ControlPlane, activity_to_dict
from .workspace import GitSnapshotter, WorkspaceError, WorkspaceManager


class InputError(ValueError):
    pass


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

    work_item = commands.add_parser("work-item", help="import durable work items")
    work_item_commands = work_item.add_subparsers(
        dest="work_item_command", required=True
    )
    work_item_import = work_item_commands.add_parser(
        "import", help="create or update a normalized work item"
    )
    work_item_import.add_argument("document", type=Path)
    work_item_import.add_argument("--project", required=True)
    work_item_import.add_argument("--imported-by", required=True)

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
    run_transition = run_commands.add_parser(
        "transition", help="apply an optimistic run-state transition"
    )
    run_transition.add_argument("run_id")
    run_transition.add_argument("state", choices=[state.value for state in RunState])
    run_transition.add_argument("--expected-version", required=True, type=int)
    run_transition.add_argument("--reason")
    run_show = run_commands.add_parser("show", help="show a run and its event stream")
    run_show.add_argument("run_id")
    run_execute = run_commands.add_parser(
        "execute", help="execute one queued run in its pinned sandbox"
    )
    run_execute.add_argument("run_id")
    run_commands.add_parser(
        "reconcile", help="clean and fail runs interrupted by an orchestrator restart"
    )
    return parser


def _build_run_coordinator(control: ControlPlane) -> RunCoordinator:
    workspaces = WorkspaceManager(
        control.database.path.parent / "workspaces",
        GitSnapshotter(),
    )
    return RunCoordinator(
        control.database,
        DockerExecutor(DockerCLI()),
        workspaces,
        control.credential_resolver,
    )


def _dispatch(
    args: argparse.Namespace,
    control: ControlPlane,
    coordinator: RunCoordinator | None = None,
) -> object:
    if args.command == "init":
        return control.initialize()
    if args.command == "status":
        return control.database.status()
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
    if args.command == "work-item" and args.work_item_command == "import":
        return control.import_work_item(
            project_key=args.project,
            envelope=WorkItemEnvelope.from_mapping(_json_file(args.document)),
            imported_by_handle=args.imported_by,
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
        if args.run_command == "transition":
            return control.transition_run(
                args.run_id,
                target=RunState(args.state),
                expected_version=args.expected_version,
                reason=args.reason,
            )
        if args.run_command == "show":
            return control.database.get_run(args.run_id)
        if args.run_command == "execute" and coordinator is not None:
            return coordinator.execute(args.run_id)
        if args.run_command == "reconcile" and coordinator is not None:
            return coordinator.reconcile()
    raise RuntimeError("unreachable command")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    control = ControlPlane(Database(args.db))
    try:
        coordinator = None
        if args.command == "run" and args.run_command in {"execute", "reconcile"}:
            coordinator = _build_run_coordinator(control)
        result = _dispatch(args, control, coordinator)
    except (
        CredentialError,
        ExecutionError,
        InputError,
        KrabError,
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
