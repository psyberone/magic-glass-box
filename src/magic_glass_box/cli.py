from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import sqlite3
import sys
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import FrameType
from typing import Any

from .advisory import AdvisoryConfigurationSpec
from .advisory_director import AdvisoryDirector
from .candidates import (
    DEFAULT_GIT_TIMEOUT_SECONDS,
    CandidateError,
    GitCandidateStore,
    default_candidate_root,
)
from .control_room_web import build_control_room_server
from .credentials import CredentialError
from .db import Database, StorageError, default_db_path
from .discovery import DiscoveryConfigurationSpec
from .discovery_director import DiscoveryDirector
from .domain import (
    AgentProfileSpec,
    ProjectContext,
    SupervisionPolicy,
    ValidationError,
    WorkItemEnvelope,
)
from .execution_operations import ExecutionRecipeAuthority
from .executor import (
    DEFAULT_RUN_TIMEOUT_SECONDS,
    DockerCLI,
    DockerExecutor,
    ExecutionError,
)
from .gates import GateError
from .gateway import FileGatewayManager, GatewayError, default_gateway_root
from .inference import (
    InferenceConnectorError,
    InferenceConnectorSpec,
    InferenceGrantSpec,
)
from .integration import IntegrationError
from .krab import KrabError
from .orchestration import (
    CandidateReviewWorkspaces,
    RunCoordinator,
    RunOrchestrationError,
)
from .provider import ProviderError
from .results import ResultError, WorkspaceResultStore, default_result_root
from .runtime import RuntimeAdapterError
from .service import ControlPlane, activity_to_dict
from .service_sidecar import (
    ServiceOperationSpec,
    ServiceSidecarError,
    ServiceSidecarSpec,
)
from .supervision import Supervisor, SupervisorError
from .worker_transport import WorkerTransportError
from .workflow_director import WorkflowDirector
from .workflows import WorkflowConfigurationSpec
from .workspace import GitSnapshotter, WorkspaceError, WorkspaceManager


class InputError(ValueError):
    pass


class _TerminationRequested(BaseException):
    """Unwind a foreground service cleanly when its process receives SIGTERM."""


class _LazyDockerImageInspector:
    """Avoid requiring Docker for policies that contain only a review gate."""

    def __init__(self) -> None:
        self._engine: DockerCLI | None = None

    def inspect_image(self, reference: str) -> object:
        if self._engine is None:
            self._engine = DockerCLI()
        return self._engine.inspect_image(reference)


@contextmanager
def _handle_sigterm() -> Iterator[None]:
    previous = signal.getsignal(signal.SIGTERM)

    def terminate(_signum: int, _frame: FrameType | None) -> None:
        raise _TerminationRequested

    signal.signal(signal.SIGTERM, terminate)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


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


def _candidate_limit(value: str) -> int:
    return _bounded_integer(value, minimum=1, maximum=500)


def _evidence_limit(value: str) -> int:
    return _bounded_integer(value, minimum=1, maximum=1000)


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


def _bounded_integer(value: str, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"must be an integer between {minimum} and {maximum}"
        ) from exc
    if not minimum <= parsed <= maximum:
        raise argparse.ArgumentTypeError(
            f"must be an integer between {minimum} and {maximum}"
        )
    return parsed


def _worker_count(value: str) -> int:
    return _bounded_integer(value, minimum=1, maximum=32)


def _supervisor_discovery_count(value: str) -> int:
    return _bounded_integer(value, minimum=12, maximum=1024)


def _action_lease_seconds(value: str) -> int:
    return _bounded_integer(value, minimum=10, maximum=3600)


def _integration_lease_seconds(value: str) -> int:
    return _bounded_integer(value, minimum=1, maximum=3600)


def _json_file(path: Path) -> dict[str, Any]:
    try:
        with path.open(mode="rb") as stream:
            raw = stream.read(1024 * 1024 + 1)
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

    workflow = commands.add_parser(
        "workflow", help="manage bounded workflow prescriptions"
    )
    workflow_commands = workflow.add_subparsers(dest="workflow_command", required=True)
    for verb in ("config-create", "config-revise"):
        configuration = workflow_commands.add_parser(
            verb,
            help=(
                "create a versioned workflow configuration"
                if verb == "config-create"
                else "append an immutable configuration revision"
            ),
        )
        configuration.add_argument("name")
        configuration.add_argument("--project", required=True)
        configuration.add_argument("--producer", required=True)
        configuration.add_argument("--reviewer", required=True)
        configuration.add_argument("--spec", required=True, type=Path)
        configuration.add_argument("--created-by", required=True)
    workflow_config_show = workflow_commands.add_parser(
        "config-show", help="show one workflow configuration revision"
    )
    workflow_config_show.add_argument("name")
    workflow_config_show.add_argument("--project", required=True)
    workflow_config_show.add_argument("--version", type=int)
    workflow_config_list = workflow_commands.add_parser(
        "config-list", help="list latest workflow configurations"
    )
    workflow_config_list.add_argument("--project")
    workflow_start = workflow_commands.add_parser(
        "start", help="start one exact pinned workflow execution"
    )
    workflow_start.add_argument("configuration_name")
    workflow_start.add_argument("--project", required=True)
    workflow_start.add_argument("--work-item", required=True)
    workflow_start.add_argument("--source-revision", required=True)
    workflow_start.add_argument(
        "--expected-configuration-version", required=True, type=int
    )
    workflow_start.add_argument(
        "--expected-context-version", required=True, dest="expected_context_version_id"
    )
    workflow_start.add_argument(
        "--expected-work-item-version",
        required=True,
        dest="expected_work_item_version_id",
    )
    workflow_start.add_argument("--expected-thread-generation", required=True, type=int)
    workflow_start.add_argument("--expected-thread-version", required=True, type=int)
    workflow_start.add_argument("--discovery-execution")
    workflow_start.add_argument("--idempotency-key", required=True)
    workflow_start.add_argument("--requested-by", required=True)
    workflow_show = workflow_commands.add_parser(
        "show", help="show one workflow execution and event stream"
    )
    workflow_show.add_argument("execution_id")
    workflow_list = workflow_commands.add_parser(
        "list", help="list bounded workflow executions"
    )
    workflow_list.add_argument("--project")
    workflow_list.add_argument("--state", choices=("active", "cancelled", "completed"))
    workflow_list.add_argument("--limit", type=_candidate_limit, default=100)
    workflow_cancel = workflow_commands.add_parser(
        "cancel", help="cancel one active workflow execution"
    )
    workflow_cancel.add_argument("execution_id")
    workflow_cancel.add_argument("--expected-version", required=True, type=int)
    workflow_cancel.add_argument("--cancelled-by", required=True)
    workflow_cancel.add_argument("--reason", required=True)
    workflow_ruling_show = workflow_commands.add_parser(
        "ruling-show", help="show one durable workflow ruling"
    )
    workflow_ruling_show.add_argument("ruling_id")
    workflow_ruling_resolve = workflow_commands.add_parser(
        "ruling-resolve", help="resolve one durable workflow ruling"
    )
    workflow_ruling_resolve.add_argument("ruling_id")
    workflow_ruling_resolve.add_argument("--expected-version", required=True, type=int)
    workflow_ruling_resolve.add_argument(
        "--decision", required=True, choices=("continue", "cancel")
    )
    workflow_ruling_resolve.add_argument("--resolved-by", required=True)
    workflow_provider_target = workflow_commands.add_parser(
        "provider-target", help="bind one provider resource to an exact workflow"
    )
    workflow_provider_target.add_argument("execution_id")
    workflow_provider_target.add_argument("--expected-version", required=True, type=int)
    workflow_provider_target.add_argument("--resource-id", required=True)
    workflow_provider_target.add_argument(
        "--merge-method", required=True, choices=("merge", "rebase", "squash")
    )
    workflow_provider_target.add_argument("--configured-by", required=True)
    workflow_manual_plan = workflow_commands.add_parser(
        "manual-plan", help="issue the workflow's exact manual landing plan"
    )
    workflow_manual_plan.add_argument("execution_id")
    workflow_manual_plan.add_argument("--expected-version", required=True, type=int)
    workflow_manual_plan.add_argument("--remote", default="origin")
    workflow_manual_plan.add_argument("--issued-by", required=True)
    workflow_manual_plan.add_argument(
        "--operation-timeout",
        dest="candidate_timeout",
        type=_positive_seconds,
        default=DEFAULT_GIT_TIMEOUT_SECONDS,
    )

    discovery = commands.add_parser(
        "discovery", help="manage bounded Discovery prescriptions"
    )
    discovery_commands = discovery.add_subparsers(
        dest="discovery_command", required=True
    )
    for verb in ("config-create", "config-revise"):
        configuration = discovery_commands.add_parser(
            verb,
            help=(
                "create a versioned Discovery configuration"
                if verb == "config-create"
                else "append an immutable Discovery configuration revision"
            ),
        )
        configuration.add_argument("name")
        configuration.add_argument("--project", required=True)
        configuration.add_argument("--investigator", required=True)
        configuration.add_argument("--coordinator", required=True)
        configuration.add_argument("--spec", required=True, type=Path)
        configuration.add_argument("--created-by", required=True)
    discovery_config_show = discovery_commands.add_parser(
        "config-show", help="show one Discovery configuration revision"
    )
    discovery_config_show.add_argument("name")
    discovery_config_show.add_argument("--project", required=True)
    discovery_config_show.add_argument("--version", type=int)
    discovery_config_list = discovery_commands.add_parser(
        "config-list", help="list latest Discovery configurations"
    )
    discovery_config_list.add_argument("--project")
    discovery_start = discovery_commands.add_parser(
        "start", help="start one exact pinned Discovery execution"
    )
    discovery_start.add_argument("configuration_name")
    discovery_start.add_argument("--project", required=True)
    discovery_start.add_argument("--work-item", required=True)
    discovery_start.add_argument("--source-revision", required=True)
    discovery_start.add_argument(
        "--expected-configuration-version", required=True, type=int
    )
    discovery_start.add_argument(
        "--expected-context-version", required=True, dest="expected_context_version_id"
    )
    discovery_start.add_argument(
        "--expected-work-item-version",
        required=True,
        dest="expected_work_item_version_id",
    )
    discovery_start.add_argument(
        "--expected-thread-generation", required=True, type=int
    )
    discovery_start.add_argument("--expected-thread-version", required=True, type=int)
    discovery_start.add_argument("--krab-session", required=True)
    discovery_start.add_argument("--krab-claim")
    discovery_start.add_argument("--idempotency-key", required=True)
    discovery_start.add_argument("--requested-by", required=True)
    discovery_show = discovery_commands.add_parser(
        "show", help="show one Discovery execution and event stream"
    )
    discovery_show.add_argument("execution_id")
    discovery_list = discovery_commands.add_parser(
        "list", help="list bounded Discovery executions"
    )
    discovery_list.add_argument("--project")
    discovery_list.add_argument(
        "--state", choices=("active", "cancelled", "completed", "failed")
    )
    discovery_list.add_argument("--limit", type=_candidate_limit, default=100)
    discovery_cancel = discovery_commands.add_parser(
        "cancel", help="cancel one active Discovery execution"
    )
    discovery_cancel.add_argument("execution_id")
    discovery_cancel.add_argument("--expected-version", required=True, type=int)
    discovery_cancel.add_argument("--cancelled-by", required=True)
    discovery_cancel.add_argument("--reason", required=True)

    advisory = commands.add_parser(
        "advisory", help="manage bounded advisory review prescriptions"
    )
    advisory_commands = advisory.add_subparsers(dest="advisory_command", required=True)
    for verb in ("config-create", "config-revise"):
        configuration = advisory_commands.add_parser(
            verb,
            help=(
                "create a versioned advisory configuration"
                if verb == "config-create"
                else "append an immutable advisory configuration revision"
            ),
        )
        configuration.add_argument("name")
        configuration.add_argument("--project", required=True)
        configuration.add_argument("--reviewer-one", required=True)
        configuration.add_argument("--reviewer-two", required=True)
        configuration.add_argument("--reviewer-three", required=True)
        configuration.add_argument("--coordinator", required=True)
        configuration.add_argument("--spec", required=True, type=Path)
        configuration.add_argument("--created-by", required=True)
    advisory_config_show = advisory_commands.add_parser(
        "config-show", help="show one advisory configuration revision"
    )
    advisory_config_show.add_argument("name")
    advisory_config_show.add_argument("--project", required=True)
    advisory_config_show.add_argument("--version", type=int)
    advisory_config_list = advisory_commands.add_parser(
        "config-list", help="list latest advisory configurations"
    )
    advisory_config_list.add_argument("--project")
    advisory_start = advisory_commands.add_parser(
        "start", help="start one exact-candidate advisory review"
    )
    advisory_start.add_argument("configuration_name")
    advisory_start.add_argument("--project", required=True)
    advisory_start.add_argument("--candidate", required=True)
    advisory_start.add_argument(
        "--expected-configuration-version", required=True, type=int
    )
    advisory_start.add_argument(
        "--expected-context-version", required=True, dest="expected_context_version_id"
    )
    advisory_start.add_argument(
        "--expected-work-item-version",
        required=True,
        dest="expected_work_item_version_id",
    )
    advisory_start.add_argument("--expected-thread-generation", required=True, type=int)
    advisory_start.add_argument("--expected-thread-version", required=True, type=int)
    advisory_start.add_argument("--idempotency-key", required=True)
    advisory_start.add_argument("--requested-by", required=True)
    advisory_show = advisory_commands.add_parser(
        "show", help="show one advisory execution and event stream"
    )
    advisory_show.add_argument("execution_id")
    advisory_list = advisory_commands.add_parser(
        "list", help="list bounded advisory executions"
    )
    advisory_list.add_argument("--project")
    advisory_list.add_argument(
        "--state", choices=("active", "cancelled", "completed", "failed")
    )
    advisory_list.add_argument("--limit", type=_candidate_limit, default=100)
    advisory_cancel = advisory_commands.add_parser(
        "cancel", help="cancel one active advisory execution"
    )
    advisory_cancel.add_argument("execution_id")
    advisory_cancel.add_argument("--expected-version", required=True, type=int)
    advisory_cancel.add_argument("--cancelled-by", required=True)
    advisory_cancel.add_argument("--reason", required=True)

    inference_connector = commands.add_parser(
        "inference-connector",
        help="manage credential-isolated inference connectors",
    )
    inference_connector_commands = inference_connector.add_subparsers(
        dest="inference_connector_command", required=True
    )
    connector_register = inference_connector_commands.add_parser(
        "register", help="register immutable connector authority"
    )
    connector_register.add_argument("name")
    connector_register.add_argument("--spec", required=True, type=Path)
    connector_register.add_argument("--created-by", required=True)
    connector_show = inference_connector_commands.add_parser(
        "show", help="show connector authority and lifecycle metadata"
    )
    connector_show.add_argument("name")
    inference_connector_commands.add_parser(
        "list", help="list connector authority and lifecycle metadata"
    )
    connector_revoke = inference_connector_commands.add_parser(
        "revoke", help="revoke an inference connector"
    )
    connector_revoke.add_argument("name")
    connector_revoke.add_argument("--expected-version", required=True, type=int)
    connector_revoke.add_argument("--revoked-by", required=True)

    service_sidecar = commands.add_parser(
        "service-sidecar",
        help="manage provider-neutral service sidecars and operations",
    )
    service_sidecar_commands = service_sidecar.add_subparsers(
        dest="service_sidecar_command", required=True
    )
    service_register = service_sidecar_commands.add_parser(
        "register", help="register immutable service-sidecar authority"
    )
    service_register.add_argument("name")
    service_register.add_argument("--spec", required=True, type=Path)
    service_register.add_argument("--created-by", required=True)
    service_show = service_sidecar_commands.add_parser(
        "show", help="show service-sidecar authority and lifecycle metadata"
    )
    service_show.add_argument("name")
    service_sidecar_commands.add_parser(
        "list", help="list service-sidecar authority and lifecycle metadata"
    )
    service_revoke = service_sidecar_commands.add_parser(
        "revoke", help="revoke a service sidecar"
    )
    service_revoke.add_argument("name")
    service_revoke.add_argument("--expected-version", required=True, type=int)
    service_revoke.add_argument("--revoked-by", required=True)
    operation_create = service_sidecar_commands.add_parser(
        "operation-create", help="create one exact durable service operation"
    )
    operation_create.add_argument("--request", required=True, type=Path)
    operation_create.add_argument("--binding")
    operation_create.add_argument("--created-by", required=True)
    operation_show = service_sidecar_commands.add_parser(
        "operation-show", help="show one durable service operation"
    )
    operation_show.add_argument("operation_id")
    operation_list = service_sidecar_commands.add_parser(
        "operation-list", help="list recent durable service operations"
    )
    operation_list.add_argument("--limit", type=_candidate_limit, default=100)
    operation_execute = service_sidecar_commands.add_parser(
        "operation-execute", help="invoke one authorized service operation"
    )
    operation_execute.add_argument("operation_id")
    operation_execute.add_argument("--owner", required=True)
    operation_execute.add_argument("--expected-version", required=True, type=int)
    operation_reconcile = service_sidecar_commands.add_parser(
        "operation-reconcile", help="reconcile one uncertain service operation"
    )
    operation_reconcile.add_argument("operation_id")
    operation_reconcile.add_argument("--owner", required=True)
    operation_reconcile.add_argument("--expected-version", required=True, type=int)
    execution_submit_create = service_sidecar_commands.add_parser(
        "execution-submit-create",
        help="derive one exact remote execution submission",
    )
    execution_submit_create.add_argument("--project", required=True)
    execution_submit_create.add_argument("--candidate", required=True)
    execution_submit_create.add_argument("--recipe-id", required=True)
    execution_submit_create.add_argument("--recipe-sha256", required=True)
    execution_submit_create.add_argument("--image-id", required=True)
    execution_submit_create.add_argument("--request-key", required=True)
    execution_submit_create.add_argument("--created-by", required=True)
    execution_followup_create = service_sidecar_commands.add_parser(
        "execution-followup-create",
        help="derive inspect, cancel, or collect from a submission",
    )
    execution_followup_create.add_argument("submission_operation_id")
    execution_followup_create.add_argument(
        "--action",
        required=True,
        choices=("inspect", "cancel", "collect"),
    )
    execution_followup_create.add_argument("--request-key", required=True)
    execution_followup_create.add_argument("--created-by", required=True)

    capability = commands.add_parser(
        "capability",
        help="bind project capabilities to explicit controllers",
    )
    capability_commands = capability.add_subparsers(
        dest="capability_command", required=True
    )
    capability_bind = capability_commands.add_parser(
        "bind", help="bind one project capability to a controller"
    )
    capability_bind.add_argument("project_key")
    capability_bind.add_argument("capability_name")
    capability_bind.add_argument("--version", required=True, type=int)
    capability_bind.add_argument("--sidecar", required=True)
    capability_bind.add_argument("--mapping", required=True)
    capability_bind.add_argument("--bound-by", required=True)
    capability_show = capability_commands.add_parser(
        "show", help="show one project capability binding"
    )
    capability_show.add_argument("binding_id")
    capability_list = capability_commands.add_parser(
        "list", help="list project capability bindings"
    )
    capability_list.add_argument("--project", dest="project_key")
    capability_list.add_argument("--include-unbound", action="store_true")
    capability_unbind = capability_commands.add_parser(
        "unbind", help="disable one project capability binding"
    )
    capability_unbind.add_argument("binding_id")
    capability_unbind.add_argument("--expected-version", required=True, type=int)
    capability_unbind.add_argument("--unbound-by", required=True)

    provider = commands.add_parser("provider", help="manage scoped provider grants")
    provider_commands = provider.add_subparsers(dest="provider_command", required=True)
    provider_connector_add = provider_commands.add_parser(
        "connector-add", help="add a connector-bound provider grant"
    )
    provider_connector_add.add_argument("name")
    provider_connector_add.add_argument("--policy", required=True, type=Path)
    provider_connector_add.add_argument("--expires-at")
    provider_connector_add.add_argument("--created-by", required=True)
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
    work_item_import_operation = work_item_commands.add_parser(
        "import-operation", help="import one completed provider work read"
    )
    work_item_import_operation.add_argument("operation_id")
    work_item_import_operation.add_argument("--project", required=True)
    work_item_import_operation.add_argument("--imported-by", required=True)
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

    interaction = commands.add_parser(
        "interaction", help="resolve durable agent interactions"
    )
    interaction_commands = interaction.add_subparsers(
        dest="interaction_command", required=True
    )
    interaction_answer = interaction_commands.add_parser(
        "answer", help="bind one explicit Krab response item"
    )
    interaction_answer.add_argument("interaction_id")
    interaction_answer.add_argument(
        "--response-item", required=True, dest="response_item_id"
    )
    interaction_answer.add_argument("--expected-version", required=True, type=int)

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
    run_create.add_argument(
        "--supervision-policy",
        type=Path,
        help="opt in with a versioned autonomous-supervision policy",
    )
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
        default=DEFAULT_RUN_TIMEOUT_SECONDS,
        help="maximum agent runtime in seconds (default: %(default)s)",
    )
    run_execute.add_argument(
        "--runtime-adapter",
        help="override the pinned profile's runtime adapter for this attempt",
    )
    run_commands.add_parser(
        "reconcile", help="clean and fail runs interrupted by an orchestrator restart"
    )

    supervisor = commands.add_parser(
        "supervisor", help="run the autonomous control loop"
    )
    supervisor_commands = supervisor.add_subparsers(
        dest="supervisor_command", required=True
    )
    supervisor_run = supervisor_commands.add_parser(
        "run", help="advance opted-in work until stopped"
    )
    supervisor_run.add_argument(
        "--poll-interval",
        type=_positive_seconds,
        default=2.0,
        help="seconds between reconciliation passes (default: %(default)s)",
    )
    supervisor_run.add_argument(
        "--max-workers",
        type=_worker_count,
        default=4,
        help="maximum concurrent run actions (default: %(default)s)",
    )
    supervisor_run.add_argument(
        "--max-discoveries-per-tick",
        type=_supervisor_discovery_count,
        default=240,
        help="maximum source rows inspected per tick (default: %(default)s)",
    )
    supervisor_run.add_argument(
        "--action-lease-seconds",
        type=_action_lease_seconds,
        default=60,
        help="durable action claim lifetime (default: %(default)s)",
    )
    supervisor_run.add_argument(
        "--run-timeout",
        type=_positive_seconds,
        default=DEFAULT_RUN_TIMEOUT_SECONDS,
        help="maximum agent runtime in seconds (default: %(default)s)",
    )
    supervisor_run.add_argument(
        "--action-timeout",
        type=_positive_seconds,
        default=None,
        help=(
            "maximum coordinator action runtime; defaults to --run-timeout plus "
            "300 seconds"
        ),
    )
    supervisor_run.add_argument(
        "--log-level",
        type=str.upper,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="WARNING",
        help="stderr diagnostic level (default: %(default)s)",
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

    candidate = commands.add_parser(
        "candidate", help="materialize and inspect trusted candidates"
    )
    candidate_commands = candidate.add_subparsers(
        dest="candidate_command", required=True
    )
    candidate_create = candidate_commands.add_parser(
        "create", help="materialize one retained result as a candidate"
    )
    candidate_create.add_argument("attempt_id")
    candidate_create.add_argument("--created-by", required=True)
    candidate_create.add_argument(
        "--operation-timeout",
        dest="candidate_timeout",
        type=_positive_seconds,
        default=DEFAULT_GIT_TIMEOUT_SECONDS,
        help="maximum candidate operation time in seconds (default: %(default)s)",
    )
    candidate_show = candidate_commands.add_parser(
        "show", help="verify and show one trusted candidate"
    )
    candidate_show.add_argument("candidate_id")
    candidate_show.add_argument(
        "--operation-timeout",
        dest="candidate_timeout",
        type=_positive_seconds,
        default=DEFAULT_GIT_TIMEOUT_SECONDS,
        help="maximum candidate operation time in seconds (default: %(default)s)",
    )
    candidate_list = candidate_commands.add_parser(
        "list", help="list recent durable candidates without reading Git objects"
    )
    candidate_scope = candidate_list.add_mutually_exclusive_group()
    candidate_scope.add_argument("--project", dest="candidate_project_key")
    candidate_scope.add_argument("--run", dest="candidate_run_id")
    candidate_scope.add_argument("--work-thread", dest="candidate_work_thread_id")
    candidate_list.add_argument(
        "--limit",
        type=_candidate_limit,
        default=100,
        help="maximum candidates to return (1-500; default: 100)",
    )

    gate = commands.add_parser("gate", help="manage exact-input integration gates")
    gate_commands = gate.add_subparsers(dest="gate_command", required=True)
    gate_policy_create = gate_commands.add_parser(
        "policy-create", help="pin a gate policy to one exact candidate"
    )
    gate_policy_create.add_argument("candidate_id")
    gate_policy_create.add_argument("--policy", required=True, type=Path)
    gate_policy_create.add_argument("--created-by", required=True)
    gate_policy_create.add_argument(
        "--fold", help="bind the fresh policy to this recorded fold"
    )
    gate_policy_show = gate_commands.add_parser(
        "policy-show", help="show one immutable gate policy"
    )
    gate_policy_show.add_argument("policy_id")
    gate_policy_list = gate_commands.add_parser(
        "policy-list", help="list recent immutable gate policies"
    )
    gate_policy_scope = gate_policy_list.add_mutually_exclusive_group()
    gate_policy_scope.add_argument("--candidate", dest="gate_candidate_id")
    gate_policy_scope.add_argument("--project", dest="gate_project_key")
    gate_policy_scope.add_argument("--work-thread", dest="gate_work_thread_id")
    gate_policy_list.add_argument(
        "--limit",
        type=_candidate_limit,
        default=100,
        help="maximum policies to return (1-500; default: 100)",
    )
    gate_review_create = gate_commands.add_parser(
        "review-create", help="create a candidate-backed independent review run"
    )
    gate_review_create.add_argument("policy_id")
    gate_review_create.add_argument("--profile", required=True)
    gate_review_create.add_argument("--requested-by", required=True)
    gate_review_create.add_argument("--expected-policy-revision", required=True)
    gate_review_create.add_argument("--krab-session")
    gate_review_create.add_argument("--krab-claim")
    gate_review_create.add_argument(
        "--supervision-policy",
        type=Path,
        help="opt in with a versioned autonomous-supervision policy",
    )
    gate_review = gate_commands.add_parser(
        "review-record", help="record independent-review evidence"
    )
    gate_review.add_argument("policy_id")
    gate_review.add_argument("--reviewer-attempt", required=True)
    gate_review.add_argument("--outcome", required=True, choices=("passed", "failed"))
    gate_review.add_argument("--recorded-by", required=True)
    gate_review.add_argument("--expected-policy-revision", required=True)
    gate_maintainer = gate_commands.add_parser(
        "maintainer-record",
        help="record an authorized product disposition for an exact candidate",
    )
    gate_maintainer.add_argument("policy_id")
    gate_maintainer.add_argument("--review", required=True, type=Path)
    gate_maintainer.add_argument("--recorded-by", required=True)
    gate_maintainer.add_argument("--expected-policy-revision", required=True)
    gate_maintainer_list = gate_commands.add_parser(
        "maintainer-list", help="list append-only maintainer dispositions"
    )
    gate_maintainer_list.add_argument("policy_id")
    gate_maintainer_list.add_argument(
        "--limit",
        type=_evidence_limit,
        default=500,
        help="maximum maintainer reviews to return (1-1000; default: 500)",
    )
    gate_changes_create = gate_commands.add_parser(
        "changes-create",
        help="return a changes-requested candidate and feedback to a developer run",
    )
    gate_changes_create.add_argument("policy_id")
    gate_feedback = gate_changes_create.add_mutually_exclusive_group(required=True)
    gate_feedback.add_argument("--review-evidence")
    gate_feedback.add_argument("--maintainer-review")
    gate_changes_create.add_argument("--review-entry")
    gate_changes_create.add_argument("--profile", required=True)
    gate_changes_create.add_argument("--requested-by", required=True)
    gate_changes_create.add_argument("--expected-policy-revision", required=True)
    gate_changes_create.add_argument("--krab-session")
    gate_changes_create.add_argument("--krab-claim")
    gate_changes_create.add_argument(
        "--supervision-policy",
        type=Path,
        help="opt in with a versioned autonomous-supervision policy",
    )
    gate_validate = gate_commands.add_parser(
        "validate", help="run one pinned local validation gate"
    )
    gate_validate.add_argument("policy_id")
    gate_validate.add_argument("validation_name")
    gate_validate.add_argument("--recorded-by", required=True)
    gate_validate.add_argument("--expected-policy-revision", required=True)
    gate_validate.add_argument(
        "--run-timeout",
        type=_positive_seconds,
        default=DEFAULT_RUN_TIMEOUT_SECONDS,
        help="maximum validation runtime in seconds (default: %(default)s)",
    )
    gate_validate.add_argument(
        "--operation-timeout",
        dest="candidate_timeout",
        type=_positive_seconds,
        default=DEFAULT_GIT_TIMEOUT_SECONDS,
        help="maximum candidate operation time in seconds (default: %(default)s)",
    )
    gate_evidence = gate_commands.add_parser(
        "evidence-list", help="list append-only gate evidence"
    )
    gate_evidence.add_argument("policy_id")
    gate_evidence.add_argument(
        "--limit",
        type=_evidence_limit,
        default=500,
        help="maximum evidence records to return (1-1000; default: 500)",
    )
    gate_readiness = gate_commands.add_parser(
        "readiness", help="derive current exact-input readiness"
    )
    gate_readiness.add_argument("policy_id")

    integration = commands.add_parser(
        "integration", help="order, refold, and land exact candidates"
    )
    integration_commands = integration.add_subparsers(
        dest="integration_command", required=True
    )
    dependency_add = integration_commands.add_parser(
        "dependency-add", help="add a hard work-item landing dependency"
    )
    dependency_add.add_argument("dependent_work_item_id")
    dependency_add.add_argument("prerequisite_work_item_id")
    dependency_add.add_argument("--created-by", required=True)
    dependency_remove = integration_commands.add_parser(
        "dependency-remove", help="remove a hard landing dependency"
    )
    dependency_remove.add_argument("dependency_id")
    dependency_remove.add_argument("--expected-version", required=True, type=int)
    dependency_remove.add_argument("--removed-by", required=True)
    dependency_list = integration_commands.add_parser(
        "dependency-list", help="list durable work-item landing dependencies"
    )
    dependency_list.add_argument("--project")
    dependency_list.add_argument("--active-only", action="store_true")
    dependency_list.add_argument("--limit", type=_candidate_limit, default=500)
    enqueue = integration_commands.add_parser(
        "enqueue", help="queue one gate policy for its integration ref"
    )
    enqueue.add_argument("policy_id")
    enqueue.add_argument("--created-by", required=True)
    queue_show = integration_commands.add_parser(
        "show", help="show one integration queue item and its event stream"
    )
    queue_show.add_argument("queue_item_id")
    queue_list = integration_commands.add_parser(
        "list", help="list durable integration queue items"
    )
    queue_list.add_argument("--project")
    queue_list.add_argument("--limit", type=_candidate_limit, default=100)
    refold = integration_commands.add_parser(
        "refold", help="replay an original result onto the current integration base"
    )
    refold.add_argument("queue_item_id")
    refold.add_argument("--expected-version", required=True, type=int)
    refold.add_argument("--created-by", required=True)
    refold.add_argument(
        "--operation-timeout",
        dest="candidate_timeout",
        type=_positive_seconds,
        default=DEFAULT_GIT_TIMEOUT_SECONDS,
    )
    activate = integration_commands.add_parser(
        "activate", help="bind a fresh folded policy and return the item to queue"
    )
    activate.add_argument("queue_item_id")
    activate.add_argument("--fold", required=True)
    activate.add_argument("--policy", required=True)
    activate.add_argument("--expected-version", required=True, type=int)
    activate.add_argument("--actor", required=True)
    claim = integration_commands.add_parser(
        "claim", help="lease one ready item on its serialized ref lane"
    )
    claim.add_argument("queue_item_id")
    claim.add_argument("--owner", required=True)
    claim.add_argument("--expected-version", required=True, type=int)
    claim.add_argument("--lease-seconds", type=_integration_lease_seconds, default=60)
    release = integration_commands.add_parser(
        "release", help="release a claimed item before authority is issued"
    )
    release.add_argument("queue_item_id")
    release.add_argument("--owner", required=True)
    release.add_argument("--expected-version", required=True, type=int)
    cancel = integration_commands.add_parser(
        "cancel", help="cancel an item before landing authority is issued"
    )
    cancel.add_argument("queue_item_id")
    cancel.add_argument("--expected-version", required=True, type=int)
    cancel.add_argument("--cancelled-by", required=True)
    plan = integration_commands.add_parser(
        "plan", help="issue an exact argv-only manual landing plan"
    )
    plan.add_argument("queue_item_id")
    plan.add_argument("--owner", required=True)
    plan.add_argument("--expected-version", required=True, type=int)
    plan.add_argument("--remote", default="origin")
    plan.add_argument("--issued-by", required=True)
    plan.add_argument(
        "--operation-timeout",
        dest="candidate_timeout",
        type=_positive_seconds,
        default=DEFAULT_GIT_TIMEOUT_SECONDS,
    )
    plan_show = integration_commands.add_parser(
        "plan-show", help="show one issued plan without its raw nonce"
    )
    plan_show.add_argument("plan_id")
    plan_reissue = integration_commands.add_parser(
        "plan-reissue",
        help="replace a lost plan nonce without releasing its lane",
    )
    plan_reissue.add_argument("plan_id")
    plan_reissue.add_argument("--expected-generation", required=True, type=int)
    plan_reissue.add_argument("--reissued-by", required=True)
    receipt = integration_commands.add_parser(
        "receipt", help="record one nonce-bound landed or not-landed outcome"
    )
    receipt.add_argument("--document", required=True, type=Path)
    receipt.add_argument("--recorded-by", required=True)
    provider_issue = integration_commands.add_parser(
        "provider-issue",
        help="issue exact landing authority to the bound provider",
    )
    provider_issue.add_argument("queue_item_id")
    provider_issue.add_argument("--owner", required=True)
    provider_issue.add_argument("--expected-version", required=True, type=int)
    provider_issue.add_argument("--resource-id", required=True)
    provider_issue.add_argument(
        "--merge-method", required=True, choices=("merge", "rebase", "squash")
    )
    provider_issue.add_argument("--issued-by", required=True)
    provider_show = integration_commands.add_parser(
        "provider-show",
        help="show one provider landing and its service operation",
    )
    provider_show.add_argument("provider_landing_id")
    provider_execute = integration_commands.add_parser(
        "provider-execute", help="execute and settle one provider landing"
    )
    provider_execute.add_argument("provider_landing_id")
    provider_execute.add_argument("--owner", required=True)
    provider_execute.add_argument("--settled-by", required=True)
    provider_settle = integration_commands.add_parser(
        "provider-settle",
        help="settle one externally completed provider landing",
    )
    provider_settle.add_argument("provider_landing_id")
    provider_settle.add_argument("--settled-by", required=True)
    return parser


def _build_result_store(control: ControlPlane) -> WorkspaceResultStore:
    return WorkspaceResultStore(default_result_root(control.database.path))


def _build_candidate_store(
    control: ControlPlane, *, timeout: float = DEFAULT_GIT_TIMEOUT_SECONDS
) -> GitCandidateStore:
    return GitCandidateStore(
        default_candidate_root(control.database.path), timeout=timeout
    )


def _build_run_coordinator(
    control: ControlPlane,
    *,
    run_timeout: float | None = DEFAULT_RUN_TIMEOUT_SECONDS,
    result_store: WorkspaceResultStore | None = None,
) -> RunCoordinator:
    workspaces = WorkspaceManager(
        control.database.path.parent / "workspaces",
        GitSnapshotter(),
    )
    retained_results = result_store or _build_result_store(control)
    candidate_workspaces = CandidateReviewWorkspaces(
        control.database,
        workspaces,
        retained_results,
        _build_candidate_store(control),
    )
    return RunCoordinator(
        control.database,
        DockerExecutor(DockerCLI(run_timeout=run_timeout)),
        workspaces,
        FileGatewayManager(
            default_gateway_root(control.database.path),
            control,
        ),
        retained_results,
        candidate_workspaces=candidate_workspaces,
    )


def _build_workflow_director(
    control: ControlPlane, coordinator: RunCoordinator
) -> WorkflowDirector:
    return WorkflowDirector(
        control,
        coordinator.result_store,
        _build_candidate_store(control),
        coordinator.executor,
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
        with _handle_sigterm():
            server.serve_forever()
    except (KeyboardInterrupt, _TerminationRequested):
        pass
    finally:
        server.server_close()
    return 0


def _serve_supervisor(
    args: argparse.Namespace,
    control: ControlPlane,
    coordinator: RunCoordinator,
) -> int:
    action_timeout = (
        args.run_timeout + 300.0 if args.action_timeout is None else args.action_timeout
    )
    if action_timeout <= args.run_timeout:
        raise InputError("--action-timeout must exceed --run-timeout")
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    control.initialize()
    workflow_director = _build_workflow_director(control, coordinator)
    discovery_director = DiscoveryDirector(control)
    advisory_director = AdvisoryDirector(control)
    supervisor = Supervisor(
        control.database,
        coordinator,
        control,
        workflow_director=workflow_director,
        discovery_director=discovery_director,
        advisory_director=advisory_director,
        poll_interval=args.poll_interval,
        max_workers=args.max_workers,
        max_discoveries_per_tick=getattr(args, "max_discoveries_per_tick", 240),
        action_lease_seconds=args.action_lease_seconds,
        action_timeout=action_timeout,
    )
    _emit(
        {
            "supervisor_id": supervisor.supervisor_id,
            "poll_interval_seconds": args.poll_interval,
            "max_workers": args.max_workers,
            "max_discoveries_per_tick": getattr(args, "max_discoveries_per_tick", 240),
            "log_level": args.log_level,
            "action_lease_seconds": args.action_lease_seconds,
            "action_timeout_seconds": action_timeout,
            "authority": "explicit_run_policy",
        }
    )
    sys.stdout.flush()
    try:
        with _handle_sigterm():
            supervisor.run_forever()
    except (KeyboardInterrupt, _TerminationRequested):
        pass
    finally:
        supervisor.close()
    return 0


def _dispatch(
    args: argparse.Namespace,
    control: ControlPlane,
    coordinator: RunCoordinator | None = None,
    result_store: WorkspaceResultStore | None = None,
    candidate_store: GitCandidateStore | None = None,
    gate_engine: DockerCLI | _LazyDockerImageInspector | None = None,
    gate_executor: DockerExecutor | None = None,
    coordinator_factory: Callable[[], RunCoordinator] | None = None,
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
    if args.command == "workflow":
        if args.workflow_command in {"config-create", "config-revise"}:
            kwargs = {
                "project_key": args.project,
                "name": args.name,
                "spec": WorkflowConfigurationSpec.from_mapping(_json_file(args.spec)),
                "producer_profile_name": args.producer,
                "reviewer_profile_name": args.reviewer,
                "created_by_handle": args.created_by,
            }
            if args.workflow_command == "config-create":
                return control.create_workflow_configuration(**kwargs)
            return control.revise_workflow_configuration(**kwargs)
        if args.workflow_command == "config-show":
            return control.get_workflow_configuration(
                project_key=args.project,
                name=args.name,
                version=args.version,
            )
        if args.workflow_command == "config-list":
            return control.list_workflow_configurations(project_key=args.project)
        if args.workflow_command == "start":
            return control.start_workflow_execution(
                project_key=args.project,
                configuration_name=args.configuration_name,
                work_item_id=args.work_item,
                source_revision=args.source_revision,
                expected_configuration_version=(args.expected_configuration_version),
                expected_context_version_id=args.expected_context_version_id,
                expected_work_item_version_id=(args.expected_work_item_version_id),
                expected_thread_generation=args.expected_thread_generation,
                expected_thread_version=args.expected_thread_version,
                idempotency_key=args.idempotency_key,
                requested_by_handle=args.requested_by,
                discovery_execution_id=args.discovery_execution,
            )
        if args.workflow_command == "show":
            return control.get_workflow_execution(args.execution_id)
        if args.workflow_command == "list":
            return control.list_workflow_executions(
                project_key=args.project,
                state=args.state,
                limit=args.limit,
            )
        if args.workflow_command == "cancel":
            return control.cancel_workflow_execution(
                args.execution_id,
                expected_version=args.expected_version,
                cancelled_by_handle=args.cancelled_by,
                reason=args.reason,
            )
        if args.workflow_command == "ruling-show":
            return control.get_workflow_ruling(args.ruling_id)
        if args.workflow_command == "ruling-resolve":
            return control.resolve_workflow_ruling(
                args.ruling_id,
                expected_version=args.expected_version,
                decision=args.decision,
                resolved_by_handle=args.resolved_by,
            )
        if args.workflow_command == "provider-target":
            return control.configure_workflow_provider_landing(
                args.execution_id,
                expected_version=args.expected_version,
                resource_id=args.resource_id,
                merge_method=args.merge_method,
                configured_by_handle=args.configured_by,
            )
        if (
            args.workflow_command == "manual-plan"
            and result_store is not None
            and candidate_store is not None
        ):
            return control.issue_workflow_manual_landing(
                args.execution_id,
                expected_version=args.expected_version,
                remote=args.remote,
                issued_by_handle=args.issued_by,
                result_store=result_store,
                candidate_store=candidate_store,
            )
    if args.command == "discovery":
        if args.discovery_command in {"config-create", "config-revise"}:
            kwargs = {
                "project_key": args.project,
                "name": args.name,
                "spec": DiscoveryConfigurationSpec.from_mapping(_json_file(args.spec)),
                "investigator_profile_name": args.investigator,
                "coordinator_profile_name": args.coordinator,
                "created_by_handle": args.created_by,
            }
            if args.discovery_command == "config-create":
                return control.create_discovery_configuration(**kwargs)
            return control.revise_discovery_configuration(**kwargs)
        if args.discovery_command == "config-show":
            return control.get_discovery_configuration(
                project_key=args.project,
                name=args.name,
                version=args.version,
            )
        if args.discovery_command == "config-list":
            return control.list_discovery_configurations(project_key=args.project)
        if args.discovery_command == "start":
            return control.start_discovery_execution(
                project_key=args.project,
                configuration_name=args.configuration_name,
                work_item_id=args.work_item,
                source_revision=args.source_revision,
                expected_configuration_version=(args.expected_configuration_version),
                expected_context_version_id=args.expected_context_version_id,
                expected_work_item_version_id=(args.expected_work_item_version_id),
                expected_thread_generation=args.expected_thread_generation,
                expected_thread_version=args.expected_thread_version,
                krab_session_id=args.krab_session,
                krab_claim_id=args.krab_claim,
                idempotency_key=args.idempotency_key,
                requested_by_handle=args.requested_by,
            )
        if args.discovery_command == "show":
            return control.get_discovery_execution(args.execution_id)
        if args.discovery_command == "list":
            return control.list_discovery_executions(
                project_key=args.project,
                state=args.state,
                limit=args.limit,
            )
        if args.discovery_command == "cancel":
            return control.cancel_discovery_execution(
                args.execution_id,
                expected_version=args.expected_version,
                cancelled_by_handle=args.cancelled_by,
                reason=args.reason,
            )
    if args.command == "advisory":
        if args.advisory_command in {"config-create", "config-revise"}:
            kwargs = {
                "project_key": args.project,
                "name": args.name,
                "spec": AdvisoryConfigurationSpec.from_mapping(_json_file(args.spec)),
                "reviewer_one_profile_name": args.reviewer_one,
                "reviewer_two_profile_name": args.reviewer_two,
                "reviewer_three_profile_name": args.reviewer_three,
                "coordinator_profile_name": args.coordinator,
                "created_by_handle": args.created_by,
            }
            if args.advisory_command == "config-create":
                return control.create_advisory_configuration(**kwargs)
            return control.revise_advisory_configuration(**kwargs)
        if args.advisory_command == "config-show":
            return control.get_advisory_configuration(
                project_key=args.project,
                name=args.name,
                version=args.version,
            )
        if args.advisory_command == "config-list":
            return control.list_advisory_configurations(project_key=args.project)
        if args.advisory_command == "start":
            return control.start_advisory_execution(
                project_key=args.project,
                configuration_name=args.configuration_name,
                candidate_id=args.candidate,
                expected_configuration_version=args.expected_configuration_version,
                expected_context_version_id=args.expected_context_version_id,
                expected_work_item_version_id=args.expected_work_item_version_id,
                expected_thread_generation=args.expected_thread_generation,
                expected_thread_version=args.expected_thread_version,
                idempotency_key=args.idempotency_key,
                requested_by_handle=args.requested_by,
            )
        if args.advisory_command == "show":
            return control.get_advisory_execution(args.execution_id)
        if args.advisory_command == "list":
            return control.list_advisory_executions(
                project_key=args.project,
                state=args.state,
                limit=args.limit,
            )
        if args.advisory_command == "cancel":
            return control.cancel_advisory_execution(
                args.execution_id,
                expected_version=args.expected_version,
                cancelled_by_handle=args.cancelled_by,
                reason=args.reason,
            )
    if args.command == "inference-connector":
        if args.inference_connector_command == "register":
            return control.register_inference_connector(
                name=args.name,
                spec=InferenceConnectorSpec.from_mapping(_json_file(args.spec)),
                created_by_handle=args.created_by,
            )
        if args.inference_connector_command == "show":
            return control.database.get_inference_connector(args.name)
        if args.inference_connector_command == "list":
            return control.database.list_inference_connectors()
        if args.inference_connector_command == "revoke":
            return control.revoke_inference_connector(
                args.name,
                expected_version=args.expected_version,
                revoked_by_handle=args.revoked_by,
            )
    if args.command == "provider":
        if args.provider_command == "connector-add":
            return control.add_connector_provider_grant(
                name=args.name,
                spec=InferenceGrantSpec.from_mapping(_json_file(args.policy)),
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
    if args.command == "service-sidecar":
        if args.service_sidecar_command == "register":
            return control.register_service_sidecar(
                name=args.name,
                spec=ServiceSidecarSpec.from_mapping(_json_file(args.spec)),
                created_by_handle=args.created_by,
            )
        if args.service_sidecar_command == "show":
            return control.database.get_service_sidecar(args.name)
        if args.service_sidecar_command == "list":
            return control.database.list_service_sidecars()
        if args.service_sidecar_command == "revoke":
            return control.revoke_service_sidecar(
                args.name,
                expected_version=args.expected_version,
                revoked_by_handle=args.revoked_by,
            )
        if args.service_sidecar_command == "operation-create":
            return control.create_service_operation(
                spec=ServiceOperationSpec.from_mapping(_json_file(args.request)),
                created_by_handle=args.created_by,
                project_capability_binding_id=args.binding,
            )
        if args.service_sidecar_command == "operation-show":
            return control.database.get_service_operation(args.operation_id)
        if args.service_sidecar_command == "operation-list":
            return control.database.list_service_operations(limit=args.limit)
        if args.service_sidecar_command == "operation-execute":
            return control.execute_service_operation(
                args.operation_id,
                owner_id=args.owner,
                expected_version=args.expected_version,
            )
        if args.service_sidecar_command == "operation-reconcile":
            return control.reconcile_service_operation(
                args.operation_id,
                owner_id=args.owner,
                expected_version=args.expected_version,
            )
        if args.service_sidecar_command == "execution-submit-create":
            return control.create_execution_submission(
                project_key=args.project,
                candidate_id=args.candidate,
                recipe=ExecutionRecipeAuthority.from_mapping(
                    {
                        "recipe_id": args.recipe_id,
                        "recipe_sha256": args.recipe_sha256,
                        "image_id": args.image_id,
                    }
                ),
                request_key=args.request_key,
                created_by_handle=args.created_by,
            )
        if args.service_sidecar_command == "execution-followup-create":
            return control.create_execution_followup(
                submission_operation_id=args.submission_operation_id,
                action=args.action,
                request_key=args.request_key,
                created_by_handle=args.created_by,
            )
    if args.command == "capability":
        if args.capability_command == "bind":
            return control.bind_project_capability(
                project_key=args.project_key,
                capability_name=args.capability_name,
                capability_version=args.version,
                sidecar_name=args.sidecar,
                mapping_name=args.mapping,
                bound_by_handle=args.bound_by,
            )
        if args.capability_command == "show":
            return control.database.get_project_capability_binding(args.binding_id)
        if args.capability_command == "list":
            return control.database.list_project_capability_bindings(
                project_key=args.project_key,
                include_unbound=args.include_unbound,
            )
        if args.capability_command == "unbind":
            return control.unbind_project_capability(
                args.binding_id,
                expected_version=args.expected_version,
                unbound_by_handle=args.unbound_by,
            )
    if args.command == "work-item":
        if args.work_item_command == "import":
            return control.import_work_item(
                project_key=args.project,
                envelope=WorkItemEnvelope.from_mapping(_json_file(args.document)),
                imported_by_handle=args.imported_by,
            )
        if args.work_item_command == "import-operation":
            return control.import_work_item_from_service_operation(
                project_key=args.project,
                operation_id=args.operation_id,
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
    if args.command == "interaction" and args.interaction_command == "answer":
        accepted = control.accept_interaction_response(
            args.interaction_id,
            response_item_id=args.response_item_id,
            expected_version=args.expected_version,
        )
        # The response title and body are intentionally transient.  Operator
        # surfaces confirm only the binding and resulting lifecycle states.
        return {
            "interaction_id": accepted.interaction_id,
            "interaction_state": accepted.interaction_state,
            "interaction_version": accepted.interaction_version,
            "response_item_id": accepted.response_item_id,
            "run_id": accepted.run_id,
            "run_state": accepted.run_state,
            "run_version": accepted.run_version,
        }
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
                supervision_policy=(
                    SupervisionPolicy.from_mapping(_json_file(args.supervision_policy))
                    if args.supervision_policy is not None
                    else None
                ),
            )
        if args.run_command == "cancel":
            requested = control.request_run_cancellation(
                args.run_id,
                requested_by_handle=args.requested_by,
                expected_run_version=args.expected_version,
                reason=args.reason,
            )
            if requested.get("state") == "cancelled":
                return requested
            if coordinator is None and coordinator_factory is not None:
                coordinator = coordinator_factory()
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
            return coordinator.execute(
                args.run_id,
                runtime_adapter=args.runtime_adapter,
            )
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
    if args.command == "candidate" and args.candidate_command == "list":
        return control.list_candidates(
            project_key=args.candidate_project_key,
            producer_run_id=args.candidate_run_id,
            work_thread_id=args.candidate_work_thread_id,
            limit=args.limit,
        )
    if (
        args.command == "candidate"
        and result_store is not None
        and candidate_store is not None
    ):
        if args.candidate_command == "create":
            return control.create_candidate(
                args.attempt_id,
                created_by_handle=args.created_by,
                result_store=result_store,
                candidate_store=candidate_store,
            )
        if args.candidate_command == "show":
            return control.get_candidate(
                args.candidate_id,
                result_store=result_store,
                candidate_store=candidate_store,
            )
    if args.command == "gate":
        if args.gate_command == "policy-create" and gate_engine is not None:
            return control.create_gate_policy(
                args.candidate_id,
                _json_file(args.policy),
                created_by_handle=args.created_by,
                image_resolver=gate_engine.inspect_image,
                fold_id=args.fold,
            )
        if args.gate_command == "policy-show":
            return control.get_gate_policy(args.policy_id)
        if args.gate_command == "policy-list":
            return control.list_gate_policies(
                candidate_id=args.gate_candidate_id,
                project_key=args.gate_project_key,
                work_thread_id=args.gate_work_thread_id,
                limit=args.limit,
            )
        if args.gate_command == "review-create":
            return control.create_review_run(
                args.policy_id,
                profile_name=args.profile,
                requested_by_handle=args.requested_by,
                expected_policy_revision=args.expected_policy_revision,
                krab_session_id=args.krab_session,
                krab_claim_id=args.krab_claim,
                supervision_policy=(
                    SupervisionPolicy.from_mapping(_json_file(args.supervision_policy))
                    if args.supervision_policy is not None
                    else None
                ),
            )
        if args.gate_command == "review-record":
            return control.record_review_evidence(
                args.policy_id,
                reviewer_attempt_id=args.reviewer_attempt,
                outcome=args.outcome,
                recorded_by_handle=args.recorded_by,
                expected_policy_revision=args.expected_policy_revision,
            )
        if args.gate_command == "maintainer-record":
            return control.record_maintainer_review(
                args.policy_id,
                review_request=_json_file(args.review),
                recorded_by_handle=args.recorded_by,
                expected_policy_revision=args.expected_policy_revision,
            )
        if args.gate_command == "maintainer-list":
            return control.list_maintainer_reviews(args.policy_id, limit=args.limit)
        if args.gate_command == "changes-create":
            return control.create_changes_run(
                args.policy_id,
                review_evidence_id=args.review_evidence,
                review_entry_id=args.review_entry,
                maintainer_review_id=args.maintainer_review,
                profile_name=args.profile,
                requested_by_handle=args.requested_by,
                expected_policy_revision=args.expected_policy_revision,
                krab_session_id=args.krab_session,
                krab_claim_id=args.krab_claim,
                supervision_policy=(
                    SupervisionPolicy.from_mapping(_json_file(args.supervision_policy))
                    if args.supervision_policy is not None
                    else None
                ),
            )
        if (
            args.gate_command == "validate"
            and result_store is not None
            and candidate_store is not None
            and gate_executor is not None
        ):
            return control.run_local_validation(
                args.policy_id,
                validation_name=args.validation_name,
                recorded_by_handle=args.recorded_by,
                expected_policy_revision=args.expected_policy_revision,
                result_store=result_store,
                candidate_store=candidate_store,
                executor=gate_executor,
            )
        if args.gate_command == "evidence-list":
            return control.list_gate_evidence(args.policy_id, limit=args.limit)
        if args.gate_command == "readiness":
            return control.gate_readiness(args.policy_id)
    if args.command == "integration":
        if args.integration_command == "dependency-add":
            return control.add_landing_dependency(
                args.dependent_work_item_id,
                args.prerequisite_work_item_id,
                created_by_handle=args.created_by,
            )
        if args.integration_command == "dependency-remove":
            return control.remove_landing_dependency(
                args.dependency_id,
                expected_version=args.expected_version,
                removed_by_handle=args.removed_by,
            )
        if args.integration_command == "dependency-list":
            return control.list_landing_dependencies(
                project_key=args.project,
                active_only=args.active_only,
                limit=args.limit,
            )
        if args.integration_command == "enqueue":
            return control.enqueue_integration(
                args.policy_id, created_by_handle=args.created_by
            )
        if args.integration_command == "show":
            return control.get_integration_queue_item(args.queue_item_id)
        if args.integration_command == "list":
            return control.list_integration_queue(
                project_key=args.project, limit=args.limit
            )
        if (
            args.integration_command == "refold"
            and result_store is not None
            and candidate_store is not None
        ):
            return control.refold_integration_item(
                args.queue_item_id,
                expected_version=args.expected_version,
                created_by_handle=args.created_by,
                result_store=result_store,
                candidate_store=candidate_store,
            )
        if args.integration_command == "activate":
            return control.activate_integration_fold(
                args.queue_item_id,
                fold_id=args.fold,
                policy_id=args.policy,
                expected_version=args.expected_version,
                actor_handle=args.actor,
            )
        if args.integration_command == "claim":
            return control.claim_integration_item(
                args.queue_item_id,
                owner_id=args.owner,
                expected_version=args.expected_version,
                lease_seconds=args.lease_seconds,
            )
        if args.integration_command == "release":
            return control.release_integration_claim(
                args.queue_item_id,
                owner_id=args.owner,
                expected_version=args.expected_version,
            )
        if args.integration_command == "cancel":
            return control.cancel_integration_item(
                args.queue_item_id,
                expected_version=args.expected_version,
                cancelled_by_handle=args.cancelled_by,
            )
        if (
            args.integration_command == "plan"
            and result_store is not None
            and candidate_store is not None
        ):
            return control.issue_landing_plan(
                args.queue_item_id,
                owner_id=args.owner,
                expected_version=args.expected_version,
                remote=args.remote,
                issued_by_handle=args.issued_by,
                result_store=result_store,
                candidate_store=candidate_store,
            )
        if args.integration_command == "plan-show":
            return control.get_landing_plan(args.plan_id)
        if args.integration_command == "plan-reissue":
            return control.reissue_landing_plan_nonce(
                args.plan_id,
                expected_generation=args.expected_generation,
                reissued_by_handle=args.reissued_by,
            )
        if args.integration_command == "receipt":
            return control.record_landing_receipt(
                _json_file(args.document), recorded_by_handle=args.recorded_by
            )
        if args.integration_command == "provider-issue":
            return control.issue_provider_landing(
                args.queue_item_id,
                owner_id=args.owner,
                expected_version=args.expected_version,
                resource_id=args.resource_id,
                merge_method=args.merge_method,
                issued_by_handle=args.issued_by,
            )
        if args.integration_command == "provider-show":
            return control.get_provider_landing(args.provider_landing_id)
        if args.integration_command == "provider-execute":
            return control.execute_provider_landing(
                args.provider_landing_id,
                owner_id=args.owner,
                settled_by_handle=args.settled_by,
            )
        if args.integration_command == "provider-settle":
            return control.settle_provider_landing(
                args.provider_landing_id,
                settled_by_handle=args.settled_by,
            )
    raise RuntimeError("unreachable command")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    control = ControlPlane(Database(args.db))
    try:
        coordinator = None
        result_store = None
        candidate_store = None
        gate_engine = None
        gate_executor = None
        coordinator_factory = None
        if args.command == "run" and args.run_command in {"execute", "reconcile"}:
            result_store = _build_result_store(control)
            coordinator = _build_run_coordinator(
                control,
                run_timeout=(
                    args.run_timeout if args.run_command == "execute" else None
                ),
                result_store=result_store,
            )
        elif args.command == "run" and args.run_command == "cancel":

            def build_cancel_coordinator() -> RunCoordinator:
                cancel_result_store = _build_result_store(control)
                return _build_run_coordinator(
                    control,
                    result_store=cancel_result_store,
                )

            coordinator_factory = build_cancel_coordinator
        elif args.command == "control-room" and args.control_room_command == "serve":
            result_store = _build_result_store(control)
            coordinator = _build_run_coordinator(
                control,
                result_store=result_store,
            )
        elif args.command == "supervisor" and args.supervisor_command == "run":
            result_store = _build_result_store(control)
            coordinator = _build_run_coordinator(
                control,
                run_timeout=args.run_timeout,
                result_store=result_store,
            )
        elif args.command == "result":
            result_store = _build_result_store(control)
        elif args.command == "candidate" and args.candidate_command != "list":
            result_store = _build_result_store(control)
            candidate_store = _build_candidate_store(
                control, timeout=args.candidate_timeout
            )
        elif args.command == "gate" and args.gate_command == "policy-create":
            gate_engine = _LazyDockerImageInspector()
        elif args.command == "gate" and args.gate_command == "validate":
            result_store = _build_result_store(control)
            candidate_store = _build_candidate_store(
                control, timeout=args.candidate_timeout
            )
            gate_engine = DockerCLI(run_timeout=args.run_timeout)
            gate_executor = DockerExecutor(gate_engine)
        elif (
            args.command == "integration"
            and args.integration_command in {"refold", "plan"}
        ) or (args.command == "workflow" and args.workflow_command == "manual-plan"):
            result_store = _build_result_store(control)
            candidate_store = _build_candidate_store(
                control, timeout=args.candidate_timeout
            )
        if (
            args.command == "control-room"
            and args.control_room_command == "serve"
            and coordinator is not None
        ):
            return _serve_control_room(args, control, coordinator)
        if (
            args.command == "supervisor"
            and args.supervisor_command == "run"
            and coordinator is not None
        ):
            return _serve_supervisor(args, control, coordinator)
        result = _dispatch(
            args,
            control,
            coordinator,
            result_store,
            candidate_store,
            gate_engine,
            gate_executor,
            coordinator_factory,
        )
    except (
        CandidateError,
        CredentialError,
        ExecutionError,
        GateError,
        GatewayError,
        InputError,
        InferenceConnectorError,
        IntegrationError,
        KrabError,
        ProviderError,
        ResultError,
        RuntimeAdapterError,
        RunOrchestrationError,
        ServiceSidecarError,
        SupervisorError,
        StorageError,
        ValidationError,
        WorkerTransportError,
        WorkspaceError,
    ) as exc:
        _emit(
            {"error": {"type": type(exc).__name__, "message": str(exc)}},
            stream=sys.stderr,
        )
        return 2
    except sqlite3.Error as exc:
        _emit(
            {
                "error": {
                    "type": type(exc).__name__,
                    "message": "database operation failed",
                }
            },
            stream=sys.stderr,
        )
        return 2
    except UnicodeError as exc:
        _emit(
            {
                "error": {
                    "type": type(exc).__name__,
                    "message": "text decoding failed",
                }
            },
            stream=sys.stderr,
        )
        return 2
    _emit(result)
    return 0
