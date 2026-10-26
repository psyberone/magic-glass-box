#!/usr/bin/env python3
"""Drive the M13 prescription chain against a real Imitation Krab checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from magic_glass_box.advisory import AdvisoryConfigurationSpec
from magic_glass_box.advisory_director import AdvisoryDirector
from magic_glass_box.candidates import GitCandidateStore
from magic_glass_box.db import Database
from magic_glass_box.discovery import DiscoveryConfigurationSpec
from magic_glass_box.discovery_director import DiscoveryDirector
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    ProjectContext,
    WorkItemEnvelope,
    WorkspaceMode,
)
from magic_glass_box.executor import DockerCLI, DockerExecutor
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.inference import InferenceConnectorSpec
from magic_glass_box.krab import KrabClient, KrabIdentity
from magic_glass_box.openai_chatgpt import ChatGPTOAuth, ChatGPTProfileStore
from magic_glass_box.orchestration import CandidateReviewWorkspaces, RunCoordinator
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.supervision import Supervisor
from magic_glass_box.workflow_director import WorkflowDirector
from magic_glass_box.workflows import WorkflowConfigurationSpec
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

try:
    from scripts.check_live_imitation_krab_beta import (
        _changed_paths,
        _connector_socket_path,
        _git,
        _source_revision,
        _write_report,
    )
    from scripts.check_live_imitation_krab_workflow import (
        WORKFLOW_BUDGET_CEILING_MICROUSD,
        _clear_stale_socket,
        _ensure_private_directory,
        _validate_state_location,
        _workflow_connector_spec,
        _workflow_grant_spec,
        _write_workflow_connector_config,
    )
    from scripts.check_live_openai_chatgpt import (
        CONNECTOR_NAME,
        GRANT_NAME,
        RUNTIME_IMAGE,
        _assert_no_secret,
        _assert_sandbox_boundaries,
        _build_image,
        _collect_regular_files,
        _deployment_digest,
        _RecordingDockerEngine,
        _serialize_sandbox_plans,
        _session_secrets,
        _start_connector,
        _stop_connector,
    )
except ModuleNotFoundError:  # Direct execution puts scripts/ on sys.path.
    from check_live_imitation_krab_beta import (  # type: ignore[no-redef]
        _changed_paths,
        _connector_socket_path,
        _git,
        _source_revision,
        _write_report,
    )
    from check_live_imitation_krab_workflow import (  # type: ignore[no-redef]
        WORKFLOW_BUDGET_CEILING_MICROUSD,
        _clear_stale_socket,
        _ensure_private_directory,
        _validate_state_location,
        _workflow_connector_spec,
        _workflow_grant_spec,
        _write_workflow_connector_config,
    )
    from check_live_openai_chatgpt import (  # type: ignore[no-redef]
        CONNECTOR_NAME,
        GRANT_NAME,
        RUNTIME_IMAGE,
        _assert_no_secret,
        _assert_sandbox_boundaries,
        _build_image,
        _collect_regular_files,
        _deployment_digest,
        _RecordingDockerEngine,
        _serialize_sandbox_plans,
        _session_secrets,
        _start_connector,
        _stop_connector,
    )


PROJECT_KEY = "m13-prescription-proof"
WORK_ITEM_EXTERNAL_ID = "m13-health-liveness-language"
DISCOVERY_CONFIGURATION = "bounded-discovery"
WORKFLOW_CONFIGURATION = "scoped-implementation"
ADVISORY_CONFIGURATION = "advisory-panel"
REPORT_NAME = "m13-report.json"
VALIDATION_NAME = "full-suite"
DEFAULT_KRAB_PORT = 18765
MAX_LIVE_BUDGET_MICROUSD = 5_000_000
MAX_PROOF_SECONDS = 14_400.0
MAX_SCOPE_ANSWER_BYTES = 4096
SCOPE_DECISION_COPY_ONLY = "copy_only"
SCOPE_DECISION_READINESS = "behavioral_readiness"
SCOPE_DECISIONS = frozenset({SCOPE_DECISION_COPY_ONLY, SCOPE_DECISION_READINESS})

PROFILE_IDENTITIES = {
    "Discovery Investigator": "discoverer",
    "Discovery Coordinator": "scope_coord",
    "Implementation Producer": "producer",
    "Gate Reviewer": "gate_reviewer",
    "Advisory Reviewer One": "adv_one",
    "Advisory Reviewer Two": "adv_two",
    "Advisory Reviewer Three": "adv_three",
    "Advisory Coordinator": "adv_coord",
}


@dataclass(frozen=True)
class ScopeAnswer:
    schema_version: int
    decision: str
    rationale: str

    @property
    def body(self) -> str:
        return f"Scope decision: {self.decision}\nRationale: {self.rationale}"


@dataclass
class _KrabFixture:
    database: Any
    server: Any
    thread: threading.Thread
    base_url: str
    project_id: str
    project_key: str
    session_id: str
    users: dict[str, str]
    token_paths: dict[str, Path]

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise RuntimeError("live Krab server did not stop")


class _ConnectorWaves:
    """Give each active durable run set a fresh bounded connector process."""

    _EXECUTABLE_STATES = frozenset({"queued", "preparing", "running"})

    def __init__(
        self,
        database: Database,
        *,
        start: Callable[[], subprocess.Popen[str]],
        stop: Callable[[subprocess.Popen[str]], tuple[str, str]],
    ) -> None:
        self.database = database
        self.start = start
        self.stop = stop
        self.bound_runs: tuple[str, ...] = ()
        self.process: subprocess.Popen[str] | None = None
        self.stdout: list[str] = []
        self.stderr: list[str] = []

    def _active_runs(self) -> tuple[str, ...]:
        return tuple(
            run["id"]
            for run in self.database.iter_active_runs()
            if run["state"] in self._EXECUTABLE_STATES
        )

    def _stop_current(self) -> None:
        if self.process is None:
            return
        stdout, stderr = self.stop(self.process)
        self.stdout.append(stdout)
        self.stderr.append(stderr)
        self.process = None

    def before_tick(self) -> None:
        active = self._active_runs()
        if not active or active == self.bound_runs:
            return
        self._stop_current()
        self.process = self.start()
        self.bound_runs = active

    def close(self) -> tuple[str, str]:
        self._stop_current()
        return "\n".join(self.stdout), "\n".join(self.stderr)


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if not 1 <= parsed <= 64:
        raise argparse.ArgumentTypeError("must be between 1 and 64")
    return parsed


def _positive_seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if not 30 <= parsed <= MAX_PROOF_SECONDS:
        raise argparse.ArgumentTypeError(
            f"must be between 30 and {MAX_PROOF_SECONDS:g} seconds"
        )
    return parsed


def _port(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a TCP port") from exc
    if not 1024 <= parsed <= 65535:
        raise argparse.ArgumentTypeError("must be between 1024 and 65535")
    return parsed


def _budget_microusd(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if not 1 <= parsed <= MAX_LIVE_BUDGET_MICROUSD:
        raise argparse.ArgumentTypeError(
            f"must be between 1 and {MAX_LIVE_BUDGET_MICROUSD} microusd"
        )
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile-dir", required=True, type=Path)
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--model", required=True)
    parser.add_argument("--source-revision")
    parser.add_argument("--scope-answer-file", type=Path)
    parser.add_argument("--krab-port", type=_port, default=DEFAULT_KRAB_PORT)
    parser.add_argument("--image", default=RUNTIME_IMAGE)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--build-timeout", type=_positive_seconds, default=900.0)
    parser.add_argument("--run-timeout", type=_positive_seconds, default=900.0)
    parser.add_argument("--proof-timeout", type=_positive_seconds, default=10_800.0)
    parser.add_argument(
        "--max-invocations",
        type=_positive_integer,
        default=64,
        help="maximum connector calls for each active durable run wave",
    )
    parser.add_argument(
        "--budget-ceiling-microusd",
        type=_budget_microusd,
        default=WORKFLOW_BUDGET_CEILING_MICROUSD,
    )
    parser.add_argument(
        "--i-understand-this-uses-live-quota",
        action="store_true",
        help="required opt-in; the prescription workers consume live quota",
    )
    return parser.parse_args(argv)


def _reject_duplicate_pairs(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate field: {key}")
        result[key] = value
    return result


def _load_scope_answer(path: Path) -> ScopeAnswer:
    try:
        with path.expanduser().open("rb") as source:
            raw = source.read(MAX_SCOPE_ANSWER_BYTES + 1)
    except OSError as exc:
        raise RuntimeError("scope answer is unavailable") from exc
    if len(raw) > MAX_SCOPE_ANSWER_BYTES:
        raise RuntimeError("scope answer is too large")
    try:
        value = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_pairs
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise RuntimeError("scope answer is invalid") from exc
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "decision",
        "rationale",
    }:
        raise RuntimeError("scope answer is invalid")
    decision = value["decision"]
    rationale = value["rationale"]
    if value["schema_version"] != 1 or decision not in SCOPE_DECISIONS:
        raise RuntimeError("scope answer is invalid")
    if (
        not isinstance(rationale, str)
        or not rationale.strip()
        or "\x00" in rationale
        or len(rationale.encode("utf-8")) > 2048
    ):
        raise RuntimeError("scope answer is invalid")
    return ScopeAnswer(1, str(decision), rationale.strip())


def _project_context(repository: Path) -> ProjectContext:
    return ProjectContext.from_mapping(
        {
            "purpose": (
                "Imitation Krab is a small, loopback-first coordination service "
                "for cooperating human and agent participants. It provides "
                "authenticated identity, project and session scoping, text "
                "messaging, bounded work claims, and explicit status transitions."
            ),
            "repository_path": str(repository),
            "default_branch": "main",
            "instructions": [
                (
                    "Preserve the separation between server-attested metadata and "
                    "untrusted participant text. Prefer small explicit primitives "
                    "with clear authority, idempotency, optimistic concurrency, "
                    "bounded reads, and honest failure behavior."
                ),
                (
                    "Krab is not an agent runtime, workflow engine, Git hosting "
                    "integration, credential broker, plugin host, code-execution "
                    "service, or distributed scheduler."
                ),
                "Use only the Python standard library unless explicitly approved.",
                "New machine-readable interfaces require documented schemas.",
                (
                    "Use RED-first happy- and sad-path tests, retain at least 85% "
                    "branch-aware coverage, and focus mutation checks on guarantees."
                ),
                (
                    "Product fit asks whether the smallest Krab-native primitive "
                    "solves the request without importing orchestration concerns."
                ),
                (
                    "MGB process approval is not product or landing approval; "
                    "maintainer authority remains explicit."
                ),
                (
                    "Run `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m "
                    "unittest discover -s tests -v`."
                ),
                "Do not commit, push, open a pull request, or use the network.",
            ],
        }
    )


def _work_item() -> WorkItemEnvelope:
    return WorkItemEnvelope.from_mapping(
        {
            "schema_version": 1,
            "source": "manual",
            "external_id": WORK_ITEM_EXTERNAL_ID,
            "source_updated_at": "2026-10-25T22:30:00Z",
            "title": "Describe the Krab health command with honest probe language",
            "brief": (
                "The `krab health` subcommand help currently says `check daemon "
                "health without credentials`, while the README correctly calls it "
                "a liveness probe and explicitly says it is not readiness. Before "
                "implementation, Discovery must obtain the decision owner's explicit "
                "choice between `copy_only` (change CLI help to `probe daemon "
                "liveness without credentials`, add the smallest stable assertion, "
                "and leave behavior unchanged) and `behavioral_readiness` (expand "
                "the command to test readiness semantics). Do not infer the choice."
            ),
            "acceptance_criteria": [
                "Discovery records an explicit decision-owner scope choice.",
                (
                    "For `copy_only`, CLI help consistently describes a liveness "
                    "probe and no health-command behavior or schema changes."
                ),
                (
                    "For `behavioral_readiness`, implementation does not begin "
                    "without an explicit product-scope ruling."
                ),
                "The complete unittest suite passes.",
            ],
            "constraints": [
                "Use only the Python standard library.",
                "Keep loopback, authentication, and credential boundaries unchanged.",
                "Do not rename the command or add a new endpoint.",
                "Use RED-first happy- and sad-path tests.",
                "Do not commit, push, or open a pull request.",
            ],
            "context": [
                {
                    "type": "note",
                    "value": (
                        "This deliberately bounded ambiguity is the M13 durable "
                        "human-scope and process-replacement proof."
                    ),
                }
            ],
            "labels": ["mgb-live", "m13", "cli", "scope-proof"],
            "priority": "normal",
        }
    )


def _profile(
    *,
    image: str,
    workspace_mode: WorkspaceMode,
    capabilities: Sequence[GatewayCapability],
    krab: bool = False,
) -> AgentProfileSpec:
    credentials = (GRANT_NAME, "krab") if krab else (GRANT_NAME,)
    return AgentProfileSpec.create(
        image=image,
        command=("codex",),
        runtime_adapter="codex",
        runtime_provider_grant=GRANT_NAME,
        workspace_mode=workspace_mode,
        network_mode=NetworkMode.GATEWAY_ONLY,
        credential_names=credentials,
        gateway_capabilities=tuple(capabilities),
    )


def _discovery_spec() -> DiscoveryConfigurationSpec:
    return DiscoveryConfigurationSpec.from_mapping(
        {
            "schema_version": 1,
            "max_findings": 6,
            "max_must_items": 3,
            "max_follow_up_proposals": 3,
        }
    )


def _workflow_spec(image_id: str) -> WorkflowConfigurationSpec:
    return WorkflowConfigurationSpec.from_mapping(
        {
            "schema_version": 1,
            "review_separation": "different_profile_and_identity",
            "maintainer_approval": True,
            "validations": [
                {
                    "name": VALIDATION_NAME,
                    "image_id": image_id,
                    "command": [
                        "/bin/sh",
                        "-c",
                        "PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m "
                        "coverage run -m unittest discover -s tests -v && "
                        "python3 -m coverage report",
                    ],
                }
            ],
            "max_correction_rounds": 2,
            "landing_mode": "manual",
        }
    )


def _advisory_spec() -> AdvisoryConfigurationSpec:
    return AdvisoryConfigurationSpec.from_mapping(
        {
            "schema_version": 1,
            "max_findings_per_reviewer": 4,
            "max_challenges_per_reviewer": 4,
            "max_report_items": 8,
        }
    )


def _profile_specs(image: str) -> dict[str, AgentProfileSpec]:
    read_provider = (
        GatewayCapability.RUN_DESCRIBE,
        GatewayCapability.PROVIDER_INVOKE,
    )
    return {
        "Discovery Investigator": _profile(
            image=image,
            workspace_mode=WorkspaceMode.READ_ONLY,
            capabilities=(*read_provider, GatewayCapability.DISCOVERY_TRACE_SUBMIT),
        ),
        "Discovery Coordinator": _profile(
            image=image,
            workspace_mode=WorkspaceMode.READ_ONLY,
            capabilities=(
                *read_provider,
                GatewayCapability.INTERACTION_OPEN,
                GatewayCapability.INTERACTION_READ,
                GatewayCapability.HANDOFF_SUBMIT,
                GatewayCapability.DISCOVERY_RULING_SUBMIT,
            ),
            krab=True,
        ),
        "Implementation Producer": _profile(
            image=image,
            workspace_mode=WorkspaceMode.COPY_ON_WRITE,
            capabilities=(
                *read_provider,
                GatewayCapability.WORK_READ,
                GatewayCapability.WORK_APPEND,
            ),
        ),
        "Gate Reviewer": _profile(
            image=image,
            workspace_mode=WorkspaceMode.READ_ONLY,
            capabilities=(
                *read_provider,
                GatewayCapability.WORK_READ,
                GatewayCapability.WORK_APPEND,
                GatewayCapability.WORKFLOW_SCOPE_REVIEW_SUBMIT,
            ),
        ),
        "Advisory Reviewer One": _profile(
            image=image,
            workspace_mode=WorkspaceMode.READ_ONLY,
            capabilities=(
                *read_provider,
                GatewayCapability.ADVISORY_BLIND_SUBMIT,
                GatewayCapability.ADVISORY_CHALLENGE_SUBMIT,
            ),
        ),
        "Advisory Reviewer Two": _profile(
            image=image,
            workspace_mode=WorkspaceMode.READ_ONLY,
            capabilities=(
                *read_provider,
                GatewayCapability.ADVISORY_BLIND_SUBMIT,
                GatewayCapability.ADVISORY_CHALLENGE_SUBMIT,
            ),
        ),
        "Advisory Reviewer Three": _profile(
            image=image,
            workspace_mode=WorkspaceMode.READ_ONLY,
            capabilities=(
                *read_provider,
                GatewayCapability.ADVISORY_BLIND_SUBMIT,
                GatewayCapability.ADVISORY_CHALLENGE_SUBMIT,
            ),
        ),
        "Advisory Coordinator": _profile(
            image=image,
            workspace_mode=WorkspaceMode.READ_ONLY,
            capabilities=(*read_provider, GatewayCapability.ADVISORY_REPORT_SUBMIT),
        ),
    }


def _load_json_object(path: Path, *, subject: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_pairs
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f"{subject} is invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{subject} is invalid")
    return value


def _initialize_krab(state_root: Path, *, repository: Path, port: int) -> _KrabFixture:
    source = repository / "src"
    if not (source / "imitation_krab" / "__init__.py").is_file():
        raise RuntimeError("Imitation Krab source is unavailable")
    if str(source) not in sys.path:
        sys.path.insert(0, str(source))
    from imitation_krab.db import Database as KrabDatabase
    from imitation_krab.http_api import make_server
    from imitation_krab.service import Service as KrabService

    root = state_root / "krab"
    _ensure_private_directory(root, exist_ok=True)
    tokens = root / "tokens"
    _ensure_private_directory(tokens, exist_ok=True)
    metadata_path = root / "fixture.json"
    database = KrabDatabase(root / "krab.db", root / "pepper.key")
    database.initialize()
    handles = ["operator", *PROFILE_IDENTITIES.values()]
    if metadata_path.exists():
        metadata = _load_json_object(metadata_path, subject="Krab fixture metadata")
        if (
            set(metadata)
            != {"schema_version", "port", "project", "session_id", "users"}
            or metadata.get("schema_version") != 1
            or metadata.get("port") != port
            or not isinstance(metadata.get("users"), dict)
            or set(metadata["users"]) != set(handles)
        ):
            raise RuntimeError("Krab fixture metadata does not match this proof")
    else:
        users: dict[str, str] = {}
        for handle in handles:
            user, token = database.create_user(handle)
            token_path = tokens / f"{handle}.token"
            token_path.write_text(token + "\n", encoding="ascii")
            os.chmod(token_path, 0o600)
            users[handle] = user["id"]
        project = database.create_project(PROJECT_KEY, "M13 prescription proof")
        for handle in handles:
            database.add_project_member(
                PROJECT_KEY,
                handle,
                "admin" if handle == "operator" else "member",
            )
        operator_token = (tokens / "operator.token").read_text(encoding="ascii").strip()
        actor = database.authenticate(operator_token)
        if actor is None:
            raise RuntimeError("Krab proof operator did not authenticate")
        session = (
            KrabService(database)
            .create_session(
                actor,
                PROJECT_KEY,
                {
                    "label": "M13 durable scope decision",
                    "participants": ["operator", "scope_coord"],
                },
                "mgb.m13.session:0001",
            )
            .payload["trusted_metadata"]["session_id"]
        )
        metadata = {
            "schema_version": 1,
            "port": port,
            "project": {"id": project["id"], "key": project["key"]},
            "session_id": session,
            "users": users,
        }
        metadata_path.write_text(
            json.dumps(metadata, separators=(",", ":"), sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.chmod(metadata_path, 0o600)
    token_paths = {handle: tokens / f"{handle}.token" for handle in handles}
    if any(not path.is_file() for path in token_paths.values()):
        raise RuntimeError("Krab fixture token set is incomplete")
    server = make_server(database, host="127.0.0.1", port=port)
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.05},
        name="m13-live-krab",
        daemon=True,
    )
    thread.start()
    return _KrabFixture(
        database=database,
        server=server,
        thread=thread,
        base_url=f"http://127.0.0.1:{port}",
        project_id=metadata["project"]["id"],
        project_key=metadata["project"]["key"],
        session_id=metadata["session_id"],
        users={str(key): str(value) for key, value in metadata["users"].items()},
        token_paths=token_paths,
    )


def _prepare_control_plane(
    control: ControlPlane,
    *,
    fixture: _KrabFixture,
    repository: Path,
    source_revision: str,
    image: str,
    image_id: str,
    connector_socket: Path,
    deployment_sha256: str,
    scope_sha256: str,
    model: str,
    budget_ceiling_microusd: int,
) -> dict[str, Any]:
    database = control.database
    database.initialize()
    operator = database.create_user("operator", "M13 Proof Operator")
    instance = database.create_krab_instance("M13 Live Krab", fixture.base_url)
    identities: dict[str, dict[str, Any]] = {}
    for handle, user_id in fixture.users.items():
        identities[handle] = database.store_krab_identity(
            instance["id"],
            KrabIdentity(user_id, handle, 1),
            "file:" + str(fixture.token_paths[handle]),
            user_id=operator["id"] if handle == "operator" else None,
        )
    project = database.create_project(
        instance_id=instance["id"],
        krab_project_id=fixture.project_id,
        krab_project_key=fixture.project_key,
        display_name="Imitation Krab M13 proof",
        context=_project_context(repository),
        created_by=operator["id"],
    )
    work_item = database.import_work_item(
        project_key=PROJECT_KEY,
        envelope=_work_item(),
        imported_by=operator["id"],
    )
    control.register_inference_connector(
        name=CONNECTOR_NAME,
        spec=_workflow_connector_spec(
            socket_path=connector_socket,
            deployment_sha256=deployment_sha256,
            scope_sha256=scope_sha256,
            model=model,
        ),
        created_by_handle="operator",
    )
    control.add_connector_provider_grant(
        name=GRANT_NAME,
        spec=_workflow_grant_spec(
            deployment_sha256=deployment_sha256,
            scope_sha256=scope_sha256,
            model=model,
            budget_ceiling_microusd=budget_ceiling_microusd,
        ),
        expires_at=None,
        created_by_handle="operator",
    )
    profiles: dict[str, dict[str, Any]] = {}
    for name, spec in _profile_specs(image).items():
        profiles[name] = database.create_agent_profile(
            name=name,
            spec=spec,
            krab_identity_id=identities[PROFILE_IDENTITIES[name]]["id"],
            created_by=operator["id"],
        )
    discovery_configuration = control.create_discovery_configuration(
        project_key=PROJECT_KEY,
        name=DISCOVERY_CONFIGURATION,
        spec=_discovery_spec(),
        investigator_profile_name="Discovery Investigator",
        coordinator_profile_name="Discovery Coordinator",
        created_by_handle="operator",
    )
    control.create_workflow_configuration(
        project_key=PROJECT_KEY,
        name=WORKFLOW_CONFIGURATION,
        spec=_workflow_spec(image_id),
        producer_profile_name="Implementation Producer",
        reviewer_profile_name="Gate Reviewer",
        created_by_handle="operator",
    )
    control.create_advisory_configuration(
        project_key=PROJECT_KEY,
        name=ADVISORY_CONFIGURATION,
        spec=_advisory_spec(),
        reviewer_one_profile_name="Advisory Reviewer One",
        reviewer_two_profile_name="Advisory Reviewer Two",
        reviewer_three_profile_name="Advisory Reviewer Three",
        coordinator_profile_name="Advisory Coordinator",
        created_by_handle="operator",
    )
    return control.start_discovery_execution(
        project_key=PROJECT_KEY,
        configuration_name=DISCOVERY_CONFIGURATION,
        work_item_id=work_item["id"],
        source_revision=source_revision,
        expected_configuration_version=discovery_configuration["version"],
        expected_context_version_id=project["context_version_id"],
        expected_work_item_version_id=work_item["work_item_version_id"],
        expected_thread_generation=1,
        expected_thread_version=1,
        krab_session_id=fixture.session_id,
        krab_claim_id=None,
        idempotency_key="m13-discovery-live-proof",
        requested_by_handle="operator",
    )


def _existing_discovery(database: Database) -> dict[str, Any]:
    database.initialize()
    executions = database.list_discovery_executions(project_key=PROJECT_KEY, limit=2)
    if len(executions) != 1:
        raise RuntimeError("M13 state must contain exactly one Discovery execution")
    return executions[0]


def _open_scope_interaction(
    database: Database, discovery: Mapping[str, Any]
) -> dict[str, Any] | None:
    coordinator = discovery["runs"].get("coordinator")
    if coordinator is None:
        return None
    run = database.get_run(coordinator["run_id"])
    return next(
        (item for item in run["interactions"] if item["state"] == "open"),
        None,
    )


def _answer_scope(
    control: ControlPlane,
    fixture: _KrabFixture,
    discovery: Mapping[str, Any],
    answer: ScopeAnswer,
) -> dict[str, Any]:
    interaction = _open_scope_interaction(control.database, discovery)
    if interaction is None:
        answered = [
            ruling
            for ruling in discovery["rulings"]
            if ruling["disposition"] == "ASK_HUMAN"
        ]
        if answered:
            return control.database.get_interaction(answered[-1]["interaction_id"])
        raise RuntimeError("scope answer was supplied before a scope question")
    token = fixture.token_paths["operator"].read_text(encoding="ascii").strip()
    coordinator_run = discovery["runs"]["coordinator"]["run_id"]
    route = control.database.get_run_transport(coordinator_run)
    response = KrabClient(fixture.base_url, token).create_session_item(
        PROJECT_KEY,
        fixture.session_id,
        recipient_handle=route["agent_krab_handle"],
        title="M13 scope decision",
        body=answer.body,
        idempotency_key="mgb.m13.scope-response:0001",
    )
    control.accept_interaction_response(
        interaction["id"],
        response_item_id=response.item_id,
        expected_version=interaction["version"],
    )
    return control.database.get_interaction(interaction["id"])


def _drive_until(
    supervisor: Supervisor,
    *,
    predicate: Callable[[], bool],
    timeout: float,
    before_tick: Callable[[], None],
) -> None:
    deadline = time.monotonic() + timeout
    quiet = False
    while time.monotonic() < deadline:
        if predicate():
            supervisor.drain(timeout=300)
            return
        before_tick()
        progress = supervisor.tick()
        changed = any(progress.values())
        time.sleep(0.05 if changed or not quiet else 0.1)
        quiet = not changed
    raise RuntimeError("M13 proof timed out before its durable stop")


def _start_workflow_if_ready(
    control: ControlPlane, discovery: Mapping[str, Any]
) -> dict[str, Any]:
    existing = control.database.list_workflow_executions(
        project_key=PROJECT_KEY, limit=2
    )
    if existing:
        if len(existing) != 1:
            raise RuntimeError("M13 state has ambiguous implementation executions")
        return existing[0]
    if discovery["state"] != "completed" or discovery["final_ruling"] is None:
        raise RuntimeError("implementation cannot start before completed Discovery")
    if discovery["final_ruling"]["disposition"] != "GO":
        raise RuntimeError("implementation requires a final Discovery GO ruling")
    coordinator_id = discovery["runs"]["coordinator"]["run_id"]
    thread = control.database.get_run_execution(coordinator_id)["work_memory"]
    configuration = control.get_workflow_configuration(
        project_key=PROJECT_KEY, name=WORKFLOW_CONFIGURATION
    )
    return control.start_workflow_execution(
        project_key=PROJECT_KEY,
        configuration_name=WORKFLOW_CONFIGURATION,
        work_item_id=discovery["work_item_id"],
        source_revision=discovery["source_revision"],
        expected_configuration_version=configuration["version"],
        expected_context_version_id=discovery["context_version_id"],
        expected_work_item_version_id=discovery["work_item_version_id"],
        expected_thread_generation=thread["generation"],
        expected_thread_version=thread["version"],
        idempotency_key="m13-implementation-live-proof",
        requested_by_handle="operator",
        discovery_execution_id=discovery["id"],
    )


def _workflow_gate(database: Database, workflow: Mapping[str, Any]) -> dict[str, Any]:
    gate = database.get_workflow_gate(workflow["id"], int(workflow["correction_round"]))
    if gate is None:
        raise RuntimeError("scoped implementation has no exact-candidate gate")
    return gate


def _start_advisory_if_ready(
    control: ControlPlane, workflow: Mapping[str, Any]
) -> dict[str, Any]:
    existing = control.database.list_advisory_executions(
        project_key=PROJECT_KEY, limit=2
    )
    if existing:
        if len(existing) != 1:
            raise RuntimeError("M13 state has ambiguous advisory executions")
        return existing[0]
    if workflow["state"] != "active" or workflow["stage"] != "maintainer":
        raise RuntimeError("advisory review requires the independently gated candidate")
    gate = _workflow_gate(control.database, workflow)
    reviewer = control.database.find_workflow_run(
        workflow["id"], workflow["correction_round"], "reviewer"
    )
    if reviewer is None or reviewer["state"] != "succeeded":
        raise RuntimeError("advisory review requires a completed independent review")
    thread = control.database.get_run_execution(reviewer["id"])["work_memory"]
    configuration = control.get_advisory_configuration(
        project_key=PROJECT_KEY, name=ADVISORY_CONFIGURATION
    )
    return control.start_advisory_execution(
        project_key=PROJECT_KEY,
        configuration_name=ADVISORY_CONFIGURATION,
        candidate_id=gate["candidate_id"],
        expected_configuration_version=configuration["version"],
        expected_context_version_id=workflow["context_version_id"],
        expected_work_item_version_id=workflow["work_item_version_id"],
        expected_thread_generation=thread["generation"],
        expected_thread_version=thread["version"],
        idempotency_key="m13-advisory-live-proof",
        requested_by_handle="operator",
    )


def _all_run_ids(
    database: Database,
    discovery: Mapping[str, Any],
    workflow: Mapping[str, Any] | None,
    advisory: Mapping[str, Any] | None,
) -> list[str]:
    result = [run["run_id"] for run in discovery["runs"].values()]
    if workflow is not None:
        for round_number in range(int(workflow["correction_round"]) + 1):
            for role in ("producer", "reviewer"):
                run = database.find_workflow_run(workflow["id"], round_number, role)
                if run is not None:
                    result.append(run["id"])
    if advisory is not None:
        for stage in advisory["runs"].values():
            result.extend(run["run_id"] for run in stage.values())
    return list(dict.fromkeys(result))


def _previous_invocations(report_path: Path) -> list[dict[str, Any]]:
    if not report_path.exists():
        return []
    value = _load_json_object(report_path, subject="M13 proof report")
    invocations = value.get("proof_invocations", [])
    if not isinstance(invocations, list) or len(invocations) > 16:
        raise RuntimeError("M13 proof report is invalid")
    if not all(isinstance(item, dict) for item in invocations):
        raise RuntimeError("M13 proof report is invalid")
    return [dict(item) for item in invocations]


def _artifact_metadata(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value[key]
        for key in (
            "id",
            "run_id",
            "attempt_id",
            "content_sha256",
            "disposition",
            "interaction_id",
            "authority",
        )
        if key in value
    }


def _write_m13_report(
    *,
    report_path: Path,
    database: Database,
    discovery: Mapping[str, Any],
    workflow: Mapping[str, Any] | None,
    advisory: Mapping[str, Any] | None,
    repository: Path,
    source_revision: str,
    state_root: Path,
    model: str,
    result_store: WorkspaceResultStore,
    phase_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    run_ids = _all_run_ids(database, discovery, workflow, advisory)
    runs = [database.get_run(run_id) for run_id in run_ids]
    invocations = [
        item for run in runs for item in database.list_provider_invocations(run["id"])
    ]
    usage = {
        field: sum(int(item[field] or 0) for item in invocations)
        for field in (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_output_tokens",
            "estimated_microusd",
            "accounted_microusd",
        )
    }
    proof_invocations = _previous_invocations(report_path)
    proof_invocations.append(dict(phase_evidence))
    ask_rulings = [
        ruling
        for ruling in discovery["rulings"]
        if ruling["disposition"] == "ASK_HUMAN"
    ]
    interaction = (
        None
        if not ask_rulings
        else database.get_interaction(ask_rulings[-1]["interaction_id"])
    )
    coordinator_run = discovery["runs"].get("coordinator")
    coordinator_attempts = (
        []
        if coordinator_run is None
        else database.get_run(coordinator_run["run_id"])["attempts"]
    )
    candidate_report = None
    scope_review = None
    if workflow is not None and workflow["stage"] in {
        "review",
        "maintainer",
        "landing",
        "completion",
    }:
        gate = _workflow_gate(database, workflow)
        candidate = database.get_candidate(gate["candidate_id"])
        reviewer = database.find_workflow_run(
            workflow["id"], workflow["correction_round"], "reviewer"
        )
        verdict = (
            None
            if reviewer is None
            else database.get_typed_review_verdict(gate["policy_id"], reviewer["id"])
        )
        if verdict is not None:
            scope_review = database.get_workflow_scope_review(
                gate["policy_id"], reviewer["id"], verdict["reviewer_attempt_id"]
            )
        candidate_report = {
            "candidate_id": candidate["id"],
            "base_revision": candidate["base_revision"],
            "head_revision": candidate["head_revision"],
            "changed_paths": _changed_paths(result_store, candidate["attempt_id"]),
            "policy_id": gate["policy_id"],
            "review_outcome": None if verdict is None else verdict["outcome"],
            "scope_review": (
                None
                if scope_review is None
                else {
                    "id": scope_review["id"],
                    "content_sha256": scope_review["content_sha256"],
                    "expected_review_outcome": scope_review["expected_review_outcome"],
                    "failed_must_keys": scope_review["failed_must_keys"],
                    "regression_probe_keys": scope_review["regression_probe_keys"],
                    "follow_up_probe_keys": scope_review["follow_up_probe_keys"],
                }
            ),
        }
    passed = (
        discovery["state"] == "completed"
        and discovery["final_ruling"] is not None
        and discovery["final_ruling"]["disposition"] == "GO"
        and interaction is not None
        and interaction["state"] == "answered"
        and len(coordinator_attempts) >= 2
        and workflow is not None
        and workflow["state"] == "active"
        and workflow["stage"] == "maintainer"
        and candidate_report is not None
        and candidate_report["review_outcome"] == "approved"
        and scope_review is not None
        and scope_review["expected_review_outcome"] == "approved"
        and advisory is not None
        and advisory["state"] == "completed"
        and advisory["report"] is not None
        and advisory["report"]["authority"] == "advisory_only"
        and len(proof_invocations) >= 2
    )
    answer_required = (
        discovery["state"] == "active"
        and interaction is not None
        and interaction["state"] == "open"
    )
    status = (
        "passed" if passed else "scope_answer_required" if answer_required else "active"
    )
    report = {
        "status": status,
        "repository": str(repository),
        "source_revision": source_revision,
        "state_directory": str(state_root),
        "model": model,
        "budget_ceiling_microusd": database.get_provider_grant(GRANT_NAME)["policy"][
            "budget"
        ]["project_microusd"],
        "provider_invocations": len(invocations),
        "usage": usage,
        "direct_worker_network": "denied",
        "credential_exposure": "none outside credential-owning host processes",
        "target_worktree_clean": not bool(_git(repository, "status", "--porcelain")),
        "discovery": {
            "execution_id": discovery["id"],
            "state": discovery["state"],
            "stage": discovery["stage"],
            "trace": (
                None
                if discovery["trace"] is None
                else _artifact_metadata(discovery["trace"])
            ),
            "rulings": [_artifact_metadata(item) for item in discovery["rulings"]],
            "interaction": (
                None
                if interaction is None
                else {
                    "id": interaction["id"],
                    "state": interaction["state"],
                    "request_item_id": interaction["request_item_id"],
                    "response_item_id": interaction["response_item_id"],
                }
            ),
            "coordinator_attempt_count": len(coordinator_attempts),
        },
        "implementation": (
            None
            if workflow is None
            else {
                "execution_id": workflow["id"],
                "state": workflow["state"],
                "stage": workflow["stage"],
                "scope_binding_id": (
                    None
                    if workflow["scope_binding"] is None
                    else workflow["scope_binding"]["id"]
                ),
                "candidate": candidate_report,
                "maintainer_authority": "pending",
                "landing_attempted": False,
            }
        ),
        "advisory": (
            None
            if advisory is None
            else {
                "execution_id": advisory["id"],
                "state": advisory["state"],
                "stage": advisory["stage"],
                "blind_reviews": {
                    role: _artifact_metadata(item)
                    for role, item in advisory["blind_reviews"].items()
                },
                "challenge_reviews": {
                    role: _artifact_metadata(item)
                    for role, item in advisory["challenge_reviews"].items()
                },
                "report": (
                    None
                    if advisory["report"] is None
                    else _artifact_metadata(advisory["report"])
                ),
                "authority": "advisory_only",
            }
        ),
        "run_ids": run_ids,
        "proof_invocations": proof_invocations,
        "orchestrator_replacement_observed": len(proof_invocations) >= 2,
        "worker_replacement_observed": len(coordinator_attempts) >= 2,
        "handoff": {
            "next_action": (
                "Provide --scope-answer-file with schema_version 1, decision "
                "copy_only or behavioral_readiness, and a bounded rationale."
                if answer_required
                else (
                    "Inspect the exact candidate; maintainer approval and landing "
                    "remain outside this proof."
                    if passed
                    else None
                )
            )
        },
    }
    _write_report(report_path, report)
    report["report_sha256"] = hashlib.sha256(report_path.read_bytes()).hexdigest()
    return report


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.i_understand_this_uses_live_quota:
        raise RuntimeError(
            "live quota opt-in is required: --i-understand-this-uses-live-quota"
        )
    if not hasattr(os, "getuid") or os.getuid() <= 0 or os.getgid() <= 0:
        raise RuntimeError("live M13 proof requires a non-root POSIX user")

    project_root = Path(__file__).resolve().parents[1]
    profile_dir = args.profile_dir.expanduser().absolute()
    repository = args.repository.expanduser().resolve(strict=True)
    state_root = args.state_dir.expanduser().absolute()
    _validate_state_location(
        state_root, protected=(project_root, profile_dir, repository)
    )
    observed_revision = _source_revision(repository, args.source_revision)
    fresh = not (state_root / "control-plane.db").exists()

    profile_store = ChatGPTProfileStore(profile_dir)
    oauth = ChatGPTOAuth(profile_store)
    session = oauth.session()
    if args.model not in {model.slug for model in oauth.list_models()}:
        raise RuntimeError("selected model is not visible to this ChatGPT account")
    initial_secrets = _session_secrets(profile_store)
    if args.build:
        _build_image(args.image, args.build_timeout)

    _ensure_private_directory(state_root, exist_ok=True)
    connector_socket = _connector_socket_path(state_root)
    connector_root = connector_socket.parent
    _ensure_private_directory(connector_root, exist_ok=True)
    fixture = _initialize_krab(state_root, repository=repository, port=args.krab_port)
    deployment_sha256 = _deployment_digest(project_root)
    database = Database(state_root / "control-plane.db")
    control = ControlPlane(database)
    docker_engine = _RecordingDockerEngine(
        DockerCLI(command_timeout=60, run_timeout=args.run_timeout)
    )
    image = docker_engine.inspect_image(args.image)
    supervisor: Supervisor | None = None
    gateways: FileGatewayManager | None = None
    connector_waves: _ConnectorWaves | None = None
    connector_stdout = ""
    connector_stderr = ""
    proof_error: BaseException | None = None
    answer = (
        None
        if args.scope_answer_file is None
        else _load_scope_answer(args.scope_answer_file)
    )
    result_store = WorkspaceResultStore(
        state_root / "results", max_paths=512, max_changed_bytes=64 * 1024 * 1024
    )
    candidate_store = GitCandidateStore(state_root / "candidates")
    try:
        if fresh:
            discovery = _prepare_control_plane(
                control,
                fixture=fixture,
                repository=repository,
                source_revision=observed_revision,
                image=args.image,
                image_id=image.image_id,
                connector_socket=connector_socket,
                deployment_sha256=deployment_sha256,
                scope_sha256=session.credential_scope_sha256,
                model=args.model,
                budget_ceiling_microusd=args.budget_ceiling_microusd,
            )
        else:
            discovery = _existing_discovery(database)
            if discovery["source_revision"] != observed_revision:
                raise RuntimeError("source repository no longer matches M13 state")
        connector = database.get_inference_connector(CONNECTOR_NAME)
        expected_connector = _workflow_connector_spec(
            socket_path=connector_socket,
            deployment_sha256=deployment_sha256,
            scope_sha256=session.credential_scope_sha256,
            model=args.model,
        )
        stored_connector = InferenceConnectorSpec.from_mapping(connector["spec"])
        if (
            connector["state"] != "active"
            or stored_connector.canonical != expected_connector.canonical
        ):
            raise RuntimeError("existing connector does not match this M13 proof")
        grant = database.get_provider_grant(GRANT_NAME)
        if (
            grant["policy"]["budget"]["project_microusd"]
            != args.budget_ceiling_microusd
        ):
            raise RuntimeError("existing provider budget does not match this proof")
        connector_config = connector_root / "config.json"
        _write_workflow_connector_config(
            connector_config,
            connector_id=connector["id"],
            deployment_sha256=deployment_sha256,
            scope_sha256=session.credential_scope_sha256,
            model=args.model,
            request_bytes=stored_connector.request_bytes,
            response_bytes=stored_connector.response_bytes,
            timeout_ms=stored_connector.timeout_ms,
        )

        discovery = database.get_discovery_execution(discovery["id"])
        if answer is not None:
            _answer_scope(control, fixture, discovery, answer)
            discovery = database.get_discovery_execution(discovery["id"])

        workspaces = WorkspaceManager(state_root / "workspaces", GitSnapshotter())
        candidate_workspaces = CandidateReviewWorkspaces(
            database, workspaces, result_store, candidate_store
        )
        gateways = FileGatewayManager(state_root / "gateways", control)
        executor = DockerExecutor(docker_engine)
        coordinator = RunCoordinator(
            database,
            executor,
            workspaces,
            gateways,
            result_store,
            candidate_workspaces=candidate_workspaces,
            lease_seconds=30,
            heartbeat_interval=5,
        )
        supervisor = Supervisor(
            database,
            coordinator,
            control,
            workflow_director=WorkflowDirector(
                control, result_store, candidate_store, executor
            ),
            discovery_director=DiscoveryDirector(control),
            advisory_director=AdvisoryDirector(control),
            poll_interval=0.1,
            max_workers=1,
            action_lease_seconds=60,
            shutdown_timeout=300,
            action_timeout=args.run_timeout + 300,
        )

        def start_connector() -> subprocess.Popen[str]:
            _clear_stale_socket(connector_socket)
            return _start_connector(
                project_root=project_root,
                profile_dir=profile_dir,
                config_path=connector_config,
                socket_path=connector_socket,
                max_invocations=args.max_invocations,
            )

        connector_waves = _ConnectorWaves(
            database, start=start_connector, stop=_stop_connector
        )

        def discovery_stop() -> bool:
            current = database.get_discovery_execution(discovery["id"])
            return (
                current["state"] != "active"
                or _open_scope_interaction(database, current) is not None
            )

        if (
            discovery["state"] == "active"
            and _open_scope_interaction(database, discovery) is None
        ):
            _drive_until(
                supervisor,
                predicate=discovery_stop,
                timeout=args.proof_timeout,
                before_tick=connector_waves.before_tick,
            )
        discovery = database.get_discovery_execution(discovery["id"])

        workflow: dict[str, Any] | None = None
        advisory: dict[str, Any] | None = None
        open_scope = _open_scope_interaction(database, discovery)
        if open_scope is not None and not any(
            ruling["disposition"] == "ASK_HUMAN"
            and ruling["interaction_id"] == open_scope["id"]
            for ruling in discovery["rulings"]
        ):
            raise RuntimeError(
                "Discovery coordinator opened a question without a durable "
                "ASK_HUMAN ruling"
            )
        if open_scope is None:
            ask_rulings = [
                ruling
                for ruling in discovery["rulings"]
                if ruling["disposition"] == "ASK_HUMAN"
            ]
            if (
                discovery["state"] != "completed"
                or discovery["final_ruling"] is None
                or discovery["final_ruling"]["disposition"] != "GO"
                or not ask_rulings
                or database.get_interaction(ask_rulings[-1]["interaction_id"])["state"]
                != "answered"
            ):
                raise RuntimeError(
                    "Discovery did not complete through the required human scope path"
                )
            workflow = _start_workflow_if_ready(control, discovery)

            def workflow_stop() -> bool:
                current = database.get_workflow_execution(workflow["id"])
                return current["state"] != "active" or current["stage"] == "maintainer"

            if workflow["state"] == "active" and workflow["stage"] != "maintainer":
                _drive_until(
                    supervisor,
                    predicate=workflow_stop,
                    timeout=args.proof_timeout,
                    before_tick=connector_waves.before_tick,
                )
            workflow = database.get_workflow_execution(workflow["id"])
            if workflow["state"] != "active" or workflow["stage"] != "maintainer":
                raise RuntimeError(
                    "scoped implementation did not reach maintainer gate"
                )
            advisory = _start_advisory_if_ready(control, workflow)

            def advisory_stop() -> bool:
                current = database.get_advisory_execution(advisory["id"])
                return current["state"] != "active"

            if advisory["state"] == "active":
                _drive_until(
                    supervisor,
                    predicate=advisory_stop,
                    timeout=args.proof_timeout,
                    before_tick=connector_waves.before_tick,
                )
            advisory = database.get_advisory_execution(advisory["id"])
            if advisory["state"] != "completed":
                raise RuntimeError("advisory prescription did not complete")
    except BaseException as exc:
        proof_error = exc
        raise
    finally:
        try:
            if supervisor is not None:
                supervisor.close(wait=True, timeout=300)
        finally:
            try:
                if gateways is not None:
                    gateways.close()
            finally:
                if connector_waves is not None:
                    try:
                        connector_stdout, connector_stderr = connector_waves.close()
                    except RuntimeError:
                        if proof_error is None:
                            raise
                fixture.close()

    final_secrets = tuple(
        dict.fromkeys(
            (*initial_secrets, *_session_secrets(profile_store, required=False))
        )
    )
    launch_plans = _serialize_sandbox_plans(docker_engine.plans)
    _assert_sandbox_boundaries(
        docker_engine.plans, profile_dir=profile_dir, secrets=final_secrets
    )
    _assert_no_secret(
        final_secrets,
        {
            "M13 state": _collect_regular_files(state_root),
            "worker runtime output": "\n".join(docker_engine.logs_seen).encode(),
            "connector logs": (connector_stdout + connector_stderr).encode(),
            "Docker sandbox launch plans": launch_plans,
        },
    )
    krab_secrets = tuple(
        path.read_text(encoding="ascii").strip()
        for path in fixture.token_paths.values()
    )
    _assert_no_secret(
        krab_secrets,
        {
            "worker runtime output": "\n".join(docker_engine.logs_seen).encode(),
            "connector logs": (connector_stdout + connector_stderr).encode(),
            "Docker sandbox launch plans": launch_plans,
        },
    )
    if any(
        str(path).encode("utf-8") in launch_plans
        for path in fixture.token_paths.values()
    ):
        raise RuntimeError("Krab credential path entered a worker launch plan")
    run_ids = _all_run_ids(database, discovery, workflow, advisory)
    if any(docker_engine.managed_containers(run_id) for run_id in run_ids):
        raise RuntimeError("M13 proof left a managed worker container behind")
    if answer is not None:
        control_bytes = b"".join(
            path.read_bytes()
            for path in state_root.glob("control-plane.db*")
            if path.is_file()
        )
        if answer.body.encode("utf-8") in control_bytes:
            raise RuntimeError("private scope response entered MGB durable state")

    phase_evidence = {
        "sequence": len(_previous_invocations(state_root / REPORT_NAME)) + 1,
        "started_from_fresh_state": fresh,
        "scope_answer_supplied": answer is not None,
        "finished_discovery_state": discovery["state"],
        "finished_discovery_stage": discovery["stage"],
        "finished_workflow_stage": None if workflow is None else workflow["stage"],
        "finished_advisory_state": None if advisory is None else advisory["state"],
        "sandbox_plan_count": len(docker_engine.plans),
        "sandbox_plans_sha256": hashlib.sha256(launch_plans).hexdigest(),
        "direct_worker_network": "denied",
        "chatgpt_credentials_in_workers": False,
        "krab_credentials_in_workers": False,
    }
    report = _write_m13_report(
        report_path=state_root / REPORT_NAME,
        database=database,
        discovery=discovery,
        workflow=workflow,
        advisory=advisory,
        repository=repository,
        source_revision=observed_revision,
        state_root=state_root,
        model=args.model,
        result_store=result_store,
        phase_evidence=phase_evidence,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
