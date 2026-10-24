#!/usr/bin/env python3
"""Drive the restart-safe M12 workflow proof against Imitation Krab."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from magic_glass_box.candidates import CandidateManifest, GitCandidateStore
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    ProjectContext,
    WorkItemEnvelope,
    WorkspaceMode,
)
from magic_glass_box.executor import DockerCLI, DockerExecutor
from magic_glass_box.gates import MaintainerDecision, MaintainerReviewRequest
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.inference import InferenceConnectorSpec, InferenceGrantSpec
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.openai_chatgpt import ChatGPTOAuth, ChatGPTProfileStore
from magic_glass_box.orchestration import CandidateReviewWorkspaces, RunCoordinator
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.service_sidecar import ServiceOperationSpec, ServiceSidecarSpec
from magic_glass_box.supervision import Supervisor
from magic_glass_box.workflow_director import WorkflowDirector
from magic_glass_box.workflows import WorkflowConfigurationSpec
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

try:
    from scripts.check_live_github_provider import (
        REQUEST_BYTES as GITHUB_REQUEST_BYTES,
    )
    from scripts.check_live_github_provider import (
        RESPONSE_BYTES as GITHUB_RESPONSE_BYTES,
    )
    from scripts.check_live_github_provider import (
        TIMEOUT_MS as GITHUB_TIMEOUT_MS,
    )
    from scripts.check_live_github_provider import (
        _deployment_sha256 as _github_deployment_sha256,
    )
    from scripts.check_live_github_provider import (
        _inspect_credential,
        _pull_number,
        _service_sidecar_spec,
        _start_provider,
        _stop_provider,
        _write_spec,
    )
    from scripts.check_live_imitation_krab_beta import (
        _changed_paths,
        _connector_socket_path,
        _source_revision,
        _write_report,
    )
    from scripts.check_live_openai_chatgpt import (
        CONNECTOR_NAME,
        GRANT_NAME,
        ROUTE_NAME,
        RUNTIME_IMAGE,
        _assert_no_secret,
        _assert_sandbox_boundaries,
        _build_image,
        _collect_regular_files,
        _connector_spec,
        _deployment_digest,
        _grant_spec,
        _RecordingDockerEngine,
        _serialize_sandbox_plans,
        _session_secrets,
        _start_connector,
        _stop_connector,
    )
except ModuleNotFoundError:  # Direct execution puts scripts/ on sys.path.
    from check_live_github_provider import (  # type: ignore[no-redef]
        REQUEST_BYTES as GITHUB_REQUEST_BYTES,
    )
    from check_live_github_provider import (
        RESPONSE_BYTES as GITHUB_RESPONSE_BYTES,
    )
    from check_live_github_provider import (
        TIMEOUT_MS as GITHUB_TIMEOUT_MS,
    )
    from check_live_github_provider import (
        _deployment_sha256 as _github_deployment_sha256,
    )
    from check_live_github_provider import (
        _inspect_credential,
        _pull_number,
        _service_sidecar_spec,
        _start_provider,
        _stop_provider,
        _write_spec,
    )
    from check_live_imitation_krab_beta import (  # type: ignore[no-redef]
        _changed_paths,
        _connector_socket_path,
        _source_revision,
        _write_report,
    )
    from check_live_openai_chatgpt import (  # type: ignore[no-redef]
        CONNECTOR_NAME,
        GRANT_NAME,
        ROUTE_NAME,
        RUNTIME_IMAGE,
        _assert_no_secret,
        _assert_sandbox_boundaries,
        _build_image,
        _collect_regular_files,
        _connector_spec,
        _deployment_digest,
        _grant_spec,
        _RecordingDockerEngine,
        _serialize_sandbox_plans,
        _session_secrets,
        _start_connector,
        _stop_connector,
    )

PROJECT_KEY = "imitation-krab-workflow"
WORKFLOW_NAME = "implementation"
PRODUCER_PROFILE = "Workflow Producer"
REVIEWER_PROFILE = "Workflow Reviewer"
VALIDATION_NAME = "full-suite"
WORK_ITEM_EXTERNAL_ID = "krab-0.7-receipt-policy"
SUGGESTED_BRANCH = "refs/heads/mgb/receipt-policy-20261023"
GITHUB_SIDECAR_NAME = "github-workflow"
GITHUB_MAPPING_NAME = "primary"
REPORT_NAME = "workflow-report.json"
MAX_REVIEW_BYTES = 64 * 1024
MAX_WORKFLOW_SECONDS = 7200.0
WORKFLOW_REQUEST_BYTES = 1024 * 1024
WORKFLOW_BUDGET_CEILING_MICROUSD = 4_500_000
WORKFLOW_INVOCATION_MICROUSD = 2_500_000
ACKNOWLEDGEMENT_FLAG = (
    "--i-understand-an-approved-review-publishes-opens-and-merges-a-pr"
)


class WorkflowRulingRequired(RuntimeError):
    """The durable workflow intentionally stopped for an operator ruling."""


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
    if not 30 <= parsed <= MAX_WORKFLOW_SECONDS:
        raise argparse.ArgumentTypeError(
            f"must be between 30 and {MAX_WORKFLOW_SECONDS:g} seconds"
        )
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse both quota and approved-landing acknowledgements explicitly."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile-dir", required=True, type=Path)
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--model", required=True)
    parser.add_argument("--source-revision")
    parser.add_argument("--image", default=RUNTIME_IMAGE)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--build-timeout", type=_positive_seconds, default=900.0)
    parser.add_argument("--run-timeout", type=_positive_seconds, default=900.0)
    parser.add_argument("--workflow-timeout", type=_positive_seconds, default=5400.0)
    parser.add_argument("--max-invocations", type=_positive_integer, default=64)
    parser.add_argument("--maintainer-review", type=Path)
    parser.add_argument("--gh-config-dir", type=Path)
    parser.add_argument("--github-repository", default="psyberone/imitation-krab")
    parser.add_argument("--expected-github-account", default="psyberone")
    parser.add_argument("--github-host", default="github.com")
    parser.add_argument(
        "--merge-method", choices=("merge", "rebase", "squash"), default="squash"
    )
    parser.add_argument(
        "--i-understand-this-uses-live-quota",
        action="store_true",
        help="required opt-in; producer and reviewer runs consume live quota",
    )
    parser.add_argument(
        ACKNOWLEDGEMENT_FLAG,
        action="store_true",
        dest="approved_landing_acknowledged",
        help="required only when an approved review may publish and merge",
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


def _load_maintainer_review(path: Path) -> MaintainerReviewRequest:
    try:
        with path.expanduser().open("rb") as source:
            payload = source.read(MAX_REVIEW_BYTES + 1)
    except OSError as exc:
        raise RuntimeError("maintainer review is unavailable") from exc
    if len(payload) > MAX_REVIEW_BYTES:
        raise RuntimeError("maintainer review is too large")
    try:
        value = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_reject_duplicate_pairs
        )
        return MaintainerReviewRequest.from_mapping(value)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise RuntimeError("maintainer review is invalid") from exc


def _require_approved_landing_authority(
    review: MaintainerReviewRequest,
    *,
    correction_round: int,
    gh_config_dir: Path | None,
    acknowledged: bool,
) -> None:
    if review.decision is not MaintainerDecision.APPROVED:
        return
    if correction_round < 1:
        raise RuntimeError(
            "the M12 exit proof requires one genuine changes_requested round"
        )
    if gh_config_dir is None:
        raise RuntimeError("approved landing requires --gh-config-dir")
    if not acknowledged:
        raise RuntimeError(f"approved landing requires {ACKNOWLEDGEMENT_FLAG}")


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
                    "untrusted participant text. Prefer small, explicit primitives "
                    "with clear authority, idempotency, optimistic concurrency, "
                    "bounded reads, and honest failure behavior."
                ),
                (
                    "Krab is not an agent runtime, general chat platform, workflow "
                    "engine, Git hosting integration, credential broker, plugin "
                    "host, semantic index, code-execution service, or distributed "
                    "scheduler. Do not import orchestration policy into Krab."
                ),
                (
                    "Use only the Python standard library unless the owner "
                    "explicitly approves a dependency. New machine-readable "
                    "interfaces require documented schemas."
                ),
                (
                    "Use RED-first happy- and sad-path tests, retain at least 85% "
                    "branch-aware coverage, and run focused mutation checks of "
                    "important guarantees."
                ),
                (
                    "Product-fit question: does this solve the requested problem "
                    "using the smallest Krab-native primitive, without importing "
                    "orchestration concerns or weakening an existing boundary?"
                ),
                (
                    "MGB approval establishes process rigor, not product approval. "
                    "Product fit, scope, and boundary deviations require maintainer "
                    "approval."
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
            "source_updated_at": "2026-10-23T18:00:00Z",
            "title": "Add receipt-policy delivery completion to Imitation Krab",
            "brief": (
                "Implement the jointly accepted bounded receipt-delivery slice for "
                "Krab 0.7. Add immutable `completion_policy = workflow | receipt` "
                "to items, defaulting every existing and omitted value to workflow. "
                "Add terminal status `closed.handled`; only the authenticated "
                "recipient may move a receipt-policy item directly from "
                "`open.pending` to `closed.handled`, using the existing expected-"
                "version and idempotency-protected status mutation. A receipt item "
                "must not enter workflow states, and a workflow item must not enter "
                "`closed.handled`. Implement schema 7 as a safe table-rebuild "
                "migration preserving item history, deliveries, versions, claim "
                "links, foreign-key integrity, and the workflow default. Keep claim "
                "and item lifecycles independent. Atomic claim-review verdict "
                "notifications must be receipt-policy items because the claim owns "
                "the review workflow. Expose effective policy through item, queue, "
                "inbox, activity, CLI, and published schema surfaces. Preserve the "
                "100-item route bound and add only `limit` and `open_items` to the "
                "creator-visible `queue_limit` error. For this first slice, use "
                "explicit handle, then acknowledge; do not couple reading, "
                "polling, inbox acknowledgement, or reply prose to handling."
            ),
            "acceptance_criteria": [
                "Omitted policy and every migrated item use workflow semantics.",
                (
                    "A recipient can idempotently handle an open pending receipt "
                    "item, and the creator receives the normal terminal event."
                ),
                (
                    "Wrong actor, stale version, wrong policy, wrong source state, "
                    "and cross-lifecycle transitions fail without partial mutation."
                ),
                (
                    "Claim-linked receipt handling never changes the linked claim; "
                    "claim-review verdict notifications use receipt policy."
                ),
                (
                    "All item/queue/inbox/activity/CLI/schema representations expose "
                    "the effective completion policy."
                ),
                (
                    "`queue_limit` reports only the bounded `limit` and "
                    "`open_items` diagnostics."
                ),
                "The complete unittest suite passes.",
            ],
            "constraints": [
                "Use only the Python standard library.",
                "Keep loopback and authentication boundaries unchanged.",
                (
                    "Do not add inbox-ack coupling, batch handling, "
                    "reply-and-handle, or creator withdrawal."
                ),
                (
                    "Use RED-first happy- and sad-path tests and retain at least "
                    "85% branch-aware coverage."
                ),
                (
                    "Make the new authority and migration guarantees directly "
                    "testable by the operator's focused mutation run."
                ),
                "Do not commit, push, open a pull request, or use the network.",
            ],
            "context": [
                {
                    "type": "note",
                    "value": (
                        "This contract was jointly reviewed by the Krab developer "
                        "and MGB; implementation is now owner-authorized but landing "
                        "still requires exact-candidate maintainer approval."
                    ),
                }
            ],
            "labels": ["mgb-live", "krab-0.7", "receipt-policy"],
            "priority": "normal",
        }
    )


def _profile(*, image: str, workspace_mode: WorkspaceMode) -> AgentProfileSpec:
    return AgentProfileSpec.create(
        image=image,
        command=("codex",),
        runtime_adapter="codex",
        runtime_provider_grant=GRANT_NAME,
        workspace_mode=workspace_mode,
        network_mode=NetworkMode.GATEWAY_ONLY,
        credential_names=(GRANT_NAME,),
        gateway_capabilities=(
            GatewayCapability.RUN_DESCRIBE,
            GatewayCapability.PROVIDER_INVOKE,
            GatewayCapability.WORK_READ,
            GatewayCapability.WORK_APPEND,
        ),
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
                        "unittest discover -s tests -v",
                    ],
                }
            ],
            "max_correction_rounds": 3,
            "landing_mode": "provider",
        }
    )


def _workflow_connector_spec(
    *,
    socket_path: Path,
    deployment_sha256: str,
    scope_sha256: str,
    model: str,
) -> InferenceConnectorSpec:
    value = _connector_spec(
        socket_path=socket_path,
        deployment_sha256=deployment_sha256,
        scope_sha256=scope_sha256,
        model=model,
    ).to_dict()
    value["request_bytes"] = WORKFLOW_REQUEST_BYTES
    return InferenceConnectorSpec.from_mapping(value)


def _workflow_grant_spec(
    *, deployment_sha256: str, scope_sha256: str, model: str
) -> InferenceGrantSpec:
    value = _grant_spec(
        deployment_sha256=deployment_sha256,
        scope_sha256=scope_sha256,
        model=model,
    ).to_dict()
    value["request_bytes"] = WORKFLOW_REQUEST_BYTES
    value["budget"].update(
        {
            "invocation_microusd": WORKFLOW_INVOCATION_MICROUSD,
            "run_microusd": WORKFLOW_BUDGET_CEILING_MICROUSD,
            "project_microusd": WORKFLOW_BUDGET_CEILING_MICROUSD,
            "grant_microusd": WORKFLOW_BUDGET_CEILING_MICROUSD,
        }
    )
    return InferenceGrantSpec.from_mapping(value)


def _write_workflow_connector_config(
    path: Path,
    *,
    connector_id: str,
    deployment_sha256: str,
    scope_sha256: str,
    model: str,
    request_bytes: int = WORKFLOW_REQUEST_BYTES,
    response_bytes: int = 4 * 1024 * 1024,
    timeout_ms: int = 120_000,
) -> None:
    value = {
        "schema_version": 1,
        "connector_id": connector_id,
        "deployment_sha256": deployment_sha256,
        "routes": [
            {
                "name": ROUTE_NAME,
                "credential_scope_sha256": scope_sha256,
                "models": [model],
                "request_bytes": request_bytes,
                "response_bytes": response_bytes,
                "timeout_ms": timeout_ms,
            }
        ],
    }
    path.write_text(
        json.dumps(value, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    os.chmod(path, 0o600)


def _github_operation_spec(
    *,
    capability: str,
    mode: str,
    mapping: Mapping[str, Any],
    deployment_sha256: str,
    idempotency_key: str,
    resource_id: str,
    object_format: str | None,
    expected_revision: str | None,
    desired_revision: str | None,
    parameters: Mapping[str, Any],
) -> ServiceOperationSpec:
    return ServiceOperationSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_name": GITHUB_SIDECAR_NAME,
            "deployment_sha256": deployment_sha256,
            "mapping": dict(mapping),
            "capability": {"name": capability, "version": 1, "mode": mode},
            "idempotency_key": idempotency_key,
            "target": {
                "repository": mapping["repository"],
                "resource_id": resource_id,
                "object_format": object_format,
                "expected_revision": expected_revision,
                "desired_revision": desired_revision,
            },
            "parameters": dict(parameters),
            "request_bytes": GITHUB_REQUEST_BYTES,
            "response_bytes": GITHUB_RESPONSE_BYTES,
            "timeout_ms": GITHUB_TIMEOUT_MS,
        }
    )


def _prepare_control_plane(
    control: ControlPlane,
    *,
    repository: Path,
    source_revision: str,
    image: str,
    image_id: str,
    connector_socket: Path,
    deployment_sha256: str,
    scope_sha256: str,
    model: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    database = control.database
    database.initialize()
    user = database.create_user("operator", "Workflow Proof Operator")
    instance = database.create_krab_instance(
        "Workflow Proof Krab", "http://127.0.0.1:8765"
    )
    producer_identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity("usr_" + "4" * 32, "workflow_producer", 1),
        "file:/not-mounted/workflow-producer.token",
        user_id=user["id"],
    )
    reviewer_identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity("usr_" + "5" * 32, "workflow_reviewer", 1),
        "file:/not-mounted/workflow-reviewer.token",
    )
    project = database.create_project(
        instance_id=instance["id"],
        krab_project_id="prj_" + "6" * 32,
        krab_project_key=PROJECT_KEY,
        display_name="Imitation Krab workflow proof",
        context=_project_context(repository),
        created_by=user["id"],
    )
    work_item = database.import_work_item(
        project_key=PROJECT_KEY,
        envelope=_work_item(),
        imported_by=user["id"],
    )
    connector = control.register_inference_connector(
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
        ),
        expires_at=None,
        created_by_handle="operator",
    )
    producer = database.create_agent_profile(
        name=PRODUCER_PROFILE,
        spec=_profile(image=image, workspace_mode=WorkspaceMode.COPY_ON_WRITE),
        krab_identity_id=producer_identity["id"],
        created_by=user["id"],
    )
    reviewer = database.create_agent_profile(
        name=REVIEWER_PROFILE,
        spec=_profile(image=image, workspace_mode=WorkspaceMode.READ_ONLY),
        krab_identity_id=reviewer_identity["id"],
        created_by=user["id"],
    )
    configuration = control.create_workflow_configuration(
        project_key=PROJECT_KEY,
        name=WORKFLOW_NAME,
        spec=_workflow_spec(image_id),
        producer_profile_name=PRODUCER_PROFILE,
        reviewer_profile_name=REVIEWER_PROFILE,
        created_by_handle="operator",
    )
    execution = control.start_workflow_execution(
        project_key=PROJECT_KEY,
        configuration_name=WORKFLOW_NAME,
        work_item_id=work_item["id"],
        source_revision=source_revision,
        expected_configuration_version=configuration["version"],
        expected_context_version_id=project["context_version_id"],
        expected_work_item_version_id=work_item["work_item_version_id"],
        expected_thread_generation=1,
        expected_thread_version=1,
        idempotency_key="imitation-krab-receipt-policy-live-proof",
        requested_by_handle="operator",
    )
    return execution, connector, {"producer": producer, "reviewer": reviewer}


def _existing_execution(
    database: Database,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    database.initialize()
    executions = database.list_workflow_executions(project_key=PROJECT_KEY, limit=2)
    if len(executions) != 1:
        raise RuntimeError("workflow state must contain exactly one execution")
    return (
        executions[0],
        database.get_inference_connector(CONNECTOR_NAME),
        {
            "producer": database.get_agent_profile(PRODUCER_PROFILE),
            "reviewer": database.get_agent_profile(REVIEWER_PROFILE),
        },
    )


def _current_gate(database: Database, execution: Mapping[str, Any]) -> dict[str, Any]:
    gate = database.get_workflow_gate(
        str(execution["id"]), int(execution["correction_round"])
    )
    if gate is None:
        raise RuntimeError("workflow has no gate for its current round")
    return gate


def _pending_maintainer(database: Database, execution: Mapping[str, Any]) -> bool:
    if execution["state"] != "active" or execution["stage"] != "maintainer":
        return False
    gate = _current_gate(database, execution)
    return not database.list_maintainer_reviews(gate["policy_id"])


def _drive_until(
    supervisor: Supervisor,
    database: Database,
    execution_id: str,
    *,
    predicate: Callable[[Mapping[str, Any]], bool],
    timeout: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_signature: tuple[object, ...] | None = None
    while time.monotonic() < deadline:
        execution = database.get_workflow_execution(execution_id)
        if predicate(execution):
            supervisor.drain(timeout=30)
            return database.get_workflow_execution(execution_id)
        open_rulings = [
            item for item in execution["rulings"] if item["state"] == "open"
        ]
        if open_rulings:
            raise WorkflowRulingRequired(
                "workflow opened a ruling: " + str(open_rulings[0]["kind"])
            )
        signature = (
            execution["state"],
            execution["stage"],
            execution["correction_round"],
            execution["version"],
        )
        result = supervisor.tick()
        if signature == last_signature and not any(result.values()):
            time.sleep(0.1)
        else:
            time.sleep(0.05)
        last_signature = signature
    raise RuntimeError("workflow proof timed out before reaching its durable stop")


def _ensure_private_directory(path: Path, *, exist_ok: bool) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=exist_ok)
    os.chmod(path, 0o700)


def _ensure_spec_file(path: Path, spec: ServiceSidecarSpec) -> None:
    expected = (spec.canonical + "\n").encode("utf-8")
    if path.exists():
        try:
            info = path.lstat()
            observed = path.read_bytes()
        except OSError as exc:
            raise RuntimeError("GitHub provider spec is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise RuntimeError("GitHub provider spec must be a regular file")
        if observed != expected:
            raise RuntimeError("GitHub provider spec changed across restart")
        return
    _write_spec(path, spec)


def _clear_stale_socket(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise RuntimeError("GitHub provider socket is unavailable") from exc
    if not stat.S_ISSOCK(info.st_mode):
        raise RuntimeError("GitHub provider socket path is not a socket")
    path.unlink()


def _register_github_authority(
    control: ControlPlane,
    *,
    spec: ServiceSidecarSpec,
) -> dict[str, dict[str, Any]]:
    control.register_service_sidecar(
        name=GITHUB_SIDECAR_NAME,
        spec=spec,
        created_by_handle="operator",
    )
    bindings: dict[str, dict[str, Any]] = {}
    for capability in ("refs.publish", "change.open", "landing.invoke"):
        bindings[capability] = control.bind_project_capability(
            project_key=PROJECT_KEY,
            capability_name=capability,
            capability_version=1,
            sidecar_name=GITHUB_SIDECAR_NAME,
            mapping_name=GITHUB_MAPPING_NAME,
            bound_by_handle="operator",
        )
    return bindings


def _settle_effect(
    control: ControlPlane,
    operation: dict[str, Any],
    *,
    owner_id: str,
) -> dict[str, Any]:
    if operation["state"] == "succeeded":
        return operation
    if operation["state"] in {"uncertain", "dispatching"}:
        settled = control.reconcile_service_operation(
            operation["id"],
            owner_id=owner_id,
            expected_version=operation["version"],
        )
    elif operation["state"] in {"queued", "retryable"}:
        settled = control.execute_service_operation(
            operation["id"],
            owner_id=owner_id,
            expected_version=operation["version"],
        )
    else:
        raise RuntimeError(
            f"service operation cannot continue from {operation['state']}"
        )
    if settled["state"] != "succeeded":
        raise RuntimeError("service operation did not succeed")
    return settled


def _operation_receipt(operation: Mapping[str, Any]) -> dict[str, Any]:
    receipts = [
        attempt["receipt"]
        for attempt in operation["attempts"]
        if attempt["outcome"] == "succeeded" and attempt["receipt"] is not None
    ]
    if len(receipts) != 1:
        raise RuntimeError("service operation has no unique successful receipt")
    return receipts[0]


def _publish_and_open(
    control: ControlPlane,
    *,
    execution: Mapping[str, Any],
    candidate: Mapping[str, Any],
    candidate_repository: Path,
    spec: ServiceSidecarSpec,
    bindings: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], int]:
    mapping = spec.mappings[0].to_dict()
    deployment = spec.deployment_sha256
    prefix = f"workflow:{execution['id']}:{candidate['head_revision']}"
    publish_spec = _github_operation_spec(
        capability="refs.publish",
        mode="effect",
        mapping=mapping,
        deployment_sha256=deployment,
        idempotency_key=prefix + ":publish",
        resource_id=SUGGESTED_BRANCH,
        object_format=candidate["object_format"],
        expected_revision=None,
        desired_revision=candidate["head_revision"],
        parameters={"source_repository": str(candidate_repository)},
    )
    published = control.create_service_operation(
        spec=publish_spec,
        created_by_handle="operator",
        project_capability_binding_id=bindings["refs.publish"]["id"],
    )
    published = _settle_effect(
        control, published, owner_id=f"workflow-publish-{execution['id']}"
    )
    publish_receipt = _operation_receipt(published)
    if publish_receipt["observed_revision"] != candidate["head_revision"]:
        raise RuntimeError("GitHub publish observed the wrong candidate")

    open_spec = _github_operation_spec(
        capability="change.open",
        mode="effect",
        mapping=mapping,
        deployment_sha256=deployment,
        idempotency_key=prefix + ":open",
        resource_id=SUGGESTED_BRANCH,
        object_format=candidate["object_format"],
        expected_revision=execution["source_revision"],
        desired_revision=candidate["head_revision"],
        parameters={
            "base_ref": "refs/heads/main",
            "body": (
                "Implemented by the restart-safe Magic Glass Box workflow.\n\n"
                f"Workflow: `{execution['id']}`\n"
                f"Candidate: `{candidate['id']}`\n"
                "Independent review, local validation, and explicit maintainer "
                "approval are recorded in the MGB audit trail."
            ),
            "draft": False,
            "head_ref": SUGGESTED_BRANCH,
            "title": _work_item().title,
        },
    )
    opened = control.create_service_operation(
        spec=open_spec,
        created_by_handle="operator",
        project_capability_binding_id=bindings["change.open"]["id"],
    )
    opened = _settle_effect(
        control, opened, owner_id=f"workflow-open-{execution['id']}"
    )
    open_receipt = _operation_receipt(opened)
    pull_number = _pull_number(
        open_receipt["remote_identity"],
        host=mapping["host"],
        repository=mapping["repository"],
    )
    if open_receipt["observed_revision"] != candidate["head_revision"]:
        raise RuntimeError("GitHub change observed the wrong candidate")
    return published, opened, pull_number


def _workflow_runs(
    database: Database, execution: Mapping[str, Any]
) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for round_number in range(int(execution["correction_round"]) + 1):
        for role in ("producer", "reviewer"):
            run = database.find_workflow_run(execution["id"], round_number, role)
            if run is not None:
                runs.append(run)
    return runs


def _workflow_rounds(
    database: Database,
    execution: Mapping[str, Any],
    result_store: WorkspaceResultStore,
) -> list[dict[str, Any]]:
    rounds: list[dict[str, Any]] = []
    for round_number in range(int(execution["correction_round"]) + 1):
        gate = database.get_workflow_gate(execution["id"], round_number)
        if gate is None:
            continue
        candidate = database.get_candidate(gate["candidate_id"])
        reviewer = database.find_workflow_run(execution["id"], round_number, "reviewer")
        verdict = (
            None
            if reviewer is None
            else database.get_typed_review_verdict(gate["policy_id"], reviewer["id"])
        )
        rounds.append(
            {
                "round": round_number,
                "candidate_id": candidate["id"],
                "candidate_head": candidate["head_revision"],
                "changed_paths": _changed_paths(result_store, candidate["attempt_id"]),
                "policy_id": gate["policy_id"],
                "reviewer_run_id": None if reviewer is None else reviewer["id"],
                "review_verdict": verdict,
                "maintainer_reviews": database.list_maintainer_reviews(
                    gate["policy_id"]
                ),
            }
        )
    return rounds


def _previous_invocations(report_path: Path) -> list[dict[str, Any]]:
    if not report_path.exists():
        return []
    try:
        value = json.loads(report_path.read_text(encoding="utf-8"))
        invocations = value.get("proof_invocations", [])
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, AttributeError) as exc:
        raise RuntimeError("previous workflow report is invalid") from exc
    if not isinstance(invocations, list) or len(invocations) > 16:
        raise RuntimeError("previous workflow report is invalid")
    return [dict(item) for item in invocations if isinstance(item, dict)]


def _report_status(execution: Mapping[str, Any]) -> str:
    if execution["state"] == "completed":
        return "passed"
    if execution["state"] != "active":
        return str(execution["state"])
    if any(ruling["state"] == "open" for ruling in execution["rulings"]):
        return "ruling_required"
    if execution["stage"] == "maintainer":
        return "pending_maintainer_approval"
    return "active"


def _write_workflow_report(
    *,
    report_path: Path,
    database: Database,
    execution: Mapping[str, Any],
    profiles: Mapping[str, Mapping[str, Any]],
    result_store: WorkspaceResultStore,
    model: str,
    repository: Path,
    state_root: Path,
    docker_engine: _RecordingDockerEngine,
    phase_evidence: Mapping[str, Any],
    github: Mapping[str, Any] | None,
) -> dict[str, Any]:
    runs = _workflow_runs(database, execution)
    invocations = [
        invocation
        for run in runs
        for invocation in database.list_provider_invocations(run["id"])
    ]
    usage = {
        field: sum(int(invocation[field] or 0) for invocation in invocations)
        for field in (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_output_tokens",
            "estimated_microusd",
            "accounted_microusd",
        )
    }
    grant = database.get_provider_grant(GRANT_NAME)
    budget_ceiling = grant["policy"]["budget"]["project_microusd"]
    proof_invocations = _previous_invocations(report_path)
    proof_invocations.append(dict(phase_evidence))
    report = {
        "status": _report_status(execution),
        "repository": str(repository),
        "source_revision": execution["source_revision"],
        "state_directory": str(state_root),
        "database": str(database.path),
        "model": model,
        "budget_ceiling_microusd": budget_ceiling,
        "provider_invocations": len(invocations),
        "usage": usage,
        "direct_worker_network": "denied",
        "credential_exposure": "none outside provider processes",
        "workflow_execution_id": execution["id"],
        "workflow_state": execution["state"],
        "workflow_stage": execution["stage"],
        "workflow_version": execution["version"],
        "correction_round": execution["correction_round"],
        "events": execution["events"],
        "rulings": execution["rulings"],
        "landing": execution["landing"],
        "completion": execution["completion"],
        "profiles": {
            role: {
                "profile_version_id": profile["profile_version_id"],
                "identity_id": profile["krab_identity_id"],
            }
            for role, profile in profiles.items()
        },
        "run_ids": [run["id"] for run in runs],
        "rounds": _workflow_rounds(database, execution, result_store),
        "proof_invocations": proof_invocations,
        "restart_recovery_observed": len(proof_invocations) > 1,
        "github": None if github is None else dict(github),
        "handoff": {
            "next_action": (
                "Inspect the exact current candidate and provide one bounded "
                "maintainer review document. No landing authority exists without "
                "an approved decision and the separate destructive opt-in."
                if execution["stage"] == "maintainer"
                else None
            )
        },
    }
    _write_report(report_path, report)
    report["report_sha256"] = hashlib.sha256(report_path.read_bytes()).hexdigest()
    return report


def _validate_state_location(state_root: Path, *, protected: Sequence[Path]) -> None:
    for item in protected:
        try:
            if state_root.resolve().is_relative_to(item.resolve()):
                raise RuntimeError(
                    "workflow state directory must remain outside code and credentials"
                )
        except OSError as exc:
            raise RuntimeError("workflow path is unavailable") from exc


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.i_understand_this_uses_live_quota:
        raise RuntimeError(
            "live quota opt-in is required: --i-understand-this-uses-live-quota"
        )
    if not hasattr(os, "getuid") or os.getuid() <= 0 or os.getgid() <= 0:
        raise RuntimeError("live workflow proof requires a non-root POSIX user")

    project_root = Path(__file__).resolve().parents[1]
    source_root = project_root / "src"
    profile_dir = args.profile_dir.expanduser().absolute()
    repository = args.repository.expanduser().resolve(strict=True)
    state_root = args.state_dir.expanduser().absolute()
    fresh = not state_root.exists()
    _validate_state_location(
        state_root,
        protected=(project_root, profile_dir, repository),
    )
    if not fresh and not state_root.is_dir():
        raise RuntimeError("workflow state path must be a directory")
    connector_socket = _connector_socket_path(state_root)
    observed_revision = _source_revision(repository, args.source_revision)

    profile_store = ChatGPTProfileStore(profile_dir)
    oauth = ChatGPTOAuth(profile_store)
    session = oauth.session()
    if args.model not in {model.slug for model in oauth.list_models()}:
        raise RuntimeError("selected model is not visible to this ChatGPT account")
    initial_secrets = _session_secrets(profile_store)
    if args.build:
        _build_image(args.image, args.build_timeout)

    _ensure_private_directory(state_root, exist_ok=not fresh)
    connector_root = connector_socket.parent
    _ensure_private_directory(connector_root, exist_ok=not fresh)
    deployment_sha256 = _deployment_digest(project_root)
    database = Database(state_root / "control-plane.db")
    control = ControlPlane(database)
    docker_engine = _RecordingDockerEngine(
        DockerCLI(command_timeout=60, run_timeout=args.run_timeout)
    )
    image = docker_engine.inspect_image(args.image)
    if fresh:
        execution, connector, profiles = _prepare_control_plane(
            control,
            repository=repository,
            source_revision=observed_revision,
            image=args.image,
            image_id=image.image_id,
            connector_socket=connector_socket,
            deployment_sha256=deployment_sha256,
            scope_sha256=session.credential_scope_sha256,
            model=args.model,
        )
    else:
        execution, connector, profiles = _existing_execution(database)
        if execution["source_revision"] != observed_revision:
            raise RuntimeError(
                "source repository no longer matches the pinned workflow"
            )

    expected_connector = _workflow_connector_spec(
        socket_path=connector_socket,
        deployment_sha256=deployment_sha256,
        scope_sha256=session.credential_scope_sha256,
        model=args.model,
    )
    acceptable_connector_specs = {expected_connector.canonical}
    if not fresh:
        acceptable_connector_specs.add(
            _connector_spec(
                socket_path=connector_socket,
                deployment_sha256=deployment_sha256,
                scope_sha256=session.credential_scope_sha256,
                model=args.model,
            ).canonical
        )
    stored_connector_spec = InferenceConnectorSpec.from_mapping(connector["spec"])
    if (
        connector["state"] != "active"
        or stored_connector_spec.canonical not in acceptable_connector_specs
    ):
        raise RuntimeError("existing connector does not match this workflow proof")
    connector_config = connector_root / "config.json"
    _write_workflow_connector_config(
        connector_config,
        connector_id=connector["id"],
        deployment_sha256=deployment_sha256,
        scope_sha256=session.credential_scope_sha256,
        model=args.model,
        request_bytes=stored_connector_spec.request_bytes,
        response_bytes=stored_connector_spec.response_bytes,
        timeout_ms=stored_connector_spec.timeout_ms,
    )

    review = (
        None
        if args.maintainer_review is None
        else _load_maintainer_review(args.maintainer_review)
    )
    execution = database.get_workflow_execution(execution["id"])
    if review is not None:
        if not _pending_maintainer(database, execution):
            raise RuntimeError("maintainer review requires the current pending gate")
        _require_approved_landing_authority(
            review,
            correction_round=execution["correction_round"],
            gh_config_dir=args.gh_config_dir,
            acknowledged=args.approved_landing_acknowledged,
        )
        gate = _current_gate(database, execution)
        policy = database.get_gate_policy(gate["policy_id"])
        control.record_maintainer_review(
            policy["id"],
            review_request=review.to_dict(),
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )
        execution = database.get_workflow_execution(execution["id"])

    result_store = WorkspaceResultStore(
        state_root / "results", max_paths=512, max_changed_bytes=64 * 1024 * 1024
    )
    candidate_store = GitCandidateStore(state_root / "candidates")
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
    director = WorkflowDirector(control, result_store, candidate_store, executor)
    supervisor = Supervisor(
        database,
        coordinator,
        control,
        workflow_director=director,
        poll_interval=0.1,
        max_workers=2,
        action_lease_seconds=60,
        shutdown_timeout=300,
        action_timeout=args.run_timeout + 300,
    )

    connector_process: subprocess.Popen[str] | None = None
    connector_stdout = ""
    connector_stderr = ""
    github_process: subprocess.Popen[bytes] | None = None
    github_report: dict[str, Any] | None = None
    proof_error: BaseException | None = None
    try:
        pending_maintainer = _pending_maintainer(database, execution)
        current_reviews = (
            database.list_maintainer_reviews(
                _current_gate(database, execution)["policy_id"]
            )
            if execution["state"] == "active"
            and execution["stage"] in {"maintainer", "landing", "completion"}
            else []
        )
        approved_landing = (
            execution["state"] == "active"
            and execution["stage"] in {"maintainer", "landing"}
            and bool(current_reviews)
            and current_reviews[0]["decision"] == "approved"
        )
        if approved_landing:
            if execution["correction_round"] < 1:
                raise RuntimeError(
                    "the M12 exit proof requires one genuine changes_requested round"
                )
            if args.gh_config_dir is None:
                raise RuntimeError("approved landing requires --gh-config-dir")
            if not args.approved_landing_acknowledged:
                raise RuntimeError(f"approved landing requires {ACKNOWLEDGEMENT_FLAG}")
            execution = _drive_until(
                supervisor,
                database,
                execution["id"],
                predicate=lambda item: (
                    item["state"] == "active"
                    and item["stage"] == "landing"
                    and item["landing"] is not None
                ),
                timeout=args.workflow_timeout,
            )
            gate = _current_gate(database, execution)
            candidate = database.get_candidate(gate["candidate_id"])
            manifest = CandidateManifest.from_mapping(candidate["manifest"])
            candidate_repository = candidate_store.repository_path(
                manifest.project_id, manifest.object_format
            )

            gh_config_dir = args.gh_config_dir.expanduser().resolve(strict=True)
            gh_bin_raw = shutil.which("gh")
            git_bin_raw = shutil.which("git")
            if gh_bin_raw is None or git_bin_raw is None:
                raise RuntimeError("the GitHub and Git CLIs are required")
            identity = _inspect_credential(
                python_bin=Path(sys.executable).resolve(),
                source_root=source_root,
                gh_config_dir=gh_config_dir,
                gh_bin=Path(gh_bin_raw).resolve(),
                host=args.github_host,
                expected_account=args.expected_github_account,
            )
            github_root = state_root / "github"
            _ensure_private_directory(github_root, exist_ok=True)
            github_socket = github_root / "provider.sock"
            _clear_stale_socket(github_socket)
            github_spec = _service_sidecar_spec(
                socket_path=github_socket,
                host=args.github_host,
                repository=args.github_repository,
                account=identity["account"],
                credential_scope_sha256=identity["credential_scope_sha256"],
                deployment_sha256=_github_deployment_sha256(source_root),
            )
            github_spec_path = github_root / "provider.json"
            _ensure_spec_file(github_spec_path, github_spec)
            github_process = _start_provider(
                python_bin=Path(sys.executable).resolve(),
                source_root=source_root,
                gh_config_dir=gh_config_dir,
                gh_bin=Path(gh_bin_raw).resolve(),
                git_bin=Path(git_bin_raw).resolve(),
                spec_path=github_spec_path,
                socket_path=github_socket,
                candidate_root=state_root / "candidates",
            )
            bindings = _register_github_authority(control, spec=github_spec)
            published, opened, pull_number = _publish_and_open(
                control,
                execution=execution,
                candidate=candidate,
                candidate_repository=candidate_repository,
                spec=github_spec,
                bindings=bindings,
            )
            control.configure_workflow_provider_landing(
                execution["id"],
                expected_version=execution["version"],
                resource_id=str(pull_number),
                merge_method=args.merge_method,
                configured_by_handle="operator",
            )
            execution = _drive_until(
                supervisor,
                database,
                execution["id"],
                predicate=lambda item: item["state"] == "completed",
                timeout=args.workflow_timeout,
            )
            github_report = {
                "account": identity["account"],
                "credential_scope_sha256": identity["credential_scope_sha256"],
                "repository": args.github_repository,
                "host": args.github_host,
                "branch": SUGGESTED_BRANCH,
                "pull_number": pull_number,
                "publish_operation_id": published["id"],
                "open_operation_id": opened["id"],
                "candidate_head": candidate["head_revision"],
                "landing_receipt_id": execution["landing"]["receipt_id"],
            }
        elif execution["state"] == "active" and execution["stage"] == "completion":
            execution = _drive_until(
                supervisor,
                database,
                execution["id"],
                predicate=lambda item: item["state"] == "completed",
                timeout=args.workflow_timeout,
            )
        elif execution["state"] == "active" and not pending_maintainer:
            connector_process = _start_connector(
                project_root=project_root,
                profile_dir=profile_dir,
                config_path=connector_config,
                socket_path=connector_socket,
                max_invocations=args.max_invocations,
            )
            execution = _drive_until(
                supervisor,
                database,
                execution["id"],
                predicate=lambda item: _pending_maintainer(database, item),
                timeout=args.workflow_timeout,
            )
        else:
            execution = database.get_workflow_execution(execution["id"])
    except WorkflowRulingRequired:
        execution = database.get_workflow_execution(execution["id"])
    except BaseException as exc:
        proof_error = exc
        raise
    finally:
        try:
            supervisor.close(wait=True, timeout=300)
        finally:
            try:
                gateways.close()
            finally:
                if github_process is not None:
                    try:
                        _stop_provider(github_process)
                    except RuntimeError:
                        if proof_error is None:
                            raise
                if connector_process is not None:
                    connector_stdout, connector_stderr = _stop_connector(
                        connector_process
                    )

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
            "workflow state": _collect_regular_files(state_root),
            "worker runtime output": "\n".join(docker_engine.logs_seen).encode(),
            "connector logs": (connector_stdout + connector_stderr).encode(),
            "Docker sandbox launch plans": launch_plans,
        },
    )
    plans_text = launch_plans.decode("utf-8", "replace")
    if args.gh_config_dir is not None and str(args.gh_config_dir) in plans_text:
        raise RuntimeError("GitHub credential path entered a worker launch plan")
    run_ids = [run["id"] for run in _workflow_runs(database, execution)]
    if any(docker_engine.managed_containers(run_id) for run_id in run_ids):
        raise RuntimeError("workflow proof left a managed worker container behind")

    phase_evidence = {
        "sequence": len(_previous_invocations(state_root / REPORT_NAME)) + 1,
        "started_from_fresh_state": fresh,
        "maintainer_decision": None if review is None else review.decision.value,
        "sandbox_plan_count": len(docker_engine.plans),
        "sandbox_plans_sha256": hashlib.sha256(launch_plans).hexdigest(),
        "direct_worker_network": "denied",
        "chatgpt_credentials_in_workers": False,
        "github_credentials_in_workers": False,
        "finished_stage": execution["stage"],
        "finished_state": execution["state"],
    }
    report = _write_workflow_report(
        report_path=state_root / REPORT_NAME,
        database=database,
        execution=execution,
        profiles=profiles,
        result_store=result_store,
        model=args.model,
        repository=repository,
        state_root=state_root,
        docker_engine=docker_engine,
        phase_evidence=phase_evidence,
        github=github_report,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
