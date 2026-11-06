#!/usr/bin/env python3
"""Run the bounded two-project M18 portable-workflow live proof."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from magic_glass_box.candidates import GitCandidateStore
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    ProjectContext,
    WorkItemEnvelope,
    WorkspaceMode,
)
from magic_glass_box.executor import (
    DockerCLI,
    DockerExecutor,
    SandboxRequest,
)
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.openai_chatgpt import ChatGPTOAuth, ChatGPTProfileStore
from magic_glass_box.orchestration import CandidateReviewWorkspaces, RunCoordinator
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.runtime import RuntimeLaunch
from magic_glass_box.service import AcceptedProviderResponse, ControlPlane
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceSidecarAmbiguousError,
    ServiceSidecarSpec,
    UnixServiceSidecarClient,
)
from magic_glass_box.supervision import Supervisor
from magic_glass_box.workflow_director import WorkflowDirector
from magic_glass_box.workflow_policy import WorkflowActivationSpec, WorkflowPolicySpec
from magic_glass_box.workflows import (
    IMPLEMENTATION_TEMPLATE_REVISION,
    WorkflowConfigurationSpec,
)
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

try:
    from scripts import check_live_imitation_krab_workflow as workflow
    from scripts.check_live_m13 import _ConnectorWaves
    from scripts.check_live_openai_chatgpt import (
        _assert_no_secret,
        _assert_sandbox_boundaries,
        _collect_regular_files,
        _deployment_digest,
        _RecordingDockerEngine,
        _serialize_sandbox_plans,
        _session_secrets,
        _start_connector,
        _stop_connector,
    )
except ModuleNotFoundError:  # Direct execution puts scripts/ on sys.path.
    import check_live_imitation_krab_workflow as workflow  # type: ignore[no-redef]
    from check_live_m13 import _ConnectorWaves  # type: ignore[no-redef]
    from check_live_openai_chatgpt import (  # type: ignore[no-redef]
        _assert_no_secret,
        _assert_sandbox_boundaries,
        _collect_regular_files,
        _deployment_digest,
        _RecordingDockerEngine,
        _serialize_sandbox_plans,
        _session_secrets,
        _start_connector,
        _stop_connector,
    )

SCHEMA_VERSION = 2
MODEL = "gpt-6.1-sol"
POLICY_NAME = "implementation-portable-v1"
CONNECTOR_NAME = "openai-chatgpt-m18"
GRANT_NAME = "openai-chatgpt-m18"
WORKER_IMAGE = "magic-glass-box/worker:m18r9-81a5cf8"
VALIDATION_IMAGE = "magic-glass-box/validation:m18r9-81a5cf8"
MGB_PROJECT = "magic-glass-box-m18"
IK_PROJECT = "imitation-krab-m18"
MGB_REVISION = "81a5cf848c2ed23d5872d10227a550971724e748"
IK_REVISION = "cc0778433d97fb1c82a7c2fde1b222b86b436e5d"
MGB_PROFILE_NAMES = ("M18 MGB Producer", "M18 MGB Reviewer")
IK_PROFILE_NAMES = ("M18 IK Producer", "M18 IK Reviewer")
CONFIGURATION_NAME = "m18-implementation"
VALIDATION_OBLIGATION = "full_quality"
MGB_VALIDATION = "mgb_full_quality"
IK_VALIDATION = "ik_full_quality"
BUDGET_MICROUSD = 5_000_000
PROOF_TIMEOUT_SECONDS = 10_800.0
RUN_TIMEOUT_SECONDS = 1_800.0
REPORT_NAME = "m18-live-report.json"
OBSERVATIONS_NAME = "observations.jsonl"
CONTROLLER_IDENTITY_NAME = "controller-identity.json"
LOST_RECEIPT_DIRECTORY = "lost-receipts"
MAX_DIAGNOSTIC_EVENTS = 32
MAX_DIAGNOSTIC_TEXT = 2048
MAX_CONTROLLER_OBSERVATIONS_BYTES = 1024 * 1024
MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES = 64 * 1024 * 1024
CONTROLLER_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
GITHUB_SIDECAR_NAMES = {
    MGB_PROJECT: "github-m18-mgb",
    IK_PROJECT: "github-m18-ik",
}
GITHUB_REPOSITORIES = {
    MGB_PROJECT: "psyberone/magic-glass-box",
    IK_PROJECT: "psyberone/imitation-krab",
}


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _canonical(value: object) -> bytes:
    return (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _bounded_text(value: object, *, limit: int = MAX_DIAGNOSTIC_TEXT) -> str | None:
    if not isinstance(value, str):
        return None
    return value[-limit:]


def _runtime_diagnostics(logs: Sequence[str]) -> list[dict[str, Any]]:
    """Retain bounded failures, never a worker transcript or reasoning stream."""
    diagnostics: list[dict[str, Any]] = []
    for raw in logs:
        failures: list[dict[str, Any]] = []
        event_count = 0
        last_event_type: str | None = None
        for line in raw.splitlines():
            if not line.strip():
                continue
            event_count += 1
            try:
                event = json.loads(line)
            except (json.JSONDecodeError, RecursionError, ValueError):
                continue
            if not isinstance(event, Mapping) or not isinstance(event.get("type"), str):
                continue
            last_event_type = event["type"]
            if event["type"] == "error":
                failures.append(
                    {
                        "type": "runtime.error",
                        "message": _bounded_text(event.get("message")),
                    }
                )
                continue
            if event["type"] == "turn.failed":
                error = event.get("error")
                message = error.get("message") if isinstance(error, Mapping) else None
                failures.append(
                    {
                        "type": "turn.failed",
                        "message": _bounded_text(message),
                    }
                )
                continue
            if event["type"] != "item.completed":
                continue
            item = event.get("item")
            if not isinstance(item, Mapping) or item.get("type") != "command_execution":
                continue
            exit_code = item.get("exit_code")
            if exit_code in {None, 0} and item.get("status") != "failed":
                continue
            failures.append(
                {
                    "type": "command.failed",
                    "exit_code": exit_code,
                    "command": _bounded_text(item.get("command"), limit=1024),
                    "output_tail": _bounded_text(item.get("aggregated_output")),
                }
            )
        diagnostics.append(
            {
                "sha256": hashlib.sha256(raw.encode()).hexdigest(),
                "event_count": event_count,
                "last_event_type": last_event_type,
                "failures": failures[-MAX_DIAGNOSTIC_EVENTS:],
            }
        )
    return diagnostics


def _write_private_json(path: Path, value: object) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(_canonical(value))
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


class _LostReceiptClient:
    """Discard one successful effect response after the provider accepted it."""

    def __init__(
        self,
        delegate: Any,
        *,
        marker_path: Path,
        project_key: str,
        lock: threading.Lock,
    ):
        self._delegate = delegate
        self._marker_path = marker_path
        self._project_key = project_key
        self._lock = lock

    def invoke(self, request: ServiceOperationRequest) -> ServiceOperationResponse:
        response = self._delegate.invoke(request)
        if (
            request.phase != "invoke"
            or request.capability.mode != "effect"
            or response.outcome != "succeeded"
            or response.receipt is None
        ):
            return response
        with self._lock:
            if self._marker_path.exists():
                return response
            receipt = response.receipt
            _write_private_json(
                self._marker_path,
                {
                    "schema_version": 1,
                    "project_key": self._project_key,
                    "operation_id": request.operation_id,
                    "capability": request.capability.name,
                    "phase": request.phase,
                    "response_sha256": hashlib.sha256(
                        _canonical(response.to_dict())
                    ).hexdigest(),
                    "remote_identity_sha256": hashlib.sha256(
                        receipt.remote_identity.encode("utf-8")
                    ).hexdigest(),
                    "observed_revision": receipt.observed_revision,
                    "evidence_sha256": receipt.evidence_sha256,
                    "dropped_at": _now(),
                },
            )
        raise ServiceSidecarAmbiguousError(
            "M18 deliberately discarded one successful provider receipt"
        )


class _LostReceiptClientFactory:
    def __init__(
        self,
        *,
        marker_root: Path,
        socket_projects: Mapping[Path, str],
        client_factory: Callable[[Path], Any] = UnixServiceSidecarClient,
    ):
        marker_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(marker_root, 0o700)
        self._marker_root = marker_root
        self._socket_projects = {
            str(path.absolute()): project_key
            for path, project_key in socket_projects.items()
        }
        self._client_factory = client_factory
        self._lock = threading.Lock()

    def __call__(self, socket_path: Path) -> Any:
        delegate = self._client_factory(socket_path)
        project_key = self._socket_projects.get(str(socket_path.absolute()))
        if project_key is None:
            return delegate
        return _LostReceiptClient(
            delegate,
            marker_path=self._marker_root / f"{project_key}.json",
            project_key=project_key,
            lock=self._lock,
        )


def _append_observation(
    state_root: Path, kind: str, summary: str, **metadata: object
) -> None:
    value = {"at": _now(), "kind": kind, "summary": summary, "metadata": metadata}
    path = state_root / OBSERVATIONS_NAME
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, _canonical(value))
    finally:
        os.close(descriptor)


def _run_checked(command: Sequence[str], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(  # noqa: S603 - fixed local proof argv
        list(command),
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"proof command failed: {detail[:2048]}")
    return completed.stdout.strip()


def _git(repository: Path, *arguments: str) -> str:
    return _run_checked(("git", "-C", str(repository), *arguments))


def _controller_identity(project_root: Path) -> dict[str, Any]:
    commit = _git(project_root, "rev-parse", "HEAD")
    diff = _run_checked(
        ("git", "-C", str(project_root), "diff", "--binary", "HEAD", "--")
    ).encode()
    git_bin = shutil.which("git")
    if git_bin is None:
        raise RuntimeError("git is required to identify the M18 controller")
    untracked_raw = subprocess.run(  # noqa: S603 - fixed local proof argv
        (
            str(Path(git_bin).resolve(strict=True)),
            "-C",
            str(project_root),
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
        ),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=120,
        check=False,
    )
    if untracked_raw.returncode != 0:
        raise RuntimeError("could not inventory untracked controller files")
    digest = hashlib.sha256()
    digest.update(b"tracked-diff\0")
    digest.update(diff)
    untracked_paths = sorted(path for path in untracked_raw.stdout.split(b"\0") if path)
    for encoded_path in untracked_paths:
        relative = Path(os.fsdecode(encoded_path))
        candidate = (project_root / relative).resolve(strict=True)
        if not candidate.is_relative_to(project_root.resolve(strict=True)):
            raise RuntimeError("controller inventory escaped the repository")
        digest.update(b"untracked\0")
        digest.update(encoded_path)
        digest.update(b"\0")
        if candidate.is_symlink():
            digest.update(os.readlink(candidate).encode())
        elif candidate.is_file():
            digest.update(candidate.read_bytes())
        else:
            raise RuntimeError("controller inventory contains a non-regular entry")
    return {
        "commit": commit,
        "dirty": bool(diff or untracked_paths),
        "dirty_sha256": digest.hexdigest(),
    }


def _pin_controller_identity(
    state_root: Path,
    observed: Mapping[str, Any],
    *,
    existing_state: bool,
    change_reason: str | None = None,
    activation_owners: Sequence[str] = (),
) -> dict[str, Any]:
    path = state_root / CONTROLLER_IDENTITY_NAME
    if not path.exists():
        if existing_state:
            raise RuntimeError("existing M18 state has no controller identity pin")
        pinned = {"pinned_at": _now(), **dict(observed)}
        _write_private_json(path, pinned)
        return pinned
    try:
        pinned = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, TypeError) as exc:
        raise RuntimeError("M18 controller identity pin is invalid") from exc
    comparable = {key: pinned.get(key) for key in ("commit", "dirty", "dirty_sha256")}
    if comparable == dict(observed):
        return pinned
    if change_reason is None or not change_reason.strip():
        raise RuntimeError(
            "M18 controller identity changed; an activation-owner observation is "
            "required before continuation"
        )
    if not activation_owners or set(activation_owners) != {"operator"}:
        raise RuntimeError(
            "controller code-change observation requires every activation owner"
        )
    replacement = {"pinned_at": _now(), **dict(observed)}
    _append_observation(
        state_root,
        "controller.code_change",
        change_reason.strip(),
        observed_by="operator",
        previous=comparable,
        replacement=dict(observed),
    )
    _write_private_json(path, replacement)
    return replacement


def _prepare_target(source: Path, target: Path, revision: str) -> Path:
    if not target.exists():
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _run_checked(
            ("git", "clone", "--no-hardlinks", "--quiet", str(source), str(target))
        )
        _git(target, "checkout", "--detach", "--quiet", revision)
    if not target.is_dir():
        raise RuntimeError("proof target is not a directory")
    if _git(target, "rev-parse", "HEAD") != revision:
        raise RuntimeError("proof target no longer matches its pinned revision")
    if _git(target, "status", "--porcelain=v1", "--untracked-files=all"):
        raise RuntimeError("proof target is not clean")
    _git(target, "cat-file", "-e", f"{revision}^{{commit}}")
    return target.resolve(strict=True)


def _project_context(project_key: str, repository: Path) -> ProjectContext:
    if project_key == MGB_PROJECT:
        return ProjectContext.from_mapping(
            {
                "purpose": (
                    "Magic Glass Box is a local-first control plane for bounded, "
                    "credential-isolated, auditable agent work."
                ),
                "repository_path": str(repository),
                "default_branch": "main",
                "instructions": [
                    "Preserve outside-scary, inside-safe trust boundaries.",
                    (
                        "Keep workers networkless; credentials remain in bounded "
                        "host-side providers and never enter the workspace."
                    ),
                    (
                        "Prefer explicit versioned authority, durable evidence, "
                        "optimistic concurrency, and honest failure states."
                    ),
                    (
                        "Use RED-first happy- and sad-path tests, retain at least "
                        "85% branch-aware coverage, and preserve the 80% focused "
                        "meaningful mutation-kill floor."
                    ),
                    (
                        "For this bounded producer run, execute only focused metadata "
                        "and wheel checks. The control plane owns the complete quality "
                        "suite in its pinned post-review validation container."
                    ),
                    (
                        "Do not widen this packaging-metadata task into versioning, "
                        "runtime-dependency, build-backend, or release-process changes."
                    ),
                    "Do not commit, push, open a pull request, or use the network.",
                ],
            }
        )
    return ProjectContext.from_mapping(
        {
            "purpose": (
                "Imitation Krab is a small, loopback-first coordination service "
                "for cooperating human and agent participants."
            ),
            "repository_path": str(repository),
            "default_branch": "0.7.0",
            "instructions": [
                (
                    "Preserve the separation between server-attested metadata and "
                    "untrusted participant text."
                ),
                (
                    "Prefer small explicit primitives with clear authority, "
                    "idempotency, optimistic concurrency, bounded reads, and "
                    "honest failure behavior."
                ),
                (
                    "Krab is not an agent runtime, general chat platform, workflow "
                    "engine, Git integration, credential broker, plugin host, "
                    "semantic index, code-execution service, or scheduler."
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
                "Do not commit, push, open a pull request, or use the network.",
            ],
        }
    )


def _work_item(project_key: str) -> WorkItemEnvelope:
    common = {
        "schema_version": 1,
        "source": "manual",
        "source_updated_at": "2026-11-04T22:30:00Z",
        "priority": "normal",
    }
    if project_key == MGB_PROJECT:
        return WorkItemEnvelope.from_mapping(
            {
                **common,
                "external_id": "M18-MGB-LICENSE-METADATA",
                "title": "Modernize package license metadata",
                "brief": (
                    "Replace the deprecated project.license TOML table with the "
                    "standard SPDX expression for MIT. Raise only the setuptools "
                    "build-system lower bound to the version family that first "
                    "supports SPDX expressions (>=77). Preserve the package version, "
                    "runtime dependencies, build backend, installed license file, "
                    "and all runtime behavior. Add only the smallest durable "
                    "verification needed to prevent regression."
                ),
                "acceptance_criteria": [
                    "pyproject.toml uses the SPDX string MIT for project.license.",
                    (
                        "A wheel build emits License-Expression: MIT and still "
                        "contains the repository MIT license file."
                    ),
                    "The setuptools project.license-table deprecation is absent.",
                    (
                        "build-system.requires declares setuptools>=77 and the wheel "
                        "proof passes with setuptools 77.0.1."
                    ),
                    "The complete quality suite passes at the existing coverage floor.",
                    (
                        "No package version, runtime dependency, backend, or "
                        "release-policy change is made."
                    ),
                ],
                "constraints": [
                    "Keep the change narrowly within package metadata and its proof.",
                    "Use RED-first verification where a repository test is warranted.",
                    (
                        "Do not regenerate runtime dependency locks for this "
                        "build-metadata-only change."
                    ),
                    "Do not leave test-output, build, coverage, or cache artifacts.",
                    (
                        "Do not run the complete repository quality suite inside the "
                        "producer sandbox; run the focused license test and wheel "
                        "proof."
                    ),
                    "Do not commit, push, open a pull request, or use the network.",
                ],
                "context": [
                    {
                        "type": "note",
                        "value": (
                            "The warning was observed during the M18 source/wheel "
                            "quality gate and becomes actionable before February 2027."
                        ),
                    }
                ],
                "labels": ["m18-live", "packaging", "license"],
            }
        )
    return WorkItemEnvelope.from_mapping(
        {
            **common,
            "external_id": "M18-IK-LIVENESS-COPY",
            "title": "Describe krab health as a liveness probe",
            "brief": (
                "Change the krab health subcommand help from `check daemon health "
                "without credentials` to `probe daemon liveness without credentials`. "
                "Add the smallest stable assertion for that public help text. Do not "
                "change request behavior, readiness semantics, schemas, or endpoints."
            ),
            "acceptance_criteria": [
                "Top-level CLI help describes krab health as a liveness probe.",
                "A focused test fails if the bounded help contract regresses.",
                "Health request behavior and machine-readable schemas are unchanged.",
                "The complete unittest suite and branch-aware coverage floor pass.",
            ],
            "constraints": [
                "Use only the Python standard library.",
                "Keep loopback and credential-free health boundaries unchanged.",
                "Do not expand liveness into readiness or compatibility checking.",
                "Use RED-first verification for the help assertion.",
                "Do not commit, push, open a pull request, or use the network.",
            ],
            "context": [
                {
                    "type": "note",
                    "value": (
                        "An earlier independent maintainer review accepted the health "
                        "feature and left this exact wording polish as non-blocking."
                    ),
                }
            ],
            "labels": ["m18-live", "cli", "liveness"],
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


def _configuration(
    project_key: str, validation_image_id: str
) -> WorkflowConfigurationSpec:
    if project_key == MGB_PROJECT:
        validation_name = MGB_VALIDATION
        command = [
            "/bin/sh",
            "-c",
            (
                "set -eu; export RUFF_CACHE_DIR=/tmp/ruff-cache; "
                "make PYTHON=/opt/mgb-validation/bin/python quality; "
                "rm -rf /tmp/m18-dist; mkdir -p /tmp/m18-dist; "
                "/opt/mgb-validation/bin/python -m build --no-isolation --wheel "
                "--outdir /tmp/m18-dist"
            ),
        ]
        rounds = 2
    else:
        validation_name = IK_VALIDATION
        command = [
            "/bin/sh",
            "-c",
            (
                "set -eu; export PYTHONDONTWRITEBYTECODE=1; "
                "export PYTHONPATH=src:tests; "
                "/opt/mgb-validation/bin/python -m coverage erase; "
                "/opt/mgb-validation/bin/python -m coverage run --branch -m unittest "
                "discover -s tests -v; "
                "/opt/mgb-validation/bin/python -m coverage report"
            ),
        ]
        rounds = 1
    return WorkflowConfigurationSpec.from_mapping(
        {
            "schema_version": 1,
            "review_separation": "different_profile_and_identity",
            "maintainer_approval": True,
            "validations": [
                {
                    "name": validation_name,
                    "image_id": validation_image_id,
                    "command": command,
                }
            ],
            "max_correction_rounds": rounds,
            "landing_mode": "provider",
        }
    )


def _policy() -> WorkflowPolicySpec:
    return WorkflowPolicySpec.from_mapping(
        {
            "schema_version": 1,
            "prescription": {
                "key": "implementation-v1",
                "revision": IMPLEMENTATION_TEMPLATE_REVISION,
            },
            "evidence_floor": [
                "maintainer_disposition",
                "reviewed_content",
                "validated_execution",
            ],
            "validation_obligations": [VALIDATION_OBLIGATION],
            "resource_maxima": {
                "review_rounds": 2,
                "seat_count": 2,
                "shared_host_concurrency": 1,
                "wall_time_seconds": 10_800,
            },
            "wake_mode": "manual",
            "required_capabilities": [
                {"mode": "effect", "name": name, "version": 1}
                for name in ("change.open", "landing.invoke", "refs.publish")
            ],
            "landing_methods": ["squash"],
            "allow_deferred_validation": True,
        }
    )


def _activation(
    *,
    project_key: str,
    policy_sha256: str,
    configuration_version_id: str,
    bindings: Mapping[str, Mapping[str, Any]],
) -> WorkflowActivationSpec:
    if project_key == MGB_PROJECT:
        validation = MGB_VALIDATION
        rounds = 2
        wall_time = 10_800
    else:
        validation = IK_VALIDATION
        rounds = 1
        wall_time = 7_200
    return WorkflowActivationSpec.from_mapping(
        {
            "schema_version": 1,
            "policy_sha256": policy_sha256,
            "workflow_configuration_version_id": configuration_version_id,
            "validation_bindings": [
                {"obligation": VALIDATION_OBLIGATION, "validation": validation}
            ],
            "capability_bindings": [
                {
                    "binding_id": bindings[name]["id"],
                    "mode": "effect",
                    "name": name,
                    "version": 1,
                }
                for name in ("change.open", "landing.invoke", "refs.publish")
            ],
            "resources": {
                "review_rounds": rounds,
                "seat_count": 2,
                "shared_host_concurrency": 1,
                "wall_time_seconds": wall_time,
            },
            "wake_bindings": [
                {"role": "producer", "wake_class": "human_turn_only"},
                {"role": "reviewer", "wake_class": "human_turn_only"},
            ],
            "merge_method": "squash",
            "retired_rules": [],
        }
    )


def _deployment_identity(base_digest: str, project_key: str, repository: str) -> str:
    manifest = {
        "provider_code_sha256": base_digest,
        "project_key": project_key,
        "repository": repository,
    }
    return hashlib.sha256(_canonical(manifest)).hexdigest()


def _github_spec(
    *,
    state_root: Path,
    project_key: str,
    repository: str,
    account: str,
    credential_scope_sha256: str,
    base_deployment_sha256: str,
) -> ServiceSidecarSpec:
    label = "mgb" if project_key == MGB_PROJECT else "ik"
    return workflow._service_sidecar_spec(
        socket_path=state_root / "github" / f"{label}.sock",
        repository=repository,
        account=account,
        credential_scope_sha256=credential_scope_sha256,
        deployment_sha256=_deployment_identity(
            base_deployment_sha256, project_key, repository
        ),
    )


def _github_specs(
    *,
    state_root: Path,
    account: str,
    credential_scope_sha256: str,
    base_deployment_sha256: str,
) -> dict[str, ServiceSidecarSpec]:
    return {
        MGB_PROJECT: _github_spec(
            state_root=state_root,
            project_key=MGB_PROJECT,
            repository="psyberone/magic-glass-box",
            account=account,
            credential_scope_sha256=credential_scope_sha256,
            base_deployment_sha256=base_deployment_sha256,
        ),
        IK_PROJECT: _github_spec(
            state_root=state_root,
            project_key=IK_PROJECT,
            repository="psyberone/imitation-krab",
            account=account,
            credential_scope_sha256=credential_scope_sha256,
            base_deployment_sha256=base_deployment_sha256,
        ),
    }


def _registered_github_specs(
    database: Database,
    *,
    expected: Mapping[str, ServiceSidecarSpec],
) -> dict[str, ServiceSidecarSpec]:
    """Retain immutable registered authorities across controller upgrades."""
    selected: dict[str, ServiceSidecarSpec] = {}
    for project_key in (MGB_PROJECT, IK_PROJECT):
        registered = database.get_service_sidecar(GITHUB_SIDECAR_NAMES[project_key])
        try:
            spec = ServiceSidecarSpec.from_mapping(registered["spec"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"registered GitHub authority changed for {project_key}"
            ) from exc
        actual = spec.to_dict()
        structural = expected[project_key].to_dict()
        structural["deployment_sha256"] = spec.deployment_sha256
        if (
            registered.get("state") != "active"
            or registered.get("content_sha256") != spec.digest
            or actual != structural
        ):
            raise RuntimeError(f"registered GitHub authority changed for {project_key}")
        selected[project_key] = spec
    return selected


def _github_specs_match_deployment(
    specs: Mapping[str, ServiceSidecarSpec],
    base_deployment_sha256: str,
) -> bool:
    return all(
        specs[project_key].deployment_sha256
        == _deployment_identity(
            base_deployment_sha256,
            project_key,
            GITHUB_REPOSITORIES[project_key],
        )
        for project_key in (MGB_PROJECT, IK_PROJECT)
    )


def _source_tree_sha256(source_root: Path) -> str:
    digest = hashlib.sha256()
    files = 0
    total = 0
    for path in sorted(source_root.rglob("*")):
        if path.is_dir():
            continue
        try:
            if path.is_symlink() or not path.is_file():
                raise RuntimeError("pinned GitHub source contains an unsafe entry")
            raw = path.read_bytes()
        except OSError as exc:
            raise RuntimeError("pinned GitHub source is unavailable") from exc
        files += 1
        total += len(raw)
        if total > MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES:
            raise RuntimeError("pinned GitHub source exceeds its size bounds")
        relative = path.relative_to(source_root).as_posix().encode("utf-8")
        digest.update(relative + b"\0")
        digest.update(len(raw).to_bytes(8, "big") + raw)
    if files == 0:
        raise RuntimeError("pinned GitHub source is empty")
    return digest.hexdigest()


def _materialize_controller_source(
    *,
    project_root: Path,
    state_root: Path,
    commit: str,
) -> tuple[Path, str, bool]:
    if not isinstance(commit, str) or CONTROLLER_COMMIT_RE.fullmatch(commit) is None:
        raise RuntimeError("controller source revision is invalid")
    deployment_root = state_root / "deployments" / "github"
    deployment_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(deployment_root, 0o700)
    destination = deployment_root / commit
    source_root = destination / "src"
    manifest_path = destination / "manifest.json"
    if destination.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("pinned GitHub source manifest is invalid") from exc
        base_digest = workflow._github_deployment_sha256(source_root)
        if (
            not isinstance(manifest, dict)
            or manifest.get("schema_version") != 1
            or manifest.get("commit") != commit
            or manifest.get("base_deployment_sha256") != base_digest
            or manifest.get("source_sha256") != _source_tree_sha256(source_root)
        ):
            raise RuntimeError("pinned GitHub source manifest is invalid")
        return source_root, base_digest, False

    git_raw = shutil.which("git")
    if git_raw is None:
        raise RuntimeError("git is required to pin GitHub provider source")
    completed = subprocess.run(  # noqa: S603 - fixed local proof argv
        (
            str(Path(git_raw).resolve(strict=True)),
            "-C",
            str(project_root),
            "archive",
            "--format=tar",
            commit,
            "src/magic_glass_box",
        ),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("registered GitHub source revision is unavailable")
    archive_raw = completed.stdout
    if not archive_raw or len(archive_raw) > MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES:
        raise RuntimeError("pinned GitHub source archive exceeds its size bounds")

    temporary = Path(tempfile.mkdtemp(prefix=".source-", dir=deployment_root))
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_raw), mode="r:") as archive:
            members = archive.getmembers()
            if any(not (member.isdir() or member.isfile()) for member in members):
                raise RuntimeError("pinned GitHub source archive is unsafe")
            try:
                archive.extractall(temporary, members=members, filter="data")
            except (OSError, tarfile.TarError) as exc:
                raise RuntimeError("pinned GitHub source archive is invalid") from exc
            if not (temporary / "src" / "magic_glass_box").is_dir():
                raise RuntimeError("pinned GitHub source archive is unsafe")
        temporary_source = temporary / "src"
        base_digest = workflow._github_deployment_sha256(temporary_source)
        source_digest = _source_tree_sha256(temporary_source)
        _write_private_json(
            temporary / "manifest.json",
            {
                "schema_version": 1,
                "commit": commit,
                "base_deployment_sha256": base_digest,
                "source_sha256": source_digest,
            },
        )
        os.replace(temporary, destination)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return source_root, base_digest, True


def _controller_commits(state_root: Path, current_commit: str) -> tuple[str, ...]:
    candidates = [current_commit]
    observations = state_root / OBSERVATIONS_NAME
    if observations.exists():
        raw = observations.read_bytes()
        if len(raw) > MAX_CONTROLLER_OBSERVATIONS_BYTES:
            raise RuntimeError("M18 observations exceed their recovery bound")
        lines = raw.splitlines()
        if len(lines) > 256:
            raise RuntimeError("M18 observations exceed their recovery bound")
        for line in lines:
            try:
                value = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
                raise RuntimeError("M18 observations are invalid") from exc
            if (
                not isinstance(value, dict)
                or value.get("kind") != "controller.code_change"
            ):
                continue
            metadata = value.get("metadata")
            if not isinstance(metadata, dict):
                raise RuntimeError("M18 controller observation is invalid")
            for key in ("previous", "replacement"):
                identity = metadata.get(key)
                if isinstance(identity, dict):
                    commit = identity.get("commit")
                    if (
                        isinstance(commit, str)
                        and CONTROLLER_COMMIT_RE.fullmatch(commit) is not None
                    ):
                        candidates.append(commit)
    return tuple(dict.fromkeys(candidates))


def _resolve_github_source_root(
    *,
    project_root: Path,
    current_source_root: Path,
    state_root: Path,
    current_commit: str,
    specs: Mapping[str, ServiceSidecarSpec],
) -> Path:
    current_digest = workflow._github_deployment_sha256(current_source_root)
    if _github_specs_match_deployment(specs, current_digest):
        return current_source_root
    for commit in _controller_commits(state_root, current_commit):
        if commit == current_commit:
            continue
        source_root, base_digest, created = _materialize_controller_source(
            project_root=project_root,
            state_root=state_root,
            commit=commit,
        )
        if _github_specs_match_deployment(specs, base_digest):
            if created:
                _append_observation(
                    state_root,
                    "sidecar.deployment_pinned",
                    "Recovered the exact registered GitHub sidecar source",
                    commit=commit,
                    base_deployment_sha256=base_digest,
                    source_sha256=_source_tree_sha256(source_root),
                )
            return source_root
    raise RuntimeError("registered GitHub deployment source is unavailable")


def _start_github_providers(
    *,
    database: Database,
    specs: Mapping[str, ServiceSidecarSpec],
    candidate_store: GitCandidateStore,
    state_root: Path,
    source_root: Path,
    gh_config_dir: Path,
    gh_bin: Path,
    git_bin: Path,
) -> list[subprocess.Popen[bytes]]:
    names = {
        MGB_PROJECT: "github-m18-mgb",
        IK_PROJECT: "github-m18-ik",
    }
    candidate_root = candidate_store.ensure_root()
    processes: list[subprocess.Popen[bytes]] = []
    try:
        for project_key in (MGB_PROJECT, IK_PROJECT):
            spec = specs[project_key]
            registered = database.get_service_sidecar(names[project_key])
            if (
                registered["state"] != "active"
                or registered["content_sha256"] != spec.digest
                or registered["spec"] != spec.to_dict()
            ):
                raise RuntimeError(
                    f"registered GitHub authority changed for {project_key}"
                )
            socket_path = Path(spec.transport.socket_path)
            workflow._clear_stale_socket(socket_path)
            spec_path = workflow._github_provider_spec_path(state_root / "github", spec)
            workflow._ensure_spec_file(spec_path, spec)
            processes.append(
                workflow._start_provider(
                    python_bin=Path(sys.executable).resolve(),
                    source_root=source_root,
                    gh_config_dir=gh_config_dir,
                    gh_bin=gh_bin,
                    git_bin=git_bin,
                    spec_path=spec_path,
                    socket_path=socket_path,
                    candidate_root=candidate_root,
                )
            )
    except BaseException as exc:
        for process in reversed(processes):
            try:
                workflow._stop_provider(process)
            except RuntimeError as cleanup_error:
                detail = _bounded_text(str(cleanup_error), limit=512) or "no detail"
                exc.add_note(
                    "secondary GitHub startup cleanup error: "
                    f"{type(cleanup_error).__name__}: {detail}"
                )
        raise
    return processes


def _stop_github_providers(processes: Sequence[subprocess.Popen[bytes]]) -> None:
    errors: list[RuntimeError] = []
    for process in reversed(processes):
        try:
            workflow._stop_provider(process)
        except RuntimeError as exc:
            errors.append(exc)
    if errors:
        raise RuntimeError(
            f"{len(errors)} GitHub provider process(es) did not stop cleanly"
        ) from errors[0]


def _register_sidecar(
    control: ControlPlane,
    *,
    project_key: str,
    name: str,
    spec: ServiceSidecarSpec,
) -> dict[str, dict[str, Any]]:
    control.register_service_sidecar(
        name=name,
        spec=spec,
        created_by_handle="operator",
    )
    return {
        capability: control.bind_project_capability(
            project_key=project_key,
            capability_name=capability,
            capability_version=1,
            sidecar_name=name,
            mapping_name="primary",
            bound_by_handle="operator",
        )
        for capability in ("change.open", "landing.invoke", "refs.publish")
    }


def _prepare_control_plane(
    control: ControlPlane,
    *,
    state_root: Path,
    targets: Mapping[str, Path],
    revisions: Mapping[str, str],
    worker_image: str,
    validation_image_id: str,
    connector_socket: Path,
    connector_deployment_sha256: str,
    credential_scope_sha256: str,
    github_specs: Mapping[str, ServiceSidecarSpec],
) -> dict[str, str]:
    database = control.database
    database.initialize()
    operator = database.create_user("operator", "M18 Live Proof Operator")
    instance = database.create_krab_instance(
        "M18 Local Authority", "http://127.0.0.1:8765"
    )
    identity_specs = (
        (MGB_PROFILE_NAMES[0], "m18_mgb_producer", "1"),
        (MGB_PROFILE_NAMES[1], "m18_mgb_reviewer", "2"),
        (IK_PROFILE_NAMES[0], "m18_ik_producer", "3"),
        (IK_PROFILE_NAMES[1], "m18_ik_reviewer", "4"),
    )
    identities: dict[str, dict[str, Any]] = {}
    for profile_name, handle, digit in identity_specs:
        identities[profile_name] = database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + digit * 32, handle, 1),
            "file:/not-mounted/" + handle + ".token",
        )

    project_ids = {MGB_PROJECT: "5", IK_PROJECT: "6"}
    for project_key in (MGB_PROJECT, IK_PROJECT):
        database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + project_ids[project_key] * 32,
            krab_project_key=project_key,
            display_name=(
                "Magic Glass Box M18 live proof"
                if project_key == MGB_PROJECT
                else "Imitation Krab M18 live proof"
            ),
            context=_project_context(project_key, targets[project_key]),
            created_by=operator["id"],
        )

    connector = control.register_inference_connector(
        name=CONNECTOR_NAME,
        spec=workflow._workflow_connector_spec(
            socket_path=connector_socket,
            deployment_sha256=connector_deployment_sha256,
            scope_sha256=credential_scope_sha256,
            model=MODEL,
        ),
        created_by_handle="operator",
    )
    grant_value = workflow._workflow_grant_spec(
        deployment_sha256=connector_deployment_sha256,
        scope_sha256=credential_scope_sha256,
        model=MODEL,
        budget_ceiling_microusd=BUDGET_MICROUSD,
    ).to_dict()
    grant_value["connector_name"] = CONNECTOR_NAME
    from magic_glass_box.inference import InferenceGrantSpec

    control.add_connector_provider_grant(
        name=GRANT_NAME,
        spec=InferenceGrantSpec.from_mapping(grant_value),
        expires_at=None,
        created_by_handle="operator",
    )

    profile_modes = {
        MGB_PROFILE_NAMES[0]: WorkspaceMode.COPY_ON_WRITE,
        MGB_PROFILE_NAMES[1]: WorkspaceMode.READ_ONLY,
        IK_PROFILE_NAMES[0]: WorkspaceMode.COPY_ON_WRITE,
        IK_PROFILE_NAMES[1]: WorkspaceMode.READ_ONLY,
    }
    for name, mode in profile_modes.items():
        database.create_agent_profile(
            name=name,
            spec=_profile(image=worker_image, workspace_mode=mode),
            krab_identity_id=identities[name]["id"],
            created_by=operator["id"],
        )

    configurations: dict[str, dict[str, Any]] = {}
    for project_key, profile_names in (
        (MGB_PROJECT, MGB_PROFILE_NAMES),
        (IK_PROJECT, IK_PROFILE_NAMES),
    ):
        configurations[project_key] = control.create_workflow_configuration(
            project_key=project_key,
            name=CONFIGURATION_NAME,
            spec=_configuration(project_key, validation_image_id),
            producer_profile_name=profile_names[0],
            reviewer_profile_name=profile_names[1],
            created_by_handle="operator",
        )

    github_root = state_root / "github"
    github_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(github_root, 0o700)
    bindings = {
        MGB_PROJECT: _register_sidecar(
            control,
            project_key=MGB_PROJECT,
            name="github-m18-mgb",
            spec=github_specs[MGB_PROJECT],
        ),
        IK_PROJECT: _register_sidecar(
            control,
            project_key=IK_PROJECT,
            name="github-m18-ik",
            spec=github_specs[IK_PROJECT],
        ),
    }

    policy = control.create_workflow_policy(
        name=POLICY_NAME,
        spec=_policy(),
        created_by_handle="operator",
    )
    executions: dict[str, str] = {}
    for project_key in (MGB_PROJECT, IK_PROJECT):
        configuration = configurations[project_key]
        activation = control.create_workflow_activation(
            project_key=project_key,
            policy_name=POLICY_NAME,
            expected_policy_version=policy["version"],
            spec=_activation(
                project_key=project_key,
                policy_sha256=policy["content_sha256"],
                configuration_version_id=configuration[
                    "workflow_configuration_version_id"
                ],
                bindings=bindings[project_key],
            ),
            acknowledged_by_handle="operator",
        )
        work_item = database.import_work_item(
            project_key=project_key,
            envelope=_work_item(project_key),
            imported_by=operator["id"],
        )
        project = database.get_project(project_key)
        execution = control.start_workflow_execution(
            project_key=project_key,
            configuration_name=CONFIGURATION_NAME,
            work_item_id=work_item["id"],
            source_revision=revisions[project_key],
            expected_configuration_version=configuration["version"],
            expected_context_version_id=project["context_version_id"],
            expected_work_item_version_id=work_item["work_item_version_id"],
            expected_thread_generation=1,
            expected_thread_version=1,
            expected_activation_version=activation["version"],
            idempotency_key=f"m18-live:{project_key}:v1",
            requested_by_handle="operator",
        )
        executions[project_key] = execution["id"]
    _append_observation(
        state_root,
        "preparation",
        "Created one portable policy and two project-local activations",
        policy_id=policy["id"],
        policy_sha256=policy["content_sha256"],
        connector_id=connector["id"],
        executions=executions,
    )
    return executions


def _existing_executions(database: Database) -> dict[str, str]:
    result: dict[str, str] = {}
    for project_key in (MGB_PROJECT, IK_PROJECT):
        executions = database.list_workflow_executions(
            project_key=project_key, state="active", limit=2
        )
        if len(executions) != 1:
            raise RuntimeError(
                "M18 state must contain exactly one active execution per project"
            )
        result[project_key] = executions[0]["id"]
    return result


def _execution_lineage(
    database: Database, executions: Mapping[str, str]
) -> dict[str, list[dict[str, Any]]]:
    lineage: dict[str, list[dict[str, Any]]] = {}
    for project_key, current_execution_id in executions.items():
        current = database.get_workflow_execution(current_execution_id)
        related = [
            execution
            for execution in database.list_workflow_executions(
                project_key=project_key, limit=100
            )
            if execution["work_item_id"] == current["work_item_id"]
        ]
        if not any(item["id"] == current_execution_id for item in related):
            raise RuntimeError("current M18 execution is missing from its lineage")
        lineage[project_key] = sorted(
            related, key=lambda item: (item["created_at"], item["id"])
        )
    return lineage


def _acknowledge_requested_wakes(
    control: ControlPlane, executions: Mapping[str, str], *, state_root: Path
) -> None:
    for execution_id in executions.values():
        for wake in control.list_workflow_manual_wakes(execution_id=execution_id):
            if wake["state"] not in {"requested", "delivered"}:
                continue
            if wake["availability"] is None:
                control.record_workflow_wake_availability(
                    wake["id"],
                    expected_wake_version=wake["version"],
                    state="available",
                    unavailable_until=None,
                    blocked_on=None,
                    observed_by_handle="operator",
                    idempotency_key=f"m18-live:availability:{wake['id']}",
                )
                wake = control.get_workflow_manual_wake(wake["id"])
            if wake["state"] == "requested":
                wake = control.transition_workflow_manual_wake(
                    wake["id"],
                    expected_version=wake["version"],
                    target_state="delivered",
                    actor_handle="operator",
                )
            control.acknowledge_workflow_manual_wake(
                wake["id"],
                expected_version=wake["version"],
                acknowledged_by_handle="operator",
            )
            _append_observation(
                state_root,
                "wake.harness_acknowledged",
                "The live-proof harness delivered and acknowledged a manual wake",
                wake_id=wake["id"],
                execution_id=execution_id,
                run_id=wake["run_id"],
                actor="operator",
            )


def _is_pending_maintainer(database: Database, execution_id: str) -> bool:
    execution = database.get_workflow_execution(execution_id)
    if execution["state"] != "active" or execution["stage"] != "maintainer":
        return False
    gate = database.get_workflow_gate(execution_id, int(execution["correction_round"]))
    if gate is None:
        return False
    return not database.list_maintainer_reviews(gate["policy_id"])


def _has_open_ruling(database: Database, execution_id: str) -> bool:
    execution = database.get_workflow_execution(execution_id)
    return any(ruling["state"] == "open" for ruling in execution["rulings"])


def _all_completed(database: Database, executions: Mapping[str, str]) -> bool:
    return all(
        database.get_workflow_execution(execution_id)["state"] == "completed"
        for execution_id in executions.values()
    )


def _uncertain_provider_effects(
    database: Database, executions: Mapping[str, str]
) -> list[dict[str, str]]:
    uncertain: list[dict[str, str]] = []
    for project_key, execution_id in executions.items():
        execution = database.get_workflow_execution(execution_id)
        landing = execution.get("landing")
        if not isinstance(landing, Mapping):
            continue
        intent = landing.get("provider_intent")
        if not isinstance(intent, Mapping):
            continue
        effects = intent.get("effects")
        if not isinstance(effects, Mapping):
            continue
        for capability, effect in effects.items():
            if not isinstance(effect, Mapping) or effect.get("operation_state") != (
                "uncertain"
            ):
                continue
            operation_id = effect.get("service_operation_id")
            if isinstance(capability, str) and isinstance(operation_id, str):
                uncertain.append(
                    {
                        "project_key": project_key,
                        "execution_id": execution_id,
                        "capability": capability,
                        "operation_id": operation_id,
                    }
                )
    return uncertain


def _build_supervisor(
    *,
    database: Database,
    control: ControlPlane,
    state_root: Path,
    docker_engine: _RecordingDockerEngine,
    allow_unenforced_worker_storage: bool,
) -> tuple[Supervisor, FileGatewayManager, WorkspaceResultStore, GitCandidateStore]:
    result_store = WorkspaceResultStore(
        state_root / "results", max_paths=512, max_changed_bytes=64 * 1024 * 1024
    )
    candidate_store = GitCandidateStore(state_root / "candidates")
    workspaces = WorkspaceManager(state_root / "workspaces", GitSnapshotter())
    candidate_workspaces = CandidateReviewWorkspaces(
        database, workspaces, result_store, candidate_store
    )
    gateways = FileGatewayManager(state_root / "gateways", control)
    executor = DockerExecutor(
        docker_engine,
        allow_unenforced_worker_storage=allow_unenforced_worker_storage,
    )
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
        poll_interval=0.1,
        max_workers=1,
        action_lease_seconds=60,
        shutdown_timeout=300,
        action_timeout=RUN_TIMEOUT_SECONDS + 300,
    )
    return supervisor, gateways, result_store, candidate_store


def _assert_gateway_carrier(
    *,
    docker_engine: DockerCLI,
    state_root: Path,
    worker_image: str,
) -> None:
    """Prove bidirectional networkless delivery before any live provider call."""

    class CarrierControl:
        def __init__(self) -> None:
            self.calls = 0

        def invoke_provider(
            self,
            run_id: str,
            *,
            grant_name: str,
            request: dict[str, Any],
        ) -> AcceptedProviderResponse:
            del run_id, request
            self.calls += 1
            if self.calls == 2:
                time.sleep(0.2)
            body = "data: " + ("x" * 116_622) + "\n\n"
            return AcceptedProviderResponse(
                invocation_id=f"pvi_{self.calls:032x}",
                grant_name=grant_name,
                provider="carrier-proof",
                model="no-remote-provider",
                status_code=200,
                request_bytes=32,
                response_bytes=len(body.encode()),
                untrusted_response={
                    "body": body,
                    "content_type": "text/event-stream",
                    "protocol": "openai-responses",
                },
            )

    container_code = """\
import os
from pathlib import Path
from magic_glass_box.worker_transport import FileGatewayClient

client = FileGatewayClient(
    Path(os.environ["MAGIC_GLASS_BOX_GATEWAY_DIR"]),
    response_directory=Path(os.environ["MAGIC_GLASS_BOX_GATEWAY_RESPONSE_DIR"]),
    timeout=5,
)
payload = {
    "grant": "carrier-proof",
    "request": {"protocol": "openai-responses", "body": {"input": "proof"}},
}
for _index in range(2):
    response = client.exchange("provider.invoke", payload, timeout=5)
    assert response["ok"] is True
    assert len(response["result"]["untrusted_response"]["body"]) > 100_000
print("m18-gateway-carrier-ok")
"""
    run_id = "run_" + secrets.token_hex(16)
    control = CarrierControl()
    with tempfile.TemporaryDirectory(
        dir=state_root,
        prefix="gateway-carrier-preflight-",
    ) as temporary:
        root = Path(temporary)
        workspace = root / "workspace"
        workspace.mkdir(mode=0o700)
        gateways = FileGatewayManager(
            root / "gateways",
            control,  # type: ignore[arg-type]
            poll_interval=0.005,
        )
        endpoint = gateways.open(
            run_id,
            capabilities=(GatewayCapability.PROVIDER_INVOKE,),
            credential_names=("carrier-proof",),
        )
        profile = AgentProfileSpec.create(
            image=worker_image,
            command=("/opt/magic-glass-box/bin/python3", "-c", container_code),
            workspace_mode=WorkspaceMode.READ_ONLY,
            network_mode=NetworkMode.GATEWAY_ONLY,
            credential_names=("carrier-proof",),
            gateway_capabilities=(GatewayCapability.PROVIDER_INVOKE,),
        )
        executor = DockerExecutor(
            docker_engine,
            allow_unenforced_worker_storage=True,
        )
        request = SandboxRequest(
            run_id=run_id,
            profile=profile,
            workspace=workspace,
            launch=RuntimeLaunch(profile.command),
            gateway_directory=endpoint.directory,
        )
        try:
            result = executor.execute(
                request,
                before_start=lambda _plan, container_id: gateways.attach_delivery(
                    run_id,
                    lambda response, marker: executor.publish_gateway_response(
                        container_id, response, marker
                    ),
                ),
            )
        finally:
            gateways.close()
        if (
            result.exit_code != 0
            or result.stdout.strip() != "m18-gateway-carrier-ok"
            or control.calls != 2
        ):
            raise RuntimeError("M18 gateway carrier preflight failed")


def _drive_one_technical_stop(
    *,
    supervisor: Supervisor,
    database: Database,
    control: ControlPlane,
    executions: Mapping[str, str],
    connector_waves: _ConnectorWaves,
    state_root: Path,
) -> str:
    if _all_completed(database, executions):
        supervisor.drain(timeout=30)
        return "both_completed"
    initially_pending = sum(
        _is_pending_maintainer(database, execution_id)
        for execution_id in executions.values()
    )
    initial_uncertain_ids = {
        effect["operation_id"]
        for effect in _uncertain_provider_effects(database, executions)
    }
    deadline = time.monotonic() + PROOF_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if _all_completed(database, executions):
            supervisor.drain(timeout=30)
            return "both_completed"
        uncertain = _uncertain_provider_effects(database, executions)
        newly_uncertain = [
            effect
            for effect in uncertain
            if effect["operation_id"] not in initial_uncertain_ids
        ]
        if newly_uncertain:
            supervisor.drain(timeout=30)
            _append_observation(
                state_root,
                "provider.receipt_discarded",
                "The live harness stopped after a deliberate lost provider receipt",
                effects=newly_uncertain,
            )
            return "planned_restart_after_provider_receipt_loss"
        pending = sum(
            _is_pending_maintainer(database, execution_id)
            for execution_id in executions.values()
        )
        if pending == len(executions):
            supervisor.drain(timeout=30)
            return "both_pending_maintainer"
        if pending > initially_pending:
            supervisor.drain(timeout=30)
            return "planned_restart_after_project_technical_gate"
        if any(
            _has_open_ruling(database, execution_id)
            for execution_id in executions.values()
        ):
            supervisor.drain(timeout=30)
            return "workflow_ruling_required"
        _acknowledge_requested_wakes(control, executions, state_root=state_root)
        connector_waves.before_tick()
        result = supervisor.tick()
        if not any(result.values()):
            time.sleep(0.1)
    raise RuntimeError("M18 proof timed out before a durable technical stop")


def _all_runs(
    database: Database, lineage: Mapping[str, Sequence[Mapping[str, Any]]]
) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for executions in lineage.values():
        for execution in executions:
            runs.extend(database.list_workflow_runs(execution["id"]))
    return runs


def _usage_totals(invocations: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {
        field: sum(int(item.get(field) or 0) for item in invocations)
        for field in (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_output_tokens",
            "estimated_microusd",
            "accounted_microusd",
        )
    }


def _provider_invocation_evidence(
    invocation: Mapping[str, Any], *, execution_id: str, run_id: str
) -> dict[str, Any]:
    fields = (
        "id",
        "outcome",
        "error_code",
        "provider",
        "protocol",
        "model",
        "http_status",
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "estimated_microusd",
        "accounted_microusd",
        "accounting_status",
        "started_at",
        "finished_at",
    )
    return {
        "execution_id": execution_id,
        "run_id": run_id,
        **{field: invocation.get(field) for field in fields},
    }


def _wake_evidence(wake: Mapping[str, Any]) -> dict[str, Any]:
    sources = {"request": "control_plane"}
    availability = wake.get("availability")
    if isinstance(availability, Mapping):
        sources["availability"] = (
            "live_harness"
            if availability.get("observed_by") == "operator"
            else "external_observer"
        )
    events = wake.get("events")
    if isinstance(events, list) and any(
        isinstance(event, Mapping)
        and event.get("kind") == "wake.delivered"
        and event.get("actor") == "operator"
        for event in events
    ):
        sources["delivery"] = "live_harness"
    if wake.get("acknowledged_by") is not None:
        sources["acknowledgement"] = (
            "live_harness"
            if wake.get("acknowledged_by") == "operator"
            else "external_observer"
        )
    return {**dict(wake), "proof_fact_sources": sources}


def _run_evidence(database: Database, run: Mapping[str, Any]) -> dict[str, Any]:
    invocations = [
        _provider_invocation_evidence(
            invocation,
            execution_id=run["workflow_execution_id"],
            run_id=run["id"],
        )
        for invocation in run["provider_invocations"]
    ]
    candidates = database.list_candidates(producer_run_id=run["id"], limit=100)
    return {
        "id": run["id"],
        "state": run["state"],
        "version": run["version"],
        "correction_round": run["workflow_round"],
        "role": run["workflow_role"],
        "generation": run["workflow_generation"],
        "predecessor_run_id": run["predecessor_run_id"],
        "replacement_kind": run["replacement_kind"],
        "profile_name": run["profile_name"],
        "profile_version_id": run["profile_version_id"],
        "attempts": [
            {
                key: attempt.get(key)
                for key in (
                    "id",
                    "sequence",
                    "state",
                    "failure_class",
                    "exit_code",
                    "reason",
                    "created_at",
                    "started_at",
                    "finished_at",
                )
            }
            for attempt in run["attempts"]
        ],
        "candidates": [
            {
                key: candidate.get(key)
                for key in (
                    "id",
                    "attempt_id",
                    "base_revision",
                    "head_revision",
                    "tree_revision",
                    "result_content_sha256",
                    "created_at",
                )
            }
            for candidate in candidates
        ],
        "provider_invocations": invocations,
        "usage": _usage_totals(invocations),
    }


def _execution_evidence(
    database: Database,
    result_store: WorkspaceResultStore,
    execution_id: str,
) -> dict[str, Any]:
    execution = database.get_workflow_execution(execution_id)
    bound_runs = database.list_workflow_runs(execution_id)
    rounds: list[dict[str, Any]] = []
    for round_number in range(int(execution["correction_round"]) + 1):
        gate = database.get_workflow_gate(execution_id, round_number)
        if gate is None:
            continue
        policy = database.get_gate_policy(gate["policy_id"])
        candidate = database.get_candidate(gate["candidate_id"])
        reviewer = database.find_workflow_run(execution_id, round_number, "reviewer")
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
                "candidate_tree": policy["tree_revision"],
                "changed_paths": workflow._changed_paths(
                    result_store, candidate["attempt_id"]
                ),
                "policy_id": policy["id"],
                "policy_revision": policy["policy_revision"],
                "readiness": database.gate_readiness(policy["id"]),
                "reviewer_run_id": None if reviewer is None else reviewer["id"],
                "review_verdict": verdict,
            }
        )
    round_causes = [
        event["metadata"]["round_cause"]
        for event in execution["events"]
        if event["kind"] in {"workflow.correction_started", "workflow.ruling_resolved"}
        and isinstance(event["metadata"].get("round_cause"), Mapping)
    ]
    run_evidence = [_run_evidence(database, run) for run in bound_runs]
    invocations = [
        invocation for run in run_evidence for invocation in run["provider_invocations"]
    ]
    return {
        "id": execution["id"],
        "project_key": execution["project_key"],
        "state": execution["state"],
        "stage": execution["stage"],
        "version": execution["version"],
        "correction_round": execution["correction_round"],
        "genuine_changes_requested": bool(round_causes),
        "round_causes": round_causes,
        "portable_policy": execution["portable_policy"],
        "responsibility": execution["responsibility"],
        "rulings": execution["rulings"],
        "rounds": rounds,
        "wakes": [
            _wake_evidence(wake)
            for wake in database.list_workflow_manual_wakes(execution_id=execution_id)
        ],
        "events": execution["events"],
        "runs": run_evidence,
        "usage": _usage_totals(invocations),
        "landing": execution["landing"],
        "closeout": execution["closeout"],
        "completion": execution["completion"],
    }


def _proof_status(database: Database, executions: Mapping[str, str]) -> str:
    if _all_completed(database, executions):
        return "completed"
    if all(
        _is_pending_maintainer(database, execution_id)
        for execution_id in executions.values()
    ):
        return "pending_maintainer_approval"
    return "active"


def _service_operation_evidence(operation: Mapping[str, Any]) -> dict[str, Any]:
    spec = operation["spec"]
    capability = spec["capability"]
    attempts: list[dict[str, Any]] = []
    for attempt in operation["attempts"]:
        receipt = attempt.get("receipt")
        receipt_evidence = None
        if isinstance(receipt, Mapping):
            receipt_evidence = {
                key: receipt.get(key)
                for key in (
                    "remote_identity",
                    "observed_revision",
                    "evidence_sha256",
                    "closed_references",
                )
            }
        attempts.append(
            {
                key: attempt.get(key)
                for key in (
                    "id",
                    "sequence",
                    "phase",
                    "outcome",
                    "provider_status",
                    "error_code",
                    "started_at",
                    "finished_at",
                )
            }
            | {"receipt": receipt_evidence}
        )
    return {
        "id": operation["id"],
        "state": operation["state"],
        "version": operation["version"],
        "sidecar_name": operation["sidecar"]["name"],
        "capability": capability["name"],
        "mode": capability["mode"],
        "input_sha256": operation["input_sha256"],
        "attempts": attempts,
    }


def _lost_receipt_evidence(state_root: Path, database: Database) -> dict[str, Any]:
    marker_root = state_root / LOST_RECEIPT_DIRECTORY
    markers: list[dict[str, Any]] = []
    for project_key in (MGB_PROJECT, IK_PROJECT):
        path = marker_root / f"{project_key}.json"
        if path.exists():
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, dict) or value.get("project_key") != project_key:
                raise RuntimeError("M18 lost-receipt marker is invalid")
            markers.append(value)
    operations = [
        _service_operation_evidence(operation)
        for operation in database.list_service_operations(limit=1000)
        if operation["spec"]["capability"]["mode"] == "effect"
    ]
    return {"markers": markers, "operations": operations}


def _report(
    *,
    state_root: Path,
    database: Database,
    executions: Mapping[str, str],
    result_store: WorkspaceResultStore,
    docker_engine: _RecordingDockerEngine,
    stop_reason: str,
    fresh: bool,
    controller_identity: Mapping[str, Any],
    proof_error: BaseException | None = None,
) -> dict[str, Any]:
    report_path = state_root / REPORT_NAME
    prior_invocations: list[dict[str, Any]] = []
    if report_path.exists():
        prior = json.loads(report_path.read_text(encoding="utf-8"))
        if isinstance(prior, dict) and isinstance(prior.get("invocations"), list):
            prior_invocations = [dict(item) for item in prior["invocations"]]
    plans = _serialize_sandbox_plans(docker_engine.plans)
    invocation = {
        "sequence": len(prior_invocations) + 1,
        "started_from_fresh_state": fresh,
        "stop_reason": stop_reason,
        "error_type": None if proof_error is None else type(proof_error).__name__,
        "sandbox_plan_count": len(docker_engine.plans),
        "sandbox_plans_sha256": hashlib.sha256(plans).hexdigest(),
        "controller": dict(controller_identity),
        "finished_at": _now(),
    }
    prior_invocations.append(invocation)
    lineage = _execution_lineage(database, executions)
    runs = _all_runs(database, lineage)
    provider_invocations = [
        item for run in runs for item in database.list_provider_invocations(run["id"])
    ]
    usage = _usage_totals(provider_invocations)
    policy = database.get_workflow_policy(name=POLICY_NAME)
    activations = {
        project_key: database.get_workflow_activation(project_key)
        for project_key in executions
    }
    execution_evidence = {
        project_key: [
            _execution_evidence(database, result_store, execution["id"])
            for execution in project_lineage
        ]
        for project_key, project_lineage in lineage.items()
    }
    provider_evidence = [
        evidence
        for project_executions in execution_evidence.values()
        for execution in project_executions
        for run in execution["runs"]
        for evidence in run["provider_invocations"]
    ]
    report = {
        "schema_version": 2,
        "status": _proof_status(database, executions),
        "model": MODEL,
        "billing_basis": "OpenAI ChatGPT subscription/quota; invoice cost unavailable",
        "budget_ceiling_microusd": BUDGET_MICROUSD,
        "provider_invocations": len(provider_invocations),
        "provider_invocation_evidence": provider_evidence,
        "provider_effect_recovery": _lost_receipt_evidence(state_root, database),
        "usage": usage,
        "source_revisions": {
            MGB_PROJECT: MGB_REVISION,
            IK_PROJECT: IK_REVISION,
        },
        "portable_policy": {
            "id": policy["id"],
            "version": policy["version"],
            "content_sha256": policy["content_sha256"],
            "activation_policy_sha256": {
                project_key: activation["policy"]["content_sha256"]
                for project_key, activation in activations.items()
            },
            "activation_content_sha256": {
                project_key: activation["content_sha256"]
                for project_key, activation in activations.items()
            },
        },
        "executions": execution_evidence,
        "active_execution_ids": dict(executions),
        "controller": dict(controller_identity),
        "gateway_carrier_preflight": "passed_without_remote_provider",
        "direct_worker_network": "denied",
        "credential_exposure": "none outside credential-owning host processes",
        "local_worker_storage": "explicit unenforced-storage risk acceptance",
        "run_ids": [run["id"] for run in runs],
        "worker_diagnostics": _runtime_diagnostics(docker_engine.logs_seen),
        "invocations": prior_invocations,
        "restart_recovery_observed": len(prior_invocations) > 1,
        "coordinator_fidelity_disposition": None,
    }
    _write_private_json(report_path, report)
    return report


def _validate_state_location(state_root: Path, *, protected: Sequence[Path]) -> None:
    for candidate in protected:
        try:
            if state_root.resolve().is_relative_to(candidate.resolve()):
                raise RuntimeError(
                    "M18 state directory must remain outside code and credentials"
                )
        except OSError as exc:
            raise RuntimeError("M18 proof path is unavailable") from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile-dir", required=True, type=Path)
    parser.add_argument("--mgb-repository", required=True, type=Path)
    parser.add_argument("--ik-repository", required=True, type=Path)
    parser.add_argument("--gh-config-dir", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--worker-image", default=WORKER_IMAGE)
    parser.add_argument("--validation-image", default=VALIDATION_IMAGE)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument(
        "--acknowledge-controller-change",
        metavar="REASON",
        help=(
            "record an activation-owner observation before continuing changed "
            "controller code"
        ),
    )
    parser.add_argument("--allow-unenforced-worker-storage", action="store_true")
    parser.add_argument("--i-understand-this-uses-live-quota", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.prepare_only and not args.i_understand_this_uses_live_quota:
        raise RuntimeError("M18 requires explicit live-quota acknowledgement")
    if not args.prepare_only and not args.allow_unenforced_worker_storage:
        raise RuntimeError("M18 requires explicit unenforced-storage acknowledgement")
    if not hasattr(os, "getuid") or os.getuid() <= 0 or os.getgid() <= 0:
        raise RuntimeError("M18 live proof requires a non-root POSIX user")

    project_root = Path(__file__).resolve().parents[1]
    observed_controller = _controller_identity(project_root)
    source_root = project_root / "src"
    profile_dir = args.profile_dir.expanduser().resolve(strict=True)
    gh_config_dir = args.gh_config_dir.expanduser().resolve(strict=True)
    mgb_repository = args.mgb_repository.expanduser().resolve(strict=True)
    ik_repository = args.ik_repository.expanduser().resolve(strict=True)
    state_root = args.state_dir.expanduser().absolute()
    _validate_state_location(
        state_root,
        protected=(
            project_root,
            profile_dir,
            gh_config_dir,
            mgb_repository,
            ik_repository,
        ),
    )
    fresh = not (state_root / "control-plane.db").exists()
    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(state_root, 0o700)
    database = Database(state_root / "control-plane.db")
    control = ControlPlane(database)
    existing_executions: dict[str, str] | None = None
    if fresh:
        if observed_controller["dirty"]:
            raise RuntimeError("a fresh M18 proof requires a clean controller tree")
        controller_identity = _pin_controller_identity(
            state_root,
            observed_controller,
            existing_state=False,
        )
    else:
        database.initialize()
        existing_executions = _existing_executions(database)
        activation_owners = [
            database.get_workflow_activation(project_key)["acknowledged_by"]
            for project_key in existing_executions
        ]
        controller_identity = _pin_controller_identity(
            state_root,
            observed_controller,
            existing_state=True,
            change_reason=args.acknowledge_controller_change,
            activation_owners=activation_owners,
        )
    targets = {
        MGB_PROJECT: _prepare_target(
            mgb_repository, state_root / "targets" / "magic-glass-box", MGB_REVISION
        ),
        IK_PROJECT: _prepare_target(
            ik_repository, state_root / "targets" / "imitation-krab", IK_REVISION
        ),
    }
    revisions = {MGB_PROJECT: MGB_REVISION, IK_PROJECT: IK_REVISION}

    connector_deployment = _deployment_digest(project_root)
    connector_root = state_root / "connector"
    connector_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(connector_root, 0o700)
    connector_socket = connector_root / "provider.sock"

    docker_cli = DockerCLI(command_timeout=60, run_timeout=RUN_TIMEOUT_SECONDS)
    docker_engine = _RecordingDockerEngine(docker_cli)
    worker_image = docker_engine.inspect_image(args.worker_image)
    validation_image = docker_engine.inspect_image(args.validation_image)
    if worker_image.image_id == validation_image.image_id:
        raise RuntimeError("M18 worker and validation images must be distinct")
    _assert_gateway_carrier(
        docker_engine=docker_cli,
        state_root=state_root,
        worker_image=args.worker_image,
    )

    profile_store = ChatGPTProfileStore(profile_dir)
    oauth = ChatGPTOAuth(profile_store)
    session = oauth.session()
    if MODEL not in {model.slug for model in oauth.list_models()}:
        raise RuntimeError("M18 model is unavailable to the selected account")
    initial_secrets = _session_secrets(profile_store)

    gh_bin_raw = shutil.which("gh")
    git_bin_raw = shutil.which("git")
    if gh_bin_raw is None or git_bin_raw is None:
        raise RuntimeError("GitHub CLI and Git are required for M18 live proof")
    gh_bin = Path(gh_bin_raw).resolve()
    git_bin = Path(git_bin_raw).resolve()
    github_identity = workflow._inspect_credential(
        python_bin=Path(sys.executable).resolve(),
        source_root=source_root,
        gh_config_dir=gh_config_dir,
        gh_bin=gh_bin,
        host="github.com",
        expected_account="psyberone",
    )
    github_deployment = workflow._github_deployment_sha256(source_root)
    expected_github_specs = _github_specs(
        state_root=state_root,
        account=github_identity["account"],
        credential_scope_sha256=github_identity["credential_scope_sha256"],
        base_deployment_sha256=github_deployment,
    )

    if fresh:
        github_specs = expected_github_specs
        github_source_root = source_root
        executions = _prepare_control_plane(
            control,
            state_root=state_root,
            targets=targets,
            revisions=revisions,
            worker_image=args.worker_image,
            validation_image_id=validation_image.image_id,
            connector_socket=connector_socket,
            connector_deployment_sha256=connector_deployment,
            credential_scope_sha256=session.credential_scope_sha256,
            github_specs=github_specs,
        )
    else:
        assert existing_executions is not None
        github_specs = _registered_github_specs(
            database,
            expected=expected_github_specs,
        )
        github_source_root = _resolve_github_source_root(
            project_root=project_root,
            current_source_root=source_root,
            state_root=state_root,
            current_commit=observed_controller["commit"],
            specs=github_specs,
        )
        executions = existing_executions

    connector = database.get_inference_connector(CONNECTOR_NAME)
    workflow._write_workflow_connector_config(
        connector_root / "config.json",
        connector_id=connector["id"],
        deployment_sha256=connector_deployment,
        scope_sha256=session.credential_scope_sha256,
        model=MODEL,
    )

    if args.prepare_only:
        result_store = WorkspaceResultStore(state_root / "results")
        report = _report(
            state_root=state_root,
            database=database,
            executions=executions,
            result_store=result_store,
            docker_engine=docker_engine,
            stop_reason="prepared_no_live_execution",
            fresh=fresh,
            controller_identity=controller_identity,
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    service_client_factory = _LostReceiptClientFactory(
        marker_root=state_root / LOST_RECEIPT_DIRECTORY,
        socket_projects={
            Path(spec.transport.socket_path): project_key
            for project_key, spec in github_specs.items()
        },
    )
    control = ControlPlane(database, service_client_factory=service_client_factory)
    supervisor, gateways, result_store, candidate_store = _build_supervisor(
        database=database,
        control=control,
        state_root=state_root,
        docker_engine=docker_engine,
        allow_unenforced_worker_storage=args.allow_unenforced_worker_storage,
    )

    def start_connector() -> subprocess.Popen[str]:
        workflow._clear_stale_socket(connector_socket)
        return _start_connector(
            project_root=project_root,
            profile_dir=profile_dir,
            config_path=connector_root / "config.json",
            socket_path=connector_socket,
            max_invocations=64,
        )

    connector_waves = _ConnectorWaves(
        database,
        start=start_connector,
        stop=_stop_connector,
    )
    connector_stdout = ""
    connector_stderr = ""
    github_processes: list[subprocess.Popen[bytes]] = []
    proof_error: BaseException | None = None
    stop_reason = "active"

    def remember_error(exc: BaseException, *, phase: str) -> None:
        nonlocal proof_error, stop_reason
        if proof_error is None:
            proof_error = exc
            stop_reason = f"error:{phase}:{type(exc).__name__}"
            return
        detail = _bounded_text(str(exc), limit=512) or "no detail"
        proof_error.add_note(f"secondary {phase} error: {type(exc).__name__}: {detail}")

    try:
        github_processes = _start_github_providers(
            database=database,
            specs=github_specs,
            candidate_store=candidate_store,
            state_root=state_root,
            source_root=github_source_root,
            gh_config_dir=gh_config_dir,
            gh_bin=gh_bin,
            git_bin=git_bin,
        )
        stop_reason = _drive_one_technical_stop(
            supervisor=supervisor,
            database=database,
            control=control,
            executions=executions,
            connector_waves=connector_waves,
            state_root=state_root,
        )
    except BaseException as exc:
        remember_error(exc, phase="execution")
    finally:
        try:
            supervisor.close(wait=True, timeout=300)
        except BaseException as exc:
            remember_error(exc, phase="supervisor_cleanup")
        try:
            gateways.close()
        except BaseException as exc:
            remember_error(exc, phase="gateway_cleanup")
        try:
            _stop_github_providers(github_processes)
        except BaseException as exc:
            remember_error(exc, phase="github_cleanup")
        try:
            connector_stdout, connector_stderr = connector_waves.close()
        except BaseException as exc:
            remember_error(exc, phase="connector_cleanup")

    try:
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
                "M18 state": _collect_regular_files(state_root),
                "worker output": "\n".join(docker_engine.logs_seen).encode(),
                "connector logs": (connector_stdout + connector_stderr).encode(),
                "sandbox plans": launch_plans,
            },
        )
        plans_text = launch_plans.decode("utf-8", "replace")
        if str(profile_dir) in plans_text or str(gh_config_dir) in plans_text:
            raise RuntimeError("credential path entered an M18 worker launch plan")
        for run in _all_runs(database, _execution_lineage(database, executions)):
            if docker_engine.managed_containers(run["id"]):
                raise RuntimeError("M18 proof left a managed worker container behind")
    except BaseException as exc:
        remember_error(exc, phase="security_verification")

    report: dict[str, Any] | None = None
    try:
        report = _report(
            state_root=state_root,
            database=database,
            executions=executions,
            result_store=result_store,
            docker_engine=docker_engine,
            stop_reason=stop_reason,
            fresh=fresh,
            controller_identity=controller_identity,
            proof_error=proof_error,
        )
        _append_observation(
            state_root,
            "invocation",
            "M18 coordinator invocation ended at a durable boundary",
            sequence=len(report["invocations"]),
            stop_reason=stop_reason,
            proof_error=None if proof_error is None else type(proof_error).__name__,
        )
        print(json.dumps(report, indent=2, sort_keys=True))
    except BaseException as exc:
        remember_error(exc, phase="reporting")
    if proof_error is not None:
        raise proof_error.with_traceback(proof_error.__traceback__)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
