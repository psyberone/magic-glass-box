#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from magic_glass_box.candidates import CandidateManifest, GitCandidateStore
from magic_glass_box.db import Database
from magic_glass_box.domain import AgentProfileSpec, ProjectContext, WorkItemEnvelope
from magic_glass_box.execution_operations import ExecutionRecipeAuthority
from magic_glass_box.execution_provider_cli import load_config
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.service_sidecar import ServiceSidecarSpec
from magic_glass_box.workspace import GitSnapshotter

MAX_CONFIG_BYTES = 32 * 1024
MAX_SCAN_FILE_BYTES = 64 * 1024 * 1024
PROJECT_KEY = "m15-live-proof"
PROJECT_EXTERNAL_ID = "magic-glass-box/magic-glass-box"
SIDECAR_NAME = "m15-forge"
MAPPING_NAME = "forge-quality"
OWNER_ID = "m15-live-proof"
PROOF_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
CONNECTION_FIELDS = {
    "schema_version",
    "ssh_binary",
    "git_binary",
    "host",
    "port",
    "account",
    "identity_file",
    "known_hosts_file",
    "remote_helper",
}
CAPABILITIES = (
    ("execution.submit", "effect"),
    ("execution.inspect", "read"),
    ("execution.cancel", "effect"),
    ("execution.collect", "read"),
)


class LiveProofError(RuntimeError):
    pass


def _exact_object(value: object, fields: set[str], subject: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise LiveProofError(f"{subject} is invalid")
    return value


def _read_private(path: Path, maximum: int, subject: str) -> bytes:
    try:
        info = path.lstat()
    except OSError as exc:
        raise LiveProofError(f"{subject} is unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_mode & 0o077
        or info.st_nlink != 1
        or (hasattr(os, "getuid") and info.st_uid != os.getuid())
    ):
        raise LiveProofError(f"{subject} must be a private regular file")
    try:
        with path.open("rb") as stream:
            value = stream.read(maximum + 1)
    except OSError as exc:
        raise LiveProofError(f"{subject} is unavailable") from exc
    if not value or len(value) > maximum:
        raise LiveProofError(f"{subject} is invalid")
    return value


def load_connection_config(path: Path) -> dict[str, Any]:
    raw = _read_private(path, MAX_CONFIG_BYTES, "connection configuration")
    try:
        document = json.loads(raw.decode("utf-8", "strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise LiveProofError("connection configuration is invalid") from exc
    config = _exact_object(document, CONNECTION_FIELDS, "connection configuration")
    if isinstance(config["schema_version"], bool) or config["schema_version"] != 1:
        raise LiveProofError("connection configuration is invalid")
    if (
        not all(
            isinstance(config[field], str) and config[field]
            for field in (
                "ssh_binary",
                "git_binary",
                "host",
                "account",
                "identity_file",
                "known_hosts_file",
                "remote_helper",
            )
        )
        or not all(
            Path(config[field]).is_absolute()
            for field in (
                "ssh_binary",
                "git_binary",
                "identity_file",
                "known_hosts_file",
                "remote_helper",
            )
        )
        or isinstance(config["port"], bool)
        or not isinstance(config["port"], int)
        or not 1 <= config["port"] <= 65535
    ):
        raise LiveProofError("connection configuration is invalid")
    return config


def write_private_json(destination: Path, value: Mapping[str, object]) -> None:
    try:
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(destination.parent, 0o700)
        if destination.exists() or destination.is_symlink():
            raise LiveProofError("private output already exists")
        raw = (
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        temporary = destination.parent / (
            f".{destination.name}.{os.getpid()}.{secrets.token_hex(8)}"
        )
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            view = memoryview(raw)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("short private write")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, destination)
        os.chmod(destination, 0o600)
    except LiveProofError:
        raise
    except OSError as exc:
        raise LiveProofError("private output could not be written") from exc


def _file_contains(path: Path, needle: bytes) -> bool:
    overlap = max(0, len(needle) - 1)
    seen = 0
    previous = b""
    try:
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(64 * 1024)
                if not chunk:
                    return False
                seen += len(chunk)
                if seen > MAX_SCAN_FILE_BYTES:
                    raise LiveProofError("containment scan file is oversized")
                combined = previous + chunk
                if needle in combined:
                    return True
                previous = combined[-overlap:] if overlap else b""
    except LiveProofError:
        raise
    except OSError as exc:
        raise LiveProofError("containment scan failed") from exc


def scan_forbidden(
    paths: Sequence[Path], forbidden: Mapping[str, bytes]
) -> tuple[str, ...]:
    found: set[str] = set()
    for label, needle in forbidden.items():
        if not isinstance(label, str) or not label or not isinstance(needle, bytes):
            raise LiveProofError("containment scan input is invalid")
        if not needle:
            continue
        for path in paths:
            try:
                info = path.lstat()
            except OSError as exc:
                raise LiveProofError("containment scan input is unavailable") from exc
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                raise LiveProofError("containment scan input is invalid")
            if _file_contains(path, needle):
                found.add(label)
                break
    return tuple(sorted(found))


def success_result(operation: Mapping[str, object]) -> dict[str, Any]:
    attempts = operation.get("attempts")
    if operation.get("state") != "succeeded" or not isinstance(attempts, list):
        raise LiveProofError("service operation has no successful result")
    results: list[dict[str, Any]] = []
    for attempt in attempts:
        if not isinstance(attempt, dict) or attempt.get("outcome") != "succeeded":
            continue
        receipt = attempt.get("receipt")
        if isinstance(receipt, dict) and isinstance(receipt.get("result"), dict):
            results.append(dict(receipt["result"]))
    if len(results) != 1:
        raise LiveProofError("service operation has no successful result")
    return results[0]


def _git(repository: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(  # noqa: S603 - executable and arguments are fixed.
            ["/usr/bin/git", "-C", str(repository), *arguments],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=True,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise LiveProofError("Git proof input is unavailable") from exc
    try:
        return completed.stdout.decode("utf-8", "strict").strip()
    except UnicodeDecodeError as exc:
        raise LiveProofError("Git proof input is invalid") from exc


def _prepare_candidate(
    repository: Path, proof_root: Path
) -> tuple[ControlPlane, dict[str, Any], GitCandidateStore, tuple[Path, ...]]:
    proof_revision = _git(repository, "rev-parse", "HEAD")
    source_revision = _git(repository, "rev-parse", "HEAD^")
    source_tree = _git(repository, "rev-parse", "HEAD^{tree}")
    if _git(repository, "status", "--porcelain"):
        raise LiveProofError("proof repository must be clean")
    database = Database(proof_root / "control-plane.db")
    database.initialize()
    control = ControlPlane(database)
    user = database.create_user("operator", "M15 Live Proof Operator")
    instance = database.create_krab_instance(
        "M15 isolated authority", "http://127.0.0.1:8765"
    )
    identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity("usr_" + "1" * 32, "m15-worker", 1),
        "file:/nonexistent/m15-live-proof-token",
        user_id=user["id"],
    )
    database.create_project(
        instance_id=instance["id"],
        krab_project_id="prj_" + "2" * 32,
        krab_project_key=PROJECT_KEY,
        display_name="M15 Live Proof",
        context=ProjectContext.from_mapping(
            {
                "purpose": "Prove controlled remote execution",
                "repository_path": str(repository),
            }
        ),
        created_by=user["id"],
    )
    work_item = database.import_work_item(
        project_key=PROJECT_KEY,
        envelope=WorkItemEnvelope.from_mapping(
            {
                "schema_version": 1,
                "source": "manual",
                "external_id": "m15-live-proof",
                "source_updated_at": "2026-10-28T00:00:00Z",
                "title": "Prove controlled remote execution",
                "brief": "Exercise exact-candidate remote validation and recovery.",
            }
        ),
        imported_by=user["id"],
    )
    database.create_agent_profile(
        name="M15 Proof Worker",
        spec=AgentProfileSpec.from_mapping(
            {"image": "m15-proof:authority-only", "command": ["m15-proof"]}
        ),
        krab_identity_id=identity["id"],
        created_by=user["id"],
    )
    run = database.create_run(
        project_key=PROJECT_KEY,
        profile_name="M15 Proof Worker",
        work_item_id=work_item["id"],
        requested_by=user["id"],
        source_revision=source_revision,
    )
    preparing = database.begin_run_attempt(
        run["id"],
        runtime_adapter="command",
        owner_id=OWNER_ID,
        expected_run_version=run["version"],
    )
    attempt = database.start_run_attempt(
        preparing["id"],
        owner_id=OWNER_ID,
        expected_attempt_version=preparing["version"],
        expected_run_version=preparing["run_version"],
        source_revision=source_revision,
        container_id="a" * 64,
        container_name="m15-live-proof",
        image_id="sha256:" + "b" * 64,
    )
    workspace = proof_root / "candidate-workspace"
    workspace.mkdir(mode=0o700)
    GitSnapshotter().snapshot(repository, source_revision, workspace)
    result_store = WorkspaceResultStore(proof_root / "results")
    result_store.stage(attempt["id"], workspace, source_revision)
    shutil.rmtree(workspace)
    workspace.mkdir(mode=0o700)
    GitSnapshotter().snapshot(repository, proof_revision, workspace)
    captured = result_store.capture(attempt["id"], workspace)
    database.record_attempt_result(attempt["id"], captured.manifest)
    database.complete_run_attempt(
        attempt["id"],
        owner_id=OWNER_ID,
        expected_attempt_version=attempt["version"],
        exit_code=0,
        image_id="sha256:" + "b" * 64,
    )
    candidate_store = GitCandidateStore(proof_root / "candidates")
    candidate = control.create_candidate(
        attempt["id"],
        created_by_handle="operator",
        result_store=result_store,
        candidate_store=candidate_store,
    )
    manifest = CandidateManifest.from_mapping(candidate["manifest"])
    if (
        manifest.tree_revision != source_tree
        or manifest.base_revision != source_revision
    ):
        raise LiveProofError("durable candidate does not match the clean proof commit")
    scan_paths = tuple(
        path
        for path in workspace.rglob("*")
        if path.is_file() and not path.is_symlink()
    )
    return control, candidate, candidate_store, scan_paths


def _write_drop_wrapper(destination: Path, *, ssh_binary: str, marker: Path) -> None:
    script = "\n".join(
        (
            "#!/bin/sh",
            "set -eu",
            f"marker={shlex.quote(str(marker))}",
            'if [ ! -e "$marker" ]; then',
            "  set +e",
            f'  {shlex.quote(ssh_binary)} "$@" >/dev/null',
            "  status=$?",
            "  set -e",
            '  if [ "$status" -eq 0 ]; then',
            '    (umask 077; : > "$marker")',
            "    exit 255",
            "  fi",
            '  exit "$status"',
            "fi",
            f'exec {shlex.quote(ssh_binary)} "$@"',
            "",
        )
    ).encode("utf-8")
    try:
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700)
        try:
            view = memoryview(script)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("short wrapper write")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise LiveProofError("lost-response shim could not be written") from exc


def _build_authority(
    *,
    connection: Mapping[str, Any],
    proof_root: Path,
    socket_path: Path,
    candidate_repository: Path,
    source_revision: str,
    image_id: str,
) -> tuple[dict[str, Any], ServiceSidecarSpec, str, Path]:
    marker = proof_root / "submit-response-discarded"
    wrapper = proof_root / "ssh-drop-first-response"
    _write_drop_wrapper(
        wrapper, ssh_binary=str(connection["ssh_binary"]), marker=marker
    )
    provider_config = dict(connection)
    provider_config["ssh_binary"] = str(wrapper)
    provider_config["repositories"] = {PROJECT_EXTERNAL_ID: str(candidate_repository)}
    config_path = proof_root / "execution-provider.json"
    write_private_json(config_path, provider_config)
    loaded = load_config(config_path)
    deployment_sha256 = hashlib.sha256(
        f"m15-live:{source_revision}:{image_id}".encode()
    ).hexdigest()
    spec = ServiceSidecarSpec.from_mapping(
        {
            "schema_version": 2,
            "connector_protocol_version": 2,
            "deployment_sha256": deployment_sha256,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "capabilities": [
                {"name": name, "version": 1, "mode": mode}
                for name, mode in CAPABILITIES
            ],
            "mappings": [
                {
                    "name": MAPPING_NAME,
                    "service": "ssh-runner",
                    "host": connection["host"],
                    "resource": {
                        "kind": "repository",
                        "external_id": PROJECT_EXTERNAL_ID,
                    },
                    "account": connection["account"],
                    "credential_scope_sha256": loaded.credential_scope_sha256,
                }
            ],
            "request_bytes": 131072,
            "response_bytes": 4194304,
            "timeout_ms": 30000,
            "max_concurrency": 1,
            "max_requests_per_minute": 60,
        }
    )
    spec_path = proof_root / "execution-sidecar.json"
    write_private_json(spec_path, spec.to_dict())
    return provider_config, spec, loaded.credential_scope_sha256, marker


def _start_provider(
    repository: Path,
    proof_root: Path,
    config_path: Path,
    spec_path: Path,
    socket_path: Path,
) -> tuple[subprocess.Popen[bytes], tuple[Any, Any]]:
    stdout = (proof_root / "provider.stdout").open("xb")
    stderr = (proof_root / "provider.stderr").open("xb")
    environment = {
        "HOME": str(proof_root),
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": os.defpath,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(repository / "src"),
        "PYTHONUNBUFFERED": "1",
    }
    try:
        process = subprocess.Popen(  # noqa: S603 - fixed local provider entrypoint.
            [
                sys.executable,
                "-m",
                "magic_glass_box.execution_provider_cli",
                "serve",
                "--config",
                str(config_path),
                "--spec",
                str(spec_path),
                "--socket-path",
                str(socket_path),
                "--max-invocations",
                "64",
            ],
            cwd=repository,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
        )
    except OSError as exc:
        stdout.close()
        stderr.close()
        raise LiveProofError("execution provider could not start") from exc
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if socket_path.exists():
            return process, (stdout, stderr)
        if process.poll() is not None:
            stdout.close()
            stderr.close()
            raise LiveProofError("execution provider exited during startup")
        time.sleep(0.05)
    process.terminate()
    process.wait(timeout=5)
    stdout.close()
    stderr.close()
    raise LiveProofError("execution provider socket did not become ready")


def _bind_authority(control: ControlPlane, spec: ServiceSidecarSpec) -> None:
    control.register_service_sidecar(
        name=SIDECAR_NAME, spec=spec, created_by_handle="operator"
    )
    for capability, _mode in CAPABILITIES:
        control.bind_project_capability(
            project_key=PROJECT_KEY,
            capability_name=capability,
            capability_version=1,
            sidecar_name=SIDECAR_NAME,
            mapping_name=MAPPING_NAME,
            bound_by_handle="operator",
        )


def _followup(
    control: ControlPlane,
    submission_id: str,
    *,
    action: str,
    request_key: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    operation = control.create_execution_followup(
        submission_operation_id=submission_id,
        action=action,
        request_key=request_key,
        created_by_handle="operator",
    )
    completed = control.execute_service_operation(
        operation["id"], owner_id=OWNER_ID, expected_version=operation["version"]
    )
    return completed, success_result(completed)


def _wait_terminal(
    control: ControlPlane,
    submission_id: str,
    *,
    prefix: str,
    timeout: float = 120,
) -> tuple[dict[str, Any], list[str]]:
    deadline = time.monotonic() + timeout
    operation_ids: list[str] = []
    count = 0
    while time.monotonic() < deadline:
        count += 1
        operation, result = _followup(
            control,
            submission_id,
            action="inspect",
            request_key=f"{prefix}-{count:04d}",
        )
        operation_ids.append(operation["id"])
        if result.get("state") in {"succeeded", "failed", "cancelled"}:
            return result, operation_ids
        time.sleep(0.25)
    raise LiveProofError("remote execution did not become terminal")


def _submit(
    control: ControlPlane,
    candidate_id: str,
    recipe: ExecutionRecipeAuthority,
    request_key: str,
) -> dict[str, Any]:
    return control.create_execution_submission(
        project_key=PROJECT_KEY,
        candidate_id=candidate_id,
        recipe=recipe,
        request_key=request_key,
        created_by_handle="operator",
    )


def _assert_evidence(
    result: Mapping[str, Any], *, expected_state: str, expect_artifact: bool
) -> dict[str, Any]:
    execution = result.get("execution")
    if not isinstance(execution, dict) or execution.get("state") != expected_state:
        raise LiveProofError("remote evidence has the wrong terminal state")
    bundle = result.get("artifact_bundle")
    if not isinstance(bundle, str):
        raise LiveProofError("remote evidence artifact is invalid")
    try:
        raw_bundle = base64.b64decode(bundle, validate=True) if bundle else b""
    except ValueError as exc:
        raise LiveProofError("remote evidence artifact is invalid") from exc
    artifact_count = result.get("artifact_count")
    artifact_bytes = result.get("artifact_bytes")
    artifact_sha256 = result.get("artifact_sha256")
    if (
        isinstance(artifact_count, bool)
        or not isinstance(artifact_count, int)
        or isinstance(artifact_bytes, bool)
        or not isinstance(artifact_bytes, int)
        or not isinstance(artifact_sha256, str)
        or hashlib.sha256(raw_bundle).hexdigest() != artifact_sha256
        or len(raw_bundle) != artifact_bytes
        or artifact_bytes <= 0
        or (expect_artifact and artifact_count != 1)
        or (not expect_artifact and artifact_count != 0)
    ):
        raise LiveProofError("remote evidence artifact identity is inconsistent")
    return dict(execution)


def _proof_report(
    *,
    proof_id: str,
    source_revision: str,
    source_tree: str,
    candidate: Mapping[str, Any],
    image_id: str,
    quality_recipe: ExecutionRecipeAuthority,
    cancel_recipe: ExecutionRecipeAuthority,
    scope_sha256: str,
    quality_submission: Mapping[str, Any],
    quality_execution: Mapping[str, Any],
    quality_collect: Mapping[str, Any],
    cancel_submission: Mapping[str, Any],
    cancel_execution: Mapping[str, Any],
    cancel_collect: Mapping[str, Any],
    inspect_operations: Sequence[str],
) -> dict[str, Any]:
    manifest = CandidateManifest.from_mapping(candidate["manifest"])
    first_attempts = quality_submission.get("attempts")
    if (
        not isinstance(first_attempts, list)
        or len(first_attempts) != 2
        or [attempt.get("outcome") for attempt in first_attempts]
        != ["uncertain", "succeeded"]
    ):
        raise LiveProofError("lost-response operation did not reconcile exactly once")
    quality_result = success_result(quality_submission)
    if quality_result.get("execution_id") != quality_execution.get("execution_id"):
        raise LiveProofError("quality reconciliation changed remote execution identity")
    return {
        "candidate": {
            "base_revision": manifest.base_revision,
            "candidate_id": candidate["id"],
            "head_revision": manifest.head_revision,
            "object_format": manifest.object_format,
            "tree_revision": manifest.tree_revision,
        },
        "cancellation": {
            "collect_operation_id": cancel_collect["id"],
            "execution_id": cancel_execution["execution_id"],
            "state": cancel_execution["state"],
            "submission_operation_id": cancel_submission["id"],
        },
        "containment": {
            "credential_scan": "passed",
            "docker_credentials": "carrier-owned",
            "forced_command": "preflight-passed",
            "worker_credential_material": "absent",
        },
        "credential_scope_sha256": scope_sha256,
        "image_id": image_id,
        "lost_response": {
            "attempt_outcomes": ["uncertain", "succeeded"],
            "execution_id": quality_execution["execution_id"],
            "reconciled_without_duplicate": True,
            "submission_operation_id": quality_submission["id"],
        },
        "proof_id": proof_id,
        "quality": {
            "artifact_bytes": success_result(quality_collect)["artifact_bytes"],
            "artifact_count": success_result(quality_collect)["artifact_count"],
            "collect_operation_id": quality_collect["id"],
            "execution_id": quality_execution["execution_id"],
            "inspect_operation_ids": list(inspect_operations),
            "state": quality_execution["state"],
        },
        "recipes": {
            "cancel": cancel_recipe.to_dict(),
            "quality": quality_recipe.to_dict(),
        },
        "schema_version": 1,
        "source_commit": source_revision,
        "source_tree": source_tree,
        "status": "passed",
    }


def run_proof(args: argparse.Namespace) -> dict[str, Any]:
    if args.confirmation != "I_UNDERSTAND_THIS_USES_FORGE":
        raise LiveProofError("live Forge proof requires the explicit confirmation")
    if PROOF_ID_RE.fullmatch(args.proof_id) is None:
        raise LiveProofError("proof ID is invalid")
    if IMAGE_ID_RE.fullmatch(args.image_id) is None:
        raise LiveProofError("image ID is invalid")
    for digest in (args.quality_recipe_sha256, args.cancel_recipe_sha256):
        if SHA256_RE.fullmatch(digest) is None:
            raise LiveProofError("recipe digest is invalid")
    repository = args.repository.expanduser().resolve(strict=True)
    proof_root = args.proof_root.expanduser().absolute()
    if proof_root.exists() or proof_root.is_symlink():
        raise LiveProofError("proof root already exists")
    proof_root.mkdir(mode=0o700, parents=True)
    os.chmod(proof_root, 0o700)
    connection = load_connection_config(args.connection_config.expanduser().absolute())
    source_revision = _git(repository, "rev-parse", "HEAD")
    source_tree = _git(repository, "rev-parse", "HEAD^{tree}")
    control, candidate, candidate_store, worker_files = _prepare_candidate(
        repository, proof_root
    )
    manifest = CandidateManifest.from_mapping(candidate["manifest"])
    candidate_repository = candidate_store.repository_path(
        manifest.project_id, manifest.object_format
    )
    quality_recipe = ExecutionRecipeAuthority.from_mapping(
        {
            "recipe_id": "m15-proof",
            "recipe_sha256": args.quality_recipe_sha256,
            "image_id": args.image_id,
        }
    )
    cancel_recipe = ExecutionRecipeAuthority.from_mapping(
        {
            "recipe_id": "cancel-proof",
            "recipe_sha256": args.cancel_recipe_sha256,
            "image_id": args.image_id,
        }
    )
    process: subprocess.Popen[bytes] | None = None
    streams: tuple[Any, Any] = ()
    with tempfile.TemporaryDirectory(prefix="mgb15-") as socket_directory:
        socket_root = Path(socket_directory)
        socket_root.chmod(0o700)
        socket_path = socket_root / "provider.sock"
        provider_config, spec, scope_sha256, marker = _build_authority(
            connection=connection,
            proof_root=proof_root,
            socket_path=socket_path,
            candidate_repository=candidate_repository,
            source_revision=source_revision,
            image_id=args.image_id,
        )
        _bind_authority(control, spec)
        process, streams = _start_provider(
            repository,
            proof_root,
            proof_root / "execution-provider.json",
            proof_root / "execution-sidecar.json",
            socket_path,
        )
        try:
            quality = _submit(
                control, candidate["id"], quality_recipe, f"{args.proof_id}-quality"
            )
            uncertain = control.execute_service_operation(
                quality["id"],
                owner_id=OWNER_ID,
                expected_version=quality["version"],
            )
            if uncertain["state"] != "uncertain" or not marker.is_file():
                raise LiveProofError("submit response was not discarded after dispatch")
            quality = control.reconcile_service_operation(
                quality["id"],
                owner_id=OWNER_ID,
                expected_version=uncertain["version"],
            )
            if quality["state"] != "succeeded":
                raise LiveProofError("uncertain submission did not reconcile")
            quality_execution, inspect_ids = _wait_terminal(
                control, quality["id"], prefix=f"{args.proof_id}-quality-inspect"
            )
            if quality_execution["state"] != "succeeded":
                raise LiveProofError("remote quality recipe did not succeed")
            quality_collect, quality_evidence = _followup(
                control,
                quality["id"],
                action="collect",
                request_key=f"{args.proof_id}-quality-evidence",
            )
            _assert_evidence(
                quality_evidence, expected_state="succeeded", expect_artifact=True
            )

            cancel_submission = _submit(
                control,
                candidate["id"],
                cancel_recipe,
                f"{args.proof_id}-cancel",
            )
            cancel_submission = control.execute_service_operation(
                cancel_submission["id"],
                owner_id=OWNER_ID,
                expected_version=cancel_submission["version"],
            )
            cancel_started = success_result(cancel_submission)
            if cancel_started["state"] not in {"queued", "running"}:
                raise LiveProofError("cancellation recipe was not active")
            cancel_operation, cancel_execution = _followup(
                control,
                cancel_submission["id"],
                action="cancel",
                request_key=f"{args.proof_id}-cancel-request",
            )
            if cancel_execution["state"] != "cancelled":
                raise LiveProofError("remote cancellation was not terminal")
            cancel_collect, cancel_evidence = _followup(
                control,
                cancel_submission["id"],
                action="collect",
                request_key=f"{args.proof_id}-cancel-evidence",
            )
            _assert_evidence(
                cancel_evidence, expected_state="cancelled", expect_artifact=False
            )
            if cancel_operation["state"] != "succeeded":
                raise LiveProofError("cancellation operation did not succeed")

            identity_path = Path(provider_config["identity_file"])
            known_hosts_path = Path(provider_config["known_hosts_file"])
            forbidden = {
                "identity_file_bytes": _read_private(
                    identity_path, 4 * 1024 * 1024, "SSH identity file"
                ),
                "identity_file_path": str(identity_path).encode(),
                "known_hosts_bytes": _read_private(
                    known_hosts_path, 4 * 1024 * 1024, "SSH known-hosts file"
                ),
                "known_hosts_path": str(known_hosts_path).encode(),
            }
            database_files = tuple(
                path
                for path in proof_root.glob("control-plane.db*")
                if path.is_file() and not path.is_symlink()
            )
            scan_paths = (
                *database_files,
                *worker_files,
                proof_root / "provider.stdout",
                proof_root / "provider.stderr",
            )
            found = scan_forbidden(scan_paths, forbidden)
            if found:
                raise LiveProofError(
                    "credential material crossed the carrier boundary: "
                    + ",".join(found)
                )
            report = _proof_report(
                proof_id=args.proof_id,
                source_revision=source_revision,
                source_tree=source_tree,
                candidate=candidate,
                image_id=args.image_id,
                quality_recipe=quality_recipe,
                cancel_recipe=cancel_recipe,
                scope_sha256=scope_sha256,
                quality_submission=quality,
                quality_execution=quality_execution,
                quality_collect=quality_collect,
                cancel_submission=cancel_submission,
                cancel_execution=cancel_execution,
                cancel_collect=cancel_collect,
                inspect_operations=inspect_ids,
            )
            report_path = proof_root / "report.json"
            write_private_json(report_path, report)
            report_found = scan_forbidden((report_path,), forbidden)
            if report_found:
                raise LiveProofError("credential material entered the proof report")
            return report
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            for stream in streams:
                stream.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the explicitly authorized M15 Forge execution proof"
    )
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--proof-root", required=True, type=Path)
    parser.add_argument("--proof-id", required=True)
    parser.add_argument("--connection-config", required=True, type=Path)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--quality-recipe-sha256", required=True)
    parser.add_argument("--cancel-recipe-sha256", required=True)
    parser.add_argument(
        "--confirmation",
        required=True,
        help="must be I_UNDERSTAND_THIS_USES_FORGE",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = run_proof(args)
    except (LiveProofError, ValueError) as exc:
        print(f"M15 live remote proof failed: {exc}", file=sys.stderr)
        return 2
    except (OSError, subprocess.SubprocessError):
        print("M15 live remote proof failed: operation failed", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
