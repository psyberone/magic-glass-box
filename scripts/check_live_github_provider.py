#!/usr/bin/env python3
"""Run the destructive, opt-in live proof for the GitHub reference provider."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import shutil
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from magic_glass_box.domain import canonical_json, content_sha256
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceOperationSpec,
    ServiceSidecarError,
    ServiceSidecarSpec,
    UnixServiceSidecarClient,
)

ACKNOWLEDGEMENT_FLAG = "--i-understand-this-publishes-a-branch-opens-and-merges-a-pr"
PROTECTED_REPOSITORIES = frozenset({"psyberone/magic-glass-box"})
REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PROOF_ID_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?$")
ACCOUNT_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})?$")
HOST_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$")
REQUEST_BYTES = 128 * 1024
RESPONSE_BYTES = 512 * 1024
TIMEOUT_MS = 120_000
PROVIDER_MAX_INVOCATIONS = 32
PROVIDER_START_SECONDS = 15.0
PROVIDER_STOP_SECONDS = 10.0


@dataclass(frozen=True)
class CandidateArtifact:
    repository: Path
    revision: str
    proof_path: str


@dataclass(frozen=True)
class ProofEvidence:
    host: str
    repository: str
    account: str
    credential_scope_sha256: str
    base_ref: str
    expected_base_revision: str
    candidate_revision: str
    branch_ref: str
    work_items_observed: int
    pull_number: int
    change_remote_identity: str
    change_head_revision: str
    change_list_contains_pull: bool
    checks_revision: str
    landed_revision: str
    landing_remote_identity: str
    landing_closed_references: tuple[str, ...]
    outcomes: Mapping[str, str]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gh-config-dir", required=True, type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--source-repository", required=True, type=Path)
    parser.add_argument("--expected-base-revision", required=True)
    parser.add_argument("--base-ref", default="refs/heads/main")
    parser.add_argument("--proof-id", required=True)
    parser.add_argument("--expected-account", required=True)
    parser.add_argument("--host", default="github.com")
    parser.add_argument(
        "--merge-method", choices=("merge", "rebase", "squash"), default="squash"
    )
    parser.add_argument(
        ACKNOWLEDGEMENT_FLAG,
        action="store_true",
        dest="acknowledged",
        help="Acknowledge that the proof leaves a merged pull request and branch.",
    )
    args = parser.parse_args(argv)
    if not args.acknowledged:
        raise ValueError("the destructive live-proof acknowledgment is required")
    if REPOSITORY_RE.fullmatch(args.repository) is None or any(
        part in {".", ".."} for part in args.repository.split("/", 1)
    ):
        raise ValueError("repository is invalid")
    if args.repository.casefold() in PROTECTED_REPOSITORIES:
        raise ValueError("the protected Magic Glass Box repository cannot be targeted")
    if SHA1_RE.fullmatch(args.expected_base_revision) is None:
        raise ValueError("expected base revision must be a lowercase SHA-1")
    if not _valid_branch_ref(args.base_ref):
        raise ValueError("base ref must be a fully qualified branch")
    if PROOF_ID_RE.fullmatch(args.proof_id) is None:
        raise ValueError("proof ID must be a short lowercase slug")
    if ACCOUNT_RE.fullmatch(args.expected_account) is None:
        raise ValueError("expected account is invalid")
    if HOST_RE.fullmatch(args.host) is None:
        raise ValueError("GitHub host is invalid")
    for path, subject in (
        (args.gh_config_dir, "GH_CONFIG_DIR"),
        (args.source_repository, "source repository"),
    ):
        if not path.is_absolute():
            raise ValueError(f"{subject} must be absolute")
        try:
            info = path.lstat()
        except OSError as exc:
            raise ValueError(f"{subject} is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"{subject} must be a real directory")
    return args


def _valid_branch_ref(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("refs/heads/"):
        return False
    branch = value.removeprefix("refs/heads/")
    components = branch.split("/")
    return bool(
        branch
        and len(value.encode("utf-8")) <= 256
        and not branch.startswith("/")
        and not branch.endswith(("/", "."))
        and not any(
            component.startswith(".") or component.endswith(".lock")
            for component in components
        )
        and ".." not in branch
        and "//" not in branch
        and "@{" not in branch
        and not any(character in " ~^:?*[\\" for character in branch)
        and not any(
            ord(character) < 0x20 or ord(character) == 0x7F for character in branch
        )
    )


def _provider_environment(source_root: Path) -> dict[str, str]:
    return {
        "PATH": os.environ.get("PATH", os.defpath),
        "PYTHONPATH": str(source_root),
        "PYTHONUNBUFFERED": "1",
    }


def _git_environment(*, index_path: Path | None = None) -> dict[str, str]:
    environment = {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "PATH": os.environ.get("PATH", os.defpath),
    }
    if index_path is not None:
        environment["GIT_INDEX_FILE"] = str(index_path)
    return environment


def _run_git(
    git_bin: Path,
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    stdin: bytes | None = None,
    environment: Mapping[str, str] | None = None,
    output_limit: int = 1024 * 1024,
) -> bytes:
    try:
        result = subprocess.run(  # noqa: S603
            [str(git_bin), *argv],
            cwd=cwd,
            input=stdin,
            capture_output=True,
            env=dict(environment or _git_environment()),
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("Git operation failed") from exc
    if (
        result.returncode != 0
        or len(result.stdout) > output_limit
        or len(result.stderr) > output_limit
    ):
        raise RuntimeError("Git operation failed")
    return result.stdout.strip()


def _accepted_remote_urls(host: str, repository: str) -> frozenset[str]:
    suffix = repository.removesuffix(".git")
    return frozenset(
        {
            f"https://{host}/{suffix}",
            f"https://{host}/{suffix}.git",
            f"ssh://git@{host}/{suffix}",
            f"ssh://git@{host}/{suffix}.git",
            f"git@{host}:{suffix}",
            f"git@{host}:{suffix}.git",
        }
    )


def _verify_source_repository(
    source_repository: Path,
    *,
    expected_base_revision: str,
    base_ref: str,
    host: str,
    repository: str,
    git_bin: Path,
) -> Path:
    candidate = Path(source_repository)
    try:
        original = candidate.lstat()
        resolved = candidate.resolve(strict=True)
        info = resolved.stat()
    except OSError as exc:
        raise RuntimeError("source repository is unavailable") from exc
    if stat.S_ISLNK(original.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise RuntimeError("source repository must be a real directory")
    if (
        _run_git(git_bin, ("rev-parse", "--is-inside-work-tree"), cwd=resolved)
        != b"true"
    ):
        raise RuntimeError("source repository is not a Git work tree")
    status = _run_git(
        git_bin,
        ("status", "--porcelain=v1", "--untracked-files=all"),
        cwd=resolved,
    )
    if status:
        raise RuntimeError("source repository must be clean")
    head = _run_git(git_bin, ("rev-parse", "--verify", "HEAD^{commit}"), cwd=resolved)
    base = _run_git(
        git_bin,
        ("rev-parse", "--verify", f"{base_ref}^{{commit}}"),
        cwd=resolved,
    )
    if head.decode("ascii") != expected_base_revision or head != base:
        raise RuntimeError("source repository does not match the expected base")
    remote = _run_git(git_bin, ("remote", "get-url", "origin"), cwd=resolved)
    try:
        remote_text = remote.decode("utf-8", "strict").rstrip("/")
    except UnicodeDecodeError as exc:
        raise RuntimeError("source repository remote is invalid") from exc
    accepted = {item.casefold() for item in _accepted_remote_urls(host, repository)}
    if remote_text.casefold() not in accepted:
        raise RuntimeError("source repository remote does not match the target")
    return resolved


def _build_candidate(
    *,
    candidate_root: Path,
    source_repository: Path,
    expected_base_revision: str,
    proof_id: str,
    git_bin: Path,
) -> CandidateArtifact:
    root = Path(candidate_root).resolve(strict=True)
    repository = root / "projects" / proof_id / "repository.git"
    repository.parent.mkdir(mode=0o700, parents=True)
    repository.parent.chmod(0o700)
    _run_git(git_bin, ("init", "--bare", str(repository)))
    _run_git(
        git_bin,
        (
            f"--git-dir={repository}",
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            str(source_repository),
            expected_base_revision,
        ),
    )
    _run_git(
        git_bin,
        (
            f"--git-dir={repository}",
            "cat-file",
            "-e",
            f"{expected_base_revision}^{{commit}}",
        ),
    )
    proof_path = f"docs/mgb-live-proof-{proof_id}.md"
    proof = (
        "# Magic Glass Box live GitHub proof\n\n"
        "This audit artifact was produced by the opt-in M11 reference-provider "
        "proof.\n\n"
        f"- Proof ID: `{proof_id}`\n"
        f"- Authorized base: `{expected_base_revision}`\n"
        "- Scope: bounded branch publication, pull request creation, checks, "
        "landing, and lost-response reconciliation\n"
    ).encode()
    blob = _run_git(
        git_bin,
        (f"--git-dir={repository}", "hash-object", "-w", "--stdin"),
        stdin=proof,
    ).decode("ascii")
    index_path = root / f"candidate-{proof_id}.index"
    environment = _git_environment(index_path=index_path)
    try:
        _run_git(
            git_bin,
            (f"--git-dir={repository}", "read-tree", expected_base_revision),
            environment=environment,
        )
        _run_git(
            git_bin,
            (
                f"--git-dir={repository}",
                "update-index",
                "--add",
                "--cacheinfo",
                f"100644,{blob},{proof_path}",
            ),
            environment=environment,
        )
        tree = _run_git(
            git_bin,
            (f"--git-dir={repository}", "write-tree"),
            environment=environment,
        ).decode("ascii")
    finally:
        index_path.unlink(missing_ok=True)
    commit_environment = _git_environment()
    commit_environment.update(
        {
            "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z",
            "GIT_AUTHOR_EMAIL": "mgb-live-proof@example.invalid",
            "GIT_AUTHOR_NAME": "Magic Glass Box",
            "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
            "GIT_COMMITTER_EMAIL": "mgb-live-proof@example.invalid",
            "GIT_COMMITTER_NAME": "Magic Glass Box",
        }
    )
    revision = _run_git(
        git_bin,
        (
            f"--git-dir={repository}",
            "commit-tree",
            tree,
            "-p",
            expected_base_revision,
        ),
        stdin=f"MGB live GitHub proof {proof_id}\n".encode(),
        environment=commit_environment,
    ).decode("ascii")
    if SHA1_RE.fullmatch(revision) is None or revision == expected_base_revision:
        raise RuntimeError("candidate revision is invalid")
    candidate_ref = f"refs/mgb/candidates/{revision}"
    _run_git(
        git_bin,
        (f"--git-dir={repository}", "update-ref", candidate_ref, revision),
    )
    verified = _run_git(
        git_bin,
        (f"--git-dir={repository}", "show-ref", "--verify", "--hash", candidate_ref),
    ).decode("ascii")
    if verified != revision:
        raise RuntimeError("candidate ref could not be verified")
    return CandidateArtifact(repository.resolve(), revision, proof_path)


def _deployment_sha256(source_root: Path) -> str:
    digest = hashlib.sha256()
    package_root = source_root / "magic_glass_box"
    files = sorted(package_root.glob("*.py"))
    if not files:
        raise RuntimeError("provider source is unavailable")
    for path in files:
        raw = path.read_bytes()
        digest.update(path.name.encode("utf-8") + b"\0")
        digest.update(len(raw).to_bytes(8, "big") + raw)
    return digest.hexdigest()


def _service_sidecar_spec(
    *,
    socket_path: Path,
    host: str = "github.com",
    repository: str,
    account: str,
    credential_scope_sha256: str,
    deployment_sha256: str,
) -> ServiceSidecarSpec:
    capabilities = (
        ("work.list", "read"),
        ("change.get", "read"),
        ("change.list", "read"),
        ("refs.publish", "effect"),
        ("change.open", "effect"),
        ("checks.read", "read"),
        ("landing.invoke", "effect"),
    )
    return ServiceSidecarSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_protocol_version": 1,
            "deployment_sha256": deployment_sha256,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "capabilities": [
                {"name": name, "version": 1, "mode": mode}
                for name, mode in capabilities
            ],
            "mappings": [
                {
                    "name": "primary",
                    "service": "github",
                    "host": host,
                    "repository": repository,
                    "account": account,
                    "credential_scope_sha256": credential_scope_sha256,
                }
            ],
            "request_bytes": REQUEST_BYTES,
            "response_bytes": RESPONSE_BYTES,
            "timeout_ms": TIMEOUT_MS,
            "max_concurrency": 1,
            "max_requests_per_minute": 60,
        }
    )


def _operation_request(
    *,
    spec: ServiceSidecarSpec,
    proof_id: str,
    capability: str,
    phase: str,
    resource_id: str,
    object_format: str | None,
    expected_revision: str | None,
    desired_revision: str | None,
    parameters: Mapping[str, Any],
) -> ServiceOperationRequest:
    selected = next(
        (item for item in spec.capabilities if item.name == capability), None
    )
    if selected is None:
        raise RuntimeError("live-proof capability is not registered")
    operation = ServiceOperationSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_name": "github-live-proof",
            "deployment_sha256": spec.deployment_sha256,
            "mapping": spec.mappings[0].to_dict(),
            "capability": selected.to_dict(),
            "idempotency_key": f"live:{proof_id}:{capability}",
            "target": {
                "repository": spec.mappings[0].repository,
                "resource_id": resource_id,
                "object_format": object_format,
                "expected_revision": expected_revision,
                "desired_revision": desired_revision,
            },
            "parameters": dict(parameters),
            "request_bytes": REQUEST_BYTES,
            "response_bytes": RESPONSE_BYTES,
            "timeout_ms": TIMEOUT_MS,
        }
    )
    spec.validate_operation("github-live-proof", operation)
    operation_id = (
        "sop_"
        + content_sha256(
            canonical_json({"capability": capability, "proof_id": proof_id})
        )[:32]
    )
    connector_id = "ssc_" + content_sha256(spec.canonical)[:32]
    deadline = (datetime.now(UTC) + timedelta(milliseconds=TIMEOUT_MS)).isoformat(
        timespec="microseconds"
    )
    return ServiceOperationRequest.create(
        operation_id=operation_id,
        connector_id=connector_id,
        connector_protocol_version=spec.connector_protocol_version,
        operation=operation,
        phase=phase,
        deadline=deadline,
    )


def _send_without_response(socket_path: Path, request: ServiceOperationRequest) -> None:
    payload = request.canonical.encode("utf-8")
    frame = struct.pack("!I", len(payload)) + payload
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(TIMEOUT_MS / 1000)
            connection.connect(str(socket_path))
            connection.sendall(frame)
    except (OSError, TimeoutError) as exc:
        raise RuntimeError("lost-response landing dispatch failed") from exc


def _inspect_credential(
    *,
    python_bin: Path,
    source_root: Path,
    gh_config_dir: Path,
    gh_bin: Path,
    host: str,
    expected_account: str,
) -> dict[str, str]:
    command = [
        str(python_bin),
        "-m",
        "magic_glass_box.github_provider_cli",
        "inspect",
        "--gh-config-dir",
        str(gh_config_dir),
        "--host",
        host,
        "--gh-bin",
        str(gh_bin),
    ]
    try:
        completed = subprocess.run(  # noqa: S603
            command,
            capture_output=True,
            env=_provider_environment(source_root),
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("GitHub credential inspection failed") from exc
    if completed.returncode != 0 or len(completed.stdout) > 64 * 1024:
        raise RuntimeError("GitHub credential inspection failed")
    try:
        document = json.loads(completed.stdout)
        credential = document["credential"]
        account = credential["account"]
        inspected_host = credential["host"]
        scope = credential["credential_scope_sha256"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("GitHub credential inspection was invalid") from exc
    if (
        not isinstance(account, str)
        or account.casefold() != expected_account.casefold()
        or inspected_host != host
        or not isinstance(scope, str)
        or SHA256_RE.fullmatch(scope) is None
    ):
        raise RuntimeError("GitHub credential identity does not match authority")
    return {"account": account, "credential_scope_sha256": scope, "host": host}


def _write_spec(path: Path, spec: ServiceSidecarSpec) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        raw = (spec.canonical + "\n").encode("utf-8")
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            if written < 1:
                raise RuntimeError("GitHub provider spec could not be written")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _start_provider(
    *,
    python_bin: Path,
    source_root: Path,
    gh_config_dir: Path,
    gh_bin: Path,
    git_bin: Path,
    spec_path: Path,
    socket_path: Path,
    candidate_root: Path,
) -> subprocess.Popen[bytes]:
    command = [
        str(python_bin),
        "-m",
        "magic_glass_box.github_provider_cli",
        "serve",
        "--gh-config-dir",
        str(gh_config_dir),
        "--spec",
        str(spec_path),
        "--socket-path",
        str(socket_path),
        "--candidate-root",
        str(candidate_root),
        "--max-invocations",
        str(PROVIDER_MAX_INVOCATIONS),
        "--gh-bin",
        str(gh_bin),
        "--git-bin",
        str(git_bin),
    ]
    try:
        process = subprocess.Popen(  # noqa: S603
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_provider_environment(source_root),
            start_new_session=True,
        )
    except OSError as exc:
        raise RuntimeError("GitHub provider could not start") from exc
    deadline = time.monotonic() + PROVIDER_START_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.communicate(timeout=1)
            raise RuntimeError("GitHub provider stopped before becoming ready")
        if socket_path.exists():
            return process
        time.sleep(0.025)
    process.terminate()
    with contextlib.suppress(subprocess.TimeoutExpired):
        process.communicate(timeout=PROVIDER_STOP_SECONDS)
    if process.poll() is None:
        process.kill()
        process.communicate()
    raise RuntimeError("GitHub provider did not become ready")


def _stop_provider(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
    try:
        stdout, stderr = process.communicate(timeout=PROVIDER_STOP_SECONDS)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        stdout, stderr = process.communicate()
        raise RuntimeError("GitHub provider did not stop cleanly") from exc
    if len(stdout) > 64 * 1024 or len(stderr) > 64 * 1024 or process.returncode != 0:
        raise RuntimeError("GitHub provider stopped with an error")


def _require_success(
    client: UnixServiceSidecarClient,
    request: ServiceOperationRequest,
) -> ServiceOperationResponse:
    try:
        response = client.invoke(request)
    except ServiceSidecarError as exc:
        raise RuntimeError(f"{request.capability.name} transport failed") from exc
    if response.outcome != "succeeded" or response.receipt is None:
        detail = response.error_code or "provider-failure"
        raise RuntimeError(
            f"{request.capability.name} returned {response.outcome} ({detail})"
        )
    return response


def _result_object(response: ServiceOperationResponse, subject: str) -> dict[str, Any]:
    assert response.receipt is not None
    result = response.receipt.result
    if not isinstance(result, dict):
        raise RuntimeError(f"{subject} did not return a normalized result")
    return result


def _pull_number(remote_identity: str, *, host: str, repository: str) -> int:
    prefix = f"github:{host}/{repository}/pull/"
    suffix = remote_identity.removeprefix(prefix)
    if remote_identity == suffix or not suffix.isdigit():
        raise RuntimeError("change.open returned an invalid remote identity")
    number = int(suffix)
    if not 1 <= number <= 9_999_999_999:
        raise RuntimeError("change.open returned an invalid pull number")
    return number


def _assert_proof(evidence: ProofEvidence) -> dict[str, Any]:
    expected_outcomes = {
        "work.list": "succeeded",
        "refs.publish": "succeeded",
        "change.open": "succeeded",
        "change.get": "succeeded",
        "change.list": "succeeded",
        "checks.read": "succeeded",
        "landing.invoke": "response_intentionally_discarded",
        "landing.reconcile": "succeeded",
    }
    identity = (
        f"github:{evidence.host}/{evidence.repository}/pull/{evidence.pull_number}"
    )
    if (
        HOST_RE.fullmatch(evidence.host) is None
        or REPOSITORY_RE.fullmatch(evidence.repository) is None
        or ACCOUNT_RE.fullmatch(evidence.account) is None
        or SHA256_RE.fullmatch(evidence.credential_scope_sha256) is None
        or SHA1_RE.fullmatch(evidence.expected_base_revision) is None
        or SHA1_RE.fullmatch(evidence.candidate_revision) is None
        or evidence.candidate_revision == evidence.expected_base_revision
        or not evidence.branch_ref.startswith("refs/heads/mgb/live-proof-")
        or not 0 <= evidence.work_items_observed <= 100
        or evidence.change_remote_identity != identity
        or evidence.change_head_revision != evidence.candidate_revision
        or not evidence.change_list_contains_pull
        or evidence.checks_revision != evidence.candidate_revision
        or SHA1_RE.fullmatch(evidence.landed_revision) is None
        or evidence.landed_revision == evidence.expected_base_revision
        or evidence.landing_remote_identity != identity
        or evidence.landing_closed_references != (identity,)
        or dict(evidence.outcomes) != expected_outcomes
    ):
        raise RuntimeError("live GitHub proof did not settle to the authorized result")
    return {
        "account": evidence.account,
        "base_ref": evidence.base_ref,
        "bounded_reads": {"work_items_observed": evidence.work_items_observed},
        "candidate_revision": evidence.candidate_revision,
        "credential_scope_sha256": evidence.credential_scope_sha256,
        "expected_base_revision": evidence.expected_base_revision,
        "landed_revision": evidence.landed_revision,
        "lost_response_reconciled": True,
        "outcomes": dict(evidence.outcomes),
        "proof_branch": evidence.branch_ref,
        "pull_number": evidence.pull_number,
        "remote_identity": identity,
        "host": evidence.host,
        "repository": evidence.repository,
        "status": "passed",
    }


def run_proof(args: argparse.Namespace) -> dict[str, Any]:
    repository_root = Path(__file__).resolve().parent.parent
    source_root = repository_root / "src"
    python_bin = Path(sys.executable).resolve()
    gh_discovered = shutil.which("gh")
    git_discovered = shutil.which("git")
    if gh_discovered is None or git_discovered is None:
        raise RuntimeError("the GitHub and Git CLIs are required")
    gh_bin = Path(gh_discovered).resolve()
    git_bin = Path(git_discovered).resolve()
    source_repository = _verify_source_repository(
        args.source_repository,
        expected_base_revision=args.expected_base_revision,
        base_ref=args.base_ref,
        host=args.host,
        repository=args.repository,
        git_bin=git_bin,
    )
    identity = _inspect_credential(
        python_bin=python_bin,
        source_root=source_root,
        gh_config_dir=args.gh_config_dir,
        gh_bin=gh_bin,
        host=args.host,
        expected_account=args.expected_account,
    )
    with tempfile.TemporaryDirectory(prefix="mgb-github-proof-", dir="/tmp") as raw:
        root = Path(raw)
        root.chmod(0o700)
        candidate_root = root / "candidates"
        candidate_root.mkdir(mode=0o700)
        candidate = _build_candidate(
            candidate_root=candidate_root,
            source_repository=source_repository,
            expected_base_revision=args.expected_base_revision,
            proof_id=args.proof_id,
            git_bin=git_bin,
        )
        socket_path = root / "github.sock"
        spec = _service_sidecar_spec(
            socket_path=socket_path,
            host=args.host,
            repository=args.repository,
            account=identity["account"],
            credential_scope_sha256=identity["credential_scope_sha256"],
            deployment_sha256=_deployment_sha256(source_root),
        )
        spec_path = root / "github-provider.json"
        _write_spec(spec_path, spec)
        process = _start_provider(
            python_bin=python_bin,
            source_root=source_root,
            gh_config_dir=args.gh_config_dir,
            gh_bin=gh_bin,
            git_bin=git_bin,
            spec_path=spec_path,
            socket_path=socket_path,
            candidate_root=candidate_root,
        )
        proof_error: BaseException | None = None
        try:
            client = UnixServiceSidecarClient(socket_path)
            outcomes: dict[str, str] = {}
            work = _require_success(
                client,
                _operation_request(
                    spec=spec,
                    proof_id=args.proof_id,
                    capability="work.list",
                    phase="invoke",
                    resource_id="issues",
                    object_format=None,
                    expected_revision=None,
                    desired_revision=None,
                    parameters={"cursor": 1, "limit": 10, "state": "open"},
                ),
            )
            outcomes["work.list"] = work.outcome
            work_items = _result_object(work, "work.list").get("items")
            if not isinstance(work_items, list) or len(work_items) > 10:
                raise RuntimeError("work.list returned an invalid bounded page")

            branch_ref = f"refs/heads/mgb/live-proof-{args.proof_id}"
            published = _require_success(
                client,
                _operation_request(
                    spec=spec,
                    proof_id=args.proof_id,
                    capability="refs.publish",
                    phase="invoke",
                    resource_id=branch_ref,
                    object_format="sha1",
                    expected_revision=None,
                    desired_revision=candidate.revision,
                    parameters={"source_repository": str(candidate.repository)},
                ),
            )
            outcomes["refs.publish"] = published.outcome
            assert published.receipt is not None
            if published.receipt.observed_revision != candidate.revision:
                raise RuntimeError("refs.publish observed the wrong candidate")

            opened = _require_success(
                client,
                _operation_request(
                    spec=spec,
                    proof_id=args.proof_id,
                    capability="change.open",
                    phase="invoke",
                    resource_id=branch_ref,
                    object_format="sha1",
                    expected_revision=args.expected_base_revision,
                    desired_revision=candidate.revision,
                    parameters={
                        "base_ref": args.base_ref,
                        "body": (
                            "Opt-in Magic Glass Box M11 live proof. This pull request "
                            "exercises only the bounded GitHub provider surface."
                        ),
                        "draft": False,
                        "head_ref": branch_ref,
                        "title": f"MGB live provider proof {args.proof_id}",
                    },
                ),
            )
            outcomes["change.open"] = opened.outcome
            assert opened.receipt is not None
            pull_number = _pull_number(
                opened.receipt.remote_identity,
                host=args.host,
                repository=args.repository,
            )

            change = _require_success(
                client,
                _operation_request(
                    spec=spec,
                    proof_id=args.proof_id,
                    capability="change.get",
                    phase="invoke",
                    resource_id=str(pull_number),
                    object_format=None,
                    expected_revision=None,
                    desired_revision=None,
                    parameters={},
                ),
            )
            outcomes["change.get"] = change.outcome
            change_result = _result_object(change, "change.get")

            changes = _require_success(
                client,
                _operation_request(
                    spec=spec,
                    proof_id=args.proof_id,
                    capability="change.list",
                    phase="invoke",
                    resource_id="changes",
                    object_format=None,
                    expected_revision=None,
                    desired_revision=None,
                    parameters={"cursor": 1, "limit": 10, "state": "all"},
                ),
            )
            outcomes["change.list"] = changes.outcome
            change_items = _result_object(changes, "change.list").get("items")
            if not isinstance(change_items, list) or len(change_items) > 10:
                raise RuntimeError("change.list returned an invalid bounded page")
            contains_pull = any(
                isinstance(item, dict)
                and item.get("number") == pull_number
                and item.get("head_revision") == candidate.revision
                for item in change_items
            )

            checks = _require_success(
                client,
                _operation_request(
                    spec=spec,
                    proof_id=args.proof_id,
                    capability="checks.read",
                    phase="invoke",
                    resource_id=str(pull_number),
                    object_format="sha1",
                    expected_revision=args.expected_base_revision,
                    desired_revision=candidate.revision,
                    parameters={"required_approvals": 0, "required_checks": []},
                ),
            )
            outcomes["checks.read"] = checks.outcome
            assert checks.receipt is not None

            landing_arguments = {
                "spec": spec,
                "proof_id": args.proof_id,
                "capability": "landing.invoke",
                "resource_id": str(pull_number),
                "object_format": "sha1",
                "expected_revision": args.expected_base_revision,
                "desired_revision": candidate.revision,
                "parameters": {
                    "assurance": "head_enforced",
                    "integration_ref": args.base_ref,
                    "merge_method": args.merge_method,
                },
            }
            _send_without_response(
                socket_path,
                _operation_request(phase="invoke", **landing_arguments),
            )
            outcomes["landing.invoke"] = "response_intentionally_discarded"
            settled = _require_success(
                client,
                _operation_request(phase="reconcile", **landing_arguments),
            )
            outcomes["landing.reconcile"] = settled.outcome
            assert settled.receipt is not None
            landed_revision = settled.receipt.observed_revision
            if landed_revision is None:
                raise RuntimeError("landing reconciliation omitted the base revision")
            return _assert_proof(
                ProofEvidence(
                    host=args.host,
                    repository=args.repository,
                    account=identity["account"],
                    credential_scope_sha256=identity["credential_scope_sha256"],
                    base_ref=args.base_ref,
                    expected_base_revision=args.expected_base_revision,
                    candidate_revision=candidate.revision,
                    branch_ref=branch_ref,
                    work_items_observed=len(work_items),
                    pull_number=pull_number,
                    change_remote_identity=opened.receipt.remote_identity,
                    change_head_revision=str(change_result.get("head_revision", "")),
                    change_list_contains_pull=contains_pull,
                    checks_revision=checks.receipt.observed_revision or "",
                    landed_revision=landed_revision,
                    landing_remote_identity=settled.receipt.remote_identity,
                    landing_closed_references=settled.receipt.closed_references,
                    outcomes=outcomes,
                )
            )
        except BaseException as exc:
            proof_error = exc
            raise
        finally:
            try:
                _stop_provider(process)
            except RuntimeError:
                if proof_error is None:
                    raise


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        report = run_proof(args)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"live GitHub provider proof: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
