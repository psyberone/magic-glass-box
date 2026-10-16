#!/usr/bin/env python3
"""Run one explicitly opted-in real-account Codex golden proof."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from magic_glass_box.candidates import GitCandidateStore
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    ProjectContext,
    RunState,
    WorkItemEnvelope,
)
from magic_glass_box.executor import DockerCLI, DockerExecutor, DockerImage, SandboxPlan
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.inference import InferenceConnectorSpec, InferenceGrantSpec
from magic_glass_box.integration import LandingPlan
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.openai_chatgpt import ChatGPTOAuth, ChatGPTProfileStore
from magic_glass_box.orchestration import RunCoordinator
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

PROOF_TEXT = "MGB_OPENAI_GOLDEN_OK\n"
PROOF_PATH = "golden-proof.txt"
RUNTIME_IMAGE = "magic-glass-box/runtime-proof:local"
ROUTE_NAME = "openai-chatgpt"
GRANT_NAME = "openai-chatgpt"
CONNECTOR_NAME = "openai-chatgpt"
REQUEST_BYTES = 2 * 1024 * 1024
RESPONSE_BYTES = 4 * 1024 * 1024
PROVIDER_TIMEOUT_MS = 120_000
MAX_DIAGNOSTIC_LOG_CHARACTERS = 4096


class _RecordingDockerEngine:
    def __init__(self, delegate: DockerCLI):
        self.delegate = delegate
        self.plans: list[SandboxPlan] = []
        self.logs_seen: list[str] = []

    def inspect_image(self, reference: str) -> DockerImage:
        return self.delegate.inspect_image(reference)

    def create(self, plan: SandboxPlan) -> str:
        self.plans.append(plan)
        return self.delegate.create(plan)

    def start(self, container_id: str) -> None:
        self.delegate.start(container_id)

    def wait(self, container_id: str) -> int:
        return self.delegate.wait(container_id)

    def logs(self, container_id: str) -> str:
        value = self.delegate.logs(container_id)
        self.logs_seen.append(value)
        return value

    def stop(self, container_id: str) -> None:
        self.delegate.stop(container_id)

    def remove(self, container_id: str) -> None:
        self.delegate.remove(container_id)

    def managed_containers(self, run_id: str) -> tuple[str, ...]:
        return self.delegate.managed_containers(run_id)


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if not 1 <= parsed <= 12:
        raise argparse.ArgumentTypeError("must be between 1 and 12")
    return parsed


def _positive_seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if not 10 <= parsed <= 900:
        raise argparse.ArgumentTypeError("must be between 10 and 900 seconds")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile-dir", required=True, type=Path)
    parser.add_argument("--model", required=True)
    parser.add_argument("--image", default=RUNTIME_IMAGE)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--build-timeout", type=_positive_seconds, default=600.0)
    parser.add_argument("--run-timeout", type=_positive_seconds, default=300.0)
    parser.add_argument("--max-invocations", type=_positive_integer, default=6)
    parser.add_argument(
        "--i-understand-this-uses-live-quota",
        action="store_true",
        help="required opt-in; this proof can consume ChatGPT-plan quota",
    )
    return parser.parse_args(argv)


def _run_checked(command: list[str], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(  # noqa: S603 - fixed local proof argv
        command,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"command failed: {detail[:4096]}")
    return completed.stdout.strip()


def _build_image(image: str, timeout: float) -> None:
    docker = shutil.which("docker")
    if docker is None:
        raise RuntimeError("Docker CLI was not found")
    repository = Path(__file__).resolve().parents[1]
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local proof argv
            [
                docker,
                "build",
                "--file",
                str(repository / "docker" / "runtime-proof.Dockerfile"),
                "--tag",
                image,
                str(repository),
            ],
            stdin=subprocess.DEVNULL,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("runtime proof image build timed out") from None
    if completed.returncode != 0:
        raise RuntimeError("runtime proof image build failed")


def _create_repository(root: Path) -> tuple[Path, str]:
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("Git CLI was not found")
    repository = root / "repository"
    repository.mkdir(mode=0o700)
    _run_checked([git, "init", "--quiet", "--initial-branch", "main"], cwd=repository)
    (repository / "README.md").write_text(
        "# Magic Glass Box live-account fixture\n",
        encoding="utf-8",
    )
    _run_checked([git, "add", "README.md"], cwd=repository)
    _run_checked(
        [
            git,
            "-c",
            "user.name=Magic Glass Box",
            "-c",
            "user.email=proof@magic-glass-box.invalid",
            "commit",
            "--quiet",
            "-m",
            "Create live-account proof fixture",
        ],
        cwd=repository,
    )
    return repository, _run_checked([git, "rev-parse", "HEAD"], cwd=repository)


def _deployment_digest(project_root: Path) -> str:
    digest = hashlib.sha256()
    for relative in (
        "src/magic_glass_box/openai_chatgpt.py",
        "src/magic_glass_box/openai_chatgpt_cli.py",
        "scripts/openai_chatgpt_connector.py",
    ):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\x00")
        digest.update((project_root / relative).read_bytes())
        digest.update(b"\x00")
    return digest.hexdigest()


def _connector_spec(
    *,
    socket_path: Path,
    deployment_sha256: str,
    scope_sha256: str,
    model: str,
) -> InferenceConnectorSpec:
    return InferenceConnectorSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_protocol_version": 1,
            "deployment_sha256": deployment_sha256,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "routes": [
                {
                    "name": ROUTE_NAME,
                    "provider": "openai",
                    "protocol": "openai-responses",
                    "models": [model],
                    "credential_scope_sha256": scope_sha256,
                }
            ],
            "request_bytes": REQUEST_BYTES,
            "response_bytes": RESPONSE_BYTES,
            "timeout_ms": PROVIDER_TIMEOUT_MS,
            "max_concurrency": 1,
        }
    )


def _grant_spec(
    *, deployment_sha256: str, scope_sha256: str, model: str
) -> InferenceGrantSpec:
    return InferenceGrantSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_name": CONNECTOR_NAME,
            "deployment_sha256": deployment_sha256,
            "route": ROUTE_NAME,
            "credential_scope_sha256": scope_sha256,
            "provider": "openai",
            "protocol": "openai-responses",
            "model": model,
            "request_bytes": REQUEST_BYTES,
            "response_bytes": RESPONSE_BYTES,
            "timeout_ms": PROVIDER_TIMEOUT_MS,
            "max_concurrency": 1,
        }
    )


def _prepare_run(
    control: ControlPlane,
    *,
    repository: Path,
    image: str,
    connector_socket: Path,
    deployment_sha256: str,
    scope_sha256: str,
    model: str,
) -> tuple[str, dict[str, Any]]:
    database = control.database
    database.initialize()
    user = database.create_user("operator", "Live Proof Operator")
    instance = database.create_krab_instance("Proof Krab", "http://127.0.0.1:8765")
    identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity("usr_" + "1" * 32, "openai_golden", 1),
        "file:/not-mounted/openai-golden-krab.token",
        user_id=user["id"],
    )
    database.create_project(
        instance_id=instance["id"],
        krab_project_id="prj_" + "2" * 32,
        krab_project_key="openai-golden",
        display_name="OpenAI golden proof",
        context=ProjectContext.from_mapping(
            {
                "purpose": "Prove one bounded real-account MGB execution.",
                "repository_path": str(repository),
                "default_branch": "main",
                "instructions": [
                    "Make only the requested tiny file change.",
                    "Do not commit, push, or open a pull request.",
                ],
            }
        ),
        created_by=user["id"],
    )
    work_item = database.import_work_item(
        project_key="openai-golden",
        envelope=WorkItemEnvelope.from_mapping(
            {
                "schema_version": 1,
                "source": "manual",
                "external_id": "m7d3-live-account-proof",
                "source_updated_at": "2026-10-15T12:00:00Z",
                "title": "Create the bounded OpenAI golden-proof artifact",
                "brief": (
                    f"Create only {PROOF_PATH}. Its complete contents must be exactly "
                    f"{PROOF_TEXT!r}. Do not modify any existing file."
                ),
            }
        ),
        imported_by=user["id"],
    )
    connector = control.register_inference_connector(
        name=CONNECTOR_NAME,
        spec=_connector_spec(
            socket_path=connector_socket,
            deployment_sha256=deployment_sha256,
            scope_sha256=scope_sha256,
            model=model,
        ),
        created_by_handle="operator",
    )
    control.add_connector_provider_grant(
        name=GRANT_NAME,
        spec=_grant_spec(
            deployment_sha256=deployment_sha256,
            scope_sha256=scope_sha256,
            model=model,
        ),
        expires_at=None,
        created_by_handle="operator",
    )
    profile = AgentProfileSpec.create(
        image=image,
        command=("codex",),
        runtime_adapter="codex",
        runtime_provider_grant=GRANT_NAME,
        network_mode=NetworkMode.GATEWAY_ONLY,
        credential_names=(GRANT_NAME,),
        gateway_capabilities=(
            GatewayCapability.RUN_DESCRIBE,
            GatewayCapability.PROVIDER_INVOKE,
        ),
    )
    database.create_agent_profile(
        name="OpenAI Golden Worker",
        spec=profile,
        krab_identity_id=identity["id"],
        created_by=user["id"],
    )
    run = database.create_run(
        project_key="openai-golden",
        profile_name="OpenAI Golden Worker",
        work_item_id=work_item["id"],
        requested_by=user["id"],
    )
    return run["id"], connector


def _write_connector_config(
    path: Path,
    *,
    connector_id: str,
    deployment_sha256: str,
    scope_sha256: str,
    model: str,
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
                "request_bytes": REQUEST_BYTES,
                "response_bytes": RESPONSE_BYTES,
                "timeout_ms": PROVIDER_TIMEOUT_MS,
            }
        ],
    }
    path.write_text(
        json.dumps(value, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    os.chmod(path, 0o600)


def _start_connector(
    *,
    project_root: Path,
    profile_dir: Path,
    config_path: Path,
    socket_path: Path,
    max_invocations: int,
) -> subprocess.Popen[str]:
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(project_root / "src"),
    }
    process = subprocess.Popen(  # noqa: S603 - fixed local proof argv
        [
            sys.executable,
            str(project_root / "scripts" / "openai_chatgpt_connector.py"),
            "serve",
            "--profile-dir",
            str(profile_dir),
            "--config",
            str(config_path),
            "--socket-path",
            str(socket_path),
            "--max-invocations",
            str(max_invocations),
        ],
        cwd=project_root,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if socket_path.exists() or process.poll() is not None:
            break
        time.sleep(0.02)
    if not socket_path.exists():
        stdout, stderr = process.communicate(timeout=2)
        raise RuntimeError("OpenAI connector did not start: " + stdout + stderr)
    return process


def _stop_connector(process: subprocess.Popen[str]) -> tuple[str, str]:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    stdout, stderr = process.communicate(timeout=2)
    return stdout, stderr


def _collect_regular_files(root: Path) -> bytes:
    values: list[bytes] = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and not path.is_symlink():
            values.append(path.read_bytes())
    return b"".join(values)


def _session_secrets(
    store: ChatGPTProfileStore, *, required: bool = True
) -> tuple[str, ...]:
    session = store.session()
    if session is None:
        if not required:
            return ()
        raise RuntimeError("ChatGPT sign-in is required")
    return (session.access_token, session.refresh_token, session.id_token)


def _assert_no_secret(secrets: Sequence[str], surfaces: Mapping[str, bytes]) -> None:
    for name, content in surfaces.items():
        if any(secret.encode("utf-8") in content for secret in secrets if secret):
            raise RuntimeError(f"OAuth credential leaked into {name}")


def _serialize_sandbox_plans(plans: Sequence[SandboxPlan]) -> bytes:
    return b"\x00".join(
        argument.encode("utf-8") for plan in plans for argument in plan.create_args
    )


def _assert_sandbox_boundaries(
    plans: Sequence[SandboxPlan],
    *,
    profile_dir: Path,
    secrets: Sequence[str],
) -> bytes:
    serialized = _serialize_sandbox_plans(plans)
    if str(profile_dir).encode("utf-8") in serialized:
        raise RuntimeError("connector profile was mounted into a sandbox")
    _assert_no_secret(secrets, {"Docker sandbox launch plans": serialized})
    for plan in plans:
        arguments = plan.create_args
        if "--network" not in arguments:
            raise RuntimeError("sandbox launch omitted its network boundary")
        network_index = arguments.index("--network")
        if network_index + 1 >= len(arguments):
            raise RuntimeError("sandbox launch omitted its network boundary")
        if arguments[network_index + 1] != "none":
            raise RuntimeError("sandbox launch granted direct network access")
    return serialized


def _assert_sandbox_plans(
    plans: Sequence[SandboxPlan],
    *,
    profile_dir: Path,
    secrets: Sequence[str],
) -> bytes:
    serialized = _assert_sandbox_boundaries(
        plans, profile_dir=profile_dir, secrets=secrets
    )
    if len(plans) != 2:
        raise RuntimeError(
            "golden proof did not execute exactly one worker and one gate"
        )
    return serialized


def _failure_diagnostic(
    *,
    run: Mapping[str, Any],
    invocations: Sequence[Mapping[str, Any]],
    worker_logs: Sequence[str],
    orchestration_error: Exception | None,
) -> dict[str, Any]:
    attempts = run.get("attempts")
    safe_attempts = []
    if isinstance(attempts, list):
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            reason = attempt.get("reason")
            safe_attempts.append(
                {
                    "exit_code": attempt.get("exit_code"),
                    "has_result": attempt.get("result") is not None,
                    "reason": reason[:512] if isinstance(reason, str) else None,
                    "state": attempt.get("state"),
                }
            )
    invocation_fields = (
        "error_code",
        "http_status",
        "model",
        "outcome",
        "protocol",
        "request_bytes",
        "response_bytes",
        "route",
    )
    safe_invocations = [
        {field: invocation.get(field) for field in invocation_fields}
        for invocation in invocations
    ]
    return {
        "attempts": safe_attempts,
        "orchestrator_error": (
            None if orchestration_error is None else type(orchestration_error).__name__
        ),
        "provider_invocations": safe_invocations,
        "run_state": run.get("state"),
        "worker_log_tail": "\n".join(worker_logs)[-MAX_DIAGNOSTIC_LOG_CHARACTERS:],
    }


def _verified_failure_diagnostic(
    *,
    run: Mapping[str, Any],
    invocations: Sequence[Mapping[str, Any]],
    worker_logs: Sequence[str],
    orchestration_error: Exception | None,
    secrets: Sequence[str],
    surfaces: Mapping[str, bytes],
) -> dict[str, Any]:
    _assert_no_secret(secrets, surfaces)
    return _failure_diagnostic(
        run=run,
        invocations=invocations,
        worker_logs=worker_logs,
        orchestration_error=orchestration_error,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.i_understand_this_uses_live_quota:
        raise RuntimeError(
            "live quota opt-in is required: --i-understand-this-uses-live-quota"
        )
    if not hasattr(os, "getuid") or os.getuid() <= 0 or os.getgid() <= 0:
        raise RuntimeError("live account proof requires a non-root POSIX user")
    project_root = Path(__file__).resolve().parents[1]
    profile_dir = args.profile_dir.expanduser().absolute()
    try:
        if profile_dir.resolve().is_relative_to(project_root.resolve()):
            raise RuntimeError("connector profile must remain outside the checkout")
    except OSError as exc:
        raise RuntimeError("connector profile is unavailable") from exc
    store = ChatGPTProfileStore(profile_dir)
    oauth = ChatGPTOAuth(store)
    session = oauth.session()
    models = oauth.list_models()
    if args.model not in {model.slug for model in models}:
        raise RuntimeError("selected model is not visible to this ChatGPT account")
    secrets_seen = list(_session_secrets(store))
    if args.build:
        _build_image(args.image, args.build_timeout)

    project_digest = _deployment_digest(project_root)
    connector_process: subprocess.Popen[str] | None = None
    connector_stdout = ""
    connector_stderr = ""
    report: dict[str, Any]
    with tempfile.TemporaryDirectory(
        prefix="mgb-openai-golden-",
        dir="/tmp",  # noqa: S108 - private disposable proof root
    ) as temporary:
        root = Path(temporary)
        os.chmod(root, 0o700)
        connector_root = root / "connector"
        connector_root.mkdir(mode=0o700)
        connector_socket = connector_root / "inference.sock"
        repository, initial_revision = _create_repository(root)
        database = Database(root / "control-plane.db")
        control = ControlPlane(database)
        run_id, connector = _prepare_run(
            control,
            repository=repository,
            image=args.image,
            connector_socket=connector_socket,
            deployment_sha256=project_digest,
            scope_sha256=session.credential_scope_sha256,
            model=args.model,
        )
        connector_config = connector_root / "config.json"
        _write_connector_config(
            connector_config,
            connector_id=connector["id"],
            deployment_sha256=project_digest,
            scope_sha256=session.credential_scope_sha256,
            model=args.model,
        )
        docker_engine = _RecordingDockerEngine(
            DockerCLI(
                command_timeout=60,
                run_timeout=args.run_timeout,
            )
        )
        executor = DockerExecutor(docker_engine)
        result_store = WorkspaceResultStore(root / "results", max_paths=16)
        candidate_store = GitCandidateStore(root / "candidates")
        gateways = FileGatewayManager(root / "gateways", control)
        coordinator = RunCoordinator(
            database,
            executor,
            WorkspaceManager(root / "workspaces", GitSnapshotter()),
            gateways,
            result_store,
            lease_seconds=30,
            heartbeat_interval=5,
        )
        orchestration_error: Exception | None = None
        try:
            connector_process = _start_connector(
                project_root=project_root,
                profile_dir=profile_dir,
                config_path=connector_config,
                socket_path=connector_socket,
                max_invocations=args.max_invocations,
            )
            run = coordinator.execute(run_id)
        except Exception as exc:
            orchestration_error = exc
            run = database.get_run(run_id)
        finally:
            try:
                gateways.close()
            finally:
                if connector_process is not None:
                    connector_stdout, connector_stderr = _stop_connector(
                        connector_process
                    )

        invocations = database.list_provider_invocations(run_id)
        secrets_seen.extend(_session_secrets(store, required=False))
        unique_secrets = tuple(dict.fromkeys(secrets_seen))
        launch_plans = _serialize_sandbox_plans(docker_engine.plans)
        control_room = json.dumps(
            control.control_room_snapshot(), separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        scanned_surfaces = {
            "MGB state and disposable repository": _collect_regular_files(root),
            "worker runtime output": "\n".join(docker_engine.logs_seen).encode(),
            "connector logs": (connector_stdout + connector_stderr).encode(),
            "control room": control_room,
            "Docker sandbox launch plans": launch_plans,
        }
        failure_diagnostic = _verified_failure_diagnostic(
            run=run,
            invocations=invocations,
            worker_logs=docker_engine.logs_seen,
            orchestration_error=orchestration_error,
            secrets=unique_secrets,
            surfaces=scanned_surfaces,
        )
        _assert_sandbox_boundaries(
            docker_engine.plans,
            profile_dir=profile_dir,
            secrets=unique_secrets,
        )
        if docker_engine.managed_containers(run_id):
            raise RuntimeError(
                "live worker container was not removed: "
                + json.dumps(failure_diagnostic, sort_keys=True)
            )
        if orchestration_error is not None or run["state"] != RunState.SUCCEEDED.value:
            raise RuntimeError(
                "live Codex run did not succeed: "
                + json.dumps(failure_diagnostic, sort_keys=True)
            )
        if len(run["attempts"]) != 1:
            raise RuntimeError("live proof did not use exactly one run attempt")
        attempt = run["attempts"][0]
        if attempt["state"] != "succeeded" or attempt["result"] is None:
            raise RuntimeError("live proof attempt did not retain a successful result")
        manifest = result_store.inspect(attempt["id"])
        if [change.path for change in manifest.changes] != [PROOF_PATH]:
            raise RuntimeError("live agent changed files outside the tiny work item")
        _result, blobs = result_store.load_bundle(
            attempt["id"], expected_digest=manifest.digest
        )
        proof_digest = hashlib.sha256(PROOF_TEXT.encode("utf-8")).hexdigest()
        if blobs.get(proof_digest) != PROOF_TEXT.encode("utf-8"):
            raise RuntimeError("live agent produced the wrong proof artifact")

        if not 1 <= len(invocations) <= args.max_invocations:
            raise RuntimeError("live provider invocation count exceeded its cap")
        if any(
            invocation["outcome"] != "succeeded"
            or invocation["dispatch_kind"] != "connector"
            or invocation["provider"] != "openai"
            or invocation["protocol"] != "openai-responses"
            or invocation["model"] != args.model
            or invocation["route"] != ROUTE_NAME
            for invocation in invocations
        ):
            raise RuntimeError("live provider audit evidence is incomplete")
        candidate = control.create_candidate(
            attempt["id"],
            created_by_handle="operator",
            result_store=result_store,
            candidate_store=candidate_store,
        )
        policy = control.create_gate_policy(
            candidate["id"],
            {
                "schema_version": 1,
                "review_separation": None,
                "validations": [
                    {
                        "name": "proof",
                        "image": args.image,
                        "command": [
                            "python3",
                            "-c",
                            (
                                "from pathlib import Path; "
                                f"assert Path({PROOF_PATH!r}).read_bytes() == "
                                f"{PROOF_TEXT.encode()!r}"
                            ),
                        ],
                    }
                ],
            },
            created_by_handle="operator",
            image_resolver=docker_engine.inspect_image,
        )
        evidence = control.run_local_validation(
            policy["id"],
            validation_name="proof",
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
            result_store=result_store,
            candidate_store=candidate_store,
            executor=executor,
        )
        readiness = control.gate_readiness(policy["id"])
        if evidence["outcome"] != "passed" or not readiness["ready"]:
            raise RuntimeError("exact-input validation gate did not pass")
        queue = control.enqueue_integration(policy["id"], created_by_handle="operator")
        claimed = control.claim_integration_item(
            queue["id"],
            owner_id="live-proof-operator",
            expected_version=queue["version"],
            lease_seconds=30,
        )
        issued = control.issue_landing_plan(
            queue["id"],
            owner_id="live-proof-operator",
            expected_version=claimed["version"],
            remote="origin",
            issued_by_handle="operator",
            result_store=result_store,
            candidate_store=candidate_store,
        )
        landing = LandingPlan.from_mapping(issued["plan"])
        if (
            _run_checked(["git", "rev-parse", "HEAD"], cwd=repository)
            != initial_revision
        ):
            raise RuntimeError("manual landing boundary changed the source repository")
        if (repository / PROOF_PATH).exists():
            raise RuntimeError("candidate escaped into the source working tree")
        if _run_checked(["git", "status", "--porcelain"], cwd=repository):
            raise RuntimeError("source repository became dirty before manual landing")

        launch_plans = _assert_sandbox_plans(
            docker_engine.plans,
            profile_dir=profile_dir,
            secrets=unique_secrets,
        )
        control_room = json.dumps(
            control.control_room_snapshot(), separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        _assert_no_secret(
            unique_secrets,
            {
                "MGB state and disposable repository": _collect_regular_files(root),
                "worker runtime output": "\n".join(docker_engine.logs_seen).encode(),
                "connector logs": (connector_stdout + connector_stderr).encode(),
                "control room": control_room,
                "Docker sandbox launch plans": launch_plans,
            },
        )
        report = {
            "attempts": 1,
            "candidate_head": candidate["head_revision"],
            "connector_invocations": len(invocations),
            "credential_exposure": "none outside connector profile",
            "direct_worker_network": "denied",
            "gate": "validation:proof passed",
            "landing_commands": len(landing.commands),
            "landing_status": "plan issued; commands not executed",
            "model": args.model,
            "result_paths": [PROOF_PATH],
            "status": "passed",
        }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
