#!/usr/bin/env python3
"""Run the first real producer/reviewer beta against Imitation Krab source."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from magic_glass_box.candidates import CandidateManifest, GitCandidateStore
from magic_glass_box.db import ConflictError, Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    ProjectContext,
    RunState,
    WorkItemEnvelope,
    WorkspaceMode,
)
from magic_glass_box.executor import DockerCLI, DockerExecutor
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.inference import MAX_UNIX_SOCKET_PATH_BYTES
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.openai_chatgpt import ChatGPTOAuth, ChatGPTProfileStore
from magic_glass_box.orchestration import CandidateReviewWorkspaces, RunCoordinator
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

try:
    from scripts.check_live_openai_chatgpt import (
        BUDGET_CEILING_MICROUSD,
        CONNECTOR_NAME,
        GRANT_NAME,
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
        _write_connector_config,
    )
except ModuleNotFoundError:  # Direct script execution puts scripts/ on sys.path.
    from check_live_openai_chatgpt import (  # type: ignore[no-redef]
        BUDGET_CEILING_MICROUSD,
        CONNECTOR_NAME,
        GRANT_NAME,
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
        _write_connector_config,
    )

PROJECT_KEY = "imitation-krab-beta"
PRODUCER_PROFILE = "Beta Producer"
REVIEWER_PROFILE = "Beta Reviewer"
SAME_IDENTITY_REVIEWER_PROFILE = "Beta Same Identity Reviewer"
VALIDATION_NAME = "full-suite"
WORK_ITEM_EXTERNAL_ID = "mgb-beta-health-cli"
SUGGESTED_BRANCH = "mgb/beta-health-command"
MAX_REPORT_REVIEW_DETAILS = 16


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
    if not 30 <= parsed <= 1800:
        raise argparse.ArgumentTypeError("must be between 30 and 1800 seconds")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile-dir", required=True, type=Path)
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--source-revision")
    parser.add_argument("--model", required=True)
    parser.add_argument("--image", default=RUNTIME_IMAGE)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--build-timeout", type=_positive_seconds, default=900.0)
    parser.add_argument("--run-timeout", type=_positive_seconds, default=600.0)
    parser.add_argument(
        "--max-invocations",
        type=_positive_integer,
        default=64,
        help="lifetime model-turn limit for the connector process (default: 64)",
    )
    parser.add_argument("--max-review-rounds", type=_positive_integer, default=2)
    parser.add_argument(
        "--resume-review-run",
        help="retry one failed policy-bound reviewer in an existing beta state",
    )
    parser.add_argument(
        "--i-understand-this-uses-live-quota",
        action="store_true",
        help="required opt-in; producer and reviewer runs consume live quota",
    )
    return parser.parse_args(argv)


def _git(repository: Path, *arguments: str) -> str:
    git_binary = shutil.which("git")
    if git_binary is None:
        raise RuntimeError("Git CLI was not found")
    completed = subprocess.run(  # noqa: S603 - fixed Git executable and argv
        [git_binary, *arguments],
        cwd=repository,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError("Git command failed: " + detail[:1024])
    return completed.stdout.strip()


def _source_revision(repository: Path, requested: str | None) -> str:
    revision_spec = f"{requested}^{{commit}}" if requested else "HEAD^{commit}"
    revision = _git(repository, "rev-parse", "--verify", revision_spec)
    if len(revision) not in {40, 64}:
        raise RuntimeError("source revision is not a Git object ID")
    if _git(repository, "status", "--porcelain"):
        raise RuntimeError("beta source repository must have a clean worktree")
    return revision


def _connector_socket_path(state_root: Path) -> Path:
    path = state_root / "connector" / "inference.sock"
    if len(str(path).encode("utf-8")) > MAX_UNIX_SOCKET_PATH_BYTES:
        raise RuntimeError(
            "beta state path is too long for the connector Unix socket path"
        )
    return path


def _work_item() -> WorkItemEnvelope:
    return WorkItemEnvelope.from_mapping(
        {
            "schema_version": 1,
            "source": "manual",
            "external_id": WORK_ITEM_EXTERNAL_ID,
            "source_updated_at": "2026-10-21T18:00:00Z",
            "title": "Add an unauthenticated health command to the Krab CLI",
            "brief": (
                "Add `krab health` for supervisors and orchestration probes. It must "
                "GET `/v1/health` without reading a token file and without "
                "initializing, migrating, or otherwise touching the local database "
                "or pepper. Honor the existing server configuration and `--json`. "
                "Machine output must include the health status and the validated "
                "`X-Krab-Version` value as `server_version` (null for a legacy "
                "server that omits it); human output should be one terminal-safe "
                "line. Keep authenticated APIClient behavior unchanged. Start with "
                "failing tests, cover success and failure paths including missing "
                "credentials and an unavailable server, update the README, and run "
                "the complete unittest suite."
            ),
            "acceptance_criteria": [
                "`krab health --json` succeeds without a token file.",
                "The command performs no local database or pepper mutation.",
                "Output reports both status and trusted server version metadata.",
                "Unavailable and malformed upstream responses fail safely.",
                "The complete unittest suite passes.",
            ],
            "constraints": [
                "Do not change existing authenticated command behavior.",
                "Do not add runtime dependencies.",
                "Do not commit, push, or open a pull request.",
            ],
            "context": [
                {
                    "type": "note",
                    "value": (
                        "The HTTP service already exposes unauthenticated "
                        "GET /v1/health."
                    ),
                },
                {
                    "type": "note",
                    "value": (
                        "This is a bounded 0.7 integration hook for Magic Glass Box."
                    ),
                },
            ],
            "labels": ["mgb-beta", "cli", "health"],
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


def _prepare_control_plane(
    control: ControlPlane,
    *,
    repository: Path,
    source_revision: str,
    image: str,
    connector_socket: Path,
    deployment_sha256: str,
    scope_sha256: str,
    model: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    database = control.database
    database.initialize()
    user = database.create_user("operator", "Beta Operator")
    instance = database.create_krab_instance("Beta Krab", "http://127.0.0.1:8765")
    producer_identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity("usr_" + "1" * 32, "beta_producer", 1),
        "file:/not-mounted/beta-producer.token",
        user_id=user["id"],
    )
    reviewer_identity = database.store_krab_identity(
        instance["id"],
        KrabIdentity("usr_" + "2" * 32, "beta_reviewer", 1),
        "file:/not-mounted/beta-reviewer.token",
    )
    database.create_project(
        instance_id=instance["id"],
        krab_project_id="prj_" + "3" * 32,
        krab_project_key=PROJECT_KEY,
        display_name="Imitation Krab beta",
        context=ProjectContext.from_mapping(
            {
                "purpose": (
                    "Exercise a real producer/reviewer software change without "
                    "granting workers direct network or repository credentials."
                ),
                "repository_path": str(repository),
                "default_branch": "main",
                "instructions": [
                    "Use RED-first tests and retain happy and sad path coverage.",
                    "Run `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m "
                    "unittest discover -s tests -v`.",
                    "Do not commit, push, open a pull request, or use the network.",
                ],
            }
        ),
        created_by=user["id"],
    )
    work_item = database.import_work_item(
        project_key=PROJECT_KEY,
        envelope=_work_item(),
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
    producer_profile = database.create_agent_profile(
        name=PRODUCER_PROFILE,
        spec=_profile(image=image, workspace_mode=WorkspaceMode.COPY_ON_WRITE),
        krab_identity_id=producer_identity["id"],
        created_by=user["id"],
    )
    reviewer_profile = database.create_agent_profile(
        name=REVIEWER_PROFILE,
        spec=_profile(image=image, workspace_mode=WorkspaceMode.READ_ONLY),
        krab_identity_id=reviewer_identity["id"],
        created_by=user["id"],
    )
    same_identity_reviewer_profile = database.create_agent_profile(
        name=SAME_IDENTITY_REVIEWER_PROFILE,
        spec=_profile(image=image, workspace_mode=WorkspaceMode.READ_ONLY),
        krab_identity_id=producer_identity["id"],
        created_by=user["id"],
    )
    run = database.create_run(
        project_key=PROJECT_KEY,
        profile_name=PRODUCER_PROFILE,
        work_item_id=work_item["id"],
        requested_by=user["id"],
        source_revision=source_revision,
    )
    return (
        run,
        connector,
        {
            "producer": producer_profile,
            "reviewer": reviewer_profile,
            "same_identity_reviewer": same_identity_reviewer_profile,
        },
    )


def _resume_failed_review(
    control: ControlPlane,
    review_run_id: str,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
    list[str],
]:
    database = control.database
    database.initialize()
    failed_review = database.get_run(review_run_id)
    if (
        failed_review["state"] != RunState.FAILED.value
        or failed_review["gate_policy_id"] is None
        or failed_review["review_candidate_id"] is None
        or failed_review["change_candidate_id"] is not None
    ):
        raise RuntimeError(
            "resume target must be a failed policy-bound independent review"
        )
    policy = database.get_gate_policy(failed_review["gate_policy_id"])
    candidate = database.get_candidate(failed_review["review_candidate_id"])
    if (
        policy["candidate_id"] != candidate["id"]
        or policy["head_revision"] != candidate["head_revision"]
    ):
        raise RuntimeError("failed reviewer no longer matches its exact candidate")
    requester = database.get_user("operator")
    review_run = database.retry_run(
        failed_review["id"],
        requested_by=requester["id"],
        expected_run_version=failed_review["version"],
    )
    return (
        review_run,
        database.get_inference_connector(CONNECTOR_NAME),
        {
            "producer": database.get_agent_profile(PRODUCER_PROFILE),
            "reviewer": database.get_agent_profile(REVIEWER_PROFILE),
            "same_identity_reviewer": database.get_agent_profile(
                SAME_IDENTITY_REVIEWER_PROFILE
            ),
        },
        candidate,
        policy,
        [candidate["producer_run_id"], failed_review["id"], review_run["id"]],
    )


def _gate_request(image: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "review_separation": "different_profile_and_identity",
        "validations": [
            {
                "name": VALIDATION_NAME,
                "image": image,
                "command": [
                    "/bin/sh",
                    "-c",
                    "PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m "
                    "unittest discover -s tests -v",
                ],
            }
        ],
    }


def _successful_attempt(run: Mapping[str, Any]) -> Mapping[str, Any]:
    attempts = run.get("attempts")
    if (
        run.get("state") != RunState.SUCCEEDED.value
        or not isinstance(attempts, list)
        or len(attempts) != 1
        or attempts[0].get("state") != "succeeded"
        or attempts[0].get("result") is None
    ):
        raise RuntimeError("agent run did not retain one successful result")
    return attempts[0]


def _assert_role_separation(
    control: ControlPlane,
    policy: Mapping[str, Any],
) -> dict[str, bool]:
    checks = {
        "producer_profile_rejected": PRODUCER_PROFILE,
        "same_identity_profile_rejected": SAME_IDENTITY_REVIEWER_PROFILE,
    }
    results: dict[str, bool] = {}
    for name, profile in checks.items():
        try:
            control.create_review_run(
                policy["id"],
                profile_name=profile,
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )
        except ConflictError:
            results[name] = True
        else:
            raise RuntimeError(f"review separation preflight failed: {name}")
    return results


def _review_outcome(entry: Mapping[str, Any]) -> str:
    summary = entry.get("summary")
    if not isinstance(summary, str):
        raise RuntimeError("review entry summary is unavailable")
    normalized = summary.strip().upper()
    if normalized.startswith("APPROVED"):
        return "passed"
    if normalized.startswith("CHANGES REQUESTED"):
        return "failed"
    raise RuntimeError("review summary must begin APPROVED or CHANGES REQUESTED")


def _review_entry(
    control: ControlPlane,
    *,
    review_run: Mapping[str, Any],
    attempt_id: str,
    head_revision: str,
) -> dict[str, Any]:
    memory = control.read_work_memory(review_run["id"], after_sequence=0, limit=64)
    entries = [
        entry
        for entry in memory["entries"]
        if entry["kind"] == "review"
        and entry["candidate_revision"] == head_revision
        and entry["author"]["run_id"] == review_run["id"]
        and entry["author"]["attempt_id"] == attempt_id
    ]
    if len(entries) != 1:
        raise RuntimeError(
            "reviewer must leave exactly one exact-head review work-log entry"
        )
    return entries[0]


def _changed_paths(result_store: WorkspaceResultStore, attempt_id: str) -> list[str]:
    return [change.path for change in result_store.inspect(attempt_id).changes]


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    temporary = path.with_name("." + path.name + ".tmp")
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.i_understand_this_uses_live_quota:
        raise RuntimeError(
            "live quota opt-in is required: --i-understand-this-uses-live-quota"
        )
    if not hasattr(os, "getuid") or os.getuid() <= 0 or os.getgid() <= 0:
        raise RuntimeError("live beta requires a non-root POSIX user")
    project_root = Path(__file__).resolve().parents[1]
    profile_dir = args.profile_dir.expanduser().absolute()
    repository = args.repository.expanduser().resolve(strict=True)
    state_root = args.state_dir.expanduser().absolute()
    resuming = args.resume_review_run is not None
    if resuming and not state_root.is_dir():
        raise RuntimeError("resume requires an existing beta state directory")
    if not resuming and state_root.exists():
        raise RuntimeError("beta state directory must not already exist")
    for protected in (project_root, profile_dir, repository):
        try:
            if state_root.resolve().is_relative_to(protected.resolve()):
                raise RuntimeError(
                    "beta state directory must remain outside code and credentials"
                )
        except OSError as exc:
            raise RuntimeError("beta path is unavailable") from exc
    connector_socket = _connector_socket_path(state_root)
    revision = _source_revision(repository, args.source_revision)
    store = ChatGPTProfileStore(profile_dir)
    oauth = ChatGPTOAuth(store)
    session = oauth.session()
    models = oauth.list_models()
    if args.model not in {model.slug for model in models}:
        raise RuntimeError("selected model is not visible to this ChatGPT account")
    initial_secrets = _session_secrets(store)
    if args.build:
        _build_image(args.image, args.build_timeout)

    state_root.mkdir(mode=0o700, parents=True, exist_ok=resuming)
    os.chmod(state_root, 0o700)
    connector_root = connector_socket.parent
    connector_root.mkdir(mode=0o700, exist_ok=resuming)
    os.chmod(connector_root, 0o700)
    deployment_sha256 = _deployment_digest(project_root)
    database = Database(state_root / "control-plane.db")
    control = ControlPlane(database)
    resume_candidate: dict[str, Any] | None = None
    resume_policy: dict[str, Any] | None = None
    resume_run_ids: list[str] = []
    if resuming:
        (
            producer_run,
            connector,
            profiles,
            resume_candidate,
            resume_policy,
            resume_run_ids,
        ) = _resume_failed_review(control, args.resume_review_run)
    else:
        producer_run, connector, profiles = _prepare_control_plane(
            control,
            repository=repository,
            source_revision=revision,
            image=args.image,
            connector_socket=connector_socket,
            deployment_sha256=deployment_sha256,
            scope_sha256=session.credential_scope_sha256,
            model=args.model,
        )
    expected_connector = _connector_spec(
        socket_path=connector_socket,
        deployment_sha256=deployment_sha256,
        scope_sha256=session.credential_scope_sha256,
        model=args.model,
    )
    if (
        connector["state"] != "active"
        or connector["spec"] != expected_connector.to_dict()
    ):
        raise RuntimeError("existing connector does not match the requested live beta")
    connector_config = connector_root / "config.json"
    _write_connector_config(
        connector_config,
        connector_id=connector["id"],
        deployment_sha256=deployment_sha256,
        scope_sha256=session.credential_scope_sha256,
        model=args.model,
    )
    docker_engine = _RecordingDockerEngine(
        DockerCLI(command_timeout=60, run_timeout=args.run_timeout)
    )
    executor = DockerExecutor(docker_engine)
    result_store = WorkspaceResultStore(
        state_root / "results", max_paths=512, max_changed_bytes=64 * 1024 * 1024
    )
    candidate_store = GitCandidateStore(state_root / "candidates")
    workspaces = WorkspaceManager(state_root / "workspaces", GitSnapshotter())
    candidate_workspaces = CandidateReviewWorkspaces(
        database, workspaces, result_store, candidate_store
    )
    gateways = FileGatewayManager(state_root / "gateways", control)
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
    connector_process: subprocess.Popen[str] | None = None
    connector_stdout = ""
    connector_stderr = ""
    run_ids: list[str] = list(resume_run_ids)
    review_records: list[dict[str, Any]] = []
    candidate_chain: list[dict[str, Any]] = []
    validation: dict[str, Any] | None = None
    readiness: dict[str, Any] | None = None
    accepted_policy: dict[str, Any] | None = None
    final_candidate: dict[str, Any] | None = None
    separation_preflight: dict[str, bool] | None = None
    pending_review_run = producer_run if resuming else None
    candidate = resume_candidate
    policy = resume_policy
    try:
        connector_process = _start_connector(
            project_root=project_root,
            profile_dir=profile_dir,
            config_path=connector_config,
            socket_path=connector_socket,
            max_invocations=args.max_invocations,
        )
        if not resuming:
            run_ids.append(producer_run["id"])
            completed_producer = coordinator.execute(producer_run["id"])
            producer_attempt = _successful_attempt(completed_producer)
            candidate = control.create_candidate(
                producer_attempt["id"],
                created_by_handle="operator",
                result_store=result_store,
                candidate_store=candidate_store,
            )
        assert candidate is not None
        candidate_chain.append(candidate)

        for round_number in range(1, args.max_review_rounds + 1):
            if policy is None:
                policy = control.create_gate_policy(
                    candidate["id"],
                    _gate_request(args.image),
                    created_by_handle="operator",
                    image_resolver=docker_engine.inspect_image,
                )
            if separation_preflight is None:
                separation_preflight = _assert_role_separation(control, policy)
            if pending_review_run is None:
                review_run = control.create_review_run(
                    policy["id"],
                    profile_name=REVIEWER_PROFILE,
                    requested_by_handle="operator",
                    expected_policy_revision=policy["policy_revision"],
                )
                run_ids.append(review_run["id"])
            else:
                review_run = pending_review_run
                pending_review_run = None
            completed_review = coordinator.execute(review_run["id"])
            review_attempt = _successful_attempt(completed_review)
            if _changed_paths(result_store, review_attempt["id"]):
                raise RuntimeError("read-only reviewer produced workspace changes")
            entry = _review_entry(
                control,
                review_run=completed_review,
                attempt_id=review_attempt["id"],
                head_revision=candidate["head_revision"],
            )
            outcome = _review_outcome(entry)
            evidence = control.record_review_evidence(
                policy["id"],
                reviewer_attempt_id=review_attempt["id"],
                outcome=outcome,
                recorded_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )
            review_records.append(
                {
                    "round": round_number,
                    "run_id": completed_review["id"],
                    "attempt_id": review_attempt["id"],
                    "entry_id": entry["id"],
                    "evidence_id": evidence["id"],
                    "outcome": outcome,
                    "summary": entry["summary"],
                    "details": list(entry["details"])[:MAX_REPORT_REVIEW_DETAILS],
                }
            )
            if outcome == "passed":
                validation = control.run_local_validation(
                    policy["id"],
                    validation_name=VALIDATION_NAME,
                    recorded_by_handle="operator",
                    expected_policy_revision=policy["policy_revision"],
                    result_store=result_store,
                    candidate_store=candidate_store,
                    executor=executor,
                )
                readiness = control.gate_readiness(policy["id"])
                if validation["outcome"] != "passed" or not readiness["ready"]:
                    raise RuntimeError("accepted candidate did not pass exact gates")
                accepted_policy = policy
                final_candidate = candidate
                break
            if round_number == args.max_review_rounds:
                raise RuntimeError("review still requested changes at the round limit")
            changes_run = control.create_changes_run(
                policy["id"],
                review_evidence_id=evidence["id"],
                review_entry_id=entry["id"],
                profile_name=PRODUCER_PROFILE,
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )
            run_ids.append(changes_run["id"])
            completed_changes = coordinator.execute(changes_run["id"])
            changes_attempt = _successful_attempt(completed_changes)
            candidate = control.create_candidate(
                changes_attempt["id"],
                created_by_handle="operator",
                result_store=result_store,
                candidate_store=candidate_store,
            )
            candidate_chain.append(candidate)
            policy = None
    finally:
        try:
            gateways.close()
        finally:
            if connector_process is not None:
                connector_stdout, connector_stderr = _stop_connector(connector_process)

    if final_candidate is None or accepted_policy is None:
        raise RuntimeError("beta ended without an accepted candidate")
    final_manifest = CandidateManifest.from_mapping(final_candidate["manifest"])
    candidate_repository = candidate_store.repository_path(
        final_manifest.project_id, final_manifest.object_format
    )
    invocations = [
        invocation
        for run_id in run_ids
        for invocation in database.list_provider_invocations(run_id)
    ]
    final_secrets = tuple(
        dict.fromkeys((*initial_secrets, *_session_secrets(store, required=False)))
    )
    launch_plans = _serialize_sandbox_plans(docker_engine.plans)
    _assert_sandbox_boundaries(
        docker_engine.plans,
        profile_dir=profile_dir,
        secrets=final_secrets,
    )
    _assert_no_secret(
        final_secrets,
        {
            "beta state": _collect_regular_files(state_root),
            "worker runtime output": "\n".join(docker_engine.logs_seen).encode(),
            "connector logs": (connector_stdout + connector_stderr).encode(),
            "Docker sandbox launch plans": launch_plans,
        },
    )
    if any(docker_engine.managed_containers(run_id) for run_id in run_ids):
        raise RuntimeError("beta left a managed worker container behind")
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
    report = {
        "status": "passed",
        "repository": str(repository),
        "source_revision": revision,
        "state_directory": str(state_root),
        "database": str(database.path),
        "model": args.model,
        "budget_ceiling_microusd": BUDGET_CEILING_MICROUSD,
        "connector_max_invocations": args.max_invocations,
        "provider_invocations": len(invocations),
        "resumed_from_review_run": args.resume_review_run,
        "usage": usage,
        "direct_worker_network": "denied",
        "credential_exposure": "none outside connector profile",
        "profiles": {
            "producer_profile_version_id": profiles["producer"]["profile_version_id"],
            "reviewer_profile_version_id": profiles["reviewer"]["profile_version_id"],
        },
        "separation_preflight": separation_preflight,
        "run_ids": run_ids,
        "candidate_chain": [
            {
                "id": item["id"],
                "base_revision": item["base_revision"],
                "head_revision": item["head_revision"],
                "tree_revision": item["tree_revision"],
                "changed_paths": _changed_paths(result_store, item["attempt_id"]),
            }
            for item in candidate_chain
        ],
        "review_rounds": review_records,
        "accepted_policy_id": accepted_policy["id"],
        "accepted_policy_revision": accepted_policy["policy_revision"],
        "validation_evidence_id": validation["id"],
        "readiness": readiness,
        "final_candidate_id": final_candidate["id"],
        "final_candidate_head": final_candidate["head_revision"],
        "handoff": {
            "suggested_branch": SUGGESTED_BRANCH,
            "suggested_pr_title": _work_item().title,
            "suggested_pr_body": (
                "Implemented by the Magic Glass Box producer/reviewer beta.\n\n"
                f"Candidate: `{final_candidate['id']}`\n"
                f"Policy: `{accepted_policy['id']}`\n"
                "Independent review and the full unittest gate passed."
            ),
            "commands_not_executed": [
                [
                    "git",
                    "fetch",
                    str(candidate_repository),
                    final_candidate["head_revision"],
                ],
                ["git", "switch", "-c", SUGGESTED_BRANCH, "FETCH_HEAD"],
                [
                    "env",
                    "PYTHONDONTWRITEBYTECODE=1",
                    "PYTHONPATH=src",
                    "python3",
                    "-m",
                    "unittest",
                    "discover",
                    "-s",
                    "tests",
                    "-v",
                ],
                ["git", "push", "--set-upstream", "origin", SUGGESTED_BRANCH],
                [
                    "gh",
                    "pr",
                    "create",
                    "--title",
                    _work_item().title,
                    "--body",
                    "Use the suggested PR body from report.json",
                ],
            ],
        },
    }
    _write_report(state_root / "report.json", report)
    report_bytes = (state_root / "report.json").read_bytes()
    report["report_sha256"] = hashlib.sha256(report_bytes).hexdigest()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
