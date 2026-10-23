#!/usr/bin/env python3
"""Replay exact maintainer feedback through the live Imitation Krab beta."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from magic_glass_box.candidates import CandidateManifest, GitCandidateStore
from magic_glass_box.db import Database
from magic_glass_box.domain import ProjectContext
from magic_glass_box.executor import DockerCLI, DockerExecutor
from magic_glass_box.gates import MaintainerDecision, MaintainerReviewRequest
from magic_glass_box.gateway import FileGatewayManager
from magic_glass_box.openai_chatgpt import ChatGPTOAuth, ChatGPTProfileStore
from magic_glass_box.orchestration import CandidateReviewWorkspaces, RunCoordinator
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.workspace import GitSnapshotter, WorkspaceManager

try:
    from scripts.check_live_imitation_krab_beta import (
        MAX_REPORT_REVIEW_DETAILS,
        PRODUCER_PROFILE,
        PROJECT_KEY,
        REVIEWER_PROFILE,
        SUGGESTED_BRANCH,
        VALIDATION_NAME,
        _assert_role_separation,
        _changed_paths,
        _connector_socket_path,
        _review_entry,
        _review_outcome,
        _source_revision,
        _successful_attempt,
        _work_item,
        _write_report,
    )
    from scripts.check_live_imitation_krab_beta import (
        _gate_request as _technical_gate_request,
    )
    from scripts.check_live_openai_chatgpt import (
        BUDGET_CEILING_MICROUSD,
        CONNECTOR_NAME,
        RUNTIME_IMAGE,
        _assert_no_secret,
        _assert_sandbox_boundaries,
        _build_image,
        _collect_regular_files,
        _connector_spec,
        _deployment_digest,
        _RecordingDockerEngine,
        _serialize_sandbox_plans,
        _session_secrets,
        _start_connector,
        _stop_connector,
        _write_connector_config,
    )
except ModuleNotFoundError:  # Direct execution puts scripts/ on sys.path.
    from check_live_imitation_krab_beta import (  # type: ignore[no-redef]
        MAX_REPORT_REVIEW_DETAILS,
        PRODUCER_PROFILE,
        PROJECT_KEY,
        REVIEWER_PROFILE,
        SUGGESTED_BRANCH,
        VALIDATION_NAME,
        _assert_role_separation,
        _changed_paths,
        _connector_socket_path,
        _review_entry,
        _review_outcome,
        _source_revision,
        _successful_attempt,
        _work_item,
        _write_report,
    )
    from check_live_imitation_krab_beta import (
        _gate_request as _technical_gate_request,
    )
    from check_live_openai_chatgpt import (  # type: ignore[no-redef]
        BUDGET_CEILING_MICROUSD,
        CONNECTOR_NAME,
        RUNTIME_IMAGE,
        _assert_no_secret,
        _assert_sandbox_boundaries,
        _build_image,
        _collect_regular_files,
        _connector_spec,
        _deployment_digest,
        _RecordingDockerEngine,
        _serialize_sandbox_plans,
        _session_secrets,
        _start_connector,
        _stop_connector,
        _write_connector_config,
    )

MAX_REVIEW_BYTES = 64 * 1024
REPORT_NAME = "maintainer-round-report.json"


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
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--review", required=True, type=Path)
    parser.add_argument("--model", required=True)
    parser.add_argument("--image", default=RUNTIME_IMAGE)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--build-timeout", type=_positive_seconds, default=900.0)
    parser.add_argument("--run-timeout", type=_positive_seconds, default=600.0)
    parser.add_argument("--max-invocations", type=_positive_integer, default=64)
    parser.add_argument("--max-review-rounds", type=_positive_integer, default=2)
    parser.add_argument(
        "--i-understand-this-uses-live-quota",
        action="store_true",
        help="required opt-in; correction and reviewer runs consume live quota",
    )
    return parser.parse_args(argv)


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate field: {key}")
        result[key] = value
    return result


def _load_changes_requested(path: Path) -> MaintainerReviewRequest:
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
        review = MaintainerReviewRequest.from_mapping(value)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise RuntimeError("maintainer review is invalid") from exc
    if review.decision is not MaintainerDecision.CHANGES_REQUESTED:
        raise RuntimeError("maintainer continuation requires changes_requested")
    return review


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


def _gate_request(image: str) -> dict[str, Any]:
    request = _technical_gate_request(image)
    request["maintainer_approval"] = True
    return request


def _assert_pending_maintainer(readiness: Mapping[str, Any]) -> None:
    gates = readiness.get("gates")
    if readiness.get("ready") is not False or not isinstance(gates, list):
        raise RuntimeError("candidate did not stop at the maintainer gate")
    states = {
        gate.get("name"): gate.get("state") for gate in gates if isinstance(gate, dict)
    }
    expected = {
        "independent-review": "passed",
        "maintainer-approval": "pending",
        f"validation:{VALIDATION_NAME}": "passed",
    }
    if states != expected:
        raise RuntimeError("candidate technical gates are not exactly complete")


def _ensure_project_context(
    control: ControlPlane, repository: Path
) -> tuple[dict[str, Any], bool]:
    expected = _project_context(repository)
    current = control.database.get_project(PROJECT_KEY)
    if current["content_sha256"] == expected.digest:
        return current, False
    revised = control.revise_project_context(
        project_key=PROJECT_KEY,
        context=expected,
        created_by_handle="operator",
    )
    return revised, True


def _usage(
    database: Database, run_ids: Sequence[str]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    invocations = [
        invocation
        for run_id in run_ids
        for invocation in database.list_provider_invocations(run_id)
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
    return invocations, usage


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
    state_root = args.state_dir.expanduser().resolve(strict=True)
    review_request = _load_changes_requested(args.review)
    for protected in (project_root, profile_dir, repository):
        if state_root.is_relative_to(protected.resolve()):
            raise RuntimeError(
                "beta state directory must remain outside code and credentials"
            )
    repository_revision = _source_revision(repository, None)
    connector_socket = _connector_socket_path(state_root)

    store = ChatGPTProfileStore(profile_dir)
    oauth = ChatGPTOAuth(store)
    session = oauth.session()
    if args.model not in {model.slug for model in oauth.list_models()}:
        raise RuntimeError("selected model is not visible to this ChatGPT account")
    initial_secrets = _session_secrets(store)
    if args.build:
        _build_image(args.image, args.build_timeout)

    deployment_sha256 = _deployment_digest(project_root)
    database = Database(state_root / "control-plane.db")
    control = ControlPlane(database)
    database.initialize()
    parent_candidate = database.get_candidate(args.candidate)
    project = database.get_project(PROJECT_KEY)
    if parent_candidate["project_id"] != project["id"]:
        raise RuntimeError("candidate does not belong to the beta project")
    project_context, context_revised = _ensure_project_context(control, repository)
    connector = database.get_inference_connector(CONNECTOR_NAME)
    profiles = {
        "producer": database.get_agent_profile(PRODUCER_PROFILE),
        "reviewer": database.get_agent_profile(REVIEWER_PROFILE),
    }
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
        raise RuntimeError("existing connector does not match this continuation")

    connector_root = connector_socket.parent
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
    run_ids: list[str] = []
    candidate_chain: list[dict[str, Any]] = []
    review_records: list[dict[str, Any]] = []
    final_candidate: dict[str, Any] | None = None
    final_policy: dict[str, Any] | None = None
    validation: dict[str, Any] | None = None
    readiness: dict[str, Any] | None = None
    external_policy: dict[str, Any] | None = None
    external_review: dict[str, Any] | None = None
    separation_preflight: dict[str, bool] | None = None
    try:
        connector_process = _start_connector(
            project_root=project_root,
            profile_dir=profile_dir,
            config_path=connector_config,
            socket_path=connector_socket,
            max_invocations=args.max_invocations,
        )
        external_policy = control.create_gate_policy(
            parent_candidate["id"],
            _gate_request(args.image),
            created_by_handle="operator",
            image_resolver=docker_engine.inspect_image,
        )
        external_review = control.record_maintainer_review(
            external_policy["id"],
            review_request=review_request.to_dict(),
            recorded_by_handle="operator",
            expected_policy_revision=external_policy["policy_revision"],
        )
        changes_run = control.create_changes_run(
            external_policy["id"],
            maintainer_review_id=external_review["id"],
            profile_name=PRODUCER_PROFILE,
            requested_by_handle="operator",
            expected_policy_revision=external_policy["policy_revision"],
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

        for round_number in range(1, args.max_review_rounds + 1):
            policy = control.create_gate_policy(
                candidate["id"],
                _gate_request(args.image),
                created_by_handle="operator",
                image_resolver=docker_engine.inspect_image,
            )
            if separation_preflight is None:
                separation_preflight = _assert_role_separation(control, policy)
            review_run = control.create_review_run(
                policy["id"],
                profile_name=REVIEWER_PROFILE,
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )
            run_ids.append(review_run["id"])
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
                if validation["outcome"] != "passed":
                    raise RuntimeError("candidate validation did not pass")
                _assert_pending_maintainer(readiness)
                final_candidate = candidate
                final_policy = policy
                break
            if round_number == args.max_review_rounds:
                raise RuntimeError("review still requested changes at the round limit")
            correction_run = control.create_changes_run(
                policy["id"],
                review_evidence_id=evidence["id"],
                review_entry_id=entry["id"],
                profile_name=PRODUCER_PROFILE,
                requested_by_handle="operator",
                expected_policy_revision=policy["policy_revision"],
            )
            run_ids.append(correction_run["id"])
            completed_correction = coordinator.execute(correction_run["id"])
            correction_attempt = _successful_attempt(completed_correction)
            candidate = control.create_candidate(
                correction_attempt["id"],
                created_by_handle="operator",
                result_store=result_store,
                candidate_store=candidate_store,
            )
            candidate_chain.append(candidate)
    finally:
        try:
            gateways.close()
        finally:
            if connector_process is not None:
                connector_stdout, connector_stderr = _stop_connector(connector_process)

    if (
        final_candidate is None
        or final_policy is None
        or validation is None
        or readiness is None
        or external_policy is None
        or external_review is None
    ):
        raise RuntimeError("maintainer continuation did not reach its honest stop")

    final_manifest = CandidateManifest.from_mapping(final_candidate["manifest"])
    candidate_repository = candidate_store.repository_path(
        final_manifest.project_id, final_manifest.object_format
    )
    invocations, usage = _usage(database, run_ids)
    final_secrets = tuple(
        dict.fromkeys((*initial_secrets, *_session_secrets(store, required=False)))
    )
    launch_plans = _serialize_sandbox_plans(docker_engine.plans)
    _assert_sandbox_boundaries(
        docker_engine.plans, profile_dir=profile_dir, secrets=final_secrets
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

    report = {
        "status": "pending_maintainer_approval",
        "repository": str(repository),
        "repository_revision_observed": repository_revision,
        "state_directory": str(state_root),
        "database": str(database.path),
        "model": args.model,
        "budget_ceiling_microusd": BUDGET_CEILING_MICROUSD,
        "connector_max_invocations": args.max_invocations,
        "provider_invocations": len(invocations),
        "usage": usage,
        "direct_worker_network": "denied",
        "credential_exposure": "none outside connector profile",
        "project_context": {
            "version_id": project_context["context_version_id"],
            "content_sha256": project_context["content_sha256"],
            "revised": context_revised,
        },
        "profiles": {
            "producer_profile_version_id": profiles["producer"]["profile_version_id"],
            "reviewer_profile_version_id": profiles["reviewer"]["profile_version_id"],
        },
        "separation_preflight": separation_preflight,
        "run_ids": run_ids,
        "maintainer_feedback": {
            "candidate_id": parent_candidate["id"],
            "policy_id": external_policy["id"],
            "review_id": external_review["id"],
            "decision": external_review["decision"],
            "summary": external_review["summary"],
            "details": external_review["details"],
        },
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
        "technical_policy_id": final_policy["id"],
        "technical_policy_revision": final_policy["policy_revision"],
        "validation_evidence_id": validation["id"],
        "readiness": readiness,
        "final_candidate_id": final_candidate["id"],
        "final_candidate_head": final_candidate["head_revision"],
        "handoff": {
            "next_action": (
                "A maintainer must inspect this exact candidate and record either "
                "approved or changes_requested. No landing authority exists yet."
            ),
            "suggested_branch_after_approval": SUGGESTED_BRANCH,
            "suggested_pr_title_after_approval": _work_item().title,
            "candidate_repository": str(candidate_repository),
            "commands_executed": [],
        },
    }
    report_path = state_root / REPORT_NAME
    _write_report(report_path, report)
    report["report_sha256"] = hashlib.sha256(report_path.read_bytes()).hexdigest()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
