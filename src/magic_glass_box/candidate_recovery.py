from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from .candidates import CandidateError, CandidateManifest, GitCandidateStore
from .results import WorkspaceResultStore

MAX_CANDIDATE_ANCESTORS = 32


class CandidateRecoveryDatabase(Protocol):
    def get_candidate_source(self, attempt_id: str) -> dict[str, Any]: ...

    def get_gate_revision(self, policy_id: str) -> dict[str, Any]: ...


def ensure_candidate_revision(
    database: CandidateRecoveryDatabase,
    result_store: WorkspaceResultStore,
    candidate_store: GitCandidateStore,
    manifest: CandidateManifest,
    repository: Path,
) -> None:
    """Recover an exact candidate, recursively restoring generated parents."""

    _ensure_candidate_revision(
        database,
        result_store,
        candidate_store,
        manifest,
        repository,
        ancestors=frozenset(),
    )


def _ensure_candidate_revision(
    database: CandidateRecoveryDatabase,
    result_store: WorkspaceResultStore,
    candidate_store: GitCandidateStore,
    manifest: CandidateManifest,
    repository: Path,
    *,
    ancestors: frozenset[tuple[str, str]],
) -> None:
    try:
        candidate_store.inspect(manifest)
    except CandidateError:
        pass
    else:
        return

    identity = (manifest.project_id, manifest.head_revision)
    if identity in ancestors or len(ancestors) >= MAX_CANDIDATE_ANCESTORS:
        raise CandidateError("candidate recovery ancestry is invalid")
    ancestors = ancestors | {identity}

    source = database.get_candidate_source(manifest.attempt_id)
    change_policy_id = source["change_policy_id"]
    if (
        change_policy_id is not None
        and source["result"].source_revision == manifest.base_revision
    ):
        revision = database.get_gate_revision(change_policy_id)
        parent = CandidateManifest.from_mapping(revision["manifest"])
        policy = revision["policy"]
        if (
            policy["candidate_id"] != source["change_candidate_id"]
            or parent.project_id != manifest.project_id
            or parent.context_version_id != manifest.context_version_id
            or parent.work_thread_id != manifest.work_thread_id
            or parent.head_revision != manifest.base_revision
            or parent.object_format != manifest.object_format
        ):
            raise CandidateError(
                "changes-requested candidate ancestry does not match durable state"
            )
        parent_repository = revision["repository_path"]
        if parent_repository is None:
            raise CandidateError(
                "changes-requested candidate has no recovery repository"
            )
        _ensure_candidate_revision(
            database,
            result_store,
            candidate_store,
            parent,
            Path(parent_repository),
            ancestors=ancestors,
        )

    result, blobs = result_store.load_bundle(
        manifest.attempt_id,
        expected_digest=manifest.result_content_sha256,
    )
    candidate_store.recover_revision(manifest, repository, result, blobs)
