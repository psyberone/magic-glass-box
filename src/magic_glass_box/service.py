from __future__ import annotations

import hashlib
import os
import secrets
import stat
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .advisory import (
    AdvisoryBlindReview,
    AdvisoryChallengeReview,
    AdvisoryConfigurationSpec,
    AdvisoryReport,
)
from .candidate_recovery import ensure_candidate_revision
from .candidates import (
    CANDIDATE_SCHEMA_VERSION,
    CandidateError,
    CandidateManifest,
    GitCandidateStore,
)
from .credentials import (
    CredentialResolver,
    PrivateFileCredentialResolver,
)
from .db import (
    BudgetExceededError,
    ConcurrentUpdateError,
    ConflictError,
    Database,
    InvocationLimitExceededError,
    NotFoundError,
)
from .discovery import DiscoveryConfigurationSpec, DiscoveryRuling, DiscoveryTrace
from .domain import (
    AgentProfileSpec,
    HandoffEnvelope,
    InteractionKind,
    NetworkMode,
    ProjectContext,
    RunState,
    SupervisionPolicy,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
    WorkLogEntry,
    WorkMemoryCheckpoint,
    WorkspaceMode,
    canonical_json,
    validate_krab_id,
)
from .execution_operations import (
    ExecutionRecipeAuthority,
    build_execution_followup,
    build_execution_submission,
)
from .gates import (
    GATE_EVIDENCE_SCHEMA_VERSION,
    ExactGateInputs,
    GateOutcome,
    GatePolicyManifest,
    MaintainerReview,
    MaintainerReviewRequest,
    ReviewEvidence,
    ValidationEvidence,
    resolve_gate_policy_request,
)
from .inference import (
    INFERENCE_SCHEMA_VERSION,
    InferenceConnectorAmbiguousError,
    InferenceConnectorClient,
    InferenceConnectorError,
    InferenceConnectorSpec,
    InferenceConnectorUnavailableError,
    InferenceGrantSpec,
    InferenceInvocationRequest,
    InferenceInvocationResponse,
    InferenceUsage,
    UnixInferenceConnectorClient,
)
from .integration import FoldManifest, LandingReceiptRequest
from .krab import (
    ActivityPage,
    InboxAck,
    InboxPage,
    KrabClient,
    KrabError,
    KrabProject,
    KrabSession,
    SessionCloseReceipt,
    SessionItem,
)
from .provider import (
    ProviderAmbiguousError,
    ProviderBudgetError,
    ProviderBusyError,
    ProviderError,
    ProviderInvocationLimitError,
    ProviderUpstreamError,
    provider_recovery_error,
)
from .results import WorkspaceResultStore
from .runtime import RuntimeLaunch
from .service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceOperationSpec,
    ServiceSidecarAmbiguousError,
    ServiceSidecarClient,
    ServiceSidecarError,
    ServiceSidecarRetryableError,
    ServiceSidecarSpec,
    ServiceSidecarUnavailableError,
    UnixServiceSidecarClient,
)
from .workflow_scope import WorkflowScopeReview
from .workflows import WorkflowConfigurationSpec

if TYPE_CHECKING:
    from .executor import DockerExecutor

ClientFactory = Callable[..., KrabClient]
InferenceClientFactory = Callable[[Path], InferenceConnectorClient]
ServiceClientFactory = Callable[[Path], ServiceSidecarClient]
PROVIDER_DISPATCH_LEASE_SECONDS = 30
PROVIDER_DISPATCH_RENEW_INTERVAL_SECONDS = 10.0


class _ProviderDispatchLeaseMonitor:
    """Keep a live cross-process provider slot; crashes still expire cleanly."""

    def __init__(
        self,
        database: Database,
        reservation_id: str,
        *,
        lease_seconds: int,
        interval: float,
    ):
        self.database = database
        self.reservation_id = reservation_id
        self.lease_seconds = lease_seconds
        self.interval = interval
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._started = False
        self._thread = threading.Thread(
            target=self._run,
            name="mgb-provider-dispatch-lease",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()
        self._started = True

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.database.renew_provider_dispatch(
                    self.reservation_id,
                    lease_seconds=self.lease_seconds,
                )
            except BaseException as exc:
                self._error = exc
                return

    def close(self) -> None:
        self._stop.set()
        if self._started:
            self._thread.join()

    def check(self) -> None:
        if self._error is not None:
            raise ProviderError(
                "provider dispatch authority could not be maintained"
            ) from self._error


@dataclass(frozen=True)
class AcceptedInteractionResponse:
    """Transient private response content paired with its durable correlation."""

    interaction_id: str
    run_id: str
    response_item_id: str
    interaction_state: str
    interaction_version: int
    run_state: str
    run_version: int
    untrusted_title: str
    untrusted_body: str
    content_risk_flags: tuple[str, ...]


@dataclass(frozen=True)
class AcceptedProviderResponse:
    """Transient untrusted provider output paired with durable audit metadata."""

    invocation_id: str
    grant_name: str
    provider: str
    model: str
    status_code: int
    request_bytes: int
    response_bytes: int
    untrusted_response: dict[str, Any]
    usage: InferenceUsage | None = None
    estimated_microusd: int | None = None


class ControlPlane:
    """Coordinate durable local state with Krab identity and messaging."""

    def __init__(
        self,
        database: Database,
        *,
        credential_resolver: CredentialResolver | None = None,
        client_factory: ClientFactory | None = None,
        inference_client_factory: InferenceClientFactory | None = None,
        service_client_factory: ServiceClientFactory | None = None,
    ):
        self.database = database
        self.credential_resolver = (
            credential_resolver or PrivateFileCredentialResolver()
        )
        self.client_factory = client_factory or KrabClient
        self.inference_client_factory = (
            inference_client_factory or UnixInferenceConnectorClient
        )
        self.service_client_factory = service_client_factory or UnixServiceSidecarClient

    def initialize(self) -> dict[str, Any]:
        return self.database.initialize()

    def control_room_snapshot(
        self,
        *,
        history_limit: int = 25,
        refresh_krab: bool = False,
    ) -> dict[str, Any]:
        from .control_room import ControlRoom

        return ControlRoom(
            self.database,
            credential_resolver=self.credential_resolver,
            client_factory=self.client_factory,
        ).snapshot(history_limit=history_limit, refresh_krab=refresh_krab)

    def create_candidate(
        self,
        attempt_id: str,
        *,
        created_by_handle: str,
        result_store: WorkspaceResultStore,
        candidate_store: GitCandidateStore,
    ) -> dict[str, Any]:
        """Materialize one immutable candidate from a completed attempt result."""

        creator = self.database.get_user(created_by_handle)
        if creator["active"] != 1:
            raise ConflictError("candidate creator is inactive")
        source = self.database.get_candidate_source(attempt_id)
        existing = self.database.find_candidate_for_result(source["attempt_result_id"])
        if source["work_thread_id"] is None:
            raise ConflictError("candidate result is not pinned to a work item")
        recorded = (
            None
            if existing is None
            else CandidateManifest.from_mapping(existing["manifest"])
        )
        if recorded is not None:
            try:
                candidate_store.inspect(recorded)
            except CandidateError:
                pass
            else:
                return existing
        repository_path = source["repository_path"]
        if repository_path is None:
            raise ConflictError("candidate project has no repository path")
        result, blobs = result_store.load_bundle(
            attempt_id, expected_digest=source["result_content_sha256"]
        )
        if result != source["result"]:
            raise ConflictError("candidate result does not match durable state")
        change_base = None
        if source["change_policy_id"] is not None:
            revision = self.database.get_gate_revision(source["change_policy_id"])
            change_base = CandidateManifest.from_mapping(revision["manifest"])
            if (
                change_base.project_id != source["project_id"]
                or change_base.context_version_id != source["context_version_id"]
                or change_base.work_thread_id != source["work_thread_id"]
                or change_base.head_revision != result.source_revision
                or revision["policy"]["candidate_id"] != source["change_candidate_id"]
            ):
                raise ConflictError(
                    "changes-requested candidate base does not match durable state"
                )
            base_repository_path = revision["repository_path"]
            if base_repository_path is None:
                raise ConflictError(
                    "changes-requested candidate has no recovery repository"
                )
            ensure_candidate_revision(
                self.database,
                result_store,
                candidate_store,
                change_base,
                Path(base_repository_path),
            )
        if existing is None:
            if change_base is None:
                materialized = candidate_store.materialize(
                    source["project_id"],
                    Path(repository_path),
                    result,
                    blobs,
                )
            else:
                materialized = candidate_store.materialize_revision(
                    source["project_id"],
                    Path(repository_path),
                    result,
                    blobs,
                    base_revision=change_base.head_revision,
                    object_format=change_base.object_format,
                )
        else:
            assert recorded is not None
            materialized = candidate_store.recover(
                recorded,
                Path(repository_path),
                result,
                blobs,
            )
        manifest = CandidateManifest(
            schema_version=CANDIDATE_SCHEMA_VERSION,
            attempt_id=attempt_id,
            attempt_result_id=source["attempt_result_id"],
            producer_run_id=source["producer_run_id"],
            project_id=source["project_id"],
            context_version_id=source["context_version_id"],
            work_thread_id=source["work_thread_id"],
            base_revision=materialized.base_revision,
            result_content_sha256=source["result_content_sha256"],
            head_revision=materialized.head_revision,
            tree_revision=materialized.tree_revision,
            object_format=materialized.object_format,
        )
        if existing is not None:
            if recorded != manifest:
                raise ConflictError(
                    "candidate materialization does not match durable record"
                )
            return existing
        return self.database.create_candidate(manifest, created_by=creator["id"])

    def get_candidate(
        self,
        candidate_id: str,
        *,
        result_store: WorkspaceResultStore,
        candidate_store: GitCandidateStore,
    ) -> dict[str, Any]:
        """Verify and return one durable candidate record."""

        candidate = self.database.get_candidate(candidate_id)
        manifest = CandidateManifest.from_mapping(candidate["manifest"])
        repository_path = self.database.get_candidate_repository_path(candidate_id)
        if repository_path is None:
            raise ConflictError("candidate project has no repository path")
        ensure_candidate_revision(
            self.database,
            result_store,
            candidate_store,
            manifest,
            Path(repository_path),
        )
        return candidate

    def list_candidates(
        self,
        *,
        project_key: str | None = None,
        producer_run_id: str | None = None,
        work_thread_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Discover recent immutable candidates without touching the Git cache."""

        project_id = None
        if project_key is not None:
            project_id = self.database.get_project(project_key)["id"]
        return self.database.list_candidates(
            project_id=project_id,
            producer_run_id=producer_run_id,
            work_thread_id=work_thread_id,
            limit=limit,
        )

    def create_gate_policy(
        self,
        candidate_id: str,
        policy_request: object,
        *,
        created_by_handle: str,
        image_resolver: Callable[[str], object],
        fold_id: str | None = None,
    ) -> dict[str, Any]:
        """Resolve mutable image references and pin one exact candidate policy."""

        creator = self.database.get_user(created_by_handle)
        if creator["active"] != 1:
            raise ConflictError("gate policy creator is inactive")
        candidate = self.database.get_candidate(candidate_id)
        context = ProjectContext.from_mapping(
            self.database.get_gate_candidate_context(candidate_id)
        )
        policy = resolve_gate_policy_request(
            policy_request, image_resolver=image_resolver
        )
        manifest = (
            GatePolicyManifest.bind(
                candidate,
                default_branch=context.default_branch,
                policy=policy,
            )
            if fold_id is None
            else GatePolicyManifest.bind_revision(
                candidate,
                self.database.get_integration_fold(fold_id)["manifest"],
                default_branch=context.default_branch,
                policy=policy,
            )
        )
        return self.database.create_gate_policy(manifest, created_by=creator["id"])

    def add_landing_dependency(
        self,
        dependent_work_item_id: str,
        prerequisite_work_item_id: str,
        *,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.add_landing_dependency(
            dependent_work_item_id,
            prerequisite_work_item_id,
            created_by=creator["id"],
        )

    def remove_landing_dependency(
        self,
        dependency_id: str,
        *,
        expected_version: int,
        removed_by_handle: str,
    ) -> dict[str, Any]:
        remover = self.database.get_user(removed_by_handle)
        return self.database.remove_landing_dependency(
            dependency_id,
            expected_version=expected_version,
            removed_by=remover["id"],
        )

    def list_landing_dependencies(
        self,
        *,
        project_key: str | None = None,
        active_only: bool = False,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        project_id = None
        if project_key is not None:
            project_id = self.database.get_project(project_key)["id"]
        return self.database.list_landing_dependencies(
            project_id=project_id,
            active_only=active_only,
            limit=limit,
        )

    def enqueue_integration(
        self,
        policy_id: str,
        *,
        created_by_handle: str,
        initial_revision: str | None = None,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.enqueue_integration(
            policy_id,
            created_by=creator["id"],
            initial_revision=initial_revision,
        )

    def accept_workflow_landing_chain(
        self,
        execution_id: str,
        *,
        expected_queue_version: int,
        accepted_by_handle: str,
    ) -> dict[str, Any] | None:
        acceptor = self.database.get_user(accepted_by_handle)
        return self.database.accept_workflow_landing_chain(
            execution_id,
            expected_queue_version=expected_queue_version,
            accepted_by=acceptor["id"],
        )

    def refold_integration_item(
        self,
        queue_item_id: str,
        *,
        expected_version: int,
        created_by_handle: str,
        result_store: WorkspaceResultStore,
        candidate_store: GitCandidateStore,
    ) -> dict[str, Any]:
        """Materialize and retain one exact fold against the current ref."""

        creator = self.database.get_user(created_by_handle)
        if creator["active"] != 1:
            raise ConflictError("fold creator is inactive")
        queue = self.database.get_integration_queue_item(queue_item_id)
        if queue["version"] != expected_version:
            raise ConcurrentUpdateError("integration queue version changed")
        if queue["state"] != "refold_required":
            raise ConflictError("integration item does not require a refold")
        candidate = self.database.get_candidate(queue["candidate_id"])
        origin = CandidateManifest.from_mapping(candidate["manifest"])
        repository_path = self.database.get_candidate_repository_path(
            queue["candidate_id"]
        )
        if repository_path is None:
            raise ConflictError("integration candidate has no recovery repository")
        result, blobs = result_store.load_bundle(
            origin.attempt_id,
            expected_digest=origin.result_content_sha256,
        )
        materialized = candidate_store.refold(
            origin,
            Path(repository_path),
            result,
            blobs,
            base_revision=queue["current_revision"],
        )
        manifest = FoldManifest.create(
            candidate,
            integration_ref=queue["integration_ref"],
            generation=self.database.next_integration_fold_generation(queue_item_id),
            materialized=materialized,
        )
        return self.database.record_integration_fold(
            queue_item_id,
            manifest,
            expected_version=expected_version,
            created_by=creator["id"],
        )

    def activate_integration_fold(
        self,
        queue_item_id: str,
        *,
        fold_id: str,
        policy_id: str,
        expected_version: int,
        actor_handle: str,
    ) -> dict[str, Any]:
        actor = self.database.get_user(actor_handle)
        return self.database.activate_integration_fold(
            queue_item_id,
            fold_id=fold_id,
            policy_id=policy_id,
            expected_version=expected_version,
            actor=actor["id"],
        )

    def claim_integration_item(
        self,
        queue_item_id: str,
        *,
        owner_id: str,
        expected_version: int,
        lease_seconds: int,
    ) -> dict[str, Any]:
        return self.database.claim_integration_item(
            queue_item_id,
            owner_id=owner_id,
            expected_version=expected_version,
            lease_seconds=lease_seconds,
        )

    def release_integration_claim(
        self,
        queue_item_id: str,
        *,
        owner_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        return self.database.release_integration_claim(
            queue_item_id,
            owner_id=owner_id,
            expected_version=expected_version,
        )

    def cancel_integration_item(
        self,
        queue_item_id: str,
        *,
        expected_version: int,
        cancelled_by_handle: str,
    ) -> dict[str, Any]:
        canceller = self.database.get_user(cancelled_by_handle)
        return self.database.cancel_integration_item(
            queue_item_id,
            expected_version=expected_version,
            cancelled_by=canceller["id"],
        )

    def issue_landing_plan(
        self,
        queue_item_id: str,
        *,
        owner_id: str,
        expected_version: int,
        remote: str,
        issued_by_handle: str,
        result_store: WorkspaceResultStore,
        candidate_store: GitCandidateStore,
        workflow_execution_id: str | None = None,
        workflow_execution_version: int | None = None,
    ) -> dict[str, Any]:
        """Issue argv-only authority for the exact policy revision."""

        issuer = self.database.get_user(issued_by_handle)
        if issuer["active"] != 1:
            raise ConflictError("landing plan issuer is inactive")
        queue = self.database.get_integration_queue_item(queue_item_id)
        revision = self.database.get_gate_revision(queue["current_policy_id"])
        manifest = CandidateManifest.from_mapping(revision["manifest"])
        repository_path = revision["repository_path"]
        if repository_path is None:
            raise ConflictError("integration candidate has no recovery repository")
        ensure_candidate_revision(
            self.database,
            result_store,
            candidate_store,
            manifest,
            Path(repository_path),
        )
        return self.database.issue_landing_plan(
            queue_item_id,
            owner_id=owner_id,
            expected_version=expected_version,
            candidate_repository=candidate_store.repository_path(
                manifest.project_id, manifest.object_format
            ),
            remote=remote,
            issued_by=issuer["id"],
            workflow_execution_id=workflow_execution_id,
            workflow_execution_version=workflow_execution_version,
        )

    def record_landing_receipt(
        self, request: object, *, recorded_by_handle: str
    ) -> dict[str, Any]:
        recorder = self.database.get_user(recorded_by_handle)
        receipt_request = LandingReceiptRequest.from_mapping(request)
        return self.database.record_landing_receipt(
            receipt_request, recorded_by=recorder["id"]
        )

    def issue_provider_landing(
        self,
        queue_item_id: str,
        *,
        owner_id: str,
        expected_version: int,
        resource_id: str,
        merge_method: str,
        issued_by_handle: str,
        workflow_execution_id: str | None = None,
        workflow_execution_version: int | None = None,
    ) -> dict[str, Any]:
        issuer = self.database.get_user(issued_by_handle)
        queue = self.database.get_integration_queue_item(queue_item_id)
        return self.database.issue_provider_landing(
            queue_item_id,
            owner_id=owner_id,
            expected_version=expected_version,
            resource_id=resource_id,
            parameters={
                "assurance": "head_enforced",
                "integration_ref": queue["integration_ref"],
                "merge_method": merge_method,
            },
            issued_by=issuer["id"],
            workflow_execution_id=workflow_execution_id,
            workflow_execution_version=workflow_execution_version,
        )

    def get_provider_landing(self, provider_landing_id: str) -> dict[str, Any]:
        landing = self.database.get_provider_landing(provider_landing_id)
        return {
            "provider_landing": landing,
            "operation": self.database.get_service_operation(
                landing["service_operation_id"]
            ),
        }

    def settle_provider_landing(
        self, provider_landing_id: str, *, settled_by_handle: str
    ) -> dict[str, Any]:
        settler = self.database.get_user(settled_by_handle)
        return self.database.settle_provider_landing(
            provider_landing_id, settled_by=settler["id"]
        )

    def execute_provider_landing(
        self,
        provider_landing_id: str,
        *,
        owner_id: str,
        settled_by_handle: str,
    ) -> dict[str, Any]:
        landing = self.database.get_provider_landing(provider_landing_id)
        if landing["state"] != "pending":
            raise ConflictError("provider landing is already resolved")
        operation = self.database.get_service_operation(landing["service_operation_id"])
        if operation["state"] in {"pending", "retryable", "throttled"}:
            operation = self.execute_service_operation(
                operation["id"],
                owner_id=owner_id,
                expected_version=operation["version"],
            )
        elif operation["state"] == "uncertain":
            operation = self.reconcile_service_operation(
                operation["id"],
                owner_id=owner_id,
                expected_version=operation["version"],
            )
        if operation["state"] not in {"succeeded", "denied", "conflict"}:
            raise ConflictError("provider landing operation is not settled")
        return self.settle_provider_landing(
            provider_landing_id, settled_by_handle=settled_by_handle
        )

    def get_integration_queue_item(self, queue_item_id: str) -> dict[str, Any]:
        return self.database.get_integration_queue_item(queue_item_id)

    def get_landing_plan(self, plan_id: str) -> dict[str, Any]:
        return self.database.get_landing_plan(plan_id)

    def reissue_landing_plan_nonce(
        self,
        plan_id: str,
        *,
        expected_generation: int,
        reissued_by_handle: str,
    ) -> dict[str, Any]:
        issuer = self.database.get_user(reissued_by_handle)
        return self.database.reissue_landing_plan_nonce(
            plan_id,
            expected_generation=expected_generation,
            reissued_by=issuer["id"],
        )

    def list_integration_queue(
        self, *, project_key: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        project_id = None
        if project_key is not None:
            project_id = self.database.get_project(project_key)["id"]
        return self.database.list_integration_queue(project_id=project_id, limit=limit)

    def get_gate_policy(self, policy_id: str) -> dict[str, Any]:
        return self.database.get_gate_policy(policy_id)

    def list_gate_policies(
        self,
        *,
        candidate_id: str | None = None,
        project_key: str | None = None,
        work_thread_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        project_id = None
        if project_key is not None:
            project_id = self.database.get_project(project_key)["id"]
        return self.database.list_gate_policies(
            candidate_id=candidate_id,
            project_id=project_id,
            work_thread_id=work_thread_id,
            limit=limit,
        )

    def record_review_evidence(
        self,
        policy_id: str,
        *,
        reviewer_attempt_id: str,
        outcome: str,
        recorded_by_handle: str,
        expected_policy_revision: str,
    ) -> dict[str, Any]:
        """Record bounded review evidence using provenance read from the ledger."""

        recorder = self.database.get_user(recorded_by_handle)
        if recorder["active"] != 1:
            raise ConflictError("gate evidence recorder is inactive")
        provenance = self.database.get_gate_review_provenance(
            policy_id, reviewer_attempt_id
        )
        policy_record = provenance["policy"]
        if policy_record["policy_revision"] != expected_policy_revision:
            raise ConflictError("gate policy revision changed")
        manifest = GatePolicyManifest.from_mapping(policy_record["manifest"])
        if manifest.policy.review_separation is None:
            raise ConflictError("gate policy does not require independent review")
        try:
            gate_outcome = GateOutcome(outcome)
        except (TypeError, ValueError) as exc:
            raise ValidationError("review outcome must be passed or failed") from exc
        evidence = ReviewEvidence(
            schema_version=GATE_EVIDENCE_SCHEMA_VERSION,
            inputs=ExactGateInputs.from_policy(policy_id, manifest),
            outcome=gate_outcome,
            separation=manifest.policy.review_separation,
            producer=provenance["producer"],
            reviewer=provenance["reviewer"],
        )
        return self.database.record_gate_evidence(evidence, recorded_by=recorder["id"])

    def record_maintainer_review(
        self,
        policy_id: str,
        *,
        review_request: object,
        recorded_by_handle: str,
        expected_policy_revision: str,
    ) -> dict[str, Any]:
        """Record one authorized product disposition for an exact candidate."""

        recorder = self.database.get_user(recorded_by_handle)
        if recorder["active"] != 1:
            raise ConflictError("maintainer review recorder is inactive")
        policy_record = self.database.get_gate_policy(policy_id)
        if policy_record["policy_revision"] != expected_policy_revision:
            raise ConflictError("gate policy revision changed")
        manifest = GatePolicyManifest.from_mapping(policy_record["manifest"])
        request = MaintainerReviewRequest.from_mapping(review_request)
        review = MaintainerReview.bind(policy_id, manifest, request)
        return self.database.record_maintainer_review(
            review, recorded_by=recorder["id"]
        )

    def create_review_run(
        self,
        policy_id: str,
        *,
        profile_name: str,
        requested_by_handle: str,
        expected_policy_revision: str,
        krab_session_id: str | None = None,
        krab_claim_id: str | None = None,
        supervision_policy: SupervisionPolicy | None = None,
    ) -> dict[str, Any]:
        """Create a run durably bound to one policy's exact candidate tree."""

        requester = self.database.get_user(requested_by_handle)
        if requester["active"] != 1:
            raise ConflictError("review run requester is inactive")
        target = self.database.get_gate_review_target(policy_id)
        if target["policy_revision"] != expected_policy_revision:
            raise ConflictError("gate policy revision changed")
        if target["review_separation"] is None:
            raise ConflictError("gate policy does not require independent review")
        return self.database.create_run(
            project_key=target["project_key"],
            profile_name=profile_name,
            work_item_id=target["work_item_id"],
            requested_by=requester["id"],
            source_revision=target["head_revision"],
            krab_session_id=krab_session_id,
            krab_claim_id=krab_claim_id,
            supervision_policy=supervision_policy,
            gate_policy_id=policy_id,
        )

    def create_changes_run(
        self,
        policy_id: str,
        *,
        review_evidence_id: str | None = None,
        review_entry_id: str | None = None,
        maintainer_review_id: str | None = None,
        profile_name: str,
        requested_by_handle: str,
        expected_policy_revision: str,
        krab_session_id: str | None = None,
        krab_claim_id: str | None = None,
        supervision_policy: SupervisionPolicy | None = None,
    ) -> dict[str, Any]:
        """Return a changes-requested candidate and exact feedback to a developer."""

        requester = self.database.get_user(requested_by_handle)
        if requester["active"] != 1:
            raise ConflictError("changes-requested run requester is inactive")
        target = self.database.get_gate_review_target(policy_id)
        if target["policy_revision"] != expected_policy_revision:
            raise ConflictError("gate policy revision changed")
        if review_evidence_id is not None and target["review_separation"] is None:
            raise ConflictError("gate policy does not require independent review")
        return self.database.create_run(
            project_key=target["project_key"],
            profile_name=profile_name,
            work_item_id=target["work_item_id"],
            requested_by=requester["id"],
            source_revision=target["head_revision"],
            krab_session_id=krab_session_id,
            krab_claim_id=krab_claim_id,
            supervision_policy=supervision_policy,
            change_policy_id=policy_id,
            change_review_evidence_id=review_evidence_id,
            change_review_entry_id=review_entry_id,
            change_maintainer_review_id=maintainer_review_id,
        )

    def gate_readiness(self, policy_id: str) -> dict[str, Any]:
        return self.database.gate_readiness(policy_id)

    def list_gate_evidence(
        self, policy_id: str, *, limit: int = 500
    ) -> list[dict[str, Any]]:
        return self.database.list_gate_evidence(policy_id, limit=limit)

    def list_maintainer_reviews(
        self, policy_id: str, *, limit: int = 500
    ) -> list[dict[str, Any]]:
        return self.database.list_maintainer_reviews(policy_id, limit=limit)

    def run_local_validation(
        self,
        policy_id: str,
        *,
        validation_name: str,
        recorded_by_handle: str,
        expected_policy_revision: str,
        result_store: WorkspaceResultStore,
        candidate_store: GitCandidateStore,
        executor: DockerExecutor,
    ) -> dict[str, Any]:
        """Run one policy-pinned check in an isolated read-only candidate tree."""

        from .executor import SandboxRequest

        recorder = self.database.get_user(recorded_by_handle)
        if recorder["active"] != 1:
            raise ConflictError("gate evidence recorder is inactive")
        policy_record = self.database.get_gate_policy(policy_id)
        if policy_record["policy_revision"] != expected_policy_revision:
            raise ConflictError("gate policy revision changed")
        policy = GatePolicyManifest.from_mapping(policy_record["manifest"])
        validation = policy.policy.validation(validation_name)
        revision = self.database.get_gate_revision(policy_id)
        candidate = CandidateManifest.from_mapping(revision["manifest"])
        if (
            candidate.head_revision != policy.head_revision
            or candidate.tree_revision != policy.tree_revision
            or candidate.base_revision != policy.expected_base_revision
            or candidate.object_format != policy.object_format
        ):
            raise ConflictError("gate policy candidate identity changed")
        repository_path = revision["repository_path"]
        if repository_path is None:
            raise ConflictError("gate policy candidate has no recovery repository")
        ensure_candidate_revision(
            self.database,
            result_store,
            candidate_store,
            candidate,
            Path(repository_path),
        )

        workspace_root = self.database.path.parent / "gate-workspaces"
        try:
            workspace_root.mkdir(mode=0o700, exist_ok=True)
            info = workspace_root.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise ConflictError("gate workspace root must be a directory")
            os.chmod(workspace_root, 0o700)
        except ConflictError:
            raise
        except OSError as exc:
            raise ConflictError("gate workspace root is unavailable") from exc

        sandbox_id = "run_" + secrets.token_hex(16)
        with tempfile.TemporaryDirectory(
            prefix="validation-", dir=workspace_root
        ) as raw_workspace:
            workspace = Path(raw_workspace)
            os.chmod(workspace, 0o700)
            candidate_store.snapshot(candidate, workspace)
            profile = AgentProfileSpec.create(
                image=validation.image_id,
                command=validation.command,
                workspace_mode=WorkspaceMode.READ_ONLY.value,
                network_mode=NetworkMode.NONE.value,
            )
            request = SandboxRequest(
                run_id=sandbox_id,
                profile=profile,
                workspace=workspace,
                launch=RuntimeLaunch(
                    validation.command,
                    (
                        (
                            "COVERAGE_FILE",
                            "/tmp/magic-glass-box-coverage",  # noqa: S108
                        ),
                    ),
                ),
            )
            started = time.monotonic()
            result = executor.execute(request)
            duration_ms = int(round((time.monotonic() - started) * 1000))
            if result.image_id != validation.image_id:
                raise ConflictError("validation image identity changed")
            output = result.stdout.encode("utf-8", "strict")
            evidence = ValidationEvidence(
                schema_version=GATE_EVIDENCE_SCHEMA_VERSION,
                inputs=ExactGateInputs.from_policy(policy_id, policy),
                outcome=(
                    GateOutcome.PASSED if result.exit_code == 0 else GateOutcome.FAILED
                ),
                validation_name=validation.name,
                image_id=result.image_id,
                command_sha256=validation.command_sha256,
                sandbox_id=sandbox_id,
                exit_code=result.exit_code,
                duration_ms=duration_ms,
                output_sha256=hashlib.sha256(output).hexdigest(),
                output_bytes=len(output),
            )
        return self.database.record_gate_evidence(evidence, recorded_by=recorder["id"])

    def create_user(
        self, handle: str, display_name: str | None = None
    ) -> dict[str, Any]:
        return self.database.create_user(handle, display_name or handle)

    def add_krab_instance(self, name: str, base_url: str) -> dict[str, Any]:
        return self.database.create_krab_instance(name, base_url)

    def add_krab_identity(
        self,
        *,
        instance_name: str,
        credential_path: Path,
        local_user_handle: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(self.credential_resolver, PrivateFileCredentialResolver):
            raise TypeError(
                "add_krab_identity with a path requires a private-file resolver"
            )
        reference = self.credential_resolver.reference(credential_path)
        token = self.credential_resolver.resolve(reference)
        instance = self.database.get_krab_instance(instance_name)
        identity = self.client_factory(instance["base_url"], token).whoami()
        user_id = None
        if local_user_handle is not None:
            user_id = self.database.get_user(local_user_handle)["id"]
        return self.database.store_krab_identity(
            instance["id"], identity, reference, user_id=user_id
        )

    def register_inference_connector(
        self,
        *,
        name: str,
        spec: InferenceConnectorSpec,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.register_inference_connector(
            name=name,
            spec=spec,
            created_by=creator["id"],
        )

    def register_service_sidecar(
        self,
        *,
        name: str,
        spec: ServiceSidecarSpec,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.register_service_sidecar(
            name=name,
            spec=spec,
            created_by=creator["id"],
        )

    def revoke_service_sidecar(
        self,
        name: str,
        *,
        expected_version: int,
        revoked_by_handle: str,
    ) -> dict[str, Any]:
        revoker = self.database.get_user(revoked_by_handle)
        return self.database.revoke_service_sidecar(
            name,
            expected_version=expected_version,
            revoked_by=revoker["id"],
        )

    def bind_project_capability(
        self,
        *,
        project_key: str,
        capability_name: str,
        capability_version: int,
        sidecar_name: str,
        mapping_name: str,
        bound_by_handle: str,
    ) -> dict[str, Any]:
        binder = self.database.get_user(bound_by_handle)
        return self.database.bind_project_capability(
            project_key=project_key,
            capability_name=capability_name,
            capability_version=capability_version,
            sidecar_name=sidecar_name,
            mapping_name=mapping_name,
            bound_by=binder["id"],
        )

    def unbind_project_capability(
        self,
        binding_id: str,
        *,
        expected_version: int,
        unbound_by_handle: str,
    ) -> dict[str, Any]:
        unbinder = self.database.get_user(unbound_by_handle)
        return self.database.unbind_project_capability(
            binding_id,
            expected_version=expected_version,
            unbound_by=unbinder["id"],
        )

    def resolve_project_capability(
        self,
        project_key: str,
        capability_name: str,
        capability_version: int,
    ) -> dict[str, Any]:
        return self.database.resolve_project_capability(
            project_key, capability_name, capability_version
        )

    def create_service_operation(
        self,
        *,
        spec: ServiceOperationSpec,
        created_by_handle: str,
        project_capability_binding_id: str | None = None,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.create_service_operation(
            spec=spec,
            created_by=creator["id"],
            project_capability_binding_id=project_capability_binding_id,
        )

    def create_execution_submission(
        self,
        *,
        project_key: str,
        candidate_id: str,
        recipe: ExecutionRecipeAuthority,
        request_key: str,
        created_by_handle: str,
    ) -> dict[str, Any]:
        """Create one exact, project-bound remote execution submission."""

        creator = self.database.get_user(created_by_handle)
        if creator["active"] != 1:
            raise ConflictError("execution operation creator is inactive")
        candidate = self.database.get_candidate(candidate_id)
        binding = self.database.resolve_project_capability(
            project_key, "execution.submit", 1
        )
        sidecar = self.database.get_service_sidecar(
            binding["controller"]["sidecar_name"]
        )
        operation = build_execution_submission(
            candidate=candidate,
            binding=binding,
            sidecar=sidecar,
            recipe=recipe,
            request_key=request_key,
        )
        return self.database.create_service_operation(
            spec=operation,
            created_by=creator["id"],
            project_capability_binding_id=binding["id"],
        )

    def create_execution_followup(
        self,
        *,
        submission_operation_id: str,
        action: str,
        request_key: str,
        created_by_handle: str,
    ) -> dict[str, Any]:
        """Create an inspect/cancel/collect operation from submission provenance."""

        if action not in {"inspect", "cancel", "collect"}:
            raise ValidationError("execution follow-up action is invalid")
        creator = self.database.get_user(created_by_handle)
        if creator["active"] != 1:
            raise ConflictError("execution operation creator is inactive")
        submission = self.database.get_service_operation(submission_operation_id)
        source_binding_id = submission["project_capability_binding_id"]
        if source_binding_id is None:
            raise ConflictError("execution submission is not project-bound")
        source_binding = self.database.get_project_capability_binding(source_binding_id)
        binding = self.database.resolve_project_capability(
            source_binding["project"]["key"], f"execution.{action}", 1
        )
        sidecar = self.database.get_service_sidecar(
            binding["controller"]["sidecar_name"]
        )
        operation = build_execution_followup(
            submission=submission,
            submission_binding=source_binding,
            binding=binding,
            sidecar=sidecar,
            action=action,
            request_key=request_key,
        )
        return self.database.create_service_operation(
            spec=operation,
            created_by=creator["id"],
            project_capability_binding_id=binding["id"],
        )

    def execute_service_operation(
        self,
        operation_id: str,
        *,
        owner_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        return self._dispatch_service_operation(
            operation_id,
            phase="invoke",
            owner_id=owner_id,
            expected_version=expected_version,
        )

    def reconcile_service_operation(
        self,
        operation_id: str,
        *,
        owner_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        return self._dispatch_service_operation(
            operation_id,
            phase="reconcile",
            owner_id=owner_id,
            expected_version=expected_version,
        )

    def _dispatch_service_operation(
        self,
        operation_id: str,
        *,
        phase: str,
        owner_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        operation = self.database.get_service_operation(operation_id)
        try:
            spec = ServiceOperationSpec.from_mapping(operation["spec"])
        except (KeyError, TypeError, ValidationError) as exc:
            raise ConflictError("service operation authority is unavailable") from exc
        lease_seconds = max(30, (spec.timeout_ms + 999) // 1000 + 5)
        dispatch = self.database.begin_service_operation_attempt(
            operation_id,
            phase=phase,
            owner_id=owner_id,
            expected_version=expected_version,
            lease_seconds=lease_seconds,
        )
        attempt_id = dispatch["attempt"]["id"]
        dispatch_version = dispatch["operation"]["version"]
        request = ServiceOperationRequest.from_mapping(dispatch["request"])
        socket_path = Path(
            dispatch["operation"]["sidecar"]["spec"]["transport"]["socket_path"]
        )
        try:
            client = self.service_client_factory(socket_path)
        except Exception as exc:
            unavailable = ServiceSidecarUnavailableError(
                "service sidecar is unavailable"
            )
            self.database.fail_service_operation_attempt(
                attempt_id,
                owner_id=owner_id,
                expected_operation_version=dispatch_version,
                outcome="unavailable",
                error_code="sidecar_unavailable",
            )
            raise unavailable from exc
        try:
            received = client.invoke(request)
            if not isinstance(received, ServiceOperationResponse):
                raise ServiceSidecarAmbiguousError(
                    "service sidecar returned an invalid response type"
                )
            try:
                response = ServiceOperationResponse.from_mapping(received.to_dict())
                response.validate_correlation(request)
            except ValidationError as exc:
                raise ServiceSidecarAmbiguousError(
                    "service sidecar returned an invalid response"
                ) from exc
        except ServiceSidecarUnavailableError:
            self.database.fail_service_operation_attempt(
                attempt_id,
                owner_id=owner_id,
                expected_operation_version=dispatch_version,
                outcome="unavailable",
                error_code="sidecar_unavailable",
            )
            raise
        except ServiceSidecarAmbiguousError as exc:
            self.database.fail_service_operation_attempt(
                attempt_id,
                owner_id=owner_id,
                expected_operation_version=dispatch_version,
                outcome="ambiguous",
                error_code="sidecar_ambiguous",
            )
            if request.capability.mode == "read":
                raise ServiceSidecarRetryableError(
                    "service sidecar read is retryable"
                ) from exc
            raise
        except ServiceSidecarError as exc:
            self.database.fail_service_operation_attempt(
                attempt_id,
                owner_id=owner_id,
                expected_operation_version=dispatch_version,
                outcome="ambiguous",
                error_code="sidecar_error",
            )
            if request.capability.mode == "read":
                raise ServiceSidecarRetryableError(
                    "service sidecar read is retryable"
                ) from exc
            raise ServiceSidecarAmbiguousError(
                "service sidecar outcome is ambiguous"
            ) from exc
        except Exception as exc:
            self.database.fail_service_operation_attempt(
                attempt_id,
                owner_id=owner_id,
                expected_operation_version=dispatch_version,
                outcome="ambiguous",
                error_code="sidecar_failure",
            )
            if request.capability.mode == "read":
                raise ServiceSidecarRetryableError(
                    "service sidecar read is retryable"
                ) from exc
            raise ServiceSidecarAmbiguousError(
                "service sidecar outcome is ambiguous"
            ) from exc
        try:
            return self.database.complete_service_operation_attempt(
                attempt_id,
                owner_id=owner_id,
                expected_operation_version=dispatch_version,
                response=response,
            )
        except Exception as exc:
            with suppress(Exception):
                self.database.fail_service_operation_attempt(
                    attempt_id,
                    owner_id=owner_id,
                    expected_operation_version=dispatch_version,
                    outcome="ambiguous",
                    error_code="finalization_failed",
                )
            if request.capability.mode == "read":
                raise ServiceSidecarRetryableError(
                    "service sidecar read result could not be finalized"
                ) from exc
            raise ServiceSidecarAmbiguousError(
                "service sidecar outcome could not be finalized"
            ) from exc

    def add_connector_provider_grant(
        self,
        *,
        name: str,
        spec: InferenceGrantSpec,
        expires_at: str | None,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.create_connector_provider_grant(
            name=name,
            spec=spec,
            expires_at=expires_at,
            created_by=creator["id"],
        )

    def revoke_inference_connector(
        self,
        name: str,
        *,
        expected_version: int,
        revoked_by_handle: str,
    ) -> dict[str, Any]:
        revoker = self.database.get_user(revoked_by_handle)
        return self.database.revoke_inference_connector(
            name,
            expected_version=expected_version,
            revoked_by=revoker["id"],
        )

    def revoke_provider_grant(
        self,
        name: str,
        *,
        expected_version: int,
        revoked_by_handle: str,
    ) -> dict[str, Any]:
        revoker = self.database.get_user(revoked_by_handle)
        return self.database.revoke_provider_grant(
            name,
            expected_version=expected_version,
            revoked_by=revoker["id"],
        )

    def invoke_provider(
        self,
        run_id: str,
        *,
        grant_name: str,
        request: dict[str, Any],
    ) -> AcceptedProviderResponse:
        request = dict(request)
        body = request.get("body")
        if isinstance(body, dict) and "model" in body:
            request["body"] = {
                key: value for key, value in body.items() if key != "model"
            }
        route = self.database.get_run_transport(run_id)
        live_attempt_has_authority = (
            route["state"] in (RunState.RUNNING.value, RunState.WAITING_FOR_INPUT.value)
            and route["active_attempt_state"] == "running"
        )
        if not live_attempt_has_authority:
            raise ConflictError(f"run is not running: {route['state']}")
        grant = self.database.get_run_provider_grant(run_id, grant_name)
        started_at = self.database.now()
        invocation_id = self.database.new_id("pinv")
        if grant["grant_kind"] != "connector":
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                invocation_id=invocation_id,
                outcome="denied",
                error_code="legacy_direct_denied",
                started_at=started_at,
            )
            raise ProviderError("provider authority must be connector-bound")
        try:
            spec = InferenceGrantSpec.from_mapping(grant["policy"])
        except (KeyError, TypeError, ValidationError) as exc:
            raise ConflictError("provider grant policy is unavailable") from exc
        deadline = (
            datetime.fromisoformat(started_at) + timedelta(milliseconds=spec.timeout_ms)
        ).isoformat(timespec="microseconds")
        connector = grant["connector"]
        pinned_authority = {
            "connector_id": connector["id"],
            "connector_protocol_version": connector["connector_protocol_version"],
            "deployment_sha256": spec.deployment_sha256,
            "route": spec.route,
            "policy_sha256": spec.digest,
            "credential_scope_sha256": spec.credential_scope_sha256,
            "provider": spec.provider,
            "protocol": spec.protocol,
            "model": spec.model,
            "request_bytes": spec.request_bytes,
            "response_bytes": spec.response_bytes,
            "timeout_ms": spec.timeout_ms,
        }
        try:
            request_bytes = len(canonical_json(request).encode("utf-8"))
        except ValidationError:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                invocation_id=invocation_id,
                outcome="denied",
                error_code="invalid_request",
                started_at=started_at,
                authority=pinned_authority,
                deadline=deadline,
            )
            raise
        if request_bytes > spec.request_bytes:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                invocation_id=invocation_id,
                outcome="denied",
                error_code="request_too_large",
                started_at=started_at,
                request_bytes=request_bytes,
                authority=pinned_authority,
                deadline=deadline,
            )
            raise ValidationError("provider request exceeds its size limit")
        try:
            reservation = self.database.reserve_provider_dispatch(
                run_id,
                grant_name,
                expected_grant_id=grant["id"],
                lease_seconds=PROVIDER_DISPATCH_LEASE_SECONDS,
            )
        except (ConflictError, NotFoundError):
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                invocation_id=invocation_id,
                outcome="denied",
                error_code="authority_unavailable",
                started_at=started_at,
                authority=pinned_authority,
                deadline=deadline,
            )
            raise
        if reservation is None:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                invocation_id=invocation_id,
                outcome="busy",
                error_code="concurrency_limit",
                started_at=started_at,
                authority=pinned_authority,
                deadline=deadline,
            )
            raise ProviderBusyError("provider grant concurrency limit is reached")
        authority = reservation["inference"]
        try:
            invocation_value: dict[str, Any] = {
                "schema_version": INFERENCE_SCHEMA_VERSION,
                "connector_protocol_version": authority["connector_protocol_version"],
                "invocation_id": invocation_id,
                "connector_id": authority["connector_id"],
                "deployment_sha256": authority["deployment_sha256"],
                "route": authority["route"],
                "grant_id": grant["id"],
                "policy_sha256": authority["policy_sha256"],
                "credential_scope_sha256": authority["credential_scope_sha256"],
                "provider": authority["provider"],
                "protocol": authority["protocol"],
                "model": authority["model"],
                "deadline": deadline,
                "request_limit_bytes": authority["request_bytes"],
                "response_limit_bytes": authority["response_bytes"],
                "request": request,
            }
            if spec.budget is not None:
                invocation_value["budget"] = (
                    spec.budget.invocation_authority().to_dict()
                )
            invocation_request = InferenceInvocationRequest.from_mapping(
                invocation_value
            )
        except (KeyError, TypeError, ValidationError) as exc:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                invocation_id=invocation_id,
                outcome="denied",
                error_code="connector_authority_invalid",
                started_at=started_at,
                authority=authority,
                deadline=deadline,
            )
            try:
                self.database.release_provider_dispatch(reservation["id"])
            except Exception as release_error:
                exc.add_note(
                    "provider dispatch reservation release also failed: "
                    f"{type(release_error).__name__}: {release_error}"
                )
            raise ProviderError("provider connector authority is invalid") from exc
        try:
            connector_client = self.inference_client_factory(
                Path(authority["socket_path"])
            )
        except Exception as exc:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                invocation_id=invocation_id,
                outcome="failed",
                error_code="connector_unavailable",
                started_at=started_at,
                authority=authority,
                deadline=deadline,
            )
            try:
                self.database.release_provider_dispatch(reservation["id"])
            except Exception as release_error:
                exc.add_note(
                    "provider dispatch reservation release also failed: "
                    f"{type(release_error).__name__}: {release_error}"
                )
            raise ProviderUpstreamError("inference connector is unavailable") from exc
        try:
            self.database.record_provider_invocation(
                run_id=run_id,
                provider_grant_id=grant["id"],
                invocation_id=invocation_id,
                dispatch_kind="connector",
                inference_connector_id=authority["connector_id"],
                connector_protocol_version=authority["connector_protocol_version"],
                deployment_sha256=authority["deployment_sha256"],
                route=authority["route"],
                policy_sha256=authority["policy_sha256"],
                credential_scope_sha256=authority["credential_scope_sha256"],
                provider=authority["provider"],
                protocol=authority["protocol"],
                model=authority["model"],
                deadline=deadline,
                outcome="dispatching",
                request_bytes=0,
                response_bytes=0,
                http_status=None,
                error_code=None,
                started_at=started_at,
                budget=spec.budget,
            )
        except BudgetExceededError as exc:
            try:
                self.database.release_provider_dispatch(reservation["id"])
            except Exception as release_error:
                exc.add_note(
                    "provider dispatch reservation release also failed: "
                    f"{type(release_error).__name__}: {release_error}"
                )
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                invocation_id=invocation_id,
                outcome="denied",
                error_code="budget_exhausted",
                started_at=started_at,
                authority=authority,
                deadline=deadline,
            )
            raise ProviderBudgetError("provider budget is exhausted") from exc
        except InvocationLimitExceededError as exc:
            try:
                self.database.release_provider_dispatch(reservation["id"])
            except Exception as release_error:
                exc.add_note(
                    "provider dispatch reservation release also failed: "
                    f"{type(release_error).__name__}: {release_error}"
                )
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                invocation_id=invocation_id,
                outcome="denied",
                error_code="invocation_limit_exhausted",
                started_at=started_at,
                authority=authority,
                deadline=deadline,
            )
            raise ProviderInvocationLimitError(
                "provider invocation limit is exhausted"
            ) from exc
        except Exception as exc:
            try:
                self.database.release_provider_dispatch(reservation["id"])
            except Exception as release_error:
                exc.add_note(
                    "provider dispatch reservation release also failed: "
                    f"{type(release_error).__name__}: {release_error}"
                )
            raise ProviderError("provider dispatch could not be recorded") from exc
        dispatch_monitor = _ProviderDispatchLeaseMonitor(
            self.database,
            reservation["id"],
            lease_seconds=PROVIDER_DISPATCH_LEASE_SECONDS,
            interval=PROVIDER_DISPATCH_RENEW_INTERVAL_SECONDS,
        )
        try:
            dispatch_monitor.start()
            received_response = connector_client.invoke(invocation_request)
            if not isinstance(received_response, InferenceInvocationResponse):
                raise InferenceConnectorAmbiguousError(
                    "inference connector returned an invalid response type"
                )
            try:
                response = InferenceInvocationResponse.from_mapping(
                    received_response.to_dict()
                )
                response.validate_correlation(invocation_request)
            except ValidationError as exc:
                raise InferenceConnectorAmbiguousError(
                    "inference connector returned an invalid response"
                ) from exc
            dispatch_monitor.close()
            dispatch_monitor.check()
        except InferenceConnectorUnavailableError as exc:
            self._finalize_provider_invocation(
                invocation_id=invocation_id,
                reservation_id=reservation["id"],
                outcome="failed",
                error_code="connector_unavailable",
                release_budget=True,
            )
            raise ProviderUpstreamError("inference connector is unavailable") from exc
        except InferenceConnectorAmbiguousError as exc:
            self._finalize_provider_invocation(
                invocation_id=invocation_id,
                reservation_id=reservation["id"],
                outcome="ambiguous",
                error_code="connector_ambiguous",
            )
            raise ProviderAmbiguousError(
                "provider invocation outcome is ambiguous"
            ) from exc
        except InferenceConnectorError as exc:
            self._finalize_provider_invocation(
                invocation_id=invocation_id,
                reservation_id=reservation["id"],
                outcome="denied",
                error_code="connector_denied",
                release_budget=True,
            )
            raise ProviderError("provider connector denied the request") from exc
        except Exception as exc:
            self._finalize_provider_invocation(
                invocation_id=invocation_id,
                reservation_id=reservation["id"],
                outcome="ambiguous",
                error_code="reservation_lost",
            )
            raise ProviderAmbiguousError(
                "provider invocation outcome is ambiguous"
            ) from exc
        finally:
            dispatch_monitor.close()
        if response.outcome != "succeeded":
            recoverable = (
                None
                if response.outcome != "failed" or response.error_code is None
                else provider_recovery_error(
                    error_code=response.error_code,
                    status_code=response.provider_status,
                    retry_after_seconds=response.retry_after_seconds,
                    request_bytes=response.request_bytes,
                    response_bytes=response.response_bytes,
                )
            )
            self._finalize_provider_invocation(
                invocation_id=invocation_id,
                reservation_id=reservation["id"],
                outcome=response.outcome,
                error_code=response.error_code or "connector_failed",
                request_bytes=response.request_bytes,
                response_bytes=response.response_bytes,
                http_status=response.provider_status,
                usage=response.usage,
                recovery_category=(
                    None if recoverable is None else recoverable.category
                ),
                retry_after_seconds=(
                    None if recoverable is None else recoverable.retry_after_seconds
                ),
            )
            if response.outcome == "ambiguous":
                raise ProviderAmbiguousError(
                    "provider invocation outcome is ambiguous",
                    status_code=response.provider_status,
                    request_bytes=response.request_bytes,
                    response_bytes=response.response_bytes,
                )
            if recoverable is not None:
                raise recoverable
            raise ProviderUpstreamError(
                "provider request failed",
                status_code=response.provider_status,
                request_bytes=response.request_bytes,
                response_bytes=response.response_bytes,
            )
        untrusted_response = response.untrusted_response
        if untrusted_response is None:
            self._finalize_provider_invocation(
                invocation_id=invocation_id,
                reservation_id=reservation["id"],
                outcome="ambiguous",
                error_code="connector_response_missing",
            )
            raise ProviderAmbiguousError("provider invocation outcome is ambiguous")
        invocation = self._finalize_provider_invocation(
            invocation_id=invocation_id,
            reservation_id=reservation["id"],
            outcome="succeeded",
            request_bytes=response.request_bytes,
            response_bytes=response.response_bytes,
            http_status=response.provider_status,
            error_code=None,
            usage=response.usage,
        )
        estimated_microusd = (
            None
            if spec.budget is None or response.usage is None
            else spec.budget.estimate_microusd(response.usage)
        )
        return AcceptedProviderResponse(
            invocation_id=invocation["id"],
            grant_name=grant_name,
            provider=spec.provider,
            model=spec.model,
            status_code=response.provider_status,
            request_bytes=response.request_bytes,
            response_bytes=response.response_bytes,
            usage=response.usage,
            estimated_microusd=estimated_microusd,
            untrusted_response=untrusted_response,
        )

    def _finalize_provider_invocation(
        self,
        *,
        invocation_id: str,
        reservation_id: str,
        outcome: str,
        error_code: str | None,
        request_bytes: int = 0,
        response_bytes: int = 0,
        http_status: int | None = None,
        usage: InferenceUsage | None = None,
        release_budget: bool = False,
        recovery_category: str | None = None,
        retry_after_seconds: int | None = None,
    ) -> dict[str, Any]:
        try:
            invocation = self.database.finalize_provider_invocation(
                invocation_id,
                outcome=outcome,
                request_bytes=request_bytes,
                response_bytes=response_bytes,
                http_status=http_status,
                error_code=error_code,
                usage=usage,
                release_budget=release_budget,
                recovery_category=recovery_category,
                retry_after_seconds=retry_after_seconds,
            )
        except Exception as exc:
            raise ProviderAmbiguousError(
                "provider invocation outcome is ambiguous"
            ) from exc
        # The durable invocation outcome is authoritative. A reservation is a
        # bounded lease, so release failure can only reduce capacity briefly.
        with suppress(Exception):
            self.database.release_provider_dispatch(reservation_id)
        return invocation

    def _record_provider_failure(
        self,
        *,
        run_id: str,
        grant_id: str,
        invocation_id: str,
        outcome: str,
        error_code: str,
        started_at: str,
        request_bytes: int = 0,
        response_bytes: int = 0,
        http_status: int | None = None,
        authority: Mapping[str, Any] | None = None,
        deadline: str | None = None,
    ) -> None:
        connector_metadata: dict[str, Any] = {}
        if authority is not None:
            connector_metadata = {
                "dispatch_kind": "connector",
                "inference_connector_id": authority["connector_id"],
                "connector_protocol_version": authority["connector_protocol_version"],
                "deployment_sha256": authority["deployment_sha256"],
                "route": authority["route"],
                "policy_sha256": authority["policy_sha256"],
                "credential_scope_sha256": authority["credential_scope_sha256"],
                "provider": authority["provider"],
                "protocol": authority["protocol"],
                "model": authority["model"],
                "deadline": deadline,
            }
        self.database.record_provider_invocation(
            run_id=run_id,
            provider_grant_id=grant_id,
            invocation_id=invocation_id,
            outcome=outcome,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
            http_status=http_status,
            error_code=error_code,
            started_at=started_at,
            **connector_metadata,
        )

    def register_project(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
        display_name: str,
        context: ProjectContext,
        created_by_handle: str,
    ) -> dict[str, Any]:
        instance, _, client = self._authenticated_client(instance_name, krab_handle)
        visible = [
            project
            for project in client.projects()
            if project.project_key == project_key
        ]
        if not visible:
            raise KrabError(
                f"Krab identity {krab_handle} cannot see project {project_key}"
            )
        project = visible[0]
        creator = self.database.get_user(created_by_handle)
        return self.database.create_project(
            instance_id=instance["id"],
            krab_project_id=project.project_id,
            krab_project_key=project.project_key,
            display_name=display_name,
            context=context,
            created_by=creator["id"],
        )

    def revise_project_context(
        self,
        *,
        project_key: str,
        context: ProjectContext,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.add_project_context(project_key, context, creator["id"])

    def create_agent_profile(
        self,
        *,
        name: str,
        instance_name: str,
        krab_handle: str,
        spec: AgentProfileSpec,
        created_by_handle: str,
    ) -> dict[str, Any]:
        instance = self.database.get_krab_instance(instance_name)
        identity = self.database.get_krab_identity(instance["id"], krab_handle)
        creator = self.database.get_user(created_by_handle)
        return self.database.create_agent_profile(
            name=name,
            spec=spec,
            krab_identity_id=identity["id"],
            created_by=creator["id"],
        )

    def revise_agent_profile(
        self,
        *,
        name: str,
        instance_name: str,
        krab_handle: str,
        spec: AgentProfileSpec,
        created_by_handle: str,
    ) -> dict[str, Any]:
        instance = self.database.get_krab_instance(instance_name)
        identity = self.database.get_krab_identity(instance["id"], krab_handle)
        creator = self.database.get_user(created_by_handle)
        return self.database.add_agent_profile_version(
            name=name,
            spec=spec,
            krab_identity_id=identity["id"],
            created_by=creator["id"],
        )

    def create_workflow_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: WorkflowConfigurationSpec,
        producer_profile_name: str,
        reviewer_profile_name: str,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.create_workflow_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            producer_profile_name=producer_profile_name,
            reviewer_profile_name=reviewer_profile_name,
            created_by=creator["id"],
        )

    def revise_workflow_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: WorkflowConfigurationSpec,
        producer_profile_name: str,
        reviewer_profile_name: str,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.revise_workflow_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            producer_profile_name=producer_profile_name,
            reviewer_profile_name=reviewer_profile_name,
            created_by=creator["id"],
        )

    def create_discovery_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: DiscoveryConfigurationSpec,
        investigator_profile_name: str,
        coordinator_profile_name: str,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.create_discovery_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            investigator_profile_name=investigator_profile_name,
            coordinator_profile_name=coordinator_profile_name,
            created_by=creator["id"],
        )

    def revise_discovery_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: DiscoveryConfigurationSpec,
        investigator_profile_name: str,
        coordinator_profile_name: str,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.revise_discovery_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            investigator_profile_name=investigator_profile_name,
            coordinator_profile_name=coordinator_profile_name,
            created_by=creator["id"],
        )

    def create_advisory_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: AdvisoryConfigurationSpec,
        reviewer_one_profile_name: str,
        reviewer_two_profile_name: str,
        reviewer_three_profile_name: str,
        coordinator_profile_name: str,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.create_advisory_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            reviewer_one_profile_name=reviewer_one_profile_name,
            reviewer_two_profile_name=reviewer_two_profile_name,
            reviewer_three_profile_name=reviewer_three_profile_name,
            coordinator_profile_name=coordinator_profile_name,
            created_by=creator["id"],
        )

    def revise_advisory_configuration(
        self,
        *,
        project_key: str,
        name: str,
        spec: AdvisoryConfigurationSpec,
        reviewer_one_profile_name: str,
        reviewer_two_profile_name: str,
        reviewer_three_profile_name: str,
        coordinator_profile_name: str,
        created_by_handle: str,
    ) -> dict[str, Any]:
        creator = self.database.get_user(created_by_handle)
        return self.database.revise_advisory_configuration(
            project_key=project_key,
            name=name,
            spec=spec,
            reviewer_one_profile_name=reviewer_one_profile_name,
            reviewer_two_profile_name=reviewer_two_profile_name,
            reviewer_three_profile_name=reviewer_three_profile_name,
            coordinator_profile_name=coordinator_profile_name,
            created_by=creator["id"],
        )

    def get_workflow_configuration(
        self, *, project_key: str, name: str, version: int | None = None
    ) -> dict[str, Any]:
        return self.database.get_workflow_configuration(
            project_key, name, version=version
        )

    def list_workflow_configurations(
        self, *, project_key: str | None = None
    ) -> list[dict[str, Any]]:
        return self.database.list_workflow_configurations(project_key=project_key)

    def get_discovery_configuration(
        self, *, project_key: str, name: str, version: int | None = None
    ) -> dict[str, Any]:
        return self.database.get_discovery_configuration(
            project_key, name, version=version
        )

    def list_discovery_configurations(
        self, *, project_key: str | None = None
    ) -> list[dict[str, Any]]:
        return self.database.list_discovery_configurations(project_key=project_key)

    def get_advisory_configuration(
        self, *, project_key: str, name: str, version: int | None = None
    ) -> dict[str, Any]:
        return self.database.get_advisory_configuration(
            project_key, name, version=version
        )

    def list_advisory_configurations(
        self, *, project_key: str | None = None
    ) -> list[dict[str, Any]]:
        return self.database.list_advisory_configurations(project_key=project_key)

    def start_discovery_execution(
        self,
        *,
        project_key: str,
        configuration_name: str,
        work_item_id: str,
        source_revision: str,
        expected_configuration_version: int,
        expected_context_version_id: str,
        expected_work_item_version_id: str,
        expected_thread_generation: int,
        expected_thread_version: int,
        krab_session_id: str,
        krab_claim_id: str | None,
        idempotency_key: str,
        requested_by_handle: str,
    ) -> dict[str, Any]:
        requester = self.database.get_user(requested_by_handle)
        return self.database.start_discovery_execution(
            project_key=project_key,
            configuration_name=configuration_name,
            work_item_id=work_item_id,
            source_revision=source_revision,
            expected_configuration_version=expected_configuration_version,
            expected_context_version_id=expected_context_version_id,
            expected_work_item_version_id=expected_work_item_version_id,
            expected_thread_generation=expected_thread_generation,
            expected_thread_version=expected_thread_version,
            krab_session_id=krab_session_id,
            krab_claim_id=krab_claim_id,
            idempotency_key=idempotency_key,
            requested_by=requester["id"],
        )

    def get_discovery_execution(self, execution_id: str) -> dict[str, Any]:
        return self.database.get_discovery_execution(execution_id)

    def list_discovery_executions(
        self,
        *,
        project_key: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self.database.list_discovery_executions(
            project_key=project_key, state=state, limit=limit
        )

    def cancel_discovery_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        cancelled_by_handle: str,
        reason: str,
    ) -> dict[str, Any]:
        operator = self.database.get_user(cancelled_by_handle)
        return self.database.cancel_discovery_execution(
            execution_id,
            expected_version=expected_version,
            cancelled_by=operator["id"],
            reason=reason,
        )

    def start_advisory_execution(
        self,
        *,
        project_key: str,
        configuration_name: str,
        candidate_id: str,
        expected_configuration_version: int,
        expected_context_version_id: str,
        expected_work_item_version_id: str,
        expected_thread_generation: int,
        expected_thread_version: int,
        idempotency_key: str,
        requested_by_handle: str,
    ) -> dict[str, Any]:
        requester = self.database.get_user(requested_by_handle)
        return self.database.start_advisory_execution(
            project_key=project_key,
            configuration_name=configuration_name,
            candidate_id=candidate_id,
            expected_configuration_version=expected_configuration_version,
            expected_context_version_id=expected_context_version_id,
            expected_work_item_version_id=expected_work_item_version_id,
            expected_thread_generation=expected_thread_generation,
            expected_thread_version=expected_thread_version,
            idempotency_key=idempotency_key,
            requested_by=requester["id"],
        )

    def get_advisory_execution(self, execution_id: str) -> dict[str, Any]:
        return self.database.get_advisory_execution(execution_id)

    def list_advisory_executions(
        self,
        *,
        project_key: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self.database.list_advisory_executions(
            project_key=project_key, state=state, limit=limit
        )

    def cancel_advisory_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        cancelled_by_handle: str,
        reason: str,
    ) -> dict[str, Any]:
        operator = self.database.get_user(cancelled_by_handle)
        return self.database.cancel_advisory_execution(
            execution_id,
            expected_version=expected_version,
            cancelled_by=operator["id"],
            reason=reason,
        )

    def start_workflow_execution(
        self,
        *,
        project_key: str,
        configuration_name: str,
        work_item_id: str,
        source_revision: str,
        expected_configuration_version: int,
        expected_context_version_id: str,
        expected_work_item_version_id: str,
        expected_thread_generation: int,
        expected_thread_version: int,
        idempotency_key: str,
        requested_by_handle: str,
        discovery_execution_id: str | None = None,
    ) -> dict[str, Any]:
        requester = self.database.get_user(requested_by_handle)
        return self.database.start_workflow_execution(
            project_key=project_key,
            configuration_name=configuration_name,
            work_item_id=work_item_id,
            source_revision=source_revision,
            expected_configuration_version=expected_configuration_version,
            expected_context_version_id=expected_context_version_id,
            expected_work_item_version_id=expected_work_item_version_id,
            expected_thread_generation=expected_thread_generation,
            expected_thread_version=expected_thread_version,
            idempotency_key=idempotency_key,
            requested_by=requester["id"],
            discovery_execution_id=discovery_execution_id,
        )

    def get_workflow_execution(self, execution_id: str) -> dict[str, Any]:
        return self.database.get_workflow_execution(execution_id)

    def list_workflow_executions(
        self,
        *,
        project_key: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self.database.list_workflow_executions(
            project_key=project_key, state=state, limit=limit
        )

    def cancel_workflow_execution(
        self,
        execution_id: str,
        *,
        expected_version: int,
        cancelled_by_handle: str,
        reason: str,
    ) -> dict[str, Any]:
        operator = self.database.get_user(cancelled_by_handle)
        return self.database.cancel_workflow_execution(
            execution_id,
            expected_version=expected_version,
            cancelled_by=operator["id"],
            reason=reason,
        )

    def get_workflow_ruling(self, ruling_id: str) -> dict[str, Any]:
        return self.database.get_workflow_ruling(ruling_id)

    def resolve_workflow_ruling(
        self,
        ruling_id: str,
        *,
        expected_version: int,
        decision: str,
        resolved_by_handle: str,
    ) -> dict[str, Any]:
        resolver = self.database.get_user(resolved_by_handle)
        return self.database.resolve_workflow_ruling(
            ruling_id,
            expected_version=expected_version,
            decision=decision,
            resolved_by=resolver["id"],
        )

    def configure_workflow_provider_landing(
        self,
        execution_id: str,
        *,
        expected_version: int,
        resource_id: str,
        merge_method: str,
        configured_by_handle: str,
    ) -> dict[str, Any]:
        configurator = self.database.get_user(configured_by_handle)
        return self.database.configure_workflow_provider_target(
            execution_id,
            expected_version=expected_version,
            resource_id=resource_id,
            merge_method=merge_method,
            created_by=configurator["id"],
        )

    def issue_workflow_manual_landing(
        self,
        execution_id: str,
        *,
        expected_version: int,
        remote: str,
        issued_by_handle: str,
        result_store: WorkspaceResultStore,
        candidate_store: GitCandidateStore,
    ) -> dict[str, Any]:
        execution = self.database.get_workflow_execution(execution_id)
        if execution["version"] != expected_version:
            raise ConcurrentUpdateError("workflow execution version changed")
        if execution["state"] != "active" or execution["stage"] != "landing":
            raise ConflictError("workflow is not awaiting landing")
        if execution["spec"]["landing_mode"] != "manual":
            raise ConflictError("workflow is configured for provider landing")
        landing = execution["landing"]
        if landing is None:
            raise ConflictError("workflow landing has not been enqueued")
        queue_item_id = landing["queue_item_id"]
        queue = self.database.get_integration_queue_item(queue_item_id)
        owner_id = f"workflow-{execution_id}"
        if queue["state"] == "queued" or (
            queue["state"] == "claimed"
            and queue["owner_id"] == owner_id
            and queue["lease_expires_at"] <= self.database.now()
        ):
            queue = self.claim_integration_item(
                queue_item_id,
                owner_id=owner_id,
                expected_version=queue["version"],
                lease_seconds=300,
            )
        elif queue["state"] != "claimed" or queue["owner_id"] != owner_id:
            raise ConflictError("workflow landing queue is not available")
        return self.issue_landing_plan(
            queue_item_id,
            owner_id=owner_id,
            expected_version=queue["version"],
            remote=remote,
            issued_by_handle=issued_by_handle,
            result_store=result_store,
            candidate_store=candidate_store,
            workflow_execution_id=execution_id,
            workflow_execution_version=expected_version,
        )

    def import_work_item(
        self,
        *,
        project_key: str,
        envelope: WorkItemEnvelope,
        imported_by_handle: str,
    ) -> dict[str, Any]:
        importer = self.database.get_user(imported_by_handle)
        return self.database.import_work_item(
            project_key=project_key,
            envelope=envelope,
            imported_by=importer["id"],
        )

    def import_work_item_from_service_operation(
        self,
        *,
        project_key: str,
        operation_id: str,
        imported_by_handle: str,
    ) -> dict[str, Any]:
        importer = self.database.get_user(imported_by_handle)
        return self.database.import_work_item_from_service_operation(
            project_key=project_key,
            operation_id=operation_id,
            imported_by=importer["id"],
        )

    def get_work_item(self, work_item_id: str) -> dict[str, Any]:
        return self.database.get_work_item(work_item_id)

    def complete_work_item(
        self,
        work_item_id: str,
        *,
        completed_by_handle: str,
        expected_thread_version: int,
        reason: str | None = None,
    ) -> dict[str, Any]:
        operator = self.database.get_user(completed_by_handle)
        return self.database.complete_work_item(
            work_item_id,
            completed_by=operator["id"],
            expected_thread_version=expected_thread_version,
            reason=reason,
        )

    def reopen_work_item(
        self,
        work_item_id: str,
        *,
        reopened_by_handle: str,
        expected_thread_version: int,
    ) -> dict[str, Any]:
        operator = self.database.get_user(reopened_by_handle)
        return self.database.reopen_work_item(
            work_item_id,
            reopened_by=operator["id"],
            expected_thread_version=expected_thread_version,
        )

    def create_run(
        self,
        *,
        project_key: str,
        profile_name: str,
        work_item_id: str,
        requested_by_handle: str,
        source_revision: str | None = None,
        krab_session_id: str | None = None,
        krab_claim_id: str | None = None,
        supervision_policy: SupervisionPolicy | None = None,
    ) -> dict[str, Any]:
        requester = self.database.get_user(requested_by_handle)
        return self.database.create_run(
            project_key=project_key,
            profile_name=profile_name,
            work_item_id=work_item_id,
            requested_by=requester["id"],
            source_revision=source_revision,
            krab_session_id=krab_session_id,
            krab_claim_id=krab_claim_id,
            supervision_policy=supervision_policy,
        )

    def transition_run(
        self,
        run_id: str,
        *,
        target: RunState,
        expected_version: int,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return self.database.transition_run(
            run_id,
            target=target,
            expected_version=expected_version,
            reason=reason,
        )

    def request_run_cancellation(
        self,
        run_id: str,
        *,
        requested_by_handle: str,
        expected_run_version: int,
        reason: str,
    ) -> dict[str, Any]:
        operator = self.database.get_user(requested_by_handle)
        return self.database.request_run_cancellation(
            run_id,
            requested_by=operator["id"],
            expected_run_version=expected_run_version,
            reason=reason,
        )

    def retry_run(
        self,
        run_id: str,
        *,
        requested_by_handle: str,
        expected_run_version: int,
    ) -> dict[str, Any]:
        operator = self.database.get_user(requested_by_handle)
        return self.database.retry_run(
            run_id,
            requested_by=operator["id"],
            expected_run_version=expected_run_version,
        )

    def reassign_run(
        self,
        run_id: str,
        *,
        profile_name: str,
        requested_by_handle: str,
        expected_run_version: int,
    ) -> dict[str, Any]:
        operator = self.database.get_user(requested_by_handle)
        return self.database.reassign_run(
            run_id,
            profile_name=profile_name,
            requested_by=operator["id"],
            expected_run_version=expected_run_version,
        )

    def submit_handoff(self, run_id: str, handoff: HandoffEnvelope) -> dict[str, Any]:
        """Persist bounded replacement state for the run's active attempt."""

        return self.database.create_attempt_handoff(run_id, handoff)

    def submit_discovery_trace(
        self,
        run_id: str,
        *,
        trace: DiscoveryTrace,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self.database.submit_discovery_trace(
            run_id, trace=trace, idempotency_key=idempotency_key
        )

    def submit_discovery_ruling(
        self,
        run_id: str,
        *,
        ruling: DiscoveryRuling,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self.database.submit_discovery_ruling(
            run_id, ruling=ruling, idempotency_key=idempotency_key
        )

    def submit_advisory_blind_review(
        self,
        run_id: str,
        *,
        review: AdvisoryBlindReview,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self.database.submit_advisory_blind_review(
            run_id, review=review, idempotency_key=idempotency_key
        )

    def submit_advisory_challenge_review(
        self,
        run_id: str,
        *,
        review: AdvisoryChallengeReview,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self.database.submit_advisory_challenge_review(
            run_id, review=review, idempotency_key=idempotency_key
        )

    def submit_advisory_report(
        self,
        run_id: str,
        *,
        report: AdvisoryReport,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self.database.submit_advisory_report(
            run_id, report=report, idempotency_key=idempotency_key
        )

    def submit_workflow_scope_review(
        self,
        run_id: str,
        *,
        review: WorkflowScopeReview,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self.database.submit_workflow_scope_review(
            run_id, review=review, idempotency_key=idempotency_key
        )

    def read_work_memory(
        self,
        run_id: str,
        *,
        after_sequence: int | None = None,
        limit: int = 16,
    ) -> dict[str, Any]:
        return self.database.get_work_memory_for_run(
            run_id, after_sequence=after_sequence, limit=limit
        )

    def append_work_log(
        self,
        run_id: str,
        entry: WorkLogEntry,
        *,
        idempotency_key: str,
        assignment_id: str | None = None,
    ) -> dict[str, Any]:
        return self.database.append_work_log(
            run_id,
            entry,
            idempotency_key=idempotency_key,
            assignment_id=assignment_id,
        )

    def checkpoint_work_memory(
        self,
        run_id: str,
        checkpoint: WorkMemoryCheckpoint,
        *,
        through_sequence: int,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self.database.create_work_memory_checkpoint(
            run_id,
            checkpoint,
            through_sequence=through_sequence,
            expected_thread_version=expected_thread_version,
            idempotency_key=idempotency_key,
        )

    def create_work_assignment(
        self,
        run_id: str,
        *,
        target_profile_name: str,
        spec: WorkAssignmentSpec,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self.database.create_work_assignment(
            run_id,
            target_profile_name=target_profile_name,
            spec=spec,
            expected_thread_version=expected_thread_version,
            idempotency_key=idempotency_key,
        )

    def claim_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        lease_seconds: int,
    ) -> dict[str, Any]:
        return self.database.claim_work_assignment(
            run_id,
            assignment_id,
            expected_assignment_version=expected_assignment_version,
            lease_seconds=lease_seconds,
        )

    def complete_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        result: WorkLogEntry,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self.database.complete_work_assignment(
            run_id,
            assignment_id,
            expected_assignment_version=expected_assignment_version,
            result=result,
            idempotency_key=idempotency_key,
        )

    def open_interaction(
        self,
        run_id: str,
        *,
        kind: InteractionKind,
        recipient_krab_handle: str,
        title: str,
        body: str,
        idempotency_key: str,
        expected_run_version: int,
    ) -> dict[str, Any]:
        if not isinstance(kind, InteractionKind):
            raise ValidationError("interaction kind is invalid")
        if (
            isinstance(expected_run_version, bool)
            or not isinstance(expected_run_version, int)
            or expected_run_version < 1
        ):
            raise ValidationError("expected_run_version must be a positive integer")
        route = self.database.get_run_transport(run_id)
        if route["version"] != expected_run_version:
            raise ConcurrentUpdateError(
                f"run version is {route['version']}, expected {expected_run_version}"
            )
        if route["state"] != RunState.RUNNING.value:
            raise ConflictError(f"run is not running: {route['state']}")
        if route["krab_session_id"] is None:
            raise ConflictError("run has no Krab session")
        recipient = self.database.get_krab_identity(
            route["krab_instance_id"], recipient_krab_handle
        )
        if recipient["local_user_id"] is None or not recipient["local_user_active"]:
            raise NotFoundError(
                "recipient Krab identity has no active bound local user"
            )
        _, agent, client = self._authenticated_client(
            route["krab_instance_name"], route["agent_krab_handle"]
        )
        request: dict[str, Any] = {
            "recipient_handle": recipient["handle"],
            "title": title,
            "body": body,
            "idempotency_key": idempotency_key,
        }
        if route["krab_claim_id"] is not None:
            request["claim_id"] = route["krab_claim_id"]
        item = client.create_session_item(
            route["project_key"], route["krab_session_id"], **request
        )
        self._verify_interaction_item(
            item,
            project_key=route["project_key"],
            session_id=route["krab_session_id"],
            creator_user_id=agent["krab_user_id"],
            recipient_user_id=recipient["krab_user_id"],
            item_id=None,
            created=True,
        )
        expected_claim_id = route["krab_claim_id"]
        actual_claim_id = (
            item.work_claim.claim_id if item.work_claim is not None else None
        )
        if actual_claim_id != expected_claim_id:
            raise KrabError("interaction request has an unexpected work-claim link")
        return self.database.open_interaction(
            run_id,
            kind=kind,
            recipient_krab_identity_id=recipient["id"],
            request_item_id=item.item_id,
            expected_run_version=expected_run_version,
        )

    def remind_interaction(
        self,
        interaction_id: str,
        *,
        idempotency_key: str,
        timeout: float = 40.0,
    ) -> int:
        """Send one idempotent bodyless Krab reminder for an open interaction."""

        interaction = self.database.get_interaction(interaction_id)
        if interaction["state"] != "open":
            raise ConflictError(f"interaction is not open: {interaction['state']}")
        route = self.database.get_run_transport(interaction["run_id"])
        if route["krab_session_id"] is None:
            raise ConflictError("run has no Krab session")
        _, _, client = self._authenticated_client(
            route["krab_instance_name"],
            route["agent_krab_handle"],
            timeout=timeout,
        )
        reminder = client.remind_session_item(
            route["project_key"],
            route["krab_session_id"],
            interaction["request_item_id"],
            idempotency_key=idempotency_key,
        )
        if reminder.project_key != route["project_key"]:
            raise KrabError("interaction reminder belongs to another project")
        if reminder.session_id != route["krab_session_id"]:
            raise KrabError("interaction reminder belongs to another session")
        if reminder.item_id != interaction["request_item_id"]:
            raise KrabError("interaction reminder belongs to another item")
        if reminder.recipient_id != interaction["recipient"]["krab_user_id"]:
            raise KrabError("interaction reminder has an unexpected recipient")
        return reminder.event_seq

    def accept_interaction_response(
        self,
        interaction_id: str,
        *,
        response_item_id: str,
        expected_version: int,
    ) -> AcceptedInteractionResponse:
        try:
            response_item_id = validate_krab_id(response_item_id, "itm")
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 1
        ):
            raise ValidationError("expected_version must be a positive integer")
        interaction = self.database.get_interaction(interaction_id)
        if interaction["version"] != expected_version:
            raise ConcurrentUpdateError(
                f"interaction version is {interaction['version']}, "
                f"expected {expected_version}"
            )
        if interaction["state"] != "open":
            raise ConflictError(f"interaction is not open: {interaction['state']}")
        if interaction["run_state"] not in {
            RunState.WAITING_FOR_INPUT.value,
            RunState.PAUSED.value,
        }:
            raise ConflictError(
                "interaction run is not waiting for input: " + interaction["run_state"]
            )
        recipient = self.database.get_user(
            interaction["recipient"]["local_user_handle"]
        )
        if recipient["active"] != 1:
            raise ConflictError("interaction recipient is inactive")
        route = self.database.get_run_transport(interaction["run_id"])
        if route["krab_session_id"] is None:
            raise ConflictError("run has no Krab session")
        _, agent, client = self._authenticated_client(
            route["krab_instance_name"], route["agent_krab_handle"]
        )
        request_item = client.get_session_item(
            route["project_key"],
            route["krab_session_id"],
            interaction["request_item_id"],
        )
        self._verify_interaction_item(
            request_item,
            project_key=route["project_key"],
            session_id=route["krab_session_id"],
            creator_user_id=agent["krab_user_id"],
            recipient_user_id=interaction["recipient"]["krab_user_id"],
            item_id=interaction["request_item_id"],
            created=False,
        )
        item = client.get_session_item(
            route["project_key"], route["krab_session_id"], response_item_id
        )
        self._verify_interaction_item(
            item,
            project_key=route["project_key"],
            session_id=route["krab_session_id"],
            creator_user_id=interaction["recipient"]["krab_user_id"],
            recipient_user_id=agent["krab_user_id"],
            item_id=response_item_id,
            created=False,
            answer_eligible=True,
        )
        request_sequence = self._interaction_item_creation_sequence(request_item)
        response_sequence = self._interaction_item_creation_sequence(item)
        if response_sequence <= request_sequence:
            raise KrabError("interaction response was not created after the request")
        answered = self.database.answer_interaction(
            interaction_id,
            response_item_id=response_item_id,
            expected_version=expected_version,
        )
        return AcceptedInteractionResponse(
            interaction_id=answered["id"],
            run_id=answered["run_id"],
            response_item_id=response_item_id,
            interaction_state=answered["state"],
            interaction_version=answered["version"],
            run_state=answered["run_state"],
            run_version=answered["run_version"],
            untrusted_title=item.untrusted_title,
            untrusted_body=item.untrusted_body,
            content_risk_flags=item.content_risk_flags,
        )

    def read_interaction_response(
        self, run_id: str, interaction_id: str
    ) -> AcceptedInteractionResponse:
        """Re-read transient response content for one explicitly bound run."""

        interaction = self.database.get_interaction(interaction_id)
        if interaction["run_id"] != run_id:
            raise NotFoundError("interaction response not found for run")
        if (
            interaction["state"] != "answered"
            or interaction["response_item_id"] is None
        ):
            raise ConflictError("interaction has no accepted response")
        route = self.database.get_run_transport(run_id)
        if route["krab_session_id"] is None:
            raise ConflictError("run has no Krab session")
        _, agent, client = self._authenticated_client(
            route["krab_instance_name"], route["agent_krab_handle"]
        )
        response_item_id = interaction["response_item_id"]
        item = client.get_session_item(
            route["project_key"], route["krab_session_id"], response_item_id
        )
        self._verify_interaction_item(
            item,
            project_key=route["project_key"],
            session_id=route["krab_session_id"],
            creator_user_id=interaction["recipient"]["krab_user_id"],
            recipient_user_id=agent["krab_user_id"],
            item_id=response_item_id,
            created=False,
        )
        return AcceptedInteractionResponse(
            interaction_id=interaction["id"],
            run_id=run_id,
            response_item_id=response_item_id,
            interaction_state=interaction["state"],
            interaction_version=interaction["version"],
            run_state=interaction["run_state"],
            run_version=interaction["run_version"],
            untrusted_title=item.untrusted_title,
            untrusted_body=item.untrusted_body,
            content_risk_flags=item.content_risk_flags,
        )

    def project_activity(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
        after: int = 0,
        limit: int = 50,
        wait: int = 0,
    ) -> ActivityPage:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.activity(project_key, after=after, limit=limit, wait=wait)

    def krab_projects(
        self, *, instance_name: str, krab_handle: str
    ) -> tuple[KrabProject, ...]:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.projects()

    def krab_sessions(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
    ) -> tuple[KrabSession, ...]:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.sessions(project_key)

    def create_krab_session(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
        label: str,
        participant_handles: tuple[str, ...],
        idempotency_key: str,
    ) -> KrabSession:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.create_session(
            project_key,
            label=label,
            participant_handles=participant_handles,
            idempotency_key=idempotency_key,
        )

    def close_krab_session(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
        session_id: str,
        idempotency_key: str,
    ) -> SessionCloseReceipt:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.close_session(
            project_key, session_id, idempotency_key=idempotency_key
        )

    def krab_inbox(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
        after: int | None = 0,
        unread: bool = False,
        limit: int = 50,
        wait: int = 0,
    ) -> InboxPage:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.inbox(
            project_key,
            after=after,
            unread=unread,
            limit=limit,
            wait=wait,
        )

    def ack_krab_inbox(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
        through: int,
        idempotency_key: str,
    ) -> InboxAck:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.ack_inbox(
            project_key,
            through=through,
            idempotency_key=idempotency_key,
        )

    def send_krab_item(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
        session_id: str,
        recipient_handle: str,
        title: str,
        body: str,
        idempotency_key: str,
        claim_id: str | None = None,
        completion_policy: str = "workflow",
    ) -> SessionItem:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.create_session_item(
            project_key,
            session_id,
            recipient_handle=recipient_handle,
            title=title,
            body=body,
            idempotency_key=idempotency_key,
            claim_id=claim_id,
            completion_policy=completion_policy,
        )

    def get_krab_item(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
        session_id: str,
        item_id: str,
    ) -> SessionItem:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.get_session_item(project_key, session_id, item_id)

    def change_krab_item_status(
        self,
        *,
        instance_name: str,
        krab_handle: str,
        project_key: str,
        session_id: str,
        item_id: str,
        status: str,
        expected_version: int,
        note: str | None,
        idempotency_key: str,
    ) -> SessionItem:
        _, _, client = self._authenticated_client(instance_name, krab_handle)
        return client.change_session_item_status(
            project_key,
            session_id,
            item_id,
            status=status,
            expected_version=expected_version,
            note=note,
            idempotency_key=idempotency_key,
        )

    def _authenticated_client(
        self,
        instance_name: str,
        krab_handle: str,
        *,
        timeout: float | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], KrabClient]:
        instance = self.database.get_krab_instance(instance_name)
        stored = self.database.get_krab_identity(instance["id"], krab_handle)
        token = self.credential_resolver.resolve(stored["credential_ref"])
        if timeout is None:
            client = self.client_factory(instance["base_url"], token)
        else:
            client = self.client_factory(instance["base_url"], token, timeout=timeout)
        actual = client.whoami()
        if actual.user_id != stored["krab_user_id"]:
            raise KrabError("credential now belongs to a different Krab identity")
        refreshed = self.database.store_krab_identity(
            instance["id"], actual, stored["credential_ref"]
        )
        return instance, refreshed, client

    @staticmethod
    def _verify_interaction_item(
        item: SessionItem,
        *,
        project_key: str,
        session_id: str,
        creator_user_id: str,
        recipient_user_id: str,
        item_id: str | None,
        created: bool,
        answer_eligible: bool = False,
    ) -> None:
        if item.project_key != project_key:
            raise KrabError("interaction item belongs to another project")
        if item.session_id != session_id:
            raise KrabError("interaction item belongs to another session")
        if item_id is not None and item.item_id != item_id:
            raise KrabError("interaction response returned another item")
        if item.creator.user_id != creator_user_id:
            raise KrabError("interaction item has an unexpected creator")
        if item.recipient.user_id != recipient_user_id:
            raise KrabError("interaction item has an unexpected recipient")
        if created:
            if item.status != "open.pending":
                raise KrabError("interaction request is not pending")
            if item.version != 1:
                raise KrabError("interaction request has an unexpected version")
            if item.event_seq is None:
                raise KrabError("interaction request has no event sequence")
        elif not item.status.startswith("open."):
            raise KrabError("interaction response item is not open")
        elif answer_eligible and item.status != "open.pending":
            raise KrabError("interaction response item is not pending")
        elif answer_eligible and item.version != 1:
            raise KrabError("interaction response item has an unexpected version")

    @staticmethod
    def _interaction_item_creation_sequence(item: SessionItem) -> int:
        created = [event for event in item.history if event.kind == "item.created"]
        if len(created) != 1:
            raise KrabError("interaction item has invalid creation history")
        event = created[0]
        if (
            event.actor.user_id != item.creator.user_id
            or event.from_status is not None
            or event.to_status != "open.pending"
        ):
            raise KrabError("interaction item has invalid creation history")
        return event.event_seq


def activity_to_dict(page: ActivityPage) -> dict[str, Any]:
    return asdict(page)


def context_from_mapping(value: Mapping[str, Any]) -> ProjectContext:
    return ProjectContext.from_mapping(value)


def profile_from_mapping(value: Mapping[str, Any]) -> AgentProfileSpec:
    return AgentProfileSpec.from_mapping(value)
