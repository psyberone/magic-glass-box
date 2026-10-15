from __future__ import annotations

import hashlib
import os
import secrets
import stat
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .candidates import (
    CANDIDATE_SCHEMA_VERSION,
    CandidateError,
    CandidateManifest,
    GitCandidateStore,
)
from .credentials import (
    CredentialError,
    CredentialResolver,
    PrivateFileCredentialResolver,
)
from .db import (
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
)
from .domain import (
    AgentProfileSpec,
    HandoffEnvelope,
    InteractionKind,
    NetworkMode,
    ProjectContext,
    ProviderGrantSpec,
    RunState,
    SupervisionPolicy,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
    WorkLogEntry,
    WorkMemoryCheckpoint,
    WorkspaceMode,
    validate_krab_id,
)
from .gates import (
    GATE_EVIDENCE_SCHEMA_VERSION,
    ExactGateInputs,
    GateOutcome,
    GatePolicyManifest,
    ReviewEvidence,
    ValidationEvidence,
    resolve_gate_policy_request,
)
from .krab import ActivityPage, KrabClient, KrabError, SessionItem
from .provider import (
    ProviderBroker,
    ProviderBusyError,
    ProviderError,
    ProviderUpstreamError,
)
from .results import WorkspaceResultStore
from .runtime import RuntimeLaunch

if TYPE_CHECKING:
    from .executor import DockerExecutor

ClientFactory = Callable[..., KrabClient]
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


class ControlPlane:
    """Coordinate durable local state with Krab identity and messaging."""

    def __init__(
        self,
        database: Database,
        *,
        credential_resolver: CredentialResolver | None = None,
        client_factory: ClientFactory | None = None,
        provider_broker: ProviderBroker | None = None,
    ):
        self.database = database
        self.credential_resolver = (
            credential_resolver or PrivateFileCredentialResolver()
        )
        self.client_factory = client_factory or KrabClient
        self.provider_broker = provider_broker or ProviderBroker()

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
        if existing is None:
            materialized = candidate_store.materialize(
                source["project_id"],
                Path(repository_path),
                result,
                blobs,
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
        try:
            candidate_store.inspect(manifest)
        except CandidateError:
            pass
        else:
            return candidate
        repository_path = self.database.get_candidate_repository_path(candidate_id)
        if repository_path is None:
            raise ConflictError("candidate project has no repository path")
        result, blobs = result_store.load_bundle(
            manifest.attempt_id,
            expected_digest=manifest.result_content_sha256,
        )
        candidate_store.recover(
            manifest,
            Path(repository_path),
            result,
            blobs,
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
        manifest = GatePolicyManifest.bind(
            candidate,
            default_branch=context.default_branch,
            policy=policy,
        )
        return self.database.create_gate_policy(manifest, created_by=creator["id"])

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

    def gate_readiness(self, policy_id: str) -> dict[str, Any]:
        return self.database.gate_readiness(policy_id)

    def list_gate_evidence(
        self, policy_id: str, *, limit: int = 500
    ) -> list[dict[str, Any]]:
        return self.database.list_gate_evidence(policy_id, limit=limit)

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
        candidate_record = self.get_candidate(
            policy.candidate_id,
            result_store=result_store,
            candidate_store=candidate_store,
        )
        candidate = CandidateManifest.from_mapping(candidate_record["manifest"])
        if (
            candidate.head_revision != policy.head_revision
            or candidate.tree_revision != policy.tree_revision
            or candidate.base_revision != policy.expected_base_revision
            or candidate.object_format != policy.object_format
        ):
            raise ConflictError("gate policy candidate identity changed")

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
                launch=RuntimeLaunch(validation.command),
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

    def add_provider_grant(
        self,
        *,
        name: str,
        spec: ProviderGrantSpec,
        credential_path: Path,
        expires_at: str | None,
        created_by_handle: str,
    ) -> dict[str, Any]:
        if not isinstance(self.credential_resolver, PrivateFileCredentialResolver):
            raise TypeError(
                "add_provider_grant with a path requires a private-file resolver"
            )
        reference = self.credential_resolver.secret_reference(credential_path)
        creator = self.database.get_user(created_by_handle)
        return self.database.create_provider_grant(
            name=name,
            spec=spec,
            credential_ref=reference,
            expires_at=expires_at,
            created_by=creator["id"],
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
        route = self.database.get_run_transport(run_id)
        live_attempt_has_authority = (
            route["state"] in (RunState.RUNNING.value, RunState.WAITING_FOR_INPUT.value)
            and route["active_attempt_state"] == "running"
        )
        if not live_attempt_has_authority:
            raise ConflictError(f"run is not running: {route['state']}")
        grant = self.database.get_run_provider_grant(run_id, grant_name)
        try:
            spec = ProviderGrantSpec.from_mapping(grant["policy"])
        except (KeyError, TypeError, ValidationError) as exc:
            raise ConflictError("provider grant policy is unavailable") from exc
        started_at = self.database.now()
        try:
            credential = self.credential_resolver.resolve_secret(
                grant["credential_ref"]
            )
        except CredentialError as exc:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                outcome="denied",
                error_code="credential_unavailable",
                started_at=started_at,
            )
            raise ProviderError("provider credential is unavailable") from exc
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
                outcome="denied",
                error_code="authority_unavailable",
                started_at=started_at,
            )
            raise
        if reservation is None:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                outcome="busy",
                error_code="concurrency_limit",
                started_at=started_at,
            )
            raise ProviderBusyError("provider grant concurrency limit is reached")
        dispatch_monitor = _ProviderDispatchLeaseMonitor(
            self.database,
            reservation["id"],
            lease_seconds=PROVIDER_DISPATCH_LEASE_SECONDS,
            interval=PROVIDER_DISPATCH_RENEW_INTERVAL_SECONDS,
        )
        try:
            dispatch_monitor.start()
            result = self.provider_broker.invoke(
                grant_id=grant["id"],
                spec=spec,
                credential=credential,
                request=request,
            )
            dispatch_monitor.close()
            dispatch_monitor.check()
        except ProviderBusyError:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                outcome="busy",
                error_code="concurrency_limit",
                started_at=started_at,
            )
            raise
        except ProviderUpstreamError as exc:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                outcome="failed",
                error_code="upstream_error",
                started_at=started_at,
                request_bytes=exc.request_bytes,
                response_bytes=exc.response_bytes,
                http_status=exc.status_code,
            )
            raise
        except ProviderError:
            self._record_provider_failure(
                run_id=run_id,
                grant_id=grant["id"],
                outcome="denied",
                error_code="request_denied",
                started_at=started_at,
            )
            raise
        finally:
            active_error = sys.exception()
            dispatch_monitor.close()
            try:
                self.database.release_provider_dispatch(reservation["id"])
            except Exception as release_error:
                if active_error is None:
                    raise
                active_error.add_note(
                    "provider dispatch reservation release also failed: "
                    f"{type(release_error).__name__}: {release_error}"
                )
        invocation = self.database.record_provider_invocation(
            run_id=run_id,
            provider_grant_id=grant["id"],
            outcome="succeeded",
            request_bytes=result.request_bytes,
            response_bytes=result.response_bytes,
            http_status=result.status_code,
            error_code=None,
            started_at=started_at,
        )
        return AcceptedProviderResponse(
            invocation_id=invocation["id"],
            grant_name=grant_name,
            provider=spec.provider,
            model=spec.model,
            status_code=result.status_code,
            request_bytes=result.request_bytes,
            response_bytes=result.response_bytes,
            untrusted_response=result.untrusted_response,
        )

    def _record_provider_failure(
        self,
        *,
        run_id: str,
        grant_id: str,
        outcome: str,
        error_code: str,
        started_at: str,
        request_bytes: int = 0,
        response_bytes: int = 0,
        http_status: int | None = None,
    ) -> None:
        self.database.record_provider_invocation(
            run_id=run_id,
            provider_grant_id=grant_id,
            outcome=outcome,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
            http_status=http_status,
            error_code=error_code,
            started_at=started_at,
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
        if interaction["run_state"] != RunState.WAITING_FOR_INPUT.value:
            raise ConflictError(
                "interaction run is not waiting for input: " + interaction["run_state"]
            )
        route = self.database.get_run_transport(interaction["run_id"])
        if route["krab_session_id"] is None:
            raise ConflictError("run has no Krab session")
        _, agent, client = self._authenticated_client(
            route["krab_instance_name"], route["agent_krab_handle"]
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
        )
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


def activity_to_dict(page: ActivityPage) -> dict[str, Any]:
    return asdict(page)


def context_from_mapping(value: Mapping[str, Any]) -> ProjectContext:
    return ProjectContext.from_mapping(value)


def profile_from_mapping(value: Mapping[str, Any]) -> AgentProfileSpec:
    return AgentProfileSpec.from_mapping(value)
