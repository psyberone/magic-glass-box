from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

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
    ProjectContext,
    ProviderGrantSpec,
    RunState,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
    WorkLogEntry,
    WorkMemoryCheckpoint,
    validate_krab_id,
)
from .krab import ActivityPage, KrabClient, KrabError, SessionItem
from .provider import (
    ProviderBroker,
    ProviderBusyError,
    ProviderError,
    ProviderUpstreamError,
)

ClientFactory = Callable[[str, str], KrabClient]


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
        if route["state"] != RunState.RUNNING.value:
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
            result = self.provider_broker.invoke(
                grant_id=grant["id"],
                spec=spec,
                credential=credential,
                request=request,
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
        self, instance_name: str, krab_handle: str
    ) -> tuple[dict[str, Any], dict[str, Any], KrabClient]:
        instance = self.database.get_krab_instance(instance_name)
        stored = self.database.get_krab_identity(instance["id"], krab_handle)
        token = self.credential_resolver.resolve(stored["credential_ref"])
        client = self.client_factory(instance["base_url"], token)
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
