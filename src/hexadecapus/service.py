from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .credentials import CredentialResolver, PrivateFileCredentialResolver
from .db import Database
from .domain import AgentProfileSpec, ProjectContext, RunState
from .krab import ActivityPage, KrabClient, KrabError

ClientFactory = Callable[[str, str], KrabClient]


class ControlPlane:
    """Application service coordinating durable Hex state with Krab identity."""

    def __init__(
        self,
        database: Database,
        *,
        credential_resolver: CredentialResolver | None = None,
        client_factory: ClientFactory | None = None,
    ):
        self.database = database
        self.credential_resolver = (
            credential_resolver or PrivateFileCredentialResolver()
        )
        self.client_factory = client_factory or KrabClient

    def initialize(self) -> dict[str, Any]:
        return self.database.initialize()

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
        hex_user_handle: str | None = None,
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
        if hex_user_handle is not None:
            user_id = self.database.get_user(hex_user_handle)["id"]
        return self.database.store_krab_identity(
            instance["id"], identity, reference, user_id=user_id
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

    def create_run(
        self,
        *,
        project_key: str,
        profile_name: str,
        requested_by_handle: str,
        source_revision: str | None = None,
        krab_session_id: str | None = None,
        krab_claim_id: str | None = None,
    ) -> dict[str, Any]:
        requester = self.database.get_user(requested_by_handle)
        return self.database.create_run(
            project_key=project_key,
            profile_name=profile_name,
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


def activity_to_dict(page: ActivityPage) -> dict[str, Any]:
    return asdict(page)


def context_from_mapping(value: Mapping[str, Any]) -> ProjectContext:
    return ProjectContext.from_mapping(value)


def profile_from_mapping(value: Mapping[str, Any]) -> AgentProfileSpec:
    return AgentProfileSpec.from_mapping(value)
