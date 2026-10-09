from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from .db import Database
from .domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    ProjectContext,
    RunState,
)
from .executor import SandboxPlan, SandboxRequest, SandboxResult
from .gateway import GatewayEndpoint
from .workspace import PreparedWorkspace


class RunOrchestrationError(RuntimeError):
    """A run could not advance without losing lifecycle or cleanup guarantees."""


class SandboxExecutor(Protocol):
    runtime_adapter: str

    def execute(
        self,
        request: SandboxRequest,
        *,
        before_start: Callable[[SandboxPlan, str], None] | None = None,
    ) -> SandboxResult: ...

    def cleanup_run(self, run_id: str) -> int: ...


class RunWorkspaceManager(Protocol):
    def prepare(
        self, run_id: str, repository: Path, revision: str | None
    ) -> PreparedWorkspace: ...

    def cleanup(self, run_id: str) -> bool: ...


class CredentialPathResolver(Protocol):
    def resolve_path(self, reference: str) -> Path: ...


class RunGatewayManager(Protocol):
    def open(
        self,
        run_id: str,
        *,
        capabilities: Sequence[GatewayCapability],
        credential_names: Sequence[str],
    ) -> GatewayEndpoint: ...

    def cleanup(self, run_id: str) -> bool: ...


class RunCoordinator:
    def __init__(
        self,
        database: Database,
        executor: SandboxExecutor,
        workspaces: RunWorkspaceManager,
        credential_resolver: CredentialPathResolver,
        gateway_manager: RunGatewayManager | None = None,
    ):
        self.database = database
        self.executor = executor
        self.workspaces = workspaces
        self.credential_resolver = credential_resolver
        self.gateway_manager = gateway_manager

    def execute(self, run_id: str) -> dict[str, Any]:
        inputs = self.database.get_run_execution(run_id)
        if inputs["state"] != RunState.QUEUED.value:
            raise RunOrchestrationError(f"run is not queued: {inputs['state']}")
        runtime_adapter = getattr(self.executor, "runtime_adapter", None)
        if not isinstance(runtime_adapter, str):
            raise RunOrchestrationError(
                "sandbox executor has no runtime adapter identity"
            )
        try:
            attempt = self.database.begin_run_attempt(
                run_id,
                runtime_adapter=runtime_adapter,
                expected_run_version=inputs["version"],
            )
        except Exception as exc:
            raise RunOrchestrationError(
                "run could not be claimed for execution"
            ) from exc

        running_attempt: dict[str, Any] | None = None
        prepared: PreparedWorkspace | None = None
        started_container: tuple[SandboxPlan, str] | None = None
        try:
            context = ProjectContext.from_mapping(inputs["context"])
            profile = AgentProfileSpec.from_mapping(inputs["profile"])
            if context.repository_path is None:
                raise RunOrchestrationError(
                    "pinned project context has no repository path"
                )
            prepared = self.workspaces.prepare(
                run_id,
                Path(context.repository_path),
                inputs["source_revision"],
            )
            gateway_directory = None
            if profile.network_mode is NetworkMode.GATEWAY_ONLY:
                credential_names = self._gateway_credential_names(
                    profile, inputs["credential_references"]
                )
                if self.gateway_manager is None:
                    raise RunOrchestrationError(
                        "gateway_only profile has no gateway manager"
                    )
                endpoint = self.gateway_manager.open(
                    run_id,
                    capabilities=profile.gateway_capabilities,
                    credential_names=credential_names,
                )
                gateway_directory = endpoint.directory
                credentials = {}
            else:
                credentials = self._credential_files(
                    profile, inputs["credential_references"]
                )
            request = SandboxRequest(
                run_id=run_id,
                profile=profile,
                workspace=prepared.path,
                credential_files=credentials,
                gateway_directory=gateway_directory,
            )

            def before_start(plan: SandboxPlan, container_id: str) -> None:
                nonlocal running_attempt, started_container
                started_container = (plan, container_id)
                running_attempt = self.database.start_run_attempt(
                    attempt["id"],
                    expected_attempt_version=attempt["version"],
                    expected_run_version=attempt["run_version"],
                    source_revision=prepared.revision,
                    container_id=container_id,
                    container_name=plan.container_name,
                    image_id=plan.image_id,
                )

            result = self.executor.execute(request, before_start=before_start)
            if running_attempt is None or started_container is None:
                raise RunOrchestrationError(
                    "sandbox executor returned before recording container start"
                )
            plan, container_id = started_container
            if (
                result.container_id != container_id
                or result.container_name != plan.container_name
                or result.image_id != plan.image_id
            ):
                raise RunOrchestrationError(
                    "sandbox executor returned inconsistent container identity"
                )
        except Exception as exc:
            self._abort(run_id, attempt["id"], exc)

        cleanup_failures = self._cleanup(run_id)
        if cleanup_failures:
            self._raise_cleanup_incomplete(cleanup_failures)

        try:
            self.database.complete_run_attempt(
                running_attempt["id"],
                expected_attempt_version=running_attempt["version"],
                exit_code=result.exit_code,
                image_id=result.image_id,
            )
        except Exception as exc:
            raise RunOrchestrationError(
                "sandbox finished but its outcome could not be recorded"
            ) from exc
        return self.database.get_run(run_id)

    def _credential_files(
        self,
        profile: AgentProfileSpec,
        references: object,
    ) -> dict[str, Path]:
        if not isinstance(references, Mapping):
            raise RunOrchestrationError("pinned credential references are invalid")
        files: dict[str, Path] = {}
        for name in profile.credential_names:
            reference = references.get(name)
            if not isinstance(reference, str):
                raise RunOrchestrationError(
                    f"pinned profile credential is unavailable: {name}"
                )
            files[name] = self.credential_resolver.resolve_path(reference)
        return files

    @staticmethod
    def _gateway_credential_names(
        profile: AgentProfileSpec,
        references: object,
    ) -> tuple[str, ...]:
        if not isinstance(references, Mapping):
            raise RunOrchestrationError("pinned credential references are invalid")
        for name in profile.credential_names:
            if not isinstance(references.get(name), str):
                raise RunOrchestrationError(
                    f"pinned profile credential is unavailable: {name}"
                )
        return profile.credential_names

    def _cleanup(self, run_id: str) -> list[BaseException]:
        failures: list[BaseException] = []
        try:
            self.executor.cleanup_run(run_id)
        except BaseException as exc:
            failures.append(exc)
        if self.gateway_manager is not None:
            try:
                self.gateway_manager.cleanup(run_id)
            except BaseException as exc:
                failures.append(exc)
        try:
            self.workspaces.cleanup(run_id)
        except BaseException as exc:
            failures.append(exc)
        return failures

    @staticmethod
    def _raise_cleanup_incomplete(failures: list[BaseException]) -> None:
        error = RunOrchestrationError(
            "run cleanup incomplete; restart reconciliation is required"
        )
        for failure in failures:
            error.add_note(f"cleanup failed: {type(failure).__name__}: {failure}")
        raise error

    def _abort(self, run_id: str, attempt_id: str, cause: Exception) -> None:
        cleanup_failures = self._cleanup(run_id)
        if cleanup_failures:
            try:
                self._raise_cleanup_incomplete(cleanup_failures)
            except RunOrchestrationError as error:
                raise error from cause
        attempt = self.database.get_run_attempt(attempt_id)
        if attempt["state"] in {
            "preparing",
            "running",
        }:
            try:
                self.database.fail_run_attempt(
                    attempt_id,
                    expected_attempt_version=attempt["version"],
                    reason=f"execution failed: {type(cause).__name__}",
                )
            except Exception as exc:
                raise RunOrchestrationError(
                    "execution failed and its terminal state could not be recorded"
                ) from exc
        raise RunOrchestrationError("run execution failed") from cause

    def reconcile(self) -> list[dict[str, Any]]:
        reconciled: list[dict[str, Any]] = []
        failures: list[tuple[str, BaseException]] = []
        for candidate in self.database.iter_active_runs():
            run_id = candidate["id"]
            cleanup_failures = self._cleanup(run_id)
            if cleanup_failures:
                failures.extend((run_id, failure) for failure in cleanup_failures)
                continue
            if candidate["state"] in {
                RunState.QUEUED.value,
                RunState.WAITING_FOR_INPUT.value,
            } and not any(
                attempt["state"] in {"preparing", "running"}
                for attempt in candidate["attempts"]
            ):
                continue
            try:
                active_attempt = next(
                    (
                        attempt
                        for attempt in reversed(candidate["attempts"])
                        if attempt["state"] in {"preparing", "running"}
                    ),
                    None,
                )
                if active_attempt is not None:
                    self.database.fail_run_attempt(
                        active_attempt["id"],
                        expected_attempt_version=active_attempt["version"],
                        reason="orchestrator restart reconciliation",
                        interrupted=True,
                    )
                    current = self.database.get_run(run_id)
                    if current["state"] == RunState.FAILED.value:
                        reconciled.append(current)
                else:
                    reconciled.append(
                        self.database.transition_run(
                            run_id,
                            target=RunState.FAILED,
                            expected_version=candidate["version"],
                            reason="orchestrator restart reconciliation",
                            metadata={"reconciled": True},
                        )
                    )
            except Exception as exc:
                failures.append((run_id, exc))
        if failures:
            affected = len({item[0] for item in failures})
            error = RunOrchestrationError(
                f"reconciliation incomplete for {affected} run(s)"
            )
            for run_id, failure in failures:
                error.add_note(f"{run_id}: {type(failure).__name__}: {failure}")
            raise error
        return reconciled
