from __future__ import annotations

import secrets
import threading
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from .db import ConflictError, Database
from .domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    ProjectContext,
    RunState,
)
from .executor import SandboxPlan, SandboxRequest, SandboxResult
from .gateway import GatewayEndpoint
from .results import CapturedResult
from .runtime import RuntimeAdapterRegistry, default_runtime_adapters
from .workspace import PreparedWorkspace


class RunOrchestrationError(RuntimeError):
    """A run could not advance without losing lifecycle or cleanup guarantees."""


class SandboxExecutor(Protocol):
    def execute(
        self,
        request: SandboxRequest,
        *,
        before_start: Callable[[SandboxPlan, str], None] | None = None,
    ) -> SandboxResult: ...

    def cleanup_run(self, run_id: str) -> int: ...

    def stop_run(self, run_id: str) -> int: ...


class RunWorkspaceManager(Protocol):
    def prepare(
        self, run_id: str, repository: Path, revision: str | None
    ) -> PreparedWorkspace: ...

    def cleanup(self, run_id: str) -> bool: ...

    def locate(self, run_id: str) -> Path | None: ...


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


class RunResultStore(Protocol):
    def stage(self, attempt_id: str, workspace: Path, source_revision: str) -> None: ...

    def capture(self, attempt_id: str, workspace: Path) -> CapturedResult: ...

    def discard_stage(self, attempt_id: str) -> bool: ...


class _AttemptLeaseMonitor:
    def __init__(
        self,
        database: Database,
        executor: SandboxExecutor,
        *,
        attempt_id: str,
        run_id: str,
        owner_id: str,
        lease_seconds: int,
        interval: float,
    ):
        self.database = database
        self.executor = executor
        self.attempt_id = attempt_id
        self.run_id = run_id
        self.owner_id = owner_id
        self.lease_seconds = lease_seconds
        self.interval = interval
        self.failure: BaseException | None = None
        self._cancel_stopped = False
        self._closed = threading.Event()
        self._pulse_lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run,
            name=f"mgb-lease-{attempt_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        while not self._closed.is_set():
            try:
                self.pulse()
            except BaseException as exc:
                self.failure = exc
                try:
                    self.executor.stop_run(self.run_id)
                except BaseException as stop_error:
                    exc.add_note(
                        "container stop after lease failure also failed: "
                        f"{type(stop_error).__name__}: {stop_error}"
                    )
                return
            if self._closed.wait(self.interval):
                return

    def pulse(self) -> None:
        with self._pulse_lock:
            control = self.database.renew_run_attempt_lease(
                self.attempt_id,
                owner_id=self.owner_id,
                lease_seconds=self.lease_seconds,
            )
            if control["cancel_requested"] and not self._cancel_stopped:
                self._cancel_stopped = self.executor.stop_run(self.run_id) > 0

    def check(self) -> None:
        if self.failure is not None:
            raise RunOrchestrationError("run attempt ownership monitor failed") from (
                self.failure
            )

    def close(self) -> None:
        self._closed.set()
        self._thread.join(timeout=10)
        if self._thread.is_alive() and self.failure is None:
            self.failure = RunOrchestrationError(
                "run attempt ownership monitor did not stop"
            )


class RunCoordinator:
    def __init__(
        self,
        database: Database,
        executor: SandboxExecutor,
        workspaces: RunWorkspaceManager,
        credential_resolver: CredentialPathResolver,
        gateway_manager: RunGatewayManager | None,
        result_store: RunResultStore,
        *,
        orchestrator_id: str | None = None,
        lease_seconds: int = 60,
        heartbeat_interval: float = 15.0,
        runtime_adapters: RuntimeAdapterRegistry | None = None,
    ):
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or lease_seconds < 10
        ):
            raise RunOrchestrationError("lease_seconds must be at least 10")
        if (
            isinstance(heartbeat_interval, bool)
            or not isinstance(heartbeat_interval, (int, float))
            or heartbeat_interval <= 0
            or heartbeat_interval >= lease_seconds
        ):
            raise RunOrchestrationError(
                "heartbeat_interval must be positive and shorter than the lease"
            )
        self.database = database
        self.executor = executor
        self.workspaces = workspaces
        self.credential_resolver = credential_resolver
        self.gateway_manager = gateway_manager
        self.result_store = result_store
        self.orchestrator_id = orchestrator_id or (
            "orchestrator-" + secrets.token_hex(16)
        )
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = float(heartbeat_interval)
        self.runtime_adapters = runtime_adapters or RuntimeAdapterRegistry(
            default_runtime_adapters()
        )

    def execute(
        self,
        run_id: str,
        *,
        runtime_adapter: str | None = None,
    ) -> dict[str, Any]:
        inputs = self.database.get_run_execution(run_id)
        if inputs["state"] != RunState.QUEUED.value:
            raise RunOrchestrationError(f"run is not queued: {inputs['state']}")
        raw_profile = inputs["profile"]
        configured_adapter = (
            raw_profile.get("runtime_adapter", "command")
            if isinstance(raw_profile, Mapping)
            else "command"
        )
        adapter_name = self.runtime_adapters.require(
            runtime_adapter or configured_adapter
        )
        raw_command = (
            raw_profile.get("command", ()) if isinstance(raw_profile, Mapping) else ()
        )
        raw_provider_grant = (
            raw_profile.get("runtime_provider_grant")
            if isinstance(raw_profile, Mapping)
            else None
        )
        launch = self.runtime_adapters.launch(
            adapter_name,
            raw_command,
            provider_grant=raw_provider_grant,
        )
        try:
            attempt = self.database.begin_run_attempt(
                run_id,
                runtime_adapter=adapter_name,
                owner_id=self.orchestrator_id,
                expected_run_version=inputs["version"],
                lease_seconds=self.lease_seconds,
            )
        except Exception as exc:
            raise RunOrchestrationError(
                "run could not be claimed for execution"
            ) from exc

        monitor = _AttemptLeaseMonitor(
            self.database,
            self.executor,
            attempt_id=attempt["id"],
            run_id=run_id,
            owner_id=self.orchestrator_id,
            lease_seconds=self.lease_seconds,
            interval=self.heartbeat_interval,
        )
        monitor.start()
        running_attempt: dict[str, Any] | None = None
        prepared: PreparedWorkspace | None = None
        started_container: tuple[SandboxPlan, str] | None = None
        try:
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
                self.result_store.stage(attempt["id"], prepared.path, prepared.revision)
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
                    launch=launch,
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
                completion = self.runtime_adapters.complete(
                    adapter_name,
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                )
            except Exception as exc:
                self._abort(
                    run_id,
                    attempt["id"],
                    exc,
                    workspace=None if prepared is None else prepared.path,
                    capture_result=running_attempt is not None,
                )

            try:
                monitor.pulse()
                monitor.check()
                self._capture_result(attempt["id"], prepared.path)
                monitor.check()
            except Exception as exc:
                cleanup_failures = self._cleanup(run_id, include_workspace=False)
                error = RunOrchestrationError(
                    "result capture incomplete; workspace retained for recovery"
                )
                for failure in cleanup_failures:
                    error.add_note(
                        f"cleanup failed: {type(failure).__name__}: {failure}"
                    )
                raise error from exc

            cleanup_failures = self._cleanup(run_id)
            if cleanup_failures:
                self._raise_cleanup_incomplete(cleanup_failures)
            monitor.check()
            monitor.close()
            monitor.check()

            try:
                current_attempt = self.database.get_run_attempt(running_attempt["id"])
                self.database.complete_run_attempt(
                    running_attempt["id"],
                    expected_attempt_version=current_attempt["version"],
                    exit_code=completion.exit_code,
                    image_id=result.image_id,
                )
            except Exception as exc:
                raise RunOrchestrationError(
                    "sandbox finished but its outcome could not be recorded"
                ) from exc
            return self.database.get_run(run_id)
        finally:
            monitor.close()

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

    def _cleanup(
        self,
        run_id: str,
        *,
        include_executor: bool = True,
        include_gateway: bool = True,
        include_workspace: bool = True,
    ) -> list[BaseException]:
        failures: list[BaseException] = []
        if include_executor:
            try:
                self.executor.cleanup_run(run_id)
            except BaseException as exc:
                failures.append(exc)
        if include_gateway and self.gateway_manager is not None:
            try:
                self.gateway_manager.cleanup(run_id)
            except BaseException as exc:
                failures.append(exc)
        if include_workspace:
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

    def _capture_result(self, attempt_id: str, workspace: Path) -> None:
        captured = self.result_store.capture(attempt_id, workspace)
        self.database.record_attempt_result(attempt_id, captured.manifest)
        self.result_store.discard_stage(attempt_id)

    def _abort(
        self,
        run_id: str,
        attempt_id: str,
        cause: Exception,
        *,
        workspace: Path | None,
        capture_result: bool,
    ) -> None:
        if workspace is not None:
            try:
                if capture_result:
                    self._capture_result(attempt_id, workspace)
                else:
                    self.result_store.discard_stage(attempt_id)
            except Exception as exc:
                cleanup_failures = self._cleanup(run_id, include_workspace=False)
                error = RunOrchestrationError(
                    "result capture incomplete; workspace retained for recovery"
                    if capture_result
                    else "run cleanup incomplete; restart reconciliation is required"
                )
                error.add_note(
                    f"execution failed before cleanup: {type(cause).__name__}"
                )
                error.add_note(f"result staging failed: {type(exc).__name__}: {exc}")
                for failure in cleanup_failures:
                    error.add_note(
                        f"cleanup failed: {type(failure).__name__}: {failure}"
                    )
                raise error from exc
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

    def reconcile(self, run_id: str | None = None) -> list[dict[str, Any]]:
        only_run_id = run_id
        reconciled: list[dict[str, Any]] = []
        failures: list[tuple[str, BaseException]] = []
        for candidate in self.database.list_expired_run_attempts():
            if only_run_id is not None and candidate["run_id"] != only_run_id:
                continue
            run_id = candidate["run_id"]
            try:
                active_attempt = self.database.claim_expired_run_attempt(
                    candidate["id"],
                    owner_id=self.orchestrator_id,
                    lease_seconds=self.lease_seconds,
                )
            except ConflictError:
                continue
            try:
                removed = self._cleanup(
                    run_id,
                    include_gateway=False,
                    include_workspace=False,
                )
                if removed:
                    failures.extend((run_id, failure) for failure in removed)
                    continue
                workspace = self.workspaces.locate(run_id)
                if (
                    active_attempt["state"] == "running"
                    and active_attempt["result"] is None
                    and workspace is not None
                ):
                    self._capture_result(active_attempt["id"], workspace)
                else:
                    self.result_store.discard_stage(active_attempt["id"])
                cleanup_failures = self._cleanup(run_id, include_executor=False)
                if cleanup_failures:
                    failures.extend((run_id, failure) for failure in cleanup_failures)
                    continue
                refreshed = self.database.get_run_attempt(active_attempt["id"])
                self.database.fail_run_attempt(
                    refreshed["id"],
                    expected_attempt_version=refreshed["version"],
                    reason="orchestrator ownership lease expired",
                    interrupted=True,
                )
                current = self.database.get_run(run_id)
                if current["state"] in {
                    RunState.FAILED.value,
                    RunState.CANCELLED.value,
                }:
                    reconciled.append(current)
            except Exception as exc:
                failures.append((run_id, exc))

        for candidate in self.database.iter_active_runs():
            if only_run_id is not None and candidate["id"] != only_run_id:
                continue
            if candidate["state"] not in {
                RunState.PREPARING.value,
                RunState.RUNNING.value,
            } or any(
                attempt["state"] in {"preparing", "running"}
                for attempt in candidate["attempts"]
            ):
                continue
            run_id = candidate["id"]
            cleanup_failures = self._cleanup(run_id)
            if cleanup_failures:
                failures.extend((run_id, failure) for failure in cleanup_failures)
                continue
            try:
                reconciled.append(
                    self.database.transition_run(
                        run_id,
                        target=RunState.FAILED,
                        expected_version=candidate["version"],
                        reason="orphaned run has no active attempt",
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
