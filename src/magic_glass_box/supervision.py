from __future__ import annotations

import logging
import math
import re
import secrets
import threading
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from .db import (
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
    StorageError,
)

logger = logging.getLogger(__name__)


class SupervisorError(RuntimeError):
    """The autonomous supervisor was configured or stopped incorrectly."""


class SupervisorActionTimeout(SupervisorError):
    """A coordinator action outlived the supervisor's ownership deadline."""


class RunAdvancer(Protocol):
    def execute(self, run_id: str) -> dict[str, Any]: ...

    def reconcile(self, run_id: str | None = None) -> list[dict[str, Any]]: ...


class WorkflowAdvancer(Protocol):
    def advance(self, execution_id: str, *, expected_version: int) -> bool: ...


class DiscoveryAdvancer(Protocol):
    def advance(self, execution_id: str, *, expected_version: int) -> bool: ...


class AdvisoryAdvancer(Protocol):
    def advance(self, execution_id: str, *, expected_version: int) -> bool: ...


class EscalationTransport(Protocol):
    def remind_interaction(
        self,
        interaction_id: str,
        *,
        idempotency_key: str,
        timeout: float,
    ) -> int: ...


@dataclass(frozen=True)
class _ClaimWorker:
    thread: threading.Thread
    claim_id: str
    version: int
    deadline: float | None


class Supervisor:
    """Drive durable actions while SQLite and attempt leases retain authority."""

    def __init__(
        self,
        database: Database,
        coordinator: RunAdvancer,
        escalation_transport: EscalationTransport,
        *,
        workflow_director: WorkflowAdvancer | None = None,
        discovery_director: DiscoveryAdvancer | None = None,
        advisory_director: AdvisoryAdvancer | None = None,
        supervisor_id: str | None = None,
        poll_interval: float = 2.0,
        max_workers: int = 4,
        action_lease_seconds: int = 60,
        max_escalations_per_tick: int = 32,
        escalation_timeout: float | None = None,
        shutdown_timeout: float = 30.0,
        action_timeout: float = 21_900.0,
    ) -> None:
        if (
            isinstance(poll_interval, bool)
            or not isinstance(poll_interval, (int, float))
            or not math.isfinite(poll_interval)
            or not 0.05 <= poll_interval <= 60.0
        ):
            raise SupervisorError("poll_interval must be between 0.05 and 60 seconds")
        if (
            isinstance(max_workers, bool)
            or not isinstance(max_workers, int)
            or not 1 <= max_workers <= 32
        ):
            raise SupervisorError("max_workers must be between 1 and 32")
        if (
            isinstance(action_lease_seconds, bool)
            or not isinstance(action_lease_seconds, int)
            or not 10 <= action_lease_seconds <= 3600
        ):
            raise SupervisorError("action_lease_seconds must be between 10 and 3600")
        if (
            isinstance(max_escalations_per_tick, bool)
            or not isinstance(max_escalations_per_tick, int)
            or not 1 <= max_escalations_per_tick <= 256
        ):
            raise SupervisorError("max_escalations_per_tick must be between 1 and 256")
        if escalation_timeout is None:
            escalation_timeout = min(30.0, action_lease_seconds / 2)
        if (
            isinstance(escalation_timeout, bool)
            or not isinstance(escalation_timeout, (int, float))
            or not math.isfinite(escalation_timeout)
            or not 0.05 <= escalation_timeout <= 60.0
            or escalation_timeout >= action_lease_seconds
        ):
            raise SupervisorError(
                "escalation_timeout must be between 0.05 and 60 seconds and "
                "shorter than action_lease_seconds"
            )
        if (
            isinstance(shutdown_timeout, bool)
            or not isinstance(shutdown_timeout, (int, float))
            or not math.isfinite(shutdown_timeout)
            or not 0.05 <= shutdown_timeout <= 300.0
        ):
            raise SupervisorError(
                "shutdown_timeout must be between 0.05 and 300 seconds"
            )
        if (
            isinstance(action_timeout, bool)
            or not isinstance(action_timeout, (int, float))
            or not math.isfinite(action_timeout)
            or not 0.05 <= action_timeout <= 2_592_000.0
        ):
            raise SupervisorError(
                "action_timeout must be between 0.05 and 2592000 seconds"
            )
        if supervisor_id is None:
            supervisor_id = "supervisor-" + secrets.token_hex(16)
        if (
            not isinstance(supervisor_id, str)
            or not supervisor_id
            or "\x00" in supervisor_id
            or len(supervisor_id.encode("utf-8")) > 128
        ):
            raise SupervisorError("supervisor_id must be bounded nonempty text")
        self.database = database
        self.coordinator = coordinator
        self.escalation_transport = escalation_transport
        self.workflow_director = workflow_director
        self.discovery_director = discovery_director
        self.advisory_director = advisory_director
        self.supervisor_id = supervisor_id
        self.poll_interval = float(poll_interval)
        self.max_workers = max_workers
        self.action_lease_seconds = action_lease_seconds
        self.max_escalations_per_tick = max_escalations_per_tick
        self.escalation_timeout = float(escalation_timeout)
        self.shutdown_timeout = float(shutdown_timeout)
        self.action_timeout = float(action_timeout)
        self._lease_refresh_interval = min(
            self.poll_interval, self.action_lease_seconds / 3
        )
        self._stop = threading.Event()
        self._action_workers: dict[str, _ClaimWorker] = {}
        self._escalation_workers: dict[str, _ClaimWorker] = {}
        self._threads_lock = threading.Lock()
        self.last_loop_error: str | None = None

    def tick(self, *, wait: bool = False) -> dict[str, int]:
        """Discover, claim, and start one bounded batch of durable work."""

        completed = self._reap()
        self._raise_if_action_overdue()
        self._renew_active_leases()
        actions_created = len(self.database.enqueue_supervisor_actions())
        escalations_created = len(self.database.create_due_supervisor_escalations())
        escalations_processed = self._start_escalations()
        actions_started = 0
        while self._worker_count() < self.max_workers:
            action = self.database.claim_supervisor_action(
                owner_id=self.supervisor_id,
                lease_seconds=self.action_lease_seconds,
            )
            if action is None:
                break
            self._start_action(action)
            actions_started += 1
        if wait:
            self.drain()
            completed += self._reap()
        return {
            "actions_created": actions_created,
            "actions_started": actions_started,
            "actions_completed": completed,
            "escalations_created": escalations_created,
            "escalations_processed": escalations_processed,
        }

    def run_forever(self) -> None:
        """Continue advancing work until stop is requested."""

        failure_streak = 0
        try:
            while not self._stop.is_set():
                try:
                    self.tick()
                except SupervisorActionTimeout:
                    raise
                except Exception as exc:
                    failure_streak = min(failure_streak + 1, 6)
                    error_code = self._exception_code(exc)
                    self.last_loop_error = error_code
                    delay = min(self.poll_interval * (2**failure_streak), 60.0)
                    logger.error(
                        "supervisor loop failed error_code=%s retry_delay_seconds=%g",
                        error_code,
                        delay,
                    )
                else:
                    failure_streak = 0
                    self.last_loop_error = None
                    delay = self.poll_interval
                self._wait_with_lease_renewal(delay)
        finally:
            self.stop()

    def _wait_with_lease_renewal(self, delay: float) -> None:
        deadline = time.monotonic() + delay
        while not self._stop.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            if self._stop.wait(min(remaining, self._lease_refresh_interval)):
                return
            if time.monotonic() >= deadline:
                return
            self._reap()
            self._raise_if_action_overdue()
            try:
                self._renew_active_leases()
            except Exception as exc:
                error_code = self._exception_code(exc)
                self.last_loop_error = error_code
                logger.error(
                    "supervisor lease heartbeat failed error_code=%s",
                    error_code,
                )

    def stop(self) -> None:
        self._stop.set()

    def close(self, *, wait: bool = True, timeout: float | None = None) -> bool:
        self.stop()
        if wait:
            return self.drain(timeout=timeout)
        self._reap()
        return self._total_worker_count() == 0

    def drain(self, timeout: float | None = None) -> bool:
        effective_timeout = self.shutdown_timeout if timeout is None else timeout
        if (
            isinstance(effective_timeout, bool)
            or not isinstance(effective_timeout, (int, float))
            or not math.isfinite(effective_timeout)
            or effective_timeout < 0
        ):
            raise SupervisorError("timeout must be a non-negative finite number")
        deadline = time.monotonic() + effective_timeout
        while True:
            self._reap()
            workers = self._workers()
            if not workers:
                return True
            if self._overdue_action_ids():
                self.stop()
                self.last_loop_error = SupervisorActionTimeout.__name__
                return False
            for worker in workers:
                remaining = max(0.0, deadline - time.monotonic())
                join_timeout = min(remaining, self._lease_refresh_interval)
                if worker.deadline is not None:
                    join_timeout = min(
                        join_timeout,
                        max(0.0, worker.deadline - time.monotonic()),
                    )
                worker.thread.join(timeout=join_timeout)
                if time.monotonic() >= deadline:
                    self._reap()
                    return self._total_worker_count() == 0
            self._reap()
            self._renew_active_leases()

    def _worker_count(self) -> int:
        with self._threads_lock:
            return len(self._action_workers)

    def _escalation_worker_count(self) -> int:
        with self._threads_lock:
            return len(self._escalation_workers)

    def _total_worker_count(self) -> int:
        with self._threads_lock:
            return len(self._action_workers) + len(self._escalation_workers)

    def _workers(self) -> tuple[_ClaimWorker, ...]:
        with self._threads_lock:
            return tuple(self._action_workers.values()) + tuple(
                self._escalation_workers.values()
            )

    def _start_action(self, action: Mapping[str, Any]) -> None:
        action_id = str(action["id"])
        thread = threading.Thread(
            target=self._run_action,
            args=(dict(action),),
            name="mgb-supervisor-" + action_id,
            daemon=True,
        )
        with self._threads_lock:
            self._action_workers[action_id] = _ClaimWorker(
                thread=thread,
                claim_id=action_id,
                version=int(action["version"]),
                deadline=time.monotonic() + self.action_timeout,
            )
            thread.start()

    def _start_escalation(self, escalation: Mapping[str, Any]) -> None:
        escalation_id = str(escalation["id"])
        thread = threading.Thread(
            target=self._run_escalation,
            args=(dict(escalation),),
            name="mgb-escalation-" + escalation_id,
            daemon=True,
        )
        with self._threads_lock:
            self._escalation_workers[escalation_id] = _ClaimWorker(
                thread=thread,
                claim_id=escalation_id,
                version=int(escalation["version"]),
                deadline=None,
            )
            thread.start()

    def _reap(self) -> int:
        with self._threads_lock:
            finished_actions = [
                action_id
                for action_id, worker in self._action_workers.items()
                if not worker.thread.is_alive()
            ]
            finished_escalations = [
                escalation_id
                for escalation_id, worker in self._escalation_workers.items()
                if not worker.thread.is_alive()
            ]
            for action_id in finished_actions:
                self._action_workers.pop(action_id, None)
            for escalation_id in finished_escalations:
                self._escalation_workers.pop(escalation_id, None)
        return len(finished_actions)

    def _overdue_action_ids(self) -> tuple[str, ...]:
        now = time.monotonic()
        with self._threads_lock:
            return tuple(
                action_id
                for action_id, worker in self._action_workers.items()
                if worker.thread.is_alive()
                and worker.deadline is not None
                and now >= worker.deadline
            )

    def _raise_if_action_overdue(self) -> None:
        overdue = self._overdue_action_ids()
        if not overdue:
            return
        self.stop()
        self.last_loop_error = SupervisorActionTimeout.__name__
        raise SupervisorActionTimeout(
            f"supervisor action {overdue[0]} exceeded its "
            f"{self.action_timeout:g} second deadline"
        )

    def _renew_active_leases(self) -> None:
        with self._threads_lock:
            action_workers = tuple(self._action_workers.values())
            escalation_workers = tuple(self._escalation_workers.values())
        now = time.monotonic()
        for worker in action_workers:
            if (
                not worker.thread.is_alive()
                or worker.deadline is not None
                and now >= worker.deadline
            ):
                continue
            with suppress(ConcurrentUpdateError, ConflictError, NotFoundError):
                self.database.renew_supervisor_action_lease(
                    worker.claim_id,
                    owner_id=self.supervisor_id,
                    expected_version=worker.version,
                    lease_seconds=self.action_lease_seconds,
                )
        for worker in escalation_workers:
            if not worker.thread.is_alive():
                continue
            with suppress(ConcurrentUpdateError, ConflictError, NotFoundError):
                self.database.renew_supervisor_escalation_lease(
                    worker.claim_id,
                    owner_id=self.supervisor_id,
                    expected_version=worker.version,
                    lease_seconds=self.action_lease_seconds,
                )

    def _run_action(self, action: dict[str, Any]) -> None:
        try:
            if action["kind"] == "run.execute":
                self.coordinator.execute(action["run_id"])
            elif action["kind"] == "attempt.reconcile":
                self.coordinator.reconcile(action["run_id"])
                if self._defer_renewed_attempt(action):
                    return
            elif action["kind"] == "run.reconcile":
                self.coordinator.reconcile(action["run_id"])
            elif action["kind"] == "workflow.advance":
                if self.workflow_director is None:
                    raise SupervisorError("workflow director is unavailable")
                finished = self.workflow_director.advance(
                    action["workflow_execution_id"],
                    expected_version=action["subject_version"],
                )
                if not finished:
                    self._defer_prescription_action(action)
                    return
            elif action["kind"] == "discovery.advance":
                if self.discovery_director is None:
                    raise SupervisorError("Discovery director is unavailable")
                finished = self.discovery_director.advance(
                    action["discovery_execution_id"],
                    expected_version=action["subject_version"],
                )
                if not finished:
                    self._defer_prescription_action(action)
                    return
            elif action["kind"] == "advisory.advance":
                if self.advisory_director is None:
                    raise SupervisorError("advisory director is unavailable")
                finished = self.advisory_director.advance(
                    action["advisory_execution_id"],
                    expected_version=action["subject_version"],
                )
                if not finished:
                    self._defer_prescription_action(action)
                    return
            else:
                raise SupervisorError("stored supervisor action kind is invalid")
        except Exception as exc:
            error_code = self._exception_code(exc)
            logger.warning(
                "supervisor action failed action_id=%s kind=%s error_code=%s",
                action["id"],
                action["kind"],
                error_code,
            )
            try:
                if self._action_is_obsolete(action):
                    self._complete_action(action)
                else:
                    self.database.fail_supervisor_action(
                        action["id"],
                        owner_id=self.supervisor_id,
                        expected_version=action["version"],
                        error_code=error_code,
                    )
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                pass
            except StorageError as persistence_error:
                logger.error(
                    "supervisor action outcome was not persisted action_id=%s "
                    "error_code=%s",
                    action["id"],
                    self._exception_code(persistence_error),
                )
        else:
            try:
                self._complete_action(action)
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                pass
            except StorageError as persistence_error:
                logger.error(
                    "supervisor action completion was not persisted action_id=%s "
                    "error_code=%s",
                    action["id"],
                    self._exception_code(persistence_error),
                )

    def _complete_action(self, action: Mapping[str, Any]) -> None:
        self.database.complete_supervisor_action(
            str(action["id"]),
            owner_id=self.supervisor_id,
            expected_version=int(action["version"]),
        )

    def _defer_prescription_action(self, action: Mapping[str, Any]) -> None:
        current = datetime.fromisoformat(self.database.now())
        available_at = (current + timedelta(seconds=self.poll_interval)).isoformat(
            timespec="microseconds"
        )
        self.database.defer_supervisor_action(
            str(action["id"]),
            owner_id=self.supervisor_id,
            expected_version=int(action["version"]),
            available_at=available_at,
        )

    def _defer_renewed_attempt(self, action: Mapping[str, Any]) -> bool:
        attempt = self.database.get_run_attempt(str(action["subject_id"]))
        if (
            attempt["state"] not in {"preparing", "running"}
            or attempt["version"] != action["subject_version"]
        ):
            return False
        lease_expires_at = attempt["lease_expires_at"]
        if lease_expires_at is None:
            raise SupervisorError("attempt recovery left ownership unresolved")
        try:
            lease_expiry = datetime.fromisoformat(lease_expires_at)
            now = datetime.fromisoformat(self.database.now())
        except (TypeError, ValueError) as exc:
            raise SupervisorError(
                "attempt recovery returned invalid ownership"
            ) from exc
        if (
            lease_expiry.tzinfo is None
            or lease_expiry.utcoffset() is None
            or now.tzinfo is None
            or now.utcoffset() is None
        ):
            raise SupervisorError("attempt recovery returned invalid ownership")
        if lease_expiry <= now:
            raise SupervisorError("attempt recovery left expired ownership active")
        self.database.defer_supervisor_action(
            str(action["id"]),
            owner_id=self.supervisor_id,
            expected_version=int(action["version"]),
            available_at=lease_expires_at,
        )
        return True

    def _action_is_obsolete(self, action: Mapping[str, Any]) -> bool:
        try:
            if action["kind"] == "run.execute":
                run = self.database.get_run(str(action["run_id"]))
                return (
                    run["state"] != "queued"
                    or run["version"] != action["subject_version"]
                )
            if action["kind"] == "attempt.reconcile":
                attempt = self.database.get_run_attempt(str(action["subject_id"]))
                return (
                    attempt["state"] not in {"preparing", "running"}
                    or attempt["version"] != action["subject_version"]
                )
            if action["kind"] == "workflow.advance":
                execution = self.database.get_workflow_execution(
                    str(action["workflow_execution_id"])
                )
                return (
                    execution["state"] != "active"
                    or execution["version"] != action["subject_version"]
                )
            if action["kind"] == "discovery.advance":
                execution = self.database.get_discovery_execution(
                    str(action["discovery_execution_id"])
                )
                return (
                    execution["state"] != "active"
                    or execution["version"] != action["subject_version"]
                )
            if action["kind"] == "advisory.advance":
                execution = self.database.get_advisory_execution(
                    str(action["advisory_execution_id"])
                )
                return (
                    execution["state"] != "active"
                    or execution["version"] != action["subject_version"]
                )
            run = self.database.get_run(str(action["run_id"]))
            return (
                run["state"] not in {"preparing", "running"}
                or run["version"] != action["subject_version"]
            )
        except (NotFoundError, StorageError):
            return False

    def _start_escalations(self) -> int:
        started = 0
        while (
            started < self.max_escalations_per_tick
            and self._escalation_worker_count() < self.max_workers
        ):
            escalation = self.database.claim_supervisor_escalation(
                owner_id=self.supervisor_id,
                lease_seconds=self.action_lease_seconds,
            )
            if escalation is None:
                break
            self._start_escalation(escalation)
            started += 1
        return started

    def _run_escalation(self, escalation: dict[str, Any]) -> None:
        try:
            interaction = self.database.get_interaction(escalation["subject_id"])
            if interaction["state"] != "open":
                self.database.dismiss_supervisor_escalation(
                    escalation["id"],
                    owner_id=self.supervisor_id,
                    expected_version=escalation["version"],
                )
                return
            event_seq = self.escalation_transport.remind_interaction(
                escalation["subject_id"],
                idempotency_key=escalation["idempotency_key"],
                timeout=self.escalation_timeout,
            )
            self.database.complete_supervisor_escalation(
                escalation["id"],
                owner_id=self.supervisor_id,
                expected_version=escalation["version"],
                krab_event_seq=event_seq,
            )
        except (ConcurrentUpdateError, ConflictError, NotFoundError):
            try:
                self.database.dismiss_supervisor_escalation(
                    escalation["id"],
                    owner_id=self.supervisor_id,
                    expected_version=escalation["version"],
                )
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                pass
            except StorageError as persistence_error:
                logger.error(
                    "supervisor escalation dismissal was not persisted "
                    "escalation_id=%s error_code=%s",
                    escalation["id"],
                    self._exception_code(persistence_error),
                )
        except Exception as exc:
            error_code = self._exception_code(exc)
            logger.warning(
                "supervisor escalation failed escalation_id=%s kind=%s error_code=%s",
                escalation["id"],
                escalation["kind"],
                error_code,
            )
            try:
                self.database.fail_supervisor_escalation(
                    escalation["id"],
                    owner_id=self.supervisor_id,
                    expected_version=escalation["version"],
                    error_code=error_code,
                )
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                pass
            except StorageError as persistence_error:
                logger.error(
                    "supervisor escalation outcome was not persisted "
                    "escalation_id=%s error_code=%s",
                    escalation["id"],
                    self._exception_code(persistence_error),
                )

    @staticmethod
    def _exception_code(exc: Exception) -> str:
        name = re.sub(r"[^A-Za-z0-9_.-]", "", type(exc).__name__)
        if not name or not name[0].isalpha():
            return "SupervisorOperationError"
        return name[:128]
