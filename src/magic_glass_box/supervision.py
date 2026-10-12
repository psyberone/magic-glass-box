from __future__ import annotations

import math
import re
import secrets
import threading
import time
from collections.abc import Mapping
from contextlib import suppress
from datetime import datetime
from typing import Any, Protocol

from .db import (
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
    StorageError,
)


class SupervisorError(RuntimeError):
    """The autonomous supervisor was configured or stopped incorrectly."""


class RunAdvancer(Protocol):
    def execute(self, run_id: str) -> dict[str, Any]: ...

    def reconcile(self, run_id: str | None = None) -> list[dict[str, Any]]: ...


class EscalationTransport(Protocol):
    def remind_interaction(
        self, interaction_id: str, *, idempotency_key: str
    ) -> int: ...


class Supervisor:
    """Drive durable actions while SQLite and attempt leases retain authority."""

    def __init__(
        self,
        database: Database,
        coordinator: RunAdvancer,
        escalation_transport: EscalationTransport,
        *,
        supervisor_id: str | None = None,
        poll_interval: float = 2.0,
        max_workers: int = 4,
        action_lease_seconds: int = 60,
        max_escalations_per_tick: int = 32,
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
        self.supervisor_id = supervisor_id
        self.poll_interval = float(poll_interval)
        self.max_workers = max_workers
        self.action_lease_seconds = action_lease_seconds
        self.max_escalations_per_tick = max_escalations_per_tick
        self._stop = threading.Event()
        self._threads: dict[str, threading.Thread] = {}
        self._threads_lock = threading.Lock()
        self.last_loop_error: str | None = None

    def tick(self, *, wait: bool = False) -> dict[str, int]:
        """Discover, claim, and start one bounded batch of durable work."""

        completed = self._reap()
        actions_created = len(self.database.enqueue_supervisor_actions())
        escalations_created = len(self.database.create_due_supervisor_escalations())
        escalations_processed = self._deliver_escalations()
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
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:
                failure_streak = min(failure_streak + 1, 6)
                self.last_loop_error = type(exc).__name__
                delay = min(self.poll_interval * (2**failure_streak), 60.0)
            else:
                failure_streak = 0
                self.last_loop_error = None
                delay = self.poll_interval
            self._stop.wait(delay)

    def stop(self) -> None:
        self._stop.set()

    def close(self, *, wait: bool = False) -> None:
        self.stop()
        if wait:
            self.drain()
        self._reap()

    def drain(self, timeout: float | None = None) -> bool:
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise SupervisorError("timeout must be a non-negative finite number")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._threads_lock:
                threads = tuple(self._threads.values())
            if not threads:
                return True
            for thread in threads:
                remaining = (
                    None if deadline is None else max(0.0, deadline - time.monotonic())
                )
                thread.join(timeout=remaining)
                if deadline is not None and time.monotonic() >= deadline:
                    self._reap()
                    return self._worker_count() == 0
            self._reap()

    def _worker_count(self) -> int:
        with self._threads_lock:
            return len(self._threads)

    def _start_action(self, action: Mapping[str, Any]) -> None:
        action_id = str(action["id"])
        thread = threading.Thread(
            target=self._run_action,
            args=(dict(action),),
            name="mgb-supervisor-" + action_id,
            daemon=True,
        )
        with self._threads_lock:
            self._threads[action_id] = thread
        thread.start()

    def _reap(self) -> int:
        with self._threads_lock:
            finished = [
                action_id
                for action_id, thread in self._threads.items()
                if not thread.is_alive()
            ]
            for action_id in finished:
                self._threads.pop(action_id, None)
        return len(finished)

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
            else:
                raise SupervisorError("stored supervisor action kind is invalid")
        except Exception as exc:
            try:
                if self._action_is_obsolete(action):
                    self._complete_action(action)
                else:
                    self.database.fail_supervisor_action(
                        action["id"],
                        owner_id=self.supervisor_id,
                        expected_version=action["version"],
                        error_code=self._exception_code(exc),
                    )
            except (
                ConcurrentUpdateError,
                ConflictError,
                NotFoundError,
                StorageError,
            ):
                pass
        else:
            with suppress(
                ConcurrentUpdateError, ConflictError, NotFoundError, StorageError
            ):
                self._complete_action(action)

    def _complete_action(self, action: Mapping[str, Any]) -> None:
        self.database.complete_supervisor_action(
            str(action["id"]),
            owner_id=self.supervisor_id,
            expected_version=int(action["version"]),
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
            run = self.database.get_run(str(action["run_id"]))
            return (
                run["state"] not in {"preparing", "running"}
                or run["version"] != action["subject_version"]
            )
        except (NotFoundError, StorageError):
            return False

    def _deliver_escalations(self) -> int:
        processed = 0
        for _ in range(self.max_escalations_per_tick):
            escalation = self.database.claim_supervisor_escalation(
                owner_id=self.supervisor_id,
                lease_seconds=self.action_lease_seconds,
            )
            if escalation is None:
                break
            processed += 1
            try:
                interaction = self.database.get_interaction(escalation["subject_id"])
                if interaction["state"] != "open":
                    self.database.dismiss_supervisor_escalation(
                        escalation["id"],
                        owner_id=self.supervisor_id,
                        expected_version=escalation["version"],
                    )
                    continue
                event_seq = self.escalation_transport.remind_interaction(
                    escalation["subject_id"],
                    idempotency_key=escalation["idempotency_key"],
                )
                self.database.complete_supervisor_escalation(
                    escalation["id"],
                    owner_id=self.supervisor_id,
                    expected_version=escalation["version"],
                    krab_event_seq=event_seq,
                )
            except (ConcurrentUpdateError, ConflictError, NotFoundError):
                with suppress(
                    ConcurrentUpdateError,
                    ConflictError,
                    NotFoundError,
                    StorageError,
                ):
                    self.database.dismiss_supervisor_escalation(
                        escalation["id"],
                        owner_id=self.supervisor_id,
                        expected_version=escalation["version"],
                    )
            except Exception as exc:
                with suppress(
                    ConcurrentUpdateError,
                    ConflictError,
                    NotFoundError,
                    StorageError,
                ):
                    self.database.fail_supervisor_escalation(
                        escalation["id"],
                        owner_id=self.supervisor_id,
                        expected_version=escalation["version"],
                        error_code=self._exception_code(exc),
                    )
        return processed

    @staticmethod
    def _exception_code(exc: Exception) -> str:
        name = re.sub(r"[^A-Za-z0-9_.-]", "", type(exc).__name__)
        if not name or not name[0].isalpha():
            return "SupervisorOperationError"
        return name[:128]
