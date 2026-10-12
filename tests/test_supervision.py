from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

from magic_glass_box.db import SCHEMA, SUPERVISION_SCHEMA, ConflictError, Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    InteractionKind,
    ProjectContext,
    SupervisionPolicy,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity


class FakeCoordinator:
    def __init__(self) -> None:
        self.execute_calls: list[str] = []
        self.reconcile_calls: list[str] = []
        self.execute_error: Exception | None = None
        self.started: threading.Event | None = None
        self.release: threading.Event | None = None

    def execute(self, run_id: str) -> dict[str, object]:
        self.execute_calls.append(run_id)
        if self.started is not None:
            self.started.set()
        if self.release is not None:
            self.release.wait(timeout=2)
        if self.execute_error is not None:
            raise self.execute_error
        return {"id": run_id, "state": "succeeded"}

    def reconcile(self, run_id: str | None = None) -> list[dict[str, object]]:
        if run_id is not None:
            self.reconcile_calls.append(run_id)
        return []


class FakeEscalationTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.timeouts: list[float] = []
        self.error: Exception | None = None

    def remind_interaction(
        self,
        interaction_id: str,
        *,
        idempotency_key: str,
        timeout: float = 40.0,
    ) -> int:
        self.calls.append((interaction_id, idempotency_key))
        self.timeouts.append(timeout)
        if self.error is not None:
            raise self.error
        return len(self.calls)


class BlockingEscalationTransport(FakeEscalationTransport):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def remind_interaction(
        self,
        interaction_id: str,
        *,
        idempotency_key: str,
        timeout: float = 40.0,
    ) -> int:
        self.started.set()
        self.release.wait(timeout=2)
        return super().remind_interaction(
            interaction_id,
            idempotency_key=idempotency_key,
            timeout=timeout,
        )


class SupervisionDatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Database(Path(self.temporary.name) / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "1" * 32, "agent_one", 1),
            "file:/private/krab-token",
            user_id=self.user["id"],
        )
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping(
                {"purpose": "Exercise autonomous supervision"}
            ),
            created_by=self.user["id"],
        )
        self.item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "supervision-1",
                    "source_updated_at": "2026-10-12T12:00:00Z",
                    "title": "Supervise work",
                    "brief": "Advance this work without an operator acting as cron.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.from_mapping(
                {"image": "agent@sha256:abc", "command": ["agent"]}
            ),
            krab_identity_id=identity["id"],
            created_by=self.user["id"],
        )

    @staticmethod
    def automatic_policy() -> SupervisionPolicy:
        return SupervisionPolicy.from_mapping(
            {
                "schema_version": 1,
                "auto_execute": True,
                "retry_backoff_seconds": [5, 30],
                "interaction_escalation_seconds": [60, 300],
                "attempt_idle_escalation_seconds": [600],
                "attempt_lease_expired_escalation_seconds": [0, 120],
                "other_attention_escalation_seconds": [300],
            }
        )

    def create_run(
        self,
        policy: SupervisionPolicy | None = None,
        *,
        with_session: bool = False,
    ) -> dict[str, object]:
        return self.database.create_run(
            project_key="box-project",
            profile_name="Worker",
            work_item_id=self.item["id"],
            requested_by=self.user["id"],
            supervision_policy=policy,
            krab_session_id="ses_" + "3" * 32 if with_session else None,
        )

    def test_run_pins_explicit_policy_and_defaults_to_manual(self) -> None:
        automatic = self.create_run(self.automatic_policy())
        manual = self.create_run()

        self.assertEqual(
            automatic["supervision_policy"], self.automatic_policy().to_dict()
        )
        self.assertEqual(
            self.database.get_run_execution(automatic["id"])["supervision_policy"],
            self.automatic_policy().to_dict(),
        )
        self.assertEqual(
            manual["supervision_policy"], SupervisionPolicy.disabled().to_dict()
        )
        self.assertEqual(
            automatic["events"][0]["metadata"],
            {"supervision_policy_sha256": self.automatic_policy().digest},
        )

    def test_replacement_inherits_policy_and_stored_policy_is_immutable(self) -> None:
        source = self.create_run(self.automatic_policy())
        terminal = self.database.request_run_cancellation(
            source["id"],
            requested_by=self.user["id"],
            expected_run_version=source["version"],
            reason="exercise retry inheritance",
        )
        successor = self.database.retry_run(
            source["id"],
            requested_by=self.user["id"],
            expected_run_version=terminal["version"],
        )

        self.assertEqual(
            successor["supervision_policy"], self.automatic_policy().to_dict()
        )
        with (
            closing(self.database.connect()) as connection,
            self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"),
        ):
            connection.execute(
                "UPDATE runs SET supervision_policy_json = ? WHERE id = ?",
                (SupervisionPolicy.disabled().canonical, successor["id"]),
            )

    def test_execute_actions_are_idempotent_leased_and_retry_on_finite_backoff(
        self,
    ) -> None:
        automatic = self.create_run(self.automatic_policy())
        self.create_run()
        start = datetime(2027, 1, 1, tzinfo=UTC)

        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            created = self.database.enqueue_supervisor_actions()
            repeated = self.database.enqueue_supervisor_actions()
            first = self.database.claim_supervisor_action(
                owner_id="supervisor-one", lease_seconds=60
            )
            competing = self.database.claim_supervisor_action(
                owner_id="supervisor-two", lease_seconds=60
            )

        self.assertEqual(len(created), 1)
        self.assertEqual(repeated, [])
        self.assertEqual(first["kind"], "run.execute")
        self.assertEqual(first["run_id"], automatic["id"])
        self.assertEqual(first["attempt_count"], 1)
        self.assertEqual(first["max_attempts"], 3)
        self.assertIsNone(competing)

        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            pending = self.database.fail_supervisor_action(
                first["id"],
                owner_id="supervisor-one",
                expected_version=first["version"],
                error_code="RunOrchestrationError",
            )
        self.assertEqual(pending["state"], "pending")
        self.assertEqual(
            pending["available_at"],
            (start + timedelta(seconds=5)).isoformat(timespec="microseconds"),
        )

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=4)).isoformat(
                timespec="microseconds"
            ),
        ):
            self.assertIsNone(
                self.database.claim_supervisor_action(
                    owner_id="supervisor-two", lease_seconds=60
                )
            )
        claimed = None
        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=5)).isoformat(
                timespec="microseconds"
            ),
        ):
            claimed = self.database.claim_supervisor_action(
                owner_id="supervisor-two", lease_seconds=60
            )
            pending = self.database.fail_supervisor_action(
                claimed["id"],
                owner_id="supervisor-two",
                expected_version=claimed["version"],
                error_code="StorageError",
            )
        self.assertEqual(claimed["attempt_count"], 2)
        self.assertEqual(
            pending["available_at"],
            (start + timedelta(seconds=35)).isoformat(timespec="microseconds"),
        )

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=35)).isoformat(
                timespec="microseconds"
            ),
        ):
            final_claim = self.database.claim_supervisor_action(
                owner_id="supervisor-one", lease_seconds=60
            )
            exhausted = self.database.fail_supervisor_action(
                final_claim["id"],
                owner_id="supervisor-one",
                expected_version=final_claim["version"],
                error_code="StorageError",
            )
        self.assertEqual(final_claim["attempt_count"], 3)
        self.assertEqual(exhausted["state"], "exhausted")
        self.assertEqual(exhausted["last_error_code"], "StorageError")

    def test_recovery_actions_only_target_expired_or_orphaned_ownership(self) -> None:
        live_run = self.create_run()
        orphan = self.create_run()
        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            attempt = self.database.begin_run_attempt(
                live_run["id"],
                runtime_adapter="command",
                owner_id="live-owner",
                expected_run_version=live_run["version"],
                lease_seconds=60,
            )
            self.database.transition_run(
                orphan["id"],
                target=self._preparing_state(),
                expected_version=orphan["version"],
            )

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=59)).isoformat(
                timespec="microseconds"
            ),
        ):
            before_expiry = self.database.enqueue_supervisor_actions()
        self.assertEqual(
            [(entry["kind"], entry["run_id"]) for entry in before_expiry],
            [("run.reconcile", orphan["id"])],
        )

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=60)).isoformat(
                timespec="microseconds"
            ),
        ):
            at_expiry = self.database.enqueue_supervisor_actions()
        self.assertEqual(len(at_expiry), 1)
        self.assertEqual(at_expiry[0]["kind"], "attempt.reconcile")
        self.assertEqual(at_expiry[0]["subject_id"], attempt["id"])

    def test_recovery_action_tracks_a_lease_renewal_that_wins_the_claim_race(
        self,
    ) -> None:
        from magic_glass_box.supervision import Supervisor

        run = self.create_run()
        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            attempt = self.database.begin_run_attempt(
                run["id"],
                runtime_adapter="command",
                owner_id="live-owner",
                expected_run_version=run["version"],
                lease_seconds=10,
            )
        renewal_time = start + timedelta(seconds=10)
        renewed_expiry = renewal_time + timedelta(seconds=60)
        with mock.patch.object(
            self.database,
            "now",
            return_value=renewal_time.isoformat(timespec="microseconds"),
        ):
            discovered = self.database.enqueue_supervisor_actions()[0]
            self.database.renew_run_attempt_lease(
                attempt["id"], owner_id="live-owner", lease_seconds=60
            )
            coordinator = FakeCoordinator()
            supervisor = Supervisor(
                self.database,
                coordinator,
                FakeEscalationTransport(),
                supervisor_id="supervisor-renewal-race",
                max_workers=1,
            )
            self.addCleanup(supervisor.close)
            supervisor.tick(wait=True)

        deferred = self.database.get_supervisor_action(discovered["id"])
        self.assertEqual(coordinator.reconcile_calls, [run["id"]])
        self.assertEqual(deferred["state"], "pending")
        self.assertEqual(deferred["attempt_count"], 0)
        self.assertEqual(
            deferred["available_at"],
            renewed_expiry.isoformat(timespec="microseconds"),
        )
        with mock.patch.object(
            self.database,
            "now",
            return_value=(renewed_expiry - timedelta(microseconds=1)).isoformat(
                timespec="microseconds"
            ),
        ):
            self.assertIsNone(
                self.database.claim_supervisor_action(
                    owner_id="another-supervisor", lease_seconds=60
                )
            )

    def test_supervisor_retries_failed_actions_on_policy_backoff_then_exhausts(
        self,
    ) -> None:
        from magic_glass_box.supervision import Supervisor

        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            run = self.create_run(self.automatic_policy())
        coordinator = FakeCoordinator()
        coordinator.execute_error = RuntimeError("private upstream failure detail")
        supervisor = Supervisor(
            self.database,
            coordinator,
            FakeEscalationTransport(),
            supervisor_id="supervisor-retry",
            max_workers=1,
        )
        self.addCleanup(supervisor.close)

        for seconds, expected_calls in ((0, 1), (4, 1), (5, 2), (34, 2), (35, 3)):
            with mock.patch.object(
                self.database,
                "now",
                return_value=(start + timedelta(seconds=seconds)).isoformat(
                    timespec="microseconds"
                ),
            ):
                supervisor.tick(wait=True)
            self.assertEqual(len(coordinator.execute_calls), expected_calls)

        action = self.database.list_supervisor_actions()[0]
        self.assertEqual(action["run_id"], run["id"])
        self.assertEqual(action["state"], "exhausted")
        self.assertEqual(action["attempt_count"], 3)
        self.assertEqual(action["last_error_code"], "RuntimeError")
        self.assertNotIn("private upstream failure detail", str(action))

    def test_escalations_are_threshold_deduplicated_and_bodyless(self) -> None:
        start = datetime(2027, 1, 1, tzinfo=UTC)
        timestamp = start.isoformat(timespec="microseconds")
        with mock.patch.object(self.database, "now", return_value=timestamp):
            run = self.create_run(self.automatic_policy(), with_session=True)
            attempt = self.database.begin_run_attempt(
                run["id"],
                runtime_adapter="command",
                owner_id="live-owner",
                expected_run_version=run["version"],
                lease_seconds=3600,
            )
            running = self.database.start_run_attempt(
                attempt["id"],
                expected_attempt_version=attempt["version"],
                expected_run_version=attempt["run_version"],
                source_revision="a" * 40,
                container_id="b" * 64,
                container_name="magic-glass-box-supervision",
                image_id="sha256:" + "c" * 64,
            )
            interaction = self.database.open_interaction(
                run["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id=self.identity_id(),
                request_item_id="itm_" + "4" * 32,
                expected_run_version=running["run_version"],
            )

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=59)).isoformat(
                timespec="microseconds"
            ),
        ):
            self.assertEqual(self.database.create_due_supervisor_escalations(), [])

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=60)).isoformat(
                timespec="microseconds"
            ),
        ):
            created = self.database.create_due_supervisor_escalations()
            repeated = self.database.create_due_supervisor_escalations()
            claimed = self.database.claim_supervisor_escalation(
                owner_id="supervisor-one", lease_seconds=60
            )
            competing = self.database.claim_supervisor_escalation(
                owner_id="supervisor-two", lease_seconds=60
            )
            delivered = self.database.complete_supervisor_escalation(
                claimed["id"],
                owner_id="supervisor-one",
                expected_version=claimed["version"],
                krab_event_seq=7,
            )

        self.assertEqual(len(created), 1)
        self.assertEqual(repeated, [])
        self.assertEqual(claimed["kind"], "interaction.pending")
        self.assertEqual(claimed["subject_id"], interaction["id"])
        self.assertEqual(claimed["threshold_seconds"], 60)
        self.assertEqual(claimed["channel"], "krab_reminder")
        self.assertIsNone(competing)
        self.assertEqual(delivered["state"], "delivered")
        self.assertEqual(delivered["krab_event_seq"], 7)

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=300)).isoformat(
                timespec="microseconds"
            ),
        ):
            second = self.database.create_due_supervisor_escalations()
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["threshold_seconds"], 300)
        encoded = str(self.database.list_supervisor_escalations()).lower()
        for forbidden in ("question", "answer", "body", "prompt", "transcript"):
            self.assertNotIn(f"'{forbidden}'", encoded)

    def test_claimed_action_lease_can_be_renewed_without_changing_its_version(
        self,
    ) -> None:
        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            self.create_run(self.automatic_policy())
            self.database.enqueue_supervisor_actions()
            claimed = self.database.claim_supervisor_action(
                owner_id="supervisor-one", lease_seconds=10
            )

        self.assertIsNotNone(claimed)
        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=5)).isoformat(
                timespec="microseconds"
            ),
        ):
            renewed = self.database.renew_supervisor_action_lease(
                claimed["id"],
                owner_id="supervisor-one",
                expected_version=claimed["version"],
                lease_seconds=10,
            )
        self.assertEqual(renewed["version"], claimed["version"])
        self.assertEqual(renewed["attempt_count"], claimed["attempt_count"])
        self.assertEqual(
            renewed["lease_expires_at"],
            (start + timedelta(seconds=15)).isoformat(timespec="microseconds"),
        )

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=10)).isoformat(
                timespec="microseconds"
            ),
        ):
            self.assertIsNone(
                self.database.claim_supervisor_action(
                    owner_id="supervisor-two", lease_seconds=10
                )
            )
            with self.assertRaisesRegex(ConflictError, "owned by this process"):
                self.database.renew_supervisor_action_lease(
                    claimed["id"],
                    owner_id="supervisor-two",
                    expected_version=claimed["version"],
                    lease_seconds=10,
                )

    def test_claimed_escalation_lease_can_be_renewed_by_its_owner(self) -> None:
        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            run = self.create_run(self.automatic_policy(), with_session=True)
            attempt = self.database.begin_run_attempt(
                run["id"],
                runtime_adapter="command",
                owner_id="live-owner",
                expected_run_version=run["version"],
                lease_seconds=3600,
            )
            running = self.database.start_run_attempt(
                attempt["id"],
                expected_attempt_version=attempt["version"],
                expected_run_version=attempt["run_version"],
                source_revision="a" * 40,
                container_id="b" * 64,
                container_name="magic-glass-box-reminder-heartbeat",
                image_id="sha256:" + "c" * 64,
            )
            self.database.open_interaction(
                run["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id=self.identity_id(),
                request_item_id="itm_" + "d" * 32,
                expected_run_version=running["run_version"],
            )

        due = start + timedelta(seconds=60)
        with mock.patch.object(
            self.database, "now", return_value=due.isoformat(timespec="microseconds")
        ):
            self.database.create_due_supervisor_escalations()
            claimed = self.database.claim_supervisor_escalation(
                owner_id="supervisor-one", lease_seconds=10
            )

        self.assertIsNotNone(claimed)
        heartbeat = due + timedelta(seconds=5)
        with mock.patch.object(
            self.database,
            "now",
            return_value=heartbeat.isoformat(timespec="microseconds"),
        ):
            renewed = self.database.renew_supervisor_escalation_lease(
                claimed["id"],
                owner_id="supervisor-one",
                expected_version=claimed["version"],
                lease_seconds=10,
            )

        self.assertEqual(renewed["version"], claimed["version"])
        self.assertEqual(
            renewed["lease_expires_at"],
            (heartbeat + timedelta(seconds=10)).isoformat(timespec="microseconds"),
        )
        with self.assertRaisesRegex(ConflictError, "owned by this process"):
            self.database.renew_supervisor_escalation_lease(
                claimed["id"],
                owner_id="supervisor-two",
                expected_version=claimed["version"],
                lease_seconds=10,
            )

    def test_idle_attempt_age_restarts_at_the_latest_owner_heartbeat(self) -> None:
        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            run = self.create_run(self.automatic_policy())
            attempt = self.database.begin_run_attempt(
                run["id"],
                runtime_adapter="command",
                owner_id="live-owner",
                expected_run_version=run["version"],
                lease_seconds=3600,
            )
            attempt = self.database.start_run_attempt(
                attempt["id"],
                expected_attempt_version=attempt["version"],
                expected_run_version=attempt["run_version"],
                source_revision="a" * 40,
                container_id="b" * 64,
                container_name="magic-glass-box-heartbeat",
                image_id="sha256:" + "c" * 64,
            )

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=500)).isoformat(
                timespec="microseconds"
            ),
        ):
            self.database.renew_run_attempt_lease(
                attempt["id"], owner_id="live-owner", lease_seconds=3600
            )
        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=600)).isoformat(
                timespec="microseconds"
            ),
        ):
            self.assertEqual(self.database.create_due_supervisor_escalations(), [])
        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=1100)).isoformat(
                timespec="microseconds"
            ),
        ):
            escalations = self.database.create_due_supervisor_escalations()

        idle = [item for item in escalations if item["kind"] == "attempt.idle"]
        self.assertEqual(len(idle), 1)
        self.assertEqual(
            idle[0]["age_started_at"],
            (start + timedelta(seconds=500)).isoformat(timespec="microseconds"),
        )

    def identity_id(self) -> str:
        with closing(self.database.connect()) as connection:
            return connection.execute(
                "SELECT id FROM krab_identities WHERE handle = 'agent_one'"
            ).fetchone()[0]

    def test_supervisor_advances_only_opted_in_runs_and_one_process_claims_work(
        self,
    ) -> None:
        from magic_glass_box.supervision import Supervisor

        automatic = self.create_run(self.automatic_policy())
        manual = self.create_run()
        first_coordinator = FakeCoordinator()
        first_coordinator.started = threading.Event()
        first_coordinator.release = threading.Event()
        second_coordinator = FakeCoordinator()
        first = Supervisor(
            self.database,
            first_coordinator,
            FakeEscalationTransport(),
            supervisor_id="supervisor-one",
            max_workers=1,
        )
        second = Supervisor(
            self.database,
            second_coordinator,
            FakeEscalationTransport(),
            supervisor_id="supervisor-two",
            max_workers=1,
        )
        self.addCleanup(first.close)
        self.addCleanup(second.close)

        first.tick()
        self.assertTrue(first_coordinator.started.wait(timeout=1))
        second.tick(wait=True)

        self.assertEqual(first_coordinator.execute_calls, [automatic["id"]])
        self.assertEqual(second_coordinator.execute_calls, [])
        self.assertEqual(self.database.get_run(manual["id"])["state"], "queued")

        first_coordinator.release.set()
        first.drain(timeout=1)
        first.tick(wait=True)
        self.assertEqual(first_coordinator.execute_calls, [automatic["id"]])
        self.assertEqual(
            self.database.list_supervisor_actions()[0]["state"], "succeeded"
        )

    def test_supervisor_configuration_is_bounded_and_drain_timeout_is_finite(
        self,
    ) -> None:
        from magic_glass_box.supervision import Supervisor, SupervisorError

        cases = (
            ({"poll_interval": 0.049}, "poll_interval"),
            ({"poll_interval": 60.01}, "poll_interval"),
            ({"poll_interval": float("nan")}, "poll_interval"),
            ({"poll_interval": True}, "poll_interval"),
            ({"max_workers": 0}, "max_workers"),
            ({"max_workers": 33}, "max_workers"),
            ({"max_workers": True}, "max_workers"),
            ({"action_lease_seconds": 9}, "action_lease_seconds"),
            ({"action_lease_seconds": 3601}, "action_lease_seconds"),
            ({"action_lease_seconds": True}, "action_lease_seconds"),
            ({"max_escalations_per_tick": 0}, "max_escalations_per_tick"),
            ({"max_escalations_per_tick": 257}, "max_escalations_per_tick"),
            ({"max_escalations_per_tick": True}, "max_escalations_per_tick"),
            ({"escalation_timeout": 0.049}, "escalation_timeout"),
            ({"escalation_timeout": 60.01}, "escalation_timeout"),
            ({"escalation_timeout": float("nan")}, "escalation_timeout"),
            ({"escalation_timeout": True}, "escalation_timeout"),
            (
                {"action_lease_seconds": 10, "escalation_timeout": 10},
                "escalation_timeout",
            ),
            ({"shutdown_timeout": 0.049}, "shutdown_timeout"),
            ({"shutdown_timeout": 300.01}, "shutdown_timeout"),
            ({"shutdown_timeout": float("nan")}, "shutdown_timeout"),
            ({"shutdown_timeout": True}, "shutdown_timeout"),
            ({"action_timeout": 0.049}, "action_timeout"),
            ({"action_timeout": 2_592_000.01}, "action_timeout"),
            ({"action_timeout": float("nan")}, "action_timeout"),
            ({"action_timeout": True}, "action_timeout"),
            ({"supervisor_id": ""}, "supervisor_id"),
            ({"supervisor_id": "x" * 129}, "supervisor_id"),
            ({"supervisor_id": "supervisor\x00bad"}, "supervisor_id"),
            ({"supervisor_id": 7}, "supervisor_id"),
        )
        for kwargs, message in cases:
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaisesRegex(SupervisorError, message),
            ):
                Supervisor(
                    self.database,
                    FakeCoordinator(),
                    FakeEscalationTransport(),
                    **kwargs,
                )

        for kwargs in (
            {"poll_interval": 0.05},
            {"poll_interval": 60.0},
            {"max_workers": 1},
            {"max_workers": 32},
            {"action_lease_seconds": 10},
            {"action_lease_seconds": 3600},
            {"max_escalations_per_tick": 1},
            {"max_escalations_per_tick": 256},
            {"action_lease_seconds": 10, "escalation_timeout": 0.05},
            {"action_lease_seconds": 3600, "escalation_timeout": 60.0},
            {"shutdown_timeout": 0.05},
            {"shutdown_timeout": 300.0},
            {"action_timeout": 0.05},
            {"action_timeout": 2_592_000.0},
            {"supervisor_id": "x" * 128},
        ):
            with self.subTest(valid=kwargs):
                supervisor = Supervisor(
                    self.database,
                    FakeCoordinator(),
                    FakeEscalationTransport(),
                    **kwargs,
                )
                supervisor.close()

        supervisor = Supervisor(
            self.database,
            FakeCoordinator(),
            FakeEscalationTransport(),
        )
        self.addCleanup(supervisor.close)
        self.assertTrue(supervisor.drain(timeout=0))
        for timeout in (-1, float("nan"), True):
            with (
                self.subTest(timeout=timeout),
                self.assertRaisesRegex(SupervisorError, "timeout"),
            ):
                supervisor.drain(timeout=timeout)

    def test_close_drains_running_workers_by_default_with_a_bounded_wait(self) -> None:
        from magic_glass_box.supervision import Supervisor

        self.create_run(self.automatic_policy())
        coordinator = FakeCoordinator()
        coordinator.started = threading.Event()
        coordinator.release = threading.Event()
        supervisor = Supervisor(
            self.database,
            coordinator,
            FakeEscalationTransport(),
            supervisor_id="supervisor-shutdown",
            shutdown_timeout=1,
        )
        self.addCleanup(coordinator.release.set)
        self.addCleanup(supervisor.close, wait=False)
        supervisor.tick()
        self.assertTrue(coordinator.started.wait(timeout=1))

        closed: list[bool] = []
        close_finished = threading.Event()

        def close() -> None:
            closed.append(supervisor.close())
            close_finished.set()

        closer = threading.Thread(target=close)
        closer.start()
        self.assertFalse(close_finished.wait(timeout=0.05))
        coordinator.release.set()
        closer.join(timeout=1)

        self.assertFalse(closer.is_alive())
        self.assertEqual(closed, [True])

    def test_close_returns_false_when_a_worker_outlives_the_shutdown_budget(
        self,
    ) -> None:
        from magic_glass_box.supervision import Supervisor

        self.create_run(self.automatic_policy())
        coordinator = FakeCoordinator()
        coordinator.started = threading.Event()
        coordinator.release = threading.Event()
        supervisor = Supervisor(
            self.database,
            coordinator,
            FakeEscalationTransport(),
            supervisor_id="supervisor-bounded-shutdown",
            shutdown_timeout=0.05,
        )
        self.addCleanup(coordinator.release.set)
        self.addCleanup(supervisor.close, wait=False)
        supervisor.tick()
        self.assertTrue(coordinator.started.wait(timeout=1))

        started = time.monotonic()
        self.assertFalse(supervisor.close())
        self.assertLess(time.monotonic() - started, 0.5)

        coordinator.release.set()
        self.assertTrue(supervisor.drain(timeout=1))

    def test_active_supervisor_worker_renews_its_durable_claim(self) -> None:
        from magic_glass_box.supervision import Supervisor

        start = datetime(2027, 1, 1, tzinfo=UTC)
        coordinator = FakeCoordinator()
        coordinator.started = threading.Event()
        coordinator.release = threading.Event()
        supervisor = Supervisor(
            self.database,
            coordinator,
            FakeEscalationTransport(),
            supervisor_id="supervisor-heartbeat",
            max_workers=1,
            action_lease_seconds=10,
        )
        self.addCleanup(coordinator.release.set)
        self.addCleanup(supervisor.close, wait=False)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            self.create_run(self.automatic_policy())
            supervisor.tick()
        self.assertTrue(coordinator.started.wait(timeout=1))

        heartbeat = start + timedelta(seconds=9)
        with mock.patch.object(
            self.database,
            "now",
            return_value=heartbeat.isoformat(timespec="microseconds"),
        ):
            supervisor.tick()
            action = self.database.list_supervisor_actions()[0]

        self.assertEqual(
            action["lease_expires_at"],
            (heartbeat + timedelta(seconds=10)).isoformat(timespec="microseconds"),
        )
        coordinator.release.set()
        self.assertTrue(supervisor.drain(timeout=1))

    def test_overdue_action_stops_renewal_and_fails_the_supervisor(self) -> None:
        from magic_glass_box.supervision import (
            Supervisor,
            SupervisorActionTimeout,
        )

        self.create_run(self.automatic_policy())
        coordinator = FakeCoordinator()
        coordinator.started = threading.Event()
        coordinator.release = threading.Event()
        with mock.patch(
            "magic_glass_box.supervision.time.monotonic", return_value=100.0
        ):
            supervisor = Supervisor(
                self.database,
                coordinator,
                FakeEscalationTransport(),
                supervisor_id="supervisor-action-deadline",
                action_timeout=0.05,
            )
            supervisor.tick()
        self.addCleanup(coordinator.release.set)
        self.addCleanup(supervisor.close, wait=False)
        self.assertTrue(coordinator.started.wait(timeout=1))

        with (
            mock.patch(
                "magic_glass_box.supervision.time.monotonic", return_value=100.049
            ),
            mock.patch.object(
                self.database,
                "renew_supervisor_action_lease",
                wraps=self.database.renew_supervisor_action_lease,
            ) as renew,
        ):
            supervisor.tick()
        renew.assert_called_once()

        with (
            mock.patch(
                "magic_glass_box.supervision.time.monotonic", return_value=100.05
            ),
            mock.patch.object(
                self.database,
                "renew_supervisor_action_lease",
                wraps=self.database.renew_supervisor_action_lease,
            ) as renew,
            self.assertRaisesRegex(
                SupervisorActionTimeout, "exceeded its 0.05 second deadline"
            ),
        ):
            supervisor.tick()
        renew.assert_not_called()
        self.assertEqual(supervisor.last_loop_error, "SupervisorActionTimeout")

        coordinator.release.set()
        with mock.patch(
            "magic_glass_box.supervision.time.monotonic", return_value=100.049
        ):
            self.assertTrue(supervisor.drain(timeout=1))

    def test_run_forever_propagates_a_fatal_action_timeout(self) -> None:
        from magic_glass_box.supervision import (
            Supervisor,
            SupervisorActionTimeout,
        )

        supervisor = Supervisor(
            self.database,
            FakeCoordinator(),
            FakeEscalationTransport(),
        )
        with (
            mock.patch.object(
                supervisor,
                "tick",
                side_effect=SupervisorActionTimeout("action deadline exceeded"),
            ),
            self.assertRaisesRegex(SupervisorActionTimeout, "deadline exceeded"),
        ):
            supervisor.run_forever()

        self.assertTrue(supervisor.close())

    def test_answered_work_and_expired_ownership_advance_automatically(self) -> None:
        from magic_glass_box.supervision import Supervisor

        start = datetime(2027, 1, 1, tzinfo=UTC)
        timestamp = start.isoformat(timespec="microseconds")
        with mock.patch.object(self.database, "now", return_value=timestamp):
            answered_run = self.create_run(self.automatic_policy(), with_session=True)
            answered_attempt = self.database.begin_run_attempt(
                answered_run["id"],
                runtime_adapter="command",
                owner_id="finished-owner",
                expected_run_version=answered_run["version"],
                lease_seconds=60,
            )
            running = self.database.start_run_attempt(
                answered_attempt["id"],
                expected_attempt_version=answered_attempt["version"],
                expected_run_version=answered_attempt["run_version"],
                source_revision="a" * 40,
                container_id="b" * 64,
                container_name="magic-glass-box-answered",
                image_id="sha256:" + "c" * 64,
            )
            interaction = self.database.open_interaction(
                answered_run["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id=self.identity_id(),
                request_item_id="itm_" + "5" * 32,
                expected_run_version=running["run_version"],
            )
            active = self.database.get_run_attempt(answered_attempt["id"])
            self.database.fail_run_attempt(
                active["id"],
                expected_attempt_version=active["version"],
                reason="worker handed control back",
                interrupted=True,
            )
            self.database.answer_interaction(
                interaction["id"],
                response_item_id="itm_" + "6" * 32,
                expected_version=interaction["version"],
            )
            expired_run = self.create_run()
            expired_attempt = self.database.begin_run_attempt(
                expired_run["id"],
                runtime_adapter="command",
                owner_id="dead-owner",
                expected_run_version=expired_run["version"],
                lease_seconds=10,
            )
            live_run = self.create_run()
            self.database.begin_run_attempt(
                live_run["id"],
                runtime_adapter="command",
                owner_id="live-owner",
                expected_run_version=live_run["version"],
                lease_seconds=60,
            )

        coordinator = FakeCoordinator()
        supervisor = Supervisor(
            self.database,
            coordinator,
            FakeEscalationTransport(),
            supervisor_id="supervisor-recovery",
            max_workers=3,
        )
        self.addCleanup(supervisor.close)
        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=10)).isoformat(
                timespec="microseconds"
            ),
        ):
            supervisor.tick(wait=True)

        self.assertEqual(coordinator.execute_calls, [answered_run["id"]])
        self.assertEqual(coordinator.reconcile_calls, [expired_run["id"]])
        self.assertNotIn(live_run["id"], coordinator.reconcile_calls)
        action = next(
            item
            for item in self.database.list_supervisor_actions()
            if item["kind"] == "attempt.reconcile"
        )
        self.assertEqual(action["subject_id"], expired_attempt["id"])

    def test_supervisor_delivers_each_stalled_question_threshold_once(self) -> None:
        from magic_glass_box.supervision import Supervisor

        start = datetime(2027, 1, 1, tzinfo=UTC)
        timestamp = start.isoformat(timespec="microseconds")
        with mock.patch.object(self.database, "now", return_value=timestamp):
            run = self.create_run(self.automatic_policy(), with_session=True)
            attempt = self.database.begin_run_attempt(
                run["id"],
                runtime_adapter="command",
                owner_id="live-owner",
                expected_run_version=run["version"],
                lease_seconds=3600,
            )
            running = self.database.start_run_attempt(
                attempt["id"],
                expected_attempt_version=attempt["version"],
                expected_run_version=attempt["run_version"],
                source_revision="a" * 40,
                container_id="b" * 64,
                container_name="magic-glass-box-stalled",
                image_id="sha256:" + "c" * 64,
            )
            interaction = self.database.open_interaction(
                run["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id=self.identity_id(),
                request_item_id="itm_" + "7" * 32,
                expected_run_version=running["run_version"],
            )

        transport = FakeEscalationTransport()
        supervisor = Supervisor(
            self.database,
            FakeCoordinator(),
            transport,
            supervisor_id="supervisor-escalation",
            max_workers=1,
        )
        self.addCleanup(supervisor.close)
        for seconds in (60, 60, 300, 300):
            with mock.patch.object(
                self.database,
                "now",
                return_value=(start + timedelta(seconds=seconds)).isoformat(
                    timespec="microseconds"
                ),
            ):
                supervisor.tick(wait=True)

        self.assertEqual(
            [call[0] for call in transport.calls],
            [interaction["id"], interaction["id"]],
        )
        self.assertEqual(len({call[1] for call in transport.calls}), 2)
        self.assertEqual(
            [item["state"] for item in self.database.list_supervisor_escalations()],
            ["delivered", "delivered"],
        )

    def test_stalled_reminder_does_not_block_the_supervisor_tick(self) -> None:
        from magic_glass_box.supervision import Supervisor

        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            run = self.create_run(self.automatic_policy(), with_session=True)
            attempt = self.database.begin_run_attempt(
                run["id"],
                runtime_adapter="command",
                owner_id="live-owner",
                expected_run_version=run["version"],
                lease_seconds=3600,
            )
            running = self.database.start_run_attempt(
                attempt["id"],
                expected_attempt_version=attempt["version"],
                expected_run_version=attempt["run_version"],
                source_revision="a" * 40,
                container_id="b" * 64,
                container_name="magic-glass-box-nonblocking-reminder",
                image_id="sha256:" + "c" * 64,
            )
            self.database.open_interaction(
                run["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id=self.identity_id(),
                request_item_id="itm_" + "a" * 32,
                expected_run_version=running["run_version"],
            )

        transport = BlockingEscalationTransport()
        supervisor = Supervisor(
            self.database,
            FakeCoordinator(),
            transport,
            supervisor_id="supervisor-nonblocking-reminder",
            action_lease_seconds=60,
            escalation_timeout=5,
        )
        self.addCleanup(transport.release.set)
        self.addCleanup(supervisor.close, wait=False)
        tick_finished = threading.Event()

        def tick() -> None:
            with mock.patch.object(
                self.database,
                "now",
                return_value=(start + timedelta(seconds=60)).isoformat(
                    timespec="microseconds"
                ),
            ):
                supervisor.tick()
            tick_finished.set()

        runner = threading.Thread(target=tick)
        runner.start()
        try:
            self.assertTrue(transport.started.wait(timeout=1))
            self.assertTrue(tick_finished.wait(timeout=0.2))
        finally:
            transport.release.set()
            runner.join(timeout=1)
        self.assertFalse(runner.is_alive())
        self.assertTrue(supervisor.drain(timeout=1))
        self.assertEqual(transport.timeouts, [5.0])

    def test_supervisor_dismisses_a_reminder_answered_before_delivery(self) -> None:
        from magic_glass_box.supervision import Supervisor

        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            run = self.create_run(self.automatic_policy(), with_session=True)
            attempt = self.database.begin_run_attempt(
                run["id"],
                runtime_adapter="command",
                owner_id="live-owner",
                expected_run_version=run["version"],
                lease_seconds=3600,
            )
            running = self.database.start_run_attempt(
                attempt["id"],
                expected_attempt_version=attempt["version"],
                expected_run_version=attempt["run_version"],
                source_revision="a" * 40,
                container_id="b" * 64,
                container_name="magic-glass-box-stale-reminder",
                image_id="sha256:" + "c" * 64,
            )
            interaction = self.database.open_interaction(
                run["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_identity_id=self.identity_id(),
                request_item_id="itm_" + "8" * 32,
                expected_run_version=running["run_version"],
            )

        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=60)).isoformat(
                timespec="microseconds"
            ),
        ):
            self.database.create_due_supervisor_escalations()
            answered = self.database.answer_interaction(
                interaction["id"],
                response_item_id="itm_" + "9" * 32,
                expected_version=interaction["version"],
            )

        transport = FakeEscalationTransport()
        supervisor = Supervisor(
            self.database,
            FakeCoordinator(),
            transport,
            supervisor_id="supervisor-stale-reminder",
            max_workers=1,
        )
        self.addCleanup(supervisor.close)
        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=61)).isoformat(
                timespec="microseconds"
            ),
        ):
            supervisor.tick(wait=True)

        self.assertEqual(answered["state"], "answered")
        self.assertEqual(transport.calls, [])
        self.assertEqual(
            self.database.list_supervisor_escalations()[0]["state"], "dismissed"
        )

    @staticmethod
    def _preparing_state():
        from magic_glass_box.domain import RunState

        return RunState.PREPARING


class SupervisionMigrationTests(unittest.TestCase):
    def test_schema_nine_migrates_once_and_defaults_existing_runs_to_manual(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "schema-nine.db"
            connection = sqlite3.connect(path)
            connection.executescript(SCHEMA.removesuffix(SUPERVISION_SCHEMA))
            connection.execute(
                "INSERT INTO users "
                "(id, handle, display_name, active, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?)",
                ("boxu_" + "1" * 32, "operator", "Operator", "then", "then"),
            )
            connection.execute("PRAGMA user_version = 9")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], 10)
            self.assertEqual(second, first)
            self.assertEqual(first["counts"]["users"], 1)
            self.assertEqual(first["counts"]["supervisor_actions"], 0)
            self.assertEqual(first["counts"]["supervisor_escalations"], 0)
            with closing(database.connect()) as migrated:
                tables = {
                    row[0]
                    for row in migrated.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table'"
                    )
                }
                run_columns = {
                    row[1] for row in migrated.execute("PRAGMA table_info(runs)")
                }
            self.assertIn("supervisor_actions", tables)
            self.assertIn("supervisor_escalations", tables)
            self.assertIn("supervision_policy_json", run_columns)


if __name__ == "__main__":
    unittest.main()
