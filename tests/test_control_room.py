from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from magic_glass_box.candidates import CandidateManifest
from magic_glass_box.control_room import ControlRoom
from magic_glass_box.credentials import CredentialError
from magic_glass_box.db import SCHEMA_VERSION, Database, StorageError
from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    InteractionKind,
    ProjectContext,
    ProviderGrantSpec,
    SupervisionPolicy,
    WorkAssignmentSpec,
    WorkItemEnvelope,
    WorkLogEntry,
)
from magic_glass_box.gates import GatePolicyManifest, GatePolicySpec
from magic_glass_box.inference import InferenceConnectorSpec, InferenceGrantSpec
from magic_glass_box.krab import (
    KrabError,
    KrabHTTPError,
    KrabIdentity,
    KrabProject,
    KrabUserRef,
    WorkClaim,
    WorkClaimPage,
)
from magic_glass_box.results import ResultManifest
from magic_glass_box.service import ControlPlane


class FakeResolver:
    def __init__(self, credential: str = "resolved-token") -> None:
        self.token = credential
        self.references: list[str] = []

    def resolve(self, reference: str) -> str:
        self.references.append(reference)
        return self.token


class FailingResolver(FakeResolver):
    def resolve(self, reference: str) -> str:
        self.references.append(reference)
        raise CredentialError("credential is intentionally unavailable")


class FakeKrabClient:
    def __init__(
        self,
        identity: KrabIdentity,
        projects: tuple[KrabProject, ...],
        claims: dict[str, WorkClaimPage],
    ) -> None:
        self.identity = identity
        self.visible_projects = projects
        self.claims = claims
        self.claim_calls: list[tuple[str, str]] = []

    def whoami(self) -> KrabIdentity:
        return self.identity

    def projects(self) -> tuple[KrabProject, ...]:
        return self.visible_projects

    def work_claims(self, project_key: str, *, kind: str) -> WorkClaimPage:
        self.claim_calls.append((project_key, kind))
        return self.claims[kind]


class ControlRoomTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Database(Path(self.temporary.name) / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Local Operator")
        self.instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.krab_identity = KrabIdentity(
            "usr_" + "1" * 32,
            "agent_one",
            3,
        )
        self.identity = self.database.store_krab_identity(
            self.instance["id"],
            self.krab_identity,
            "file:/private/credential-super-secret",
            user_id=self.user["id"],
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping(
                {"purpose": "Exercise the control-room projection"}
            ),
            created_by=self.user["id"],
        )
        connector_spec = InferenceConnectorSpec.from_mapping(
            {
                "schema_version": 1,
                "connector_protocol_version": 1,
                "deployment_sha256": "a" * 64,
                "transport": {
                    "kind": "unix",
                    "socket_path": str(
                        Path(self.temporary.name) / "private-connector.sock"
                    ),
                },
                "routes": [
                    {
                        "name": "chatgpt-main",
                        "provider": "openai",
                        "protocol": "openai-responses",
                        "models": ["gpt-test"],
                        "credential_scope_sha256": "b" * 64,
                    }
                ],
                "request_bytes": 32768,
                "response_bytes": 1048576,
                "timeout_ms": 30000,
                "max_concurrency": 2,
            }
        )
        self.connector = self.database.register_inference_connector(
            name="openai-personal",
            spec=connector_spec,
            created_by=self.user["id"],
        )
        self.policy = InferenceGrantSpec.from_mapping(
            {
                "schema_version": 1,
                "connector_name": "openai-personal",
                "deployment_sha256": "a" * 64,
                "route": "chatgpt-main",
                "credential_scope_sha256": "b" * 64,
                "provider": "openai",
                "protocol": "openai-responses",
                "model": "gpt-test",
                "request_bytes": 32768,
                "response_bytes": 1048576,
                "timeout_ms": 30000,
                "max_concurrency": 2,
            }
        )
        self.grant = self.database.create_connector_provider_grant(
            name="openai-main",
            spec=self.policy,
            expires_at=None,
            created_by=self.user["id"],
        )
        self.profile = self.database.create_agent_profile(
            name="Codex Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc",
                command=["codex", "exec", "--secret-command-arg"],
                credential_names=["openai-main"],
                gateway_capabilities=[GatewayCapability.PROVIDER_INVOKE],
            ),
            krab_identity_id=self.identity["id"],
            created_by=self.user["id"],
        )
        self.item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "github",
                    "external_id": "owner/repository#42",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Visible work-item title",
                    "brief": "private work item brief",
                }
            ),
            imported_by=self.user["id"],
        )

    def create_run(
        self,
        *,
        claim: bool = False,
        supervision_policy: SupervisionPolicy | None = None,
    ) -> dict[str, Any]:
        return self.database.create_run(
            project_key="box-project",
            profile_name="Codex Worker",
            work_item_id=self.item["id"],
            requested_by=self.user["id"],
            krab_session_id="ses_" + "3" * 32 if claim else None,
            krab_claim_id="clm_" + "4" * 32 if claim else None,
            supervision_policy=supervision_policy,
        )

    def start_run(self) -> tuple[dict[str, Any], dict[str, Any]]:
        run = self.create_run(claim=True)
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id="orchestrator-primary",
            expected_run_version=run["version"],
        )
        running = self.database.start_run_attempt(
            attempt["id"],
            owner_id="orchestrator-primary",
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision="a" * 40,
            container_id="b" * 64,
            container_name="magic-glass-box-test",
            image_id="sha256:" + "c" * 64,
        )
        return run, running

    def test_snapshot_redacts_private_fields_present_in_the_fixture(self) -> None:
        run, attempt = self.start_run()
        assignment = self.database.create_work_assignment(
            run["id"],
            target_profile_name="Codex Worker",
            spec=WorkAssignmentSpec.from_mapping(
                {
                    "schema_version": 1,
                    "title": "Visible assignment title",
                    "objective": "assignment objective secret",
                    "scope_paths": ["src"],
                    "acceptance_checks": ["Tests pass"],
                    "base_revision": None,
                }
            ),
            expected_thread_version=1,
            idempotency_key="assignment:control-room",
        )
        self.database.append_work_log(
            run["id"],
            WorkLogEntry.from_mapping(
                {
                    "schema_version": 1,
                    "kind": "blocker",
                    "summary": "blocker private detail",
                    "details": [],
                    "artifacts": [],
                    "related_interaction_ids": [],
                    "candidate_revision": None,
                }
            ),
            idempotency_key="blocker:control-room",
        )
        result = self.database.record_attempt_result(
            attempt["id"],
            ResultManifest(1, attempt["id"], "a" * 40, ()),
        )
        interaction = self.database.open_interaction(
            run["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.identity["id"],
            request_item_id="itm_" + "5" * 32,
            expected_run_version=attempt["run_version"],
        )
        room = ControlRoom(self.database)

        with mock.patch.object(
            self.database, "connect", wraps=self.database.connect
        ) as connect:
            snapshot = room.snapshot(history_limit=25)

        self.assertEqual(connect.call_count, 1)
        self.assertEqual(snapshot["snapshot"]["schema_version"], SCHEMA_VERSION)
        self.assertEqual(
            snapshot["snapshot"]["consistency"],
            "single_sqlite_read_transaction",
        )
        self.assertEqual(snapshot["summary"]["runs"]["active"], 1)
        self.assertEqual(snapshot["summary"]["interactions"]["open"], 1)
        self.assertEqual(snapshot["summary"]["assignments"]["open"], 1)
        self.assertEqual(snapshot["summary"]["retained_results"], 1)
        self.assertEqual(
            snapshot["users"][0]["krab_identity_ids"], [self.identity["id"]]
        )
        self.assertEqual(snapshot["projects"][0]["context_version"], 1)
        self.assertEqual(snapshot["profiles"][0]["command_argv_count"], 3)
        self.assertEqual(
            snapshot["provider_grants"][0]["policy"]["protocol"],
            "openai-responses",
        )
        self.assertEqual(
            snapshot["provider_grants"][0]["connector"]["id"],
            self.connector["id"],
        )
        self.assertEqual(
            snapshot["work_items"][0]["untrusted_title"],
            "Visible work-item title",
        )
        self.assertEqual(snapshot["work_items"][0]["blocker_entries"], 1)
        self.assertEqual(snapshot["assignments"][0]["id"], assignment["id"])
        self.assertEqual(
            snapshot["assignments"][0]["untrusted_title"],
            "Visible assignment title",
        )
        self.assertEqual(snapshot["runs"][0]["id"], run["id"])
        self.assertEqual(snapshot["runs"][0]["state"], "waiting_for_input")
        self.assertEqual(snapshot["attempts"][0]["owner_id"], "orchestrator-primary")
        self.assertEqual(snapshot["interactions"][0]["id"], interaction["id"])
        self.assertEqual(snapshot["retained_results"][0]["id"], result["id"])
        self.assertEqual(snapshot["run_authorities"][0]["grant_id"], self.grant["id"])
        self.assertEqual(snapshot["krab"]["status"], "not_refreshed")
        attention = {entry["kind"] for entry in snapshot["attention"]}
        self.assertTrue(
            {"interaction.pending", "assignment.open", "work.blocker"}.issubset(
                attention
            )
        )

        self.assertEqual(
            set(snapshot),
            {
                "snapshot",
                "summary",
                "attention",
                "users",
                "krab_instances",
                "projects",
                "profiles",
                "provider_grants",
                "provider_budgets",
                "provider_recoveries",
                "work_items",
                "assignments",
                "runs",
                "attempts",
                "interactions",
                "retained_results",
                "candidates",
                "gate_policies",
                "landing_dependencies",
                "integration_queue",
                "run_authorities",
                "supervisor_actions",
                "escalations",
                "krab",
            },
        )
        self.assertEqual(
            set(snapshot["snapshot"]),
            {
                "database",
                "schema_version",
                "generated_at",
                "consistency",
                "history_limit",
                "terminal_history_truncated",
            },
        )
        self.assertEqual(
            set(snapshot["summary"]),
            {
                "users",
                "krab_identities",
                "projects",
                "profiles",
                "provider_grants",
                "provider_budgets",
                "provider_recoveries",
                "work_items",
                "runs",
                "attempts",
                "interactions",
                "assignments",
                "retained_results",
                "candidates",
                "integration_gate_policies",
                "integration_gate_review_runs",
                "integration_gate_change_runs",
                "integration_maintainer_reviews",
                "integration_gate_maintainer_change_runs",
                "integration_gate_evidence",
                "integration_queue",
                "landing_dependencies",
                "pending_landing_plans",
                "pending_provider_landings",
                "supervisor_actions",
                "escalations",
                "attention",
            },
        )
        self.assertEqual(
            snapshot["summary"]["provider_grants"],
            {"total": 1, "available": 1},
        )
        self.assertEqual(
            snapshot["summary"]["runs"],
            {"total": 1, "active": 1, "terminal": 0, "waiting_for_input": 1},
        )
        self.assertEqual(
            snapshot["summary"]["attempts"],
            {"total": 1, "active": 1, "expired_leases": 0, "running": 1},
        )
        self.assertEqual(
            snapshot["summary"]["interactions"],
            {"total": 1, "open": 1, "answered": 0, "cancelled": 0},
        )
        self.assertEqual(
            snapshot["summary"]["assignments"],
            {"total": 1, "open": 1, "claimed": 0, "completed": 0, "cancelled": 0},
        )
        self.assertEqual(
            snapshot["summary"]["supervisor_actions"],
            {
                "total": 0,
                "pending": 0,
                "claimed": 0,
                "succeeded": 0,
                "exhausted": 0,
            },
        )
        self.assertEqual(
            snapshot["summary"]["escalations"],
            {
                "total": 0,
                "pending": 0,
                "claimed": 0,
                "delivered": 0,
                "dismissed": 0,
                "exhausted": 0,
            },
        )
        self.assertEqual(
            set(snapshot["users"][0]),
            {
                "id",
                "handle",
                "display_name",
                "active",
                "created_at",
                "updated_at",
                "krab_identity_ids",
            },
        )
        self.assertEqual(snapshot["users"][0]["handle"], "operator")
        self.assertEqual(snapshot["users"][0]["display_name"], "Local Operator")
        self.assertEqual(
            set(snapshot["krab_instances"][0]),
            {"id", "name", "base_url", "created_at", "updated_at", "identities"},
        )
        self.assertEqual(
            set(snapshot["krab_instances"][0]["identities"][0]),
            {
                "id",
                "krab_user_id",
                "handle",
                "token_version",
                "local_user_id",
                "verified_at",
                "created_at",
                "updated_at",
            },
        )
        self.assertEqual(
            set(snapshot["projects"][0]),
            {
                "id",
                "krab_project_id",
                "project_key",
                "display_name",
                "active",
                "created_at",
                "updated_at",
                "krab_instance_id",
                "krab_instance_name",
                "krab_base_url",
                "context_version_id",
                "context_version",
                "context_sha256",
            },
        )
        self.assertEqual(
            set(snapshot["profiles"][0]),
            {
                "id",
                "name",
                "active",
                "profile_version_id",
                "profile_version",
                "content_sha256",
                "image",
                "command_argv_count",
                "runtime_adapter",
                "runtime_provider_grant",
                "workspace_mode",
                "network_mode",
                "credential_names",
                "gateway_capabilities",
                "krab_identity_id",
                "krab_user_id",
                "krab_handle",
                "krab_instance_name",
                "created_at",
                "updated_at",
            },
        )
        self.assertEqual(snapshot["profiles"][0]["image"], "agent@sha256:abc")
        self.assertEqual(snapshot["profiles"][0]["runtime_adapter"], "command")
        self.assertIsNone(snapshot["profiles"][0]["runtime_provider_grant"])
        self.assertEqual(snapshot["profiles"][0]["credential_names"], ["openai-main"])
        self.assertEqual(
            snapshot["profiles"][0]["gateway_capabilities"], ["provider.invoke"]
        )
        self.assertEqual(
            set(snapshot["provider_grants"][0]),
            {
                "id",
                "name",
                "state",
                "version",
                "grant_kind",
                "available",
                "expired",
                "expires_at",
                "revoked_at",
                "revoked_by",
                "created_by",
                "created_at",
                "updated_at",
                "content_sha256",
                "policy",
                "connector",
            },
        )
        self.assertEqual(snapshot["provider_grants"][0]["grant_kind"], "connector")
        self.assertEqual(
            snapshot["provider_grants"][0]["policy"],
            self.policy.to_dict(),
        )
        self.assertEqual(
            snapshot["provider_grants"][0]["connector"],
            {
                "id": self.connector["id"],
                "name": "openai-personal",
                "state": "active",
                "version": 1,
            },
        )
        self.assertEqual(
            set(snapshot["work_items"][0]),
            {
                "id",
                "project_key",
                "source",
                "external_id",
                "source_updated_at",
                "work_item_version_id",
                "version",
                "content_sha256",
                "untrusted_title",
                "work_thread_id",
                "generation",
                "state",
                "thread_version",
                "log_sequence",
                "blocker_entries",
                "last_blocker_at",
                "run_count",
                "active_run_count",
                "created_at",
                "updated_at",
                "completed_at",
            },
        )
        self.assertEqual(
            set(snapshot["assignments"][0]),
            {
                "id",
                "sequence",
                "work_thread_id",
                "work_item_id",
                "project_key",
                "generation",
                "thread_state",
                "untrusted_title",
                "scope_path_count",
                "acceptance_check_count",
                "base_revision",
                "content_sha256",
                "target_profile_name",
                "target_profile_version_id",
                "target_profile_version",
                "state",
                "version",
                "created_by_run_id",
                "created_by_attempt_id",
                "claimed_by_run_id",
                "claimed_by_attempt_id",
                "lease_expires_at",
                "lease_expired",
                "result_entry_id",
                "created_at",
                "updated_at",
                "completed_at",
            },
        )
        self.assertEqual(snapshot["assignments"][0]["scope_path_count"], 1)
        self.assertEqual(snapshot["assignments"][0]["acceptance_check_count"], 1)
        self.assertEqual(
            set(snapshot["runs"][0]),
            {
                "id",
                "state",
                "version",
                "project_key",
                "context_version_id",
                "context_version",
                "profile_name",
                "profile_version_id",
                "profile_version",
                "requested_by",
                "work_item_id",
                "work_item_version_id",
                "work_item_version",
                "work_thread_id",
                "gate_policy_id",
                "review_candidate_id",
                "change_policy_id",
                "change_candidate_id",
                "change_review_evidence_id",
                "change_review_entry_id",
                "maintainer_change_policy_id",
                "maintainer_change_candidate_id",
                "change_maintainer_review_id",
                "untrusted_work_item_title",
                "source_revision",
                "predecessor_run_id",
                "replacement_kind",
                "krab_session_id",
                "krab_claim_id",
                "created_at",
                "updated_at",
                "started_at",
                "finished_at",
                "supervision_policy",
                "attempt_count",
                "interaction_count",
                "retained_result_count",
                "candidate_count",
                "authority_count",
                "provider_recovery_count",
            },
        )
        self.assertEqual(snapshot["runs"][0]["attempt_count"], 1)
        self.assertEqual(snapshot["runs"][0]["interaction_count"], 1)
        self.assertEqual(snapshot["runs"][0]["retained_result_count"], 1)
        self.assertEqual(snapshot["runs"][0]["candidate_count"], 0)
        self.assertEqual(snapshot["runs"][0]["authority_count"], 1)
        self.assertEqual(snapshot["runs"][0]["provider_recovery_count"], 0)
        self.assertFalse(snapshot["runs"][0]["supervision_policy"]["auto_execute"])
        self.assertEqual(snapshot["supervisor_actions"], [])
        self.assertEqual(snapshot["escalations"], [])
        self.assertTrue(
            all(
                isinstance(entry["age_seconds"], int) for entry in snapshot["attention"]
            )
        )
        self.assertEqual(
            set(snapshot["attempts"][0]),
            {
                "id",
                "run_id",
                "sequence",
                "runtime_adapter",
                "state",
                "version",
                "source_revision",
                "container_name",
                "image_id",
                "exit_code",
                "reason",
                "owner_id",
                "heartbeat_at",
                "lease_expires_at",
                "cancel_requested_at",
                "cancel_requested_by",
                "cancel_reason",
                "created_at",
                "started_at",
                "finished_at",
                "profile_name",
                "profile_version_id",
                "profile_version",
                "lease_expired",
            },
        )
        self.assertEqual(
            set(snapshot["interactions"][0]),
            {
                "id",
                "run_id",
                "attempt_id",
                "kind",
                "state",
                "version",
                "recipient_krab_identity_id",
                "recipient_krab_user_id",
                "recipient_krab_handle",
                "request_item_id",
                "response_item_id",
                "created_at",
                "updated_at",
                "resolved_at",
            },
        )
        self.assertEqual(
            set(snapshot["retained_results"][0]),
            {
                "id",
                "attempt_id",
                "run_id",
                "schema_version",
                "content_sha256",
                "changed_paths",
                "changed_bytes",
                "created_at",
            },
        )
        self.assertEqual(
            set(snapshot["run_authorities"][0]),
            {
                "run_id",
                "authority_name",
                "grant_id",
                "name",
                "state",
                "expires_at",
                "revoked_at",
                "unavailable_since",
                "expired",
                "available",
            },
        )
        self.assertEqual(
            snapshot["krab"],
            {
                "status": "not_refreshed",
                "refreshed_at": None,
                "roles": [],
                "work_claims": [],
                "errors": [],
            },
        )

        encoded = json.dumps(snapshot, sort_keys=True)
        for forbidden in (
            "credential-super-secret",
            "private work item brief",
            "assignment objective secret",
            "blocker private detail",
            "--secret-command-arg",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, encoded)

    def test_revoked_connector_is_projected_with_real_unavailable_age(self) -> None:
        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            run = self.create_run()
        revoked_at = start + timedelta(seconds=60)
        with mock.patch.object(
            self.database,
            "now",
            return_value=revoked_at.isoformat(timespec="microseconds"),
        ):
            self.database.revoke_inference_connector(
                self.connector["name"],
                expected_version=self.connector["version"],
                revoked_by=self.user["id"],
            )
        with mock.patch.object(
            self.database,
            "now",
            return_value=(revoked_at + timedelta(seconds=300)).isoformat(
                timespec="microseconds"
            ),
        ):
            snapshot = ControlRoom(self.database).snapshot()

        authority = snapshot["run_authorities"][0]
        self.assertEqual(authority["run_id"], run["id"])
        self.assertFalse(authority["available"])
        self.assertEqual(authority["state"], "revoked")
        self.assertEqual(
            authority["revoked_at"],
            revoked_at.isoformat(timespec="microseconds"),
        )
        self.assertEqual(authority["unavailable_since"], authority["revoked_at"])
        attention = next(
            item
            for item in snapshot["attention"]
            if item["kind"] == "authority.unavailable"
        )
        self.assertEqual(attention["age_seconds"], 300)

    def test_historical_direct_authority_has_a_stable_unavailable_age(self) -> None:
        start = datetime(2027, 1, 1, tzinfo=UTC)
        with mock.patch.object(
            self.database, "now", return_value=start.isoformat(timespec="microseconds")
        ):
            direct = self.database.create_provider_grant(
                name="historical-direct",
                spec=ProviderGrantSpec.from_mapping(
                    {
                        "provider": "openai",
                        "adapter": "json_http",
                        "destination": "https://api.openai.com/v1/responses",
                        "method": "POST",
                        "model": "gpt-test",
                        "credential_header": "Authorization",
                        "credential_prefix": "Bearer ",
                        "static_headers": {},
                        "request_bytes": 16384,
                        "response_bytes": 32768,
                        "timeout_ms": 10000,
                        "max_concurrency": 1,
                    }
                ),
                credential_ref="env:OPENAI_API_KEY",
                expires_at=None,
                created_by=self.user["id"],
            )
            run = self.create_run()
            with self.database.connect() as connection:
                connection.execute(
                    "INSERT INTO run_provider_grants "
                    "(run_id, authority_name, provider_grant_id, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        run["id"],
                        direct["name"],
                        direct["id"],
                        start.isoformat(timespec="microseconds"),
                    ),
                )
        with mock.patch.object(
            self.database,
            "now",
            return_value=(start + timedelta(seconds=300)).isoformat(
                timespec="microseconds"
            ),
        ):
            snapshot = ControlRoom(self.database).snapshot()

        authority = next(
            item
            for item in snapshot["run_authorities"]
            if item["grant_id"] == direct["id"]
        )
        self.assertEqual(authority["state"], "unsupported")
        self.assertEqual(
            authority["unavailable_since"], start.isoformat(timespec="microseconds")
        )
        attention = next(
            item
            for item in snapshot["attention"]
            if item["kind"] == "authority.unavailable"
            and item["subject_id"] == direct["id"]
        )
        self.assertEqual(attention["age_seconds"], 300)

    def test_active_runs_are_never_hidden_by_terminal_history_limit(self) -> None:
        terminal_ids: list[str] = []
        for _ in range(3):
            run = self.create_run()
            cancelled = self.database.request_run_cancellation(
                run["id"],
                requested_by=self.user["id"],
                expected_run_version=run["version"],
                reason="test cancellation",
            )
            terminal_ids.append(cancelled["id"])
        active = self.create_run()

        snapshot = ControlRoom(self.database).snapshot(history_limit=1)

        self.assertEqual(snapshot["summary"]["runs"]["total"], 4)
        self.assertEqual(snapshot["summary"]["runs"]["active"], 1)
        self.assertEqual(len(snapshot["runs"]), 2)
        self.assertIn(active["id"], {run["id"] for run in snapshot["runs"]})
        self.assertIn(terminal_ids[-1], {run["id"] for run in snapshot["runs"]})
        self.assertTrue(snapshot["snapshot"]["terminal_history_truncated"])

        active_only = ControlRoom(self.database).snapshot(history_limit=0)
        self.assertEqual([run["id"] for run in active_only["runs"]], [active["id"]])

    def test_attention_projection_covers_every_operator_condition(self) -> None:
        attention = ControlRoom._attention(
            work_items=[
                {
                    "id": "work-blocked",
                    "state": "active",
                    "blocker_entries": 2,
                    "last_blocker_at": "2026-10-10T10:00:00Z",
                },
                {
                    "id": "work-complete",
                    "state": "completed",
                    "blocker_entries": 1,
                    "last_blocker_at": "2026-10-10T09:00:00Z",
                },
            ],
            assignments=[
                {
                    "id": "assignment-open",
                    "thread_state": "active",
                    "state": "open",
                    "lease_expired": False,
                    "work_item_id": "work-blocked",
                    "target_profile_name": "Codex Worker",
                    "created_at": "2026-10-10T10:01:00Z",
                    "claimed_by_run_id": None,
                    "lease_expires_at": None,
                },
                {
                    "id": "assignment-expired",
                    "thread_state": "active",
                    "state": "claimed",
                    "lease_expired": True,
                    "work_item_id": "work-blocked",
                    "target_profile_name": "Codex Worker",
                    "created_at": "2026-10-10T10:02:00Z",
                    "claimed_by_run_id": "run-terminal",
                    "lease_expires_at": "2026-10-10T10:03:00Z",
                },
                {
                    "id": "assignment-inactive-thread",
                    "thread_state": "completed",
                    "state": "open",
                    "lease_expired": False,
                    "work_item_id": "work-complete",
                    "target_profile_name": "Codex Worker",
                    "created_at": "2026-10-10T10:04:00Z",
                    "claimed_by_run_id": None,
                    "lease_expires_at": None,
                },
            ],
            runs=[
                {
                    "id": "run-preparing",
                    "state": "preparing",
                    "updated_at": "2026-10-10T10:10:00Z",
                },
                {
                    "id": "run-running",
                    "state": "running",
                    "updated_at": "2026-10-10T10:11:00Z",
                },
                {
                    "id": "run-waiting",
                    "state": "waiting_for_input",
                    "updated_at": "2026-10-10T10:12:00Z",
                },
                {
                    "id": "run-waiting-with-question",
                    "state": "waiting_for_input",
                    "updated_at": "2026-10-10T10:13:00Z",
                },
                {
                    "id": "run-terminal",
                    "state": "failed",
                    "updated_at": "2026-10-10T10:14:00Z",
                },
            ],
            attempts=[
                {
                    "id": "attempt-active",
                    "run_id": "run-running",
                    "state": "running",
                    "owner_id": "orchestrator-primary",
                    "lease_expired": True,
                    "lease_expires_at": "2026-10-10T10:05:00Z",
                    "cancel_requested_at": "2026-10-10T10:06:00Z",
                }
            ],
            interactions=[
                {
                    "id": "interaction-open",
                    "run_id": "run-waiting-with-question",
                    "state": "open",
                    "kind": "question",
                    "recipient_krab_handle": "operator",
                    "created_at": "2026-10-10T10:07:00Z",
                },
                {
                    "id": "interaction-answered",
                    "run_id": "run-waiting",
                    "state": "answered",
                    "kind": "question",
                    "recipient_krab_handle": "operator",
                    "created_at": "2026-10-10T10:08:00Z",
                },
            ],
            authorities=[
                {
                    "run_id": "run-running",
                    "available": False,
                    "grant_id": "grant-revoked",
                    "authority_name": "provider-main",
                    "state": "revoked",
                    "expires_at": None,
                },
                {
                    "run_id": "run-preparing",
                    "available": True,
                    "grant_id": "grant-active",
                    "authority_name": "provider-backup",
                    "state": "active",
                    "expires_at": None,
                },
                {
                    "run_id": "run-terminal",
                    "available": False,
                    "grant_id": "grant-terminal",
                    "authority_name": "provider-old",
                    "state": "revoked",
                    "expires_at": None,
                },
            ],
        )

        self.assertEqual(
            attention,
            [
                {
                    "kind": "assignment.open",
                    "severity": "action",
                    "subject_type": "assignment",
                    "subject_id": "assignment-open",
                    "work_item_id": "work-blocked",
                    "target_profile_name": "Codex Worker",
                    "created_at": "2026-10-10T10:01:00Z",
                },
                {
                    "kind": "interaction.pending",
                    "severity": "action",
                    "subject_type": "interaction",
                    "subject_id": "interaction-open",
                    "run_id": "run-waiting-with-question",
                    "interaction_kind": "question",
                    "recipient_krab_handle": "operator",
                    "created_at": "2026-10-10T10:07:00Z",
                },
                {
                    "kind": "run.cancellation_requested",
                    "severity": "action",
                    "subject_type": "attempt",
                    "subject_id": "attempt-active",
                    "run_id": "run-running",
                    "owner_id": "orchestrator-primary",
                    "requested_at": "2026-10-10T10:06:00Z",
                },
                {
                    "kind": "assignment.lease_expired",
                    "severity": "warning",
                    "subject_type": "assignment",
                    "subject_id": "assignment-expired",
                    "work_item_id": "work-blocked",
                    "claimed_by_run_id": "run-terminal",
                    "lease_expires_at": "2026-10-10T10:03:00Z",
                },
                {
                    "kind": "attempt.lease_expired",
                    "severity": "warning",
                    "subject_type": "attempt",
                    "subject_id": "attempt-active",
                    "run_id": "run-running",
                    "owner_id": "orchestrator-primary",
                    "lease_expires_at": "2026-10-10T10:05:00Z",
                },
                {
                    "kind": "authority.unavailable",
                    "severity": "warning",
                    "subject_type": "provider_grant",
                    "subject_id": "grant-revoked",
                    "run_id": "run-running",
                    "authority_name": "provider-main",
                    "state": "revoked",
                    "expires_at": None,
                },
                {
                    "kind": "run.missing_active_attempt",
                    "severity": "warning",
                    "subject_type": "run",
                    "subject_id": "run-preparing",
                    "state": "preparing",
                    "updated_at": "2026-10-10T10:10:00Z",
                },
                {
                    "kind": "run.waiting_without_interaction",
                    "severity": "warning",
                    "subject_type": "run",
                    "subject_id": "run-waiting",
                    "updated_at": "2026-10-10T10:12:00Z",
                },
                {
                    "kind": "work.blocker",
                    "severity": "warning",
                    "subject_type": "work_item",
                    "subject_id": "work-blocked",
                    "blocker_entries": 2,
                    "last_blocker_at": "2026-10-10T10:00:00Z",
                },
            ],
        )

    def test_live_krab_overlay_reports_roles_claims_and_local_title_link(self) -> None:
        self.create_run(claim=True)
        issue = WorkClaim(
            claim_id="clm_" + "4" * 32,
            project_key="box-project",
            kind="issue",
            external_id="owner/repository#42",
            status="active",
            version=3,
            review_round=0,
            assignee=KrabUserRef(self.krab_identity.user_id, "agent_one"),
            created_by=KrabUserRef(self.krab_identity.user_id, "agent_one"),
            created_at="2026-10-08T12:00:00Z",
            updated_at="2026-10-10T12:00:00Z",
        )
        claims = {
            "issue": WorkClaimPage("box-project", "issue", (issue,)),
            "pull_request": WorkClaimPage("box-project", "pull_request", ()),
        }
        client = FakeKrabClient(
            self.krab_identity,
            (
                KrabProject(
                    project_id=self.project["krab_project_id"],
                    project_key="box-project",
                    role="coordinator",
                    label="Untrusted remote label",
                    content_risk_flags=("instruction_override",),
                ),
            ),
            claims,
        )
        resolver = FakeResolver()
        factory_calls: list[tuple[str, str]] = []

        def factory(base_url: str, token: str) -> FakeKrabClient:
            factory_calls.append((base_url, token))
            return client

        snapshot = ControlRoom(
            self.database,
            credential_resolver=resolver,
            client_factory=factory,
        ).snapshot(refresh_krab=True)

        self.assertEqual(snapshot["krab"]["status"], "refreshed")
        self.assertIsInstance(snapshot["krab"]["refreshed_at"], str)
        self.assertEqual(snapshot["krab"]["errors"], [])
        self.assertEqual(
            snapshot["krab"]["roles"],
            [
                {
                    "identity_id": self.identity["id"],
                    "krab_user_id": self.krab_identity.user_id,
                    "krab_handle": "agent_one",
                    "local_user_id": self.user["id"],
                    "project_key": "box-project",
                    "role": "coordinator",
                }
            ],
        )
        self.assertEqual(
            snapshot["krab"]["work_claims"],
            [
                {
                    "claim_id": issue.claim_id,
                    "project_key": "box-project",
                    "kind": "issue",
                    "external_id": "owner/repository#42",
                    "status": "active",
                    "version": 3,
                    "review_round": 0,
                    "assignee": {
                        "user_id": self.krab_identity.user_id,
                        "handle": "agent_one",
                    },
                    "created_by": {
                        "user_id": self.krab_identity.user_id,
                        "handle": "agent_one",
                    },
                    "created_at": "2026-10-08T12:00:00Z",
                    "updated_at": "2026-10-10T12:00:00Z",
                    "read_via_identity_id": self.identity["id"],
                    "local_work_item": {
                        "id": self.item["id"],
                        "untrusted_title": "Visible work-item title",
                    },
                }
            ],
        )
        self.assertEqual(
            client.claim_calls,
            [("box-project", "issue"), ("box-project", "pull_request")],
        )
        self.assertEqual(
            resolver.references,
            ["file:/private/credential-super-secret"],
        )
        self.assertEqual(
            factory_calls,
            [("http://127.0.0.1:8765", "resolved-token")],
        )
        self.assertNotIn("resolved-token", json.dumps(snapshot))

    def test_live_krab_refresh_degrades_per_identity_without_hiding_local_state(
        self,
    ) -> None:
        resolver = FailingResolver()
        factory = mock.Mock()

        snapshot = ControlRoom(
            self.database,
            credential_resolver=resolver,
            client_factory=factory,
        ).snapshot(refresh_krab=True)

        self.assertEqual(snapshot["krab"]["status"], "degraded")
        self.assertEqual(snapshot["krab"]["roles"], [])
        self.assertEqual(snapshot["krab"]["work_claims"], [])
        self.assertEqual(
            snapshot["krab"]["errors"][0]["identity_id"], self.identity["id"]
        )
        self.assertEqual(snapshot["projects"][0]["project_key"], "box-project")
        self.assertEqual(
            snapshot["krab"]["errors"][0]["message"],
            "configured credential is unavailable",
        )
        factory.assert_not_called()

    def test_live_krab_refresh_redacts_upstream_and_host_failure_details(
        self,
    ) -> None:
        failures = (
            (
                "whoami",
                KrabHTTPError(
                    401,
                    "unauthorized",
                    "token rejected: UPSTREAM-ERROR-BODY-MARKER",
                ),
            ),
            (
                "whoami",
                KrabError(
                    "Krab is unavailable: [Errno 61] connection refused at "
                    "/Users/operator/private/socket"
                ),
            ),
            (
                "projects",
                KrabError(
                    "Krab is unavailable: host detail from "
                    "/Users/operator/private/projects.sock"
                ),
            ),
        )

        for method, failure in failures:
            with self.subTest(method=method, failure=type(failure).__name__):
                client = mock.Mock()
                client.whoami.return_value = self.krab_identity
                getattr(client, method).side_effect = failure
                snapshot = ControlRoom(
                    self.database,
                    credential_resolver=FakeResolver(),
                    client_factory=lambda _base_url, _token, client=client: client,
                ).snapshot(refresh_krab=True)

                self.assertEqual(snapshot["krab"]["status"], "degraded")
                self.assertEqual(
                    snapshot["krab"]["errors"][0]["message"],
                    "Krab identity refresh failed",
                )
                encoded = json.dumps(snapshot, sort_keys=True)
                for private_detail in (
                    "UPSTREAM-ERROR-BODY-MARKER",
                    "token rejected",
                    "/Users/operator/private/socket",
                    "/Users/operator/private/projects.sock",
                    "connection refused",
                    "host detail",
                ):
                    self.assertNotIn(private_detail, encoded)

    def test_live_krab_refresh_isolates_identity_and_project_mismatches(self) -> None:
        second_krab_identity = KrabIdentity(
            "usr_" + "5" * 32,
            "z_project_agent",
            1,
        )
        second_identity = self.database.store_krab_identity(
            self.instance["id"],
            second_krab_identity,
            "file:/private/project-mismatch-secret",
        )
        wrong_identity_client = mock.Mock()
        wrong_identity_client.whoami.return_value = KrabIdentity(
            "usr_" + "6" * 32,
            "unexpected_agent",
            1,
        )
        mismatch_client = FakeKrabClient(
            second_krab_identity,
            (
                KrabProject(
                    project_id="prj_" + "7" * 32,
                    project_key="unknown-project",
                    role="member",
                    label="Unknown",
                    content_risk_flags=(),
                ),
                KrabProject(
                    project_id="prj_" + "8" * 32,
                    project_key="box-project",
                    role="admin",
                    label="Mismatched",
                    content_risk_flags=(),
                ),
            ),
            {},
        )
        factory = mock.Mock(side_effect=[wrong_identity_client, mismatch_client])

        snapshot = ControlRoom(
            self.database,
            credential_resolver=FakeResolver(),
            client_factory=factory,
        ).snapshot(refresh_krab=True)

        self.assertEqual(snapshot["krab"]["status"], "degraded")
        self.assertIsInstance(snapshot["krab"]["refreshed_at"], str)
        self.assertEqual(snapshot["krab"]["roles"], [])
        self.assertEqual(snapshot["krab"]["work_claims"], [])
        self.assertEqual(
            snapshot["krab"]["errors"],
            [
                {
                    "scope": "identity",
                    "identity_id": self.identity["id"],
                    "krab_handle": "agent_one",
                    "error_type": "KrabError",
                    "message": ("credential now belongs to a different Krab identity"),
                },
                {
                    "scope": "role",
                    "identity_id": second_identity["id"],
                    "project_key": "box-project",
                    "error_type": "KrabError",
                    "message": "Krab project ID no longer matches local state",
                },
            ],
        )
        wrong_identity_client.projects.assert_not_called()
        self.assertEqual(mismatch_client.claim_calls, [])
        encoded = json.dumps(snapshot, sort_keys=True)
        self.assertNotIn("project-mismatch-secret", encoded)

    def test_live_krab_refresh_degrades_one_claim_queue_and_keeps_the_other(
        self,
    ) -> None:
        claim = WorkClaim(
            claim_id="clm_" + "9" * 32,
            project_key="box-project",
            kind="pull_request",
            external_id="owner/repository#404",
            status="under_review",
            version=7,
            review_round=2,
            assignee=None,
            created_by=KrabUserRef(self.krab_identity.user_id, "agent_one"),
            created_at="2026-10-08T13:00:00Z",
            updated_at="2026-10-10T13:00:00Z",
        )
        client = mock.Mock()
        client.whoami.return_value = self.krab_identity
        client.projects.return_value = (
            KrabProject(
                project_id=self.project["krab_project_id"],
                project_key="box-project",
                role="admin",
                label="Remote label",
                content_risk_flags=(),
            ),
        )

        def work_claims(project_key: str, *, kind: str) -> WorkClaimPage:
            if kind == "issue":
                raise KrabError("issue queue unavailable")
            return WorkClaimPage(project_key, kind, (claim,))

        client.work_claims.side_effect = work_claims

        snapshot = ControlRoom(
            self.database,
            credential_resolver=FakeResolver(),
            client_factory=lambda _base_url, _token: client,
        ).snapshot(refresh_krab=True)

        self.assertEqual(snapshot["krab"]["status"], "degraded")
        self.assertEqual(
            snapshot["krab"]["errors"],
            [
                {
                    "scope": "work_claims",
                    "identity_id": self.identity["id"],
                    "project_key": "box-project",
                    "kind": "issue",
                    "error_type": "KrabError",
                    "message": "Krab work-claim refresh failed",
                }
            ],
        )
        self.assertEqual(
            snapshot["krab"]["work_claims"],
            [
                {
                    "claim_id": claim.claim_id,
                    "project_key": "box-project",
                    "kind": "pull_request",
                    "external_id": "owner/repository#404",
                    "status": "under_review",
                    "version": 7,
                    "review_round": 2,
                    "assignee": None,
                    "created_by": {
                        "user_id": self.krab_identity.user_id,
                        "handle": "agent_one",
                    },
                    "created_at": "2026-10-08T13:00:00Z",
                    "updated_at": "2026-10-10T13:00:00Z",
                    "read_via_identity_id": self.identity["id"],
                    "local_work_item": None,
                }
            ],
        )
        self.assertEqual(
            client.work_claims.call_args_list,
            [
                mock.call("box-project", kind="issue"),
                mock.call("box-project", kind="pull_request"),
            ],
        )

    def test_live_krab_claims_use_the_highest_role_available(self) -> None:
        admin_krab_identity = KrabIdentity(
            "usr_" + "a" * 32,
            "z_admin_agent",
            1,
        )
        admin_identity = self.database.store_krab_identity(
            self.instance["id"],
            admin_krab_identity,
            "file:/private/admin-secret",
        )
        member_client = FakeKrabClient(
            self.krab_identity,
            (
                KrabProject(
                    project_id=self.project["krab_project_id"],
                    project_key="box-project",
                    role="member",
                    label="Member",
                    content_risk_flags=(),
                ),
            ),
            {
                "issue": WorkClaimPage("box-project", "issue", ()),
                "pull_request": WorkClaimPage("box-project", "pull_request", ()),
            },
        )
        admin_client = FakeKrabClient(
            admin_krab_identity,
            (
                KrabProject(
                    project_id=self.project["krab_project_id"],
                    project_key="box-project",
                    role="admin",
                    label="Admin",
                    content_risk_flags=(),
                ),
            ),
            {
                "issue": WorkClaimPage("box-project", "issue", ()),
                "pull_request": WorkClaimPage("box-project", "pull_request", ()),
            },
        )
        factory = mock.Mock(side_effect=[member_client, admin_client])

        snapshot = ControlRoom(
            self.database,
            credential_resolver=FakeResolver(),
            client_factory=factory,
        ).snapshot(refresh_krab=True)

        self.assertEqual(snapshot["krab"]["status"], "refreshed")
        self.assertEqual(
            [role["role"] for role in snapshot["krab"]["roles"]],
            ["member", "admin"],
        )
        self.assertEqual(member_client.claim_calls, [])
        self.assertEqual(
            admin_client.claim_calls,
            [("box-project", "issue"), ("box-project", "pull_request")],
        )
        self.assertEqual(
            {role["identity_id"] for role in snapshot["krab"]["roles"]},
            {self.identity["id"], admin_identity["id"]},
        )

    def test_snapshot_rejects_invalid_limits_and_corrupt_stored_content(self) -> None:
        room = ControlRoom(self.database)
        for invalid in (True, -1, 501, 1.5, "1"):
            with (
                self.subTest(invalid=invalid),
                self.assertRaisesRegex(ValueError, "history_limit"),
            ):
                room.snapshot(history_limit=invalid)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "refresh_krab"):
            room.snapshot(refresh_krab=1)  # type: ignore[arg-type]

        with closing(self.database.connect()) as connection:
            connection.execute("DROP TRIGGER work_item_version_immutable_update")
            connection.execute(
                "UPDATE work_item_versions SET content_json = '{}' WHERE id = ?",
                (self.item["work_item_version_id"],),
            )
        with self.assertRaisesRegex(StorageError, "stored work item"):
            room.snapshot()

    def test_snapshot_rejects_corrupt_profile_and_provider_policy(self) -> None:
        with closing(self.database.connect()) as connection:
            connection.execute("DROP TRIGGER agent_profile_version_immutable_update")
            connection.execute(
                "UPDATE agent_profile_versions SET spec_json = '{}' WHERE id = ?",
                (self.profile["profile_version_id"],),
            )
        with self.assertRaisesRegex(StorageError, "stored agent profile"):
            ControlRoom(self.database).snapshot()

        path = Path(self.temporary.name) / "provider-corruption.db"
        database = Database(path)
        database.initialize()
        user = database.create_user("provider-operator", "Provider Operator")
        spec = ProviderGrantSpec.from_mapping(
            {
                "provider": "openai",
                "adapter": "json_http",
                "destination": "https://api.openai.com/v1/responses",
                "method": "POST",
                "model": "gpt-test",
                "credential_header": "Authorization",
                "credential_prefix": "Bearer ",
                "static_headers": {},
                "request_bytes": 32768,
                "response_bytes": 1048576,
                "timeout_ms": 30000,
                "max_concurrency": 1,
            }
        )
        grant = database.create_provider_grant(
            name="provider-main",
            spec=spec,
            credential_ref="file:/private/provider",
            expires_at=None,
            created_by=user["id"],
        )
        with closing(database.connect()) as connection:
            connection.execute("DROP TRIGGER provider_grant_policy_immutable")
            connection.execute(
                "UPDATE provider_grants SET policy_json = '{}' WHERE id = ?",
                (grant["id"],),
            )
        with self.assertRaisesRegex(StorageError, "stored provider grant policy"):
            ControlRoom(database).snapshot()

    def test_snapshot_surfaces_connector_grant_without_private_transport(self) -> None:
        socket_path = Path(self.temporary.name) / "private" / "inference.sock"
        connector_spec = InferenceConnectorSpec.from_mapping(
            {
                "schema_version": 1,
                "connector_protocol_version": 1,
                "deployment_sha256": "a" * 64,
                "transport": {"kind": "unix", "socket_path": str(socket_path)},
                "routes": [
                    {
                        "name": "chatgpt-main",
                        "provider": "openai",
                        "protocol": "openai-responses",
                        "models": ["gpt-test"],
                        "credential_scope_sha256": "b" * 64,
                    }
                ],
                "request_bytes": 32768,
                "response_bytes": 65536,
                "timeout_ms": 30000,
                "max_concurrency": 2,
            }
        )
        connector = self.database.register_inference_connector(
            name="openai-sidecar",
            spec=connector_spec,
            created_by=self.user["id"],
        )
        grant_spec = InferenceGrantSpec.from_mapping(
            {
                "schema_version": 1,
                "connector_name": "openai-sidecar",
                "deployment_sha256": "a" * 64,
                "route": "chatgpt-main",
                "credential_scope_sha256": "b" * 64,
                "provider": "openai",
                "protocol": "openai-responses",
                "model": "gpt-test",
                "request_bytes": 16384,
                "response_bytes": 32768,
                "timeout_ms": 10000,
                "max_concurrency": 1,
            }
        )
        self.database.create_connector_provider_grant(
            name="openai-sidecar-main",
            spec=grant_spec,
            expires_at=None,
            created_by=self.user["id"],
        )

        snapshot = ControlRoom(self.database).snapshot()
        grant = next(
            item
            for item in snapshot["provider_grants"]
            if item["name"] == "openai-sidecar-main"
        )

        self.assertEqual(grant["grant_kind"], "connector")
        self.assertTrue(grant["available"])
        self.assertEqual(
            grant["connector"],
            {
                "id": connector["id"],
                "name": "openai-sidecar",
                "state": "active",
                "version": 1,
            },
        )
        self.assertEqual(grant["policy"], grant_spec.to_dict())
        encoded = json.dumps(grant, sort_keys=True)
        self.assertNotIn(str(socket_path), encoded)
        self.assertNotIn("credential_ref", encoded)

    def test_snapshot_rejects_corrupt_assignment_and_result(self) -> None:
        run, _attempt = self.start_run()
        assignment = self.database.create_work_assignment(
            run["id"],
            target_profile_name="Codex Worker",
            spec=WorkAssignmentSpec.from_mapping(
                {
                    "schema_version": 1,
                    "title": "Delegated work",
                    "objective": "Private objective",
                    "scope_paths": [],
                    "acceptance_checks": [],
                    "base_revision": None,
                }
            ),
            expected_thread_version=1,
            idempotency_key="assignment:corruption",
        )
        with closing(self.database.connect()) as connection:
            connection.execute("DROP TRIGGER work_assignment_identity_immutable")
            connection.execute(
                "UPDATE work_assignments SET spec_json = '{}' WHERE id = ?",
                (assignment["id"],),
            )
        with self.assertRaisesRegex(StorageError, "stored work assignment"):
            ControlRoom(self.database).snapshot()

        path = Path(self.temporary.name) / "result-corruption.db"
        database = Database(path)
        database.initialize()
        user = database.create_user("result-operator", "Result Operator")
        instance = database.create_krab_instance("Result Krab", "http://127.0.0.1:8765")
        identity = database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "8" * 32, "result_agent", 1),
            "file:/private/result-krab",
        )
        database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "9" * 32,
            krab_project_key="result-project",
            display_name="Result Project",
            context=ProjectContext.from_mapping({"purpose": "Result corruption"}),
            created_by=user["id"],
        )
        database.create_agent_profile(
            name="Result Worker",
            spec=AgentProfileSpec.create(
                image="agent@sha256:result", command=["agent"]
            ),
            krab_identity_id=identity["id"],
            created_by=user["id"],
        )
        item = database.import_work_item(
            project_key="result-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "test",
                    "external_id": "result-corruption",
                    "source_updated_at": "2026-10-10T12:00:00Z",
                    "title": "Result corruption",
                    "brief": "Private result-corruption brief",
                }
            ),
            imported_by=user["id"],
        )
        result_run = database.create_run(
            project_key="result-project",
            profile_name="Result Worker",
            work_item_id=item["id"],
            requested_by=user["id"],
        )
        attempt = database.begin_run_attempt(
            result_run["id"],
            runtime_adapter="docker",
            owner_id="result-owner",
            expected_run_version=result_run["version"],
        )
        attempt = database.start_run_attempt(
            attempt["id"],
            owner_id="result-owner",
            expected_attempt_version=attempt["version"],
            expected_run_version=attempt["run_version"],
            source_revision="e" * 40,
            container_id="f" * 64,
            container_name="result-corruption-test",
            image_id="sha256:" + "0" * 64,
        )
        result = database.record_attempt_result(
            attempt["id"],
            ResultManifest(1, attempt["id"], "e" * 40, ()),
        )
        with closing(database.connect()) as connection:
            connection.execute("DROP TRIGGER attempt_result_immutable_update")
            connection.execute(
                "UPDATE attempt_results SET manifest_json = '{}' WHERE id = ?",
                (result["id"],),
            )
        with self.assertRaisesRegex(StorageError, "stored attempt result"):
            ControlRoom(database).snapshot()

    def test_snapshot_rejects_invalid_stored_expiry_timestamp(self) -> None:
        with closing(self.database.connect()) as connection:
            connection.execute("DROP TRIGGER provider_grant_policy_immutable")
            connection.execute(
                "UPDATE provider_grants SET expires_at = ? WHERE id = ?",
                ("2026-10-10T12:00:00", self.grant["id"]),
            )

        with self.assertRaisesRegex(StorageError, "provider expiry timestamp"):
            ControlRoom(self.database).snapshot()

    def test_empty_database_has_a_complete_snapshot_shape(self) -> None:
        path = Path(self.temporary.name) / "empty.db"
        empty = Database(path)
        empty.initialize()

        snapshot = ControlRoom(empty).snapshot()

        self.assertEqual(snapshot["summary"]["runs"]["total"], 0)
        self.assertEqual(snapshot["summary"]["attention"], 0)
        for collection in (
            "attention",
            "users",
            "krab_instances",
            "projects",
            "profiles",
            "provider_grants",
            "provider_recoveries",
            "work_items",
            "assignments",
            "runs",
            "attempts",
            "interactions",
            "retained_results",
            "candidates",
            "gate_policies",
            "run_authorities",
            "supervisor_actions",
            "escalations",
        ):
            self.assertEqual(snapshot[collection], [])

    def test_snapshot_surfaces_candidate_identity_without_manifest_content(
        self,
    ) -> None:
        run, attempt = self.start_run()
        result = self.database.record_attempt_result(
            attempt["id"],
            ResultManifest(1, attempt["id"], "a" * 40, ()),
        )
        self.database.complete_run_attempt(
            attempt["id"],
            owner_id="orchestrator-primary",
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id="sha256:" + "c" * 64,
        )
        source = self.database.get_candidate_source(attempt["id"])
        candidate = self.database.create_candidate(
            CandidateManifest(
                schema_version=1,
                attempt_id=attempt["id"],
                attempt_result_id=result["id"],
                producer_run_id=run["id"],
                project_id=source["project_id"],
                context_version_id=source["context_version_id"],
                work_thread_id=source["work_thread_id"],
                base_revision="a" * 40,
                result_content_sha256=result["content_sha256"],
                head_revision="b" * 40,
                tree_revision="c" * 40,
                object_format="sha1",
            ),
            created_by=self.user["id"],
        )

        snapshot = ControlRoom(self.database).snapshot()

        self.assertEqual(snapshot["summary"]["candidates"], 1)
        self.assertEqual(snapshot["runs"][0]["candidate_count"], 1)
        self.assertEqual(
            snapshot["candidates"],
            [
                {
                    "id": candidate["id"],
                    "attempt_result_id": result["id"],
                    "schema_version": 1,
                    "project_id": source["project_id"],
                    "context_version_id": source["context_version_id"],
                    "work_thread_id": source["work_thread_id"],
                    "producer_run_id": run["id"],
                    "producer_attempt_id": attempt["id"],
                    "base_revision": "a" * 40,
                    "head_revision": "b" * 40,
                    "tree_revision": "c" * 40,
                    "object_format": "sha1",
                    "result_content_sha256": result["content_sha256"],
                    "content_sha256": candidate["content_sha256"],
                    "created_by_handle": "operator",
                    "created_at": candidate["created_at"],
                }
            ],
        )
        self.assertNotIn("manifest", snapshot["candidates"][0])

    def test_snapshot_surfaces_redacted_exact_input_gate_readiness(self) -> None:
        run, attempt = self.start_run()
        result = self.database.record_attempt_result(
            attempt["id"], ResultManifest(1, attempt["id"], "a" * 40, ())
        )
        self.database.complete_run_attempt(
            attempt["id"],
            owner_id="orchestrator-primary",
            expected_attempt_version=attempt["version"],
            exit_code=0,
            image_id="sha256:" + "c" * 64,
        )
        source = self.database.get_candidate_source(attempt["id"])
        candidate = self.database.create_candidate(
            CandidateManifest(
                schema_version=1,
                attempt_id=attempt["id"],
                attempt_result_id=result["id"],
                producer_run_id=run["id"],
                project_id=source["project_id"],
                context_version_id=source["context_version_id"],
                work_thread_id=source["work_thread_id"],
                base_revision="a" * 40,
                result_content_sha256=result["content_sha256"],
                head_revision="b" * 40,
                tree_revision="c" * 40,
                object_format="sha1",
            ),
            created_by=self.user["id"],
        )
        policy = self.database.create_gate_policy(
            GatePolicyManifest.bind(
                candidate,
                default_branch="main",
                policy=GatePolicySpec.create(
                    review_separation="different_profile_and_identity",
                    maintainer_approval=True,
                ),
            ),
            created_by=self.user["id"],
        )
        maintainer = ControlPlane(self.database).record_maintainer_review(
            policy["id"],
            review_request={
                "schema_version": 1,
                "decision": "changes_requested",
                "summary": "PRIVATE PRODUCT JUDGMENT",
                "details": ["PRIVATE ACTIONABLE DETAIL"],
            },
            recorded_by_handle="operator",
            expected_policy_revision=policy["policy_revision"],
        )

        snapshot = ControlRoom(self.database).snapshot()

        self.assertEqual(snapshot["summary"]["integration_gate_policies"], 1)
        self.assertEqual(snapshot["summary"]["integration_gate_evidence"], 0)
        self.assertEqual(snapshot["summary"]["integration_maintainer_reviews"], 1)
        self.assertEqual(
            snapshot["gate_policies"],
            [
                {
                    "id": policy["id"],
                    "candidate_id": candidate["id"],
                    "project_id": source["project_id"],
                    "work_thread_id": source["work_thread_id"],
                    "producer_run_id": run["id"],
                    "repository_identity": source["project_id"],
                    "integration_ref": "refs/heads/main",
                    "expected_base_revision": "a" * 40,
                    "head_revision": "b" * 40,
                    "tree_revision": "c" * 40,
                    "object_format": "sha1",
                    "policy_revision": policy["policy_revision"],
                    "content_sha256": policy["content_sha256"],
                    "review_separation": "different_profile_and_identity",
                    "required_count": 2,
                    "passed_count": 0,
                    "failed_count": 1,
                    "pending_count": 1,
                    "ready": False,
                    "latest_evidence_at": maintainer["created_at"],
                    "created_by_handle": "operator",
                    "created_at": policy["created_at"],
                }
            ],
        )
        pending = [
            entry for entry in snapshot["attention"] if entry["kind"] == "gate.failed"
        ]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["subject_id"], policy["id"])
        encoded = json.dumps(snapshot["gate_policies"], sort_keys=True)
        self.assertNotIn("manifest", encoded)
        self.assertNotIn("command", encoded)
        self.assertNotIn("PRIVATE PRODUCT JUDGMENT", encoded)
        self.assertNotIn("PRIVATE ACTIONABLE DETAIL", encoded)

    def test_snapshot_surfaces_durable_supervisor_work_without_private_content(
        self,
    ) -> None:
        policy = SupervisionPolicy.from_mapping(
            {
                "schema_version": 1,
                "auto_execute": True,
                "retry_backoff_seconds": [5],
                "interaction_escalation_seconds": [],
                "attempt_idle_escalation_seconds": [],
                "attempt_lease_expired_escalation_seconds": [],
                "other_attention_escalation_seconds": [],
            }
        )
        run = self.create_run(supervision_policy=policy)
        action = self.database.enqueue_supervisor_actions()[0]

        snapshot = ControlRoom(self.database).snapshot()

        self.assertEqual(snapshot["runs"][0]["id"], run["id"])
        self.assertTrue(snapshot["runs"][0]["supervision_policy"]["auto_execute"])
        self.assertEqual(snapshot["supervisor_actions"][0]["id"], action["id"])
        self.assertEqual(snapshot["supervisor_actions"][0]["state"], "pending")
        self.assertNotIn("retry_backoff_json", snapshot["supervisor_actions"][0])
        encoded = json.dumps(snapshot, sort_keys=True).lower()
        for forbidden in ("question_body", "answer_body", "transcript", "prompt"):
            self.assertNotIn(forbidden, encoded)


if __name__ == "__main__":
    unittest.main()
