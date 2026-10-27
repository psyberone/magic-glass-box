from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path

from magic_glass_box.control_room import ControlRoom
from magic_glass_box.db import (
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
)
from magic_glass_box.discovery import (
    DiscoveryConfigurationSpec,
    DiscoveryRuling,
    DiscoveryTrace,
)
from magic_glass_box.discovery_director import (
    DISCOVERY_RUN_SUPERVISION,
    DiscoveryDirector,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    InteractionKind,
    ProjectContext,
    ValidationError,
    WorkItemEnvelope,
    canonical_json,
    content_sha256,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import WorkspaceResultStore
from magic_glass_box.service import ControlPlane
from magic_glass_box.storage.schema import SCHEMA_VERSION
from magic_glass_box.supervision import Supervisor
from magic_glass_box.workflow_director import WORKFLOW_RUN_SUPERVISION
from magic_glass_box.workflows import WorkflowConfigurationSpec

OWNER_ID = "discovery-ledger-test"
IMAGE_ID = "sha256:" + "9" * 64
SOURCE_REVISION = "a" * 40


def trace_value(**overrides: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "summary": "The requested change belongs in the existing catalog layer.",
        "findings": [
            {
                "key": "catalog-boundary",
                "classification": "constraint",
                "summary": "The catalog owns immutable profile revisions.",
                "source_refs": [{"kind": "acceptance_criterion", "index": 0}],
                "evidence": [
                    {
                        "path": "src/magic_glass_box/storage/catalog.py",
                        "line_start": 141,
                        "line_end": 165,
                        "summary": "Creation and revision authority live together.",
                    }
                ],
            }
        ],
        "recommended_checks": ["Run the focused catalog tests."],
        "open_questions": [],
        **overrides,
    }


def ruling_value(**overrides: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "disposition": "GO",
        "summary": "The change is bounded and ready for implementation.",
        "trace_finding_keys": ["catalog-boundary"],
        "interaction_id": None,
        "work_ready_brief": {
            "objective": "Add the primitive without widening the catalog.",
            "must_items": [
                {
                    "key": "preserve-authority",
                    "requirement": "Keep profile revisions immutable.",
                    "source_refs": [{"kind": "acceptance_criterion", "index": 0}],
                    "finding_keys": ["catalog-boundary"],
                }
            ],
            "scope_paths": ["src/magic_glass_box/storage/catalog.py"],
            "verification_checks": ["Run the focused catalog tests."],
            "non_goals": ["Do not add provider-specific behavior."],
            "follow_up_proposals": [],
        },
        **overrides,
    }


class DiscoveryExecutionStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.results = WorkspaceResultStore(self.root / "results")
        self.user = self.database.create_user("operator", "Local Operator")
        self.instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.decision_owner = self.identity("operator", "0", user_id=self.user["id"])
        self.investigator_identity = self.identity("investigator", "1")
        self.coordinator_identity = self.identity("coordinator", "2")
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "3" * 32,
            krab_project_key="discovery-project",
            display_name="Discovery Project",
            context=ProjectContext.from_mapping(
                {
                    "purpose": "Prove bounded discovery execution",
                    "instructions": ["Keep product authority explicit"],
                }
            ),
            created_by=self.user["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="discovery-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "discovery-execution-1",
                    "source_updated_at": "2026-10-25T12:00:00Z",
                    "title": "Prove the Discovery ledger",
                    "brief": "Investigate before implementation begins.",
                    "acceptance_criteria": [
                        "Preserve immutable profile revision authority."
                    ],
                }
            ),
            imported_by=self.user["id"],
        )
        self.investigator = self.profile(
            "Investigator",
            self.investigator_identity["id"],
            capabilities=["run.describe", "discovery.trace.submit"],
        )
        self.coordinator = self.profile(
            "Coordinator",
            self.coordinator_identity["id"],
            credentials=["krab"],
            capabilities=[
                "run.describe",
                "interaction.open",
                "interaction.read",
                "handoff.submit",
                "discovery.ruling.submit",
            ],
        )
        self.configuration = self.database.create_discovery_configuration(
            project_key="discovery-project",
            name="default",
            spec=DiscoveryConfigurationSpec.from_mapping(
                {
                    "schema_version": 1,
                    "max_findings": 2,
                    "max_must_items": 2,
                    "max_follow_up_proposals": 2,
                }
            ),
            investigator_profile_name="Investigator",
            coordinator_profile_name="Coordinator",
            created_by=self.user["id"],
        )

    def identity(
        self, handle: str, fill: str, *, user_id: str | None = None
    ) -> dict[str, object]:
        return self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity(
                user_id="usr_" + fill * 32,
                handle=handle,
                token_version=1,
            ),
            f"file:/private/{handle}",
            user_id=user_id,
        )

    def profile(
        self,
        name: str,
        identity_id: str,
        *,
        credentials: list[str] | None = None,
        capabilities: list[str],
    ) -> dict[str, object]:
        return self.database.create_agent_profile(
            name=name,
            spec=AgentProfileSpec.create(
                image="agent@sha256:discovery",
                command=["agent", "run"],
                workspace_mode="read_only",
                network_mode="gateway_only",
                credential_names=credentials or [],
                gateway_capabilities=capabilities,
            ),
            krab_identity_id=identity_id,
            created_by=self.user["id"],
        )

    def start(self, **overrides: object) -> dict[str, object]:
        arguments: dict[str, object] = {
            "project_key": "discovery-project",
            "configuration_name": "default",
            "work_item_id": self.work_item["id"],
            "source_revision": SOURCE_REVISION,
            "expected_configuration_version": 1,
            "expected_context_version_id": self.project["context_version_id"],
            "expected_work_item_version_id": self.work_item["work_item_version_id"],
            "expected_thread_generation": 1,
            "expected_thread_version": 1,
            "krab_session_id": "ses_" + "4" * 32,
            "krab_claim_id": "clm_" + "5" * 32,
            "idempotency_key": "start-discovery-1",
            "requested_by": self.user["id"],
        }
        arguments.update(overrides)
        return self.database.start_discovery_execution(**arguments)

    def create_role_run(
        self, execution: dict[str, object], role: str
    ) -> dict[str, object]:
        return self.database.create_run(
            project_key="discovery-project",
            profile_name=execution["roles"][role]["profile_name"],
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision=SOURCE_REVISION,
            krab_session_id=(
                execution["krab_session_id"] if role == "coordinator" else None
            ),
            krab_claim_id=(
                execution["krab_claim_id"] if role == "coordinator" else None
            ),
            discovery_execution_id=execution["id"],
            discovery_role=role,
        )

    def run_attempt(
        self,
        run: dict[str, object],
        submit: Callable[[dict[str, object]], object] | None = None,
        *,
        complete: bool = True,
        exit_code: int = 0,
    ) -> dict[str, object]:
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id=OWNER_ID,
            expected_run_version=run["version"],
        )
        attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=SOURCE_REVISION,
            container_id="8" * 64,
            container_name=f"discovery-{run['id']}",
            image_id=IMAGE_ID,
        )
        if submit is not None:
            submit(attempt)
        if not complete:
            return attempt
        workspace = self.root / f"workspace-{attempt['id']}"
        workspace.mkdir()
        (workspace / "README.md").write_text("unchanged\n", encoding="utf-8")
        self.results.stage(attempt["id"], workspace, SOURCE_REVISION)
        captured = self.results.capture(attempt["id"], workspace)
        self.database.record_attempt_result(attempt["id"], captured.manifest)
        return self.database.complete_run_attempt(
            attempt["id"],
            owner_id=OWNER_ID,
            expected_attempt_version=attempt["version"],
            exit_code=exit_code,
            image_id=IMAGE_ID,
        )

    def prepare_coordinate(
        self,
    ) -> tuple[dict[str, object], dict[str, object]]:
        execution = self.start()
        investigator = self.create_role_run(execution, "investigator")
        self.run_attempt(
            investigator,
            lambda _attempt: self.database.submit_discovery_trace(
                investigator["id"],
                trace=DiscoveryTrace.from_mapping(trace_value()),
                idempotency_key="trace-key-1",
            ),
        )
        current = self.database.get_discovery_execution(execution["id"])
        advanced = self.database.advance_discovery_execution(
            execution["id"],
            expected_version=current["version"],
            expected_stage="investigate",
            target_stage="coordinate",
            event_kind="discovery.investigation_completed",
            metadata={"investigator_run_id": investigator["id"]},
        )
        return advanced, investigator

    def complete_discovery(self, **ruling_overrides: object) -> dict[str, object]:
        execution, _investigator = self.prepare_coordinate()
        coordinator = self.create_role_run(execution, "coordinator")
        ruling = DiscoveryRuling.from_mapping(ruling_value(**ruling_overrides))
        self.run_attempt(
            coordinator,
            lambda _attempt: self.database.submit_discovery_ruling(
                coordinator["id"],
                ruling=ruling,
                idempotency_key="final-ruling-1",
            ),
        )
        current = self.database.get_discovery_execution(execution["id"])
        completion = self.database.advance_discovery_execution(
            execution["id"],
            expected_version=current["version"],
            expected_stage="coordinate",
            target_stage="completion",
            event_kind="discovery.coordination_completed",
            metadata={"coordinator_run_id": coordinator["id"]},
        )
        return self.database.complete_discovery_execution(
            execution["id"], expected_version=completion["version"]
        )

    def create_implementation_configuration(self) -> dict[str, object]:
        producer_identity = self.identity("producer", "6")
        reviewer_identity = self.identity("reviewer", "7")
        producer = self.database.create_agent_profile(
            name="Producer",
            spec=AgentProfileSpec.create(
                image="agent@sha256:producer",
                command=["agent", "run"],
                workspace_mode="copy_on_write",
                network_mode="gateway_only",
                gateway_capabilities=["run.describe"],
            ),
            krab_identity_id=producer_identity["id"],
            created_by=self.user["id"],
        )
        reviewer = self.database.create_agent_profile(
            name="Reviewer",
            spec=AgentProfileSpec.create(
                image="agent@sha256:reviewer",
                command=["agent", "run"],
                workspace_mode="read_only",
                network_mode="gateway_only",
                gateway_capabilities=[
                    "run.describe",
                    "work.append",
                    "workflow.scope-review.submit",
                ],
            ),
            krab_identity_id=reviewer_identity["id"],
            created_by=self.user["id"],
        )
        return self.database.create_workflow_configuration(
            project_key="discovery-project",
            name="implementation",
            spec=WorkflowConfigurationSpec.from_mapping(
                {
                    "schema_version": 1,
                    "review_separation": "different_profile_and_identity",
                    "maintainer_approval": True,
                    "validations": [],
                    "max_correction_rounds": 2,
                    "landing_mode": "manual",
                }
            ),
            producer_profile_name=producer["name"],
            reviewer_profile_name=reviewer["name"],
            created_by=self.user["id"],
        )

    def start_implementation(
        self,
        discovery_execution_id: str,
        *,
        source_revision: str = SOURCE_REVISION,
        idempotency_key: str = "implementation-from-discovery",
    ) -> dict[str, object]:
        return ControlPlane(self.database).start_workflow_execution(
            project_key="discovery-project",
            configuration_name="implementation",
            work_item_id=self.work_item["id"],
            source_revision=source_revision,
            expected_configuration_version=1,
            expected_context_version_id=self.project["context_version_id"],
            expected_work_item_version_id=self.work_item["work_item_version_id"],
            expected_thread_generation=1,
            expected_thread_version=1,
            discovery_execution_id=discovery_execution_id,
            idempotency_key=idempotency_key,
            requested_by_handle="operator",
        )

    def test_start_pins_exact_inputs_decision_owner_and_idempotency(self) -> None:
        execution = self.start()
        repeated = self.start()

        self.assertEqual(repeated["id"], execution["id"])
        self.assertEqual(
            set(execution),
            {
                "cancel_reason",
                "configuration_id",
                "configuration_name",
                "configuration_version",
                "configuration_version_id",
                "content_sha256",
                "context_version",
                "context_version_id",
                "created_at",
                "decision_owner",
                "events",
                "failure_reason",
                "final_ruling",
                "finished_at",
                "id",
                "krab_claim_id",
                "krab_session_id",
                "project_id",
                "project_key",
                "requested_by",
                "roles",
                "rulings",
                "runs",
                "source_revision",
                "spec",
                "stage",
                "state",
                "template",
                "template_definition_id",
                "trace",
                "updated_at",
                "version",
                "work_item_id",
                "work_item_version",
                "work_item_version_id",
                "work_thread_generation",
                "work_thread_id",
            },
        )
        self.assertEqual(execution["state"], "active")
        self.assertEqual(execution["stage"], "investigate")
        self.assertEqual(execution["version"], 1)
        self.assertEqual(execution["project_id"], self.project["id"])
        self.assertEqual(execution["project_key"], "discovery-project")
        self.assertEqual(execution["configuration_name"], "default")
        self.assertEqual(execution["configuration_version"], 1)
        self.assertEqual(execution["context_version"], 1)
        self.assertEqual(execution["work_item_id"], self.work_item["id"])
        self.assertEqual(execution["work_item_version"], 1)
        self.assertEqual(execution["work_thread_generation"], 1)
        self.assertEqual(execution["source_revision"], SOURCE_REVISION)
        self.assertEqual(execution["krab_session_id"], "ses_" + "4" * 32)
        self.assertEqual(execution["krab_claim_id"], "clm_" + "5" * 32)
        self.assertEqual(execution["requested_by"], "operator")
        self.assertIsNone(execution["cancel_reason"])
        self.assertIsNone(execution["failure_reason"])
        self.assertIsNone(execution["finished_at"])
        self.assertIsNone(execution["trace"])
        self.assertEqual(execution["rulings"], [])
        self.assertIsNone(execution["final_ruling"])
        self.assertEqual(execution["runs"], {})
        self.assertEqual(execution["created_at"], execution["updated_at"])
        self.assertEqual(
            execution["context_version_id"], self.project["context_version_id"]
        )
        self.assertEqual(
            execution["work_item_version_id"],
            self.work_item["work_item_version_id"],
        )
        self.assertEqual(
            execution["decision_owner"],
            {
                "local_user_handle": "operator",
                "krab_identity_id": self.decision_owner["id"],
                "krab_user_id": "usr_" + "0" * 32,
                "krab_handle": "operator",
            },
        )
        self.assertEqual(set(execution["roles"]), {"investigator", "coordinator"})
        self.assertEqual(
            {
                key: value
                for key, value in execution["events"][0].items()
                if key != "created_at"
            },
            {
                "sequence": 1,
                "kind": "discovery.started",
                "from_state": None,
                "to_state": "active",
                "actor": "operator",
                "metadata": {},
            },
        )
        expected_request = {
            "schema_version": 1,
            "project_key": "discovery-project",
            "configuration_name": "default",
            "work_item_id": self.work_item["id"],
            "source_revision": SOURCE_REVISION,
            "expected_configuration_version": 1,
            "expected_context_version_id": self.project["context_version_id"],
            "expected_work_item_version_id": self.work_item["work_item_version_id"],
            "expected_thread_generation": 1,
            "expected_thread_version": 1,
            "krab_session_id": "ses_" + "4" * 32,
            "krab_claim_id": "clm_" + "5" * 32,
        }
        with self.database.connect() as connection:
            stored = connection.execute(
                "SELECT request_json, content_sha256 FROM discovery_executions "
                "WHERE id = ?",
                (execution["id"],),
            ).fetchone()
        self.assertEqual(json.loads(stored["request_json"]), expected_request)
        self.assertEqual(
            stored["content_sha256"], content_sha256(canonical_json(expected_request))
        )
        self.assertEqual(execution["content_sha256"], stored["content_sha256"])

        with self.assertRaises(ConflictError):
            self.start(source_revision="b" * 40)
        with self.assertRaises(ConcurrentUpdateError):
            self.start(
                idempotency_key="stale-context",
                expected_context_version_id="pcv_" + "0" * 32,
            )

    def test_completed_go_ruling_can_pin_one_implementation_scope(self) -> None:
        discovery = self.complete_discovery()
        self.create_implementation_configuration()

        execution = self.start_implementation(discovery["id"])
        repeated = self.start_implementation(discovery["id"])

        self.assertEqual(repeated, execution)
        self.assertEqual(
            execution["scope_binding"],
            {
                "discovery_execution_id": discovery["id"],
                "discovery_ruling_id": discovery["final_ruling"]["id"],
                "discovery_ruling_sha256": discovery["final_ruling"]["content_sha256"],
                "work_ready_brief": ruling_value()["work_ready_brief"],
                "created_at": execution["created_at"],
            },
        )
        with self.database.connect() as connection:
            request = json.loads(
                connection.execute(
                    "SELECT request_json FROM workflow_executions WHERE id = ?",
                    (execution["id"],),
                ).fetchone()["request_json"]
            )
        self.assertEqual(request["discovery_execution_id"], discovery["id"])
        producer = self.database.create_run(
            project_key="discovery-project",
            profile_name="Producer",
            work_item_id=self.work_item["id"],
            requested_by=self.user["id"],
            source_revision=SOURCE_REVISION,
            supervision_policy=WORKFLOW_RUN_SUPERVISION,
            workflow_execution_id=execution["id"],
            workflow_round=0,
            workflow_role="producer",
        )
        assignment = self.database.get_run_execution(producer["id"])
        self.assertEqual(
            assignment["workflow_assignment"],
            {
                "execution_id": execution["id"],
                "correction_round": 0,
                "role": "producer",
                "stage": "build",
                "scope_binding": execution["scope_binding"],
            },
        )
        for statement in (
            "UPDATE workflow_scope_bindings SET created_at = created_at "
            "WHERE workflow_execution_id = ?",
            "DELETE FROM workflow_scope_bindings WHERE workflow_execution_id = ?",
        ):
            with (
                self.subTest(statement=statement),
                self.database.connect() as connection,
                self.assertRaises(sqlite3.IntegrityError),
            ):
                connection.execute(statement, (execution["id"],))

    def test_implementation_scope_requires_a_completed_go_ruling(self) -> None:
        discovery = self.complete_discovery(
            disposition="NO_GO",
            summary="The issue must not proceed.",
            work_ready_brief=None,
        )
        self.create_implementation_configuration()

        with self.assertRaisesRegex(ConflictError, "completed GO ruling"):
            self.start_implementation(discovery["id"])

    def test_implementation_scope_rejects_a_different_source_revision(self) -> None:
        discovery = self.complete_discovery()
        self.create_implementation_configuration()

        with self.assertRaisesRegex(ConflictError, "source revision"):
            self.start_implementation(
                discovery["id"],
                source_revision="b" * 40,
                idempotency_key="mismatched-discovery-source",
            )

    def test_scoped_implementation_requires_live_role_gateway_surfaces(self) -> None:
        discovery = self.complete_discovery()
        self.create_implementation_configuration()
        blind_producer_identity = self.identity("blind-producer", "8")
        blind_reviewer_identity = self.identity("blind-reviewer", "9")
        self.database.create_agent_profile(
            name="Blind Producer",
            spec=AgentProfileSpec.create(
                image="agent@sha256:blind-producer",
                command=["agent", "run"],
                workspace_mode="copy_on_write",
                network_mode="none",
            ),
            krab_identity_id=blind_producer_identity["id"],
            created_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Blind Reviewer",
            spec=AgentProfileSpec.create(
                image="agent@sha256:blind-reviewer",
                command=["agent", "run"],
                workspace_mode="read_only",
                network_mode="gateway_only",
                gateway_capabilities=["run.describe", "work.append"],
            ),
            krab_identity_id=blind_reviewer_identity["id"],
            created_by=self.user["id"],
        )
        spec = WorkflowConfigurationSpec.from_mapping(
            {
                "schema_version": 1,
                "review_separation": "different_profile_and_identity",
                "maintainer_approval": True,
                "validations": [],
                "max_correction_rounds": 2,
                "landing_mode": "manual",
            }
        )
        for name, producer, reviewer, missing in (
            ("blind-producer", "Blind Producer", "Reviewer", "run.describe"),
            (
                "blind-reviewer",
                "Producer",
                "Blind Reviewer",
                "workflow.scope-review.submit",
            ),
        ):
            self.database.create_workflow_configuration(
                project_key="discovery-project",
                name=name,
                spec=spec,
                producer_profile_name=producer,
                reviewer_profile_name=reviewer,
                created_by=self.user["id"],
            )
            with (
                self.subTest(role=name),
                self.assertRaisesRegex(ConflictError, missing.replace(".", r"\.")),
            ):
                ControlPlane(self.database).start_workflow_execution(
                    project_key="discovery-project",
                    configuration_name=name,
                    work_item_id=self.work_item["id"],
                    source_revision=SOURCE_REVISION,
                    expected_configuration_version=1,
                    expected_context_version_id=self.project["context_version_id"],
                    expected_work_item_version_id=self.work_item[
                        "work_item_version_id"
                    ],
                    expected_thread_generation=1,
                    expected_thread_version=1,
                    discovery_execution_id=discovery["id"],
                    idempotency_key=f"missing-scope-surface-{name}",
                    requested_by_handle="operator",
                )

    def test_start_validates_bounds_and_pinned_versions(self) -> None:
        invalid_values = (
            ("source_revision", 42),
            ("expected_configuration_version", "1"),
            ("expected_thread_generation", 0),
            ("expected_thread_version", []),
            ("idempotency_key", None),
        )
        for field, value in invalid_values:
            with self.subTest(field=field), self.assertRaises(ValidationError):
                self.start(**{field: value})

        missing = (
            (
                {
                    "project_key": "missing-project",
                    "idempotency_key": "missing-project",
                },
                NotFoundError,
                "active project",
            ),
            (
                {"configuration_name": "missing", "idempotency_key": "missing-config"},
                NotFoundError,
                "configuration not found",
            ),
            (
                {
                    "work_item_id": "wki_" + "f" * 32,
                    "idempotency_key": "missing-work-item",
                },
                NotFoundError,
                "work item not found",
            ),
            (
                {
                    "expected_configuration_version": 2,
                    "idempotency_key": "stale-config",
                },
                ConflictError,
                "configuration version changed",
            ),
            (
                {
                    "expected_work_item_version_id": "wiv_" + "e" * 32,
                    "idempotency_key": "stale-item",
                },
                ConcurrentUpdateError,
                "work item version changed",
            ),
            (
                {
                    "expected_thread_generation": 2,
                    "idempotency_key": "stale-generation",
                },
                ConcurrentUpdateError,
                "thread generation changed",
            ),
            (
                {"expected_thread_version": 2, "idempotency_key": "stale-thread"},
                ConcurrentUpdateError,
                "thread version changed",
            ),
        )
        for overrides, error, message in missing:
            with self.subTest(message=message), self.assertRaisesRegex(error, message):
                self.start(**overrides)

        self.start(idempotency_key="first-active")
        with self.assertRaisesRegex(
            ConflictError, "active workflow or active prescription"
        ):
            self.start(idempotency_key="second-active")

    def test_start_requires_one_decision_owner_and_only_owner_may_cancel(self) -> None:
        unbound = self.database.create_user("unbound", "Unbound Operator")
        with self.assertRaisesRegex(ConflictError, "exactly one Krab identity"):
            self.start(
                requested_by=unbound["id"],
                idempotency_key="unbound-requester",
            )

        execution = self.start()
        another = self.database.create_user("another", "Another Operator")
        with self.assertRaisesRegex(ConflictError, "decision owner"):
            self.database.cancel_discovery_execution(
                execution["id"],
                expected_version=execution["version"],
                cancelled_by=another["id"],
                reason="Not my decision.",
            )
        cancelled = self.database.cancel_discovery_execution(
            execution["id"],
            expected_version=execution["version"],
            cancelled_by=self.user["id"],
            reason="Owner stopped discovery.",
        )
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(cancelled["version"], 2)
        self.assertEqual(cancelled["cancel_reason"], "Owner stopped discovery.")
        self.assertIsNone(cancelled["failure_reason"])
        self.assertIsNotNone(cancelled["finished_at"])
        self.assertEqual(
            {
                key: value
                for key, value in cancelled["events"][-1].items()
                if key != "created_at"
            },
            {
                "sequence": 2,
                "kind": "discovery.cancelled",
                "from_state": "active",
                "to_state": "cancelled",
                "actor": "operator",
                "metadata": {"reason": "Owner stopped discovery."},
            },
        )
        self.assertEqual(
            [
                item["id"]
                for item in self.database.list_discovery_executions(
                    project_key="discovery-project", state="cancelled", limit=1
                )
            ],
            [execution["id"]],
        )
        self.assertEqual(
            self.database.list_discovery_executions(
                project_key="missing-project", state="cancelled"
            ),
            [],
        )
        for arguments in (
            {"limit": True},
            {"limit": 0},
            {"limit": 501},
            {"state": "unknown"},
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValidationError):
                self.database.list_discovery_executions(**arguments)
        with self.assertRaisesRegex(ConflictError, "not active"):
            self.database.cancel_discovery_execution(
                execution["id"],
                expected_version=cancelled["version"],
                cancelled_by=self.user["id"],
                reason="Already cancelled.",
            )

    def test_start_rejects_ambiguous_local_decision_owner_binding(self) -> None:
        self.identity("operator-two", "a", user_id=self.user["id"])
        with self.assertRaisesRegex(ConflictError, "exactly one Krab identity"):
            self.start(idempotency_key="ambiguous-owner")

    def test_advance_requires_successful_runs_and_exact_evidence(self) -> None:
        execution = self.start(idempotency_key="advance-bounds")
        with self.assertRaises(ValidationError):
            self.database.advance_discovery_execution(
                execution["id"],
                expected_version=execution["version"],
                expected_stage="investigate",
                target_stage="completion",
                event_kind="discovery.investigation_completed",
                metadata={},
            )
        with self.assertRaises(ValidationError):
            self.database.advance_discovery_execution(
                execution["id"],
                expected_version=execution["version"],
                expected_stage="investigate",
                target_stage="coordinate",
                event_kind="discovery.investigation_completed",
                metadata=[],
            )
        with self.assertRaisesRegex(ConflictError, "run has not succeeded"):
            self.database.advance_discovery_execution(
                execution["id"],
                expected_version=execution["version"],
                expected_stage="investigate",
                target_stage="coordinate",
                event_kind="discovery.investigation_completed",
                metadata={},
            )

        investigator = self.create_role_run(execution, "investigator")
        with self.assertRaisesRegex(ConflictError, "run has not succeeded"):
            self.database.advance_discovery_execution(
                execution["id"],
                expected_version=execution["version"],
                expected_stage="investigate",
                target_stage="coordinate",
                event_kind="discovery.investigation_completed",
                metadata={},
            )
        self.run_attempt(investigator)
        with self.assertRaisesRegex(ConflictError, "has no trace"):
            self.database.advance_discovery_execution(
                execution["id"],
                expected_version=execution["version"],
                expected_stage="investigate",
                target_stage="coordinate",
                event_kind="discovery.investigation_completed",
                metadata={},
            )
        self.database.cancel_discovery_execution(
            execution["id"],
            expected_version=execution["version"],
            cancelled_by=self.user["id"],
            reason="Evidence-negative test complete.",
        )

        coordinate, _investigator = self.prepare_coordinate()
        coordinator = self.create_role_run(coordinate, "coordinator")
        self.run_attempt(coordinator)
        current = self.database.get_discovery_execution(coordinate["id"])
        with self.assertRaisesRegex(ConflictError, "no final ruling"):
            self.database.advance_discovery_execution(
                coordinate["id"],
                expected_version=current["version"],
                expected_stage="coordinate",
                target_stage="completion",
                event_kind="discovery.coordination_completed",
                metadata={},
            )

    def test_failed_execution_records_terminal_reason_and_event(self) -> None:
        execution = self.start(idempotency_key="failed-discovery")
        with self.assertRaisesRegex(ConflictError, "not completing"):
            self.database.complete_discovery_execution(
                execution["id"], expected_version=execution["version"]
            )
        failed = self.database.fail_discovery_execution(
            execution["id"],
            expected_version=execution["version"],
            reason="Investigator runtime exhausted its bounded retry policy.",
        )
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed["version"], 2)
        self.assertIsNone(failed["cancel_reason"])
        self.assertEqual(
            failed["failure_reason"],
            "Investigator runtime exhausted its bounded retry policy.",
        )
        self.assertIsNotNone(failed["finished_at"])
        self.assertEqual(
            {
                key: value
                for key, value in failed["events"][-1].items()
                if key != "created_at"
            },
            {
                "sequence": 2,
                "kind": "discovery.failed",
                "from_state": "active",
                "to_state": "failed",
                "actor": "operator",
                "metadata": {
                    "reason": "Investigator runtime exhausted its bounded retry policy."
                },
            },
        )
        with self.assertRaisesRegex(ConflictError, "not active"):
            self.database.fail_discovery_execution(
                execution["id"],
                expected_version=failed["version"],
                reason="Cannot fail twice.",
            )

    def test_role_runs_pin_profiles_stage_and_trace_provenance(self) -> None:
        execution = self.start()
        with self.assertRaises(ConflictError):
            self.create_role_run(execution, "coordinator")

        investigator = self.create_role_run(execution, "investigator")
        self.assertEqual(
            investigator["profile_version_id"],
            execution["roles"]["investigator"]["profile_version_id"],
        )
        self.assertEqual(investigator["discovery_execution_id"], execution["id"])
        self.assertEqual(investigator["discovery_role"], "investigator")
        with self.assertRaises(ConflictError):
            self.create_role_run(execution, "investigator")

        with self.assertRaises(ValidationError):
            self.database.create_run(
                project_key="discovery-project",
                profile_name="Investigator",
                work_item_id=self.work_item["id"],
                requested_by=self.user["id"],
                source_revision=SOURCE_REVISION,
                discovery_execution_id=execution["id"],
            )

        with self.assertRaises(NotFoundError):
            self.database.create_run(
                project_key="discovery-project",
                profile_name="Coordinator",
                work_item_id=self.work_item["id"],
                requested_by=self.user["id"],
                source_revision=SOURCE_REVISION,
                discovery_execution_id=execution["id"],
                discovery_role="investigator",
            )

        records: list[dict[str, object]] = []

        def submit(attempt: dict[str, object]) -> None:
            trace = DiscoveryTrace.from_mapping(trace_value())
            first = self.database.submit_discovery_trace(
                investigator["id"], trace=trace, idempotency_key="trace-key-1"
            )
            repeated = self.database.submit_discovery_trace(
                investigator["id"], trace=trace, idempotency_key="trace-key-1"
            )
            self.assertEqual(repeated, first)
            self.assertEqual(
                set(first),
                {
                    "id",
                    "execution_id",
                    "run_id",
                    "attempt_id",
                    "trace",
                    "content_sha256",
                    "idempotency_key",
                    "created_at",
                },
            )
            self.assertTrue(first["id"].startswith("dtr_"))
            self.assertEqual(first["execution_id"], execution["id"])
            self.assertEqual(first["run_id"], investigator["id"])
            self.assertEqual(first["attempt_id"], attempt["id"])
            self.assertEqual(first["trace"], trace.to_dict())
            self.assertEqual(first["content_sha256"], trace.digest)
            self.assertEqual(first["idempotency_key"], "trace-key-1")
            self.assertIsInstance(first["created_at"], str)
            records.append(first)
            with self.assertRaises(ConflictError):
                self.database.submit_discovery_trace(
                    investigator["id"],
                    trace=DiscoveryTrace.from_mapping(
                        trace_value(summary="A different trace.")
                    ),
                    idempotency_key="trace-key-1",
                )
            with self.assertRaisesRegex(ConflictError, "already has a trace"):
                self.database.submit_discovery_trace(
                    investigator["id"], trace=trace, idempotency_key="trace-key-2"
                )

        self.run_attempt(investigator, submit)
        stored = self.database.get_discovery_execution(execution["id"])
        self.assertEqual(stored["trace"], records[0])
        worker_view = self.database.get_run_execution(investigator["id"])
        self.assertEqual(
            worker_view["discovery_assignment"],
            {
                "execution_id": execution["id"],
                "role": "investigator",
                "stage": "investigate",
                "decision_owner_handle": "operator",
                "configuration": self.configuration["spec"],
                "trace": records[0],
                "rulings": [],
            },
        )

    def test_trace_enforces_configuration_and_pinned_work_item_sources(self) -> None:
        execution = self.start()
        investigator = self.create_role_run(execution, "investigator")

        with self.assertRaisesRegex(ConflictError, "active worker attempt"):
            self.database.submit_discovery_trace(
                investigator["id"],
                trace=DiscoveryTrace.from_mapping(trace_value()),
                idempotency_key="queued-trace",
            )
        with self.assertRaisesRegex(NotFoundError, "Discovery-bound run"):
            self.database.submit_discovery_trace(
                "run_" + "d" * 32,
                trace=DiscoveryTrace.from_mapping(trace_value()),
                idempotency_key="missing-run",
            )
        with self.assertRaises(ValidationError):
            self.database.submit_discovery_trace(
                investigator["id"], trace=trace_value(), idempotency_key="wrong-type"
            )
        with self.assertRaises(ValidationError):
            self.database.submit_discovery_trace(
                investigator["id"],
                trace=DiscoveryTrace.from_mapping(trace_value()),
                idempotency_key="short",
            )

        def submit(_attempt: dict[str, object]) -> None:
            finding = trace_value()["findings"][0]
            oversized = DiscoveryTrace.from_mapping(
                trace_value(
                    findings=[
                        {**finding, "key": f"finding-{index}"} for index in range(3)
                    ]
                )
            )
            invalid_source = DiscoveryTrace.from_mapping(
                trace_value(
                    findings=[
                        {
                            **finding,
                            "source_refs": [
                                {"kind": "acceptance_criterion", "index": 1}
                            ],
                        }
                    ]
                )
            )
            for key, trace in (
                ("too-many-findings", oversized),
                ("invalid-source", invalid_source),
            ):
                with self.subTest(key=key), self.assertRaises(ConflictError):
                    self.database.submit_discovery_trace(
                        investigator["id"], trace=trace, idempotency_key=key
                    )

        self.run_attempt(investigator, submit, complete=False)

    def test_final_ruling_is_trace_bound_and_completion_is_optimistic(self) -> None:
        execution, _investigator = self.prepare_coordinate()
        coordinator = self.create_role_run(execution, "coordinator")

        records: list[dict[str, object]] = []

        def submit(attempt: dict[str, object]) -> None:
            ruling = DiscoveryRuling.from_mapping(ruling_value())
            record = self.database.submit_discovery_ruling(
                coordinator["id"], ruling=ruling, idempotency_key="ruling-1"
            )
            repeated = self.database.submit_discovery_ruling(
                coordinator["id"], ruling=ruling, idempotency_key="ruling-1"
            )
            self.assertEqual(repeated, record)
            self.assertEqual(
                set(record),
                {
                    "id",
                    "execution_id",
                    "sequence",
                    "run_id",
                    "attempt_id",
                    "disposition",
                    "interaction_id",
                    "ruling",
                    "content_sha256",
                    "idempotency_key",
                    "created_at",
                },
            )
            self.assertTrue(record["id"].startswith("drl_"))
            self.assertEqual(record["execution_id"], execution["id"])
            self.assertEqual(record["sequence"], 1)
            self.assertEqual(record["run_id"], coordinator["id"])
            self.assertEqual(record["attempt_id"], attempt["id"])
            self.assertEqual(record["disposition"], "GO")
            self.assertIsNone(record["interaction_id"])
            self.assertEqual(record["ruling"], ruling.to_dict())
            self.assertEqual(record["content_sha256"], ruling.digest)
            self.assertEqual(record["idempotency_key"], "ruling-1")
            self.assertIsInstance(record["created_at"], str)
            records.append(record)
            invalid = DiscoveryRuling.from_mapping(
                ruling_value(trace_finding_keys=["unknown-finding"])
            )
            with self.assertRaises(ConflictError):
                self.database.submit_discovery_ruling(
                    coordinator["id"],
                    ruling=invalid,
                    idempotency_key="unknown-finding",
                )
            with self.assertRaisesRegex(ConflictError, "another Discovery ruling"):
                self.database.submit_discovery_ruling(
                    coordinator["id"],
                    ruling=DiscoveryRuling.from_mapping(
                        ruling_value(
                            disposition="NO_GO",
                            summary="The same key cannot change its ruling.",
                            work_ready_brief=None,
                        )
                    ),
                    idempotency_key="ruling-1",
                )
            with self.assertRaisesRegex(ConflictError, "already has a final ruling"):
                self.database.submit_discovery_ruling(
                    coordinator["id"],
                    ruling=DiscoveryRuling.from_mapping(
                        ruling_value(
                            disposition="NO_GO",
                            summary="A second final ruling is forbidden.",
                            work_ready_brief=None,
                        )
                    ),
                    idempotency_key="ruling-2",
                )

        self.run_attempt(coordinator, submit)
        current = self.database.get_discovery_execution(execution["id"])
        self.assertEqual(current["final_ruling"], records[0])
        completion = self.database.advance_discovery_execution(
            execution["id"],
            expected_version=current["version"],
            expected_stage="coordinate",
            target_stage="completion",
            event_kind="discovery.coordination_completed",
            metadata={"coordinator_run_id": coordinator["id"]},
        )
        completed = self.database.complete_discovery_execution(
            execution["id"], expected_version=completion["version"]
        )
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["stage"], "completion")
        self.assertEqual(completed["version"], 4)
        self.assertIsNotNone(completed["finished_at"])
        self.assertEqual(
            [event["kind"] for event in completed["events"]],
            [
                "discovery.started",
                "discovery.investigation_completed",
                "discovery.coordination_completed",
                "discovery.completed",
            ],
        )
        self.assertEqual(
            completed["events"][2]["metadata"],
            {
                "coordinator_run_id": coordinator["id"],
                "from_stage": "coordinate",
                "to_stage": "completion",
            },
        )
        self.assertEqual(completed["events"][3]["metadata"], {})
        self.assertEqual(completed["events"][3]["from_state"], "active")
        self.assertEqual(completed["events"][3]["to_state"], "completed")
        self.assertEqual(completed["events"][3]["actor"], "operator")
        with self.assertRaises(ConcurrentUpdateError):
            self.database.complete_discovery_execution(
                execution["id"], expected_version=completion["version"]
            )

    def test_ruling_enforces_configured_brief_and_source_bounds(self) -> None:
        execution, _investigator = self.prepare_coordinate()
        coordinator = self.create_role_run(execution, "coordinator")

        def submit(_attempt: dict[str, object]) -> None:
            base = ruling_value()
            brief = base["work_ready_brief"]
            must = brief["must_items"][0]
            invalid_rulings = (
                (
                    "too-many-musts",
                    ruling_value(
                        work_ready_brief={
                            **brief,
                            "must_items": [
                                {**must, "key": f"must-{index}"} for index in range(3)
                            ],
                        }
                    ),
                    "MUST limit",
                ),
                (
                    "too-many-follow-ups",
                    ruling_value(
                        work_ready_brief={
                            **brief,
                            "follow_up_proposals": [
                                f"Follow-up {index}" for index in range(3)
                            ],
                        }
                    ),
                    "follow-up limit",
                ),
                (
                    "invalid-must-source",
                    ruling_value(
                        work_ready_brief={
                            **brief,
                            "must_items": [
                                {
                                    **must,
                                    "source_refs": [
                                        {"kind": "acceptance_criterion", "index": 1}
                                    ],
                                }
                            ],
                        }
                    ),
                    "outside the pinned work item",
                ),
                (
                    "unknown-must-finding",
                    ruling_value(
                        work_ready_brief={
                            **brief,
                            "must_items": [
                                {**must, "finding_keys": ["unknown-finding"]}
                            ],
                        }
                    ),
                    "unknown trace finding",
                ),
                (
                    "unearned-interaction",
                    ruling_value(interaction_id="int_" + "e" * 32),
                    "no preceding human question",
                ),
            )
            for key, value, message in invalid_rulings:
                with (
                    self.subTest(key=key),
                    self.assertRaisesRegex(ConflictError, message),
                ):
                    self.database.submit_discovery_ruling(
                        coordinator["id"],
                        ruling=DiscoveryRuling.from_mapping(value),
                        idempotency_key=key,
                    )

        self.run_attempt(coordinator, submit, complete=False)

    def test_ask_human_is_bound_to_the_open_decision_owner_interaction(self) -> None:
        execution, _investigator = self.prepare_coordinate()
        coordinator = self.create_role_run(execution, "coordinator")
        attempt = self.run_attempt(coordinator, complete=False)
        opened = self.database.open_interaction(
            coordinator["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.decision_owner["id"],
            request_item_id="itm_" + "6" * 32,
            expected_run_version=attempt["run_version"],
        )
        ruling = DiscoveryRuling.from_mapping(
            ruling_value(
                disposition="ASK_HUMAN",
                interaction_id=opened["id"],
                work_ready_brief=None,
            )
        )
        stored = self.database.submit_discovery_ruling(
            coordinator["id"], ruling=ruling, idempotency_key="ask-key-1"
        )
        self.assertEqual(stored["ruling"]["disposition"], "ASK_HUMAN")
        self.assertEqual(stored["interaction_id"], opened["id"])
        waiting_snapshot = ControlRoom(Database(self.database.path)).snapshot()
        projected = waiting_snapshot["discovery_executions"][0]
        self.assertEqual(projected["coordinator_run_state"], "waiting_for_input")
        self.assertEqual(
            (
                projected["latest_ruling_id"],
                projected["latest_ruling_disposition"],
                projected["latest_ruling_interaction_id"],
                projected["latest_ruling_interaction_state"],
            ),
            (stored["id"], "ASK_HUMAN", opened["id"], "open"),
        )
        self.assertIn(
            opened["id"],
            {
                item["subject_id"]
                for item in waiting_snapshot["attention"]
                if item["kind"] == "interaction.pending"
            },
        )
        self.assertNotIn(ruling.summary, json.dumps(projected, sort_keys=True))

        with self.assertRaises(ConflictError):
            self.database.submit_discovery_ruling(
                coordinator["id"],
                ruling=DiscoveryRuling.from_mapping(ruling_value()),
                idempotency_key="premature-final",
            )

        answered = self.database.answer_interaction(
            opened["id"],
            response_item_id="itm_" + "7" * 32,
            expected_version=opened["version"],
        )
        final = self.database.submit_discovery_ruling(
            coordinator["id"],
            ruling=DiscoveryRuling.from_mapping(
                ruling_value(interaction_id=answered["id"])
            ),
            idempotency_key="answered-final",
        )
        self.assertEqual(final["disposition"], "GO")
        self.assertEqual(final["interaction_id"], answered["id"])

    def test_ask_human_rejects_an_interaction_for_another_operator(self) -> None:
        execution, _investigator = self.prepare_coordinate()
        coordinator = self.create_role_run(execution, "coordinator")
        attempt = self.run_attempt(coordinator, complete=False)
        other_user = self.database.create_user("other", "Other Operator")
        other_identity = self.identity("other", "b", user_id=other_user["id"])
        opened = self.database.open_interaction(
            coordinator["id"],
            kind=InteractionKind.CHOICE,
            recipient_krab_identity_id=other_identity["id"],
            request_item_id="itm_" + "c" * 32,
            expected_run_version=attempt["run_version"],
        )
        with self.assertRaisesRegex(ConflictError, "decision-owner"):
            self.database.submit_discovery_ruling(
                coordinator["id"],
                ruling=DiscoveryRuling.from_mapping(
                    ruling_value(
                        disposition="ASK_HUMAN",
                        interaction_id=opened["id"],
                        work_ready_brief=None,
                    )
                ),
                idempotency_key="wrong-owner",
            )

    def test_ask_human_rulings_have_a_hard_execution_limit(self) -> None:
        execution, _investigator = self.prepare_coordinate()
        coordinator = self.create_role_run(execution, "coordinator")
        attempt = self.run_attempt(coordinator, complete=False)
        opened = self.database.open_interaction(
            coordinator["id"],
            kind=InteractionKind.QUESTION,
            recipient_krab_identity_id=self.decision_owner["id"],
            request_item_id="itm_" + "9" * 32,
            expected_run_version=attempt["run_version"],
        )
        for index in range(8):
            stored = self.database.submit_discovery_ruling(
                coordinator["id"],
                ruling=DiscoveryRuling.from_mapping(
                    ruling_value(
                        disposition="ASK_HUMAN",
                        summary=f"Bounded question {index}.",
                        interaction_id=opened["id"],
                        work_ready_brief=None,
                    )
                ),
                idempotency_key=f"bounded-ask-{index}",
            )
            self.assertEqual(stored["sequence"], index + 1)
            self.assertEqual(stored["disposition"], "ASK_HUMAN")
        with self.assertRaisesRegex(ConflictError, "question limit"):
            self.database.submit_discovery_ruling(
                coordinator["id"],
                ruling=DiscoveryRuling.from_mapping(
                    ruling_value(
                        disposition="ASK_HUMAN",
                        summary="This ninth question must not be accepted.",
                        interaction_id=opened["id"],
                        work_ready_brief=None,
                    )
                ),
                idempotency_key="bounded-ask-8",
            )

    def test_director_runs_fixed_roles_and_completes_from_final_ruling(self) -> None:
        execution = self.start()
        director = DiscoveryDirector(ControlPlane(self.database))

        self.assertFalse(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        created = self.database.get_discovery_execution(execution["id"])
        investigator = self.database.get_run(created["runs"]["investigator"]["run_id"])
        self.assertEqual(
            self.database.get_run_execution(investigator["id"])["supervision_policy"],
            DISCOVERY_RUN_SUPERVISION.to_dict(),
        )
        self.assertFalse(
            DiscoveryDirector(ControlPlane(self.database)).advance(
                execution["id"], expected_version=execution["version"]
            )
        )
        self.assertEqual(
            len(self.database.get_discovery_execution(execution["id"])["runs"]), 1
        )

        trace = DiscoveryTrace.from_mapping(trace_value())
        self.run_attempt(
            investigator,
            lambda _attempt: self.database.submit_discovery_trace(
                investigator["id"], trace=trace, idempotency_key="directed-trace"
            ),
        )
        self.assertTrue(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        coordinate = self.database.get_discovery_execution(execution["id"])
        self.assertEqual(
            (coordinate["stage"], coordinate["version"]), ("coordinate", 2)
        )
        self.assertEqual(
            coordinate["events"][-1]["metadata"],
            {
                "from_stage": "investigate",
                "investigator_run_id": investigator["id"],
                "to_stage": "coordinate",
                "trace_id": coordinate["trace"]["id"],
                "trace_sha256": trace.digest,
            },
        )

        self.assertFalse(
            director.advance(coordinate["id"], expected_version=coordinate["version"])
        )
        directed = self.database.get_discovery_execution(execution["id"])
        coordinator = self.database.get_run(directed["runs"]["coordinator"]["run_id"])
        self.assertEqual(
            self.database.get_run_execution(coordinator["id"])["supervision_policy"],
            DISCOVERY_RUN_SUPERVISION.to_dict(),
        )
        self.assertFalse(
            director.advance(directed["id"], expected_version=directed["version"])
        )
        ruling = DiscoveryRuling.from_mapping(ruling_value())
        self.run_attempt(
            coordinator,
            lambda _attempt: self.database.submit_discovery_ruling(
                coordinator["id"],
                ruling=ruling,
                idempotency_key="directed-ruling",
            ),
        )
        self.assertTrue(
            director.advance(directed["id"], expected_version=directed["version"])
        )
        completion = self.database.get_discovery_execution(execution["id"])
        self.assertEqual(
            (completion["stage"], completion["version"]), ("completion", 3)
        )
        self.assertEqual(
            completion["events"][-1]["metadata"],
            {
                "coordinator_run_id": coordinator["id"],
                "disposition": "GO",
                "from_stage": "coordinate",
                "ruling_id": completion["final_ruling"]["id"],
                "ruling_sha256": ruling.digest,
                "to_stage": "completion",
            },
        )
        self.assertTrue(
            DiscoveryDirector(ControlPlane(self.database)).advance(
                completion["id"], expected_version=completion["version"]
            )
        )
        finished = self.database.get_discovery_execution(execution["id"])
        self.assertEqual(
            (finished["state"], finished["stage"]), ("completed", "completion")
        )
        self.assertEqual(finished["events"][-1]["kind"], "discovery.completed")
        projected = ControlRoom(Database(self.database.path)).snapshot()[
            "discovery_executions"
        ][0]
        self.assertEqual(projected["id"], finished["id"])
        self.assertEqual(projected["state"], "completed")
        self.assertEqual(projected["investigator_run_id"], investigator["id"])
        self.assertEqual(projected["coordinator_run_id"], coordinator["id"])
        self.assertEqual(projected["trace_id"], finished["trace"]["id"])
        self.assertEqual(
            (
                projected["latest_ruling_id"],
                projected["latest_ruling_disposition"],
            ),
            (finished["final_ruling"]["id"], "GO"),
        )
        encoded = json.dumps(projected, sort_keys=True)
        self.assertNotIn(trace.summary, encoded)
        self.assertNotIn(ruling.summary, encoded)
        assert ruling.work_ready_brief is not None
        self.assertNotIn(ruling.work_ready_brief.objective, encoded)

    def test_director_fails_closed_when_a_successful_role_omits_evidence(self) -> None:
        execution = self.start()
        director = DiscoveryDirector(ControlPlane(self.database))
        self.assertFalse(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        created = self.database.get_discovery_execution(execution["id"])
        investigator = self.database.get_run(created["runs"]["investigator"]["run_id"])
        self.run_attempt(investigator)

        self.assertTrue(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        failed = self.database.get_discovery_execution(execution["id"])
        self.assertEqual(failed["state"], "failed")
        self.assertIn("succeeded without a Discovery trace", failed["failure_reason"])
        self.assertEqual(failed["events"][-1]["kind"], "discovery.failed")

    def test_director_fails_closed_when_a_role_run_fails(self) -> None:
        execution = self.start()
        director = DiscoveryDirector(ControlPlane(self.database))
        self.assertFalse(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        created = self.database.get_discovery_execution(execution["id"])
        investigator = self.database.get_run(created["runs"]["investigator"]["run_id"])
        self.run_attempt(investigator, exit_code=17)

        self.assertTrue(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        failed = self.database.get_discovery_execution(execution["id"])
        self.assertEqual(failed["state"], "failed")
        self.assertIn("investigator run", failed["failure_reason"])
        self.assertIn("failed", failed["failure_reason"])

    def test_director_fails_closed_when_coordinator_omits_a_final_ruling(self) -> None:
        execution, _investigator = self.prepare_coordinate()
        director = DiscoveryDirector(ControlPlane(self.database))
        self.assertFalse(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        created = self.database.get_discovery_execution(execution["id"])
        coordinator = self.database.get_run(created["runs"]["coordinator"]["run_id"])
        self.run_attempt(coordinator)

        self.assertTrue(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        failed = self.database.get_discovery_execution(execution["id"])
        self.assertEqual(failed["state"], "failed")
        self.assertIn(
            "succeeded without a final Discovery ruling", failed["failure_reason"]
        )

    def test_director_fails_closed_when_the_coordinator_run_fails(self) -> None:
        execution, _investigator = self.prepare_coordinate()
        director = DiscoveryDirector(ControlPlane(self.database))
        self.assertFalse(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        created = self.database.get_discovery_execution(execution["id"])
        coordinator = self.database.get_run(created["runs"]["coordinator"]["run_id"])
        self.run_attempt(coordinator, exit_code=17)

        self.assertTrue(
            director.advance(execution["id"], expected_version=execution["version"])
        )
        failed = self.database.get_discovery_execution(execution["id"])
        self.assertEqual(failed["state"], "failed")
        self.assertIn("coordinator run", failed["failure_reason"])
        self.assertIn("failed", failed["failure_reason"])

    def test_director_treats_stale_or_terminal_execution_as_finished(self) -> None:
        execution = self.start()
        director = DiscoveryDirector(ControlPlane(self.database))

        self.assertTrue(
            director.advance(execution["id"], expected_version=execution["version"] + 1)
        )
        self.assertEqual(
            self.database.get_discovery_execution(execution["id"])["runs"], {}
        )
        cancelled = self.database.cancel_discovery_execution(
            execution["id"],
            expected_version=execution["version"],
            cancelled_by=self.user["id"],
            reason="operator stopped the discovery",
        )
        self.assertTrue(
            director.advance(cancelled["id"], expected_version=cancelled["version"])
        )

    def test_director_waits_for_every_nonterminal_run_state(self) -> None:
        for state in (
            "queued",
            "preparing",
            "running",
            "waiting_for_input",
            "paused",
        ):
            with self.subTest(state=state):
                self.assertTrue(DiscoveryDirector._run_is_active({"state": state}))
        for state in ("succeeded", "failed", "cancelled"):
            with self.subTest(state=state):
                self.assertFalse(DiscoveryDirector._run_is_active({"state": state}))

    def test_active_discovery_excludes_an_implementation_prescription(self) -> None:
        producer = self.database.create_agent_profile(
            name="Producer",
            spec=AgentProfileSpec.create(
                image="agent@sha256:producer",
                command=["agent", "run"],
                workspace_mode="copy_on_write",
                network_mode="none",
            ),
            krab_identity_id=self.investigator_identity["id"],
            created_by=self.user["id"],
        )
        reviewer = self.database.create_agent_profile(
            name="Reviewer",
            spec=AgentProfileSpec.create(
                image="agent@sha256:reviewer",
                command=["agent", "run"],
                workspace_mode="read_only",
                network_mode="none",
            ),
            krab_identity_id=self.coordinator_identity["id"],
            created_by=self.user["id"],
        )
        self.database.create_workflow_configuration(
            project_key="discovery-project",
            name="implementation",
            spec=WorkflowConfigurationSpec.from_mapping(
                {
                    "schema_version": 1,
                    "review_separation": "different_profile_and_identity",
                    "maintainer_approval": True,
                    "validations": [],
                    "max_correction_rounds": 2,
                    "landing_mode": "manual",
                }
            ),
            producer_profile_name=producer["name"],
            reviewer_profile_name=reviewer["name"],
            created_by=self.user["id"],
        )
        self.start()

        with self.assertRaisesRegex(
            ConflictError, "active workflow or active prescription"
        ):
            self.database.start_workflow_execution(
                project_key="discovery-project",
                configuration_name="implementation",
                work_item_id=self.work_item["id"],
                source_revision=SOURCE_REVISION,
                expected_configuration_version=1,
                expected_context_version_id=self.project["context_version_id"],
                expected_work_item_version_id=self.work_item["work_item_version_id"],
                expected_thread_generation=1,
                expected_thread_version=1,
                idempotency_key="implementation-during-discovery",
                requested_by=self.user["id"],
            )

    def test_active_discovery_keeps_its_work_thread_open(self) -> None:
        self.start()

        with self.assertRaisesRegex(ConflictError, "active Discovery prescription"):
            self.database.complete_work_item(
                self.work_item["id"],
                completed_by=self.user["id"],
                expected_thread_version=1,
                reason="must not close around active discovery",
            )

    def test_supervisor_keeps_one_version_bound_discovery_action(self) -> None:
        class Coordinator:
            @staticmethod
            def execute(_run_id: str) -> dict[str, object]:
                return {}

            @staticmethod
            def reconcile(_run_id: str | None = None) -> list[dict[str, object]]:
                return []

        class Director:
            def __init__(self) -> None:
                self.calls: list[tuple[str, int]] = []

            def advance(self, execution_id: str, *, expected_version: int) -> bool:
                self.calls.append((execution_id, expected_version))
                return False

        class Transport:
            @staticmethod
            def remind_interaction(*_args: object, **_kwargs: object) -> int:
                return 1

        execution = self.start()
        created = self.database.enqueue_supervisor_actions()
        self.assertEqual(len(created), 1)
        self.assertEqual(self.database.enqueue_supervisor_actions(), [])
        self.assertEqual(created[0]["kind"], "discovery.advance")
        self.assertIsNone(created[0]["run_id"])
        self.assertIsNone(created[0]["workflow_execution_id"])
        self.assertEqual(created[0]["discovery_execution_id"], execution["id"])

        director = Director()
        supervisor = Supervisor(
            self.database,
            Coordinator(),
            Transport(),
            discovery_director=director,
            poll_interval=0.05,
            action_lease_seconds=10,
            action_timeout=1,
        )
        supervisor.tick(wait=True)

        action = self.database.get_supervisor_action(created[0]["id"])
        self.assertEqual(action["state"], "pending")
        self.assertEqual(action["attempt_count"], 0)
        self.assertEqual(director.calls, [(execution["id"], execution["version"])])
        self.assertEqual(len(self.database.list_supervisor_actions()), 1)

    def test_execution_ledger_is_immutable_and_schemas_34_35_migrate_once(self) -> None:
        execution, _investigator = self.prepare_coordinate()
        coordinator = self.create_role_run(execution, "coordinator")
        self.run_attempt(
            coordinator,
            lambda _attempt: self.database.submit_discovery_ruling(
                coordinator["id"],
                ruling=DiscoveryRuling.from_mapping(
                    ruling_value(disposition="NO_GO", work_ready_brief=None)
                ),
                idempotency_key="immutable-ruling",
            ),
        )
        statements = (
            (
                "UPDATE discovery_executions SET source_revision = ? WHERE id = ?",
                ("b" * 40, execution["id"]),
            ),
            (
                "UPDATE discovery_execution_events SET metadata_json = '{}' "
                "WHERE execution_id = ?",
                (execution["id"],),
            ),
            (
                "DELETE FROM discovery_run_bindings WHERE execution_id = ?",
                (execution["id"],),
            ),
            (
                "UPDATE discovery_traces SET trace_json = '{}' WHERE execution_id = ?",
                (execution["id"],),
            ),
            (
                "DELETE FROM discovery_rulings WHERE execution_id = ?",
                (execution["id"],),
            ),
        )
        for statement, parameters in statements:
            with (
                self.subTest(statement=statement),
                self.database.connect() as connection,
                self.assertRaises(sqlite3.IntegrityError),
            ):
                connection.execute(statement, parameters)

        with self.database.connect() as connection:
            connection.executescript(
                """
                DROP TABLE workflow_scope_reviews;
                DROP TABLE workflow_scope_bindings;
                DROP TABLE advisory_reports;
                DROP TABLE advisory_challenge_reviews;
                DROP TABLE advisory_blind_reviews;
                DROP TABLE advisory_run_bindings;
                DROP TABLE advisory_execution_events;
                DROP TABLE advisory_executions;
                DROP TRIGGER discovery_ruling_immutable_delete;
                DROP TRIGGER discovery_ruling_immutable_update;
                DROP TRIGGER discovery_trace_immutable_delete;
                DROP TRIGGER discovery_trace_immutable_update;
                DROP TRIGGER discovery_run_binding_immutable_delete;
                DROP TRIGGER discovery_run_binding_immutable_update;
                DROP TRIGGER discovery_execution_event_immutable_delete;
                DROP TRIGGER discovery_execution_event_immutable_update;
                DROP TRIGGER discovery_execution_immutable_delete;
                DROP TRIGGER discovery_execution_transition;
                DROP TRIGGER discovery_execution_identity_immutable;
                DROP TABLE discovery_rulings;
                DROP TABLE discovery_traces;
                DROP TABLE discovery_run_bindings;
                DROP TABLE discovery_execution_events;
                DROP TABLE discovery_executions;
                DROP TRIGGER workflow_template_definition_immutable_delete;
                DELETE FROM workflow_template_definitions
                WHERE template_key = 'advisory-review-v1';
                CREATE TRIGGER workflow_template_definition_immutable_delete
                BEFORE DELETE ON workflow_template_definitions
                BEGIN
                    SELECT RAISE(
                        ABORT, 'workflow template definitions cannot be deleted'
                    );
                END;
                PRAGMA user_version = 33;
                """
            )

        migrated = Database(self.database.path)
        first = migrated.initialize()
        second = migrated.initialize()
        self.assertEqual(SCHEMA_VERSION, 42)
        self.assertEqual(first, second)
        self.assertEqual(first["schema_version"], SCHEMA_VERSION)
        with migrated.connect() as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name LIKE 'discovery_%'"
                )
            }
            supervisor_columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(supervisor_actions)")
            }
        self.assertTrue(
            {
                "discovery_executions",
                "discovery_execution_events",
                "discovery_run_bindings",
                "discovery_traces",
                "discovery_rulings",
            }.issubset(tables)
        )
        self.assertIn("discovery_execution_id", supervisor_columns)


if __name__ == "__main__":
    unittest.main()
