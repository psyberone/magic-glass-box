from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError

from magic_glass_box.control_room import ControlRoom
from magic_glass_box.db import (
    SCHEMA_V45,
    SCHEMA_V46,
    SCHEMA_VERSION,
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    RunState,
    ValidationError,
    WorkItemEnvelope,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.service import ControlPlane
from magic_glass_box.workflow_director import WorkflowDirector
from magic_glass_box.workflow_policy import (
    WorkflowActivationSpec,
    WorkflowPolicySpec,
)
from magic_glass_box.workflows import (
    IMPLEMENTATION_TEMPLATE_REVISION,
    WorkflowConfigurationSpec,
)
from tests.test_service_sidecars import sidecar_spec


class WorkflowPolicySpecTests(unittest.TestCase):
    @staticmethod
    def value(**overrides: object) -> dict[str, object]:
        return {
            "schema_version": 1,
            "prescription": {
                "key": "implementation-v1",
                "revision": IMPLEMENTATION_TEMPLATE_REVISION,
            },
            "evidence_floor": [
                "maintainer_disposition",
                "reviewed_content",
                "validated_execution",
            ],
            "validation_obligations": ["coverage", "mutation", "tests"],
            "resource_maxima": {
                "review_rounds": 4,
                "seat_count": 4,
                "shared_host_concurrency": 2,
                "wall_time_seconds": 21600,
            },
            "wake_mode": "manual",
            "required_capabilities": [
                {"mode": "effect", "name": "change.open", "version": 1},
                {"mode": "effect", "name": "landing.invoke", "version": 1},
                {"mode": "effect", "name": "refs.publish", "version": 1},
            ],
            "landing_methods": ["squash"],
            "allow_deferred_validation": True,
            **overrides,
        }

    def test_policy_normalizes_order_and_has_project_independent_digest(self) -> None:
        first = WorkflowPolicySpec.from_mapping(self.value())
        reordered = self.value(
            evidence_floor=list(reversed(self.value()["evidence_floor"])),
            validation_obligations=list(
                reversed(self.value()["validation_obligations"])
            ),
            required_capabilities=list(reversed(self.value()["required_capabilities"])),
        )
        second = WorkflowPolicySpec.from_mapping(reordered)

        self.assertEqual(first, second)
        self.assertEqual(first.digest, second.digest)
        self.assertEqual(first.to_dict(), self.value())
        self.assertEqual(len(first.digest), 64)

    def test_policy_rejects_weak_local_or_unbounded_contracts(self) -> None:
        missing = self.value()
        missing.pop("wake_mode")
        cases = (
            None,
            missing,
            {**self.value(), "provider": "github"},
            self.value(schema_version=True),
            self.value(schema_version=2),
            self.value(prescription={"key": "implementation-v1"}),
            self.value(
                prescription={
                    "key": "discovery-v1",
                    "revision": IMPLEMENTATION_TEMPLATE_REVISION,
                }
            ),
            self.value(evidence_floor=["reviewed_content"]),
            self.value(
                evidence_floor=[
                    "reviewed_content",
                    "reviewed_content",
                    "validated_execution",
                    "maintainer_disposition",
                ]
            ),
            self.value(validation_obligations=[]),
            self.value(validation_obligations=["Bad Obligation"]),
            self.value(validation_obligations=["tests", "tests"]),
            self.value(
                resource_maxima={
                    "review_rounds": 0,
                    "seat_count": 4,
                    "shared_host_concurrency": 2,
                    "wall_time_seconds": 21600,
                }
            ),
            self.value(
                resource_maxima={
                    "review_rounds": 4,
                    "seat_count": True,
                    "shared_host_concurrency": 2,
                    "wall_time_seconds": 21600,
                }
            ),
            self.value(
                resource_maxima={
                    "review_rounds": 4,
                    "seat_count": 4,
                    "shared_host_concurrency": 0,
                    "wall_time_seconds": 21600,
                }
            ),
            self.value(
                resource_maxima={
                    "review_rounds": 4,
                    "seat_count": 4,
                    "shared_host_concurrency": 2,
                    "wall_time_seconds": 0,
                }
            ),
            self.value(wake_mode="unattended"),
            self.value(required_capabilities=[]),
            self.value(
                required_capabilities=[
                    {"mode": "read", "name": "change.open", "version": 1}
                ]
            ),
            self.value(
                required_capabilities=[
                    {"mode": "effect", "name": "change.open", "version": 1},
                    {"mode": "effect", "name": "change.open", "version": 1},
                ]
            ),
            self.value(landing_methods=[]),
            self.value(landing_methods=["octopus"]),
            self.value(allow_deferred_validation=1),
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                WorkflowPolicySpec.from_mapping(value)  # type: ignore[arg-type]


class WorkflowActivationSpecTests(unittest.TestCase):
    @staticmethod
    def value(**overrides: object) -> dict[str, object]:
        return {
            "schema_version": 1,
            "policy_sha256": "a" * 64,
            "workflow_configuration_version_id": "wfv_" + "1" * 32,
            "validation_bindings": [
                {"obligation": "coverage", "validation": "coverage"},
                {"obligation": "mutation", "validation": "mutation"},
                {"obligation": "tests", "validation": "unit"},
            ],
            "capability_bindings": [
                {
                    "binding_id": "cpb_" + "2" * 32,
                    "mode": "effect",
                    "name": "change.open",
                    "version": 1,
                },
                {
                    "binding_id": "cpb_" + "3" * 32,
                    "mode": "effect",
                    "name": "landing.invoke",
                    "version": 1,
                },
                {
                    "binding_id": "cpb_" + "4" * 32,
                    "mode": "effect",
                    "name": "refs.publish",
                    "version": 1,
                },
            ],
            "resources": {
                "review_rounds": 3,
                "seat_count": 2,
                "shared_host_concurrency": 1,
                "wall_time_seconds": 10800,
            },
            "wake_bindings": [
                {"role": "producer", "wake_class": "human_turn_only"},
                {"role": "reviewer", "wake_class": "human_turn_only"},
            ],
            "merge_method": "squash",
            "retired_rules": ["legacy-merge-check"],
            **overrides,
        }

    def test_activation_normalizes_only_declared_local_bindings(self) -> None:
        spec = WorkflowActivationSpec.from_mapping(self.value())
        reordered = self.value(
            validation_bindings=list(reversed(self.value()["validation_bindings"])),
            capability_bindings=list(reversed(self.value()["capability_bindings"])),
            wake_bindings=list(reversed(self.value()["wake_bindings"])),
        )

        self.assertEqual(spec, WorkflowActivationSpec.from_mapping(reordered))
        self.assertEqual(spec.to_dict(), self.value())
        self.assertEqual(len(spec.digest), 64)

    def test_activation_rejects_implicit_or_unbounded_bindings(self) -> None:
        missing = self.value()
        missing.pop("policy_sha256")
        duplicate_validation = list(self.value()["validation_bindings"])
        duplicate_validation.append(copy.deepcopy(duplicate_validation[0]))
        duplicate_capability = list(self.value()["capability_bindings"])
        duplicate_capability.append(copy.deepcopy(duplicate_capability[0]))
        cases = (
            None,
            missing,
            {**self.value(), "command": ["gh", "pr", "merge"]},
            self.value(schema_version=True),
            self.value(policy_sha256="A" * 64),
            self.value(workflow_configuration_version_id="default"),
            self.value(validation_bindings=[]),
            self.value(validation_bindings=duplicate_validation),
            self.value(
                validation_bindings=[
                    {"obligation": "tests", "validation": "Bad Validation"}
                ]
            ),
            self.value(capability_bindings=[]),
            self.value(capability_bindings=duplicate_capability),
            self.value(
                capability_bindings=[
                    {
                        "binding_id": "cpb_" + "2" * 32,
                        "mode": "read",
                        "name": "change.open",
                        "version": 1,
                    }
                ]
            ),
            self.value(
                resources={
                    "review_rounds": 0,
                    "seat_count": 2,
                    "shared_host_concurrency": 1,
                    "wall_time_seconds": 10800,
                }
            ),
            self.value(wake_bindings=[]),
            self.value(
                wake_bindings=[{"role": "producer", "wake_class": "human_turn_only"}]
            ),
            self.value(
                wake_bindings=[{"role": "producer", "wake_class": "self_polling"}]
            ),
            self.value(merge_method="octopus"),
            self.value(retired_rules=["same", "same"]),
            self.value(retired_rules=[" "]),
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                WorkflowActivationSpec.from_mapping(value)  # type: ignore[arg-type]


class WorkflowPolicyPublishedSchemaTests(unittest.TestCase):
    repository = Path(__file__).resolve().parents[1]

    def test_examples_match_published_and_runtime_contracts(self) -> None:
        cases = (
            ("workflow-policy", WorkflowPolicySpec.from_mapping),
            ("workflow-activation", WorkflowActivationSpec.from_mapping),
        )
        for name, factory in cases:
            with self.subTest(name=name):
                schema = json.loads(
                    (self.repository / "schemas" / f"{name}-v1.schema.json").read_text()
                )
                example = json.loads(
                    (self.repository / "examples" / f"{name}.json").read_text()
                )
                Draft202012Validator.check_schema(schema)
                validator = Draft202012Validator(schema)
                validator.validate(example)
                parsed = factory(example)
                self.assertEqual(parsed.to_dict(), example)
                validator.validate(parsed.to_dict())

                extra = {**example, "provider": "github"}
                with self.assertRaises(SchemaValidationError):
                    validator.validate(extra)
                with self.assertRaises(ValidationError):
                    factory(extra)

    def test_runtime_projection_examples_match_published_contracts(self) -> None:
        for name in ("workflow-manual-wake", "workflow-responsibility"):
            with self.subTest(name=name):
                schema = json.loads(
                    (self.repository / "schemas" / f"{name}-v1.schema.json").read_text()
                )
                example = json.loads(
                    (self.repository / "examples" / f"{name}.json").read_text()
                )
                Draft202012Validator.check_schema(schema)
                Draft202012Validator(schema).validate(example)

                with self.assertRaises(SchemaValidationError):
                    Draft202012Validator(schema).validate(
                        {**example, "provider": "github"}
                    )


class WorkflowResponsibilityProjectionTests(unittest.TestCase):
    @staticmethod
    def project(
        *,
        stage: str = "build",
        execution_state: str = "active",
        activated: bool = True,
        ruling: bool = False,
        run_state: str | None = None,
        wake_state: str | None = None,
        landing_state: str | None = None,
        provider_target: bool = False,
    ) -> dict[str, object]:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        try:
            connection.executescript(
                """
                CREATE TABLE workflow_executions (
                    id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    correction_round INTEGER NOT NULL,
                    version INTEGER NOT NULL,
                    workflow_activation_version_id TEXT
                );
                CREATE TABLE workflow_rulings (
                    id TEXT PRIMARY KEY,
                    execution_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE runs (
                    id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL
                );
                CREATE TABLE workflow_run_bindings (
                    execution_id TEXT NOT NULL,
                    correction_round INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    generation INTEGER NOT NULL
                );
                CREATE TABLE workflow_manual_wakes (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL
                );
                CREATE TABLE integration_queue_items (
                    id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL
                );
                CREATE TABLE workflow_landing_bindings (
                    execution_id TEXT NOT NULL,
                    queue_item_id TEXT NOT NULL
                );
                CREATE TABLE workflow_provider_targets (
                    id TEXT PRIMARY KEY,
                    execution_id TEXT NOT NULL
                );
                """
            )
            connection.execute(
                "INSERT INTO workflow_executions VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "wfx_1",
                    execution_state,
                    stage,
                    2,
                    7,
                    "wav_1" if activated else None,
                ),
            )
            if ruling:
                connection.execute(
                    "INSERT INTO workflow_rulings VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        "wfr_1",
                        "wfx_1",
                        "open",
                        3,
                        "resource_exhausted",
                        "2026-11-02T12:00:00+00:00",
                    ),
                )
            if run_state is not None:
                role = "reviewer" if stage == "review" else "producer"
                connection.execute(
                    "INSERT INTO runs VALUES (?, ?, ?)", ("run_1", run_state, 5)
                )
                connection.execute(
                    "INSERT INTO workflow_run_bindings VALUES (?, ?, ?, ?, ?)",
                    ("wfx_1", 2, role, "run_1", 4),
                )
                if wake_state is not None:
                    connection.execute(
                        "INSERT INTO workflow_manual_wakes VALUES (?, ?, ?, ?)",
                        ("wwk_1", "run_1", wake_state, 6),
                    )
            if landing_state is not None:
                connection.execute(
                    "INSERT INTO integration_queue_items VALUES (?, ?, ?)",
                    ("inq_1", landing_state, 8),
                )
                connection.execute(
                    "INSERT INTO workflow_landing_bindings VALUES (?, ?)",
                    ("wfx_1", "inq_1"),
                )
                if provider_target:
                    connection.execute(
                        "INSERT INTO workflow_provider_targets VALUES (?, ?)",
                        ("wpt_1", "wfx_1"),
                    )
            return Database._workflow_responsibility(connection, "wfx_1")
        finally:
            connection.close()

    @staticmethod
    def expected(
        stage: str,
        status: str,
        holder: str,
        reason: str,
        basis: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return {
            "status": status,
            "holder": holder,
            "reason": reason,
            "execution_version": 7,
            "stage": stage,
            "correction_round": 2,
            "basis": {} if basis is None else basis,
        }

    def test_projection_covers_every_durable_responsibility_state(self) -> None:
        run_basis = {
            "run_id": "run_1",
            "run_version": 5,
            "run_generation": 4,
        }
        wake_basis = {
            "run_id": "run_1",
            "wake_id": "wwk_1",
            "wake_version": 6,
        }
        landing_basis = {"queue_item_id": "inq_1", "queue_version": 8}
        cases = (
            (
                "pre-policy",
                {"activated": False},
                {"status": "pre_policy"},
            ),
            (
                "terminal",
                {"execution_state": "cancelled"},
                self.expected(
                    "build",
                    "terminal",
                    "none",
                    "execution_terminal",
                    {"execution_state": "cancelled"},
                ),
            ),
            (
                "open ruling",
                {"ruling": True},
                self.expected(
                    "build",
                    "action_required",
                    "owner",
                    "ruling_resolution",
                    {
                        "ruling_id": "wfr_1",
                        "ruling_kind": "resource_exhausted",
                        "ruling_version": 3,
                    },
                ),
            ),
            (
                "build admission",
                {},
                self.expected(
                    "build", "action_required", "control_plane", "role_admission"
                ),
            ),
            (
                "review admission",
                {"stage": "review"},
                self.expected(
                    "review", "action_required", "control_plane", "role_admission"
                ),
            ),
            (
                "wake missing",
                {"run_state": "queued"},
                self.expected(
                    "build",
                    "attention",
                    "control_room",
                    "manual_wake_missing",
                    run_basis,
                ),
            ),
            (
                "wake acknowledgement",
                {"run_state": "queued", "wake_state": "requested"},
                self.expected(
                    "build",
                    "action_required",
                    "owner",
                    "manual_wake_acknowledgement",
                    wake_basis,
                ),
            ),
            (
                "run launch",
                {"run_state": "queued", "wake_state": "acknowledged"},
                self.expected(
                    "build",
                    "action_required",
                    "control_plane",
                    "run_launch",
                    wake_basis,
                ),
            ),
            (
                "producer preparing",
                {"run_state": "preparing", "wake_state": "acknowledged"},
                self.expected(
                    "build", "working", "producer", "run_execution", run_basis
                ),
            ),
            (
                "reviewer running",
                {
                    "stage": "review",
                    "run_state": "running",
                    "wake_state": "acknowledged",
                },
                self.expected(
                    "review", "working", "reviewer", "run_execution", run_basis
                ),
            ),
            (
                "interaction",
                {"run_state": "waiting_for_input", "wake_state": "acknowledged"},
                self.expected(
                    "build",
                    "action_required",
                    "owner",
                    "run_interaction",
                    run_basis,
                ),
            ),
            (
                "resume",
                {"run_state": "paused", "wake_state": "acknowledged"},
                self.expected(
                    "build",
                    "action_required",
                    "control_plane",
                    "run_resume",
                    run_basis,
                ),
            ),
            *(
                (
                    f"terminal run {state}",
                    {"run_state": state, "wake_state": "acknowledged"},
                    self.expected(
                        "build",
                        "action_required",
                        "control_plane",
                        "stage_transition",
                        run_basis,
                    ),
                )
                for state in ("succeeded", "failed", "cancelled")
            ),
            (
                "unknown run state",
                {"run_state": "mystery", "wake_state": "acknowledged"},
                self.expected(
                    "build",
                    "attention",
                    "control_room",
                    "run_state_unresolved",
                    run_basis,
                ),
            ),
            (
                "maintainer disposition",
                {"stage": "maintainer"},
                self.expected(
                    "maintainer",
                    "action_required",
                    "owner",
                    "maintainer_disposition",
                ),
            ),
            (
                "landing admission",
                {"stage": "landing"},
                self.expected(
                    "landing",
                    "action_required",
                    "control_plane",
                    "landing_admission",
                ),
            ),
            (
                "provider target",
                {"stage": "landing", "landing_state": "queued"},
                self.expected(
                    "landing",
                    "action_required",
                    "owner",
                    "provider_target",
                    landing_basis,
                ),
            ),
            *(
                (
                    f"landing dispatch {state}",
                    {
                        "stage": "landing",
                        "landing_state": state,
                        "provider_target": True,
                    },
                    self.expected(
                        "landing",
                        "action_required",
                        "control_plane",
                        "landing_dispatch",
                        landing_basis,
                    ),
                )
                for state in ("queued", "claimed")
            ),
            (
                "provider receipt",
                {
                    "stage": "landing",
                    "landing_state": "awaiting_receipt",
                    "provider_target": True,
                },
                self.expected(
                    "landing",
                    "waiting",
                    "landing_actuator",
                    "provider_receipt",
                    landing_basis,
                ),
            ),
            (
                "landed",
                {
                    "stage": "landing",
                    "landing_state": "landed",
                    "provider_target": True,
                },
                self.expected(
                    "landing",
                    "action_required",
                    "control_plane",
                    "stage_transition",
                    landing_basis,
                ),
            ),
            (
                "refold required",
                {
                    "stage": "landing",
                    "landing_state": "refold_required",
                    "provider_target": True,
                },
                self.expected(
                    "landing",
                    "action_required",
                    "control_plane",
                    "landing_refold_resolution",
                    landing_basis,
                ),
            ),
            (
                "unknown landing state",
                {
                    "stage": "landing",
                    "landing_state": "mystery",
                    "provider_target": True,
                },
                self.expected(
                    "landing",
                    "attention",
                    "control_room",
                    "landing_state_unresolved",
                    {**landing_basis, "queue_state": "mystery"},
                ),
            ),
            (
                "completion",
                {"stage": "completion"},
                self.expected(
                    "completion",
                    "action_required",
                    "control_plane",
                    "workflow_completion",
                ),
            ),
            (
                "unknown stage",
                {"stage": "mystery"},
                self.expected(
                    "mystery",
                    "attention",
                    "control_room",
                    "workflow_stage_unresolved",
                ),
            ),
        )

        for name, inputs, expected in cases:
            with self.subTest(name=name):
                self.assertEqual(self.project(**inputs), expected)


class WorkflowPolicyStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("owner", "Project Owner")
        self.instance = self.database.create_krab_instance(
            "local", "http://127.0.0.1:8765"
        )
        self.project = self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id="prj_" + "1" * 32,
            krab_project_key="policy-project",
            display_name="Policy Project",
            context=ProjectContext.from_mapping({"purpose": "Prove policy reuse"}),
            created_by=self.user["id"],
        )
        producer_identity = self.create_identity("producer", "2")
        reviewer_identity = self.create_identity("reviewer", "3")
        self.create_profile(
            "Producer", producer_identity["id"], workspace_mode="copy_on_write"
        )
        self.create_profile(
            "Reviewer", reviewer_identity["id"], workspace_mode="read_only"
        )
        self.configuration = self.database.create_workflow_configuration(
            project_key="policy-project",
            name="default",
            spec=WorkflowConfigurationSpec.from_mapping(
                {
                    "schema_version": 1,
                    "review_separation": "different_profile_and_identity",
                    "maintainer_approval": True,
                    "validations": [
                        {
                            "name": "coverage",
                            "image_id": "sha256:" + "4" * 64,
                            "command": ["make", "coverage"],
                        },
                        {
                            "name": "mutation",
                            "image_id": "sha256:" + "4" * 64,
                            "command": ["make", "mutation"],
                        },
                        {
                            "name": "unit",
                            "image_id": "sha256:" + "4" * 64,
                            "command": ["make", "test"],
                        },
                    ],
                    "max_correction_rounds": 3,
                    "landing_mode": "provider",
                }
            ),
            producer_profile_name="Producer",
            reviewer_profile_name="Reviewer",
            created_by=self.user["id"],
        )
        connector = sidecar_spec(
            self.root / "forge.sock",
            capabilities=[
                {"mode": "effect", "name": "change.open", "version": 1},
                {"mode": "effect", "name": "landing.invoke", "version": 1},
                {"mode": "effect", "name": "refs.publish", "version": 1},
            ],
        )
        self.database.register_service_sidecar(
            name="forge",
            spec=connector,
            created_by=self.user["id"],
        )
        self.bindings = {
            name: self.database.bind_project_capability(
                project_key="policy-project",
                capability_name=name,
                capability_version=1,
                sidecar_name="forge",
                mapping_name="primary",
                bound_by=self.user["id"],
            )
            for name in ("change.open", "landing.invoke", "refs.publish")
        }

    def create_identity(self, handle: str, fill: str) -> dict[str, object]:
        return self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity(user_id="usr_" + fill * 32, handle=handle, token_version=1),
            f"file:/private/{handle}",
        )

    def create_profile(
        self, name: str, identity_id: str, *, workspace_mode: str
    ) -> dict[str, object]:
        return self.database.create_agent_profile(
            name=name,
            spec=AgentProfileSpec.create(
                image="agent@sha256:abc",
                command=["agent", "run"],
                workspace_mode=workspace_mode,
                network_mode="none",
            ),
            krab_identity_id=identity_id,
            created_by=self.user["id"],
        )

    @staticmethod
    def policy_spec(**overrides: object) -> WorkflowPolicySpec:
        return WorkflowPolicySpec.from_mapping(
            WorkflowPolicySpecTests.value(**overrides)
        )

    def activation_spec(
        self,
        policy: dict[str, object],
        **overrides: object,
    ) -> WorkflowActivationSpec:
        capability_bindings = [
            {
                "binding_id": self.bindings[name]["id"],
                "mode": "effect",
                "name": name,
                "version": 1,
            }
            for name in ("change.open", "landing.invoke", "refs.publish")
        ]
        return WorkflowActivationSpec.from_mapping(
            {
                "schema_version": 1,
                "policy_sha256": policy["content_sha256"],
                "workflow_configuration_version_id": self.configuration[
                    "workflow_configuration_version_id"
                ],
                "validation_bindings": [
                    {"obligation": "coverage", "validation": "coverage"},
                    {"obligation": "mutation", "validation": "mutation"},
                    {"obligation": "tests", "validation": "unit"},
                ],
                "capability_bindings": capability_bindings,
                "resources": {
                    "review_rounds": 3,
                    "seat_count": 2,
                    "shared_host_concurrency": 1,
                    "wall_time_seconds": 10800,
                },
                "wake_bindings": [
                    {"role": "producer", "wake_class": "human_turn_only"},
                    {"role": "reviewer", "wake_class": "human_turn_only"},
                ],
                "merge_method": "squash",
                "retired_rules": ["legacy-merge-check"],
                **overrides,
            }
        )

    def create_policy(self) -> dict[str, object]:
        return self.database.create_workflow_policy(
            name="standard-delivery",
            spec=self.policy_spec(),
            created_by=self.user["id"],
        )

    def create_work_item(self, external_id: str) -> dict[str, object]:
        return self.database.import_work_item(
            project_key="policy-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": external_id,
                    "source_updated_at": "2026-11-02T12:00:00Z",
                    "title": "Exercise one activated workflow",
                    "brief": "Prove the exact portable policy pin.",
                }
            ),
            imported_by=self.user["id"],
        )

    def start_activated(
        self,
        work_item: dict[str, object],
        *,
        expected_activation_version: int,
        idempotency_key: str,
    ) -> dict[str, object]:
        return self.database.start_workflow_execution(
            project_key="policy-project",
            configuration_name="default",
            work_item_id=work_item["id"],
            source_revision="a" * 40,
            expected_configuration_version=1,
            expected_context_version_id=self.project["context_version_id"],
            expected_work_item_version_id=work_item["work_item_version_id"],
            expected_thread_generation=1,
            expected_thread_version=1,
            expected_activation_version=expected_activation_version,
            idempotency_key=idempotency_key,
            requested_by=self.user["id"],
        )

    def activate(self, **overrides: object) -> dict[str, object]:
        policy = self.create_policy()
        return self.database.create_workflow_activation(
            project_key="policy-project",
            policy_name="standard-delivery",
            expected_policy_version=1,
            spec=self.activation_spec(policy, **overrides),
            acknowledged_by=self.user["id"],
        )

    def create_producer_run(
        self,
        execution: dict[str, object],
        work_item: dict[str, object],
    ) -> dict[str, object]:
        return self.database.create_run(
            project_key="policy-project",
            profile_name="Producer",
            work_item_id=work_item["id"],
            requested_by=self.user["id"],
            source_revision="a" * 40,
            workflow_execution_id=execution["id"],
            workflow_round=0,
            workflow_role="producer",
        )

    def test_policy_and_activation_pin_one_portable_digest_and_local_bindings(
        self,
    ) -> None:
        policy = self.create_policy()
        activation = self.database.create_workflow_activation(
            project_key="policy-project",
            policy_name="standard-delivery",
            expected_policy_version=1,
            spec=self.activation_spec(policy),
            acknowledged_by=self.user["id"],
        )

        self.assertRegex(policy["id"], r"^wfp_[0-9a-f]{32}$")
        self.assertEqual(policy["name"], "standard-delivery")
        self.assertEqual(policy["version"], 1)
        self.assertEqual(policy["spec"], self.policy_spec().to_dict())
        self.assertEqual(policy["content_sha256"], self.policy_spec().digest)
        self.assertEqual(policy["created_by"], "owner")
        self.assertRegex(activation["id"], r"^wpa_[0-9a-f]{32}$")
        self.assertRegex(
            activation["workflow_activation_version_id"],
            r"^wav_[0-9a-f]{32}$",
        )
        self.assertEqual(activation["project_key"], "policy-project")
        self.assertEqual(activation["version"], 1)
        self.assertEqual(activation["policy"]["id"], policy["id"])
        self.assertEqual(
            activation["policy"]["content_sha256"], policy["content_sha256"]
        )
        self.assertEqual(
            activation["workflow_configuration_version_id"],
            self.configuration["workflow_configuration_version_id"],
        )
        self.assertEqual(activation["spec"], self.activation_spec(policy).to_dict())
        self.assertEqual(activation["acknowledged_by"], "owner")
        self.assertEqual(self.database.get_workflow_policy("standard-delivery"), policy)
        self.assertEqual(self.database.list_workflow_policies(), [policy])
        self.assertEqual(
            self.database.get_workflow_activation("policy-project"), activation
        )
        self.assertEqual(self.database.list_workflow_activations(), [activation])
        counts = self.database.status()["counts"]
        self.assertEqual(counts["workflow_policies"], 1)
        self.assertEqual(counts["workflow_activations"], 1)
        self.assertEqual(counts["workflow_activation_versions"], 1)
        self.assertEqual(counts["workflow_activation_capability_bindings"], 3)

        restarted = Database(self.database.path)
        restarted.initialize()
        self.assertEqual(
            restarted.get_workflow_activation("policy-project"), activation
        )

    def test_policy_and_activation_revisions_preserve_exact_history(self) -> None:
        first_policy = self.create_policy()
        first_activation = self.database.create_workflow_activation(
            project_key="policy-project",
            policy_name="standard-delivery",
            expected_policy_version=1,
            spec=self.activation_spec(first_policy),
            acknowledged_by=self.user["id"],
        )
        second_policy = self.database.revise_workflow_policy(
            name="standard-delivery",
            spec=self.policy_spec(
                resource_maxima={
                    "review_rounds": 5,
                    "seat_count": 4,
                    "shared_host_concurrency": 2,
                    "wall_time_seconds": 21600,
                }
            ),
            created_by=self.user["id"],
        )
        second_activation = self.database.revise_workflow_activation(
            project_key="policy-project",
            policy_name="standard-delivery",
            expected_policy_version=2,
            spec=self.activation_spec(second_policy, retired_rules=[]),
            acknowledged_by=self.user["id"],
        )

        self.assertEqual(second_policy["version"], 2)
        self.assertEqual(second_activation["version"], 2)
        self.assertEqual(
            self.database.get_workflow_policy("standard-delivery", version=1),
            first_policy,
        )
        self.assertEqual(
            self.database.get_workflow_activation("policy-project", version=1),
            first_activation,
        )
        with self.assertRaisesRegex(ConflictError, "identical"):
            self.database.revise_workflow_policy(
                name="standard-delivery",
                spec=self.policy_spec(
                    resource_maxima={
                        "review_rounds": 5,
                        "seat_count": 4,
                        "shared_host_concurrency": 2,
                        "wall_time_seconds": 21600,
                    }
                ),
                created_by=self.user["id"],
            )
        with self.assertRaisesRegex(ConflictError, "identical"):
            self.database.revise_workflow_activation(
                project_key="policy-project",
                policy_name="standard-delivery",
                expected_policy_version=2,
                spec=self.activation_spec(second_policy, retired_rules=[]),
                acknowledged_by=self.user["id"],
            )

    def test_activation_fails_closed_on_missing_or_mismatched_binding(self) -> None:
        policy = self.create_policy()
        valid = self.activation_spec(policy).to_dict()
        cases = []
        mismatched_digest = copy.deepcopy(valid)
        mismatched_digest["policy_sha256"] = "b" * 64
        cases.append(mismatched_digest)
        missing_capability = copy.deepcopy(valid)
        missing_capability["capability_bindings"].pop()
        cases.append(missing_capability)
        missing_validation = copy.deepcopy(valid)
        missing_validation["validation_bindings"].pop()
        cases.append(missing_validation)
        unknown_validation = copy.deepcopy(valid)
        unknown_validation["validation_bindings"][0]["validation"] = "unknown"
        cases.append(unknown_validation)
        over_budget = copy.deepcopy(valid)
        over_budget["resources"]["wall_time_seconds"] = 30000
        cases.append(over_budget)
        wrong_rounds = copy.deepcopy(valid)
        wrong_rounds["resources"]["review_rounds"] = 2
        cases.append(wrong_rounds)
        wrong_method = copy.deepcopy(valid)
        wrong_method["merge_method"] = "merge"
        cases.append(wrong_method)
        nonexistent_binding = copy.deepcopy(valid)
        nonexistent_binding["capability_bindings"][0]["binding_id"] = "cpb_" + "f" * 32
        cases.append(nonexistent_binding)

        for index, value in enumerate(cases):
            with (
                self.subTest(index=index),
                self.assertRaises((ConflictError, NotFoundError)),
            ):
                self.database.create_workflow_activation(
                    project_key="policy-project",
                    policy_name="standard-delivery",
                    expected_policy_version=1,
                    spec=WorkflowActivationSpec.from_mapping(value),
                    acknowledged_by=self.user["id"],
                )

        self.assertEqual(self.database.list_workflow_activations(), [])

    def test_policy_and_activation_lifecycle_boundaries_fail_explicitly(self) -> None:
        spec = self.policy_spec()
        with self.assertRaisesRegex(ValidationError, "WorkflowPolicySpec"):
            self.database.create_workflow_policy(
                name="invalid", spec=spec.to_dict(), created_by=self.user["id"]
            )
        with self.assertRaisesRegex(NotFoundError, "workflow policy not found"):
            self.database.revise_workflow_policy(
                name="missing", spec=spec, created_by=self.user["id"]
            )
        with self.assertRaisesRegex(NotFoundError, "workflow policy not found"):
            self.database.get_workflow_policy("missing")

        policy = self.create_policy()
        with self.assertRaisesRegex(ConflictError, "already exists"):
            self.database.create_workflow_policy(
                name="standard-delivery", spec=spec, created_by=self.user["id"]
            )
        with self.assertRaisesRegex(ValidationError, "positive integer"):
            self.database.get_workflow_policy("standard-delivery", version=0)
        with self.assertRaisesRegex(ValidationError, "WorkflowActivationSpec"):
            self.database.create_workflow_activation(
                project_key="policy-project",
                policy_name="standard-delivery",
                expected_policy_version=1,
                spec=self.activation_spec(policy).to_dict(),
                acknowledged_by=self.user["id"],
            )
        with self.assertRaisesRegex(NotFoundError, "project not found"):
            self.database.create_workflow_activation(
                project_key="missing-project",
                policy_name="standard-delivery",
                expected_policy_version=1,
                spec=self.activation_spec(policy),
                acknowledged_by=self.user["id"],
            )
        with self.assertRaisesRegex(ConflictError, "policy version changed"):
            self.database.create_workflow_activation(
                project_key="policy-project",
                policy_name="standard-delivery",
                expected_policy_version=2,
                spec=self.activation_spec(policy),
                acknowledged_by=self.user["id"],
            )
        with self.assertRaisesRegex(NotFoundError, "activation not found"):
            self.database.revise_workflow_activation(
                project_key="policy-project",
                policy_name="standard-delivery",
                expected_policy_version=1,
                spec=self.activation_spec(policy),
                acknowledged_by=self.user["id"],
            )

        self.database.create_workflow_activation(
            project_key="policy-project",
            policy_name="standard-delivery",
            expected_policy_version=1,
            spec=self.activation_spec(policy),
            acknowledged_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "activation already exists"):
            self.database.create_workflow_activation(
                project_key="policy-project",
                policy_name="standard-delivery",
                expected_policy_version=1,
                spec=self.activation_spec(policy),
                acknowledged_by=self.user["id"],
            )
        with self.assertRaisesRegex(ValidationError, "positive integer"):
            self.database.get_workflow_activation("policy-project", version=True)
        with self.assertRaisesRegex(NotFoundError, "activation not found"):
            self.database.get_workflow_activation("missing-project")

    def test_activation_rejects_an_unavailable_controller_or_project(self) -> None:
        policy = self.create_policy()
        self.database.revoke_service_sidecar(
            "forge", expected_version=1, revoked_by=self.user["id"]
        )
        with self.assertRaisesRegex(ConflictError, "controller is unavailable"):
            self.database.create_workflow_activation(
                project_key="policy-project",
                policy_name="standard-delivery",
                expected_policy_version=1,
                spec=self.activation_spec(policy),
                acknowledged_by=self.user["id"],
            )

        with self.database.connect() as connection:
            connection.execute(
                "UPDATE projects SET active = 0 WHERE id = ?", (self.project["id"],)
            )
        with self.assertRaisesRegex(ConflictError, "project is inactive"):
            self.database.create_workflow_activation(
                project_key="policy-project",
                policy_name="standard-delivery",
                expected_policy_version=1,
                spec=self.activation_spec(policy),
                acknowledged_by=self.user["id"],
            )

    def test_schema_migrates_v45_without_backfilling_old_workflows(self) -> None:
        path = self.root / "v45.db"
        with sqlite3.connect(path) as connection:
            Database._execute_statements(connection, SCHEMA_V45)
            connection.execute("PRAGMA user_version = 45")
            connection.commit()
        path.chmod(0o600)
        migrated = Database(path)
        status = migrated.initialize()

        self.assertEqual(status["schema_version"], SCHEMA_VERSION)
        self.assertEqual(status["counts"]["workflow_policies"], 0)
        self.assertEqual(status["counts"]["workflow_activations"], 0)

        with migrated.connect() as connection:
            columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(workflow_executions)"
                ).fetchall()
            }
        self.assertIn("workflow_activation_version_id", columns)
        self.assertEqual(
            migrated.list_workflow_executions(project_key="policy-project"), []
        )

    def test_schema_migrates_v46_without_synthesizing_manual_wakes(self) -> None:
        path = self.root / "v46.db"
        with sqlite3.connect(path) as connection:
            Database._execute_statements(connection, SCHEMA_V46)
            connection.execute("PRAGMA user_version = 46")
            connection.commit()
        path.chmod(0o600)
        migrated = Database(path)

        status = migrated.initialize()

        self.assertEqual(status["schema_version"], SCHEMA_VERSION)
        self.assertEqual(status["counts"]["workflow_manual_wakes"], 0)
        self.assertEqual(status["counts"]["workflow_manual_wake_events"], 0)
        self.assertEqual(migrated.list_workflow_manual_wakes(), [])

    def test_attempt_admission_bypasses_only_pre_workflow_schema_versions(
        self,
    ) -> None:
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("PRAGMA user_version = 28")
        Database._enforce_workflow_attempt_admission(
            connection,
            run_id="run_legacy",
            project_id="project_legacy",
        )

        connection.execute("PRAGMA user_version = 29")
        with self.assertRaises(sqlite3.OperationalError):
            Database._enforce_workflow_attempt_admission(
                connection,
                run_id="run_current",
                project_id="project_current",
            )

    def test_control_plane_exposes_owner_acknowledged_contracts(self) -> None:
        control = ControlPlane(self.database)
        policy = control.create_workflow_policy(
            name="standard-delivery",
            spec=self.policy_spec(),
            created_by_handle="owner",
        )
        activation = control.create_workflow_activation(
            project_key="policy-project",
            policy_name="standard-delivery",
            expected_policy_version=1,
            spec=self.activation_spec(policy),
            acknowledged_by_handle="owner",
        )

        self.assertEqual(control.get_workflow_policy(name="standard-delivery"), policy)
        self.assertEqual(control.list_workflow_policies(), [policy])
        self.assertEqual(
            control.get_workflow_activation(project_key="policy-project"),
            activation,
        )
        self.assertEqual(
            control.list_workflow_activations(project_key="policy-project"),
            [activation],
        )

    def test_execution_pins_exact_activation_and_rechecks_availability(self) -> None:
        policy = self.create_policy()
        activation = self.database.create_workflow_activation(
            project_key="policy-project",
            policy_name="standard-delivery",
            expected_policy_version=1,
            spec=self.activation_spec(policy),
            acknowledged_by=self.user["id"],
        )
        work_item = self.create_work_item("activated-1")

        with self.assertRaisesRegex(ConflictError, "activation version changed"):
            self.start_activated(
                work_item,
                expected_activation_version=2,
                idempotency_key="wrong-activation",
            )

        execution = self.start_activated(
            work_item,
            expected_activation_version=1,
            idempotency_key="activated-workflow",
        )
        self.assertEqual(
            execution["portable_policy"],
            {
                "status": "activated",
                "activation_id": activation["id"],
                "activation_version_id": activation["workflow_activation_version_id"],
                "activation_version": 1,
                "activation_sha256": activation["content_sha256"],
                "policy_id": policy["id"],
                "policy_name": "standard-delivery",
                "policy_version": 1,
                "policy_sha256": policy["content_sha256"],
                "resources": activation["spec"]["resources"],
                "wake_bindings": activation["spec"]["wake_bindings"],
                "merge_method": "squash",
            },
        )
        self.assertEqual(
            execution["workflow_activation_version_id"],
            activation["workflow_activation_version_id"],
        )
        self.assertEqual(
            self.database.get_workflow_execution(execution["id"]), execution
        )

        unavailable_item = self.create_work_item("activated-2")
        binding = self.bindings["landing.invoke"]
        self.database.unbind_project_capability(
            binding["id"],
            expected_version=binding["version"],
            unbound_by=self.user["id"],
        )
        with self.assertRaisesRegex(ConflictError, "capability is unavailable"):
            self.start_activated(
                unavailable_item,
                expected_activation_version=1,
                idempotency_key="unavailable-activation",
            )

    def test_activated_role_requires_exact_manual_wake_acknowledgement(self) -> None:
        activation = self.activate()
        work_item = self.create_work_item("manual-wake")
        execution = self.start_activated(
            work_item,
            expected_activation_version=1,
            idempotency_key="manual-wake",
        )
        self.assertEqual(
            execution["responsibility"],
            {
                "status": "action_required",
                "holder": "control_plane",
                "reason": "role_admission",
                "execution_version": 1,
                "stage": "build",
                "correction_round": 0,
                "basis": {},
            },
        )

        run = self.create_producer_run(execution, work_item)
        wakes = self.database.list_workflow_manual_wakes(execution_id=execution["id"])
        self.assertEqual(len(wakes), 1)
        wake = wakes[0]
        self.assertRegex(wake["id"], r"^wwk_[0-9a-f]{32}$")
        self.assertEqual(wake["run_id"], run["id"])
        self.assertEqual(wake["execution_id"], execution["id"])
        self.assertEqual(
            wake["workflow_activation_version_id"],
            activation["workflow_activation_version_id"],
        )
        self.assertEqual(wake["execution_version"], 1)
        self.assertEqual(wake["correction_round"], 0)
        self.assertEqual(wake["role"], "producer")
        self.assertEqual(wake["wake_class"], "human_turn_only")
        self.assertEqual(wake["state"], "requested")
        self.assertEqual(wake["version"], 1)
        self.assertEqual(wake["recipient"], "owner")
        self.assertIsNone(wake["acknowledged_by"])
        self.assertEqual(
            [
                {
                    key: event[key]
                    for key in ("sequence", "kind", "from_state", "to_state", "actor")
                }
                for event in wake["events"]
            ],
            [
                {
                    "sequence": 1,
                    "kind": "wake.requested",
                    "from_state": None,
                    "to_state": "requested",
                    "actor": None,
                }
            ],
        )
        self.assertEqual(
            self.database.get_workflow_execution(execution["id"])["responsibility"],
            {
                "status": "action_required",
                "holder": "owner",
                "reason": "manual_wake_acknowledgement",
                "execution_version": 1,
                "stage": "build",
                "correction_round": 0,
                "basis": {
                    "run_id": run["id"],
                    "wake_id": wake["id"],
                    "wake_version": 1,
                },
            },
        )
        with self.assertRaisesRegex(ConflictError, "manual wake"):
            self.database.begin_run_attempt(
                run["id"],
                runtime_adapter="docker",
                owner_id="worker-one",
                expected_run_version=1,
            )
        snapshot = ControlRoom(self.database).snapshot()
        projected = next(
            item for item in snapshot["workflows"] if item["id"] == execution["id"]
        )
        self.assertEqual(
            projected["responsibility"]["reason"],
            "manual_wake_acknowledgement",
        )
        wake_attention = next(
            item
            for item in snapshot["attention"]
            if item["kind"] == "workflow.manual_wake_requested"
        )
        self.assertEqual(wake_attention["subject_id"], wake["id"])

        other = self.database.create_user("other", "Other Operator")
        with self.assertRaisesRegex(ConflictError, "recipient"):
            self.database.acknowledge_workflow_manual_wake(
                wake["id"],
                expected_version=1,
                acknowledged_by=other["id"],
            )
        control = ControlPlane(self.database)
        self.assertEqual(control.get_workflow_manual_wake(wake["id"]), wake)
        self.assertEqual(
            control.list_workflow_manual_wakes(execution_id=execution["id"]),
            [wake],
        )
        acknowledged = control.acknowledge_workflow_manual_wake(
            wake["id"],
            expected_version=1,
            acknowledged_by_handle="owner",
        )
        self.assertEqual(acknowledged["state"], "acknowledged")
        self.assertEqual(acknowledged["version"], 2)
        self.assertEqual(acknowledged["acknowledged_by"], "owner")
        self.assertEqual(
            [
                (
                    event["sequence"],
                    event["kind"],
                    event["from_state"],
                    event["to_state"],
                    event["actor"],
                )
                for event in acknowledged["events"]
            ],
            [
                (1, "wake.requested", None, "requested", None),
                (2, "wake.acknowledged", "requested", "acknowledged", "owner"),
            ],
        )
        with self.assertRaises(ValidationError):
            self.database.acknowledge_workflow_manual_wake(
                "x" * 65,
                expected_version=1,
                acknowledged_by=self.user["id"],
            )
        with self.assertRaises(ValidationError):
            self.database.acknowledge_workflow_manual_wake(
                wake["id"],
                expected_version=2,
                acknowledged_by="x" * 65,
            )
        with self.assertRaises(ConcurrentUpdateError):
            self.database.acknowledge_workflow_manual_wake(
                wake["id"],
                expected_version=1,
                acknowledged_by=self.user["id"],
            )
        self.assertEqual(
            self.database.get_workflow_execution(execution["id"])["responsibility"][
                "reason"
            ],
            "run_launch",
        )
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id="worker-one",
            expected_run_version=1,
        )
        self.assertEqual(attempt["state"], "preparing")

    def test_replacement_run_requires_its_own_manual_wake(self) -> None:
        self.activate()
        work_item = self.create_work_item("replacement-wake")
        execution = self.start_activated(
            work_item,
            expected_activation_version=1,
            idempotency_key="replacement-wake",
        )
        original = self.create_producer_run(execution, work_item)
        first_wake = self.database.list_workflow_manual_wakes(
            execution_id=execution["id"]
        )[0]
        self.database.acknowledge_workflow_manual_wake(
            first_wake["id"],
            expected_version=1,
            acknowledged_by=self.user["id"],
        )
        cancelled = self.database.transition_run(
            original["id"],
            target=RunState.CANCELLED,
            expected_version=1,
            reason="prove replacement wake isolation",
        )

        replacement = self.database.retry_run(
            original["id"],
            requested_by=self.user["id"],
            expected_run_version=cancelled["version"],
        )
        wakes = self.database.list_workflow_manual_wakes(execution_id=execution["id"])
        self.assertEqual(len(wakes), 2)
        replacement_wake = wakes[-1]
        self.assertEqual(replacement_wake["run_id"], replacement["id"])
        self.assertEqual(replacement_wake["state"], "requested")
        self.assertNotEqual(replacement_wake["id"], first_wake["id"])
        with self.assertRaisesRegex(ConflictError, "manual wake"):
            self.database.begin_run_attempt(
                replacement["id"],
                runtime_adapter="docker",
                owner_id="replacement-worker",
                expected_run_version=1,
            )

    def test_pre_policy_run_does_not_acquire_manual_wake_authority(self) -> None:
        work_item = self.create_work_item("pre-policy-wake")
        execution = self.database.start_workflow_execution(
            project_key="policy-project",
            configuration_name="default",
            work_item_id=work_item["id"],
            source_revision="a" * 40,
            expected_configuration_version=1,
            expected_context_version_id=self.project["context_version_id"],
            expected_work_item_version_id=work_item["work_item_version_id"],
            expected_thread_generation=1,
            expected_thread_version=1,
            idempotency_key="pre-policy-wake",
            requested_by=self.user["id"],
        )
        run = self.create_producer_run(execution, work_item)

        self.assertEqual(execution["responsibility"], {"status": "pre_policy"})
        self.assertEqual(
            self.database.list_workflow_manual_wakes(execution_id=execution["id"]),
            [],
        )
        attempt = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="docker",
            owner_id="legacy-worker",
            expected_run_version=1,
        )
        self.assertEqual(attempt["state"], "preparing")

    def test_activation_shared_host_concurrency_is_an_atomic_admission_limit(
        self,
    ) -> None:
        self.activate()
        runs = []
        for index in range(2):
            item = self.create_work_item(f"concurrency-{index}")
            execution = self.start_activated(
                item,
                expected_activation_version=1,
                idempotency_key=f"concurrency-{index}",
            )
            run = self.create_producer_run(execution, item)
            wake = self.database.list_workflow_manual_wakes(
                execution_id=execution["id"]
            )[0]
            self.database.acknowledge_workflow_manual_wake(
                wake["id"],
                expected_version=1,
                acknowledged_by=self.user["id"],
            )
            runs.append(run)

        self.database.begin_run_attempt(
            runs[0]["id"],
            runtime_adapter="docker",
            owner_id="worker-one",
            expected_run_version=1,
        )
        with self.assertRaisesRegex(ConflictError, "shared-host concurrency"):
            self.database.begin_run_attempt(
                runs[1]["id"],
                runtime_adapter="docker",
                owner_id="worker-two",
                expected_run_version=1,
            )

    def test_activation_wall_time_opens_a_non_extendable_owner_ruling(self) -> None:
        self.activate()
        work_item = self.create_work_item("wall-time")
        execution = self.start_activated(
            work_item,
            expected_activation_version=1,
            idempotency_key="wall-time",
        )
        observed = (
            datetime.fromisoformat(execution["created_at"]) + timedelta(seconds=10800)
        ).isoformat(timespec="microseconds")
        director = WorkflowDirector(
            ControlPlane(self.database),
            result_store=None,  # type: ignore[arg-type]
            candidate_store=None,  # type: ignore[arg-type]
            validation_executor=None,  # type: ignore[arg-type]
        )

        with mock.patch.object(self.database, "now", return_value=observed):
            self.assertTrue(director.advance(execution["id"], expected_version=1))

        stopped = self.database.get_workflow_execution(execution["id"])
        self.assertEqual(len(stopped["rulings"]), 1)
        ruling = stopped["rulings"][0]
        self.assertEqual(
            (ruling["kind"], ruling["state"]), ("resource_exhausted", "open")
        )
        self.assertEqual(stopped["responsibility"]["reason"], "ruling_resolution")
        with self.assertRaisesRegex(ConflictError, "cannot be increased"):
            self.database.resolve_workflow_ruling(
                ruling["id"],
                expected_version=1,
                decision="continue",
                resolved_by=self.user["id"],
            )
        resolved = self.database.resolve_workflow_ruling(
            ruling["id"],
            expected_version=1,
            decision="cancel",
            resolved_by=self.user["id"],
        )
        self.assertEqual(resolved["decision"], "cancel")
        self.assertEqual(
            self.database.get_workflow_execution(execution["id"])["state"],
            "cancelled",
        )
