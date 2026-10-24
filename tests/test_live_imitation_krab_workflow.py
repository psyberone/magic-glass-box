from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from magic_glass_box.db import Database, NotFoundError
from magic_glass_box.gates import MaintainerReviewRequest
from magic_glass_box.service import ControlPlane
from scripts.check_live_imitation_krab_workflow import (
    ACKNOWLEDGEMENT_FLAG,
    GITHUB_MAPPING_NAME,
    GITHUB_SIDECAR_NAME,
    PROJECT_KEY,
    VALIDATION_NAME,
    WORKFLOW_BUDGET_CEILING_MICROUSD,
    WORKFLOW_INVOCATION_MICROUSD,
    WORKFLOW_MAX_OUTPUT_TOKENS,
    WORKFLOW_REQUEST_BYTES,
    WorkflowRulingRequired,
    _drive_until,
    _find_completed_github_effect,
    _github_operation_spec,
    _github_provider_spec_path,
    _load_maintainer_review,
    _prepare_control_plane,
    _project_context,
    _register_github_authority,
    _require_approved_landing_authority,
    _RunScopedConnector,
    _service_sidecar_spec,
    _settle_effect,
    _work_item,
    _workflow_spec,
    _write_workflow_connector_config,
    parse_args,
)


def _load_review(
    decision: str, *, details: list[str] | None = None
) -> MaintainerReviewRequest:
    return MaintainerReviewRequest.from_mapping(
        {
            "schema_version": 1,
            "decision": decision,
            "summary": "Exact candidate maintainer decision.",
            "details": [] if details is None else details,
        }
    )


class LiveImitationKrabWorkflowContractTests(unittest.TestCase):
    def test_work_item_pins_the_accepted_receipt_policy_slice(self) -> None:
        work_item = _work_item()

        self.assertIn("completion_policy", work_item.brief)
        self.assertIn("workflow | receipt", work_item.brief)
        self.assertIn("closed.handled", work_item.brief)
        self.assertIn("schema 7", work_item.brief)
        self.assertIn("explicit handle, then acknowledge", work_item.brief)
        self.assertIn("limit", work_item.brief)
        self.assertIn("open_items", work_item.brief)
        self.assertIn("claim-review verdict", work_item.brief)
        self.assertIn("item, queue, inbox, activity, CLI", work_item.brief)
        self.assertTrue(any("85%" in item for item in work_item.constraints))
        self.assertTrue(any("mutation run" in item for item in work_item.constraints))
        self.assertTrue(any("Do not commit" in item for item in work_item.constraints))

    def test_project_context_carries_product_judgment_and_owner_authority(
        self,
    ) -> None:
        context = _project_context(Path("/source/imitation-krab"))
        document = context.to_dict()
        instructions = "\n".join(document["instructions"])

        self.assertIn("loopback-first coordination service", document["purpose"])
        self.assertIn("not an agent runtime", instructions)
        self.assertIn("standard library", instructions)
        self.assertIn("Product-fit question", instructions)
        self.assertIn("maintainer approval", instructions)
        self.assertIn("Do not commit", instructions)

    def test_workflow_requires_independent_review_validation_and_provider_landing(
        self,
    ) -> None:
        image_id = "sha256:" + "a" * 64
        spec = _workflow_spec(image_id)

        self.assertEqual(spec.review_separation.value, "different_profile_and_identity")
        self.assertTrue(spec.maintainer_approval)
        self.assertEqual(spec.max_correction_rounds, 3)
        self.assertEqual(spec.landing_mode.value, "provider")
        self.assertEqual(len(spec.validations), 1)
        self.assertEqual(spec.validations[0].name, VALIDATION_NAME)
        self.assertEqual(spec.validations[0].image_id, image_id)
        validation = " ".join(spec.validations[0].command)
        self.assertIn("coverage run", validation)
        self.assertIn("unittest", validation)
        self.assertIn("coverage report", validation)

    def test_runtime_proof_image_contains_the_configured_coverage_tool(self) -> None:
        dockerfile = (
            Path(__file__).resolve().parents[1] / "docker" / "runtime-proof.Dockerfile"
        ).read_text(encoding="utf-8")

        self.assertIn("python3-coverage", dockerfile)

    def test_control_plane_setup_pins_the_exact_workflow_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = Database(root / "control-plane.db")
            control = ControlPlane(database)

            execution, connector, profiles = _prepare_control_plane(
                control,
                repository=root / "source",
                source_revision="d" * 40,
                image="runtime-proof:local",
                image_id="sha256:" + "a" * 64,
                connector_socket=root / "connector.sock",
                deployment_sha256="b" * 64,
                scope_sha256="c" * 64,
                model="gpt-test",
                budget_ceiling_microusd=3_200_000,
            )

            self.assertEqual(execution["stage"], "build")
            self.assertEqual(execution["source_revision"], "d" * 40)
            self.assertEqual(execution["spec"]["landing_mode"], "provider")
            self.assertTrue(execution["spec"]["maintainer_approval"])
            self.assertEqual(connector["state"], "active")
            self.assertEqual(connector["spec"]["request_bytes"], WORKFLOW_REQUEST_BYTES)
            grant = database.get_provider_grant("openai-chatgpt")
            self.assertEqual(grant["policy"]["request_bytes"], WORKFLOW_REQUEST_BYTES)
            self.assertEqual(
                grant["policy"]["budget"]["project_microusd"],
                3_200_000,
            )
            self.assertEqual(
                grant["policy"]["budget"]["invocation_microusd"],
                WORKFLOW_INVOCATION_MICROUSD,
            )
            self.assertEqual(
                grant["policy"]["budget"]["max_output_tokens"],
                WORKFLOW_MAX_OUTPUT_TOKENS,
            )
            self.assertNotEqual(
                profiles["producer"]["krab_identity_id"],
                profiles["reviewer"]["krab_identity_id"],
            )
            self.assertEqual(
                profiles["reviewer"]["spec"]["workspace_mode"], "read_only"
            )

    def test_connector_config_matches_the_larger_bounded_request_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "config.json"
            _write_workflow_connector_config(
                path,
                connector_id="ifc_" + "a" * 32,
                deployment_sha256="b" * 64,
                scope_sha256="c" * 64,
                model="gpt-test",
            )

            document = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(document["routes"][0]["request_bytes"], 768 * 1024)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_open_ruling_stops_the_driver_without_advancing(self) -> None:
        execution = {
            "id": "wfx_test",
            "state": "active",
            "stage": "build",
            "correction_round": 0,
            "version": 2,
            "rulings": [{"state": "open", "kind": "producer_failed"}],
        }

        class Database:
            @staticmethod
            def get_workflow_execution(_execution_id: str):
                return execution

        class Supervisor:
            @staticmethod
            def drain(*, timeout: float) -> bool:
                raise AssertionError(f"unexpected drain({timeout})")

            @staticmethod
            def tick() -> dict[str, int]:
                raise AssertionError("unexpected tick")

        with self.assertRaisesRegex(WorkflowRulingRequired, "producer_failed"):
            _drive_until(
                Supervisor(),  # type: ignore[arg-type]
                Database(),  # type: ignore[arg-type]
                "wfx_test",
                predicate=lambda _item: False,
                timeout=30,
            )

    def test_live_and_destructive_opt_ins_are_independent(self) -> None:
        arguments = parse_args(
            [
                "--profile-dir",
                "/private/profile",
                "--repository",
                "/source/krab",
                "--state-dir",
                "/state/workflow",
                "--model",
                "gpt-test",
            ]
        )

        self.assertFalse(arguments.i_understand_this_uses_live_quota)
        self.assertFalse(arguments.approved_landing_acknowledged)
        self.assertIsNone(arguments.maintainer_review)
        self.assertIsNone(arguments.gh_config_dir)
        self.assertEqual(arguments.max_invocations, 64)
        self.assertEqual(
            arguments.budget_ceiling_microusd,
            WORKFLOW_BUDGET_CEILING_MICROUSD,
        )
        acknowledged = parse_args(
            [
                "--profile-dir",
                "/private/profile",
                "--repository",
                "/source/krab",
                "--state-dir",
                "/state/workflow",
                "--model",
                "gpt-test",
                ACKNOWLEDGEMENT_FLAG,
            ]
        )
        self.assertTrue(acknowledged.approved_landing_acknowledged)

    def test_live_budget_override_is_positive_and_capped_at_five_dollars(
        self,
    ) -> None:
        prefix = [
            "--profile-dir",
            "/private/profile",
            "--repository",
            "/source/krab",
            "--state-dir",
            "/state/workflow",
            "--model",
            "gpt-test",
            "--budget-ceiling-microusd",
        ]

        self.assertEqual(
            parse_args([*prefix, "3200000"]).budget_ceiling_microusd,
            3_200_000,
        )
        for invalid in ("0", "5000001", "not-an-integer"):
            with (
                self.subTest(invalid=invalid),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                parse_args([*prefix, invalid])

    def test_connector_allowance_rotates_once_per_active_workflow_run(self) -> None:
        runs = {
            (0, "producer"): {"id": "run_producer", "state": "queued"},
            (0, "reviewer"): {"id": "run_reviewer", "state": "queued"},
        }

        class Database:
            @staticmethod
            def find_workflow_run(
                _execution_id: str, correction_round: int, role: str
            ) -> dict[str, str] | None:
                return runs.get((correction_round, role))

        started: list[str] = []
        stopped: list[str] = []

        def start(run_id: str) -> str:
            started.append(run_id)
            return "process:" + run_id

        def stop(process: str) -> tuple[str, str]:
            stopped.append(process)
            return ("stdout:" + process, "stderr:" + process)

        scope = _RunScopedConnector(
            Database(),  # type: ignore[arg-type]
            start=start,  # type: ignore[arg-type]
            stop=stop,  # type: ignore[arg-type]
        )
        build = {
            "id": "wfx_test",
            "state": "active",
            "stage": "build",
            "correction_round": 0,
        }
        review = {**build, "stage": "review"}

        scope.before_tick(build)
        scope.before_tick(build)
        scope.before_tick(review)
        scope.before_tick({**review, "stage": "maintainer"})
        stdout, stderr = scope.close()

        self.assertEqual(started, ["run_producer", "run_reviewer"])
        self.assertEqual(
            stopped,
            ["process:run_producer", "process:run_reviewer"],
        )
        self.assertIn("stdout:process:run_producer", stdout)
        self.assertIn("stdout:process:run_reviewer", stdout)
        self.assertIn("stderr:process:run_producer", stderr)
        self.assertIn("stderr:process:run_reviewer", stderr)

    def test_drive_prepares_the_run_connector_before_supervisor_tick(self) -> None:
        execution = {
            "id": "wfx_test",
            "state": "active",
            "stage": "build",
            "correction_round": 0,
            "version": 1,
            "rulings": [],
        }
        order: list[str] = []

        class Database:
            @staticmethod
            def get_workflow_execution(_execution_id: str):
                return execution

        class Supervisor:
            @staticmethod
            def tick() -> dict[str, int]:
                order.append("tick")
                execution["stage"] = "maintainer"
                return {"actions_started": 1}

            @staticmethod
            def drain(*, timeout: float) -> bool:
                self.assertEqual(timeout, 30)
                order.append("drain")
                return True

        result = _drive_until(
            Supervisor(),  # type: ignore[arg-type]
            Database(),  # type: ignore[arg-type]
            "wfx_test",
            predicate=lambda item: item["stage"] == "maintainer",
            timeout=30,
            before_tick=lambda _item: order.append("connector"),
        )

        self.assertEqual(result["stage"], "maintainer")
        self.assertEqual(order, ["connector", "tick", "drain"])

    def test_maintainer_review_is_bounded_exact_and_allows_both_decisions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "review.json"
            for decision in ("approved", "changes_requested"):
                details = (
                    ["Add the missing exact migration preservation test."]
                    if decision == "changes_requested"
                    else []
                )
                path.write_text(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "decision": decision,
                            "summary": "Maintainer decision for the exact candidate.",
                            "details": details,
                        }
                    ),
                    encoding="utf-8",
                )
                self.assertEqual(_load_maintainer_review(path).decision.value, decision)

            path.write_text('{"decision":"approved","decision":"approved"}')
            with self.assertRaisesRegex(RuntimeError, "invalid"):
                _load_maintainer_review(path)

            path.write_bytes(b"{" + b" " * (64 * 1024))
            with self.assertRaisesRegex(RuntimeError, "too large"):
                _load_maintainer_review(path)

    def test_approval_requires_a_real_correction_and_separate_landing_authority(
        self,
    ) -> None:
        approved = _load_review("approved")
        changes = _load_review(
            "changes_requested", details=["Correct the migration preservation test."]
        )

        _require_approved_landing_authority(
            changes,
            correction_round=0,
            gh_config_dir=None,
            acknowledged=False,
        )
        for correction_round, gh_config_dir, acknowledged, message in (
            (0, Path("/private/gh"), True, "genuine changes_requested"),
            (1, None, True, "--gh-config-dir"),
            (1, Path("/private/gh"), False, ACKNOWLEDGEMENT_FLAG),
        ):
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(RuntimeError, message),
            ):
                _require_approved_landing_authority(
                    approved,
                    correction_round=correction_round,
                    gh_config_dir=gh_config_dir,
                    acknowledged=acknowledged,
                )
        _require_approved_landing_authority(
            approved,
            correction_round=1,
            gh_config_dir=Path("/private/gh"),
            acknowledged=True,
        )

    def test_github_operations_pin_the_bound_capability_and_exact_revisions(
        self,
    ) -> None:
        mapping = {
            "name": "primary",
            "service": "github",
            "host": "github.com",
            "repository": "psyberone/imitation-krab",
            "account": "psyberone",
            "credential_scope_sha256": "b" * 64,
        }
        spec = _github_operation_spec(
            capability="refs.publish",
            mode="effect",
            mapping=mapping,
            deployment_sha256="c" * 64,
            idempotency_key="workflow:publish",
            resource_id="refs/heads/mgb/receipt-policy",
            object_format="sha1",
            expected_revision=None,
            desired_revision="d" * 40,
            parameters={"source_repository": "/state/candidates/repository.git"},
        )

        self.assertEqual(spec.connector_name, GITHUB_SIDECAR_NAME)
        self.assertEqual(spec.capability.name, "refs.publish")
        self.assertEqual(spec.mapping.to_dict(), mapping)
        self.assertEqual(spec.target.desired_revision, "d" * 40)
        self.assertEqual(
            spec.parameters["source_repository"],
            "/state/candidates/repository.git",
        )
        self.assertEqual(PROJECT_KEY, "imitation-krab-workflow")

    def test_pending_github_effect_is_invoked_once(self) -> None:
        calls: list[tuple[str, str, int]] = []

        class Control:
            @staticmethod
            def execute_service_operation(
                operation_id: str, *, owner_id: str, expected_version: int
            ) -> dict[str, object]:
                calls.append((operation_id, owner_id, expected_version))
                return {
                    "id": operation_id,
                    "state": "succeeded",
                    "version": expected_version + 2,
                }

            @staticmethod
            def reconcile_service_operation(*args: object, **kwargs: object) -> None:
                raise AssertionError((args, kwargs))

        settled = _settle_effect(
            Control(),  # type: ignore[arg-type]
            {"id": "sop_publish", "state": "pending", "version": 1},
            owner_id="workflow-publish-wfx_test",
        )

        self.assertEqual(settled["state"], "succeeded")
        self.assertEqual(
            calls,
            [("sop_publish", "workflow-publish-wfx_test", 1)],
        )

    def test_github_authority_registration_resumes_the_exact_binding(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = Database(root / "control-plane.db")
            control = ControlPlane(database)
            _prepare_control_plane(
                control,
                repository=root / "source",
                source_revision="d" * 40,
                image="runtime-proof:local",
                image_id="sha256:" + "a" * 64,
                connector_socket=root / "connector.sock",
                deployment_sha256="b" * 64,
                scope_sha256="c" * 64,
                model="gpt-test",
            )
            spec = _service_sidecar_spec(
                socket_path=root / "github.sock",
                repository="psyberone/imitation-krab",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="f" * 64,
            )

            first = _register_github_authority(control, spec=spec)
            resumed = _register_github_authority(control, spec=spec)

            self.assertEqual(resumed, first)
            self.assertEqual(len(database.list_service_sidecars()), 1)
            self.assertEqual(
                len(database.list_project_capability_bindings(project_key=PROJECT_KEY)),
                3,
            )
            changed_spec = _service_sidecar_spec(
                socket_path=root / "different.sock",
                repository="psyberone/imitation-krab",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="f" * 64,
            )
            with self.assertRaisesRegex(RuntimeError, "authority does not match"):
                _register_github_authority(control, spec=changed_spec)

    def test_github_authority_rotates_only_the_deployment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = Database(root / "control-plane.db")
            control = ControlPlane(database)
            _prepare_control_plane(
                control,
                repository=root / "source",
                source_revision="d" * 40,
                image="runtime-proof:local",
                image_id="sha256:" + "a" * 64,
                connector_socket=root / "connector.sock",
                deployment_sha256="b" * 64,
                scope_sha256="c" * 64,
                model="gpt-test",
            )
            old_spec = _service_sidecar_spec(
                socket_path=root / "github.sock",
                repository="psyberone/imitation-krab",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="f" * 64,
            )
            old_bindings = _register_github_authority(control, spec=old_spec)
            new_spec = _service_sidecar_spec(
                socket_path=root / "github.sock",
                repository="psyberone/imitation-krab",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="1" * 64,
            )

            rotated = _register_github_authority(control, spec=new_spec)
            resumed = _register_github_authority(control, spec=new_spec)

            self.assertEqual(rotated, resumed)
            self.assertEqual(
                {item["controller"]["deployment_sha256"] for item in rotated.values()},
                {"1" * 64},
            )
            self.assertTrue(
                all(
                    item["controller"]["sidecar_name"].startswith(
                        GITHUB_SIDECAR_NAME + "-"
                    )
                    for item in rotated.values()
                )
            )
            self.assertEqual(
                database.get_service_sidecar(GITHUB_SIDECAR_NAME)["state"],
                "revoked",
            )
            history = database.list_project_capability_bindings(
                project_key=PROJECT_KEY, include_unbound=True
            )
            self.assertEqual(len(history), 6)
            self.assertEqual(
                {
                    item["state"]
                    for item in history
                    if item["id"]
                    in {binding["id"] for binding in old_bindings.values()}
                },
                {"unbound"},
            )

    def test_github_authority_rotation_rejects_mapping_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = Database(root / "control-plane.db")
            control = ControlPlane(database)
            _prepare_control_plane(
                control,
                repository=root / "source",
                source_revision="d" * 40,
                image="runtime-proof:local",
                image_id="sha256:" + "a" * 64,
                connector_socket=root / "connector.sock",
                deployment_sha256="b" * 64,
                scope_sha256="c" * 64,
                model="gpt-test",
            )
            original = _service_sidecar_spec(
                socket_path=root / "github.sock",
                repository="psyberone/imitation-krab",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="f" * 64,
            )
            _register_github_authority(control, spec=original)
            changed = _service_sidecar_spec(
                socket_path=root / "github.sock",
                repository="another/repository",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="1" * 64,
            )

            with self.assertRaisesRegex(RuntimeError, "authority does not match"):
                _register_github_authority(control, spec=changed)

            self.assertEqual(
                database.get_service_sidecar(GITHUB_SIDECAR_NAME)["state"], "active"
            )
            self.assertEqual(
                len(database.list_project_capability_bindings(project_key=PROJECT_KEY)),
                3,
            )

    def test_github_authority_rotation_rejects_unresolved_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = Database(root / "control-plane.db")
            control = ControlPlane(database)
            _prepare_control_plane(
                control,
                repository=root / "source",
                source_revision="d" * 40,
                image="runtime-proof:local",
                image_id="sha256:" + "a" * 64,
                connector_socket=root / "connector.sock",
                deployment_sha256="b" * 64,
                scope_sha256="c" * 64,
                model="gpt-test",
            )
            old_spec = _service_sidecar_spec(
                socket_path=root / "github.sock",
                repository="psyberone/imitation-krab",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="f" * 64,
            )
            bindings = _register_github_authority(control, spec=old_spec)
            operation = _github_operation_spec(
                capability="refs.publish",
                mode="effect",
                mapping=old_spec.mappings[0].to_dict(),
                deployment_sha256=old_spec.deployment_sha256,
                idempotency_key="workflow:wfx_test:head:publish",
                resource_id="refs/heads/mgb/test",
                object_format="sha1",
                expected_revision=None,
                desired_revision="d" * 40,
                parameters={"source_repository": "/private/candidate.git"},
            )
            control.create_service_operation(
                spec=operation,
                created_by_handle="operator",
                project_capability_binding_id=bindings["refs.publish"]["id"],
            )
            new_spec = _service_sidecar_spec(
                socket_path=root / "github.sock",
                repository="psyberone/imitation-krab",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="1" * 64,
            )

            with self.assertRaisesRegex(RuntimeError, "unresolved operation"):
                _register_github_authority(control, spec=new_spec)

            self.assertEqual(
                database.get_service_sidecar(GITHUB_SIDECAR_NAME)["state"], "active"
            )

    def test_github_authority_does_not_replace_an_unrelated_controller(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = Database(root / "control-plane.db")
            control = ControlPlane(database)
            _prepare_control_plane(
                control,
                repository=root / "source",
                source_revision="d" * 40,
                image="runtime-proof:local",
                image_id="sha256:" + "a" * 64,
                connector_socket=root / "connector.sock",
                deployment_sha256="b" * 64,
                scope_sha256="c" * 64,
                model="gpt-test",
            )
            other = _service_sidecar_spec(
                socket_path=root / "other.sock",
                repository="psyberone/imitation-krab",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="f" * 64,
            )
            control.register_service_sidecar(
                name="other-github",
                spec=other,
                created_by_handle="operator",
            )
            original_binding = control.bind_project_capability(
                project_key=PROJECT_KEY,
                capability_name="refs.publish",
                capability_version=1,
                sidecar_name="other-github",
                mapping_name=GITHUB_MAPPING_NAME,
                bound_by_handle="operator",
            )
            desired = _service_sidecar_spec(
                socket_path=root / "github.sock",
                repository="psyberone/imitation-krab",
                account="psyberone",
                credential_scope_sha256="e" * 64,
                deployment_sha256="1" * 64,
            )

            with self.assertRaisesRegex(RuntimeError, "authority does not match"):
                _register_github_authority(control, spec=desired)

            observed = database.resolve_project_capability(
                PROJECT_KEY, "refs.publish", 1
            )
            self.assertEqual(observed["id"], original_binding["id"])
            with self.assertRaises(NotFoundError):
                database.get_service_sidecar(GITHUB_SIDECAR_NAME)

    def test_github_provider_specs_are_immutable_per_deployment(self) -> None:
        root = Path("/private/github")
        first = _service_sidecar_spec(
            socket_path=root / "provider.sock",
            repository="psyberone/imitation-krab",
            account="psyberone",
            credential_scope_sha256="e" * 64,
            deployment_sha256="f" * 64,
        )
        second = _service_sidecar_spec(
            socket_path=root / "provider.sock",
            repository="psyberone/imitation-krab",
            account="psyberone",
            credential_scope_sha256="e" * 64,
            deployment_sha256="1" * 64,
        )

        self.assertNotEqual(
            _github_provider_spec_path(root, first),
            _github_provider_spec_path(root, second),
        )
        self.assertEqual(
            _github_provider_spec_path(root, first).parent,
            root,
        )

    def test_completed_github_effect_is_reused_across_deployments(self) -> None:
        expected = _github_operation_spec(
            connector_name=GITHUB_SIDECAR_NAME + "-" + "1" * 48,
            capability="refs.publish",
            mode="effect",
            mapping={
                "name": "primary",
                "service": "github",
                "host": "github.com",
                "repository": "psyberone/imitation-krab",
                "account": "psyberone",
                "credential_scope_sha256": "e" * 64,
            },
            deployment_sha256="1" * 64,
            idempotency_key="workflow:wfx_test:head:publish",
            resource_id="refs/heads/mgb/test",
            object_format="sha1",
            expected_revision=None,
            desired_revision="d" * 40,
            parameters={"source_repository": "/private/candidate.git"},
        )
        historical = expected.to_dict()
        historical["connector_name"] = GITHUB_SIDECAR_NAME
        historical["deployment_sha256"] = "f" * 64

        class DatabaseStub:
            @staticmethod
            def list_service_operations(*, limit: int) -> list[dict[str, object]]:
                self.assertEqual(limit, 1000)
                return [
                    {
                        "id": "sop_historical",
                        "state": "succeeded",
                        "spec": historical,
                        "attempts": [],
                    }
                ]

        selected = _find_completed_github_effect(
            DatabaseStub(),
            expected,  # type: ignore[arg-type]
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["id"], "sop_historical")

    def test_changed_or_unresolved_github_effect_is_not_replayed(self) -> None:
        expected = _github_operation_spec(
            capability="refs.publish",
            mode="effect",
            mapping={
                "name": "primary",
                "service": "github",
                "host": "github.com",
                "repository": "psyberone/imitation-krab",
                "account": "psyberone",
                "credential_scope_sha256": "e" * 64,
            },
            deployment_sha256="1" * 64,
            idempotency_key="workflow:wfx_test:head:publish",
            resource_id="refs/heads/mgb/test",
            object_format="sha1",
            expected_revision=None,
            desired_revision="d" * 40,
            parameters={"source_repository": "/private/candidate.git"},
        )
        historical = expected.to_dict()
        historical["deployment_sha256"] = "f" * 64

        for state, changed_revision, message in (
            ("pending", "d" * 40, "unresolved"),
            ("succeeded", "e" * 40, "reused with changes"),
        ):
            value = json.loads(json.dumps(historical))
            value["target"]["desired_revision"] = changed_revision

            class DatabaseStub:
                def __init__(self, operation_state: str, operation_spec: object):
                    self.operation_state = operation_state
                    self.operation_spec = operation_spec

                def list_service_operations(
                    self, *, limit: int
                ) -> list[dict[str, object]]:
                    return [
                        {
                            "id": "sop_historical",
                            "state": self.operation_state,
                            "spec": self.operation_spec,
                            "attempts": [],
                        }
                    ]

            with (
                self.subTest(state=state, changed_revision=changed_revision),
                self.assertRaisesRegex(RuntimeError, message),
            ):
                _find_completed_github_effect(
                    DatabaseStub(state, value),
                    expected,  # type: ignore[arg-type]
                )


if __name__ == "__main__":
    unittest.main()
