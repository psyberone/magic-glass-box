from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from magic_glass_box.db import Database
from magic_glass_box.gates import MaintainerReviewRequest
from magic_glass_box.service import ControlPlane
from scripts.check_live_imitation_krab_workflow import (
    ACKNOWLEDGEMENT_FLAG,
    GITHUB_SIDECAR_NAME,
    PROJECT_KEY,
    VALIDATION_NAME,
    WORKFLOW_BUDGET_CEILING_MICROUSD,
    WORKFLOW_REQUEST_BYTES,
    WorkflowRulingRequired,
    _drive_until,
    _github_operation_spec,
    _load_maintainer_review,
    _prepare_control_plane,
    _project_context,
    _require_approved_landing_authority,
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
        self.assertIn("unittest", " ".join(spec.validations[0].command))

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
                WORKFLOW_BUDGET_CEILING_MICROUSD,
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
            self.assertEqual(document["routes"][0]["request_bytes"], 1024 * 1024)
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


if __name__ == "__main__":
    unittest.main()
