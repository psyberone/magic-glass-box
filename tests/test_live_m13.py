from __future__ import annotations

import json
import tempfile
import threading
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path

from magic_glass_box.db import Database
from magic_glass_box.domain import GatewayCapability, WorkspaceMode
from magic_glass_box.service import ControlPlane
from scripts.check_live_m13 import (
    MAX_LIVE_BUDGET_MICROUSD,
    PROFILE_IDENTITIES,
    SCOPE_DECISION_COPY_ONLY,
    _advisory_spec,
    _discovery_spec,
    _KrabFixture,
    _load_scope_answer,
    _prepare_control_plane,
    _profile,
    _work_item,
    _workflow_requires_stop,
    _workflow_spec,
    parse_args,
)


class LiveM13ContractTests(unittest.TestCase):
    def test_work_item_requires_an_explicit_bounded_scope_choice(self) -> None:
        item = _work_item()

        self.assertIn("copy_only", item.brief)
        self.assertIn("behavioral_readiness", item.brief)
        self.assertIn("must obtain", item.brief)
        self.assertTrue(
            any("liveness" in criterion for criterion in item.acceptance_criteria)
        )
        self.assertIn("Do not commit, push, or open a pull request.", item.constraints)

    def test_role_profiles_expose_only_their_bounded_gateway_surface(self) -> None:
        investigator = _profile(
            image="proof:local",
            workspace_mode=WorkspaceMode.READ_ONLY,
            capabilities=(
                GatewayCapability.RUN_DESCRIBE,
                GatewayCapability.PROVIDER_INVOKE,
                GatewayCapability.DISCOVERY_TRACE_SUBMIT,
            ),
        )
        coordinator = _profile(
            image="proof:local",
            workspace_mode=WorkspaceMode.READ_ONLY,
            capabilities=(
                GatewayCapability.RUN_DESCRIBE,
                GatewayCapability.PROVIDER_INVOKE,
                GatewayCapability.INTERACTION_OPEN,
                GatewayCapability.INTERACTION_READ,
                GatewayCapability.HANDOFF_SUBMIT,
                GatewayCapability.DISCOVERY_RULING_SUBMIT,
            ),
            krab=True,
        )

        self.assertEqual(investigator.workspace_mode, WorkspaceMode.READ_ONLY)
        self.assertEqual(investigator.credential_names, ("openai-chatgpt",))
        self.assertNotIn(
            GatewayCapability.DISCOVERY_RULING_SUBMIT,
            investigator.gateway_capabilities,
        )
        self.assertEqual(
            coordinator.credential_names,
            ("openai-chatgpt", "krab"),
        )
        self.assertNotIn(
            GatewayCapability.WORK_APPEND,
            coordinator.gateway_capabilities,
        )

    def test_fixed_prescriptions_keep_budgets_and_authority_bounded(self) -> None:
        discovery = _discovery_spec()
        implementation = _workflow_spec("sha256:" + "a" * 64)
        advisory = _advisory_spec()

        self.assertEqual(discovery.max_must_items, 3)
        self.assertEqual(discovery.max_follow_up_proposals, 3)
        self.assertTrue(implementation.maintainer_approval)
        self.assertEqual(implementation.landing_mode.value, "manual")
        self.assertEqual(len(implementation.validations), 1)
        self.assertEqual(advisory.max_findings_per_reviewer, 4)
        self.assertEqual(advisory.max_challenges_per_reviewer, 4)
        self.assertEqual(advisory.max_report_items, 8)

    def test_fresh_state_pins_all_three_prescriptions_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            handles = ["operator", *PROFILE_IDENTITIES.values()]
            fixture = _KrabFixture(
                database=object(),
                server=object(),
                thread=threading.Thread(),
                base_url="http://127.0.0.1:18765",
                project_id="prj_" + "a" * 32,
                project_key="m13-prescription-proof",
                session_id="ses_" + "b" * 32,
                users={
                    handle: "usr_" + f"{index + 1:x}" * 32
                    for index, handle in enumerate(handles)
                },
                token_paths={
                    handle: root / "tokens" / f"{handle}.token" for handle in handles
                },
            )
            database = Database(root / "control-plane.db")

            discovery = _prepare_control_plane(
                ControlPlane(database),
                fixture=fixture,
                repository=root / "source",
                source_revision="c" * 40,
                image="proof:local",
                image_id="sha256:" + "d" * 64,
                connector_socket=root / "connector.sock",
                deployment_sha256="e" * 64,
                scope_sha256="f" * 64,
                model="gpt-test",
                budget_ceiling_microusd=MAX_LIVE_BUDGET_MICROUSD,
            )

            self.assertEqual(discovery["state"], "active")
            self.assertEqual(discovery["stage"], "investigate")
            self.assertEqual(
                database.get_discovery_configuration(
                    "m13-prescription-proof", "bounded-discovery"
                )["template"]["key"],
                "discovery-v1",
            )
            self.assertEqual(
                database.get_workflow_configuration(
                    "m13-prescription-proof", "scoped-implementation"
                )["template"]["key"],
                "implementation-v1",
            )
            self.assertEqual(
                database.get_advisory_configuration(
                    "m13-prescription-proof", "advisory-panel"
                )["template"]["key"],
                "advisory-review-v1",
            )
            self.assertEqual(
                database.get_provider_grant("openai-chatgpt")["policy"]["budget"][
                    "project_microusd"
                ],
                MAX_LIVE_BUDGET_MICROUSD,
            )

    def test_scope_answer_is_strict_bounded_and_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "answer.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "decision": SCOPE_DECISION_COPY_ONLY,
                        "rationale": "Keep the existing liveness-only behavior.",
                    }
                ),
                encoding="utf-8",
            )

            answer = _load_scope_answer(path)

            self.assertEqual(answer.decision, SCOPE_DECISION_COPY_ONLY)
            self.assertIn("liveness-only", answer.rationale)
            for invalid in (
                {},
                {"schema_version": 1, "decision": "invented", "rationale": "x"},
                {
                    "schema_version": 1,
                    "decision": SCOPE_DECISION_COPY_ONLY,
                    "rationale": "x",
                    "extra": True,
                },
            ):
                path.write_text(json.dumps(invalid), encoding="utf-8")
                with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                    _load_scope_answer(path)

            path.write_bytes(b"{" + b" " * 4096)
            with self.assertRaisesRegex(RuntimeError, "too large"):
                _load_scope_answer(path)

    def test_workflow_wait_stops_immediately_for_an_operator_ruling(self) -> None:
        class Ledger:
            execution: dict[str, object]

            def get_workflow_execution(self, _execution_id: str) -> dict[str, object]:
                return self.execution

        ledger = Ledger()
        ledger.execution = {
            "state": "active",
            "stage": "review",
            "rulings": [{"state": "open", "kind": "review_verdict_missing"}],
        }
        self.assertTrue(_workflow_requires_stop(ledger, "wfx_1"))

        ledger.execution = {
            "state": "active",
            "stage": "review",
            "rulings": [],
        }
        self.assertFalse(_workflow_requires_stop(ledger, "wfx_1"))

    def test_cli_requires_explicit_paths_and_caps_the_budget(self) -> None:
        arguments = [
            "--profile-dir",
            "/private/profile",
            "--repository",
            "/source/krab",
            "--state-dir",
            "/private/state",
            "--model",
            "gpt-test",
            "--budget-ceiling-microusd",
            str(MAX_LIVE_BUDGET_MICROUSD),
            "--i-understand-this-uses-live-quota",
        ]

        parsed = parse_args(arguments)

        self.assertTrue(parsed.i_understand_this_uses_live_quota)
        self.assertEqual(parsed.budget_ceiling_microusd, MAX_LIVE_BUDGET_MICROUSD)
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            parse_args(
                arguments[:-3]
                + [
                    "--budget-ceiling-microusd",
                    str(MAX_LIVE_BUDGET_MICROUSD + 1),
                ]
            )


if __name__ == "__main__":
    unittest.main()
