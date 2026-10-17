from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from magic_glass_box.executor import SandboxPlan
from magic_glass_box.openai_chatgpt import ChatGPTProfileStore
from scripts.check_live_openai_chatgpt import (
    BUDGET_CEILING_MICROUSD,
    BUDGET_MAX_OUTPUT_TOKENS,
    MAX_DIAGNOSTIC_LOG_CHARACTERS,
    _assert_sandbox_boundaries,
    _connector_spec,
    _failure_diagnostic,
    _grant_spec,
    _serialize_sandbox_plans,
    _session_secrets,
    _verified_failure_diagnostic,
)


class LiveOpenAIChatGPTProofContractTests(unittest.TestCase):
    def test_live_proof_uses_v2_with_a_five_dollar_hard_ceiling(self) -> None:
        self.assertEqual(BUDGET_CEILING_MICROUSD, 5_000_000)
        connector = _connector_spec(
            socket_path=Path("/private/openai.sock"),
            deployment_sha256="a" * 64,
            scope_sha256="b" * 64,
            model="gpt-test",
        )
        grant = _grant_spec(
            deployment_sha256="a" * 64,
            scope_sha256="b" * 64,
            model="gpt-test",
        )

        self.assertEqual(connector.connector_protocol_version, 2)
        self.assertIsNotNone(grant.budget)
        assert grant.budget is not None
        self.assertEqual(grant.budget.max_output_tokens, BUDGET_MAX_OUTPUT_TOKENS)
        self.assertEqual(grant.budget.run_microusd, BUDGET_CEILING_MICROUSD)
        self.assertEqual(grant.budget.project_microusd, BUDGET_CEILING_MICROUSD)
        self.assertEqual(grant.budget.grant_microusd, BUDGET_CEILING_MICROUSD)
        self.assertLessEqual(
            grant.budget.maximum_invocation_microusd(grant.request_bytes),
            grant.budget.invocation_microusd,
        )
        connector.validate_grant("openai-chatgpt", grant)

    def test_session_secrets_can_tolerate_a_terminally_cleared_session(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            store = ChatGPTProfileStore(Path(temporary_directory) / "profile")

            with self.assertRaisesRegex(RuntimeError, "sign-in is required"):
                _session_secrets(store)
            self.assertEqual(_session_secrets(store, required=False), ())

    def test_failure_diagnostic_is_bounded_and_allowlists_audit_metadata(self) -> None:
        report = _failure_diagnostic(
            run={
                "state": "failed",
                "attempts": [
                    {
                        "state": "failed",
                        "exit_code": 1,
                        "reason": "runtime exited unsuccessfully",
                        "result": None,
                        "events": [{"private": "do not expose"}],
                    }
                ],
                "private": "do not expose",
            },
            invocations=[
                {
                    "outcome": "failed",
                    "http_status": 200,
                    "error_code": "subscription_sharing_unsupported_capability",
                    "request_bytes": 123,
                    "response_bytes": 456,
                    "model": "gpt-test",
                    "route": "openai-chatgpt",
                    "protocol": "openai-responses",
                    "connector_protocol_version": 2,
                    "accounting_status": "reported",
                    "accounted_microusd": 7,
                    "estimated_microusd": 7,
                    "reserved_microusd": 1000000,
                    "input_tokens": 3,
                    "cached_input_tokens": 1,
                    "output_tokens": 2,
                    "reasoning_output_tokens": 1,
                    "request": "do not expose",
                    "response": "do not expose",
                }
            ],
            worker_logs=["prefix-" + "x" * MAX_DIAGNOSTIC_LOG_CHARACTERS + "-tail"],
            orchestration_error=RuntimeError("private exception detail"),
        )

        self.assertEqual(
            set(report),
            {
                "attempts",
                "orchestrator_error",
                "provider_invocations",
                "run_state",
                "worker_log_tail",
            },
        )
        self.assertEqual(report["run_state"], "failed")
        self.assertEqual(report["orchestrator_error"], "RuntimeError")
        self.assertEqual(
            report["attempts"],
            [
                {
                    "exit_code": 1,
                    "has_result": False,
                    "reason": "runtime exited unsuccessfully",
                    "state": "failed",
                }
            ],
        )
        self.assertEqual(
            report["provider_invocations"],
            [
                {
                    "accounted_microusd": 7,
                    "accounting_status": "reported",
                    "cached_input_tokens": 1,
                    "connector_protocol_version": 2,
                    "error_code": "subscription_sharing_unsupported_capability",
                    "estimated_microusd": 7,
                    "http_status": 200,
                    "input_tokens": 3,
                    "model": "gpt-test",
                    "outcome": "failed",
                    "output_tokens": 2,
                    "protocol": "openai-responses",
                    "reasoning_output_tokens": 1,
                    "request_bytes": 123,
                    "reserved_microusd": 1000000,
                    "response_bytes": 456,
                    "route": "openai-chatgpt",
                }
            ],
        )
        self.assertEqual(len(report["worker_log_tail"]), MAX_DIAGNOSTIC_LOG_CHARACTERS)
        self.assertTrue(report["worker_log_tail"].endswith("-tail"))
        self.assertNotIn("private", repr(report))

    def test_verified_failure_diagnostic_refuses_a_leaking_surface(self) -> None:
        arguments = {
            "run": {"state": "failed", "attempts": []},
            "invocations": [],
            "worker_logs": [],
            "orchestration_error": None,
            "secrets": ("access-secret",),
        }

        with self.assertRaisesRegex(RuntimeError, "worker output"):
            _verified_failure_diagnostic(
                **arguments,
                surfaces={"worker output": b"prefix access-secret suffix"},
            )

        report = _verified_failure_diagnostic(
            **arguments,
            surfaces={"worker output": b"safe"},
        )
        self.assertEqual(report["run_state"], "failed")

    def test_failure_plan_still_enforces_the_sandbox_boundary(self) -> None:
        profile = Path("/private/openai-profile")
        valid = SandboxPlan(
            "run_" + "1" * 32,
            "magic-glass-box-" + "1" * 32,
            "sha256:" + "a" * 64,
            ("container", "create", "--network", "none"),
        )

        _assert_sandbox_boundaries(
            (valid,), profile_dir=profile, secrets=("access-secret",)
        )
        self.assertIn(b"--network", _serialize_sandbox_plans((valid,)))

        cases = (
            (
                ("container", "create", "--network", "bridge"),
                "direct network access",
            ),
            (("container", "create", "--network"), "network boundary"),
            (
                ("container", "create", "--network", "none", str(profile)),
                "profile was mounted",
            ),
            (
                ("container", "create", "--network", "none", "access-secret"),
                "Docker sandbox launch plans",
            ),
        )
        for create_args, message in cases:
            with (
                self.subTest(create_args=create_args),
                self.assertRaisesRegex(RuntimeError, message),
            ):
                _assert_sandbox_boundaries(
                    (
                        SandboxPlan(
                            valid.run_id,
                            valid.container_name,
                            valid.image_id,
                            create_args,
                        ),
                    ),
                    profile_dir=profile,
                    secrets=("access-secret",),
                )


if __name__ == "__main__":
    unittest.main()
