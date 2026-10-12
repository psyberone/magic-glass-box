from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from magic_glass_box.credentials import PrivateFileCredentialResolver
from magic_glass_box.db import Database
from magic_glass_box.domain import GatewayCapability, NetworkMode, RunState
from magic_glass_box.service import ControlPlane
from scripts.check_live_provider_gateway import (
    _assert_proof,
    _prepare_run,
    _provider_policy,
    parse_args,
)


class LiveProviderGatewayProofContractTests(unittest.TestCase):
    def test_parser_defaults_and_accepts_explicit_overrides(self) -> None:
        defaults = parse_args([])
        self.assertEqual(defaults.image, "python:3.12-slim")
        self.assertEqual(defaults.docker_timeout, 60.0)

        overridden = parse_args(["--image", "proof:test", "--docker-timeout", "12.5"])
        self.assertEqual(overridden.image, "proof:test")
        self.assertEqual(overridden.docker_timeout, 12.5)

    def test_provider_policy_pins_every_authority_boundary(self) -> None:
        policy = _provider_policy(8765)

        self.assertEqual(policy.provider, "example-ai")
        self.assertEqual(policy.adapter, "json_http")
        self.assertEqual(policy.destination, "http://127.0.0.1:8765/v1/invoke")
        self.assertEqual(policy.method, "POST")
        self.assertEqual(policy.model, "pinned-model")
        self.assertEqual(policy.credential_header, "authorization")
        self.assertEqual(policy.credential_prefix, "Bearer ")
        self.assertEqual(policy.static_headers, ())
        self.assertEqual(policy.request_bytes, 4096)
        self.assertEqual(policy.response_bytes, 4096)
        self.assertEqual(policy.timeout_ms, 5000)
        self.assertEqual(policy.max_concurrency, 1)

    def test_prepare_run_pins_profile_work_and_provider_authority(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mgb-provider-proof-") as temporary:
            root = Path(temporary)
            root.chmod(0o700)
            secret_path = root / "provider.secret"
            secret_path.write_text("test-secret\n", encoding="ascii")
            secret_path.chmod(0o600)
            database = Database(root / "control-plane.db")
            control = ControlPlane(
                database,
                credential_resolver=PrivateFileCredentialResolver(),
            )
            policy = _provider_policy(8765)

            run_id, profile, attempt = _prepare_run(
                control, secret_path, policy, "proof:test"
            )

            execution = database.get_run_execution(run_id)
            self.assertEqual(execution["state"], RunState.PREPARING.value)
            self.assertEqual(
                execution["work_item"]["title"], "Prove scoped provider access"
            )
            self.assertEqual(
                execution["credential_references"],
                {
                    "example-main": f"file:{secret_path}",
                    "krab": "file:/not-mounted/krab.token",
                },
            )
            authority = database.get_run_provider_grant(run_id, "example-main")
            self.assertEqual(authority["policy"], policy.to_dict())
            self.assertEqual(profile.image, "proof:test")
            self.assertEqual(profile.network_mode, NetworkMode.GATEWAY_ONLY)
            self.assertEqual(profile.credential_names, ("example-main",))
            self.assertEqual(
                profile.gateway_capabilities,
                (GatewayCapability.PROVIDER_INVOKE,),
            )
            self.assertEqual(attempt["run_id"], run_id)
            self.assertEqual(attempt["state"], "preparing")
            self.assertEqual(attempt["run_state"], RunState.PREPARING.value)
            self.assertEqual(attempt["runtime_adapter"], "command")

    def test_proof_verdict_accepts_only_the_pinned_request_without_leaks(self) -> None:
        credential = "live_secret"
        request = {
            "authorization": "Bearer live_secret",
            "body": {"input": "live hello", "model": "pinned-model"},
        }
        invocation = {"outcome": "succeeded"}

        _assert_proof(
            exit_code=0,
            provider_requests=[request],
            audit_invocations=[invocation],
            credential=credential,
            durable_bytes=b"safe database",
            gateway_bytes=b"safe mailbox",
        )

        cases = (
            (
                {"exit_code": 1},
                "worker exited 1",
            ),
            (
                {"provider_requests": []},
                "exactly the pinned request",
            ),
            (
                {"audit_invocations": []},
                "invocation was not audited",
            ),
            (
                {"audit_invocations": [{"outcome": "failed"}]},
                "invocation was not audited",
            ),
            (
                {"durable_bytes": b"prefix live_secret suffix"},
                "durable state",
            ),
            (
                {"gateway_bytes": b"prefix live_secret suffix"},
                "worker gateway",
            ),
        )
        defaults = {
            "exit_code": 0,
            "provider_requests": [request],
            "audit_invocations": [invocation],
            "credential": credential,
            "durable_bytes": b"safe database",
            "gateway_bytes": b"safe mailbox",
        }
        for override, message in cases:
            with (
                self.subTest(override=override),
                self.assertRaisesRegex(RuntimeError, message),
            ):
                _assert_proof(**(defaults | override))


if __name__ == "__main__":
    unittest.main()
