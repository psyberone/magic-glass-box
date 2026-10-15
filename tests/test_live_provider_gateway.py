from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from magic_glass_box.db import Database
from magic_glass_box.domain import GatewayCapability, NetworkMode, RunState
from magic_glass_box.service import ControlPlane
from scripts.check_live_provider_gateway import (
    _assert_proof,
    _connector_policy,
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
        socket_path = Path("/tmp/example-sidecar.sock")  # noqa: S108
        connector = _connector_policy(socket_path)
        policy = _provider_policy()

        self.assertEqual(connector.transport.socket_path, str(socket_path))
        self.assertEqual(connector.deployment_sha256, "a" * 64)
        self.assertEqual(policy.provider, "example-ai")
        self.assertEqual(policy.protocol, "example-json")
        self.assertEqual(policy.route, "fake-main")
        self.assertEqual(policy.connector_name, "example-sidecar")
        self.assertEqual(policy.model, "pinned-model")
        self.assertEqual(policy.credential_scope_sha256, "b" * 64)
        self.assertEqual(policy.request_bytes, 4096)
        self.assertEqual(policy.response_bytes, 4096)
        self.assertEqual(policy.timeout_ms, 5000)
        self.assertEqual(policy.max_concurrency, 1)

    def test_prepare_run_pins_profile_work_and_provider_authority(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mgb-provider-proof-") as temporary:
            root = Path(temporary)
            root.chmod(0o700)
            socket_path = root / "connector.sock"
            database = Database(root / "control-plane.db")
            control = ControlPlane(database)
            policy = _provider_policy()

            run_id, profile, attempt = _prepare_run(
                control, socket_path, policy, "proof:test"
            )

            execution = database.get_run_execution(run_id)
            self.assertEqual(execution["state"], RunState.PREPARING.value)
            self.assertEqual(
                execution["work_item"]["title"], "Prove scoped provider access"
            )
            self.assertEqual(
                execution["credential_references"],
                {"krab": "file:/not-mounted/krab.token"},
            )
            self.assertEqual(execution["provider_authority_names"], ["example-main"])
            authority = database.get_run_provider_grant(run_id, "example-main")
            self.assertEqual(authority["policy"], policy.to_dict())
            self.assertEqual(authority["grant_kind"], "connector")
            self.assertNotIn("credential_ref", authority)
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
        invocation = {
            "outcome": "succeeded",
            "dispatch_kind": "connector",
            "provider": "example-ai",
            "protocol": "example-json",
            "model": "pinned-model",
            "route": "fake-main",
        }

        _assert_proof(
            exit_code=0,
            connector_invocations=1,
            audit_invocations=[invocation],
            credential=credential,
            scanned_surfaces={
                "MGB state": b"safe database",
                "worker gateway": b"safe mailbox",
            },
        )

        cases = (
            (
                {"exit_code": 1},
                "worker exited 1",
            ),
            (
                {"connector_invocations": 0},
                "exactly one pinned invocation",
            ),
            (
                {"audit_invocations": []},
                "invocation was not audited",
            ),
            (
                {"audit_invocations": [{**invocation, "outcome": "failed"}]},
                "invocation was not audited",
            ),
            (
                {"scanned_surfaces": {"worker gateway": b"prefix live_secret suffix"}},
                "worker gateway",
            ),
        )
        defaults = {
            "exit_code": 0,
            "connector_invocations": 1,
            "audit_invocations": [invocation],
            "credential": credential,
            "scanned_surfaces": {
                "MGB state": b"safe database",
                "worker gateway": b"safe mailbox",
            },
        }
        for override, message in cases:
            with (
                self.subTest(override=override),
                self.assertRaisesRegex(RuntimeError, message),
            ):
                _assert_proof(**(defaults | override))


if __name__ == "__main__":
    unittest.main()
