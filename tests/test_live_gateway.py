from __future__ import annotations

import unittest

from magic_glass_box.domain import GatewayCapability
from magic_glass_box.gateway import RunGateway
from scripts.check_live_gateway import RUN_ID, ProofControl, _assert_proof, parse_args


class LiveGatewayProofContractTests(unittest.TestCase):
    def test_parser_defaults_and_accepts_explicit_overrides(self) -> None:
        defaults = parse_args([])
        self.assertEqual(defaults.image, "python:3.12-slim")
        self.assertEqual(defaults.docker_timeout, 60.0)

        overridden = parse_args(["--image", "proof:test", "--docker-timeout", "12.5"])
        self.assertEqual(overridden.image, "proof:test")
        self.assertEqual(overridden.docker_timeout, 12.5)

    def test_proof_database_satisfies_the_run_describe_contract(self) -> None:
        control = ProofControl()
        gateway = RunGateway(
            control,
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=(),
        )

        response = gateway.handle(
            {
                "operation": "run.describe",
                "payload": {},
                "request_id": "proof-contract-0001",
                "version": 1,
            }
        )

        self.assertTrue(response["ok"])
        self.assertIsNone(response["result"]["handoff"])
        self.assertIsNone(response["result"]["work_memory"])
        self.assertEqual(control.database.describe_calls, 1)

    def test_proof_verdict_requires_success_and_exactly_one_dispatch(self) -> None:
        _assert_proof(exit_code=0, describe_calls=1)

        with self.assertRaisesRegex(RuntimeError, "worker exited 1"):
            _assert_proof(exit_code=1, describe_calls=1)
        with self.assertRaisesRegex(RuntimeError, "not dispatched exactly once"):
            _assert_proof(exit_code=0, describe_calls=0)
        with self.assertRaisesRegex(RuntimeError, "not dispatched exactly once"):
            _assert_proof(exit_code=0, describe_calls=2)


if __name__ == "__main__":
    unittest.main()
