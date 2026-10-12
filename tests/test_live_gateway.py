from __future__ import annotations

import unittest

from magic_glass_box.domain import GatewayCapability
from magic_glass_box.gateway import RunGateway
from scripts.check_live_gateway import RUN_ID, ProofControl


class LiveGatewayProofContractTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
