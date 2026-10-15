from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from magic_glass_box.db import Database
from magic_glass_box.inference import InferenceConnectorSpec, InferenceGrantSpec
from magic_glass_box.service import ControlPlane


def connector_spec(socket_path: Path) -> InferenceConnectorSpec:
    return InferenceConnectorSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_protocol_version": 1,
            "deployment_sha256": "a" * 64,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "routes": [
                {
                    "name": "chatgpt-main",
                    "provider": "openai",
                    "protocol": "openai-responses",
                    "models": ["gpt-test"],
                    "credential_scope_sha256": "b" * 64,
                }
            ],
            "request_bytes": 32768,
            "response_bytes": 65536,
            "timeout_ms": 30000,
            "max_concurrency": 2,
        }
    )


def grant_spec() -> InferenceGrantSpec:
    return InferenceGrantSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_name": "openai-personal",
            "deployment_sha256": "a" * 64,
            "route": "chatgpt-main",
            "credential_scope_sha256": "b" * 64,
            "provider": "openai",
            "protocol": "openai-responses",
            "model": "gpt-test",
            "request_bytes": 16384,
            "response_bytes": 32768,
            "timeout_ms": 10000,
            "max_concurrency": 1,
        }
    )


class InferenceConnectorServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.control = ControlPlane(Database(self.root / "control-plane.db"))
        self.control.initialize()
        self.control.create_user("operator", "Local Operator")

    def test_operator_registers_binds_and_revokes_without_a_credential(self) -> None:
        connector = self.control.register_inference_connector(
            name="openai-personal",
            spec=connector_spec(self.root / "openai.sock"),
            created_by_handle="operator",
        )
        grant = self.control.add_connector_provider_grant(
            name="openai-main",
            spec=grant_spec(),
            expires_at=None,
            created_by_handle="operator",
        )
        revoked = self.control.revoke_inference_connector(
            "openai-personal",
            expected_version=1,
            revoked_by_handle="operator",
        )

        self.assertEqual(grant["connector"]["id"], connector["id"])
        self.assertNotIn("credential_ref", grant)
        self.assertEqual(revoked["state"], "revoked")


if __name__ == "__main__":
    unittest.main()
