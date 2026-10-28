from __future__ import annotations

import importlib.util
import io
import json
import tarfile
import unittest
from pathlib import Path

PROOF_PATH = Path(__file__).resolve().parent.parent / "docker" / "m15_remote_proof.py"


def load_proof_module():
    spec = importlib.util.spec_from_file_location("m15_remote_proof", PROOF_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("M15 proof image entrypoint could not be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class M15RemoteProofImageTests(unittest.TestCase):
    def test_forbidden_environment_is_detected_without_returning_values(self) -> None:
        proof = load_proof_module()

        self.assertEqual(
            proof.forbidden_environment(
                {
                    "HOME": "/tmp/home",  # noqa: S108 - inert test value.
                    "MGB_ARTIFACT_PATH": "/mgb-artifacts.tar.gz",
                    "OPENAI_API_KEY": "do-not-return-this",
                    "SSH_AUTH_SOCK": "/private/socket",
                }
            ),
            ("OPENAI_API_KEY", "SSH_AUTH_SOCK"),
        )

    def test_only_loopback_network_interface_is_accepted(self) -> None:
        proof = load_proof_module()
        loopback = (
            "Inter-|   Receive                              |  Transmit\n"
            " face |bytes packets errs drop fifo frame compressed multicast|"
            "bytes packets errs drop fifo colls carrier compressed\n"
            "    lo: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
        )

        self.assertEqual(proof.network_interfaces(loopback), ("lo",))
        self.assertEqual(
            proof.network_interfaces(
                loopback + " tunl0: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
            ),
            ("lo", "tunl0"),
        )
        with self.assertRaisesRegex(proof.ProofError, "network interface"):
            proof.network_interfaces(
                loopback + "  eth0: 1 1 0 0 0 0 0 0 1 1 0 0 0 0 0 0\n"
            )

    def test_network_routes_must_be_empty_or_loopback_only(self) -> None:
        proof = load_proof_module()
        ipv4 = (
            "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\n"
        )
        ipv6 = (
            "00000000000000000000000000000001 80 "
            "00000000000000000000000000000000 00 "
            "00000000000000000000000000000000 00000000 00000002 "
            "00000000 80200001 lo\n"
        )

        proof.assert_loopback_routes(ipv4, ipv6)
        with self.assertRaisesRegex(proof.ProofError, "network route"):
            proof.assert_loopback_routes(ipv4 + "eth0 00000000 0100000A\n", ipv6)

    def test_evidence_archive_is_deterministic_and_bounded(self) -> None:
        proof = load_proof_module()
        summary = {
            "boundary": {"network": "loopback-only", "workspace": "read-only"},
            "schema_version": 1,
            "status": "passed",
        }

        first = proof.evidence_archive(summary)
        second = proof.evidence_archive(summary)

        self.assertEqual(first, second)
        self.assertLessEqual(len(first), proof.MAX_EVIDENCE_BYTES)
        with tarfile.open(fileobj=io.BytesIO(first), mode="r:gz") as archive:
            members = archive.getmembers()
            self.assertEqual([member.name for member in members], ["m15-proof.json"])
            self.assertEqual(members[0].mtime, 0)
            extracted = archive.extractfile(members[0])
            self.assertIsNotNone(extracted)
            assert extracted is not None
            self.assertEqual(
                json.loads(extracted.read().decode("utf-8", "strict")), summary
            )

    def test_test_command_is_fixed_to_the_m15_slice(self) -> None:
        proof = load_proof_module()

        command = proof.test_command(Path("/workspace"))

        self.assertEqual(command[0], proof.sys.executable)
        self.assertEqual(command[1:3], ("-m", "unittest"))
        self.assertEqual(
            command[3:],
            (
                "tests.test_execution_operations",
                "tests.test_execution_provider",
                "tests.test_execution_provider_cli",
                "tests.test_remote_runner",
                "-v",
            ),
        )


if __name__ == "__main__":
    unittest.main()
