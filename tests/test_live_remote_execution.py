from __future__ import annotations

import base64
import hashlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "check_live_remote_execution.py"
)


def load_live_proof_module():
    spec = importlib.util.spec_from_file_location(
        "check_live_remote_execution", SCRIPT_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("M15 live proof module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class M15LiveRemoteExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)

    def test_connection_config_is_private_and_exact(self) -> None:
        proof = load_live_proof_module()
        document = {
            "account": "runner",
            "git_binary": "/usr/bin/git",
            "host": "runner.example.test",
            "identity_file": "/private/runner_ed25519",
            "known_hosts_file": "/private/runner_known_hosts",
            "port": 22,
            "remote_helper": "/opt/mgb/bin/magic-glass-box-remote-runner",
            "schema_version": 1,
            "ssh_binary": "/usr/bin/ssh",
        }
        config = self.root / "connection.json"
        config.write_text(json.dumps(document), encoding="utf-8")
        config.chmod(0o600)

        self.assertEqual(proof.load_connection_config(config), document)

        config.chmod(0o644)
        with self.assertRaisesRegex(proof.LiveProofError, "private"):
            proof.load_connection_config(config)

    def test_scan_reports_labels_without_returning_sensitive_values(self) -> None:
        proof = load_live_proof_module()
        clean = self.root / "clean"
        dirty = self.root / "dirty"
        clean.write_bytes(b"bounded normalized evidence")
        dirty.write_bytes(b"prefix-private-key-material-suffix")

        self.assertEqual(
            proof.scan_forbidden(
                (clean,),
                {"identity_file": b"private-key-material"},
            ),
            (),
        )
        self.assertEqual(
            proof.scan_forbidden(
                (clean, dirty),
                {"identity_file": b"private-key-material"},
            ),
            ("identity_file",),
        )

    def test_success_result_requires_one_correlated_receipt(self) -> None:
        proof = load_live_proof_module()
        operation = {
            "state": "succeeded",
            "attempts": [
                {
                    "outcome": "succeeded",
                    "receipt": {"result": {"execution_id": "rex_" + "1" * 32}},
                }
            ],
        }

        self.assertEqual(
            proof.success_result(operation),
            {"execution_id": "rex_" + "1" * 32},
        )
        with self.assertRaisesRegex(proof.LiveProofError, "successful result"):
            proof.success_result({**operation, "state": "uncertain"})

    def test_cancelled_evidence_retains_its_empty_result_bundle(self) -> None:
        proof = load_live_proof_module()
        bundle = b"bounded empty result bundle"
        evidence = {
            "artifact_bundle": base64.b64encode(bundle).decode("ascii"),
            "artifact_bytes": len(bundle),
            "artifact_count": 0,
            "artifact_sha256": hashlib.sha256(bundle).hexdigest(),
            "execution": {"state": "cancelled"},
        }

        self.assertEqual(
            proof._assert_evidence(
                evidence,
                expected_state="cancelled",
                expect_artifact=False,
            ),
            {"state": "cancelled"},
        )

    def test_quality_evidence_requires_the_expected_proof_document(self) -> None:
        proof = load_live_proof_module()

        def evidence(summary: dict[str, object]) -> dict[str, object]:
            payload = (json.dumps(summary, sort_keys=True) + "\n").encode()
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode="w:gz") as archive:
                member = tarfile.TarInfo("m15-proof.json")
                member.size = len(payload)
                archive.addfile(member, io.BytesIO(payload))
            bundle = output.getvalue()
            return {
                "artifact_bundle": base64.b64encode(bundle).decode("ascii"),
                "artifact_bytes": len(bundle),
                "artifact_count": 1,
                "artifact_sha256": hashlib.sha256(bundle).hexdigest(),
                "execution": {"state": "succeeded"},
            }

        summary: dict[str, object] = {
            "boundary": {
                "docker_authority": "absent",
                "network_interfaces": ["lo"],
                "network_routes": "loopback-only",
                "root_filesystem": "read-only",
                "runtime_user": "non-root",
                "workspace": "read-only",
            },
            "python": "3.11.15",
            "schema_version": 1,
            "status": "passed",
            "test_count": 79,
            "test_modules": [
                "tests.test_execution_operations",
                "tests.test_execution_provider",
                "tests.test_execution_provider_cli",
                "tests.test_remote_runner",
            ],
        }

        self.assertEqual(
            proof._assert_evidence(
                evidence(summary),
                expected_state="succeeded",
                expect_artifact=True,
            ),
            {"state": "succeeded"},
        )
        with self.assertRaisesRegex(proof.LiveProofError, "quality artifact"):
            proof._assert_evidence(
                evidence({**summary, "status": "not-really-tested"}),
                expected_state="succeeded",
                expect_artifact=True,
            )

    def test_atomic_private_json_is_mode_0600(self) -> None:
        proof = load_live_proof_module()
        destination = self.root / "report.json"

        proof.write_private_json(destination, {"status": "passed"})

        self.assertEqual(
            json.loads(destination.read_text("utf-8")), {"status": "passed"}
        )
        self.assertEqual(os.stat(destination).st_mode & 0o777, 0o600)

    def test_candidate_matches_the_clean_head_tree_and_retains_a_real_change(
        self,
    ) -> None:
        proof = load_live_proof_module()
        git_binary = shutil.which("git")
        if git_binary is None:
            self.skipTest("Git is required")
        repository = self.root / "repository"
        repository.mkdir()

        def git(*arguments: str) -> str:
            completed = subprocess.run(  # noqa: S603 - test-controlled Git call.
                [git_binary, "-C", str(repository), *arguments],
                check=True,
                capture_output=True,
            )
            return completed.stdout.decode("utf-8", "strict").strip()

        git("init", "--quiet", "--initial-branch=main")
        git("config", "user.name", "M15 Test")
        git("config", "user.email", "m15@example.invalid")
        source = repository / "proof.txt"
        source.write_text("before\n", encoding="utf-8")
        git("add", "proof.txt")
        git("commit", "--quiet", "-m", "base")
        source.write_text("after\n", encoding="utf-8")
        git("add", "proof.txt")
        git("commit", "--quiet", "-m", "proof")

        _control, candidate, _store, scan_paths = proof._prepare_candidate(
            repository, self.root / "proof-state"
        )
        manifest = proof.CandidateManifest.from_mapping(candidate["manifest"])

        self.assertEqual(manifest.base_revision, git("rev-parse", "HEAD^"))
        self.assertEqual(manifest.tree_revision, git("rev-parse", "HEAD^{tree}"))
        self.assertIn("proof.txt", {path.name for path in scan_paths})


if __name__ == "__main__":
    unittest.main()
