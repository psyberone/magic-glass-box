from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
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
