from __future__ import annotations

import json
import os
import shutil
import socket
import struct
import subprocess
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from magic_glass_box.domain import canonical_json
from magic_glass_box.service_sidecar import ServiceSidecarAmbiguousError
from scripts.check_live_github_provider import (
    ACKNOWLEDGEMENT_FLAG,
    CandidateArtifact,
    ProofEvidence,
    _assert_proof,
    _build_candidate,
    _operation_request,
    _provider_environment,
    _require_success,
    _send_without_response,
    _service_sidecar_spec,
    _verify_source_repository,
    parse_args,
)

BASE = "a" * 40
HEAD = "b" * 40
DEPLOYMENT = "c" * 64
SCOPE = "d" * 64


def run_git(*argv: str, cwd: Path | None = None) -> str:
    result = subprocess.run(  # noqa: S603
        [shutil.which("git") or "git", *argv],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


class LiveGitHubProviderProofTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="mgb-live-github-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.config_dir = self.root / "gh-config"
        self.config_dir.mkdir(mode=0o700)
        self.source = self.root / "source"
        self.source.mkdir()
        run_git("init", "--initial-branch=main", cwd=self.source)
        run_git("config", "user.name", "MGB Test", cwd=self.source)
        run_git("config", "user.email", "mgb-test@example.invalid", cwd=self.source)
        (self.source / "README.md").write_text("# Fixture\n", encoding="utf-8")
        run_git("add", "README.md", cwd=self.source)
        run_git("commit", "-m", "Initial fixture", cwd=self.source)
        run_git(
            "remote",
            "add",
            "origin",
            "https://github.com/acme/widgets.git",
            cwd=self.source,
        )
        self.base = run_git("rev-parse", "HEAD", cwd=self.source)

    def valid_argv(self) -> list[str]:
        return [
            "--gh-config-dir",
            str(self.config_dir),
            "--repository",
            "acme/widgets",
            "--source-repository",
            str(self.source),
            "--expected-base-revision",
            self.base,
            "--base-ref",
            "refs/heads/main",
            "--proof-id",
            "m11-20261021",
            "--expected-account",
            "mgb-operator",
            ACKNOWLEDGEMENT_FLAG,
        ]

    def test_parser_requires_exact_destructive_authority(self) -> None:
        parsed = parse_args(self.valid_argv())

        self.assertEqual(parsed.repository, "acme/widgets")
        self.assertEqual(parsed.expected_base_revision, self.base)
        self.assertEqual(parsed.base_ref, "refs/heads/main")
        self.assertEqual(parsed.proof_id, "m11-20261021")
        self.assertEqual(parsed.expected_account, "mgb-operator")
        self.assertTrue(parsed.acknowledged)

        invalid_cases = (
            (self.valid_argv()[:-1], "acknowledgment"),
            (
                [
                    "psyberone/magic-glass-box" if item == "acme/widgets" else item
                    for item in self.valid_argv()
                ],
                "protected",
            ),
            (
                [
                    "not-a-revision" if item == self.base else item
                    for item in self.valid_argv()
                ],
                "revision",
            ),
            (
                [
                    "refs/tags/main" if item == "refs/heads/main" else item
                    for item in self.valid_argv()
                ],
                "branch",
            ),
            (
                [
                    "refs/heads/main..bad" if item == "refs/heads/main" else item
                    for item in self.valid_argv()
                ],
                "branch",
            ),
            (
                [
                    "UPPER" if item == "m11-20261021" else item
                    for item in self.valid_argv()
                ],
                "proof",
            ),
        )
        for argv, message in invalid_cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValueError, message),
            ):
                parse_args(argv)

    def test_source_verification_requires_clean_exact_clone(self) -> None:
        resolved = _verify_source_repository(
            self.source,
            expected_base_revision=self.base,
            base_ref="refs/heads/main",
            host="github.com",
            repository="acme/widgets",
            git_bin=Path(shutil.which("git") or "git").resolve(),
        )
        self.assertEqual(resolved, self.source.resolve())

        (self.source / "dirty.txt").write_text("dirty\n", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "clean"):
            _verify_source_repository(
                self.source,
                expected_base_revision=self.base,
                base_ref="refs/heads/main",
                host="github.com",
                repository="acme/widgets",
                git_bin=Path(shutil.which("git") or "git").resolve(),
            )
        (self.source / "dirty.txt").unlink()

        with self.assertRaisesRegex(RuntimeError, "remote"):
            _verify_source_repository(
                self.source,
                expected_base_revision=self.base,
                base_ref="refs/heads/main",
                host="github.com",
                repository="acme/other",
                git_bin=Path(shutil.which("git") or "git").resolve(),
            )

    def test_candidate_is_a_child_commit_in_the_private_candidate_store(self) -> None:
        candidate_root = self.root / "candidates"
        candidate_root.mkdir(mode=0o700)

        candidate = _build_candidate(
            candidate_root=candidate_root,
            source_repository=self.source,
            expected_base_revision=self.base,
            proof_id="m11-20261021",
            git_bin=Path(shutil.which("git") or "git").resolve(),
        )

        self.assertTrue(candidate.repository.is_relative_to(candidate_root.resolve()))
        self.assertEqual(candidate.proof_path, "docs/mgb-live-proof-m11-20261021.md")
        self.assertRegex(candidate.revision, r"^[0-9a-f]{40}$")
        self.assertEqual(
            run_git(
                f"--git-dir={candidate.repository}",
                "rev-parse",
                f"{candidate.revision}^",
            ),
            self.base,
        )
        self.assertEqual(
            run_git(
                f"--git-dir={candidate.repository}",
                "show-ref",
                "--hash",
                f"refs/mgb/candidates/{candidate.revision}",
            ),
            candidate.revision,
        )
        proof = run_git(
            f"--git-dir={candidate.repository}",
            "show",
            f"{candidate.revision}:{candidate.proof_path}",
        )
        self.assertIn("Magic Glass Box live GitHub proof", proof)
        self.assertIn(self.base, proof)
        self.assertFalse((self.source / candidate.proof_path).exists())

        second_root = self.root / "second-candidates"
        second_root.mkdir(mode=0o700)
        rebuilt = _build_candidate(
            candidate_root=second_root,
            source_repository=self.source,
            expected_base_revision=self.base,
            proof_id="m11-20261021",
            git_bin=Path(shutil.which("git") or "git").resolve(),
        )
        self.assertEqual(rebuilt.revision, candidate.revision)

    def test_spec_and_operations_pin_authority_without_credentials(self) -> None:
        socket_path = self.root / "github.sock"
        spec = _service_sidecar_spec(
            socket_path=socket_path,
            repository="acme/widgets",
            account="mgb-operator",
            credential_scope_sha256=SCOPE,
            deployment_sha256=DEPLOYMENT,
        )
        candidate = CandidateArtifact(
            repository=self.root / "candidates" / "repository.git",
            revision=HEAD,
            proof_path="docs/mgb-live-proof-proof.md",
        )
        request = _operation_request(
            spec=spec,
            proof_id="proof",
            capability="refs.publish",
            phase="invoke",
            resource_id="refs/heads/mgb/live-proof-proof",
            object_format="sha1",
            expected_revision=None,
            desired_revision=HEAD,
            parameters={"source_repository": str(candidate.repository)},
        )

        self.assertEqual(request.mapping.repository, "acme/widgets")
        self.assertEqual(request.mapping.account, "mgb-operator")
        self.assertEqual(request.mapping.credential_scope_sha256, SCOPE)
        self.assertEqual(request.target.desired_revision, HEAD)
        self.assertEqual(request.phase, "invoke")
        serialized = canonical_json(request.to_dict())
        self.assertNotIn(str(self.config_dir), serialized)
        self.assertNotIn("token", serialized.casefold())

        environment = _provider_environment(Path("/project/src"))
        self.assertEqual(set(environment), {"PATH", "PYTHONPATH", "PYTHONUNBUFFERED"})
        self.assertNotIn("GH_TOKEN", environment)
        self.assertNotIn("GITHUB_TOKEN", environment)

    def test_send_without_response_dispatches_one_complete_frame(self) -> None:
        socket_path = self.root / "lost.sock"
        spec = _service_sidecar_spec(
            socket_path=socket_path,
            repository="acme/widgets",
            account="mgb-operator",
            credential_scope_sha256=SCOPE,
            deployment_sha256=DEPLOYMENT,
        )
        request = _operation_request(
            spec=spec,
            proof_id="proof",
            capability="landing.invoke",
            phase="invoke",
            resource_id="42",
            object_format="sha1",
            expected_revision=BASE,
            desired_revision=HEAD,
            parameters={
                "assurance": "head_enforced",
                "integration_ref": "refs/heads/main",
                "merge_method": "squash",
            },
        )
        received: list[bytes] = []
        ready = threading.Event()
        closed = threading.Event()

        def server() -> None:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(socket_path))
                os.chmod(socket_path, 0o600)
                listener.listen(1)
                ready.set()
                connection, _ = listener.accept()
                with connection:
                    length = struct.unpack("!I", connection.recv(4))[0]
                    payload = bytearray()
                    while len(payload) < length:
                        payload.extend(connection.recv(length - len(payload)))
                    received.append(bytes(payload))
                    closed.set()

        thread = threading.Thread(target=server, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(1))

        _send_without_response(socket_path, request)

        self.assertTrue(closed.wait(1))
        thread.join(timeout=1)
        self.assertEqual(json.loads(received[0]), request.to_dict())

    def test_transport_failure_names_only_the_failed_capability(self) -> None:
        spec = _service_sidecar_spec(
            socket_path=self.root / "github.sock",
            repository="acme/widgets",
            account="mgb-operator",
            credential_scope_sha256=SCOPE,
            deployment_sha256=DEPLOYMENT,
        )
        request = _operation_request(
            spec=spec,
            proof_id="proof",
            capability="change.get",
            phase="invoke",
            resource_id="42",
            object_format=None,
            expected_revision=None,
            desired_revision=None,
            parameters={},
        )

        class FailingClient:
            @staticmethod
            def invoke(_request: object) -> object:
                raise ServiceSidecarAmbiguousError("private transport detail")

        with self.assertRaisesRegex(RuntimeError, "change.get transport failed"):
            _require_success(FailingClient(), request)  # type: ignore[arg-type]

    def test_proof_verdict_is_bounded_and_rejects_wrong_settlement(self) -> None:
        identity = "github:github.com/acme/widgets/pull/42"
        evidence = ProofEvidence(
            host="github.com",
            repository="acme/widgets",
            account="mgb-operator",
            credential_scope_sha256=SCOPE,
            base_ref="refs/heads/main",
            expected_base_revision=BASE,
            candidate_revision=HEAD,
            branch_ref="refs/heads/mgb/live-proof-proof",
            work_items_observed=3,
            pull_number=42,
            change_remote_identity=identity,
            change_head_revision=HEAD,
            change_list_contains_pull=True,
            checks_revision=HEAD,
            landed_revision="e" * 40,
            landing_remote_identity=identity,
            landing_closed_references=(identity,),
            outcomes={
                "work.list": "succeeded",
                "refs.publish": "succeeded",
                "change.open": "succeeded",
                "change.get": "succeeded",
                "change.list": "succeeded",
                "checks.read": "succeeded",
                "landing.invoke": "response_intentionally_discarded",
                "landing.reconcile": "succeeded",
            },
        )

        report = _assert_proof(evidence)

        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["pull_number"], 42)
        self.assertEqual(report["landed_revision"], "e" * 40)
        self.assertEqual(report["bounded_reads"], {"work_items_observed": 3})
        self.assertNotIn("title", canonical_json(report).casefold())

        invalid = (
            replace(evidence, change_head_revision=BASE),
            replace(evidence, change_list_contains_pull=False),
            replace(evidence, checks_revision=BASE),
            replace(evidence, landed_revision=BASE),
            replace(evidence, landing_closed_references=()),
            replace(
                evidence,
                outcomes=evidence.outcomes | {"checks.read": "retryable"},
            ),
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                _assert_proof(value)


if __name__ == "__main__":
    unittest.main()
