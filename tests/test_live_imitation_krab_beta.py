from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from magic_glass_box.db import ConflictError
from magic_glass_box.domain import GatewayCapability, WorkspaceMode
from scripts.check_live_imitation_krab_beta import (
    PRODUCER_PROFILE,
    SAME_IDENTITY_REVIEWER_PROFILE,
    _assert_role_separation,
    _connector_socket_path,
    _profile,
    _resume_failed_review,
    _review_outcome,
    _source_revision,
    _work_item,
    parse_args,
)


class LiveImitationKrabBetaContractTests(unittest.TestCase):
    def test_failed_bound_reviewer_resumes_as_an_audited_retry(self) -> None:
        failed = {
            "id": "run_failed",
            "state": "failed",
            "version": 5,
            "gate_policy_id": "gpl_one",
            "review_candidate_id": "cnd_one",
            "change_candidate_id": None,
        }
        candidate = {
            "id": "cnd_one",
            "producer_run_id": "run_producer",
            "head_revision": "a" * 40,
        }
        policy = {
            "id": "gpl_one",
            "candidate_id": "cnd_one",
            "head_revision": "a" * 40,
        }

        class Database:
            def initialize(self) -> None:
                return None

            def get_run(self, run_id: str) -> dict[str, object]:
                self.assert_value(run_id, "run_failed")
                return failed

            def get_gate_policy(self, policy_id: str) -> dict[str, object]:
                self.assert_value(policy_id, "gpl_one")
                return policy

            def get_candidate(self, candidate_id: str) -> dict[str, object]:
                self.assert_value(candidate_id, "cnd_one")
                return candidate

            def get_user(self, handle: str) -> dict[str, str]:
                self.assert_value(handle, "operator")
                return {"id": "boxu_operator"}

            def retry_run(self, run_id: str, **kwargs: object) -> dict[str, str]:
                self.assert_value(run_id, "run_failed")
                self.assert_value(kwargs["requested_by"], "boxu_operator")
                self.assert_value(kwargs["expected_run_version"], 5)
                return {"id": "run_retry"}

            def get_inference_connector(self, name: str) -> dict[str, str]:
                self.assert_value(name, "openai-chatgpt")
                return {"id": "ifc_one"}

            def get_agent_profile(self, name: str) -> dict[str, str]:
                return {"name": name}

            @staticmethod
            def assert_value(actual: object, expected: object) -> None:
                if actual != expected:
                    raise AssertionError(f"{actual!r} != {expected!r}")

        class Control:
            database = Database()

        review, connector, profiles, shown, shown_policy, run_ids = (
            _resume_failed_review(Control(), "run_failed")  # type: ignore[arg-type]
        )

        self.assertEqual(review["id"], "run_retry")
        self.assertEqual(connector["id"], "ifc_one")
        self.assertEqual(profiles["producer"]["name"], PRODUCER_PROFILE)
        self.assertIs(shown, candidate)
        self.assertIs(shown_policy, policy)
        self.assertEqual(run_ids, ["run_producer", "run_failed", "run_retry"])

    def test_connector_socket_path_is_rejected_before_state_is_created(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            long_root = Path(temporary) / ("nested-" * 20)
            with self.assertRaisesRegex(RuntimeError, "socket path"):
                _connector_socket_path(long_root)
            self.assertFalse(long_root.exists())

    def test_role_separation_preflight_requires_both_rejections(self) -> None:
        class Control:
            def __init__(self) -> None:
                self.profiles: list[str] = []

            def create_review_run(self, _policy_id: str, **kwargs: object) -> None:
                profile = str(kwargs["profile_name"])
                self.profiles.append(profile)
                if profile in {PRODUCER_PROFILE, SAME_IDENTITY_REVIEWER_PROFILE}:
                    raise ConflictError("separation denied")

        control = Control()
        result = _assert_role_separation(
            control,  # type: ignore[arg-type]
            {"id": "gpl_test", "policy_revision": "a" * 64},
        )

        self.assertEqual(
            result,
            {
                "producer_profile_rejected": True,
                "same_identity_profile_rejected": True,
            },
        )
        self.assertEqual(
            control.profiles,
            [PRODUCER_PROFILE, SAME_IDENTITY_REVIEWER_PROFILE],
        )

    def test_work_item_and_profiles_pin_the_beta_role_contract(self) -> None:
        work_item = _work_item()
        producer = _profile(
            image="runtime@sha256:" + "a" * 64,
            workspace_mode=WorkspaceMode.COPY_ON_WRITE,
        )
        reviewer = _profile(
            image="runtime@sha256:" + "a" * 64,
            workspace_mode=WorkspaceMode.READ_ONLY,
        )

        self.assertIn("krab health", work_item.brief)
        self.assertIn("without reading a token file", work_item.brief)
        self.assertIs(producer.workspace_mode, WorkspaceMode.COPY_ON_WRITE)
        self.assertIs(reviewer.workspace_mode, WorkspaceMode.READ_ONLY)
        self.assertIn(GatewayCapability.WORK_APPEND, reviewer.gateway_capabilities)
        self.assertEqual(
            producer.runtime_provider_grant, reviewer.runtime_provider_grant
        )

    def test_review_outcome_accepts_only_the_bounded_verdict_prefixes(self) -> None:
        self.assertEqual(
            _review_outcome({"summary": "APPROVED — looks good"}), "passed"
        )
        self.assertEqual(
            _review_outcome({"summary": "CHANGES REQUESTED: add a test"}), "failed"
        )
        for entry in ({}, {"summary": 7}, {"summary": "Looks fine"}):
            with self.subTest(entry=entry), self.assertRaises(RuntimeError):
                _review_outcome(entry)

    def test_source_revision_requires_a_clean_real_commit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repository = Path(temporary)
            git = shutil.which("git")
            self.assertIsNotNone(git)
            subprocess.run(  # noqa: S603 - discovered test-controlled Git binary
                [git, "init", "--quiet", "--initial-branch", "main"],
                cwd=repository,
                check=True,
            )
            (repository / "README.md").write_text("clean\n", encoding="utf-8")
            subprocess.run(  # noqa: S603 - discovered test-controlled Git binary
                [git, "add", "README.md"], cwd=repository, check=True
            )
            subprocess.run(  # noqa: S603 - discovered test-controlled Git binary
                [
                    git,
                    "-c",
                    "user.name=Beta Test",
                    "-c",
                    "user.email=beta@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "Initial",
                ],
                cwd=repository,
                check=True,
            )

            revision = _source_revision(repository, None)
            self.assertEqual(len(revision), 40)
            (repository / "README.md").write_text("dirty\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "clean worktree"):
                _source_revision(repository, revision)

    def test_live_quota_opt_in_is_explicit_and_defaults_are_bounded(self) -> None:
        arguments = parse_args(
            [
                "--profile-dir",
                "/private/profile",
                "--repository",
                "/source/krab",
                "--state-dir",
                "/state/beta",
                "--model",
                "gpt-test",
            ]
        )

        self.assertFalse(arguments.i_understand_this_uses_live_quota)
        self.assertEqual(arguments.max_invocations, 64)
        self.assertEqual(arguments.max_review_rounds, 2)
        self.assertIsNone(arguments.resume_review_run)


if __name__ == "__main__":
    unittest.main()
