from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.check_live_imitation_krab_maintainer_round import (
    _assert_pending_maintainer,
    _gate_request,
    _load_changes_requested,
    _project_context,
    parse_args,
)


class LiveImitationKrabMaintainerRoundContractTests(unittest.TestCase):
    def test_project_context_durably_states_identity_boundaries_and_fit(self) -> None:
        context = _project_context(Path("/source/imitation-krab"))

        self.assertEqual(context.repository_path, "/source/imitation-krab")
        self.assertIn("loopback-first coordination service", context.purpose)
        instructions = "\n".join(context.instructions)
        self.assertIn("server-attested metadata", instructions)
        self.assertIn("not an agent runtime", instructions)
        self.assertIn("smallest Krab-native primitive", instructions)
        self.assertIn("85% branch-aware coverage", instructions)

    def test_review_input_is_bounded_and_must_request_actionable_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "review.json"
            document = {
                "schema_version": 1,
                "decision": "changes_requested",
                "summary": "Four bounded corrections are required.",
                "details": ["Publish the JSON output schema."],
            }
            path.write_text(json.dumps(document), encoding="utf-8")

            self.assertEqual(_load_changes_requested(path).to_dict(), document)

            path.write_text(
                json.dumps({**document, "decision": "approved"}), encoding="utf-8"
            )
            with self.assertRaisesRegex(RuntimeError, "changes_requested"):
                _load_changes_requested(path)

            path.write_bytes(b"{" + b" " * (64 * 1024))
            with self.assertRaisesRegex(RuntimeError, "too large"):
                _load_changes_requested(path)

    def test_gate_requires_independent_review_validation_and_maintainer(self) -> None:
        request = _gate_request("runtime-proof:local")

        self.assertTrue(request["maintainer_approval"])
        self.assertEqual(request["review_separation"], "different_profile_and_identity")
        self.assertEqual(request["validations"][0]["name"], "full-suite")

    def test_completion_requires_only_the_maintainer_gate_to_be_pending(self) -> None:
        readiness = {
            "ready": False,
            "gates": [
                {"name": "independent-review", "state": "passed"},
                {"name": "maintainer-approval", "state": "pending"},
                {"name": "validation:full-suite", "state": "passed"},
            ],
        }

        _assert_pending_maintainer(readiness)

        for invalid in (
            {**readiness, "ready": True},
            {
                "ready": False,
                "gates": [
                    {"name": "independent-review", "state": "failed"},
                    {"name": "maintainer-approval", "state": "pending"},
                    {"name": "validation:full-suite", "state": "passed"},
                ],
            },
            {
                "ready": False,
                "gates": [
                    {"name": "independent-review", "state": "passed"},
                    {"name": "maintainer-approval", "state": "passed"},
                    {"name": "validation:full-suite", "state": "passed"},
                ],
            },
        ):
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                _assert_pending_maintainer(invalid)

    def test_live_quota_opt_in_is_explicit(self) -> None:
        arguments = parse_args(
            [
                "--profile-dir",
                "/private/profile",
                "--repository",
                "/source/krab",
                "--state-dir",
                "/state/beta",
                "--candidate",
                "cnd_" + "a" * 32,
                "--review",
                "/private/review.json",
                "--model",
                "gpt-test",
            ]
        )

        self.assertFalse(arguments.i_understand_this_uses_live_quota)
        self.assertEqual(arguments.max_invocations, 64)
        self.assertEqual(arguments.max_review_rounds, 2)


if __name__ == "__main__":
    unittest.main()
