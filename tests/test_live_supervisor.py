from __future__ import annotations

import unittest

from scripts.check_live_supervisor import _assert_proof, automatic_policy, run_proof


class LiveSupervisorProofContractTests(unittest.TestCase):
    def test_policy_requires_explicit_execution_and_bounded_thresholds(self) -> None:
        policy = automatic_policy()

        self.assertTrue(policy.auto_execute)
        self.assertEqual(policy.retry_backoff_seconds, (1, 5))
        self.assertEqual(policy.interaction_escalation_seconds, (10, 30))

    def test_live_durable_supervisor_proof_reaches_every_exit_condition(self) -> None:
        evidence = run_proof()

        _assert_proof(evidence)
        self.assertEqual(evidence["automatic_run_state"], "failed")
        self.assertEqual(evidence["answered_run_state"], "failed")
        self.assertEqual(evidence["manual_run_state"], "queued")
        self.assertEqual(evidence["reminder_thresholds"], [10, 30])

    def test_proof_verdict_rejects_duplicate_or_unsafe_advancement(self) -> None:
        evidence = run_proof()

        duplicate = {**evidence, "duplicate_action_subjects": True}
        with self.assertRaisesRegex(RuntimeError, "duplicate durable action"):
            _assert_proof(duplicate)

        live_recovered = {**evidence, "live_attempt_state": "failed"}
        with self.assertRaisesRegex(RuntimeError, "live attempt"):
            _assert_proof(live_recovered)

        missing_reminder = {**evidence, "reminder_thresholds": [10]}
        with self.assertRaisesRegex(RuntimeError, "escalation thresholds"):
            _assert_proof(missing_reminder)


if __name__ == "__main__":
    unittest.main()
