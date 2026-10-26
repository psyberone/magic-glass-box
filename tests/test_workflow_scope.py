from __future__ import annotations

import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError

from magic_glass_box.domain import ValidationError, WorkLogReviewOutcome
from magic_glass_box.workflow_scope import (
    ScopeProbeClassification,
    WorkflowScopeReview,
)


def scope_review_value(**overrides: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "must_results": [
            {
                "key": "bounded-read",
                "outcome": "passed",
                "summary": "The response read remains bounded.",
                "evidence": ["The focused timeout and byte-limit tests pass."],
            }
        ],
        "probes": [
            {
                "key": "new-timeout-gap",
                "summary": "The candidate introduces one unbounded timeout path.",
                "base_finding": "absent",
                "head_finding": "present",
                "evidence": ["The probe fails only at the candidate head."],
            },
            {
                "key": "existing-help-gap",
                "summary": "The help text was already inconsistent.",
                "base_finding": "present",
                "head_finding": "present",
                "evidence": ["The same probe fails at base and head."],
            },
            {
                "key": "fixed-parser-gap",
                "summary": "The candidate closes a pre-existing parser gap.",
                "base_finding": "present",
                "head_finding": "absent",
                "evidence": ["The probe fails at base and passes at head."],
            },
            {
                "key": "clean-ownership-path",
                "summary": "No ownership regression is observable.",
                "base_finding": "absent",
                "head_finding": "absent",
                "evidence": ["The ownership probe passes at base and head."],
            },
        ],
        **overrides,
    }


class WorkflowScopeReviewTests(unittest.TestCase):
    def test_classifies_probe_matrix_and_derives_the_only_valid_verdict(self) -> None:
        review = WorkflowScopeReview.from_mapping(scope_review_value())

        self.assertEqual(
            [probe.classification for probe in review.probes],
            [
                ScopeProbeClassification.REGRESSION,
                ScopeProbeClassification.FOLLOW_UP,
                ScopeProbeClassification.RESOLVED,
                ScopeProbeClassification.CLEAN,
            ],
        )
        self.assertEqual(review.failed_must_keys, ())
        self.assertEqual(review.regression_probe_keys, ("new-timeout-gap",))
        self.assertEqual(review.follow_up_probe_keys, ("existing-help-gap",))
        self.assertTrue(review.blocking)
        self.assertIs(
            review.expected_review_outcome,
            WorkLogReviewOutcome.CHANGES_REQUESTED,
        )
        self.assertEqual(json.loads(review.canonical), review.to_dict())
        self.assertEqual(len(review.digest), 64)

        clean = WorkflowScopeReview.from_mapping(
            scope_review_value(
                probes=[],
                must_results=[
                    {
                        "key": "bounded-read",
                        "outcome": "passed",
                        "summary": "The response read remains bounded.",
                        "evidence": ["The focused safety tests pass."],
                    }
                ],
            )
        )
        self.assertFalse(clean.blocking)
        self.assertIs(clean.expected_review_outcome, WorkLogReviewOutcome.APPROVED)

        failed_must = WorkflowScopeReview.from_mapping(
            scope_review_value(
                probes=[],
                must_results=[
                    {
                        "key": "bounded-read",
                        "outcome": "failed",
                        "summary": "The bounded read can be bypassed.",
                        "evidence": ["The required sad-path check still fails."],
                    }
                ],
            )
        )
        self.assertEqual(failed_must.failed_must_keys, ("bounded-read",))
        self.assertTrue(failed_must.blocking)

    def test_rejects_unbounded_ambiguous_or_reviewer_classified_input(self) -> None:
        must = scope_review_value()["must_results"][0]  # type: ignore[index]
        probe = scope_review_value()["probes"][0]  # type: ignore[index]
        missing = scope_review_value()
        missing.pop("probes")
        cases = (
            None,
            {**scope_review_value(), "unexpected": True},
            missing,
            scope_review_value(schema_version=True),
            scope_review_value(schema_version=2),
            scope_review_value(must_results=[]),
            scope_review_value(must_results=[must] * 6),
            scope_review_value(must_results=[must, must]),
            scope_review_value(must_results=[{**must, "key": "NOT VALID"}]),
            scope_review_value(must_results=[{**must, "outcome": "waived"}]),
            scope_review_value(must_results=[{**must, "summary": "   "}]),
            scope_review_value(must_results=[{**must, "evidence": []}]),
            scope_review_value(must_results=[{**must, "evidence": ["same", "same"]}]),
            scope_review_value(must_results=[{**must, "evidence": ["e" * 2049]}]),
            scope_review_value(probes=[probe] * 2),
            scope_review_value(probes=[probe] * 17),
            scope_review_value(probes=[{**probe, "base_finding": True}]),
            scope_review_value(probes=[{**probe, "head_finding": "unknown"}]),
            scope_review_value(probes=[{**probe, "classification": "follow_up"}]),
            scope_review_value(probes=[{**probe, "evidence": []}]),
            scope_review_value(
                must_results=[must],
                probes=[{**probe, "key": must["key"]}],  # type: ignore[index]
            ),
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                WorkflowScopeReview.from_mapping(value)  # type: ignore[arg-type]

        oversized_musts = [
            {
                "key": f"must-{index}",
                "outcome": "passed",
                "summary": "s" * 4096,
                "evidence": [f"{index}-{slot}-" + "e" * 2040 for slot in range(8)],
            }
            for index in range(5)
        ]
        with self.assertRaisesRegex(ValidationError, "65536 UTF-8 bytes"):
            WorkflowScopeReview.from_mapping(
                scope_review_value(must_results=oversized_musts, probes=[])
            )

    def test_published_schema_accepts_contract_and_rejects_derived_authority(
        self,
    ) -> None:
        repository = Path(__file__).resolve().parents[1]
        schema = json.loads(
            (repository / "schemas/workflow-scope-review-v1.schema.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        value = scope_review_value()

        validator.validate(value)
        with self.assertRaises(SchemaValidationError):
            validator.validate({**value, "gate_outcome": "passed"})
        with self.assertRaises(SchemaValidationError):
            validator.validate(
                {
                    **value,
                    "probes": [
                        {
                            **value["probes"][0],  # type: ignore[index]
                            "classification": "follow_up",
                        }
                    ],
                }
            )


if __name__ == "__main__":
    unittest.main()
