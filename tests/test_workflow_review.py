from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError

from magic_glass_box.domain import ValidationError
from magic_glass_box.reviewer_measurement import (
    MAX_MEASUREMENT_ARGUMENTS,
    MAX_MEASUREMENT_COMPARISON_RULES,
    MAX_MEASUREMENT_COUNT,
    MAX_MEASUREMENT_DURATION_MS,
    MAX_MEASUREMENT_OUTPUT_BYTES,
    MAX_MEASUREMENT_TOOL_OBSERVATIONS,
    ReviewerMeasurement,
)
from magic_glass_box.workflow_rounds import WorkflowRoundCause


def exact_inputs() -> dict[str, object]:
    return {
        "policy_id": "gpl_" + "1" * 32,
        "candidate_id": "cnd_" + "2" * 32,
        "repository_identity": "boxp_" + "3" * 32,
        "integration_ref": "refs/heads/main",
        "expected_base_revision": "4" * 40,
        "head_revision": "5" * 40,
        "tree_revision": "6" * 40,
        "object_format": "sha1",
        "policy_revision": "7" * 64,
    }


def measurement_value() -> dict[str, object]:
    return {
        "schema_version": 1,
        "inputs": exact_inputs(),
        "attempt_id": "att_" + "8" * 32,
        "name": "focused-tests",
        "command": ["python", "-m", "pytest", "tests/test_exact.py"],
        "architecture": "arm64",
        "code_origin": "/workspace/src/magic_glass_box",
        "code_origin_sha256": "9" * 64,
        "interpreter": "CPython 3.13.7",
        "tool_observations": [
            {"name": "pytest", "status": "present", "version": "pytest 9.0.1"},
            {"name": "pytest-xdist", "status": "absent", "version": None},
        ],
        "exit_code": 0,
        "duration_ms": 321,
        "collected_count": 3,
        "executed_count": 3,
        "output_sha256": "a" * 64,
        "output_bytes": 1234,
        "result_field_semantics": {
            "exit_code": "process exit status from the exact argv",
            "collected_count": "tests selected by the exact pytest command",
            "executed_count": "selected tests that reached an outcome",
            "output_sha256": "SHA-256 of combined stdout and stderr bytes",
        },
        "comparison_rules": ["normalize generated timestamps"],
    }


def reviewer_cause_value() -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "reviewer_finding",
        "finding_classes": ["regression", "scope_must_failure"],
        "policy_id": "gpl_" + "1" * 32,
        "reviewer_run_id": "run_" + "2" * 32,
        "reviewer_attempt_id": "att_" + "3" * 32,
        "review_evidence_id": "gev_" + "4" * 32,
        "review_entry_id": "wle_" + "5" * 32,
        "scope_review_id": "wsr_" + "6" * 32,
        "maintainer_review_id": None,
        "gate_name": None,
    }


def gate_cause_value() -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "gate_error",
        "finding_classes": ["gate_error"],
        "policy_id": "gpl_" + "1" * 32,
        "reviewer_run_id": None,
        "reviewer_attempt_id": None,
        "review_evidence_id": None,
        "review_entry_id": None,
        "scope_review_id": None,
        "maintainer_review_id": None,
        "gate_name": "focused_tests",
    }


class ReviewerMeasurementTests(unittest.TestCase):
    def test_round_trips_one_exact_supplemental_measurement(self) -> None:
        measurement = ReviewerMeasurement.from_mapping(measurement_value())

        self.assertEqual(measurement.to_dict(), measurement_value())
        self.assertEqual(len(measurement.digest), 64)

    def test_rejects_ambiguous_or_unbounded_measurement_facts(self) -> None:
        cases = (
            {**measurement_value(), "provider_token": "secret"},
            {**measurement_value(), "command": []},
            {**measurement_value(), "executed_count": 4},
            {**measurement_value(), "code_origin_sha256": "not-a-digest"},
            {**measurement_value(), "comparison_rules": ["x"] * 17},
            {**measurement_value(), "attempt_id": None},
            {**measurement_value(), "architecture": None},
            {**measurement_value(), "exit_code": True},
            {**measurement_value(), "collected_count": 0, "executed_count": 0},
            {**measurement_value(), "executed_count": 0},
            {**measurement_value(), "tool_observations": []},
            {
                **measurement_value(),
                "tool_observations": [
                    {"name": "pytest", "status": "present", "version": None}
                ],
            },
            {
                **measurement_value(),
                "tool_observations": [
                    {"name": "pytest", "status": [], "version": "pytest 9.0.1"}
                ],
            },
            {
                **measurement_value(),
                "result_field_semantics": {
                    "exit_code": "process status",
                    "collected_count": "selected tests",
                    "executed_count": "executed tests",
                },
            },
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                ReviewerMeasurement.from_mapping(value)

    def test_enforces_exact_measurement_boundaries_and_canonical_tools(self) -> None:
        bounded = measurement_value()
        bounded.update(
            {
                "command": ["python"] * MAX_MEASUREMENT_ARGUMENTS,
                "comparison_rules": [
                    f"comparison-{index:02d}"
                    for index in range(MAX_MEASUREMENT_COMPARISON_RULES)
                ],
                "tool_observations": [
                    {
                        "name": f"tool-{index:02d}",
                        "status": "present" if index == 0 else "absent",
                        "version": "1.0" if index == 0 else None,
                    }
                    for index in range(MAX_MEASUREMENT_TOOL_OBSERVATIONS)
                ],
                "exit_code": 255,
                "duration_ms": MAX_MEASUREMENT_DURATION_MS,
                "collected_count": MAX_MEASUREMENT_COUNT,
                "executed_count": MAX_MEASUREMENT_COUNT,
                "output_bytes": MAX_MEASUREMENT_OUTPUT_BYTES,
            }
        )
        self.assertEqual(
            ReviewerMeasurement.from_mapping(bounded).to_dict(),
            bounded,
        )
        one_argument = {**measurement_value(), "command": ["python"]}
        self.assertEqual(
            ReviewerMeasurement.from_mapping(one_argument).command,
            ("python",),
        )
        zero_sized_result = {
            **measurement_value(),
            "duration_ms": 0,
            "output_bytes": 0,
        }
        self.assertEqual(
            ReviewerMeasurement.from_mapping(zero_sized_result).to_dict(),
            zero_sized_result,
        )

        invalid = []
        for field in (
            "duration_ms",
            "collected_count",
            "executed_count",
            "output_bytes",
        ):
            invalid.append({**measurement_value(), field: True})
        invalid.extend(
            (
                {**measurement_value(), "schema_version": True},
                {
                    **measurement_value(),
                    "command": ["python"] * (MAX_MEASUREMENT_ARGUMENTS + 1),
                },
                {**measurement_value(), "command": ["python\0pytest"]},
                {**measurement_value(), "comparison_rules": "abc"},
                {**measurement_value(), "name": []},
                {**measurement_value(), "code_origin": "x" * 1025},
                {**measurement_value(), "interpreter": "x" * 257},
                {
                    **measurement_value(),
                    "tool_observations": [
                        {"name": 7, "status": "present", "version": "1.0"}
                    ],
                },
                {
                    **measurement_value(),
                    "tool_observations": [
                        {"name": "pytest", "status": "present", "version": "1"},
                        {"name": "pytest", "status": "absent", "version": None},
                    ],
                },
                {
                    **measurement_value(),
                    "tool_observations": [
                        {"name": "z-tool", "status": "present", "version": "1"},
                        {"name": "a-tool", "status": "absent", "version": None},
                    ],
                },
                {
                    **measurement_value(),
                    "tool_observations": [
                        {"name": "pytest", "status": "absent", "version": None}
                    ],
                },
                {
                    **measurement_value(),
                    "tool_observations": [
                        {
                            "name": f"tool-{index:02d}",
                            "status": "present" if index == 0 else "absent",
                            "version": "1.0" if index == 0 else None,
                        }
                        for index in range(MAX_MEASUREMENT_TOOL_OBSERVATIONS + 1)
                    ],
                },
                {
                    **measurement_value(),
                    "tool_observations": [
                        {
                            "name": "pytest",
                            "status": "present",
                            "version": "x" * 257,
                        }
                    ],
                },
                {**measurement_value(), "exit_code": "0"},
                {**measurement_value(), "exit_code": 256},
                {**measurement_value(), "output_sha256": ["a" * 64]},
                {
                    **measurement_value(),
                    "result_field_semantics": {
                        **measurement_value()["result_field_semantics"],
                        "exit_code": "x" * 1025,
                    },
                },
                {
                    **measurement_value(),
                    "duration_ms": MAX_MEASUREMENT_DURATION_MS + 1,
                },
                {
                    **measurement_value(),
                    "output_bytes": MAX_MEASUREMENT_OUTPUT_BYTES + 1,
                },
            )
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                ReviewerMeasurement.from_mapping(copy.deepcopy(value))


class WorkflowRoundCauseTests(unittest.TestCase):
    def test_round_trips_reviewer_and_maintainer_causes(self) -> None:
        reviewer = WorkflowRoundCause.from_mapping(reviewer_cause_value())
        self.assertEqual(reviewer.to_dict(), reviewer_cause_value())

        maintainer_value = {
            "schema_version": 1,
            "kind": "maintainer_request",
            "finding_classes": ["product_disposition"],
            "policy_id": "gpl_" + "1" * 32,
            "reviewer_run_id": None,
            "reviewer_attempt_id": None,
            "review_evidence_id": None,
            "review_entry_id": None,
            "scope_review_id": None,
            "maintainer_review_id": "mrv_" + "7" * 32,
            "gate_name": None,
        }
        self.assertEqual(
            WorkflowRoundCause.from_mapping(maintainer_value).to_dict(),
            maintainer_value,
        )
        self.assertEqual(
            WorkflowRoundCause.from_mapping(gate_cause_value()).to_dict(),
            gate_cause_value(),
        )

        maximum_classes = {
            **gate_cause_value(),
            "finding_classes": [f"finding_{index:02d}" for index in range(16)],
        }
        self.assertEqual(
            WorkflowRoundCause.from_mapping(maximum_classes).to_dict(),
            maximum_classes,
        )

    def test_rejects_a_cause_without_exact_source_provenance(self) -> None:
        missing_gate_name = reviewer_cause_value()
        del missing_gate_name["gate_name"]
        maintainer_with_scope = {
            **gate_cause_value(),
            "kind": "maintainer_request",
            "finding_classes": ["product_disposition"],
            "scope_review_id": "wsr_" + "6" * 32,
            "maintainer_review_id": "mrv_" + "7" * 32,
            "gate_name": None,
        }
        cases = (
            {**reviewer_cause_value(), "provider_token": "secret"},
            missing_gate_name,
            {**reviewer_cause_value(), "schema_version": True},
            {**reviewer_cause_value(), "reviewer_run_id": []},
            {**reviewer_cause_value(), "reviewer_attempt_id": None},
            {**reviewer_cause_value(), "finding_classes": []},
            {**reviewer_cause_value(), "finding_classes": ["regression"] * 2},
            {**reviewer_cause_value(), "finding_classes": [7]},
            {
                **reviewer_cause_value(),
                "kind": "maintainer_request",
                "maintainer_review_id": "mrv_" + "7" * 32,
            },
            maintainer_with_scope,
            {**gate_cause_value(), "gate_name": None},
            {**gate_cause_value(), "gate_name": []},
            {**gate_cause_value(), "reviewer_run_id": "run_" + "2" * 32},
            {**gate_cause_value(), "scope_review_id": "wsr_" + "6" * 32},
            {
                **gate_cause_value(),
                "maintainer_review_id": "mrv_" + "7" * 32,
            },
            {**reviewer_cause_value(), "kind": []},
            {**reviewer_cause_value(), "policy_id": None},
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                WorkflowRoundCause.from_mapping(value)


class WorkflowReviewPublishedSchemaTests(unittest.TestCase):
    repository = Path(__file__).resolve().parents[1]

    def test_examples_match_published_and_runtime_contracts(self) -> None:
        cases = (
            (
                "workflow-reviewer-measurement",
                ReviewerMeasurement.from_mapping,
            ),
            ("workflow-round-cause", WorkflowRoundCause.from_mapping),
        )
        for name, factory in cases:
            with self.subTest(name=name):
                schema = json.loads(
                    (self.repository / "schemas" / f"{name}-v1.schema.json").read_text()
                )
                example = json.loads(
                    (self.repository / "examples" / f"{name}.json").read_text()
                )
                Draft202012Validator.check_schema(schema)
                validator = Draft202012Validator(schema)
                validator.validate(example)
                self.assertEqual(factory(example).to_dict(), example)

                with self.assertRaises(SchemaValidationError):
                    validator.validate({**example, "provider_token": "secret"})
                if name == "workflow-reviewer-measurement":
                    with self.assertRaises(SchemaValidationError):
                        validator.validate(
                            {
                                **example,
                                "tool_observations": [
                                    {
                                        "name": "pytest-xdist",
                                        "status": "absent",
                                        "version": None,
                                    }
                                ],
                            }
                        )


if __name__ == "__main__":
    unittest.main()
