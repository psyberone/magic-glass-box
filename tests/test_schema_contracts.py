from __future__ import annotations

import copy
import json
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError as SchemaValidationError

from magic_glass_box.candidates import CandidateError, CandidateManifest
from magic_glass_box.domain import (
    HandoffEnvelope,
    SupervisionPolicy,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
    WorkLogEntry,
    WorkMemoryCheckpoint,
)
from magic_glass_box.executor import DockerImage
from magic_glass_box.gates import (
    GateError,
    GatePolicyManifest,
    parse_gate_evidence,
    resolve_gate_policy_request,
)
from magic_glass_box.results import ResultError, ResultManifest


class PublishedSchemaContractTests(unittest.TestCase):
    repository = Path(__file__).resolve().parents[1]
    contracts: tuple[tuple[str, Callable[[dict[str, Any]], object]], ...] = (
        ("handoff", HandoffEnvelope.from_mapping),
        ("work-item", WorkItemEnvelope.from_mapping),
        ("work-log-entry", WorkLogEntry.from_mapping),
        ("work-memory-checkpoint", WorkMemoryCheckpoint.from_mapping),
        ("work-assignment", WorkAssignmentSpec.from_mapping),
        ("supervision-policy", SupervisionPolicy.from_mapping),
    )

    def load(
        self, name: str
    ) -> tuple[Draft202012Validator, dict[str, Any], Callable[..., object]]:
        schema = json.loads(
            (self.repository / "schemas" / f"{name}-v1.schema.json").read_text(
                encoding="utf-8"
            )
        )
        example = json.loads(
            (self.repository / "examples" / f"{name}.json").read_text(encoding="utf-8")
        )
        factory = dict(self.contracts)[name]
        Draft202012Validator.check_schema(schema)
        return (
            Draft202012Validator(schema, format_checker=FormatChecker()),
            example,
            factory,
        )

    def assert_rejected_by_both(
        self,
        validator: Draft202012Validator,
        factory: Callable[[dict[str, Any]], object],
        candidate: dict[str, Any],
    ) -> None:
        with self.assertRaises(SchemaValidationError):
            validator.validate(candidate)
        with self.assertRaises(ValidationError):
            factory(candidate)

    def test_examples_are_valid_under_schema_and_runtime_contract(self) -> None:
        for name, _ in self.contracts:
            with self.subTest(name=name):
                validator, example, factory = self.load(name)
                validator.validate(example)
                parsed = factory(example)
                normalized = parsed.to_dict()
                validator.validate(normalized)
                if name != "work-item":
                    self.assertEqual(normalized, example)

    def test_shared_strictness_rejects_missing_extra_boolean_and_blank_fields(
        self,
    ) -> None:
        text_field = {
            "handoff": "summary",
            "work-item": "title",
            "work-log-entry": "summary",
            "work-memory-checkpoint": "summary",
            "work-assignment": "title",
        }
        for name, _ in self.contracts:
            validator, example, factory = self.load(name)
            missing = copy.deepcopy(example)
            missing.pop("schema_version")
            candidates = [
                missing,
                {**copy.deepcopy(example), "unexpected": True},
                {**copy.deepcopy(example), "schema_version": True},
            ]
            if name in text_field:
                candidates.append(
                    {**copy.deepcopy(example), text_field[name]: " \n\t "}
                )
            for candidate in candidates:
                with self.subTest(name=name, candidate=candidate):
                    self.assert_rejected_by_both(validator, factory, candidate)

    def test_selected_structural_guards_are_shared_with_runtime(self) -> None:
        for name, field in (
            ("handoff", "artifacts"),
            ("work-log-entry", "artifacts"),
            ("work-memory-checkpoint", "artifacts"),
        ):
            validator, example, factory = self.load(name)
            candidate = copy.deepcopy(example)
            candidate[field][0]["path"] = "../outside"
            with self.subTest(name=name):
                self.assert_rejected_by_both(validator, factory, candidate)

        validator, example, factory = self.load("work-assignment")
        unsafe_assignment = copy.deepcopy(example)
        unsafe_assignment["scope_paths"] = ["../outside"]
        duplicate_assignment = copy.deepcopy(example)
        duplicate_assignment["acceptance_checks"] = ["same", "same"]
        for candidate in (unsafe_assignment, duplicate_assignment):
            self.assert_rejected_by_both(validator, factory, candidate)

        validator, example, factory = self.load("work-item")
        unsafe_item = copy.deepcopy(example)
        unsafe_item["context"] = [{"type": "workspace_path", "value": "../outside"}]
        invalid_timestamp = copy.deepcopy(example)
        invalid_timestamp["source_updated_at"] = "2026-10-10T12:00:00"
        duplicate_labels = copy.deepcopy(example)
        duplicate_labels["labels"] = ["same", "same"]
        for candidate in (unsafe_item, invalid_timestamp, duplicate_labels):
            self.assert_rejected_by_both(validator, factory, candidate)

    def test_runtime_semantic_guards_extend_portable_json_schema(self) -> None:
        validator, example, factory = self.load("handoff")
        duplicate_path = copy.deepcopy(example)
        duplicate_path["artifacts"].append(
            {
                "path": duplicate_path["artifacts"][0]["path"],
                "description": "A second description for the same path.",
            }
        )
        multibyte_summary = copy.deepcopy(example)
        multibyte_summary["summary"] = "é" * 3000
        for candidate in (duplicate_path, multibyte_summary):
            with self.subTest(case="handoff-runtime-only", candidate=candidate):
                validator.validate(candidate)
                with self.assertRaises(ValidationError):
                    factory(candidate)

        validator, example, factory = self.load("supervision-policy")
        descending_schedule = copy.deepcopy(example)
        descending_schedule["retry_backoff_seconds"] = [30, 5]
        validator.validate(descending_schedule)
        with self.assertRaises(ValidationError):
            factory(descending_schedule)


class ArtifactSchemaContractTests(unittest.TestCase):
    repository = Path(__file__).resolve().parents[1]

    def load(self, name: str) -> tuple[Draft202012Validator, dict[str, Any]]:
        schema = json.loads(
            (self.repository / "schemas" / f"{name}-v1.schema.json").read_text()
        )
        example = json.loads(
            (self.repository / "examples" / f"{name}.json").read_text()
        )
        Draft202012Validator.check_schema(schema)
        return Draft202012Validator(schema), example

    def test_result_and_candidate_examples_match_runtime_contracts(self) -> None:
        result_validator, result = self.load("result")
        result_validator.validate(result)
        parsed_result = ResultManifest.from_mapping(result)
        self.assertEqual(parsed_result.to_dict(), result)

        candidate_validator, candidate = self.load("candidate")
        candidate_validator.validate(candidate)
        parsed_candidate = CandidateManifest.from_mapping(candidate)
        self.assertEqual(parsed_candidate.to_dict(), candidate)

        runtime_candidate = CandidateManifest(
            schema_version=1,
            attempt_id="att_" + "1" * 32,
            attempt_result_id="rsl_" + "2" * 32,
            producer_run_id="run_" + "3" * 32,
            project_id="boxp_" + "4" * 32,
            context_version_id="ctx_" + "5" * 32,
            work_thread_id="wth_" + "6" * 32,
            base_revision="a" * 40,
            result_content_sha256="b" * 64,
            head_revision="c" * 40,
            tree_revision="d" * 40,
            object_format="sha1",
        )
        candidate_validator.validate(runtime_candidate.to_dict())

    def test_artifact_schemas_and_runtime_reject_invalid_identity(self) -> None:
        result_validator, result = self.load("result")
        invalid_results = []
        for path in ("../outside", "nested//file.txt", ".GIT/config"):
            unsafe_result = copy.deepcopy(result)
            unsafe_result["changes"][0]["path"] = path
            invalid_results.append(unsafe_result)
        inconsistent_add = copy.deepcopy(result)
        inconsistent_add["changes"][0]["before"] = copy.deepcopy(
            inconsistent_add["changes"][0]["after"]
        )
        invalid_results.append(inconsistent_add)
        for invalid_result in invalid_results:
            with self.subTest(invalid_result=invalid_result):
                with self.assertRaises(SchemaValidationError):
                    result_validator.validate(invalid_result)
                with self.assertRaises(ResultError):
                    ResultManifest.from_mapping(invalid_result)

        candidate_validator, candidate = self.load("candidate")
        wrong_format = copy.deepcopy(candidate)
        wrong_format["base_revision"] = "f" * 64
        with self.assertRaises(SchemaValidationError):
            candidate_validator.validate(wrong_format)
        with self.assertRaises(CandidateError):
            CandidateManifest.from_mapping(wrong_format)

    def test_result_symlink_containment_is_a_runtime_semantic_guard(self) -> None:
        validator, result = self.load("result")
        escaping_symlink = copy.deepcopy(result)
        escaping_symlink["changes"][0]["after"] = {
            "kind": "symlink",
            "target": "../../outside",
        }

        validator.validate(escaping_symlink)
        with self.assertRaises(ResultError):
            ResultManifest.from_mapping(escaping_symlink)

    def test_gate_policy_and_evidence_examples_match_runtime_contracts(self) -> None:
        request_validator, request = self.load("gate-policy-request")
        request_validator.validate(request)
        resolved = resolve_gate_policy_request(
            request,
            image_resolver=lambda _reference: DockerImage("sha256:" + "c" * 64, ()),
        )

        policy_validator, policy = self.load("gate-policy")
        policy_validator.validate(policy)
        parsed_policy = GatePolicyManifest.from_mapping(policy)
        self.assertEqual(parsed_policy.to_dict(), policy)
        self.assertEqual(resolved, parsed_policy.policy)

        evidence_schema = json.loads(
            (self.repository / "schemas/gate-evidence-v1.schema.json").read_text()
        )
        Draft202012Validator.check_schema(evidence_schema)
        evidence_validator = Draft202012Validator(evidence_schema)
        for name in ("gate-review-evidence", "gate-validation-evidence"):
            evidence = json.loads(
                (self.repository / f"examples/{name}.json").read_text()
            )
            with self.subTest(name=name):
                evidence_validator.validate(evidence)
                self.assertEqual(parse_gate_evidence(evidence).to_dict(), evidence)

    def test_gate_schemas_and_runtime_reject_mutable_or_inconsistent_inputs(
        self,
    ) -> None:
        request_validator, request = self.load("gate-policy-request")
        empty = copy.deepcopy(request)
        empty["review_separation"] = None
        empty["validations"] = []
        with self.assertRaises(SchemaValidationError):
            request_validator.validate(empty)
        with self.assertRaises(GateError):
            resolve_gate_policy_request(
                empty,
                image_resolver=lambda _reference: DockerImage("sha256:" + "c" * 64, ()),
            )

        oversized_argument = copy.deepcopy(request)
        oversized_argument["validations"][0]["command"] = ["x" * 1025]
        with self.assertRaises(SchemaValidationError):
            request_validator.validate(oversized_argument)
        with self.assertRaises(GateError):
            resolve_gate_policy_request(
                oversized_argument,
                image_resolver=lambda _reference: DockerImage("sha256:" + "c" * 64, ()),
            )

        run_only_review = copy.deepcopy(request)
        run_only_review["review_separation"] = "different_run"
        with self.assertRaises(SchemaValidationError):
            request_validator.validate(run_only_review)
        with self.assertRaises(GateError):
            resolve_gate_policy_request(
                run_only_review,
                image_resolver=lambda _reference: DockerImage("sha256:" + "c" * 64, ()),
            )

        policy_validator, policy = self.load("gate-policy")
        mutable = copy.deepcopy(policy)
        mutable["policy"]["validations"][0]["image_id"] = "python:latest"
        with self.assertRaises(SchemaValidationError):
            policy_validator.validate(mutable)
        with self.assertRaises(GateError):
            GatePolicyManifest.from_mapping(mutable)

        evidence_schema = json.loads(
            (self.repository / "schemas/gate-evidence-v1.schema.json").read_text()
        )
        evidence_validator = Draft202012Validator(evidence_schema)
        validation = json.loads(
            (self.repository / "examples/gate-validation-evidence.json").read_text()
        )
        validation["outcome"] = "failed"
        with self.assertRaises(SchemaValidationError):
            evidence_validator.validate(validation)
        with self.assertRaises(GateError):
            parse_gate_evidence(validation)


if __name__ == "__main__":
    unittest.main()
