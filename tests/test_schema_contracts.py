from __future__ import annotations

import copy
import json
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError as SchemaValidationError

from magic_glass_box.domain import (
    HandoffEnvelope,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
    WorkLogEntry,
    WorkMemoryCheckpoint,
)


class PublishedSchemaParityTests(unittest.TestCase):
    repository = Path(__file__).resolve().parents[1]
    contracts: tuple[tuple[str, Callable[[dict[str, Any]], object]], ...] = (
        ("handoff", HandoffEnvelope.from_mapping),
        ("work-item", WorkItemEnvelope.from_mapping),
        ("work-log-entry", WorkLogEntry.from_mapping),
        ("work-memory-checkpoint", WorkMemoryCheckpoint.from_mapping),
        ("work-assignment", WorkAssignmentSpec.from_mapping),
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
            candidates = (
                missing,
                {**copy.deepcopy(example), "unexpected": True},
                {**copy.deepcopy(example), "schema_version": True},
                {**copy.deepcopy(example), text_field[name]: " \n\t "},
            )
            for candidate in candidates:
                with self.subTest(name=name, candidate=candidate):
                    self.assert_rejected_by_both(validator, factory, candidate)

    def test_path_list_and_timestamp_guards_match_runtime_validation(self) -> None:
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


if __name__ == "__main__":
    unittest.main()
