from __future__ import annotations

import copy
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError as SchemaValidationError

from magic_glass_box.db import Database
from magic_glass_box.domain import ValidationError
from magic_glass_box.provider_effects import (
    ChangeOpenReadback,
    LandingReadback,
    WorkflowProviderIntent,
)
from magic_glass_box.workflow_closeout import (
    WorkflowCloseoutDeclaration,
    validate_delivery_closeout,
)


class WorkflowCloseoutDeclarationTests(unittest.TestCase):
    repository = Path(__file__).resolve().parents[1]

    @staticmethod
    def value(**overrides: object) -> dict[str, object]:
        return {
            "schema_version": 1,
            "obligations": [
                {
                    "kind": "deferred_validation",
                    "obligation": "tests",
                    "deadline": "2026-11-04T00:00:00-06:00",
                    "reason": (
                        "Run the longer cross-platform validation after closeout."
                    ),
                },
                {
                    "kind": "deferred_validation",
                    "obligation": "mutation",
                    "deadline": "2026-11-05T06:00:00Z",
                    "reason": (
                        "Retain the broader mutation campaign as a visible follow-up."
                    ),
                },
            ],
            **overrides,
        }

    def test_declaration_is_canonical_and_normalizes_deadlines_and_order(self) -> None:
        first = WorkflowCloseoutDeclaration.from_mapping(self.value())
        reordered_value = self.value()
        reordered_value["obligations"] = list(
            reversed(copy.deepcopy(reordered_value["obligations"]))
        )
        second = WorkflowCloseoutDeclaration.from_mapping(reordered_value)

        self.assertEqual(first, second)
        self.assertEqual(
            first.to_dict(),
            {
                "schema_version": 1,
                "obligations": [
                    {
                        "kind": "deferred_validation",
                        "obligation": "mutation",
                        "deadline": "2026-11-05T06:00:00.000000Z",
                        "reason": (
                            "Retain the broader mutation campaign as a visible "
                            "follow-up."
                        ),
                    },
                    {
                        "kind": "deferred_validation",
                        "obligation": "tests",
                        "deadline": "2026-11-04T06:00:00.000000Z",
                        "reason": (
                            "Run the longer cross-platform validation after closeout."
                        ),
                    },
                ],
            },
        )
        self.assertEqual(first.digest, second.digest)
        self.assertEqual(len(first.digest), 64)

    def test_empty_declaration_is_explicit_and_valid(self) -> None:
        declaration = WorkflowCloseoutDeclaration.from_mapping(
            {"schema_version": 1, "obligations": []}
        )

        self.assertEqual(
            declaration.to_dict(), {"schema_version": 1, "obligations": []}
        )

    def test_declaration_rejects_ambiguous_or_unbounded_obligations(self) -> None:
        missing = self.value()
        missing.pop("schema_version")
        duplicate = self.value()
        duplicate["obligations"] = [
            copy.deepcopy(duplicate["obligations"][0]),
            copy.deepcopy(duplicate["obligations"][0]),
        ]
        oversized = self.value()
        oversized["obligations"] = [
            {
                "kind": "deferred_validation",
                "obligation": f"gate-{index}",
                "deadline": "2026-11-05T06:00:00Z",
                "reason": "Bounded follow-up.",
            }
            for index in range(33)
        ]
        cases = (
            None,
            missing,
            {**self.value(), "provider": "github"},
            self.value(schema_version=True),
            self.value(schema_version=2),
            self.value(obligations=None),
            oversized,
            duplicate,
            self.value(
                obligations=[
                    {
                        "kind": "waiver",
                        "obligation": "tests",
                        "deadline": "2026-11-05T06:00:00Z",
                        "reason": "Skip it.",
                    }
                ]
            ),
            self.value(
                obligations=[
                    {
                        "kind": "deferred_validation",
                        "obligation": "Bad Obligation",
                        "deadline": "2026-11-05T06:00:00Z",
                        "reason": "Invalid name.",
                    }
                ]
            ),
            self.value(
                obligations=[
                    {
                        "kind": "deferred_validation",
                        "obligation": "tests",
                        "deadline": "tomorrow",
                        "reason": "Invalid deadline.",
                    }
                ]
            ),
            self.value(
                obligations=[
                    {
                        "kind": "deferred_validation",
                        "obligation": "tests",
                        "deadline": "2026-11-05T06:00:00Z",
                        "reason": "x" * 4097,
                    }
                ]
            ),
            self.value(
                obligations=[
                    {
                        "kind": "deferred_validation",
                        "obligation": "tests",
                        "deadline": "2026-11-05T06:00:00Z",
                        "reason": " \n\t ",
                    }
                ]
            ),
        )

        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                WorkflowCloseoutDeclaration.from_mapping(value)

    def test_published_declaration_and_projection_contracts_are_exact(self) -> None:
        for name in (
            "workflow-closeout-declaration",
            "workflow-closeout-obligation",
            "workflow-closeout",
        ):
            with self.subTest(name=name):
                schema = json.loads(
                    (self.repository / "schemas" / f"{name}-v1.schema.json").read_text(
                        encoding="utf-8"
                    )
                )
                example = json.loads(
                    (self.repository / "examples" / f"{name}.json").read_text(
                        encoding="utf-8"
                    )
                )
                Draft202012Validator.check_schema(schema)
                validator = Draft202012Validator(schema, format_checker=FormatChecker())
                validator.validate(example)
                with self.assertRaises(SchemaValidationError):
                    validator.validate({**example, "provider": "github"})

        declaration = json.loads(
            (
                self.repository / "examples" / "workflow-closeout-declaration.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            WorkflowCloseoutDeclaration.from_mapping(declaration).to_dict(),
            declaration,
        )

    def test_identical_declarations_are_reusable_across_executions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Database(Path(temporary) / "control-plane.db")
            database.initialize()
            with database.connect() as connection:
                indexes = connection.execute(
                    "PRAGMA index_list('workflow_closeout_declarations')"
                ).fetchall()
                unique_columns = {
                    tuple(
                        column["name"]
                        for column in connection.execute(
                            f"PRAGMA index_info('{index['name']}')"
                        ).fetchall()
                    )
                    for index in indexes
                    if index["unique"]
                }

        self.assertIn(("workflow_execution_id",), unique_columns)
        self.assertNotIn(("content_sha256",), unique_columns)

    def test_delivery_closeout_proves_every_metadata_and_closure_dimension(
        self,
    ) -> None:
        def example(name: str) -> dict[str, object]:
            return json.loads(
                (self.repository / "examples" / f"{name}.json").read_text(
                    encoding="utf-8"
                )
            )

        intent = WorkflowProviderIntent.from_mapping(
            example("workflow-provider-intent")
        )
        change = ChangeOpenReadback.from_mapping(example("change-open-readback"))
        landing = LandingReadback.from_mapping(example("landing-readback"))
        identity = "github:github.com/psyberone/magic-glass-box/pull/42"

        self.assertEqual(
            validate_delivery_closeout(
                intent=intent,
                change_readback=change,
                landing_readback=landing,
                remote_identity=identity,
                closed_references=(identity,),
            ),
            {
                "primary_landing": "conformant",
                "proposal_metadata": "conformant",
                "proposal_closure": "closed",
                "secondary_effects": "none",
                "replication": "not_required",
                "tracker": "not_required",
            },
        )

        metadata_cases = (
            replace(change, state="closed"),
            replace(change, base_ref="refs/heads/release"),
            replace(change, head_ref="refs/heads/mgb/other"),
            replace(change, title="Different title"),
            replace(change, body="Different body"),
            replace(change, draft=True),
        )
        for changed in metadata_cases:
            with (
                self.subTest(changed=changed),
                self.assertRaisesRegex(ValidationError, "proposal metadata"),
            ):
                validate_delivery_closeout(
                    intent=intent,
                    change_readback=changed,
                    landing_readback=landing,
                    remote_identity=identity,
                    closed_references=(identity,),
                )

        readback_cases = (
            replace(landing, candidate_revision="e" * 40),
            replace(landing, change_id="43"),
            replace(landing, base_ref="refs/heads/release"),
            replace(landing, head_ref="refs/heads/mgb/other"),
            replace(landing, title="Different title"),
            replace(landing, body="Different body"),
            replace(landing, draft=True),
            replace(landing, state="open"),
            replace(landing, merge_method="merge"),
        )
        for changed in readback_cases:
            with (
                self.subTest(changed=changed),
                self.assertRaisesRegex(ValidationError, "provider readback"),
            ):
                validate_delivery_closeout(
                    intent=intent,
                    change_readback=change,
                    landing_readback=changed,
                    remote_identity=identity,
                    closed_references=(identity,),
                )

        secondary_cases = (
            (replace(intent, secondary_effects=("comment",)), change, landing),
            (intent, replace(change, secondary_effects=("comment",)), landing),
            (intent, change, replace(landing, secondary_effects=("comment",))),
        )
        for changed_intent, changed_change, changed_landing in secondary_cases:
            with (
                self.subTest(
                    secondary=(changed_intent, changed_change, changed_landing)
                ),
                self.assertRaisesRegex(ValidationError, "secondary effects"),
            ):
                validate_delivery_closeout(
                    intent=changed_intent,
                    change_readback=changed_change,
                    landing_readback=changed_landing,
                    remote_identity=identity,
                    closed_references=(identity,),
                )

        for closed_references in ((), (identity, identity), ("github:other",)):
            with (
                self.subTest(closed_references=closed_references),
                self.assertRaisesRegex(ValidationError, "provider closure"),
            ):
                validate_delivery_closeout(
                    intent=intent,
                    change_readback=change,
                    landing_readback=landing,
                    remote_identity=identity,
                    closed_references=closed_references,
                )
