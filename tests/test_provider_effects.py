from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from magic_glass_box.domain import ValidationError
from magic_glass_box.provider_effects import (
    ChangeOpenReadback,
    LandingReadback,
    RefPublicationReadback,
    WorkflowProviderIntent,
)
from magic_glass_box.service_sidecar import ServiceOperationResponse
from tests.test_github_provider import (
    BASE,
    HEAD,
    FakeGitHubAPI,
    GitHubProvider,
    api_response,
    pull,
    ref,
    request,
    sidecar_spec,
)


class ProviderEffectReadbackTests(unittest.TestCase):
    repository = Path(__file__).resolve().parents[1]

    def test_published_provider_effect_contracts_match_runtime(self) -> None:
        contracts = (
            ("workflow-provider-intent", WorkflowProviderIntent.from_mapping),
            ("ref-publication-readback", RefPublicationReadback.from_mapping),
            ("change-open-readback", ChangeOpenReadback.from_mapping),
            ("landing-readback", LandingReadback.from_mapping),
        )
        for name, factory in contracts:
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
                self.assertEqual(factory(example).to_dict(), example)

    def test_published_provider_effect_projections_validate(self) -> None:
        for name in (
            "workflow-provider-effect",
            "workflow-landing-proof",
            "integration-target-hold",
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
                Draft202012Validator(schema, format_checker=FormatChecker()).validate(
                    example
                )

    def test_workflow_provider_intent_is_project_and_artifact_bound(self) -> None:
        value = {
            "schema_version": 1,
            "project_id": "boxp_" + "1" * 32,
            "work_item_id": "wki_" + "2" * 32,
            "work_item_version_id": "wiv_" + "3" * 32,
            "candidate_id": "cnd_" + "4" * 32,
            "gate_policy_id": "gpl_" + "5" * 32,
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "title": "Add the bounded health probe",
            "body": "Exact project-local proposal body.",
            "draft": False,
            "merge_method": "squash",
            "secondary_effects": [],
        }

        intent = WorkflowProviderIntent.from_mapping(value)

        self.assertEqual(intent.to_dict(), value)
        self.assertEqual(len(intent.digest), 64)

    def test_workflow_provider_intent_rejects_cross_project_or_raw_provider_data(
        self,
    ) -> None:
        valid = {
            "schema_version": 1,
            "project_id": "boxp_" + "1" * 32,
            "work_item_id": "wki_" + "2" * 32,
            "work_item_version_id": "wiv_" + "3" * 32,
            "candidate_id": "cnd_" + "4" * 32,
            "gate_policy_id": "gpl_" + "5" * 32,
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "title": "Add the bounded health probe",
            "body": "Exact project-local proposal body.",
            "draft": False,
            "merge_method": "squash",
            "secondary_effects": [],
        }
        cases = (
            {**valid, "project_id": "prj_" + "1" * 32},
            {**valid, "work_item_id": "../work"},
            {**valid, "head_ref": "refs/heads/../main"},
            {**valid, "merge_method": "octopus"},
            {**valid, "provider": "github"},
            {**valid, "secondary_effects": ["close:42"]},
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                WorkflowProviderIntent.from_mapping(value)

    def test_all_provider_effect_branch_refs_use_the_canonical_git_contract(
        self,
    ) -> None:
        contracts = (
            (
                "workflow-provider-intent",
                WorkflowProviderIntent.from_mapping,
                ("base_ref", "head_ref"),
            ),
            (
                "ref-publication-readback",
                RefPublicationReadback.from_mapping,
                ("target_ref",),
            ),
            (
                "change-open-readback",
                ChangeOpenReadback.from_mapping,
                ("base_ref", "head_ref"),
            ),
            (
                "landing-readback",
                LandingReadback.from_mapping,
                ("base_ref", "head_ref", "primary_ref"),
            ),
        )
        invalid_refs = (
            "refs/heads/a..b",
            "refs/heads//leading-empty-component",
            "refs/heads/topic.lock",
            "refs/heads/topic/.hidden",
            "refs/heads/topic.",
            "refs/heads/topic//child",
        )
        for name, factory, fields in contracts:
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
            validator = Draft202012Validator(schema, format_checker=FormatChecker())
            for field in fields:
                for invalid_ref in invalid_refs:
                    value = {**example, field: invalid_ref}
                    with self.subTest(
                        contract=name, field=field, invalid_ref=invalid_ref
                    ):
                        self.assertTrue(list(validator.iter_errors(value)))
                        with self.assertRaises(ValidationError):
                            factory(value)

    def test_ref_publication_readback_is_canonical_and_exact(self) -> None:
        value = {
            "schema_version": 1,
            "candidate_id": "cnd_" + "1" * 32,
            "target_ref": "refs/heads/mgb/candidate-1",
            "published_revision": "a" * 40,
            "secondary_effects": [],
        }

        readback = RefPublicationReadback.from_mapping(value)

        self.assertEqual(readback.to_dict(), value)
        self.assertEqual(readback.secondary_effects, ())

    def test_change_open_readback_retains_provider_stored_metadata(self) -> None:
        value = {
            "schema_version": 1,
            "change_id": "42",
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "base_revision": "a" * 40,
            "head_revision": "b" * 40,
            "title": "Add the bounded health probe",
            "body": "Exact project-local proposal body.",
            "draft": False,
            "state": "open",
            "secondary_effects": [],
        }

        readback = ChangeOpenReadback.from_mapping(value)

        self.assertEqual(readback.to_dict(), value)
        self.assertEqual(readback.title, value["title"])

    def test_change_open_readback_accepts_an_explicit_empty_body(self) -> None:
        value = {
            "schema_version": 1,
            "change_id": "42",
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "base_revision": "a" * 40,
            "head_revision": "b" * 40,
            "title": "Add the bounded health probe",
            "body": "",
            "draft": False,
            "state": "open",
            "secondary_effects": [],
        }

        self.assertEqual(ChangeOpenReadback.from_mapping(value).to_dict(), value)

    def test_landing_readback_separates_primary_and_landing_revisions(self) -> None:
        value = {
            "schema_version": 1,
            "change_id": "42",
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "candidate_revision": "b" * 40,
            "landing_revision": "c" * 40,
            "landing_tree": "d" * 40,
            "primary_ref": "refs/heads/main",
            "primary_revision": "e" * 40,
            "landing_revision_reachable": True,
            "merge_method": "squash",
            "title": "Add the bounded health probe",
            "body": "Exact project-local proposal body.",
            "draft": False,
            "state": "closed",
            "secondary_effects": [],
        }

        readback = LandingReadback.from_mapping(value)

        self.assertEqual(readback.to_dict(), value)
        self.assertNotEqual(readback.landing_revision, readback.primary_revision)
        self.assertEqual(readback.state, "closed")

    def test_readbacks_reject_ambiguous_or_provider_shaped_extras(self) -> None:
        valid_change = {
            "schema_version": 1,
            "change_id": "42",
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "base_revision": "a" * 40,
            "head_revision": "b" * 40,
            "title": "MGB work",
            "body": "Bounded body.",
            "draft": False,
            "state": "open",
            "secondary_effects": [],
        }
        cases = (
            None,
            {**valid_change, "html_url": "https://github.example/pull/42"},
            {**valid_change, "schema_version": True},
            {**valid_change, "change_id": "../42"},
            {**valid_change, "head_ref": "main"},
            {**valid_change, "base_revision": "a" * 39},
            {**valid_change, "draft": 0},
            {**valid_change, "state": "merged"},
            {**valid_change, "secondary_effects": ["close:1", "close:1"]},
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                ChangeOpenReadback.from_mapping(value)  # type: ignore[arg-type]

    def test_service_response_rejects_wrong_change_metadata_on_correlation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            socket_path = Path(temporary) / "github.sock"
            operation = request(
                socket_path,
                "change.open",
                resource_id="refs/heads/mgb/op-42",
                parameters={
                    "base_ref": "refs/heads/main",
                    "body": "Bounded summary.",
                    "draft": True,
                    "head_ref": "refs/heads/mgb/op-42",
                    "title": "Candidate 42",
                },
            )
            created = pull(draft=True)
            created.pop("merged")
            response = GitHubProvider(
                sidecar_spec(socket_path),
                FakeGitHubAPI(
                    [
                        api_response(200, []),
                        api_response(200, ref(BASE)),
                        api_response(200, ref(HEAD, "mgb/op-42")),
                        api_response(201, created),
                        api_response(200, created),
                    ]
                ),
            ).invoke(operation)
            value = response.to_dict()
            assert value["receipt"] is not None
            value["receipt"]["result"]["title"] = "Imitation Krab candidate"
            mismatched = ServiceOperationResponse.from_mapping(value)

            with self.assertRaisesRegex(
                ValidationError, "change-open result does not match"
            ):
                mismatched.validate_correlation(operation)


if __name__ == "__main__":
    unittest.main()
