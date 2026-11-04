from __future__ import annotations

import copy
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
                example = json.loads(
                    (self.repository / "examples" / f"{name}.json").read_text(
                        encoding="utf-8"
                    )
                )
                schema = json.loads(
                    (
                        self.repository
                        / "schemas"
                        / f"{name}-v{example['schema_version']}.schema.json"
                    ).read_text(encoding="utf-8")
                )
                Draft202012Validator.check_schema(schema)
                validator = Draft202012Validator(schema, format_checker=FormatChecker())
                validator.validate(example)
                self.assertEqual(factory(example).to_dict(), example)

    def test_published_provider_effect_projections_validate(self) -> None:
        for name, version in (
            ("workflow-provider-effect", 2),
            ("workflow-landing-proof", 1),
            ("integration-target-hold", 1),
        ):
            with self.subTest(name=name):
                schema = json.loads(
                    (
                        self.repository / "schemas" / f"{name}-v{version}.schema.json"
                    ).read_text(encoding="utf-8")
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

    def test_provider_effect_projection_enforces_fact_lifecycle(self) -> None:
        schema = json.loads(
            (
                self.repository / "schemas" / "workflow-provider-effect-v2.schema.json"
            ).read_text(encoding="utf-8")
        )
        example = json.loads(
            (self.repository / "examples" / "workflow-provider-effect.json").read_text(
                encoding="utf-8"
            )
        )
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        contradictory = (
            {
                **example,
                "state": "pending",
                "conforms_intent": None,
                "resolved_at": None,
            },
            {**example, "resolved_at": None},
            {
                **example,
                "state": "failed",
                "conforms_intent": None,
            },
        )

        for projection in contradictory:
            with self.subTest(state=projection["state"]):
                self.assertTrue(list(validator.iter_errors(projection)))

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
            example = json.loads(
                (self.repository / "examples" / f"{name}.json").read_text(
                    encoding="utf-8"
                )
            )
            schema = json.loads(
                (
                    self.repository
                    / "schemas"
                    / f"{name}-v{example['schema_version']}.schema.json"
                ).read_text(encoding="utf-8")
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
        self.assertEqual(
            ChangeOpenReadback.from_mapping({**value, "state": "closed"}).state,
            "closed",
        )

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

    def test_change_open_v2_retains_observed_closure_and_secondary_effects(
        self,
    ) -> None:
        identity = "github:github.com/acme/widgets/issues/12"
        value = {
            "schema_version": 2,
            "change_id": "42",
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "base_revision": "a" * 40,
            "head_revision": "b" * 40,
            "title": "Add the bounded health probe",
            "body": "Exact project-local proposal body.",
            "draft": False,
            "state": "open",
            "closure_references": [identity],
            "secondary_effects": [f"issue.close:{identity}"],
        }

        readback = ChangeOpenReadback.from_mapping(value)

        self.assertEqual(readback.to_dict(), value)
        self.assertEqual(readback.closure_references, (identity,))

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
        self.assertEqual(readback.landing_parent_revisions, ())
        self.assertEqual(readback.landing_commit_message, "")
        self.assertEqual(readback.first_parent_status, "contains")
        self.assertEqual(readback.closure_references, ())
        unreachable = LandingReadback.from_mapping(
            {**value, "landing_revision_reachable": False}
        )
        self.assertEqual(unreachable.first_parent_status, "absent")
        open_readback = LandingReadback.from_mapping({**value, "state": "open"})
        self.assertEqual(open_readback.state, "open")

    def test_observed_readback_collection_and_byte_boundaries_are_exact(
        self,
    ) -> None:
        references = [
            f"github:github.com/acme/widgets/issues/{number}" for number in range(1, 65)
        ]
        change = {
            "schema_version": 2,
            "change_id": "42",
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "base_revision": "a" * 40,
            "head_revision": "b" * 40,
            "title": "Add the bounded health probe",
            "body": "Exact project-local proposal body.",
            "draft": False,
            "state": "open",
            "closure_references": references,
            "secondary_effects": [
                f"issue.close:{reference}" for reference in references
            ],
        }
        landing = {
            "schema_version": 2,
            "change_id": "42",
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "candidate_revision": "b" * 40,
            "landing_revision": "c" * 40,
            "landing_tree": "d" * 40,
            "landing_parent_revisions": [f"{number:040x}" for number in range(1, 17)],
            "landing_commit_message": "x" * 65536,
            "primary_ref": "refs/heads/main",
            "primary_revision": "e" * 40,
            "first_parent_status": "contains",
            "title": "Add the bounded health probe",
            "body": "Exact project-local proposal body.",
            "draft": False,
            "state": "closed",
            "closure_references": [],
            "secondary_effects": [],
        }

        self.assertEqual(
            len(ChangeOpenReadback.from_mapping(change).closure_references), 64
        )
        parsed_landing = LandingReadback.from_mapping(landing)
        self.assertEqual(len(parsed_landing.landing_parent_revisions), 16)
        self.assertEqual(len(parsed_landing.landing_commit_message), 65536)
        with self.assertRaises(ValidationError):
            LandingReadback.from_mapping(
                {**landing, "landing_commit_message": "x" * 65537}
            )
        with self.assertRaises(ValidationError):
            ChangeOpenReadback.from_mapping(
                {
                    **change,
                    "closure_references": (references[0],),
                    "secondary_effects": [f"issue.close:{references[0]}"],
                }
            )

    def test_landing_v2_uses_observed_first_parent_and_commit_evidence(self) -> None:
        value = {
            "schema_version": 2,
            "change_id": "42",
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "candidate_revision": "b" * 40,
            "landing_revision": "c" * 40,
            "landing_tree": "d" * 40,
            "landing_parent_revisions": ["a" * 40],
            "landing_commit_message": (
                "Add the bounded health probe\n\nExact project-local proposal body."
            ),
            "primary_ref": "refs/heads/main",
            "primary_revision": "e" * 40,
            "first_parent_status": "contains",
            "title": "Add the bounded health probe",
            "body": "Exact project-local proposal body.",
            "draft": False,
            "state": "closed",
            "closure_references": [],
            "secondary_effects": [],
        }

        readback = LandingReadback.from_mapping(value)

        self.assertEqual(readback.to_dict(), value)
        self.assertEqual(readback.landing_parent_revisions, ("a" * 40,))
        self.assertEqual(readback.first_parent_status, "contains")
        self.assertFalse(hasattr(readback, "merge_method"))
        self.assertFalse(hasattr(readback, "landing_revision_reachable"))

    def test_landing_v2_rejects_inconsistent_first_parent_evidence(self) -> None:
        valid = {
            "schema_version": 2,
            "change_id": "42",
            "base_ref": "refs/heads/main",
            "head_ref": "refs/heads/mgb/candidate-1",
            "candidate_revision": "b" * 40,
            "landing_revision": "c" * 40,
            "landing_tree": "d" * 40,
            "landing_parent_revisions": ["a" * 40],
            "landing_commit_message": "Candidate 42\n\nBounded summary.",
            "primary_ref": "refs/heads/main",
            "primary_revision": "c" * 40,
            "first_parent_status": "contains",
            "title": "Candidate 42",
            "body": "Bounded summary.",
            "draft": False,
            "state": "closed",
            "closure_references": [],
            "secondary_effects": [],
        }
        cases = (
            {**valid, "first_parent_status": "reachable"},
            {**valid, "landing_parent_revisions": []},
            {**valid, "landing_commit_message": ""},
            {**valid, "closure_references": ["not a remote identity"]},
            {**valid, "state": "open"},
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                LandingReadback.from_mapping(value)

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

    def test_readbacks_reject_an_unknown_integer_schema_version(self) -> None:
        value = {
            "schema_version": 3,
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

        with self.assertRaisesRegex(
            ValidationError, "change-open readback schema_version must be 1 or 2"
        ):
            ChangeOpenReadback.from_mapping(value)

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
            mismatches = (
                (
                    "title",
                    lambda document: document["receipt"]["result"].__setitem__(
                        "title", "Imitation Krab candidate"
                    ),
                ),
                (
                    "body",
                    lambda document: document["receipt"]["result"].__setitem__(
                        "body", "Different body."
                    ),
                ),
                (
                    "draft",
                    lambda document: document["receipt"]["result"].__setitem__(
                        "draft", False
                    ),
                ),
                (
                    "receipt revision",
                    lambda document: document["receipt"].__setitem__(
                        "observed_revision", "c" * 40
                    ),
                ),
            )
            for label, mutate in mismatches:
                mismatched_value = copy.deepcopy(value)
                mutate(mismatched_value)
                mismatched = ServiceOperationResponse.from_mapping(mismatched_value)
                with (
                    self.subTest(label=label),
                    self.assertRaisesRegex(
                        ValidationError, "change-open result does not match"
                    ),
                ):
                    mismatched.validate_correlation(operation)

    def test_service_response_rejects_each_landing_correlation_mismatch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            socket_path = Path(temporary) / "github.sock"
            parameters = {
                "assurance": "head_enforced",
                "integration_ref": "refs/heads/main",
                "merge_method": "squash",
                "title": "Candidate 42",
                "body": "Bounded summary.",
                "draft": False,
                "expected_secondary_effects": [],
            }
            operation = request(
                socket_path,
                "landing.invoke",
                parameters=parameters,
            )
            result = {
                "schema_version": 2,
                "change_id": "42",
                "base_ref": "refs/heads/main",
                "head_ref": "refs/heads/mgb/op-42",
                "candidate_revision": HEAD,
                "landing_revision": "c" * 40,
                "landing_tree": "d" * 40,
                "landing_parent_revisions": [BASE],
                "landing_commit_message": "Candidate 42\n\nBounded summary.",
                "primary_ref": "refs/heads/main",
                "primary_revision": "c" * 40,
                "first_parent_status": "contains",
                "title": "Candidate 42",
                "body": "Bounded summary.",
                "draft": False,
                "state": "closed",
                "closure_references": [],
                "secondary_effects": [],
            }
            value = {
                **operation.correlation_dict(),
                "outcome": "succeeded",
                "provider_status": 200,
                "request_bytes": 64,
                "response_bytes": 256,
                "error_code": None,
                "retry_after_seconds": None,
                "receipt": {
                    "remote_identity": "github:github.com/acme/widgets/pull/42",
                    "observed_revision": "c" * 40,
                    "evidence_sha256": "3" * 64,
                    "closed_references": [],
                    "result": result,
                },
            }
            ServiceOperationResponse.from_mapping(value).validate_correlation(operation)

            mismatches = (
                ("candidate", ("result", "candidate_revision", "e" * 40)),
                ("receipt", ("receipt", "observed_revision", "e" * 40)),
            )
            for label, (_scope, field, replacement) in mismatches:
                mismatched_value = copy.deepcopy(value)
                if _scope == "result":
                    mismatched_value["receipt"]["result"][field] = replacement
                else:
                    mismatched_value["receipt"][field] = replacement
                with (
                    self.subTest(label=label),
                    self.assertRaisesRegex(
                        ValidationError, "landing result does not match"
                    ),
                ):
                    ServiceOperationResponse.from_mapping(
                        mismatched_value
                    ).validate_correlation(operation)

            legacy_request_shape = request(
                socket_path,
                "landing.invoke",
                parameters={
                    key: value
                    for key, value in parameters.items()
                    if key != "expected_secondary_effects"
                },
            )
            legacy_shaped_value = {
                **value,
                **legacy_request_shape.correlation_dict(),
            }
            with self.assertRaisesRegex(
                ValidationError, "landing result does not match"
            ):
                ServiceOperationResponse.from_mapping(
                    legacy_shaped_value
                ).validate_correlation(legacy_request_shape)


if __name__ == "__main__":
    unittest.main()
