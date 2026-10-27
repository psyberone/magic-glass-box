from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from magic_glass_box.db import SCHEMA_VERSION, ConflictError, Database, NotFoundError
from magic_glass_box.domain import ProjectContext, ValidationError, WorkItemEnvelope
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationResponse,
)
from magic_glass_box.storage.schema import SCHEMA_V42
from magic_glass_box.work_intake import (
    WorkItemListResult,
    WorkItemReference,
    WorkItemStateResult,
)
from tests.test_service_sidecars import resource_operation_spec, resource_sidecar_spec


def work_item() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "example-work",
        "external_id": "ABC-42",
        "source_updated_at": "2026-10-27T12:00:00.000000Z",
        "title": "Prove normalized work intake",
        "brief": "Import one bounded provider result through explicit authority.",
        "acceptance_criteria": ["The import retains exact provenance"],
        "constraints": ["Do not retain a raw provider document"],
        "context": [],
        "labels": ["m14"],
        "priority": "normal",
    }


def work_reference(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "object_id": "ABC-42",
        "external_id": "ABC-42",
        "source_updated_at": "2026-10-27T12:00:00.000000Z",
        "title": "Prove normalized work intake",
        "assignees": ["agent-one"],
        "labels": ["m14"],
    }
    value.update(overrides)
    return value


def operation_request(
    *, capability: str, parameters: dict[str, object] | None = None
) -> ServiceOperationRequest:
    default_parameters: dict[str, object] = (
        {"limit": 100}
        if capability == "work.list"
        else ({"desired_state": "done"} if capability == "work.state" else {})
    )
    operation = resource_operation_spec(
        capability={
            "name": capability,
            "version": 1,
            "mode": "effect" if capability == "work.state" else "read",
        },
        idempotency_key=f"{capability}-normalized-result",
        target={
            **resource_operation_spec().target.to_dict(),
            "object_id": "work-items" if capability == "work.list" else "ABC-42",
        },
        parameters=default_parameters if parameters is None else parameters,
    )
    return ServiceOperationRequest.create(
        operation_id="sop_" + "1" * 32,
        connector_id="ssc_" + "2" * 32,
        connector_protocol_version=2,
        operation=operation,
        phase="invoke",
        deadline="2026-10-27T18:00:00.000000+00:00",
    )


def response_for(request: ServiceOperationRequest, result: object) -> dict[str, object]:
    value = request.correlation_dict()
    value.update(
        {
            "outcome": "succeeded",
            "provider_status": 200,
            "request_bytes": 128,
            "response_bytes": 512,
            "error_code": None,
            "retry_after_seconds": None,
            "receipt": {
                "remote_identity": "example-work:ABC-42",
                "observed_revision": None,
                "evidence_sha256": "e" * 64,
                "closed_references": [],
                "result": result,
            },
        }
    )
    return value


class WorkControllerResultTests(unittest.TestCase):
    def test_published_list_result_matches_the_runtime_contract(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        schema = json.loads(
            (repository / "schemas/work-item-list-result-v1.schema.json").read_text(
                encoding="utf-8"
            )
        )
        example = json.loads(
            (repository / "examples/work-item-list-result.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        validator.validate(example)

        result = WorkItemListResult.from_mapping(example)
        self.assertEqual(result.to_dict(), example)
        self.assertEqual(
            schema["$id"],
            "urn:magic-glass-box:schema:work-item-list-result:v1",
        )

    def test_published_state_result_matches_the_runtime_contract(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        schema = json.loads(
            (repository / "schemas/work-item-state-result-v1.schema.json").read_text(
                encoding="utf-8"
            )
        )
        example = json.loads(
            (repository / "examples/work-item-state-result.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        validator.validate(example)
        self.assertEqual(WorkItemStateResult.from_mapping(example).to_dict(), example)
        self.assertEqual(
            schema["$id"],
            "urn:magic-glass-box:schema:work-item-state-result:v1",
        )

        invalid = (
            [],
            {"schema_version": 1},
            {**example, "provider_status": "Done"},
            {**example, "schema_version": True},
            {**example, "object_id": " "},
            {**example, "desired_state": "open"},
            {**example, "state": "open"},
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                WorkItemStateResult.from_mapping(value)

    def test_reference_and_list_result_are_strict_bounded_and_canonical(self) -> None:
        reference = WorkItemReference.from_mapping(work_reference())
        result = WorkItemListResult.from_mapping(
            {
                "schema_version": 1,
                "items": [reference.to_dict()],
                "truncated": False,
            }
        )

        self.assertEqual(result.items, (reference,))
        self.assertEqual(result.to_dict()["items"], [work_reference()])
        self.assertLessEqual(len(result.canonical.encode("utf-8")), 256 * 1024)

        for forbidden in ("cursor", "next_cursor", "state", "query", "description"):
            with (
                self.subTest(forbidden=forbidden),
                self.assertRaisesRegex(ValidationError, "unknown fields"),
            ):
                WorkItemListResult.from_mapping(
                    {
                        "schema_version": 1,
                        "items": [work_reference()],
                        "truncated": False,
                        forbidden: "provider-native",
                    }
                )

    def test_reference_and_list_reject_incomplete_or_ambiguous_metadata(self) -> None:
        reference_cases = (
            ([], "must be an object"),
            ({**work_reference(), "state": "open"}, "unknown fields"),
            (
                {
                    key: value
                    for key, value in work_reference().items()
                    if key != "object_id"
                },
                "missing fields",
            ),
            (work_reference(title=" \n"), "must not be blank"),
            (work_reference(assignees="agent-one"), "list of at most 64"),
            (work_reference(labels=["m14", "m14"]), "must be unique"),
        )
        for value, message in reference_cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                WorkItemReference.from_mapping(value)  # type: ignore[arg-type]

        list_cases = (
            ([], "must be an object"),
            ({"schema_version": 1, "items": []}, "missing fields"),
            (
                {"schema_version": 1, "items": [], "truncated": 0},
                "must be a boolean",
            ),
        )
        for value, message in list_cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                WorkItemListResult.from_mapping(value)  # type: ignore[arg-type]

    def test_reference_accepts_its_exact_collection_boundaries(self) -> None:
        reference = WorkItemReference.from_mapping(
            work_reference(
                assignees=[f"agent-{index}" for index in range(64)],
                labels=[f"label-{index}" for index in range(64)],
            )
        )

        self.assertEqual(len(reference.assignees), 64)
        self.assertEqual(len(reference.labels), 64)

    def test_list_result_rejects_noncanonical_duplicates_and_size_overflow(
        self,
    ) -> None:
        invalid = (
            ({"schema_version": True, "items": [], "truncated": False}, "schema"),
            ({"schema_version": 1, "items": "items", "truncated": False}, "items"),
            (
                {
                    "schema_version": 1,
                    "items": [work_reference(), work_reference()],
                    "truncated": False,
                },
                "unique",
            ),
            (
                {
                    "schema_version": 1,
                    "items": [
                        work_reference(),
                        work_reference(external_id="ABC-43"),
                    ],
                    "truncated": False,
                },
                "unique",
            ),
            (
                {
                    "schema_version": 1,
                    "items": [
                        work_reference(),
                        work_reference(object_id="ABC-43"),
                    ],
                    "truncated": False,
                },
                "unique",
            ),
            (
                {
                    "schema_version": 1,
                    "items": [work_reference(title="x" * 513)],
                    "truncated": False,
                },
                "title",
            ),
            (
                {
                    "schema_version": 1,
                    "items": [
                        work_reference(
                            object_id=f"ABC-{index}", external_id=f"ABC-{index}"
                        )
                        for index in range(101)
                    ],
                    "truncated": True,
                },
                "100",
            ),
        )
        for value, message in invalid:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                WorkItemListResult.from_mapping(value)

        with self.assertRaisesRegex(ValidationError, "byte limit"):
            WorkItemListResult.from_mapping(
                {
                    "schema_version": 1,
                    "items": [
                        work_reference(
                            object_id=f"ABC-{index}",
                            external_id=f"ABC-{index}",
                            title="t" * 512,
                            assignees=[
                                f"person-{index}-{entry}-" + "a" * 230
                                for entry in range(16)
                            ],
                            labels=[
                                f"label-{index}-{entry}-" + "b" * 100
                                for entry in range(16)
                            ],
                        )
                        for index in range(100)
                    ],
                    "truncated": True,
                }
            )

    def test_work_results_are_validated_at_the_controller_ingress(self) -> None:
        get_request = operation_request(capability="work.get")
        get_response = ServiceOperationResponse.from_mapping(
            response_for(get_request, work_item())
        )
        self.assertEqual(
            WorkItemEnvelope.from_mapping(get_response.receipt.result).external_id,
            "ABC-42",
        )

        list_request = operation_request(capability="work.list")
        list_result = {
            "schema_version": 1,
            "items": [work_reference()],
            "truncated": False,
        }
        list_response = ServiceOperationResponse.from_mapping(
            response_for(list_request, list_result)
        )
        self.assertEqual(list_response.receipt.result, list_result)

        state_request = operation_request(capability="work.state")
        state_result = {
            "desired_state": "done",
            "object_id": "ABC-42",
            "schema_version": 1,
            "state": "done",
        }
        state_response = ServiceOperationResponse.from_mapping(
            response_for(state_request, state_result)
        )
        self.assertEqual(state_response.receipt.result, state_result)
        state_response.validate_correlation(state_request)

        wrong_object = ServiceOperationResponse.from_mapping(
            response_for(state_request, {**state_result, "object_id": "ABC-43"})
        )
        with self.assertRaises(ValidationError):
            wrong_object.validate_correlation(state_request)

        wrong_parameters_request = operation_request(
            capability="work.state", parameters={"desired_state": "open"}
        )
        wrong_parameters = ServiceOperationResponse.from_mapping(
            response_for(wrong_parameters_request, state_result)
        )
        with self.assertRaises(ValidationError):
            wrong_parameters.validate_correlation(wrong_parameters_request)

        malformed = (
            (get_request, {**work_item(), "raw": {"provider": "body"}}),
            (list_request, {"items": [], "next_cursor": "jira-token"}),
            (list_request, [{"external_id": "ABC-42"}]),
            (state_request, {**state_result, "transition_id": "31"}),
            (state_request, {**state_result, "state": "provider-done"}),
        )
        for request, result in malformed:
            with self.subTest(result=result), self.assertRaises(ValidationError):
                ServiceOperationResponse.from_mapping(response_for(request, result))

    def test_work_result_must_be_canonical_and_present_on_success(self) -> None:
        request = operation_request(capability="work.get")
        noncanonical = {
            **work_item(),
            "source_updated_at": "2026-10-27T12:00:00+00:00",
        }
        with self.assertRaisesRegex(ValidationError, "canonical"):
            ServiceOperationResponse.from_mapping(response_for(request, noncanonical))

        missing = response_for(request, work_item())
        del missing["receipt"]["result"]
        with self.assertRaisesRegex(ValidationError, "result"):
            ServiceOperationResponse.from_mapping(missing)


class WorkItemServiceImportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        instance = self.database.create_krab_instance("local", "http://localhost:8765")
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "1" * 32,
            krab_project_key="demo-project",
            display_name="Demo Project",
            context=ProjectContext.from_mapping({"purpose": "Test work intake"}),
            created_by=self.user["id"],
        )
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="other-project",
            display_name="Other Project",
            context=ProjectContext.from_mapping({"purpose": "Prove project scope"}),
            created_by=self.user["id"],
        )
        self.database.register_service_sidecar(
            name="example-work-sidecar",
            spec=resource_sidecar_spec(self.root / "work.sock"),
            created_by=self.user["id"],
        )
        self.binding = self.database.bind_project_capability(
            project_key="demo-project",
            capability_name="work.get",
            capability_version=1,
            sidecar_name="example-work-sidecar",
            mapping_name="primary",
            bound_by=self.user["id"],
        )

    def complete_get(
        self,
        *,
        result: dict[str, object] | None = None,
        idempotency_key: str = "get-work-abc-42",
        binding_id: str | None = None,
    ) -> dict[str, object]:
        operation = self.database.create_service_operation(
            spec=resource_operation_spec(idempotency_key=idempotency_key),
            created_by=self.user["id"],
            project_capability_binding_id=(
                self.binding["id"] if binding_id is None else binding_id
            ),
        )
        dispatch = self.database.begin_service_operation_attempt(
            operation["id"],
            phase="invoke",
            owner_id="supervisor-one",
            expected_version=1,
            lease_seconds=30,
        )
        request = ServiceOperationRequest.from_mapping(dispatch["request"])
        return self.database.complete_service_operation_attempt(
            dispatch["attempt"]["id"],
            owner_id="supervisor-one",
            expected_operation_version=2,
            response=ServiceOperationResponse.from_mapping(
                response_for(request, result or work_item())
            ),
        )

    def test_completed_bound_get_imports_once_with_exact_provenance(self) -> None:
        operation = self.complete_get()

        first = self.database.import_work_item_from_service_operation(
            project_key="demo-project",
            operation_id=operation["id"],
            imported_by=self.user["id"],
        )
        repeated = self.database.import_work_item_from_service_operation(
            project_key="demo-project",
            operation_id=operation["id"],
            imported_by=self.user["id"],
        )

        self.assertEqual(first["import_status"], "created")
        self.assertEqual(repeated["import_status"], "unchanged")
        self.assertEqual(repeated["id"], first["id"])
        self.assertTrue(first["id"].startswith("wki_"))
        self.assertTrue(first["work_item_version_id"].startswith("wiv_"))
        self.assertTrue(first["work_thread"]["thread_id"].startswith("wth_"))
        self.assertEqual(len(repeated["service_imports"]), 1)
        provenance = repeated["service_imports"][0]
        self.assertTrue(provenance["id"].startswith("wsi_"))
        self.assertEqual(provenance["service_operation_id"], operation["id"])
        self.assertEqual(
            provenance["service_operation_attempt_id"], operation["attempts"][0]["id"]
        )
        self.assertEqual(provenance["operation_sha256"], operation["content_sha256"])
        self.assertEqual(provenance["capability_binding_id"], self.binding["id"])
        self.assertEqual(provenance["imported_by"], "operator")
        self.assertEqual(
            provenance["work_item_version_id"], first["work_item_version_id"]
        )
        self.assertEqual(len(provenance["receipt_sha256"]), 64)
        self.assertEqual(
            self.database.status()["counts"]["work_item_service_imports"], 1
        )

    def test_distinct_reads_of_the_same_revision_are_separate_no_op_imports(
        self,
    ) -> None:
        first_operation = self.complete_get(idempotency_key="same-revision-first")
        first = self.database.import_work_item_from_service_operation(
            project_key="demo-project",
            operation_id=first_operation["id"],
            imported_by=self.user["id"],
        )
        second_operation = self.complete_get(idempotency_key="same-revision-second")
        second = self.database.import_work_item_from_service_operation(
            project_key="demo-project",
            operation_id=second_operation["id"],
            imported_by=self.user["id"],
        )

        self.assertEqual(second["import_status"], "unchanged")
        self.assertEqual(second["id"], first["id"])
        self.assertEqual(second["version"], 1)
        self.assertEqual(len(second["service_imports"]), 2)
        self.assertEqual(
            [item["service_operation_id"] for item in second["service_imports"]],
            [first_operation["id"], second_operation["id"]],
        )

    def test_import_rejects_invalid_or_unknown_operation_identity(self) -> None:
        with self.assertRaisesRegex(ValidationError, "operation id"):
            self.database.import_work_item_from_service_operation(
                project_key="demo-project",
                operation_id=object(),  # type: ignore[arg-type]
                imported_by=self.user["id"],
            )
        with self.assertRaisesRegex(NotFoundError, "service operation not found"):
            self.database.import_work_item_from_service_operation(
                project_key="demo-project",
                operation_id="sop_" + "f" * 32,
                imported_by=self.user["id"],
            )

    def test_later_reads_upsert_and_each_retain_their_receipt(self) -> None:
        first_operation = self.complete_get()
        first = self.database.import_work_item_from_service_operation(
            project_key="demo-project",
            operation_id=first_operation["id"],
            imported_by=self.user["id"],
        )
        later_document = {
            **work_item(),
            "source_updated_at": "2026-10-28T12:00:00.000000Z",
            "brief": "Import the changed normalized result without synchronizing.",
        }
        later_operation = self.complete_get(
            result=later_document,
            idempotency_key="get-work-abc-42-later",
        )
        later = self.database.import_work_item_from_service_operation(
            project_key="demo-project",
            operation_id=later_operation["id"],
            imported_by=self.user["id"],
        )

        self.assertEqual(later["id"], first["id"])
        self.assertEqual(later["version"], 2)
        self.assertEqual(later["import_status"], "updated")
        self.assertEqual(len(later["service_imports"]), 2)
        self.assertEqual(
            [entry["service_operation_id"] for entry in later["service_imports"]],
            [first_operation["id"], later_operation["id"]],
        )
        self.assertEqual(
            later["service_imports"][-1]["work_item_version_id"],
            later["work_item_version_id"],
        )

    def test_import_requires_succeeded_project_bound_work_get(self) -> None:
        pending = self.database.create_service_operation(
            spec=resource_operation_spec(idempotency_key="pending-get"),
            created_by=self.user["id"],
            project_capability_binding_id=self.binding["id"],
        )
        for project_key, operation_id, message in (
            ("demo-project", pending["id"], "succeeded"),
            ("other-project", self.complete_get()["id"], "project"),
        ):
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ConflictError, message),
            ):
                self.database.import_work_item_from_service_operation(
                    project_key=project_key,
                    operation_id=operation_id,
                    imported_by=self.user["id"],
                )

        list_binding = self.database.bind_project_capability(
            project_key="demo-project",
            capability_name="work.list",
            capability_version=1,
            sidecar_name="example-work-sidecar",
            mapping_name="primary",
            bound_by=self.user["id"],
        )
        list_operation = self.database.create_service_operation(
            spec=resource_operation_spec(
                capability={"name": "work.list", "version": 1, "mode": "read"},
                idempotency_key="list-is-not-importable",
                target={
                    **resource_operation_spec().target.to_dict(),
                    "object_id": "work-items",
                },
                parameters={"limit": 100},
            ),
            created_by=self.user["id"],
            project_capability_binding_id=list_binding["id"],
        )
        list_dispatch = self.database.begin_service_operation_attempt(
            list_operation["id"],
            phase="invoke",
            owner_id="supervisor-list",
            expected_version=1,
            lease_seconds=30,
        )
        list_request = ServiceOperationRequest.from_mapping(list_dispatch["request"])
        list_operation = self.database.complete_service_operation_attempt(
            list_dispatch["attempt"]["id"],
            owner_id="supervisor-list",
            expected_operation_version=2,
            response=ServiceOperationResponse.from_mapping(
                response_for(
                    list_request,
                    {"schema_version": 1, "items": [], "truncated": False},
                )
            ),
        )
        with self.assertRaisesRegex(ConflictError, "work.get"):
            self.database.import_work_item_from_service_operation(
                project_key="demo-project",
                operation_id=list_operation["id"],
                imported_by=self.user["id"],
            )

        unbound = self.database.create_service_operation(
            spec=resource_operation_spec(idempotency_key="unbound-get"),
            created_by=self.user["id"],
        )
        dispatch = self.database.begin_service_operation_attempt(
            unbound["id"],
            phase="invoke",
            owner_id="supervisor-two",
            expected_version=1,
            lease_seconds=30,
        )
        request = ServiceOperationRequest.from_mapping(dispatch["request"])
        completed = self.database.complete_service_operation_attempt(
            dispatch["attempt"]["id"],
            owner_id="supervisor-two",
            expected_operation_version=2,
            response=ServiceOperationResponse.from_mapping(
                response_for(request, work_item())
            ),
        )
        with self.assertRaisesRegex(ConflictError, "project-bound"):
            self.database.import_work_item_from_service_operation(
                project_key="demo-project",
                operation_id=completed["id"],
                imported_by=self.user["id"],
            )

    def test_disabling_work_get_stops_new_reads_without_erasing_a_result(self) -> None:
        completed = self.complete_get()
        self.database.unbind_project_capability(
            self.binding["id"],
            expected_version=1,
            unbound_by=self.user["id"],
        )

        imported = self.database.import_work_item_from_service_operation(
            project_key="demo-project",
            operation_id=completed["id"],
            imported_by=self.user["id"],
        )
        self.assertEqual(imported["import_status"], "created")
        with self.assertRaisesRegex(ConflictError, "capability is unavailable"):
            self.database.create_service_operation(
                spec=resource_operation_spec(idempotency_key="disabled-get"),
                created_by=self.user["id"],
                project_capability_binding_id=self.binding["id"],
            )

    def test_schema_forty_three_retains_an_immutable_import_ledger(self) -> None:
        self.assertEqual(SCHEMA_VERSION, 43)
        operation = self.complete_get()
        imported = self.database.import_work_item_from_service_operation(
            project_key="demo-project",
            operation_id=operation["id"],
            imported_by=self.user["id"],
        )
        provenance = imported["service_imports"][0]
        with closing(self.database.connect()) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE work_item_service_imports SET imported_by = ? WHERE id = ?",
                    (self.user["id"], provenance["id"]),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM work_item_service_imports WHERE id = ?",
                    (provenance["id"],),
                )

    def test_schema_forty_two_migrates_to_the_import_ledger_once(self) -> None:
        path = self.root / "schema-v42.db"
        with sqlite3.connect(path) as connection:
            connection.executescript(SCHEMA_V42)
            connection.execute("PRAGMA user_version = 42")
        path.chmod(0o600)
        database = Database(path)

        first = database.initialize()
        second = database.initialize()

        self.assertEqual(first, second)
        self.assertEqual(first["schema_version"], 43)
        self.assertEqual(first["counts"]["work_item_service_imports"], 0)
        with closing(database.connect()) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table'"
                )
            }
        self.assertIn("work_item_service_imports", tables)


if __name__ == "__main__":
    unittest.main()
