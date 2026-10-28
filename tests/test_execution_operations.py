from __future__ import annotations

import hashlib
import unittest
from copy import deepcopy

from magic_glass_box.db import ConflictError
from magic_glass_box.execution_operations import (
    ExecutionOperationError,
    ExecutionRecipeAuthority,
    build_execution_followup,
    build_execution_submission,
)
from magic_glass_box.service import ControlPlane
from magic_glass_box.service_sidecar import ServiceSidecarSpec

PROJECT_ID = "boxp_" + "1" * 32
CANDIDATE_ID = "cnd_" + "2" * 32
BASE = "a" * 40
HEAD = "b" * 40
TREE = "c" * 40
DEPLOYMENT = "d" * 64
SCOPE = "e" * 64
RECIPE_SHA256 = "4" * 64
IMAGE_ID = "sha256:" + "5" * 64
EXECUTION_ID = "rex_" + "3" * 32


def sidecar_spec() -> ServiceSidecarSpec:
    return ServiceSidecarSpec.from_mapping(
        {
            "schema_version": 2,
            "connector_protocol_version": 2,
            "deployment_sha256": DEPLOYMENT,
            "transport": {
                "kind": "unix",
                "socket_path": "/private/tmp/mgb-execution-tests/runner.sock",
            },
            "capabilities": [
                {"name": "execution.submit", "version": 1, "mode": "effect"},
                {"name": "execution.inspect", "version": 1, "mode": "read"},
                {"name": "execution.cancel", "version": 1, "mode": "effect"},
                {"name": "execution.collect", "version": 1, "mode": "read"},
            ],
            "mappings": [
                {
                    "name": "azure-quality",
                    "service": "ssh-runner",
                    "host": "runner.example.test",
                    "resource": {
                        "kind": "repository",
                        "external_id": "acme/widgets",
                    },
                    "account": "runner",
                    "credential_scope_sha256": SCOPE,
                }
            ],
            "request_bytes": 131072,
            "response_bytes": 4194304,
            "timeout_ms": 30000,
            "max_concurrency": 2,
            "max_requests_per_minute": 60,
        }
    )


def sidecar_record() -> dict[str, object]:
    spec = sidecar_spec()
    return {
        "id": "ssc_" + "6" * 32,
        "name": "remote-main",
        "state": "active",
        "content_sha256": spec.digest,
        "deployment_sha256": DEPLOYMENT,
        "spec": spec.to_dict(),
    }


def binding(capability: str) -> dict[str, object]:
    spec = sidecar_spec()
    mode = (
        "effect" if capability in {"execution.submit", "execution.cancel"} else "read"
    )
    return {
        "id": "cpb_" + capability.encode().hex()[:32].ljust(32, "0"),
        "state": "active",
        "project": {"id": PROJECT_ID, "key": "krab", "active": 1},
        "capability": {"name": capability, "version": 1, "mode": mode},
        "controller": {
            "sidecar_id": "ssc_" + "6" * 32,
            "sidecar_name": "remote-main",
            "sidecar_state": "active",
            "connector_protocol_version": 2,
            "sidecar_content_sha256": spec.digest,
            "deployment_sha256": DEPLOYMENT,
            "mapping": spec.mappings[0].to_dict(),
        },
    }


def candidate() -> dict[str, object]:
    return {
        "id": CANDIDATE_ID,
        "project_id": PROJECT_ID,
        "context_version_id": "ctx_" + "7" * 32,
        "work_thread_id": "wth_" + "8" * 32,
        "base_revision": BASE,
        "head_revision": HEAD,
        "tree_revision": TREE,
        "object_format": "sha1",
    }


def recipe() -> ExecutionRecipeAuthority:
    return ExecutionRecipeAuthority.from_mapping(
        {
            "recipe_id": "quality",
            "recipe_sha256": RECIPE_SHA256,
            "image_id": IMAGE_ID,
        }
    )


def successful_submission() -> dict[str, object]:
    spec = build_execution_submission(
        candidate=candidate(),
        binding=binding("execution.submit"),
        sidecar=sidecar_record(),
        recipe=recipe(),
        request_key="first-run",
    )
    result = {
        "schema_version": 1,
        "execution_id": EXECUTION_ID,
        "state": "running",
        "candidate_revision": HEAD,
        "object_format": "sha1",
        "recipe_id": "quality",
        "recipe_sha256": RECIPE_SHA256,
        "image_id": IMAGE_ID,
        "source_bytes": len(b"candidate archive"),
        "source_sha256": hashlib.sha256(b"candidate archive").hexdigest(),
        "submitted_at": "2026-10-28T12:00:00.000000+00:00",
        "started_at": "2026-10-28T12:00:01.000000+00:00",
        "finished_at": None,
        "exit_code": None,
    }
    return {
        "id": "sop_" + "9" * 32,
        "state": "succeeded",
        "project_capability_binding_id": binding("execution.submit")["id"],
        "spec": spec.to_dict(),
        "sidecar": {
            "id": "ssc_" + "6" * 32,
            "content_sha256": sidecar_spec().digest,
        },
        "attempts": [
            {
                "id": "soa_" + "a" * 32,
                "outcome": "succeeded",
                "receipt": {
                    "remote_identity": f"execution:{EXECUTION_ID}",
                    "observed_revision": HEAD,
                    "evidence_sha256": "f" * 64,
                    "closed_references": [],
                    "result": result,
                },
            }
        ],
    }


class ExecutionOperationBuilderTests(unittest.TestCase):
    def test_recipe_authority_is_strict_and_contains_no_command(self) -> None:
        authority = recipe()
        self.assertEqual(
            authority.to_dict(),
            {
                "image_id": IMAGE_ID,
                "recipe_id": "quality",
                "recipe_sha256": RECIPE_SHA256,
            },
        )
        with self.assertRaises(ExecutionOperationError):
            ExecutionRecipeAuthority.from_mapping(
                {**authority.to_dict(), "command": ["make", "quality"]}
            )
        for changed in (
            {**authority.to_dict(), "recipe_id": "Bad Recipe"},
            {**authority.to_dict(), "recipe_sha256": "bad"},
            {**authority.to_dict(), "image_id": "latest"},
        ):
            with (
                self.subTest(changed=changed),
                self.assertRaises(ExecutionOperationError),
            ):
                ExecutionRecipeAuthority.from_mapping(changed)

    def test_submission_is_derived_from_exact_candidate_and_active_binding(
        self,
    ) -> None:
        operation = build_execution_submission(
            candidate=candidate(),
            binding=binding("execution.submit"),
            sidecar=sidecar_record(),
            recipe=recipe(),
            request_key="first-run",
        )

        self.assertEqual(
            operation.to_dict(),
            {
                "capability": {
                    "mode": "effect",
                    "name": "execution.submit",
                    "version": 1,
                },
                "connector_name": "remote-main",
                "deployment_sha256": DEPLOYMENT,
                "idempotency_key": (f"execution-submit:{CANDIDATE_ID}:first-run"),
                "mapping": sidecar_spec().mappings[0].to_dict(),
                "parameters": recipe().to_dict(),
                "request_bytes": 131072,
                "response_bytes": 4194304,
                "schema_version": 2,
                "target": {
                    "desired_revision": HEAD,
                    "expected_revision": BASE,
                    "object_format": "sha1",
                    "object_id": CANDIDATE_ID,
                    "resource": {
                        "external_id": "acme/widgets",
                        "kind": "repository",
                    },
                },
                "timeout_ms": 30000,
            },
        )
        self.assertNotIn("runner.example.test", str(operation.parameters))

    def test_submission_rejects_cross_project_candidate_and_wrong_capability(
        self,
    ) -> None:
        other = candidate()
        other["project_id"] = "boxp_" + "f" * 32
        with self.assertRaises(ConflictError):
            build_execution_submission(
                candidate=other,
                binding=binding("execution.submit"),
                sidecar=sidecar_record(),
                recipe=recipe(),
                request_key="first-run",
            )

    def test_submission_rejects_inactive_or_malformed_authority(self) -> None:
        inactive = binding("execution.submit")
        inactive["state"] = "inactive"
        with self.assertRaises(ConflictError):
            build_execution_submission(
                candidate=candidate(),
                binding=inactive,
                sidecar=sidecar_record(),
                recipe=recipe(),
                request_key="first-run",
            )

        malformed = binding("execution.submit")
        del malformed["controller"]["mapping"]
        with self.assertRaises(ExecutionOperationError):
            build_execution_submission(
                candidate=candidate(),
                binding=malformed,
                sidecar=sidecar_record(),
                recipe=recipe(),
                request_key="first-run",
            )

        for changed_candidate, key in (
            ({**candidate(), "id": "bad"}, "first-run"),
            (candidate(), "Bad Key"),
        ):
            with self.subTest(key=key), self.assertRaises(ExecutionOperationError):
                build_execution_submission(
                    candidate=changed_candidate,
                    binding=binding("execution.submit"),
                    sidecar=sidecar_record(),
                    recipe=recipe(),
                    request_key=key,
                )
        with self.assertRaises(ExecutionOperationError):
            build_execution_submission(
                candidate=candidate(),
                binding=binding("execution.inspect"),
                sidecar=sidecar_record(),
                recipe=recipe(),
                request_key="first-run",
            )

    def test_followup_is_derived_from_successful_submission_receipt(self) -> None:
        source = successful_submission()
        operation = build_execution_followup(
            submission=source,
            submission_binding=binding("execution.submit"),
            binding=binding("execution.collect"),
            sidecar=sidecar_record(),
            action="collect",
            request_key="terminal-evidence",
        )

        self.assertEqual(
            operation.to_dict(),
            {
                "capability": {
                    "mode": "read",
                    "name": "execution.collect",
                    "version": 1,
                },
                "connector_name": "remote-main",
                "deployment_sha256": DEPLOYMENT,
                "idempotency_key": (
                    f"execution-collect:{source['id']}:terminal-evidence"
                ),
                "mapping": sidecar_spec().mappings[0].to_dict(),
                "parameters": {
                    "execution_id": EXECUTION_ID,
                    "image_id": IMAGE_ID,
                    "recipe_id": "quality",
                    "recipe_sha256": RECIPE_SHA256,
                    "submission_operation_id": source["id"],
                },
                "request_bytes": 131072,
                "response_bytes": 4194304,
                "schema_version": 2,
                "target": {
                    "desired_revision": HEAD,
                    "expected_revision": BASE,
                    "object_format": "sha1",
                    "object_id": CANDIDATE_ID,
                    "resource": {
                        "external_id": "acme/widgets",
                        "kind": "repository",
                    },
                },
                "timeout_ms": 30000,
            },
        )

    def test_followup_rejects_untrusted_or_cross_controller_provenance(self) -> None:
        source = successful_submission()
        source["state"] = "uncertain"
        with self.assertRaises(ConflictError):
            build_execution_followup(
                submission=source,
                submission_binding=binding("execution.submit"),
                binding=binding("execution.inspect"),
                sidecar=sidecar_record(),
                action="inspect",
                request_key="poll-one",
            )

    def test_followup_rejects_missing_receipt_and_identity_mismatches(self) -> None:
        invalid_documents: list[tuple[dict[str, object], type[Exception]]] = []
        invalid_action = successful_submission()
        invalid_documents.append((invalid_action, ExecutionOperationError))

        invalid_id = successful_submission()
        invalid_id["id"] = "bad"
        invalid_documents.append((invalid_id, ExecutionOperationError))

        no_attempt = successful_submission()
        no_attempt["attempts"] = []
        invalid_documents.append((no_attempt, ConflictError))

        wrong_remote = successful_submission()
        wrong_remote["attempts"][0]["receipt"]["remote_identity"] = (
            "execution:rex_" + "0" * 32
        )
        invalid_documents.append((wrong_remote, ConflictError))

        for index, (source, error) in enumerate(invalid_documents):
            with self.subTest(index=index), self.assertRaises(error):
                build_execution_followup(
                    submission=source,
                    submission_binding=binding("execution.submit"),
                    binding=binding("execution.inspect"),
                    sidecar=sidecar_record(),
                    action="shell" if index == 0 else "inspect",
                    request_key="poll-one",
                )

        source = successful_submission()
        wrong_binding = binding("execution.submit")
        wrong_binding["id"] = "cpb_" + "f" * 32
        with self.assertRaises(ConflictError):
            build_execution_followup(
                submission=source,
                submission_binding=wrong_binding,
                binding=binding("execution.inspect"),
                sidecar=sidecar_record(),
                action="inspect",
                request_key="poll-one",
            )

        source = successful_submission()
        source["sidecar"] = None
        with self.assertRaises(ExecutionOperationError):
            build_execution_followup(
                submission=source,
                submission_binding=binding("execution.submit"),
                binding=binding("execution.inspect"),
                sidecar=sidecar_record(),
                action="inspect",
                request_key="poll-one",
            )

        source = successful_submission()
        changed_sidecar = deepcopy(sidecar_record())
        changed_sidecar["id"] = "ssc_" + "0" * 32
        with self.assertRaises(ConflictError):
            build_execution_followup(
                submission=source,
                submission_binding=binding("execution.submit"),
                binding=binding("execution.inspect"),
                sidecar=changed_sidecar,
                action="inspect",
                request_key="poll-one",
            )

        source = successful_submission()
        wrong = binding("execution.inspect")
        wrong["controller"]["mapping"]["credential_scope_sha256"] = "0" * 64
        with self.assertRaises(ConflictError):
            build_execution_followup(
                submission=source,
                submission_binding=binding("execution.submit"),
                binding=wrong,
                sidecar=sidecar_record(),
                action="inspect",
                request_key="poll-one",
            )


class FakeExecutionDatabase:
    def __init__(self) -> None:
        self.created: list[dict[str, object]] = []
        self.submission = successful_submission()

    def get_user(self, handle: str) -> dict[str, object]:
        return {"id": "boxu_" + "b" * 32, "handle": handle, "active": 1}

    def get_candidate(self, candidate_id: str) -> dict[str, object]:
        self.assert_equal(candidate_id, CANDIDATE_ID)
        return candidate()

    def resolve_project_capability(
        self, project_key: str, capability_name: str, capability_version: int
    ) -> dict[str, object]:
        self.assert_equal(project_key, "krab")
        self.assert_equal(capability_version, 1)
        return binding(capability_name)

    def get_service_sidecar(self, name: str) -> dict[str, object]:
        self.assert_equal(name, "remote-main")
        return sidecar_record()

    def create_service_operation(self, **values: object) -> dict[str, object]:
        self.created.append(values)
        return {"id": "sop_" + "c" * 32, **values}

    def get_service_operation(self, operation_id: str) -> dict[str, object]:
        self.assert_equal(operation_id, self.submission["id"])
        return self.submission

    def get_project_capability_binding(self, binding_id: str) -> dict[str, object]:
        self.assert_equal(binding_id, binding("execution.submit")["id"])
        return binding("execution.submit")

    @staticmethod
    def assert_equal(actual: object, expected: object) -> None:
        if actual != expected:
            raise AssertionError(f"{actual!r} != {expected!r}")


class ExecutionControlPlaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.database = FakeExecutionDatabase()
        self.control = ControlPlane(self.database)  # type: ignore[arg-type]

    def test_control_plane_creates_candidate_derived_submission(self) -> None:
        created = self.control.create_execution_submission(
            project_key="krab",
            candidate_id=CANDIDATE_ID,
            recipe=recipe(),
            request_key="first-run",
            created_by_handle="operator",
        )

        spec = created["spec"]
        self.assertEqual(spec.target.object_id, CANDIDATE_ID)
        self.assertEqual(
            created["project_capability_binding_id"],
            binding("execution.submit")["id"],
        )

    def test_control_plane_creates_source_derived_followup(self) -> None:
        created = self.control.create_execution_followup(
            submission_operation_id=self.database.submission["id"],
            action="collect",
            request_key="terminal-evidence",
            created_by_handle="operator",
        )

        spec = created["spec"]
        self.assertEqual(spec.capability.name, "execution.collect")
        self.assertEqual(
            spec.parameters["submission_operation_id"],
            self.database.submission["id"],
        )


if __name__ == "__main__":
    unittest.main()
