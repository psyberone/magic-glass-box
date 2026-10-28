from __future__ import annotations

import base64
import hashlib
import unittest
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from magic_glass_box.domain import ValidationError, canonical_json
from magic_glass_box.execution_provider import (
    ControlledExecutionProvider,
    ExecutionBackendAmbiguous,
    ExecutionBackendUnavailable,
    ExecutionEvidenceResult,
    ExecutionJobSpec,
    ExecutionProviderError,
    ExecutionStatusResult,
)
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationSpec,
    ServiceSidecarSpec,
)

CONNECTOR_ID = "ssc_" + "1" * 32
DEPLOYMENT = "d" * 64
SCOPE = "e" * 64
BASE = "a" * 40
HEAD = "b" * 40
CANDIDATE_ID = "cnd_" + "2" * 32
EXECUTION_ID = "rex_" + "3" * 32
RECIPE_SHA256 = "4" * 64
IMAGE_ID = "sha256:" + "5" * 64
DEADLINE = "2099-01-01T00:00:00.000000+00:00"


def status_result(
    *,
    state: str = "running",
    execution_id: str = EXECUTION_ID,
    candidate_revision: str = HEAD,
    recipe_id: str = "quality",
    recipe_sha256: str = RECIPE_SHA256,
    image_id: str = IMAGE_ID,
) -> ExecutionStatusResult:
    values: dict[str, object] = {
        "schema_version": 1,
        "execution_id": execution_id,
        "state": state,
        "candidate_revision": candidate_revision,
        "object_format": "sha1",
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha256,
        "image_id": image_id,
        "source_bytes": len(b"candidate archive"),
        "source_sha256": hashlib.sha256(b"candidate archive").hexdigest(),
        "submitted_at": "2026-10-28T12:00:00.000000+00:00",
        "started_at": "2026-10-28T12:00:01.000000+00:00",
        "finished_at": None,
        "exit_code": None,
    }
    if state == "queued":
        values["started_at"] = None
    elif state == "succeeded":
        values.update(
            {
                "finished_at": "2026-10-28T12:00:05.000000+00:00",
                "exit_code": 0,
            }
        )
    elif state == "failed":
        values.update(
            {
                "finished_at": "2026-10-28T12:00:05.000000+00:00",
                "exit_code": 7,
            }
        )
    elif state == "cancelled":
        values.update(
            {
                "finished_at": "2026-10-28T12:00:05.000000+00:00",
                "exit_code": None,
            }
        )
    return ExecutionStatusResult.from_mapping(values)


def evidence_result(*, state: str = "succeeded") -> ExecutionEvidenceResult:
    bundle = b"remote evidence\n"
    return ExecutionEvidenceResult.from_mapping(
        {
            "schema_version": 1,
            "execution": status_result(state=state).to_dict(),
            "duration_ms": 4000,
            "output_bytes": 18,
            "output_sha256": "6" * 64,
            "artifact_bytes": len(bundle),
            "artifact_count": 1,
            "artifact_sha256": hashlib.sha256(bundle).hexdigest(),
            "artifact_encoding": "tar+gzip+base64",
            "artifact_bundle": base64.b64encode(bundle).decode("ascii"),
        }
    )


def sidecar_spec(socket_path: Path, *, schema_version: int = 1) -> ServiceSidecarSpec:
    mapping_resource: dict[str, object]
    if schema_version == 1:
        mapping_resource = {"repository": "acme/widgets"}
    else:
        mapping_resource = {
            "resource": {"kind": "repository", "external_id": "acme/widgets"}
        }
    return ServiceSidecarSpec.from_mapping(
        {
            "schema_version": schema_version,
            "connector_protocol_version": schema_version,
            "deployment_sha256": DEPLOYMENT,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "capabilities": [
                {"name": "execution.submit", "version": 1, "mode": "effect"},
                {"name": "execution.inspect", "version": 1, "mode": "read"},
                {"name": "execution.cancel", "version": 1, "mode": "effect"},
                {"name": "execution.collect", "version": 1, "mode": "read"},
            ],
            "mappings": [
                {
                    **mapping_resource,
                    "name": "azure-quality",
                    "service": "ssh-runner",
                    "host": "runner.example.test",
                    "account": "runner",
                    "credential_scope_sha256": SCOPE,
                }
            ],
            "request_bytes": 128 * 1024,
            "response_bytes": 4 * 1024 * 1024,
            "timeout_ms": 30000,
            "max_concurrency": 2,
            "max_requests_per_minute": 60,
        }
    )


def request(
    socket_path: Path,
    capability: str,
    *,
    phase: str = "invoke",
    parameters: Mapping[str, Any] | None = None,
    schema_version: int = 1,
    operation_id: str = "sop_" + "7" * 32,
) -> ServiceOperationRequest:
    spec = sidecar_spec(socket_path, schema_version=schema_version)
    values = {
        "recipe_id": "quality",
        "recipe_sha256": RECIPE_SHA256,
        "image_id": IMAGE_ID,
    }
    if capability != "execution.submit":
        values["execution_id"] = EXECUTION_ID
        values["submission_operation_id"] = "sop_" + "7" * 32
    values.update(parameters or {})
    if schema_version == 1:
        target: dict[str, object] = {
            "repository": "acme/widgets",
            "resource_id": CANDIDATE_ID,
            "object_format": "sha1",
            "expected_revision": BASE,
            "desired_revision": HEAD,
        }
    else:
        target = {
            "resource": {"kind": "repository", "external_id": "acme/widgets"},
            "object_id": CANDIDATE_ID,
            "object_format": "sha1",
            "expected_revision": BASE,
            "desired_revision": HEAD,
        }
    operation = ServiceOperationSpec.from_mapping(
        {
            "schema_version": schema_version,
            "connector_name": "remote-main",
            "deployment_sha256": DEPLOYMENT,
            "mapping": spec.mappings[0].to_dict(),
            "capability": {
                "name": capability,
                "version": 1,
                "mode": (
                    "effect"
                    if capability in {"execution.submit", "execution.cancel"}
                    else "read"
                ),
            },
            "idempotency_key": f"m15:{capability}:{CANDIDATE_ID}",
            "target": target,
            "parameters": values,
            "request_bytes": 128 * 1024,
            "response_bytes": 4 * 1024 * 1024,
            "timeout_ms": 30000,
        }
    )
    return ServiceOperationRequest.create(
        operation_id=operation_id,
        connector_id=CONNECTOR_ID,
        connector_protocol_version=schema_version,
        operation=operation,
        phase=phase,
        deadline=DEADLINE,
    )


class FakeExecutionBackend:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ExecutionJobSpec, str | None]] = []
        self.submission = status_result()
        self.inspection: ExecutionStatusResult | None = status_result()
        self.cancellation: ExecutionStatusResult | None = status_result(
            state="cancelled"
        )
        self.reconciled_cancellation: ExecutionStatusResult | None = self.cancellation
        self.evidence: ExecutionEvidenceResult | None = evidence_result()
        self.reconciled: ExecutionStatusResult | None = self.submission
        self.error: BaseException | None = None

    def _raise(self) -> None:
        if self.error is not None:
            raise self.error

    def submit(self, job: ExecutionJobSpec, *, deadline: str) -> ExecutionStatusResult:
        self.calls.append(("submit", job, None))
        self._raise()
        return self.submission

    def reconcile_submit(
        self, job: ExecutionJobSpec, *, deadline: str
    ) -> ExecutionStatusResult | None:
        self.calls.append(("reconcile_submit", job, None))
        self._raise()
        return self.reconciled

    def inspect(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionStatusResult | None:
        self.calls.append(("inspect", job, execution_id))
        self._raise()
        return self.inspection

    def cancel(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionStatusResult | None:
        self.calls.append(("cancel", job, execution_id))
        self._raise()
        return self.cancellation

    def reconcile_cancel(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionStatusResult | None:
        self.calls.append(("reconcile_cancel", job, execution_id))
        self._raise()
        return self.reconciled_cancellation

    def collect(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionEvidenceResult | None:
        self.calls.append(("collect", job, execution_id))
        self._raise()
        return self.evidence


class ExecutionContractTests(unittest.TestCase):
    socket_path = Path("/private/tmp/mgb-execution-tests/runner.sock")

    def test_status_lifecycle_is_strict_and_canonical(self) -> None:
        for state in ("queued", "running", "succeeded", "failed", "cancelled"):
            with self.subTest(state=state):
                result = status_result(state=state)
                self.assertEqual(
                    ExecutionStatusResult.from_mapping(result.to_dict()), result
                )

        invalid = status_result(state="succeeded").to_dict()
        invalid["exit_code"] = 1
        with self.assertRaisesRegex(ValidationError, "exit code"):
            ExecutionStatusResult.from_mapping(invalid)

        invalid = status_result(state="running").to_dict()
        invalid["finished_at"] = "2026-10-28T12:00:05.000000+00:00"
        with self.assertRaisesRegex(ValidationError, "lifecycle"):
            ExecutionStatusResult.from_mapping(invalid)

        invalid = status_result().to_dict()
        invalid["candidate_revision"] = "b" * 64
        with self.assertRaisesRegex(ValidationError, "candidate revision"):
            ExecutionStatusResult.from_mapping(invalid)

    def test_evidence_is_terminal_bounded_and_digest_bound(self) -> None:
        evidence = evidence_result()
        self.assertEqual(
            ExecutionEvidenceResult.from_mapping(evidence.to_dict()), evidence
        )
        self.assertEqual(evidence.artifact_bundle, b"remote evidence\n")

        for field, value, message in (
            ("artifact_sha256", "0" * 64, "digest"),
            ("artifact_bytes", 1, "size"),
            ("artifact_count", 100_001, "count"),
        ):
            invalid = evidence.to_dict()
            invalid[field] = value
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(ValidationError, message),
            ):
                ExecutionEvidenceResult.from_mapping(invalid)

        invalid = evidence.to_dict()
        invalid["execution"] = status_result(state="running").to_dict()
        with self.assertRaisesRegex(ValidationError, "terminal"):
            ExecutionEvidenceResult.from_mapping(invalid)

    def test_status_rejects_malformed_identity_authority_and_time(self) -> None:
        valid = status_result().to_dict()
        cases = (
            ("extra", {**valid, "extra": True}, "fields"),
            ("version", {**valid, "schema_version": 2}, "schema_version"),
            ("identity", {**valid, "execution_id": "rex_bad"}, "identity"),
            ("state", {**valid, "state": "unknown"}, "state"),
            ("format", {**valid, "object_format": "md5"}, "format"),
            ("recipe", {**valid, "recipe_id": "Bad Recipe"}, "recipe"),
            ("digest", {**valid, "recipe_sha256": "A" * 64}, "digest"),
            ("image", {**valid, "image_id": "latest"}, "image"),
            ("source-size", {**valid, "source_bytes": True}, "source size"),
            ("source-digest", {**valid, "source_sha256": "0"}, "digest"),
            ("exit", {**valid, "exit_code": 256}, "exit code"),
            (
                "time-order",
                {
                    **valid,
                    "submitted_at": "2026-10-28T12:00:02.000000+00:00",
                },
                "timestamps",
            ),
        )
        for name, document, message in cases:
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(ValidationError, message),
            ):
                ExecutionStatusResult.from_mapping(document)

    def test_evidence_rejects_malformed_encoding_duration_and_output(self) -> None:
        valid = evidence_result().to_dict()
        cases = (
            ({**valid, "schema_version": False}, "schema_version"),
            ({**valid, "duration_ms": 3}, "duration"),
            ({**valid, "artifact_encoding": "zip"}, "encoding"),
            ({**valid, "artifact_bundle": 1}, "base64 text"),
            ({**valid, "artifact_bundle": "!"}, "base64"),
            ({**valid, "output_bytes": -1}, "output size"),
            ({**valid, "output_sha256": "BAD"}, "output digest"),
        )
        for document, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                ExecutionEvidenceResult.from_mapping(document)

        cancelled = evidence_result(state="cancelled").to_dict()
        cancelled["execution"]["started_at"] = None
        cancelled["duration_ms"] = 0
        parsed = ExecutionEvidenceResult.from_mapping(cancelled)
        self.assertEqual(parsed.duration_ms, 0)
        self.assertEqual(
            parsed.digest, hashlib.sha256(parsed.canonical.encode()).hexdigest()
        )

    def test_job_spec_rejects_every_untrusted_authority_field(self) -> None:
        valid = FakeExecutionBackend().submission
        values = {
            "operation_id": "sop_" + "7" * 32,
            "mapping_name": "azure-quality",
            "repository": "acme/widgets",
            "candidate_id": CANDIDATE_ID,
            "object_format": "sha1",
            "base_revision": BASE,
            "candidate_revision": HEAD,
            "recipe_id": valid.recipe_id,
            "recipe_sha256": valid.recipe_sha256,
            "image_id": valid.image_id,
        }
        changes = (
            ("operation_id", "bad"),
            ("mapping_name", "Bad Name"),
            ("repository", ""),
            ("candidate_id", "bad"),
            ("base_revision", "a" * 64),
            ("candidate_revision", "b" * 64),
            ("recipe_id", "Bad"),
            ("recipe_sha256", "bad"),
            ("image_id", "latest"),
        )
        for field, value in changes:
            with self.subTest(field=field), self.assertRaises(ValidationError):
                ExecutionJobSpec(**{**values, field: value})
        parsed = ExecutionJobSpec(**values)
        self.assertEqual(
            parsed.digest, hashlib.sha256(parsed.canonical.encode()).hexdigest()
        )

    def test_core_capabilities_have_fixed_modes(self) -> None:
        spec = sidecar_spec(self.socket_path)
        self.assertEqual(
            [(item.name, item.mode) for item in spec.capabilities],
            [
                ("execution.submit", "effect"),
                ("execution.inspect", "read"),
                ("execution.cancel", "effect"),
                ("execution.collect", "read"),
            ],
        )
        changed = spec.to_dict()
        changed["capabilities"][0]["mode"] = "read"
        with self.assertRaisesRegex(ValidationError, "core-defined mode"):
            ServiceSidecarSpec.from_mapping(changed)


class ControlledExecutionProviderTests(unittest.TestCase):
    socket_path = ExecutionContractTests.socket_path

    def provider(self) -> tuple[ControlledExecutionProvider, FakeExecutionBackend]:
        backend = FakeExecutionBackend()
        return ControlledExecutionProvider(
            sidecar_spec(self.socket_path), backend
        ), backend

    def test_submit_pins_candidate_recipe_and_image(self) -> None:
        provider, backend = self.provider()

        response = provider.invoke(request(self.socket_path, "execution.submit"))

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.provider_status, 202)
        self.assertEqual(response.receipt.result, status_result().to_dict())
        self.assertEqual([call[0] for call in backend.calls], ["submit"])
        job = backend.calls[0][1]
        self.assertEqual(job.repository, "acme/widgets")
        self.assertEqual(job.candidate_id, CANDIDATE_ID)
        self.assertEqual(job.base_revision, BASE)
        self.assertEqual(job.candidate_revision, HEAD)
        self.assertEqual(job.recipe_id, "quality")
        self.assertEqual(job.recipe_sha256, RECIPE_SHA256)
        self.assertEqual(job.image_id, IMAGE_ID)
        self.assertNotIn("runner.example.test", canonical_json(job.to_dict()))

    def test_current_resource_authority_v2_is_supported_end_to_end(self) -> None:
        backend = FakeExecutionBackend()
        provider = ControlledExecutionProvider(
            sidecar_spec(self.socket_path, schema_version=2), backend
        )

        response = provider.invoke(
            request(
                self.socket_path,
                "execution.submit",
                schema_version=2,
            )
        )

        self.assertEqual(response.schema_version, 2)
        self.assertEqual(response.connector_protocol_version, 2)
        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(backend.calls[0][1].repository, "acme/widgets")

    def test_submit_reconcile_observes_and_never_resubmits(self) -> None:
        provider, backend = self.provider()

        response = provider.invoke(
            request(self.socket_path, "execution.submit", phase="reconcile")
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual([call[0] for call in backend.calls], ["reconcile_submit"])

        backend.calls.clear()
        backend.reconciled = None
        absent = provider.invoke(
            request(self.socket_path, "execution.submit", phase="reconcile")
        )
        self.assertEqual(absent.outcome, "not_found")
        self.assertEqual([call[0] for call in backend.calls], ["reconcile_submit"])

    def test_inspect_cancel_and_collect_are_separate_surfaces(self) -> None:
        provider, backend = self.provider()

        inspected = provider.invoke(request(self.socket_path, "execution.inspect"))
        cancelled = provider.invoke(request(self.socket_path, "execution.cancel"))
        collected = provider.invoke(request(self.socket_path, "execution.collect"))

        self.assertEqual(inspected.receipt.result, status_result().to_dict())
        self.assertEqual(
            cancelled.receipt.result, status_result(state="cancelled").to_dict()
        )
        self.assertEqual(collected.receipt.result, evidence_result().to_dict())
        self.assertEqual(
            [call[0] for call in backend.calls], ["inspect", "cancel", "collect"]
        )
        self.assertTrue(all(call[2] == EXECUTION_ID for call in backend.calls))

    def test_cancel_reconciliation_resumes_only_observed_cancel_intent(self) -> None:
        provider, backend = self.provider()

        reconciled = provider.invoke(
            request(self.socket_path, "execution.cancel", phase="reconcile")
        )

        self.assertEqual(reconciled.outcome, "succeeded")
        self.assertEqual([call[0] for call in backend.calls], ["reconcile_cancel"])

        backend.calls.clear()
        backend.reconciled_cancellation = None
        absent = provider.invoke(
            request(self.socket_path, "execution.cancel", phase="reconcile")
        )
        self.assertEqual(absent.outcome, "not_found")
        self.assertEqual([call[0] for call in backend.calls], ["reconcile_cancel"])

    def test_followup_uses_submission_identity_for_the_remote_job(self) -> None:
        provider, backend = self.provider()

        provider.invoke(
            request(
                self.socket_path,
                "execution.inspect",
                operation_id="sop_" + "8" * 32,
            )
        )

        self.assertEqual(backend.calls[0][1].operation_id, "sop_" + "7" * 32)

    def test_capabilities_reject_extra_parameters_and_mismatched_results(self) -> None:
        provider, backend = self.provider()
        extra = provider.invoke(
            request(
                self.socket_path,
                "execution.submit",
                parameters={"command": "make test"},
            )
        )
        self.assertEqual(extra.outcome, "denied")
        self.assertEqual(backend.calls, [])

        backend.submission = status_result(candidate_revision="c" * 40)
        mismatch = provider.invoke(request(self.socket_path, "execution.submit"))
        self.assertEqual(mismatch.outcome, "uncertain")
        self.assertEqual(mismatch.error_code, "execution-result-ambiguous")

    def test_lost_effect_response_is_uncertain_but_read_failure_is_retryable(
        self,
    ) -> None:
        provider, backend = self.provider()
        backend.error = ExecutionBackendAmbiguous("lost response")

        submit = provider.invoke(request(self.socket_path, "execution.submit"))
        inspect = provider.invoke(request(self.socket_path, "execution.inspect"))

        self.assertEqual(submit.outcome, "uncertain")
        self.assertEqual(inspect.outcome, "retryable")
        self.assertEqual(inspect.retry_after_seconds, 15)

    def test_known_pre_dispatch_failure_and_missing_remote_results_are_typed(
        self,
    ) -> None:
        provider, backend = self.provider()
        backend.error = ExecutionBackendUnavailable("not dispatched")
        retry = provider.invoke(
            request(self.socket_path, "execution.submit", phase="reconcile")
        )
        self.assertEqual(retry.outcome, "retryable")
        self.assertEqual(retry.error_code, "execution-transport-unavailable")

        invoke_retry = provider.invoke(
            request(self.socket_path, "execution.submit", phase="invoke")
        )
        self.assertEqual(invoke_retry.outcome, "retryable")
        self.assertEqual(invoke_retry.error_code, "execution-transport-unavailable")

        backend.error = None
        backend.inspection = None
        backend.cancellation = None
        backend.evidence = None
        self.assertEqual(
            provider.invoke(request(self.socket_path, "execution.inspect")).outcome,
            "conflict",
        )
        self.assertEqual(
            provider.invoke(request(self.socket_path, "execution.cancel")).outcome,
            "conflict",
        )
        self.assertEqual(
            provider.invoke(request(self.socket_path, "execution.collect")).outcome,
            "conflict",
        )

    def test_authority_and_result_budgets_fail_closed(self) -> None:
        provider, backend = self.provider()
        original = request(self.socket_path, "execution.inspect")
        mismatches = (
            replace(original, schema_version=2),
            replace(original, connector_protocol_version=2),
            replace(original, deployment_sha256="0" * 64),
            replace(original, request_limit_bytes=provider.spec.request_bytes + 1),
            replace(original, response_limit_bytes=provider.spec.response_bytes + 1),
        )
        for changed in mismatches:
            with (
                self.subTest(changed=changed),
                self.assertRaises(ExecutionProviderError),
            ):
                provider._validate_authority(changed)

        tiny_response = replace(
            request(self.socket_path, "execution.submit"), response_limit_bytes=1
        )
        response = provider.invoke(tiny_response)
        self.assertEqual(response.outcome, "uncertain")
        self.assertEqual(response.error_code, "execution-result-ambiguous")

    def test_provider_constructor_and_request_type_are_strict(self) -> None:
        with self.assertRaisesRegex(ExecutionProviderError, "supported"):
            ControlledExecutionProvider(None, FakeExecutionBackend())  # type: ignore[arg-type]
        with self.assertRaisesRegex(ExecutionProviderError, "backend"):
            ControlledExecutionProvider(sidecar_spec(self.socket_path), object())  # type: ignore[arg-type]
        provider, _ = self.provider()
        with self.assertRaisesRegex(ExecutionProviderError, "request"):
            provider.invoke({})  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
