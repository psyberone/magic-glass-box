from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from magic_glass_box.db import Database
from magic_glass_box.service import ControlPlane
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceSidecarAmbiguousError,
    ServiceSidecarError,
    ServiceSidecarUnavailableError,
)
from tests.test_service_sidecars import operation_spec, sidecar_spec


class DeterministicLostReplySidecar:
    def __init__(self, remote: dict[str, dict[str, Any]]):
        self.remote = remote
        self.calls: list[ServiceOperationRequest] = []
        self.create_count = 0

    def invoke(self, request: ServiceOperationRequest) -> ServiceOperationResponse:
        self.calls.append(request)
        if request.phase == "invoke":
            if request.input_sha256 not in self.remote:
                self.create_count += 1
                self.remote[request.input_sha256] = {
                    "remote_identity": "change-42",
                    "observed_revision": "d" * 40,
                    "evidence_sha256": "e" * 64,
                    "closed_references": [],
                }
            raise ServiceSidecarAmbiguousError("simulated lost successful reply")
        response = request.correlation_dict()
        response.update(
            {
                "outcome": "succeeded",
                "provider_status": 200,
                "request_bytes": 512,
                "response_bytes": 128,
                "error_code": None,
                "retry_after_seconds": None,
                "receipt": self.remote[request.input_sha256],
            }
        )
        return ServiceOperationResponse.from_mapping(response)


class UnavailableSidecar:
    def invoke(self, _request: ServiceOperationRequest) -> ServiceOperationResponse:
        raise ServiceSidecarUnavailableError("offline")


class InvalidResponseSidecar:
    def invoke(self, _request: ServiceOperationRequest) -> object:
        return {"outcome": "succeeded", "raw_provider_body": "forbidden"}


def successful_response(
    request: ServiceOperationRequest,
) -> ServiceOperationResponse:
    response = request.correlation_dict()
    response.update(
        {
            "outcome": "succeeded",
            "provider_status": 201,
            "request_bytes": 512,
            "response_bytes": 128,
            "error_code": None,
            "retry_after_seconds": None,
            "receipt": {
                "remote_identity": "change-42",
                "observed_revision": "d" * 40,
                "evidence_sha256": "e" * 64,
                "closed_references": [],
            },
        }
    )
    return ServiceOperationResponse.from_mapping(response)


class SuccessfulSidecar:
    def invoke(self, request: ServiceOperationRequest) -> ServiceOperationResponse:
        return successful_response(request)


class MismatchedResponseSidecar:
    def invoke(self, request: ServiceOperationRequest) -> ServiceOperationResponse:
        response = successful_response(request).to_dict()
        response["operation_id"] = "sop_" + "f" * 32
        return ServiceOperationResponse.from_mapping(response)


class CrashingSidecar:
    def invoke(self, _request: ServiceOperationRequest) -> ServiceOperationResponse:
        raise RuntimeError("unexpected adapter failure")


class FailedSidecar:
    def invoke(self, _request: ServiceOperationRequest) -> ServiceOperationResponse:
        raise ServiceSidecarError("adapter rejected the operation")


class ServiceSidecarServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        self.database.register_service_sidecar(
            name="example-sidecar",
            spec=sidecar_spec(self.root / "service.sock"),
            created_by=self.user["id"],
        )
        self.operation = self.database.create_service_operation(
            spec=operation_spec(),
            created_by=self.user["id"],
        )

    def test_replacement_supervisor_reconciles_lost_success_without_duplicate(
        self,
    ) -> None:
        remote: dict[str, dict[str, Any]] = {}
        first_client = DeterministicLostReplySidecar(remote)
        first_paths: list[Path] = []

        def first_factory(path: Path) -> DeterministicLostReplySidecar:
            first_paths.append(path)
            return first_client

        first_supervisor = ControlPlane(
            self.database, service_client_factory=first_factory
        )
        with self.assertRaises(ServiceSidecarAmbiguousError):
            first_supervisor.execute_service_operation(
                self.operation["id"],
                owner_id="supervisor-one",
                expected_version=1,
            )
        uncertain = self.database.get_service_operation(self.operation["id"])
        self.assertEqual(uncertain["state"], "uncertain")
        self.assertEqual(uncertain["version"], 3)
        self.assertEqual(first_client.create_count, 1)
        self.assertEqual(first_paths, [self.root / "service.sock"])

        replacement_client = DeterministicLostReplySidecar(remote)
        replacement_supervisor = ControlPlane(
            self.database,
            service_client_factory=lambda _path: replacement_client,
        )
        recovered = replacement_supervisor.reconcile_service_operation(
            self.operation["id"],
            owner_id="supervisor-two",
            expected_version=3,
        )

        self.assertEqual(recovered["state"], "succeeded")
        self.assertEqual(first_client.create_count, 1)
        self.assertEqual(replacement_client.create_count, 0)
        self.assertEqual([call.phase for call in first_client.calls], ["invoke"])
        self.assertEqual(
            [call.phase for call in replacement_client.calls], ["reconcile"]
        )
        self.assertEqual(len(remote), 1)
        self.assertEqual(
            [attempt["outcome"] for attempt in recovered["attempts"]],
            ["ambiguous", "succeeded"],
        )

    def test_pre_dispatch_unavailability_is_retryable_not_uncertain(self) -> None:
        control = ControlPlane(
            self.database,
            service_client_factory=lambda _path: UnavailableSidecar(),
        )
        with self.assertRaises(ServiceSidecarUnavailableError):
            control.execute_service_operation(
                self.operation["id"],
                owner_id="supervisor-one",
                expected_version=1,
            )
        operation = self.database.get_service_operation(self.operation["id"])
        self.assertEqual(operation["state"], "retryable")
        self.assertIsNotNone(operation["eligible_at"])
        self.assertEqual(operation["attempts"][0]["outcome"], "unavailable")

    def test_client_construction_failure_is_durably_retryable(self) -> None:
        def unavailable_factory(_path: Path) -> UnavailableSidecar:
            raise OSError("socket setup failed")

        control = ControlPlane(
            self.database,
            service_client_factory=unavailable_factory,
        )
        with self.assertRaises(ServiceSidecarUnavailableError):
            control.execute_service_operation(
                self.operation["id"],
                owner_id="supervisor-one",
                expected_version=1,
            )

        operation = self.database.get_service_operation(self.operation["id"])
        self.assertEqual(operation["state"], "retryable")
        self.assertEqual(operation["attempts"][0]["outcome"], "unavailable")
        self.assertEqual(operation["attempts"][0]["error_code"], "sidecar_unavailable")

    def test_invalid_post_dispatch_response_is_durably_uncertain(self) -> None:
        control = ControlPlane(
            self.database,
            service_client_factory=lambda _path: InvalidResponseSidecar(),
        )
        with self.assertRaises(ServiceSidecarAmbiguousError):
            control.execute_service_operation(
                self.operation["id"],
                owner_id="supervisor-one",
                expected_version=1,
            )

        operation = self.database.get_service_operation(self.operation["id"])
        self.assertEqual(operation["state"], "uncertain")
        self.assertEqual(operation["attempts"][0]["outcome"], "ambiguous")
        self.assertEqual(operation["attempts"][0]["error_code"], "sidecar_ambiguous")

    def test_typed_response_with_wrong_correlation_is_durably_uncertain(self) -> None:
        control = ControlPlane(
            self.database,
            service_client_factory=lambda _path: MismatchedResponseSidecar(),
        )

        with self.assertRaises(ServiceSidecarAmbiguousError) as raised:
            control.execute_service_operation(
                self.operation["id"],
                owner_id="supervisor-one",
                expected_version=1,
            )

        self.assertEqual(
            str(raised.exception), "service sidecar returned an invalid response"
        )
        operation = self.database.get_service_operation(self.operation["id"])
        self.assertEqual(operation["state"], "uncertain")
        self.assertEqual(operation["attempts"][0]["outcome"], "ambiguous")
        self.assertEqual(operation["attempts"][0]["error_code"], "sidecar_ambiguous")

    def test_unexpected_post_dispatch_failure_is_durably_uncertain(self) -> None:
        control = ControlPlane(
            self.database,
            service_client_factory=lambda _path: CrashingSidecar(),
        )

        with self.assertRaises(ServiceSidecarAmbiguousError) as raised:
            control.execute_service_operation(
                self.operation["id"],
                owner_id="supervisor-one",
                expected_version=1,
            )

        self.assertEqual(str(raised.exception), "service sidecar outcome is ambiguous")
        operation = self.database.get_service_operation(self.operation["id"])
        self.assertEqual(operation["state"], "uncertain")
        self.assertEqual(operation["attempts"][0]["outcome"], "ambiguous")
        self.assertEqual(operation["attempts"][0]["error_code"], "sidecar_failure")

    def test_post_dispatch_sidecar_error_is_durably_uncertain(self) -> None:
        control = ControlPlane(
            self.database,
            service_client_factory=lambda _path: FailedSidecar(),
        )

        with self.assertRaises(ServiceSidecarAmbiguousError) as raised:
            control.execute_service_operation(
                self.operation["id"],
                owner_id="supervisor-one",
                expected_version=1,
            )

        self.assertEqual(str(raised.exception), "service sidecar outcome is ambiguous")
        operation = self.database.get_service_operation(self.operation["id"])
        self.assertEqual(operation["state"], "uncertain")
        self.assertEqual(operation["attempts"][0]["outcome"], "ambiguous")
        self.assertEqual(operation["attempts"][0]["error_code"], "sidecar_error")

    def test_finalization_failure_is_durably_uncertain(self) -> None:
        control = ControlPlane(
            self.database,
            service_client_factory=lambda _path: SuccessfulSidecar(),
        )

        with (
            mock.patch.object(
                self.database,
                "complete_service_operation_attempt",
                side_effect=RuntimeError("database write failed"),
            ),
            self.assertRaises(ServiceSidecarAmbiguousError) as raised,
        ):
            control.execute_service_operation(
                self.operation["id"],
                owner_id="supervisor-one",
                expected_version=1,
            )

        self.assertEqual(
            str(raised.exception), "service sidecar outcome could not be finalized"
        )
        operation = self.database.get_service_operation(self.operation["id"])
        self.assertEqual(operation["state"], "uncertain")
        self.assertEqual(operation["attempts"][0]["outcome"], "ambiguous")
        self.assertEqual(operation["attempts"][0]["error_code"], "finalization_failed")

    def test_attempt_lease_preserves_timeout_buffer_exactly(self) -> None:
        control = ControlPlane(
            self.database,
            service_client_factory=lambda _path: UnavailableSidecar(),
        )

        for timeout_ms, expected_lease_seconds in (
            (10_000, 30),
            (25_000, 30),
            (25_002, 31),
        ):
            with self.subTest(timeout_ms=timeout_ms):
                operation = self.database.create_service_operation(
                    spec=operation_spec(
                        idempotency_key=f"lease-{timeout_ms}",
                        timeout_ms=timeout_ms,
                    ),
                    created_by=self.user["id"],
                )
                with (
                    mock.patch.object(
                        self.database,
                        "begin_service_operation_attempt",
                        wraps=self.database.begin_service_operation_attempt,
                    ) as begin_attempt,
                    self.assertRaises(ServiceSidecarUnavailableError),
                ):
                    control.execute_service_operation(
                        operation["id"],
                        owner_id="supervisor-one",
                        expected_version=1,
                    )
                self.assertEqual(
                    begin_attempt.call_args.kwargs["lease_seconds"],
                    expected_lease_seconds,
                )


if __name__ == "__main__":
    unittest.main()
