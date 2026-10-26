from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from magic_glass_box.advisory import (
    AdvisoryBlindReview,
    AdvisoryChallengeReview,
    AdvisoryReport,
)
from magic_glass_box.db import ConflictError, NotFoundError, StorageError
from magic_glass_box.discovery import DiscoveryRuling, DiscoveryTrace
from magic_glass_box.domain import (
    GatewayCapability,
    HandoffEnvelope,
    ValidationError,
    WorkAssignmentSpec,
    WorkLogEntry,
    WorkMemoryCheckpoint,
    canonical_json,
)
from magic_glass_box.gateway import (
    GATEWAY_REQUEST_NAME,
    GATEWAY_REQUEST_READY_NAME,
    GATEWAY_RESPONSE_NAME,
    GATEWAY_RESPONSE_READY_NAME,
    MAX_GATEWAY_REQUEST_BYTES,
    FileGatewayManager,
    GatewayError,
    RunGateway,
    default_gateway_root,
)
from magic_glass_box.krab import KrabError
from magic_glass_box.provider import (
    ProviderAmbiguousError,
    ProviderBudgetError,
    ProviderBusyError,
    ProviderError,
    ProviderQuotaError,
    ProviderRateLimitError,
    ProviderUpstreamError,
)
from magic_glass_box.service import (
    AcceptedInteractionResponse,
    AcceptedProviderResponse,
)
from magic_glass_box.workflow_scope import WorkflowScopeReview

RUN_ID = "run_" + "1" * 32
OTHER_RUN_ID = "run_" + "2" * 32
INTERACTION_ID = "int_" + "3" * 32
ATTEMPT_ID = "att_" + "4" * 32
HANDOFF_ID = "hof_" + "5" * 32
ASSIGNMENT_ID = "was_" + "6" * 32
WORK_ENTRY_ID = "wle_" + "7" * 32


class FakeGatewayDatabase:
    def __init__(self) -> None:
        self.recovery_checkpoint: dict[str, Any] | None = None
        self.interactions = [
            {
                "id": INTERACTION_ID,
                "kind": "question",
                "state": "answered",
                "version": 2,
                "request_item_id": "itm_" + "6" * 32,
                "response_item_id": "itm_" + "7" * 32,
                "recipient": {"krab_handle": "reviewer"},
            }
        ]

    def get_run_execution(self, run_id: str) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped its bound run")
        return {
            "id": RUN_ID,
            "state": "running",
            "version": 3,
            "source_revision": "a" * 40,
            "project_key": "box-project",
            "context": {
                "purpose": "Build the box",
                "instructions": ["Keep changes focused"],
                "repository_path": "/private/host/repository",
                "default_branch": "main",
                "build_commands": [],
                "test_commands": [["make", "quality"]],
                "allowed_tools": ["git"],
            },
            "profile": {
                "image": "agent@sha256:secret-host-policy",
                "command": ["agent", "run"],
                "workspace_mode": "copy_on_write",
                "network_mode": "gateway_only",
                "credential_names": ["krab"],
                "gateway_capabilities": [
                    "run.describe",
                    "interaction.open",
                    "interaction.read",
                ],
            },
            "work_item_id": "wki_" + "4" * 32,
            "work_item_version_id": "wiv_" + "5" * 32,
            "work_item_version": 2,
            "work_item": {
                "id": "wki_" + "4" * 32,
                "version": 2,
                "source": "manual",
                "external_id": "local-7",
                "source_updated_at": "2026-10-09T12:00:00.000000Z",
                "schema_version": 1,
                "title": "Build the gateway",
                "brief": "Use one capability channel.",
                "acceptance_criteria": [],
                "constraints": [],
                "context": [],
                "labels": [],
                "priority": "normal",
            },
            "credential_references": {"krab": "file:/private/agent.token"},
            "handoff": {
                "id": HANDOFF_ID,
                "attempt_id": ATTEMPT_ID,
                "schema_version": 1,
                "work_state": "waiting_for_input",
                "summary": "Continue after the accepted response.",
                "decisions": [],
                "artifacts": [],
                "next_steps": ["Read the response."],
                "pending_interaction_ids": [INTERACTION_ID],
            },
            "recovery_checkpoint": self.recovery_checkpoint,
            "review_assignment": None,
            "changes_requested": None,
            "workflow_assignment": None,
            "advisory_assignment": None,
            "work_memory": {
                "thread_id": "wth_" + "8" * 32,
                "work_item_id": "wki_" + "4" * 32,
                "generation": 1,
                "state": "active",
                "version": 3,
                "log_sequence": 1,
                "checkpoint": None,
                "entries": [],
                "entries_truncated": False,
                "next_after_sequence": None,
                "assignments": [],
            },
        }

    def get_work_memory_for_run(
        self, run_id: str, *, after_sequence: int | None = None, limit: int = 32
    ) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped its bound run")
        return {
            "thread_id": "wth_" + "8" * 32,
            "version": 3,
            "log_sequence": 1,
            "requested_after_sequence": after_sequence,
            "requested_limit": limit,
            "entries": [],
            "entries_truncated": False,
            "next_after_sequence": None,
            "assignments": [],
            "checkpoint": None,
        }

    def get_run(self, run_id: str) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped its bound run")
        return {
            "id": RUN_ID,
            "state": "waiting_for_input",
            "version": 4,
            "interactions": self.interactions,
        }

    def get_run_transport(self, run_id: str) -> dict[str, Any]:
        if run_id != RUN_ID:
            raise AssertionError("gateway escaped its bound run")
        return {"version": 3}


class FakeGatewayControl:
    def __init__(self) -> None:
        self.database = FakeGatewayDatabase()
        self.open_calls: list[tuple[str, dict[str, Any]]] = []
        self.read_calls: list[tuple[str, str]] = []
        self.provider_calls: list[tuple[str, str, dict[str, Any]]] = []
        self.handoff_calls: list[tuple[str, HandoffEnvelope]] = []
        self.work_calls: list[tuple[str, str, dict[str, Any]]] = []
        self.discovery_calls: list[tuple[str, str, object, str]] = []
        self.advisory_calls: list[tuple[str, str, object, str]] = []
        self.workflow_scope_calls: list[tuple[str, WorkflowScopeReview, str]] = []
        self.provider_failure: Exception | None = None

    def open_interaction(self, run_id: str, **kwargs: Any) -> dict[str, Any]:
        self.open_calls.append((run_id, kwargs))
        return {
            "id": INTERACTION_ID,
            "state": "open",
            "version": 1,
            "run_state": "waiting_for_input",
            "run_version": 4,
            "request_item_id": "itm_" + "6" * 32,
        }

    def read_interaction_response(
        self, run_id: str, interaction_id: str
    ) -> AcceptedInteractionResponse:
        self.read_calls.append((run_id, interaction_id))
        return AcceptedInteractionResponse(
            interaction_id=interaction_id,
            run_id=run_id,
            response_item_id="itm_" + "7" * 32,
            interaction_state="answered",
            interaction_version=2,
            run_state="running",
            run_version=6,
            untrusted_title="Human response",
            untrusted_body="Proceed with option B.",
            content_risk_flags=("untrusted_text",),
        )

    def invoke_provider(
        self, run_id: str, *, grant_name: str, request: dict[str, Any]
    ) -> AcceptedProviderResponse:
        self.provider_calls.append((run_id, grant_name, request))
        if self.provider_failure is not None:
            raise self.provider_failure
        return AcceptedProviderResponse(
            invocation_id="pinv_" + "8" * 32,
            grant_name=grant_name,
            provider="openai",
            model="gpt-test",
            status_code=200,
            request_bytes=41,
            response_bytes=36,
            untrusted_response={"output_text": "done"},
        )

    def submit_handoff(self, run_id: str, handoff: HandoffEnvelope) -> dict[str, Any]:
        self.handoff_calls.append((run_id, handoff))
        return {
            "id": HANDOFF_ID,
            "attempt_id": ATTEMPT_ID,
            "content_sha256": handoff.digest,
            **handoff.to_dict(),
        }

    def append_work_log(
        self,
        run_id: str,
        entry: WorkLogEntry,
        *,
        idempotency_key: str,
        assignment_id: str | None = None,
    ) -> dict[str, Any]:
        self.work_calls.append(
            (
                "append",
                run_id,
                {
                    "entry": entry,
                    "idempotency_key": idempotency_key,
                    "assignment_id": assignment_id,
                },
            )
        )
        return {"id": WORK_ENTRY_ID, "sequence": 2, **entry.to_dict()}

    def checkpoint_work_memory(
        self,
        run_id: str,
        checkpoint: WorkMemoryCheckpoint,
        *,
        through_sequence: int,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.work_calls.append(
            (
                "checkpoint",
                run_id,
                {
                    "checkpoint": checkpoint,
                    "through_sequence": through_sequence,
                    "expected_thread_version": expected_thread_version,
                    "idempotency_key": idempotency_key,
                },
            )
        )
        return {"id": "wmc_" + "9" * 32, "version": 1, "through_sequence": 2}

    def create_work_assignment(
        self,
        run_id: str,
        *,
        target_profile_name: str,
        spec: WorkAssignmentSpec,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.work_calls.append(
            (
                "assign",
                run_id,
                {
                    "target_profile_name": target_profile_name,
                    "spec": spec,
                    "expected_thread_version": expected_thread_version,
                    "idempotency_key": idempotency_key,
                },
            )
        )
        return {"id": ASSIGNMENT_ID, "state": "open", "version": 1}

    def claim_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        lease_seconds: int,
    ) -> dict[str, Any]:
        self.work_calls.append(
            (
                "claim",
                run_id,
                {
                    "assignment_id": assignment_id,
                    "expected_assignment_version": expected_assignment_version,
                    "lease_seconds": lease_seconds,
                },
            )
        )
        return {
            "id": assignment_id,
            "state": "claimed",
            "version": 2,
            "lease_expires_at": "2026-10-10T12:10:00.000000+00:00",
        }

    def complete_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        result: WorkLogEntry,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.work_calls.append(
            (
                "complete",
                run_id,
                {
                    "assignment_id": assignment_id,
                    "expected_assignment_version": expected_assignment_version,
                    "result": result,
                    "idempotency_key": idempotency_key,
                },
            )
        )
        return {
            "id": assignment_id,
            "state": "completed",
            "version": 3,
            "result_entry_id": WORK_ENTRY_ID,
        }

    def submit_discovery_trace(
        self,
        run_id: str,
        *,
        trace: DiscoveryTrace,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.discovery_calls.append(("trace", run_id, trace, idempotency_key))
        return {
            "id": "dtr_" + "8" * 32,
            "run_id": run_id,
            "trace": trace.to_dict(),
            "content_sha256": trace.digest,
        }

    def submit_discovery_ruling(
        self,
        run_id: str,
        *,
        ruling: DiscoveryRuling,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.discovery_calls.append(("ruling", run_id, ruling, idempotency_key))
        return {
            "id": "drl_" + "9" * 32,
            "run_id": run_id,
            "ruling": ruling.to_dict(),
            "content_sha256": ruling.digest,
        }

    def submit_advisory_blind_review(
        self,
        run_id: str,
        *,
        review: AdvisoryBlindReview,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.advisory_calls.append(("blind", run_id, review, idempotency_key))
        return {
            "id": "abr_" + "a" * 32,
            "run_id": run_id,
            "review": review.to_dict(),
            "content_sha256": review.digest,
        }

    def submit_advisory_challenge_review(
        self,
        run_id: str,
        *,
        review: AdvisoryChallengeReview,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.advisory_calls.append(("challenge", run_id, review, idempotency_key))
        return {
            "id": "acr_" + "b" * 32,
            "run_id": run_id,
            "review": review.to_dict(),
            "content_sha256": review.digest,
        }

    def submit_advisory_report(
        self,
        run_id: str,
        *,
        report: AdvisoryReport,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.advisory_calls.append(("report", run_id, report, idempotency_key))
        return {
            "id": "arp_" + "c" * 32,
            "run_id": run_id,
            "report": report.to_dict(),
            "content_sha256": report.digest,
        }

    def submit_workflow_scope_review(
        self,
        run_id: str,
        *,
        review: WorkflowScopeReview,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.workflow_scope_calls.append((run_id, review, idempotency_key))
        return {
            "id": "wsr_" + "d" * 32,
            "run_id": run_id,
            "review": review.to_dict(),
            "content_sha256": review.digest,
            "expected_review_outcome": review.expected_review_outcome.value,
            "failed_must_keys": list(review.failed_must_keys),
            "regression_probe_keys": list(review.regression_probe_keys),
            "follow_up_probe_keys": list(review.follow_up_probe_keys),
        }


def request(operation: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "version": 1,
        "request_id": "req-12345678",
        "operation": operation,
        "payload": payload or {},
    }


class RunGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.control = FakeGatewayControl()
        self.gateway = RunGateway(
            self.control,
            RUN_ID,
            capabilities=tuple(GatewayCapability),
            credential_names=("krab", "openai-main"),
        )

    def test_describe_exposes_pinned_agent_inputs_without_host_authority(self) -> None:
        response = self.gateway.handle(request("run.describe"))

        self.assertTrue(response["ok"])
        result = response["result"]
        self.assertEqual(result["run_id"], RUN_ID)
        self.assertEqual(result["state"], "waiting_for_input")
        self.assertEqual(result["version"], 4)
        self.assertEqual(result["work_item"]["title"], "Build the gateway")
        self.assertEqual(result["context"]["purpose"], "Build the box")
        self.assertEqual(result["handoff"]["id"], HANDOFF_ID)
        self.assertIsNone(result["recovery_checkpoint"])
        self.assertIsNone(result["review_assignment"])
        self.assertIsNone(result["changes_requested"])
        self.assertIsNone(result["workflow_assignment"])
        self.assertIsNone(result["discovery_assignment"])
        self.assertIsNone(result["advisory_assignment"])
        self.assertEqual(result["work_memory"]["version"], 3)
        self.assertEqual(
            result["authority"],
            {
                "workspace_mode": "copy_on_write",
                "network_mode": "gateway_only",
                "credential_names": ["krab", "openai-main"],
                "gateway_capabilities": sorted(
                    capability.value for capability in GatewayCapability
                ),
            },
        )
        self.assertEqual(
            result["interactions"],
            [
                {
                    "id": INTERACTION_ID,
                    "kind": "question",
                    "recipient_handle": "reviewer",
                    "request_item_id": "itm_" + "6" * 32,
                    "response_item_id": "itm_" + "7" * 32,
                    "state": "answered",
                    "version": 2,
                }
            ],
        )
        encoded = json.dumps(result, sort_keys=True)
        self.assertNotIn("repository_path", encoded)
        self.assertNotIn("credential_references", encoded)
        self.assertNotIn("agent.token", encoded)
        self.assertNotIn("secret-host-policy", encoded)

    def test_discovery_submissions_are_typed_and_bound_to_the_gateway_run(self) -> None:
        trace = {
            "schema_version": 1,
            "summary": "The request is bounded by an existing catalog.",
            "findings": [
                {
                    "key": "catalog-boundary",
                    "classification": "constraint",
                    "summary": "The catalog owns the requested primitive.",
                    "source_refs": [{"kind": "brief", "index": None}],
                    "evidence": [],
                }
            ],
            "recommended_checks": [],
            "open_questions": [],
        }
        ruling = {
            "schema_version": 1,
            "disposition": "NO_GO",
            "summary": "The request needs a narrower product decision.",
            "trace_finding_keys": ["catalog-boundary"],
            "interaction_id": None,
            "work_ready_brief": None,
        }

        trace_response = self.gateway.handle(
            request(
                "discovery.trace.submit",
                {"trace": trace, "idempotency_key": "discovery-trace-1"},
            )
        )
        ruling_response = self.gateway.handle(
            request(
                "discovery.ruling.submit",
                {"ruling": ruling, "idempotency_key": "discovery-ruling-1"},
            )
        )

        self.assertTrue(trace_response["ok"])
        self.assertTrue(ruling_response["ok"])
        self.assertEqual(trace_response["result"]["agent_action"], "exit")
        self.assertEqual(ruling_response["result"]["disposition"], "NO_GO")
        self.assertEqual(ruling_response["result"]["agent_action"], "exit")
        self.assertEqual(
            [(call[0], call[1], call[3]) for call in self.control.discovery_calls],
            [
                ("trace", RUN_ID, "discovery-trace-1"),
                ("ruling", RUN_ID, "discovery-ruling-1"),
            ],
        )
        self.assertIsInstance(self.control.discovery_calls[0][2], DiscoveryTrace)
        self.assertIsInstance(self.control.discovery_calls[1][2], DiscoveryRuling)

        for operation, payload in (
            (
                "discovery.trace.submit",
                {"trace": trace, "idempotency_key": "short", "extra": True},
            ),
            (
                "discovery.ruling.submit",
                {
                    "ruling": {**ruling, "disposition": "MAYBE"},
                    "idempotency_key": "invalid-ruling-1",
                },
            ),
        ):
            with self.subTest(operation=operation):
                response = self.gateway.handle(request(operation, payload))
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], "invalid_request")
        self.assertEqual(len(self.control.discovery_calls), 2)

    def test_discovery_question_ruling_directs_a_durable_handoff(self) -> None:
        response = self.gateway.handle(
            request(
                "discovery.ruling.submit",
                {
                    "ruling": {
                        "schema_version": 1,
                        "disposition": "ASK_HUMAN",
                        "summary": "The owner must choose the supported boundary.",
                        "trace_finding_keys": ["catalog-boundary"],
                        "interaction_id": INTERACTION_ID,
                        "work_ready_brief": None,
                    },
                    "idempotency_key": "discovery-question-1",
                },
            )
        )

        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["disposition"], "ASK_HUMAN")
        self.assertEqual(response["result"]["agent_action"], "submit_handoff_then_exit")

    def test_advisory_submissions_are_typed_and_bound_to_the_gateway_run(self) -> None:
        blind = {
            "schema_version": 1,
            "summary": "The exact candidate has no advisory findings.",
            "risk_level": "none",
            "findings": [],
        }
        challenge = {
            "schema_version": 1,
            "summary": "No peer findings require a challenge.",
            "challenges": [],
        }
        report = {
            "schema_version": 1,
            "authority": "advisory_only",
            "attention": "none",
            "summary": "The advisory panel found no actionable concerns.",
            "items": [],
            "follow_up_checks": [],
        }

        responses = (
            self.gateway.handle(
                request(
                    "advisory.blind.submit",
                    {"review": blind, "idempotency_key": "advisory-blind-1"},
                )
            ),
            self.gateway.handle(
                request(
                    "advisory.challenge.submit",
                    {
                        "review": challenge,
                        "idempotency_key": "advisory-challenge-1",
                    },
                )
            ),
            self.gateway.handle(
                request(
                    "advisory.report.submit",
                    {"report": report, "idempotency_key": "advisory-report-1"},
                )
            ),
        )

        self.assertTrue(all(response["ok"] for response in responses))
        self.assertEqual(
            [response["result"]["agent_action"] for response in responses],
            ["exit", "exit", "exit"],
        )
        self.assertEqual(
            [response["result"]["content_sha256"] for response in responses],
            [call[2].digest for call in self.control.advisory_calls],
        )
        self.assertEqual(
            [
                response["result"][key]
                for response, key in zip(
                    responses,
                    ("blind_review_id", "challenge_review_id", "report_id"),
                    strict=True,
                )
            ],
            ["abr_" + "a" * 32, "acr_" + "b" * 32, "arp_" + "c" * 32],
        )
        self.assertEqual(
            [(call[0], call[1], call[3]) for call in self.control.advisory_calls],
            [
                ("blind", RUN_ID, "advisory-blind-1"),
                ("challenge", RUN_ID, "advisory-challenge-1"),
                ("report", RUN_ID, "advisory-report-1"),
            ],
        )
        self.assertIsInstance(self.control.advisory_calls[0][2], AdvisoryBlindReview)
        self.assertIsInstance(
            self.control.advisory_calls[1][2], AdvisoryChallengeReview
        )
        self.assertIsInstance(self.control.advisory_calls[2][2], AdvisoryReport)

        for operation, payload in (
            (
                "advisory.blind.submit",
                {"review": blind, "idempotency_key": "short", "extra": True},
            ),
            (
                "advisory.blind.submit",
                {"review": [], "idempotency_key": "invalid-blind-1"},
            ),
            (
                "advisory.challenge.submit",
                {"review": [], "idempotency_key": "invalid-challenge-1"},
            ),
            (
                "advisory.report.submit",
                {"report": [], "idempotency_key": "invalid-report-shape-1"},
            ),
            (
                "advisory.report.submit",
                {
                    "report": {**report, "authority": "gate"},
                    "idempotency_key": "invalid-report-1",
                },
            ),
        ):
            with self.subTest(operation=operation):
                response = self.gateway.handle(request(operation, payload))
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], "invalid_request")
        self.assertEqual(len(self.control.advisory_calls), 3)

    def test_workflow_scope_review_is_typed_and_outcome_is_derived(self) -> None:
        review = {
            "schema_version": 1,
            "must_results": [
                {
                    "key": "preserve-authority",
                    "outcome": "passed",
                    "summary": "The immutable authority boundary is preserved.",
                    "evidence": ["Focused authority test passed."],
                }
            ],
            "probes": [
                {
                    "key": "new-regression",
                    "summary": "The candidate introduces a new failure.",
                    "base_finding": "absent",
                    "head_finding": "present",
                    "evidence": ["The reproducer fails only at candidate HEAD."],
                }
            ],
        }

        response = self.gateway.handle(
            request(
                "workflow.scope-review.submit",
                {"review": review, "idempotency_key": "scope-review-1"},
            )
        )

        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["scope_review_id"], "wsr_" + "d" * 32)
        self.assertEqual(
            response["result"]["expected_review_outcome"], "changes_requested"
        )
        self.assertEqual(response["result"]["failed_must_keys"], [])
        self.assertEqual(
            response["result"]["regression_probe_keys"], ["new-regression"]
        )
        self.assertEqual(response["result"]["follow_up_probe_keys"], [])
        self.assertEqual(
            [(call[0], call[2]) for call in self.control.workflow_scope_calls],
            [(RUN_ID, "scope-review-1")],
        )
        self.assertIsInstance(
            self.control.workflow_scope_calls[0][1], WorkflowScopeReview
        )

        for payload in (
            {
                "review": review,
                "idempotency_key": "scope-review-2",
                "outcome": "approved",
            },
            {
                "review": {
                    **review,
                    "probes": [
                        {
                            **review["probes"][0],
                            "classification": "follow_up",
                        }
                    ],
                },
                "idempotency_key": "scope-review-3",
            },
        ):
            rejected = self.gateway.handle(
                request("workflow.scope-review.submit", payload)
            )
            self.assertFalse(rejected["ok"])
            self.assertEqual(rejected["error"]["code"], "invalid_request")
        self.assertEqual(len(self.control.workflow_scope_calls), 1)

    def test_work_memory_operations_are_typed_and_bound_to_the_run(self) -> None:
        entry = {
            "schema_version": 1,
            "kind": "finding",
            "summary": "The persistence boundary is sound.",
            "details": [],
            "artifacts": [],
            "related_interaction_ids": [],
            "candidate_revision": "a" * 40,
        }
        checkpoint = {
            "schema_version": 1,
            "summary": "Worker findings are ready.",
            "decisions": [],
            "artifacts": [],
            "next_steps": ["Review both findings."],
            "open_blockers": [],
            "candidate_revision": "a" * 40,
        }
        spec = {
            "schema_version": 1,
            "title": "Review persistence",
            "objective": "Review the durable storage boundary.",
            "scope_paths": ["src/magic_glass_box/db.py"],
            "acceptance_checks": ["Run focused tests."],
            "base_revision": "a" * 40,
        }
        calls = (
            (
                "work.read",
                {"after_sequence": 1, "limit": 16},
                "requested_after_sequence",
                1,
            ),
            (
                "work.append",
                {
                    "assignment_id": None,
                    "idempotency_key": "work.append:0001",
                    "entry": entry,
                },
                "entry_id",
                WORK_ENTRY_ID,
            ),
            (
                "work.checkpoint",
                {
                    "expected_thread_version": 3,
                    "through_sequence": 2,
                    "idempotency_key": "work.checkpoint:0001",
                    "checkpoint": checkpoint,
                },
                "checkpoint_id",
                "wmc_" + "9" * 32,
            ),
            (
                "work.assign",
                {
                    "expected_thread_version": 4,
                    "target_profile_name": "Codex Worker",
                    "idempotency_key": "work.assign:0001",
                    "spec": spec,
                },
                "assignment_id",
                ASSIGNMENT_ID,
            ),
            (
                "work.assignment.claim",
                {
                    "assignment_id": ASSIGNMENT_ID,
                    "expected_assignment_version": 1,
                    "lease_seconds": 600,
                },
                "state",
                "claimed",
            ),
            (
                "work.assignment.complete",
                {
                    "assignment_id": ASSIGNMENT_ID,
                    "expected_assignment_version": 2,
                    "idempotency_key": "work.result:0001",
                    "result": {**entry, "kind": "assignment_result"},
                },
                "result_entry_id",
                WORK_ENTRY_ID,
            ),
        )
        for operation, payload, result_key, expected in calls:
            with self.subTest(operation=operation):
                response = self.gateway.handle(request(operation, payload))
                self.assertTrue(response["ok"], response)
                self.assertEqual(response["result"][result_key], expected)
                if operation == "work.assignment.claim":
                    self.assertEqual(
                        response["result"],
                        {
                            "assignment_id": ASSIGNMENT_ID,
                            "state": "claimed",
                            "version": 2,
                            "lease_expires_at": ("2026-10-10T12:10:00.000000+00:00"),
                        },
                    )

        self.assertTrue(
            all(call[1] == RUN_ID for call in self.control.work_calls),
            self.control.work_calls,
        )

    def test_work_memory_payloads_fail_closed_before_dispatch(self) -> None:
        cases = (
            request("work.read", {"after_sequence": -1, "limit": 16}),
            request("work.read", {"after_sequence": None, "limit": 17}),
            request(
                "work.append",
                {
                    "assignment_id": None,
                    "idempotency_key": "short",
                    "entry": {},
                },
            ),
            request(
                "work.assign",
                {
                    "expected_thread_version": 1,
                    "target_profile_name": "Bad/Profile",
                    "idempotency_key": "work.assign:0001",
                    "spec": {},
                },
            ),
            request(
                "work.assignment.claim",
                {
                    "assignment_id": "was_bad",
                    "expected_assignment_version": 1,
                    "lease_seconds": 600,
                },
            ),
        )
        for candidate in cases:
            with self.subTest(operation=candidate["operation"]):
                response = self.gateway.handle(candidate)
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], "invalid_request")
        self.assertEqual(self.control.work_calls, [])

    def test_describe_bounds_historical_interaction_metadata(self) -> None:
        self.control.database.interactions = [
            {
                "id": "int_" + f"{index:032x}",
                "kind": "question",
                "state": "answered",
                "version": 2,
                "request_item_id": "itm_" + f"{index:032x}",
                "response_item_id": "itm_" + f"{index + 1000:032x}",
                "recipient": {"krab_handle": "reviewer"},
            }
            for index in range(130)
        ]

        result = self.gateway.handle(request("run.describe"))["result"]

        self.assertEqual(result["interaction_count"], 130)
        self.assertTrue(result["interactions_truncated"])
        self.assertEqual(len(result["interactions"]), 128)
        self.assertEqual(result["interactions"][0]["id"], "int_" + f"{2:032x}")

    def test_describe_exposes_bodyless_recovery_checkpoint_provenance(self) -> None:
        checkpoint = {
            "id": "rcp_" + "1" * 32,
            "attempt_id": ATTEMPT_ID,
            "source_run_id": OTHER_RUN_ID,
            "source_attempt_id": "att_" + "2" * 32,
            "result_id": "rsl_" + "3" * 32,
            "source_revision": "a" * 40,
            "content_sha256": "b" * 64,
            "changed_paths": 3,
            "changed_bytes": 128,
            "created_at": "2026-10-18T12:00:00.000000+00:00",
            "applied_at": "2026-10-18T12:00:01.000000+00:00",
        }
        self.control.database.recovery_checkpoint = checkpoint

        result = self.gateway.handle(request("run.describe"))["result"]

        self.assertEqual(result["recovery_checkpoint"], checkpoint)
        self.assertNotIn("manifest", result["recovery_checkpoint"])
        self.assertNotIn("files", result["recovery_checkpoint"])

    def test_open_uses_bound_run_and_current_version_then_instructs_exit(self) -> None:
        response = self.gateway.handle(
            request(
                "interaction.open",
                {
                    "kind": "approval",
                    "recipient_handle": "reviewer",
                    "title": "Approve the migration?",
                    "body": "The migration is ready for review.",
                    "idempotency_key": "gateway.request:0001",
                },
            )
        )

        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["interaction_id"], INTERACTION_ID)
        self.assertEqual(response["result"]["agent_action"], "submit_handoff_then_exit")
        self.assertEqual(
            self.control.open_calls,
            [
                (
                    RUN_ID,
                    {
                        "kind": "approval",
                        "recipient_krab_handle": "reviewer",
                        "title": "Approve the migration?",
                        "body": "The migration is ready for review.",
                        "idempotency_key": "gateway.request:0001",
                        "expected_run_version": 3,
                    },
                )
            ],
        )

    def test_submit_handoff_uses_bound_run_and_returns_exit_instruction(self) -> None:
        payload = {
            "schema_version": 1,
            "work_state": "waiting_for_input",
            "summary": "Waiting for the reviewer to choose a policy.",
            "decisions": ["Retain network isolation."],
            "artifacts": [
                {
                    "path": "docs/architecture.md",
                    "description": "Relevant boundary documentation.",
                }
            ],
            "next_steps": ["Read the response and apply the choice."],
            "pending_interaction_ids": [INTERACTION_ID],
        }

        response = self.gateway.handle(request("handoff.submit", payload))

        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["handoff_id"], HANDOFF_ID)
        self.assertEqual(response["result"]["attempt_id"], ATTEMPT_ID)
        self.assertEqual(response["result"]["agent_action"], "exit")
        self.assertEqual(len(self.control.handoff_calls), 1)
        run_id, handoff = self.control.handoff_calls[0]
        self.assertEqual(run_id, RUN_ID)
        self.assertEqual(handoff.to_dict(), payload)

    def test_read_can_only_request_a_response_for_the_bound_run(self) -> None:
        response = self.gateway.handle(
            request("interaction.read", {"interaction_id": INTERACTION_ID})
        )
        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["untrusted_body"], "Proceed with option B.")
        self.assertEqual(self.control.read_calls, [(RUN_ID, INTERACTION_ID)])

        cross_run = self.gateway.handle(
            request(
                "interaction.read",
                {
                    "interaction_id": INTERACTION_ID,
                    "run_id": OTHER_RUN_ID,
                },
            )
        )
        self.assertFalse(cross_run["ok"])
        self.assertEqual(cross_run["error"]["code"], "invalid_request")
        self.assertEqual(self.control.read_calls, [(RUN_ID, INTERACTION_ID)])

    def test_provider_invoke_uses_only_a_declared_run_grant(self) -> None:
        response = self.gateway.handle(
            request(
                "provider.invoke",
                {"grant": "openai-main", "request": {"input": "hello"}},
            )
        )
        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["provider"], "openai")
        self.assertEqual(response["result"]["model"], "gpt-test")
        self.assertEqual(
            response["result"]["untrusted_response"], {"output_text": "done"}
        )
        self.assertEqual(
            self.control.provider_calls,
            [(RUN_ID, "openai-main", {"input": "hello"})],
        )

        denied = self.gateway.handle(
            request(
                "provider.invoke",
                {"grant": "other-provider", "request": {"input": "hello"}},
            )
        )
        self.assertFalse(denied["ok"])
        self.assertEqual(denied["error"]["code"], "forbidden")
        self.assertEqual(len(self.control.provider_calls), 1)

    def test_provider_payload_and_failures_are_bounded_and_redacted(self) -> None:
        for payload in (
            {"grant": "openai-main", "request": []},
            {
                "grant": "openai-main",
                "request": {"model": "worker-selected"},
            },
            {
                "grant": "openai-main",
                "request": {"input": "hello"},
                "destination": "https://attacker.example",
            },
        ):
            with self.subTest(payload=payload):
                response = self.gateway.handle(request("provider.invoke", payload))
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], "invalid_request")
        self.assertEqual(self.control.provider_calls, [])

        failures = (
            (ProviderBusyError("SECRET busy"), "busy", None),
            (ProviderBudgetError("SECRET budget"), "budget_exhausted", None),
            (
                ProviderQuotaError(
                    "SECRET quota", retry_after_seconds=900, status_code=429
                ),
                "quota_exhausted",
                900,
            ),
            (
                ProviderRateLimitError(
                    "SECRET rate", retry_after_seconds=45, status_code=429
                ),
                "rate_limited",
                45,
            ),
            (ProviderAmbiguousError("SECRET ambiguous"), "ambiguous", None),
            (ProviderUpstreamError("SECRET upstream"), "upstream_error", None),
            (ProviderError("SECRET denied"), "forbidden", None),
        )
        for failure, code, retry_after_seconds in failures:
            with self.subTest(code=code):
                self.control.provider_failure = failure
                response = self.gateway.handle(
                    request(
                        "provider.invoke",
                        {"grant": "openai-main", "request": {"input": "hello"}},
                    )
                )
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], code)
                self.assertEqual(
                    response["error"].get("retry_after_seconds"),
                    retry_after_seconds,
                )
                self.assertNotIn("SECRET", json.dumps(response))

    def test_protocol_and_capability_errors_fail_closed(self) -> None:
        limited = RunGateway(
            self.control,
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        cases = (
            (
                {**request("run.describe"), "extra": True},
                "invalid_request",
                "invalid gateway request",
            ),
            (
                {**request("run.describe"), "version": 2},
                "invalid_request",
                "unsupported gateway protocol version",
            ),
            (
                request("unknown.operation"),
                "invalid_request",
                "unknown gateway operation",
            ),
            (
                request("interaction.open"),
                "forbidden",
                "operation is not authorized for this run",
            ),
        )
        for candidate, code, message in cases:
            with self.subTest(code=code, candidate=candidate):
                response = limited.handle(candidate)
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], code)
                self.assertEqual(response["error"]["message"], message)

        without_credential = RunGateway(
            self.control,
            RUN_ID,
            capabilities=(GatewayCapability.INTERACTION_OPEN,),
            credential_names=(),
        )
        denied = without_credential.handle(request("interaction.open"))
        self.assertFalse(denied["ok"])
        self.assertEqual(denied["error"]["code"], "forbidden")
        self.assertEqual(
            denied["error"]["message"], "operation has no authorized credential"
        )

        invalid_id = limited.handle({**request("run.describe"), "request_id": "short"})
        self.assertIsNone(invalid_id["request_id"])
        self.assertEqual(invalid_id["error"]["code"], "invalid_request")
        self.assertEqual(invalid_id["error"]["message"], "invalid request_id")

    def test_constructor_rejects_invalid_binding_policy(self) -> None:
        cases = (
            ("run-bad", (GatewayCapability.RUN_DESCRIBE,), ("krab",), "run"),
            (RUN_ID, ("provider.anywhere",), ("krab",), "capability"),
            (
                RUN_ID,
                (GatewayCapability.RUN_DESCRIBE, GatewayCapability.RUN_DESCRIBE),
                ("krab",),
                "duplicates",
            ),
            (RUN_ID, (GatewayCapability.RUN_DESCRIBE,), ("",), "credential"),
            (
                RUN_ID,
                (GatewayCapability.RUN_DESCRIBE,),
                ("krab", "krab"),
                "credential",
            ),
            (
                RUN_ID,
                (GatewayCapability.RUN_DESCRIBE,),
                (["krab"],),
                "credential",
            ),
        )
        for run_id, capabilities, credentials, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(GatewayError, message),
            ):
                RunGateway(
                    self.control,
                    run_id,
                    capabilities=capabilities,
                    credential_names=credentials,
                )

    def test_operation_payloads_reject_bad_fields_without_dispatch(self) -> None:
        valid_open = {
            "kind": "question",
            "recipient_handle": "reviewer",
            "title": "Need input",
            "body": "Choose a path.",
            "idempotency_key": "gateway.request:0002",
        }
        cases = (
            (request("run.describe", {"extra": True}), "invalid operation payload"),
            (
                request("interaction.open", {**valid_open, "kind": "unknown"}),
                "invalid interaction kind",
            ),
            (
                request("interaction.open", {**valid_open, "recipient_handle": "BAD"}),
                "invalid recipient_handle",
            ),
            (
                request("interaction.open", {**valid_open, "title": ""}),
                "invalid title",
            ),
            (
                request("interaction.open", {**valid_open, "title": "bad\x00title"}),
                "invalid title",
            ),
            (
                request(
                    "interaction.open", {**valid_open, "idempotency_key": "bad key"}
                ),
                "invalid idempotency_key",
            ),
            (
                request("interaction.read", {"interaction_id": "int_bad"}),
                "invalid interaction_id",
            ),
        )
        for candidate, message in cases:
            with self.subTest(operation=candidate["operation"]):
                response = self.gateway.handle(candidate)
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], "invalid_request")
                self.assertEqual(response["error"]["message"], message)
        self.assertEqual(self.control.open_calls, [])
        self.assertEqual(self.control.read_calls, [])

    def test_domain_and_dependency_failures_are_typed_and_secrets_redacted(
        self,
    ) -> None:
        cases = (
            (ValidationError("bad input"), "invalid_request", "bad input"),
            (NotFoundError("missing"), "not_found", "missing"),
            (ConflictError("stale"), "conflict", "stale"),
            (KrabError("SECRET upstream"), "upstream_error", "upstream request failed"),
            (StorageError("SECRET database"), "internal_error", "operation failed"),
            (RuntimeError("SECRET crash"), "internal_error", "operation failed"),
        )
        for failure, code, message in cases:
            with (
                self.subTest(code=code),
                mock.patch.object(self.gateway, "_describe", side_effect=failure),
            ):
                response = self.gateway.handle(request("run.describe"))
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], code)
                self.assertIn(message, response["error"]["message"])
                if "SECRET" in str(failure):
                    self.assertNotIn("SECRET", json.dumps(response))


class FileGatewayManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(dir="/tmp")  # noqa: S108
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "gateways"
        self.control = FakeGatewayControl()
        self.manager = FileGatewayManager(
            self.root, self.control, poll_interval=0.01, request_timeout=0.25
        )
        self.addCleanup(self.manager.close)
        self.sequence = 0

    def exchange(self, directory: Path, value: bytes) -> dict[str, Any]:
        self.sequence += 1
        self.commit(directory, value, sequence=self.sequence)
        return self.wait_response(directory, self.sequence)

    def commit(
        self,
        directory: Path,
        value: bytes,
        *,
        sequence: int,
        digest: str | None = None,
    ) -> None:
        (directory / GATEWAY_REQUEST_NAME).write_bytes(value)
        marker = {
            "sequence": sequence,
            "sha256": digest or hashlib.sha256(value).hexdigest(),
            "size": len(value),
        }
        (directory / GATEWAY_REQUEST_READY_NAME).write_text(
            canonical_json(marker) + "\n", encoding="ascii"
        )

    def wait_response(self, directory: Path, sequence: int) -> dict[str, Any]:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            ready = (directory / GATEWAY_RESPONSE_READY_NAME).read_bytes()
            if ready:
                response_marker = json.loads(ready)
                if response_marker["sequence"] == sequence:
                    raw = (directory / GATEWAY_RESPONSE_NAME).read_bytes()
                    if (
                        response_marker["size"] == len(raw)
                        and response_marker["sha256"] == hashlib.sha256(raw).hexdigest()
                    ):
                        return json.loads(raw)
            time.sleep(0.01)
        self.fail("gateway response timed out")

    def test_mailbox_is_private_framed_and_removed_on_cleanup(self) -> None:
        endpoint = self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        self.assertEqual(endpoint.directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(
            sorted(path.name for path in endpoint.directory.iterdir()),
            sorted(
                (
                    GATEWAY_REQUEST_NAME,
                    GATEWAY_REQUEST_READY_NAME,
                    GATEWAY_RESPONSE_NAME,
                    GATEWAY_RESPONSE_READY_NAME,
                )
            ),
        )
        for path in endpoint.directory.iterdir():
            self.assertTrue(stat.S_ISREG(path.lstat().st_mode))
            self.assertEqual(path.lstat().st_mode & 0o777, 0o600)

        response = self.exchange(
            endpoint.directory,
            (json.dumps(request("run.describe")) + "\n").encode(),
        )
        self.assertTrue(response["ok"])
        self.assertEqual(response["request_id"], "req-12345678")
        self.assertTrue(self.manager.cleanup(RUN_ID))
        self.assertFalse(endpoint.directory.exists())
        self.assertFalse(self.manager.cleanup(RUN_ID))

    def test_cleanup_detaches_from_an_in_flight_gateway_operation(self) -> None:
        endpoint = self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        entered = threading.Event()
        release = threading.Event()
        describe = self.control.database.get_run_execution

        def blocked_describe(run_id: str) -> dict[str, Any]:
            entered.set()
            release.wait(timeout=10)
            return describe(run_id)

        self.control.database.get_run_execution = blocked_describe  # type: ignore[method-assign]
        raw = (json.dumps(request("run.describe")) + "\n").encode()
        self.commit(endpoint.directory, raw, sequence=1)
        self.assertTrue(entered.wait(timeout=1))

        started = time.monotonic()
        try:
            self.assertTrue(self.manager.cleanup(RUN_ID))
        finally:
            release.set()

        self.assertLess(time.monotonic() - started, 0.5)
        self.assertFalse(endpoint.directory.exists())

    def test_malformed_oversized_and_duplicate_requests_are_bounded(self) -> None:
        endpoint = self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        for raw in (
            b"not-json\n",
            b'{"version":1,"version":1}\n',
            b"{" + b"x" * MAX_GATEWAY_REQUEST_BYTES + b"}\n",
        ):
            with self.subTest(size=len(raw)):
                response = self.exchange(endpoint.directory, raw)
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], "invalid_request")

    def test_commit_mismatch_and_replayed_sequence_fail_closed(self) -> None:
        endpoint = self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        raw = (json.dumps(request("run.describe")) + "\n").encode()
        self.sequence = 1
        self.commit(endpoint.directory, raw, sequence=1, digest="0" * 64)
        invalid_commit = self.wait_response(endpoint.directory, 1)
        self.assertFalse(invalid_commit["ok"])
        self.assertIn("commit", invalid_commit["error"]["message"])

        self.commit(endpoint.directory, raw, sequence=1)
        deadline = time.monotonic() + 2
        replayed = invalid_commit
        while time.monotonic() < deadline:
            ready = json.loads(
                (endpoint.directory / GATEWAY_RESPONSE_READY_NAME).read_bytes()
            )
            raw_response = (endpoint.directory / GATEWAY_RESPONSE_NAME).read_bytes()
            if (
                ready["sequence"] == 1
                and ready["size"] == len(raw_response)
                and ready["sha256"] == hashlib.sha256(raw_response).hexdigest()
            ):
                candidate = json.loads(raw_response)
                if candidate != invalid_commit:
                    replayed = candidate
                    break
            time.sleep(0.01)
        self.assertFalse(replayed["ok"])
        self.assertIn("not increasing", replayed["error"]["message"])

        valid = self.exchange(endpoint.directory, raw)
        self.assertTrue(valid["ok"])
        self.commit(endpoint.directory, raw, sequence=1)
        replayed = self.wait_response(endpoint.directory, 1)
        self.assertFalse(replayed["ok"])
        self.assertIn("not increasing", replayed["error"]["message"])

    def test_persistent_malformed_marker_times_out_as_invalid_request(self) -> None:
        endpoint = self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        marker = {"sequence": 1, "sha256": "not-a-digest", "size": 1}
        (endpoint.directory / GATEWAY_REQUEST_READY_NAME).write_text(
            canonical_json(marker) + "\n", encoding="ascii"
        )

        response = self.wait_response(endpoint.directory, 1)

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "invalid_request")
        self.assertIn("marker", response["error"]["message"])

    def test_transient_missing_request_marker_does_not_kill_the_channel(self) -> None:
        endpoint = self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        marker_path = endpoint.directory / GATEWAY_REQUEST_READY_NAME
        marker_path.unlink()
        time.sleep(0.05)
        marker_path.touch(mode=0o600)

        response = self.exchange(
            endpoint.directory,
            (json.dumps(request("run.describe")) + "\n").encode(),
        )

        self.assertTrue(response["ok"])

    def test_duplicate_or_unsafe_endpoint_state_is_rejected(self) -> None:
        self.manager.open(
            RUN_ID,
            capabilities=(GatewayCapability.RUN_DESCRIBE,),
            credential_names=("krab",),
        )
        with self.assertRaisesRegex(GatewayError, "already active"):
            self.manager.open(
                RUN_ID,
                capabilities=(GatewayCapability.RUN_DESCRIBE,),
                credential_names=("krab",),
            )

        other_root = Path(self.temporary.name) / "other"
        other_root.mkdir(mode=0o755)
        with self.assertRaisesRegex(GatewayError, "mode 0700"):
            FileGatewayManager(other_root, self.control).open(
                OTHER_RUN_ID,
                capabilities=(GatewayCapability.RUN_DESCRIBE,),
                credential_names=("krab",),
            )

    def test_default_root_is_short_stable_and_overrideable(self) -> None:
        database = Path("/a/very/long/private/location/control-plane.db")
        with mock.patch.dict(os.environ, {}, clear=True):
            first = default_gateway_root(database)
            second = default_gateway_root(database)
            other = default_gateway_root(Path("/another/control-plane.db"))
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        self.assertEqual(first.parent, Path("/tmp"))  # noqa: S108
        self.assertNotIn("private", str(first))

        override = Path(self.temporary.name) / "runtime"
        with mock.patch.dict(
            os.environ, {"MAGIC_GLASS_BOX_RUNTIME_DIR": str(override)}, clear=True
        ):
            self.assertEqual(default_gateway_root(database).parent, override)


if __name__ == "__main__":
    unittest.main()
