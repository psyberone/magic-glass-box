from __future__ import annotations

import unittest
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from magic_glass_box.domain import (
    ValidationError,
    canonical_json,
    content_sha256,
)
from magic_glass_box.jira_provider import (
    JiraAPIResponse,
    JiraProvider,
    JiraProviderError,
    JiraTransportError,
)
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationSpec,
    ServiceSidecarSpec,
)

CONNECTOR_ID = "ssc_" + "1" * 32
DEPLOYMENT = "d" * 64
SCOPE = "e" * 64
DEADLINE = "2099-01-01T00:00:00.000000+00:00"


def sidecar_spec(
    socket_path: Path, *, include_state: bool = False
) -> ServiceSidecarSpec:
    capabilities = [
        {"name": "work.get", "version": 1, "mode": "read"},
        {"name": "work.list", "version": 1, "mode": "read"},
    ]
    if include_state:
        capabilities.append({"name": "work.state", "version": 1, "mode": "effect"})
    return ServiceSidecarSpec.from_mapping(
        {
            "schema_version": 2,
            "connector_protocol_version": 2,
            "deployment_sha256": DEPLOYMENT,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "capabilities": capabilities,
            "mappings": [
                {
                    "name": "primary",
                    "service": "jira",
                    "host": "acme.atlassian.net",
                    "resource": {"kind": "work_project", "external_id": "ABC"},
                    "account": "operator@example.test",
                    "credential_scope_sha256": SCOPE,
                }
            ],
            "request_bytes": 128 * 1024,
            "response_bytes": 512 * 1024,
            "timeout_ms": 30000,
            "max_concurrency": 2,
            "max_requests_per_minute": 60,
        }
    )


def request(
    socket_path: Path,
    capability: str,
    *,
    object_id: str | None = None,
    parameters: Mapping[str, Any] | None = None,
    phase: str = "invoke",
) -> ServiceOperationRequest:
    spec = sidecar_spec(socket_path, include_state=capability == "work.state")
    operation = ServiceOperationSpec.from_mapping(
        {
            "schema_version": 2,
            "connector_name": "jira-main",
            "deployment_sha256": DEPLOYMENT,
            "mapping": spec.mappings[0].to_dict(),
            "capability": {
                "name": capability,
                "version": 1,
                "mode": "effect" if capability == "work.state" else "read",
            },
            "idempotency_key": (
                f"test:{capability}:{content_sha256(object_id or 'issues')[:16]}"
            ),
            "target": {
                "resource": {"kind": "work_project", "external_id": "ABC"},
                "object_id": object_id
                or ("ABC-42" if capability in {"work.get", "work.state"} else "issues"),
                "object_format": None,
                "expected_revision": None,
                "desired_revision": None,
            },
            "parameters": dict(parameters or {}),
            "request_bytes": 128 * 1024,
            "response_bytes": 512 * 1024,
            "timeout_ms": 30000,
        }
    )
    return ServiceOperationRequest.create(
        operation_id="sop_" + "2" * 32,
        connector_id=CONNECTOR_ID,
        connector_protocol_version=2,
        operation=operation,
        phase=phase,
        deadline=DEADLINE,
    )


def adf(*text: str) -> dict[str, Any]:
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {
                "type": "paragraph",
                "content": [{"type": "text", "text": value}],
            }
            for value in text
        ],
    }


_DEFAULT_DESCRIPTION = object()


def issue(
    *,
    key: str = "ABC-42",
    title: str = "Bounded Jira issue",
    description: object = _DEFAULT_DESCRIPTION,
    assignee: object = ...,
) -> dict[str, Any]:
    if description is _DEFAULT_DESCRIPTION:
        description = adf("Implement the bounded behavior.", "Keep Jira private.")
    if assignee is ...:
        assignee = {"accountId": "jira-account-123", "displayName": "Operator"}
    return {
        "id": "10042",
        "key": key,
        "fields": {
            "summary": title,
            "description": description,
            "updated": "2098-12-31T23:00:00.000+0000",
            "labels": ["agent-ready", "backend"],
            "assignee": assignee,
            "status": {"name": "In Progress"},
        },
    }


def status_issue(*, key: str = "ABC-42", status_id: str = "3") -> dict[str, Any]:
    return {
        "id": "10042",
        "key": key,
        "fields": {"status": {"id": status_id, "name": "Provider status"}},
    }


def api_response(
    status: int,
    body: object,
    *,
    retry_after_seconds: int | None = None,
    response_bytes: int | None = None,
) -> JiraAPIResponse:
    size = (
        len(canonical_json(body).encode("utf-8"))
        if response_bytes is None
        else response_bytes
    )
    return JiraAPIResponse(status, body, size, retry_after_seconds)


class FakeJiraAPI:
    def __init__(self, responses: list[JiraAPIResponse | BaseException]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def request(
        self,
        method: str,
        *,
        host: str,
        endpoint: str,
        body: Mapping[str, Any] | None,
        deadline: str,
        response_limit_bytes: int,
    ) -> JiraAPIResponse:
        self.calls.append(
            {
                "method": method,
                "host": host,
                "endpoint": endpoint,
                "body": None if body is None else dict(body),
                "deadline": deadline,
                "response_limit_bytes": response_limit_bytes,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class JiraProviderTests(unittest.TestCase):
    socket_path = Path("/private/tmp/mgb-jira-tests/jira.sock")

    def provider(
        self, *responses: JiraAPIResponse | BaseException
    ) -> tuple[JiraProvider, FakeJiraAPI]:
        api = FakeJiraAPI(list(responses))
        return JiraProvider(sidecar_spec(self.socket_path), api), api

    def state_provider(
        self, *responses: JiraAPIResponse | BaseException
    ) -> tuple[JiraProvider, FakeJiraAPI]:
        api = FakeJiraAPI(list(responses))
        return (
            JiraProvider(
                sidecar_spec(self.socket_path, include_state=True),
                api,
                state_targets={"ABC": {"done": "10001"}},
            ),
            api,
        )

    def test_work_get_normalizes_exact_issue_and_adf_without_raw_provider_fields(
        self,
    ) -> None:
        provider, api = self.provider(api_response(200, issue()))

        response = provider.invoke(request(self.socket_path, "work.get"))

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.provider_status, 200)
        self.assertEqual(response.request_bytes, 0)
        self.assertIsNotNone(response.receipt)
        assert response.receipt is not None
        self.assertEqual(
            response.receipt.remote_identity,
            "jira:acme.atlassian.net/ABC-42",
        )
        self.assertEqual(
            response.receipt.result,
            {
                "acceptance_criteria": [],
                "brief": "Implement the bounded behavior.\nKeep Jira private.",
                "constraints": [],
                "context": [],
                "external_id": "acme.atlassian.net/browse/ABC-42",
                "labels": ["agent-ready", "backend"],
                "priority": "normal",
                "schema_version": 1,
                "source": "jira",
                "source_updated_at": "2098-12-31T23:00:00.000000Z",
                "title": "Bounded Jira issue",
            },
        )
        self.assertEqual(
            response.receipt.evidence_sha256,
            content_sha256(canonical_json({"kind": "work", **response.receipt.result})),
        )
        self.assertEqual(
            api.calls,
            [
                {
                    "method": "GET",
                    "host": "acme.atlassian.net",
                    "endpoint": (
                        "/rest/api/3/issue/ABC-42"
                        "?fields=summary%2Cupdated%2Cassignee%2Clabels%2Cdescription"
                    ),
                    "body": None,
                    "deadline": DEADLINE,
                    "response_limit_bytes": 512 * 1024,
                }
            ],
        )
        serialized = canonical_json(response.to_dict())
        self.assertNotIn("In Progress", serialized)
        self.assertNotIn("displayName", serialized)
        self.assertNotIn("10042", serialized)

    def test_work_get_uses_title_when_description_is_null_or_blank(self) -> None:
        for description in (None, adf("   ")):
            with self.subTest(description=description):
                provider, _api = self.provider(
                    api_response(200, issue(description=description))
                )
                response = provider.invoke(request(self.socket_path, "work.get"))
                assert response.receipt is not None
                assert isinstance(response.receipt.result, dict)
                self.assertEqual(response.receipt.result["brief"], "Bounded Jira issue")

    def test_work_get_rejects_wrong_issue_or_project_without_returning_payload(
        self,
    ) -> None:
        for key in ("ABC-43", "OTHER-42", "abc-42"):
            with self.subTest(key=key):
                provider, _api = self.provider(api_response(200, issue(key=key)))
                response = provider.invoke(request(self.socket_path, "work.get"))
                self.assertEqual(response.outcome, "denied")
                self.assertEqual(response.error_code, "jira-request-invalid")
                self.assertIsNone(response.receipt)

    def test_work_list_constructs_project_query_and_returns_normalized_window(
        self,
    ) -> None:
        search = {
            "issues": [
                issue(),
                issue(key="ABC-43", title="Second issue", assignee=None),
            ],
            "isLast": False,
            "nextPageToken": "provider-secret-cursor",
            "names": {"summary": "Summary"},
        }
        provider, api = self.provider(api_response(200, search))

        response = provider.invoke(
            request(self.socket_path, "work.list", parameters={"limit": 2})
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.provider_status, 200)
        assert response.receipt is not None
        self.assertEqual(
            response.receipt.result,
            {
                "items": [
                    {
                        "assignees": ["jira-account-123"],
                        "external_id": "acme.atlassian.net/browse/ABC-42",
                        "labels": ["agent-ready", "backend"],
                        "object_id": "ABC-42",
                        "source_updated_at": "2098-12-31T23:00:00.000000Z",
                        "title": "Bounded Jira issue",
                    },
                    {
                        "assignees": [],
                        "external_id": "acme.atlassian.net/browse/ABC-43",
                        "labels": ["agent-ready", "backend"],
                        "object_id": "ABC-43",
                        "source_updated_at": "2098-12-31T23:00:00.000000Z",
                        "title": "Second issue",
                    },
                ],
                "schema_version": 1,
                "truncated": True,
            },
        )
        self.assertEqual(
            response.receipt.evidence_sha256,
            content_sha256(
                canonical_json({"kind": "work-list", **response.receipt.result})
            ),
        )
        self.assertEqual(len(api.calls), 1)
        call = api.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["endpoint"], "/rest/api/3/search/jql")
        expected_body = {
            "fields": ["summary", "updated", "assignee", "labels"],
            "jql": 'project = "ABC" ORDER BY updated DESC, key ASC',
            "maxResults": 2,
        }
        self.assertEqual(call["body"], expected_body)
        self.assertEqual(
            response.request_bytes,
            len(canonical_json(expected_body).encode("utf-8")),
        )
        serialized = canonical_json(response.to_dict())
        self.assertNotIn("provider-secret-cursor", serialized)
        self.assertNotIn("In Progress", serialized)
        self.assertNotIn("description", serialized)

    def test_work_list_marks_a_last_page_complete_without_requiring_a_cursor(
        self,
    ) -> None:
        provider, _api = self.provider(
            api_response(200, {"issues": [issue()], "isLast": True})
        )
        response = provider.invoke(
            request(self.socket_path, "work.list", parameters={"limit": 100})
        )
        assert response.receipt is not None
        assert isinstance(response.receipt.result, dict)
        self.assertFalse(response.receipt.result["truncated"])

    def test_work_list_accepts_the_minimum_limit(self) -> None:
        provider, api = self.provider(api_response(200, {"issues": [], "isLast": True}))

        response = provider.invoke(
            request(self.socket_path, "work.list", parameters={"limit": 1})
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(api.calls[0]["body"]["maxResults"], 1)

    def test_work_list_keeps_provider_cursor_private_but_enforces_its_bounds(
        self,
    ) -> None:
        accepted = {
            "issues": [],
            "isLast": False,
            "nextPageToken": "x" * 4096,
        }
        provider, _api = self.provider(api_response(200, accepted))
        response = provider.invoke(
            request(self.socket_path, "work.list", parameters={"limit": 1})
        )
        self.assertEqual(response.outcome, "succeeded")
        self.assertNotIn("nextPageToken", canonical_json(response.to_dict()))

        for token in ("", 1, "x" * 4097):
            with self.subTest(token=token):
                provider, _api = self.provider(
                    api_response(
                        200,
                        {
                            "issues": [],
                            "isLast": False,
                            "nextPageToken": token,
                        },
                    )
                )
                response = provider.invoke(
                    request(
                        self.socket_path,
                        "work.list",
                        parameters={"limit": 1},
                    )
                )
                self.assertEqual(response.outcome, "denied")

    def test_work_list_ignores_an_unrequested_provider_description(self) -> None:
        item = issue(description=adf("x" * 65537))
        provider, _api = self.provider(
            api_response(200, {"issues": [item], "isLast": True})
        )

        response = provider.invoke(
            request(self.socket_path, "work.list", parameters={"limit": 1})
        )

        self.assertEqual(response.outcome, "succeeded")

    def test_work_list_requires_bounded_generic_parameters_and_target(self) -> None:
        bad_parameters = (
            {},
            {"limit": True},
            {"limit": 0},
            {"limit": 101},
            {"limit": 10, "jql": "project = SECRET"},
            {"limit": 10, "cursor": "jira-token"},
        )
        for parameters in bad_parameters:
            with self.subTest(parameters=parameters):
                provider, api = self.provider()
                response = provider.invoke(
                    request(self.socket_path, "work.list", parameters=parameters)
                )
                self.assertEqual(response.outcome, "denied")
                self.assertEqual(api.calls, [])
        provider, api = self.provider()
        response = provider.invoke(
            request(
                self.socket_path,
                "work.list",
                object_id="ABC-42",
                parameters={"limit": 10},
            )
        )
        self.assertEqual(response.outcome, "denied")
        self.assertEqual(api.calls, [])

    def test_work_list_rejects_inconsistent_or_oversized_provider_pages(self) -> None:
        bad_pages = (
            {"issues": [], "isLast": False},
            {"issues": [], "isLast": "false", "nextPageToken": "next"},
            {"issues": [issue(), issue(key="ABC-42")], "isLast": True},
            {"issues": [issue(key="OTHER-1")], "isLast": True},
        )
        for page in bad_pages:
            with self.subTest(page=page):
                provider, _api = self.provider(api_response(200, page))
                response = provider.invoke(
                    request(self.socket_path, "work.list", parameters={"limit": 1})
                )
                self.assertEqual(response.outcome, "denied")

    def test_http_statuses_map_to_bounded_read_outcomes(self) -> None:
        cases = (
            (401, None, "denied", "jira-denied", None),
            (403, None, "denied", "jira-denied", None),
            (404, None, "conflict", "jira-conflict", None),
            (429, 23, "throttled", "jira-throttled", 23),
            (503, None, "retryable", "jira-upstream-unavailable", 15),
        )
        for status, upstream_retry, outcome, error_code, retry in cases:
            with self.subTest(status=status):
                provider, _api = self.provider(
                    api_response(
                        status,
                        {"errorMessages": ["private"]},
                        retry_after_seconds=upstream_retry,
                    )
                )
                response = provider.invoke(request(self.socket_path, "work.get"))
                self.assertEqual(response.outcome, outcome)
                self.assertEqual(response.error_code, error_code)
                self.assertEqual(response.retry_after_seconds, retry)
                self.assertNotIn("private", canonical_json(response.to_dict()))

    def test_transport_failure_and_response_budget_are_retryable(self) -> None:
        failures: tuple[BaseException | JiraAPIResponse, ...] = (
            JiraTransportError("private network detail"),
            api_response(200, issue(), response_bytes=512 * 1024 + 1),
        )
        for failure in failures:
            with self.subTest(failure=failure):
                provider, _api = self.provider(failure)
                response = provider.invoke(request(self.socket_path, "work.get"))
                self.assertEqual(response.outcome, "retryable")
                self.assertEqual(response.error_code, "jira-transport-unavailable")
                self.assertNotIn(
                    "private network detail", canonical_json(response.to_dict())
                )

    def test_provider_accepts_an_exact_transport_response_budget(self) -> None:
        probe_provider, _probe_api = self.provider(api_response(200, issue()))
        probe = probe_provider.invoke(request(self.socket_path, "work.get"))
        assert probe.receipt is not None
        assert probe.receipt.result is not None
        exact_limit = len(canonical_json(probe.receipt.result).encode("utf-8"))

        provider, _api = self.provider(
            api_response(200, issue(), response_bytes=exact_limit)
        )
        bounded_request = replace(
            request(self.socket_path, "work.get"),
            response_limit_bytes=exact_limit,
        )

        response = provider.invoke(bounded_request)

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.response_bytes, exact_limit)

    def test_adf_is_bounded_and_structurally_validated(self) -> None:
        too_deep: dict[str, Any] = {"type": "text", "text": "bottom"}
        for _ in range(70):
            too_deep = {"type": "panel", "content": [too_deep]}
        bad_descriptions: tuple[object, ...] = (
            {"type": "doc", "version": 2, "content": []},
            {"type": "paragraph", "version": 1, "content": []},
            {"type": "doc", "version": 1, "content": "not-a-list"},
            {"type": "doc", "version": 1, "content": [too_deep]},
            adf("x" * 65537),
        )
        for description in bad_descriptions:
            with self.subTest(description=description):
                provider, _api = self.provider(
                    api_response(200, issue(description=description))
                )
                response = provider.invoke(request(self.socket_path, "work.get"))
                self.assertEqual(response.outcome, "denied")

    def test_adf_plain_text_hard_break_and_node_count_are_bounded(self) -> None:
        provider, _api = self.provider(
            api_response(200, issue(description="Plain Jira description"))
        )
        response = provider.invoke(request(self.socket_path, "work.get"))
        assert response.receipt is not None
        assert isinstance(response.receipt.result, dict)
        self.assertEqual(response.receipt.result["brief"], "Plain Jira description")

        hard_break = {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": "first"},
                        {"type": "hardBreak"},
                        {"type": "text", "text": "second"},
                    ],
                }
            ],
        }
        provider, _api = self.provider(api_response(200, issue(description=hard_break)))
        response = provider.invoke(request(self.socket_path, "work.get"))
        assert response.receipt is not None
        assert isinstance(response.receipt.result, dict)
        self.assertEqual(response.receipt.result["brief"], "first\nsecond")

        too_many_nodes = {
            "type": "doc",
            "version": 1,
            "content": [{"type": "hardBreak"} for _ in range(4097)],
        }
        provider, _api = self.provider(
            api_response(200, issue(description=too_many_nodes))
        )
        response = provider.invoke(request(self.socket_path, "work.get"))
        self.assertEqual(response.outcome, "denied")

    def test_adf_exact_structural_limits_are_accepted(self) -> None:
        exact_nodes = {
            "type": "doc",
            "version": 1,
            "content": [{"type": "hardBreak"} for _ in range(4096)],
        }
        provider, _api = self.provider(
            api_response(200, issue(description=exact_nodes))
        )
        response = provider.invoke(request(self.socket_path, "work.get"))
        self.assertEqual(response.outcome, "succeeded")

        exact_depth: dict[str, Any] = {"type": "text", "text": "bottom"}
        for _ in range(63):
            exact_depth = {"type": "panel", "content": [exact_depth]}
        provider, _api = self.provider(
            api_response(
                200,
                issue(
                    description={
                        "type": "doc",
                        "version": 1,
                        "content": [exact_depth],
                    }
                ),
            )
        )
        response = provider.invoke(request(self.socket_path, "work.get"))
        self.assertEqual(response.outcome, "succeeded")
        assert response.receipt is not None
        assert isinstance(response.receipt.result, dict)
        self.assertEqual(response.receipt.result["brief"], "bottom")

        exact_bytes = {
            "type": "doc",
            "version": 1,
            "content": [{"type": "text", "text": "x" * 65536}],
        }
        provider, _api = self.provider(
            api_response(200, issue(description=exact_bytes))
        )
        response = provider.invoke(request(self.socket_path, "work.get"))
        self.assertEqual(response.outcome, "succeeded")

    def test_adf_accounts_for_cumulative_text_and_block_separators(self) -> None:
        oversized_after_separator = {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": "x" * 32768},
                        {"type": "text", "text": "y" * 32768},
                    ],
                }
            ],
        }
        provider, _api = self.provider(
            api_response(200, issue(description=oversized_after_separator))
        )
        response = provider.invoke(request(self.socket_path, "work.get"))
        self.assertEqual(response.outcome, "denied")

    def test_adf_preserves_inline_text_and_bounds_node_types(self) -> None:
        inline_text = {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": "left"},
                        {"type": "text", "text": "right"},
                    ],
                }
            ],
        }
        provider, _api = self.provider(
            api_response(200, issue(description=inline_text))
        )
        response = provider.invoke(request(self.socket_path, "work.get"))
        assert response.receipt is not None
        assert isinstance(response.receipt.result, dict)
        self.assertEqual(response.receipt.result["brief"], "leftright")

        oversized_node_type = {
            "type": "doc",
            "version": 1,
            "content": [{"type": "x" * 65}],
        }
        provider, _api = self.provider(
            api_response(200, issue(description=oversized_node_type))
        )
        response = provider.invoke(request(self.socket_path, "work.get"))
        self.assertEqual(response.outcome, "denied")

    def test_malformed_issue_metadata_is_rejected_without_raw_payload(self) -> None:
        malformed = (
            [],
            {**issue(), "fields": []},
            issue(title=""),
            issue() | {"fields": {**issue()["fields"], "updated": "not-a-time"}},
            issue() | {"fields": {**issue()["fields"], "labels": "not-a-list"}},
            issue() | {"fields": {**issue()["fields"], "labels": ["same", "same"]}},
            issue()
            | {
                "fields": {
                    **issue()["fields"],
                    "assignee": {"displayName": "No stable account"},
                }
            },
        )
        for value in malformed:
            with self.subTest(value=value):
                provider, _api = self.provider(api_response(200, value))
                response = provider.invoke(request(self.socket_path, "work.get"))
                self.assertEqual(response.outcome, "denied")

    def test_provider_rejects_invalid_construction_and_transport_response(self) -> None:
        with self.assertRaises(JiraProviderError):
            JiraProvider("not-a-spec", FakeJiraAPI([]))  # type: ignore[arg-type]
        unsupported = sidecar_spec(self.socket_path).to_dict()
        unsupported["capabilities"].append(
            {"name": "work.extension", "version": 1, "mode": "read"}
        )
        with self.assertRaises(JiraProviderError):
            JiraProvider(ServiceSidecarSpec.from_mapping(unsupported), FakeJiraAPI([]))
        with self.assertRaises(JiraProviderError):
            JiraProvider(sidecar_spec(self.socket_path), object())  # type: ignore[arg-type]
        with self.assertRaises(JiraProviderError):
            JiraProvider(
                sidecar_spec(self.socket_path),
                FakeJiraAPI([]),
                state_targets=[],  # type: ignore[arg-type]
            )
        with self.assertRaises(JiraProviderError):
            JiraProvider(
                sidecar_spec(self.socket_path),
                FakeJiraAPI([]),
                state_targets={1: {"done": "10001"}},  # type: ignore[dict-item]
            )

        provider, _api = self.provider(object())  # type: ignore[arg-type]
        response = provider.invoke(request(self.socket_path, "work.get"))
        self.assertEqual(response.outcome, "retryable")
        with self.assertRaises(JiraProviderError):
            provider.invoke("not-a-request")  # type: ignore[arg-type]

    def test_authority_rejects_wrong_service_resource_mode_and_tampering(self) -> None:
        base = sidecar_spec(self.socket_path).to_dict()
        mutations = []
        wrong_service = {
            **base,
            "mappings": [{**base["mappings"][0], "service": "github"}],
        }
        mutations.append(wrong_service)
        wrong_resource = {
            **base,
            "mappings": [
                {
                    **base["mappings"][0],
                    "resource": {"kind": "repository", "external_id": "acme/widgets"},
                }
            ],
        }
        mutations.append(wrong_resource)
        wrong_mode = {
            **base,
            "capabilities": [
                {"name": "work.get", "version": 1, "mode": "effect"},
                base["capabilities"][1],
            ],
        }
        for raw in mutations:
            with self.subTest(raw=raw), self.assertRaises(JiraProviderError):
                JiraProvider(ServiceSidecarSpec.from_mapping(raw), FakeJiraAPI([]))
        with self.assertRaises(ValidationError):
            ServiceSidecarSpec.from_mapping(wrong_mode)

        provider, api = self.provider()
        original = request(self.socket_path, "work.get")
        tampered = replace(original, deployment_sha256="f" * 64)
        response = provider.invoke(tampered)
        self.assertEqual(response.outcome, "denied")
        self.assertEqual(api.calls, [])

    def test_response_value_objects_reject_invalid_accounting(self) -> None:
        invalid = (
            {
                "status": True,
                "body": {},
                "response_bytes": 0,
                "retry_after_seconds": None,
            },
            {
                "status": 99,
                "body": {},
                "response_bytes": 0,
                "retry_after_seconds": None,
            },
            {
                "status": 200,
                "body": {},
                "response_bytes": True,
                "retry_after_seconds": None,
            },
            {
                "status": 200,
                "body": {},
                "response_bytes": -1,
                "retry_after_seconds": None,
            },
            {"status": 429, "body": {}, "response_bytes": 0, "retry_after_seconds": 0},
        )
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(JiraProviderError):
                JiraAPIResponse(**values)

    def test_work_state_is_idempotent_when_target_status_is_already_observed(
        self,
    ) -> None:
        provider, api = self.state_provider(
            api_response(200, status_issue(status_id="10001"))
        )

        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.provider_status, 200)
        assert response.receipt is not None
        self.assertEqual(
            response.receipt.result,
            {
                "desired_state": "done",
                "object_id": "ABC-42",
                "schema_version": 1,
                "state": "done",
            },
        )
        self.assertEqual(
            response.receipt.evidence_sha256,
            content_sha256(
                canonical_json({"kind": "work-state", **response.receipt.result})
            ),
        )
        self.assertEqual(len(api.calls), 1)
        self.assertEqual(api.calls[0]["method"], "GET")
        self.assertEqual(
            api.calls[0]["endpoint"],
            "/rest/api/3/issue/ABC-42?fields=status",
        )

    def test_work_state_discovers_one_transition_applies_and_observes_it(self) -> None:
        transitions = {
            "transitions": [
                {"id": "31", "name": "Finish", "to": {"id": "10001"}},
                {"id": "41", "name": "Pause", "to": {"id": "10002"}},
            ]
        }
        provider, api = self.state_provider(
            api_response(200, status_issue()),
            api_response(200, transitions),
            api_response(204, None),
            api_response(200, status_issue(status_id="10001")),
        )

        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(
            [(call["method"], call["endpoint"], call["body"]) for call in api.calls],
            [
                ("GET", "/rest/api/3/issue/ABC-42?fields=status", None),
                ("GET", "/rest/api/3/issue/ABC-42/transitions", None),
                (
                    "POST",
                    "/rest/api/3/issue/ABC-42/transitions",
                    {"transition": {"id": "31"}},
                ),
                ("GET", "/rest/api/3/issue/ABC-42?fields=status", None),
            ],
        )
        serialized = canonical_json(response.to_dict())
        self.assertNotIn('"31"', serialized)
        self.assertNotIn("10001", serialized)
        self.assertNotIn("Finish", serialized)

    def test_work_state_reconcile_observes_without_repeating_the_effect(self) -> None:
        provider, api = self.state_provider(
            api_response(200, status_issue(status_id="10001"))
        )
        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
                phase="reconcile",
            )
        )
        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(len(api.calls), 1)

        provider, api = self.state_provider(api_response(200, status_issue()))
        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
                phase="reconcile",
            )
        )
        self.assertEqual(response.outcome, "not_found")
        self.assertEqual(response.error_code, "jira-state-not-observed")
        self.assertEqual(response.provider_status, 200)
        self.assertEqual(len(api.calls), 1)

    def test_work_state_lost_response_and_failed_observation_are_uncertain(
        self,
    ) -> None:
        transitions = {"transitions": [{"id": "31", "to": {"id": "10001"}}]}
        cases: tuple[
            tuple[tuple[JiraAPIResponse | BaseException, ...], int | None], ...
        ] = (
            (
                (
                    api_response(200, status_issue()),
                    api_response(200, transitions),
                    JiraTransportError("lost transition response"),
                ),
                None,
            ),
            (
                (
                    api_response(200, status_issue()),
                    api_response(200, transitions),
                    api_response(204, None),
                    JiraTransportError("lost observation response"),
                ),
                None,
            ),
            (
                (
                    api_response(200, status_issue()),
                    api_response(200, transitions),
                    api_response(204, None),
                    api_response(200, status_issue(status_id="10002")),
                ),
                200,
            ),
        )
        for responses, provider_status in cases:
            with self.subTest(responses=responses):
                provider, _api = self.state_provider(*responses)
                response = provider.invoke(
                    request(
                        self.socket_path,
                        "work.state",
                        parameters={"desired_state": "done"},
                    )
                )
                self.assertEqual(response.outcome, "uncertain")
                self.assertEqual(response.error_code, "jira-state-ambiguous")
                self.assertEqual(response.provider_status, provider_status)

    def test_work_state_requires_one_available_transition_before_dispatch(self) -> None:
        transition_pages = (
            {"transitions": []},
            {
                "transitions": [
                    {"id": "31", "to": {"id": "10001"}},
                    {"id": "32", "to": {"id": "10001"}},
                ]
            },
        )
        for transitions in transition_pages:
            with self.subTest(transitions=transitions):
                provider, api = self.state_provider(
                    api_response(200, status_issue()),
                    api_response(200, transitions),
                )
                response = provider.invoke(
                    request(
                        self.socket_path,
                        "work.state",
                        parameters={"desired_state": "done"},
                    )
                )
                self.assertEqual(response.outcome, "conflict")
                self.assertEqual(response.error_code, "jira-transition-unavailable")
                self.assertEqual(response.provider_status, 200)
                self.assertEqual([call["method"] for call in api.calls], ["GET", "GET"])

    def test_work_state_bounds_transition_documents_and_upstream_failures(self) -> None:
        malformed_transitions: tuple[object, ...] = (
            [],
            {"transitions": "not-a-list"},
            {"transitions": [{}]},
            {"transitions": [{"id": "x", "to": {"id": "10001"}}]},
            {"transitions": [{"id": "31", "to": {"id": 10001}}]},
            {
                "transitions": [
                    {"id": str(index + 1), "to": {"id": "10002"}}
                    for index in range(101)
                ]
            },
        )
        for transitions in malformed_transitions:
            with self.subTest(transitions=transitions):
                provider, _api = self.state_provider(
                    api_response(200, status_issue()),
                    api_response(200, transitions),
                )
                response = provider.invoke(
                    request(
                        self.socket_path,
                        "work.state",
                        parameters={"desired_state": "done"},
                    )
                )
                self.assertEqual(response.outcome, "denied")

        provider, _api = self.state_provider(
            api_response(503, {"private": "status failure"})
        )
        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
            )
        )
        self.assertEqual(response.outcome, "retryable")

        provider, _api = self.state_provider(
            api_response(200, status_issue(status_id=1))  # type: ignore[arg-type]
        )
        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
            )
        )
        self.assertEqual(response.outcome, "denied")

    def test_work_state_accepts_exactly_one_hundred_available_transitions(
        self,
    ) -> None:
        transitions = {
            "transitions": [
                {"id": str(index + 1), "to": {"id": "10002"}} for index in range(99)
            ]
            + [{"id": "100", "to": {"id": "10001"}}]
        }
        provider, _api = self.state_provider(
            api_response(200, status_issue()),
            api_response(200, transitions),
            api_response(204, None),
            api_response(200, status_issue(status_id="10001")),
        )

        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
            )
        )

        self.assertEqual(response.outcome, "succeeded")

        provider, _api = self.state_provider(
            api_response(200, status_issue()),
            api_response(503, {"private": "transition discovery failure"}),
        )
        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
            )
        )
        self.assertEqual(response.outcome, "retryable")

    def test_work_state_treats_unexpected_success_and_server_failure_as_uncertain(
        self,
    ) -> None:
        transitions = {"transitions": [{"id": "31", "to": {"id": "10001"}}]}
        for status in (200, 500, 503):
            with self.subTest(status=status):
                provider, _api = self.state_provider(
                    api_response(200, status_issue()),
                    api_response(200, transitions),
                    api_response(status, {}),
                )
                response = provider.invoke(
                    request(
                        self.socket_path,
                        "work.state",
                        parameters={"desired_state": "done"},
                    )
                )
                self.assertEqual(response.outcome, "uncertain")
                self.assertEqual(response.error_code, "jira-state-ambiguous")
                self.assertEqual(response.provider_status, status)

        provider, _api = self.state_provider(
            api_response(200, status_issue()),
            api_response(200, transitions),
            api_response(204, None),
            api_response(503, {"private": "observation failure"}),
        )
        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
            )
        )
        self.assertEqual(response.outcome, "uncertain")
        self.assertEqual(response.error_code, "jira-state-ambiguous")
        self.assertEqual(response.provider_status, 503)

    def test_work_state_passes_remaining_response_budget_to_each_call(self) -> None:
        initial = api_response(200, status_issue())
        transitions = api_response(
            200,
            {"transitions": [{"id": "31", "to": {"id": "10001"}}]},
        )
        applied = api_response(204, None, response_bytes=0)
        observed = api_response(200, status_issue(status_id="10001"))
        provider, api = self.state_provider(
            initial,
            transitions,
            applied,
            observed,
        )

        response = provider.invoke(
            request(
                self.socket_path,
                "work.state",
                parameters={"desired_state": "done"},
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        total = 512 * 1024
        self.assertEqual(
            [call["response_limit_bytes"] for call in api.calls],
            [
                total,
                total - initial.response_bytes,
                total - initial.response_bytes - transitions.response_bytes,
                total - initial.response_bytes - transitions.response_bytes,
            ],
        )

    def test_work_state_rejects_unmapped_state_and_requires_private_policy(
        self,
    ) -> None:
        for parameters in ({}, {"desired_state": "open"}, {"desired_state": 1}):
            with self.subTest(parameters=parameters):
                provider, api = self.state_provider()
                response = provider.invoke(
                    request(self.socket_path, "work.state", parameters=parameters)
                )
                self.assertEqual(response.outcome, "denied")
                self.assertEqual(api.calls, [])

        with self.assertRaises(JiraProviderError):
            JiraProvider(
                sidecar_spec(self.socket_path, include_state=True),
                FakeJiraAPI([]),
            )
        with self.assertRaises(JiraProviderError):
            JiraProvider(
                sidecar_spec(self.socket_path, include_state=True),
                FakeJiraAPI([]),
                state_targets={"ABC": {"done": "not-a-status-id"}},
            )


if __name__ == "__main__":
    unittest.main()
