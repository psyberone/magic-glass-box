from __future__ import annotations

import json
import tempfile
import unittest
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from magic_glass_box.domain import WorkItemEnvelope, canonical_json, content_sha256
from magic_glass_box.github_provider import (
    GitHubAPIResponse,
    GitHubProvider,
    GitHubProviderError,
    GitHubTransportError,
    github_remote_identity,
)
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationSpec,
    ServiceSidecarSpec,
)

BASE = "a" * 40
HEAD = "b" * 40
MERGED = "c" * 40
CONNECTOR_ID = "ssc_" + "1" * 32
DEPLOYMENT = "d" * 64
SCOPE = "e" * 64
DEADLINE = "2099-01-01T00:00:00.000000+00:00"


def sidecar_spec(socket_path: Path) -> ServiceSidecarSpec:
    return ServiceSidecarSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_protocol_version": 1,
            "deployment_sha256": DEPLOYMENT,
            "transport": {"kind": "unix", "socket_path": str(socket_path)},
            "capabilities": [
                {"name": "work.get", "version": 1, "mode": "read"},
                {"name": "work.list", "version": 1, "mode": "read"},
                {"name": "change.get", "version": 1, "mode": "read"},
                {"name": "change.list", "version": 1, "mode": "read"},
                {"name": "change.open", "version": 1, "mode": "effect"},
                {"name": "checks.read", "version": 1, "mode": "read"},
                {"name": "landing.invoke", "version": 1, "mode": "effect"},
            ],
            "mappings": [
                {
                    "name": "primary",
                    "service": "github",
                    "host": "github.com",
                    "repository": "acme/widgets",
                    "account": "mgb-bot",
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
    phase: str = "invoke",
    resource_id: str = "42",
    expected_revision: str | None = BASE,
    desired_revision: str | None = HEAD,
    object_format: str | None = "sha1",
    parameters: Mapping[str, Any] | None = None,
) -> ServiceOperationRequest:
    modes = {
        "work.get": "read",
        "work.list": "read",
        "change.get": "read",
        "change.list": "read",
        "change.open": "effect",
        "checks.read": "read",
        "landing.invoke": "effect",
        "refs.publish": "effect",
    }
    operation = ServiceOperationSpec.from_mapping(
        {
            "schema_version": 1,
            "connector_name": "github-main",
            "deployment_sha256": DEPLOYMENT,
            "mapping": sidecar_spec(socket_path).mappings[0].to_dict(),
            "capability": {
                "name": capability,
                "version": 1,
                "mode": modes[capability],
            },
            "idempotency_key": (
                f"test:{capability}:{content_sha256(resource_id)[:16]}"
            ),
            "target": {
                "repository": "acme/widgets",
                "resource_id": resource_id,
                "object_format": object_format,
                "expected_revision": expected_revision,
                "desired_revision": desired_revision,
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
        connector_protocol_version=1,
        operation=operation,
        phase=phase,
        deadline=DEADLINE,
    )


def pull(
    *,
    state: str = "open",
    merged: bool = False,
    base_sha: str = BASE,
    head_sha: str = HEAD,
) -> dict[str, Any]:
    return {
        "number": 42,
        "state": state,
        "merged": merged,
        "merged_at": "2098-12-31T23:59:00Z" if merged else None,
        "merge_commit_sha": MERGED if merged else None,
        "head": {"sha": head_sha, "ref": "mgb/op-42", "label": "acme:mgb/op-42"},
        "base": {"sha": base_sha, "ref": "main"},
    }


def ref(revision: str, name: str = "main") -> dict[str, Any]:
    return {
        "ref": f"refs/heads/{name}",
        "object": {"type": "commit", "sha": revision},
    }


def issue(
    *,
    number: int = 42,
    state: str = "open",
    title: str = "Bounded issue",
    body: str | None = "Implement the bounded behavior.",
) -> dict[str, Any]:
    return {
        "number": number,
        "state": state,
        "title": title,
        "body": body,
        "updated_at": "2098-12-31T23:00:00Z",
        "labels": [{"name": "agent-ready"}],
        "assignees": [{"login": "mgb-bot"}],
    }


def change(
    *, number: int = 42, state: str = "open", merged: bool = False
) -> dict[str, Any]:
    value = pull(state=state, merged=merged)
    value.update(
        {
            "title": "Candidate 42",
            "draft": False,
            "updated_at": "2098-12-31T23:00:00Z",
        }
    )
    return value


class FakeGitHubAPI:
    def __init__(self, responses: list[GitHubAPIResponse | BaseException]) -> None:
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
    ) -> GitHubAPIResponse:
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


class FakeGitPublisher:
    def __init__(self, failure: BaseException | None = None) -> None:
        self.failure = failure
        self.calls: list[dict[str, str]] = []

    def publish(
        self,
        *,
        source_repository: str,
        source_ref: str,
        desired_revision: str,
        host: str,
        repository: str,
        target_ref: str,
        deadline: str,
    ) -> None:
        self.calls.append(
            {
                "deadline": deadline,
                "desired_revision": desired_revision,
                "host": host,
                "repository": repository,
                "source_ref": source_ref,
                "source_repository": source_repository,
                "target_ref": target_ref,
            }
        )
        if self.failure is not None:
            raise self.failure


def api_response(
    status: int, body: object, *, retry_after_seconds: int | None = None
) -> GitHubAPIResponse:
    return GitHubAPIResponse(
        status=status,
        body=body,
        response_bytes=128,
        retry_after_seconds=retry_after_seconds,
    )


def expected_api_call(
    method: str,
    endpoint: str,
    *,
    body: Mapping[str, Any] | None = None,
    response_limit_bytes: int = 512 * 1024,
) -> dict[str, Any]:
    return {
        "method": method,
        "host": "github.com",
        "endpoint": endpoint,
        "body": None if body is None else dict(body),
        "deadline": DEADLINE,
        "response_limit_bytes": response_limit_bytes,
    }


def change_summary(*, merged: bool = False) -> dict[str, Any]:
    return {
        "base_ref": "main",
        "draft": False,
        "external_id": "github.com/acme/widgets/pull/42",
        "head_ref": "mgb/op-42",
        "head_revision": HEAD,
        "merged": merged,
        "number": 42,
        "state": "open",
        "title": "Candidate 42",
        "updated_at": "2098-12-31T23:00:00.000000Z",
    }


class GitHubProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.socket_path = Path(self.temporary.name) / "github.sock"
        self.spec = sidecar_spec(self.socket_path)

    def test_github_response_and_remote_identity_contracts_reject_bad_values(
        self,
    ) -> None:
        self.assertEqual(
            github_remote_identity("github.example", "acme/widgets", 42),
            "github:github.example/acme/widgets/pull/42",
        )
        for kwargs in (
            {"status": True, "response_bytes": 0, "retry_after_seconds": None},
            {"status": 99, "response_bytes": 0, "retry_after_seconds": None},
            {"status": 200, "response_bytes": -1, "retry_after_seconds": None},
            {"status": 200, "response_bytes": 0, "retry_after_seconds": 0},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(GitHubProviderError):
                GitHubAPIResponse(body={}, **kwargs)
        for pull_number in (True, 0, 10_000_000_000):
            with (
                self.subTest(pull_number=pull_number),
                self.assertRaises(GitHubProviderError),
            ):
                github_remote_identity("github.com", "acme/widgets", pull_number)

    def test_issue_label_and_assignee_limits_are_exact(self) -> None:
        provider = GitHubProvider(self.spec, FakeGitHubAPI([]))
        labels = [{"name": f"label-{index}"} for index in range(64)]
        assignees = [{"login": f"user-{index}"} for index in range(64)]
        self.assertEqual(
            provider._issue_labels(labels),
            tuple(f"label-{index}" for index in range(64)),
        )
        self.assertEqual(
            provider._issue_assignees(assignees),
            tuple(f"user-{index}" for index in range(64)),
        )
        for parser, values in (
            (provider._issue_labels, labels + [{"name": "overflow"}]),
            (provider._issue_labels, [{"name": "same"}, {"name": "same"}]),
            (provider._issue_assignees, assignees + [{"login": "overflow"}]),
            (
                provider._issue_assignees,
                [{"login": "same"}, {"login": "same"}],
            ),
        ):
            with (
                self.subTest(parser=parser.__name__, count=len(values)),
                self.assertRaises(GitHubProviderError),
            ):
                parser(values)

    def test_provider_requires_supported_transport_and_publication_actuator(
        self,
    ) -> None:
        unsupported = self.spec.to_dict()
        unsupported["capabilities"].append(
            {"name": "work.comment", "version": 1, "mode": "effect"}
        )
        with self.assertRaisesRegex(GitHubProviderError, "unsupported"):
            GitHubProvider(
                ServiceSidecarSpec.from_mapping(unsupported), FakeGitHubAPI([])
            )
        with self.assertRaisesRegex(GitHubProviderError, "transport"):
            GitHubProvider(self.spec, object())  # type: ignore[arg-type]
        with self.assertRaisesRegex(GitHubProviderError, "publisher"):
            GitHubProvider(self.publishing_spec(), FakeGitHubAPI([]))

    def test_published_github_examples_form_exact_compatible_authority(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        spec = ServiceSidecarSpec.from_mapping(
            json.loads(
                (repository / "examples/github-service-sidecar.json").read_text()
            )
        )
        for name in (
            "github-work-get",
            "github-work-list",
            "github-change-get",
            "github-change-list",
            "github-ref-publication",
            "github-change-open",
            "github-checks-read",
        ):
            with self.subTest(name=name):
                operation = ServiceOperationSpec.from_mapping(
                    json.loads((repository / f"examples/{name}.json").read_text())
                )
                spec.validate_operation("github-main", operation)

    def publishing_spec(self) -> ServiceSidecarSpec:
        value = self.spec.to_dict()
        value["capabilities"].append(
            {"name": "refs.publish", "version": 1, "mode": "effect"}
        )
        return ServiceSidecarSpec.from_mapping(value)

    def test_work_get_returns_an_import_ready_normalized_envelope(self) -> None:
        api = FakeGitHubAPI([api_response(200, issue())])

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "work.get",
                resource_id="42",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(
            response.receipt.remote_identity,
            "github:github.com/acme/widgets/issues/42",
        )
        self.assertEqual(
            response.receipt.result,
            {
                "acceptance_criteria": [],
                "brief": "Implement the bounded behavior.",
                "constraints": [],
                "context": [],
                "external_id": "github.com/acme/widgets/issues/42",
                "labels": ["agent-ready"],
                "priority": "normal",
                "schema_version": 1,
                "source": "github",
                "source_updated_at": "2098-12-31T23:00:00.000000Z",
                "title": "Bounded issue",
            },
        )
        self.assertEqual(
            api.calls,
            [expected_api_call("GET", "repos/acme/widgets/issues/42")],
        )
        self.assertEqual(response.request_bytes, 0)
        self.assertEqual(response.response_bytes, 128)
        self.assertEqual(
            response.receipt.evidence_sha256,
            content_sha256(
                canonical_json(
                    {
                        "kind": "work",
                        "state": "open",
                        **response.receipt.result,
                    }
                )
            ),
        )
        self.assertEqual(
            WorkItemEnvelope.from_mapping(response.receipt.result).title,
            "Bounded issue",
        )

    def test_work_get_uses_title_when_issue_body_is_empty_or_absent(self) -> None:
        for body in (None, "", "   "):
            with self.subTest(body=body):
                response = GitHubProvider(
                    self.spec, FakeGitHubAPI([api_response(200, issue(body=body))])
                ).invoke(
                    request(
                        self.socket_path,
                        "work.get",
                        resource_id="42",
                        expected_revision=None,
                        desired_revision=None,
                        object_format=None,
                    )
                )
                self.assertEqual(response.outcome, "succeeded")
                self.assertEqual(response.receipt.result["brief"], "Bounded issue")

    def test_work_list_is_bounded_paginated_and_excludes_pull_requests(self) -> None:
        pull_as_issue = issue(number=43, title="A pull request")
        pull_as_issue["pull_request"] = {"url": "not-retained"}
        api = FakeGitHubAPI([api_response(200, [issue(), pull_as_issue])])

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "work.list",
                resource_id="issues",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
                parameters={"cursor": 1, "limit": 2, "state": "open"},
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(
            response.receipt.result,
            {
                "items": [
                    {
                        "assignees": ["mgb-bot"],
                        "external_id": "github.com/acme/widgets/issues/42",
                        "labels": ["agent-ready"],
                        "object_id": "42",
                        "source_updated_at": "2098-12-31T23:00:00.000000Z",
                        "title": "Bounded issue",
                    }
                ],
                "schema_version": 1,
                "truncated": True,
            },
        )
        self.assertNotIn("body", str(response.receipt.result))
        self.assertEqual(
            api.calls,
            [
                expected_api_call(
                    "GET",
                    "repos/acme/widgets/issues?"
                    "state=open&sort=updated&direction=desc&per_page=2&page=1",
                )
            ],
        )
        self.assertEqual(response.request_bytes, 0)
        self.assertEqual(response.response_bytes, 128)
        self.assertEqual(
            response.receipt.evidence_sha256,
            content_sha256(
                canonical_json({"kind": "work-list", **response.receipt.result})
            ),
        )

    def test_change_get_and_list_return_normalized_metadata(self) -> None:
        get_api = FakeGitHubAPI([api_response(200, change())])
        get_response = GitHubProvider(self.spec, get_api).invoke(
            request(
                self.socket_path,
                "change.get",
                resource_id="42",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
            )
        )
        self.assertEqual(get_response.outcome, "succeeded")
        self.assertEqual(get_response.provider_status, 200)
        self.assertEqual(get_response.receipt.result, change_summary())
        self.assertEqual(
            get_response.receipt.remote_identity,
            "github:github.com/acme/widgets/pull/42",
        )
        self.assertIsNone(get_response.receipt.observed_revision)
        self.assertEqual(get_response.receipt.closed_references, ())
        get_response.validate_correlation(
            request(
                self.socket_path,
                "change.get",
                resource_id="42",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
            )
        )
        self.assertEqual(
            get_api.calls,
            [expected_api_call("GET", "repos/acme/widgets/pulls/42")],
        )
        self.assertEqual(
            get_response.receipt.evidence_sha256,
            content_sha256(
                canonical_json({"kind": "change-read", **get_response.receipt.result})
            ),
        )

        list_api = FakeGitHubAPI([api_response(200, [change()])])
        list_response = GitHubProvider(self.spec, list_api).invoke(
            request(
                self.socket_path,
                "change.list",
                resource_id="changes",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
                parameters={"cursor": 1, "limit": 100, "state": "all"},
            )
        )
        self.assertEqual(list_response.outcome, "succeeded")
        self.assertEqual(
            list_response.receipt.result,
            {"items": [change_summary()], "next_cursor": None},
        )

        self.assertEqual(
            list_response.receipt.remote_identity,
            "github:github.com/acme/widgets/pulls",
        )
        self.assertIsNone(list_response.receipt.observed_revision)
        self.assertEqual(list_response.receipt.closed_references, ())
        self.assertEqual(
            list_response.receipt.evidence_sha256,
            content_sha256(
                canonical_json({"kind": "change-list", **list_response.receipt.result})
            ),
        )
        self.assertEqual(
            list_api.calls,
            [
                expected_api_call(
                    "GET",
                    "repos/acme/widgets/pulls?"
                    "state=all&sort=updated&direction=desc&per_page=100&page=1",
                )
            ],
        )

    def test_change_get_requires_exact_merged_state(self) -> None:
        document = change()
        document.pop("merged")
        response = GitHubProvider(
            self.spec, FakeGitHubAPI([api_response(200, document)])
        ).invoke(
            request(
                self.socket_path,
                "change.get",
                resource_id="42",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
            )
        )

        self.assertEqual(response.outcome, "denied")

    def test_read_targets_pagination_and_provider_documents_fail_closed(self) -> None:
        invalid_operations = (
            request(
                self.socket_path,
                "work.list",
                resource_id="issues",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
                parameters={"cursor": 0, "limit": 10, "state": "open"},
            ),
            request(
                self.socket_path,
                "work.list",
                resource_id="issues",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
                parameters={"cursor": 1, "limit": 101, "state": "open"},
            ),
            request(
                self.socket_path,
                "change.list",
                resource_id="wrong",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
                parameters={"cursor": 1, "limit": 10, "state": "open"},
            ),
            request(
                self.socket_path,
                "change.list",
                resource_id="changes",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
                parameters={"cursor": 1, "limit": 10, "state": "unknown"},
            ),
        )
        for operation in invalid_operations:
            api = FakeGitHubAPI([])
            with self.subTest(capability=operation.capability.name):
                response = GitHubProvider(self.spec, api).invoke(operation)
                self.assertEqual(response.outcome, "denied")
                self.assertEqual(api.calls, [])

        exhausted = GitHubProvider(
            self.spec, FakeGitHubAPI([api_response(200, [issue()])])
        ).invoke(
            request(
                self.socket_path,
                "work.list",
                resource_id="issues",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
                parameters={
                    "cursor": 10_000,
                    "limit": 1,
                    "state": "open",
                },
            )
        )
        self.assertEqual(exhausted.outcome, "denied")

        pull_issue = issue()
        pull_issue["pull_request"] = {}
        malformed_cases = (
            (
                "work.get",
                pull_issue,
                {"resource_id": "42", "parameters": {}},
            ),
            (
                "work.list",
                [issue(number=index + 1) for index in range(2)],
                {
                    "resource_id": "issues",
                    "parameters": {"cursor": 1, "limit": 1, "state": "open"},
                },
            ),
            (
                "change.get",
                {**change(), "draft": "false"},
                {"resource_id": "42", "parameters": {}},
            ),
        )
        for capability, document, values in malformed_cases:
            with self.subTest(capability=capability):
                response = GitHubProvider(
                    self.spec, FakeGitHubAPI([api_response(200, document)])
                ).invoke(
                    request(
                        self.socket_path,
                        capability,
                        expected_revision=None,
                        desired_revision=None,
                        object_format=None,
                        **values,
                    )
                )
                self.assertEqual(response.outcome, "denied")

    def test_ref_publication_pushes_only_an_absent_exact_candidate_ref(self) -> None:
        source = "/private/mgb/candidates/projects/boxp_test/repository.git"
        source_ref = f"refs/mgb/candidates/{HEAD}"
        target_ref = "refs/heads/mgb/cnd-" + "3" * 32
        api = FakeGitHubAPI(
            [
                api_response(404, {"message": "missing"}),
                api_response(200, ref(HEAD, target_ref.removeprefix("refs/heads/"))),
            ]
        )
        publisher = FakeGitPublisher()

        response = GitHubProvider(
            self.publishing_spec(), api, publisher=publisher
        ).invoke(
            request(
                self.socket_path,
                "refs.publish",
                resource_id=target_ref,
                expected_revision=None,
                parameters={"source_repository": source},
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.receipt.observed_revision, HEAD)
        self.assertEqual(
            publisher.calls,
            [
                {
                    "deadline": DEADLINE,
                    "desired_revision": HEAD,
                    "host": "github.com",
                    "repository": "acme/widgets",
                    "source_ref": source_ref,
                    "source_repository": source,
                    "target_ref": target_ref,
                }
            ],
        )

    def test_ref_publication_reconcile_observes_success_without_repush(self) -> None:
        target_ref = "refs/heads/mgb/cnd-" + "3" * 32
        api = FakeGitHubAPI(
            [api_response(200, ref(HEAD, target_ref.removeprefix("refs/heads/")))]
        )
        publisher = FakeGitPublisher()

        response = GitHubProvider(
            self.publishing_spec(), api, publisher=publisher
        ).invoke(
            request(
                self.socket_path,
                "refs.publish",
                phase="reconcile",
                resource_id=target_ref,
                expected_revision=None,
                parameters={
                    "source_repository": "/private/mgb/candidates/repository.git",
                },
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(publisher.calls, [])

    def test_ref_publication_lost_response_becomes_uncertain_then_reconciles(
        self,
    ) -> None:
        target_ref = "refs/heads/mgb/cnd-" + "3" * 32
        source = "/private/mgb/candidates/repository.git"
        failed_publisher = FakeGitPublisher(
            GitHubTransportError("private push diagnostic", ambiguous=True)
        )
        uncertain = GitHubProvider(
            self.publishing_spec(),
            FakeGitHubAPI([api_response(404, {"message": "missing"})]),
            publisher=failed_publisher,
        ).invoke(
            request(
                self.socket_path,
                "refs.publish",
                resource_id=target_ref,
                expected_revision=None,
                parameters={"source_repository": source},
            )
        )

        self.assertEqual(uncertain.outcome, "uncertain")
        self.assertNotIn("private push diagnostic", str(uncertain.to_dict()))

        replacement = FakeGitPublisher()
        settled = GitHubProvider(
            self.publishing_spec(),
            FakeGitHubAPI(
                [api_response(200, ref(HEAD, target_ref.removeprefix("refs/heads/")))]
            ),
            publisher=replacement,
        ).invoke(
            request(
                self.socket_path,
                "refs.publish",
                phase="reconcile",
                resource_id=target_ref,
                expected_revision=None,
                parameters={"source_repository": source},
            )
        )

        self.assertEqual(settled.outcome, "succeeded")
        self.assertEqual(replacement.calls, [])

    def test_ref_publication_never_overwrites_or_repushes_a_missing_ref(self) -> None:
        target_ref = "refs/heads/mgb/cnd-" + "3" * 32
        publisher = FakeGitPublisher()
        conflict = GitHubProvider(
            self.publishing_spec(),
            FakeGitHubAPI(
                [
                    api_response(
                        200,
                        ref("f" * 40, target_ref.removeprefix("refs/heads/")),
                    )
                ]
            ),
            publisher=publisher,
        ).invoke(
            request(
                self.socket_path,
                "refs.publish",
                resource_id=target_ref,
                expected_revision=None,
                parameters={"source_repository": "/private/candidates/repository.git"},
            )
        )
        self.assertEqual(conflict.outcome, "conflict")

        missing = GitHubProvider(
            self.publishing_spec(),
            FakeGitHubAPI([api_response(404, {"message": "missing"})]),
            publisher=publisher,
        ).invoke(
            request(
                self.socket_path,
                "refs.publish",
                phase="reconcile",
                resource_id=target_ref,
                expected_revision=None,
                parameters={"source_repository": "/private/candidates/repository.git"},
            )
        )
        self.assertEqual(missing.outcome, "not_found")
        self.assertEqual(publisher.calls, [])

    def test_landing_enforces_head_and_reports_actual_provider_revision(self) -> None:
        api = FakeGitHubAPI(
            [
                api_response(200, pull()),
                api_response(200, ref(BASE)),
                api_response(200, {"sha": MERGED, "merged": True, "message": "ok"}),
                api_response(200, pull(state="closed", merged=True)),
                api_response(200, ref(MERGED)),
            ]
        )
        provider = GitHubProvider(self.spec, api)

        response = provider.invoke(
            request(
                self.socket_path,
                "landing.invoke",
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "squash",
                },
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.receipt.observed_revision, MERGED)
        identity = github_remote_identity("github.com", "acme/widgets", 42)
        self.assertEqual(response.receipt.remote_identity, identity)
        self.assertEqual(response.receipt.closed_references, (identity,))
        self.assertEqual(api.calls[2]["body"], {"merge_method": "squash", "sha": HEAD})
        self.assertEqual(api.calls[2]["method"], "PUT")
        self.assertNotIn("expected_base", api.calls[2]["body"])

    def test_landing_precondition_drift_settles_without_false_success(self) -> None:
        drift = "f" * 40
        api = FakeGitHubAPI(
            [
                api_response(200, pull(base_sha=drift)),
                api_response(200, ref(drift)),
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "landing.invoke",
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "merge",
                },
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.receipt.observed_revision, drift)
        self.assertEqual(response.receipt.closed_references, ())
        self.assertEqual([call["method"] for call in api.calls], ["GET", "GET"])

    def test_landing_reconcile_discovers_lost_success_without_second_merge(
        self,
    ) -> None:
        api = FakeGitHubAPI(
            [
                api_response(200, pull(state="closed", merged=True)),
                api_response(200, ref(MERGED)),
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "landing.invoke",
                phase="reconcile",
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "rebase",
                },
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.receipt.observed_revision, MERGED)
        self.assertTrue(all(call["method"] == "GET" for call in api.calls))

    def test_change_open_is_deterministic_and_reconcile_does_not_duplicate(
        self,
    ) -> None:
        created = pull()
        created.pop("merged")
        api = FakeGitHubAPI(
            [
                api_response(200, []),
                api_response(200, ref(BASE)),
                api_response(200, ref(HEAD, "mgb/op-42")),
                api_response(201, created),
            ]
        )
        operation = request(
            self.socket_path,
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

        opened = GitHubProvider(self.spec, api).invoke(operation)

        self.assertEqual(opened.outcome, "succeeded")
        self.assertEqual(opened.provider_status, 201)
        self.assertEqual(opened.receipt.observed_revision, HEAD)
        self.assertEqual(api.calls[-1]["method"], "POST")
        self.assertEqual(api.calls[-1]["body"]["head"], "acme:mgb/op-42")

        reconcile_api = FakeGitHubAPI([api_response(200, [created])])
        reconciled = GitHubProvider(self.spec, reconcile_api).invoke(
            request(
                self.socket_path,
                "change.open",
                phase="reconcile",
                resource_id="refs/heads/mgb/op-42",
                parameters=operation.parameters,
            )
        )
        self.assertEqual(reconciled.outcome, "succeeded")
        self.assertEqual(reconciled.provider_status, 200)
        self.assertEqual(len(reconcile_api.calls), 1)

    def test_change_reconcile_reports_absence_without_creating(self) -> None:
        api = FakeGitHubAPI([api_response(200, [])])
        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "change.open",
                phase="reconcile",
                resource_id="refs/heads/mgb/op-42",
                parameters={
                    "base_ref": "refs/heads/main",
                    "body": "Bounded summary.",
                    "draft": True,
                    "head_ref": "refs/heads/mgb/op-42",
                    "title": "Candidate 42",
                },
            )
        )

        self.assertEqual(response.outcome, "not_found")
        self.assertEqual([call["method"] for call in api.calls], ["GET"])

    def test_checks_bind_exact_head_required_contexts_and_latest_approvals(
        self,
    ) -> None:
        api = FakeGitHubAPI(
            [
                api_response(200, pull()),
                api_response(
                    200,
                    {
                        "total_count": 1,
                        "check_runs": [
                            {
                                "name": "unit",
                                "status": "completed",
                                "conclusion": "success",
                            }
                        ],
                    },
                ),
                api_response(200, {"state": "success", "statuses": []}),
                api_response(
                    200,
                    [
                        {
                            "id": 1,
                            "state": "APPROVED",
                            "submitted_at": "2098-12-31T23:00:00Z",
                            "user": {"login": "reviewer"},
                        }
                    ],
                ),
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "checks.read",
                parameters={"required_approvals": 1, "required_checks": ["unit"]},
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.receipt.observed_revision, HEAD)
        self.assertEqual(response.receipt.closed_references, ())
        self.assertEqual(response.request_bytes, 0)
        self.assertEqual(response.response_bytes, 512)
        self.assertEqual(
            api.calls,
            [
                expected_api_call("GET", "repos/acme/widgets/pulls/42"),
                expected_api_call(
                    "GET",
                    "repos/acme/widgets/commits/"
                    f"{HEAD}/check-runs?filter=latest&per_page=100",
                    response_limit_bytes=512 * 1024 - 128,
                ),
                expected_api_call(
                    "GET",
                    f"repos/acme/widgets/commits/{HEAD}/status?per_page=100",
                    response_limit_bytes=512 * 1024 - 256,
                ),
                expected_api_call(
                    "GET",
                    "repos/acme/widgets/pulls/42/reviews?per_page=100",
                    response_limit_bytes=512 * 1024 - 384,
                ),
            ],
        )
        self.assertEqual(
            response.receipt.evidence_sha256,
            content_sha256(
                canonical_json(
                    {
                        "approved_by": ["reviewer"],
                        "head_revision": HEAD,
                        "kind": "checks",
                        "pull_number": 42,
                        "required_approvals": 1,
                        "required_checks": [{"conclusion": "success", "name": "unit"}],
                        "status": "success",
                    }
                )
            ),
        )

    def test_pending_checks_are_retryable_and_never_enter_reconciliation(self) -> None:
        api = FakeGitHubAPI(
            [
                api_response(200, pull()),
                api_response(
                    200,
                    {
                        "total_count": 1,
                        "check_runs": [
                            {
                                "name": "unit",
                                "status": "in_progress",
                                "conclusion": None,
                            }
                        ],
                    },
                ),
                api_response(200, {"state": "pending", "statuses": []}),
                api_response(200, []),
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "checks.read",
                parameters={"required_approvals": 0, "required_checks": ["unit"]},
            )
        )

        self.assertEqual(response.outcome, "retryable")
        self.assertEqual(response.error_code, "github-checks-pending")
        self.assertEqual(response.retry_after_seconds, 15)
        self.assertIsNone(response.receipt)

    def test_unrelated_legacy_status_does_not_block_required_check_run(self) -> None:
        api = FakeGitHubAPI(
            [
                api_response(200, pull()),
                api_response(
                    200,
                    {
                        "total_count": 1,
                        "check_runs": [
                            {
                                "name": "unit",
                                "status": "completed",
                                "conclusion": "success",
                            }
                        ],
                    },
                ),
                api_response(
                    200,
                    {
                        "state": "pending",
                        "total_count": 1,
                        "statuses": [{"context": "deploy", "state": "pending"}],
                    },
                ),
                api_response(200, []),
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "checks.read",
                parameters={"required_approvals": 0, "required_checks": ["unit"]},
            )
        )

        self.assertEqual(response.outcome, "succeeded")

    def test_duplicate_named_check_runs_fail_closed(self) -> None:
        api = FakeGitHubAPI(
            [
                api_response(200, pull()),
                api_response(
                    200,
                    {
                        "total_count": 2,
                        "check_runs": [
                            {
                                "name": "unit",
                                "status": "completed",
                                "conclusion": "success",
                            },
                            {
                                "name": "unit",
                                "status": "completed",
                                "conclusion": "failure",
                            },
                        ],
                    },
                ),
                api_response(200, {"state": "success", "statuses": []}),
                api_response(200, []),
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "checks.read",
                parameters={"required_approvals": 0, "required_checks": ["unit"]},
            )
        )

        self.assertEqual(response.outcome, "denied")
        self.assertEqual(response.error_code, "github-request-invalid")

    def test_full_pull_or_review_page_requires_bounded_exhaustion_proof(self) -> None:
        change_api = FakeGitHubAPI(
            [
                api_response(
                    200,
                    [pull(head_sha="f" * 40) for _index in range(100)],
                ),
                api_response(200, [pull()]),
            ]
        )
        change = GitHubProvider(self.spec, change_api).invoke(
            request(
                self.socket_path,
                "change.open",
                phase="reconcile",
                resource_id="refs/heads/mgb/op-42",
                parameters={
                    "base_ref": "refs/heads/main",
                    "body": "Bounded summary.",
                    "draft": True,
                    "head_ref": "refs/heads/mgb/op-42",
                    "title": "Candidate 42",
                },
            )
        )
        self.assertEqual(change.outcome, "denied")
        self.assertEqual(len(change_api.calls), 2)

        review = {
            "id": 1,
            "state": "COMMENTED",
            "submitted_at": "2098-12-31T23:00:00Z",
            "user": {"login": "reviewer"},
        }
        review_api = FakeGitHubAPI(
            [
                api_response(200, pull()),
                api_response(200, {"total_count": 0, "check_runs": []}),
                api_response(200, {"state": "success", "statuses": []}),
                api_response(200, [review for _index in range(100)]),
                api_response(200, [review]),
            ]
        )
        checks = GitHubProvider(self.spec, review_api).invoke(
            request(
                self.socket_path,
                "checks.read",
                parameters={"required_approvals": 0, "required_checks": []},
            )
        )
        self.assertEqual(checks.outcome, "denied")
        self.assertEqual(len(review_api.calls), 5)

    def test_named_legacy_commit_status_can_satisfy_required_check(self) -> None:
        api = FakeGitHubAPI(
            [
                api_response(200, pull()),
                api_response(200, {"total_count": 0, "check_runs": []}),
                api_response(
                    200,
                    {
                        "state": "success",
                        "total_count": 1,
                        "statuses": [{"context": "legacy-ci", "state": "success"}],
                    },
                ),
                api_response(200, []),
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "checks.read",
                parameters={
                    "required_approvals": 0,
                    "required_checks": ["legacy-ci"],
                },
            )
        )

        self.assertEqual(response.outcome, "succeeded")

    def test_pending_and_comment_reviews_do_not_erase_submitted_approval(self) -> None:
        api = FakeGitHubAPI(
            [
                api_response(200, pull()),
                api_response(200, {"total_count": 0, "check_runs": []}),
                api_response(200, {"state": "pending", "statuses": []}),
                api_response(
                    200,
                    [
                        {
                            "id": 1,
                            "state": "APPROVED",
                            "submitted_at": "2098-12-31T22:00:00Z",
                            "user": {"login": "reviewer"},
                        },
                        {
                            "id": 2,
                            "state": "COMMENTED",
                            "submitted_at": "2098-12-31T23:00:00Z",
                            "user": {"login": "reviewer"},
                        },
                        {
                            "id": 3,
                            "state": "PENDING",
                            "user": {"login": "draft-reviewer"},
                        },
                    ],
                ),
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "checks.read",
                parameters={"required_approvals": 1, "required_checks": []},
            )
        )

        self.assertEqual(response.outcome, "succeeded")

    def test_named_failed_check_or_latest_change_request_denies_readiness(self) -> None:
        for check_conclusion, reviews in (
            ("failure", []),
            (
                "success",
                [
                    {
                        "id": 1,
                        "state": "CHANGES_REQUESTED",
                        "submitted_at": "2098-12-31T23:00:00Z",
                        "user": {"login": "reviewer"},
                    }
                ],
            ),
        ):
            with self.subTest(check_conclusion=check_conclusion, reviews=reviews):
                api = FakeGitHubAPI(
                    [
                        api_response(200, pull()),
                        api_response(
                            200,
                            {
                                "total_count": 1,
                                "check_runs": [
                                    {
                                        "name": "unit",
                                        "status": "completed",
                                        "conclusion": check_conclusion,
                                    }
                                ],
                            },
                        ),
                        api_response(200, {"state": "success", "statuses": []}),
                        api_response(200, reviews),
                    ]
                )
                response = GitHubProvider(self.spec, api).invoke(
                    request(
                        self.socket_path,
                        "checks.read",
                        parameters={
                            "required_approvals": 0,
                            "required_checks": ["unit"],
                        },
                    )
                )
                self.assertEqual(response.outcome, "denied")
                self.assertEqual(response.error_code, "github-checks-failed")

    def test_upstream_failures_are_normalized_by_retry_safety(self) -> None:
        cases = (
            (api_response(429, {}, retry_after_seconds=7), "throttled", 7),
            (api_response(403, {}), "denied", None),
            (api_response(503, {}), "retryable", 15),
            (api_response(404, {}), "conflict", None),
        )
        for upstream, outcome, retry_after in cases:
            with self.subTest(status=upstream.status):
                response = GitHubProvider(self.spec, FakeGitHubAPI([upstream])).invoke(
                    request(
                        self.socket_path,
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
                )
                self.assertEqual(response.outcome, outcome)
                self.assertEqual(response.retry_after_seconds, retry_after)

    def test_transport_and_byte_budgets_preserve_effect_retry_safety(self) -> None:
        operation = request(
            self.socket_path,
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
        cases = (
            (
                replace(operation, response_limit_bytes=1),
                [api_response(200, [])],
                "retryable",
            ),
            (
                replace(operation, response_limit_bytes=128),
                [api_response(200, [])],
                "retryable",
            ),
            (
                replace(operation, request_limit_bytes=1),
                [
                    api_response(200, []),
                    api_response(200, ref(BASE)),
                    api_response(200, ref(HEAD, "mgb/op-42")),
                ],
                "denied",
            ),
            (
                operation,
                [
                    api_response(200, []),
                    api_response(200, ref(BASE)),
                    api_response(200, ref(HEAD, "mgb/op-42")),
                    GitHubTransportError("private", ambiguous=False),
                ],
                "retryable",
            ),
            (
                operation,
                [
                    api_response(200, []),
                    api_response(200, ref(BASE)),
                    api_response(200, ref(HEAD, "mgb/op-42")),
                    api_response(503, {}),
                ],
                "uncertain",
            ),
        )
        for bounded_operation, responses, expected in cases:
            with self.subTest(expected=expected, responses=len(responses)):
                result = GitHubProvider(
                    self.spec, FakeGitHubAPI(list(responses))
                ).invoke(bounded_operation)
                self.assertEqual(result.outcome, expected)

        invalid_transport = FakeGitHubAPI([])
        invalid_transport.responses.append(object())  # type: ignore[arg-type]
        invalid = GitHubProvider(self.spec, invalid_transport).invoke(operation)
        self.assertEqual(invalid.outcome, "retryable")

    def test_malformed_github_documents_fail_closed(self) -> None:
        change = request(
            self.socket_path,
            "change.open",
            phase="reconcile",
            resource_id="refs/heads/mgb/op-42",
            parameters={
                "base_ref": "refs/heads/main",
                "body": "Bounded summary.",
                "draft": True,
                "head_ref": "refs/heads/mgb/op-42",
                "title": "Candidate 42",
            },
        )
        for document in ({}, [object()]):
            with self.subTest(change_document=type(document).__name__):
                response = GitHubProvider(
                    self.spec, FakeGitHubAPI([api_response(200, document)])
                ).invoke(change)
                self.assertEqual(response.outcome, "denied")

        landing = request(
            self.socket_path,
            "landing.invoke",
            parameters={
                "assurance": "head_enforced",
                "integration_ref": "refs/heads/main",
                "merge_method": "merge",
            },
        )
        malformed_pulls: list[object] = []
        for key, value in (
            ("number", 0),
            ("state", "unknown"),
            ("merged", None),
            ("merge_commit_sha", "bad"),
            ("merged_at", 7),
        ):
            document = pull()
            document[key] = value
            malformed_pulls.append(document)
        bad_head = pull()
        bad_head["head"] = {"sha": "bad", "ref": "mgb/op-42"}
        malformed_pulls.append(bad_head)
        for document in malformed_pulls:
            with self.subTest(pull_document=document):
                response = GitHubProvider(
                    self.spec, FakeGitHubAPI([api_response(200, document)])
                ).invoke(landing)
                self.assertEqual(response.outcome, "denied")

        target_ref = "refs/heads/mgb/cnd-" + "3" * 32
        invalid_ref = ref(HEAD, target_ref.removeprefix("refs/heads/"))
        invalid_ref["object"] = {"type": "tag", "sha": HEAD}
        response = GitHubProvider(
            self.publishing_spec(),
            FakeGitHubAPI([api_response(200, invalid_ref)]),
            publisher=FakeGitPublisher(),
        ).invoke(
            request(
                self.socket_path,
                "refs.publish",
                resource_id=target_ref,
                expected_revision=None,
                parameters={"source_repository": "/private/repository.git"},
            )
        )
        self.assertEqual(response.outcome, "denied")

    def test_malformed_check_status_and_review_documents_fail_closed(self) -> None:
        operation = request(
            self.socket_path,
            "checks.read",
            parameters={"required_approvals": 0, "required_checks": []},
        )
        valid_checks = {"total_count": 0, "check_runs": []}
        valid_status = {"state": "success", "statuses": []}
        cases = (
            [
                api_response(200, pull()),
                api_response(200, {"total_count": 101, "check_runs": []}),
            ],
            [
                api_response(200, pull()),
                api_response(200, valid_checks),
                api_response(200, {"state": "unknown", "statuses": []}),
            ],
            [
                api_response(200, pull()),
                api_response(200, valid_checks),
                api_response(
                    200,
                    {
                        "state": "success",
                        "statuses": [
                            {"context": "unit", "state": "success"},
                            {"context": "unit", "state": "failure"},
                        ],
                    },
                ),
            ],
            [
                api_response(200, pull()),
                api_response(200, valid_checks),
                api_response(200, valid_status),
                api_response(
                    200,
                    [
                        {
                            "id": True,
                            "state": "APPROVED",
                            "submitted_at": "2098-12-31T23:00:00Z",
                            "user": {"login": "reviewer"},
                        }
                    ],
                ),
            ],
        )
        for responses in cases:
            with self.subTest(responses=len(responses)):
                result = GitHubProvider(
                    self.spec, FakeGitHubAPI(list(responses))
                ).invoke(operation)
                self.assertEqual(result.outcome, "denied")

    def test_check_status_and_review_normalizers_preserve_exact_semantics(self) -> None:
        self.assertEqual(
            GitHubProvider._normalize_checks(
                {
                    "total_count": 4,
                    "check_runs": [
                        {
                            "name": "success",
                            "status": "completed",
                            "conclusion": "success",
                        },
                        {
                            "name": "neutral",
                            "status": "completed",
                            "conclusion": "neutral",
                        },
                        {
                            "name": "queued",
                            "status": "queued",
                            "conclusion": None,
                        },
                        {
                            "name": "waiting",
                            "status": "waiting",
                            "conclusion": None,
                        },
                    ],
                }
            ),
            [
                {"conclusion": "success", "name": "success", "status": "completed"},
                {"conclusion": "neutral", "name": "neutral", "status": "completed"},
                {"conclusion": None, "name": "queued", "status": "queued"},
                {"conclusion": None, "name": "waiting", "status": "waiting"},
            ],
        )
        self.assertEqual(
            GitHubProvider._normalize_statuses(
                {
                    "state": "pending",
                    "total_count": 4,
                    "statuses": [
                        {"context": "success", "state": "success"},
                        {"context": "failure", "state": "failure"},
                        {"context": "error", "state": "error"},
                        {"context": "pending", "state": "pending"},
                    ],
                }
            ),
            [
                {
                    "conclusion": "success",
                    "name": "success",
                    "status": "completed",
                },
                {
                    "conclusion": "failure",
                    "name": "failure",
                    "status": "completed",
                },
                {
                    "conclusion": "failure",
                    "name": "error",
                    "status": "completed",
                },
                {"conclusion": None, "name": "pending", "status": "in_progress"},
            ],
        )
        self.assertEqual(
            GitHubProvider._normalize_reviews(
                [
                    {
                        "id": 4,
                        "state": "CHANGES_REQUESTED",
                        "submitted_at": "2098-12-31T23:00:00Z",
                        "user": {"login": "later"},
                    },
                    {
                        "id": 2,
                        "state": "COMMENTED",
                        "submitted_at": "2098-12-31T22:00:00Z",
                        "user": {"login": "commenter"},
                    },
                    {
                        "id": 1,
                        "state": "APPROVED",
                        "submitted_at": "2098-12-31T22:00:00Z",
                        "user": {"login": "reviewer"},
                    },
                    {
                        "id": 3,
                        "state": "DISMISSED",
                        "submitted_at": "2098-12-31T22:30:00Z",
                        "user": {"login": "dismissed"},
                    },
                    {
                        "id": 5,
                        "state": "PENDING",
                        "submitted_at": None,
                        "user": {"login": "draft"},
                    },
                ]
            ),
            [
                {
                    "id": 1,
                    "login": "reviewer",
                    "state": "APPROVED",
                    "submitted_at": "2098-12-31T22:00:00.000000Z",
                },
                {
                    "id": 2,
                    "login": "commenter",
                    "state": "COMMENTED",
                    "submitted_at": "2098-12-31T22:00:00.000000Z",
                },
                {
                    "id": 3,
                    "login": "dismissed",
                    "state": "DISMISSED",
                    "submitted_at": "2098-12-31T22:30:00.000000Z",
                },
                {
                    "id": 4,
                    "login": "later",
                    "state": "CHANGES_REQUESTED",
                    "submitted_at": "2098-12-31T23:00:00.000000Z",
                },
            ],
        )

    def test_review_timestamps_are_validated_normalized_and_sorted(self) -> None:
        self.assertEqual(
            GitHubProvider._normalize_reviews(
                [
                    {
                        "id": 2,
                        "state": "CHANGES_REQUESTED",
                        "submitted_at": "2099-01-01T00:30:00+02:00",
                        "user": {"login": "reviewer"},
                    },
                    {
                        "id": 1,
                        "state": "APPROVED",
                        "submitted_at": "2098-12-31T23:00:00Z",
                        "user": {"login": "reviewer"},
                    },
                ]
            ),
            [
                {
                    "id": 2,
                    "login": "reviewer",
                    "state": "CHANGES_REQUESTED",
                    "submitted_at": "2098-12-31T22:30:00.000000Z",
                },
                {
                    "id": 1,
                    "login": "reviewer",
                    "state": "APPROVED",
                    "submitted_at": "2098-12-31T23:00:00.000000Z",
                },
            ],
        )
        with self.assertRaises(GitHubProviderError):
            GitHubProvider._normalize_reviews(
                [
                    {
                        "id": 1,
                        "state": "APPROVED",
                        "submitted_at": "not-a-timestamp",
                        "user": {"login": "reviewer"},
                    }
                ]
            )

    def test_review_normalizer_enforces_exact_bounds_and_submitted_shape(self) -> None:
        valid_review = {
            "id": 1,
            "state": "APPROVED",
            "submitted_at": "2098-12-31T23:00:00Z",
            "user": {"login": "reviewer"},
        }
        self.assertEqual(
            len(GitHubProvider._normalize_reviews([valid_review] * 100)), 100
        )
        with self.assertRaises(GitHubProviderError):
            GitHubProvider._normalize_reviews([valid_review] * 101)

        malformed = (
            {
                **valid_review,
                "id": "1",
            },
            {
                **valid_review,
                "user": {"login": ""},
            },
            {
                **valid_review,
                "user": {"login": 1},
            },
            {
                **valid_review,
                "state": "PENDING",
            },
        )
        for review in malformed:
            with (
                self.subTest(review=review),
                self.assertRaises(GitHubProviderError),
            ):
                GitHubProvider._normalize_reviews([review])

        self.assertEqual(
            GitHubProvider._normalize_reviews(
                [
                    {
                        "id": 1,
                        "state": "PENDING",
                        "submitted_at": None,
                        "user": {"login": "draft"},
                    },
                    valid_review,
                ]
            ),
            [
                {
                    "id": 1,
                    "login": "reviewer",
                    "state": "APPROVED",
                    "submitted_at": "2098-12-31T23:00:00.000000Z",
                }
            ],
        )

    def test_read_and_check_numeric_boundaries_are_exact(self) -> None:
        accepted_list = GitHubProvider(
            self.spec, FakeGitHubAPI([api_response(200, [])])
        ).invoke(
            request(
                self.socket_path,
                "work.list",
                resource_id="issues",
                expected_revision=None,
                desired_revision=None,
                object_format=None,
                parameters={"cursor": 10_000, "limit": 100, "state": "closed"},
            )
        )
        self.assertEqual(accepted_list.outcome, "succeeded")
        self.assertEqual(
            accepted_list.receipt.result,
            {"items": [], "schema_version": 1, "truncated": False},
        )

        checks_request = request(
            self.socket_path,
            "checks.read",
            parameters={
                "required_approvals": 32,
                "required_checks": [f"check-{index}" for index in range(64)],
            },
        )
        provider = GitHubProvider(self.spec, FakeGitHubAPI([]))
        self.assertEqual(
            provider._checks_parameters(checks_request),
            {
                "required_approvals": 32,
                "required_checks": tuple(f"check-{index}" for index in range(64)),
            },
        )

        invalid_lists = (
            {"cursor": True, "limit": 1, "state": "open"},
            {"cursor": 10_001, "limit": 1, "state": "open"},
            {"cursor": 1, "limit": False, "state": "open"},
            {"cursor": 1, "limit": 0, "state": "open"},
        )
        for parameters in invalid_lists:
            with self.subTest(parameters=parameters):
                response = GitHubProvider(self.spec, FakeGitHubAPI([])).invoke(
                    request(
                        self.socket_path,
                        "work.list",
                        resource_id="issues",
                        expected_revision=None,
                        desired_revision=None,
                        object_format=None,
                        parameters=parameters,
                    )
                )
                self.assertEqual(response.outcome, "denied")

        for approvals, required_checks in (
            (-1, []),
            (33, []),
            (True, []),
            (0, [f"check-{index}" for index in range(65)]),
        ):
            with self.subTest(approvals=approvals, checks=len(required_checks)):
                response = GitHubProvider(self.spec, FakeGitHubAPI([])).invoke(
                    request(
                        self.socket_path,
                        "checks.read",
                        parameters={
                            "required_approvals": approvals,
                            "required_checks": required_checks,
                        },
                    )
                )
                self.assertEqual(response.outcome, "denied")

    def test_wrong_request_authority_is_denied_before_api_dispatch(self) -> None:
        changed = self.spec.to_dict()
        changed["deployment_sha256"] = "f" * 64
        api = FakeGitHubAPI([])
        response = GitHubProvider(ServiceSidecarSpec.from_mapping(changed), api).invoke(
            request(
                self.socket_path,
                "landing.invoke",
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "merge",
                },
            )
        )
        self.assertEqual(response.outcome, "denied")
        self.assertEqual(api.calls, [])

    def test_every_request_authority_dimension_fails_closed(self) -> None:
        base = request(
            self.socket_path,
            "landing.invoke",
            parameters={
                "assurance": "head_enforced",
                "integration_ref": "refs/heads/main",
                "merge_method": "merge",
            },
        )
        variants = (
            replace(base, request_limit_bytes=self.spec.request_bytes + 1),
            replace(base, response_limit_bytes=self.spec.response_bytes + 1),
            replace(base, mapping=replace(base.mapping, name="secondary")),
            replace(base, capability=replace(base.capability, version=2)),
            replace(
                base,
                target=replace(
                    base.target,
                    resource=replace(
                        base.target.resource,
                        external_id="other/widgets",
                    ),
                ),
            ),
            replace(base, target=replace(base.target, object_format="sha256")),
        )
        for variant in variants:
            api = FakeGitHubAPI([])
            with self.subTest(variant=variant):
                response = GitHubProvider(self.spec, api).invoke(variant)
                self.assertEqual(response.outcome, "denied")
                self.assertEqual(api.calls, [])

    def test_capability_parameters_fail_closed_before_provider_dispatch(self) -> None:
        cases = (
            request(self.socket_path, "change.open"),
            request(
                self.socket_path,
                "change.open",
                resource_id="refs/heads/main",
                parameters={
                    "base_ref": "refs/heads/main",
                    "body": "body",
                    "draft": True,
                    "head_ref": "refs/heads/main",
                    "title": "title",
                },
            ),
            request(
                self.socket_path,
                "change.open",
                resource_id="refs/heads/topic",
                parameters={
                    "base_ref": "refs/heads/main",
                    "body": "body",
                    "draft": "yes",
                    "head_ref": "refs/heads/topic",
                    "title": "title",
                },
            ),
            request(
                self.socket_path,
                "change.open",
                resource_id="refs/heads/topic",
                expected_revision=None,
                parameters={
                    "base_ref": "refs/heads/main",
                    "body": "body",
                    "draft": True,
                    "head_ref": "refs/heads/topic",
                    "title": "title",
                },
            ),
            request(
                self.socket_path,
                "checks.read",
                parameters={"required_approvals": -1, "required_checks": []},
            ),
            request(
                self.socket_path,
                "checks.read",
                parameters={"required_approvals": 0, "required_checks": "unit"},
            ),
            request(
                self.socket_path,
                "checks.read",
                parameters={"required_approvals": 0, "required_checks": [""]},
            ),
            request(
                self.socket_path,
                "checks.read",
                parameters={
                    "required_approvals": 0,
                    "required_checks": ["unit", "unit"],
                },
            ),
            request(
                self.socket_path,
                "checks.read",
                desired_revision=None,
                parameters={"required_approvals": 0, "required_checks": []},
            ),
            request(
                self.socket_path,
                "landing.invoke",
                parameters={
                    "assurance": "best_effort",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "merge",
                },
            ),
            request(
                self.socket_path,
                "landing.invoke",
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "octopus",
                },
            ),
            request(
                self.socket_path,
                "landing.invoke",
                desired_revision=None,
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "merge",
                },
            ),
        )
        for operation in cases:
            api = FakeGitHubAPI([])
            with self.subTest(capability=operation.capability.name):
                response = GitHubProvider(self.spec, api).invoke(operation)
                self.assertEqual(response.outcome, "denied")
                self.assertEqual(api.calls, [])

        publisher = FakeGitPublisher()
        for operation in (
            request(
                self.socket_path,
                "refs.publish",
                resource_id="refs/heads/topic",
                expected_revision=None,
                parameters={"source_repository": "relative/repository.git"},
            ),
            request(
                self.socket_path,
                "refs.publish",
                resource_id="refs/heads/topic",
                parameters={"source_repository": "/private/repository.git"},
            ),
        ):
            api = FakeGitHubAPI([])
            response = GitHubProvider(
                self.publishing_spec(), api, publisher=publisher
            ).invoke(operation)
            self.assertEqual(response.outcome, "denied")
            self.assertEqual(api.calls, [])
        self.assertEqual(publisher.calls, [])

    def test_transport_loss_after_effect_is_uncertain_and_redacted(self) -> None:
        api = FakeGitHubAPI(
            [
                GitHubTransportError(
                    "raw credential-bearing upstream error", ambiguous=True
                )
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "landing.invoke",
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "merge",
                },
            )
        )

        self.assertEqual(response.outcome, "uncertain")
        self.assertEqual(response.error_code, "github-transport-ambiguous")
        self.assertNotIn(
            "raw credential-bearing upstream error", str(response.to_dict())
        )

    def test_malformed_followup_after_merge_dispatch_is_uncertain(self) -> None:
        api = FakeGitHubAPI(
            [
                api_response(200, pull()),
                api_response(200, ref(BASE)),
                api_response(200, {"sha": MERGED, "merged": True}),
                api_response(200, {"number": 42}),
            ]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "landing.invoke",
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "merge",
                },
            )
        )

        self.assertEqual(response.outcome, "uncertain")
        self.assertEqual(response.error_code, "github-response-invalid")

    def test_failed_followup_observation_after_merge_dispatch_is_uncertain(
        self,
    ) -> None:
        for followup in (
            GitHubTransportError("read did not dispatch", ambiguous=False),
            api_response(403, {"message": "credential changed"}),
        ):
            with self.subTest(followup=type(followup).__name__):
                api = FakeGitHubAPI(
                    [
                        api_response(200, pull()),
                        api_response(200, ref(BASE)),
                        api_response(200, {"sha": MERGED, "merged": True}),
                        followup,
                    ]
                )

                response = GitHubProvider(self.spec, api).invoke(
                    request(
                        self.socket_path,
                        "landing.invoke",
                        parameters={
                            "assurance": "head_enforced",
                            "integration_ref": "refs/heads/main",
                            "merge_method": "merge",
                        },
                    )
                )

                self.assertEqual(response.outcome, "uncertain")
                self.assertIsNone(response.receipt)

    def test_reconcile_does_not_close_a_pull_for_a_different_base_ref(self) -> None:
        wrong_base = pull(state="closed", merged=True)
        wrong_base["base"]["ref"] = "release"
        api = FakeGitHubAPI(
            [api_response(200, wrong_base), api_response(200, ref(MERGED))]
        )

        response = GitHubProvider(self.spec, api).invoke(
            request(
                self.socket_path,
                "landing.invoke",
                phase="reconcile",
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "merge",
                },
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.receipt.closed_references, ())

    def test_wrong_provider_authority_fails_before_api_dispatch(self) -> None:
        changed = self.spec.to_dict()
        changed["mappings"][0]["service"] = "gitlab"
        api = FakeGitHubAPI([])

        with self.assertRaisesRegex(ValueError, "GitHub service mappings"):
            GitHubProvider(ServiceSidecarSpec.from_mapping(changed), api)

        unsafe_repository = self.spec.to_dict()
        unsafe_repository["mappings"][0]["repository"] = "acme/widgets/extra"
        with self.assertRaisesRegex(ValueError, "GitHub repository mapping"):
            GitHubProvider(ServiceSidecarSpec.from_mapping(unsafe_repository), api)

        self.assertEqual(api.calls, [])

    def test_git_invalid_branch_components_fail_before_api_dispatch(self) -> None:
        for head_ref in (
            "main",
            "refs/heads/",
            "refs/heads//topic",
            "refs/heads/topic/",
            "refs/heads/topic.",
            "refs/heads/.hidden",
            "refs/heads/topic/.hidden",
            "refs/heads/topic.lock",
            "refs/heads/topic..next",
            "refs/heads/topic//next",
            "refs/heads/topic@{next",
            "refs/heads/topic name",
            "refs/heads/topic~name",
            "refs/heads/topic^name",
            "refs/heads/topic:name",
            "refs/heads/topic?name",
            "refs/heads/topic*name",
            "refs/heads/topic[name",
            "refs/heads/topic\\name",
            "refs/heads/topic\x1fname",
            "refs/heads/topic\x7fname",
            "refs/heads/" + "x" * 246,
        ):
            with self.subTest(head_ref=head_ref):
                api = FakeGitHubAPI([])
                response = GitHubProvider(self.spec, api).invoke(
                    request(
                        self.socket_path,
                        "change.open",
                        resource_id=head_ref,
                        parameters={
                            "base_ref": "refs/heads/main",
                            "body": "Bounded summary.",
                            "draft": True,
                            "head_ref": head_ref,
                            "title": "Candidate 42",
                        },
                    )
                )

                self.assertEqual(response.outcome, "denied")
                self.assertEqual(api.calls, [])


if __name__ == "__main__":
    unittest.main()
