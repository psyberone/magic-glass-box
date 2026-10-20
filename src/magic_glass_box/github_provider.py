from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, urlencode

from .domain import (
    ValidationError,
    WorkItemEnvelope,
    canonical_json,
    content_sha256,
    parse_rfc3339_timestamp,
    require_string,
)
from .service_sidecar import (
    ServiceOperationReceipt,
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceSidecarSpec,
)

SUPPORTED_CAPABILITIES = frozenset(
    {
        ("work.get", 1),
        ("work.list", 1),
        ("change.get", 1),
        ("change.list", 1),
        ("change.open", 1),
        ("checks.read", 1),
        ("landing.invoke", 1),
        ("refs.publish", 1),
    }
)
PULL_NUMBER_RE = re.compile(r"^[1-9][0-9]{0,9}$")
GITHUB_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")
SUCCESSFUL_CHECK_CONCLUSIONS = frozenset({"neutral", "skipped", "success"})
FAILED_CHECK_CONCLUSIONS = frozenset(
    {
        "action_required",
        "cancelled",
        "failure",
        "stale",
        "startup_failure",
        "timed_out",
    }
)
MAX_REQUIRED_CHECKS = 64
MAX_REQUIRED_APPROVALS = 32
MAX_READ_PAGE_SIZE = 100
MAX_READ_CURSOR = 10_000
DEFAULT_RETRY_SECONDS = 15


class GitHubProviderError(ValueError):
    """A GitHub provider contract or normalized upstream response is invalid."""


class GitHubTransportError(RuntimeError):
    """The bounded GitHub transport failed without exposing upstream content."""

    def __init__(self, message: str, *, ambiguous: bool) -> None:
        super().__init__(message)
        self.ambiguous = ambiguous


@dataclass(frozen=True)
class GitHubAPIResponse:
    status: int
    body: object
    response_bytes: int
    retry_after_seconds: int | None

    def __post_init__(self) -> None:
        if (
            isinstance(self.status, bool)
            or not isinstance(self.status, int)
            or not 100 <= self.status <= 599
        ):
            raise GitHubProviderError("GitHub response status is invalid")
        if (
            isinstance(self.response_bytes, bool)
            or not isinstance(self.response_bytes, int)
            or self.response_bytes < 0
        ):
            raise GitHubProviderError("GitHub response size is invalid")
        if self.retry_after_seconds is not None and (
            isinstance(self.retry_after_seconds, bool)
            or not isinstance(self.retry_after_seconds, int)
            or not 1 <= self.retry_after_seconds <= 30 * 24 * 60 * 60
        ):
            raise GitHubProviderError("GitHub retry delay is invalid")


class GitHubAPI(Protocol):
    def request(
        self,
        method: str,
        *,
        host: str,
        endpoint: str,
        body: Mapping[str, Any] | None,
        deadline: str,
        response_limit_bytes: int,
    ) -> GitHubAPIResponse: ...


class GitPublisher(Protocol):
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
    ) -> None: ...


@dataclass
class _Invocation:
    request: ServiceOperationRequest
    request_bytes: int = 0
    response_bytes: int = 0
    effect_dispatched: bool = False


def github_remote_identity(host: str, repository: str, pull_number: int) -> str:
    if not isinstance(pull_number, int) or isinstance(pull_number, bool):
        raise GitHubProviderError("GitHub pull number is invalid")
    if PULL_NUMBER_RE.fullmatch(str(pull_number)) is None:
        raise GitHubProviderError("GitHub pull number is invalid")
    return f"github:{host}/{repository}/pull/{pull_number}"


def _exact_parameters(
    value: Mapping[str, Any], *, fields: set[str], capability: str
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GitHubProviderError(f"{capability} parameters must be an object")
    missing = fields - set(value)
    unknown = set(value) - fields
    if missing or unknown:
        raise GitHubProviderError(f"{capability} parameters are invalid")
    return dict(value)


def _branch_ref(value: object, *, field: str) -> str:
    try:
        text = require_string(value, field=field, max_bytes=256)
    except ValidationError as exc:
        raise GitHubProviderError(str(exc)) from exc
    if not text.startswith("refs/heads/"):
        raise GitHubProviderError(f"{field} must be a branch ref")
    branch = text.removeprefix("refs/heads/")
    components = branch.split("/")
    if (
        not branch
        or branch.startswith("/")
        or branch.endswith(("/", "."))
        or any(
            component.startswith(".") or component.endswith(".lock")
            for component in components
        )
        or ".." in branch
        or "//" in branch
        or "@{" in branch
        or any(character in " ~^:?*[\\" for character in branch)
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in branch)
    ):
        raise GitHubProviderError(f"{field} is invalid")
    return text


def _pull_number(value: object) -> int:
    if not isinstance(value, str) or PULL_NUMBER_RE.fullmatch(value) is None:
        raise GitHubProviderError("GitHub pull number is invalid")
    return int(value)


def _repository_endpoint(repository: str, suffix: str) -> str:
    owner, name = repository.split("/", 1)
    return f"repos/{quote(owner, safe='')}/{quote(name, safe='')}/{suffix}"


def _ref_endpoint(repository: str, ref: str) -> str:
    relative = ref.removeprefix("refs/")
    return _repository_endpoint(repository, f"git/ref/{quote(relative, safe='/')}")


def _object(value: object, *, subject: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GitHubProviderError(f"GitHub {subject} response is invalid")
    return value


def _array(value: object, *, subject: str) -> list[object]:
    if not isinstance(value, list):
        raise GitHubProviderError(f"GitHub {subject} response is invalid")
    return value


class GitHubProvider:
    """Translate strict semantic operations into a narrow GitHub API surface."""

    def __init__(
        self,
        spec: ServiceSidecarSpec,
        api: GitHubAPI,
        *,
        publisher: GitPublisher | None = None,
    ) -> None:
        if not isinstance(spec, ServiceSidecarSpec):
            raise GitHubProviderError("GitHub sidecar spec is invalid")
        if not spec.mappings or any(
            mapping.service != "github" for mapping in spec.mappings
        ):
            raise GitHubProviderError("GitHub service mappings are required")
        if any(
            GITHUB_REPOSITORY_RE.fullmatch(mapping.repository) is None
            or any(part in {".", ".."} for part in mapping.repository.split("/", 1))
            for mapping in spec.mappings
        ):
            raise GitHubProviderError("GitHub repository mapping is invalid")
        unsupported = {
            (capability.name, capability.version) for capability in spec.capabilities
        } - SUPPORTED_CAPABILITIES
        if unsupported:
            raise GitHubProviderError("GitHub sidecar capability is unsupported")
        if not hasattr(api, "request"):
            raise GitHubProviderError("GitHub API transport is invalid")
        supports_publication = any(
            (capability.name, capability.version) == ("refs.publish", 1)
            for capability in spec.capabilities
        )
        if supports_publication and (
            publisher is None or not hasattr(publisher, "publish")
        ):
            raise GitHubProviderError("GitHub ref publisher is required")
        self.spec = spec
        self.api = api
        self.publisher = publisher

    def invoke(self, request: ServiceOperationRequest) -> ServiceOperationResponse:
        if not isinstance(request, ServiceOperationRequest):
            raise GitHubProviderError("GitHub operation request is invalid")
        invocation = _Invocation(request)
        try:
            self._validate_authority(request)
            if request.capability.name == "work.get":
                return self._work_get(invocation)
            if request.capability.name == "work.list":
                return self._work_list(invocation)
            if request.capability.name == "change.get":
                return self._change_get(invocation)
            if request.capability.name == "change.list":
                return self._change_list(invocation)
            if request.capability.name == "change.open":
                return self._change_open(invocation)
            if request.capability.name == "checks.read":
                return self._checks_read(invocation)
            if request.capability.name == "refs.publish":
                return self._ref_publish(invocation)
            if request.capability.name == "landing.invoke":
                return self._landing(invocation)
            raise GitHubProviderError("GitHub capability is unsupported")
        except GitHubTransportError as exc:
            if (
                exc.ambiguous or invocation.effect_dispatched
            ) and request.capability.mode == "effect":
                return self._failure(
                    invocation,
                    outcome="uncertain",
                    error_code="github-transport-ambiguous",
                )
            return self._failure(
                invocation,
                outcome="retryable",
                error_code="github-transport-unavailable",
                retry_after_seconds=DEFAULT_RETRY_SECONDS,
            )
        except GitHubProviderError:
            if invocation.effect_dispatched:
                return self._failure(
                    invocation,
                    outcome="uncertain",
                    error_code="github-response-invalid",
                )
            return self._failure(
                invocation,
                outcome="denied",
                error_code="github-request-invalid",
            )

    def _validate_authority(self, request: ServiceOperationRequest) -> None:
        if request.connector_protocol_version != self.spec.connector_protocol_version:
            raise GitHubProviderError("GitHub connector protocol does not match")
        if request.deployment_sha256 != self.spec.deployment_sha256:
            raise GitHubProviderError("GitHub deployment does not match")
        if request.request_limit_bytes > self.spec.request_bytes:
            raise GitHubProviderError("GitHub request limit exceeds authority")
        if request.response_limit_bytes > self.spec.response_bytes:
            raise GitHubProviderError("GitHub response limit exceeds authority")
        mapping = next(
            (item for item in self.spec.mappings if item.name == request.mapping.name),
            None,
        )
        if mapping is None or mapping != request.mapping:
            raise GitHubProviderError("GitHub mapping does not match authority")
        capability = next(
            (
                item
                for item in self.spec.capabilities
                if (item.name, item.version)
                == (request.capability.name, request.capability.version)
            ),
            None,
        )
        if capability is None or capability != request.capability:
            raise GitHubProviderError("GitHub capability does not match authority")
        if request.target.repository != request.mapping.repository:
            raise GitHubProviderError("GitHub target repository does not match")
        metadata_reads = {"work.get", "work.list", "change.get", "change.list"}
        expected_format = None if request.capability.name in metadata_reads else "sha1"
        if request.target.object_format != expected_format:
            raise GitHubProviderError("GitHub v1 operation object format is invalid")

    def _call(
        self,
        invocation: _Invocation,
        method: str,
        endpoint: str,
        *,
        body: Mapping[str, Any] | None = None,
    ) -> GitHubAPIResponse:
        request = invocation.request
        raw_body = b"" if body is None else canonical_json(dict(body)).encode("utf-8")
        next_request_bytes = invocation.request_bytes + len(raw_body)
        if next_request_bytes > request.request_limit_bytes:
            invocation.request_bytes = request.request_limit_bytes
            raise GitHubProviderError("GitHub request budget is exhausted")
        invocation.request_bytes = next_request_bytes
        remaining = request.response_limit_bytes - invocation.response_bytes
        if remaining < 1:
            raise GitHubTransportError(
                "GitHub response budget is exhausted",
                ambiguous=method not in {"GET", "HEAD"},
            )
        effect_call = method not in {"GET", "HEAD"}
        previously_dispatched = invocation.effect_dispatched
        if effect_call:
            invocation.effect_dispatched = True
        try:
            response = self.api.request(
                method,
                host=request.mapping.host,
                endpoint=endpoint,
                body=body,
                deadline=request.deadline,
                response_limit_bytes=remaining,
            )
        except GitHubTransportError as exc:
            if effect_call and not previously_dispatched and not exc.ambiguous:
                invocation.effect_dispatched = False
            raise
        if not isinstance(response, GitHubAPIResponse):
            raise GitHubTransportError(
                "GitHub transport response is invalid",
                ambiguous=method not in {"GET", "HEAD"},
            )
        next_response_bytes = invocation.response_bytes + response.response_bytes
        if next_response_bytes > request.response_limit_bytes:
            invocation.response_bytes = request.response_limit_bytes
            raise GitHubTransportError(
                "GitHub response budget is exhausted",
                ambiguous=method not in {"GET", "HEAD"},
            )
        invocation.response_bytes = next_response_bytes
        return response

    def _failure_for_response(
        self,
        invocation: _Invocation,
        response: GitHubAPIResponse,
        *,
        observation_after_effect: bool = False,
    ) -> ServiceOperationResponse:
        if observation_after_effect:
            return self._failure(
                invocation,
                outcome="uncertain",
                provider_status=response.status,
                error_code="github-observation-ambiguous",
            )
        if response.status == 429 or response.retry_after_seconds is not None:
            return self._failure(
                invocation,
                outcome="throttled",
                provider_status=response.status,
                error_code="github-throttled",
                retry_after_seconds=response.retry_after_seconds
                or DEFAULT_RETRY_SECONDS,
            )
        if response.status in {401, 403}:
            return self._failure(
                invocation,
                outcome="denied",
                provider_status=response.status,
                error_code="github-denied",
            )
        if response.status >= 500:
            if invocation.effect_dispatched:
                return self._failure(
                    invocation,
                    outcome="uncertain",
                    provider_status=response.status,
                    error_code="github-upstream-ambiguous",
                )
            return self._failure(
                invocation,
                outcome="retryable",
                provider_status=response.status,
                error_code="github-upstream-unavailable",
                retry_after_seconds=DEFAULT_RETRY_SECONDS,
            )
        return self._failure(
            invocation,
            outcome="conflict",
            provider_status=response.status,
            error_code="github-conflict",
        )

    @staticmethod
    def _read_target(request: ServiceOperationRequest, *, resource_id: str) -> None:
        target = request.target
        if (
            target.resource_id != resource_id
            or target.object_format is not None
            or target.expected_revision is not None
            or target.desired_revision is not None
        ):
            raise GitHubProviderError("GitHub read target is invalid")

    @staticmethod
    def _list_parameters(
        request: ServiceOperationRequest, capability: str
    ) -> dict[str, Any]:
        parameters = _exact_parameters(
            request.parameters,
            fields={"cursor", "limit", "state"},
            capability=capability,
        )
        cursor = parameters["cursor"]
        limit = parameters["limit"]
        state = parameters["state"]
        if (
            isinstance(cursor, bool)
            or not isinstance(cursor, int)
            or not 1 <= cursor <= MAX_READ_CURSOR
        ):
            raise GitHubProviderError(f"{capability} cursor is invalid")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_READ_PAGE_SIZE
        ):
            raise GitHubProviderError(f"{capability} limit is invalid")
        if state not in {"all", "closed", "open"}:
            raise GitHubProviderError(f"{capability} state is invalid")
        return {"cursor": cursor, "limit": limit, "state": state}

    @staticmethod
    def _next_cursor(parameters: Mapping[str, Any], item_count: int) -> int | None:
        if item_count != parameters["limit"]:
            return None
        if parameters["cursor"] >= MAX_READ_CURSOR:
            raise GitHubProviderError("GitHub read pagination exceeds its limit")
        return int(parameters["cursor"]) + 1

    @staticmethod
    def _timestamp(value: object, *, field: str) -> str:
        try:
            parsed = parse_rfc3339_timestamp(value, field=field)
        except ValidationError as exc:
            raise GitHubProviderError(str(exc)) from exc
        return (
            parsed.astimezone(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )

    @staticmethod
    def _issue_labels(value: object) -> tuple[str, ...]:
        raw = _array(value, subject="issue labels")
        if len(raw) > 64:
            raise GitHubProviderError("GitHub issue labels exceed their limit")
        labels: list[str] = []
        for raw_label in raw:
            label = _object(raw_label, subject="issue label")
            try:
                labels.append(
                    require_string(
                        label.get("name"), field="issue label", max_bytes=128
                    )
                )
            except ValidationError as exc:
                raise GitHubProviderError(str(exc)) from exc
        if len(set(labels)) != len(labels):
            raise GitHubProviderError("GitHub issue labels are ambiguous")
        return tuple(labels)

    @staticmethod
    def _issue_assignees(value: object) -> tuple[str, ...]:
        raw = _array(value, subject="issue assignees")
        if len(raw) > 64:
            raise GitHubProviderError("GitHub issue assignees exceed their limit")
        assignees: list[str] = []
        for raw_assignee in raw:
            assignee = _object(raw_assignee, subject="issue assignee")
            try:
                assignees.append(
                    require_string(
                        assignee.get("login"), field="issue assignee", max_bytes=256
                    )
                )
            except ValidationError as exc:
                raise GitHubProviderError(str(exc)) from exc
        if len(set(assignees)) != len(assignees):
            raise GitHubProviderError("GitHub issue assignees are ambiguous")
        return tuple(assignees)

    def _normalize_issue(
        self,
        value: object,
        request: ServiceOperationRequest,
        *,
        include_brief: bool,
    ) -> tuple[dict[str, Any], WorkItemEnvelope]:
        document = _object(value, subject="issue")
        if "pull_request" in document:
            raise GitHubProviderError("GitHub issue response identifies a pull request")
        number = document.get("number")
        state = document.get("state")
        if (
            isinstance(number, bool)
            or not isinstance(number, int)
            or PULL_NUMBER_RE.fullmatch(str(number)) is None
            or state not in {"open", "closed"}
        ):
            raise GitHubProviderError("GitHub issue response is invalid")
        try:
            title = require_string(
                document.get("title"), field="issue title", max_bytes=512
            )
        except ValidationError as exc:
            raise GitHubProviderError(str(exc)) from exc
        body = document.get("body")
        if include_brief and body:
            try:
                brief = require_string(body, field="issue body", max_bytes=65536)
            except ValidationError as exc:
                raise GitHubProviderError(str(exc)) from exc
            if not brief.strip():
                brief = title
        elif include_brief and body is not None and not isinstance(body, str):
            raise GitHubProviderError("issue body must be text or null")
        else:
            brief = title
        labels = self._issue_labels(document.get("labels"))
        assignees = self._issue_assignees(document.get("assignees"))
        external_id = (
            f"{request.mapping.host}/{request.mapping.repository}/issues/{number}"
        )
        try:
            envelope = WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "github",
                    "external_id": external_id,
                    "source_updated_at": document.get("updated_at"),
                    "title": title,
                    "brief": brief,
                    "labels": list(labels),
                }
            )
        except ValidationError as exc:
            raise GitHubProviderError(str(exc)) from exc
        return (
            {
                "assignees": list(assignees),
                "external_id": external_id,
                "labels": list(labels),
                "number": number,
                "state": state,
                "title": title,
                "updated_at": envelope.source_updated_at,
            },
            envelope,
        )

    def _work_get(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        _exact_parameters(request.parameters, fields=set(), capability="work.get")
        number = _pull_number(request.target.resource_id)
        self._read_target(request, resource_id=str(number))
        response = self._call(
            invocation,
            "GET",
            _repository_endpoint(request.mapping.repository, f"issues/{number}"),
        )
        if response.status != 200:
            return self._failure_for_response(invocation, response)
        summary, envelope = self._normalize_issue(
            response.body, request, include_brief=True
        )
        if summary["number"] != number:
            raise GitHubProviderError("GitHub issue response does not match its target")
        result = envelope.to_dict()
        identity = f"github:{request.mapping.host}/{request.mapping.repository}/issues/"
        identity += str(number)
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=identity,
            observed_revision=None,
            evidence={"kind": "work", "state": summary["state"], **result},
            closed_references=(),
            result=result,
        )

    def _work_list(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        self._read_target(request, resource_id="issues")
        parameters = self._list_parameters(request, "work.list")
        query = urlencode(
            {
                "state": parameters["state"],
                "sort": "updated",
                "direction": "desc",
                "per_page": str(parameters["limit"]),
                "page": str(parameters["cursor"]),
            }
        )
        response = self._call(
            invocation,
            "GET",
            _repository_endpoint(request.mapping.repository, f"issues?{query}"),
        )
        if response.status != 200:
            return self._failure_for_response(invocation, response)
        raw_items = _array(response.body, subject="issue list")
        if len(raw_items) > parameters["limit"]:
            raise GitHubProviderError("GitHub issue list exceeds its requested limit")
        items: list[dict[str, Any]] = []
        for value in raw_items:
            document = _object(value, subject="issue")
            if "pull_request" in document:
                continue
            summary, _envelope = self._normalize_issue(
                document, request, include_brief=False
            )
            items.append(summary)
        result = {
            "items": items,
            "next_cursor": self._next_cursor(parameters, len(raw_items)),
        }
        identity = f"github:{request.mapping.host}/{request.mapping.repository}/issues"
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=identity,
            observed_revision=None,
            evidence={"kind": "work-list", **result},
            closed_references=(),
            result=result,
        )

    def _normalize_change(
        self, value: object, request: ServiceOperationRequest, *, require_merged: bool
    ) -> dict[str, Any]:
        document = _object(value, subject="pull request")
        normalized = self._normalize_pull(document, require_merged=require_merged)
        try:
            title = require_string(
                document.get("title"), field="pull request title", max_bytes=512
            )
        except ValidationError as exc:
            raise GitHubProviderError(str(exc)) from exc
        draft = document.get("draft")
        if not isinstance(draft, bool):
            raise GitHubProviderError("GitHub pull request draft state is invalid")
        number = normalized["number"]
        return {
            "base_ref": normalized["base_ref"],
            "draft": draft,
            "external_id": (
                f"{request.mapping.host}/{request.mapping.repository}/pull/{number}"
            ),
            "head_ref": normalized["head_ref"],
            "head_revision": normalized["head_sha"],
            "merged": normalized["merged"],
            "number": number,
            "state": normalized["state"],
            "title": title,
            "updated_at": self._timestamp(
                document.get("updated_at"), field="pull request updated_at"
            ),
        }

    def _change_get(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        _exact_parameters(request.parameters, fields=set(), capability="change.get")
        number = _pull_number(request.target.resource_id)
        self._read_target(request, resource_id=str(number))
        response = self._call(
            invocation,
            "GET",
            _repository_endpoint(request.mapping.repository, f"pulls/{number}"),
        )
        if response.status != 200:
            return self._failure_for_response(invocation, response)
        result = self._normalize_change(response.body, request, require_merged=True)
        if result["number"] != number:
            raise GitHubProviderError(
                "GitHub pull request response does not match its target"
            )
        identity = github_remote_identity(
            request.mapping.host, request.mapping.repository, number
        )
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=identity,
            observed_revision=result["head_revision"],
            evidence={"kind": "change-read", **result},
            closed_references=(identity,) if result["merged"] else (),
            result=result,
        )

    def _change_list(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        self._read_target(request, resource_id="changes")
        parameters = self._list_parameters(request, "change.list")
        query = urlencode(
            {
                "state": parameters["state"],
                "sort": "updated",
                "direction": "desc",
                "per_page": str(parameters["limit"]),
                "page": str(parameters["cursor"]),
            }
        )
        response = self._call(
            invocation,
            "GET",
            _repository_endpoint(request.mapping.repository, f"pulls?{query}"),
        )
        if response.status != 200:
            return self._failure_for_response(invocation, response)
        raw_items = _array(response.body, subject="pull list")
        if len(raw_items) > parameters["limit"]:
            raise GitHubProviderError("GitHub pull list exceeds its requested limit")
        items = [
            self._normalize_change(value, request, require_merged=False)
            for value in raw_items
        ]
        result = {
            "items": items,
            "next_cursor": self._next_cursor(parameters, len(raw_items)),
        }
        identity = f"github:{request.mapping.host}/{request.mapping.repository}/pulls"
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=identity,
            observed_revision=None,
            evidence={"kind": "change-list", **result},
            closed_references=(),
            result=result,
        )

    def _change_parameters(self, request: ServiceOperationRequest) -> dict[str, Any]:
        parameters = _exact_parameters(
            request.parameters,
            fields={"base_ref", "body", "draft", "head_ref", "title"},
            capability="change.open",
        )
        base_ref = _branch_ref(parameters["base_ref"], field="base_ref")
        head_ref = _branch_ref(parameters["head_ref"], field="head_ref")
        if base_ref == head_ref or request.target.resource_id != head_ref:
            raise GitHubProviderError("change.open refs are invalid")
        try:
            title = require_string(parameters["title"], field="title", max_bytes=512)
            body = require_string(parameters["body"], field="body", max_bytes=32768)
        except ValidationError as exc:
            raise GitHubProviderError(str(exc)) from exc
        if not isinstance(parameters["draft"], bool):
            raise GitHubProviderError("change.open draft must be a boolean")
        if (
            request.target.expected_revision is None
            or request.target.desired_revision is None
        ):
            raise GitHubProviderError("change.open requires exact revisions")
        return {
            "base_ref": base_ref,
            "body": body,
            "draft": parameters["draft"],
            "head_ref": head_ref,
            "title": title,
        }

    def _pulls_endpoint(
        self,
        request: ServiceOperationRequest,
        params: dict[str, Any],
        *,
        page: int = 1,
    ) -> str:
        owner = request.mapping.repository.split("/", 1)[0]
        head = params["head_ref"].removeprefix("refs/heads/")
        query_parameters = {
            "state": "all",
            "head": f"{owner}:{head}",
            "base": params["base_ref"].removeprefix("refs/heads/"),
            "per_page": "100",
        }
        if page > 1:
            query_parameters["page"] = str(page)
        query = urlencode(query_parameters)
        return _repository_endpoint(request.mapping.repository, f"pulls?{query}")

    def _find_change(
        self, invocation: _Invocation, parameters: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, ServiceOperationResponse | None]:
        response = self._call(
            invocation,
            "GET",
            self._pulls_endpoint(invocation.request, parameters),
        )
        if response.status != 200:
            return None, self._failure_for_response(invocation, response)
        values = _array(response.body, subject="pull list")
        if len(values) == 100:
            second_page = self._call(
                invocation,
                "GET",
                self._pulls_endpoint(invocation.request, parameters, page=2),
            )
            if second_page.status != 200:
                return None, self._failure_for_response(invocation, second_page)
            if _array(second_page.body, subject="pull list"):
                raise GitHubProviderError("GitHub pull list exceeds the bounded page")
        desired = invocation.request.target.desired_revision
        for value in values:
            item = self._normalize_pull(value, require_merged=False)
            if (
                item["head_sha"] == desired
                and item["head_ref"]
                == parameters["head_ref"].removeprefix("refs/heads/")
                and item["base_ref"]
                == parameters["base_ref"].removeprefix("refs/heads/")
            ):
                return item, None
        return None, None

    def _change_open(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        parameters = self._change_parameters(request)
        existing, failure = self._find_change(invocation, parameters)
        if failure is not None:
            return failure
        if existing is not None:
            return self._change_success(invocation, existing, provider_status=200)
        if request.phase == "reconcile":
            return self._failure(
                invocation,
                outcome="not_found",
                provider_status=200,
                error_code="github-change-not-found",
            )

        base, failure = self._get_ref(invocation, parameters["base_ref"])
        if failure is not None:
            return failure
        head, failure = self._get_ref(invocation, parameters["head_ref"])
        if failure is not None:
            return failure
        if (
            base != request.target.expected_revision
            or head != request.target.desired_revision
        ):
            return self._failure(
                invocation,
                outcome="conflict",
                provider_status=200,
                error_code="github-revision-conflict",
            )
        response = self._call(
            invocation,
            "POST",
            _repository_endpoint(request.mapping.repository, "pulls"),
            body={
                "base": parameters["base_ref"].removeprefix("refs/heads/"),
                "body": parameters["body"],
                "draft": parameters["draft"],
                "head": (
                    f"{request.mapping.repository.split('/', 1)[0]}:"
                    f"{parameters['head_ref'].removeprefix('refs/heads/')}"
                ),
                "title": parameters["title"],
            },
        )
        if response.status != 201:
            return self._failure_for_response(invocation, response)
        return self._change_success(
            invocation,
            self._normalize_pull(response.body, require_merged=False),
            provider_status=201,
        )

    def _change_success(
        self,
        invocation: _Invocation,
        pull: dict[str, Any],
        *,
        provider_status: int,
    ) -> ServiceOperationResponse:
        request = invocation.request
        if pull["head_sha"] != request.target.desired_revision:
            return self._failure(
                invocation,
                outcome="conflict",
                provider_status=provider_status,
                error_code="github-revision-conflict",
            )
        identity = github_remote_identity(
            request.mapping.host, request.mapping.repository, pull["number"]
        )
        return self._success(
            invocation,
            provider_status=provider_status,
            remote_identity=identity,
            observed_revision=pull["head_sha"],
            evidence={"kind": "change", **pull},
            closed_references=(identity,) if pull["merged"] else (),
        )

    def _checks_parameters(self, request: ServiceOperationRequest) -> dict[str, Any]:
        parameters = _exact_parameters(
            request.parameters,
            fields={"required_approvals", "required_checks"},
            capability="checks.read",
        )
        approvals = parameters["required_approvals"]
        if (
            isinstance(approvals, bool)
            or not isinstance(approvals, int)
            or not 0 <= approvals <= MAX_REQUIRED_APPROVALS
        ):
            raise GitHubProviderError("required_approvals is invalid")
        raw_checks = parameters["required_checks"]
        if not isinstance(raw_checks, list) or len(raw_checks) > MAX_REQUIRED_CHECKS:
            raise GitHubProviderError("required_checks is invalid")
        try:
            checks = tuple(
                require_string(item, field="required check", max_bytes=256)
                for item in raw_checks
            )
        except ValidationError as exc:
            raise GitHubProviderError(str(exc)) from exc
        if len(set(checks)) != len(checks):
            raise GitHubProviderError("required_checks must be unique")
        if request.target.desired_revision is None:
            raise GitHubProviderError("checks.read requires an exact head revision")
        return {"required_approvals": approvals, "required_checks": checks}

    def _checks_read(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        parameters = self._checks_parameters(request)
        number = _pull_number(request.target.resource_id)
        pull, failure = self._get_pull(invocation, number)
        if failure is not None:
            return failure
        assert pull is not None
        if pull["head_sha"] != request.target.desired_revision:
            return self._failure(
                invocation,
                outcome="conflict",
                provider_status=200,
                error_code="github-revision-conflict",
            )
        checks_response = self._call(
            invocation,
            "GET",
            _repository_endpoint(
                request.mapping.repository,
                f"commits/{pull['head_sha']}/check-runs?filter=latest&per_page=100",
            ),
        )
        if checks_response.status != 200:
            return self._failure_for_response(invocation, checks_response)
        checks = self._normalize_checks(checks_response.body)
        status_response = self._call(
            invocation,
            "GET",
            _repository_endpoint(
                request.mapping.repository,
                f"commits/{pull['head_sha']}/status?per_page=100",
            ),
        )
        if status_response.status != 200:
            return self._failure_for_response(invocation, status_response)
        statuses = self._normalize_statuses(status_response.body)
        reviews_response = self._call(
            invocation,
            "GET",
            _repository_endpoint(
                request.mapping.repository, f"pulls/{number}/reviews?per_page=100"
            ),
        )
        if reviews_response.status != 200:
            return self._failure_for_response(invocation, reviews_response)

        raw_reviews = _array(reviews_response.body, subject="reviews")
        if len(raw_reviews) == 100:
            second_page = self._call(
                invocation,
                "GET",
                _repository_endpoint(
                    request.mapping.repository,
                    f"pulls/{number}/reviews?per_page=100&page=2",
                ),
            )
            if second_page.status != 200:
                return self._failure_for_response(invocation, second_page)
            if _array(second_page.body, subject="reviews"):
                raise GitHubProviderError("GitHub reviews exceed the bounded page")

        reviews = self._normalize_reviews(raw_reviews)
        selected = {item["name"]: item for item in checks}
        for item in statuses:
            selected.setdefault(item["name"], item)
        required = parameters["required_checks"]
        required_results = [selected.get(name) for name in required]
        pending = any(
            item is None or item["status"] != "completed" or item["conclusion"] is None
            for item in required_results
        )
        failed = any(
            item is not None and item["conclusion"] in FAILED_CHECK_CONCLUSIONS
            for item in required_results
        )
        latest_reviews: dict[str, dict[str, Any]] = {}
        for review in reviews:
            if review["state"] != "COMMENTED":
                latest_reviews[review["login"]] = review
        changes_requested = any(
            item["state"] == "CHANGES_REQUESTED" for item in latest_reviews.values()
        )
        approved = sorted(
            login
            for login, item in latest_reviews.items()
            if item["state"] == "APPROVED"
        )
        if failed or changes_requested:
            return self._failure(
                invocation,
                outcome="denied",
                provider_status=200,
                error_code="github-checks-failed",
            )
        if pending or len(approved) < parameters["required_approvals"]:
            return self._failure(
                invocation,
                outcome="retryable",
                provider_status=200,
                error_code="github-checks-pending",
                retry_after_seconds=DEFAULT_RETRY_SECONDS,
            )
        identity = github_remote_identity(
            request.mapping.host, request.mapping.repository, number
        )
        evidence = {
            "approved_by": approved,
            "head_revision": pull["head_sha"],
            "kind": "checks",
            "pull_number": number,
            "required_approvals": parameters["required_approvals"],
            "required_checks": [
                {"conclusion": selected[name]["conclusion"], "name": name}
                for name in required
            ],
            "status": "success",
        }
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=identity,
            observed_revision=pull["head_sha"],
            evidence=evidence,
            closed_references=(identity,) if pull["merged"] else (),
        )

    def _ref_publish_parameters(
        self, request: ServiceOperationRequest
    ) -> dict[str, str]:
        parameters = _exact_parameters(
            request.parameters,
            fields={"source_repository"},
            capability="refs.publish",
        )
        try:
            source_repository = require_string(
                parameters["source_repository"],
                field="source_repository",
                max_bytes=4096,
            )
        except ValidationError as exc:
            raise GitHubProviderError(str(exc)) from exc
        if not Path(source_repository).is_absolute():
            raise GitHubProviderError("source_repository must be absolute")
        target_ref = _branch_ref(request.target.resource_id, field="target ref")
        if (
            request.target.expected_revision is not None
            or request.target.desired_revision is None
        ):
            raise GitHubProviderError(
                "refs.publish requires a new ref and exact desired revision"
            )
        return {
            "source_repository": source_repository,
            "target_ref": target_ref,
        }

    def _ref_publish(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        parameters = self._ref_publish_parameters(request)
        target_ref = parameters["target_ref"]
        observed, failure = self._get_optional_ref(invocation, target_ref)
        if failure is not None:
            return failure
        desired = request.target.desired_revision
        assert desired is not None
        if observed == desired:
            return self._ref_publication_success(invocation, target_ref, desired)
        if observed is not None:
            return self._failure(
                invocation,
                outcome="conflict",
                provider_status=200,
                error_code="github-ref-conflict",
            )
        if request.phase == "reconcile":
            return self._failure(
                invocation,
                outcome="not_found",
                provider_status=404,
                error_code="github-ref-not-found",
            )
        assert self.publisher is not None
        self.publisher.publish(
            source_repository=parameters["source_repository"],
            source_ref=f"refs/mgb/candidates/{desired}",
            desired_revision=desired,
            host=request.mapping.host,
            repository=request.mapping.repository,
            target_ref=target_ref,
            deadline=request.deadline,
        )
        invocation.effect_dispatched = True
        observed, failure = self._get_optional_ref(
            invocation, target_ref, observation_after_effect=True
        )
        if failure is not None:
            return failure
        if observed == desired:
            return self._ref_publication_success(invocation, target_ref, desired)
        if observed is not None:
            return self._failure(
                invocation,
                outcome="conflict",
                provider_status=200,
                error_code="github-ref-conflict",
            )
        return self._failure(
            invocation,
            outcome="uncertain",
            provider_status=404,
            error_code="github-ref-unobserved",
        )

    def _ref_publication_success(
        self, invocation: _Invocation, target_ref: str, observed_revision: str
    ) -> ServiceOperationResponse:
        request = invocation.request
        identity = (
            f"github:{request.mapping.host}/{request.mapping.repository}/ref/"
            f"{target_ref}"
        )
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=identity,
            observed_revision=observed_revision,
            evidence={
                "kind": "ref-publication",
                "observed_revision": observed_revision,
                "target_ref": target_ref,
            },
            closed_references=(),
        )

    def _landing_parameters(self, request: ServiceOperationRequest) -> dict[str, Any]:
        parameters = _exact_parameters(
            request.parameters,
            fields={"assurance", "integration_ref", "merge_method"},
            capability="landing.invoke",
        )
        integration_ref = _branch_ref(
            parameters["integration_ref"], field="integration_ref"
        )
        if parameters["assurance"] != "head_enforced":
            raise GitHubProviderError("GitHub landing assurance is invalid")
        if parameters["merge_method"] not in {"merge", "rebase", "squash"}:
            raise GitHubProviderError("GitHub merge method is invalid")
        if (
            request.target.expected_revision is None
            or request.target.desired_revision is None
        ):
            raise GitHubProviderError("landing.invoke requires exact revisions")
        return {
            "assurance": "head_enforced",
            "integration_ref": integration_ref,
            "merge_method": parameters["merge_method"],
        }

    def _landing(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        parameters = self._landing_parameters(request)
        number = _pull_number(request.target.resource_id)
        pull, failure = self._get_pull(invocation, number)
        if failure is not None:
            return failure
        assert pull is not None
        observed, failure = self._get_ref(invocation, parameters["integration_ref"])
        if failure is not None:
            return failure
        assert observed is not None
        if request.phase == "reconcile":
            return self._landing_settlement(invocation, pull, observed)
        if (
            pull["merged"]
            or pull["state"] != "open"
            or pull["head_sha"] != request.target.desired_revision
            or pull["base_ref"]
            != parameters["integration_ref"].removeprefix("refs/heads/")
            or observed != request.target.expected_revision
        ):
            return self._landing_settlement(invocation, pull, observed)
        merge = self._call(
            invocation,
            "PUT",
            _repository_endpoint(request.mapping.repository, f"pulls/{number}/merge"),
            body={
                "merge_method": parameters["merge_method"],
                "sha": request.target.desired_revision,
            },
        )
        if merge.status not in {200, 405, 409, 422}:
            return self._failure_for_response(invocation, merge)
        pull, failure = self._get_pull(
            invocation, number, observation_after_effect=True
        )
        if failure is not None:
            return failure
        observed, failure = self._get_ref(
            invocation,
            parameters["integration_ref"],
            observation_after_effect=True,
        )
        if failure is not None:
            return failure
        assert pull is not None and observed is not None
        return self._landing_settlement(invocation, pull, observed)

    def _landing_settlement(
        self,
        invocation: _Invocation,
        pull: dict[str, Any],
        observed_revision: str,
    ) -> ServiceOperationResponse:
        request = invocation.request
        identity = github_remote_identity(
            request.mapping.host, request.mapping.repository, pull["number"]
        )
        landed = bool(
            pull["merged"]
            and pull["head_sha"] == request.target.desired_revision
            and pull["base_ref"]
            == invocation.request.parameters["integration_ref"].removeprefix(
                "refs/heads/"
            )
        )
        evidence = {
            "base_ref": pull["base_ref"],
            "head_revision": pull["head_sha"],
            "kind": "landing",
            "landed": landed,
            "merge_commit_sha": pull["merge_commit_sha"],
            "merged_at": pull["merged_at"],
            "observed_revision": observed_revision,
            "pull_number": pull["number"],
            "state": pull["state"],
        }
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=identity,
            observed_revision=observed_revision,
            evidence=evidence,
            closed_references=(identity,) if landed else (),
        )

    def _get_pull(
        self,
        invocation: _Invocation,
        number: int,
        *,
        observation_after_effect: bool = False,
    ) -> tuple[dict[str, Any] | None, ServiceOperationResponse | None]:
        response = self._call(
            invocation,
            "GET",
            _repository_endpoint(
                invocation.request.mapping.repository, f"pulls/{number}"
            ),
        )
        if response.status != 200:
            return None, self._failure_for_response(
                invocation,
                response,
                observation_after_effect=observation_after_effect,
            )
        return self._normalize_pull(response.body), None

    def _get_ref(
        self,
        invocation: _Invocation,
        ref: str,
        *,
        observation_after_effect: bool = False,
    ) -> tuple[str | None, ServiceOperationResponse | None]:
        revision, failure = self._get_optional_ref(
            invocation,
            ref,
            observation_after_effect=observation_after_effect,
        )
        if revision is None and failure is None:
            return None, self._failure(
                invocation,
                outcome="conflict",
                provider_status=404,
                error_code="github-ref-not-found",
            )
        return revision, failure

    def _get_optional_ref(
        self,
        invocation: _Invocation,
        ref: str,
        *,
        observation_after_effect: bool = False,
    ) -> tuple[str | None, ServiceOperationResponse | None]:
        response = self._call(
            invocation,
            "GET",
            _ref_endpoint(invocation.request.mapping.repository, ref),
        )
        if response.status == 404:
            return None, None
        if response.status != 200:
            return None, self._failure_for_response(
                invocation,
                response,
                observation_after_effect=observation_after_effect,
            )
        document = _object(response.body, subject="reference")
        object_value = _object(document.get("object"), subject="reference object")
        revision = object_value.get("sha")
        if (
            document.get("ref") != ref
            or object_value.get("type") != "commit"
            or not isinstance(revision, str)
            or re.fullmatch(r"[0-9a-f]{40}", revision) is None
        ):
            raise GitHubProviderError("GitHub reference response is invalid")
        return revision, None

    @staticmethod
    def _normalize_pull(
        value: object, *, require_merged: bool = True
    ) -> dict[str, Any]:
        document = _object(value, subject="pull request")
        number = document.get("number")
        if (
            isinstance(number, bool)
            or not isinstance(number, int)
            or PULL_NUMBER_RE.fullmatch(str(number)) is None
        ):
            raise GitHubProviderError("GitHub pull request response is invalid")
        state = document.get("state")
        raw_merged = document.get("merged")
        head = _object(document.get("head"), subject="pull request head")
        base = _object(document.get("base"), subject="pull request base")
        if (
            state not in {"open", "closed"}
            or (require_merged and not isinstance(raw_merged, bool))
            or (raw_merged is not None and not isinstance(raw_merged, bool))
        ):
            raise GitHubProviderError("GitHub pull request response is invalid")
        head_sha = head.get("sha")
        base_sha = base.get("sha")
        head_ref = head.get("ref")
        base_ref = base.get("ref")
        if not all(
            isinstance(item, str) for item in (head_sha, base_sha, head_ref, base_ref)
        ) or any(
            re.fullmatch(r"[0-9a-f]{40}", item) is None for item in (head_sha, base_sha)
        ):
            raise GitHubProviderError("GitHub pull request response is invalid")
        merge_commit_sha = document.get("merge_commit_sha")
        if merge_commit_sha is not None and (
            not isinstance(merge_commit_sha, str)
            or re.fullmatch(r"[0-9a-f]{40}", merge_commit_sha) is None
        ):
            raise GitHubProviderError("GitHub pull request response is invalid")
        merged_at = document.get("merged_at")
        if merged_at is not None and not isinstance(merged_at, str):
            raise GitHubProviderError("GitHub pull request response is invalid")
        merged = raw_merged if isinstance(raw_merged, bool) else merged_at is not None
        return {
            "base_ref": base_ref,
            "base_sha": base_sha,
            "head_ref": head_ref,
            "head_sha": head_sha,
            "merge_commit_sha": merge_commit_sha,
            "merged": merged,
            "merged_at": merged_at,
            "number": number,
            "state": state,
        }

    @staticmethod
    def _normalize_checks(value: object) -> list[dict[str, str | None]]:
        document = _object(value, subject="check runs")
        raw = _array(document.get("check_runs"), subject="check runs")
        total = document.get("total_count")
        if isinstance(total, bool) or not isinstance(total, int) or total < len(raw):
            raise GitHubProviderError("GitHub check runs response is invalid")
        if total > 100:
            raise GitHubProviderError("GitHub check runs exceed the bounded page")
        result: list[dict[str, str | None]] = []
        names: set[str] = set()
        for value_item in raw:
            item = _object(value_item, subject="check run")
            name = item.get("name")
            status = item.get("status")
            conclusion = item.get("conclusion")
            if (
                not isinstance(name, str)
                or not name
                or status not in {"completed", "in_progress", "queued", "waiting"}
                or (
                    conclusion is not None
                    and conclusion
                    not in SUCCESSFUL_CHECK_CONCLUSIONS | FAILED_CHECK_CONCLUSIONS
                )
            ):
                raise GitHubProviderError("GitHub check run response is invalid")
            if name in names:
                raise GitHubProviderError("GitHub check run names are ambiguous")
            names.add(name)
            result.append({"conclusion": conclusion, "name": name, "status": status})
        return result

    @staticmethod
    def _normalize_statuses(value: object) -> list[dict[str, str | None]]:
        document = _object(value, subject="commit status")
        state = document.get("state")
        if state not in {"error", "failure", "pending", "success"}:
            raise GitHubProviderError("GitHub commit status response is invalid")
        raw = _array(document.get("statuses"), subject="commit statuses")
        total = document.get("total_count", len(raw))
        if (
            isinstance(total, bool)
            or not isinstance(total, int)
            or total < len(raw)
            or total > 100
        ):
            raise GitHubProviderError("GitHub commit status response is invalid")
        result: list[dict[str, str | None]] = []
        names: set[str] = set()
        for value_item in raw:
            item = _object(value_item, subject="commit status")
            try:
                name = require_string(
                    item.get("context"), field="commit status context", max_bytes=256
                )
            except ValidationError as exc:
                raise GitHubProviderError(str(exc)) from exc
            item_state = item.get("state")
            if item_state not in {"error", "failure", "pending", "success"}:
                raise GitHubProviderError("GitHub commit status response is invalid")
            if name in names:
                raise GitHubProviderError("GitHub commit status contexts are ambiguous")
            names.add(name)
            result.append(
                {
                    "conclusion": (
                        "success"
                        if item_state == "success"
                        else "failure"
                        if item_state in {"error", "failure"}
                        else None
                    ),
                    "name": name,
                    "status": "in_progress" if item_state == "pending" else "completed",
                }
            )
        return result

    @staticmethod
    def _normalize_reviews(value: object) -> list[dict[str, Any]]:
        raw = _array(value, subject="reviews")
        if len(raw) > 100:
            raise GitHubProviderError("GitHub reviews exceed the bounded page")
        result: list[dict[str, Any]] = []
        for value_item in raw:
            item = _object(value_item, subject="review")
            user = _object(item.get("user"), subject="review user")
            login = user.get("login")
            state = item.get("state")
            identifier = item.get("id")
            submitted_at = item.get("submitted_at")
            if (
                not isinstance(login, str)
                or not login
                or state
                not in {
                    "APPROVED",
                    "CHANGES_REQUESTED",
                    "COMMENTED",
                    "DISMISSED",
                    "PENDING",
                }
                or isinstance(identifier, bool)
                or not isinstance(identifier, int)
                or (state == "PENDING" and submitted_at is not None)
                or (state != "PENDING" and not isinstance(submitted_at, str))
            ):
                raise GitHubProviderError("GitHub review response is invalid")
            if state == "PENDING":
                continue
            normalized_submitted_at = GitHubProvider._timestamp(
                submitted_at, field="review submitted_at"
            )
            result.append(
                {
                    "id": identifier,
                    "login": login,
                    "state": state,
                    "submitted_at": normalized_submitted_at,
                }
            )
        result.sort(key=lambda item: (item["submitted_at"], item["id"]))
        return result

    def _success(
        self,
        invocation: _Invocation,
        *,
        provider_status: int,
        remote_identity: str,
        observed_revision: str | None,
        evidence: Mapping[str, Any],
        closed_references: tuple[str, ...],
        result: object | None = None,
    ) -> ServiceOperationResponse:
        receipt_value: dict[str, Any] = {
            "closed_references": list(closed_references),
            "evidence_sha256": content_sha256(canonical_json(dict(evidence))),
            "observed_revision": observed_revision,
            "remote_identity": remote_identity,
        }
        if result is not None:
            receipt_value["result"] = result
        receipt = ServiceOperationReceipt.from_mapping(receipt_value)
        return ServiceOperationResponse.from_mapping(
            {
                **invocation.request.correlation_dict(),
                "error_code": None,
                "outcome": "succeeded",
                "provider_status": provider_status,
                "receipt": receipt.to_dict(),
                "request_bytes": invocation.request_bytes,
                "response_bytes": invocation.response_bytes,
                "retry_after_seconds": None,
            }
        )

    def _failure(
        self,
        invocation: _Invocation,
        *,
        outcome: str,
        error_code: str,
        provider_status: int | None = None,
        retry_after_seconds: int | None = None,
    ) -> ServiceOperationResponse:
        return ServiceOperationResponse.from_mapping(
            {
                **invocation.request.correlation_dict(),
                "error_code": error_code,
                "outcome": outcome,
                "provider_status": provider_status,
                "receipt": None,
                "request_bytes": invocation.request_bytes,
                "response_bytes": invocation.response_bytes,
                "retry_after_seconds": retry_after_seconds,
            }
        )
