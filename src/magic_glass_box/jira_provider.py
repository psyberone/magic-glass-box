from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
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
from .work_intake import WorkItemListResult

SUPPORTED_CAPABILITIES = frozenset(
    {("work.get", 1), ("work.list", 1), ("work.state", 1)}
)
PROJECT_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]{1,31}$")
ISSUE_KEY_RE = re.compile(r"^([A-Z][A-Z0-9_]{1,31})-([1-9][0-9]{0,9})$")
JIRA_ID_RE = re.compile(r"^[1-9][0-9]{0,31}$")
MAX_READ_PAGE_SIZE = 100
MAX_ADF_NODES = 4096
MAX_ADF_DEPTH = 64
MAX_BRIEF_BYTES = 65536
DEFAULT_RETRY_SECONDS = 15
ISSUE_FIELDS = ("summary", "updated", "assignee", "labels", "description")
LIST_FIELDS = ("summary", "updated", "assignee", "labels")


class JiraProviderError(ValueError):
    """A Jira provider contract or normalized upstream response is invalid."""


class JiraTransportError(RuntimeError):
    """The bounded Jira transport failed without exposing upstream content."""


@dataclass(frozen=True)
class JiraAPIResponse:
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
            raise JiraProviderError("Jira response status is invalid")
        if (
            isinstance(self.response_bytes, bool)
            or not isinstance(self.response_bytes, int)
            or self.response_bytes < 0
        ):
            raise JiraProviderError("Jira response size is invalid")
        if self.retry_after_seconds is not None and (
            isinstance(self.retry_after_seconds, bool)
            or not isinstance(self.retry_after_seconds, int)
            or not 1 <= self.retry_after_seconds <= 30 * 24 * 60 * 60
        ):
            raise JiraProviderError("Jira retry delay is invalid")


class JiraAPI(Protocol):
    def request(
        self,
        method: str,
        *,
        host: str,
        endpoint: str,
        body: Mapping[str, Any] | None,
        deadline: str,
        response_limit_bytes: int,
    ) -> JiraAPIResponse: ...


@dataclass
class _Invocation:
    request: ServiceOperationRequest
    request_bytes: int = 0
    response_bytes: int = 0
    effect_dispatched: bool = False


def _object(value: object, *, subject: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise JiraProviderError(f"{subject} must be an object")
    return value


def _array(value: object, *, subject: str) -> list[object]:
    if not isinstance(value, list):
        raise JiraProviderError(f"{subject} must be an array")
    return value


def _exact_parameters(
    value: Mapping[str, Any], *, fields: set[str], capability: str
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise JiraProviderError(f"{capability} parameters are invalid")
    return dict(value)


def _project_key(value: object) -> str:
    if not isinstance(value, str) or PROJECT_KEY_RE.fullmatch(value) is None:
        raise JiraProviderError("Jira project mapping is invalid")
    return value


def _issue_key(value: object, *, project: str) -> str:
    if not isinstance(value, str):
        raise JiraProviderError("Jira issue key is invalid")
    match = ISSUE_KEY_RE.fullmatch(value)
    if match is None or match.group(1) != project:
        raise JiraProviderError("Jira issue key is outside the mapped project")
    return value


def _timestamp(value: object) -> str:
    if isinstance(value, str) and re.search(r"[+-][0-9]{4}$", value):
        value = value[:-2] + ":" + value[-2:]
    try:
        parsed = parse_rfc3339_timestamp(value, field="Jira issue updated timestamp")
    except ValidationError as exc:
        raise JiraProviderError(str(exc)) from exc
    return (
        parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _bounded_text(value: object, *, field: str, max_bytes: int) -> str:
    try:
        return require_string(value, field=field, max_bytes=max_bytes)
    except ValidationError as exc:
        raise JiraProviderError(str(exc)) from exc


def _adf_text(value: object) -> str:
    if isinstance(value, str):
        return _bounded_text(
            value, field="Jira issue description", max_bytes=MAX_BRIEF_BYTES
        )
    document = _object(value, subject="Jira issue description")
    if document.get("type") != "doc" or document.get("version") != 1:
        raise JiraProviderError("Jira issue description is not ADF version 1")

    parts: list[str] = []
    node_count = 0
    byte_count = 0
    block_types = frozenset(
        {
            "blockquote",
            "bulletList",
            "codeBlock",
            "heading",
            "listItem",
            "orderedList",
            "panel",
            "paragraph",
            "rule",
            "table",
            "tableCell",
            "tableHeader",
            "tableRow",
        }
    )

    def append(text: str) -> None:
        nonlocal byte_count
        byte_count += len(text.encode("utf-8"))
        if byte_count > MAX_BRIEF_BYTES:
            raise JiraProviderError("Jira issue description exceeds its limit")
        parts.append(text)

    def visit(raw_node: object, depth: int) -> None:
        nonlocal node_count
        if depth > MAX_ADF_DEPTH:
            raise JiraProviderError("Jira issue description nesting exceeds its limit")
        node_count += 1
        if node_count > MAX_ADF_NODES:
            raise JiraProviderError("Jira issue description nodes exceed their limit")
        node = _object(raw_node, subject="Jira ADF node")
        node_type = _bounded_text(
            node.get("type"), field="Jira ADF node type", max_bytes=64
        )
        content = node.get("content", [])
        if not isinstance(content, list):
            raise JiraProviderError("Jira ADF node content must be an array")
        if node_type == "text":
            text = node.get("text")
            if not isinstance(text, str):
                raise JiraProviderError("Jira ADF text node is invalid")
            append(text)
        elif node_type == "hardBreak":
            append("\n")
        for child in content:
            visit(child, depth + 1)
        if node_type in block_types:
            append("\n")

    content = document.get("content")
    if not isinstance(content, list):
        raise JiraProviderError("Jira issue description content must be an array")
    for child in content:
        visit(child, 1)
    return "\n".join(
        line.strip() for line in "".join(parts).splitlines() if line.strip()
    )


class JiraProvider:
    """Narrow provider-neutral work reads backed by one Jira project."""

    def __init__(
        self,
        spec: ServiceSidecarSpec,
        api: JiraAPI,
        *,
        state_targets: Mapping[str, Mapping[str, str]] | None = None,
    ) -> None:
        if not isinstance(spec, ServiceSidecarSpec) or spec.schema_version != 2:
            raise JiraProviderError("Jira requires a service-sidecar v2 authority")
        if not spec.mappings or any(
            mapping.service != "jira"
            or mapping.resource.kind != "work_project"
            or PROJECT_KEY_RE.fullmatch(mapping.resource.external_id) is None
            for mapping in spec.mappings
        ):
            raise JiraProviderError("Jira work-project mappings are required")
        capability_keys = {
            (capability.name, capability.version) for capability in spec.capabilities
        }
        if capability_keys - SUPPORTED_CAPABILITIES:
            raise JiraProviderError("Jira sidecar capability is unsupported")
        if any(
            capability.mode != ("effect" if capability.name == "work.state" else "read")
            for capability in spec.capabilities
        ):
            raise JiraProviderError("Jira capability mode is invalid")
        if not hasattr(api, "request"):
            raise JiraProviderError("Jira API transport is invalid")
        self.spec = spec
        self.api = api
        self.state_targets = self._validate_state_targets(state_targets)

    def _validate_state_targets(
        self, value: Mapping[str, Mapping[str, str]] | None
    ) -> dict[tuple[str, str], str]:
        supports_state = any(
            (capability.name, capability.version) == ("work.state", 1)
            for capability in self.spec.capabilities
        )
        if value is None:
            if supports_state:
                raise JiraProviderError("Jira state policy is required")
            return {}
        if not isinstance(value, dict):
            raise JiraProviderError("Jira state policy is invalid")
        normalized: dict[tuple[str, str], str] = {}
        for raw_project, raw_states in value.items():
            project = _project_key(raw_project)
            if not isinstance(raw_states, dict) or set(raw_states) != {"done"}:
                raise JiraProviderError("Jira state policy is invalid")
            status_id = raw_states["done"]
            if (
                not isinstance(status_id, str)
                or JIRA_ID_RE.fullmatch(status_id) is None
            ):
                raise JiraProviderError("Jira state target is invalid")
            normalized[(project, "done")] = status_id
        required_projects = {
            mapping.resource.external_id for mapping in self.spec.mappings
        }
        if supports_state and any(
            (project, "done") not in normalized for project in required_projects
        ):
            raise JiraProviderError("Jira state policy does not cover every mapping")
        return normalized

    def invoke(self, request: ServiceOperationRequest) -> ServiceOperationResponse:
        if not isinstance(request, ServiceOperationRequest):
            raise JiraProviderError("Jira operation request is invalid")
        invocation = _Invocation(request)
        try:
            self._validate_authority(request)
            if request.capability.name == "work.get":
                return self._work_get(invocation)
            if request.capability.name == "work.list":
                return self._work_list(invocation)
            if request.capability.name == "work.state":
                return self._work_state(invocation)
            raise JiraProviderError("Jira capability is unsupported")
        except JiraTransportError:
            if invocation.effect_dispatched:
                return self._failure(
                    invocation,
                    outcome="uncertain",
                    error_code="jira-state-ambiguous",
                )
            return self._failure(
                invocation,
                outcome="retryable",
                error_code="jira-transport-unavailable",
                retry_after_seconds=DEFAULT_RETRY_SECONDS,
            )
        except (JiraProviderError, ValidationError):
            if invocation.effect_dispatched:
                return self._failure(
                    invocation,
                    outcome="uncertain",
                    error_code="jira-state-ambiguous",
                )
            return self._failure(
                invocation,
                outcome="denied",
                error_code="jira-request-invalid",
            )

    def _validate_authority(self, request: ServiceOperationRequest) -> None:
        if request.schema_version != 2:
            raise JiraProviderError("Jira operation must use service schema v2")
        if request.connector_protocol_version != self.spec.connector_protocol_version:
            raise JiraProviderError("Jira connector protocol does not match")
        if request.deployment_sha256 != self.spec.deployment_sha256:
            raise JiraProviderError("Jira deployment does not match")
        if request.request_limit_bytes > self.spec.request_bytes:
            raise JiraProviderError("Jira request limit exceeds authority")
        if request.response_limit_bytes > self.spec.response_bytes:
            raise JiraProviderError("Jira response limit exceeds authority")
        mapping = next(
            (item for item in self.spec.mappings if item.name == request.mapping.name),
            None,
        )
        if mapping is None or mapping != request.mapping:
            raise JiraProviderError("Jira mapping does not match authority")
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
            raise JiraProviderError("Jira capability does not match authority")
        if (
            request.mapping.resource.kind != "work_project"
            or request.target.resource != request.mapping.resource
            or request.target.object_format is not None
            or request.target.expected_revision is not None
            or request.target.desired_revision is not None
        ):
            raise JiraProviderError("Jira read target is invalid")
        if request.capability.mode == "read" and request.phase != "invoke":
            raise JiraProviderError("Jira reads cannot reconcile")

    def _call(
        self,
        invocation: _Invocation,
        method: str,
        endpoint: str,
        *,
        body: Mapping[str, Any] | None = None,
    ) -> JiraAPIResponse:
        request = invocation.request
        raw_body = b"" if body is None else canonical_json(dict(body)).encode("utf-8")
        next_request_bytes = invocation.request_bytes + len(raw_body)
        if next_request_bytes > request.request_limit_bytes:
            invocation.request_bytes = request.request_limit_bytes
            raise JiraProviderError("Jira request budget is exhausted")
        invocation.request_bytes = next_request_bytes
        remaining = request.response_limit_bytes - invocation.response_bytes
        if remaining < 1:
            raise JiraTransportError("Jira response budget is exhausted")
        if request.capability.mode == "effect" and method not in {"GET", "HEAD"}:
            invocation.effect_dispatched = True
        response = self.api.request(
            method,
            host=request.mapping.host,
            endpoint=endpoint,
            body=body,
            deadline=request.deadline,
            response_limit_bytes=remaining,
        )
        if not isinstance(response, JiraAPIResponse):
            raise JiraTransportError("Jira transport response is invalid")
        next_response_bytes = invocation.response_bytes + response.response_bytes
        if next_response_bytes > request.response_limit_bytes:
            invocation.response_bytes = request.response_limit_bytes
            raise JiraTransportError("Jira response budget is exhausted")
        invocation.response_bytes = next_response_bytes
        return response

    def _failure_for_response(
        self, invocation: _Invocation, response: JiraAPIResponse
    ) -> ServiceOperationResponse:
        if response.status == 429 or response.retry_after_seconds is not None:
            return self._failure(
                invocation,
                outcome="throttled",
                provider_status=response.status,
                error_code="jira-throttled",
                retry_after_seconds=response.retry_after_seconds
                or DEFAULT_RETRY_SECONDS,
            )
        if response.status in {401, 403}:
            return self._failure(
                invocation,
                outcome="denied",
                provider_status=response.status,
                error_code="jira-denied",
            )
        if response.status >= 500:
            return self._failure(
                invocation,
                outcome="retryable",
                provider_status=response.status,
                error_code="jira-upstream-unavailable",
                retry_after_seconds=DEFAULT_RETRY_SECONDS,
            )
        return self._failure(
            invocation,
            outcome="conflict",
            provider_status=response.status,
            error_code="jira-conflict",
        )

    @staticmethod
    def _labels(value: object) -> tuple[str, ...]:
        raw = _array(value, subject="Jira issue labels")
        if len(raw) > 64:
            raise JiraProviderError("Jira issue labels exceed their limit")
        labels = tuple(
            _bounded_text(item, field="Jira issue label", max_bytes=128) for item in raw
        )
        if len(set(labels)) != len(labels):
            raise JiraProviderError("Jira issue labels are ambiguous")
        return labels

    @staticmethod
    def _assignees(value: object) -> tuple[str, ...]:
        if value is None:
            return ()
        assignee = _object(value, subject="Jira issue assignee")
        return (
            _bounded_text(
                assignee.get("accountId"),
                field="Jira issue assignee accountId",
                max_bytes=256,
            ),
        )

    def _normalize_issue(
        self,
        value: object,
        request: ServiceOperationRequest,
        *,
        include_brief: bool,
    ) -> tuple[dict[str, Any], WorkItemEnvelope]:
        document = _object(value, subject="Jira issue")
        project = _project_key(request.mapping.resource.external_id)
        key = _issue_key(document.get("key"), project=project)
        fields = _object(document.get("fields"), subject="Jira issue fields")
        title = _bounded_text(
            fields.get("summary"), field="Jira issue summary", max_bytes=512
        )
        labels = self._labels(fields.get("labels"))
        assignees = self._assignees(fields.get("assignee"))
        updated_at = _timestamp(fields.get("updated"))
        brief = title
        if include_brief and fields.get("description") is not None:
            brief = _adf_text(fields["description"])
            if not brief.strip():
                brief = title
        external_id = f"{request.mapping.host}/browse/{key}"
        try:
            envelope = WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "jira",
                    "external_id": external_id,
                    "source_updated_at": updated_at,
                    "title": title,
                    "brief": brief,
                    "labels": list(labels),
                }
            )
        except ValidationError as exc:
            raise JiraProviderError(str(exc)) from exc
        return (
            {
                "assignees": list(assignees),
                "external_id": external_id,
                "key": key,
                "labels": list(labels),
                "title": title,
                "updated_at": envelope.source_updated_at,
            },
            envelope,
        )

    def _work_get(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        _exact_parameters(request.parameters, fields=set(), capability="work.get")
        project = _project_key(request.mapping.resource.external_id)
        key = _issue_key(request.target.object_id, project=project)
        query = urlencode({"fields": ",".join(ISSUE_FIELDS)})
        response = self._call(
            invocation,
            "GET",
            f"/rest/api/3/issue/{quote(key, safe='')}?{query}",
        )
        if response.status != 200:
            return self._failure_for_response(invocation, response)
        summary, envelope = self._normalize_issue(
            response.body, request, include_brief=True
        )
        if summary["key"] != key:
            raise JiraProviderError("Jira issue response does not match its target")
        result = envelope.to_dict()
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=f"jira:{request.mapping.host}/{key}",
            evidence={"kind": "work", **result},
            result=result,
        )

    def _work_list(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        if request.target.object_id != "issues":
            raise JiraProviderError("Jira work list target is invalid")
        parameters = _exact_parameters(
            request.parameters, fields={"limit"}, capability="work.list"
        )
        limit = parameters["limit"]
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_READ_PAGE_SIZE
        ):
            raise JiraProviderError("work.list limit is invalid")
        project = _project_key(request.mapping.resource.external_id)
        body = {
            "fields": list(LIST_FIELDS),
            "jql": f'project = "{project}" ORDER BY updated DESC, key ASC',
            "maxResults": limit,
        }
        response = self._call(
            invocation,
            "POST",
            "/rest/api/3/search/jql",
            body=body,
        )
        if response.status != 200:
            return self._failure_for_response(invocation, response)
        page = _object(response.body, subject="Jira issue search")
        raw_items = _array(page.get("issues"), subject="Jira issue search items")
        if len(raw_items) > limit:
            raise JiraProviderError("Jira issue search exceeds its requested limit")
        is_last = page.get("isLast")
        if not isinstance(is_last, bool):
            raise JiraProviderError("Jira issue search pagination is invalid")
        if not is_last:
            token = page.get("nextPageToken")
            if (
                not isinstance(token, str)
                or not token
                or len(token.encode("utf-8")) > 4096
            ):
                raise JiraProviderError("Jira issue search pagination is invalid")
        items: list[dict[str, Any]] = []
        for value in raw_items:
            summary, _envelope = self._normalize_issue(
                value, request, include_brief=False
            )
            items.append(
                {
                    "assignees": summary["assignees"],
                    "external_id": summary["external_id"],
                    "labels": summary["labels"],
                    "object_id": summary["key"],
                    "source_updated_at": summary["updated_at"],
                    "title": summary["title"],
                }
            )
        try:
            result = WorkItemListResult.from_mapping(
                {"schema_version": 1, "items": items, "truncated": not is_last}
            ).to_dict()
        except ValidationError as exc:
            raise JiraProviderError(str(exc)) from exc
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=f"jira:{request.mapping.host}/projects/{project}/issues",
            evidence={"kind": "work-list", **result},
            result=result,
        )

    def _status(
        self,
        invocation: _Invocation,
        key: str,
        *,
        observation_after_effect: bool = False,
    ) -> tuple[str | None, ServiceOperationResponse | None]:
        response = self._call(
            invocation,
            "GET",
            f"/rest/api/3/issue/{quote(key, safe='')}?fields=status",
        )
        if response.status != 200:
            if observation_after_effect:
                return None, self._failure(
                    invocation,
                    outcome="uncertain",
                    provider_status=response.status,
                    error_code="jira-state-ambiguous",
                )
            return None, self._failure_for_response(invocation, response)
        document = _object(response.body, subject="Jira issue status")
        if document.get("key") != key:
            raise JiraProviderError("Jira issue status does not match its target")
        fields = _object(document.get("fields"), subject="Jira issue status fields")
        status = _object(fields.get("status"), subject="Jira issue status")
        status_id = status.get("id")
        if not isinstance(status_id, str) or JIRA_ID_RE.fullmatch(status_id) is None:
            raise JiraProviderError("Jira issue status is invalid")
        return status_id, None

    def _transition_id(
        self,
        invocation: _Invocation,
        key: str,
        target_status_id: str,
    ) -> tuple[str | None, ServiceOperationResponse | None]:
        response = self._call(
            invocation,
            "GET",
            f"/rest/api/3/issue/{quote(key, safe='')}/transitions",
        )
        if response.status != 200:
            return None, self._failure_for_response(invocation, response)
        document = _object(response.body, subject="Jira transitions")
        transitions = _array(
            document.get("transitions"), subject="Jira transitions list"
        )
        if len(transitions) > 100:
            raise JiraProviderError("Jira transitions exceed their limit")
        matches: list[str] = []
        for raw_transition in transitions:
            transition = _object(raw_transition, subject="Jira transition")
            transition_id = transition.get("id")
            destination = _object(
                transition.get("to"), subject="Jira transition destination"
            )
            destination_id = destination.get("id")
            if (
                not isinstance(transition_id, str)
                or JIRA_ID_RE.fullmatch(transition_id) is None
                or not isinstance(destination_id, str)
                or JIRA_ID_RE.fullmatch(destination_id) is None
            ):
                raise JiraProviderError("Jira transition is invalid")
            if destination_id == target_status_id:
                matches.append(transition_id)
        if len(matches) != 1:
            return None, self._failure(
                invocation,
                outcome="conflict",
                provider_status=200,
                error_code="jira-transition-unavailable",
            )
        return matches[0], None

    def _state_success(
        self, invocation: _Invocation, *, key: str, desired_state: str
    ) -> ServiceOperationResponse:
        result = {
            "desired_state": desired_state,
            "object_id": key,
            "schema_version": 1,
            "state": desired_state,
        }
        return self._success(
            invocation,
            provider_status=200,
            remote_identity=f"jira:{invocation.request.mapping.host}/{key}",
            evidence={"kind": "work-state", **result},
            result=result,
        )

    def _work_state(self, invocation: _Invocation) -> ServiceOperationResponse:
        request = invocation.request
        parameters = _exact_parameters(
            request.parameters, fields={"desired_state"}, capability="work.state"
        )
        desired_state = parameters["desired_state"]
        if desired_state != "done":
            raise JiraProviderError("Jira desired state is unsupported")
        project = _project_key(request.mapping.resource.external_id)
        key = _issue_key(request.target.object_id, project=project)
        target_status_id = self.state_targets.get((project, desired_state))
        if target_status_id is None:
            raise JiraProviderError("Jira desired state is not mapped")
        observed, failure = self._status(invocation, key)
        if failure is not None:
            return failure
        if observed == target_status_id:
            return self._state_success(invocation, key=key, desired_state=desired_state)
        if request.phase == "reconcile":
            return self._failure(
                invocation,
                outcome="not_found",
                provider_status=200,
                error_code="jira-state-not-observed",
            )
        transition_id, failure = self._transition_id(invocation, key, target_status_id)
        if failure is not None:
            return failure
        assert transition_id is not None
        response = self._call(
            invocation,
            "POST",
            f"/rest/api/3/issue/{quote(key, safe='')}/transitions",
            body={"transition": {"id": transition_id}},
        )
        if response.status != 204:
            if response.status >= 500 or 200 <= response.status < 300:
                return self._failure(
                    invocation,
                    outcome="uncertain",
                    provider_status=response.status,
                    error_code="jira-state-ambiguous",
                )
            return self._failure_for_response(invocation, response)
        observed, failure = self._status(invocation, key, observation_after_effect=True)
        if failure is not None:
            return failure
        if observed != target_status_id:
            return self._failure(
                invocation,
                outcome="uncertain",
                provider_status=200,
                error_code="jira-state-ambiguous",
            )
        return self._state_success(invocation, key=key, desired_state=desired_state)

    def _success(
        self,
        invocation: _Invocation,
        *,
        provider_status: int,
        remote_identity: str,
        evidence: Mapping[str, Any],
        result: object,
    ) -> ServiceOperationResponse:
        receipt = ServiceOperationReceipt.from_mapping(
            {
                "closed_references": [],
                "evidence_sha256": content_sha256(canonical_json(dict(evidence))),
                "observed_revision": None,
                "remote_identity": remote_identity,
                "result": result,
            }
        )
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
