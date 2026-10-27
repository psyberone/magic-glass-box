from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from magic_glass_box.domain import canonical_json, content_sha256
from magic_glass_box.jira_provider import JiraProvider, JiraTransportError
from magic_glass_box.jira_provider_cli import (
    JiraHTTP,
    JiraProviderCLIError,
    load_credential,
    load_spec,
    main,
    serve_unix,
    validate_spec_credentials,
)
from magic_glass_box.service_sidecar import UnixServiceSidecarClient
from tests.test_jira_provider import (
    FakeJiraAPI,
    api_response,
    issue,
    request,
    sidecar_spec,
    status_issue,
)


class FakeHTTPResponse:
    def __init__(
        self,
        status: int,
        body: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.body = body
        self.headers = headers or {}
        self.read_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return self.body if size < 0 else self.body[:size]

    def __enter__(self) -> FakeHTTPResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeOpener:
    def __init__(self, responses: list[object]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[object, float]] = []

    def open(self, outbound: object, *, timeout: float) -> object:
        self.calls.append((outbound, timeout))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class BrokenHTTPResponse(FakeHTTPResponse):
    def read(self, size: int = -1) -> bytes:
        raise OSError("private read failure")


class JiraProviderCLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.credential_path = self.root / "jira-credential.json"
        self.token = "jira-secret-api-token"  # noqa: S105 - synthetic fixture
        self.write_credential()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_credential(self, **updates: object) -> Path:
        value: dict[str, object] = {
            "schema_version": 1,
            "base_url": "https://acme.atlassian.net",
            "account": "operator@example.test",
            "api_token": self.token,
        }
        value.update(updates)
        self.credential_path.write_text(json.dumps(value), encoding="utf-8")
        self.credential_path.chmod(0o600)
        return self.credential_path

    def test_load_credential_returns_only_one_way_identity(self) -> None:
        credential = load_credential(self.credential_path)

        expected_scope = content_sha256(
            canonical_json(
                {
                    "account": "operator@example.test",
                    "host": "acme.atlassian.net",
                    "token_sha256": hashlib.sha256(self.token.encode()).hexdigest(),
                }
            )
        )
        self.assertEqual(credential.base_url, "https://acme.atlassian.net")
        self.assertEqual(credential.host, "acme.atlassian.net")
        self.assertEqual(credential.account, "operator@example.test")
        self.assertEqual(credential.api_token, self.token)
        self.assertEqual(
            credential.identity.to_dict(),
            {
                "account": "operator@example.test",
                "credential_scope_sha256": expected_scope,
                "host": "acme.atlassian.net",
            },
        )
        self.assertNotIn(self.token, canonical_json(credential.identity.to_dict()))

    def test_credential_file_must_be_absolute_private_regular_and_not_a_symlink(
        self,
    ) -> None:
        with self.assertRaises(JiraProviderCLIError):
            load_credential(Path("relative.json"))

        self.credential_path.chmod(0o640)
        with self.assertRaises(JiraProviderCLIError):
            load_credential(self.credential_path)

        self.write_credential()
        link = self.root / "credential-link.json"
        link.symlink_to(self.credential_path)
        with self.assertRaises(JiraProviderCLIError):
            load_credential(link)

        directory = self.root / "directory"
        directory.mkdir()
        directory.chmod(0o600)
        with self.assertRaises(JiraProviderCLIError):
            load_credential(directory)

        with (
            patch(
                "magic_glass_box.jira_provider_cli.os.fstat",
                return_value=SimpleNamespace(
                    st_mode=stat.S_IFREG | 0o600,
                    st_uid=os.getuid() + 1,
                ),
            ),
            self.assertRaises(JiraProviderCLIError),
        ):
            load_credential(self.credential_path)

    def test_credential_document_is_strict_and_bounded(self) -> None:
        bad_values = (
            {"schema_version": 2},
            {"base_url": "http://acme.atlassian.net"},
            {"base_url": "https://user@acme.atlassian.net"},
            {"base_url": "https://acme.atlassian.net/path"},
            {"base_url": "https://acme.atlassian.net?query=yes"},
            {"account": ""},
            {"account": "a" * 257},
            {"api_token": ""},
            {"extra": "not-allowed"},
        )
        for update in bad_values:
            with self.subTest(update=update):
                self.write_credential(**update)
                with self.assertRaises(JiraProviderCLIError):
                    load_credential(self.credential_path)

        self.credential_path.write_bytes(b'{"schema_version":1,"schema_version":1}')
        self.credential_path.chmod(0o600)
        with self.assertRaises(JiraProviderCLIError):
            load_credential(self.credential_path)

        for raw in (b"", b"[]", b'{"value":NaN}', b"\xff"):
            with self.subTest(raw=raw):
                self.credential_path.write_bytes(raw)
                self.credential_path.chmod(0o600)
                with self.assertRaises(JiraProviderCLIError):
                    load_credential(self.credential_path)

        for update in (
            {"base_url": "https://acme.atlassian.net:99999"},
            {"base_url": "https://acme..atlassian.net"},
            {"account": "name:with-colon"},
            {"api_token": "line-one\nline-two"},
        ):
            with self.subTest(update=update):
                self.write_credential(**update)
                with self.assertRaises(JiraProviderCLIError):
                    load_credential(self.credential_path)

    def test_credential_may_carry_private_core_state_to_status_mapping(self) -> None:
        self.write_credential(state_targets={"ABC": {"done": "10001"}})

        credential = load_credential(self.credential_path)

        self.assertEqual(credential.state_targets, {"ABC": {"done": "10001"}})
        self.assertNotIn("10001", canonical_json(credential.identity.to_dict()))
        bad_policies: tuple[object, ...] = (
            [],
            {"abc": {"done": "10001"}},
            {"ABC": {}},
            {"ABC": {"done": "0"}},
            {"ABC": {"done": "10001", "open": "1"}},
        )
        for policy in bad_policies:
            with self.subTest(policy=policy):
                self.write_credential(state_targets=policy)
                with self.assertRaises(JiraProviderCLIError):
                    load_credential(self.credential_path)

        self.credential_path.write_bytes(b"{" + b"x" * (128 * 1024))
        self.credential_path.chmod(0o600)
        with self.assertRaises(JiraProviderCLIError):
            load_credential(self.credential_path)

    def test_http_transport_builds_exact_authenticated_get(self) -> None:
        raw = canonical_json(issue()).encode("utf-8")
        upstream = FakeHTTPResponse(200, raw, {"Content-Length": str(len(raw))})
        opener = FakeOpener([upstream])
        transport = JiraHTTP(load_credential(self.credential_path), opener=opener)

        response = transport.request(
            "GET",
            host="acme.atlassian.net",
            endpoint=(
                "/rest/api/3/issue/ABC-42"
                "?fields=summary%2Cupdated%2Cassignee%2Clabels%2Cdescription"
            ),
            body=None,
            deadline="2099-01-01T00:00:00.000000Z",
            response_limit_bytes=64 * 1024,
        )

        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, issue())
        self.assertEqual(response.response_bytes, len(raw))
        self.assertEqual(upstream.read_sizes, [64 * 1024 + 1])
        self.assertEqual(len(opener.calls), 1)
        outbound, timeout = opener.calls[0]
        self.assertEqual(
            outbound.full_url,
            "https://acme.atlassian.net/rest/api/3/issue/ABC-42"
            "?fields=summary%2Cupdated%2Cassignee%2Clabels%2Cdescription",
        )
        self.assertEqual(outbound.get_method(), "GET")
        self.assertIsNone(outbound.data)
        authorization = outbound.get_header("Authorization")
        self.assertIsNotNone(authorization)
        self.assertTrue(authorization.startswith("Basic "))
        self.assertNotIn(self.token, authorization)
        self.assertGreater(timeout, 0)
        self.assertLessEqual(timeout, 120)

    def test_http_transport_posts_canonical_json_and_parses_retry_after(self) -> None:
        body = {
            "fields": ["summary"],
            "jql": 'project = "ABC"',
            "maxResults": 10,
        }
        upstream = FakeHTTPResponse(
            429,
            b'{"errorMessages":["private"]}',
            {"Retry-After": "17"},
        )
        opener = FakeOpener([upstream])
        transport = JiraHTTP(load_credential(self.credential_path), opener=opener)

        response = transport.request(
            "POST",
            host="acme.atlassian.net",
            endpoint="/rest/api/3/search/jql",
            body=body,
            deadline="2099-01-01T00:00:00.000000Z",
            response_limit_bytes=4096,
        )

        self.assertEqual(response.status, 429)
        self.assertEqual(response.retry_after_seconds, 17)
        outbound, _timeout = opener.calls[0]
        self.assertEqual(outbound.data, canonical_json(body).encode("utf-8"))
        self.assertEqual(outbound.get_header("Content-type"), "application/json")

    def test_http_transport_allows_only_bounded_transition_discovery_and_effect(
        self,
    ) -> None:
        get_response = FakeHTTPResponse(200, b'{"transitions":[]}')
        post_response = FakeHTTPResponse(204, b"")
        opener = FakeOpener([get_response, post_response])
        transport = JiraHTTP(load_credential(self.credential_path), opener=opener)

        discovered = transport.request(
            "GET",
            host="acme.atlassian.net",
            endpoint="/rest/api/3/issue/ABC-42/transitions",
            body=None,
            deadline="2099-01-01T00:00:00.000000Z",
            response_limit_bytes=4096,
        )
        applied = transport.request(
            "POST",
            host="acme.atlassian.net",
            endpoint="/rest/api/3/issue/ABC-42/transitions",
            body={"transition": {"id": "31"}},
            deadline="2099-01-01T00:00:00.000000Z",
            response_limit_bytes=4096,
        )

        self.assertEqual(discovered.status, 200)
        self.assertEqual(applied.status, 204)
        outbound, _timeout = opener.calls[1]
        self.assertEqual(
            outbound.data, canonical_json({"transition": {"id": "31"}}).encode()
        )

    def test_http_transport_allows_bounded_status_observation(self) -> None:
        raw = b'{"fields":{"status":{"id":"3"}},"key":"ABC-42"}'
        opener = FakeOpener([FakeHTTPResponse(200, raw)])
        transport = JiraHTTP(load_credential(self.credential_path), opener=opener)

        response = transport.request(
            "GET",
            host="acme.atlassian.net",
            endpoint="/rest/api/3/issue/ABC-42?fields=status",
            body=None,
            deadline="2099-01-01T00:00:00.000000Z",
            response_limit_bytes=4096,
        )

        self.assertEqual(response.status, 200)
        outbound, _timeout = opener.calls[0]
        self.assertEqual(
            outbound.full_url,
            "https://acme.atlassian.net/rest/api/3/issue/ABC-42?fields=status",
        )

    def test_provider_and_http_transport_compose_for_the_state_effect(self) -> None:
        self.write_credential(state_targets={"ABC": {"done": "10001"}})
        credential = load_credential(self.credential_path)
        upstream = (
            FakeHTTPResponse(200, canonical_json(status_issue()).encode()),
            FakeHTTPResponse(
                200,
                canonical_json(
                    {"transitions": [{"id": "31", "to": {"id": "10001"}}]}
                ).encode(),
            ),
            FakeHTTPResponse(204, b""),
            FakeHTTPResponse(
                200,
                canonical_json(status_issue(status_id="10001")).encode(),
            ),
        )
        opener = FakeOpener(list(upstream))
        socket_path = self.root / "jira-state.sock"
        provider = JiraProvider(
            sidecar_spec(socket_path, include_state=True),
            JiraHTTP(credential, opener=opener),
            state_targets=credential.state_targets,
        )

        response = provider.invoke(
            request(
                socket_path,
                "work.state",
                parameters={"desired_state": "done"},
            )
        )

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(
            [call[0].get_method() for call in opener.calls],
            ["GET", "GET", "POST", "GET"],
        )
        self.assertEqual(
            [call[0].full_url for call in opener.calls],
            [
                "https://acme.atlassian.net/rest/api/3/issue/ABC-42?fields=status",
                "https://acme.atlassian.net/rest/api/3/issue/ABC-42/transitions",
                "https://acme.atlassian.net/rest/api/3/issue/ABC-42/transitions",
                "https://acme.atlassian.net/rest/api/3/issue/ABC-42?fields=status",
            ],
        )

    def test_http_error_response_is_normalized_without_leaking_body(self) -> None:
        error = HTTPError(
            "https://acme.atlassian.net/rest/api/3/issue/ABC-42",
            403,
            "private reason",
            {"Retry-After": "9"},
            io.BytesIO(b'{"errorMessages":["private body"]}'),
        )
        transport = JiraHTTP(
            load_credential(self.credential_path), opener=FakeOpener([error])
        )
        response = transport.request(
            "GET",
            host="acme.atlassian.net",
            endpoint="/rest/api/3/issue/ABC-42?fields=summary",
            body=None,
            deadline="2099-01-01T00:00:00.000000Z",
            response_limit_bytes=4096,
        )
        self.assertEqual(response.status, 403)
        self.assertEqual(response.retry_after_seconds, 9)

    def test_http_transport_rejects_open_surface_and_bad_bounds(self) -> None:
        transport = JiraHTTP(
            load_credential(self.credential_path), opener=FakeOpener([])
        )
        bad_calls = (
            ("DELETE", "acme.atlassian.net", "/rest/api/3/issue/ABC-42", None, 100),
            ("GET", "other.atlassian.net", "/rest/api/3/issue/ABC-42", None, 100),
            ("GET", "acme.atlassian.net", "/rest/api/3/myself", None, 100),
            ("POST", "acme.atlassian.net", "/rest/api/3/search/jql?raw=yes", {}, 100),
            ("GET", "acme.atlassian.net", "/rest/api/3/issue/ABC-42", {}, 100),
            (
                "GET",
                "acme.atlassian.net",
                "/rest/api/3/issue/ABC-42?fields=summary",
                {},
                100,
            ),
            (
                "DELETE",
                "acme.atlassian.net",
                "/rest/api/3/issue/ABC-42?fields=summary",
                None,
                100,
            ),
            (
                "GET",
                "acme.atlassian.net",
                "/rest/api/3/issue/ABC-42?fields=summary#fragment",
                None,
                100,
            ),
            (
                "GET",
                "acme.atlassian.net",
                "https:/rest/api/3/issue/ABC-42?fields=summary",
                None,
                100,
            ),
            (
                "GET",
                "acme.atlassian.net",
                "//foreign/rest/api/3/issue/ABC-42?fields=summary",
                None,
                100,
            ),
            (
                "GET",
                "acme.atlassian.net",
                "/rest/api/3/issue/ABC-42?fields=summary,summary",
                None,
                100,
            ),
            ("POST", "acme.atlassian.net", "/rest/api/3/search/jql", None, 100),
            ("GET", "acme.atlassian.net", "/rest/api/3/issue/ABC-42", None, 0),
            (
                "POST",
                "acme.atlassian.net",
                "/rest/api/3/issue/ABC-42/transitions",
                {"transition": {"id": "not-numeric"}},
                100,
            ),
            (
                "POST",
                "acme.atlassian.net",
                "/rest/api/3/issue/ABC-42/transitions",
                {"transition": {"id": "31"}, "raw": True},
                100,
            ),
            (
                "POST",
                "acme.atlassian.net",
                "/rest/api/3/issue/ABC-42/transitions",
                {"transition": ["id"]},
                100,
            ),
        )
        for method, host, endpoint, body, limit in bad_calls:
            with (
                self.subTest(method=method, endpoint=endpoint),
                self.assertRaises(JiraTransportError),
            ):
                transport.request(
                    method,
                    host=host,
                    endpoint=endpoint,
                    body=body,
                    deadline="2099-01-01T00:00:00.000000Z",
                    response_limit_bytes=limit,
                )

    def test_http_transport_enforces_deadline_size_json_and_retry_bounds(self) -> None:
        cases = (
            FakeHTTPResponse(200, b"{}", {"Content-Length": "5000"}),
            FakeHTTPResponse(200, b"{}", {"Content-Length": "invalid"}),
            FakeHTTPResponse(200, b"x" * 101),
            FakeHTTPResponse(200, b"not-json"),
            FakeHTTPResponse(200, b'{"same":1,"same":2}'),
            FakeHTTPResponse(200, b"\xff"),
            FakeHTTPResponse(429, b"{}", {"Retry-After": "0"}),
            FakeHTTPResponse(99, b"{}"),
            BrokenHTTPResponse(200, b"{}"),
            object(),
            SimpleNamespace(status=200, headers={}),
            FakeHTTPResponse(200, "{}"),  # type: ignore[arg-type]
            URLError("private network failure"),
        )
        for upstream in cases:
            with self.subTest(upstream=upstream):
                transport = JiraHTTP(
                    load_credential(self.credential_path),
                    opener=FakeOpener([upstream]),
                )
                with self.assertRaises(JiraTransportError) as caught:
                    transport.request(
                        "GET",
                        host="acme.atlassian.net",
                        endpoint="/rest/api/3/issue/ABC-42?fields=summary",
                        body=None,
                        deadline="2099-01-01T00:00:00.000000Z",
                        response_limit_bytes=100,
                    )
                self.assertNotIn("private", str(caught.exception))

        transport = JiraHTTP(
            load_credential(self.credential_path), opener=FakeOpener([])
        )
        with self.assertRaises(JiraTransportError):
            transport.request(
                "GET",
                host="acme.atlassian.net",
                endpoint="/rest/api/3/issue/ABC-42?fields=summary",
                body=None,
                deadline="2000-01-01T00:00:00.000000Z",
                response_limit_bytes=100,
            )

        with self.assertRaises(JiraProviderCLIError):
            JiraHTTP("not-a-credential")  # type: ignore[arg-type]

    def test_http_transport_accepts_exact_size_and_retry_boundaries(self) -> None:
        raw = b"{}"
        exact = FakeHTTPResponse(200, raw, {"Content-Length": str(len(raw))})
        transport = JiraHTTP(
            load_credential(self.credential_path), opener=FakeOpener([exact])
        )
        response = transport.request(
            "GET",
            host="acme.atlassian.net",
            endpoint="/rest/api/3/issue/ABC-42?fields=summary",
            body=None,
            deadline="2099-01-01T00:00:00.000000Z",
            response_limit_bytes=len(raw),
        )
        self.assertEqual(response.response_bytes, len(raw))

        maximum_retry = 30 * 24 * 60 * 60
        for retry in (1, maximum_retry):
            with self.subTest(retry=retry):
                upstream = FakeHTTPResponse(
                    429,
                    raw,
                    {"Retry-After": str(retry)},
                )
                transport = JiraHTTP(
                    load_credential(self.credential_path),
                    opener=FakeOpener([upstream]),
                )
                response = transport.request(
                    "GET",
                    host="acme.atlassian.net",
                    endpoint="/rest/api/3/issue/ABC-42?fields=summary",
                    body=None,
                    deadline="2099-01-01T00:00:00.000000Z",
                    response_limit_bytes=100,
                )
                self.assertEqual(response.retry_after_seconds, retry)

        upstream = FakeHTTPResponse(
            429,
            raw,
            {"Retry-After": str(maximum_retry + 1)},
        )
        transport = JiraHTTP(
            load_credential(self.credential_path), opener=FakeOpener([upstream])
        )
        with self.assertRaises(JiraTransportError):
            transport.request(
                "GET",
                host="acme.atlassian.net",
                endpoint="/rest/api/3/issue/ABC-42?fields=summary",
                body=None,
                deadline="2099-01-01T00:00:00.000000Z",
                response_limit_bytes=100,
            )

    def test_spec_loader_rejects_untrusted_shapes_and_invalid_authority(self) -> None:
        with self.assertRaises(JiraProviderCLIError):
            load_spec(Path("relative.json"))
        with self.assertRaises(JiraProviderCLIError):
            load_spec(self.root / "missing.json")
        directory = self.root / "spec-directory"
        directory.mkdir()
        with self.assertRaises(JiraProviderCLIError):
            load_spec(directory)
        spec_path = self.root / "bad-spec.json"
        for raw in (b"", b"[]", b'{"same":1,"same":2}', b"{" + b"x" * (1024 * 1024)):
            with self.subTest(raw_size=len(raw)):
                spec_path.write_bytes(raw)
                with self.assertRaises(JiraProviderCLIError):
                    load_spec(spec_path)

        with self.assertRaises(JiraProviderCLIError):
            validate_spec_credentials(
                "not-a-spec", load_credential(self.credential_path)
            )  # type: ignore[arg-type]

    def test_spec_loading_and_credential_validation_bind_exact_identity(self) -> None:
        credential = load_credential(self.credential_path)
        raw = sidecar_spec(self.root / "jira.sock").to_dict()
        raw["mappings"][0]["credential_scope_sha256"] = (
            credential.identity.credential_scope_sha256
        )
        spec_path = self.root / "jira-spec.json"
        spec_path.write_text(json.dumps(raw), encoding="utf-8")
        spec = load_spec(spec_path)

        self.assertEqual(
            validate_spec_credentials(spec, credential), credential.identity
        )

        for field, value in (
            ("host", "other.atlassian.net"),
            ("account", "other@example.test"),
            ("credential_scope_sha256", "f" * 64),
        ):
            with self.subTest(field=field):
                changed = sidecar_spec(self.root / "jira.sock").to_dict()
                changed["mappings"][0][field] = value
                changed_spec_path = self.root / f"bad-{field}.json"
                changed_spec_path.write_text(json.dumps(changed), encoding="utf-8")
                with self.assertRaises(JiraProviderCLIError):
                    validate_spec_credentials(load_spec(changed_spec_path), credential)

    def test_real_unix_carrier_round_trip_and_cleanup(self) -> None:
        socket_path = self.root / "carrier" / "jira.sock"
        provider = JiraProvider(
            sidecar_spec(socket_path), FakeJiraAPI([api_response(200, issue())])
        )
        outcome: list[object] = []

        def run() -> None:
            try:
                outcome.append(serve_unix(socket_path, 1, provider))
            except BaseException as exc:  # pragma: no cover - surfaced below
                outcome.append(exc)

        thread = threading.Thread(target=run)
        thread.start()
        for _ in range(200):
            if socket_path.exists():
                break
            time.sleep(0.005)
        self.assertTrue(socket_path.exists())

        response = UnixServiceSidecarClient(socket_path).invoke(
            request(socket_path, "work.get")
        )
        thread.join(timeout=5)

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(outcome, [1])
        self.assertFalse(thread.is_alive())
        self.assertFalse(socket_path.exists())

    def test_carrier_rejects_socket_outside_registered_authority(self) -> None:
        provider = JiraProvider(
            sidecar_spec(self.root / "registered.sock"), FakeJiraAPI([])
        )
        with self.assertRaises(JiraProviderCLIError):
            serve_unix(self.root / "other.sock", 1, provider)

    def test_inspect_cli_prints_no_secret_and_errors_are_sanitized(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["inspect", "--credential-file", str(self.credential_path)])
        self.assertEqual(code, 0)
        self.assertEqual(stderr.getvalue(), "")
        parsed = json.loads(stdout.getvalue())
        self.assertEqual(parsed["status"], "configured")
        self.assertNotIn(self.token, stdout.getvalue())
        self.assertNotIn(str(self.credential_path), stdout.getvalue())

        self.credential_path.write_text("not-json", encoding="utf-8")
        self.credential_path.chmod(0o600)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["inspect", "--credential-file", str(self.credential_path)])
        self.assertEqual(code, 2)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("credential file is invalid", stderr.getvalue())
        self.assertNotIn("not-json", stderr.getvalue())

    def test_serve_cli_wires_private_configuration_without_printing_it(self) -> None:
        self.write_credential()
        credential = load_credential(self.credential_path)
        socket_path = self.root / "jira.sock"
        raw_spec = sidecar_spec(socket_path).to_dict()
        raw_spec["mappings"][0]["credential_scope_sha256"] = (
            credential.identity.credential_scope_sha256
        )
        spec_path = self.root / "jira-spec.json"
        spec_path.write_text(json.dumps(raw_spec), encoding="utf-8")
        stdout = io.StringIO()
        with (
            patch("magic_glass_box.jira_provider_cli.serve_unix", return_value=3),
            redirect_stdout(stdout),
        ):
            code = main(
                [
                    "serve",
                    "--credential-file",
                    str(self.credential_path),
                    "--spec",
                    str(spec_path),
                    "--socket-path",
                    str(socket_path),
                    "--max-invocations",
                    "3",
                ]
            )
        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(stdout.getvalue()), {"invocations": 3, "status": "stopped"}
        )
        self.assertNotIn(self.token, stdout.getvalue())

        stderr = io.StringIO()
        with (
            patch(
                "magic_glass_box.jira_provider_cli.load_credential",
                side_effect=OSError("private path"),
            ),
            redirect_stderr(stderr),
        ):
            code = main(["inspect", "--credential-file", str(self.credential_path)])
        self.assertEqual(code, 2)
        self.assertNotIn("private path", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
