from __future__ import annotations

import json
import threading
import unittest
from typing import Any

from magic_glass_box.domain import ProviderGrantSpec, ValidationError
from magic_glass_box.provider import (
    BufferedHTTPProviderAdapter,
    JSONHTTPProviderAdapter,
    ProviderBroker,
    ProviderBusyError,
    ProviderError,
    ProviderInvocationResult,
    ProviderUpstreamError,
)


def policy(**overrides: object) -> ProviderGrantSpec:
    value: dict[str, object] = {
        "provider": "openai",
        "adapter": "json_http",
        "destination": "https://api.openai.com/v1/responses",
        "method": "POST",
        "model": "gpt-test",
        "credential_header": "Authorization",
        "credential_prefix": "Bearer ",
        "static_headers": {"OpenAI-Organization": "org-test"},
        "request_bytes": 32768,
        "response_bytes": 1048576,
        "timeout_ms": 30000,
        "max_concurrency": 1,
    }
    value.update(overrides)
    return ProviderGrantSpec.from_mapping(value)


class ProviderGrantSpecTests(unittest.TestCase):
    def test_policy_is_canonical_and_normalizes_security_sensitive_fields(self) -> None:
        spec = policy()
        self.assertEqual(spec.provider, "openai")
        self.assertEqual(spec.method, "POST")
        self.assertEqual(spec.credential_header, "authorization")
        self.assertEqual(spec.static_headers, (("openai-organization", "org-test"),))
        self.assertEqual(ProviderGrantSpec.from_mapping(spec.to_dict()), spec)
        self.assertNotIn("secret", spec.canonical)

    def test_policy_rejects_unsafe_or_unbounded_configuration(self) -> None:
        cases = (
            ({"adapter": "arbitrary"}, "adapter"),
            ({"destination": "http://api.example.com/v1"}, "HTTPS"),
            ({"destination": "https://user:pass@example.com/v1"}, "credentials"),
            ({"destination": "https://example.com/v1#fragment"}, "fragment"),
            ({"method": "GET"}, "POST"),
            ({"credential_header": "Host"}, "credential header"),
            ({"credential_prefix": "bad\n"}, "credential prefix"),
            ({"static_headers": {"Authorization": "fixed"}}, "header"),
            ({"request_bytes": 0}, "request_bytes"),
            ({"request_bytes": 2 * 1024 * 1024 + 1}, "request_bytes"),
            ({"response_bytes": 4 * 1024 * 1024 + 1}, "response_bytes"),
            ({"timeout_ms": 99}, "timeout_ms"),
            ({"max_concurrency": 0}, "max_concurrency"),
            ({"extra": True}, "unknown fields"),
        )
        for override, message in cases:
            with (
                self.subTest(override=override),
                self.assertRaisesRegex(ValidationError, message),
            ):
                policy(**override)

    def test_vendor_stream_adapters_have_bounded_larger_envelopes(self) -> None:
        for adapter in ("openai_responses", "anthropic_messages"):
            with self.subTest(adapter=adapter):
                spec = policy(
                    adapter=adapter,
                    request_bytes=2 * 1024 * 1024,
                    response_bytes=4 * 1024 * 1024,
                )
                self.assertEqual(spec.adapter, adapter)

    def test_loopback_http_is_available_for_local_provider_adapters(self) -> None:
        spec = policy(destination="http://127.0.0.1:8080/v1/invoke")
        self.assertEqual(spec.destination, "http://127.0.0.1:8080/v1/invoke")


class RecordingAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[ProviderGrantSpec, str, dict[str, Any]]] = []
        self.started = threading.Event()
        self.release = threading.Event()
        self.block = False

    def invoke(
        self,
        spec: ProviderGrantSpec,
        credential: str,
        request: dict[str, Any],
    ) -> ProviderInvocationResult:
        self.calls.append((spec, credential, request))
        self.started.set()
        if self.block:
            self.release.wait(2)
        return ProviderInvocationResult(
            status_code=200,
            request_bytes=41,
            response_bytes=32,
            untrusted_response={"output_text": "done"},
        )


class ProviderBrokerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = RecordingAdapter()
        self.broker = ProviderBroker(adapters={"json_http": self.adapter})
        self.spec = policy()

    def test_invocation_uses_the_pinned_adapter_and_returns_bounded_json(self) -> None:
        result = self.broker.invoke(
            grant_id="pgr_" + "1" * 32,
            spec=self.spec,
            credential="provider-secret",
            request={"input": "hello"},
        )
        self.assertEqual(result.untrusted_response, {"output_text": "done"})
        self.assertEqual(
            self.adapter.calls,
            [(self.spec, "provider-secret", {"input": "hello"})],
        )

    def test_model_override_and_non_object_requests_fail_closed(self) -> None:
        for request, message in (
            ({"model": "other"}, "model"),
            (["not", "an", "object"], "object"),
            ({"input": float("nan")}, "finite JSON"),
            ({"input": "x" * 32768}, "size limit"),
        ):
            with (
                self.subTest(request=request),
                self.assertRaisesRegex(ProviderError, message),
            ):
                self.broker.invoke(
                    grant_id="pgr_" + "1" * 32,
                    spec=self.spec,
                    credential="provider-secret",
                    request=request,  # type: ignore[arg-type]
                )
        self.assertEqual(self.adapter.calls, [])

    def test_concurrency_limit_rejects_instead_of_queueing(self) -> None:
        self.adapter.block = True
        first = threading.Thread(
            target=self.broker.invoke,
            kwargs={
                "grant_id": "pgr_" + "1" * 32,
                "spec": self.spec,
                "credential": "provider-secret",
                "request": {"input": "first"},
            },
        )
        first.start()
        self.assertTrue(self.adapter.started.wait(1))
        try:
            with self.assertRaises(ProviderBusyError):
                self.broker.invoke(
                    grant_id="pgr_" + "1" * 32,
                    spec=self.spec,
                    credential="provider-secret",
                    request={"input": "second"},
                )
        finally:
            self.adapter.release.set()
            first.join(2)
        self.assertFalse(first.is_alive())

    def test_unknown_adapter_and_invalid_results_are_redacted(self) -> None:
        with self.assertRaisesRegex(ProviderError, "unavailable"):
            ProviderBroker(adapters={}).invoke(
                grant_id="pgr_" + "1" * 32,
                spec=self.spec,
                credential="secret",
                request={"input": "hello"},
            )

        class BadAdapter:
            def invoke(self, *args: object, **kwargs: object) -> object:
                return {"secret": "leak"}

        with self.assertRaisesRegex(ProviderError, "invalid response"):
            ProviderBroker(adapters={"json_http": BadAdapter()}).invoke(
                grant_id="pgr_" + "1" * 32,
                spec=self.spec,
                credential="secret",
                request={"input": "hello"},
            )


class FakeResponse:
    def __init__(
        self,
        *,
        status: int = 200,
        body: bytes = b'{"output_text":"done"}',
        content_length: str | None = None,
        content_type: str | None = "application/json",
    ):
        self.status = status
        self.body = body
        self.content_length = content_length
        self.content_type = content_type

    def getheader(self, name: str) -> str | None:
        if name == "Content-Length":
            return self.content_length
        if name == "Content-Type":
            return self.content_type
        raise AssertionError(f"unexpected header lookup: {name}")

    def read(self, amount: int) -> bytes:
        return self.body[:amount]


class FakeConnection:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.requests: list[tuple[str, str, bytes, dict[str, str]]] = []
        self.closed = False
        self.failure: Exception | None = None

    def request(
        self, method: str, target: str, *, body: bytes, headers: dict[str, str]
    ) -> None:
        if self.failure is not None:
            raise self.failure
        self.requests.append((method, target, body, headers))

    def getresponse(self) -> FakeResponse:
        return self.response

    def close(self) -> None:
        self.closed = True


class JSONHTTPProviderAdapterTests(unittest.TestCase):
    def adapter(
        self, response: FakeResponse
    ) -> tuple[JSONHTTPProviderAdapter, FakeConnection, list[tuple[object, ...]]]:
        connection = FakeConnection(response)
        calls: list[tuple[object, ...]] = []

        def factory(
            scheme: str, host: str, port: int | None, timeout: float
        ) -> FakeConnection:
            calls.append((scheme, host, port, timeout))
            return connection

        return JSONHTTPProviderAdapter(factory), connection, calls

    def test_request_uses_only_policy_pinned_transport_and_model(self) -> None:
        adapter, connection, calls = self.adapter(FakeResponse(content_length="22"))
        spec = policy(
            destination="https://api.example:8443/v1/invoke?mode=sync",
            static_headers={"X-Tenant": "tenant-one"},
        )

        result = adapter.invoke(spec, "private-key", {"input": "hello"})

        self.assertEqual(calls, [("https", "api.example", 8443, 30.0)])
        self.assertTrue(connection.closed)
        method, target, body, headers = connection.requests[0]
        self.assertEqual(method, "POST")
        self.assertEqual(target, "/v1/invoke?mode=sync")
        self.assertEqual(json.loads(body), {"input": "hello", "model": "gpt-test"})
        self.assertEqual(headers["authorization"], "Bearer private-key")
        self.assertEqual(headers["x-tenant"], "tenant-one")
        self.assertEqual(headers["content-length"], str(len(body)))
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.response_bytes, 22)
        self.assertEqual(result.untrusted_response, {"output_text": "done"})

    def test_transport_status_and_size_failures_are_metadata_only(self) -> None:
        cases = (
            (FakeResponse(status=429, body=b'{"error":"limited"}'), {}, 429),
            (FakeResponse(content_length="not-an-int"), {}, 200),
            (FakeResponse(content_length="-1"), {}, 200),
            (
                FakeResponse(content_length="9", body=b'{"long":"response"}'),
                {"response_bytes": 8},
                200,
            ),
            (
                FakeResponse(body=b'{"long":"response"}'),
                {"response_bytes": 8},
                200,
            ),
        )
        for response, overrides, expected_status in cases:
            with self.subTest(response=response, overrides=overrides):
                adapter, connection, _ = self.adapter(response)
                with self.assertRaises(ProviderUpstreamError) as raised:
                    adapter.invoke(policy(**overrides), "secret", {"input": "x"})
                self.assertEqual(raised.exception.status_code, expected_status)
                self.assertNotIn("secret", str(raised.exception))
                self.assertNotIn("limited", str(raised.exception))
                self.assertTrue(connection.closed)

    def test_malformed_json_and_network_failure_are_redacted(self) -> None:
        malformed = (
            b"not-json",
            b"[]",
            b'{"duplicate":1,"duplicate":2}',
            b'{"number":NaN}',
            b"\xff",
        )
        for body in malformed:
            with self.subTest(body=body):
                adapter, connection, _ = self.adapter(FakeResponse(body=body))
                with self.assertRaisesRegex(ProviderUpstreamError, "invalid response"):
                    adapter.invoke(policy(), "secret", {"input": "x"})
                self.assertTrue(connection.closed)

        adapter, connection, _ = self.adapter(FakeResponse())
        connection.failure = OSError("SECRET network detail")
        with self.assertRaisesRegex(ProviderUpstreamError, "unavailable") as raised:
            adapter.invoke(policy(), "secret", {"input": "x"})
        self.assertNotIn("SECRET", str(raised.exception))
        self.assertTrue(connection.closed)


class BufferedHTTPProviderAdapterTests(unittest.TestCase):
    def adapter(
        self, protocol: str, response: FakeResponse
    ) -> tuple[BufferedHTTPProviderAdapter, FakeConnection]:
        connection = FakeConnection(response)

        def factory(
            _scheme: str, _host: str, _port: int | None, _timeout: float
        ) -> FakeConnection:
            return connection

        return BufferedHTTPProviderAdapter(protocol, factory), connection

    def test_streaming_request_is_model_pinned_and_response_is_buffered(self) -> None:
        raw = b'data: {"type":"response.completed"}\n\n'
        adapter, connection = self.adapter(
            "openai-responses",
            FakeResponse(
                body=raw,
                content_length=str(len(raw)),
                content_type="text/event-stream; charset=utf-8",
            ),
        )

        result = adapter.invoke(
            policy(adapter="openai_responses", request_bytes=2 * 1024 * 1024),
            "private-key",
            {
                "protocol": "openai-responses",
                "body": {"stream": True, "input": "hello"},
                "headers": {"x-codex-beta-features": "feature-one"},
            },
        )

        body = json.loads(connection.requests[0][2])
        self.assertEqual(body["model"], "gpt-test")
        self.assertTrue(body["stream"])
        self.assertEqual(connection.requests[0][3]["accept"], "text/event-stream")
        self.assertEqual(
            connection.requests[0][3]["x-codex-beta-features"], "feature-one"
        )
        self.assertEqual(
            result.untrusted_response,
            {
                "body": raw.decode(),
                "content_type": "text/event-stream; charset=utf-8",
                "protocol": "openai-responses",
            },
        )

    def test_protocol_shape_and_response_encoding_fail_closed(self) -> None:
        adapter, _ = self.adapter(
            "anthropic-messages",
            FakeResponse(body=b"\xff", content_type="text/event-stream"),
        )
        spec = policy(adapter="anthropic_messages")
        invalid = (
            (
                {
                    "protocol": "openai-responses",
                    "body": {"stream": True},
                    "headers": {},
                },
                "protocol",
            ),
            (
                {
                    "protocol": "anthropic-messages",
                    "body": [],
                    "headers": {},
                },
                "body",
            ),
            (
                {
                    "protocol": "anthropic-messages",
                    "body": {"stream": True, "model": "worker-model"},
                    "headers": {},
                },
                "model",
            ),
            (
                {
                    "protocol": "anthropic-messages",
                    "body": {"stream": False},
                    "headers": {},
                },
                "stream",
            ),
            (
                {
                    "protocol": "anthropic-messages",
                    "body": {"stream": True},
                    "headers": {"authorization": "SECRET"},
                },
                "header",
            ),
        )
        for request, message in invalid:
            with (
                self.subTest(request=request),
                self.assertRaisesRegex(ProviderError, message),
            ):
                adapter.invoke(spec, "secret", request)  # type: ignore[arg-type]

        with self.assertRaisesRegex(ProviderUpstreamError, "invalid response"):
            adapter.invoke(
                spec,
                "secret",
                {
                    "protocol": "anthropic-messages",
                    "body": {"stream": True},
                    "headers": {},
                },
            )


if __name__ == "__main__":
    unittest.main()
