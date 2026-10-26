from __future__ import annotations

import contextlib
import hashlib
import http.client
import io
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from magic_glass_box.gateway import MAX_GATEWAY_SEQUENCE, FileGatewayManager
from magic_glass_box.service import AcceptedProviderResponse
from magic_glass_box.worker_transport import (
    PROVIDER_GATEWAY_TIMEOUT_SECONDS,
    FileGatewayClient,
    ProviderHTTPTransport,
    ProviderRetryableTransportError,
    WorkerTransportError,
    _strict_json,
    _TransportHandler,
    _TransportServer,
    main,
)

RUN_ID = "run_" + "1" * 32


class ProviderControl:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def invoke_provider(
        self, run_id: str, *, grant_name: str, request: dict[str, Any]
    ) -> AcceptedProviderResponse:
        self.calls.append((run_id, grant_name, request))
        protocol = request.get("protocol", "openai-responses")
        return AcceptedProviderResponse(
            invocation_id="pvi_" + "2" * 32,
            grant_name=grant_name,
            provider="openai",
            model="pinned-model",
            status_code=200,
            request_bytes=100,
            response_bytes=120,
            untrusted_response={
                "body": 'data: {"type":"response.completed"}\n\n',
                "content_type": "text/event-stream",
                "protocol": protocol,
            },
        )


class FileGatewayClientTests(unittest.TestCase):
    def test_strict_json_rejects_nonfinite_exponent_numbers(self) -> None:
        for raw in (b'{"value":1e999}', b'{"value":-1e999}'):
            with (
                self.subTest(raw=raw),
                self.assertRaisesRegex(WorkerTransportError, "invalid JSON"),
            ):
                _strict_json(raw, subject="gateway payload JSON", limit=1024)

    def test_exchange_commits_and_verifies_multiple_mailbox_requests(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            root = Path(temporary) / "gateway"
            control = ProviderControl()
            manager = FileGatewayManager(root, control, poll_interval=0.005)
            try:
                endpoint = manager.open(
                    RUN_ID,
                    capabilities=("provider.invoke",),
                    credential_names=("openai-main",),
                )
                client = FileGatewayClient(endpoint.directory, timeout=1)

                first = client.exchange(
                    "provider.invoke",
                    {
                        "grant": "openai-main",
                        "request": {
                            "body": {"input": "hello"},
                            "protocol": "openai-responses",
                        },
                    },
                )
                denied = client.exchange(
                    "provider.invoke",
                    {
                        "grant": "other",
                        "request": {
                            "body": {"input": "no"},
                            "protocol": "openai-responses",
                        },
                    },
                )

                self.assertTrue(first["ok"])
                self.assertFalse(denied["ok"])
                self.assertEqual(denied["error"]["code"], "forbidden")
                self.assertEqual(len(control.calls), 1)
            finally:
                manager.close()

    def test_published_hyphenated_operation_reaches_the_gateway_mailbox(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            mailbox = Path(temporary) / "mailbox"
            mailbox.mkdir()
            for name in (
                "request.json",
                "request.ready",
                "response.json",
                "response.ready",
            ):
                (mailbox / name).write_bytes(b"")
            client = FileGatewayClient(mailbox, timeout=0.1)

            with mock.patch.object(
                client,
                "_wait_response",
                return_value={"ok": True},
            ):
                response = client.exchange("workflow.scope-review.submit", {})

            self.assertEqual(response, {"ok": True})
            committed = json.loads((mailbox / "request.json").read_bytes())
            self.assertEqual(committed["operation"], "workflow.scope-review.submit")

    def test_missing_or_unsafe_mailbox_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            missing = Path(temporary) / "missing"
            with self.assertRaisesRegex(WorkerTransportError, "mailbox"):
                FileGatewayClient(missing)

            mailbox = Path(temporary) / "mailbox"
            mailbox.mkdir()
            for name in (
                "request.json",
                "request.ready",
                "response.json",
                "response.ready",
            ):
                (mailbox / name).write_bytes(b"")
            os.unlink(mailbox / "response.json")
            os.symlink(mailbox / "request.json", mailbox / "response.json")
            with self.assertRaisesRegex(WorkerTransportError, "unsafe"):
                FileGatewayClient(mailbox)

    def test_invalid_client_inputs_and_an_unserved_request_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            mailbox = Path(temporary) / "mailbox"
            mailbox.mkdir()
            for name in (
                "request.json",
                "request.ready",
                "response.json",
                "response.ready",
            ):
                (mailbox / name).write_bytes(b"")
            with self.assertRaisesRegex(WorkerTransportError, "timeout"):
                FileGatewayClient(mailbox, timeout=0)

            client = FileGatewayClient(mailbox, timeout=0.1)
            with self.assertRaisesRegex(WorkerTransportError, "operation"):
                client.exchange("Bad Operation", {})
            with self.assertRaisesRegex(WorkerTransportError, "payload"):
                client.exchange("run.describe", [])  # type: ignore[arg-type]
            with self.assertRaisesRegex(WorkerTransportError, "timeout"):
                client.exchange("run.describe", {}, timeout=0)
            with self.assertRaises(WorkerTransportError) as rejected_timeout:
                client.exchange("run.describe", {}, timeout=None)
            self.assertEqual(
                str(rejected_timeout.exception),
                "gateway timeout is invalid",
            )
            self.assertEqual((mailbox / "request.ready").read_bytes(), b"")

            with mock.patch.object(
                client,
                "_wait_response",
                return_value={"ok": True},
            ) as wait_response:
                self.assertEqual(
                    client.exchange("run.describe", {}, timeout=300),
                    {"ok": True},
                )
                wait_response.assert_called_once()
                self.assertEqual(wait_response.call_args.args[2], 300.0)
                committed = (mailbox / "request.ready").read_bytes()
                with self.assertRaisesRegex(WorkerTransportError, "timeout"):
                    client.exchange("run.describe", {}, timeout=300.1)
            self.assertEqual((mailbox / "request.ready").read_bytes(), committed)

            with self.assertRaisesRegex(WorkerTransportError, "ambiguous"):
                client.exchange("run.describe", {})

    def test_sequence_exhaustion_fails_before_overwriting_the_mailbox(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            mailbox = Path(temporary) / "mailbox"
            mailbox.mkdir()
            for name in (
                "request.json",
                "request.ready",
                "response.json",
                "response.ready",
            ):
                (mailbox / name).write_bytes(b"")
            (mailbox / "request.ready").write_text(
                json.dumps(
                    {
                        "sequence": MAX_GATEWAY_SEQUENCE,
                        "sha256": "0" * 64,
                        "size": 1,
                    }
                )
            )
            client = FileGatewayClient(mailbox, timeout=0.1)

            with self.assertRaises(WorkerTransportError) as exhausted:
                client.exchange("run.describe", {})

            self.assertEqual(
                str(exhausted.exception),
                "gateway mailbox sequence is exhausted",
            )
            self.assertEqual((mailbox / "request.json").read_bytes(), b"")

    def test_provider_exchange_continues_while_the_host_publishes_progress(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            root = Path(temporary) / "gateway"
            control = ProviderControl()
            invoke = control.invoke_provider

            def delayed_invoke(*args: Any, **kwargs: Any) -> AcceptedProviderResponse:
                time.sleep(0.25)
                return invoke(*args, **kwargs)

            control.invoke_provider = delayed_invoke  # type: ignore[method-assign]
            manager = FileGatewayManager(root, control, poll_interval=0.005)
            try:
                with mock.patch(
                    "magic_glass_box.gateway.GATEWAY_PROGRESS_INTERVAL_SECONDS",
                    0.02,
                    create=True,
                ):
                    endpoint = manager.open(
                        RUN_ID,
                        capabilities=("provider.invoke",),
                        credential_names=("openai-main",),
                    )
                client = FileGatewayClient(endpoint.directory, timeout=0.1)
                transport = ProviderHTTPTransport(
                    client,
                    dialect="openai-responses",
                    grant="openai-main",
                )

                with mock.patch(
                    "magic_glass_box.worker_transport.PROVIDER_GATEWAY_TIMEOUT_SECONDS",
                    0.1,
                ):
                    response = transport.invoke(
                        "/v1/responses",
                        b'{"input":"slow","model":"worker-model","stream":true}',
                    )

                self.assertEqual(response.status_code, 200)
            finally:
                manager.close()

    def test_stale_provider_progress_cannot_extend_the_wait_forever(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            mailbox = Path(temporary) / "mailbox"
            mailbox.mkdir()
            for name in (
                "request.json",
                "request.ready",
                "response.json",
                "response.ready",
            ):
                (mailbox / name).write_bytes(b"")
            client = FileGatewayClient(mailbox, timeout=0.1)

            def publish_one_progress_frame() -> None:
                deadline = time.monotonic() + 1
                while not (mailbox / "request.ready").read_bytes():
                    if time.monotonic() >= deadline:
                        return
                    time.sleep(0.005)
                time.sleep(0.05)
                request = json.loads((mailbox / "request.json").read_bytes())
                raw = (
                    json.dumps(
                        {
                            "version": 1,
                            "request_id": request["request_id"],
                            "pending": True,
                            "heartbeat": 1,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                ).encode()
                (mailbox / "response.json").write_bytes(raw)
                marker = {
                    "sequence": 1,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "size": len(raw),
                }
                (mailbox / "response.ready").write_text(
                    json.dumps(marker, separators=(",", ":"), sort_keys=True) + "\n"
                )

            publisher = threading.Thread(target=publish_one_progress_frame)
            publisher.start()
            try:
                with self.assertRaisesRegex(WorkerTransportError, "ambiguous"):
                    client.exchange("provider.invoke", {}, timeout=0.1)
            finally:
                publisher.join(1)
            self.assertFalse(publisher.is_alive())

    def test_response_commit_race_retries_the_newer_bounded_body(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            mailbox = Path(temporary) / "mailbox"
            mailbox.mkdir()
            for name in (
                "request.json",
                "request.ready",
                "response.json",
                "response.ready",
            ):
                (mailbox / name).write_bytes(b"")
            client = FileGatewayClient(mailbox, timeout=0.5)
            request_id = "worker-" + "1" * 32
            progress = (
                json.dumps(
                    {
                        "heartbeat": 1,
                        "pending": True,
                        "request_id": request_id,
                        "version": 1,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode()
            final = (
                json.dumps(
                    {
                        "ok": True,
                        "request_id": request_id,
                        "result": {"padding": "x" * 200},
                        "version": 1,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode()

            def marker(raw: bytes) -> bytes:
                return (
                    json.dumps(
                        {
                            "sequence": 1,
                            "sha256": hashlib.sha256(raw).hexdigest(),
                            "size": len(raw),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                ).encode()

            ready_reads = iter((marker(progress), marker(final)))

            def racing_read(path: Path, limit: int) -> bytes:
                if path.name == "response.ready":
                    return next(ready_reads)
                if limit < len(final):
                    raise WorkerTransportError(
                        "gateway mailbox response exceeds its size limit"
                    )
                return final

            with mock.patch(
                "magic_glass_box.worker_transport._read_file",
                side_effect=racing_read,
            ):
                response = client._wait_response(  # noqa: SLF001
                    1,
                    request_id,
                    0.5,
                    allow_progress=True,
                )

            self.assertTrue(response["ok"])
            self.assertEqual(response["result"]["padding"], "x" * 200)


class RecordingGateway:
    def __init__(self, protocol: str):
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.timeouts: list[float | None] = []
        self.response: dict[str, Any] = {
            "version": 1,
            "request_id": "worker-response",
            "ok": True,
            "result": {
                "invocation_id": "pvi_" + "3" * 32,
                "grant_name": "provider-main",
                "provider": "test",
                "model": "pinned-model",
                "status_code": 200,
                "request_bytes": 10,
                "response_bytes": 20,
                "untrusted_response": {
                    "body": "data: done\n\n",
                    "content_type": "text/event-stream; charset=utf-8",
                    "protocol": protocol,
                },
            },
        }

    def exchange(
        self,
        operation: str,
        payload: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        self.calls.append((operation, payload))
        self.timeouts.append(timeout)
        return self.response


class ProviderHTTPTransportTests(unittest.TestCase):
    def test_loopback_handler_has_a_bounded_idle_timeout(self) -> None:
        self.assertIsInstance(_TransportHandler.timeout, float)
        self.assertGreater(_TransportHandler.timeout, 0)

    def test_vendor_dialects_strip_model_and_forward_buffered_sse(self) -> None:
        cases = (
            ("openai-responses", "/v1/responses"),
            ("anthropic-messages", "/v1/messages"),
            ("anthropic-messages", "/v1/messages?beta=true"),
        )
        for dialect, path in cases:
            with self.subTest(dialect=dialect):
                gateway = RecordingGateway(dialect)
                transport = ProviderHTTPTransport(
                    gateway, dialect=dialect, grant="provider-main"
                )
                response = transport.invoke(
                    path,
                    json.dumps(
                        {
                            "model": "worker-selected",
                            "stream": True,
                            "messages": [{"role": "user", "content": "hello"}],
                        }
                    ).encode(),
                    headers={
                        (
                            "x-codex-beta-features"
                            if dialect == "openai-responses"
                            else "anthropic-beta"
                        ): "feature-one",
                        "authorization": "SECRET discarded",
                    },
                )

                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.content_type, "text/event-stream; charset=utf-8"
                )
                self.assertEqual(response.body, b"data: done\n\n")
                operation, payload = gateway.calls[0]
                self.assertEqual(operation, "provider.invoke")
                self.assertEqual(payload["grant"], "provider-main")
                self.assertEqual(payload["request"]["protocol"], dialect)
                self.assertNotIn("model", payload["request"]["body"])
                self.assertEqual(gateway.timeouts[0], PROVIDER_GATEWAY_TIMEOUT_SECONDS)
                self.assertEqual(
                    payload["request"]["headers"],
                    {
                        (
                            "x-codex-beta-features"
                            if dialect == "openai-responses"
                            else "anthropic-beta"
                        ): "feature-one"
                    },
                )

    def test_malformed_or_unapproved_requests_fail_before_the_gateway(self) -> None:
        gateway = RecordingGateway("openai-responses")
        transport = ProviderHTTPTransport(
            gateway, dialect="openai-responses", grant="provider-main"
        )
        cases = (
            ("/v1/messages", b"{}", "route"),
            (
                "/v1/responses?beta=true",
                b'{"stream":true,"model":"x"}',
                "route",
            ),
            (
                "https://untrusted.example/v1/responses",
                b'{"stream":true,"model":"x"}',
                "route",
            ),
            ("/v1/responses", b"[]", "object"),
            ("/v1/responses", b'{"stream":false,"model":"x"}', "stream"),
            ("/v1/responses", b'{"stream":true}', "model"),
            ("/v1/responses", b'{"stream":true,"model":""}', "model"),
            (
                "/v1/responses",
                json.dumps({"stream": True, "model": "x" * 257}).encode(),
                "model",
            ),
            (
                "/v1/responses",
                b'{"stream":true,"model":"x","model":"y"}',
                "JSON",
            ),
        )
        for path, body, message in cases:
            with (
                self.subTest(path=path, body=body),
                self.assertRaisesRegex(WorkerTransportError, message),
            ):
                transport.invoke(path, body)
        self.assertEqual(gateway.calls, [])

    def test_protocol_headers_are_bounded_printable_text(self) -> None:
        gateway = RecordingGateway("openai-responses")
        transport = ProviderHTTPTransport(
            gateway, dialect="openai-responses", grant="provider-main"
        )
        request = b'{"model":"x","stream":true}'
        accepted = (" ", "~", "x" * 8192)
        for value in accepted:
            with self.subTest(accepted=repr(value)[:80]):
                response = transport.invoke(
                    "/v1/responses",
                    request,
                    headers={"x-codex-beta-features": value},
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    gateway.calls[-1][1]["request"]["headers"],
                    {"x-codex-beta-features": value},
                )

        transport.invoke(
            "/v1/responses",
            request,
            headers={
                "OpenAI-Beta": "responses=v1",
                "X-Codex-Beta-Features": "tools",
            },
        )
        self.assertEqual(
            gateway.calls[-1][1]["request"]["headers"],
            {
                "openai-beta": "responses=v1",
                "x-codex-beta-features": "tools",
            },
        )

        with self.assertRaisesRegex(WorkerTransportError, "header"):
            transport.invoke(
                "/v1/responses",
                request,
                headers={
                    "OpenAI-Beta": "one",
                    "openai-beta": "two",
                },
            )

        rejected: tuple[object, ...] = (
            "",
            "feature\nvalue",
            "\x7f",
            "x" * 8193,
            7,
        )
        for value in rejected:
            with (
                self.subTest(value=repr(value)[:80]),
                self.assertRaisesRegex(WorkerTransportError, "header"),
            ):
                transport.invoke(
                    "/v1/responses",
                    request,
                    headers={"x-codex-beta-features": value},  # type: ignore[dict-item]
                )
        self.assertEqual(len(gateway.calls), len(accepted) + 1)

    def test_gateway_errors_and_malformed_responses_are_redacted(self) -> None:
        gateway = RecordingGateway("openai-responses")
        transport = ProviderHTTPTransport(
            gateway, dialect="openai-responses", grant="provider-main"
        )
        request = b'{"model":"x","stream":true}'
        gateway.response = {
            "version": 1,
            "request_id": "worker-response",
            "ok": False,
            "error": {"code": "forbidden", "message": "SECRET detail"},
        }
        with self.assertRaisesRegex(WorkerTransportError, "denied") as denied:
            transport.invoke("/v1/responses", request)
        self.assertNotIn("SECRET", str(denied.exception))

        gateway.response = {"ok": True, "result": {"status_code": 200}}
        with self.assertRaisesRegex(WorkerTransportError, "invalid response"):
            transport.invoke("/v1/responses", request)

    def test_retryable_gateway_failure_preserves_delay_without_poisoning(self) -> None:
        gateway = RecordingGateway("openai-responses")
        gateway.response = {
            "version": 1,
            "request_id": "worker-response",
            "ok": False,
            "error": {
                "code": "rate_limited",
                "message": "SECRET detail",
                "retry_after_seconds": 45,
            },
        }
        transport = ProviderHTTPTransport(
            gateway, dialect="openai-responses", grant="provider-main"
        )
        request = b'{"model":"x","stream":true}'

        for _attempt in range(2):
            with self.assertRaises(ProviderRetryableTransportError) as raised:
                transport.invoke("/v1/responses", request)
            self.assertEqual(raised.exception.category, "rate_limited")
            self.assertEqual(raised.exception.retry_after_seconds, 45)
            self.assertNotIn("SECRET", str(raised.exception))

        self.assertEqual(len(gateway.calls), 2)

    def test_retryable_failure_reaches_vendor_as_bounded_http_429(self) -> None:
        gateway = RecordingGateway("openai-responses")
        gateway.response = {
            "version": 1,
            "request_id": "worker-response",
            "ok": False,
            "error": {
                "code": "quota_exhausted",
                "message": "redacted",
                "retry_after_seconds": 45,
            },
        }
        transport = ProviderHTTPTransport(
            gateway, dialect="openai-responses", grant="provider-main"
        )
        server = _TransportServer(("127.0.0.1", 0), transport)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            connection = http.client.HTTPConnection(
                "127.0.0.1", server.server_port, timeout=2
            )
            connection.request(
                "POST",
                "/v1/responses",
                body='{"model":"x","stream":true}',
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            body = json.loads(response.read())
            connection.close()

            self.assertEqual(response.status, 429)
            self.assertEqual(response.getheader("Retry-After"), "45")
            self.assertEqual(body["error"]["type"], "rate_limit_error")
            self.assertEqual(len(gateway.calls), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_ambiguous_result_poison_prevents_vendor_retry_redispatch(self) -> None:
        gateway = RecordingGateway("openai-responses")
        gateway.response = {
            "version": 1,
            "request_id": "worker-response",
            "ok": False,
            "error": {"code": "ambiguous", "message": "SECRET detail"},
        }
        transport = ProviderHTTPTransport(
            gateway, dialect="openai-responses", grant="provider-main"
        )
        request = b'{"model":"x","stream":true}'

        for _attempt in range(2):
            with self.assertRaisesRegex(WorkerTransportError, "ambiguous") as caught:
                transport.invoke("/v1/responses", request)
            self.assertNotIn("SECRET", str(caught.exception))

        self.assertEqual(len(gateway.calls), 1)

    def test_committed_mailbox_timeout_poison_prevents_vendor_retry(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            mailbox = Path(temporary) / "mailbox"
            mailbox.mkdir()
            for name in (
                "request.json",
                "request.ready",
                "response.json",
                "response.ready",
            ):
                (mailbox / name).write_bytes(b"")
            transport = ProviderHTTPTransport(
                FileGatewayClient(mailbox, timeout=0.1),
                dialect="openai-responses",
                grant="provider-main",
            )
            request = b'{"model":"x","stream":true}'

            with (
                mock.patch(
                    "magic_glass_box.worker_transport.PROVIDER_GATEWAY_TIMEOUT_SECONDS",
                    0.1,
                ),
                self.assertRaisesRegex(WorkerTransportError, "ambiguous"),
            ):
                transport.invoke("/v1/responses", request)
            committed = (mailbox / "request.ready").read_bytes()

            with self.assertRaisesRegex(WorkerTransportError, "ambiguous"):
                transport.invoke("/v1/responses", request)

            self.assertTrue(committed)
            self.assertEqual((mailbox / "request.ready").read_bytes(), committed)

    def test_malformed_post_dispatch_reply_poison_prevents_vendor_retry(
        self,
    ) -> None:
        gateway = RecordingGateway("openai-responses")
        gateway.response = {"ok": True, "result": {"status_code": 200}}
        transport = ProviderHTTPTransport(
            gateway, dialect="openai-responses", grant="provider-main"
        )
        request = b'{"model":"x","stream":true}'

        with self.assertRaisesRegex(WorkerTransportError, "invalid response"):
            transport.invoke("/v1/responses", request)
        with self.assertRaisesRegex(WorkerTransportError, "ambiguous"):
            transport.invoke("/v1/responses", request)

        self.assertEqual(len(gateway.calls), 1)

    def test_transport_configuration_and_response_envelope_are_strict(self) -> None:
        gateway = RecordingGateway("openai-responses")
        for dialect, grant in (
            ("other", "provider-main"),
            ("openai-responses", "Bad Grant"),
            ("openai-responses", "krab"),
        ):
            with (
                self.subTest(dialect=dialect, grant=grant),
                self.assertRaises(WorkerTransportError),
            ):
                ProviderHTTPTransport(gateway, dialect=dialect, grant=grant)

        request = b'{"model":"x","stream":true}'
        upstream = gateway.response["result"]["untrusted_response"]
        for replacement in (
            {**upstream, "protocol": "anthropic-messages"},
            {**upstream, "content_type": "application/json"},
            {**upstream, "body": ""},
            {**upstream, "extra": True},
        ):
            with self.subTest(replacement=replacement):
                gateway.response["result"]["untrusted_response"] = replacement
                transport = ProviderHTTPTransport(
                    gateway, dialect="openai-responses", grant="provider-main"
                )
                with self.assertRaisesRegex(WorkerTransportError, "invalid response"):
                    transport.invoke("/v1/responses", request)

        gateway.response["result"]["untrusted_response"] = upstream
        result = dict(gateway.response["result"])
        for replacement in (
            {**result, "grant_name": "other-provider"},
            {**result, "status_code": True},
            {**result, "status_code": 300},
            {**result, "untrusted_response": None},
            {**result, "extra": True},
        ):
            with self.subTest(replacement=replacement):
                gateway.response["result"] = replacement
                transport = ProviderHTTPTransport(
                    gateway, dialect="openai-responses", grant="provider-main"
                )
                with self.assertRaisesRegex(WorkerTransportError, "invalid response"):
                    transport.invoke("/v1/responses", request)


class WorkerTransportCLITests(unittest.TestCase):
    def _manager(
        self, temporary: str
    ) -> tuple[FileGatewayManager, Path, ProviderControl]:
        control = ProviderControl()
        manager = FileGatewayManager(
            Path(temporary) / "gateways", control, poll_interval=0.005
        )
        endpoint = manager.open(
            RUN_ID,
            capabilities=("provider.invoke",),
            credential_names=("provider-main",),
        )
        return manager, endpoint.directory, control

    def test_call_subcommand_prints_success_and_failure_envelopes(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            manager, directory, _control = self._manager(temporary)
            try:
                environment = {"MAGIC_GLASS_BOX_GATEWAY_DIR": str(directory)}
                payload = json.dumps(
                    {
                        "grant": "provider-main",
                        "request": {
                            "protocol": "openai-responses",
                            "body": {"stream": True},
                        },
                    }
                )
                output = io.StringIO()
                with (
                    mock.patch.dict(os.environ, environment, clear=False),
                    contextlib.redirect_stdout(output),
                ):
                    self.assertEqual(main(["call", "provider.invoke", payload]), 0)
                self.assertTrue(json.loads(output.getvalue())["ok"])

                output = io.StringIO()
                denied = payload.replace("provider-main", "other-provider")
                with (
                    mock.patch.dict(os.environ, environment, clear=False),
                    contextlib.redirect_stdout(output),
                ):
                    self.assertEqual(main(["call", "provider.invoke", denied]), 2)
                self.assertFalse(json.loads(output.getvalue())["ok"])
            finally:
                manager.close()

    def test_serve_subcommand_bridges_both_dialects_then_returns_child_exit(
        self,
    ) -> None:
        cases = (
            ("openai-responses", "/v1/responses"),
            ("anthropic-messages", "/v1/messages?beta=true"),
        )
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            manager, directory, control = self._manager(temporary)
            try:
                for dialect, target in cases:
                    with self.subTest(dialect=dialect):
                        probe = socket.socket()
                        probe.bind(("127.0.0.1", 0))
                        port = probe.getsockname()[1]
                        probe.close()

                        def run_child(
                            command: list[str],
                            *,
                            check: bool,
                            bridge_port: int = port,
                            bridge_target: str = target,
                        ) -> subprocess.CompletedProcess[str]:
                            self.assertEqual(command, ["vendor", "--headless"])
                            self.assertFalse(check)
                            rejected = http.client.HTTPConnection(
                                "127.0.0.1", bridge_port, timeout=2
                            )
                            rejected.request(
                                "POST",
                                bridge_target,
                                body=json.dumps(
                                    {
                                        "model": "worker-choice",
                                        "stream": True,
                                        "messages": [],
                                    }
                                ),
                                headers={"Content-Type": "application/json-patch"},
                            )
                            rejected_response = rejected.getresponse()
                            rejected_response.read()
                            rejected.close()
                            self.assertEqual(rejected_response.status, 400)

                            connection = http.client.HTTPConnection(
                                "127.0.0.1", bridge_port, timeout=2
                            )
                            body = json.dumps(
                                {
                                    "model": "worker-choice",
                                    "stream": True,
                                    "messages": [],
                                }
                            )
                            connection.request(
                                "POST",
                                bridge_target,
                                body=body,
                                headers={"Content-Type": "application/json"},
                            )
                            response = connection.getresponse()
                            raw = response.read()
                            connection.close()
                            self.assertEqual(response.status, 200)
                            self.assertEqual(
                                response.getheader("Cache-Control"), "no-store"
                            )
                            self.assertIn(b"response.completed", raw)
                            return subprocess.CompletedProcess(command, 17)

                        with (
                            mock.patch.dict(
                                os.environ,
                                {"MAGIC_GLASS_BOX_GATEWAY_DIR": str(directory)},
                                clear=False,
                            ),
                            mock.patch(
                                "magic_glass_box.worker_transport.TRANSPORT_PORT",
                                port,
                            ),
                            mock.patch(
                                "magic_glass_box.worker_transport.subprocess.run",
                                side_effect=run_child,
                            ),
                        ):
                            self.assertEqual(
                                main(
                                    [
                                        "serve",
                                        "--dialect",
                                        dialect,
                                        "--grant",
                                        "provider-main",
                                        "--",
                                        "vendor",
                                        "--headless",
                                    ]
                                ),
                                17,
                            )
                self.assertEqual(
                    [call[2]["protocol"] for call in control.calls],
                    ["openai-responses", "anthropic-messages"],
                )
            finally:
                manager.close()

    def test_cli_errors_are_redacted_and_nonzero(self) -> None:
        error = io.StringIO()
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            contextlib.redirect_stderr(error),
        ):
            self.assertEqual(main(["call", "run.describe", "{}"]), 2)
        self.assertIn("environment", error.getvalue())

        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            mailbox = Path(temporary) / "empty"
            mailbox.mkdir()
            error = io.StringIO()
            with (
                mock.patch.dict(
                    os.environ,
                    {"MAGIC_GLASS_BOX_GATEWAY_DIR": str(mailbox)},
                    clear=True,
                ),
                contextlib.redirect_stderr(error),
            ):
                self.assertEqual(
                    main(
                        [
                            "serve",
                            "--dialect",
                            "openai-responses",
                            "--grant",
                            "provider-main",
                            "--",
                        ]
                    ),
                    2,
                )
            self.assertNotIn("Traceback", error.getvalue())

    def test_cli_help_and_empty_payload_errors_are_operator_readable(self) -> None:
        output = io.StringIO()
        with (
            contextlib.redirect_stdout(output),
            self.assertRaises(SystemExit) as help_exit,
        ):
            main(["--help"])
        self.assertEqual(help_exit.exception.code, 0)
        self.assertIn(
            "Use a run-scoped Magic Glass Box worker gateway.",
            output.getvalue(),
        )

        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            self.assertEqual(main(["call", "run.describe", ""]), 2)
        self.assertEqual(
            error.getvalue(),
            "magic-glass-box-worker: gateway payload JSON exceeds its size limit\n",
        )


if __name__ == "__main__":
    unittest.main()
