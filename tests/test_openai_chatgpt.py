from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import types
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, urlsplit

from magic_glass_box.inference import InferenceInvocationRequest
from magic_glass_box.openai_chatgpt import (
    CHATGPT_DIRECT_SCOPE,
    BoundedHTTPSClient,
    ChatGPTAuthenticationError,
    ChatGPTConnectorError,
    ChatGPTOAuth,
    ChatGPTProfileStore,
    ChatGPTResponseAmbiguousError,
    ChatGPTResponseFailure,
    ChatGPTSession,
    HTTPResponse,
    HTTPTransportError,
    OpenAIChatGPTConnector,
    OpenAIConnectorConfig,
    OpenAIResponsesClient,
)

CLIENT_ID = "oaiapp_" + "a" * 32
CONNECTOR_ID = "ifc_" + "1" * 32
DEPLOYMENT = "b" * 64
GRANT_ID = "pgr_" + "2" * 32
POLICY = "c" * 64
MODEL = "gpt-test"
RSA_JWK = {
    "alg": "RS256",
    "e": "AQAB",
    "key_ops": ["verify"],
    "kid": "signing-key",
    "kty": "RSA",
    "n": (
        "mbWnt0WP12Ib3_utytE5BBtunnDfdgqWqRPX91YIvPrmMJb1H4hQ3nm0f5Z8qqng"
        "oD4JwktmqKbFNuQLeAHfJivUD8UBxDSnjHbNrTZ4lChrTJJeOCZqOajRlHTNq-_IFr"
        "1nZWwHLeEZaxD8iMPC-bGQcqJmzLnxV-h2WA-TCPjpW6HBnvwKPedxNR8BerKsAubQ"
        "C0EqCjc3ZKyeicSD4ZoZy4RTynQM66V03MuO4944ssqFVyhacvDgeDRrJZKWWGeuau"
        "s7RApFre8EvsPhdQIBbKJLE1_dgeCHnrDJ_p36xDHTMbBpeP2WyGDAtkNgkD3PlA8v"
        "cORd1fOmROy0WQ"
    ),
    "use": "sig",
}
VALID_RSA_TOKEN = (
    "eyJhbGciOiJSUzI1NiIsImtpZCI6InNpZ25pbmcta2V5IiwidHlwIjoiSldUIn0."  # noqa: S105 - public-key-signed test fixture
    "eyJhdWQiOiJvYWlhcHBfYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWEi"
    "LCJlbWFpbCI6Im9wZXJhdG9yQGV4YW1wbGUuY29tIiwiZXhwIjo0MTAyNDQ0ODAwLC"
    "Jpc3MiOiJodHRwczovL2F1dGgub3BlbmFpLmNvbSIsIm5vbmNlIjoiZXhwZWN0ZWQt"
    "bm9uY2UiLCJzdWIiOiJhY2NvdW50LXN1YmplY3QifQ."
    "BfcV9QdgjNX-6diuaUGkcLefvQAdZSFOEcLJnYlZ_jPgaR7O1ReZFnadyoqeHFGirb"
    "G8B85sB62MUPCKXO3n5iT0mt6kNGdWwmVvtvbVRxQho1wjzZNDHa9lWPch2M63sOvN"
    "45lMErNu--Pxg0U1wkUUF6QRf_DFl1CmD-gIWOsD8APZmgoVa6aiy58QD5XrJ0sq-d"
    "CLq2S8cNCnITzPpF7OW8hY42BaENp2z16dxJFkyDMp7YC9ZLSi5w5ZD2MzuqZs0AOJ"
    "r00UCb0E5XKh0JTEwQVG9Ed8PmMhNjVT_SM7eY1R2QrDdKjpxYn29zz1o2jMzHsVG4l"
    "jRdkHlTjQyA"
)
WRONG_AUDIENCE_RSA_TOKEN = (
    "eyJhbGciOiJSUzI1NiIsImtpZCI6InNpZ25pbmcta2V5IiwidHlwIjoiSldUIn0."  # noqa: S105 - public-key-signed test fixture
    "eyJhdWQiOiJhbm90aGVyLWNsaWVudCIsImVtYWlsIjoib3BlcmF0b3JAZXhhbXBsZS"
    "5jb20iLCJleHAiOjQxMDI0NDQ4MDAsImlzcyI6Imh0dHBzOi8vYXV0aC5vcGVuYWku"
    "Y29tIiwibm9uY2UiOiJleHBlY3RlZC1ub25jZSIsInN1YiI6ImFjY291bnQtc3Viam"
    "VjdCJ9."
    "FXbhgtGaWy-psApyX7jL8rr_Cnmxe1YTykkvEOfaL0c3618il5U3V2fgRkkD65fHxF"
    "f0zVWiLYign1UUJ8tuXX-IRc4Vo0DC5ChP3MGWTXKfirBVdjQbTzRe4KWfJMxXtV0g"
    "fJdpdevrEljR1nkMXnRoKhNW_blk2YbgMUzsShC8rGktoM9_NKzpDBmSaplCGKEOdIm"
    "vLfxZtOYoRhMoREH_fJ29qDjjrdvklqgg5N6FvWo0Ex8gw4wbd2kSK0YC5ix3Ct834"
    "t1fSKBc-toOt1XYWJqMjLu2cGmrqNgDtMnwf-Ojc6sWt8P4iCDKkaZDVQo2EipAEtLQ"
    "QEVNjsJ5iQ"
)
HS256_TOKEN = (
    "eyJhbGciOiJIUzI1NiIsImtpZCI6InNpZ25pbmcta2V5IiwidHlwIjoiSldUIn0."  # noqa: S105 - deliberately untrusted test fixture
    "eyJhdWQiOiJvYWlhcHBfYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWEi"
    "LCJlbWFpbCI6Im9wZXJhdG9yQGV4YW1wbGUuY29tIiwiZXhwIjo0MTAyNDQ0ODAwLC"
    "Jpc3MiOiJodHRwczovL2F1dGgub3BlbmFpLmNvbSIsIm5vbmNlIjoiZXhwZWN0ZWQt"
    "bm9uY2UiLCJzdWIiOiJhY2NvdW50LXN1YmplY3QifQ."
    "Me7P6vNjNMAn6I6DIDV-YIy5uidoCCu_2p7i4vB5dRU"
)


class FakeHTTPClient:
    def __init__(self, *responses: HTTPResponse | Exception):
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
        max_response_bytes: int,
    ) -> HTTPResponse:
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "body": body,
                "timeout": timeout,
                "max_response_bytes": max_response_bytes,
            }
        )
        if not self.responses:
            raise AssertionError("unexpected HTTP request")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeWireResponse:
    def __init__(
        self,
        body: bytes,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        read_error: Exception | None = None,
    ) -> None:
        self.body = body
        self.status = status
        self.headers = headers or {}
        self.read_error = read_error

    def getheader(self, name: str) -> str | None:
        return self.headers.get(name)

    def read(self, length: int) -> bytes:
        if self.read_error is not None:
            raise self.read_error
        return self.body[:length]


class FakeConnection:
    def __init__(
        self,
        response: FakeWireResponse,
        *,
        connect_error: Exception | None = None,
        request_error: Exception | None = None,
    ) -> None:
        self.response = response
        self.connect_error = connect_error
        self.request_error = request_error
        self.requests: list[tuple[str, str, bytes | None, dict[str, str]]] = []
        self.closed = False

    def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error

    def request(
        self,
        method: str,
        target: str,
        *,
        body: bytes | None,
        headers: dict[str, str],
    ) -> None:
        if self.request_error is not None:
            raise self.request_error
        self.requests.append((method, target, body, headers))

    def getresponse(self) -> FakeWireResponse:
        return self.response

    def close(self) -> None:
        self.closed = True


class BoundedHTTPSClientTests(unittest.TestCase):
    def test_connection_factory_failure_is_a_pre_dispatch_transport_error(self) -> None:
        def unavailable(_host: str, _port: int | None, _timeout: float):
            raise OSError("private resolver detail")

        client = BoundedHTTPSClient(unavailable)

        with self.assertRaises(HTTPTransportError) as raised:
            client.request(
                "GET",
                "https://api.openai.com/v1/models",
                headers={},
                body=None,
                timeout=1,
                max_response_bytes=1024,
            )

        self.assertFalse(raised.exception.ambiguous)
        self.assertNotIn("resolver", str(raised.exception))

    def test_success_is_bounded_and_preserves_only_response_metadata(self) -> None:
        connection = FakeConnection(
            FakeWireResponse(
                b"okay",
                headers={
                    "Content-Length": "4",
                    "Content-Type": "application/json",
                    "request-id": "req_fallback",
                },
            )
        )
        factory_calls: list[tuple[str, int | None, float]] = []

        def factory(host: str, port: int | None, timeout: float):
            factory_calls.append((host, port, timeout))
            return connection

        response = BoundedHTTPSClient(factory).request(
            "POST",
            "https://api.openai.com:443/v1/responses?trace=one",
            headers={"content-type": "application/json"},
            body=b"{}",
            timeout=2,
            max_response_bytes=4,
        )

        self.assertEqual(factory_calls, [("api.openai.com", 443, 2.0)])
        self.assertEqual(
            connection.requests,
            [
                (
                    "POST",
                    "/v1/responses?trace=one",
                    b"{}",
                    {"content-type": "application/json"},
                )
            ],
        )
        self.assertEqual(response.body, b"okay")
        self.assertEqual(response.content_type, "application/json")
        self.assertEqual(response.request_id, "req_fallback")
        self.assertTrue(connection.closed)

    def test_invalid_destination_and_request_fail_before_connection(self) -> None:
        invalid = (
            ("GET", "http://api.openai.com/v1/models", 1, 1),
            ("GET", "https://user@api.openai.com/v1/models", 1, 1),
            ("GET", "https://api.openai.com/v1/models#fragment", 1, 1),
            ("GET", "https://api.openai.com:bad/v1/models", 1, 1),
            ("DELETE", "https://api.openai.com/v1/models", 1, 1),
            ("GET", "https://api.openai.com/v1/models", 0, 1),
            ("GET", "https://api.openai.com/v1/models", True, 1),
            ("GET", "https://api.openai.com/v1/models", 1, 0),
            ("GET", "https://api.openai.com/v1/models", 1, True),
        )
        for method, url, timeout, limit in invalid:
            with (
                self.subTest(method=method, url=url, timeout=timeout, limit=limit),
                self.assertRaises(HTTPTransportError) as raised,
            ):
                BoundedHTTPSClient(
                    lambda *_args: self.fail("must not connect")
                ).request(
                    method,
                    url,
                    headers={},
                    body=None,
                    timeout=timeout,
                    max_response_bytes=limit,
                )
            self.assertFalse(raised.exception.ambiguous)

    def test_pre_dispatch_failure_is_safe_and_post_dispatch_failure_is_ambiguous(
        self,
    ) -> None:
        cases = (
            (
                FakeConnection(FakeWireResponse(b""), connect_error=OSError("private")),
                False,
            ),
            (
                FakeConnection(FakeWireResponse(b""), request_error=OSError("private")),
                True,
            ),
            (
                FakeConnection(
                    FakeWireResponse(b"", read_error=OSError("private read detail"))
                ),
                True,
            ),
        )
        for connection, ambiguous in cases:
            with (
                self.subTest(ambiguous=ambiguous),
                self.assertRaises(HTTPTransportError) as raised,
            ):
                BoundedHTTPSClient(
                    lambda *_args, selected=connection: selected
                ).request(
                    "GET",
                    "https://api.openai.com/v1/models",
                    headers={},
                    body=None,
                    timeout=1,
                    max_response_bytes=8,
                )
            self.assertIs(raised.exception.ambiguous, ambiguous)
            self.assertNotIn("private", str(raised.exception))
            self.assertTrue(connection.closed)

    def test_invalid_or_oversized_response_is_post_dispatch_ambiguous(self) -> None:
        responses = (
            FakeWireResponse(b"", headers={"Content-Length": "not-an-integer"}),
            FakeWireResponse(b"", headers={"Content-Length": "9"}),
            FakeWireResponse(b"123456789"),
        )
        for response in responses:
            connection = FakeConnection(response)
            with (
                self.subTest(headers=response.headers, body=response.body),
                self.assertRaises(HTTPTransportError) as raised,
            ):
                BoundedHTTPSClient(
                    lambda *_args, selected=connection: selected
                ).request(
                    "GET",
                    "https://api.openai.com/v1/models",
                    headers={},
                    body=None,
                    timeout=1,
                    max_response_bytes=8,
                )
            self.assertTrue(raised.exception.ambiguous)
            self.assertTrue(connection.closed)


def token_response(**overrides: object) -> HTTPResponse:
    value: dict[str, object] = {
        "access_token": "access-secret",
        "refresh_token": "refresh-secret",
        "id_token": "id-secret",
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": (
            "openid profile email offline_access resource.invoke "
            "chatgpt.tokens.use.direct"
        ),
    }
    value.update(overrides)
    return HTTPResponse(
        status_code=200,
        content_type="application/json",
        body=json.dumps(value).encode(),
        request_id="req_token",
    )


def session_mapping(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 1,
        "client_id": CLIENT_ID,
        "ext_agent_host_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
        "issuer": "https://auth.openai.com",
        "subject": "account-subject",
        "email": "operator@example.com",
        "id_token": "id-secret",
        "access_token": "access-secret",
        "refresh_token": "refresh-secret",
        "token_type": "Bearer",
        "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        "scopes": [
            "chatgpt.tokens.use.direct",
            "email",
            "offline_access",
            "openid",
            "profile",
            "resource.invoke",
        ],
    }
    value.update(overrides)
    return value


def completed_sse() -> bytes:
    return (
        b'event: response.created\ndata: {"type":"response.created"}\n\n'
        b'event: response.completed\ndata: {"type":"response.completed",'
        b'"response":{"status":"completed"}}\n\n'
        b"data: [DONE]\n\n"
    )


def invocation_request(
    scope: str,
    *,
    request: dict[str, Any] | None = None,
    **overrides: object,
) -> InferenceInvocationRequest:
    value: dict[str, object] = {
        "schema_version": 1,
        "connector_protocol_version": 1,
        "invocation_id": "pinv_" + "3" * 32,
        "connector_id": CONNECTOR_ID,
        "deployment_sha256": DEPLOYMENT,
        "route": "chatgpt-main",
        "grant_id": GRANT_ID,
        "policy_sha256": POLICY,
        "credential_scope_sha256": scope,
        "provider": "openai",
        "protocol": "openai-responses",
        "model": MODEL,
        "deadline": (datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
        "request_limit_bytes": 65536,
        "response_limit_bytes": 65536,
        "request": request
        or {
            "protocol": "openai-responses",
            "body": {
                "input": [{"role": "user", "content": "hello"}],
                "store": False,
                "stream": True,
            },
            "headers": {"x-codex-beta-features": "feature-one"},
        },
    }
    value.update(overrides)
    return InferenceInvocationRequest.from_mapping(value)


class ChatGPTProfileStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.profile = self.root / "openai-profile"
        self.store = ChatGPTProfileStore(self.profile)

    def test_store_persists_a_private_host_registration_and_redacted_session(
        self,
    ) -> None:
        host_id = self.store.host_id()
        self.assertTrue(host_id.startswith("urn:uuid:"))
        self.assertEqual(self.store.host_id(), host_id)

        self.store.save_registration(CLIENT_ID)
        session = ChatGPTSession.from_mapping(
            session_mapping(ext_agent_host_id=host_id)
        )
        self.store.save_session(session)

        self.assertEqual(self.store.registration(), CLIENT_ID)
        self.assertEqual(self.store.session(), session)
        self.assertNotIn("access-secret", repr(session))
        self.assertNotIn("refresh-secret", repr(session))
        self.assertNotIn("id-secret", repr(session))
        self.assertEqual(stat.S_IMODE(self.profile.stat().st_mode), 0o700)
        for name in ("host.json", "registration.json", "session.json"):
            self.assertEqual(
                stat.S_IMODE((self.profile / name).stat().st_mode),
                0o600,
            )

    def test_store_rejects_relative_public_and_malformed_state(self) -> None:
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "absolute"):
            ChatGPTProfileStore(Path("relative"))

        self.profile.mkdir(mode=0o755)
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "0700"):
            self.store.host_id()

        self.profile.chmod(0o700)
        session_path = self.profile / "session.json"
        session_path.write_text("not-json", encoding="utf-8")
        session_path.chmod(0o600)
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "invalid"):
            self.store.session()

    def test_clear_session_retains_registration_for_reauthorization(self) -> None:
        host_id = self.store.host_id()
        self.store.save_registration(CLIENT_ID)
        self.store.save_session(
            ChatGPTSession.from_mapping(session_mapping(ext_agent_host_id=host_id))
        )

        self.store.clear_session()

        self.assertIsNone(self.store.session())
        self.assertEqual(self.store.registration(), CLIENT_ID)
        self.assertEqual(self.store.host_id(), host_id)

    def test_session_accepts_case_insensitive_bearer_token_type(self) -> None:
        session = ChatGPTSession.from_mapping(
            session_mapping(token_type="bearer")  # noqa: S106 - OAuth token type
        )

        self.assertEqual(session.token_type, "Bearer")

    def test_session_schema_rejects_malformed_or_ambiguous_identity(self) -> None:
        missing_subject = session_mapping()
        missing_subject.pop("subject")
        cases = (
            ([], "session"),
            (missing_subject, "shape"),
            (session_mapping(schema_version=2), "version"),
            (session_mapping(client_id="not-a-client"), "client_id"),
            (session_mapping(ext_agent_host_id="not-a-host"), "host"),
            (session_mapping(issuer="https://example.com"), "issuer"),
            (session_mapping(subject=""), "subject"),
            (session_mapping(email=7), "email"),
            (session_mapping(access_token=""), "access token"),
            (
                session_mapping(token_type="Basic"),  # noqa: S106 - OAuth token type
                "token type",
            ),
            (session_mapping(expires_at="not-a-time"), "expiry"),
            (session_mapping(expires_at="2026-01-01T00:00:00"), "expiry"),
            (session_mapping(scopes=[]), "scopes"),
            (session_mapping(scopes=["openid", "openid"]), "scopes"),
        )
        for value, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ChatGPTAuthenticationError, message),
            ):
                ChatGPTSession.from_mapping(value)  # type: ignore[arg-type]

    def test_store_rejects_unsafe_records_and_mismatched_sessions(self) -> None:
        self.profile.mkdir(mode=0o700)
        unsafe = self.profile / "session.json"
        unsafe.write_text(json.dumps(session_mapping()), encoding="utf-8")
        unsafe.chmod(0o644)
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "unsafe"):
            self.store.session()

        unsafe.chmod(0o600)
        unsafe.write_bytes(b"x" * (512 * 1024 + 1))
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "oversized"):
            self.store.session()
        unsafe.unlink()

        host_id = self.store.host_id()
        self.store.save_registration(CLIENT_ID)
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "session is invalid"):
            self.store.save_session("wrong")  # type: ignore[arg-type]
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "host does not match"):
            self.store.save_session(ChatGPTSession.from_mapping(session_mapping()))
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "registration"):
            self.store.save_session(
                ChatGPTSession.from_mapping(
                    session_mapping(
                        client_id="oaiapp_" + "z" * 32,
                        ext_agent_host_id=host_id,
                    )
                )
            )

        lock = self.profile / ".refresh.lock"
        lock.write_text("", encoding="utf-8")
        lock.chmod(0o644)
        with (
            self.assertRaisesRegex(ChatGPTAuthenticationError, "lock is unsafe"),
            self.store.refresh_lock(),
        ):
            pass

    def test_store_rejects_a_symlink_profile_directory(self) -> None:
        target = self.root / "target"
        target.mkdir(mode=0o700)
        self.profile.symlink_to(target, target_is_directory=True)

        with self.assertRaisesRegex(ChatGPTAuthenticationError, "unsafe"):
            self.store.host_id()


class ChatGPTOAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        root.chmod(0o700)
        self.store = ChatGPTProfileStore(root / "profile")
        self.claims = {
            "iss": "https://auth.openai.com",
            "sub": "account-subject",
            "email": "operator@example.com",
        }

    def oauth(self, http: FakeHTTPClient) -> ChatGPTOAuth:
        return ChatGPTOAuth(
            self.store,
            http_client=http,
            id_token_validator=lambda token, client_id, nonce: {
                **self.claims,
                "validated_token": token,
                "validated_client": client_id,
                "validated_nonce": nonce,
            },
        )

    def test_begin_uses_dynamic_registration_then_the_issued_client(self) -> None:
        oauth = self.oauth(FakeHTTPClient())
        initial = oauth.begin("http://127.0.0.1:54321/auth/callback")
        query = parse_qs(urlsplit(initial.authorization_url).query)

        self.assertEqual(query["client_id"], ["dynamic_agent_client"])
        self.assertEqual(query["agent_name_hint"], ["Magic Glass Box"])
        self.assertEqual(query["ext_agent_host_id"], [self.store.host_id()])
        self.assertEqual(query["resource"], ["https://api.openai.com/v1"])
        self.assertIn(CHATGPT_DIRECT_SCOPE, query["scope"][0].split())
        self.assertEqual(query["code_challenge_method"], ["S256"])
        self.assertNotEqual(query["code_challenge"], [initial.code_verifier])

        self.store.save_registration(CLIENT_ID)
        returning = oauth.begin("http://127.0.0.1:54322/auth/callback")
        returning_query = parse_qs(urlsplit(returning.authorization_url).query)
        self.assertEqual(returning_query["client_id"], [CLIENT_ID])
        self.assertNotIn("agent_name_hint", returning_query)

    def test_complete_saves_registration_before_exchange_and_session_after_validation(
        self,
    ) -> None:
        http = FakeHTTPClient(token_response())
        oauth = self.oauth(http)
        attempt = oauth.begin("http://127.0.0.1:54321/auth/callback")

        session = oauth.complete(
            attempt,
            {
                "code": "authorization-code",
                "state": attempt.state,
                "client_id": CLIENT_ID,
            },
        )

        self.assertEqual(self.store.registration(), CLIENT_ID)
        self.assertEqual(self.store.session(), session)
        self.assertTrue(session.sharing)
        self.assertEqual(session.subject, "account-subject")
        request = http.requests[0]
        self.assertEqual(
            request["url"], "https://auth.openai.com/api/accounts/oauth/token"
        )
        form = parse_qs(request["body"].decode())
        self.assertEqual(form["client_id"], [CLIENT_ID])
        self.assertEqual(form["code_verifier"], [attempt.code_verifier])
        self.assertNotIn("client_secret", form)

    def test_registration_survives_failed_code_exchange(self) -> None:
        http = FakeHTTPClient(HTTPTransportError("private detail", ambiguous=False))
        oauth = self.oauth(http)
        attempt = oauth.begin("http://127.0.0.1:54321/auth/callback")

        with self.assertRaisesRegex(ChatGPTAuthenticationError, "exchange failed"):
            oauth.complete(
                attempt,
                {
                    "code": "authorization-code",
                    "state": attempt.state,
                    "client_id": CLIENT_ID,
                },
            )

        self.assertEqual(self.store.registration(), CLIENT_ID)
        self.assertIsNone(self.store.session())

    def test_callback_state_client_and_direct_permission_fail_closed(self) -> None:
        oauth = self.oauth(FakeHTTPClient(token_response()))
        attempt = oauth.begin("http://127.0.0.1:54321/auth/callback")
        invalid = (
            ({"code": "code", "state": "wrong", "client_id": CLIENT_ID}, "state"),
            ({"code": "code", "state": attempt.state}, "client_id"),
            (
                {"error": "access_denied", "state": attempt.state},
                "denied",
            ),
        )
        for callback, message in invalid:
            with (
                self.subTest(callback=callback),
                self.assertRaisesRegex(ChatGPTAuthenticationError, message),
            ):
                oauth.complete(attempt, callback)

        self.store.save_registration(CLIENT_ID)
        returning = oauth.begin("http://127.0.0.1:54321/auth/callback")
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "client_id"):
            oauth.complete(
                returning,
                {
                    "code": "code",
                    "state": returning.state,
                    "client_id": "oaiapp_" + "z" * 32,
                },
            )

        limited = self.oauth(
            FakeHTTPClient(token_response(scope="openid profile email offline_access"))
        )
        limited_attempt = limited.begin("http://127.0.0.1:54321/auth/callback")
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "plan usage"):
            limited.complete(
                limited_attempt,
                {
                    "code": "code",
                    "state": limited_attempt.state,
                },
            )

    def test_access_token_refreshes_once_and_rotates_the_saved_session(self) -> None:
        host_id = self.store.host_id()
        self.store.save_registration(CLIENT_ID)
        self.store.save_session(
            ChatGPTSession.from_mapping(
                session_mapping(
                    ext_agent_host_id=host_id,
                    expires_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                )
            )
        )
        http = FakeHTTPClient(
            token_response(
                access_token="new-access",  # noqa: S106 - test token
                refresh_token="new-refresh",  # noqa: S106 - test token
                id_token="new-id",  # noqa: S106 - test token
            )
        )
        oauth = self.oauth(http)

        self.assertEqual(oauth.access_token(), "new-access")
        refreshed = self.store.session()
        self.assertIsNotNone(refreshed)
        self.assertEqual(refreshed.access_token, "new-access")
        self.assertEqual(refreshed.refresh_token, "new-refresh")
        self.assertEqual(refreshed.id_token, "new-id")
        form = parse_qs(http.requests[0]["body"].decode())
        self.assertEqual(form["grant_type"], ["refresh_token"])
        self.assertEqual(form["refresh_token"], ["refresh-secret"])
        self.assertNotIn("scope", form)

    def test_terminal_refresh_error_clears_tokens_but_transient_failure_preserves_them(
        self,
    ) -> None:
        host_id = self.store.host_id()
        self.store.save_registration(CLIENT_ID)
        expired = ChatGPTSession.from_mapping(
            session_mapping(
                ext_agent_host_id=host_id,
                expires_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
            )
        )
        self.store.save_session(expired)
        terminal = self.oauth(
            FakeHTTPClient(
                HTTPResponse(
                    400,
                    "application/json",
                    b'{"error":"invalid_grant"}',
                    "req_terminal",
                )
            )
        )
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "sign in again"):
            terminal.access_token()
        self.assertIsNone(self.store.session())
        self.assertEqual(self.store.registration(), CLIENT_ID)

        self.store.save_session(expired)
        transient = self.oauth(
            FakeHTTPClient(
                HTTPResponse(503, "application/json", b'{"error":"unavailable"}', None)
            )
        )
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "temporarily"):
            transient.access_token()
        self.assertEqual(self.store.session(), expired)

    def test_list_models_uses_the_selected_session_and_filters_visibility(self) -> None:
        host_id = self.store.host_id()
        self.store.save_registration(CLIENT_ID)
        self.store.save_session(
            ChatGPTSession.from_mapping(session_mapping(ext_agent_host_id=host_id))
        )
        http = FakeHTTPClient(
            HTTPResponse(
                200,
                "application/json",
                b'{"models":['
                b'{"slug":"gpt-visible","display_name":"Visible","visibility":"list"},'
                b'{"slug":"gpt-hidden","display_name":"Hidden","visibility":"hide"}'
                b"]}",
                "req_models",
            )
        )

        models = self.oauth(http).list_models()

        self.assertEqual(
            [(model.slug, model.display_name) for model in models],
            [("gpt-visible", "Visible")],
        )
        self.assertEqual(
            http.requests[0]["headers"]["authorization"], "Bearer access-secret"
        )

    @unittest.skipIf(
        bool(os.environ.get("MUTANT_UNDER_TEST")),
        "Mutmut reloads cryptography classes in-process; fake backend covers mutants",
    )
    def test_real_rs256_id_token_validation_checks_signature_audience_and_nonce(
        self,
    ) -> None:
        response = HTTPResponse(
            200,
            "application/json",
            json.dumps({"keys": [RSA_JWK]}).encode(),
            "req_jwks",
        )
        oauth = ChatGPTOAuth(self.store, http_client=FakeHTTPClient(response))

        validated = oauth._validate_id_token(
            VALID_RSA_TOKEN, CLIENT_ID, "expected-nonce"
        )

        self.assertEqual(validated["sub"], "account-subject")
        self.assertEqual(validated["aud"], CLIENT_ID)

        mismatch = ChatGPTOAuth(
            self.store,
            http_client=FakeHTTPClient(response),
        )
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "nonce"):
            mismatch._validate_id_token(VALID_RSA_TOKEN, CLIENT_ID, "wrong-nonce")

    @unittest.skipIf(
        bool(os.environ.get("MUTANT_UNDER_TEST")),
        "Mutmut reloads cryptography classes in-process; fake backend covers mutants",
    )
    def test_real_id_token_validation_rejects_untrusted_keys_and_claims(self) -> None:
        cases = (
            (
                HTTPResponse(503, "application/json", b"{}", None),
                VALID_RSA_TOKEN,
            ),
            (
                HTTPResponse(200, "application/json", b'{"keys":[]}', None),
                VALID_RSA_TOKEN,
            ),
            (
                HTTPResponse(
                    200,
                    "application/json",
                    json.dumps({"keys": [RSA_JWK]}).encode(),
                    None,
                ),
                WRONG_AUDIENCE_RSA_TOKEN,
            ),
            (
                HTTPResponse(
                    200,
                    "application/json",
                    json.dumps({"keys": [RSA_JWK]}).encode(),
                    None,
                ),
                HS256_TOKEN,
            ),
        )
        for response, token in cases:
            with (
                self.subTest(status=response.status_code, token=token[:12]),
                self.assertRaisesRegex(ChatGPTAuthenticationError, "validation"),
            ):
                ChatGPTOAuth(
                    self.store,
                    http_client=FakeHTTPClient(response),
                )._validate_id_token(token, CLIENT_ID, "expected-nonce")

    def test_id_token_validation_contract_with_reload_safe_backend(self) -> None:
        calls: dict[str, object] = {}

        class FakePyJWK:
            @staticmethod
            def from_dict(value: object) -> types.SimpleNamespace:
                calls["jwk"] = value
                return types.SimpleNamespace(key="selected-signing-key")

        class FakeDecoder:
            def decode(
                self,
                token: str,
                key: object,
                *,
                algorithms: list[str],
                audience: str,
                issuer: str,
                options: dict[str, list[str]],
            ) -> dict[str, object]:
                calls["decode"] = {
                    "algorithms": algorithms,
                    "audience": audience,
                    "issuer": issuer,
                    "key": key,
                    "options": options,
                    "token": token,
                }
                return {
                    "aud": CLIENT_ID,
                    "exp": 4102444800,
                    "iss": "https://auth.openai.com",
                    "nonce": "expected-nonce",
                    "sub": "account-subject",
                }

        fake_jwt = types.SimpleNamespace(
            PyJWK=FakePyJWK,
            PyJWT=FakeDecoder,
            get_unverified_header=lambda token: (
                calls.setdefault("header_token", token)
                and {"alg": "RS256", "kid": "signing-key"}
            ),
        )
        response = HTTPResponse(
            200,
            "application/json",
            json.dumps({"keys": [RSA_JWK]}).encode(),
            "req_jwks",
        )
        with mock.patch.dict(sys.modules, {"jwt": fake_jwt}):
            claims = ChatGPTOAuth(
                self.store,
                http_client=FakeHTTPClient(response),
            )._validate_id_token("signed-token", CLIENT_ID, "expected-nonce")

        self.assertEqual(claims["sub"], "account-subject")
        self.assertEqual(calls["header_token"], "signed-token")
        self.assertEqual(calls["jwk"], RSA_JWK)
        self.assertEqual(
            calls["decode"],
            {
                "algorithms": ["RS256"],
                "audience": CLIENT_ID,
                "issuer": "https://auth.openai.com",
                "key": "selected-signing-key",
                "options": {"require": ["aud", "exp", "iss", "sub"]},
                "token": "signed-token",
            },
        )

    def test_token_response_rejects_invalid_protocol_and_identity_fields(self) -> None:
        oauth = self.oauth(FakeHTTPClient())
        host_id = self.store.host_id()
        valid: dict[str, object] = {
            "access_token": "access-secret",
            "refresh_token": "refresh-secret",
            "id_token": "id-secret",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "openid resource.invoke chatgpt.tokens.use.direct",
        }
        cases = (
            ({"access_token": ""}, "access token"),
            ({"token_type": "Basic"}, "type"),
            ({"expires_in": True}, "expiry"),
            ({"expires_in": 0}, "expiry"),
            ({"scope": []}, "scopes"),
            ({"scope": "openid openid"}, "scopes"),
        )
        for overrides, message in cases:
            with (
                self.subTest(overrides=overrides),
                self.assertRaisesRegex(ChatGPTAuthenticationError, message),
            ):
                oauth._session_from_token(
                    {**valid, **overrides},
                    client_id=CLIENT_ID,
                    host_id=host_id,
                    nonce="nonce",
                    previous=None,
                )

        wrong_identity = ChatGPTOAuth(
            self.store,
            http_client=FakeHTTPClient(),
            id_token_validator=lambda *_args: {
                "iss": "https://auth.openai.com",
                "sub": "different-account",
            },
        )
        previous = ChatGPTSession.from_mapping(
            session_mapping(ext_agent_host_id=host_id)
        )
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "identity changed"):
            wrong_identity._session_from_token(
                valid,
                client_id=CLIENT_ID,
                host_id=host_id,
                nonce=None,
                previous=previous,
            )

    def test_oauth_and_model_discovery_failures_are_redacted_and_non_destructive(
        self,
    ) -> None:
        oauth = self.oauth(
            FakeHTTPClient(HTTPResponse(400, "application/json", b"{}", None))
        )
        attempt = oauth.begin("http://127.0.0.1:54321/auth/callback")
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "exchange failed"):
            oauth.complete(
                attempt,
                {
                    "code": "authorization-code",
                    "state": attempt.state,
                    "client_id": CLIENT_ID,
                },
            )

        host_id = self.store.host_id()
        self.store.save_registration(CLIENT_ID)
        self.store.save_session(
            ChatGPTSession.from_mapping(session_mapping(ext_agent_host_id=host_id))
        )
        failures: tuple[HTTPResponse | Exception, ...] = (
            HTTPTransportError("private", ambiguous=False),
            HTTPResponse(403, "application/json", b"{}", None),
            HTTPResponse(200, "application/json", b'{"models":"wrong"}', None),
            HTTPResponse(
                200,
                "application/json",
                b'{"models":[{"slug":7,"display_name":"Bad","visibility":"list"}]}',
                None,
            ),
        )
        for failure in failures:
            with (
                self.subTest(failure=failure),
                self.assertRaises(ChatGPTAuthenticationError) as raised,
            ):
                self.oauth(FakeHTTPClient(failure)).list_models()
            self.assertNotIn("private", str(raised.exception))

    def test_nonterminal_refresh_failure_and_removed_scope_preserve_registration(
        self,
    ) -> None:
        host_id = self.store.host_id()
        self.store.save_registration(CLIENT_ID)
        expired = ChatGPTSession.from_mapping(
            session_mapping(
                ext_agent_host_id=host_id,
                expires_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
            )
        )
        self.store.save_session(expired)
        ordinary = self.oauth(
            FakeHTTPClient(
                HTTPResponse(
                    400, "application/json", b'{"error":"invalid_client"}', None
                )
            )
        )
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "renewal failed"):
            ordinary.access_token()
        self.assertEqual(self.store.session(), expired)

        no_sharing = self.oauth(
            FakeHTTPClient(token_response(scope="openid offline_access"))
        )
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "no longer authorized"):
            no_sharing.access_token()
        self.assertIsNone(self.store.session())
        self.assertEqual(self.store.registration(), CLIENT_ID)


class OpenAIResponsesClientTests(unittest.TestCase):
    def request(self, **body_overrides: object) -> dict[str, Any]:
        body: dict[str, Any] = {
            "input": [{"role": "user", "content": "hello"}],
            "store": False,
            "stream": True,
        }
        body.update(body_overrides)
        return {
            "protocol": "openai-responses",
            "body": body,
            "headers": {"x-codex-beta-features": "feature-one"},
        }

    def invoke(
        self,
        http: FakeHTTPClient,
        request: dict[str, Any] | None = None,
    ):
        return OpenAIResponsesClient(http).invoke(
            access_token="access-secret",  # noqa: S106 - test token
            model=MODEL,
            request=request or self.request(),
            request_limit_bytes=65536,
            response_limit_bytes=65536,
            deadline=datetime.now(UTC) + timedelta(seconds=30),
        )

    def test_completed_stream_is_forwarded_with_pinned_model_and_headers(self) -> None:
        raw = completed_sse()
        http = FakeHTTPClient(
            HTTPResponse(200, "text/event-stream; charset=utf-8", raw, "req_live")
        )

        result = self.invoke(
            http,
            self.request(
                client_metadata={"originator": "magic-glass-box"},
                include=["reasoning.encrypted_content"],
                instructions="Work only through the local tools.",
                parallel_tool_calls=True,
                prompt_cache_key="mgb-test",
                reasoning={"effort": "medium"},
                text={"format": {"type": "text"}},
                tool_choice="auto",
                tools=[
                    {
                        "type": "function",
                        "name": "run_describe",
                        "description": "Read bounded run context.",
                        "parameters": {"type": "object", "properties": {}},
                        "strict": True,
                    },
                    {
                        "type": "namespace",
                        "name": "workspace",
                        "description": "Local workspace tools.",
                        "tools": [
                            {
                                "type": "function",
                                "name": "write_file",
                                "description": "Write a local file.",
                                "parameters": {
                                    "type": "object",
                                    "properties": {},
                                },
                                "strict": True,
                            }
                        ],
                    },
                ],
            ),
        )

        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.response_bytes, len(raw))
        self.assertEqual(
            result.untrusted_response,
            {
                "body": raw.decode(),
                "content_type": "text/event-stream; charset=utf-8",
                "protocol": "openai-responses",
            },
        )
        sent = json.loads(http.requests[0]["body"])
        self.assertEqual(sent["model"], MODEL)
        self.assertIs(sent["store"], False)
        self.assertIs(sent["stream"], True)
        self.assertEqual(
            http.requests[0]["headers"]["authorization"], "Bearer access-secret"
        )
        self.assertEqual(
            http.requests[0]["headers"]["x-codex-beta-features"],
            "feature-one",
        )

    def test_completed_headerless_stream_is_validated_and_normalized(self) -> None:
        raw = completed_sse()

        result = self.invoke(
            FakeHTTPClient(HTTPResponse(200, None, raw, "req_headerless"))
        )

        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.untrusted_response["content_type"], "text/event-stream")

    def test_stream_error_event_preserves_its_machine_readable_code(self) -> None:
        raw = (
            b'data: {"type":"error","code":'
            b'"subscription_sharing_unsupported_capability",'
            b'"param":"tools","message":"private upstream detail"}\n\n'
        )

        with self.assertRaises(ChatGPTResponseFailure) as raised:
            self.invoke(
                FakeHTTPClient(
                    HTTPResponse(200, "text/event-stream", raw, "req_stream_error")
                )
            )

        self.assertEqual(
            raised.exception.error_code,
            "subscription_sharing_unsupported_capability",
        )
        self.assertEqual(raised.exception.status_code, 200)
        self.assertNotIn("private upstream detail", str(raised.exception))

    def test_preview_incompatible_or_worker_selected_authority_fails_before_http(
        self,
    ) -> None:
        invalid = (
            ({**self.request(), "extra": True}, "shape"),
            (self.request(model="worker-model"), "model"),
            (self.request(store=True), "store"),
            (self.request(stream=False), "stream"),
            (self.request(input="not-an-array"), "input"),
            (self.request(max_output_tokens=100), "unsupported"),
            (self.request(service_tier="priority"), "unsupported"),
            (self.request(future_provider_capability=True), "unsupported"),
            (
                self.request(
                    input=[{"type": "message", "role": "system", "content": "no"}]
                ),
                "system",
            ),
            (
                {
                    **self.request(),
                    "headers": {"authorization": "Bearer worker-secret"},
                },
                "header",
            ),
        )
        for request, message in invalid:
            http = FakeHTTPClient()
            with (
                self.subTest(request=request),
                self.assertRaisesRegex(ChatGPTResponseFailure, message),
            ):
                self.invoke(http, request)
            self.assertEqual(http.requests, [])

    def test_known_stream_and_http_errors_preserve_only_bounded_metadata(self) -> None:
        cases = (
            (
                HTTPResponse(
                    200,
                    "text/event-stream",
                    b'data: {"type":"response.failed","response":{"error":'
                    b'{"code":"subscription_sharing_usage_limit_exceeded"}}}\n\n',
                    "req_stream",
                ),
                "subscription_sharing_usage_limit_exceeded",
                200,
            ),
            (
                HTTPResponse(
                    429,
                    "application/json",
                    b'{"error":{"code":"subscription_sharing_usage_limit_exceeded",'
                    b'"message":"private upstream detail"}}',
                    "req_http",
                ),
                "subscription_sharing_usage_limit_exceeded",
                429,
            ),
        )
        for response, code, status in cases:
            with self.subTest(status=status):
                with self.assertRaises(ChatGPTResponseFailure) as raised:
                    self.invoke(FakeHTTPClient(response))
                self.assertEqual(raised.exception.error_code, code)
                self.assertEqual(raised.exception.status_code, status)
                self.assertNotIn("private upstream detail", str(raised.exception))
                self.assertNotIn("access-secret", str(raised.exception))

    def test_interrupted_or_malformed_post_dispatch_outcome_is_ambiguous(self) -> None:
        responses: tuple[HTTPResponse | Exception, ...] = (
            HTTPResponse(
                200,
                "text/event-stream",
                b'data: {"type":"response.created"}\n\n',
                "req_truncated",
            ),
            HTTPResponse(200, "text/event-stream", b"not-sse", "req_malformed"),
            HTTPResponse(
                200,
                "application/json",
                completed_sse(),
                "req_wrong_content_type",
            ),
            HTTPTransportError("private network detail", ambiguous=True),
        )
        for response in responses:
            with (
                self.subTest(response=response),
                self.assertRaisesRegex(ChatGPTResponseAmbiguousError, "ambiguous"),
            ):
                self.invoke(FakeHTTPClient(response))

        with self.assertRaisesRegex(ChatGPTResponseFailure, "unavailable"):
            self.invoke(
                FakeHTTPClient(
                    HTTPTransportError("private network detail", ambiguous=False)
                )
            )

    def test_protocol_shape_tool_and_header_matrix_fails_before_http(self) -> None:
        invalid = (
            (
                {**self.request(), "protocol": "anthropic-messages"},
                "protocol_mismatch",
            ),
            ({**self.request(), "body": []}, "invalid_request"),
            (self.request(tools={}), "invalid_request"),
            (self.request(tools=[{"type": "function"}]), "unsupported_capability"),
            (self.request(tools=[{"type": "mcp"}]), "unsupported_capability"),
            (
                self.request(
                    tools=[{"type": "web_search", "external_web_access": False}]
                ),
                "unsupported_capability",
            ),
            (
                self.request(tools=[{"type": "future_hosted_tool"}]),
                "unsupported_capability",
            ),
            (self.request(tools=["not-an-object"]), "unsupported_capability"),
            (
                self.request(
                    tools=[
                        {
                            "type": "namespace",
                            "name": "malformed",
                            "description": "Invalid namespace.",
                            "tools": [],
                        }
                    ]
                ),
                "unsupported_capability",
            ),
            (
                self.request(
                    tools=[
                        {
                            "type": "namespace",
                            "name": "malformed",
                            "description": "Invalid namespace.",
                            "tools": "not-an-array",
                        }
                    ]
                ),
                "unsupported_capability",
            ),
            (
                self.request(
                    tools=[
                        {
                            "type": "namespace",
                            "name": "malformed",
                            "description": "Invalid namespace.",
                            "tools": [{"type": "function"}],
                        }
                    ]
                ),
                "unsupported_capability",
            ),
            (
                self.request(
                    tools=[
                        {
                            "type": "namespace",
                            "name": "malformed",
                            "description": "Invalid namespace.",
                            "tools": [
                                {
                                    "type": "function",
                                    "name": "nested",
                                    "description": "Nested function.",
                                    "parameters": {
                                        "type": "object",
                                        "properties": {},
                                    },
                                    "strict": True,
                                }
                            ],
                            "future_field": True,
                        }
                    ]
                ),
                "unsupported_capability",
            ),
            (
                self.request(
                    tools=[
                        {
                            "type": "namespace",
                            "name": "unsafe",
                            "description": "Nested hosted tool.",
                            "tools": [{"type": "web_search"}],
                        }
                    ]
                ),
                "unsupported_capability",
            ),
            (self.request(tools=[{}] * 257), "invalid_request"),
            (self.request(reasoning={"effort": float("nan")}), "invalid_request"),
            ({**self.request(), "headers": []}, "invalid_request"),
            ({**self.request(), "headers": {7: "value"}}, "invalid_request"),
            (
                {
                    **self.request(),
                    "headers": {
                        "X-Codex-Beta-Features": "one",
                        "x-codex-beta-features": "two",
                    },
                },
                "invalid_request",
            ),
            ({**self.request(), "headers": {"openai-beta": ""}}, "invalid_request"),
            (
                {**self.request(), "headers": {"openai-beta": "line\nbreak"}},
                "invalid_request",
            ),
        )
        for request, error_code in invalid:
            http = FakeHTTPClient()
            with (
                self.subTest(request=request),
                self.assertRaises(ChatGPTResponseFailure) as raised,
            ):
                self.invoke(http, request)  # type: ignore[arg-type]
            self.assertEqual(raised.exception.error_code, error_code)
            self.assertEqual(http.requests, [])

    def test_auth_deadline_and_request_size_fail_before_http(self) -> None:
        client = OpenAIResponsesClient(FakeHTTPClient())
        common = {
            "model": MODEL,
            "request": self.request(),
            "request_limit_bytes": 65536,
            "response_limit_bytes": 65536,
            "deadline": datetime.now(UTC) + timedelta(seconds=30),
        }
        with self.assertRaises(ChatGPTResponseFailure) as missing_auth:
            client.invoke(access_token="", **common)
        self.assertEqual(missing_auth.exception.error_code, "authentication_required")

        with self.assertRaises(ChatGPTResponseFailure) as expired:
            client.invoke(
                access_token="access-secret",  # noqa: S106 - test token
                **{**common, "deadline": datetime.now(UTC) - timedelta(seconds=1)},
            )
        self.assertEqual(expired.exception.error_code, "deadline_expired")

        with self.assertRaises(ChatGPTResponseFailure) as oversized:
            client.invoke(
                access_token="access-secret",  # noqa: S106 - test token
                **{**common, "request_limit_bytes": 1},
            )
        self.assertEqual(oversized.exception.error_code, "request_too_large")

    def test_terminal_stream_variants_are_classified_without_replay(self) -> None:
        incomplete = HTTPResponse(
            200,
            "text/event-stream",
            b'data: {"type":"response.incomplete"}\n\n',
            None,
        )
        with self.assertRaises(ChatGPTResponseFailure) as raised:
            self.invoke(FakeHTTPClient(incomplete))
        self.assertEqual(raised.exception.error_code, "response_incomplete")

        ambiguous = (
            HTTPResponse(200, "application/json", completed_sse(), None),
            HTTPResponse(
                200,
                "text/event-stream",
                b'data: {"type":"response.completed",'
                b'"response":{"status":"failed"}}\n\n',
                None,
            ),
            HTTPResponse(
                200,
                "text/event-stream",
                completed_sse() + completed_sse(),
                None,
            ),
            HTTPResponse(200, "text/event-stream", b"data: {bad-json}\n\n", None),
            HTTPResponse(200, "text/event-stream", b"data: \xff\n\n", None),
        )
        for response in ambiguous:
            with (
                self.subTest(
                    content_type=response.content_type, body=response.body[:20]
                ),
                self.assertRaises(ChatGPTResponseAmbiguousError),
            ):
                self.invoke(FakeHTTPClient(response))

    def test_http_error_fallback_and_encoded_response_limit_are_bounded(self) -> None:
        malformed = HTTPResponse(500, "application/json", b"not-json", None)
        with self.assertRaises(ChatGPTResponseFailure) as raised:
            self.invoke(FakeHTTPClient(malformed))
        self.assertEqual(raised.exception.error_code, "upstream_error")

        raw = completed_sse()
        with self.assertRaises(ChatGPTResponseAmbiguousError) as oversized:
            OpenAIResponsesClient(
                FakeHTTPClient(HTTPResponse(200, "text/event-stream", raw, None))
            ).invoke(
                access_token="access-secret",  # noqa: S106 - test token
                model=MODEL,
                request=self.request(),
                request_limit_bytes=65536,
                response_limit_bytes=len(raw),
                deadline=datetime.now(UTC) + timedelta(seconds=30),
            )
        self.assertEqual(oversized.exception.error_code, "upstream_ambiguous")


class OpenAIChatGPTConnectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = ChatGPTSession.from_mapping(session_mapping())
        self.scope = self.session.credential_scope_sha256
        self.config = OpenAIConnectorConfig.from_mapping(
            {
                "schema_version": 1,
                "connector_id": CONNECTOR_ID,
                "deployment_sha256": DEPLOYMENT,
                "routes": [
                    {
                        "name": "chatgpt-main",
                        "credential_scope_sha256": self.scope,
                        "models": [MODEL],
                        "request_bytes": 65536,
                        "response_bytes": 65536,
                        "timeout_ms": 30000,
                    }
                ],
            }
        )

    def connector(self, http: FakeHTTPClient) -> OpenAIChatGPTConnector:
        class Credentials:
            def __init__(self, session: ChatGPTSession):
                self._session = session

            def session(self) -> ChatGPTSession:
                return self._session

            def access_token(self) -> str:
                return self._session.access_token

        return OpenAIChatGPTConnector(
            self.config,
            credentials=Credentials(self.session),
            responses=OpenAIResponsesClient(http),
        )

    def test_connector_maps_one_exact_authorized_completed_invocation(self) -> None:
        request = invocation_request(self.scope)
        response = self.connector(
            FakeHTTPClient(
                HTTPResponse(200, "text/event-stream", completed_sse(), "req_live")
            )
        ).invoke(request)

        self.assertEqual(response.correlation, request.correlation)
        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.provider_status, 200)
        self.assertEqual(
            response.untrusted_response["protocol"],  # type: ignore[index]
            "openai-responses",
        )

    def test_connector_rejects_wrong_authority_before_provider_transport(self) -> None:
        cases = (
            ({"connector_id": "ifc_" + "9" * 32}, "connector"),
            ({"deployment_sha256": "9" * 64}, "deployment"),
            ({"route": "other-route"}, "route"),
            ({"credential_scope_sha256": "9" * 64}, "credential scope"),
            ({"provider": "anthropic"}, "provider"),
            ({"protocol": "anthropic-messages"}, "protocol"),
            ({"model": "other-model"}, "model"),
            ({"request_limit_bytes": 65537}, "request limit"),
            ({"response_limit_bytes": 65537}, "response limit"),
        )
        for overrides, message in cases:
            http = FakeHTTPClient()
            response = self.connector(http).invoke(
                invocation_request(self.scope, **overrides)
            )
            with self.subTest(overrides=overrides):
                self.assertEqual(response.outcome, "failed")
                self.assertRegex(response.error_code or "", message.replace(" ", "_"))
                self.assertEqual(http.requests, [])

    def test_connector_preserves_known_and_ambiguous_outcomes(self) -> None:
        failed = self.connector(
            FakeHTTPClient(
                HTTPResponse(
                    429,
                    "application/json",
                    b'{"error":{"code":"subscription_sharing_usage_limit_exceeded"}}',
                    "req_limit",
                )
            )
        ).invoke(invocation_request(self.scope))
        ambiguous = self.connector(
            FakeHTTPClient(HTTPTransportError("private", ambiguous=True))
        ).invoke(invocation_request(self.scope))

        self.assertEqual(failed.outcome, "failed")
        self.assertEqual(
            failed.error_code,
            "subscription_sharing_usage_limit_exceeded",
        )
        self.assertEqual(ambiguous.outcome, "ambiguous")
        self.assertEqual(ambiguous.error_code, "upstream_ambiguous")

    def test_configuration_schema_and_route_limits_fail_closed(self) -> None:
        def value() -> dict[str, object]:
            return {
                "schema_version": 1,
                "connector_id": CONNECTOR_ID,
                "deployment_sha256": DEPLOYMENT,
                "routes": [
                    {
                        "name": "chatgpt-main",
                        "credential_scope_sha256": self.scope,
                        "models": [MODEL],
                        "request_bytes": 65536,
                        "response_bytes": 65536,
                        "timeout_ms": 30000,
                    }
                ],
            }

        invalid: list[object] = [
            [],
            {**value(), "extra": True},
            {**value(), "schema_version": 2},
            {**value(), "connector_id": "wrong"},
            {**value(), "deployment_sha256": "wrong"},
            {**value(), "routes": []},
        ]
        route_overrides = (
            {"name": "UPPER"},
            {"credential_scope_sha256": "wrong"},
            {"models": []},
            {"models": [MODEL, MODEL]},
            {"request_bytes": True},
            {"response_bytes": 4 * 1024 * 1024 + 1},
            {"timeout_ms": 99},
        )
        for overrides in route_overrides:
            candidate = value()
            route = dict(candidate["routes"][0])  # type: ignore[index]
            route.update(overrides)
            candidate["routes"] = [route]
            invalid.append(candidate)
        duplicate = value()
        duplicate["routes"] = [
            duplicate["routes"][0],  # type: ignore[index]
            duplicate["routes"][0],  # type: ignore[index]
        ]
        invalid.append(duplicate)

        for candidate in invalid:
            with (
                self.subTest(candidate=candidate),
                self.assertRaises(ChatGPTConnectorError),
            ):
                OpenAIConnectorConfig.from_mapping(candidate)  # type: ignore[arg-type]

        self.assertIsNone(self.config.route("not-configured"))

    def test_deadline_authority_and_credential_failures_do_not_reach_provider(
        self,
    ) -> None:
        deadline_cases = (
            (
                (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                "deadline_expired",
            ),
            (
                (datetime.now(UTC) + timedelta(minutes=2)).isoformat(),
                "deadline_exceeds_route",
            ),
        )
        for deadline, error_code in deadline_cases:
            http = FakeHTTPClient()
            response = self.connector(http).invoke(
                invocation_request(self.scope, deadline=deadline)
            )
            with self.subTest(error_code=error_code):
                self.assertEqual(response.error_code, error_code)
                self.assertEqual(http.requests, [])

        class Credentials:
            def __init__(
                self,
                selected: ChatGPTSession,
                error: ChatGPTAuthenticationError | None = None,
            ) -> None:
                self.selected = selected
                self.error = error

            def session(self) -> ChatGPTSession:
                if self.error is not None:
                    raise self.error
                return self.selected

            def access_token(self) -> str:
                if self.error is not None:
                    raise self.error
                return self.selected.access_token

        mismatched = ChatGPTSession.from_mapping(
            session_mapping(subject="another-account")
        )
        cases = (
            Credentials(mismatched),
            Credentials(
                self.session,
                ChatGPTAuthenticationError("private credential detail"),
            ),
        )
        for credentials in cases:
            http = FakeHTTPClient()
            connector = OpenAIChatGPTConnector(
                self.config,
                credentials=credentials,
                responses=OpenAIResponsesClient(http),
            )
            response = connector.invoke(invocation_request(self.scope))
            self.assertIn(
                response.error_code,
                {"authentication_required", "credential_scope_mismatch"},
            )
            self.assertEqual(http.requests, [])

    def test_connector_requires_typed_requests_and_caps_failure_accounting(
        self,
    ) -> None:
        connector = self.connector(FakeHTTPClient())
        with self.assertRaisesRegex(
            ChatGPTConnectorError, "InferenceInvocationRequest"
        ):
            connector.invoke({})  # type: ignore[arg-type]

        class FailingResponses:
            def invoke(self, **_kwargs: object):
                raise ChatGPTResponseFailure(
                    "bounded failure",
                    error_code="upstream_error",
                    request_bytes=999999,
                    response_bytes=999999,
                )

        class Credentials:
            def session(self) -> ChatGPTSession:
                return self_session

            def access_token(self) -> str:
                return self_session.access_token

        self_session = self.session
        capped = OpenAIChatGPTConnector(
            self.config,
            credentials=Credentials(),
            responses=FailingResponses(),  # type: ignore[arg-type]
        ).invoke(invocation_request(self.scope))
        self.assertEqual(capped.request_bytes, 65536)
        self.assertEqual(capped.response_bytes, 65536)


if __name__ == "__main__":
    unittest.main()
