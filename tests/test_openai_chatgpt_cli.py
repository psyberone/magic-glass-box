from __future__ import annotations

import argparse
import contextlib
import http.client
import io
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from magic_glass_box.inference import (
    InferenceInvocationRequest,
    InferenceInvocationResponse,
    UnixInferenceConnectorClient,
)
from magic_glass_box.openai_chatgpt import (
    AuthorizationAttempt,
    ChatGPTAuthenticationError,
    ChatGPTConnectorError,
    ChatGPTModel,
    ChatGPTProfileStore,
    ChatGPTSession,
)
from magic_glass_box.openai_chatgpt_cli import (
    _callback_seconds,
    _CallbackServer,
    _login,
    _normalize_callback_target,
    _positive_integer,
    _read_request,
    _recv_exact,
    _serve,
    _status_payload,
    build_parser,
    load_config,
    main,
    serve_unix,
)

CLIENT_ID = "oaiapp_" + "a" * 32
CONNECTOR_ID = "ifc_" + "1" * 32
DEPLOYMENT = "b" * 64


def session(host_id: str) -> ChatGPTSession:
    return ChatGPTSession.from_mapping(
        {
            "schema_version": 1,
            "client_id": CLIENT_ID,
            "ext_agent_host_id": host_id,
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
                "offline_access",
                "openid",
                "resource.invoke",
            ],
        }
    )


def config_value(scope: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "connector_id": CONNECTOR_ID,
        "deployment_sha256": DEPLOYMENT,
        "routes": [
            {
                "name": "chatgpt-main",
                "credential_scope_sha256": scope,
                "models": ["gpt-test"],
                "request_bytes": 65536,
                "response_bytes": 65536,
                "timeout_ms": 30000,
            }
        ],
    }


def invocation(scope: str) -> InferenceInvocationRequest:
    return InferenceInvocationRequest.from_mapping(
        {
            "schema_version": 1,
            "connector_protocol_version": 1,
            "invocation_id": "pinv_" + "3" * 32,
            "connector_id": CONNECTOR_ID,
            "deployment_sha256": DEPLOYMENT,
            "route": "chatgpt-main",
            "grant_id": "pgr_" + "2" * 32,
            "policy_sha256": "c" * 64,
            "credential_scope_sha256": scope,
            "provider": "openai",
            "protocol": "openai-responses",
            "model": "gpt-test",
            "deadline": (datetime.now(UTC) + timedelta(seconds=10)).isoformat(),
            "request_limit_bytes": 65536,
            "response_limit_bytes": 65536,
            "request": {
                "protocol": "openai-responses",
                "body": {"input": [], "store": False, "stream": True},
                "headers": {},
            },
        }
    )


class EchoConnector:
    def __init__(self) -> None:
        self.requests: list[InferenceInvocationRequest] = []

    def invoke(
        self, request: InferenceInvocationRequest
    ) -> InferenceInvocationResponse:
        self.requests.append(request)
        authority = request.to_dict()
        authority.pop("request")
        return InferenceInvocationResponse.from_mapping(
            {
                **authority,
                "outcome": "succeeded",
                "provider_status": 200,
                "request_bytes": 10,
                "response_bytes": 20,
                "error_code": None,
                "response": {"output_text": "done"},
            }
        )


class OpenAIChatGPTCLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.store = ChatGPTProfileStore(self.root / "profile")
        host_id = self.store.host_id()
        self.store.save_registration(CLIENT_ID)
        self.session = session(host_id)
        self.store.save_session(self.session)

    def test_parser_has_explicit_account_and_server_commands(self) -> None:
        parser = build_parser()
        login = parser.parse_args(
            ["login", "--profile-dir", str(self.store.directory), "--no-browser"]
        )
        status = parser.parse_args(
            ["status", "--profile-dir", str(self.store.directory)]
        )
        models = parser.parse_args(
            ["models", "--profile-dir", str(self.store.directory)]
        )
        serve = parser.parse_args(
            [
                "serve",
                "--profile-dir",
                str(self.store.directory),
                "--config",
                str(self.root / "config.json"),
                "--socket-path",
                str(self.root / "connector.sock"),
                "--max-invocations",
                "3",
            ]
        )

        self.assertTrue(login.no_browser)
        self.assertEqual(status.command, "status")
        self.assertEqual(models.command, "models")
        self.assertEqual(serve.max_invocations, 3)

    def test_callback_parser_accepts_one_callback_and_rejects_ambiguous_values(
        self,
    ) -> None:
        self.assertEqual(
            _normalize_callback_target(
                "/auth/callback?code=abc&state=state&client_id=" + CLIENT_ID
            ),
            {"client_id": CLIENT_ID, "code": "abc", "state": "state"},
        )
        self.assertEqual(
            _normalize_callback_target(
                "/auth/callback?error=access_denied&state=state"
            ),
            {"error": "access_denied", "state": "state"},
        )
        for target in (
            "/wrong?code=abc&state=state",
            "/auth/callback?code=one&code=two&state=state",
            "/auth/callback?code=abc",
            "/auth/callback?code=&state=state",
        ):
            with (
                self.subTest(target=target),
                self.assertRaises(ChatGPTAuthenticationError),
            ):
                _normalize_callback_target(target)

    def test_status_is_metadata_only_before_and_after_sign_in(self) -> None:
        payload = _status_payload(self.store)

        self.assertEqual(payload["status"], "connected")
        self.assertEqual(payload["email"], "operator@example.com")
        self.assertEqual(
            payload["credential_scope_sha256"],
            self.session.credential_scope_sha256,
        )
        encoded = json.dumps(payload)
        for secret in ("access-secret", "refresh-secret", "id-secret"):
            self.assertNotIn(secret, encoded)

        self.store.clear_session()
        self.assertEqual(_status_payload(self.store)["status"], "registration-only")

    def test_config_loader_requires_one_private_regular_json_file(self) -> None:
        path = self.root / "connector.json"
        path.write_text(
            json.dumps(config_value(self.session.credential_scope_sha256)),
            encoding="utf-8",
        )
        path.chmod(0o600)

        loaded = load_config(path)

        self.assertEqual(loaded.connector_id, CONNECTOR_ID)
        path.chmod(0o644)
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "private"):
            load_config(path)

        path.chmod(0o600)
        path.write_text('{"schema_version":1,"schema_version":1}', encoding="utf-8")
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "invalid"):
            load_config(path)

    def test_unix_server_frames_one_request_and_removes_only_its_socket(self) -> None:
        connector = EchoConnector()
        socket_path = self.root / "connector.sock"
        result: list[int] = []
        failure: list[BaseException] = []

        def run() -> None:
            try:
                result.append(serve_unix(socket_path, 1, connector))
            except BaseException as exc:  # pragma: no cover - asserted below
                failure.append(exc)

        thread = threading.Thread(target=run)
        thread.start()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not socket_path.exists():
            time.sleep(0.01)
        self.assertTrue(socket_path.exists())

        request = invocation(self.session.credential_scope_sha256)
        response = UnixInferenceConnectorClient(socket_path).invoke(request)
        thread.join(3)

        self.assertEqual(failure, [])
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [1])
        self.assertEqual(connector.requests, [request])
        self.assertEqual(response.outcome, "succeeded")
        self.assertFalse(socket_path.exists())

        protected = self.root / "operator-data"
        protected.write_text("keep", encoding="utf-8")
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "already exists"):
            serve_unix(protected, 1, connector)
        self.assertEqual(protected.read_text(encoding="utf-8"), "keep")

    def test_unix_server_isolates_malformed_and_idle_clients(self) -> None:
        connector = EchoConnector()
        socket_path = self.root / "resilient-connector.sock"
        result: list[int] = []
        failure: list[BaseException] = []

        def run() -> None:
            try:
                result.append(
                    serve_unix(
                        socket_path,
                        1,
                        connector,
                        client_timeout=0.05,
                    )
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                failure.append(exc)

        thread = threading.Thread(target=run)
        thread.start()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not socket_path.exists():
            time.sleep(0.01)
        self.assertTrue(socket_path.exists())

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as malformed:
            malformed.connect(str(socket_path))
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as idle:
            idle.connect(str(socket_path))
            time.sleep(0.1)

        request = invocation(self.session.credential_scope_sha256)
        response = UnixInferenceConnectorClient(socket_path).invoke(request)
        thread.join(3)

        self.assertEqual(failure, [])
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [1])
        self.assertEqual(connector.requests, [request])
        self.assertEqual(response.outcome, "succeeded")
        self.assertFalse(socket_path.exists())

    def test_unix_server_removes_socket_after_sigterm(self) -> None:
        socket_path = self.root / "terminated-connector.sock"
        script = "\n".join(
            (
                "import sys",
                "from pathlib import Path",
                "from magic_glass_box.openai_chatgpt_cli import serve_unix",
                "class Connector:",
                "    def invoke(self, request):",
                "        raise AssertionError('no invocation expected')",
                "serve_unix(Path(sys.argv[1]), 1, Connector())",
            )
        )
        environment = os.environ.copy()
        source_root = str(Path(__file__).resolve().parents[1] / "src")
        environment["PYTHONPATH"] = os.pathsep.join(
            value for value in (source_root, environment.get("PYTHONPATH", "")) if value
        )
        process = subprocess.Popen(  # noqa: S603 - fixed interpreter argv
            [sys.executable, "-c", script, str(socket_path)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
        )
        self.addCleanup(lambda: process.kill() if process.poll() is None else None)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not socket_path.exists():
            if process.poll() is not None:
                break
            time.sleep(0.01)
        self.assertTrue(socket_path.exists())

        process.terminate()
        stdout, stderr = process.communicate(timeout=3)

        self.assertEqual(process.returncode, 0, (stdout, stderr))
        self.assertFalse(socket_path.exists())

    def test_status_command_never_prints_tokens(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["status", "--profile-dir", str(self.store.directory)])

        self.assertEqual(exit_code, 0)
        value = json.loads(stdout.getvalue())
        self.assertEqual(value["status"], "connected")
        for secret in ("access-secret", "refresh-secret", "id-secret"):
            self.assertNotIn(secret, stdout.getvalue())

    def test_numeric_argument_types_reject_invalid_bounds(self) -> None:
        self.assertEqual(_positive_integer("3"), 3)
        self.assertEqual(_callback_seconds("1.5"), 1.5)
        for value in ("not-a-number", "0", "-1"):
            with (
                self.subTest(integer=value),
                self.assertRaises(argparse.ArgumentTypeError),
            ):
                _positive_integer(value)
        for value in ("not-a-number", "0", "901"):
            with (
                self.subTest(seconds=value),
                self.assertRaises(argparse.ArgumentTypeError),
            ):
                _callback_seconds(value)

    def test_loopback_callback_server_accepts_valid_and_rejects_invalid_targets(
        self,
    ) -> None:
        with _CallbackServer() as server:
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            connection = http.client.HTTPConnection(*server.server_address, timeout=2)
            connection.request("GET", "/wrong?code=abc&state=state")
            response = connection.getresponse()
            self.assertEqual(response.status, 400)
            self.assertIn(b"invalid", response.read())
            connection.close()
            thread.join(2)
            self.assertIsNone(server.result)

            thread = threading.Thread(target=server.handle_request)
            thread.start()
            connection = http.client.HTTPConnection(*server.server_address, timeout=2)
            connection.request("GET", "/auth/callback?code=abc&state=state")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertIn(b"close this window", response.read())
            connection.close()
            thread.join(2)
            self.assertEqual(server.result, {"code": "abc", "state": "state"})

    def test_login_prints_only_the_authorization_url_when_browser_is_disabled(
        self,
    ) -> None:
        attempt = AuthorizationAttempt(
            authorization_url="https://auth.openai.com/authorize?state=public",
            state="state",
            nonce="nonce",
            code_verifier="verifier",
            redirect_uri="http://127.0.0.1:43210/auth/callback",
            requested_client_id=CLIENT_ID,
            new_registration=False,
        )

        class FakeOAuth:
            completed: dict[str, str] | None = None

            def __init__(self, _store: ChatGPTProfileStore) -> None:
                pass

            def begin(self, _redirect_uri: str) -> AuthorizationAttempt:
                return attempt

            def complete(
                self,
                _attempt: AuthorizationAttempt,
                callback: dict[str, str],
            ) -> ChatGPTSession:
                self.completed = callback
                return self_session

        class FakeCallbackServer:
            result: dict[str, str] | None = None
            server_address = ("127.0.0.1", 43210)
            timeout = 0.0

            def __enter__(self):
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def handle_request(self) -> None:
                self.result = {"code": "abc", "state": "state"}

        self_session = self.session
        stdout = io.StringIO()
        with (
            patch("magic_glass_box.openai_chatgpt_cli.ChatGPTOAuth", FakeOAuth),
            patch(
                "magic_glass_box.openai_chatgpt_cli._CallbackServer",
                FakeCallbackServer,
            ),
            contextlib.redirect_stdout(stdout),
        ):
            payload = _login(
                self.store,
                no_browser=True,
                callback_timeout=5,
            )

        first_line = json.loads(stdout.getvalue().splitlines()[0])
        self.assertEqual(first_line, {"authorization_url": attempt.authorization_url})
        self.assertEqual(payload["status"], "connected")
        for secret in ("access-secret", "refresh-secret", "id-secret", "verifier"):
            self.assertNotIn(secret, stdout.getvalue())

    def test_status_and_config_failure_paths_are_explicit(self) -> None:
        self.store.clear_session()
        registration = self.store.directory / "registration.json"
        registration.unlink()
        self.assertEqual(_status_payload(self.store)["status"], "not-connected")

        with self.assertRaisesRegex(ChatGPTAuthenticationError, "absolute"):
            load_config(Path("relative.json"))
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "unavailable"):
            load_config(self.root / "missing.json")
        empty = self.root / "empty.json"
        empty.write_bytes(b"")
        empty.chmod(0o600)
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "empty"):
            load_config(empty)

    def test_request_framing_rejects_truncation_invalid_json_and_oversize(self) -> None:
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        right.close()
        with self.assertRaisesRegex(ChatGPTConnectorError, "ended early"):
            _recv_exact(left, 1)

        for raw in (
            struct.pack("!I", 0),
            struct.pack("!I", 4) + b"nope",
        ):
            left, right = socket.socketpair()
            with left, right:
                right.sendall(raw)
                with self.assertRaises(ChatGPTConnectorError):
                    _read_request(left)

    def test_server_and_serve_configuration_boundaries_fail_closed(self) -> None:
        connector = EchoConnector()
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "absolute"):
            serve_unix(Path("connector.sock"), 1, connector)
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "positive"):
            serve_unix(self.root / "connector.sock", 0, connector)
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "timeout"):
            serve_unix(
                self.root / "connector.sock",
                1,
                connector,
                client_timeout=0,
            )

        public = self.root / "public"
        public.mkdir(mode=0o755)
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "private"):
            serve_unix(public / "connector.sock", 1, connector)

        config_path = self.root / "mismatch.json"
        config_path.write_text(json.dumps(config_value("f" * 64)), encoding="utf-8")
        config_path.chmod(0o600)
        args = argparse.Namespace(
            profile_dir=self.store.directory,
            config=config_path,
            socket_path=self.root / "connector.sock",
            max_invocations=1,
        )
        with self.assertRaisesRegex(ChatGPTAuthenticationError, "selected"):
            _serve(args)

    def test_main_dispatches_models_login_and_serve_and_redacts_errors(self) -> None:
        class FakeOAuth:
            def __init__(self, _store: ChatGPTProfileStore) -> None:
                pass

            def list_models(self) -> tuple[ChatGPTModel, ...]:
                return (ChatGPTModel("gpt-test", "GPT Test"),)

        commands = (
            (
                ["login", "--profile-dir", str(self.store.directory), "--no-browser"],
                "_login",
                {"status": "connected"},
            ),
            (
                [
                    "serve",
                    "--profile-dir",
                    str(self.store.directory),
                    "--config",
                    str(self.root / "config.json"),
                    "--socket-path",
                    str(self.root / "connector.sock"),
                    "--max-invocations",
                    "1",
                ],
                "_serve",
                {"invocations": 1, "status": "stopped"},
            ),
        )
        for argv, target, result in commands:
            stdout = io.StringIO()
            with (
                patch(
                    f"magic_glass_box.openai_chatgpt_cli.{target}", return_value=result
                ),
                contextlib.redirect_stdout(stdout),
            ):
                self.assertEqual(main(argv), 0)
            self.assertEqual(json.loads(stdout.getvalue()), result)

        stdout = io.StringIO()
        with (
            patch("magic_glass_box.openai_chatgpt_cli.ChatGPTOAuth", FakeOAuth),
            contextlib.redirect_stdout(stdout),
        ):
            self.assertEqual(
                main(["models", "--profile-dir", str(self.store.directory)]),
                0,
            )
        self.assertEqual(json.loads(stdout.getvalue())["models"][0]["slug"], "gpt-test")

        stderr = io.StringIO()
        with (
            patch(
                "magic_glass_box.openai_chatgpt_cli._status_payload",
                side_effect=OSError("bounded failure"),
            ),
            contextlib.redirect_stderr(stderr),
        ):
            self.assertEqual(
                main(["status", "--profile-dir", str(self.store.directory)]),
                2,
            )
        self.assertIn("bounded failure", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
