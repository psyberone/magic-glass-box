from __future__ import annotations

import io
import json
import os
import shlex
import socket
import stat
import struct
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

from magic_glass_box.domain import canonical_json, content_sha256
from magic_glass_box.github_provider import (
    GitHubAPIResponse,
    GitHubProvider,
    GitHubProviderError,
    GitHubTransportError,
)
from magic_glass_box.github_provider_cli import (
    BoundedCommandError,
    BoundedCommandRunner,
    GitHubCLI,
    GitHubCommandResult,
    GitHubCredentialIdentity,
    GitHubGitPublisher,
    GitHubProviderCLIError,
    _parse_included_response,
    _read_request,
    _write_response,
    build_parser,
    load_spec,
    main,
    serve_unix,
    validate_spec_credentials,
)
from magic_glass_box.service_sidecar import ServiceSidecarSpec, UnixServiceSidecarClient
from tests.test_github_provider import (
    MERGED,
    api_response,
    pull,
    ref,
    request,
    sidecar_spec,
)


def included(
    status: int,
    body: object,
    *,
    headers: Mapping[str, str] | None = None,
) -> bytes:
    reason = "OK" if 200 <= status < 300 else "Not Found"
    lines = [f"HTTP/2.0 {status} {reason}"]
    lines.extend(f"{name}: {value}" for name, value in (headers or {}).items())
    return (
        "\r\n".join(lines).encode("ascii")
        + b"\r\n\r\n"
        + canonical_json(body).encode("utf-8")
        + b"\n"
    )


class FakeRunner:
    def __init__(self, results: Sequence[GitHubCommandResult | BaseException]) -> None:
        self.results = list(results)
        self.calls: list[dict[str, Any]] = []

    def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None,
        env: Mapping[str, str],
        timeout_seconds: float,
        stdout_limit_bytes: int,
        stderr_limit_bytes: int,
    ) -> GitHubCommandResult:
        self.calls.append(
            {
                "argv": list(argv),
                "stdin": stdin,
                "env": dict(env),
                "timeout_seconds": timeout_seconds,
                "stdout_limit_bytes": stdout_limit_bytes,
                "stderr_limit_bytes": stderr_limit_bytes,
            }
        )
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


class FakeAPI:
    def __init__(self, responses: Sequence[GitHubAPIResponse]) -> None:
        self.responses = list(responses)

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
        return self.responses.pop(0)


class GitHubProviderCLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.config_dir = self.root / "gh-config"
        self.config_dir.mkdir(mode=0o700)
        self.gh_bin = self.root / "gh"
        self.gh_bin.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        self.gh_bin.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        self.git_bin = self.root / "git"
        self.git_bin.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        self.git_bin.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

    def client(self, runner: FakeRunner) -> GitHubCLI:
        return GitHubCLI(
            gh_bin=self.gh_bin,
            config_dir=self.config_dir,
            runner=runner,
        )

    def test_include_parser_accepts_error_status_and_strict_bounded_json(self) -> None:
        raw = included(404, {"message": "missing"}, headers={"Retry-After": "7"})

        response = _parse_included_response(raw, body_limit_bytes=1024)

        self.assertEqual(response.status, 404)
        self.assertEqual(response.body, {"message": "missing"})
        self.assertEqual(response.retry_after_seconds, 7)
        self.assertEqual(response.response_bytes, len(b'{"message":"missing"}\n'))

        invalid = b'HTTP/2.0 200 OK\r\n\r\n{"duplicated":1,"duplicated":2}'
        with self.assertRaisesRegex(GitHubProviderCLIError, "invalid"):
            _parse_included_response(invalid, body_limit_bytes=1024)
        with self.assertRaisesRegex(GitHubProviderCLIError, "exceeds"):
            _parse_included_response(raw, body_limit_bytes=2)

    def test_include_parser_handles_interim_and_rejects_malformed_frames(self) -> None:
        interim = b"HTTP/1.1 100 Continue\r\n\r\n" + included(200, {"ok": True})
        parsed = _parse_included_response(interim, body_limit_bytes=1024)
        self.assertEqual(parsed.status, 200)
        self.assertEqual(parsed.body, {"ok": True})

        empty = _parse_included_response(
            b"HTTP/2 204 No Content\r\n\r\n", body_limit_bytes=1
        )
        self.assertEqual(empty.status, 204)
        self.assertIsNone(empty.body)

        malformed = (
            b"no headers",
            b"NOT-HTTP 200\r\n\r\n{}",
            b"HTTP/2 200 OK\r\nbad header\r\n\r\n{}",
            b"HTTP/2 200 OK\r\nRetry-After: 1\r\nRetry-After: 2\r\n\r\n{}",
            b"HTTP/2 200 OK\r\nRetry-After: 0\r\n\r\n{}",
            b"HTTP/2 200 OK\r\n\r\nNaN",
            b"HTTP/2 200 OK\r\n\r\n42",
            b"HTTP/2 200 OK\r\n\r\n\xff",
        )
        for raw in malformed:
            with self.subTest(raw=raw[:32]), self.assertRaises(GitHubProviderCLIError):
                _parse_included_response(raw, body_limit_bytes=1024)

    def test_api_transport_uses_only_explicit_environment_and_argv(self) -> None:
        runner = FakeRunner(
            [
                GitHubCommandResult(
                    returncode=1,
                    stdout=included(404, {"message": "missing"}),
                    stderr=b"gh: secret-bearing diagnostic",
                )
            ]
        )

        response = self.client(runner).request(
            "POST",
            host="github.com",
            endpoint="repos/acme/widgets/pulls",
            body={"title": "bounded"},
            deadline="2099-01-01T00:00:00.000000+00:00",
            response_limit_bytes=4096,
        )

        self.assertEqual(response.status, 404)
        call = runner.calls[0]
        self.assertEqual(
            call["argv"],
            [
                str(self.gh_bin.resolve()),
                "api",
                "--include",
                "--hostname",
                "github.com",
                "--method",
                "POST",
                "-H",
                "Accept: application/vnd.github+json",
                "-H",
                "X-GitHub-Api-Version: 2026-03-10",
                "--input",
                "-",
                "repos/acme/widgets/pulls",
            ],
        )
        self.assertEqual(call["stdin"], b'{"title":"bounded"}')
        self.assertEqual(call["timeout_seconds"], 120.0)
        self.assertEqual(call["stdout_limit_bytes"], 4096 + 64 * 1024)
        self.assertEqual(call["stderr_limit_bytes"], 64 * 1024)
        self.assertEqual(
            set(call["env"]),
            {
                "GH_CONFIG_DIR",
                "GH_NO_EXTENSION_UPDATE_NOTIFIER",
                "GH_NO_UPDATE_NOTIFIER",
                "GH_PAGER",
                "GH_PROMPT_DISABLED",
                "GH_TELEMETRY",
                "NO_COLOR",
                "PAGER",
                "PATH",
            },
        )
        self.assertNotIn("GH_TOKEN", call["env"])
        self.assertNotIn("GITHUB_TOKEN", call["env"])
        self.assertEqual(call["env"]["GH_CONFIG_DIR"], str(self.config_dir.resolve()))
        self.assertNotIn(b"secret-bearing", canonical_json(response.body).encode())

    def test_api_transport_rejects_unbounded_inputs_and_redacts_failures(self) -> None:
        client = self.client(FakeRunner([]))
        invalid = (
            {"method": "TRACE", "endpoint": "repos/acme/widgets", "body": None},
            {"method": "GET", "endpoint": "user", "body": None},
            {"method": "GET", "endpoint": "repos/acme/ bad", "body": None},
            {"method": "POST", "endpoint": "repos/acme/widgets", "body": []},
        )
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(GitHubTransportError):
                client.request(
                    **values,  # type: ignore[arg-type]
                    host="github.com",
                    deadline="2099-01-01T00:00:00.000000+00:00",
                    response_limit_bytes=1024,
                )
        with self.assertRaises(GitHubTransportError):
            client.request(
                "GET",
                host="github.com",
                endpoint="repos/acme/widgets",
                body=None,
                deadline="2099-01-01T00:00:00.000000+00:00",
                response_limit_bytes=0,
            )

        for method, dispatched, ambiguous in (
            ("POST", False, False),
            ("POST", True, True),
            ("GET", True, False),
        ):
            failing = self.client(
                FakeRunner([BoundedCommandError("private", dispatched=dispatched)])
            )
            with (
                self.subTest(method=method, dispatched=dispatched),
                self.assertRaises(GitHubTransportError) as captured,
            ):
                failing.request(
                    method,
                    host="github.com",
                    endpoint="repos/acme/widgets",
                    body=None,
                    deadline="2099-01-01T00:00:00.000000+00:00",
                    response_limit_bytes=1024,
                )
            self.assertEqual(captured.exception.ambiguous, ambiguous)
            self.assertNotIn("private", str(captured.exception))

        malformed = self.client(
            FakeRunner([GitHubCommandResult(0, b"bad", b"private")])
        )
        with self.assertRaises(GitHubTransportError) as captured:
            malformed.request(
                "POST",
                host="github.com",
                endpoint="repos/acme/widgets",
                body=None,
                deadline="2099-01-01T00:00:00.000000+00:00",
                response_limit_bytes=1024,
            )
        self.assertTrue(captured.exception.ambiguous)

    def test_credential_identity_is_fingerprinted_without_returning_token(self) -> None:
        token = "ghp_do-not-leak"  # noqa: S105
        runner = FakeRunner(
            [
                GitHubCommandResult(0, included(200, {"login": "mgb-bot"}), b""),
                GitHubCommandResult(0, f"{token}\n".encode(), b""),
            ]
        )

        identity = self.client(runner).credential_identity("github.com")

        expected = content_sha256(
            canonical_json(
                {
                    "account": "mgb-bot",
                    "host": "github.com",
                    "token_sha256": content_sha256(token),
                }
            )
        )
        self.assertEqual(identity.account, "mgb-bot")
        self.assertEqual(identity.host, "github.com")
        self.assertEqual(identity.credential_scope_sha256, expected)
        self.assertNotIn(token, repr(identity))
        environment = self.client(FakeRunner([])).environment
        self.assertEqual(
            runner.calls,
            [
                {
                    "argv": [
                        str(self.gh_bin.resolve()),
                        "api",
                        "--include",
                        "--hostname",
                        "github.com",
                        "--method",
                        "GET",
                        "-H",
                        "Accept: application/vnd.github+json",
                        "-H",
                        "X-GitHub-Api-Version: 2026-03-10",
                        "user",
                    ],
                    "stdin": None,
                    "env": environment,
                    "timeout_seconds": 30.0,
                    "stdout_limit_bytes": 128 * 1024,
                    "stderr_limit_bytes": 64 * 1024,
                },
                {
                    "argv": [
                        str(self.gh_bin.resolve()),
                        "auth",
                        "token",
                        "--hostname",
                        "github.com",
                    ],
                    "stdin": None,
                    "env": environment,
                    "timeout_seconds": 30.0,
                    "stdout_limit_bytes": 64 * 1024,
                    "stderr_limit_bytes": 64 * 1024,
                },
            ],
        )

    def test_credential_inspection_rejects_bad_account_and_token_responses(
        self,
    ) -> None:
        cases = (
            [GitHubCommandResult(0, included(403, {}), b"")],
            [GitHubCommandResult(0, included(200, {"login": ""}), b"")],
            [
                GitHubCommandResult(0, included(200, {"login": "mgb-bot"}), b""),
                BoundedCommandError("private", dispatched=False),
            ],
            [
                GitHubCommandResult(0, included(200, {"login": "mgb-bot"}), b""),
                GitHubCommandResult(1, b"", b"private"),
            ],
            [
                GitHubCommandResult(0, included(200, {"login": "mgb-bot"}), b""),
                GitHubCommandResult(0, b"one\ntwo", b""),
            ],
        )
        for results in cases:
            with (
                self.subTest(results=len(results)),
                self.assertRaises(GitHubProviderCLIError),
            ):
                self.client(FakeRunner(results)).credential_identity("github.com")

    def test_spec_credentials_must_match_every_mapping(self) -> None:
        spec = sidecar_spec(self.root / "github.sock")
        expected = spec.mappings[0]
        runner = FakeRunner(
            [
                GitHubCommandResult(0, included(200, {"login": expected.account}), b""),
                GitHubCommandResult(0, b"wrong-token\n", b""),
            ]
        )

        with self.assertRaisesRegex(GitHubProviderCLIError, "credential scope"):
            validate_spec_credentials(spec, self.client(runner))

        account_runner = FakeRunner(
            [
                GitHubCommandResult(0, included(200, {"login": "another-user"}), b""),
                GitHubCommandResult(0, b"token\n", b""),
            ]
        )
        with self.assertRaisesRegex(GitHubProviderCLIError, "account"):
            validate_spec_credentials(spec, self.client(account_runner))

    def test_load_spec_requires_an_absolute_regular_strict_json_file(self) -> None:
        spec = sidecar_spec(self.root / "github.sock")
        path = self.root / "provider.json"
        path.write_text(canonical_json(spec.to_dict()), encoding="utf-8")

        self.assertEqual(load_spec(path), spec)

        duplicate = self.root / "duplicate.json"
        duplicate.write_text(
            '{"schema_version":1,"schema_version":1}', encoding="utf-8"
        )
        with self.assertRaisesRegex(GitHubProviderCLIError, "invalid"):
            load_spec(duplicate)
        with self.assertRaisesRegex(GitHubProviderCLIError, "absolute"):
            load_spec(Path("provider.json"))

        empty = self.root / "empty.json"
        empty.write_bytes(b"")
        with self.assertRaisesRegex(GitHubProviderCLIError, "empty"):
            load_spec(empty)
        linked = self.root / "linked.json"
        linked.symlink_to(path)
        with self.assertRaisesRegex(GitHubProviderCLIError, "unavailable"):
            load_spec(linked)

    def test_real_unix_carrier_returns_a_correlated_provider_response(self) -> None:
        socket_path = self.root / "github.sock"
        spec = sidecar_spec(socket_path)
        provider = GitHubProvider(
            spec,
            FakeAPI(
                [
                    api_response(200, pull(state="closed", merged=True)),
                    api_response(200, ref(MERGED)),
                ]
            ),
        )
        completed: list[int] = []
        server = threading.Thread(
            target=lambda: completed.append(serve_unix(socket_path, 1, provider)),
            daemon=True,
        )
        server.start()
        deadline = time.monotonic() + 5
        while not socket_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(socket_path.exists())

        response = UnixServiceSidecarClient(socket_path).invoke(
            request(
                socket_path,
                "landing.invoke",
                phase="reconcile",
                parameters={
                    "assurance": "head_enforced",
                    "integration_ref": "refs/heads/main",
                    "merge_method": "squash",
                },
            )
        )
        server.join(timeout=5)

        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.receipt.observed_revision, MERGED)
        self.assertEqual(completed, [1])
        self.assertFalse(socket_path.exists())

    def test_bounded_runner_stops_oversized_output(self) -> None:
        with self.assertRaises(BoundedCommandError) as captured:
            BoundedCommandRunner().run(
                [sys.executable, "-c", "import sys; sys.stdout.write('x' * 10000)"],
                stdin=None,
                env={"PATH": os.defpath},
                timeout_seconds=5,
                stdout_limit_bytes=128,
                stderr_limit_bytes=128,
            )

        self.assertTrue(captured.exception.dispatched)

    def test_bounded_runner_validates_inputs_times_out_and_streams_stdin(self) -> None:
        runner = BoundedCommandRunner()
        base = {
            "stdin": None,
            "env": {"PATH": os.defpath},
            "timeout_seconds": 5,
            "stdout_limit_bytes": 128,
            "stderr_limit_bytes": 128,
        }
        for argv, changes in (
            ([], {}),
            ([sys.executable], {"timeout_seconds": 0}),
            ([sys.executable], {"stdout_limit_bytes": 0}),
            ([sys.executable], {"stdin": "not-bytes"}),
        ):
            with (
                self.subTest(argv=argv, changes=changes),
                self.assertRaises(BoundedCommandError) as captured,
            ):
                runner.run(argv, **{**base, **changes})  # type: ignore[arg-type]
            self.assertFalse(captured.exception.dispatched)

        with self.assertRaises(BoundedCommandError) as missing:
            runner.run([str(self.root / "missing-command")], **base)
        self.assertFalse(missing.exception.dispatched)

        with self.assertRaises(BoundedCommandError) as timeout:
            runner.run(
                [sys.executable, "-c", "import time; time.sleep(1)"],
                **{**base, "timeout_seconds": 0.01},
            )
        self.assertTrue(timeout.exception.dispatched)

        streamed = runner.run(
            [
                sys.executable,
                "-c",
                "import sys; d=sys.stdin.buffer.read(); "
                "sys.stdout.buffer.write(d); sys.stderr.write('bounded')",
            ],
            **{
                **base,
                "stdin": b"payload",
                "stdout_limit_bytes": 1024,
                "stderr_limit_bytes": 1024,
            },
        )
        self.assertEqual(streamed.returncode, 0)
        self.assertEqual(streamed.stdout, b"payload")
        self.assertEqual(streamed.stderr, b"bounded")

    def test_bounded_runner_passes_only_the_explicit_environment(self) -> None:
        with patch.dict(os.environ, {"MGB_PARENT_SECRET": "must-not-leak"}):
            result = BoundedCommandRunner().run(
                [
                    sys.executable,
                    "-c",
                    "import os,sys; "
                    "sys.stdout.write(os.environ.get('MGB_CHILD_VALUE', '')); "
                    "sys.stderr.write(os.environ.get('MGB_PARENT_SECRET', ''))",
                ],
                stdin=None,
                env={"MGB_CHILD_VALUE": "bounded", "PATH": os.defpath},
                timeout_seconds=5,
                stdout_limit_bytes=128,
                stderr_limit_bytes=128,
            )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"bounded")
        self.assertEqual(result.stderr, b"")

    def test_git_publisher_verifies_private_candidate_then_pushes_exact_sha(
        self,
    ) -> None:
        candidate_root = self.root / "candidates"
        repository = candidate_root / "projects" / "project" / "sha1" / "repository.git"
        repository.mkdir(parents=True)
        source_ref = f"refs/mgb/candidates/{MERGED}"
        target_ref = "refs/heads/mgb/candidate-42"
        runner = FakeRunner(
            [
                GitHubCommandResult(0, f"{MERGED}\n".encode(), b""),
                GitHubCommandResult(0, b"ok refs/heads/mgb/candidate-42\n", b""),
            ]
        )
        publisher = GitHubGitPublisher(
            gh_bin=self.gh_bin,
            git_bin=self.git_bin,
            config_dir=self.config_dir,
            candidate_root=candidate_root,
            runner=runner,
        )

        publisher.publish(
            source_repository=str(repository),
            source_ref=source_ref,
            desired_revision=MERGED,
            host="github.com",
            repository="acme/widgets",
            target_ref=target_ref,
            deadline="2099-01-01T00:00:00.000000+00:00",
        )

        expected_environment = {
            "GH_CONFIG_DIR": str(self.config_dir.resolve()),
            "GH_NO_EXTENSION_UPDATE_NOTIFIER": "1",
            "GH_NO_UPDATE_NOTIFIER": "1",
            "GH_PROMPT_DISABLED": "1",
            "GH_TELEMETRY": "false",
            "GCM_INTERACTIVE": "never",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            "NO_COLOR": "1",
            "PATH": os.environ.get("PATH", os.defpath),
        }
        self.assertEqual(
            runner.calls,
            [
                {
                    "argv": [
                        str(self.git_bin.resolve()),
                        f"--git-dir={repository.resolve()}",
                        "show-ref",
                        "--verify",
                        "--hash",
                        source_ref,
                    ],
                    "stdin": None,
                    "env": expected_environment,
                    "timeout_seconds": 120.0,
                    "stdout_limit_bytes": 256,
                    "stderr_limit_bytes": 64 * 1024,
                },
                {
                    "argv": [
                        str(self.git_bin.resolve()),
                        "-c",
                        "credential.helper=",
                        "-c",
                        "credential.helper="
                        f"!{shlex.quote(str(self.gh_bin.resolve()))} "
                        "auth git-credential",
                        "-c",
                        "core.askPass=",
                        f"--git-dir={repository.resolve()}",
                        "push",
                        "--porcelain",
                        "--no-verify",
                        "--recurse-submodules=no",
                        f"--force-with-lease={target_ref}:",
                        "https://github.com/acme/widgets.git",
                        f"{MERGED}:{target_ref}",
                    ],
                    "stdin": None,
                    "env": expected_environment,
                    "timeout_seconds": 120.0,
                    "stdout_limit_bytes": 64 * 1024,
                    "stderr_limit_bytes": 64 * 1024,
                },
            ],
        )

    def test_git_publisher_rejects_outside_or_mismatched_candidate_before_push(
        self,
    ) -> None:
        candidate_root = self.root / "candidates"
        candidate_root.mkdir()
        outside = self.root / "outside.git"
        outside.mkdir()
        publisher = GitHubGitPublisher(
            gh_bin=self.gh_bin,
            git_bin=self.git_bin,
            config_dir=self.config_dir,
            candidate_root=candidate_root,
            runner=FakeRunner([]),
        )
        with self.assertRaises(GitHubProviderError):
            publisher.publish(
                source_repository=str(outside),
                source_ref=f"refs/mgb/candidates/{MERGED}",
                desired_revision=MERGED,
                host="github.com",
                repository="acme/widgets",
                target_ref="refs/heads/mgb/candidate-42",
                deadline="2099-01-01T00:00:00.000000+00:00",
            )

        inside = candidate_root / "repository.git"
        inside.mkdir()
        runner = FakeRunner([GitHubCommandResult(0, f"{'f' * 40}\n".encode(), b"")])
        publisher = GitHubGitPublisher(
            gh_bin=self.gh_bin,
            git_bin=self.git_bin,
            config_dir=self.config_dir,
            candidate_root=candidate_root,
            runner=runner,
        )
        with self.assertRaises(GitHubProviderError):
            publisher.publish(
                source_repository=str(inside),
                source_ref=f"refs/mgb/candidates/{MERGED}",
                desired_revision=MERGED,
                host="github.com",
                repository="acme/widgets",
                target_ref="refs/heads/mgb/candidate-42",
                deadline="2099-01-01T00:00:00.000000+00:00",
            )
        self.assertEqual(len(runner.calls), 1)

    def test_git_push_failure_is_ambiguous_without_exposing_diagnostics(self) -> None:
        candidate_root = self.root / "candidates"
        repository = candidate_root / "repository.git"
        repository.mkdir(parents=True)
        runner = FakeRunner(
            [
                GitHubCommandResult(0, f"{MERGED}\n".encode(), b""),
                GitHubCommandResult(1, b"", b"credential-bearing failure"),
            ]
        )
        publisher = GitHubGitPublisher(
            gh_bin=self.gh_bin,
            git_bin=self.git_bin,
            config_dir=self.config_dir,
            candidate_root=candidate_root,
            runner=runner,
        )

        with self.assertRaises(GitHubTransportError) as captured:
            publisher.publish(
                source_repository=str(repository),
                source_ref=f"refs/mgb/candidates/{MERGED}",
                desired_revision=MERGED,
                host="github.com",
                repository="acme/widgets",
                target_ref="refs/heads/mgb/candidate-42",
                deadline="2099-01-01T00:00:00.000000+00:00",
            )

        self.assertTrue(captured.exception.ambiguous)
        self.assertNotIn("credential-bearing", str(captured.exception))

    def test_git_publisher_rejects_invalid_authority_and_runner_failures(self) -> None:
        candidate_root = self.root / "candidates"
        repository = candidate_root / "repository.git"
        repository.mkdir(parents=True)

        publisher = GitHubGitPublisher(
            gh_bin=self.gh_bin,
            git_bin=self.git_bin,
            config_dir=self.config_dir,
            candidate_root=candidate_root,
            runner=FakeRunner([]),
        )
        common = {
            "source_repository": str(repository),
            "source_ref": f"refs/mgb/candidates/{MERGED}",
            "desired_revision": MERGED,
            "host": "github.com",
            "repository": "acme/widgets",
            "target_ref": "refs/heads/mgb/candidate-42",
            "deadline": "2099-01-01T00:00:00.000000+00:00",
        }
        for changes in (
            {"desired_revision": "bad"},
            {"source_ref": "refs/heads/main"},
            {"source_repository": "relative.git"},
        ):
            with self.subTest(changes=changes), self.assertRaises(GitHubProviderError):
                publisher.publish(**{**common, **changes})

        verify_failure = GitHubGitPublisher(
            gh_bin=self.gh_bin,
            git_bin=self.git_bin,
            config_dir=self.config_dir,
            candidate_root=candidate_root,
            runner=FakeRunner([BoundedCommandError("private", dispatched=True)]),
        )
        with self.assertRaises(GitHubProviderError):
            verify_failure.publish(**common)

        push_failure = GitHubGitPublisher(
            gh_bin=self.gh_bin,
            git_bin=self.git_bin,
            config_dir=self.config_dir,
            candidate_root=candidate_root,
            runner=FakeRunner(
                [
                    GitHubCommandResult(0, f"{MERGED}\n".encode(), b""),
                    BoundedCommandError("private", dispatched=False),
                ]
            ),
        )
        with self.assertRaises(GitHubTransportError) as captured:
            push_failure.publish(**common)
        self.assertFalse(captured.exception.ambiguous)

    def test_path_and_socket_authority_validation_fails_closed(self) -> None:
        with self.assertRaisesRegex(GitHubProviderCLIError, "absolute"):
            GitHubCLI(gh_bin=self.gh_bin, config_dir=Path("relative"))
        config_link = self.root / "config-link"
        config_link.symlink_to(self.config_dir, target_is_directory=True)
        with self.assertRaisesRegex(GitHubProviderCLIError, "real directory"):
            GitHubCLI(gh_bin=self.gh_bin, config_dir=config_link)
        non_executable = self.root / "not-executable"
        non_executable.write_text("no", encoding="utf-8")
        with self.assertRaisesRegex(GitHubProviderCLIError, "executable"):
            GitHubCLI(gh_bin=non_executable, config_dir=self.config_dir)
        with self.assertRaisesRegex(GitHubProviderCLIError, "absolute"):
            GitHubGitPublisher(
                gh_bin=self.gh_bin,
                git_bin=self.git_bin,
                config_dir=self.config_dir,
                candidate_root=Path("relative"),
            )

        provider = GitHubProvider(
            sidecar_spec(self.root / "authorized.sock"), FakeAPI([])
        )
        with (
            patch(
                "magic_glass_box.github_provider_cli.serve_unix_socket",
                return_value=0,
            ) as carrier,
            self.assertRaisesRegex(GitHubProviderCLIError, "does not match"),
        ):
            serve_unix(self.root / "other.sock", 1, provider)
        carrier.assert_not_called()

    def test_request_frame_helpers_reject_truncation_and_oversized_response(
        self,
    ) -> None:
        spec = sidecar_spec(self.root / "github.sock")
        sender, receiver = socket.socketpair()
        self.addCleanup(sender.close)
        self.addCleanup(receiver.close)
        sender.sendall(struct.pack("!I", 0))
        sender.shutdown(socket.SHUT_WR)
        with self.assertRaisesRegex(GitHubProviderCLIError, "oversized"):
            _read_request(receiver, spec)

        operation = request(
            self.root / "github.sock",
            "landing.invoke",
            phase="reconcile",
            parameters={
                "assurance": "head_enforced",
                "integration_ref": "refs/heads/main",
                "merge_method": "squash",
            },
        )
        response = GitHubProvider(
            spec,
            FakeAPI(
                [
                    api_response(200, pull(state="closed", merged=True)),
                    api_response(200, ref(MERGED)),
                ]
            ),
        ).invoke(operation)
        with (
            patch("magic_glass_box.github_provider_cli.MAX_SERVICE_FRAME_OVERHEAD", 0),
            self.assertRaisesRegex(GitHubProviderCLIError, "oversized"),
        ):
            _write_response(
                sender, replace(operation, response_limit_bytes=1), response
            )

        request_sender, request_receiver = socket.socketpair()
        self.addCleanup(request_sender.close)
        self.addCleanup(request_receiver.close)
        request_raw = operation.canonical.encode("utf-8")
        request_sender.sendall(struct.pack("!I", len(request_raw)) + request_raw)
        request_sender.shutdown(socket.SHUT_WR)
        self.assertEqual(_read_request(request_receiver, spec), operation)

        response_sender, response_receiver = socket.socketpair()
        self.addCleanup(response_sender.close)
        self.addCleanup(response_receiver.close)
        _write_response(response_sender, operation, response)
        response_length = struct.unpack("!I", response_receiver.recv(4))[0]
        response_raw = b""
        while len(response_raw) < response_length:
            response_raw += response_receiver.recv(response_length - len(response_raw))
        self.assertEqual(
            json.loads(response_raw),
            response.to_dict(),
        )

    def test_main_emits_redacted_json_and_normalized_errors(self) -> None:
        identity = GitHubCredentialIdentity("github.com", "mgb-bot", "a" * 64)
        output = io.StringIO()
        with (
            patch("magic_glass_box.github_provider_cli.GitHubCLI") as client_type,
            redirect_stdout(output),
        ):
            client_type.return_value.credential_identity.return_value = identity
            code = main(
                [
                    "inspect",
                    "--gh-config-dir",
                    str(self.config_dir),
                    "--gh-bin",
                    str(self.gh_bin),
                ]
            )
        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(output.getvalue()),
            {"credential": identity.to_dict(), "status": "connected"},
        )

        errors = io.StringIO()
        with (
            patch(
                "magic_glass_box.github_provider_cli.GitHubCLI",
                side_effect=GitHubProviderCLIError("bounded failure"),
            ),
            redirect_stderr(errors),
        ):
            code = main(
                [
                    "inspect",
                    "--gh-config-dir",
                    str(self.config_dir),
                    "--gh-bin",
                    str(self.gh_bin),
                ]
            )
        self.assertEqual(code, 2)
        self.assertIn("bounded failure", errors.getvalue())

    def test_parser_preserves_exact_operator_arguments_and_defaults(self) -> None:
        parser = build_parser()
        self.assertEqual(
            vars(
                parser.parse_args(
                    [
                        "inspect",
                        "--gh-config-dir",
                        str(self.config_dir),
                    ]
                )
            ),
            {
                "command": "inspect",
                "gh_bin": None,
                "gh_config_dir": self.config_dir,
                "host": "github.com",
            },
        )
        socket_path = self.root / "github.sock"
        spec_path = self.root / "provider.json"
        candidate_root = self.root / "candidates"
        self.assertEqual(
            vars(
                parser.parse_args(
                    [
                        "serve",
                        "--gh-config-dir",
                        str(self.config_dir),
                        "--spec",
                        str(spec_path),
                        "--socket-path",
                        str(socket_path),
                        "--max-invocations",
                        "7",
                        "--gh-bin",
                        str(self.gh_bin),
                        "--git-bin",
                        str(self.git_bin),
                        "--candidate-root",
                        str(candidate_root),
                    ]
                )
            ),
            {
                "candidate_root": candidate_root,
                "command": "serve",
                "gh_bin": self.gh_bin,
                "gh_config_dir": self.config_dir,
                "git_bin": self.git_bin,
                "max_invocations": 7,
                "socket_path": socket_path,
                "spec": spec_path,
            },
        )

    def test_main_serve_wires_exact_authority_and_requires_candidate_root(self) -> None:
        socket_path = self.root / "github.sock"
        candidate_root = self.root / "candidates"
        candidate_root.mkdir()
        spec_value = sidecar_spec(socket_path).to_dict()
        spec_value["capabilities"].append(
            {"name": "refs.publish", "version": 1, "mode": "effect"}
        )
        spec = ServiceSidecarSpec.from_mapping(spec_value)
        spec_path = self.root / "provider.json"
        spec_path.write_text(canonical_json(spec.to_dict()), encoding="utf-8")
        output = io.StringIO()
        provider = object()
        publisher = object()
        with (
            patch(
                "magic_glass_box.github_provider_cli.load_spec", return_value=spec
            ) as load,
            patch("magic_glass_box.github_provider_cli.GitHubCLI") as client_type,
            patch(
                "magic_glass_box.github_provider_cli.validate_spec_credentials"
            ) as validate,
            patch(
                "magic_glass_box.github_provider_cli.GitHubGitPublisher",
                return_value=publisher,
            ) as publisher_type,
            patch(
                "magic_glass_box.github_provider_cli.GitHubProvider",
                return_value=provider,
            ) as provider_type,
            patch(
                "magic_glass_box.github_provider_cli.serve_unix", return_value=3
            ) as carrier,
            redirect_stdout(output),
        ):
            client_type.return_value.gh_bin = self.gh_bin
            client_type.return_value.config_dir = self.config_dir
            code = main(
                [
                    "serve",
                    "--gh-config-dir",
                    str(self.config_dir),
                    "--spec",
                    str(spec_path),
                    "--socket-path",
                    str(socket_path),
                    "--max-invocations",
                    "3",
                    "--gh-bin",
                    str(self.gh_bin),
                    "--git-bin",
                    str(self.git_bin),
                    "--candidate-root",
                    str(candidate_root),
                ]
            )
        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(output.getvalue()), {"invocations": 3, "status": "stopped"}
        )
        load.assert_called_once_with(spec_path)
        validate.assert_called_once_with(spec, client_type.return_value)
        publisher_type.assert_called_once_with(
            gh_bin=self.gh_bin,
            git_bin=self.git_bin,
            config_dir=self.config_dir,
            candidate_root=candidate_root,
        )
        provider_type.assert_called_once_with(
            spec, client_type.return_value, publisher=publisher
        )
        carrier.assert_called_once_with(socket_path, 3, provider)

        errors = io.StringIO()
        with (
            patch("magic_glass_box.github_provider_cli.load_spec", return_value=spec),
            patch("magic_glass_box.github_provider_cli.GitHubCLI") as client_type,
            patch("magic_glass_box.github_provider_cli.validate_spec_credentials"),
            redirect_stderr(errors),
        ):
            code = main(
                [
                    "serve",
                    "--gh-config-dir",
                    str(self.config_dir),
                    "--spec",
                    str(spec_path),
                    "--socket-path",
                    str(socket_path),
                    "--max-invocations",
                    "1",
                ]
            )
        self.assertEqual(code, 2)
        self.assertIn("candidate root is required", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
