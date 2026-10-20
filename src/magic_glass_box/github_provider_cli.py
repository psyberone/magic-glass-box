from __future__ import annotations

import argparse
import contextlib
import errno
import hashlib
import hmac
import json
import math
import os
import re
import selectors
import shlex
import shutil
import signal
import socket
import stat
import struct
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from .domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    parse_rfc3339_timestamp,
)
from .github_provider import (
    GitHubAPIResponse,
    GitHubProvider,
    GitHubProviderError,
    GitHubTransportError,
)
from .service_sidecar import (
    MAX_SERVICE_FRAME_OVERHEAD,
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceSidecarSpec,
)
from .sidecar import UnixSidecarError, serve_unix_socket

MAX_SPEC_BYTES = 1024 * 1024
MAX_HEADER_BYTES = 64 * 1024
MAX_STDERR_BYTES = 64 * 1024
MAX_TOKEN_BYTES = 64 * 1024
MAX_COMMAND_SECONDS = 120.0
INSPECTION_SECONDS = 30.0
GITHUB_API_VERSION = "2026-03-10"
SAFE_METHODS = frozenset({"GET", "HEAD"})
METHODS = frozenset({"DELETE", "GET", "HEAD", "PATCH", "POST", "PUT"})
STATUS_LINE_RE = re.compile(
    rb"^HTTP/(?:1\.[01]|2(?:\.0)?|3(?:\.0)?) ([1-5][0-9]{2})(?:[ \t].*)?$"
)
HEADER_NAME_RE = re.compile(rb"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


class GitHubProviderCLIError(ValueError):
    """The local GitHub provider process is misconfigured or received bad data."""


class BoundedCommandError(RuntimeError):
    """A subprocess failed without retaining or exposing its raw output."""

    def __init__(self, message: str, *, dispatched: bool) -> None:
        super().__init__(message)
        self.dispatched = dispatched


@dataclass(frozen=True)
class GitHubCommandResult:
    returncode: int
    stdout: bytes
    stderr: bytes


class CommandRunner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None,
        env: Mapping[str, str],
        timeout_seconds: float,
        stdout_limit_bytes: int,
        stderr_limit_bytes: int,
    ) -> GitHubCommandResult: ...


class BoundedCommandRunner:
    """Run one argv-only process with one deadline and bounded captured bytes."""

    @staticmethod
    def _validate(
        argv: Sequence[str],
        timeout_seconds: float,
        stdout_limit_bytes: int,
        stderr_limit_bytes: int,
    ) -> tuple[str, ...]:
        command = tuple(argv)
        if not command or any(
            not isinstance(item, str) or not item for item in command
        ):
            raise BoundedCommandError("GitHub CLI command is invalid", dispatched=False)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= MAX_COMMAND_SECONDS
        ):
            raise BoundedCommandError(
                "GitHub CLI deadline is invalid", dispatched=False
            )
        for limit in (stdout_limit_bytes, stderr_limit_bytes):
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
                raise BoundedCommandError(
                    "GitHub CLI output limit is invalid", dispatched=False
                )
        return command

    @staticmethod
    def _stop(process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            process.kill()
        with contextlib.suppress(OSError, subprocess.TimeoutExpired):
            process.wait(timeout=1)

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
        command = self._validate(
            argv, timeout_seconds, stdout_limit_bytes, stderr_limit_bytes
        )
        if stdin is not None and not isinstance(stdin, bytes):
            raise BoundedCommandError("GitHub CLI input is invalid", dispatched=False)
        try:
            process = subprocess.Popen(  # noqa: S603
                command,
                stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=dict(env),
                close_fds=True,
                start_new_session=True,
            )
        except OSError as exc:
            raise BoundedCommandError(
                "GitHub CLI could not start", dispatched=False
            ) from exc

        selector = selectors.DefaultSelector()
        stdout = bytearray()
        stderr = bytearray()
        input_offset = 0
        deadline = time.monotonic() + float(timeout_seconds)
        try:
            assert process.stdout is not None and process.stderr is not None
            os.set_blocking(process.stdout.fileno(), False)
            os.set_blocking(process.stderr.fileno(), False)
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            if process.stdin is not None:
                os.set_blocking(process.stdin.fileno(), False)
                selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")

            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BoundedCommandError(
                        "GitHub CLI deadline expired", dispatched=True
                    )
                events = selector.select(min(remaining, 0.25))
                for key, _mask in events:
                    stream = key.fileobj
                    if key.data == "stdin":
                        assert process.stdin is not None and stdin is not None
                        try:
                            written = os.write(
                                process.stdin.fileno(),
                                stdin[input_offset : input_offset + 65536],
                            )
                            input_offset += written
                        except OSError as exc:
                            if exc.errno not in {errno.EPIPE, errno.EBADF}:
                                raise
                            input_offset = len(stdin)
                        if input_offset >= len(stdin):
                            selector.unregister(stream)
                            process.stdin.close()
                        continue

                    target = stdout if key.data == "stdout" else stderr
                    limit = (
                        stdout_limit_bytes
                        if key.data == "stdout"
                        else stderr_limit_bytes
                    )
                    chunk = os.read(
                        stream.fileno(), min(65536, limit + 1 - len(target))
                    )
                    if not chunk:
                        selector.unregister(stream)
                        stream.close()
                        continue
                    target.extend(chunk)
                    if len(target) > limit:
                        raise BoundedCommandError(
                            "GitHub CLI output exceeds its limit", dispatched=True
                        )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BoundedCommandError(
                    "GitHub CLI deadline expired", dispatched=True
                )
            try:
                returncode = process.wait(timeout=remaining)
            except subprocess.TimeoutExpired as exc:
                raise BoundedCommandError(
                    "GitHub CLI deadline expired", dispatched=True
                ) from exc
            return GitHubCommandResult(returncode, bytes(stdout), bytes(stderr))
        except BoundedCommandError:
            self._stop(process)
            raise
        except OSError as exc:
            self._stop(process)
            raise BoundedCommandError(
                "GitHub CLI transport failed", dispatched=True
            ) from exc
        finally:
            selector.close()
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None and not stream.closed:
                    stream.close()


@dataclass(frozen=True)
class GitHubCredentialIdentity:
    host: str
    account: str
    credential_scope_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "account": self.account,
            "credential_scope_sha256": self.credential_scope_sha256,
            "host": self.host,
        }


def _strict_json_value(raw: bytes) -> object:
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise GitHubProviderCLIError("GitHub response JSON is invalid") from exc

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise GitHubProviderCLIError("GitHub response JSON is invalid")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise GitHubProviderCLIError("GitHub response JSON is invalid")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except GitHubProviderCLIError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise GitHubProviderCLIError("GitHub response JSON is invalid") from exc
    if value is not None and not isinstance(value, (dict, list)):
        raise GitHubProviderCLIError("GitHub response JSON is invalid")
    return value


def _header_end(raw: bytes, offset: int) -> tuple[int, int]:
    candidates = [
        (index, len(separator))
        for separator in (b"\r\n\r\n", b"\n\n")
        if (index := raw.find(separator, offset)) >= 0
    ]
    if not candidates:
        raise GitHubProviderCLIError("GitHub included response is invalid")
    return min(candidates)


def _parse_header_block(raw: bytes) -> tuple[int, dict[bytes, list[bytes]]]:
    if len(raw) > MAX_HEADER_BYTES:
        raise GitHubProviderCLIError("GitHub response headers exceed their limit")
    lines = raw.splitlines()
    if not lines:
        raise GitHubProviderCLIError("GitHub included response is invalid")
    match = STATUS_LINE_RE.fullmatch(lines[0])
    if match is None:
        raise GitHubProviderCLIError("GitHub included response is invalid")
    headers: dict[bytes, list[bytes]] = {}
    for line in lines[1:]:
        name, separator, value = line.partition(b":")
        if not separator or HEADER_NAME_RE.fullmatch(name) is None:
            raise GitHubProviderCLIError("GitHub included response is invalid")
        normalized = name.lower()
        headers.setdefault(normalized, []).append(value.strip())
    return int(match.group(1)), headers


def _parse_included_response(raw: bytes, *, body_limit_bytes: int) -> GitHubAPIResponse:
    if (
        isinstance(body_limit_bytes, bool)
        or not isinstance(body_limit_bytes, int)
        or body_limit_bytes < 1
    ):
        raise GitHubProviderCLIError("GitHub response limit is invalid")
    offset = 0
    total_headers = 0
    while True:
        end, separator_length = _header_end(raw, offset)
        block = raw[offset:end]
        total_headers += len(block) + separator_length
        if total_headers > MAX_HEADER_BYTES:
            raise GitHubProviderCLIError("GitHub response headers exceed their limit")
        status, headers = _parse_header_block(block)
        body_offset = end + separator_length
        if 100 <= status < 200 and raw[body_offset:].startswith(b"HTTP/"):
            offset = body_offset
            continue
        break

    body = raw[body_offset:]
    if len(body) > body_limit_bytes:
        raise GitHubProviderCLIError("GitHub response body exceeds its limit")
    retry_values = headers.get(b"retry-after", [])
    retry_after: int | None = None
    if retry_values:
        if len(retry_values) != 1 or not retry_values[0].isdigit():
            raise GitHubProviderCLIError("GitHub retry header is invalid")
        retry_after = int(retry_values[0])
        if not 1 <= retry_after <= 30 * 24 * 60 * 60:
            raise GitHubProviderCLIError("GitHub retry header is invalid")
    parsed = None if not body.strip() else _strict_json_value(body)
    return GitHubAPIResponse(
        status=status,
        body=parsed,
        response_bytes=len(body),
        retry_after_seconds=retry_after,
    )


def _validate_config_dir(path: Path) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise GitHubProviderCLIError("GH_CONFIG_DIR must be absolute")
    try:
        original = candidate.lstat()
        resolved = candidate.resolve(strict=True)
        info = resolved.stat()
    except OSError as exc:
        raise GitHubProviderCLIError("GH_CONFIG_DIR is unavailable") from exc
    if stat.S_ISLNK(original.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise GitHubProviderCLIError("GH_CONFIG_DIR must be a real directory")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise GitHubProviderCLIError("GH_CONFIG_DIR must be owned by this user")
    return resolved


def _validate_gh_bin(path: Path | None) -> Path:
    if path is None:
        discovered = shutil.which("gh")
        if discovered is None:
            raise GitHubProviderCLIError("GitHub CLI is unavailable")
        candidate = Path(discovered)
    else:
        candidate = Path(path)
        if not candidate.is_absolute():
            raise GitHubProviderCLIError("GitHub CLI path must be absolute")
    try:
        resolved = candidate.resolve(strict=True)
        info = resolved.stat()
    except OSError as exc:
        raise GitHubProviderCLIError("GitHub CLI is unavailable") from exc
    if not stat.S_ISREG(info.st_mode) or not os.access(resolved, os.X_OK):
        raise GitHubProviderCLIError("GitHub CLI must be an executable file")
    return resolved


def _validate_git_bin(path: Path | None) -> Path:
    if path is None:
        discovered = shutil.which("git")
        if discovered is None:
            raise GitHubProviderCLIError("Git CLI is unavailable")
        candidate = Path(discovered)
    else:
        candidate = Path(path)
        if not candidate.is_absolute():
            raise GitHubProviderCLIError("Git CLI path must be absolute")
    try:
        resolved = candidate.resolve(strict=True)
        info = resolved.stat()
    except OSError as exc:
        raise GitHubProviderCLIError("Git CLI is unavailable") from exc
    if not stat.S_ISREG(info.st_mode) or not os.access(resolved, os.X_OK):
        raise GitHubProviderCLIError("Git CLI must be an executable file")
    return resolved


def _validate_candidate_root(path: Path) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise GitHubProviderCLIError("candidate root must be absolute")
    try:
        original = candidate.lstat()
        resolved = candidate.resolve(strict=True)
        info = resolved.stat()
    except OSError as exc:
        raise GitHubProviderCLIError("candidate root is unavailable") from exc
    if stat.S_ISLNK(original.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise GitHubProviderCLIError("candidate root must be a real directory")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise GitHubProviderCLIError("candidate root must be owned by this user")
    return resolved


def _deadline_after(seconds: float) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat(
        timespec="microseconds"
    )


def _remaining_seconds(deadline: str) -> float:
    try:
        parsed = parse_rfc3339_timestamp(deadline, field="GitHub operation deadline")
    except ValidationError as exc:
        raise GitHubTransportError(
            "GitHub operation deadline is invalid", ambiguous=False
        ) from exc
    remaining = (parsed.astimezone(UTC) - datetime.now(UTC)).total_seconds()
    if remaining <= 0:
        raise GitHubTransportError(
            "GitHub operation deadline has expired", ambiguous=False
        )
    return min(remaining, MAX_COMMAND_SECONDS)


class GitHubCLI:
    """Credential-isolated, argv-only transport for the narrow GitHub provider."""

    def __init__(
        self,
        *,
        gh_bin: Path | None,
        config_dir: Path,
        runner: CommandRunner | None = None,
    ) -> None:
        self.gh_bin = _validate_gh_bin(gh_bin)
        self.config_dir = _validate_config_dir(config_dir)
        self.runner = runner or BoundedCommandRunner()
        self.environment = {
            "GH_CONFIG_DIR": str(self.config_dir),
            "GH_NO_EXTENSION_UPDATE_NOTIFIER": "1",
            "GH_NO_UPDATE_NOTIFIER": "1",
            "GH_PAGER": "cat",
            "GH_PROMPT_DISABLED": "1",
            "GH_TELEMETRY": "false",
            "NO_COLOR": "1",
            "PAGER": "cat",
            "PATH": os.environ.get("PATH", os.defpath),
        }

    def _run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None,
        timeout_seconds: float,
        stdout_limit_bytes: int,
    ) -> GitHubCommandResult:
        return self.runner.run(
            argv,
            stdin=stdin,
            env=self.environment,
            timeout_seconds=timeout_seconds,
            stdout_limit_bytes=stdout_limit_bytes,
            stderr_limit_bytes=MAX_STDERR_BYTES,
        )

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
        if method not in METHODS or not endpoint.startswith("repos/"):
            raise GitHubTransportError("GitHub API request is invalid", ambiguous=False)
        if any(
            ord(character) < 0x21 or ord(character) == 0x7F for character in endpoint
        ):
            raise GitHubTransportError(
                "GitHub API endpoint is invalid", ambiguous=False
            )
        if body is not None and not isinstance(body, dict):
            raise GitHubTransportError(
                "GitHub API request body is invalid", ambiguous=False
            )
        if (
            isinstance(response_limit_bytes, bool)
            or not isinstance(response_limit_bytes, int)
            or response_limit_bytes < 1
        ):
            raise GitHubTransportError(
                "GitHub API response limit is invalid", ambiguous=False
            )
        stdin = None if body is None else canonical_json(dict(body)).encode("utf-8")
        argv = [
            str(self.gh_bin),
            "api",
            "--include",
            "--hostname",
            host,
            "--method",
            method,
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            f"X-GitHub-Api-Version: {GITHUB_API_VERSION}",
        ]
        if stdin is not None:
            argv.extend(("--input", "-"))
        argv.append(endpoint)
        try:
            result = self._run(
                argv,
                stdin=stdin,
                timeout_seconds=_remaining_seconds(deadline),
                stdout_limit_bytes=response_limit_bytes + MAX_HEADER_BYTES,
            )
        except BoundedCommandError as exc:
            raise GitHubTransportError(
                "GitHub CLI transport failed",
                ambiguous=exc.dispatched and method not in SAFE_METHODS,
            ) from exc
        try:
            return _parse_included_response(
                result.stdout, body_limit_bytes=response_limit_bytes
            )
        except GitHubProviderCLIError as exc:
            raise GitHubTransportError(
                "GitHub CLI response is invalid", ambiguous=method not in SAFE_METHODS
            ) from exc

    def credential_identity(self, host: str) -> GitHubCredentialIdentity:
        return _credential_identity(self, host)


class GitHubGitPublisher:
    """Publish one exact trusted candidate commit through GitHub credentials."""

    def __init__(
        self,
        *,
        gh_bin: Path | None,
        git_bin: Path | None,
        config_dir: Path,
        candidate_root: Path,
        runner: CommandRunner | None = None,
    ) -> None:
        self.gh_bin = _validate_gh_bin(gh_bin)
        self.git_bin = _validate_git_bin(git_bin)
        self.config_dir = _validate_config_dir(config_dir)
        self.candidate_root = _validate_candidate_root(candidate_root)
        self.runner = runner or BoundedCommandRunner()
        self.environment = {
            "GH_CONFIG_DIR": str(self.config_dir),
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

    def _source_repository(self, value: str) -> Path:
        candidate = Path(value)
        if not candidate.is_absolute():
            raise GitHubProviderError("candidate repository must be absolute")
        try:
            original = candidate.lstat()
            resolved = candidate.resolve(strict=True)
            info = resolved.stat()
            resolved.relative_to(self.candidate_root)
        except (OSError, ValueError) as exc:
            raise GitHubProviderError("candidate repository is unavailable") from exc
        if stat.S_ISLNK(original.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise GitHubProviderError("candidate repository is unavailable")
        return resolved

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
        if re.fullmatch(r"[0-9a-f]{40}", desired_revision) is None:
            raise GitHubProviderError("candidate revision is invalid")
        if source_ref != f"refs/mgb/candidates/{desired_revision}":
            raise GitHubProviderError("candidate source ref is invalid")
        source = self._source_repository(source_repository)
        verify = [
            str(self.git_bin),
            f"--git-dir={source}",
            "show-ref",
            "--verify",
            "--hash",
            source_ref,
        ]
        try:
            verified = self.runner.run(
                verify,
                stdin=None,
                env=self.environment,
                timeout_seconds=_remaining_seconds(deadline),
                stdout_limit_bytes=256,
                stderr_limit_bytes=MAX_STDERR_BYTES,
            )
        except BoundedCommandError as exc:
            raise GitHubProviderError("candidate revision cannot be verified") from exc
        if (
            verified.returncode != 0
            or verified.stdout.strip() != desired_revision.encode("ascii")
        ):
            raise GitHubProviderError("candidate revision cannot be verified")

        helper = f"!{shlex.quote(str(self.gh_bin))} auth git-credential"
        push = [
            str(self.git_bin),
            "-c",
            "credential.helper=",
            "-c",
            f"credential.helper={helper}",
            "-c",
            "core.askPass=",
            f"--git-dir={source}",
            "push",
            "--porcelain",
            "--no-verify",
            "--recurse-submodules=no",
            f"--force-with-lease={target_ref}:",
            f"https://{host}/{repository}.git",
            f"{desired_revision}:{target_ref}",
        ]
        try:
            result = self.runner.run(
                push,
                stdin=None,
                env=self.environment,
                timeout_seconds=_remaining_seconds(deadline),
                stdout_limit_bytes=MAX_STDERR_BYTES,
                stderr_limit_bytes=MAX_STDERR_BYTES,
            )
        except BoundedCommandError as exc:
            raise GitHubTransportError(
                "Git publication failed", ambiguous=exc.dispatched
            ) from exc
        if result.returncode != 0:
            raise GitHubTransportError("Git publication failed", ambiguous=True)


def _account_response(client: GitHubCLI, host: str) -> GitHubAPIResponse:
    argv = [
        str(client.gh_bin),
        "api",
        "--include",
        "--hostname",
        host,
        "--method",
        "GET",
        "-H",
        "Accept: application/vnd.github+json",
        "-H",
        f"X-GitHub-Api-Version: {GITHUB_API_VERSION}",
        "user",
    ]
    try:
        result = client._run(
            argv,
            stdin=None,
            timeout_seconds=INSPECTION_SECONDS,
            stdout_limit_bytes=64 * 1024 + MAX_HEADER_BYTES,
        )
        return _parse_included_response(result.stdout, body_limit_bytes=64 * 1024)
    except (BoundedCommandError, GitHubProviderCLIError) as exc:
        raise GitHubProviderCLIError("GitHub account inspection failed") from exc


def _credential_identity(client: GitHubCLI, host: str) -> GitHubCredentialIdentity:
    account_response = _account_response(client, host)
    if account_response.status != 200 or not isinstance(account_response.body, dict):
        raise GitHubProviderCLIError("GitHub account inspection failed")
    account = account_response.body.get("login")
    if (
        not isinstance(account, str)
        or not account
        or len(account.encode("utf-8")) > 256
    ):
        raise GitHubProviderCLIError("GitHub account inspection failed")
    try:
        token_result = client._run(
            [str(client.gh_bin), "auth", "token", "--hostname", host],
            stdin=None,
            timeout_seconds=INSPECTION_SECONDS,
            stdout_limit_bytes=MAX_TOKEN_BYTES,
        )
    except BoundedCommandError as exc:
        raise GitHubProviderCLIError("GitHub credential inspection failed") from exc
    token = token_result.stdout.strip()
    if (
        token_result.returncode != 0
        or not token
        or b"\n" in token
        or b"\r" in token
        or len(token) > MAX_TOKEN_BYTES
    ):
        raise GitHubProviderCLIError("GitHub credential inspection failed")
    token_sha256 = hashlib.sha256(token).hexdigest()
    scope = content_sha256(
        canonical_json(
            {
                "account": account,
                "host": host,
                "token_sha256": token_sha256,
            }
        )
    )
    return GitHubCredentialIdentity(host, account, scope)


def validate_spec_credentials(
    spec: ServiceSidecarSpec, client: GitHubCLI
) -> tuple[GitHubCredentialIdentity, ...]:
    identities = {
        host: client.credential_identity(host)
        for host in sorted({mapping.host for mapping in spec.mappings})
    }
    for mapping in spec.mappings:
        identity = identities[mapping.host]
        if identity.account.casefold() != mapping.account.casefold():
            raise GitHubProviderCLIError(
                "GitHub mapping account does not match the selected credential"
            )
        if not hmac.compare_digest(
            identity.credential_scope_sha256, mapping.credential_scope_sha256
        ):
            raise GitHubProviderCLIError(
                "GitHub mapping credential scope does not match"
            )
    return tuple(identities[host] for host in sorted(identities))


def _strict_json_object(raw: bytes, *, subject: str) -> dict[str, Any]:
    value = _strict_json_value(raw)
    if not isinstance(value, dict):
        raise GitHubProviderCLIError(f"{subject} is invalid")
    return value


def load_spec(path: Path) -> ServiceSidecarSpec:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise GitHubProviderCLIError("GitHub provider spec path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise GitHubProviderCLIError("GitHub provider spec is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise GitHubProviderCLIError("GitHub provider spec must be a regular file")
        raw = os.read(descriptor, MAX_SPEC_BYTES + 1)
    finally:
        os.close(descriptor)
    if not raw or len(raw) > MAX_SPEC_BYTES:
        raise GitHubProviderCLIError("GitHub provider spec is empty or oversized")
    try:
        return ServiceSidecarSpec.from_mapping(
            _strict_json_object(raw, subject="GitHub provider spec")
        )
    except (ValidationError, GitHubProviderCLIError) as exc:
        raise GitHubProviderCLIError("GitHub provider spec is invalid") from exc


def _recv_exact(connection: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise GitHubProviderCLIError("GitHub provider request ended early")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_request(
    connection: socket.socket, spec: ServiceSidecarSpec
) -> ServiceOperationRequest:
    length = struct.unpack("!I", _recv_exact(connection, 4))[0]
    maximum = spec.request_bytes + MAX_SERVICE_FRAME_OVERHEAD
    if not 1 <= length <= maximum:
        raise GitHubProviderCLIError("GitHub provider request frame is oversized")
    raw = _recv_exact(connection, length)
    try:
        return ServiceOperationRequest.from_mapping(
            _strict_json_object(raw, subject="GitHub provider request")
        )
    except (ValidationError, GitHubProviderCLIError) as exc:
        raise GitHubProviderCLIError("GitHub provider request is invalid") from exc


def _write_response(
    connection: socket.socket,
    request: ServiceOperationRequest,
    response: ServiceOperationResponse,
) -> None:
    raw = canonical_json(response.to_dict()).encode("utf-8")
    if len(raw) > request.response_limit_bytes + MAX_SERVICE_FRAME_OVERHEAD:
        raise GitHubProviderCLIError("GitHub provider response frame is oversized")
    connection.sendall(struct.pack("!I", len(raw)) + raw)


def serve_unix(
    socket_path: Path,
    max_invocations: int,
    provider: GitHubProvider,
) -> int:
    if Path(socket_path) != Path(provider.spec.transport.socket_path):
        raise GitHubProviderCLIError(
            "GitHub provider socket does not match its authority"
        )

    def handle(connection: socket.socket) -> None:
        request = _read_request(connection, provider.spec)
        response = provider.invoke(request)
        _write_response(connection, request, response)

    try:
        return serve_unix_socket(
            socket_path,
            max_invocations,
            handle,
            recoverable_errors=(
                GitHubProviderCLIError,
                GitHubProviderError,
                ValidationError,
                OSError,
            ),
            client_timeout=min(300.0, provider.spec.timeout_ms / 1000 + 1.0),
        )
    except UnixSidecarError as exc:
        raise GitHubProviderCLIError(str(exc)) from exc


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="magic-glass-box-github-provider",
        description="Run the credential-isolated GitHub reference provider.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="Show non-secret credential identity")
    inspect.add_argument("--gh-config-dir", required=True, type=Path)
    inspect.add_argument("--host", default="github.com")
    inspect.add_argument("--gh-bin", type=Path)

    serve = commands.add_parser("serve", help="Serve the bounded provider carrier")
    serve.add_argument("--gh-config-dir", required=True, type=Path)
    serve.add_argument("--spec", required=True, type=Path)
    serve.add_argument("--socket-path", required=True, type=Path)
    serve.add_argument("--max-invocations", required=True, type=_positive_integer)
    serve.add_argument("--gh-bin", type=Path)
    serve.add_argument("--git-bin", type=Path)
    serve.add_argument("--candidate-root", type=Path)
    return parser


def _serve(args: argparse.Namespace) -> dict[str, object]:
    spec = load_spec(args.spec)
    client = GitHubCLI(
        gh_bin=args.gh_bin,
        config_dir=args.gh_config_dir,
    )
    validate_spec_credentials(spec, client)
    supports_publication = any(
        (capability.name, capability.version) == ("refs.publish", 1)
        for capability in spec.capabilities
    )
    if supports_publication and args.candidate_root is None:
        raise GitHubProviderCLIError("candidate root is required for refs.publish")
    publisher = (
        GitHubGitPublisher(
            gh_bin=client.gh_bin,
            git_bin=args.git_bin,
            config_dir=client.config_dir,
            candidate_root=args.candidate_root,
        )
        if supports_publication
        else None
    )
    completed = serve_unix(
        args.socket_path,
        args.max_invocations,
        GitHubProvider(spec, client, publisher=publisher),
    )
    return {"invocations": completed, "status": "stopped"}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "inspect":
            client = GitHubCLI(
                gh_bin=args.gh_bin,
                config_dir=args.gh_config_dir,
            )
            result: object = {
                "credential": client.credential_identity(args.host).to_dict(),
                "status": "connected",
            }
        else:
            result = _serve(args)
    except (GitHubProviderCLIError, GitHubProviderError, GitHubTransportError) as exc:
        print(f"magic-glass-box-github-provider: {exc}", file=sys.stderr)
        return 2
    except OSError:
        print("magic-glass-box-github-provider: operation failed", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
