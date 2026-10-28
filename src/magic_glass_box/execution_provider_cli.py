from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import hmac
import io
import json
import math
import os
import re
import selectors
import signal
import socket
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from .domain import (
    ValidationError,
    canonical_json,
    content_sha256,
    parse_rfc3339_timestamp,
    require_string,
)
from .execution import (
    ExecutionEvidenceResult,
    ExecutionJobSpec,
    ExecutionStatusResult,
)
from .execution_provider import (
    ControlledExecutionProvider,
    ExecutionBackendAmbiguous,
    ExecutionBackendUnavailable,
    ExecutionProviderError,
)
from .service_sidecar import (
    MAX_SERVICE_FRAME_OVERHEAD,
    ServiceOperationRequest,
    ServiceOperationResponse,
    ServiceSidecarSpec,
)
from .sidecar import UnixSidecarError, serve_unix_socket
from .workspace import GitSnapshotter, SnapshotResult, WorkspaceError

MAX_CONFIG_BYTES = 128 * 1024
MAX_SPEC_BYTES = 1024 * 1024
MAX_CANDIDATE_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_REMOTE_REQUEST_BYTES = 96 * 1024 * 1024
MAX_REMOTE_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_COMMAND_STDOUT_BYTES = MAX_CANDIDATE_ARCHIVE_BYTES
MAX_REMOTE_STDERR_BYTES = 64 * 1024
MAX_SSH_SECONDS = 120.0
PROCESS_STOP_SECONDS = 0.5
HOST_RE = re.compile(
    r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)
ACCOUNT_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,63}$")
REMOTE_HELPER_RE = re.compile(r"^/[A-Za-z0-9._/-]{1,511}$")


class ExecutionProviderCLIError(ValueError):
    """The local controlled-execution carrier is unsafe or misconfigured."""


@dataclass(frozen=True)
class SSHCredentialIdentity:
    host: str
    account: str
    credential_scope_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "account": self.account,
            "credential_scope_sha256": self.credential_scope_sha256,
            "host": self.host,
        }


@dataclass(frozen=True)
class SSHRunnerConfig:
    ssh_binary: Path
    git_binary: Path
    host: str
    port: int
    account: str
    identity_file: Path = field(repr=False)
    known_hosts_file: Path = field(repr=False)
    remote_helper: str
    credential_scope_sha256: str
    repositories: Mapping[str, Path] = field(repr=False)

    @property
    def identity(self) -> SSHCredentialIdentity:
        return SSHCredentialIdentity(
            host=self.host,
            account=self.account,
            credential_scope_sha256=self.credential_scope_sha256,
        )


@dataclass(frozen=True)
class BoundedCommandResult:
    returncode: int
    stdout: bytes
    stderr: bytes


class CommandRunner(Protocol):
    def run(
        self,
        command: list[str],
        *,
        input_bytes: bytes,
        timeout: float,
        stdout_limit: int,
        environment: dict[str, str],
    ) -> BoundedCommandResult: ...


class CandidateArchiveSource(Protocol):
    def archive(self, job: ExecutionJobSpec) -> bytes: ...


class CandidateSnapshotter(Protocol):
    def snapshot(
        self, repository: Path, revision: str | None, destination: Path
    ) -> SnapshotResult: ...


def _strict_json_object(raw: bytes, *, subject: str) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise ExecutionProviderCLIError(f"{subject} is invalid") from exc

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ExecutionProviderCLIError(f"{subject} is invalid")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ExecutionProviderCLIError(f"{subject} is invalid")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique,
            parse_constant=reject_constant,
        )
    except ExecutionProviderCLIError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ExecutionProviderCLIError(f"{subject} is invalid") from exc
    if not isinstance(value, dict):
        raise ExecutionProviderCLIError(f"{subject} is invalid")
    return value


def _read_file(
    path: Path,
    *,
    maximum: int,
    subject: str,
    private: bool,
) -> bytes:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise ExecutionProviderCLIError(f"{subject} path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise ExecutionProviderCLIError(f"{subject} is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ExecutionProviderCLIError(f"{subject} must be a regular file")
        if private and info.st_mode & 0o077:
            raise ExecutionProviderCLIError(
                f"{subject} must not be group/world accessible"
            )
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise ExecutionProviderCLIError(f"{subject} must be owned by this user")
        raw = os.read(descriptor, maximum + 1)
    finally:
        os.close(descriptor)
    if not raw or len(raw) > maximum:
        raise ExecutionProviderCLIError(f"{subject} is empty or oversized")
    return raw


def _trusted_executable(path: object, *, field_name: str) -> Path:
    try:
        text = require_string(path, field=field_name, max_bytes=1024)
    except ValidationError as exc:
        raise ExecutionProviderCLIError("runner configuration is invalid") from exc
    candidate = Path(text)
    if not candidate.is_absolute():
        raise ExecutionProviderCLIError(f"{field_name} path must be absolute")
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise ExecutionProviderCLIError(f"{field_name} is unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or not info.st_mode & 0o100
        or info.st_mode & 0o022
    ):
        raise ExecutionProviderCLIError(f"{field_name} is unsafe")
    if hasattr(os, "getuid") and info.st_uid not in {0, os.getuid()}:
        raise ExecutionProviderCLIError(f"{field_name} is unsafe")
    return candidate


def _repository_directory(value: object) -> Path:
    try:
        text = require_string(value, field="candidate repository", max_bytes=4096)
    except ValidationError as exc:
        raise ExecutionProviderCLIError("runner configuration is invalid") from exc
    path = Path(text)
    if not path.is_absolute():
        raise ExecutionProviderCLIError("candidate repository path must be absolute")
    try:
        info = path.lstat()
    except OSError as exc:
        raise ExecutionProviderCLIError("candidate repository is unavailable") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ExecutionProviderCLIError("candidate repository is unsafe")
    return path


def _private_path(value: object, *, field_name: str) -> Path:
    try:
        text = require_string(value, field=field_name, max_bytes=4096)
    except ValidationError as exc:
        raise ExecutionProviderCLIError("runner configuration is invalid") from exc
    path = Path(text)
    _read_file(path, maximum=4 * 1024 * 1024, subject=field_name, private=True)
    return path


def load_config(path: Path) -> SSHRunnerConfig:
    raw = _read_file(
        path,
        maximum=MAX_CONFIG_BYTES,
        subject="runner configuration",
        private=True,
    )
    document = _strict_json_object(raw, subject="runner configuration")
    fields = {
        "schema_version",
        "ssh_binary",
        "git_binary",
        "host",
        "port",
        "account",
        "identity_file",
        "known_hosts_file",
        "remote_helper",
        "repositories",
    }
    if set(document) != fields:
        raise ExecutionProviderCLIError("runner configuration is invalid")
    if isinstance(document["schema_version"], bool) or document["schema_version"] != 1:
        raise ExecutionProviderCLIError("runner configuration is invalid")
    host = document["host"]
    account = document["account"]
    port = document["port"]
    remote_helper = document["remote_helper"]
    if (
        not isinstance(host, str)
        or HOST_RE.fullmatch(host) is None
        or host.endswith((".", "-"))
        or ".." in host
        or not isinstance(account, str)
        or ACCOUNT_RE.fullmatch(account) is None
        or isinstance(port, bool)
        or not isinstance(port, int)
        or not 1 <= port <= 65535
        or not isinstance(remote_helper, str)
        or REMOTE_HELPER_RE.fullmatch(remote_helper) is None
        or any(
            part in {"", ".", ".."} for part in PurePosixPath(remote_helper).parts[1:]
        )
    ):
        raise ExecutionProviderCLIError("runner configuration is invalid")
    raw_repositories = document["repositories"]
    if not isinstance(raw_repositories, dict) or not 1 <= len(raw_repositories) <= 64:
        raise ExecutionProviderCLIError("runner configuration is invalid")
    repositories: dict[str, Path] = {}
    for name, raw_path in raw_repositories.items():
        if (
            not isinstance(name, str)
            or not name
            or len(name.encode("utf-8")) > 512
            or any(mark in name for mark in ":@?#")
        ):
            raise ExecutionProviderCLIError("candidate repository mapping is invalid")
        repositories[name] = _repository_directory(raw_path)
    identity_file = _private_path(
        document["identity_file"], field_name="SSH identity file"
    )
    known_hosts_file = _private_path(
        document["known_hosts_file"], field_name="SSH known-hosts file"
    )
    identity_raw = _read_file(
        identity_file,
        maximum=4 * 1024 * 1024,
        subject="SSH identity file",
        private=True,
    )
    known_hosts_raw = _read_file(
        known_hosts_file,
        maximum=4 * 1024 * 1024,
        subject="SSH known-hosts file",
        private=True,
    )
    scope = content_sha256(
        canonical_json(
            {
                "account": account,
                "host": host,
                "identity_sha256": hashlib.sha256(identity_raw).hexdigest(),
                "known_hosts_sha256": hashlib.sha256(known_hosts_raw).hexdigest(),
                "port": port,
                "remote_helper": remote_helper,
                "candidate_sources_sha256": content_sha256(
                    canonical_json(
                        {
                            name: str(repository)
                            for name, repository in sorted(repositories.items())
                        }
                    )
                ),
            }
        )
    )
    return SSHRunnerConfig(
        ssh_binary=_trusted_executable(document["ssh_binary"], field_name="SSH binary"),
        git_binary=_trusted_executable(document["git_binary"], field_name="Git binary"),
        host=host,
        port=port,
        account=account,
        identity_file=identity_file,
        known_hosts_file=known_hosts_file,
        remote_helper=remote_helper,
        credential_scope_sha256=scope,
        repositories=repositories,
    )


def validate_spec_config(
    spec: ServiceSidecarSpec, config: SSHRunnerConfig
) -> SSHCredentialIdentity:
    if not isinstance(spec, ServiceSidecarSpec) or not isinstance(
        config, SSHRunnerConfig
    ):
        raise ExecutionProviderCLIError("runner credential authority is invalid")
    identity = config.identity
    for mapping in spec.mappings:
        if mapping.service != "ssh-runner" or mapping.host != identity.host:
            raise ExecutionProviderCLIError(
                "runner mapping host does not match the selected credential"
            )
        if mapping.account != identity.account:
            raise ExecutionProviderCLIError(
                "runner mapping account does not match the selected credential"
            )
        if not hmac.compare_digest(
            mapping.credential_scope_sha256, identity.credential_scope_sha256
        ):
            raise ExecutionProviderCLIError(
                "runner mapping credential scope does not match"
            )
        if mapping.repository not in config.repositories:
            raise ExecutionProviderCLIError(
                "runner mapping has no candidate repository source"
            )
    return identity


class BoundedCommandRunner:
    @staticmethod
    def _stop(process: subprocess.Popen[bytes]) -> None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except OSError:
            with suppress(OSError):
                process.terminate()
        try:
            process.wait(timeout=PROCESS_STOP_SECONDS)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except OSError:
                with suppress(OSError):
                    process.kill()
            with suppress(subprocess.TimeoutExpired):
                process.wait(timeout=PROCESS_STOP_SECONDS)

    def run(
        self,
        command: list[str],
        *,
        input_bytes: bytes,
        timeout: float,
        stdout_limit: int,
        environment: dict[str, str],
    ) -> BoundedCommandResult:
        if (
            not command
            or not Path(command[0]).is_absolute()
            or len(input_bytes) > MAX_REMOTE_REQUEST_BYTES
            or isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
            or not 1 <= stdout_limit <= MAX_COMMAND_STDOUT_BYTES
        ):
            raise ExecutionProviderCLIError("SSH command request is invalid")
        process = subprocess.Popen(  # noqa: S603 - absolute configured SSH binary.
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            start_new_session=True,
        )
        assert process.stdin is not None
        assert process.stdout is not None
        assert process.stderr is not None
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        offset = 0
        deadline = time.monotonic() + float(timeout)
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            try:
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(command, timeout)
                    events = selector.select(remaining)
                    if not events:
                        raise subprocess.TimeoutExpired(command, timeout)
                    for key, _ in events:
                        name = key.data
                        if name == "stdin":
                            try:
                                written = os.write(
                                    key.fd, input_bytes[offset : offset + 64 * 1024]
                                )
                            except BrokenPipeError:
                                written = 0
                                offset = len(input_bytes)
                            else:
                                offset += written
                            if offset >= len(input_bytes) or written == 0:
                                selector.unregister(key.fileobj)
                                process.stdin.close()
                            continue
                        chunk = os.read(key.fd, 64 * 1024)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                            continue
                        ceiling = (
                            stdout_limit
                            if name == "stdout"
                            else MAX_REMOTE_STDERR_BYTES
                        )
                        if len(buffers[name]) + len(chunk) > ceiling:
                            raise ExecutionProviderCLIError(
                                "SSH command output exceeds its limit"
                            )
                        buffers[name].extend(chunk)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout)
                returncode = process.wait(timeout=remaining)
            except BaseException:
                self._stop(process)
                raise
            finally:
                for stream in (process.stdin, process.stdout, process.stderr):
                    if not stream.closed:
                        stream.close()
        return BoundedCommandResult(
            returncode,
            bytes(buffers["stdout"]),
            bytes(buffers["stderr"]),
        )


class _BoundedArchiveBuffer(io.BytesIO):
    def write(self, value: bytes) -> int:
        if self.tell() + len(value) > MAX_CANDIDATE_ARCHIVE_BYTES:
            raise ExecutionProviderCLIError("candidate archive is oversized")
        return super().write(value)


class GitCandidateArchiveSource:
    """Package raw bytes from one exact trusted candidate commit."""

    def __init__(
        self,
        config: SSHRunnerConfig,
        *,
        snapshotter: CandidateSnapshotter | None = None,
    ) -> None:
        self.config = config
        self.snapshotter = snapshotter or GitSnapshotter(
            binary=config.git_binary, timeout=120.0
        )

    @staticmethod
    def _package(directory: Path) -> bytes:
        records: list[tuple[str, Path, os.stat_result]] = []
        pending = [directory]
        while pending:
            current = pending.pop()
            try:
                entries = sorted(os.scandir(current), key=lambda entry: entry.name)
            except OSError as exc:
                raise ExecutionProviderCLIError(
                    "candidate snapshot cannot be packaged"
                ) from exc
            for entry in entries:
                path = Path(entry.path)
                try:
                    info = entry.stat(follow_symlinks=False)
                    relative = path.relative_to(directory).as_posix()
                except (OSError, ValueError) as exc:
                    raise ExecutionProviderCLIError(
                        "candidate snapshot cannot be packaged"
                    ) from exc
                records.append((relative, path, info))
                if stat.S_ISDIR(info.st_mode):
                    pending.append(path)

        output = _BoundedArchiveBuffer()
        try:
            with (
                gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed,
                tarfile.open(
                    fileobj=compressed,
                    mode="w|",
                    format=tarfile.PAX_FORMAT,
                ) as archive,
            ):
                for relative, path, info in sorted(records):
                    member = tarfile.TarInfo(relative)
                    member.uid = 0
                    member.gid = 0
                    member.uname = ""
                    member.gname = ""
                    member.mtime = 0
                    if stat.S_ISDIR(info.st_mode):
                        member.type = tarfile.DIRTYPE
                        member.mode = 0o755
                        archive.addfile(member)
                    elif stat.S_ISREG(info.st_mode):
                        member.size = info.st_size
                        member.mode = 0o755 if info.st_mode & 0o111 else 0o644
                        with path.open("rb") as source:
                            archive.addfile(member, source)
                    elif stat.S_ISLNK(info.st_mode):
                        member.type = tarfile.SYMTYPE
                        member.mode = 0o777
                        member.linkname = os.readlink(path)
                        archive.addfile(member)
                    else:
                        raise ExecutionProviderCLIError(
                            "candidate snapshot contains an unsupported file"
                        )
        except ExecutionProviderCLIError:
            raise
        except (OSError, tarfile.TarError) as exc:
            raise ExecutionProviderCLIError(
                "candidate snapshot cannot be packaged"
            ) from exc
        return output.getvalue()

    def archive(self, job: ExecutionJobSpec) -> bytes:
        repository = self.config.repositories.get(job.repository)
        if repository is None:
            raise ExecutionProviderCLIError("candidate repository is unavailable")
        try:
            with tempfile.TemporaryDirectory(
                prefix="mgb-execution-source-"
            ) as temporary:
                destination = Path(temporary) / "workspace"
                destination.mkdir(mode=0o700)
                snapshot = self.snapshotter.snapshot(
                    repository, job.candidate_revision, destination
                )
                if snapshot.revision != job.candidate_revision:
                    raise ExecutionProviderCLIError(
                        "candidate snapshot identity changed"
                    )
                archive = self._package(destination)
        except ExecutionProviderCLIError:
            raise
        except (
            OSError,
            TimeoutError,
            subprocess.TimeoutExpired,
            WorkspaceError,
        ) as exc:
            raise ExecutionProviderCLIError("candidate archive export failed") from exc
        if not archive or len(archive) > MAX_CANDIDATE_ARCHIVE_BYTES:
            raise ExecutionProviderCLIError("candidate archive is empty or oversized")
        return archive


class SSHExecutionBackend:
    def __init__(
        self,
        config: SSHRunnerConfig,
        *,
        runner: CommandRunner | None = None,
        candidate_source: CandidateArchiveSource | None = None,
    ) -> None:
        if not isinstance(config, SSHRunnerConfig):
            raise ExecutionProviderCLIError("runner configuration is invalid")
        self.config = config
        self.runner = runner or BoundedCommandRunner()
        self.candidate_source = candidate_source or GitCandidateArchiveSource(config)

    def _command(self) -> list[str]:
        config = self.config
        return [
            str(config.ssh_binary),
            "-F",
            os.devnull,
            "-o",
            "BatchMode=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "PasswordAuthentication=no",
            "-o",
            "KbdInteractiveAuthentication=no",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "ForwardAgent=no",
            "-o",
            "RequestTTY=no",
            "-o",
            "PermitLocalCommand=no",
            "-o",
            "LogLevel=ERROR",
            "-o",
            "ConnectionAttempts=1",
            "-o",
            "ConnectTimeout=30",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=2",
            "-o",
            f"UserKnownHostsFile={config.known_hosts_file}",
            "-o",
            f"GlobalKnownHostsFile={os.devnull}",
            "-i",
            str(config.identity_file),
            "-p",
            str(config.port),
            f"{config.account}@{config.host}",
            config.remote_helper,
            "invoke",
        ]

    @staticmethod
    def _remaining(deadline: str) -> float:
        try:
            parsed = parse_rfc3339_timestamp(
                deadline, field="execution operation deadline"
            )
        except ValidationError as exc:
            raise ExecutionBackendUnavailable(
                "execution operation deadline is invalid"
            ) from exc
        remaining = (parsed.astimezone(UTC) - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise ExecutionBackendUnavailable(
                "execution operation deadline has expired"
            )
        return min(remaining, MAX_SSH_SECONDS)

    def _invoke(
        self,
        action: str,
        job: ExecutionJobSpec,
        execution_id: str | None,
        *,
        deadline: str,
        effect: bool,
        evidence: bool = False,
    ) -> ExecutionStatusResult | ExecutionEvidenceResult | None:
        try:
            archive = self.candidate_source.archive(job)
        except (ExecutionProviderCLIError, OSError, TimeoutError) as exc:
            raise ExecutionBackendUnavailable(
                "candidate source is unavailable before dispatch"
            ) from exc
        archive_sha256 = hashlib.sha256(archive).hexdigest()
        candidate_archive: dict[str, object] | None = None
        if action in {"submit", "reconcile_submit"}:
            candidate_archive = {
                "bundle": base64.b64encode(archive).decode("ascii"),
                "bytes": len(archive),
                "encoding": "tar+gzip+base64",
                "sha256": archive_sha256,
            }
        payload = canonical_json(
            {
                "action": action,
                "candidate_archive": candidate_archive,
                "deadline": deadline,
                "execution_id": execution_id,
                "job": job.to_dict(),
                "schema_version": 1,
            }
        ).encode("utf-8")
        environment = {
            "HOME": str(self.config.identity_file.parent),
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": os.defpath,
        }
        try:
            completed = self.runner.run(
                self._command(),
                input_bytes=payload,
                timeout=self._remaining(deadline),
                stdout_limit=MAX_REMOTE_RESPONSE_BYTES,
                environment=environment,
            )
            if completed.returncode != 0:
                raise ExecutionProviderCLIError("remote execution helper failed")
            response = _strict_json_object(
                completed.stdout, subject="remote execution response"
            )
            if set(response) != {"schema_version", "outcome", "result"} or (
                isinstance(response["schema_version"], bool)
                or response["schema_version"] != 1
            ):
                raise ExecutionProviderCLIError("remote execution response is invalid")
            outcome = response["outcome"]
            if outcome == "not_found" and response["result"] is None:
                return None
            if outcome != "found" or not isinstance(response["result"], dict):
                raise ExecutionProviderCLIError("remote execution response is invalid")
            if evidence:
                parsed_evidence = ExecutionEvidenceResult.from_mapping(
                    response["result"]
                )
                parsed_status = parsed_evidence.execution
                result: ExecutionStatusResult | ExecutionEvidenceResult = (
                    parsed_evidence
                )
            else:
                parsed_status = ExecutionStatusResult.from_mapping(response["result"])
                result = parsed_status
            if parsed_status.source_bytes != len(archive) or not hmac.compare_digest(
                parsed_status.source_sha256, archive_sha256
            ):
                raise ExecutionProviderCLIError(
                    "remote execution source does not match the candidate"
                )
            return result
        except (ExecutionBackendUnavailable, ExecutionBackendAmbiguous):
            raise
        except (
            ExecutionProviderCLIError,
            ValidationError,
            OSError,
            TimeoutError,
            subprocess.TimeoutExpired,
        ) as exc:
            error_type = (
                ExecutionBackendAmbiguous if effect else ExecutionBackendUnavailable
            )
            raise error_type("remote execution transport failed") from exc

    def submit(self, job: ExecutionJobSpec, *, deadline: str) -> ExecutionStatusResult:
        result = self._invoke("submit", job, None, deadline=deadline, effect=True)
        if not isinstance(result, ExecutionStatusResult):
            raise ExecutionBackendAmbiguous("remote submission result is unavailable")
        return result

    def reconcile_submit(
        self, job: ExecutionJobSpec, *, deadline: str
    ) -> ExecutionStatusResult | None:
        result = self._invoke(
            "reconcile_submit", job, None, deadline=deadline, effect=False
        )
        if result is not None and not isinstance(result, ExecutionStatusResult):
            raise ExecutionBackendUnavailable("remote submission result is invalid")
        return result

    def inspect(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionStatusResult | None:
        result = self._invoke(
            "inspect", job, execution_id, deadline=deadline, effect=False
        )
        if result is not None and not isinstance(result, ExecutionStatusResult):
            raise ExecutionBackendUnavailable("remote execution result is invalid")
        return result

    def cancel(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionStatusResult | None:
        result = self._invoke(
            "cancel", job, execution_id, deadline=deadline, effect=True
        )
        if result is not None and not isinstance(result, ExecutionStatusResult):
            raise ExecutionBackendAmbiguous("remote cancellation result is invalid")
        return result

    def reconcile_cancel(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionStatusResult | None:
        result = self._invoke(
            "reconcile_cancel",
            job,
            execution_id,
            deadline=deadline,
            effect=False,
        )
        if result is not None and not isinstance(result, ExecutionStatusResult):
            raise ExecutionBackendUnavailable("remote cancellation result is invalid")
        return result

    def collect(
        self, job: ExecutionJobSpec, execution_id: str, *, deadline: str
    ) -> ExecutionEvidenceResult | None:
        result = self._invoke(
            "collect",
            job,
            execution_id,
            deadline=deadline,
            effect=False,
            evidence=True,
        )
        if result is not None and not isinstance(result, ExecutionEvidenceResult):
            raise ExecutionBackendUnavailable("remote evidence result is invalid")
        return result


def load_spec(path: Path) -> ServiceSidecarSpec:
    raw = _read_file(
        path,
        maximum=MAX_SPEC_BYTES,
        subject="execution provider spec",
        private=False,
    )
    try:
        return ServiceSidecarSpec.from_mapping(
            _strict_json_object(raw, subject="execution provider spec")
        )
    except (ValidationError, ExecutionProviderCLIError) as exc:
        raise ExecutionProviderCLIError("execution provider spec is invalid") from exc


def _recv_exact(connection: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise ExecutionProviderCLIError("execution provider request ended early")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_request(
    connection: socket.socket, spec: ServiceSidecarSpec
) -> ServiceOperationRequest:
    raw_length = _recv_exact(connection, 4)
    length = struct.unpack("!I", raw_length)[0]
    if not 1 <= length <= spec.request_bytes + MAX_SERVICE_FRAME_OVERHEAD:
        raise ExecutionProviderCLIError("execution provider request is oversized")
    try:
        return ServiceOperationRequest.from_mapping(
            _strict_json_object(
                _recv_exact(connection, length),
                subject="execution provider request",
            )
        )
    except (ValidationError, ExecutionProviderCLIError) as exc:
        raise ExecutionProviderCLIError(
            "execution provider request is invalid"
        ) from exc


def _write_response(
    connection: socket.socket,
    request: ServiceOperationRequest,
    response: ServiceOperationResponse,
) -> None:
    raw = canonical_json(response.to_dict()).encode("utf-8")
    if len(raw) > request.response_limit_bytes + MAX_SERVICE_FRAME_OVERHEAD:
        raise ExecutionProviderCLIError("execution provider response is oversized")
    connection.sendall(struct.pack("!I", len(raw)) + raw)


def serve_unix(
    socket_path: Path,
    max_invocations: int,
    provider: ControlledExecutionProvider,
) -> int:
    if Path(socket_path) != Path(provider.spec.transport.socket_path):
        raise ExecutionProviderCLIError(
            "execution provider socket does not match its authority"
        )

    def handle(connection: socket.socket) -> None:
        request = _read_request(connection, provider.spec)
        _write_response(connection, request, provider.invoke(request))

    try:
        return serve_unix_socket(
            socket_path,
            max_invocations,
            handle,
            recoverable_errors=(
                ExecutionProviderCLIError,
                ExecutionProviderError,
                ValidationError,
                OSError,
            ),
            client_timeout=min(300.0, provider.spec.timeout_ms / 1000 + 1.0),
        )
    except UnixSidecarError as exc:
        raise ExecutionProviderCLIError(str(exc)) from exc


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
        prog="magic-glass-box-execution-provider",
        description="Run the credential-isolated controlled SSH runner provider.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="Show non-secret SSH identity")
    inspect.add_argument("--config", required=True, type=Path)
    serve = commands.add_parser("serve", help="Serve the bounded provider carrier")
    serve.add_argument("--config", required=True, type=Path)
    serve.add_argument("--spec", required=True, type=Path)
    serve.add_argument("--socket-path", required=True, type=Path)
    serve.add_argument("--max-invocations", required=True, type=_positive_integer)
    return parser


def _serve(args: argparse.Namespace) -> dict[str, object]:
    config = load_config(args.config)
    spec = load_spec(args.spec)
    validate_spec_config(spec, config)
    completed = serve_unix(
        args.socket_path,
        args.max_invocations,
        ControlledExecutionProvider(spec, SSHExecutionBackend(config)),
    )
    return {"invocations": completed, "status": "stopped"}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "inspect":
            result: object = {
                "credential": load_config(args.config).identity.to_dict(),
                "status": "configured",
            }
        else:
            result = _serve(args)
    except (
        ExecutionBackendAmbiguous,
        ExecutionBackendUnavailable,
        ExecutionProviderCLIError,
        ExecutionProviderError,
    ) as exc:
        print(f"magic-glass-box-execution-provider: {exc}", file=sys.stderr)
        return 2
    except OSError:
        print("magic-glass-box-execution-provider: operation failed", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
