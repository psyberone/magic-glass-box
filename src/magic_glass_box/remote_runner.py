from __future__ import annotations

import argparse
import base64
import fcntl
import gzip
import hashlib
import hmac
import io
import json
import os
import re
import shutil
import stat
import sys
import tarfile
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from .domain import (
    ValidationError,
    canonical_json,
    parse_rfc3339_timestamp,
    require_string,
    validate_provider_name,
)
from .execution import (
    EXECUTION_ID_RE,
    MAX_EXECUTION_ARTIFACT_BYTES,
    ExecutionEvidenceResult,
    ExecutionJobSpec,
    ExecutionStatusResult,
)
from .execution_provider_cli import BoundedCommandRunner
from .path_safety import (
    UnsafePathError,
    UnsafeSymlinkError,
    validate_repo_path,
    validate_symlink_graph,
    validate_symlink_target,
)
from .results import ResultError, WorkspaceResultStore

MAX_CONFIG_BYTES = 1024 * 1024
MAX_CANDIDATE_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_REMOTE_REQUEST_BYTES = 96 * 1024 * 1024
MAX_REPOSITORIES = 64
MAX_RECIPES = 64
MAX_COMMAND_ARGUMENTS = 64
MAX_COMMAND_ARGUMENT_BYTES = 1024
MAX_SOURCE_PATHS = 200_000
MAX_SOURCE_BYTES = 512 * 1024 * 1024
MAX_DOCKER_OUTPUT_BYTES = 4 * 1024 * 1024
TIMEOUT_EXIT_CODE = 124
MIN_MEMORY_BYTES = 64 * 1024 * 1024
MAX_MEMORY_BYTES = 64 * 1024 * 1024 * 1024
IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DOCKER_STATUS = frozenset(
    {"created", "restarting", "running", "removing", "paused", "exited", "dead"}
)


class RemoteRunnerError(RuntimeError):
    """The remote helper rejected or could not safely execute a pinned job."""


def _exact(value: object, *, fields: set[str], subject: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise RemoteRunnerError(f"{subject} fields are invalid")
    return value


def _integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise RemoteRunnerError(f"{field} is invalid")
    return value


def _private_file(path: Path, *, subject: str, maximum: int) -> bytes:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise RemoteRunnerError(f"{subject} path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise RemoteRunnerError(f"{subject} is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise RemoteRunnerError(f"{subject} must be a regular file")
        if info.st_mode & 0o077:
            raise RemoteRunnerError(f"{subject} must use mode 0600")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise RemoteRunnerError(f"{subject} must be owned by this user")
        raw = os.read(descriptor, maximum + 1)
    finally:
        os.close(descriptor)
    if not raw or len(raw) > maximum:
        raise RemoteRunnerError(f"{subject} is empty or oversized")
    return raw


def _strict_json(raw: bytes, *, subject: str) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise RemoteRunnerError(f"{subject} is invalid") from exc

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RemoteRunnerError(f"{subject} is invalid")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise RemoteRunnerError(f"{subject} is invalid")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique,
            parse_constant=reject_constant,
        )
    except RemoteRunnerError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise RemoteRunnerError(f"{subject} is invalid") from exc
    if not isinstance(value, dict):
        raise RemoteRunnerError(f"{subject} is invalid")
    return value


def _trusted_executable(value: object, *, field: str) -> Path:
    try:
        text = require_string(value, field=field, max_bytes=4096)
    except ValidationError as exc:
        raise RemoteRunnerError("remote runner configuration is invalid") from exc
    path = Path(text)
    if not path.is_absolute():
        raise RemoteRunnerError(f"{field} path must be absolute")
    try:
        info = path.lstat()
    except OSError as exc:
        raise RemoteRunnerError(f"{field} is unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or not info.st_mode & 0o100
        or info.st_mode & 0o022
        or (hasattr(os, "getuid") and info.st_uid not in {0, os.getuid()})
    ):
        raise RemoteRunnerError(f"{field} is unsafe")
    return path


def _private_directory(value: object, *, field: str, create: bool) -> Path:
    try:
        text = require_string(value, field=field, max_bytes=4096)
    except ValidationError as exc:
        raise RemoteRunnerError("remote runner configuration is invalid") from exc
    path = Path(text)
    if not path.is_absolute() or any(mark in text for mark in {",", ":"}):
        raise RemoteRunnerError(f"{field} path is invalid")
    try:
        if create:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = path.lstat()
    except OSError as exc:
        raise RemoteRunnerError(f"{field} is unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_mode & 0o077
        or (hasattr(os, "getuid") and info.st_uid != os.getuid())
    ):
        raise RemoteRunnerError(f"{field} must be a private directory")
    return path


def _command(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_COMMAND_ARGUMENTS:
        raise RemoteRunnerError("remote recipe command is invalid")
    result: list[str] = []
    for argument in value:
        try:
            text = require_string(
                argument,
                field="remote recipe command argument",
                max_bytes=MAX_COMMAND_ARGUMENT_BYTES,
            )
        except ValidationError as exc:
            raise RemoteRunnerError("remote recipe command is invalid") from exc
        if "\0" in text:
            raise RemoteRunnerError("remote recipe command is invalid")
        result.append(text)
    return tuple(result)


@dataclass(frozen=True)
class RemoteRecipe:
    recipe_id: str
    image_id: str
    command: tuple[str, ...]
    timeout_seconds: int
    stop_seconds: int
    cpu_millis: int
    memory_bytes: int
    pids_limit: int
    max_artifact_bytes: int
    max_artifacts: int

    @classmethod
    def from_mapping(cls, recipe_id: object, value: object) -> RemoteRecipe:
        try:
            name = validate_provider_name(recipe_id, field="remote recipe ID")
        except ValidationError as exc:
            raise RemoteRunnerError("remote recipe ID is invalid") from exc
        item = _exact(
            value,
            fields={
                "image_id",
                "command",
                "timeout_seconds",
                "stop_seconds",
                "cpu_millis",
                "memory_bytes",
                "pids_limit",
                "max_artifact_bytes",
                "max_artifacts",
            },
            subject="remote recipe",
        )
        image_id = item["image_id"]
        if not isinstance(image_id, str) or IMAGE_ID_RE.fullmatch(image_id) is None:
            raise RemoteRunnerError("remote recipe image identity is invalid")
        return cls(
            recipe_id=name,
            image_id=image_id,
            command=_command(item["command"]),
            timeout_seconds=_integer(
                item["timeout_seconds"],
                field="remote recipe timeout",
                minimum=10,
                maximum=24 * 60 * 60,
            ),
            stop_seconds=_integer(
                item["stop_seconds"],
                field="remote recipe stop timeout",
                minimum=1,
                maximum=300,
            ),
            cpu_millis=_integer(
                item["cpu_millis"],
                field="remote recipe CPU limit",
                minimum=100,
                maximum=64_000,
            ),
            memory_bytes=_integer(
                item["memory_bytes"],
                field="remote recipe memory limit",
                minimum=MIN_MEMORY_BYTES,
                maximum=MAX_MEMORY_BYTES,
            ),
            pids_limit=_integer(
                item["pids_limit"],
                field="remote recipe PID limit",
                minimum=16,
                maximum=4096,
            ),
            max_artifact_bytes=_integer(
                item["max_artifact_bytes"],
                field="remote recipe artifact byte limit",
                minimum=1,
                maximum=MAX_EXECUTION_ARTIFACT_BYTES // 2,
            ),
            max_artifacts=_integer(
                item["max_artifacts"],
                field="remote recipe artifact count",
                minimum=1,
                maximum=10_000,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": list(self.command),
            "cpu_millis": self.cpu_millis,
            "image_id": self.image_id,
            "max_artifact_bytes": self.max_artifact_bytes,
            "max_artifacts": self.max_artifacts,
            "memory_bytes": self.memory_bytes,
            "pids_limit": self.pids_limit,
            "recipe_id": self.recipe_id,
            "stop_seconds": self.stop_seconds,
            "timeout_seconds": self.timeout_seconds,
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(
            canonical_json(self.to_dict()).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class RemoteRunnerConfig:
    state_directory: Path
    docker_binary: Path
    git_binary: Path
    repositories: frozenset[str]
    recipes: Mapping[str, RemoteRecipe]

    def identity(self) -> dict[str, object]:
        return {
            "recipes": [
                {
                    "image_id": recipe.image_id,
                    "recipe_id": recipe.recipe_id,
                    "recipe_sha256": recipe.digest,
                }
                for recipe in sorted(
                    self.recipes.values(), key=lambda item: item.recipe_id
                )
            ],
            "repositories": sorted(self.repositories),
            "schema_version": 1,
        }


def load_remote_config(path: Path) -> RemoteRunnerConfig:
    document = _strict_json(
        _private_file(
            path,
            subject="remote runner configuration",
            maximum=MAX_CONFIG_BYTES,
        ),
        subject="remote runner configuration",
    )
    item = _exact(
        document,
        fields={
            "schema_version",
            "state_directory",
            "docker_binary",
            "git_binary",
            "repositories",
            "recipes",
        },
        subject="remote runner configuration",
    )
    if isinstance(item["schema_version"], bool) or item["schema_version"] != 1:
        raise RemoteRunnerError("remote runner configuration version is invalid")
    raw_repositories = item["repositories"]
    raw_recipes = item["recipes"]
    if (
        not isinstance(raw_repositories, list)
        or not 1 <= len(raw_repositories) <= MAX_REPOSITORIES
        or not isinstance(raw_recipes, dict)
        or not 1 <= len(raw_recipes) <= MAX_RECIPES
    ):
        raise RemoteRunnerError("remote runner configuration is invalid")
    repositories: list[str] = []
    for name in raw_repositories:
        if not isinstance(name, str) or not name or len(name.encode("utf-8")) > 512:
            raise RemoteRunnerError("remote repository mapping is invalid")
        if any(mark in name for mark in ":@?#"):
            raise RemoteRunnerError("remote repository mapping is invalid")
        repositories.append(name)
    if len(set(repositories)) != len(repositories):
        raise RemoteRunnerError("remote repository mappings must be unique")
    recipes = {
        name: RemoteRecipe.from_mapping(name, value)
        for name, value in raw_recipes.items()
    }
    return RemoteRunnerConfig(
        state_directory=_private_directory(
            item["state_directory"], field="remote runner state", create=True
        ),
        docker_binary=_trusted_executable(item["docker_binary"], field="Docker binary"),
        git_binary=_trusted_executable(item["git_binary"], field="Git binary"),
        repositories=frozenset(repositories),
        recipes=recipes,
    )


@dataclass(frozen=True)
class DockerState:
    status: str
    started_at: str | None
    finished_at: str | None
    exit_code: int | None

    def __post_init__(self) -> None:
        if self.status not in DOCKER_STATUS:
            raise RemoteRunnerError("Docker state is invalid")
        for field, value in (
            ("started_at", self.started_at),
            ("finished_at", self.finished_at),
        ):
            if value is not None:
                try:
                    parse_rfc3339_timestamp(value, field=f"Docker {field}")
                except ValidationError as exc:
                    raise RemoteRunnerError("Docker state is invalid") from exc
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool)
            or not isinstance(self.exit_code, int)
            or not 0 <= self.exit_code <= 255
        ):
            raise RemoteRunnerError("Docker state is invalid")


class DockerBackend(Protocol):
    def create(self, **values: object) -> None: ...

    def start(self, container_name: str, *, timeout_seconds: int) -> None: ...

    def inspect(self, container_name: str) -> DockerState | None: ...

    def cancel(
        self, container_name: str, *, stop_seconds: int, timeout_seconds: int
    ) -> None: ...

    def logs(
        self, container_name: str, *, timeout_seconds: int
    ) -> tuple[bytes, bytes]: ...


class CandidateMaterializer(Protocol):
    def materialize(self, archive: bytes, destination: Path) -> None: ...


class ArtifactMaterializer(Protocol):
    def materialize(
        self,
        archive: bytes,
        destination: Path,
        *,
        max_bytes: int,
        max_paths: int,
    ) -> None: ...


class DockerCLI:
    def __init__(self, binary: Path, config_directory: Path) -> None:
        self.binary = binary
        self.config_directory = config_directory
        self.runner = BoundedCommandRunner()

    def _run(
        self,
        arguments: Sequence[str],
        *,
        timeout: int,
        output_limit: int = 1024 * 1024,
        allowed: frozenset[int] = frozenset({0}),
    ) -> tuple[bytes, bytes, int]:
        environment = {
            "DOCKER_CONFIG": str(self.config_directory),
            "HOME": str(self.config_directory),
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": os.defpath,
        }
        try:
            result = self.runner.run(
                [str(self.binary), *arguments],
                input_bytes=b"",
                timeout=float(timeout),
                stdout_limit=output_limit,
                environment=environment,
            )
        except Exception as exc:
            raise RemoteRunnerError("Docker command failed") from exc
        if result.returncode not in allowed:
            raise RemoteRunnerError("Docker command failed")
        return result.stdout, result.stderr, result.returncode

    def create(self, **values: object) -> None:
        command = values["command"]
        if not isinstance(command, tuple):
            raise RemoteRunnerError("Docker recipe command is invalid")
        cpu_millis = values["cpu_millis"]
        if not isinstance(cpu_millis, int):
            raise RemoteRunnerError("Docker CPU limit is invalid")
        self._run(
            [
                "create",
                "--name",
                str(values["container_name"]),
                "--pull",
                "never",
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                str(values["pids_limit"]),
                "--memory",
                str(values["memory_bytes"]),
                "--cpus",
                f"{cpu_millis / 1000:.3f}",
                "--workdir",
                "/workspace",
                "--mount",
                f"type=bind,src={values['workspace']},dst=/workspace,readonly",
                "--mount",
                f"type=bind,src={values['artifact_upload']},dst=/mgb-artifacts.tar.gz",
                "--env",
                "MGB_ARTIFACT_PATH=/mgb-artifacts.tar.gz",
                "--ulimit",
                f"fsize={values['max_artifact_bytes']}:{values['max_artifact_bytes']}",
                "--log-driver",
                "local",
                "--log-opt",
                "max-size=4m",
                "--log-opt",
                "max-file=1",
                "--tmpfs",
                "/tmp:rw,noexec,nosuid,nodev,size=268435456",  # noqa: S108
                str(values["image_id"]),
                *command,
            ],
            timeout=30,
        )

    def start(self, container_name: str, *, timeout_seconds: int) -> None:
        self._run(["start", container_name], timeout=min(timeout_seconds, 120))

    def inspect(self, container_name: str) -> DockerState | None:
        stdout, _, returncode = self._run(
            ["inspect", "--format", "{{json .State}}", container_name],
            timeout=30,
            allowed=frozenset({0, 1}),
        )
        if returncode == 1:
            return None
        document = _strict_json(stdout, subject="Docker state")
        required = {"Status", "StartedAt", "FinishedAt", "ExitCode"}
        if not required.issubset(document):
            raise RemoteRunnerError("Docker state is invalid")

        def docker_time(value: object) -> str | None:
            if not isinstance(value, str):
                raise RemoteRunnerError("Docker state is invalid")
            if value.startswith("0001-01-01T00:00:00"):
                return None
            try:
                return (
                    parse_rfc3339_timestamp(value, field="Docker timestamp")
                    .astimezone(UTC)
                    .isoformat(timespec="microseconds")
                )
            except ValidationError as exc:
                raise RemoteRunnerError("Docker state is invalid") from exc

        exit_code = document["ExitCode"]
        if isinstance(exit_code, bool) or not isinstance(exit_code, int):
            raise RemoteRunnerError("Docker state is invalid")
        return DockerState(
            status=document["Status"],
            started_at=docker_time(document["StartedAt"]),
            finished_at=docker_time(document["FinishedAt"]),
            exit_code=exit_code if document["Status"] in {"exited", "dead"} else None,
        )

    def cancel(
        self, container_name: str, *, stop_seconds: int, timeout_seconds: int
    ) -> None:
        self._run(
            ["stop", "--time", str(stop_seconds), container_name],
            timeout=min(timeout_seconds, stop_seconds + 30),
        )

    def logs(self, container_name: str, *, timeout_seconds: int) -> tuple[bytes, bytes]:
        stdout, stderr, _ = self._run(
            ["logs", container_name],
            timeout=min(timeout_seconds, 120),
            output_limit=MAX_DOCKER_OUTPUT_BYTES,
        )
        if len(stdout) + len(stderr) > MAX_DOCKER_OUTPUT_BYTES:
            raise RemoteRunnerError("Docker logs exceed their limit")
        return stdout, stderr


class CandidateArchiveMaterializer:
    @staticmethod
    def _safe_extract(
        archive: bytes,
        destination: Path,
        *,
        mode: str,
        max_bytes: int,
        max_paths: int,
    ) -> None:
        paths: set[str] = set()
        directories: set[str] = set()
        kinds: dict[str, str] = {}
        regular: list[tuple[str, Path, int]] = []
        symlinks: list[tuple[str, str]] = []
        total = 0
        count = 0
        stream_mode = {"r:gz": "r|gz"}.get(mode)
        if stream_mode is None:
            raise RemoteRunnerError("candidate archive encoding is invalid")
        try:
            source = tarfile.open(  # noqa: SIM115
                fileobj=io.BytesIO(archive), mode=stream_mode
            )
        except (OSError, tarfile.TarError) as exc:
            raise RemoteRunnerError("candidate archive is invalid") from exc
        try:
            with (
                source,
                tempfile.TemporaryDirectory(
                    prefix=".mgb-extract-", dir=destination.parent
                ) as raw_spool,
            ):
                spool = Path(raw_spool)
                for member in source:
                    count += 1
                    if count > max_paths:
                        raise RemoteRunnerError("archive has too many paths")
                    raw_name = member.name.rstrip("/")
                    while raw_name.startswith("./"):
                        raw_name = raw_name[2:]
                    if raw_name in {"", "."}:
                        continue
                    try:
                        relative = validate_repo_path(raw_name)
                    except UnsafePathError as exc:
                        raise RemoteRunnerError(
                            "candidate archive path is unsafe"
                        ) from exc
                    if relative in paths:
                        raise RemoteRunnerError(
                            "candidate archive paths are duplicated"
                        )
                    paths.add(relative)
                    if member.isdir():
                        directories.add(relative)
                        kinds[relative] = "directory"
                        continue
                    if member.issym():
                        kinds[relative] = "symlink"
                        try:
                            target = validate_symlink_target(relative, member.linkname)
                        except UnsafeSymlinkError as exc:
                            raise RemoteRunnerError(
                                "candidate archive symlink is unsafe"
                            ) from exc
                        symlinks.append((relative, target))
                        continue
                    if not member.isfile():
                        raise RemoteRunnerError("candidate archive file type is unsafe")
                    if member.size < 0:
                        raise RemoteRunnerError("archive file size is invalid")
                    kinds[relative] = "file"
                    total += member.size
                    if total > max_bytes:
                        raise RemoteRunnerError("archive content is oversized")
                    extracted = source.extractfile(member)
                    if extracted is None:
                        raise RemoteRunnerError("candidate archive file is invalid")
                    spooled = spool.joinpath(*PurePosixPath(relative).parts)
                    spooled.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
                    try:
                        descriptor = os.open(spooled, flags, 0o600)
                        with os.fdopen(descriptor, "wb") as output:
                            remaining = member.size
                            while remaining:
                                chunk = extracted.read(min(1024 * 1024, remaining))
                                if not chunk:
                                    raise RemoteRunnerError(
                                        "candidate archive file is truncated"
                                    )
                                output.write(chunk)
                                remaining -= len(chunk)
                            if extracted.read(1):
                                raise RemoteRunnerError(
                                    "candidate archive file exceeds its size"
                                )
                    finally:
                        extracted.close()
                    regular.append((relative, spooled, member.mode))

                for path in kinds:
                    for parent in PurePosixPath(path).parents:
                        parent_text = parent.as_posix()
                        if parent_text == ".":
                            continue
                        parent_kind = kinds.get(parent_text)
                        if parent_kind is not None and parent_kind != "directory":
                            raise RemoteRunnerError(
                                "candidate archive path aliases a non-directory"
                            )
                leaves = {PurePosixPath(path): target for path, target in symlinks}
                try:
                    validate_symlink_graph(leaves)
                except UnsafeSymlinkError as exc:
                    raise RemoteRunnerError(
                        "candidate symlink graph is unsafe"
                    ) from exc
                for path in sorted(
                    directories
                    | {
                        parent.as_posix()
                        for value in paths
                        for parent in PurePosixPath(value).parents
                        if parent.as_posix() != "."
                    },
                    key=lambda value: (len(PurePosixPath(value).parts), value),
                ):
                    output_directory = destination.joinpath(*PurePosixPath(path).parts)
                    output_directory.mkdir(mode=0o755, exist_ok=True)
                    output_directory.chmod(0o755)
                for relative, spooled, member_mode in regular:
                    target = destination.joinpath(*PurePosixPath(relative).parts)
                    try:
                        target.lstat()
                    except FileNotFoundError:
                        pass
                    else:
                        raise RemoteRunnerError(
                            "candidate archive destination is not empty"
                        )
                    os.replace(spooled, target)
                    target.chmod(0o755 if member_mode & 0o111 else 0o644)
                for relative, target in symlinks:
                    destination.joinpath(*PurePosixPath(relative).parts).symlink_to(
                        target
                    )
                destination.chmod(0o755)
        except RemoteRunnerError:
            raise
        except (OSError, tarfile.TarError) as exc:
            raise RemoteRunnerError("candidate archive extraction failed") from exc

    def materialize(self, archive: bytes, destination: Path) -> None:
        if (
            not isinstance(archive, bytes)
            or not archive
            or len(archive) > MAX_CANDIDATE_ARCHIVE_BYTES
        ):
            raise RemoteRunnerError("candidate archive is empty or oversized")
        self._safe_extract(
            archive,
            destination,
            mode="r:gz",
            max_bytes=MAX_SOURCE_BYTES,
            max_paths=MAX_SOURCE_PATHS,
        )


class ArtifactArchiveMaterializer:
    def materialize(
        self,
        archive: bytes,
        destination: Path,
        *,
        max_bytes: int,
        max_paths: int,
    ) -> None:
        if not isinstance(archive, bytes) or not archive or len(archive) > max_bytes:
            raise RemoteRunnerError("artifact archive is empty or oversized")
        CandidateArchiveMaterializer._safe_extract(
            archive,
            destination,
            mode="r:gz",
            max_bytes=max_bytes,
            max_paths=max_paths,
        )


class RemoteRunner:
    def __init__(
        self,
        config: RemoteRunnerConfig,
        *,
        docker: DockerBackend | None = None,
        materializer: CandidateMaterializer | None = None,
        artifact_materializer: ArtifactMaterializer | None = None,
        clock: Any | None = None,
    ) -> None:
        if not isinstance(config, RemoteRunnerConfig):
            raise RemoteRunnerError("remote runner configuration is invalid")
        self.config = config
        docker_config = _private_directory(
            str(config.state_directory / "docker-client"),
            field="Docker client state",
            create=True,
        )
        self.docker = docker or DockerCLI(config.docker_binary, docker_config)
        self.materializer = materializer or CandidateArchiveMaterializer()
        self.artifact_materializer = (
            artifact_materializer or ArtifactArchiveMaterializer()
        )
        self.clock = clock or (
            lambda: datetime.now(UTC).isoformat(timespec="microseconds")
        )
        self.jobs = config.state_directory / "jobs"
        self.jobs.mkdir(mode=0o700, exist_ok=True)
        os.chmod(self.jobs, 0o700)
        self.evidence = config.state_directory / "evidence"
        self.evidence.mkdir(mode=0o700, exist_ok=True)
        os.chmod(self.evidence, 0o700)
        self.lock_path = config.state_directory / "runner.lock"

    @contextmanager
    def _lock(self) -> Iterator[None]:
        descriptor = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @staticmethod
    def _execution_id(job: ExecutionJobSpec) -> str:
        return (
            "rex_" + hashlib.sha256(job.operation_id.encode("ascii")).hexdigest()[:32]
        )

    @staticmethod
    def _attempt_id(execution_id: str) -> str:
        return "att_" + execution_id.removeprefix("rex_")

    def _job_directory(self, execution_id: str) -> Path:
        if EXECUTION_ID_RE.fullmatch(execution_id) is None:
            raise RemoteRunnerError("remote execution ID is invalid")
        return self.jobs / execution_id

    def artifact_directory(self, execution_id: str) -> Path:
        path = self._job_directory(execution_id) / "artifacts"
        try:
            info = path.lstat()
        except OSError as exc:
            raise RemoteRunnerError("remote artifact directory is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise RemoteRunnerError("remote artifact directory is unsafe")
        return path

    def artifact_upload_path(self, execution_id: str) -> Path:
        path = self._job_directory(execution_id) / "artifact-upload.tar.gz"
        try:
            info = path.lstat()
        except OSError as exc:
            raise RemoteRunnerError("remote artifact upload is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise RemoteRunnerError("remote artifact upload is unsafe")
        return path

    @staticmethod
    def _atomic_json(path: Path, value: object) -> None:
        raw = canonical_json(value).encode("utf-8")
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            descriptor = os.open(temporary, flags, 0o600)
            with os.fdopen(descriptor, "wb") as output:
                output.write(raw)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
        except OSError as exc:
            temporary.unlink(missing_ok=True)
            raise RemoteRunnerError("remote job state cannot be written") from exc

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        try:
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                raise RemoteRunnerError("remote job state is invalid")
            raw = path.read_bytes()
        except OSError as exc:
            raise RemoteRunnerError("remote job state is unavailable") from exc
        if not raw or len(raw) > MAX_REMOTE_REQUEST_BYTES:
            raise RemoteRunnerError("remote job state is invalid")
        return _strict_json(raw, subject="remote job state")

    def _recipe(self, job: ExecutionJobSpec) -> RemoteRecipe:
        recipe = self.config.recipes.get(job.recipe_id)
        if (
            recipe is None
            or job.repository not in self.config.repositories
            or recipe.digest != job.recipe_sha256
            or recipe.image_id != job.image_id
        ):
            raise RemoteRunnerError("remote job authority is unavailable")
        return recipe

    def _load_job(self, execution_id: str) -> ExecutionJobSpec | None:
        path = self._job_directory(execution_id) / "job.json"
        if not path.exists():
            return None
        try:
            return ExecutionJobSpec(**self._read_json(path))
        except (TypeError, ValidationError) as exc:
            raise RemoteRunnerError("remote job state is invalid") from exc

    def _assert_job(self, job: ExecutionJobSpec, execution_id: str) -> None:
        stored = self._load_job(execution_id)
        if stored is None:
            raise RemoteRunnerError("remote job is unavailable")
        if stored != job:
            raise RemoteRunnerError("remote execution ID belongs to a different job")

    def _assert_archive(self, archive: bytes, execution_id: str) -> None:
        source = _exact(
            self._read_json(self._job_directory(execution_id) / "source.json"),
            fields={"bytes", "sha256"},
            subject="remote candidate source",
        )
        digest = hashlib.sha256(archive).hexdigest()
        if source != {"bytes": len(archive), "sha256": digest}:
            raise RemoteRunnerError("remote candidate archive does not match")

    def _materialize_candidate(
        self, execution_id: str, candidate_archive: bytes
    ) -> None:
        directory = self._job_directory(execution_id)
        marker = directory / "materialized"
        if marker.exists():
            return
        workspace = directory / "workspace"
        try:
            if workspace.exists():
                shutil.rmtree(workspace)
            workspace.mkdir(mode=0o700)
            self.materializer.materialize(candidate_archive, workspace)
            descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(descriptor)
        except RemoteRunnerError:
            raise
        except OSError as exc:
            raise RemoteRunnerError("candidate materialization failed") from exc

    def _create_container(
        self,
        execution_id: str,
        recipe: RemoteRecipe,
    ) -> None:
        directory = self._job_directory(execution_id)
        self.docker.create(
            artifact_upload=directory / "artifact-upload.tar.gz",
            command=recipe.command,
            container_name=self._container_name(execution_id),
            cpu_millis=recipe.cpu_millis,
            image_id=recipe.image_id,
            max_artifact_bytes=recipe.max_artifact_bytes,
            memory_bytes=recipe.memory_bytes,
            network_disabled=True,
            pids_limit=recipe.pids_limit,
            read_only_root=True,
            timeout_seconds=recipe.timeout_seconds,
            workspace=directory / "workspace",
        )

    @staticmethod
    def _container_name(execution_id: str) -> str:
        return "mgb-" + execution_id.removeprefix("rex_")

    def _result_store(self, recipe: RemoteRecipe) -> WorkspaceResultStore:
        return WorkspaceResultStore(
            self.evidence,
            max_changed_bytes=recipe.max_artifact_bytes,
            max_paths=recipe.max_artifacts,
            git_binary=self.config.git_binary,
        )

    def submit(
        self, job: ExecutionJobSpec, candidate_archive: bytes
    ) -> ExecutionStatusResult:
        if not isinstance(job, ExecutionJobSpec):
            raise RemoteRunnerError("remote job is invalid")
        if (
            not isinstance(candidate_archive, bytes)
            or not candidate_archive
            or len(candidate_archive) > MAX_CANDIDATE_ARCHIVE_BYTES
        ):
            raise RemoteRunnerError("remote candidate archive is empty or oversized")
        recipe = self._recipe(job)
        execution_id = self._execution_id(job)
        with self._lock():
            directory = self._job_directory(execution_id)
            if directory.exists():
                self._assert_job(job, execution_id)
                self._assert_archive(candidate_archive, execution_id)
                state = self.docker.inspect(self._container_name(execution_id))
                if state is None:
                    self._result_store(recipe).stage(
                        self._attempt_id(execution_id),
                        directory / "artifacts",
                        job.candidate_revision,
                    )
                    self._materialize_candidate(execution_id, candidate_archive)
                    self._create_container(execution_id, recipe)
                    state = self.docker.inspect(self._container_name(execution_id))
                if state is not None and state.status == "created":
                    self.docker.start(
                        self._container_name(execution_id),
                        timeout_seconds=recipe.timeout_seconds,
                    )
                result = self.inspect(job, execution_id, locked=True)
                if result is None:
                    raise RemoteRunnerError("remote job state is unavailable")
                return result
            submitted_at = self.clock()
            try:
                timeout_at = (
                    (
                        parse_rfc3339_timestamp(
                            submitted_at, field="remote submission timestamp"
                        )
                        + timedelta(seconds=recipe.timeout_seconds)
                    )
                    .astimezone(UTC)
                    .isoformat(timespec="microseconds")
                )
            except ValidationError as exc:
                raise RemoteRunnerError(
                    "remote submission timestamp is invalid"
                ) from exc
            initializing = self.jobs / f".{execution_id}.initializing"
            try:
                if initializing.exists():
                    shutil.rmtree(initializing)
                initializing.mkdir(mode=0o700)
                (initializing / "artifacts").mkdir(mode=0o700)
                upload_descriptor = os.open(
                    initializing / "artifact-upload.tar.gz",
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o666,
                )
                os.close(upload_descriptor)
                # The enclosing per-job directory is 0700. The bind-mounted
                # upload itself must remain writable by a non-root image user.
                os.chmod(initializing / "artifact-upload.tar.gz", 0o666)  # noqa: S103
                self._atomic_json(initializing / "job.json", job.to_dict())
                self._atomic_json(
                    initializing / "source.json",
                    {
                        "bytes": len(candidate_archive),
                        "sha256": hashlib.sha256(candidate_archive).hexdigest(),
                    },
                )
                self._atomic_json(
                    initializing / "runtime.json",
                    {
                        "container_name": self._container_name(execution_id),
                        "submitted_at": submitted_at,
                        "timeout_at": timeout_at,
                    },
                )
                os.replace(initializing, directory)
                artifacts = directory / "artifacts"
                self._result_store(recipe).stage(
                    self._attempt_id(execution_id), artifacts, job.candidate_revision
                )
                self._materialize_candidate(execution_id, candidate_archive)
                self._create_container(execution_id, recipe)
                self.docker.start(
                    self._container_name(execution_id),
                    timeout_seconds=recipe.timeout_seconds,
                )
            except Exception as exc:
                if initializing.exists():
                    shutil.rmtree(initializing, ignore_errors=True)
                raise RemoteRunnerError("remote job submission failed") from exc
            result = self.inspect(job, execution_id, locked=True)
            if result is None:
                raise RemoteRunnerError("remote job state is unavailable")
            return result

    def reconcile_submission(
        self, job: ExecutionJobSpec, candidate_archive: bytes
    ) -> ExecutionStatusResult | None:
        """Resume an accepted exact submission; never create absent remote intent."""

        if not isinstance(job, ExecutionJobSpec):
            raise RemoteRunnerError("remote job is invalid")
        execution_id = self._execution_id(job)
        if not self._job_directory(execution_id).exists():
            return None
        return self.submit(job, candidate_archive)

    def find(self, job: ExecutionJobSpec) -> ExecutionStatusResult | None:
        execution_id = self._execution_id(job)
        with self._lock():
            if not self._job_directory(execution_id).exists():
                return None
            self._assert_job(job, execution_id)
            return self.inspect(job, execution_id, locked=True)

    def inspect(
        self, job: ExecutionJobSpec, execution_id: str, *, locked: bool = False
    ) -> ExecutionStatusResult | None:
        if not locked:
            with self._lock():
                return self.inspect(job, execution_id, locked=True)
        directory = self._job_directory(execution_id)
        if not directory.exists():
            return None
        self._assert_job(job, execution_id)
        runtime = _exact(
            self._read_json(directory / "runtime.json"),
            fields={"container_name", "submitted_at", "timeout_at"},
            subject="remote job runtime",
        )
        if runtime["container_name"] != self._container_name(execution_id):
            raise RemoteRunnerError("remote job runtime is invalid")
        try:
            submitted_at = (
                parse_rfc3339_timestamp(
                    runtime["submitted_at"], field="remote submission timestamp"
                )
                .astimezone(UTC)
                .isoformat(timespec="microseconds")
            )
        except ValidationError as exc:
            raise RemoteRunnerError("remote job runtime is invalid") from exc
        try:
            timeout_at = parse_rfc3339_timestamp(
                runtime["timeout_at"], field="remote execution timeout"
            )
            observed_at = parse_rfc3339_timestamp(
                self.clock(), field="remote observation timestamp"
            )
        except ValidationError as exc:
            raise RemoteRunnerError("remote job runtime is invalid") from exc
        state = self.docker.inspect(self._container_name(execution_id))
        if state is None:
            return None
        if state.status in {"created", "running", "restarting", "paused"} and (
            observed_at >= timeout_at
        ):
            marker = directory / "timeout-requested"
            if not marker.exists():
                descriptor = os.open(
                    marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                )
                os.close(descriptor)
            recipe = self._recipe(job)
            self.docker.cancel(
                self._container_name(execution_id),
                stop_seconds=recipe.stop_seconds,
                timeout_seconds=recipe.timeout_seconds,
            )
            state = self.docker.inspect(self._container_name(execution_id))
            if state is None:
                raise RemoteRunnerError("remote timeout outcome is unavailable")
        cancelled = (directory / "cancel-requested").exists()
        timed_out = (directory / "timeout-requested").exists()
        if state.status == "created":
            lifecycle = ("queued", None, None, None)
        elif state.status in {"running", "restarting", "paused"}:
            lifecycle = ("running", state.started_at, None, None)
        elif state.status in {"exited", "dead"}:
            if state.finished_at is None:
                raise RemoteRunnerError("remote Docker state is incomplete")
            lifecycle_state = (
                "cancelled"
                if cancelled
                else "failed"
                if timed_out
                else "succeeded"
                if state.exit_code == 0
                else "failed"
            )
            exit_code = (
                None
                if cancelled
                else TIMEOUT_EXIT_CODE
                if timed_out and state.exit_code == 0
                else state.exit_code
            )
            lifecycle = (
                lifecycle_state,
                state.started_at,
                state.finished_at,
                exit_code,
            )
        else:
            raise RemoteRunnerError("remote Docker state is unavailable")
        recipe = self._recipe(job)
        source = _exact(
            self._read_json(directory / "source.json"),
            fields={"bytes", "sha256"},
            subject="remote candidate source",
        )
        return ExecutionStatusResult.from_mapping(
            {
                "schema_version": 1,
                "execution_id": execution_id,
                "state": lifecycle[0],
                "candidate_revision": job.candidate_revision,
                "object_format": job.object_format,
                "recipe_id": recipe.recipe_id,
                "recipe_sha256": recipe.digest,
                "image_id": recipe.image_id,
                "source_bytes": source["bytes"],
                "source_sha256": source["sha256"],
                "submitted_at": submitted_at,
                "started_at": lifecycle[1],
                "finished_at": lifecycle[2],
                "exit_code": lifecycle[3],
            }
        )

    def cancel(
        self, job: ExecutionJobSpec, execution_id: str
    ) -> ExecutionStatusResult | None:
        recipe = self._recipe(job)
        with self._lock():
            current = self.inspect(job, execution_id, locked=True)
            if current is None:
                return None
            if current.state in {"succeeded", "failed", "cancelled"}:
                return current
            marker = self._job_directory(execution_id) / "cancel-requested"
            if not marker.exists():
                descriptor = os.open(
                    marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                )
                os.close(descriptor)
            self.docker.cancel(
                self._container_name(execution_id),
                stop_seconds=recipe.stop_seconds,
                timeout_seconds=recipe.timeout_seconds,
            )
            result = self.inspect(job, execution_id, locked=True)
            if result is None:
                raise RemoteRunnerError("remote cancellation outcome is unavailable")
            return result

    def reconcile_cancellation(
        self, job: ExecutionJobSpec, execution_id: str
    ) -> ExecutionStatusResult | None:
        recipe = self._recipe(job)
        with self._lock():
            directory = self._job_directory(execution_id)
            if not directory.exists():
                return None
            self._assert_job(job, execution_id)
            if not (directory / "cancel-requested").exists():
                return None
            current = self.inspect(job, execution_id, locked=True)
            if current is None:
                raise RemoteRunnerError("remote cancellation outcome is unavailable")
            if current.state == "cancelled":
                return current
            if current.state not in {"queued", "running"}:
                raise RemoteRunnerError("remote cancellation outcome is inconsistent")
            self.docker.cancel(
                self._container_name(execution_id),
                stop_seconds=recipe.stop_seconds,
                timeout_seconds=recipe.timeout_seconds,
            )
            result = self.inspect(job, execution_id, locked=True)
            if result is None:
                raise RemoteRunnerError("remote cancellation outcome is unavailable")
            return result

    @staticmethod
    def _package(directory: Path) -> bytes:
        output = io.BytesIO()
        with (
            gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed,
            tarfile.open(
                fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT
            ) as archive,
        ):
            files = [directory / "manifest.json"]
            blobs = directory / "blobs"
            if blobs.exists():
                files.extend(sorted(blobs.iterdir(), key=lambda path: path.name))
            for path in files:
                info = path.stat()
                if not stat.S_ISREG(info.st_mode):
                    raise RemoteRunnerError("captured evidence is invalid")
                relative = path.relative_to(directory).as_posix()
                tar_info = tarfile.TarInfo(relative)
                tar_info.size = info.st_size
                tar_info.mode = 0o600
                tar_info.mtime = 0
                tar_info.uid = 0
                tar_info.gid = 0
                tar_info.uname = ""
                tar_info.gname = ""
                with path.open("rb") as source:
                    archive.addfile(tar_info, source)
        result = output.getvalue()
        if len(result) > MAX_EXECUTION_ARTIFACT_BYTES:
            raise RemoteRunnerError("captured evidence exceeds its transport limit")
        return result

    def _read_artifact_upload(self, execution_id: str, recipe: RemoteRecipe) -> bytes:
        path = self.artifact_upload_path(execution_id)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
            with os.fdopen(descriptor, "rb") as source:
                initial = os.fstat(source.fileno())
                if initial.st_size > recipe.max_artifact_bytes:
                    raise RemoteRunnerError("remote artifact upload is oversized")
                raw = source.read(recipe.max_artifact_bytes + 1)
                final = os.fstat(source.fileno())
        except OSError as exc:
            raise RemoteRunnerError("remote artifact upload cannot be read") from exc
        identity = (
            initial.st_dev,
            initial.st_ino,
            initial.st_size,
            initial.st_mtime_ns,
        )
        final_identity = (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
        )
        if identity != final_identity or len(raw) != initial.st_size:
            raise RemoteRunnerError("remote artifact upload changed while being read")
        return raw

    def collect(
        self, job: ExecutionJobSpec, execution_id: str
    ) -> ExecutionEvidenceResult | None:
        recipe = self._recipe(job)
        with self._lock():
            status = self.inspect(job, execution_id, locked=True)
            if status is None:
                return None
            if status.state not in {"succeeded", "failed", "cancelled"}:
                raise RemoteRunnerError("remote evidence is not terminal")
            try:
                artifact_archive = self._read_artifact_upload(execution_id, recipe)
                artifact_directory = self.artifact_directory(execution_id)
                shutil.rmtree(artifact_directory)
                artifact_directory.mkdir(mode=0o700)
                if artifact_archive:
                    self.artifact_materializer.materialize(
                        artifact_archive,
                        artifact_directory,
                        max_bytes=recipe.max_artifact_bytes,
                        max_paths=recipe.max_artifacts,
                    )
                captured = self._result_store(recipe).capture(
                    self._attempt_id(execution_id),
                    artifact_directory,
                )
            except (ResultError, RemoteRunnerError, OSError) as exc:
                raise RemoteRunnerError("remote artifact capture failed") from exc
            bundle = self._package(captured.directory)
            stdout, stderr = self.docker.logs(
                self._container_name(execution_id),
                timeout_seconds=recipe.timeout_seconds,
            )
            if len(stdout) + len(stderr) > MAX_DOCKER_OUTPUT_BYTES:
                raise RemoteRunnerError("remote output exceeds its limit")
            framed = (
                len(stdout).to_bytes(8, "big")
                + stdout
                + len(stderr).to_bytes(8, "big")
                + stderr
            )
            assert status.finished_at is not None
            if status.started_at is None:
                duration_ms = 0
            else:
                started = parse_rfc3339_timestamp(
                    status.started_at, field="remote start timestamp"
                )
                finished = parse_rfc3339_timestamp(
                    status.finished_at, field="remote finish timestamp"
                )
                duration_ms = int((finished - started).total_seconds() * 1000)
            return ExecutionEvidenceResult.from_mapping(
                {
                    "schema_version": 1,
                    "execution": status.to_dict(),
                    "duration_ms": duration_ms,
                    "output_bytes": len(stdout) + len(stderr),
                    "output_sha256": hashlib.sha256(framed).hexdigest(),
                    "artifact_bytes": len(bundle),
                    "artifact_count": captured.manifest.changed_paths,
                    "artifact_sha256": hashlib.sha256(bundle).hexdigest(),
                    "artifact_encoding": "tar+gzip+base64",
                    "artifact_bundle": base64.b64encode(bundle).decode("ascii"),
                }
            )


def handle_request(runner: RemoteRunner, value: object) -> dict[str, object]:
    request = _exact(
        value,
        fields={
            "schema_version",
            "action",
            "candidate_archive",
            "deadline",
            "execution_id",
            "job",
        },
        subject="remote execution request",
    )
    if isinstance(request["schema_version"], bool) or request["schema_version"] != 1:
        raise RemoteRunnerError("remote execution request version is invalid")
    try:
        deadline = parse_rfc3339_timestamp(
            request["deadline"], field="remote execution deadline"
        )
    except ValidationError as exc:
        raise RemoteRunnerError("remote execution deadline is invalid") from exc
    if deadline.astimezone(UTC) <= datetime.now(UTC):
        raise RemoteRunnerError("remote execution deadline has expired")
    action = request["action"]
    if action not in {
        "submit",
        "reconcile_submit",
        "inspect",
        "cancel",
        "reconcile_cancel",
        "collect",
    }:
        raise RemoteRunnerError("remote execution action is invalid")
    try:
        job = ExecutionJobSpec(**request["job"])
    except (TypeError, ValidationError) as exc:
        raise RemoteRunnerError("remote execution job is invalid") from exc
    execution_id = request["execution_id"]
    raw_archive = request["candidate_archive"]
    if action in {"submit", "reconcile_submit"}:
        if execution_id is not None:
            raise RemoteRunnerError("remote execution identity is invalid")
    elif (
        not isinstance(execution_id, str)
        or EXECUTION_ID_RE.fullmatch(execution_id) is None
    ):
        raise RemoteRunnerError("remote execution identity is invalid")
    candidate_archive: bytes | None = None
    if action in {"submit", "reconcile_submit"}:
        archive = _exact(
            raw_archive,
            fields={"bundle", "bytes", "encoding", "sha256"},
            subject="remote candidate archive",
        )
        if archive["encoding"] != "tar+gzip+base64" or not isinstance(
            archive["bundle"], str
        ):
            raise RemoteRunnerError("remote candidate archive is invalid")
        try:
            candidate_archive = base64.b64decode(archive["bundle"], validate=True)
        except (ValueError, TypeError) as exc:
            raise RemoteRunnerError("remote candidate archive is invalid") from exc
        expected_bytes = archive["bytes"]
        expected_sha256 = archive["sha256"]
        if (
            isinstance(expected_bytes, bool)
            or not isinstance(expected_bytes, int)
            or not 1 <= expected_bytes <= MAX_CANDIDATE_ARCHIVE_BYTES
            or len(candidate_archive) != expected_bytes
            or not isinstance(expected_sha256, str)
            or SHA256_RE.fullmatch(expected_sha256) is None
            or not hmac.compare_digest(
                hashlib.sha256(candidate_archive).hexdigest(), expected_sha256
            )
        ):
            raise RemoteRunnerError("remote candidate archive is invalid")
    elif raw_archive is not None:
        raise RemoteRunnerError("remote candidate archive is invalid for this action")
    if action == "submit":
        assert candidate_archive is not None
        result: ExecutionStatusResult | ExecutionEvidenceResult | None = runner.submit(
            job, candidate_archive
        )
    elif action == "reconcile_submit":
        assert candidate_archive is not None
        result = runner.reconcile_submission(job, candidate_archive)
    elif action == "inspect":
        assert isinstance(execution_id, str)
        result = runner.inspect(job, execution_id)
    elif action == "cancel":
        assert isinstance(execution_id, str)
        result = runner.cancel(job, execution_id)
    elif action == "reconcile_cancel":
        assert isinstance(execution_id, str)
        result = runner.reconcile_cancellation(job, execution_id)
    else:
        assert isinstance(execution_id, str)
        result = runner.collect(job, execution_id)
    return {
        "schema_version": 1,
        "outcome": "not_found" if result is None else "found",
        "result": None if result is None else result.to_dict(),
    }


def default_config_path() -> Path:
    return Path.home() / ".config" / "magic-glass-box" / "remote-runner.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="magic-glass-box-remote-runner",
        description="Run pinned validation recipes on an operator-owned host.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="Show non-secret runner identity")
    inspect.add_argument("--config", type=Path, default=default_config_path())
    invoke = commands.add_parser("invoke", help="Handle one bounded provider request")
    invoke.add_argument("--config", type=Path, default=default_config_path())
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_remote_config(args.config)
        if args.command == "inspect":
            result: object = {"runner": config.identity(), "status": "configured"}
        else:
            raw = sys.stdin.buffer.read(MAX_REMOTE_REQUEST_BYTES + 1)
            if not raw or len(raw) > MAX_REMOTE_REQUEST_BYTES:
                raise RemoteRunnerError(
                    "remote execution request is empty or oversized"
                )
            result = handle_request(
                RemoteRunner(config),
                _strict_json(raw, subject="remote execution request"),
            )
    except RemoteRunnerError as exc:
        print(f"magic-glass-box-remote-runner: {exc}", file=sys.stderr)
        return 2
    except OSError:
        print("magic-glass-box-remote-runner: operation failed", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
