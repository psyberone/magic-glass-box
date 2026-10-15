from __future__ import annotations

import fcntl
import hashlib
import math
import os
import re
import selectors
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from bisect import bisect_left
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .domain import canonical_json, content_sha256, require_string
from .path_safety import (
    MAX_PATH_BYTES,
    UnsafePathError,
    UnsafeSymlinkError,
    validate_repo_path,
    validate_symlink_graph,
    validate_symlink_target,
)
from .results import FileState, ResultManifest
from .workspace import GitSnapshotter, SnapshotResult, WorkspaceError

CANDIDATE_SCHEMA_VERSION = 1
OBJECT_ID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CANDIDATE_ID_PATTERNS = {
    "attempt_id": re.compile(r"^att_[0-9a-f]{32}$"),
    "attempt_result_id": re.compile(r"^rsl_[0-9a-f]{32}$"),
    "producer_run_id": re.compile(r"^run_[0-9a-f]{32}$"),
    "project_id": re.compile(r"^boxp_[0-9a-f]{32}$"),
    "context_version_id": re.compile(r"^ctx_[0-9a-f]{32}$"),
    "work_thread_id": re.compile(r"^wth_[0-9a-f]{32}$"),
}
GIT_OBJECT_FORMATS = {"sha1": 40, "sha256": 64}
DEFAULT_GIT_TIMEOUT_SECONDS = 60.0
FIXED_GIT_DATE = "@946684800 +0000"
FIXED_GIT_TIMESTAMP = "946684800 +0000"

MAX_BASE_TREE_ENTRIES = 200_000
MAX_BASE_TREE_BYTES = 32 * 1024 * 1024
MAX_BASE_IMPORT_BYTES = 512 * 1024 * 1024
MAX_POST_IMAGE_BYTES = 64 * 1024 * 1024
MAX_COMMAND_INPUT_BYTES = 32 * 1024 * 1024
MAX_STDOUT_BYTES = 32 * 1024 * 1024
MAX_STDERR_BYTES = 64 * 1024
MAX_COMMANDS = 64
LOCK_POLL_SECONDS = 0.01
PROCESS_STOP_SECONDS = 0.5


class CandidateError(RuntimeError):
    """A retained result could not be represented as a trusted candidate."""


@dataclass(frozen=True)
class CandidateManifest:
    schema_version: int
    attempt_id: str
    attempt_result_id: str
    producer_run_id: str
    project_id: str
    context_version_id: str
    work_thread_id: str
    base_revision: str
    result_content_sha256: str
    head_revision: str
    tree_revision: str
    object_format: str

    def __post_init__(self) -> None:
        if self.schema_version != CANDIDATE_SCHEMA_VERSION or isinstance(
            self.schema_version, bool
        ):
            raise CandidateError(
                f"candidate schema_version must be {CANDIDATE_SCHEMA_VERSION}"
            )
        for field, value in (
            ("attempt_id", self.attempt_id),
            ("attempt_result_id", self.attempt_result_id),
            ("producer_run_id", self.producer_run_id),
            ("project_id", self.project_id),
            ("context_version_id", self.context_version_id),
            ("work_thread_id", self.work_thread_id),
        ):
            if (
                not isinstance(value, str)
                or CANDIDATE_ID_PATTERNS[field].fullmatch(value) is None
            ):
                raise CandidateError(f"candidate {field} is invalid")
        length = GIT_OBJECT_FORMATS.get(self.object_format)
        if length is None:
            raise CandidateError("candidate object_format is invalid")
        for field, value in (
            ("base_revision", self.base_revision),
            ("head_revision", self.head_revision),
            ("tree_revision", self.tree_revision),
        ):
            if (
                not isinstance(value, str)
                or OBJECT_ID_RE.fullmatch(value) is None
                or len(value) != length
            ):
                raise CandidateError(f"candidate {field} is invalid")
        if (
            not isinstance(self.result_content_sha256, str)
            or SHA256_RE.fullmatch(self.result_content_sha256) is None
        ):
            raise CandidateError("candidate result_content_sha256 is invalid")

    @classmethod
    def from_mapping(cls, value: object) -> CandidateManifest:
        if not isinstance(value, dict):
            raise CandidateError("candidate manifest must be an object")
        fields = {
            "schema_version",
            "attempt_id",
            "attempt_result_id",
            "producer_run_id",
            "project_id",
            "context_version_id",
            "work_thread_id",
            "base_revision",
            "result_content_sha256",
            "head_revision",
            "tree_revision",
            "object_format",
        }
        unknown = set(value) - fields
        missing = fields - set(value)
        if unknown:
            raise CandidateError(
                "candidate manifest contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        if missing:
            raise CandidateError(
                "candidate manifest is missing fields: " + ", ".join(sorted(missing))
            )
        return cls(**{field: value[field] for field in fields})  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "attempt_result_id": self.attempt_result_id,
            "base_revision": self.base_revision,
            "context_version_id": self.context_version_id,
            "head_revision": self.head_revision,
            "object_format": self.object_format,
            "producer_run_id": self.producer_run_id,
            "project_id": self.project_id,
            "result_content_sha256": self.result_content_sha256,
            "schema_version": self.schema_version,
            "tree_revision": self.tree_revision,
            "work_thread_id": self.work_thread_id,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class MaterializedCandidate:
    base_revision: str
    head_revision: str
    tree_revision: str
    object_format: str


@dataclass(frozen=True)
class _TreeEntry:
    path: str
    mode: str
    kind: str
    object_id: str
    size: int


@dataclass(frozen=True)
class _BaseObject:
    size: int
    sha256: str
    body: bytes | None = None


@dataclass
class _Operation:
    deadline: float
    commands: int = 0


class _BatchReader:
    """Deadline-bound reader for a single Git object batch stream."""

    def __init__(self, stream: object, deadline: float):
        self.stream = stream
        self.deadline = deadline
        self.buffer = bytearray()
        self.selector = selectors.DefaultSelector()
        self.selector.register(stream, selectors.EVENT_READ)

    def close(self) -> None:
        self.selector.close()

    def _fill(self) -> None:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0 or not self.selector.select(remaining):
            raise subprocess.TimeoutExpired("git cat-file --batch", 0)
        chunk = os.read(self.stream.fileno(), 64 * 1024)  # type: ignore[attr-defined]
        if not chunk:
            raise CandidateError("Git returned a truncated object batch")
        self.buffer.extend(chunk)

    def line(self, limit: int) -> bytes:
        while b"\n" not in self.buffer:
            if len(self.buffer) > limit:
                raise CandidateError("Git returned a malformed object batch")
            self._fill()
        end = self.buffer.index(b"\n") + 1
        if end > limit:
            raise CandidateError("Git returned a malformed object batch")
        value = bytes(self.buffer[:end])
        del self.buffer[:end]
        return value

    def chunks(self, size: int) -> Iterator[bytes]:
        remaining = size
        while remaining:
            if not self.buffer:
                self._fill()
            count = min(remaining, len(self.buffer), 64 * 1024)
            value = bytes(self.buffer[:count])
            del self.buffer[:count]
            remaining -= count
            yield value

    def exact(self, size: int) -> bytes:
        return b"".join(self.chunks(size))


class GitCandidateStore:
    """Build deterministic commits in a private, reconstructable Git cache."""

    def __init__(
        self,
        root: Path,
        *,
        binary: Path | None = None,
        timeout: float = DEFAULT_GIT_TIMEOUT_SECONDS,
    ):
        if binary is None:
            discovered = shutil.which("git")
            if discovered is None:
                raise CandidateError("Git CLI was not found")
            binary = Path(discovered)
        if not Path(binary).is_absolute():
            raise CandidateError("Git CLI path must be absolute")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise CandidateError("Git operation timeout must be positive")
        self.root = Path(root).expanduser().absolute()
        self.binary = Path(binary)
        self.timeout = float(timeout)

    def ensure_root(self) -> Path:
        try:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            info = self.root.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise CandidateError("candidate store must be a directory")
            os.chmod(self.root, 0o700)
            for name in ("home", "tmp", "projects"):
                path = self.root / name
                path.mkdir(mode=0o700, exist_ok=True)
                os.chmod(path, 0o700)
        except CandidateError:
            raise
        except OSError as exc:
            raise CandidateError("candidate store is unavailable") from exc
        return self.root

    @staticmethod
    def _project_key(project_id: str) -> str:
        try:
            value = require_string(project_id, field="project_id", max_bytes=64)
        except ValueError as exc:
            raise CandidateError(str(exc)) from exc
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _format_directory(
        self, project_id: str, object_format: str, *, create: bool
    ) -> Path:
        if object_format not in GIT_OBJECT_FORMATS:
            raise CandidateError("candidate object format is invalid")
        directory = (
            self.root / "projects" / self._project_key(project_id) / object_format
        )
        try:
            if create:
                self.ensure_root()
                directory.mkdir(mode=0o700, parents=True, exist_ok=True)
                os.chmod(directory.parent, 0o700)
                os.chmod(directory, 0o700)
            elif not directory.is_dir():
                raise CandidateError("candidate repository is unavailable")
        except CandidateError:
            raise
        except OSError as exc:
            raise CandidateError("candidate repository is unavailable") from exc
        return directory

    @staticmethod
    def _sweep_abandoned_operations(directory: Path) -> None:
        try:
            for path in directory.iterdir():
                if not path.name.startswith("operation-"):
                    continue
                if stat.S_ISDIR(path.lstat().st_mode):
                    shutil.rmtree(path)
                else:
                    path.unlink()
        except OSError as exc:
            raise CandidateError(
                "candidate operation cleanup could not be completed"
            ) from exc

    @staticmethod
    def _source_repository(path: Path) -> Path:
        candidate = Path(path).expanduser().absolute()
        try:
            info = candidate.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise CandidateError("candidate source repository must be a directory")
            resolved = candidate.resolve(strict=True)
        except CandidateError:
            raise
        except OSError as exc:
            raise CandidateError("candidate source repository is unavailable") from exc
        if resolved == Path(resolved.anchor):
            raise CandidateError(
                "candidate source repository must not be a filesystem root"
            )
        return resolved

    def _environment(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        root = self.ensure_root()
        environment = {
            "GIT_ALLOW_PROTOCOL": "file",
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_NO_LAZY_FETCH": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "HOME": str(root / "home"),
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": os.defpath,
            "TEMP": str(root / "tmp"),
            "TMP": str(root / "tmp"),
            "TMPDIR": str(root / "tmp"),
            "XDG_CONFIG_HOME": str(root / "home"),
        }
        if extra:
            environment.update(extra)
        return environment

    def _new_operation(self) -> _Operation:
        return _Operation(time.monotonic() + self.timeout)

    @staticmethod
    def _remaining(operation: _Operation) -> float:
        remaining = operation.deadline - time.monotonic()
        if remaining <= 0:
            raise CandidateError("candidate operation timed out")
        return remaining

    @staticmethod
    def _stop(process: subprocess.Popen[bytes]) -> None:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=PROCESS_STOP_SECONDS)
        except subprocess.TimeoutExpired:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            with suppress(subprocess.TimeoutExpired):
                process.wait(timeout=PROCESS_STOP_SECONDS)

    def _command(self, arguments: Sequence[str]) -> list[str]:
        return [
            str(self.binary),
            "-c",
            f"core.hooksPath={os.devnull}",
            "-c",
            "core.fsmonitor=false",
            "-c",
            f"core.attributesFile={os.devnull}",
            "-c",
            "credential.helper=",
            "-c",
            "protocol.allow=never",
            "-c",
            "submodule.recurse=false",
            "-c",
            "maintenance.auto=false",
            "-c",
            "gc.auto=0",
            *arguments,
        ]

    def _run(
        self,
        arguments: Sequence[str],
        *,
        label: str,
        operation: _Operation,
        output_limit: int = MAX_STDOUT_BYTES,
        input_bytes: bytes | None = None,
        extra_environment: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        allowed_returncodes: frozenset[int] = frozenset({0}),
    ) -> bytes:
        operation.commands += 1
        if operation.commands > MAX_COMMANDS:
            raise CandidateError("candidate operation exceeds the command limit")
        if input_bytes is not None and len(input_bytes) > MAX_COMMAND_INPUT_BYTES:
            raise CandidateError("Git command exceeds the input limit")
        command = self._command(arguments)
        try:
            process = subprocess.Popen(  # noqa: S603 - configured absolute Git
                command,
                cwd=cwd,
                stdin=subprocess.DEVNULL if input_bytes is None else subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=self._environment(extra_environment),
                start_new_session=True,
            )
            try:
                stdout, stderr = process.communicate(
                    input_bytes, timeout=self._remaining(operation)
                )
            except subprocess.TimeoutExpired:
                self._stop(process)
                raise CandidateError("candidate operation timed out") from None
        except CandidateError:
            raise
        except OSError:
            raise CandidateError("cannot execute Git CLI") from None
        if len(stdout) > output_limit or len(stderr) > MAX_STDERR_BYTES:
            raise CandidateError("Git command exceeds output limit")
        if process.returncode not in allowed_returncodes:
            detail = stderr.decode("utf-8", "replace").strip()
            suffix = f": {detail[:4096]}" if detail else ""
            raise CandidateError(f"Git cannot {label}{suffix}")
        return stdout

    @contextmanager
    def _lock(
        self,
        project_id: str,
        object_format: str,
        operation: _Operation,
        *,
        create: bool,
    ) -> Iterator[Path]:
        directory = self._format_directory(project_id, object_format, create=create)
        try:
            descriptor = os.open(directory / ".lock", os.O_RDWR | os.O_CREAT, 0o600)
        except OSError as exc:
            raise CandidateError("candidate store lock is unavailable") from exc
        try:
            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    time.sleep(min(LOCK_POLL_SECONDS, self._remaining(operation)))
            yield directory
        finally:
            with suppress(OSError):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @staticmethod
    def _object_id(raw: bytes, *, label: str, length: int) -> str:
        try:
            value = raw.removesuffix(b"\n").decode("ascii", "strict")
        except UnicodeDecodeError:
            raise CandidateError(f"Git returned an invalid {label}") from None
        if OBJECT_ID_RE.fullmatch(value) is None or len(value) != length:
            raise CandidateError(f"Git returned an invalid {label}")
        return value

    def _source_identity(
        self, source: Path, revision: str, operation: _Operation
    ) -> tuple[str, str]:
        raw_format = self._run(
            ["-C", str(source), "rev-parse", "--show-object-format"],
            label="read the source object format",
            operation=operation,
            output_limit=32,
        )
        try:
            object_format = raw_format.removesuffix(b"\n").decode("ascii")
        except UnicodeDecodeError:
            raise CandidateError("Git returned an invalid object format") from None
        length = GIT_OBJECT_FORMATS.get(object_format)
        if length is None:
            raise CandidateError("candidate source object format is unsupported")
        base = self._object_id(
            self._run(
                [
                    "-C",
                    str(source),
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    f"{revision}^{{commit}}",
                ],
                label="resolve the candidate base",
                operation=operation,
                output_limit=256,
            ),
            label="candidate base revision",
            length=length,
        )
        if base != revision:
            raise CandidateError("candidate result source revision is not immutable")
        return object_format, base

    def _repository(
        self,
        directory: Path,
        object_format: str,
        operation: _Operation,
        *,
        create: bool,
    ) -> Path:
        repository = directory / "repository.git"
        if not repository.exists():
            if not create:
                raise CandidateError("candidate repository is unavailable")
            self._run(
                [
                    "init",
                    "--quiet",
                    "--bare",
                    f"--object-format={object_format}",
                    str(repository),
                ],
                label="initialize the candidate repository",
                operation=operation,
                output_limit=4096,
            )
        if not repository.is_dir():
            raise CandidateError("candidate repository is unavailable")
        bare = self._run(
            [f"--git-dir={repository}", "rev-parse", "--is-bare-repository"],
            label="validate the candidate repository",
            operation=operation,
            output_limit=32,
        )
        raw_format = self._run(
            [f"--git-dir={repository}", "rev-parse", "--show-object-format"],
            label="validate the candidate repository format",
            operation=operation,
            output_limit=32,
        )
        if bare != b"true\n" or raw_format != object_format.encode() + b"\n":
            raise CandidateError("candidate repository format does not match")
        return repository

    def _read_ref(
        self,
        repository: Path,
        ref: str,
        *,
        object_length: int,
        operation: _Operation,
    ) -> str | None:
        raw = self._run(
            [f"--git-dir={repository}", "show-ref", "--verify", "--hash", ref],
            label="read a candidate ref",
            operation=operation,
            output_limit=256,
            allowed_returncodes=frozenset({0, 1, 128}),
        )
        if not raw:
            return None
        return self._object_id(raw, label="candidate ref", length=object_length)

    def _publish_ref(
        self,
        repository: Path,
        ref: str,
        value: str,
        *,
        object_length: int,
        operation: _Operation,
    ) -> None:
        current = self._read_ref(
            repository, ref, object_length=object_length, operation=operation
        )
        if current == value:
            return
        if current is not None:
            raise CandidateError("candidate ref does not match recorded identity")
        self._run(
            [
                f"--git-dir={repository}",
                "update-ref",
                "--no-deref",
                ref,
                value,
                "0" * object_length,
            ],
            label="publish a candidate ref",
            operation=operation,
            output_limit=1024,
        )

    def _ensure_base(
        self,
        repository: Path,
        source: Path | None,
        base: str,
        *,
        object_length: int,
        operation: _Operation,
    ) -> None:
        ref = f"refs/mgb/bases/{base}"
        current = self._read_ref(
            repository, ref, object_length=object_length, operation=operation
        )
        if current not in {None, base}:
            raise CandidateError("candidate base ref does not match")
        available = self._stored_commit(
            repository,
            base,
            object_length=object_length,
            operation=operation,
        )
        if not available:
            if source is None:
                raise CandidateError("candidate base object is unavailable")
            self._run(
                [
                    "-c",
                    "protocol.file.allow=always",
                    f"--git-dir={repository}",
                    "fetch",
                    "--quiet",
                    "--no-tags",
                    "--no-recurse-submodules",
                    "--no-write-fetch-head",
                    "--depth=1",
                    str(source),
                    base,
                ],
                label="copy the candidate base commit",
                operation=operation,
                output_limit=4096,
            )
            copied = self._object_id(
                self._run(
                    [
                        f"--git-dir={repository}",
                        "rev-parse",
                        "--verify",
                        "--end-of-options",
                        f"{base}^{{commit}}",
                    ],
                    label="verify the copied candidate base",
                    operation=operation,
                    output_limit=256,
                ),
                label="copied candidate base",
                length=object_length,
            )
            if copied != base:
                raise CandidateError("copied candidate base does not match")
        if current is None:
            self._publish_ref(
                repository,
                ref,
                base,
                object_length=object_length,
                operation=operation,
            )

    def _stored_commit(
        self,
        repository: Path,
        revision: str,
        *,
        object_length: int,
        operation: _Operation,
    ) -> bool:
        raw = self._run(
            [
                f"--git-dir={repository}",
                "rev-parse",
                "--verify",
                "--end-of-options",
                f"{revision}^{{commit}}",
            ],
            label="find the candidate base",
            operation=operation,
            output_limit=256,
            allowed_returncodes=frozenset({0, 1, 128}),
        )
        if not raw:
            return False
        found = self._object_id(
            raw,
            label="candidate base revision",
            length=object_length,
        )
        if found != revision:
            raise CandidateError("candidate base object does not match")
        return True

    @staticmethod
    def _tree_path(raw: bytes) -> str:
        try:
            name = raw.decode("utf-8", "strict")
            return validate_repo_path(name)
        except (UnicodeDecodeError, UnsafePathError):
            raise CandidateError("Git tree contains an unsafe path") from None

    @classmethod
    def _parse_tree(cls, raw: bytes, *, object_length: int) -> dict[str, _TreeEntry]:
        if raw and not raw.endswith(b"\0"):
            raise CandidateError("Git returned an invalid candidate base tree")
        records = raw[:-1].split(b"\0") if raw else []
        if len(records) > MAX_BASE_TREE_ENTRIES:
            raise CandidateError("candidate base tree contains too many entries")
        expected_kinds = {
            "040000": "tree",
            "100644": "blob",
            "100755": "blob",
            "120000": "blob",
            "160000": "commit",
        }
        entries: dict[str, _TreeEntry] = {}
        import_bytes = 0
        for record in records:
            try:
                header, raw_path = record.split(b"\t", 1)
                mode, kind, object_id, raw_size = header.decode("ascii").split()
            except (UnicodeDecodeError, ValueError):
                raise CandidateError(
                    "Git returned an invalid candidate base tree"
                ) from None
            if (
                expected_kinds.get(mode) != kind
                or OBJECT_ID_RE.fullmatch(object_id) is None
                or len(object_id) != object_length
            ):
                raise CandidateError("Git returned an invalid candidate base tree")
            if kind == "blob":
                if not raw_size.isdecimal() or len(raw_size) > 12:
                    raise CandidateError("Git returned an invalid candidate base tree")
                size = int(raw_size)
                import_bytes += size
                if import_bytes > MAX_BASE_IMPORT_BYTES:
                    raise CandidateError(
                        "candidate base tree exceeds the import byte limit"
                    )
            elif raw_size == "-":
                size = 0
            else:
                raise CandidateError("Git returned an invalid candidate base tree")
            path = cls._tree_path(raw_path)
            if path in entries:
                raise CandidateError("Git returned duplicate candidate base paths")
            entries[path] = _TreeEntry(path, mode, kind, object_id, size)
        for path, entry in entries.items():
            for parent in PurePosixPath(path).parents:
                if parent == PurePosixPath("."):
                    continue
                ancestor = entries.get(parent.as_posix())
                if ancestor is None or ancestor.mode != "040000":
                    raise CandidateError("Git returned an invalid candidate base tree")
            if entry.mode == "120000" and entry.size > MAX_PATH_BYTES:
                raise CandidateError("candidate base symlink is oversized")
        return entries

    def _base_tree(
        self,
        repository: Path,
        base: str,
        *,
        object_length: int,
        operation: _Operation,
        bare: bool = False,
    ) -> dict[str, _TreeEntry]:
        location = [f"--git-dir={repository}"] if bare else ["-C", str(repository)]
        raw = self._run(
            [
                *location,
                "ls-tree",
                "-r",
                "-t",
                "-l",
                "-z",
                "--full-tree",
                base,
            ],
            label="list the candidate base tree",
            operation=operation,
            output_limit=MAX_BASE_TREE_BYTES,
        )
        return self._parse_tree(raw, object_length=object_length)

    @staticmethod
    def _git_object_id(object_format: str, body: bytes) -> str:
        digest = hashlib.new(object_format)
        digest.update(b"blob " + str(len(body)).encode("ascii") + b"\0")
        digest.update(body)
        return digest.hexdigest()

    def _read_base_objects(
        self,
        repository: Path,
        entries: Sequence[_TreeEntry],
        *,
        object_format: str,
        operation: _Operation,
    ) -> dict[str, _BaseObject]:
        unique: dict[str, _TreeEntry] = {}
        for entry in entries:
            current = unique.get(entry.object_id)
            if current is None or entry.mode == "120000":
                unique[entry.object_id] = entry
        ordered = [unique[key] for key in sorted(unique)]
        if not ordered:
            return {}
        request_bytes = sum(len(entry.object_id) + 1 for entry in ordered)
        if request_bytes > MAX_COMMAND_INPUT_BYTES:
            raise CandidateError("Git command exceeds the input limit")
        operation.commands += 1
        if operation.commands > MAX_COMMANDS:
            raise CandidateError("candidate operation exceeds the command limit")
        command = self._command([f"--git-dir={repository}", "cat-file", "--batch"])
        with tempfile.TemporaryFile() as error_file:
            try:
                process = subprocess.Popen(  # noqa: S603 - configured absolute Git
                    command,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=error_file,
                    env=self._environment(),
                    start_new_session=True,
                )
            except OSError:
                raise CandidateError("cannot execute Git CLI") from None
            assert process.stdout is not None
            reader = _BatchReader(process.stdout, operation.deadline)
            objects: dict[str, _BaseObject] = {}
            try:
                assert process.stdin is not None
                for entry in ordered:
                    try:
                        process.stdin.write(entry.object_id.encode("ascii") + b"\n")
                        process.stdin.flush()
                    except (BrokenPipeError, OSError) as exc:
                        raise CandidateError(
                            "Git object batch closed unexpectedly"
                        ) from exc
                    header = reader.line(256).removesuffix(b"\n")
                    try:
                        raw_id, raw_kind, raw_size = header.split(b" ")
                        object_id = raw_id.decode("ascii")
                        kind = raw_kind.decode("ascii")
                        size_text = raw_size.decode("ascii")
                    except (UnicodeDecodeError, ValueError):
                        raise CandidateError(
                            "Git returned a malformed object batch"
                        ) from None
                    if (
                        object_id != entry.object_id
                        or kind != "blob"
                        or not size_text.isdecimal()
                        or len(size_text) > 12
                        or int(size_text) != entry.size
                    ):
                        raise CandidateError("Git returned a malformed object batch")
                    git_digest = hashlib.new(object_format)
                    git_digest.update(
                        b"blob " + str(entry.size).encode("ascii") + b"\0"
                    )
                    content_digest = hashlib.sha256()
                    retained = bytearray() if entry.mode == "120000" else None
                    for chunk in reader.chunks(entry.size):
                        git_digest.update(chunk)
                        content_digest.update(chunk)
                        if retained is not None:
                            retained.extend(chunk)
                    if reader.exact(1) != b"\n" or git_digest.hexdigest() != object_id:
                        raise CandidateError(
                            "Git returned an invalid candidate base object"
                        )
                    objects[object_id] = _BaseObject(
                        entry.size,
                        content_digest.hexdigest(),
                        None if retained is None else bytes(retained),
                    )
                process.stdin.close()
                status = process.wait(timeout=self._remaining(operation))
                error_file.seek(0)
                stderr = error_file.read(MAX_STDERR_BYTES + 1)
                if len(stderr) > MAX_STDERR_BYTES:
                    raise CandidateError("Git command exceeds output limit")
                if status != 0:
                    detail = stderr.decode("utf-8", "replace").strip()
                    suffix = f": {detail[:4096]}" if detail else ""
                    raise CandidateError(
                        f"Git cannot read candidate base objects{suffix}"
                    )
                return objects
            except subprocess.TimeoutExpired:
                self._stop(process)
                raise CandidateError("candidate operation timed out") from None
            except BaseException:
                self._stop(process)
                raise
            finally:
                reader.close()
                for stream in (process.stdin, process.stdout):
                    if stream is not None and not stream.closed:
                        stream.close()

    @staticmethod
    def _validate_result_blobs(
        result: ResultManifest, blobs: Mapping[str, bytes]
    ) -> None:
        required = {
            change.after.sha256
            for change in result.changes
            if change.needs_blob and change.after is not None
        }
        if set(blobs) != required:
            raise CandidateError("candidate result blob set does not match")
        for change in result.changes:
            if not change.needs_blob or change.after is None:
                continue
            digest = change.after.sha256
            assert digest is not None and change.after.size is not None
            content = blobs[digest]
            if (
                not isinstance(content, bytes)
                or len(content) != change.after.size
                or hashlib.sha256(content).hexdigest() != digest
            ):
                raise CandidateError("candidate result blob does not match")

    def _validate_base(
        self,
        result: ResultManifest,
        entries: Mapping[str, _TreeEntry],
        objects: Mapping[str, _BaseObject],
    ) -> dict[str, str | None]:
        changes = {change.path: change for change in result.changes}
        leaf_paths = sorted(
            path for path, entry in entries.items() if entry.mode != "040000"
        )
        base_objects: dict[str, str | None] = {}
        for change in result.changes:
            path = PurePosixPath(change.path)
            for parent in path.parents:
                if parent == PurePosixPath("."):
                    continue
                ancestor = entries.get(parent.as_posix())
                if ancestor is None or ancestor.mode == "040000":
                    continue
                if ancestor.mode == "160000":
                    raise CandidateError("candidate change is beneath a Git link")
                ancestor_change = changes.get(ancestor.path)
                if ancestor_change is None or ancestor_change.after is not None:
                    raise CandidateError(f"candidate base changed: {change.path}")
            entry = entries.get(change.path)
            if entry is None:
                if change.before is not None:
                    raise CandidateError(f"candidate base changed: {change.path}")
                base_objects[change.path] = None
                continue
            if entry.mode == "160000":
                raise CandidateError("candidate change targets a Git link")
            if entry.mode == "040000":
                if change.before is not None or change.after is None:
                    raise CandidateError(f"candidate base changed: {change.path}")
                prefix = change.path + "/"
                cursor = bisect_left(leaf_paths, prefix)
                descendants: list[str] = []
                while cursor < len(leaf_paths) and leaf_paths[cursor].startswith(
                    prefix
                ):
                    descendants.append(leaf_paths[cursor])
                    cursor += 1
                if not descendants or any(
                    entries[path].mode == "160000"
                    or path not in changes
                    or changes[path].after is not None
                    for path in descendants
                ):
                    raise CandidateError(
                        "candidate directory replacement is incomplete"
                    )
                base_objects[change.path] = None
                continue
            base_object = objects.get(entry.object_id)
            if base_object is None:
                raise CandidateError("candidate base object is unavailable")
            if entry.mode == "120000":
                body = base_object.body
                if body is None:
                    raise CandidateError("candidate base symlink is unavailable")
                try:
                    target = body.decode("utf-8", "strict")
                    validate_symlink_target(change.path, target)
                except (UnicodeDecodeError, UnsafeSymlinkError):
                    raise CandidateError("candidate base symlink is unsafe") from None
                state = FileState(kind="symlink", target=target)
            else:
                state = FileState(
                    kind="file",
                    mode=0o755 if entry.mode == "100755" else 0o644,
                    size=base_object.size,
                    sha256=base_object.sha256,
                )
            if state != change.before:
                raise CandidateError(f"candidate base changed: {change.path}")
            base_objects[change.path] = entry.object_id
        links: dict[PurePosixPath, str] = {}
        for entry in entries.values():
            if entry.mode != "120000":
                continue
            base_object = objects.get(entry.object_id)
            if base_object is None or base_object.body is None:
                raise CandidateError("candidate base symlink is unavailable")
            try:
                target = base_object.body.decode("utf-8", "strict")
                validate_symlink_target(entry.path, target)
            except (UnicodeDecodeError, UnsafeSymlinkError):
                raise CandidateError("candidate base symlink is unsafe") from None
            links[PurePosixPath(entry.path)] = target
        for change in result.changes:
            key = PurePosixPath(change.path)
            if change.after is not None and change.after.kind == "symlink":
                assert change.after.target is not None
                links[key] = change.after.target
            else:
                links.pop(key, None)
        try:
            validate_symlink_graph(links)
        except UnsafeSymlinkError as exc:
            raise CandidateError("candidate contains an unsafe symlink") from exc
        return base_objects

    @staticmethod
    def _commit_message(base: str, result_digest: str) -> bytes:
        return (
            "Magic Glass Box candidate\n\n"
            f"Base: {base}\n"
            f"Result: sha256:{result_digest}\n"
        ).encode("ascii")

    @classmethod
    def _expected_commit(
        cls, tree: str, base: str, result_content_sha256: str
    ) -> bytes:
        identity = "Magic Glass Box <candidate@magic-glass-box.invalid>"
        return (
            f"tree {tree}\n"
            f"parent {base}\n"
            f"author {identity} {FIXED_GIT_TIMESTAMP}\n"
            f"committer {identity} {FIXED_GIT_TIMESTAMP}\n\n"
        ).encode("ascii") + cls._commit_message(base, result_content_sha256)

    def _store_post_images(
        self,
        repository: Path,
        result: ResultManifest,
        blobs: Mapping[str, bytes],
        operation_directory: Path,
        *,
        object_format: str,
        object_length: int,
        operation: _Operation,
    ) -> dict[str, str]:
        staged = operation_directory / "post-images"
        staged.mkdir(mode=0o700)
        expected: dict[str, bytes] = {}
        path_objects: dict[str, str] = {}
        total = 0
        for change in result.changes:
            after = change.after
            if after is None:
                continue
            if after.kind == "symlink":
                assert after.target is not None
                body = after.target.encode("utf-8")
            elif change.needs_blob:
                assert after.sha256 is not None
                body = blobs[after.sha256]
            else:
                continue
            object_id = self._git_object_id(object_format, body)
            path_objects[change.path] = object_id
            if object_id in expected:
                continue
            total += len(body)
            if total > MAX_POST_IMAGE_BYTES:
                raise CandidateError("candidate post-images exceed the byte limit")
            try:
                (staged / object_id).write_bytes(body)
            except OSError as exc:
                raise CandidateError("candidate post-image staging failed") from exc
            expected[object_id] = body
        if not expected:
            return path_objects
        request = b"".join(object_id.encode() + b"\n" for object_id in expected)
        raw = self._run(
            [
                f"--git-dir={repository}",
                "hash-object",
                "-w",
                "--no-filters",
                "--stdin-paths",
            ],
            label="store candidate post-images",
            operation=operation,
            output_limit=len(expected) * (object_length + 1),
            input_bytes=request,
            cwd=staged,
        )
        returned = raw.splitlines()
        if returned != [value.encode() for value in expected]:
            raise CandidateError("Git returned invalid candidate blob IDs")
        return path_objects

    @staticmethod
    def _index_records(
        result: ResultManifest,
        base_objects: Mapping[str, str | None],
        post_objects: Mapping[str, str],
        *,
        object_length: int,
    ) -> bytes:
        records: list[bytes] = []
        deletions = sorted(
            (change for change in result.changes if change.after is None),
            key=lambda change: (-len(PurePosixPath(change.path).parts), change.path),
        )
        for change in deletions:
            records.append(
                b"0 " + b"0" * object_length + b"\t" + change.path.encode() + b"\0"
            )
        for change in result.changes:
            after = change.after
            if after is None:
                continue
            if after.kind == "symlink":
                mode = "120000"
                object_id = post_objects[change.path]
            else:
                assert after.mode is not None
                mode = "100755" if after.mode == 0o755 else "100644"
                object_id = (
                    post_objects[change.path]
                    if change.needs_blob
                    else base_objects[change.path]
                )
                if object_id is None:
                    raise CandidateError("candidate base blob is unavailable")
            records.append(f"{mode} {object_id}\t{change.path}".encode() + b"\0")
        raw = b"".join(records)
        if len(raw) > MAX_COMMAND_INPUT_BYTES:
            raise CandidateError("candidate index exceeds the input limit")
        return raw

    def _verify(
        self,
        repository: Path,
        candidate: MaterializedCandidate,
        *,
        result_content_sha256: str,
        operation: _Operation,
    ) -> MaterializedCandidate:
        object_length = GIT_OBJECT_FORMATS[candidate.object_format]
        ref = f"refs/mgb/candidates/{candidate.head_revision}"
        if (
            self._read_ref(
                repository,
                ref,
                object_length=object_length,
                operation=operation,
            )
            != candidate.head_revision
        ):
            raise CandidateError("candidate ref is missing or does not match")
        raw_commit = self._run(
            [
                f"--git-dir={repository}",
                "cat-file",
                "commit",
                candidate.head_revision,
            ],
            label="read the candidate commit",
            operation=operation,
            output_limit=4096,
        )
        if raw_commit != self._expected_commit(
            candidate.tree_revision,
            candidate.base_revision,
            result_content_sha256,
        ):
            raise CandidateError("candidate commit does not match recorded identity")
        self._run(
            [
                f"--git-dir={repository}",
                "cat-file",
                "-e",
                f"{candidate.tree_revision}^{{tree}}",
            ],
            label="verify the candidate tree",
            operation=operation,
            output_limit=1024,
        )
        self._run(
            [
                f"--git-dir={repository}",
                "cat-file",
                "-e",
                f"{candidate.base_revision}^{{commit}}",
            ],
            label="verify the candidate base",
            operation=operation,
            output_limit=1024,
        )
        return candidate

    def materialize(
        self,
        project_id: str,
        repository: Path,
        result: ResultManifest,
        blobs: Mapping[str, bytes],
    ) -> MaterializedCandidate:
        return self._materialize(project_id, repository, result, blobs, expected=None)

    def _materialize(
        self,
        project_id: str,
        repository: Path,
        result: ResultManifest,
        blobs: Mapping[str, bytes],
        *,
        expected: CandidateManifest | None,
        requested_base: str | None = None,
        requested_object_format: str | None = None,
    ) -> MaterializedCandidate:
        if not isinstance(result, ResultManifest):
            raise CandidateError("candidate result must be a validated manifest")
        if not result.changes:
            raise CandidateError("candidate result contains no changes")
        self._validate_result_blobs(result, blobs)
        operation = self._new_operation()
        source: Path | None = None
        if (requested_base is None) != (requested_object_format is None):
            raise CandidateError("candidate requested revision is incomplete")
        if requested_base is None and expected is None:
            source = self._source_repository(repository)
            object_format, base = self._source_identity(
                source, result.source_revision, operation
            )
        elif requested_base is not None and requested_object_format is not None:
            object_format = requested_object_format
            base = requested_base
        else:
            assert expected is not None
            object_format = expected.object_format
            base = expected.base_revision
        object_length = GIT_OBJECT_FORMATS.get(object_format)
        if (
            object_length is None
            or not isinstance(base, str)
            or OBJECT_ID_RE.fullmatch(base) is None
            or len(base) != object_length
        ):
            raise CandidateError("candidate requested base revision is invalid")
        if expected is not None and (
            expected.project_id != project_id
            or expected.attempt_id != result.attempt_id
            or expected.result_content_sha256 != result.digest
        ):
            raise CandidateError("candidate recovery identity does not match")
        with self._lock(project_id, object_format, operation, create=True) as directory:
            self._sweep_abandoned_operations(directory)
            store = self._repository(directory, object_format, operation, create=True)
            if source is None and not self._stored_commit(
                store,
                base,
                object_length=object_length,
                operation=operation,
            ):
                source = self._source_repository(repository)
                source_format, source_base = self._source_identity(
                    source, base, operation
                )
                if source_format != object_format or source_base != base:
                    raise CandidateError("candidate recovery identity does not match")
            self._ensure_base(
                store,
                source,
                base,
                object_length=object_length,
                operation=operation,
            )
            entries = self._base_tree(
                store,
                base,
                object_length=object_length,
                operation=operation,
                bare=True,
            )
            changed_paths = {change.path for change in result.changes}
            needed = {
                entry.object_id: entry
                for entry in entries.values()
                if entry.kind == "blob"
                and (entry.mode == "120000" or entry.path in changed_paths)
            }
            base_object_states = self._read_base_objects(
                store,
                list(needed.values()),
                object_format=object_format,
                operation=operation,
            )
            base_objects = self._validate_base(result, entries, base_object_states)
            try:
                with tempfile.TemporaryDirectory(
                    prefix="operation-", dir=directory
                ) as raw_temporary:
                    temporary = Path(raw_temporary)
                    post_objects = self._store_post_images(
                        store,
                        result,
                        blobs,
                        temporary,
                        object_format=object_format,
                        object_length=object_length,
                        operation=operation,
                    )
                    index = temporary / "candidate.index"
                    index_environment = {"GIT_INDEX_FILE": str(index)}
                    self._run(
                        [f"--git-dir={store}", "read-tree", base],
                        label="seed the candidate index",
                        operation=operation,
                        output_limit=1024,
                        extra_environment=index_environment,
                    )
                    records = self._index_records(
                        result,
                        base_objects,
                        post_objects,
                        object_length=object_length,
                    )
                    self._run(
                        [f"--git-dir={store}", "update-index", "-z", "--index-info"],
                        label="update the candidate index",
                        operation=operation,
                        output_limit=1024,
                        input_bytes=records,
                        extra_environment=index_environment,
                    )
                    tree = self._object_id(
                        self._run(
                            [f"--git-dir={store}", "write-tree"],
                            label="write the candidate tree",
                            operation=operation,
                            output_limit=256,
                            extra_environment=index_environment,
                        ),
                        label="candidate tree ID",
                        length=object_length,
                    )
            except OSError as exc:
                raise CandidateError("candidate staging area is unavailable") from exc
            identity_environment = {
                "GIT_AUTHOR_DATE": FIXED_GIT_DATE,
                "GIT_AUTHOR_EMAIL": "candidate@magic-glass-box.invalid",
                "GIT_AUTHOR_NAME": "Magic Glass Box",
                "GIT_COMMITTER_DATE": FIXED_GIT_DATE,
                "GIT_COMMITTER_EMAIL": "candidate@magic-glass-box.invalid",
                "GIT_COMMITTER_NAME": "Magic Glass Box",
            }
            head = self._object_id(
                self._run(
                    [f"--git-dir={store}", "commit-tree", tree, "-p", base],
                    label="write the candidate commit",
                    operation=operation,
                    output_limit=256,
                    input_bytes=self._commit_message(base, result.digest),
                    extra_environment=identity_environment,
                ),
                label="candidate commit ID",
                length=object_length,
            )
            materialized = MaterializedCandidate(base, head, tree, object_format)
            if expected is not None and materialized != MaterializedCandidate(
                expected.base_revision,
                expected.head_revision,
                expected.tree_revision,
                expected.object_format,
            ):
                raise CandidateError(
                    "candidate materialization does not match recorded identity"
                )
            self._publish_ref(
                store,
                f"refs/mgb/candidates/{head}",
                head,
                object_length=object_length,
                operation=operation,
            )
            return self._verify(
                store,
                materialized,
                result_content_sha256=result.digest,
                operation=operation,
            )

    def recover(
        self,
        manifest: CandidateManifest,
        repository: Path,
        result: ResultManifest,
        blobs: Mapping[str, bytes],
    ) -> MaterializedCandidate:
        if not isinstance(manifest, CandidateManifest):
            raise CandidateError("candidate recovery manifest is invalid")
        if result.source_revision != manifest.base_revision:
            raise CandidateError("candidate recovery identity does not match")
        return self._materialize(
            manifest.project_id,
            repository,
            result,
            blobs,
            expected=manifest,
        )

    def refold(
        self,
        origin: CandidateManifest,
        repository: Path,
        result: ResultManifest,
        blobs: Mapping[str, bytes],
        *,
        base_revision: str,
    ) -> MaterializedCandidate:
        """Replay an original result against one exact, newer base revision."""

        if not isinstance(origin, CandidateManifest):
            raise CandidateError("candidate origin manifest is invalid")
        if (
            origin.attempt_id != result.attempt_id
            or origin.result_content_sha256 != result.digest
            or origin.base_revision != result.source_revision
        ):
            raise CandidateError("candidate refold origin does not match")
        return self._materialize(
            origin.project_id,
            repository,
            result,
            blobs,
            expected=None,
            requested_base=base_revision,
            requested_object_format=origin.object_format,
        )

    def recover_revision(
        self,
        manifest: CandidateManifest,
        repository: Path,
        result: ResultManifest,
        blobs: Mapping[str, bytes],
    ) -> MaterializedCandidate:
        """Recover an exact original or derived revision from retained inputs."""

        if not isinstance(manifest, CandidateManifest):
            raise CandidateError("candidate recovery manifest is invalid")
        return self._materialize(
            manifest.project_id,
            repository,
            result,
            blobs,
            expected=manifest,
        )

    def repository_path(self, project_id: str, object_format: str) -> Path:
        """Return the validated private repository backing one project."""

        operation = self._new_operation()
        with self._lock(
            project_id,
            object_format,
            operation,
            create=False,
        ) as directory:
            return self._repository(directory, object_format, operation, create=False)

    def inspect(self, manifest: CandidateManifest) -> MaterializedCandidate:
        if not isinstance(manifest, CandidateManifest):
            raise CandidateError("candidate manifest is invalid")
        operation = self._new_operation()
        with self._lock(
            manifest.project_id,
            manifest.object_format,
            operation,
            create=False,
        ) as directory:
            repository = self._repository(
                directory, manifest.object_format, operation, create=False
            )
            return self._verify(
                repository,
                MaterializedCandidate(
                    manifest.base_revision,
                    manifest.head_revision,
                    manifest.tree_revision,
                    manifest.object_format,
                ),
                result_content_sha256=manifest.result_content_sha256,
                operation=operation,
            )

    def snapshot(
        self, manifest: CandidateManifest, destination: Path
    ) -> SnapshotResult:
        """Materialize one verified candidate tree into an empty workspace."""

        if not isinstance(manifest, CandidateManifest):
            raise CandidateError("candidate manifest is invalid")
        operation = self._new_operation()
        with self._lock(
            manifest.project_id,
            manifest.object_format,
            operation,
            create=False,
        ) as directory:
            repository = self._repository(
                directory, manifest.object_format, operation, create=False
            )
            self._verify(
                repository,
                MaterializedCandidate(
                    manifest.base_revision,
                    manifest.head_revision,
                    manifest.tree_revision,
                    manifest.object_format,
                ),
                result_content_sha256=manifest.result_content_sha256,
                operation=operation,
            )
            try:
                snapshot = GitSnapshotter(
                    binary=self.binary,
                    timeout=max(0.001, operation.deadline - time.monotonic()),
                ).snapshot(repository, manifest.head_revision, destination)
            except WorkspaceError as exc:
                raise CandidateError(
                    "candidate validation workspace could not be materialized"
                ) from exc
        if snapshot.revision != manifest.head_revision:
            raise CandidateError("candidate validation workspace identity changed")
        return snapshot


def default_candidate_root(database_path: Path) -> Path:
    return Path(database_path).expanduser().absolute().parent / "candidates"
