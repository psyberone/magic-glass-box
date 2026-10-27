from __future__ import annotations

import base64
import binascii
import errno
import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .domain import canonical_json
from .path_safety import (
    UnsafePathError,
    UnsafeSymlinkError,
    portable_path_identity,
    reserved_git_component,
    validate_repo_path,
    validate_symlink_graph,
    validate_symlink_target,
)
from .workspace import MAX_SNAPSHOT_BYTES

RESULT_SCHEMA_VERSION = 1
BASELINE_SCHEMA_VERSION = 3
ATTEMPT_ID_RE = re.compile(r"^att_[0-9a-f]{32}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
REVISION_RE = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
MAX_RESULT_PATHS = 100_000
MAX_INSPECTED_PATHS = 200_000
MAX_SCAN_BATCH_PATHS = 1024
MAX_CHANGED_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
READ_CHUNK_BYTES = 1024 * 1024
MAX_PINNED_IGNORE_BYTES = 1024 * 1024
MAX_IGNORE_BATCH_BYTES = 256 * 1024
IGNORE_COMMAND_TIMEOUT = 10.0


class ResultError(RuntimeError):
    """A worker result could not be retained or applied safely."""


class _PinnedIgnoreRules:
    """Ask Git to apply only the baseline's committed ignore files."""

    def __init__(self, git_binary: Path, root: Path, rules: dict[str, bytes]):
        self.git_binary = git_binary
        self._temporary = tempfile.TemporaryDirectory(prefix="ignore-", dir=root)
        temporary = Path(self._temporary.name)
        self.worktree = temporary / "rules"
        self.git_directory = temporary / "repository.git"
        self.environment = {
            "PATH": os.defpath,
            "LC_ALL": "C",
            "LANG": "C",
            "HOME": str(temporary),
            "XDG_CONFIG_HOME": str(temporary),
            "TMPDIR": str(temporary),
            "TEMP": str(temporary),
            "TMP": str(temporary),
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_NO_LAZY_FETCH": "1",
        }
        self.worktree.mkdir(mode=0o700)
        for relative, raw in rules.items():
            destination = self.worktree.joinpath(*PurePosixPath(relative).parts)
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            with os.fdopen(descriptor, "wb") as output:
                output.write(raw)
        try:
            completed = subprocess.run(  # noqa: S603
                [str(git_binary), "init", "--quiet", "--bare", str(self.git_directory)],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=IGNORE_COMMAND_TIMEOUT,
                check=False,
                env=self.environment,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            self.close()
            raise ResultError("cannot initialize pinned result ignore rules") from exc
        if completed.returncode != 0:
            self.close()
            raise ResultError("cannot initialize pinned result ignore rules")

    def close(self) -> None:
        self._temporary.cleanup()

    def ignored(self, paths: Sequence[tuple[str, bool]]) -> set[str]:
        ignored: set[str] = set()
        batch: list[tuple[str, str]] = []
        batch_bytes = 0
        for path, is_directory in paths:
            token = path + ("/" if is_directory else "")
            size = len(token.encode("utf-8")) + 1
            if batch and batch_bytes + size > MAX_IGNORE_BATCH_BYTES:
                ignored.update(self._check_batch(batch))
                batch = []
                batch_bytes = 0
            batch.append((path, token))
            batch_bytes += size
        if batch:
            ignored.update(self._check_batch(batch))
        return ignored

    def _check_batch(self, batch: Sequence[tuple[str, str]]) -> set[str]:
        submitted = {token: path for path, token in batch}
        raw = b"\0".join(token.encode("utf-8") for token in submitted) + b"\0"
        try:
            completed = subprocess.run(  # noqa: S603
                [
                    str(self.git_binary),
                    "-c",
                    "core.excludesFile=/dev/null",
                    f"--git-dir={self.git_directory}",
                    f"--work-tree={self.worktree}",
                    "check-ignore",
                    "--no-index",
                    "-z",
                    "--stdin",
                ],
                input=raw,
                capture_output=True,
                timeout=IGNORE_COMMAND_TIMEOUT,
                check=False,
                env=self.environment,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ResultError("cannot evaluate pinned result ignore rules") from exc
        if completed.returncode not in {0, 1} or completed.stderr:
            raise ResultError("cannot evaluate pinned result ignore rules")
        try:
            tokens = [
                value.decode("utf-8", "strict")
                for value in completed.stdout.split(b"\0")
                if value
            ]
        except UnicodeDecodeError as exc:
            raise ResultError(
                "pinned result ignore rules returned invalid output"
            ) from exc
        if any(token not in submitted for token in tokens):
            raise ResultError("pinned result ignore rules returned invalid output")
        return {submitted[token] for token in tokens}


def _digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe_path(value: object) -> str:
    try:
        return validate_repo_path(value)
    except UnsafePathError as exc:
        if "reserved .git" in str(exc):
            raise ResultError("result contains a reserved .git path") from exc
        raise ResultError("result path must be a safe relative POSIX path") from exc


def _safe_symlink(path: str, target: object) -> str:
    try:
        return validate_symlink_target(path, target)
    except UnsafeSymlinkError as exc:
        raise ResultError(f"result contains an unsafe symlink: {path}") from exc


def _validate_portable_leaf_paths(paths: Sequence[str], *, context: str) -> None:
    entries = sorted(
        (portable_path_identity(path), path.parts)
        for path in (PurePosixPath(value) for value in paths)
    )
    for (previous, previous_parts), (identity, parts) in zip(
        entries, entries[1:], strict=False
    ):
        if identity == previous:
            raise ResultError(f"{context} contain portable aliases")
        shared = 0
        while (
            shared < len(previous)
            and shared < len(identity)
            and previous[shared] == identity[shared]
        ):
            if previous_parts[shared] != parts[shared]:
                raise ResultError(f"{context} contain portable aliases")
            shared += 1
        if shared == len(previous) and len(previous) < len(identity):
            raise ResultError(f"{context} contain portable leaf/descendant aliases")


@dataclass(frozen=True)
class FileState:
    kind: str
    mode: int | None = None
    size: int | None = None
    sha256: str | None = None
    target: str | None = None

    def __post_init__(self) -> None:
        if self.kind == "file":
            if self.mode not in {0o644, 0o755}:
                raise ResultError("result file mode is invalid")
            if (
                isinstance(self.size, bool)
                or not isinstance(self.size, int)
                or self.size < 0
                or not isinstance(self.sha256, str)
                or DIGEST_RE.fullmatch(self.sha256) is None
                or self.target is not None
            ):
                raise ResultError("result file state is invalid")
        elif self.kind == "symlink":
            if (
                self.mode is not None
                or self.size is not None
                or self.sha256 is not None
                or not isinstance(self.target, str)
            ):
                raise ResultError("result symlink state is invalid")
        else:
            raise ResultError("result file kind is invalid")

    @classmethod
    def from_mapping(cls, value: object, *, path: str) -> FileState:
        if not isinstance(value, dict):
            raise ResultError("result file state must be an object")
        kind = value.get("kind")
        if kind == "file" and set(value) == {"kind", "mode", "size", "sha256"}:
            return cls(
                kind=kind,
                mode=value["mode"],
                size=value["size"],
                sha256=value["sha256"],
            )
        if kind == "symlink" and set(value) == {"kind", "target"}:
            return cls(kind=kind, target=_safe_symlink(path, value["target"]))
        raise ResultError("result file state has invalid fields")

    def to_dict(self) -> dict[str, Any]:
        if self.kind == "file":
            return {
                "kind": self.kind,
                "mode": self.mode,
                "sha256": self.sha256,
                "size": self.size,
            }
        return {"kind": self.kind, "target": self.target}


@dataclass(frozen=True)
class ResultChange:
    path: str
    operation: str
    before: FileState | None
    after: FileState | None

    def __post_init__(self) -> None:
        _safe_path(self.path)
        valid = (
            (
                self.operation == "added"
                and self.before is None
                and self.after is not None
            )
            or (
                self.operation == "deleted"
                and self.before is not None
                and self.after is None
            )
            or (
                self.operation == "modified"
                and self.before is not None
                and self.after is not None
                and self.before != self.after
            )
        )
        if not valid:
            raise ResultError("result change is inconsistent")

    @classmethod
    def from_mapping(cls, value: object) -> ResultChange:
        if not isinstance(value, dict) or set(value) != {
            "after",
            "before",
            "operation",
            "path",
        }:
            raise ResultError("result change has invalid fields")
        path = _safe_path(value["path"])
        before = (
            None
            if value["before"] is None
            else FileState.from_mapping(value["before"], path=path)
        )
        after = (
            None
            if value["after"] is None
            else FileState.from_mapping(value["after"], path=path)
        )
        return cls(path, value["operation"], before, after)

    def to_dict(self) -> dict[str, Any]:
        return {
            "after": None if self.after is None else self.after.to_dict(),
            "before": None if self.before is None else self.before.to_dict(),
            "operation": self.operation,
            "path": self.path,
        }

    @property
    def needs_blob(self) -> bool:
        return bool(
            self.after is not None
            and self.after.kind == "file"
            and (
                self.before is None
                or self.before.kind != "file"
                or self.before.sha256 != self.after.sha256
                or self.before.size != self.after.size
            )
        )


@dataclass(frozen=True)
class ResultManifest:
    schema_version: int
    attempt_id: str
    source_revision: str
    changes: tuple[ResultChange, ...]

    def __post_init__(self) -> None:
        if self.schema_version != RESULT_SCHEMA_VERSION or isinstance(
            self.schema_version, bool
        ):
            raise ResultError(f"result schema_version must be {RESULT_SCHEMA_VERSION}")
        if (
            not isinstance(self.attempt_id, str)
            or ATTEMPT_ID_RE.fullmatch(self.attempt_id) is None
        ):
            raise ResultError("result attempt identifier is invalid")
        if (
            not isinstance(self.source_revision, str)
            or REVISION_RE.fullmatch(self.source_revision) is None
        ):
            raise ResultError("result source revision is invalid")
        paths = [change.path for change in self.changes]
        if paths != sorted(paths) or len(paths) != len(set(paths)):
            raise ResultError("result changes must use unique sorted paths")
        baseline_paths = {
            PurePosixPath(change.path)
            for change in self.changes
            if change.before is not None
        }
        if any(
            parent in baseline_paths
            for path in baseline_paths
            for parent in path.parents
        ):
            raise ResultError(
                "result baseline paths contain a leaf/descendant collision"
            )
        final_paths = {
            PurePosixPath(change.path)
            for change in self.changes
            if change.after is not None
        }
        if any(
            parent in final_paths for path in final_paths for parent in path.parents
        ):
            raise ResultError("result final paths contain a leaf/descendant collision")
        sizes_by_digest: dict[str, int] = {}
        for change in self.changes:
            for state in (change.before, change.after):
                if state is None or state.kind != "file":
                    continue
                assert state.sha256 is not None and state.size is not None
                known_size = sizes_by_digest.setdefault(state.sha256, state.size)
                if known_size != state.size:
                    raise ResultError("result blob digest has inconsistent sizes")
        if len(paths) > MAX_RESULT_PATHS:
            raise ResultError("result contains too many changed paths")
        if len(self.canonical.encode("utf-8")) > MAX_MANIFEST_BYTES:
            raise ResultError("result manifest exceeds the size limit")

    @classmethod
    def from_mapping(cls, value: object) -> ResultManifest:
        if not isinstance(value, dict) or set(value) != {
            "attempt_id",
            "changes",
            "schema_version",
            "source_revision",
        }:
            raise ResultError("result manifest has invalid fields")
        changes = value["changes"]
        if not isinstance(changes, list):
            raise ResultError("result changes must be a list")
        return cls(
            schema_version=value["schema_version"],
            attempt_id=value["attempt_id"],
            source_revision=value["source_revision"],
            changes=tuple(ResultChange.from_mapping(item) for item in changes),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "changes": [change.to_dict() for change in self.changes],
            "schema_version": self.schema_version,
            "source_revision": self.source_revision,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return _digest_bytes(self.canonical.encode("utf-8"))

    @property
    def changed_paths(self) -> int:
        return len(self.changes)

    @property
    def changed_bytes(self) -> int:
        return sum(
            change.after.size or 0
            for change in self.changes
            if change.needs_blob and change.after is not None
        )


@dataclass(frozen=True)
class CapturedResult:
    manifest: ResultManifest
    directory: Path


@dataclass(frozen=True)
class _DirectoryReplacement:
    path: str
    directories: tuple[str, ...]


class WorkspaceResultStore:
    """Retain inert, bounded changes from a stopped worker workspace."""

    def __init__(
        self,
        root: Path,
        *,
        max_changed_bytes: int = MAX_CHANGED_BYTES,
        max_paths: int = MAX_RESULT_PATHS,
        git_binary: Path | None = None,
    ):
        if (
            isinstance(max_changed_bytes, bool)
            or not isinstance(max_changed_bytes, int)
            or max_changed_bytes < 1
        ):
            raise ResultError("max_changed_bytes must be a positive integer")
        if (
            isinstance(max_paths, bool)
            or not isinstance(max_paths, int)
            or not 1 <= max_paths <= MAX_RESULT_PATHS
        ):
            raise ResultError(f"max_paths must be between 1 and {MAX_RESULT_PATHS}")
        self.root = Path(root).expanduser().absolute()
        self.max_changed_bytes = max_changed_bytes
        self.max_paths = max_paths
        if git_binary is None:
            discovered = shutil.which("git")
            self.git_binary = None if discovered is None else Path(discovered)
        elif not Path(git_binary).is_absolute():
            raise ResultError("Git binary path must be absolute")
        else:
            self.git_binary = Path(git_binary)

    def ensure_root(self) -> Path:
        try:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            info = self.root.lstat()
        except OSError as exc:
            raise ResultError("result store is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ResultError("result store must be a private directory")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise ResultError("result store is not owned by the current user")
        if info.st_mode & 0o077:
            raise ResultError("result store must use mode 0700")
        staging = self.root / ".staging"
        try:
            staging.mkdir(mode=0o700, exist_ok=True)
            staging_info = staging.lstat()
        except OSError as exc:
            raise ResultError("result staging area is unavailable") from exc
        if stat.S_ISLNK(staging_info.st_mode) or not stat.S_ISDIR(staging_info.st_mode):
            raise ResultError("result staging area must be a private directory")
        if staging_info.st_mode & 0o077:
            raise ResultError("result staging area must use mode 0700")
        return self.root

    @staticmethod
    def _attempt_id(value: str) -> str:
        if not isinstance(value, str) or ATTEMPT_ID_RE.fullmatch(value) is None:
            raise ResultError("invalid result attempt identifier")
        return value

    @staticmethod
    def _workspace(path: Path) -> Path:
        candidate = Path(path).expanduser().absolute()
        try:
            info = candidate.lstat()
        except OSError as exc:
            raise ResultError("result workspace is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ResultError("result workspace must be a directory")
        return candidate

    @staticmethod
    def _file_state(
        path: Path,
        relative: str,
        *,
        max_bytes: int | None = None,
    ) -> FileState:
        try:
            info = path.lstat()
        except OSError as exc:
            raise ResultError(f"cannot inspect result path: {relative}") from exc
        if stat.S_ISLNK(info.st_mode):
            try:
                target = os.readlink(path)
            except OSError as exc:
                raise ResultError(f"cannot read result symlink: {relative}") from exc
            return FileState(kind="symlink", target=_safe_symlink(relative, target))
        if not stat.S_ISREG(info.st_mode):
            raise ResultError(f"result contains an unsupported file type: {relative}")
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
            with os.fdopen(descriptor, "rb") as source:
                opened = os.fstat(source.fileno())
                if not stat.S_ISREG(opened.st_mode):
                    raise ResultError(
                        f"result contains an unsupported file type: {relative}"
                    )
                if max_bytes is not None and opened.st_size > max_bytes:
                    raise ResultError("result scan exceeds the size limit")
                digest = hashlib.sha256()
                size = 0
                while chunk := source.read(READ_CHUNK_BYTES):
                    size += len(chunk)
                    digest.update(chunk)
                finished = os.fstat(source.fileno())
        except OSError as exc:
            raise ResultError(f"cannot read result file: {relative}") from exc
        identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        final_identity = (
            finished.st_dev,
            finished.st_ino,
            finished.st_size,
            finished.st_mtime_ns,
        )
        if identity != final_identity or size != opened.st_size:
            raise ResultError(f"result file changed while being read: {relative}")
        mode = 0o755 if opened.st_mode & 0o111 else 0o644
        return FileState(kind="file", mode=mode, size=size, sha256=digest.hexdigest())

    @staticmethod
    def _protected_paths(baseline_paths: Sequence[str]) -> frozenset[str]:
        protected: set[str] = set(baseline_paths)
        for value in baseline_paths:
            path = PurePosixPath(value)
            protected.update(
                parent.as_posix() for parent in path.parents if parent.as_posix() != "."
            )
        return frozenset(protected)

    @contextmanager
    def _ignore_rules(
        self, rules: dict[str, bytes]
    ) -> Iterator[_PinnedIgnoreRules | None]:
        if not rules:
            yield None
            return
        if self.git_binary is None:
            raise ResultError("Git is required to evaluate pinned result ignore rules")
        matcher = _PinnedIgnoreRules(
            self.git_binary, self.ensure_root() / ".staging", rules
        )
        try:
            yield matcher
        finally:
            matcher.close()

    def _scan(
        self,
        workspace: Path,
        *,
        baseline: Mapping[str, FileState] | None = None,
        ignore_files: dict[str, bytes] | None = None,
        max_changed_bytes: int | None = None,
    ) -> dict[str, FileState]:
        workspace = self._workspace(workspace)
        states: dict[str, FileState] = {}
        path_count = 0
        inspected_count = 0
        pending = [PurePosixPath(".")]
        protected = self._protected_paths(() if baseline is None else tuple(baseline))
        leaves: list[tuple[Path, str, os.stat_result]] = []
        try:
            with self._ignore_rules(ignore_files or {}) as matcher:
                while pending:
                    relative_directory = pending.pop()
                    directory = (
                        workspace
                        if relative_directory.as_posix() == "."
                        else workspace.joinpath(*relative_directory.parts)
                    )
                    inspected: list[
                        tuple[Path, PurePosixPath, str, os.stat_result]
                    ] = []
                    with os.scandir(directory) as iterator:
                        for entry in iterator:
                            inspected_count += 1
                            if inspected_count > MAX_INSPECTED_PATHS:
                                raise ResultError(
                                    "result contains too many workspace entries"
                                )
                            relative_path = (
                                PurePosixPath(entry.name)
                                if relative_directory.as_posix() == "."
                                else relative_directory / entry.name
                            )
                            relative = _safe_path(relative_path.as_posix())
                            info = entry.stat(follow_symlinks=False)
                            inspected.append(
                                (Path(entry.path), relative_path, relative, info)
                            )
                            if len(inspected) == MAX_SCAN_BATCH_PATHS:
                                path_count = self._collect_scan_batch(
                                    inspected,
                                    matcher=matcher,
                                    protected=protected,
                                    pending=pending,
                                    leaves=leaves,
                                    path_count=path_count,
                                )
                                inspected.clear()
                    if inspected:
                        path_count = self._collect_scan_batch(
                            inspected,
                            matcher=matcher,
                            protected=protected,
                            pending=pending,
                            leaves=leaves,
                            path_count=path_count,
                        )
        except ResultError:
            raise
        except OSError as exc:
            raise ResultError("cannot enumerate result workspace") from exc

        if baseline is None:
            scan_limit = MAX_SNAPSHOT_BYTES
            regular_bytes = sum(
                info.st_size for _, _, info in leaves if stat.S_ISREG(info.st_mode)
            )
            if regular_bytes > scan_limit:
                raise ResultError("result baseline content exceeds the size limit")
        else:
            if max_changed_bytes is None:
                raise ResultError("result changed-content limit is unavailable")
            baseline_bytes = sum(
                state.size or 0 for state in baseline.values() if state.kind == "file"
            )
            scan_limit = baseline_bytes + max_changed_bytes
            definite_changed_bytes = 0
            for _, relative, info in leaves:
                if not stat.S_ISREG(info.st_mode):
                    continue
                previous = baseline.get(relative)
                if (
                    previous is None
                    or previous.kind != "file"
                    or previous.size != info.st_size
                ):
                    definite_changed_bytes += info.st_size
                    if definite_changed_bytes > max_changed_bytes:
                        raise ResultError(
                            "result changed content exceeds the size limit"
                        )

        read_bytes = 0
        changed_bytes = 0
        for path, relative, _ in sorted(leaves, key=lambda item: item[1]):
            state = self._file_state(
                path,
                relative,
                max_bytes=max(0, scan_limit - read_bytes),
            )
            states[relative] = state
            if state.kind != "file":
                continue
            assert state.size is not None and state.sha256 is not None
            read_bytes += state.size
            if read_bytes > scan_limit:
                raise ResultError("result scan exceeds the size limit")
            if baseline is None:
                continue
            previous = baseline.get(relative)
            if (
                previous is None
                or previous.kind != "file"
                or previous.size != state.size
                or previous.sha256 != state.sha256
            ):
                changed_bytes += state.size
                assert max_changed_bytes is not None
                if changed_bytes > max_changed_bytes:
                    raise ResultError("result changed content exceeds the size limit")
        try:
            _validate_portable_leaf_paths(
                tuple(states), context="result workspace paths"
            )
            validate_symlink_graph(
                {
                    PurePosixPath(path): state.target
                    for path, state in states.items()
                    if state.kind == "symlink" and state.target is not None
                }
            )
        except UnsafeSymlinkError as exc:
            raise ResultError("result contains an unsafe symlink") from exc
        return dict(sorted(states.items()))

    def _collect_scan_batch(
        self,
        inspected: Sequence[tuple[Path, PurePosixPath, str, os.stat_result]],
        *,
        matcher: _PinnedIgnoreRules | None,
        protected: frozenset[str],
        pending: list[PurePosixPath],
        leaves: list[tuple[Path, str, os.stat_result]],
        path_count: int,
    ) -> int:
        candidates = [
            (relative, stat.S_ISDIR(info.st_mode))
            for _, relative_path, relative, info in inspected
            if matcher is not None
            and relative not in protected
            and relative_path.name != ".gitignore"
        ]
        ignored = set() if matcher is None else matcher.ignored(candidates)
        for path, relative_path, relative, info in inspected:
            if relative in ignored:
                continue
            path_count += 1
            if path_count > self.max_paths:
                raise ResultError("result contains too many paths")
            if stat.S_ISDIR(info.st_mode):
                pending.append(relative_path)
                continue
            if not (stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)):
                raise ResultError(
                    f"result contains an unsupported file type: {relative}"
                )
            leaves.append((path, relative, info))
        return path_count

    @staticmethod
    def _read_pinned_ignore(path: Path, relative: str, expected: FileState) -> bytes:
        assert (
            expected.kind == "file"
            and expected.size is not None
            and expected.sha256 is not None
        )
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            before = path.lstat()
            if not stat.S_ISREG(before.st_mode):
                raise ResultError("cannot pin result ignore rules")
            descriptor = os.open(path, flags)
            with os.fdopen(descriptor, "rb") as input_file:
                opened = os.fstat(input_file.fileno())
                raw = input_file.read(MAX_PINNED_IGNORE_BYTES + 1)
                finished = os.fstat(input_file.fileno())
            current = path.lstat()
        except OSError as exc:
            raise ResultError("cannot pin result ignore rules") from exc
        identities = {
            (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
            for info in (before, opened, finished, current)
        }
        if (
            len(identities) != 1
            or not stat.S_ISREG(opened.st_mode)
            or len(raw) != expected.size
            or _digest_bytes(raw) != expected.sha256
        ):
            raise ResultError(f"cannot pin result ignore rules: {relative}")
        return raw

    @staticmethod
    def _pinned_ignore_files(
        workspace: Path, states: dict[str, FileState]
    ) -> dict[str, str]:
        pinned: dict[str, str] = {}
        total = 0
        for relative, state in states.items():
            if PurePosixPath(relative).name != ".gitignore" or state.kind != "file":
                continue
            path = workspace.joinpath(*PurePosixPath(relative).parts)
            raw = WorkspaceResultStore._read_pinned_ignore(path, relative, state)
            total += len(raw)
            if total > MAX_PINNED_IGNORE_BYTES:
                raise ResultError("result ignore rules exceed the size limit")
            pinned[relative] = base64.b64encode(raw).decode("ascii")
        return pinned

    def _stage_path(self, attempt_id: str) -> Path:
        return self.ensure_root() / ".staging" / f"{attempt_id}.json"

    @staticmethod
    def _read_file(
        path: Path,
        *,
        label: str,
        max_bytes: int,
        expected_size: int | None = None,
        expected_sha256: str | None = None,
        retain: bool,
    ) -> bytes | None:
        """Read trusted control-plane state with ordinary integrity bounds."""

        chunks: list[bytes] = []
        digest = hashlib.sha256()
        size = 0
        try:
            info = path.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > max_bytes:
                raise ResultError(label)
            with path.open("rb") as source:
                while size <= max_bytes:
                    chunk = source.read(min(READ_CHUNK_BYTES, max_bytes + 1 - size))
                    if not chunk:
                        break
                    size += len(chunk)
                    digest.update(chunk)
                    if retain:
                        chunks.append(chunk)
        except ResultError:
            raise
        except OSError as exc:
            raise ResultError(label) from exc
        if (
            size > max_bytes
            or (expected_size is not None and size != expected_size)
            or (expected_sha256 is not None and digest.hexdigest() != expected_sha256)
        ):
            raise ResultError(label)
        return b"".join(chunks) if retain else None

    @classmethod
    def _read_json(cls, path: Path, *, label: str) -> dict[str, Any]:
        raw = cls._read_file(
            path,
            label=f"{label} is invalid",
            max_bytes=MAX_MANIFEST_BYTES,
            retain=True,
        )
        assert raw is not None
        if not raw:
            raise ResultError(f"{label} is invalid")

        def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise ResultError(f"{label} contains duplicate fields")
                result[key] = value
            return result

        try:
            value = json.loads(raw, object_pairs_hook=unique)
        except ResultError:
            raise
        except (json.JSONDecodeError, UnicodeDecodeError, RecursionError) as exc:
            raise ResultError(f"{label} is invalid") from exc
        if not isinstance(value, dict):
            raise ResultError(f"{label} is invalid")
        return value

    def _read_staged_json(self, attempt_id: str) -> dict[str, Any]:
        return self._read_json(
            self._stage_path(attempt_id),
            label="result baseline",
        )

    @staticmethod
    def _write_new(path: Path, raw: bytes) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags, 0o600)
            with os.fdopen(descriptor, "wb") as output:
                output.write(raw)
                output.flush()
                os.fsync(output.fileno())
        except OSError as exc:
            raise ResultError("cannot write result artifact") from exc

    @staticmethod
    def _gitlinks(value: Sequence[str]) -> tuple[str, ...]:
        try:
            gitlinks = tuple(_safe_path(path) for path in value)
        except TypeError as exc:
            raise ResultError("result gitlink paths are invalid") from exc
        if gitlinks != tuple(sorted(set(gitlinks))):
            raise ResultError("result gitlink paths are invalid")
        return gitlinks

    @staticmethod
    def _validate_gitlinks(workspace: Path, gitlinks: Sequence[str]) -> None:
        for relative in gitlinks:
            try:
                parts = PurePosixPath(relative).parts
                path = workspace
                for part in parts[:-1]:
                    path /= part
                    parent = path.lstat()
                    if stat.S_ISLNK(parent.st_mode) or not stat.S_ISDIR(parent.st_mode):
                        raise ResultError(f"worker changed gitlink path: {relative}")
                path /= parts[-1]
                info = path.lstat()
                unchanged = (
                    stat.S_ISDIR(info.st_mode)
                    and not stat.S_ISLNK(info.st_mode)
                    and next(path.iterdir(), None) is None
                )
            except ResultError:
                raise
            except (OSError, UnicodeError) as exc:
                raise ResultError(f"worker changed gitlink path: {relative}") from exc
            if not unchanged:
                raise ResultError(f"worker changed gitlink path: {relative}")

    def stage(
        self,
        attempt_id: str,
        workspace: Path,
        source_revision: str,
        *,
        gitlinks: Sequence[str] = (),
    ) -> None:
        attempt_id = self._attempt_id(attempt_id)
        if (
            not isinstance(source_revision, str)
            or REVISION_RE.fullmatch(source_revision) is None
        ):
            raise ResultError("result source revision is invalid")
        workspace = self._workspace(workspace)
        gitlinks = self._gitlinks(gitlinks)
        self._validate_gitlinks(workspace, gitlinks)
        states = self._scan(workspace)
        value = {
            "attempt_id": attempt_id,
            "files": {path: state.to_dict() for path, state in states.items()},
            "gitlinks": list(gitlinks),
            "ignore_files": self._pinned_ignore_files(workspace, states),
            "schema_version": BASELINE_SCHEMA_VERSION,
            "source_revision": source_revision,
        }
        raw = canonical_json(value).encode("utf-8")
        if len(raw) > MAX_MANIFEST_BYTES:
            raise ResultError("result baseline exceeds the size limit")
        stage_path = self._stage_path(attempt_id)
        try:
            self._write_new(stage_path, raw)
        except ResultError as write_error:
            try:
                existing = self._read_staged_json(attempt_id)
            except ResultError:
                raise write_error from None
            if canonical_json(existing).encode("utf-8") != raw:
                raise ResultError(
                    "result attempt already has a different baseline"
                ) from None

    def _load_baseline(
        self, attempt_id: str
    ) -> tuple[str, dict[str, FileState], dict[str, bytes], tuple[str, ...]]:
        value = self._read_staged_json(attempt_id)
        version = value.get("schema_version")
        expected_fields = {"attempt_id", "files", "schema_version", "source_revision"}
        if version in {2, BASELINE_SCHEMA_VERSION}:
            expected_fields.add("ignore_files")
        if version == BASELINE_SCHEMA_VERSION:
            expected_fields.add("gitlinks")
        if set(value) != expected_fields:
            raise ResultError("result baseline is invalid")
        if version not in {1, 2, BASELINE_SCHEMA_VERSION} or isinstance(version, bool):
            raise ResultError("result baseline is invalid")
        if value["attempt_id"] != attempt_id:
            raise ResultError("result baseline belongs to another attempt")
        revision = value["source_revision"]
        if not isinstance(revision, str) or REVISION_RE.fullmatch(revision) is None:
            raise ResultError("result baseline is invalid")
        raw_files = value["files"]
        if not isinstance(raw_files, dict) or len(raw_files) > self.max_paths:
            raise ResultError("result baseline is invalid")
        states: dict[str, FileState] = {}
        for raw_path, raw_state in raw_files.items():
            path = _safe_path(raw_path)
            states[path] = FileState.from_mapping(raw_state, path=path)
        if list(states) != sorted(states):
            raise ResultError("result baseline paths are not sorted")
        if (
            sum(state.size or 0 for state in states.values() if state.kind == "file")
            > MAX_SNAPSHOT_BYTES
        ):
            raise ResultError("result baseline is invalid")
        ignore_files: dict[str, bytes] = {}
        encoded_rules = value.get("ignore_files", {})
        if not isinstance(encoded_rules, dict):
            raise ResultError("result baseline is invalid")
        total = 0
        for raw_path, encoded in encoded_rules.items():
            path = _safe_path(raw_path)
            if PurePosixPath(path).name != ".gitignore" or not isinstance(encoded, str):
                raise ResultError("result baseline is invalid")
            try:
                raw = base64.b64decode(encoded, validate=True)
            except (ValueError, binascii.Error) as exc:
                raise ResultError("result baseline is invalid") from exc
            total += len(raw)
            if total > MAX_PINNED_IGNORE_BYTES:
                raise ResultError("result baseline is invalid")
            ignore_files[path] = raw
        if list(ignore_files) != sorted(ignore_files):
            raise ResultError("result baseline is invalid")
        raw_gitlinks = value.get("gitlinks", ())
        if not isinstance(raw_gitlinks, list) and raw_gitlinks != ():
            raise ResultError("result baseline is invalid")
        gitlinks = self._gitlinks(raw_gitlinks)
        return revision, states, ignore_files, gitlinks

    @staticmethod
    def _changes(
        before: dict[str, FileState], after: dict[str, FileState]
    ) -> tuple[ResultChange, ...]:
        changes: list[ResultChange] = []
        for path in sorted(set(before) | set(after)):
            old = before.get(path)
            new = after.get(path)
            if old == new:
                continue
            operation = (
                "added" if old is None else "deleted" if new is None else "modified"
            )
            changes.append(ResultChange(path, operation, old, new))
        return tuple(changes)

    @staticmethod
    def _copy_blob(source: Path, target: Path, expected: FileState) -> None:
        assert expected.kind == "file" and expected.sha256 is not None
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        source_descriptor: int | None = None
        target_descriptor: int | None = None
        try:
            source_descriptor = os.open(source, flags)
            target_descriptor = os.open(
                target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
            )
            digest = hashlib.sha256()
            size = 0
            with ExitStack() as stack:
                input_file = stack.enter_context(os.fdopen(source_descriptor, "rb"))
                source_descriptor = None
                output_file = stack.enter_context(os.fdopen(target_descriptor, "wb"))
                target_descriptor = None
                while chunk := input_file.read(READ_CHUNK_BYTES):
                    size += len(chunk)
                    digest.update(chunk)
                    output_file.write(chunk)
                output_file.flush()
                os.fsync(output_file.fileno())
        except OSError as exc:
            raise ResultError("cannot retain changed result content") from exc
        finally:
            if source_descriptor is not None:
                os.close(source_descriptor)
            if target_descriptor is not None:
                os.close(target_descriptor)
        if size != expected.size or digest.hexdigest() != expected.sha256:
            raise ResultError("result file changed before it could be retained")

    def capture(self, attempt_id: str, workspace: Path) -> CapturedResult:
        attempt_id = self._attempt_id(attempt_id)
        root = self.ensure_root()
        final = root / attempt_id
        if final.exists():
            return CapturedResult(self.inspect(attempt_id), final)
        workspace = self._workspace(workspace)
        revision, before, ignore_files, gitlinks = self._load_baseline(attempt_id)
        self._validate_gitlinks(workspace, gitlinks)
        after = self._scan(
            workspace,
            baseline=before,
            ignore_files=ignore_files,
            max_changed_bytes=self.max_changed_bytes,
        )
        manifest = ResultManifest(
            RESULT_SCHEMA_VERSION,
            attempt_id,
            revision,
            self._changes(before, after),
        )
        if manifest.changed_paths > self.max_paths:
            raise ResultError("result contains too many changed paths")
        if manifest.changed_bytes > self.max_changed_bytes:
            raise ResultError("result changed content exceeds the size limit")
        temporary = Path(tempfile.mkdtemp(prefix=f".{attempt_id}.", dir=root))
        try:
            os.chmod(temporary, 0o700)
            blobs = temporary / "blobs"
            blobs.mkdir(mode=0o700)
            copied: set[str] = set()
            for change in manifest.changes:
                if not change.needs_blob or change.after is None:
                    continue
                digest = change.after.sha256
                assert digest is not None
                if digest in copied:
                    continue
                source = workspace.joinpath(*PurePosixPath(change.path).parts)
                self._copy_blob(source, blobs / digest, change.after)
                copied.add(digest)
            self._validate_gitlinks(workspace, gitlinks)
            self._write_new(
                temporary / "manifest.json", manifest.canonical.encode("utf-8")
            )
            try:
                temporary.rename(final)
            except OSError as exc:
                if exc.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                existing = self.inspect(attempt_id)
                if existing.digest != manifest.digest:
                    raise ResultError(
                        "result attempt already has different captured content"
                    ) from None
                return CapturedResult(existing, final)
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
        return CapturedResult(manifest, final)

    def inspect(
        self, attempt_id: str, *, expected_digest: str | None = None
    ) -> ResultManifest:
        manifest, _ = self._load_bundle(
            attempt_id,
            expected_digest=expected_digest,
            retain=False,
        )
        return manifest

    @staticmethod
    def _expected_blobs(manifest: ResultManifest) -> dict[str, FileState]:
        expected: dict[str, FileState] = {}
        for change in manifest.changes:
            if not change.needs_blob or change.after is None:
                continue
            state = change.after
            assert state.sha256 is not None
            expected.setdefault(state.sha256, state)
        return expected

    @classmethod
    def _read_blob_set(
        cls,
        directory: Path,
        expected: Mapping[str, FileState],
        *,
        retain: bool,
    ) -> dict[str, bytes]:
        seen: set[str] = set()
        loaded: dict[str, bytes] = {}
        try:
            entries = list(directory.iterdir())
        except OSError as exc:
            raise ResultError("result blobs are unavailable") from exc
        for path in entries:
            name = path.name
            if (
                len(seen) >= len(expected)
                or DIGEST_RE.fullmatch(name) is None
                or name not in expected
                or name in seen
            ):
                raise ResultError("result blob set does not match the manifest")
            state = expected[name]
            assert state.size is not None and state.sha256 is not None
            content = cls._read_file(
                path,
                label="result blob digest does not match",
                max_bytes=state.size,
                expected_size=state.size,
                expected_sha256=state.sha256,
                retain=retain,
            )
            seen.add(name)
            if content is not None:
                loaded[name] = content
        if seen != set(expected):
            raise ResultError("result blob set does not match the manifest")
        return loaded

    def _load_bundle(
        self,
        attempt_id: str,
        *,
        expected_digest: str | None,
        retain: bool,
    ) -> tuple[ResultManifest, dict[str, bytes]]:
        attempt_id = self._attempt_id(attempt_id)
        if expected_digest is not None and (
            not isinstance(expected_digest, str)
            or DIGEST_RE.fullmatch(expected_digest) is None
        ):
            raise ResultError("expected result digest is invalid")
        directory = self.ensure_root() / attempt_id
        manifest = ResultManifest.from_mapping(
            self._read_json(directory / "manifest.json", label="result manifest")
        )
        if manifest.attempt_id != attempt_id:
            raise ResultError("result manifest belongs to another attempt")
        if expected_digest is not None and manifest.digest != expected_digest:
            raise ResultError("result manifest digest does not match")
        if manifest.changed_paths > self.max_paths:
            raise ResultError("result contains too many changed paths")
        if manifest.changed_bytes > self.max_changed_bytes:
            raise ResultError("result changed content exceeds the size limit")
        blobs = self._read_blob_set(
            directory / "blobs",
            self._expected_blobs(manifest),
            retain=retain,
        )
        return manifest, blobs

    def load_bundle(
        self, attempt_id: str, *, expected_digest: str | None = None
    ) -> tuple[ResultManifest, dict[str, bytes]]:
        """Load one trusted result bundle without re-reading its files."""

        return self._load_bundle(
            attempt_id,
            expected_digest=expected_digest,
            retain=True,
        )

    def load_blobs(
        self, attempt_id: str, *, expected_digest: str | None = None
    ) -> dict[str, bytes]:
        """Return every verified changed-file body for trusted materialization."""

        _, blobs = self.load_bundle(attempt_id, expected_digest=expected_digest)
        return blobs

    @staticmethod
    def _target_root(path: Path) -> Path:
        candidate = Path(path).expanduser().absolute()
        try:
            info = candidate.lstat()
        except OSError as exc:
            raise ResultError("result target is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ResultError("result target must be a directory")
        if candidate == Path(candidate.anchor):
            raise ResultError("result target must not be a filesystem root")
        return candidate

    @staticmethod
    def _target_path(root: Path, relative: str) -> Path:
        parts = PurePosixPath(relative).parts
        current = root
        for part in parts[:-1]:
            current /= part
            try:
                info = current.lstat()
            except FileNotFoundError:
                break
            except OSError as exc:
                raise ResultError(f"cannot inspect result target: {relative}") from exc
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise ResultError(f"target parent is not a directory: {relative}")
        return root.joinpath(*parts)

    def _preflight_directory_replacement(
        self,
        root: Path,
        change: ResultChange,
        expected_leaves: Mapping[str, FileState],
    ) -> _DirectoryReplacement:
        prefix = PurePosixPath(change.path)
        expected_directories = {change.path}
        for relative in expected_leaves:
            path = PurePosixPath(relative)
            expected_directories.update(
                parent.as_posix()
                for parent in path.parents
                if parent == prefix or prefix in parent.parents
            )

        # An empty directory at an added-leaf path is unknown checkout state,
        # not evidence of the deleted tree represented by the manifest.
        if not expected_leaves:
            raise ResultError(f"result target changed: {change.path}")

        pending = [change.path]
        seen_directories: set[str] = set()
        seen_leaves: set[str] = set()
        scanned_entries = 0
        expected_entries = len(expected_leaves) + len(expected_directories) - 1
        try:
            while pending:
                relative_directory = pending.pop()
                directory = root.joinpath(*PurePosixPath(relative_directory).parts)
                info = directory.lstat()
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    raise ResultError(f"result target changed: {change.path}")
                with os.scandir(directory) as iterator:
                    seen_directories.add(relative_directory)
                    for entry in iterator:
                        scanned_entries += 1
                        if scanned_entries > expected_entries:
                            raise ResultError(f"result target changed: {change.path}")
                        relative = _safe_path(
                            (PurePosixPath(relative_directory) / entry.name).as_posix()
                        )
                        info = entry.stat(follow_symlinks=False)
                        if stat.S_ISDIR(info.st_mode):
                            if relative not in expected_directories:
                                raise ResultError(
                                    f"result target changed: {change.path}"
                                )
                            pending.append(relative)
                            continue
                        expected = expected_leaves.get(relative)
                        if expected is None:
                            raise ResultError(f"result target changed: {change.path}")
                        if self._file_state(Path(entry.path), relative) != expected:
                            raise ResultError(f"result target changed: {change.path}")
                        seen_leaves.add(relative)
        except ResultError:
            raise
        except (OSError, UnicodeError) as exc:
            raise ResultError(f"result target changed: {change.path}") from exc

        if seen_directories != expected_directories or seen_leaves != set(
            expected_leaves
        ):
            raise ResultError(f"result target changed: {change.path}")
        return _DirectoryReplacement(
            path=change.path,
            directories=tuple(
                sorted(
                    expected_directories,
                    key=lambda value: (-len(PurePosixPath(value).parts), value),
                )
            ),
        )

    @staticmethod
    def _index_deleted_descendants(
        changes: Sequence[ResultChange],
    ) -> tuple[frozenset[PurePosixPath], dict[str, dict[str, FileState]]]:
        deleted_leaves: set[PurePosixPath] = set()
        descendants: dict[str, dict[str, FileState]] = {}
        for change in changes:
            if change.after is not None:
                continue
            assert change.before is not None
            path = PurePosixPath(change.path)
            deleted_leaves.add(path)
            for parent in path.parents:
                relative = parent.as_posix()
                if relative == ".":
                    break
                descendants.setdefault(relative, {})[change.path] = change.before
        return frozenset(deleted_leaves), descendants

    def _preflight_apply(
        self, root: Path, changes: Sequence[ResultChange]
    ) -> tuple[_DirectoryReplacement, ...]:
        if not isinstance(changes, tuple):
            changes = tuple(changes)
        _validate_portable_leaf_paths(
            tuple(change.path for change in changes if change.before is not None),
            context="result baseline paths",
        )
        _validate_portable_leaf_paths(
            tuple(change.path for change in changes if change.after is not None),
            context="result final paths",
        )
        replacements: list[_DirectoryReplacement] = []
        deleted_leaves, deleted_descendants = self._index_deleted_descendants(changes)
        for change in changes:
            path = PurePosixPath(change.path)
            if any(parent in deleted_leaves for parent in path.parents):
                # An exact leaf scheduled for deletion masks descendants in
                # the current checkout. Added descendants are preflighted as
                # absent, then their parent directories are created only
                # after every baseline leaf has passed verification.
                continue
            target = self._target_path(root, change.path)
            try:
                info = target.lstat()
            except FileNotFoundError:
                current = None
            except OSError as exc:
                raise ResultError(
                    f"cannot inspect result target: {change.path}"
                ) from exc
            else:
                if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                    if change.before is None and change.after is not None:
                        replacements.append(
                            self._preflight_directory_replacement(
                                root,
                                change,
                                deleted_descendants.get(change.path, {}),
                            )
                        )
                        continue
                    raise ResultError(f"result target changed: {change.path}")
                try:
                    current = self._file_state(target, change.path)
                except ResultError as exc:
                    raise ResultError(f"result target changed: {change.path}") from exc
            if current != change.before and self._is_portable_deleted_alias(
                root,
                change,
                current,
                changes,
            ):
                current = None
            if current != change.before:
                raise ResultError(f"result target changed: {change.path}")
        return tuple(replacements)

    @staticmethod
    def _path_has_exact_spelling(root: Path, path: PurePosixPath) -> bool:
        current = root
        try:
            for component in path.parts:
                with os.scandir(current) as iterator:
                    entry = next(
                        (
                            candidate
                            for candidate in iterator
                            if candidate.name == component
                        ),
                        None,
                    )
                if entry is None:
                    return False
                current = Path(entry.path)
        except OSError:
            return False
        return True

    @classmethod
    def _is_portable_deleted_alias(
        cls,
        root: Path,
        change: ResultChange,
        current: FileState | None,
        changes: Sequence[ResultChange],
    ) -> bool:
        if change.before is not None or current is None:
            return False

        path = PurePosixPath(change.path)
        identity = portable_path_identity(path)
        target = root.joinpath(*path.parts)
        try:
            target_info = target.lstat()
        except OSError:
            return False
        if cls._path_has_exact_spelling(root, path):
            return False

        for candidate in changes:
            candidate_path = PurePosixPath(candidate.path)
            if (
                candidate.after is not None
                or candidate.before is None
                or candidate.path == change.path
                or portable_path_identity(candidate_path) != identity
            ):
                continue
            alias = root.joinpath(*candidate_path.parts)
            try:
                alias_info = alias.lstat()
            except OSError:
                continue
            if (
                cls._path_has_exact_spelling(root, candidate_path)
                and (target_info.st_dev, target_info.st_ino)
                == (alias_info.st_dev, alias_info.st_ino)
                and candidate.before == current
            ):
                return True
        return False

    @staticmethod
    def _ensure_parents(root: Path, target: Path) -> None:
        missing: list[Path] = []
        current = target.parent
        while current != root:
            try:
                info = current.lstat()
            except FileNotFoundError:
                missing.append(current)
                current = current.parent
                continue
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise ResultError("result target parent is not a directory")
            break
        for directory in reversed(missing):
            directory.mkdir(mode=0o755)

    @staticmethod
    def _prune_empty(root: Path, start: Path) -> None:
        current = start
        while current != root:
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    @staticmethod
    def _atomic_mode_only_file(
        target: Path,
        relative: str,
        expected: FileState,
        mode: int,
    ) -> None:
        assert (
            expected.kind == "file"
            and expected.mode is not None
            and expected.size is not None
            and expected.sha256 is not None
        )
        temporary = target.parent / f".mgb-{secrets.token_hex(16)}"
        digest = hashlib.sha256()
        size = 0
        try:
            with target.open("rb") as source, temporary.open("xb") as output:
                while size <= expected.size:
                    chunk = source.read(min(READ_CHUNK_BYTES, expected.size + 1 - size))
                    if not chunk:
                        break
                    size += len(chunk)
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fchmod(output.fileno(), mode)
                os.fsync(output.fileno())
        except OSError as exc:
            temporary.unlink(missing_ok=True)
            raise ResultError(f"cannot apply result mode: {relative}") from exc
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        if size != expected.size or digest.hexdigest() != expected.sha256:
            temporary.unlink(missing_ok=True)
            raise ResultError(f"result target changed: {relative}")
        try:
            os.replace(temporary, target)
        except OSError as exc:
            temporary.unlink(missing_ok=True)
            raise ResultError(f"cannot apply result mode: {relative}") from exc

    @staticmethod
    def _atomic_file(target: Path, content: bytes, mode: int) -> None:
        temporary = target.parent / f".mgb-{secrets.token_hex(16)}"
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(temporary, flags, 0o600)
            with os.fdopen(descriptor, "wb") as output:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            temporary.chmod(mode)
            os.replace(temporary, target)
        except OSError as exc:
            temporary.unlink(missing_ok=True)
            raise ResultError("cannot apply result file") from exc

    @staticmethod
    def _atomic_symlink(target: Path, link_target: str) -> None:
        temporary = target.parent / f".mgb-{secrets.token_hex(16)}"
        try:
            temporary.symlink_to(link_target)
            os.replace(temporary, target)
        except OSError as exc:
            temporary.unlink(missing_ok=True)
            raise ResultError("cannot apply result symlink") from exc

    @staticmethod
    def _validate_applied_symlinks(root: Path, changes: Sequence[ResultChange]) -> None:
        final_states = {change.path: change.after for change in changes}
        final_identities = {
            portable_path_identity(PurePosixPath(path)) for path in final_states
        }
        changed_links = {
            PurePosixPath(path): state.target
            for path, state in final_states.items()
            if state is not None
            and state.kind == "symlink"
            and state.target is not None
        }
        if not changed_links:
            return
        changed_by_identity = {
            portable_path_identity(path): target
            for path, target in changed_links.items()
        }

        existing_links: dict[PurePosixPath, str] = {}
        existing_by_identity: dict[tuple[str, ...], str] = {}
        pending = [PurePosixPath(".")]
        inspected = 0
        try:
            while pending:
                relative_directory = pending.pop()
                directory = (
                    root
                    if relative_directory.as_posix() == "."
                    else root.joinpath(*relative_directory.parts)
                )
                with os.scandir(directory) as iterator:
                    entries = tuple(iterator)
                for entry in entries:
                    relative_path = (
                        PurePosixPath(entry.name)
                        if relative_directory.as_posix() == "."
                        else relative_directory / entry.name
                    )
                    if any(
                        reserved_git_component(part) for part in relative_path.parts
                    ):
                        continue
                    _safe_path(relative_path.as_posix())
                    identity = portable_path_identity(relative_path)
                    if identity in final_identities or any(
                        identity[:depth] in final_identities
                        for depth in range(1, len(identity))
                    ):
                        continue
                    inspected += 1
                    if inspected > MAX_RESULT_PATHS:
                        raise ResultError(
                            "result target contains too many paths for "
                            "symlink validation"
                        )
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISLNK(info.st_mode):
                        target = os.readlink(entry.path)
                        if identity in existing_by_identity:
                            raise ResultError(
                                "result target contains portable symlink aliases"
                            )
                        existing_links[relative_path] = target
                        existing_by_identity[identity] = target
                    elif stat.S_ISDIR(info.st_mode):
                        pending.append(relative_path)
        except ResultError:
            raise
        except (OSError, UnsafeSymlinkError) as exc:
            raise ResultError("cannot inspect result symlink graph") from exc

        for path, state in final_states.items():
            if state is None:
                continue
            candidate = PurePosixPath(path)
            identity = portable_path_identity(candidate)
            if any(
                identity[:depth] in changed_by_identity
                or identity[:depth] in existing_by_identity
                for depth in range(1, len(identity))
            ):
                raise ResultError("result contains a path beneath a symlink")

        try:
            validate_symlink_graph(
                changed_links,
                lookup=lambda path: existing_by_identity.get(
                    portable_path_identity(path)
                ),
            )
            for source, target in existing_links.items():
                touched_changed = False

                def lookup(path: PurePosixPath) -> str | None:
                    nonlocal touched_changed
                    identity = portable_path_identity(path)
                    changed = changed_by_identity.get(identity)
                    if identity in changed_by_identity:
                        touched_changed = True
                        return changed
                    return existing_by_identity.get(identity)

                try:
                    validate_symlink_graph({source: target}, lookup=lookup)
                except UnsafeSymlinkError:
                    if touched_changed:
                        raise
        except UnsafeSymlinkError as exc:
            raise ResultError("result contains an unsafe symlink") from exc

    def apply(
        self,
        attempt_id: str,
        repository: Path,
        *,
        expected_digest: str | None = None,
    ) -> dict[str, Any]:
        manifest, blobs = self.load_bundle(attempt_id, expected_digest=expected_digest)
        root = self._target_root(repository)
        prepared_files: dict[str, bytes] = {}
        for change in manifest.changes:
            after = change.after
            if after is None or after.kind != "file" or not change.needs_blob:
                continue
            assert after.sha256 is not None
            prepared_files[change.path] = blobs[after.sha256]

        replacements = self._preflight_apply(root, manifest.changes)
        self._validate_applied_symlinks(root, manifest.changes)

        deleted = [change for change in manifest.changes if change.after is None]
        replacement_roots = {
            PurePosixPath(replacement.path) for replacement in replacements
        }
        ordinary_deleted_parents: set[Path] = set()
        for change in sorted(
            deleted, key=lambda item: len(PurePosixPath(item.path).parts), reverse=True
        ):
            target = self._target_path(root, change.path)
            try:
                target.unlink()
            except OSError as exc:
                raise ResultError(
                    f"cannot delete result target: {change.path}"
                ) from exc
            path = PurePosixPath(change.path)
            if not any(prefix in path.parents for prefix in replacement_roots):
                ordinary_deleted_parents.add(target.parent)

        for replacement in replacements:
            for relative in replacement.directories:
                target = self._target_path(root, relative)
                try:
                    target.rmdir()
                except OSError as exc:
                    raise ResultError(
                        f"cannot replace result target directory: {replacement.path}"
                    ) from exc
        for parent in sorted(
            ordinary_deleted_parents,
            key=lambda item: len(item.parts),
            reverse=True,
        ):
            self._prune_empty(root, parent)

        for change in (item for item in manifest.changes if item.after is not None):
            after = change.after
            assert after is not None
            target = self._target_path(root, change.path)
            self._ensure_parents(root, target)
            if after.kind == "symlink":
                assert after.target is not None
                self._atomic_symlink(target, after.target)
            elif change.needs_blob:
                assert after.mode is not None
                self._atomic_file(target, prepared_files[change.path], after.mode)
            else:
                assert after.mode is not None
                assert change.before is not None and change.before.kind == "file"
                self._atomic_mode_only_file(
                    target,
                    change.path,
                    change.before,
                    after.mode,
                )
        return {
            "attempt_id": attempt_id,
            "changed_bytes": manifest.changed_bytes,
            "changed_paths": manifest.changed_paths,
            "content_sha256": manifest.digest,
            "source_revision": manifest.source_revision,
        }

    def discard_stage(self, attempt_id: str) -> bool:
        attempt_id = self._attempt_id(attempt_id)
        path = self._stage_path(attempt_id)
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise ResultError("cannot remove result baseline") from exc
        return True


def default_result_root(database_path: Path) -> Path:
    return Path(database_path).expanduser().absolute().parent / "results"
