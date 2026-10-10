from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .domain import canonical_json

RESULT_SCHEMA_VERSION = 1
ATTEMPT_ID_RE = re.compile(r"^att_[0-9a-f]{32}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
REVISION_RE = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
MAX_RESULT_PATHS = 100_000
MAX_CHANGED_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_PATH_BYTES = 1024
READ_CHUNK_BYTES = 1024 * 1024


class ResultError(RuntimeError):
    """A worker result could not be retained or applied safely."""


def _digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe_path(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ResultError("result path must be a non-empty string")
    if len(value.encode("utf-8")) > MAX_PATH_BYTES:
        raise ResultError("result path is too long")
    path = PurePosixPath(value)
    if (
        value.startswith("/")
        or "\\" in value
        or value != path.as_posix()
        or path.as_posix() == "."
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ResultError("result path must be a safe relative POSIX path")
    if ".git" in path.parts:
        raise ResultError("result contains a reserved .git path")
    return value


def _safe_symlink(path: str, target: object) -> str:
    if not isinstance(target, str) or not target or "\x00" in target:
        raise ResultError(f"result contains an unsafe symlink: {path}")
    link = PurePosixPath(target)
    if link.is_absolute():
        raise ResultError(f"result contains an unsafe symlink: {path}")
    depth = len(PurePosixPath(path).parent.parts)
    for part in link.parts:
        if part == "..":
            depth -= 1
            if depth < 0:
                raise ResultError(f"result contains an unsafe symlink: {path}")
        elif part not in {"", "."}:
            depth += 1
    if len(target.encode("utf-8")) > MAX_PATH_BYTES:
        raise ResultError(f"result contains an unsafe symlink: {path}")
    return target


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


class WorkspaceResultStore:
    """Retain inert, bounded changes from a stopped worker workspace."""

    def __init__(
        self,
        root: Path,
        *,
        max_changed_bytes: int = MAX_CHANGED_BYTES,
        max_paths: int = MAX_RESULT_PATHS,
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
    def _file_state(path: Path, relative: str) -> FileState:
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

    def _scan(self, workspace: Path) -> dict[str, FileState]:
        workspace = self._workspace(workspace)
        states: dict[str, FileState] = {}
        path_count = 0
        pending = [PurePosixPath(".")]
        try:
            while pending:
                relative_directory = pending.pop()
                directory = (
                    workspace
                    if relative_directory.as_posix() == "."
                    else workspace.joinpath(*relative_directory.parts)
                )
                with os.scandir(directory) as iterator:
                    entries = sorted(iterator, key=lambda item: item.name)
                for entry in entries:
                    path_count += 1
                    if path_count > self.max_paths:
                        raise ResultError("result contains too many paths")
                    relative_path = (
                        PurePosixPath(entry.name)
                        if relative_directory.as_posix() == "."
                        else relative_directory / entry.name
                    )
                    relative = _safe_path(relative_path.as_posix())
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        pending.append(relative_path)
                        continue
                    states[relative] = self._file_state(Path(entry.path), relative)
        except ResultError:
            raise
        except OSError as exc:
            raise ResultError("cannot enumerate result workspace") from exc
        return dict(sorted(states.items()))

    def _stage_path(self, attempt_id: str) -> Path:
        return self.ensure_root() / ".staging" / f"{attempt_id}.json"

    @staticmethod
    def _read_json(path: Path, *, label: str) -> dict[str, Any]:
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise ResultError(f"{label} is unavailable") from exc
        if not raw or len(raw) > MAX_MANIFEST_BYTES:
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

    def stage(self, attempt_id: str, workspace: Path, source_revision: str) -> None:
        attempt_id = self._attempt_id(attempt_id)
        if (
            not isinstance(source_revision, str)
            or REVISION_RE.fullmatch(source_revision) is None
        ):
            raise ResultError("result source revision is invalid")
        states = self._scan(workspace)
        value = {
            "attempt_id": attempt_id,
            "files": {path: state.to_dict() for path, state in states.items()},
            "schema_version": RESULT_SCHEMA_VERSION,
            "source_revision": source_revision,
        }
        raw = canonical_json(value).encode("utf-8")
        if len(raw) > MAX_MANIFEST_BYTES:
            raise ResultError("result baseline exceeds the size limit")
        stage_path = self._stage_path(attempt_id)
        try:
            self._write_new(stage_path, raw)
        except ResultError:
            if not stage_path.exists():
                raise
            existing = self._read_json(stage_path, label="result baseline")
            if canonical_json(existing).encode("utf-8") != raw:
                raise ResultError(
                    "result attempt already has a different baseline"
                ) from None

    def _load_baseline(self, attempt_id: str) -> tuple[str, dict[str, FileState]]:
        value = self._read_json(self._stage_path(attempt_id), label="result baseline")
        if set(value) != {"attempt_id", "files", "schema_version", "source_revision"}:
            raise ResultError("result baseline is invalid")
        if value["schema_version"] != RESULT_SCHEMA_VERSION:
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
        return revision, states

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
        try:
            source_descriptor = os.open(source, flags)
            target_descriptor = os.open(
                target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
            )
            digest = hashlib.sha256()
            size = 0
            with (
                os.fdopen(source_descriptor, "rb") as input_file,
                os.fdopen(target_descriptor, "wb") as output_file,
            ):
                while chunk := input_file.read(READ_CHUNK_BYTES):
                    size += len(chunk)
                    digest.update(chunk)
                    output_file.write(chunk)
                output_file.flush()
                os.fsync(output_file.fileno())
        except OSError as exc:
            raise ResultError("cannot retain changed result content") from exc
        if size != expected.size or digest.hexdigest() != expected.sha256:
            raise ResultError("result file changed before it could be retained")

    def capture(self, attempt_id: str, workspace: Path) -> CapturedResult:
        attempt_id = self._attempt_id(attempt_id)
        root = self.ensure_root()
        final = root / attempt_id
        if final.exists():
            return CapturedResult(self.inspect(attempt_id), final)
        workspace = self._workspace(workspace)
        revision, before = self._load_baseline(attempt_id)
        after = self._scan(workspace)
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
            self._write_new(
                temporary / "manifest.json", manifest.canonical.encode("utf-8")
            )
            try:
                temporary.rename(final)
            except FileExistsError:
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
        attempt_id = self._attempt_id(attempt_id)
        if expected_digest is not None and (
            not isinstance(expected_digest, str)
            or DIGEST_RE.fullmatch(expected_digest) is None
        ):
            raise ResultError("expected result digest is invalid")
        directory = self.ensure_root() / attempt_id
        try:
            info = directory.lstat()
        except OSError as exc:
            raise ResultError("captured result is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ResultError("captured result is invalid")
        value = self._read_json(directory / "manifest.json", label="result manifest")
        manifest = ResultManifest.from_mapping(value)
        if manifest.attempt_id != attempt_id:
            raise ResultError("result manifest belongs to another attempt")
        if expected_digest is not None and manifest.digest != expected_digest:
            raise ResultError("result manifest digest does not match")
        required = {
            change.after.sha256
            for change in manifest.changes
            if change.needs_blob and change.after is not None
        }
        blobs = directory / "blobs"
        try:
            with os.scandir(blobs) as iterator:
                entries = {entry.name: entry for entry in iterator}
        except OSError as exc:
            raise ResultError("result blobs are unavailable") from exc
        if set(entries) != required or any(
            DIGEST_RE.fullmatch(name) is None for name in entries
        ):
            raise ResultError("result blob set does not match the manifest")
        expected_by_digest = {
            change.after.sha256: change.after
            for change in manifest.changes
            if change.needs_blob and change.after is not None
        }
        for digest, entry in entries.items():
            state = self._file_state(Path(entry.path), f"blobs/{digest}")
            expected = expected_by_digest[digest]
            if (
                state.kind != "file"
                or state.size != expected.size
                or state.sha256 != digest
            ):
                raise ResultError("result blob digest does not match")
        return manifest

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

    def _current_state(self, root: Path, relative: str) -> FileState | None:
        target = self._target_path(root, relative)
        try:
            target.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ResultError(f"cannot inspect result target: {relative}") from exc
        return self._file_state(target, relative)

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
    def _atomic_file(target: Path, blob: Path, mode: int) -> None:
        temporary = target.parent / f".mgb-{secrets.token_hex(16)}"
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(temporary, flags, 0o600)
            with os.fdopen(descriptor, "wb") as output, blob.open("rb") as source:
                shutil.copyfileobj(source, output)
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

    def apply(
        self,
        attempt_id: str,
        repository: Path,
        *,
        expected_digest: str | None = None,
    ) -> dict[str, Any]:
        manifest = self.inspect(attempt_id, expected_digest=expected_digest)
        root = self._target_root(repository)
        for change in manifest.changes:
            current = self._current_state(root, change.path)
            if current != change.before:
                raise ResultError(f"result target changed: {change.path}")

        deleted = [change for change in manifest.changes if change.after is None]
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
            self._prune_empty(root, target.parent)

        directory = self.ensure_root() / attempt_id
        for change in (item for item in manifest.changes if item.after is not None):
            after = change.after
            assert after is not None
            target = self._target_path(root, change.path)
            self._ensure_parents(root, target)
            if after.kind == "symlink":
                assert after.target is not None
                self._atomic_symlink(target, after.target)
            elif not change.needs_blob:
                assert after.mode is not None
                try:
                    target.chmod(after.mode)
                except OSError as exc:
                    raise ResultError(
                        f"cannot apply result mode: {change.path}"
                    ) from exc
            else:
                assert after.sha256 is not None and after.mode is not None
                self._atomic_file(
                    target, directory / "blobs" / after.sha256, after.mode
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
