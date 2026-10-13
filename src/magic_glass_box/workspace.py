from __future__ import annotations

import io
import os
import re
import shutil
import stat
import subprocess
import tarfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

from .path_safety import UnsafeSymlinkError, validate_symlink_graph

RUN_ID_RE = re.compile(r"^run_[0-9a-f]{32}$")
COMMIT_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
MAX_ARCHIVE_BYTES = 512 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 100_000


class WorkspaceError(RuntimeError):
    """A per-run workspace could not be prepared without weakening isolation."""


@dataclass(frozen=True)
class PreparedWorkspace:
    path: Path
    revision: str


class Snapshotter(Protocol):
    def snapshot(
        self, repository: Path, revision: str | None, destination: Path
    ) -> str: ...


GitRunner = Callable[..., subprocess.CompletedProcess[bytes]]


class GitSnapshotter:
    """Export tracked files from one immutable Git commit."""

    def __init__(
        self,
        *,
        binary: Path | None = None,
        runner: GitRunner | None = None,
        timeout: float = 60.0,
    ):
        if binary is None:
            discovered = shutil.which("git")
            if discovered is None:
                raise WorkspaceError("Git CLI was not found")
            binary = Path(discovered)
        if not Path(binary).is_absolute():
            raise WorkspaceError("Git CLI path must be absolute")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or timeout <= 0
        ):
            raise WorkspaceError("Git command timeout must be positive")
        self.binary = Path(binary)
        self.runner = runner or subprocess.run
        self.timeout = float(timeout)

    def _run(self, arguments: list[str], *, operation: str) -> bytes:
        try:
            completed = self.runner(  # noqa: S603 - absolute configured binary
                [str(self.binary), *arguments],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                check=False,
                timeout=self.timeout,
            )
        except subprocess.TimeoutExpired:
            raise WorkspaceError(f"Git {operation} timed out") from None
        except OSError:
            raise WorkspaceError("cannot execute Git CLI") from None
        if completed.returncode != 0:
            detail = completed.stderr.decode("utf-8", "replace").strip()
            suffix = f": {detail[:4096]}" if detail else ""
            raise WorkspaceError(f"Git cannot {operation}{suffix}")
        if not isinstance(completed.stdout, bytes):
            raise WorkspaceError(f"Git returned invalid {operation} output")
        return completed.stdout

    def snapshot(
        self, repository: Path, revision: str | None, destination: Path
    ) -> str:
        requested = "HEAD" if revision is None else revision
        if (
            not isinstance(requested, str)
            or not requested
            or "\x00" in requested
            or len(requested.encode("utf-8")) > 512
        ):
            raise WorkspaceError("source revision is invalid")
        resolved = self._run(
            [
                "-C",
                str(repository),
                "rev-parse",
                "--verify",
                "--end-of-options",
                f"{requested}^{{commit}}",
            ],
            operation="resolve source revision",
        ).strip()
        try:
            commit = resolved.decode("ascii")
        except UnicodeDecodeError:
            raise WorkspaceError("Git returned an invalid commit ID") from None
        if COMMIT_RE.fullmatch(commit) is None:
            raise WorkspaceError("Git returned an invalid commit ID")

        archive = self._run(
            ["-C", str(repository), "archive", "--format=tar", commit],
            operation="archive source revision",
        )
        if len(archive) > MAX_ARCHIVE_BYTES:
            raise WorkspaceError("Git archive exceeds the workspace size limit")
        self._extract(archive, destination)
        return commit

    @staticmethod
    def _member_path(name: str) -> PurePosixPath:
        path = PurePosixPath(name)
        if (
            not name
            or path.is_absolute()
            or any(part in {"", ".", ".."} for part in path.parts)
            or path.parts[0] == ".git"
        ):
            raise WorkspaceError("Git archive contains an unsafe path")
        return path

    @staticmethod
    def _safe_link(path: PurePosixPath, target: str) -> None:
        link = PurePosixPath(target)
        if not target or link.is_absolute():
            raise WorkspaceError("Git archive contains an unsafe symlink")
        depth = len(path.parent.parts)
        for part in link.parts:
            if part == "..":
                depth -= 1
                if depth < 0:
                    raise WorkspaceError("Git archive contains an unsafe symlink")
            elif part not in {"", "."}:
                depth += 1

    @classmethod
    def _extract(cls, raw: bytes, destination: Path) -> None:
        try:
            with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
                members = archive.getmembers()
                if len(members) > MAX_ARCHIVE_MEMBERS:
                    raise WorkspaceError("Git archive contains too many files")
                paths: dict[PurePosixPath, tarfile.TarInfo] = {}
                symlinks: set[PurePosixPath] = set()
                total_size = 0
                for member in members:
                    path = cls._member_path(member.name)
                    if path in paths:
                        raise WorkspaceError("Git archive contains duplicate paths")
                    if not (member.isdir() or member.isfile() or member.issym()):
                        raise WorkspaceError(
                            "Git archive contains an unsupported file type"
                        )
                    if member.isfile():
                        total_size += member.size
                        if total_size > MAX_ARCHIVE_BYTES:
                            raise WorkspaceError(
                                "Git archive exceeds the workspace size limit"
                            )
                    if member.issym():
                        cls._safe_link(path, member.linkname)
                        symlinks.add(path)
                    paths[path] = member
                for path in paths:
                    if any(parent in symlinks for parent in path.parents):
                        raise WorkspaceError(
                            "Git archive places content beneath a symlink"
                        )
                try:
                    validate_symlink_graph(
                        {path: paths[path].linkname for path in symlinks}
                    )
                except UnsafeSymlinkError as exc:
                    raise WorkspaceError(
                        "Git archive contains an unsafe symlink"
                    ) from exc

                for path, member in paths.items():
                    target = destination.joinpath(*path.parts)
                    if member.isdir():
                        target.mkdir(mode=0o755, parents=True, exist_ok=True)
                    elif member.isfile():
                        target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
                        source = archive.extractfile(member)
                        if source is None:
                            raise WorkspaceError("Git archive file is unreadable")
                        with source, target.open("xb") as output:
                            shutil.copyfileobj(source, output)
                        target.chmod(0o755 if member.mode & 0o111 else 0o644)
                for path in symlinks:
                    member = paths[path]
                    target = destination.joinpath(*path.parts)
                    target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
                    target.symlink_to(member.linkname)
        except (tarfile.TarError, OSError) as exc:
            if isinstance(exc, WorkspaceError):
                raise
            raise WorkspaceError("Git returned an invalid archive") from exc


class WorkspaceManager:
    def __init__(self, root: Path, snapshotter: Snapshotter):
        self.root = Path(root).expanduser().absolute()
        self.snapshotter = snapshotter

    def ensure_root(self) -> Path:
        try:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            info = self.root.lstat()
        except OSError as exc:
            raise WorkspaceError(f"workspace root is unavailable: {self.root}") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise WorkspaceError("workspace root must be a private directory")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise WorkspaceError("workspace root is not owned by the current user")
        if info.st_mode & 0o077:
            raise WorkspaceError("workspace root must use mode 0700")
        return self.root

    @staticmethod
    def _repository(path: Path) -> Path:
        candidate = Path(path).expanduser().absolute()
        try:
            info = candidate.lstat()
        except OSError as exc:
            raise WorkspaceError(f"repository is unavailable: {candidate}") from exc
        if stat.S_ISLNK(info.st_mode):
            raise WorkspaceError("repository must not be a symlink")
        if not stat.S_ISDIR(info.st_mode):
            raise WorkspaceError("repository must be a directory")
        resolved = candidate.resolve(strict=True)
        if resolved == Path(resolved.anchor):
            raise WorkspaceError("repository must not be a filesystem root")
        return resolved

    @staticmethod
    def _validate_run_id(run_id: str) -> str:
        if not isinstance(run_id, str) or RUN_ID_RE.fullmatch(run_id) is None:
            raise WorkspaceError("invalid run identifier")
        return run_id

    def prepare(
        self, run_id: str, repository: Path, revision: str | None
    ) -> PreparedWorkspace:
        run_id = self._validate_run_id(run_id)
        source = self._repository(repository)
        root = self.ensure_root()
        destination = root / run_id
        try:
            destination.mkdir(mode=0o700)
        except FileExistsError:
            raise WorkspaceError("run workspace already exists") from None
        except OSError as exc:
            raise WorkspaceError("cannot create run workspace") from exc
        try:
            resolved = self.snapshotter.snapshot(source, revision, destination)
        except BaseException:
            shutil.rmtree(destination, ignore_errors=True)
            raise
        return PreparedWorkspace(path=destination, revision=resolved)

    def locate(self, run_id: str) -> Path | None:
        run_id = self._validate_run_id(run_id)
        destination = self.ensure_root() / run_id
        try:
            info = destination.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise WorkspaceError("cannot inspect run workspace") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise WorkspaceError("run workspace must be a directory, not a symlink")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise WorkspaceError("run workspace is not owned by the current user")
        return destination

    def cleanup(self, run_id: str) -> bool:
        run_id = self._validate_run_id(run_id)
        destination = self.locate(run_id)
        if destination is None:
            return False
        try:
            shutil.rmtree(destination)
        except OSError as exc:
            raise WorkspaceError("cannot remove run workspace") from exc
        return True
