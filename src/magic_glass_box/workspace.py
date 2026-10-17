from __future__ import annotations

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
from collections.abc import Callable, Iterator, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

from .path_safety import (
    MAX_PATH_BYTES,
    UnsafePathError,
    UnsafeSymlinkError,
    validate_repo_path,
    validate_symlink_graph,
)
from .path_safety import (
    MAX_PATH_DEPTH as _MAX_PATH_DEPTH,
)

MAX_PATH_DEPTH = _MAX_PATH_DEPTH

RUN_ID_RE = re.compile(r"^run_[0-9a-f]{32}$")
COMMIT_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
MAX_SNAPSHOT_BYTES = 512 * 1024 * 1024
MAX_TREE_ENTRIES = 100_000
MAX_TREE_BYTES = 32 * 1024 * 1024
MAX_STDERR_BYTES = 64 * 1024


class WorkspaceError(RuntimeError):
    """A per-run workspace could not be prepared without weakening isolation."""


@dataclass(frozen=True)
class PreparedWorkspace:
    path: Path
    revision: str
    gitlinks: tuple[str, ...] = ()


@dataclass(frozen=True)
class SnapshotResult:
    """The immutable revision and exact Gitlinks materialized by a snapshot."""

    revision: str
    gitlinks: tuple[str, ...] = ()


class Snapshotter(Protocol):
    def snapshot(
        self, repository: Path, revision: str | None, destination: Path
    ) -> SnapshotResult: ...


GitRunner = Callable[..., subprocess.CompletedProcess[bytes]]


@dataclass(frozen=True)
class _TreeEntry:
    path: PurePosixPath
    mode: str
    oid: str
    size: int


class _BatchReader:
    """Deadline-bound buffered reader for Git's cat-file batch protocol."""

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
            raise WorkspaceError("Git returned a truncated object batch")
        self.buffer.extend(chunk)

    def line(self, limit: int) -> bytes:
        while b"\n" not in self.buffer:
            if len(self.buffer) > limit:
                raise WorkspaceError("Git returned an invalid object batch")
            self._fill()
        end = self.buffer.index(b"\n") + 1
        if end > limit:
            raise WorkspaceError("Git returned an invalid object batch")
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


class GitSnapshotter:
    """Materialize raw tracked bytes from one immutable Git commit."""

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
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise WorkspaceError("Git command timeout must be positive")
        self.binary = Path(binary)
        self.runner = runner
        self.timeout = float(timeout)

    @staticmethod
    def _environment(private: Path) -> dict[str, str]:
        # Start fresh: inherited GIT_* values can redirect even rev-parse.
        return {
            "PATH": os.defpath,
            "LC_ALL": "C",
            "LANG": "C",
            "HOME": str(private),
            "XDG_CONFIG_HOME": str(private),
            "TMPDIR": str(private),
            "TEMP": str(private),
            "TMP": str(private),
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_NO_LAZY_FETCH": "1",
        }

    def _bounded_process(
        self, command: list[str], environment: dict[str, str], limit: int
    ) -> subprocess.CompletedProcess[bytes]:
        # Drain both pipes concurrently, aborting before accumulating unbounded
        # output. communicate()/capture_output alone cannot enforce this bound.
        with (
            subprocess.Popen(  # noqa: S603 - absolute configured Git binary
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=environment,
            ) as process,
            selectors.DefaultSelector() as selector,
        ):
            assert process.stdout is not None and process.stderr is not None
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            buffers = {"stdout": bytearray(), "stderr": bytearray()}
            deadline = time.monotonic() + self.timeout
            try:
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(command, self.timeout)
                    for key, _ in selector.select(remaining):
                        chunk = os.read(key.fd, 64 * 1024)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        name = key.data
                        ceiling = limit if name == "stdout" else MAX_STDERR_BYTES
                        if len(buffers[name]) + len(chunk) > ceiling:
                            raise WorkspaceError("Git command exceeds output limit")
                        buffers[name].extend(chunk)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, self.timeout)
                status = process.wait(timeout=remaining)
            except BaseException:
                process.kill()
                process.wait()
                raise
            return subprocess.CompletedProcess(
                command, status, bytes(buffers["stdout"]), bytes(buffers["stderr"])
            )

    def _run(
        self,
        arguments: list[str],
        *,
        operation: str,
        environment: dict[str, str],
        limit: int,
    ) -> bytes:
        command = self._command(arguments)
        try:
            if self.runner is None:
                completed = self._bounded_process(command, environment, limit)
            else:
                completed = self.runner(
                    command,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    check=False,
                    timeout=self.timeout,
                    env=environment,
                )
        except subprocess.TimeoutExpired:
            raise WorkspaceError(f"Git {operation} timed out") from None
        except OSError:
            raise WorkspaceError("cannot execute Git CLI") from None
        if not isinstance(completed.stdout, bytes) or not isinstance(
            completed.stderr, bytes
        ):
            raise WorkspaceError(f"Git returned invalid {operation} output")
        if len(completed.stdout) > limit or len(completed.stderr) > MAX_STDERR_BYTES:
            raise WorkspaceError("Git command exceeds output limit")
        if completed.returncode != 0:
            detail = completed.stderr.decode("utf-8", "replace").strip()
            suffix = f": {detail[:4096]}" if detail else ""
            raise WorkspaceError(f"Git cannot {operation}{suffix}")
        return completed.stdout

    def _command(self, arguments: list[str]) -> list[str]:
        return [
            str(self.binary),
            "-c",
            "core.hooksPath=" + os.devnull,
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.attributesFile=" + os.devnull,
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

    @staticmethod
    def _member_path(raw: bytes) -> PurePosixPath:
        try:
            name = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise WorkspaceError("Git tree contains an unsafe path") from None
        try:
            validate_repo_path(name)
        except UnsafePathError:
            raise WorkspaceError("Git tree contains an unsafe path") from None
        return PurePosixPath(name)

    @classmethod
    def _parse_tree(cls, raw: bytes, commit: str) -> list[_TreeEntry]:
        if raw and not raw.endswith(b"\0"):
            raise WorkspaceError("Git returned an invalid tree listing")
        records = raw[:-1].split(b"\0") if raw else []
        if len(records) > MAX_TREE_ENTRIES:
            raise WorkspaceError("Git tree contains too many entries")
        entries: dict[PurePosixPath, _TreeEntry] = {}
        total_size = 0
        modes = {
            "040000": "tree",
            "100644": "blob",
            "100755": "blob",
            "120000": "blob",
            "160000": "commit",
        }
        for record in records:
            try:
                header, path_raw = record.split(b"\t", 1)
                mode, kind, oid, advertised = header.decode("ascii").split()
            except (ValueError, UnicodeDecodeError):
                raise WorkspaceError("Git returned an invalid tree record") from None
            if mode not in modes:
                raise WorkspaceError("Git tree contains an unsupported mode")
            if (
                kind != modes[mode]
                or COMMIT_RE.fullmatch(oid) is None
                or len(oid) != len(commit)
            ):
                raise WorkspaceError("Git returned an invalid tree record")
            if kind == "blob":
                if (
                    not advertised.isascii()
                    or not advertised.isdecimal()
                    or len(advertised) > 12
                ):
                    raise WorkspaceError("Git tree contains an invalid blob size")
                size = int(advertised)
                total_size += size
                if total_size > MAX_SNAPSHOT_BYTES:
                    raise WorkspaceError("Git tree exceeds the workspace size limit")
                if mode == "120000" and size > MAX_PATH_BYTES:
                    raise WorkspaceError("Git tree contains an oversized symlink")
            elif advertised != "-":
                raise WorkspaceError("Git returned an invalid tree size")
            else:
                size = 0
            path = cls._member_path(path_raw)
            if path in entries:
                raise WorkspaceError("Git tree contains duplicate paths")
            entries[path] = _TreeEntry(path, mode, oid, size)
        for path in entries:
            for parent in path.parents:
                if parent == PurePosixPath("."):
                    continue
                ancestor = entries.get(parent)
                if ancestor is None:
                    raise WorkspaceError("Git tree omits a parent directory")
                if ancestor.mode != "040000":
                    raise WorkspaceError("Git tree places content beneath a leaf")
        return list(entries.values())

    @staticmethod
    def _stop_batch(process: subprocess.Popen[bytes]) -> None:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=0.2)
        except subprocess.TimeoutExpired:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            with suppress(subprocess.TimeoutExpired):
                process.wait(timeout=0.5)

    @staticmethod
    def _read_batch_blob(
        process: subprocess.Popen[bytes],
        reader: _BatchReader,
        entry: _TreeEntry,
        *,
        object_format: str,
        output: object | None = None,
        retain: bool = False,
    ) -> bytes | None:
        assert process.stdin is not None
        try:
            process.stdin.write(entry.oid.encode("ascii") + b"\n")
            process.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise WorkspaceError("Git object batch closed unexpectedly") from exc
        header = reader.line(256).removesuffix(b"\n")
        try:
            raw_id, raw_kind, raw_size = header.split(b" ")
            object_id = raw_id.decode("ascii")
            kind = raw_kind.decode("ascii")
            size_text = raw_size.decode("ascii")
        except (UnicodeDecodeError, ValueError):
            raise WorkspaceError("Git returned an invalid object batch") from None
        if (
            object_id != entry.oid
            or kind != "blob"
            or not size_text.isdecimal()
            or int(size_text) != entry.size
        ):
            raise WorkspaceError("Git returned an invalid object batch")
        digest = hashlib.new(object_format)
        digest.update(b"blob " + str(entry.size).encode("ascii") + b"\0")
        chunks: list[bytes] = []
        for chunk in reader.chunks(entry.size):
            digest.update(chunk)
            if output is not None and output.write(chunk) != len(chunk):  # type: ignore[attr-defined]
                raise WorkspaceError("cannot write source blob")
            if retain:
                chunks.append(chunk)
        if reader.exact(1) != b"\n" or digest.hexdigest() != entry.oid:
            raise WorkspaceError("Git returned an invalid blob size or hash")
        return b"".join(chunks) if retain else None

    def _materialize_batch(
        self,
        repository: Path,
        destination: Path,
        entries: Sequence[_TreeEntry],
        environment: dict[str, str],
        *,
        object_format: str,
    ) -> None:
        command = self._command(["-C", str(repository), "cat-file", "--batch"])
        with tempfile.TemporaryFile() as error_file, tempfile.TemporaryFile() as spool:
            try:
                process = subprocess.Popen(  # noqa: S603 - absolute configured Git
                    command,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=error_file,
                    env=environment,
                    start_new_session=True,
                )
            except OSError:
                raise WorkspaceError("cannot execute Git CLI") from None
            assert process.stdout is not None
            reader = _BatchReader(process.stdout, time.monotonic() + self.timeout)
            try:
                links: dict[PurePosixPath, str] = {}
                spooled: dict[PurePosixPath, tuple[int, int]] = {}
                for entry in entries:
                    if entry.mode != "120000":
                        continue
                    body = self._read_batch_blob(
                        process,
                        reader,
                        entry,
                        object_format=object_format,
                        retain=True,
                    )
                    assert body is not None
                    try:
                        links[entry.path] = body.decode("utf-8")
                    except UnicodeDecodeError:
                        raise WorkspaceError("Git contains an unsafe symlink") from None
                try:
                    validate_symlink_graph(links)
                except UnsafeSymlinkError as exc:
                    raise WorkspaceError("Git contains an unsafe symlink") from exc

                for entry in entries:
                    if entry.mode not in {"100644", "100755"}:
                        continue
                    offset = spool.tell()
                    self._read_batch_blob(
                        process,
                        reader,
                        entry,
                        object_format=object_format,
                        output=spool,
                    )
                    spooled[entry.path] = (offset, entry.size)
                assert process.stdin is not None
                process.stdin.close()
                remaining = reader.deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, self.timeout)
                status = process.wait(timeout=remaining)
                error_file.seek(0)
                stderr = error_file.read(MAX_STDERR_BYTES + 1)
                if len(stderr) > MAX_STDERR_BYTES:
                    raise WorkspaceError("Git command exceeds output limit")
                if status != 0:
                    detail = stderr.decode("utf-8", "replace").strip()
                    suffix = f": {detail[:4096]}" if detail else ""
                    raise WorkspaceError(f"Git cannot read source blobs{suffix}")

                # The hostile Git boundary is fully validated before the first
                # destination mutation. Regular blobs spool to a private
                # temporary file so this remains disk-backed and bounded by the
                # aggregate tree-size ceiling rather than process memory.
                directories = [e for e in entries if e.mode in {"040000", "160000"}]
                for entry in sorted(directories, key=lambda e: len(e.path.parts)):
                    directory = destination.joinpath(*entry.path.parts)
                    directory.mkdir(mode=0o755)
                    if entry.mode == "160000":
                        directory.chmod(0o555)
                for entry in entries:
                    if entry.mode not in {"100644", "100755"}:
                        continue
                    target = destination.joinpath(*entry.path.parts)
                    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
                    descriptor = os.open(target, flags, 0o600)
                    with os.fdopen(descriptor, "wb") as output:
                        offset, remaining_bytes = spooled[entry.path]
                        spool.seek(offset)
                        while remaining_bytes:
                            chunk = spool.read(min(64 * 1024, remaining_bytes))
                            if not chunk or output.write(chunk) != len(chunk):
                                raise WorkspaceError("cannot write source blob")
                            remaining_bytes -= len(chunk)
                        os.fchmod(
                            output.fileno(),
                            0o755 if entry.mode == "100755" else 0o644,
                        )
                for path, target in links.items():
                    destination.joinpath(*path.parts).symlink_to(target)
            except subprocess.TimeoutExpired:
                self._stop_batch(process)
                raise WorkspaceError("Git read source blobs timed out") from None
            except BaseException:
                self._stop_batch(process)
                raise
            finally:
                reader.close()
                for stream in (process.stdin, process.stdout):
                    if stream is not None and not stream.closed:
                        stream.close()

    def snapshot(
        self, repository: Path, revision: str | None, destination: Path
    ) -> SnapshotResult:
        requested = "HEAD" if revision is None else revision
        try:
            valid = (
                isinstance(requested, str)
                and bool(requested)
                and "\0" not in requested
                and len(requested.encode("utf-8")) <= 512
            )
        except UnicodeEncodeError:
            valid = False
        if not valid:
            raise WorkspaceError("source revision is invalid")
        try:
            info = destination.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise WorkspaceError("snapshot destination must be a directory")
            if any(destination.iterdir()):
                raise WorkspaceError("snapshot destination must be empty")
            with tempfile.TemporaryDirectory(prefix="mgb-snapshot-") as directory:
                environment = self._environment(Path(directory))
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
                    environment=environment,
                    limit=256,
                )
                try:
                    commit = resolved.removesuffix(b"\n").decode("ascii")
                except UnicodeDecodeError:
                    raise WorkspaceError("Git returned an invalid commit ID") from None
                if COMMIT_RE.fullmatch(commit) is None:
                    raise WorkspaceError("Git returned an invalid commit ID")
                raw = self._run(
                    [
                        "-C",
                        str(repository),
                        "ls-tree",
                        "-r",
                        "-t",
                        "-l",
                        "-z",
                        "--full-tree",
                        commit,
                    ],
                    operation="list source tree",
                    environment=environment,
                    limit=MAX_TREE_BYTES,
                )
                entries = self._parse_tree(raw, commit)
                gitlinks = tuple(
                    sorted(
                        entry.path.as_posix()
                        for entry in entries
                        if entry.mode == "160000"
                    )
                )
                if self.runner is None:
                    self._materialize_batch(
                        repository,
                        destination,
                        entries,
                        environment,
                        object_format="sha1" if len(commit) == 40 else "sha256",
                    )
                    return SnapshotResult(commit, gitlinks)
                bodies: dict[PurePosixPath, bytes] = {}
                links: dict[PurePosixPath, str] = {}
                for entry in entries:
                    if entry.mode in {"040000", "160000"}:
                        continue
                    body = self._run(
                        ["-C", str(repository), "cat-file", "blob", entry.oid],
                        operation="read source blob",
                        environment=environment,
                        limit=entry.size + 1,
                    )
                    digest = hashlib.new("sha1" if len(commit) == 40 else "sha256")
                    digest.update(b"blob " + str(len(body)).encode("ascii") + b"\0")
                    digest.update(body)
                    if len(body) != entry.size or digest.hexdigest() != entry.oid:
                        raise WorkspaceError(
                            "Git returned an invalid blob size or hash"
                        )
                    bodies[entry.path] = body
                    if entry.mode == "120000":
                        try:
                            links[entry.path] = body.decode("utf-8")
                        except UnicodeDecodeError:
                            raise WorkspaceError(
                                "Git contains an unsafe symlink"
                            ) from None
                try:
                    validate_symlink_graph(links)
                except UnsafeSymlinkError as exc:
                    raise WorkspaceError("Git contains an unsafe symlink") from exc
                # All identities, paths, and the complete link graph are checked
                # before writing. Gitlinks become empty directories, never reads.
                directories = [e for e in entries if e.mode in {"040000", "160000"}]
                for entry in sorted(directories, key=lambda e: len(e.path.parts)):
                    directory = destination.joinpath(*entry.path.parts)
                    directory.mkdir(mode=0o755)
                    if entry.mode == "160000":
                        directory.chmod(0o555)
                for entry in entries:
                    if entry.mode not in {"100644", "100755"}:
                        continue
                    target = destination.joinpath(*entry.path.parts)
                    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
                    descriptor = os.open(target, flags, 0o600)
                    with os.fdopen(descriptor, "wb") as output:
                        output.write(bodies[entry.path])
                        os.fchmod(
                            output.fileno(), 0o755 if entry.mode == "100755" else 0o644
                        )
                for path, target in links.items():
                    destination.joinpath(*path.parts).symlink_to(target)
                return SnapshotResult(commit, gitlinks)
        except OSError as exc:
            raise WorkspaceError("cannot materialize Git tree snapshot") from exc


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
        source = self._repository(repository)
        return self.prepare_materialized(
            run_id,
            lambda destination: self.snapshotter.snapshot(
                source, revision, destination
            ),
        )

    def prepare_materialized(
        self,
        run_id: str,
        materialize: Callable[[Path], SnapshotResult],
    ) -> PreparedWorkspace:
        """Create a run workspace from an already trusted snapshot source."""

        run_id = self._validate_run_id(run_id)
        root = self.ensure_root()
        destination = root / run_id
        try:
            destination.mkdir(mode=0o700)
        except FileExistsError:
            raise WorkspaceError("run workspace already exists") from None
        except OSError as exc:
            raise WorkspaceError("cannot create run workspace") from exc
        try:
            snapshot = materialize(destination)
        except BaseException:
            shutil.rmtree(destination, ignore_errors=True)
            raise
        if not isinstance(snapshot, SnapshotResult):
            shutil.rmtree(destination, ignore_errors=True)
            raise WorkspaceError("workspace materializer returned an invalid snapshot")
        return PreparedWorkspace(
            path=destination,
            revision=snapshot.revision,
            gitlinks=snapshot.gitlinks,
        )

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
            self._make_directories_removable(destination)
            shutil.rmtree(destination)
        except OSError as exc:
            raise WorkspaceError("cannot remove run workspace") from exc
        return True

    @staticmethod
    def _make_directories_removable(destination: Path) -> None:
        def make_owner_writable(path: Path) -> None:
            info = path.lstat()
            if not stat.S_ISDIR(info.st_mode):
                return
            required = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
            mode = stat.S_IMODE(info.st_mode)
            if mode & required != required:
                path.chmod(mode | required)

        def raise_walk_error(error: OSError) -> None:
            raise error

        make_owner_writable(destination)
        for current, directories, _files in os.walk(
            destination,
            topdown=True,
            followlinks=False,
            onerror=raise_walk_error,
        ):
            make_owner_writable(Path(current))
            for name in directories:
                make_owner_writable(Path(current) / name)
