from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
import time
import unittest
import zlib
from pathlib import Path
from unittest import mock

import magic_glass_box.workspace as workspace_module
from magic_glass_box.workspace import (
    GitSnapshotter,
    SnapshotResult,
    WorkspaceError,
    WorkspaceManager,
)

RUN_ID = "run_" + "1" * 32
COMMIT = "a" * 40


class FakeSnapshotter:
    def __init__(self, *, error: Exception | None = None):
        self.error = error
        self.calls: list[tuple[Path, str | None, Path]] = []

    def snapshot(
        self, repository: Path, revision: str | None, destination: Path
    ) -> SnapshotResult:
        self.calls.append((repository, revision, destination))
        if self.error is not None:
            raise self.error
        (destination / "tracked.txt").write_text("snapshot", encoding="utf-8")
        return SnapshotResult(COMMIT)


class WorkspaceManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        os.chmod(self.base, 0o700)
        self.repository = self.base / "repository"
        self.repository.mkdir(mode=0o700)
        self.root = self.base / "workspaces"

    def test_prepare_creates_an_isolated_workspace_and_cleanup_is_idempotent(
        self,
    ) -> None:
        snapshotter = FakeSnapshotter()
        manager = WorkspaceManager(self.root, snapshotter)

        prepared = manager.prepare(RUN_ID, self.repository, "main")

        self.assertEqual(prepared.path, self.root / RUN_ID)
        self.assertEqual(prepared.revision, COMMIT)
        self.assertEqual((prepared.path / "tracked.txt").read_text(), "snapshot")
        self.assertEqual(
            snapshotter.calls,
            [(self.repository.resolve(), "main", prepared.path)],
        )
        self.assertEqual(prepared.path.stat().st_mode & 0o777, 0o700)
        (prepared.path / "tracked.txt").write_text("agent edit", encoding="utf-8")
        self.assertFalse((self.repository / "tracked.txt").exists())
        self.assertTrue(manager.cleanup(RUN_ID))
        self.assertFalse(prepared.path.exists())
        self.assertFalse(manager.cleanup(RUN_ID))

    def test_cleanup_removes_worker_created_read_only_directories(self) -> None:
        manager = WorkspaceManager(self.root, FakeSnapshotter())
        prepared = manager.prepare(RUN_ID, self.repository, None)
        package = prepared.path / "vendor" / "pkg"
        package.mkdir(parents=True)
        (package / "module.py").write_text("worker output", encoding="utf-8")
        os.chmod(package, 0o500)
        os.chmod(package.parent, 0o500)

        self.assertTrue(manager.cleanup(RUN_ID))
        self.assertFalse(prepared.path.exists())

    def test_prepare_does_not_infer_gitlinks_from_ordinary_empty_directories(
        self,
    ) -> None:
        class EmptyDirectorySnapshotter:
            def snapshot(
                self,
                repository: Path,
                revision: str | None,
                destination: Path,
            ) -> SnapshotResult:
                del repository, revision
                (destination / "ordinary-empty").mkdir()
                return SnapshotResult(COMMIT)

        prepared = WorkspaceManager(self.root, EmptyDirectorySnapshotter()).prepare(
            RUN_ID, self.repository, None
        )

        self.assertEqual(prepared.gitlinks, ())

    def test_prepare_rejects_unsafe_sources_and_existing_destination(self) -> None:
        manager = WorkspaceManager(self.root, FakeSnapshotter())
        missing = self.base / "missing"
        file_source = self.base / "file"
        file_source.write_text("not a directory", encoding="utf-8")
        link = self.base / "repository-link"
        link.symlink_to(self.repository, target_is_directory=True)

        for run_id, source, message in (
            ("run_bad", self.repository, "run identifier"),
            (RUN_ID, missing, "unavailable"),
            (RUN_ID, file_source, "directory"),
            (RUN_ID, link, "symlink"),
            (RUN_ID, Path(source_anchor(self.repository)), "filesystem root"),
        ):
            with (
                self.subTest(source=source),
                self.assertRaisesRegex(WorkspaceError, message),
            ):
                manager.prepare(run_id, source, None)

        manager.ensure_root()
        destination = self.root / RUN_ID
        destination.mkdir(mode=0o700)
        with self.assertRaisesRegex(WorkspaceError, "already exists"):
            manager.prepare(RUN_ID, self.repository, None)

    def test_failed_snapshot_removes_staging_directory(self) -> None:
        manager = WorkspaceManager(
            self.root, FakeSnapshotter(error=RuntimeError("archive failed"))
        )
        with self.assertRaisesRegex(RuntimeError, "archive failed"):
            manager.prepare(RUN_ID, self.repository, None)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_failed_snapshot_cleanup_cannot_mask_the_primary_error(self) -> None:
        manager = WorkspaceManager(
            self.root, FakeSnapshotter(error=RuntimeError("snapshot failed"))
        )

        def remove_only_when_errors_are_ignored(
            path: Path, *, ignore_errors: bool = False
        ) -> None:
            self.assertEqual(path, self.root / RUN_ID)
            if not ignore_errors:
                raise OSError("cleanup failed")

        with (
            mock.patch(
                "magic_glass_box.workspace.shutil.rmtree",
                side_effect=remove_only_when_errors_are_ignored,
            ),
            self.assertRaisesRegex(RuntimeError, "snapshot failed"),
        ):
            manager.prepare(RUN_ID, self.repository, None)

    def test_cleanup_refuses_a_workspace_symlink(self) -> None:
        manager = WorkspaceManager(self.root, FakeSnapshotter())
        manager.ensure_root()
        (self.root / RUN_ID).symlink_to(self.repository, target_is_directory=True)
        with self.assertRaisesRegex(WorkspaceError, "symlink"):
            manager.cleanup(RUN_ID)
        self.assertTrue(self.repository.exists())

    def test_workspace_root_and_cleanup_reject_unsafe_filesystem_state(self) -> None:
        self.root.write_text("not a directory", encoding="utf-8")
        manager = WorkspaceManager(self.root, FakeSnapshotter())
        with self.assertRaisesRegex(WorkspaceError, "unavailable"):
            manager.ensure_root()
        self.root.unlink()

        self.root.mkdir(mode=0o755)
        with self.assertRaisesRegex(WorkspaceError, "0700"):
            manager.ensure_root()
        os.chmod(self.root, 0o700)

        destination = self.root / RUN_ID
        destination.write_text("not a directory", encoding="utf-8")
        with self.assertRaisesRegex(WorkspaceError, "directory"):
            manager.cleanup(RUN_ID)
        destination.unlink()
        destination.mkdir(mode=0o700)
        with (
            mock.patch.object(manager, "ensure_root", return_value=self.root),
            mock.patch(
                "magic_glass_box.workspace.os.getuid", return_value=os.getuid() + 1
            ),
            self.assertRaisesRegex(WorkspaceError, "run workspace is not owned"),
        ):
            manager.locate(RUN_ID)
        with (
            mock.patch(
                "magic_glass_box.workspace.shutil.rmtree",
                side_effect=OSError("busy"),
            ),
            self.assertRaisesRegex(WorkspaceError, "cannot remove"),
        ):
            manager.cleanup(RUN_ID)

        with (
            mock.patch(
                "magic_glass_box.workspace.os.getuid", return_value=os.getuid() + 1
            ),
            self.assertRaisesRegex(WorkspaceError, "root is not owned"),
        ):
            manager.ensure_root()

    def test_workspace_root_creation_includes_missing_private_parents(self) -> None:
        nested_root = self.base / "private" / "nested" / "workspaces"

        created = WorkspaceManager(nested_root, FakeSnapshotter()).ensure_root()

        self.assertEqual(created, nested_root)
        self.assertTrue(nested_root.is_dir())
        self.assertEqual(nested_root.stat().st_mode & 0o777, 0o700)

    def test_workspace_root_replacement_after_mkdir_is_rejected(self) -> None:
        manager = WorkspaceManager(self.root, FakeSnapshotter())
        real_mkdir = Path.mkdir
        replaced = False

        def replace_created_root(
            path: Path,
            mode: int = 0o777,
            parents: bool = False,
            exist_ok: bool = False,
        ) -> None:
            nonlocal replaced
            real_mkdir(path, mode=mode, parents=parents, exist_ok=exist_ok)
            if path == self.root and not replaced:
                replaced = True
                path.rmdir()
                path.write_bytes(b"not a workspace directory")
                path.chmod(0o600)

        with (
            mock.patch.object(
                Path, "mkdir", autospec=True, side_effect=replace_created_root
            ),
            self.assertRaisesRegex(WorkspaceError, "private directory"),
        ):
            manager.ensure_root()

        self.assertTrue(replaced)
        self.assertTrue(self.root.is_file())


def source_anchor(path: Path) -> str:
    return path.resolve().anchor


def blob_id(body: bytes, *, sha256: bool = False) -> str:
    raw = b"blob " + str(len(body)).encode() + b"\0" + body
    return hashlib.new("sha256" if sha256 else "sha1", raw).hexdigest()


def tree_entry(
    path: str, body: bytes = b"", *, mode: str = "100644", sha256: bool = False
) -> bytes:
    kind = "tree" if mode == "040000" else "commit" if mode == "160000" else "blob"
    size = "-" if kind != "blob" else str(len(body))
    return (
        f"{mode} {kind} {blob_id(body, sha256=sha256)} {size}\t{path}".encode() + b"\0"
    )


class GitSnapshotterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repository = self.root / "repository"
        self.repository.mkdir()
        self.destination = self.root / "destination"
        self.destination.mkdir()

    def snapshotter(self, tree: bytes, blobs: list[bytes] = (), commit: str = COMMIT):
        responses = [commit.encode() + b"\n", tree, *blobs]
        self.calls: list[tuple[list[str], dict]] = []

        def run(args, **kwargs):
            self.calls.append((args, kwargs))
            return subprocess.CompletedProcess(args, 0, responses.pop(0), b"")

        return GitSnapshotter(binary=Path("/usr/bin/git"), runner=run)

    def test_constructor_discovers_git_and_rejects_unsafe_settings(self) -> None:
        with mock.patch(
            "magic_glass_box.workspace.shutil.which", return_value="/usr/bin/git"
        ) as which:
            snapshotter = GitSnapshotter()
        which.assert_called_once_with("git")
        self.assertEqual(snapshotter.binary, Path("/usr/bin/git"))
        self.assertEqual(snapshotter.timeout, 60.0)
        with (
            mock.patch("magic_glass_box.workspace.shutil.which", return_value=None),
            self.assertRaisesRegex(WorkspaceError, "not found"),
        ):
            GitSnapshotter()
        with self.assertRaisesRegex(WorkspaceError, "absolute"):
            GitSnapshotter(binary=Path("git"))
        for timeout in (0, -1, True, "60", float("inf"), float("nan")):
            with self.subTest(timeout=timeout), self.assertRaises(WorkspaceError):
                GitSnapshotter(binary=Path("/usr/bin/git"), timeout=timeout)

    def test_raw_files_executable_symlink_directory_and_gitlink(self) -> None:
        body = b"raw\r\n$Id$\n"
        tree = (
            tree_entry("src", mode="040000")
            + tree_entry("src/main", body, mode="100755")
            + tree_entry("current", b"src/main", mode="120000")
            + tree_entry("module", mode="160000")
        )
        snapshotter = self.snapshotter(tree, [body, b"src/main"])
        self.assertEqual(
            snapshotter.snapshot(self.repository, "main", self.destination),
            SnapshotResult(COMMIT, ("module",)),
        )
        self.assertEqual((self.destination / "src/main").read_bytes(), body)
        self.assertEqual((self.destination / "src/main").stat().st_mode & 0o777, 0o755)
        self.assertEqual((self.destination / "src").stat().st_mode & 0o777, 0o755)
        self.assertEqual(os.readlink(self.destination / "current"), "src/main")
        self.assertEqual((self.destination / "module").stat().st_mode & 0o777, 0o555)
        self.assertEqual(list((self.destination / "module").iterdir()), [])
        self.assertIn("main^{commit}", self.calls[0][0])
        self.assertIn("ls-tree", self.calls[1][0])
        self.assertTrue(all("archive" not in args for args, _ in self.calls))

    def test_sha256_blobs_and_empty_tree(self) -> None:
        body = b"sha256"
        snapshotter = self.snapshotter(
            tree_entry("file", body, sha256=True), [body], "b" * 64
        )
        self.assertEqual(
            snapshotter.snapshot(self.repository, None, self.destination),
            SnapshotResult("b" * 64),
        )
        self.assertEqual((self.destination / "file").read_bytes(), body)
        self.assertIn("HEAD^{commit}", self.calls[0][0])
        empty_destination = self.root / "empty-destination"
        empty_destination.mkdir()
        self.snapshotter(b"").snapshot(self.repository, None, empty_destination)

    def test_regular_files_are_created_exclusively_without_following_symlinks(
        self,
    ) -> None:
        body = b"raw file"
        target = self.destination / "file"
        opens: list[tuple[Path, int, int]] = []
        original_open = os.open

        def record_open(
            path: str | bytes | os.PathLike[str],
            flags: int,
            mode: int = 0o777,
            **kwargs: object,
        ):
            if Path(path) == target:
                opens.append((Path(path), flags, mode))
            return original_open(path, flags, mode, **kwargs)  # type: ignore[arg-type]

        with mock.patch.object(workspace_module.os, "open", side_effect=record_open):
            self.snapshotter(tree_entry("file", body), [body]).snapshot(
                self.repository, None, self.destination
            )

        self.assertEqual(
            opens,
            [
                (
                    target,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                )
            ],
        )
        self.assertEqual(target.read_bytes(), body)
        self.assertEqual(target.stat().st_mode & 0o777, 0o644)

    def test_malformed_tree_records_and_unsafe_paths_fail_before_writes(self) -> None:
        cases = [
            (b"no framing", "tree"),
            (b"100644 blob short 1\tfile\0", "tree"),
            (b"100644 blob " + b"a" * 64 + b" 1\tfile\0", "tree"),
            (b"100600 blob " + b"a" * 40 + b" 1\tfile\0", "mode"),
            (b"100644 tree " + b"a" * 40 + b" -\tfile\0", "tree"),
            (b"100644 blob " + b"a" * 40 + b" -1\tfile\0", "size"),
            (tree_entry("duplicate") * 2, "duplicate"),
            (tree_entry("leaf") + tree_entry("leaf/child"), "beneath"),
            (tree_entry("missing/child"), "directory"),
            (tree_entry("module", mode="160000") + tree_entry("module/x"), "beneath"),
        ]
        for path in (
            "../escape",
            "/absolute",
            ".git/config",
            "dir/.GiT/config",
            "a//b",
            "a/./b",
            ".",
            "trailing/",
            "",
        ):
            cases.append((tree_entry(path), "unsafe path"))
        cases.append((tree_entry("file").replace(b"file", b"\xff"), "unsafe path"))
        for tree, message in cases:
            with (
                self.subTest(tree=tree),
                self.assertRaisesRegex(WorkspaceError, message),
            ):
                self.snapshotter(tree).snapshot(self.repository, None, self.destination)
            self.assertEqual(list(self.destination.iterdir()), [])

    def test_tree_path_byte_and_depth_limits_accept_only_the_exact_boundary(
        self,
    ) -> None:
        byte_boundary = b"a" * workspace_module.MAX_PATH_BYTES
        depth_boundary = b"/".join([b"a"] * workspace_module.MAX_PATH_DEPTH)

        self.assertEqual(
            GitSnapshotter._member_path(byte_boundary).as_posix(),
            byte_boundary.decode("ascii"),
        )
        self.assertEqual(
            len(GitSnapshotter._member_path(depth_boundary).parts),
            workspace_module.MAX_PATH_DEPTH,
        )
        for raw in (byte_boundary + b"a", depth_boundary + b"/a"):
            with (
                self.subTest(length=len(raw)),
                self.assertRaisesRegex(WorkspaceError, "unsafe path"),
            ):
                GitSnapshotter._member_path(raw)

    def test_size_entry_and_listing_limits(self) -> None:
        for constant, value, tree, message in (
            ("MAX_SNAPSHOT_BYTES", 1, tree_entry("file", b"xx"), "size limit"),
            ("MAX_TREE_ENTRIES", 0, tree_entry("file"), "too many"),
            ("MAX_TREE_BYTES", 1, tree_entry("file"), "output limit"),
        ):
            with (
                self.subTest(constant=constant),
                mock.patch(f"magic_glass_box.workspace.{constant}", value, create=True),
                self.assertRaisesRegex(WorkspaceError, message),
            ):
                self.snapshotter(tree).snapshot(self.repository, None, self.destination)
            self.assertEqual(list(self.destination.iterdir()), [])

    def test_tree_parser_enforces_exact_and_aggregate_resource_boundaries(
        self,
    ) -> None:
        with mock.patch.object(workspace_module, "MAX_TREE_ENTRIES", 1):
            self.assertEqual(
                len(GitSnapshotter._parse_tree(tree_entry("one"), COMMIT)),
                1,
            )
            with self.assertRaisesRegex(WorkspaceError, "too many"):
                GitSnapshotter._parse_tree(
                    tree_entry("one") + tree_entry("two"), COMMIT
                )

        with mock.patch.object(workspace_module, "MAX_SNAPSHOT_BYTES", 2):
            exact = GitSnapshotter._parse_tree(tree_entry("one", b"xx"), COMMIT)
            self.assertEqual(exact[0].size, 2)
            with self.assertRaisesRegex(WorkspaceError, "size limit"):
                GitSnapshotter._parse_tree(
                    tree_entry("one", b"xx") + tree_entry("two", b"xx"),
                    COMMIT,
                )

        with mock.patch.object(workspace_module, "MAX_PATH_BYTES", 2):
            exact_link = GitSnapshotter._parse_tree(
                tree_entry("l", b"xx", mode="120000"), COMMIT
            )
            self.assertEqual(exact_link[0].size, 2)
            with self.assertRaisesRegex(WorkspaceError, "oversized symlink"):
                GitSnapshotter._parse_tree(
                    tree_entry("l", b"xxx", mode="120000"), COMMIT
                )

        exact_digits = b"100644 blob " + b"a" * 40 + b" 000000000000\tfile\0"
        self.assertEqual(
            GitSnapshotter._parse_tree(exact_digits, COMMIT)[0].size,
            0,
        )
        with self.assertRaisesRegex(WorkspaceError, "invalid blob size"):
            GitSnapshotter._parse_tree(
                exact_digits.replace(b"000000000000", b"0000000000000"),
                COMMIT,
            )

        framed = GitSnapshotter._parse_tree(tree_entry("tab\tpath", b"x"), COMMIT)
        self.assertEqual(framed[0].path.as_posix(), "tab\tpath")

        structural = GitSnapshotter._parse_tree(
            tree_entry("directory", mode="040000")
            + tree_entry("gitlink", mode="160000"),
            COMMIT,
        )
        self.assertEqual(
            [
                (entry.path.as_posix(), type(entry.size), entry.size)
                for entry in structural
            ],
            [("directory", int, 0), ("gitlink", int, 0)],
        )

    def test_corrupt_blob_size_and_digest_are_rejected(self) -> None:
        for body in (b"x", b"bad"):
            with (
                self.subTest(body=body),
                self.assertRaisesRegex(WorkspaceError, "blob"),
            ):
                self.snapshotter(tree_entry("file", b"raw"), [body]).snapshot(
                    self.repository, None, self.destination
                )
            self.assertEqual(list(self.destination.iterdir()), [])

    def test_unsafe_symlinks_and_graph_are_rejected_before_writes(self) -> None:
        for targets in (
            {"link": b"/outside"},
            {"link": b"../../outside"},
            {"link": b"\xff"},
            {"link": b""},
            {"link": b"bad\0target"},
            {"evil/x": b"..", "evil/y": b"x/.."},
            {"x": b"y", "y": b"x"},
        ):
            tree = tree_entry("evil", mode="040000") if "evil/x" in targets else b""
            tree += b"".join(
                tree_entry(path, body, mode="120000") for path, body in targets.items()
            )
            with (
                self.subTest(targets=targets),
                self.assertRaisesRegex(WorkspaceError, "symlink"),
            ):
                self.snapshotter(tree, list(targets.values())).snapshot(
                    self.repository, None, self.destination
                )
            self.assertEqual(list(self.destination.iterdir()), [])

    def test_invalid_revision_and_command_failures(self) -> None:
        for revision in ("", "bad\0revision", "x" * 513, 7, "\ud800"):
            with (
                self.subTest(revision=repr(revision)),
                self.assertRaises(WorkspaceError),
            ):
                self.snapshotter(b"").snapshot(
                    self.repository, revision, self.destination
                )
        boundary_snapshotter = self.snapshotter(b"")
        self.assertEqual(
            boundary_snapshotter.snapshot(self.repository, "x" * 512, self.destination),
            SnapshotResult(COMMIT),
        )
        self.assertIn(("x" * 512) + "^{commit}", self.calls[0][0])
        for failure in (
            subprocess.TimeoutExpired(["git"], 1),
            OSError("missing"),
            subprocess.CompletedProcess([], 1, b"", b"missing object"),
            subprocess.CompletedProcess([], 0, b"\xff\n", b""),
            subprocess.CompletedProcess([], 0, "wrong type", b""),
        ):
            runner = mock.Mock()
            if isinstance(failure, subprocess.CompletedProcess):
                runner.return_value = failure
            else:
                runner.side_effect = failure
            with self.subTest(failure=failure), self.assertRaises(WorkspaceError):
                GitSnapshotter(binary=Path("/usr/bin/git"), runner=runner).snapshot(
                    self.repository, None, self.destination
                )

    def test_missing_blob_and_blob_timeout(self) -> None:
        for failure in (
            subprocess.CompletedProcess([], 1, b"", b"missing blob"),
            subprocess.TimeoutExpired(["git"], 1),
        ):
            responses = [
                subprocess.CompletedProcess([], 0, COMMIT.encode() + b"\n", b""),
                subprocess.CompletedProcess([], 0, tree_entry("file", b"x"), b""),
                failure,
            ]

            def run(*args, responses=responses, **kwargs):
                result = responses.pop(0)
                if isinstance(result, BaseException):
                    raise result
                return result

            with self.subTest(failure=failure), self.assertRaises(WorkspaceError):
                GitSnapshotter(binary=Path("/usr/bin/git"), runner=run).snapshot(
                    self.repository, None, self.destination
                )

    def test_fresh_git_environment_and_configuration_controls(self) -> None:
        snapshotter = self.snapshotter(b"")
        with mock.patch.dict(
            os.environ,
            {"GIT_DIR": "/hostile", "GIT_CONFIG_COUNT": "1", "EVIL": "yes"},
        ):
            snapshotter.snapshot(self.repository, None, self.destination)
        for args, kwargs in self.calls:
            environment = kwargs["env"]
            private = environment["HOME"]
            self.assertTrue(Path(private).is_absolute())
            self.assertNotEqual(private, "None")
            self.assertEqual(
                kwargs,
                {
                    "stdin": subprocess.DEVNULL,
                    "capture_output": True,
                    "check": False,
                    "timeout": 60.0,
                    "env": environment,
                },
            )
            self.assertEqual(
                environment,
                {
                    "PATH": os.defpath,
                    "LC_ALL": "C",
                    "LANG": "C",
                    "HOME": private,
                    "XDG_CONFIG_HOME": private,
                    "TMPDIR": private,
                    "TEMP": private,
                    "TMP": private,
                    "GIT_CONFIG_GLOBAL": os.devnull,
                    "GIT_CONFIG_SYSTEM": os.devnull,
                    "GIT_CONFIG_NOSYSTEM": "1",
                    "GIT_ATTR_NOSYSTEM": "1",
                    "GIT_TERMINAL_PROMPT": "0",
                    "GIT_NO_REPLACE_OBJECTS": "1",
                    "GIT_OPTIONAL_LOCKS": "0",
                    "GIT_NO_LAZY_FETCH": "1",
                },
            )
            self.assertEqual(
                args[:17],
                [
                    "/usr/bin/git",
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
                ],
            )

    def test_real_process_reads_fixed_chunks_and_closes_streams(self) -> None:
        snapshotter = GitSnapshotter(timeout=2)
        environment = snapshotter._environment(self.root)
        read_sizes: list[int] = []
        processes: list[subprocess.Popen[bytes]] = []
        popen_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        original_read = os.read
        original_popen = subprocess.Popen

        def record_read(descriptor: int, size: int) -> bytes:
            read_sizes.append(size)
            return original_read(descriptor, size)

        def capture_process(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
            popen_calls.append((args, kwargs))
            process = original_popen(*args, **kwargs)  # type: ignore[arg-type]
            processes.append(process)
            return process

        command = [
            sys.executable,
            "-c",
            "import os; os.write(1, b'x' * (128 * 1024 + 1))",
        ]
        with (
            mock.patch.object(workspace_module.os, "read", side_effect=record_read),
            mock.patch.object(
                workspace_module.subprocess,
                "Popen",
                side_effect=capture_process,
            ),
        ):
            completed = snapshotter._bounded_process(
                command, environment, 128 * 1024 + 1
            )

        self.assertEqual(completed.args, command)
        self.assertEqual(completed.stdout, b"x" * (128 * 1024 + 1))
        self.assertEqual(
            popen_calls,
            [
                (
                    (command,),
                    {
                        "stdin": subprocess.DEVNULL,
                        "stdout": subprocess.PIPE,
                        "stderr": subprocess.PIPE,
                        "env": environment,
                    },
                )
            ],
        )
        self.assertIn(64 * 1024, read_sizes)
        self.assertLessEqual(max(read_sizes), 64 * 1024)
        self.assertEqual(len(processes), 1)
        self.assertTrue(
            all(
                stream is None or stream.closed
                for stream in (processes[0].stdout, processes[0].stderr)
            )
        )

    def test_real_process_post_eof_wait_obeys_the_same_deadline(self) -> None:
        snapshotter = GitSnapshotter(timeout=0.1)
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            snapshotter._bounded_process(
                [
                    sys.executable,
                    "-c",
                    "import os, time; os.close(1); os.close(2); time.sleep(2)",
                ],
                snapshotter._environment(self.root),
                16,
            )
        self.assertLess(time.monotonic() - started, 1)

    def test_bounded_process_treats_exact_deadline_as_expired(self) -> None:
        class DeterministicSelector:
            def __init__(self) -> None:
                self.members: dict[object, mock.Mock] = {}
                self.select_calls = 0

            def __enter__(self):
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def register(self, fileobj: object, _event: int, name: str) -> None:
                self.members[fileobj] = mock.Mock(
                    fd=len(self.members) + 10,
                    fileobj=fileobj,
                    data=name,
                )

            def unregister(self, fileobj: object) -> None:
                self.members.pop(fileobj)

            def get_map(self) -> dict[object, mock.Mock]:
                return self.members

            def select(self, _timeout: float) -> list[tuple[mock.Mock, int]]:
                self.select_calls += 1
                return [
                    (key, workspace_module.selectors.EVENT_READ)
                    for key in self.members.values()
                ]

        def process() -> mock.MagicMock:
            child = mock.MagicMock()
            child.stdout = object()
            child.stderr = object()
            child.__enter__.return_value = child
            child.__exit__.return_value = None
            child.wait.return_value = 0
            return child

        snapshotter = GitSnapshotter(binary=Path("/usr/bin/git"), timeout=1)
        environment = snapshotter._environment(self.root)

        before_select = DeterministicSelector()
        with (
            mock.patch.object(
                workspace_module.subprocess, "Popen", return_value=process()
            ),
            mock.patch.object(
                workspace_module.selectors,
                "DefaultSelector",
                return_value=before_select,
            ),
            mock.patch.object(workspace_module.os, "read", return_value=b""),
            mock.patch.object(
                workspace_module.time, "monotonic", side_effect=(10, 11, 12)
            ),
            self.assertRaises(subprocess.TimeoutExpired),
        ):
            snapshotter._bounded_process(["git"], environment, 1)
        self.assertEqual(before_select.select_calls, 0)

        after_eof = DeterministicSelector()
        child = process()
        with (
            mock.patch.object(workspace_module.subprocess, "Popen", return_value=child),
            mock.patch.object(
                workspace_module.selectors,
                "DefaultSelector",
                return_value=after_eof,
            ),
            mock.patch.object(workspace_module.os, "read", return_value=b""),
            mock.patch.object(
                workspace_module.time,
                "monotonic",
                side_effect=(10, 10.5, 11),
            ),
            self.assertRaises(subprocess.TimeoutExpired),
        ):
            snapshotter._bounded_process(["git"], environment, 1)
        self.assertEqual(after_eof.select_calls, 1)
        child.wait.assert_called_once_with()

    def test_run_caps_untrusted_diagnostics_and_honors_exact_output_limits(
        self,
    ) -> None:
        environment = GitSnapshotter._environment(self.root)
        stderr = b"\xff" + b"x" * 5000 + b"\n"
        runner = mock.Mock(return_value=subprocess.CompletedProcess([], 1, b"", stderr))
        snapshotter = GitSnapshotter(binary=Path("/usr/bin/git"), runner=runner)

        with self.assertRaises(WorkspaceError) as caught:
            snapshotter._run(
                ["version"],
                operation="inspect the failure",
                environment=environment,
                limit=64,
            )
        detail = stderr.decode("utf-8", "replace").strip()
        self.assertEqual(
            str(caught.exception),
            f"Git cannot inspect the failure: {detail[:4096]}",
        )

        runner.return_value = subprocess.CompletedProcess([], 1, b"", b"")
        with self.assertRaises(WorkspaceError) as empty_caught:
            snapshotter._run(
                ["version"],
                operation="inspect the empty failure",
                environment=environment,
                limit=64,
            )
        self.assertEqual(
            str(empty_caught.exception),
            "Git cannot inspect the empty failure",
        )

        for stdout, command_stderr in (
            (b"x" * 64, b""),
            (b"", b"x" * workspace_module.MAX_STDERR_BYTES),
        ):
            with self.subTest(boundary=(len(stdout), len(command_stderr))):
                runner.return_value = subprocess.CompletedProcess(
                    [], 0, stdout, command_stderr
                )
                self.assertEqual(
                    snapshotter._run(
                        ["version"],
                        operation="inspect exact output",
                        environment=environment,
                        limit=64,
                    ),
                    stdout,
                )

        for stdout, command_stderr in (
            (b"x" * 65, b""),
            (b"", b"x" * (workspace_module.MAX_STDERR_BYTES + 1)),
        ):
            with self.subTest(over_limit=(len(stdout), len(command_stderr))):
                runner.return_value = subprocess.CompletedProcess(
                    [], 0, stdout, command_stderr
                )
                with self.assertRaisesRegex(WorkspaceError, "output limit"):
                    snapshotter._run(
                        ["version"],
                        operation="inspect excess output",
                        environment=environment,
                        limit=64,
                    )

    def test_real_process_output_stderr_timeout_and_exit_are_bounded(self) -> None:
        snapshotter = GitSnapshotter(timeout=0.1)
        environment = snapshotter._environment(self.root)
        for script, message in (
            ("import os; os.write(1, b'x' * 10000)", "output limit"),
            ("import os; os.write(2, b'x' * 100000)", "output limit"),
        ):
            with (
                self.subTest(script=script),
                self.assertRaisesRegex(WorkspaceError, message),
            ):
                snapshotter._bounded_process(
                    [sys.executable, "-c", script], environment, 10
                )
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            snapshotter._bounded_process(
                [sys.executable, "-c", "import time; time.sleep(2)"], environment, 10
            )
        self.assertLess(time.monotonic() - started, 1)
        result = snapshotter._bounded_process(
            [sys.executable, "-c", "import sys; sys.exit(7)"], environment, 10
        )
        self.assertEqual(result.returncode, 7)

    def test_destination_must_be_empty_and_not_a_symlink(self) -> None:
        (self.destination / "existing").write_bytes(b"preserve")
        with self.assertRaisesRegex(WorkspaceError, "empty"):
            self.snapshotter(b"").snapshot(self.repository, None, self.destination)
        self.assertEqual((self.destination / "existing").read_bytes(), b"preserve")
        link = self.root / "destination-link"
        link.symlink_to(self.destination)
        with self.assertRaisesRegex(WorkspaceError, "directory"):
            self.snapshotter(b"").snapshot(self.repository, None, link)


class RealGitSnapshotterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repository = self.root / "repository"
        self.repository.mkdir()
        self.destination = self.root / "destination"
        self.destination.mkdir()

    def git(self, *args: str, input: bytes | None = None) -> bytes:
        return subprocess.run(  # noqa: S603 - controlled real Git test fixture
            [str(GitSnapshotter().binary), "-C", str(self.repository), *args],
            input=input,
            capture_output=True,
            check=True,
            env={
                "PATH": os.defpath,
                "HOME": str(self.root),
                "LC_ALL": "C",
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
            },
        ).stdout

    def initialize(self, *, sha256: bool = False) -> None:
        arguments = ["init", "-q"]
        if sha256:
            arguments.append("--object-format=sha256")
        self.git(*arguments)
        self.git("config", "user.name", "Snapshot Test")
        self.git("config", "user.email", "snapshot@example.invalid")

    def test_attributes_are_inert_raw_blob_bytes_and_export_ignore_is_visible(self):
        self.initialize()
        (self.repository / ".gitattributes").write_bytes(
            b"crlf text eol=crlf\nident ident\nsubst export-subst\n"
            b"hidden export-ignore\nfiltered filter=hostile\n"
        )
        bodies = {
            "crlf": b"line one\nline two\n",
            "ident": b"$Id$\n",
            "subst": b"$Format:%H$\n",
            "hidden": b"visible raw data\n",
            "filtered": b"raw filter input\n",
        }
        for path, body in bodies.items():
            (self.repository / path).write_bytes(body)
        self.git("add", ".")
        self.git("commit", "-qm", "raw attributes")
        sentinel = self.root / "smudge-executed"
        self.git("config", "filter.hostile.smudge", f"touch '{sentinel}'; cat")
        self.git("config", "filter.hostile.required", "true")
        expected = {
            path: self.git("cat-file", "blob", f"HEAD:{path}") for path in bodies
        }
        GitSnapshotter().snapshot(self.repository, None, self.destination)
        self.assertFalse(sentinel.exists())
        for path, body in expected.items():
            with self.subTest(path=path):
                self.assertTrue((self.destination / path).exists())
                self.assertEqual((self.destination / path).read_bytes(), body)

    def test_hostile_inherited_git_configuration_and_repository_variables(self):
        self.initialize()
        (self.repository / "file").write_bytes(b"source")
        self.git("add", ".")
        self.git("commit", "-qm", "source")
        commit = self.git("rev-parse", "HEAD").decode().strip()
        sentinel = self.root / "hostile-executed"
        hostile = {
            "GIT_DIR": str(self.root / "missing.git"),
            "GIT_WORK_TREE": str(self.root / "missing"),
            "GIT_INDEX_FILE": str(self.root / "hostile.index"),
            "GIT_OBJECT_DIRECTORY": str(self.root / "missing-objects"),
            "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(self.root / "missing-alternates"),
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "core.fsmonitor",
            "GIT_CONFIG_VALUE_0": f"touch '{sentinel}'",
            "GIT_CONFIG_PARAMETERS": "'core.bare=true'",
        }
        with mock.patch.dict(os.environ, hostile):
            resolved = GitSnapshotter().snapshot(
                self.repository, None, self.destination
            )
        self.assertEqual(resolved, SnapshotResult(commit))
        self.assertEqual((self.destination / "file").read_bytes(), b"source")
        self.assertFalse(sentinel.exists())
        self.assertFalse((self.root / "hostile.index").exists())

    def test_on_disk_git_alternates_cannot_escape_the_registered_repository(self):
        self.initialize()
        (self.repository / "private.txt").write_bytes(b"private alternate object\n")
        self.git("add", "private.txt")
        self.git("commit", "-qm", "private alternate commit")
        commit = self.git("rev-parse", "HEAD").decode().strip()
        shared = self.root / "shared-source"
        subprocess.run(  # noqa: S603 - controlled real Git test fixture
            [
                str(GitSnapshotter().binary),
                "clone",
                "--quiet",
                "--shared",
                "--no-checkout",
                str(self.repository),
                str(shared),
            ],
            check=True,
            env={"PATH": os.defpath, "LC_ALL": "C"},
        )
        alternate = shared / ".git" / "objects" / "info" / "alternates"
        self.assertTrue(alternate.read_text(encoding="utf-8").strip())

        with self.assertRaisesRegex(WorkspaceError, "alternates"):
            GitSnapshotter().snapshot(shared, commit, self.destination)

        self.assertEqual(list(self.destination.iterdir()), [])

    def test_git_commondir_cannot_escape_the_registered_repository(self):
        self.initialize()
        (self.repository / "private.txt").write_bytes(b"private common object\n")
        self.git("add", "private.txt")
        self.git("commit", "-qm", "private common commit")
        commit = self.git("rev-parse", "HEAD").decode().strip()
        redirected = self.root / "commondir-source"
        subprocess.run(  # noqa: S603 - controlled real Git test fixture
            [str(GitSnapshotter().binary), "init", "-q", str(redirected)],
            check=True,
            env={"PATH": os.defpath, "LC_ALL": "C"},
        )
        git_directory = redirected / ".git"
        (git_directory / "HEAD").write_text(commit + "\n", encoding="ascii")
        (git_directory / "commondir").write_text(
            str(self.repository / ".git") + "\n", encoding="utf-8"
        )

        with self.assertRaisesRegex(WorkspaceError, "common Git directory"):
            GitSnapshotter().snapshot(redirected, commit, self.destination)

        self.assertEqual(list(self.destination.iterdir()), [])

    def test_symlinked_git_object_entries_cannot_escape_the_repository(self):
        self.initialize()
        (self.repository / "private.txt").write_bytes(b"private packed object\n")
        self.git("add", "private.txt")
        self.git("commit", "-qm", "private packed commit")
        self.git("gc", "--prune=now", "--quiet")
        commit = self.git("rev-parse", "HEAD").decode().strip()
        redirected = self.root / "object-link-source"
        subprocess.run(  # noqa: S603 - controlled real Git test fixture
            [str(GitSnapshotter().binary), "init", "-q", str(redirected)],
            check=True,
            env={"PATH": os.defpath, "LC_ALL": "C"},
        )
        pack_directory = redirected / ".git" / "objects" / "pack"
        pack_directory.rmdir()
        pack_directory.symlink_to(
            self.repository / ".git" / "objects" / "pack", target_is_directory=True
        )
        (redirected / ".git" / "HEAD").write_text(commit + "\n", encoding="ascii")

        with self.assertRaisesRegex(WorkspaceError, "object store"):
            GitSnapshotter().snapshot(redirected, commit, self.destination)

        self.assertEqual(list(self.destination.iterdir()), [])

    def test_raw_blobs_stream_through_one_batch_process(self):
        self.initialize()
        for index in range(50):
            (self.repository / f"file-{index:03d}").write_bytes(
                (f"body {index}\n" * 1024).encode()
            )
        self.git("add", ".")
        self.git("commit", "-qm", "many blobs")
        commands: list[list[str]] = []
        original_popen = subprocess.Popen

        def record(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
            commands.append(list(args[0]))  # type: ignore[arg-type]
            return original_popen(*args, **kwargs)  # type: ignore[arg-type]

        with mock.patch.object(
            workspace_module.subprocess,
            "Popen",
            side_effect=record,
        ):
            GitSnapshotter().snapshot(self.repository, None, self.destination)

        blob_commands = [command for command in commands if "cat-file" in command]
        self.assertEqual(len(blob_commands), 1)
        self.assertIn("--batch", blob_commands[0])
        self.assertFalse(any("blob" in command for command in blob_commands))
        self.assertEqual(
            (self.destination / "file-049").read_text(), "body 49\n" * 1024
        )

    def test_production_batch_validates_every_blob_before_destination_writes(self):
        self.initialize()
        (self.repository / "file").write_bytes(b"raw")
        self.git("add", ".")
        self.git("commit", "-qm", "corrupt source")
        object_id = self.git("rev-parse", "HEAD:file").decode().strip()
        loose_object = (
            self.repository / ".git" / "objects" / object_id[:2] / object_id[2:]
        )
        loose_object.chmod(0o600)
        loose_object.write_bytes(zlib.compress(b"blob 3\0bad"))

        with self.assertRaisesRegex(WorkspaceError, "blob size or hash"):
            GitSnapshotter().snapshot(self.repository, None, self.destination)

        self.assertEqual(list(self.destination.iterdir()), [])

    def test_sha256_raw_snapshot_executable_symlink_and_gitlink(self):
        self.initialize(sha256=True)
        (self.repository / "script").write_bytes(b"#!/bin/sh\nexit 0\n")
        (self.repository / "script").chmod(0o755)
        (self.repository / "link").symlink_to("script")
        self.git("add", ".")
        self.git("commit", "-qm", "raw sha256")
        base = self.git("rev-parse", "HEAD").decode().strip()
        self.git("update-index", "--add", "--cacheinfo", f"160000,{base},module")
        self.git("commit", "-qm", "gitlink")
        snapshot = GitSnapshotter().snapshot(self.repository, None, self.destination)
        self.assertEqual(len(snapshot.revision), 64)
        self.assertEqual(snapshot.gitlinks, ("module",))
        self.assertEqual((self.destination / "script").stat().st_mode & 0o777, 0o755)
        self.assertEqual((self.destination / "module").stat().st_mode & 0o777, 0o555)
        self.assertEqual(os.readlink(self.destination / "link"), "script")
        self.assertEqual(list((self.destination / "module").iterdir()), [])

        manager = WorkspaceManager(self.root / "workspaces", GitSnapshotter())
        prepared = manager.prepare("run_" + "1" * 32, self.repository, None)
        self.assertEqual(prepared.gitlinks, ("module",))


if __name__ == "__main__":
    unittest.main()
