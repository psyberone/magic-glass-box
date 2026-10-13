from __future__ import annotations

import io
import os
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from magic_glass_box.workspace import (
    GitSnapshotter,
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
    ) -> str:
        self.calls.append((repository, revision, destination))
        if self.error is not None:
            raise self.error
        (destination / "tracked.txt").write_text("snapshot", encoding="utf-8")
        return COMMIT


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
        (prepared.path / "tracked.txt").write_text("agent edit", encoding="utf-8")
        self.assertFalse((self.repository / "tracked.txt").exists())
        self.assertTrue(manager.cleanup(RUN_ID))
        self.assertFalse(prepared.path.exists())
        self.assertFalse(manager.cleanup(RUN_ID))

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

    def test_cleanup_refuses_a_workspace_symlink(self) -> None:
        manager = WorkspaceManager(self.root, FakeSnapshotter())
        manager.ensure_root()
        (self.root / RUN_ID).symlink_to(self.repository, target_is_directory=True)
        with self.assertRaisesRegex(WorkspaceError, "symlink"):
            manager.cleanup(RUN_ID)
        self.assertTrue(self.repository.exists())

    def test_workspace_root_and_cleanup_reject_unsafe_filesystem_state(self) -> None:
        self.root.mkdir(mode=0o755)
        manager = WorkspaceManager(self.root, FakeSnapshotter())
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


def source_anchor(path: Path) -> str:
    return path.resolve().anchor


def tar_bytes(entries: list[tuple[str, bytes, str | None]]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name, content, link in entries:
            member = tarfile.TarInfo(name)
            if link is not None:
                member.type = tarfile.SYMTYPE
                member.linkname = link
            else:
                member.size = len(content)
            archive.addfile(member, None if link is not None else io.BytesIO(content))
    return output.getvalue()


class GitSnapshotterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repository = self.root / "repository"
        self.repository.mkdir()
        self.destination = self.root / "destination"
        self.destination.mkdir()

    def runner(self, responses: list[subprocess.CompletedProcess[bytes]]):
        calls: list[list[str]] = []

        def run(
            args: list[str], **_kwargs: object
        ) -> subprocess.CompletedProcess[bytes]:
            calls.append(args)
            return responses.pop(0)

        return calls, run

    def test_constructor_discovers_git_and_rejects_unsafe_settings(self) -> None:
        with mock.patch(
            "magic_glass_box.workspace.shutil.which", return_value="/usr/bin/git"
        ):
            discovered = GitSnapshotter(runner=mock.Mock())
        self.assertEqual(discovered.binary, Path("/usr/bin/git"))
        with (
            mock.patch("magic_glass_box.workspace.shutil.which", return_value=None),
            self.assertRaisesRegex(WorkspaceError, "not found"),
        ):
            GitSnapshotter(runner=mock.Mock())
        with self.assertRaisesRegex(WorkspaceError, "absolute"):
            GitSnapshotter(binary=Path("git"), runner=mock.Mock())
        for timeout in (0, -1, True, "60"):
            with (
                self.subTest(timeout=timeout),
                self.assertRaisesRegex(WorkspaceError, "timeout must be positive"),
            ):
                GitSnapshotter(
                    binary=Path("/usr/bin/git"),
                    runner=mock.Mock(),
                    timeout=timeout,  # type: ignore[arg-type]
                )

    def test_exports_only_archive_members_and_safe_relative_symlinks(self) -> None:
        archive = tar_bytes(
            [
                ("src/main.py", b"print('safe')\n", None),
                ("current.py", b"", "src/main.py"),
            ]
        )
        calls, runner = self.runner(
            [
                subprocess.CompletedProcess([], 0, (COMMIT + "\n").encode(), b""),
                subprocess.CompletedProcess([], 0, archive, b""),
            ]
        )
        snapshotter = GitSnapshotter(binary=Path("/usr/bin/git"), runner=runner)

        revision = snapshotter.snapshot(self.repository, "main", self.destination)

        self.assertEqual(revision, COMMIT)
        self.assertEqual(
            (self.destination / "src" / "main.py").read_bytes(), b"print('safe')\n"
        )
        self.assertEqual(os.readlink(self.destination / "current.py"), "src/main.py")
        self.assertIn("main^{commit}", calls[0])
        self.assertIn(COMMIT, calls[1])

    def test_rejects_a_relative_symlink_chain_that_escapes(self) -> None:
        archive = tar_bytes(
            [
                ("evil/x", b"", ".."),
                ("evil/y", b"", "x/.."),
            ]
        )
        _, runner = self.runner(
            [
                subprocess.CompletedProcess([], 0, (COMMIT + "\n").encode(), b""),
                subprocess.CompletedProcess([], 0, archive, b""),
            ]
        )
        snapshotter = GitSnapshotter(binary=Path("/usr/bin/git"), runner=runner)

        with self.assertRaisesRegex(WorkspaceError, "unsafe symlink"):
            snapshotter.snapshot(self.repository, "main", self.destination)

    def test_rejects_git_failures_and_unsafe_archives(self) -> None:
        cases = (
            (
                [subprocess.CompletedProcess([], 1, b"", b"unknown revision")],
                "cannot resolve",
            ),
            (
                [subprocess.CompletedProcess([], 0, b"not-a-hash\n", b"")],
                "invalid commit",
            ),
            (
                [
                    subprocess.CompletedProcess([], 0, (COMMIT + "\n").encode(), b""),
                    subprocess.CompletedProcess(
                        [], 0, tar_bytes([("../escape", b"bad", None)]), b""
                    ),
                ],
                "unsafe path",
            ),
            (
                [
                    subprocess.CompletedProcess([], 0, (COMMIT + "\n").encode(), b""),
                    subprocess.CompletedProcess(
                        [], 0, tar_bytes([("escape", b"", "../../outside")]), b""
                    ),
                ],
                "symlink",
            ),
            (
                [
                    subprocess.CompletedProcess([], 0, (COMMIT + "\n").encode(), b""),
                    subprocess.CompletedProcess(
                        [], 0, tar_bytes([("/absolute", b"bad", None)]), b""
                    ),
                ],
                "unsafe path",
            ),
            (
                [
                    subprocess.CompletedProcess([], 0, (COMMIT + "\n").encode(), b""),
                    subprocess.CompletedProcess(
                        [], 0, tar_bytes([(".git/config", b"bad", None)]), b""
                    ),
                ],
                "unsafe path",
            ),
            (
                [
                    subprocess.CompletedProcess([], 0, (COMMIT + "\n").encode(), b""),
                    subprocess.CompletedProcess(
                        [],
                        0,
                        tar_bytes(
                            [
                                ("linked", b"", "target"),
                                ("linked/file", b"bad", None),
                            ]
                        ),
                        b"",
                    ),
                ],
                "beneath a symlink",
            ),
            (
                [
                    subprocess.CompletedProcess([], 0, (COMMIT + "\n").encode(), b""),
                    subprocess.CompletedProcess(
                        [],
                        0,
                        tar_bytes(
                            [("duplicate", b"one", None), ("duplicate", b"two", None)]
                        ),
                        b"",
                    ),
                ],
                "duplicate paths",
            ),
        )
        for responses, message in cases:
            with self.subTest(message=message):
                _, runner = self.runner(list(responses))
                snapshotter = GitSnapshotter(binary=Path("/usr/bin/git"), runner=runner)
                with self.assertRaisesRegex(WorkspaceError, message):
                    snapshotter.snapshot(self.repository, "main", self.destination)

    def test_rejects_invalid_revision_output_and_runner_failures(self) -> None:
        failures: tuple[BaseException | subprocess.CompletedProcess[bytes], ...] = (
            subprocess.TimeoutExpired(["git"], 60),
            OSError("missing"),
            subprocess.CompletedProcess([], 0, b"\xff\n", b""),
        )
        for failure in failures:
            with self.subTest(failure=failure):
                runner = mock.Mock()
                if isinstance(failure, subprocess.CompletedProcess):
                    runner.return_value = failure
                else:
                    runner.side_effect = failure
                snapshotter = GitSnapshotter(binary=Path("/usr/bin/git"), runner=runner)
                with self.assertRaises(WorkspaceError):
                    snapshotter.snapshot(self.repository, "main", self.destination)

        for revision in ("", "bad\x00revision", "x" * 513, 7):
            with (
                self.subTest(revision=revision),
                self.assertRaisesRegex(WorkspaceError, "source revision"),
            ):
                GitSnapshotter(
                    binary=Path("/usr/bin/git"), runner=mock.Mock()
                ).snapshot(
                    self.repository,
                    revision,  # type: ignore[arg-type]
                    self.destination,
                )

    def test_archive_limits_and_file_types_fail_closed(self) -> None:
        special = io.BytesIO()
        with tarfile.open(fileobj=special, mode="w") as archive:
            member = tarfile.TarInfo("named-pipe")
            member.type = tarfile.FIFOTYPE
            archive.addfile(member)

        for archive, patch_target, message in (
            (b"not-a-tar", None, "invalid archive"),
            (special.getvalue(), None, "unsupported file type"),
            (tar_bytes([("file", b"xx", None)]), "MAX_ARCHIVE_BYTES", "size limit"),
            (tar_bytes([("file", b"x", None)]), "MAX_ARCHIVE_MEMBERS", "too many"),
        ):
            with self.subTest(message=message):
                _, runner = self.runner(
                    [
                        subprocess.CompletedProcess(
                            [], 0, (COMMIT + "\n").encode(), b""
                        ),
                        subprocess.CompletedProcess([], 0, archive, b""),
                    ]
                )
                snapshotter = GitSnapshotter(binary=Path("/usr/bin/git"), runner=runner)
                patcher = (
                    mock.patch(f"magic_glass_box.workspace.{patch_target}", 0)
                    if patch_target is not None
                    else mock.patch(
                        "magic_glass_box.workspace.MAX_ARCHIVE_BYTES",
                        512 * 1024 * 1024,
                    )
                )
                with patcher, self.assertRaisesRegex(WorkspaceError, message):
                    snapshotter.snapshot(self.repository, "main", self.destination)


if __name__ == "__main__":
    unittest.main()
