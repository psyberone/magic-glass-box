from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from magic_glass_box.results import ResultError, WorkspaceResultStore

ATTEMPT_ID = "att_" + "1" * 32
COMMIT = "a" * 40


class WorkspaceResultStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir(mode=0o700)
        (self.workspace / "delete.txt").write_text("delete me\n", encoding="utf-8")
        (self.workspace / "modify.txt").write_text("before\n", encoding="utf-8")
        script = self.workspace / "script.sh"
        script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        script.chmod(0o755)
        self.results = WorkspaceResultStore(self.root / "results")

    def test_capture_is_bounded_content_addressed_and_idempotent(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "delete.txt").unlink()
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        (self.workspace / "added.bin").write_bytes(b"\x00\xffresult")
        (self.workspace / "script.sh").chmod(0o644)
        (self.workspace / "link").symlink_to("modify.txt")

        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        replayed = self.results.capture(ATTEMPT_ID, self.workspace)

        self.assertEqual(replayed.manifest.digest, captured.manifest.digest)
        self.assertEqual(replayed.directory, captured.directory)
        self.assertEqual(captured.manifest.attempt_id, ATTEMPT_ID)
        self.assertEqual(captured.manifest.source_revision, COMMIT)
        self.assertEqual(
            [(change.path, change.operation) for change in captured.manifest.changes],
            [
                ("added.bin", "added"),
                ("delete.txt", "deleted"),
                ("link", "added"),
                ("modify.txt", "modified"),
                ("script.sh", "modified"),
            ],
        )
        self.assertEqual(captured.manifest.changed_paths, 5)
        self.assertEqual(captured.manifest.changed_bytes, len(b"\x00\xffresultafter\n"))
        inspected = self.results.inspect(ATTEMPT_ID)
        self.assertEqual(inspected.digest, captured.manifest.digest)

    def test_apply_reconstructs_result_after_full_conflict_preflight(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "delete.txt").unlink()
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        (self.workspace / "new").mkdir()
        (self.workspace / "new" / "tool.sh").write_text(
            "#!/bin/sh\nexit 1\n", encoding="utf-8"
        )
        (self.workspace / "new" / "tool.sh").chmod(0o755)
        (self.workspace / "relative-link").symlink_to("new/tool.sh")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "target"
        target.mkdir(mode=0o700)
        (target / "delete.txt").write_text("delete me\n", encoding="utf-8")
        (target / "modify.txt").write_text("before\n", encoding="utf-8")
        (target / "script.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        (target / "script.sh").chmod(0o755)

        applied = self.results.apply(ATTEMPT_ID, target)

        self.assertEqual(applied["changed_paths"], 4)
        self.assertFalse((target / "delete.txt").exists())
        self.assertEqual((target / "modify.txt").read_text(), "after\n")
        self.assertEqual((target / "new" / "tool.sh").stat().st_mode & 0o777, 0o755)
        self.assertEqual(os.readlink(target / "relative-link"), "new/tool.sh")

    def test_apply_refuses_any_conflict_before_changing_any_path(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "delete.txt").unlink()
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "target"
        target.mkdir(mode=0o700)
        (target / "delete.txt").write_text("delete me\n", encoding="utf-8")
        (target / "modify.txt").write_text("local edit\n", encoding="utf-8")
        (target / "script.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        (target / "script.sh").chmod(0o755)

        with self.assertRaisesRegex(ResultError, "target changed: modify.txt"):
            self.results.apply(ATTEMPT_ID, target)

        self.assertTrue((target / "delete.txt").exists())
        self.assertEqual((target / "modify.txt").read_text(), "local edit\n")

    def test_capture_rejects_unsafe_or_unbounded_worker_output(self) -> None:
        cases = ("absolute symlink", "special file", "byte limit", "path limit")
        for index, case in enumerate(cases):
            with self.subTest(case=case):
                attempt_id = "att_" + str(index + 2) * 32
                workspace = self.root / f"workspace-{index}"
                workspace.mkdir(mode=0o700)
                (workspace / "base.txt").write_text("base\n", encoding="utf-8")
                store = WorkspaceResultStore(
                    self.root / f"results-{index}",
                    max_changed_bytes=4,
                    max_paths=1 if case == "path limit" else 100_000,
                )
                store.stage(attempt_id, workspace, COMMIT)
                if case == "absolute symlink":
                    (workspace / "unsafe").symlink_to("/etc/passwd")
                    message = "unsafe symlink"
                elif case == "special file":
                    os.mkfifo(workspace / "pipe")
                    message = "unsupported file type"
                elif case == "byte limit":
                    (workspace / "large.txt").write_text("12345", encoding="ascii")
                    message = "changed content exceeds"
                else:
                    (workspace / "empty-one").mkdir()
                    (workspace / "empty-two").mkdir()
                    message = "too many paths"
                with self.assertRaisesRegex(ResultError, message):
                    store.capture(attempt_id, workspace)

    def test_inspect_detects_blob_and_manifest_tampering(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        state = captured.manifest.changes[0].after
        assert state is not None and state.sha256 is not None
        blob = captured.directory / "blobs" / state.sha256
        blob.write_bytes(b"tampered")

        with self.assertRaisesRegex(ResultError, "blob digest does not match"):
            self.results.inspect(ATTEMPT_ID)

        blob.write_bytes(b"after\n")
        (captured.directory / "manifest.json").write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(ResultError, "result manifest"):
            self.results.inspect(ATTEMPT_ID)

    def test_stage_rejects_changed_replay_and_reserved_git_metadata(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("changed baseline\n")
        with self.assertRaisesRegex(ResultError, "different baseline"):
            self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)

        other = self.root / "other-workspace"
        (other / ".git").mkdir(parents=True)
        with self.assertRaisesRegex(ResultError, "reserved .git path"):
            self.results.stage("att_" + "9" * 32, other, COMMIT)


if __name__ == "__main__":
    unittest.main()
