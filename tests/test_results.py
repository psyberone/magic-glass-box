from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

import magic_glass_box.results as results_module
from magic_glass_box.results import (
    FileState,
    ResultChange,
    ResultError,
    ResultManifest,
    WorkspaceResultStore,
    _PinnedIgnoreRules,
)

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

    def test_concurrent_capture_loser_reuses_the_identical_published_bundle(
        self,
    ) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        first = self.results.capture(ATTEMPT_ID, self.workspace)

        with mock.patch.object(Path, "exists", return_value=False):
            replayed = self.results.capture(ATTEMPT_ID, self.workspace)

        self.assertEqual(replayed.manifest.digest, first.manifest.digest)
        self.assertEqual(replayed.directory, first.directory)

    def test_load_bundle_returns_the_verified_manifest_and_blob_map(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)

        manifest, blobs = self.results.load_bundle(
            ATTEMPT_ID, expected_digest=captured.manifest.digest
        )

        self.assertEqual(manifest, captured.manifest)
        self.assertEqual(blobs, {hashlib.sha256(b"after\n").hexdigest(): b"after\n"})

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

    def test_capture_and_apply_nested_directory_to_file_replacement(self) -> None:
        original = self.workspace / "package" / "nested" / "original.txt"
        original.parent.mkdir(parents=True)
        original.write_bytes(b"original\n")
        (original.parent / "original-link").symlink_to("original.txt")
        sibling = self.workspace / "package" / "sibling.txt"
        sibling.write_bytes(b"sibling\n")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        shutil.rmtree(self.workspace / "package")
        (self.workspace / "package").write_bytes(b"replacement\r\n")

        captured = self.results.capture(ATTEMPT_ID, self.workspace)

        self.assertEqual(
            [(change.path, change.operation) for change in captured.manifest.changes],
            [
                ("package", "added"),
                ("package/nested/original-link", "deleted"),
                ("package/nested/original.txt", "deleted"),
                ("package/sibling.txt", "deleted"),
            ],
        )
        target = self.root / "directory-to-file-target"
        target_original = target / "package" / "nested" / "original.txt"
        target_original.parent.mkdir(parents=True)
        target_original.write_bytes(b"original\n")
        (target_original.parent / "original-link").symlink_to("original.txt")
        (target / "package" / "sibling.txt").write_bytes(b"sibling\n")

        self.results.apply(ATTEMPT_ID, target)

        self.assertTrue((target / "package").is_file())
        self.assertEqual((target / "package").read_bytes(), b"replacement\r\n")

    def test_capture_and_apply_nested_directory_to_symlink_replacement(self) -> None:
        original = self.workspace / "redirect" / "nested" / "original.txt"
        original.parent.mkdir(parents=True)
        original.write_bytes(b"original\n")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        shutil.rmtree(self.workspace / "redirect")
        (self.workspace / "redirect").symlink_to("modify.txt")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "directory-to-symlink-target"
        target_original = target / "redirect" / "nested" / "original.txt"
        target_original.parent.mkdir(parents=True)
        target_original.write_bytes(b"original\n")
        (target / "modify.txt").write_text("before\n", encoding="utf-8")

        self.results.apply(ATTEMPT_ID, target)

        self.assertTrue((target / "redirect").is_symlink())
        self.assertEqual(os.readlink(target / "redirect"), "modify.txt")

    def test_capture_and_apply_file_to_nested_directory_replacement(self) -> None:
        component = self.workspace / "component"
        component.write_bytes(b"original component\n")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        component.unlink()
        tool = component / "nested" / "tool.sh"
        tool.parent.mkdir(parents=True)
        tool.write_bytes(b"#!/bin/sh\r\nexit 0\r\n")
        tool.chmod(0o755)
        (component / "README.md").write_bytes(b"directory replacement\n")

        captured = self.results.capture(ATTEMPT_ID, self.workspace)

        self.assertEqual(
            [(change.path, change.operation) for change in captured.manifest.changes],
            [
                ("component", "deleted"),
                ("component/README.md", "added"),
                ("component/nested/tool.sh", "added"),
            ],
        )
        target = self.root / "file-to-directory-target"
        target.mkdir()
        (target / "component").write_bytes(b"original component\n")

        self.results.apply(ATTEMPT_ID, target)

        self.assertTrue((target / "component").is_dir())
        self.assertEqual(
            (target / "component" / "README.md").read_bytes(),
            b"directory replacement\n",
        )
        applied_tool = target / "component" / "nested" / "tool.sh"
        self.assertEqual(applied_tool.read_bytes(), b"#!/bin/sh\r\nexit 0\r\n")
        self.assertEqual(applied_tool.stat().st_mode & 0o777, 0o755)

    def test_capture_and_apply_symlink_to_nested_directory_replacement(self) -> None:
        redirect = self.workspace / "redirect"
        redirect.symlink_to("modify.txt")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        redirect.unlink()
        body = redirect / "nested" / "body.txt"
        body.parent.mkdir(parents=True)
        body.write_bytes(b"directory body\n")
        (body.parent / "body-link").symlink_to("body.txt")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "symlink-to-directory-target"
        target.mkdir()
        (target / "redirect").symlink_to("modify.txt")

        self.results.apply(ATTEMPT_ID, target)

        target_redirect = target / "redirect"
        self.assertTrue(target_redirect.is_dir())
        self.assertEqual(
            (target_redirect / "nested" / "body.txt").read_bytes(),
            b"directory body\n",
        )
        self.assertEqual(
            os.readlink(target_redirect / "nested" / "body-link"), "body.txt"
        )

    def test_leaf_to_directory_rejects_conflicts_before_deleting_the_leaf(
        self,
    ) -> None:
        component = self.workspace / "component"
        component.write_bytes(b"original component\n")
        conflict = self.workspace / "z-conflict.txt"
        conflict.write_bytes(b"before\n")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        component.unlink()
        child = component / "nested" / "child.txt"
        child.parent.mkdir(parents=True)
        child.write_bytes(b"child\n")
        (self.workspace / "delete.txt").unlink()
        conflict.write_bytes(b"after\n")
        self.results.capture(ATTEMPT_ID, self.workspace)

        cases = (
            ("mismatched-leaf", b"operator edit\n", b"before\n", "component"),
            (
                "later-conflict",
                b"original component\n",
                b"operator edit\n",
                r"z-conflict\.txt",
            ),
        )
        for index, (case, component_body, conflict_body, conflict_path) in enumerate(
            cases
        ):
            with self.subTest(case=case):
                target = self.root / f"leaf-conflict-target-{index}"
                target.mkdir()
                (target / "component").write_bytes(component_body)
                (target / "delete.txt").write_bytes(b"delete me\n")
                (target / "z-conflict.txt").write_bytes(conflict_body)

                with self.assertRaisesRegex(
                    ResultError, rf"^result target changed: {conflict_path}$"
                ):
                    self.results.apply(ATTEMPT_ID, target)

                self.assertEqual((target / "component").read_bytes(), component_body)
                self.assertEqual((target / "delete.txt").read_bytes(), b"delete me\n")

    def test_directory_replacement_rejects_every_unknown_leaf_before_mutation(
        self,
    ) -> None:
        (self.workspace / ".gitignore").write_text(
            "package/cache.bin\n", encoding="utf-8"
        )
        original = self.workspace / "package" / "nested" / "original.txt"
        original.parent.mkdir(parents=True)
        original.write_bytes(b"original\n")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        shutil.rmtree(self.workspace / "package")
        (self.workspace / "package").write_bytes(b"replacement\n")
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        self.results.capture(ATTEMPT_ID, self.workspace)

        for index, extra in enumerate(("untracked.txt", "cache.bin")):
            with self.subTest(extra=extra):
                target = self.root / f"extra-leaf-target-{index}"
                target_original = target / "package" / "nested" / "original.txt"
                target_original.parent.mkdir(parents=True)
                target_original.write_bytes(b"original\n")
                (target / "package" / extra).write_bytes(b"operator state\n")
                (target / "modify.txt").write_text("before\n", encoding="utf-8")

                with self.assertRaisesRegex(
                    ResultError, r"^result target changed: package$"
                ):
                    self.results.apply(ATTEMPT_ID, target)

                self.assertEqual(target_original.read_bytes(), b"original\n")
                self.assertEqual(
                    (target / "package" / extra).read_bytes(), b"operator state\n"
                )
                self.assertEqual(
                    (target / "modify.txt").read_text(encoding="utf-8"), "before\n"
                )

    def test_directory_replacement_requires_the_complete_exact_baseline_tree(
        self,
    ) -> None:
        original = self.workspace / "package" / "nested" / "original.txt"
        original.parent.mkdir(parents=True)
        original.write_bytes(b"original\n")
        sibling = self.workspace / "package" / "sibling.txt"
        sibling.write_bytes(b"sibling\n")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        shutil.rmtree(self.workspace / "package")
        (self.workspace / "package").write_bytes(b"replacement\n")
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        self.results.capture(ATTEMPT_ID, self.workspace)

        for index, case in enumerate(("changed", "missing", "empty-extra")):
            with self.subTest(case=case):
                target = self.root / f"inexact-tree-target-{index}"
                target_original = target / "package" / "nested" / "original.txt"
                target_original.parent.mkdir(parents=True)
                target_original.write_bytes(
                    b"changed\n" if case == "changed" else b"original\n"
                )
                target_sibling = target / "package" / "sibling.txt"
                target_sibling.write_bytes(b"sibling\n")
                if case == "missing":
                    target_sibling.unlink()
                elif case == "empty-extra":
                    (target / "package" / "operator-empty").mkdir()
                (target / "modify.txt").write_text("before\n", encoding="utf-8")

                with self.assertRaisesRegex(
                    ResultError, r"^result target changed: package$"
                ):
                    self.results.apply(ATTEMPT_ID, target)

                self.assertTrue((target / "package").is_dir())
                self.assertEqual(
                    (target / "modify.txt").read_text(encoding="utf-8"), "before\n"
                )

    def test_added_leaf_rejects_a_preexisting_empty_directory_before_mutation(
        self,
    ) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "delete.txt").unlink()
        (self.workspace / "new.txt").write_text("new\n", encoding="utf-8")
        self.results.capture(ATTEMPT_ID, self.workspace)
        target = self.root / "empty-directory-target"
        target.mkdir()
        (target / "delete.txt").write_text("delete me\n", encoding="utf-8")
        (target / "new.txt").mkdir()

        with self.assertRaisesRegex(ResultError, r"^result target changed: new\.txt$"):
            self.results.apply(ATTEMPT_ID, target)

        self.assertTrue((target / "delete.txt").is_file())
        self.assertTrue((target / "new.txt").is_dir())

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

    def test_apply_loads_the_trusted_bundle_then_preflights_once(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "delete.txt").unlink()
        (self.workspace / "modify.txt").write_bytes(b"verified result\n")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "single-preflight-target"
        target.mkdir(mode=0o700)
        (target / "delete.txt").write_text("delete me\n", encoding="utf-8")
        (target / "modify.txt").write_text("before\n", encoding="utf-8")
        (target / "script.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        (target / "script.sh").chmod(0o755)
        with mock.patch.object(
            self.results,
            "_preflight_apply",
            wraps=self.results._preflight_apply,
        ) as preflight:
            self.results.apply(ATTEMPT_ID, target)

        preflight.assert_called_once()
        self.assertEqual((target / "modify.txt").read_bytes(), b"verified result\n")
        self.assertFalse((target / "delete.txt").exists())

    def test_mode_only_apply_replaces_a_hardlink_with_a_fresh_inode(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "script.sh").chmod(0o644)
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "mode-hardlink-target"
        target.mkdir(mode=0o700)
        script = target / "script.sh"
        script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        script.chmod(0o755)
        alias = self.root / "outside-script-alias"
        try:
            os.link(script, alias)
        except OSError as exc:
            self.skipTest(f"hard links are unavailable: {exc}")

        self.results.apply(ATTEMPT_ID, target)

        self.assertEqual(script.read_bytes(), alias.read_bytes())
        self.assertEqual(script.stat().st_mode & 0o777, 0o644)
        self.assertEqual(alias.stat().st_mode & 0o777, 0o755)
        self.assertNotEqual(script.stat().st_ino, alias.stat().st_ino)

    def test_large_mode_only_apply_streams_to_a_fresh_inode(self) -> None:
        body = b"0123456789abcdef" * (4 * 1024 * 1024 // 16)
        large = self.workspace / "large.bin"
        large.write_bytes(body)
        large.chmod(0o755)
        store = WorkspaceResultStore(
            self.root / "streaming-results",
            max_changed_bytes=1,
        )
        attempt_id = "att_" + "e" * 32
        store.stage(attempt_id, self.workspace, COMMIT)
        large.chmod(0o644)
        captured = store.capture(attempt_id, self.workspace)
        self.assertEqual(captured.manifest.changed_bytes, 0)

        target = self.root / "streaming-mode-target"
        target.mkdir(mode=0o700)
        target_large = target / "large.bin"
        target_large.write_bytes(body)
        target_large.chmod(0o755)
        alias = self.root / "outside-large-alias"
        try:
            os.link(target_large, alias)
        except OSError as exc:
            self.skipTest(f"hard links are unavailable: {exc}")
        store.apply(attempt_id, target)

        self.assertEqual(target_large.read_bytes(), body)
        self.assertEqual(target_large.stat().st_mode & 0o777, 0o644)
        self.assertEqual(alias.read_bytes(), body)
        self.assertEqual(alias.stat().st_mode & 0o777, 0o755)
        self.assertNotEqual(target_large.stat().st_ino, alias.stat().st_ino)

    def test_apply_revalidates_legacy_symlink_graph_before_any_change(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        directory = self.workspace / "evil"
        directory.mkdir()
        (directory / "x").symlink_to("..")
        (directory / "y").symlink_to("x/..")
        with mock.patch("magic_glass_box.results.validate_symlink_graph"):
            self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "target"
        target.mkdir(mode=0o700)
        (target / "delete.txt").write_text("delete me\n", encoding="utf-8")
        (target / "modify.txt").write_text("before\n", encoding="utf-8")
        (target / "script.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        (target / "script.sh").chmod(0o755)

        with self.assertRaisesRegex(ResultError, "unsafe symlink"):
            self.results.apply(ATTEMPT_ID, target)

        self.assertFalse((target / "evil").exists())
        self.assertEqual((target / "modify.txt").read_text(), "before\n")

    def test_apply_rejects_escape_through_unchanged_checkout_symlink(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "new-link").symlink_to("existing/..")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "target"
        target.mkdir(mode=0o700)
        (target / "delete.txt").write_text("delete me\n", encoding="utf-8")
        (target / "modify.txt").write_text("before\n", encoding="utf-8")
        (target / "script.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        (target / "script.sh").chmod(0o755)
        (target / "existing").symlink_to("..")

        with self.assertRaisesRegex(
            ResultError, r"^result contains an unsafe symlink$"
        ):
            self.results.apply(ATTEMPT_ID, target)

        self.assertFalse((target / "new-link").exists())
        self.assertEqual((target / "modify.txt").read_text(), "before\n")

    def test_capture_rejects_escape_composed_through_case_alias(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        directory = self.workspace / "d"
        directory.mkdir()
        (directory / "up").symlink_to("..")
        (self.workspace / "escape").symlink_to("D/up/..")

        with self.assertRaisesRegex(
            ResultError, r"^result contains an unsafe symlink$"
        ):
            self.results.capture(ATTEMPT_ID, self.workspace)

    def test_apply_rejects_escape_through_portable_checkout_link_alias(
        self,
    ) -> None:
        target = self.root / "portable-link-target"
        directory = target / "d"
        directory.mkdir(parents=True)
        (directory / "up").symlink_to("..")
        changes = (
            ResultChange(
                "escape",
                "added",
                None,
                FileState(kind="symlink", target="D/up/.."),
            ),
        )

        with self.assertRaisesRegex(
            ResultError, r"^result contains an unsafe symlink$"
        ):
            self.results._validate_applied_symlinks(target, changes)

    def test_apply_rejects_unchanged_link_that_escapes_through_new_link(self) -> None:
        nested = self.workspace / "a" / "b"
        nested.mkdir(parents=True)
        (nested / "file.txt").write_text("inside\n", encoding="utf-8")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (nested / "file.txt").unlink()
        nested.rmdir()
        nested.symlink_to("..")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "reverse-link-target"
        target_nested = target / "a" / "b"
        target_nested.mkdir(parents=True)
        (target_nested / "file.txt").write_text("inside\n", encoding="utf-8")
        (target / "entry").symlink_to("a/b/..")

        with self.assertRaisesRegex(
            ResultError, r"^result contains an unsafe symlink$"
        ):
            self.results.apply(ATTEMPT_ID, target)

        self.assertTrue(target_nested.is_dir())
        self.assertEqual((target_nested / "file.txt").read_text(), "inside\n")

    def test_apply_supports_case_only_rename_on_case_insensitive_checkout(
        self,
    ) -> None:
        source = self.workspace / "case-name.txt"
        source.write_text("same bytes\n", encoding="utf-8")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        source.rename(self.workspace / "CASE-NAME.txt")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "case-rename-target"
        target.mkdir(mode=0o700)
        old_target = target / "case-name.txt"
        old_target.write_text("same bytes\n", encoding="utf-8")
        if not (target / "CASE-NAME.txt").exists():
            # The behavior is filesystem-specific; case-sensitive hosts still
            # exercise the ordinary delete/add path elsewhere in this suite.
            return

        self.results.apply(ATTEMPT_ID, target)

        self.assertNotIn("case-name.txt", os.listdir(target))
        self.assertIn("CASE-NAME.txt", os.listdir(target))
        self.assertEqual((target / "CASE-NAME.txt").read_text(), "same bytes\n")

    def test_apply_supports_parent_directory_case_only_rename(
        self,
    ) -> None:
        source = self.workspace / "old-dir" / "name.txt"
        source.parent.mkdir()
        source.write_text("same bytes\n", encoding="utf-8")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        source.parent.rename(self.workspace / "OLD-DIR")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "directory-case-rename-target"
        old_target = target / "old-dir" / "name.txt"
        old_target.parent.mkdir(parents=True)
        old_target.write_text("same bytes\n", encoding="utf-8")
        if not (target / "OLD-DIR" / "name.txt").exists():
            return

        self.results.apply(ATTEMPT_ID, target)

        self.assertNotIn("old-dir", os.listdir(target))
        self.assertIn("OLD-DIR", os.listdir(target))
        self.assertEqual((target / "OLD-DIR" / "name.txt").read_text(), "same bytes\n")

    def test_case_only_rename_rejects_a_changed_source_alias(self) -> None:
        source = self.workspace / "case-conflict.txt"
        source.write_text("baseline\n", encoding="utf-8")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        source.rename(self.workspace / "CASE-CONFLICT.txt")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "case-rename-conflict-target"
        target.mkdir(mode=0o700)
        old_target = target / "case-conflict.txt"
        old_target.write_text("local edit\n", encoding="utf-8")
        if not (target / "CASE-CONFLICT.txt").exists():
            return

        with self.assertRaisesRegex(
            ResultError, r"^result target changed: CASE-CONFLICT.txt$"
        ):
            self.results.apply(ATTEMPT_ID, target)

        self.assertEqual(os.listdir(target), ["case-conflict.txt"])
        self.assertEqual(old_target.read_text(), "local edit\n")

    def test_apply_preflight_rejects_portable_final_path_aliases(self) -> None:
        first_body = b"one\n"
        second_body = b"two\n"
        first = FileState(
            kind="file",
            mode=0o644,
            size=len(first_body),
            sha256=hashlib.sha256(first_body).hexdigest(),
        )
        second = FileState(
            kind="file",
            mode=0o644,
            size=len(second_body),
            sha256=hashlib.sha256(second_body).hexdigest(),
        )
        cases = (
            (
                ResultChange("README", "added", None, first),
                ResultChange("readme", "added", None, second),
            ),
            (
                ResultChange("caf\u00e9.txt", "added", None, first),
                ResultChange("cafe\u0301.txt", "added", None, second),
            ),
            (
                ResultChange("Directory/one", "added", None, first),
                ResultChange("directory/two", "added", None, second),
            ),
            (
                ResultChange("Foo", "deleted", first, None),
                ResultChange("foo", "added", None, first),
                ResultChange("FOO", "added", None, second),
            ),
        )
        target = self.root / "portable-alias-target"
        target.mkdir(mode=0o700)
        for changes in cases:
            with (
                self.subTest(paths=[change.path for change in changes]),
                self.assertRaisesRegex(
                    ResultError,
                    r"^result final paths contain portable aliases$",
                ),
            ):
                self.results._preflight_apply(target, changes)

    def test_apply_preflight_rejects_portable_leaf_descendant_alias(self) -> None:
        body = b"body\n"
        regular = FileState(
            kind="file",
            mode=0o644,
            size=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
        )
        changes = (
            ResultChange("leaf", "added", None, regular),
            ResultChange("leaf/child", "added", None, regular),
        )
        target = self.root / "portable-descendant-target"
        target.mkdir(mode=0o700)

        with self.assertRaisesRegex(
            ResultError,
            r"^result final paths contain portable leaf/descendant aliases$",
        ):
            self.results._preflight_apply(target, changes)

    def test_apply_preflight_rejects_portable_baseline_aliases(self) -> None:
        body = b"old\n"
        regular = FileState(
            kind="file",
            mode=0o644,
            size=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
        )
        changes = (
            ResultChange("OLD", "deleted", regular, None),
            ResultChange("old", "deleted", regular, None),
        )
        target = self.root / "portable-baseline-alias-target"
        target.mkdir(mode=0o700)

        with self.assertRaisesRegex(
            ResultError,
            r"^result baseline paths contain portable aliases$",
        ):
            self.results._preflight_apply(target, changes)

    def test_apply_uses_final_file_before_existing_checkout_symlink(self) -> None:
        inside = self.workspace / "inside"
        inside.mkdir()
        pivot = inside / "pivot"
        pivot.symlink_to("..")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        pivot.unlink()
        pivot.write_text("replacement\n", encoding="utf-8")
        (self.workspace / "new-link").symlink_to("inside/pivot/escape/../..")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "target"
        target.mkdir(mode=0o700)
        (target / "delete.txt").write_text("delete me\n", encoding="utf-8")
        (target / "modify.txt").write_text("before\n", encoding="utf-8")
        (target / "script.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        (target / "script.sh").chmod(0o755)
        target_inside = target / "inside"
        target_inside.mkdir()
        (target_inside / "pivot").symlink_to("..")
        (target / "escape").symlink_to("..")

        self.results.apply(ATTEMPT_ID, target)

        self.assertEqual((target_inside / "pivot").read_text(), "replacement\n")
        self.assertEqual(os.readlink(target / "new-link"), "inside/pivot/escape/../..")

    def test_apply_preflight_rejects_path_beneath_new_symlink(self) -> None:
        regular = FileState(kind="file", mode=0o644, size=0, sha256="0" * 64)
        changes = (
            ResultChange("removed", "deleted", regular, None),
            ResultChange(
                "link", "added", None, FileState(kind="symlink", target="safe")
            ),
            ResultChange("link/file", "added", None, regular),
        )

        with self.assertRaisesRegex(
            ResultError, r"^result contains a path beneath a symlink$"
        ):
            self.results._validate_applied_symlinks(self.root, changes)

    def test_manifest_rejects_final_leaf_and_descendant_collisions(self) -> None:
        regular = FileState(kind="file", mode=0o644, size=0, sha256="0" * 64)
        symlink = FileState(kind="symlink", target="safe")
        for parent in (regular, symlink):
            with (
                self.subTest(parent=parent.kind),
                self.assertRaisesRegex(
                    ResultError,
                    r"^result final paths contain a leaf/descendant collision$",
                ),
            ):
                ResultManifest(
                    1,
                    ATTEMPT_ID,
                    COMMIT,
                    (
                        ResultChange("leaf", "added", None, parent),
                        ResultChange("leaf/child", "added", None, regular),
                    ),
                )

    def test_manifest_rejects_baseline_leaf_and_descendant_collisions(self) -> None:
        regular = FileState(kind="file", mode=0o644, size=0, sha256="0" * 64)
        changed = FileState(kind="file", mode=0o755, size=0, sha256="0" * 64)

        with self.assertRaisesRegex(
            ResultError,
            r"^result baseline paths contain a leaf/descendant collision$",
        ):
            ResultManifest(
                1,
                ATTEMPT_ID,
                COMMIT,
                (
                    ResultChange("leaf", "deleted", regular, None),
                    ResultChange("leaf/child", "modified", regular, changed),
                ),
            )

    def test_size_is_part_of_changed_file_content_identity(self) -> None:
        digest = "0" * 64
        before = FileState(kind="file", mode=0o644, size=1, sha256=digest)
        after = FileState(kind="file", mode=0o644, size=2, sha256=digest)
        change = ResultChange("body.bin", "modified", before, after)

        self.assertTrue(change.needs_blob)
        with self.assertRaisesRegex(ResultError, "inconsistent sizes"):
            ResultManifest(1, ATTEMPT_ID, COMMIT, (change,))

    def test_manifest_rejects_inconsistent_sizes_for_one_blob_digest(self) -> None:
        digest = "0" * 64
        with self.assertRaisesRegex(
            ResultError,
            r"^result blob digest has inconsistent sizes$",
        ):
            ResultManifest(
                1,
                ATTEMPT_ID,
                COMMIT,
                (
                    ResultChange(
                        "one.bin",
                        "added",
                        None,
                        FileState(kind="file", mode=0o644, size=1, sha256=digest),
                    ),
                    ResultChange(
                        "two.bin",
                        "added",
                        None,
                        FileState(kind="file", mode=0o644, size=2, sha256=digest),
                    ),
                ),
            )

    def test_changed_byte_limit_counts_duplicate_content_per_output_path(self) -> None:
        body = b"shared body\n"
        store = WorkspaceResultStore(
            self.root / "duplicate-results",
            max_changed_bytes=len(body) * 2,
        )
        attempt_id = "att_" + "d" * 32
        store.stage(attempt_id, self.workspace, COMMIT)
        (self.workspace / "one.bin").write_bytes(body)
        (self.workspace / "two.bin").write_bytes(body)

        captured = store.capture(attempt_id, self.workspace)

        self.assertEqual(captured.manifest.changed_bytes, len(body) * 2)
        self.assertEqual(len(tuple((captured.directory / "blobs").iterdir())), 1)
        constrained = WorkspaceResultStore(
            self.root / "duplicate-results",
            max_changed_bytes=len(body) * 2 - 1,
        )
        with self.assertRaisesRegex(ResultError, "changed content exceeds"):
            constrained.inspect(attempt_id)

    def test_capture_and_apply_allow_a_chained_symlink_that_stays_inside(self) -> None:
        shared = self.workspace / "shared"
        shared.mkdir()
        (shared / "file.txt").write_text("safe\n", encoding="utf-8")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        links = self.workspace / "links"
        links.mkdir()
        (links / "inner").symlink_to("../shared")
        (links / "outer").symlink_to("inner/file.txt")
        self.results.capture(ATTEMPT_ID, self.workspace)

        target = self.root / "target"
        target.mkdir(mode=0o700)
        (target / "delete.txt").write_text("delete me\n", encoding="utf-8")
        (target / "modify.txt").write_text("before\n", encoding="utf-8")
        (target / "script.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        (target / "script.sh").chmod(0o755)
        target_shared = target / "shared"
        target_shared.mkdir()
        (target_shared / "file.txt").write_text("safe\n", encoding="utf-8")

        self.results.apply(ATTEMPT_ID, target)

        self.assertEqual(os.readlink(target / "links" / "inner"), "../shared")
        self.assertEqual(os.readlink(target / "links" / "outer"), "inner/file.txt")
        self.assertEqual((target / "links" / "outer").read_text(), "safe\n")

    def test_capture_rejects_unsafe_or_unbounded_worker_output(self) -> None:
        cases = (
            "absolute symlink",
            "git symlink",
            "special file",
            "byte limit",
            "path limit",
        )
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
                elif case == "git symlink":
                    (workspace / "unsafe").symlink_to(".GiT/config")
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

    def test_capture_rejects_obvious_aggregate_oversize_before_reading_it(self) -> None:
        store = WorkspaceResultStore(
            self.root / "early-limit-results",
            max_changed_bytes=5,
        )
        attempt_id = "att_" + "7" * 32
        store.stage(attempt_id, self.workspace, COMMIT)
        (self.workspace / "added-one.bin").write_bytes(b"123")
        (self.workspace / "added-two.bin").write_bytes(b"456")
        original_file_state = store._file_state
        read_paths: list[str] = []

        def record_read(*args: object, **kwargs: object) -> FileState:
            read_paths.append(str(args[1]))
            return original_file_state(*args, **kwargs)  # type: ignore[arg-type]

        with (
            mock.patch.object(store, "_file_state", side_effect=record_read),
            self.assertRaisesRegex(ResultError, "changed content exceeds"),
        ):
            store.capture(attempt_id, self.workspace)

        self.assertNotIn("added-one.bin", read_paths)
        self.assertNotIn("added-two.bin", read_paths)

    def test_capture_rejects_a_relative_symlink_chain_that_escapes(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        directory = self.workspace / "evil"
        directory.mkdir()
        (directory / "x").symlink_to("..")
        (directory / "y").symlink_to("x/..")

        with self.assertRaisesRegex(ResultError, "unsafe symlink"):
            self.results.capture(ATTEMPT_ID, self.workspace)

    def test_capture_rejects_any_worker_change_to_a_staged_gitlink(self) -> None:
        module = self.workspace / "module"
        module.mkdir()
        self.results.stage(
            ATTEMPT_ID,
            self.workspace,
            COMMIT,
            gitlinks=("module",),
        )

        (module / "worker.txt").write_text(
            "not a submodule checkout\n", encoding="utf-8"
        )

        with self.assertRaisesRegex(ResultError, "gitlink"):
            self.results.capture(ATTEMPT_ID, self.workspace)

    def test_capture_rejects_a_symlink_replacing_a_gitlink_parent(self) -> None:
        nested = self.workspace / "nested"
        module = nested / "module"
        module.mkdir(parents=True)
        self.results.stage(
            ATTEMPT_ID,
            self.workspace,
            COMMIT,
            gitlinks=("nested/module",),
        )
        nested.rename(self.workspace / "moved")
        nested.symlink_to("moved", target_is_directory=True)

        with self.assertRaisesRegex(ResultError, "gitlink"):
            self.results.capture(ATTEMPT_ID, self.workspace)

    def test_pinned_ignore_git_commands_ignore_the_ambient_git_environment(
        self,
    ) -> None:
        (self.workspace / ".gitignore").write_text("ignored/\n", encoding="utf-8")
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        ignored = self.workspace / "ignored"
        ignored.mkdir()
        (ignored / "cache.bin").write_bytes(b"ignored")
        (self.workspace / "kept.txt").write_text("kept\n", encoding="utf-8")

        hostile = {
            "GIT_DIR": str(self.root / "redirected.git"),
            "GIT_WORK_TREE": str(self.root / "redirected-worktree"),
            "GIT_CONFIG_GLOBAL": str(self.root / "hostile.gitconfig"),
            "GIT_TRACE": "1",
        }
        with mock.patch.dict(os.environ, hostile, clear=False):
            captured = self.results.capture(ATTEMPT_ID, self.workspace)

        self.assertEqual(
            [change.path for change in captured.manifest.changes],
            ["kept.txt"],
        )

    def test_capture_uses_pinned_gitignore_without_hiding_tracked_work(self) -> None:
        workspace = self.root / "ignored-workspace"
        workspace.mkdir(mode=0o700)
        (workspace / ".gitignore").write_text(
            "node_modules/\ngenerated/\n", encoding="utf-8"
        )
        generated = workspace / "generated"
        generated.mkdir()
        (generated / "tracked.txt").write_text("before\n", encoding="utf-8")
        store = WorkspaceResultStore(self.root / "ignored-results", max_paths=6)
        attempt_id = "att_" + "8" * 32
        store.stage(attempt_id, workspace, COMMIT)

        # A worker cannot change the ignore policy after its baseline is pinned.
        (workspace / ".gitignore").write_text("*\n", encoding="utf-8")
        (workspace / "kept.txt").write_text("work product\n", encoding="utf-8")
        (generated / "tracked.txt").write_text("after\n", encoding="utf-8")
        (generated / "cache.bin").write_bytes(b"ignored")
        dependencies = workspace / "node_modules"
        dependencies.mkdir()
        for index in range(20):
            (dependencies / f"dependency-{index}").write_text("ignored")

        captured = store.capture(attempt_id, workspace)

        self.assertEqual(
            [change.path for change in captured.manifest.changes],
            [".gitignore", "generated/tracked.txt", "kept.txt"],
        )

    def test_capture_bounds_raw_entries_before_filtering_ignored_paths(self) -> None:
        workspace = self.root / "raw-entry-limit-workspace"
        workspace.mkdir(mode=0o700)
        (workspace / ".gitignore").write_text("ignored-*\n", encoding="utf-8")
        store = WorkspaceResultStore(self.root / "raw-entry-limit-results")
        attempt_id = "att_" + "f" * 32
        store.stage(attempt_id, workspace, COMMIT)
        for index in range(10):
            (workspace / f"ignored-{index:02d}").write_text(
                "ignored\n", encoding="utf-8"
            )

        original_scandir = os.scandir
        inspected = 0

        class CountingScandir:
            def __init__(self, path: os.PathLike[str] | str):
                self._iterator = original_scandir(path)

            def __enter__(self) -> CountingScandir:
                self._iterator.__enter__()
                return self

            def __exit__(self, *args: object) -> None:
                self._iterator.__exit__(*args)

            def __iter__(self) -> CountingScandir:
                return self

            def __next__(self) -> os.DirEntry[str]:
                nonlocal inspected
                entry = next(self._iterator)
                inspected += 1
                return entry

        def counted_scandir(
            path: int | os.PathLike[str] | str,
        ) -> CountingScandir | os.ScandirIterator[str]:
            if not isinstance(path, int) and Path(path) == workspace:
                return CountingScandir(path)
            return original_scandir(path)

        with (
            mock.patch.object(results_module, "MAX_INSPECTED_PATHS", 3),
            mock.patch.object(results_module, "MAX_SCAN_BATCH_PATHS", 2),
            mock.patch.object(os, "scandir", side_effect=counted_scandir),
            self.assertRaisesRegex(ResultError, "too many workspace entries"),
        ):
            store.capture(attempt_id, workspace)

        self.assertEqual(inspected, 4)

    def test_ignore_matching_batches_exact_tokens_at_the_byte_ceiling(self) -> None:
        matcher = object.__new__(_PinnedIgnoreRules)
        with (
            mock.patch(
                "magic_glass_box.results.MAX_IGNORE_BATCH_BYTES",
                8,
            ),
            mock.patch.object(
                matcher,
                "_check_batch",
                side_effect=({"abc"}, {"ghi"}),
            ) as check_batch,
        ):
            ignored = matcher.ignored(
                (("abc", False), ("def", False), ("ghi", True), ("jk", False))
            )

        self.assertEqual(ignored, {"abc", "ghi"})
        self.assertEqual(
            check_batch.call_args_list,
            [
                mock.call([("abc", "abc"), ("def", "def")]),
                mock.call([("ghi", "ghi/"), ("jk", "jk")]),
            ],
        )

    def test_pinned_ignore_files_enforce_the_combined_exact_byte_limit(self) -> None:
        workspace = self.root / "ignore-limit-workspace"
        workspace.mkdir(mode=0o700)
        (workspace / ".gitignore").write_text("a\n", encoding="utf-8")
        (workspace / "README.md").write_text("proof\n", encoding="utf-8")
        nested = workspace / "nested"
        nested.mkdir()
        nested_ignore = nested / ".gitignore"
        nested_ignore.write_text("b\n", encoding="utf-8")
        store = WorkspaceResultStore(self.root / "ignore-limit-results")

        with mock.patch("magic_glass_box.results.MAX_PINNED_IGNORE_BYTES", 4):
            store.stage("att_" + "9" * 32, workspace, COMMIT)
            nested_ignore.write_text("bc\n", encoding="utf-8")
            with self.assertRaisesRegex(
                ResultError,
                "^result ignore rules exceed the size limit$",
            ):
                store.stage("att_" + "a" * 32, workspace, COMMIT)

    def test_pinned_ignore_read_requires_the_scanned_size_and_digest(self) -> None:
        ignore_file = self.workspace / ".gitignore"
        ignore_file.write_text("dependencies/\n", encoding="utf-8")
        state = self.results._file_state(ignore_file, ".gitignore")

        for expected in (
            replace(state, size=state.size + 1 if state.size is not None else 1),
            replace(state, sha256="0" * 64),
        ):
            with (
                self.subTest(expected=expected),
                self.assertRaisesRegex(
                    ResultError,
                    "^cannot pin result ignore rules: \\.gitignore$",
                ),
            ):
                self.results._read_pinned_ignore(
                    ignore_file,
                    ".gitignore",
                    expected,
                )

    def test_stage_rejects_an_ignore_file_swapped_after_scanning(self) -> None:
        ignore_file = self.workspace / ".gitignore"
        ignore_file.write_text("dependencies/\n", encoding="utf-8")
        replacement = self.root / "replacement-ignore"
        replacement.write_text("*\n", encoding="utf-8")
        original_scan = self.results._scan

        def scan_then_swap(*args: object, **kwargs: object) -> object:
            states = original_scan(*args, **kwargs)  # type: ignore[arg-type]
            ignore_file.unlink()
            ignore_file.symlink_to(replacement)
            return states

        with (
            mock.patch.object(self.results, "_scan", side_effect=scan_then_swap),
            self.assertRaisesRegex(ResultError, "pin result ignore rules"),
        ):
            self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)

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

    def test_inspect_accepts_equivalent_links_in_the_trusted_result_store(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        manifest = captured.directory / "manifest.json"
        alias = self.root / "manifest-hardlink"
        try:
            os.link(manifest, alias)
        except OSError as exc:
            self.skipTest(f"hard links are unavailable: {exc}")

        self.assertEqual(self.results.inspect(ATTEMPT_ID), captured.manifest)

        alias.unlink()
        outside = self.root / "outside-manifest"
        manifest.replace(outside)
        manifest.symlink_to(outside)
        self.assertEqual(self.results.inspect(ATTEMPT_ID), captured.manifest)

    def test_inspect_rejects_a_fifo_manifest_without_reading_it(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        manifest = captured.directory / "manifest.json"
        raw = manifest.read_bytes()
        manifest.unlink()
        try:
            os.mkfifo(manifest, mode=0o600)
        except OSError as exc:
            self.skipTest(f"FIFOs are unavailable: {exc}")

        with (
            mock.patch.object(Path, "read_bytes", return_value=raw) as read_bytes,
            self.assertRaisesRegex(ResultError, "result manifest"),
        ):
            self.results.inspect(ATTEMPT_ID)

        read_bytes.assert_not_called()

    def test_inspect_bounds_manifest_before_allocating_its_full_contents(
        self,
    ) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        manifest = captured.directory / "manifest.json"
        limit = manifest.stat().st_size - 1

        with (
            mock.patch("magic_glass_box.results.MAX_MANIFEST_BYTES", limit),
            mock.patch.object(
                Path,
                "read_bytes",
                side_effect=AssertionError("unbounded manifest read"),
            ) as read_bytes,
            self.assertRaisesRegex(ResultError, "result manifest is invalid"),
        ):
            self.results.inspect(ATTEMPT_ID)

        read_bytes.assert_not_called()

    def test_inspect_accepts_an_equivalent_trusted_blob_directory_link(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        blobs = captured.directory / "blobs"
        outside = self.root / "outside-blobs"
        blobs.replace(outside)
        blobs.symlink_to(outside, target_is_directory=True)

        self.assertEqual(self.results.inspect(ATTEMPT_ID), captured.manifest)

    def test_inspect_does_not_reaudit_modes_inside_the_private_store(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)

        for path, message in (
            (captured.directory, "captured result"),
            (captured.directory / "blobs", "result blobs"),
        ):
            with self.subTest(path=path.name, former_error=message):
                path.chmod(0o755)
                self.assertEqual(self.results.inspect(ATTEMPT_ID), captured.manifest)
                path.chmod(0o700)

    def test_capture_accepts_an_equivalent_trusted_staged_baseline_link(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        stage = self.root / "results" / ".staging" / f"{ATTEMPT_ID}.json"
        outside = self.root / "outside-baseline"
        stage.replace(outside)
        stage.symlink_to(outside)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")

        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        self.assertEqual(captured.manifest.attempt_id, ATTEMPT_ID)

    def test_inspect_reenforces_the_configured_changed_byte_limit(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_bytes(b"larger result body\n")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        constrained = WorkspaceResultStore(
            self.root / "results",
            max_changed_bytes=captured.manifest.changed_bytes - 1,
        )

        with self.assertRaisesRegex(
            ResultError, r"^result changed content exceeds the size limit$"
        ):
            constrained.inspect(ATTEMPT_ID)

    def test_inspect_reenforces_the_configured_changed_path_limit(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "one.txt").write_bytes(b"one\n")
        (self.workspace / "two.txt").write_bytes(b"two\n")
        self.results.capture(ATTEMPT_ID, self.workspace)
        constrained = WorkspaceResultStore(self.root / "results", max_paths=1)

        with self.assertRaisesRegex(
            ResultError, r"^result contains too many changed paths$"
        ):
            constrained.inspect(ATTEMPT_ID)

    def test_inspect_and_load_accept_an_equivalent_trusted_hardlink(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        state = captured.manifest.changes[0].after
        assert state is not None and state.sha256 is not None
        blob = captured.directory / "blobs" / state.sha256
        try:
            os.link(blob, self.root / "result-blob-link")
        except OSError as exc:
            self.skipTest(f"hard links are unavailable: {exc}")

        self.assertEqual(self.results.inspect(ATTEMPT_ID), captured.manifest)
        self.assertEqual(self.results.load_blobs(ATTEMPT_ID)[state.sha256], b"after\n")

    def test_inspect_accepts_an_equivalent_trusted_blob_link(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("after\n", encoding="utf-8")
        captured = self.results.capture(ATTEMPT_ID, self.workspace)
        state = captured.manifest.changes[0].after
        assert state is not None and state.sha256 is not None
        blob = captured.directory / "blobs" / state.sha256
        replacement = self.root / "external-result-body"
        replacement.write_bytes(blob.read_bytes())
        replacement.chmod(0o600)
        blob.unlink()
        blob.symlink_to(replacement)

        self.assertEqual(self.results.inspect(ATTEMPT_ID), captured.manifest)

    def test_directory_replacement_indexes_deletions_once(self) -> None:
        body = b"leaf\n"
        leaf = FileState(
            kind="file",
            mode=0o644,
            size=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
        )
        replacement = FileState(
            kind="file",
            mode=0o644,
            size=1,
            sha256=hashlib.sha256(b"x").hexdigest(),
        )
        target = self.root / "many-directory-replacements"
        target.mkdir(mode=0o700)
        changes: list[ResultChange] = []
        replacement_count = 32
        for index in range(replacement_count):
            relative = f"tree-{index:03d}"
            directory = target / relative
            directory.mkdir()
            (directory / "leaf.txt").write_bytes(body)
            changes.extend(
                (
                    ResultChange(relative, "added", None, replacement),
                    ResultChange(f"{relative}/leaf.txt", "deleted", leaf, None),
                )
            )

        class CountingChanges(list[ResultChange]):
            iterations = 0

            def __iter__(self):  # type: ignore[no-untyped-def]
                self.iterations += 1
                return super().__iter__()

        counted = CountingChanges(sorted(changes, key=lambda change: change.path))

        plans = self.results._preflight_apply(target, counted)

        self.assertEqual(len(plans), replacement_count)
        self.assertLessEqual(counted.iterations, 2)

    def test_stage_rejects_changed_replay_and_reserved_git_metadata(self) -> None:
        self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)
        (self.workspace / "modify.txt").write_text("changed baseline\n")
        with self.assertRaisesRegex(ResultError, "different baseline"):
            self.results.stage(ATTEMPT_ID, self.workspace, COMMIT)

        other = self.root / "other-workspace"
        (other / ".git").mkdir(parents=True)
        with self.assertRaisesRegex(ResultError, "reserved .git path"):
            self.results.stage("att_" + "9" * 32, other, COMMIT)

    def test_stage_rejects_cross_platform_aliases_for_git_metadata(self) -> None:
        aliases = (
            ".GIT",
            ".Git . . .",
            "git~1",
            ".git::$INDEX_ALLOCATION",
            ".g\u200cit",
        )
        for index, alias in enumerate(aliases):
            with self.subTest(alias=alias):
                workspace = self.root / f"reserved-workspace-{index}"
                hook = workspace / alias / "hooks" / "pre-commit"
                hook.parent.mkdir(parents=True)
                hook.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
                hook.chmod(0o755)
                store = WorkspaceResultStore(self.root / f"reserved-results-{index}")
                with self.assertRaisesRegex(ResultError, "reserved .git path"):
                    store.stage("att_" + f"{index:x}" * 32, workspace, COMMIT)

        safe_workspace = self.root / "safe-workspace"
        workflow = safe_workspace / ".github" / "workflows" / "ci.yml"
        workflow.parent.mkdir(parents=True)
        workflow.write_text("name: CI\n", encoding="utf-8")
        safe_store = WorkspaceResultStore(self.root / "safe-results")
        safe_store.stage("att_" + "f" * 32, safe_workspace, COMMIT)


if __name__ == "__main__":
    unittest.main()
