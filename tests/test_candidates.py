from __future__ import annotations

import hashlib
import os
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest import mock

from magic_glass_box.candidates import (
    CANDIDATE_SCHEMA_VERSION,
    MAX_BASE_IMPORT_BYTES,
    CandidateError,
    CandidateManifest,
    GitCandidateStore,
    MaterializedCandidate,
)
from magic_glass_box.db import (
    CANDIDATE_SCHEMA,
    SCHEMA,
    SCHEMA_VERSION,
    ConflictError,
    Database,
    NotFoundError,
)
from magic_glass_box.domain import AgentProfileSpec, ProjectContext, WorkItemEnvelope
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import (
    FileState,
    ResultChange,
    ResultManifest,
    WorkspaceResultStore,
)
from magic_glass_box.service import ControlPlane
from magic_glass_box.workspace import MAX_SNAPSHOT_BYTES, GitSnapshotter


def git(binary: Path, *arguments: str, cwd: Path | None = None) -> str:
    completed = subprocess.run(  # noqa: S603 - test-controlled Git executable
        [str(binary), *arguments],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=True,
    )
    return completed.stdout.decode("utf-8", "strict").strip()


class CandidateManifestTests(unittest.TestCase):
    def manifest(self) -> CandidateManifest:
        return CandidateManifest(
            CANDIDATE_SCHEMA_VERSION,
            "att_" + "1" * 32,
            "rsl_" + "2" * 32,
            "run_" + "3" * 32,
            "boxp_" + "4" * 32,
            "ctx_" + "5" * 32,
            "wth_" + "6" * 32,
            "a" * 40,
            "b" * 64,
            "c" * 40,
            "d" * 40,
            "sha1",
        )

    def test_round_trip_is_canonical_and_content_addressed(self) -> None:
        manifest = self.manifest()

        self.assertEqual(CandidateManifest.from_mapping(manifest.to_dict()), manifest)
        self.assertEqual(len(manifest.digest), 64)
        self.assertEqual(manifest.canonical, manifest.canonical)

    def test_rejects_invalid_shape_identity_and_object_ids(self) -> None:
        manifest = self.manifest()
        with self.assertRaisesRegex(CandidateError, "must be an object"):
            CandidateManifest.from_mapping([])
        with self.assertRaisesRegex(CandidateError, "unknown fields"):
            CandidateManifest.from_mapping({**manifest.to_dict(), "extra": True})
        value = manifest.to_dict()
        value.pop("attempt_id")
        with self.assertRaisesRegex(CandidateError, "missing fields"):
            CandidateManifest.from_mapping(value)
        for field, bad in (
            ("schema_version", 2),
            ("attempt_id", "run_" + "1" * 32),
            ("attempt_result_id", "res_" + "2" * 32),
            ("producer_run_id", "not-an-id"),
            ("project_id", ""),
            ("context_version_id", "ctxv_" + "5" * 32),
            ("work_thread_id", "wth_not-hex"),
            ("object_format", "md5"),
            ("base_revision", "f" * 64),
            ("head_revision", "not-an-object"),
            ("tree_revision", "e" * 39),
            ("result_content_sha256", "0" * 63),
        ):
            with self.subTest(field=field), self.assertRaises(CandidateError):
                replace(manifest, **{field: bad})


class CandidateKernelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        discovered = shutil.which("git")
        if discovered is None:
            self.skipTest("Git CLI is required")
        self.git_binary = Path(discovered)
        self.repository = self.root / "source"
        self.repository.mkdir()
        git(
            self.git_binary,
            "init",
            "--quiet",
            "--initial-branch=main",
            cwd=self.repository,
        )
        (self.repository / "tracked.txt").write_text("before\n", encoding="utf-8")
        (self.repository / "delete.txt").write_text("delete\n", encoding="utf-8")
        (self.repository / "script.sh").write_text("#!/bin/sh\n", encoding="utf-8")
        nested = self.repository / "nested"
        nested.mkdir()
        (nested / "base.txt").write_text("nested\n", encoding="utf-8")
        (self.repository / "base-link").symlink_to("tracked.txt")
        git(self.git_binary, "add", ".", cwd=self.repository)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "base",
            cwd=self.repository,
        )
        self.base = git(self.git_binary, "rev-parse", "HEAD", cwd=self.repository)
        (self.repository / "operator-only.txt").write_text(
            "do not import\n", encoding="utf-8"
        )

        self.attempt_id = "att_" + "1" * 32
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        GitSnapshotter(binary=self.git_binary).snapshot(
            self.repository, self.base, self.workspace
        )
        self.results = WorkspaceResultStore(
            self.root / "results", git_binary=self.git_binary
        )
        self.results.stage(self.attempt_id, self.workspace, self.base)
        (self.workspace / "tracked.txt").write_text("after\n", encoding="utf-8")
        (self.workspace / "delete.txt").unlink()
        (self.workspace / "added.txt").write_bytes(b"new\x00content")
        (self.workspace / "script.sh").chmod(0o755)
        (self.workspace / "latest").symlink_to("tracked.txt")
        captured = self.results.capture(self.attempt_id, self.workspace)
        self.result, self.blobs = self.results.load_bundle(
            self.attempt_id, expected_digest=captured.manifest.digest
        )
        self.project_id = "boxp_" + "2" * 32
        self.store = GitCandidateStore(self.root / "candidates", binary=self.git_binary)

    def candidate_repository(self, object_format: str = "sha1") -> Path:
        key = hashlib.sha256(self.project_id.encode()).hexdigest()
        return self.store.root / "projects" / key / object_format / "repository.git"

    def manifest(self, materialized: MaterializedCandidate) -> CandidateManifest:
        return CandidateManifest(
            CANDIDATE_SCHEMA_VERSION,
            self.attempt_id,
            "rsl_" + "3" * 32,
            "run_" + "4" * 32,
            self.project_id,
            "ctx_" + "5" * 32,
            "wth_" + "6" * 32,
            materialized.base_revision,
            self.result.digest,
            materialized.head_revision,
            materialized.tree_revision,
            materialized.object_format,
        )

    def candidate_git(self, *arguments: str) -> str:
        return git(
            self.git_binary,
            f"--git-dir={self.candidate_repository()}",
            *arguments,
        )

    def test_materializes_raw_deterministic_candidate_without_source_mutation(
        self,
    ) -> None:
        source_head = git(self.git_binary, "rev-parse", "HEAD", cwd=self.repository)
        source_refs = git(
            self.git_binary,
            "for-each-ref",
            "--format=%(refname):%(objectname)",
            cwd=self.repository,
        )
        source_status = git(
            self.git_binary,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            cwd=self.repository,
        )
        first = self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )
        second = self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )

        self.assertEqual(second, first)
        self.assertEqual(first.base_revision, self.base)
        self.assertEqual(first.object_format, "sha1")
        self.assertEqual(
            self.candidate_git("show", f"{first.head_revision}:tracked.txt"),
            "after",
        )
        self.assertEqual(
            self.candidate_git("show", f"{first.head_revision}:added.txt"),
            "new\x00content",
        )
        with self.assertRaises(subprocess.CalledProcessError):
            self.candidate_git("cat-file", "-e", f"{first.head_revision}:delete.txt")
        self.assertEqual(
            self.candidate_git("rev-parse", f"{first.head_revision}^"), self.base
        )
        self.assertEqual(
            self.candidate_git("ls-tree", first.head_revision, "script.sh").split()[0],
            "100755",
        )
        self.assertEqual(
            self.candidate_git("show", f"{first.head_revision}:latest"),
            "tracked.txt",
        )
        with self.assertRaises(subprocess.CalledProcessError):
            self.candidate_git(
                "cat-file", "-e", f"{first.head_revision}:operator-only.txt"
            )
        self.assertEqual(
            git(self.git_binary, "rev-parse", "HEAD", cwd=self.repository),
            source_head,
        )
        self.assertEqual(
            git(
                self.git_binary,
                "for-each-ref",
                "--format=%(refname):%(objectname)",
                cwd=self.repository,
            ),
            source_refs,
        )
        self.assertEqual(
            git(
                self.git_binary,
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                cwd=self.repository,
            ),
            source_status,
        )
        self.assertTrue((self.repository / "operator-only.txt").exists())
        self.assertEqual(
            self.store.inspect(self.manifest(first)),
            first,
        )

    def test_candidate_import_rejects_a_source_backed_by_git_alternates(self) -> None:
        shared = self.root / "shared-source"
        git(
            self.git_binary,
            "clone",
            "--quiet",
            "--shared",
            "--no-checkout",
            str(self.repository),
            str(shared),
            cwd=self.root,
        )
        alternate = shared / ".git" / "objects" / "info" / "alternates"
        self.assertTrue(alternate.read_text(encoding="utf-8").strip())

        with self.assertRaisesRegex(CandidateError, "alternates"):
            self.store.materialize(self.project_id, shared, self.result, self.blobs)

        self.assertFalse(self.candidate_repository().exists())

    def test_refolds_original_result_onto_new_base_and_recovers_exact_revision(
        self,
    ) -> None:
        original = self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )
        origin_manifest = self.manifest(original)
        (self.repository / "nested" / "base.txt").write_text(
            "landed-a\n", encoding="utf-8"
        )
        git(self.git_binary, "add", "nested/base.txt", cwd=self.repository)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "land A",
            cwd=self.repository,
        )
        new_base = git(self.git_binary, "rev-parse", "HEAD", cwd=self.repository)

        first = self.store.refold(
            origin_manifest,
            self.repository,
            self.result,
            self.blobs,
            base_revision=new_base,
        )
        second = self.store.refold(
            origin_manifest,
            self.repository,
            self.result,
            self.blobs,
            base_revision=new_base,
        )

        self.assertEqual(second, first)
        self.assertEqual(
            self.candidate_git("rev-parse", f"{first.head_revision}^"), new_base
        )
        self.assertEqual(
            self.candidate_git("show", f"{first.head_revision}:tracked.txt"),
            "after",
        )
        self.assertEqual(
            self.candidate_git("show", f"{first.head_revision}:nested/base.txt"),
            "landed-a",
        )
        self.assertEqual(
            self.candidate_git("show", f"{original.head_revision}:nested/base.txt"),
            "nested",
        )

        exact = replace(
            origin_manifest,
            base_revision=first.base_revision,
            head_revision=first.head_revision,
            tree_revision=first.tree_revision,
        )
        self.assertEqual(self.store.inspect(exact), first)
        self.assertEqual(
            self.store.repository_path(self.project_id, first.object_format),
            self.candidate_repository(),
        )

        shutil.rmtree(self.store.root)
        recovered = self.store.recover_revision(
            exact, self.repository, self.result, self.blobs
        )
        self.assertEqual(recovered, first)
        self.assertEqual(self.store.inspect(exact), first)

    def test_refold_rejects_new_base_that_changed_a_worker_preimage(self) -> None:
        original = self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )
        origin_manifest = self.manifest(original)
        (self.repository / "tracked.txt").write_text("someone else\n", encoding="utf-8")
        git(self.git_binary, "add", "tracked.txt", cwd=self.repository)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "conflict",
            cwd=self.repository,
        )
        conflicting_base = git(
            self.git_binary, "rev-parse", "HEAD", cwd=self.repository
        )

        with self.assertRaisesRegex(CandidateError, "base"):
            self.store.refold(
                origin_manifest,
                self.repository,
                self.result,
                self.blobs,
                base_revision=conflicting_base,
            )

    def test_common_umask_succeeds_and_hostile_git_configuration_is_inert(
        self,
    ) -> None:
        (self.repository / "tracked.txt").write_text(
            "replacement base\n", encoding="utf-8"
        )
        git(self.git_binary, "add", "tracked.txt", cwd=self.repository)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "replacement",
            cwd=self.repository,
        )
        replacement = git(self.git_binary, "rev-parse", "HEAD", cwd=self.repository)
        git(
            self.git_binary,
            "replace",
            self.base,
            replacement,
            cwd=self.repository,
        )
        marker = self.root / "hook-ran"
        hook = self.repository / ".git" / "hooks" / "reference-transaction"
        hook.write_text(f"#!/bin/sh\ntouch '{marker}'\n", encoding="utf-8")
        hook.chmod(0o755)
        hostile = self.root / "hostile-global-config"
        hostile.write_text("[core]\n\thooksPath = /does/not/exist\n", encoding="utf-8")
        old_umask = os.umask(0o002)
        try:
            with mock.patch.dict(
                os.environ,
                {
                    "GIT_CONFIG_GLOBAL": str(hostile),
                    "GIT_DIR": str(self.root / "wrong.git"),
                    "GIT_WORK_TREE": str(self.root / "wrong-tree"),
                },
            ):
                materialized = self.store.materialize(
                    self.project_id, self.repository, self.result, self.blobs
                )
        finally:
            os.umask(old_umask)

        self.assertFalse(marker.exists())
        self.assertEqual(self.store.inspect(self.manifest(materialized)), materialized)

    def test_timeout_terminates_the_git_process_group(self) -> None:
        process = mock.MagicMock()
        process.pid = 4242
        process.wait.return_value = 0

        with (
            mock.patch(
                "magic_glass_box.candidates.GitCandidateStore._bounded_process",
                side_effect=subprocess.TimeoutExpired(["git"], 1),
            ),
            self.assertRaisesRegex(CandidateError, "timed out"),
        ):
            self.store._run(
                ["version"],
                label="finish the test command",
                operation=self.store._new_operation(),
            )

        with mock.patch("magic_glass_box.candidates.os.killpg") as killpg:
            self.store._stop(process)  # noqa: SLF001 - process-group contract
        killpg.assert_called_once_with(4242, signal.SIGTERM)

    def test_git_output_limit_stops_the_process_before_full_allocation(self) -> None:
        marker = self.root / "fake-git-completed"
        fake_git = self.root / "fake-git"
        fake_git.write_text(
            "#!/bin/sh\n"
            "dd if=/dev/zero bs=65536 count=128 2>/dev/null\n"
            ': > "$MGB_TEST_MARKER"\n',
            encoding="utf-8",
        )
        fake_git.chmod(0o755)
        store = GitCandidateStore(self.root / "bounded-store", binary=fake_git)

        with self.assertRaisesRegex(CandidateError, "output limit"):
            store._run(  # noqa: SLF001 - exercise the hostile process boundary
                ["ignored"],
                label="bound fake Git output",
                operation=store._new_operation(),  # noqa: SLF001
                output_limit=1024 * 1024,
                extra_environment={"MGB_TEST_MARKER": str(marker)},
            )

        self.assertFalse(marker.exists())

    def test_recover_rebuilds_deleted_cache_and_missing_ref(self) -> None:
        original = self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )
        manifest = self.manifest(original)
        shutil.rmtree(self.store.root / "projects")

        rebuilt = self.store.recover(manifest, self.repository, self.result, self.blobs)
        self.assertEqual(rebuilt, original)

        ref = f"refs/mgb/candidates/{original.head_revision}"
        self.candidate_git("update-ref", "-d", ref)
        with self.assertRaisesRegex(CandidateError, "ref is missing"):
            self.store.inspect(manifest)
        repaired = self.store.recover(
            manifest, self.repository, self.result, self.blobs
        )
        self.assertEqual(repaired, original)

    def test_materialization_sweeps_abandoned_operation_directories(self) -> None:
        self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )
        directory = self.store._format_directory(self.project_id, "sha1", create=False)
        abandoned = directory / "operation-crashed"
        abandoned.mkdir()
        (abandoned / "partial-index").write_bytes(b"incomplete")

        self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )

        self.assertFalse(abandoned.exists())

    def test_recover_repairs_a_missing_candidate_ref_from_the_cached_base(
        self,
    ) -> None:
        original = self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )
        manifest = self.manifest(original)
        ref = f"refs/mgb/candidates/{original.head_revision}"
        self.candidate_git("update-ref", "-d", ref)
        self.repository.rename(self.root / "source-moved")

        repaired = self.store.recover(
            manifest, self.repository, self.result, self.blobs
        )

        self.assertEqual(repaired, original)
        self.assertEqual(self.store.inspect(manifest), original)

    def test_base_blob_validation_has_no_smaller_aggregate_cap_than_import(
        self,
    ) -> None:
        self.assertEqual(MAX_BASE_IMPORT_BYTES, MAX_SNAPSHOT_BYTES)
        deleted = next(
            change for change in self.result.changes if change.path == "delete.txt"
        )
        result = replace(self.result, changes=(deleted,))

        materialized = self.store.materialize(
            self.project_id, self.repository, result, {}
        )

        with self.assertRaises(subprocess.CalledProcessError):
            self.candidate_git(
                "cat-file", "-e", f"{materialized.head_revision}:delete.txt"
            )

    def test_inspection_is_targeted_and_never_runs_whole_repository_fsck(self) -> None:
        materialized = self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )
        with mock.patch.object(self.store, "_run", wraps=self.store._run) as run:
            self.store.inspect(self.manifest(materialized))

        commands = [tuple(call.args[0]) for call in run.call_args_list]
        self.assertFalse(any("fsck" in command for command in commands))
        self.assertTrue(any("cat-file" in command for command in commands))

    def test_rejects_bad_blob_sets_empty_results_and_changed_bases(self) -> None:
        digest = next(iter(self.blobs))
        bad_sets = (
            {},
            {**self.blobs, "0" * 64: b"extra"},
            {**self.blobs, digest: b"wrong"},
        )
        for blobs in bad_sets:
            with (
                self.subTest(blobs=blobs.keys()),
                self.assertRaisesRegex(CandidateError, "blob"),
            ):
                self.store.materialize(
                    self.project_id, self.repository, self.result, blobs
                )
        empty = ResultManifest(1, self.attempt_id, self.base, ())
        with self.assertRaisesRegex(CandidateError, "no changes"):
            self.store.materialize(self.project_id, self.repository, empty, {})
        with self.assertRaisesRegex(CandidateError, "validated manifest"):
            self.store.materialize(  # type: ignore[arg-type]
                self.project_id, self.repository, object(), {}
            )
        wrong_before = replace(
            self.result,
            changes=(
                replace(
                    next(c for c in self.result.changes if c.path == "tracked.txt"),
                    before=FileState(
                        kind="file",
                        mode=0o644,
                        size=5,
                        sha256=hashlib.sha256(b"wrong").hexdigest(),
                    ),
                ),
            ),
        )
        changed_blob = {
            wrong_before.changes[0].after.sha256: self.blobs[
                wrong_before.changes[0].after.sha256
            ]
        }
        with self.assertRaisesRegex(CandidateError, "base changed"):
            self.store.materialize(
                self.project_id, self.repository, wrong_before, changed_blob
            )

    def test_recovery_requires_the_recorded_identity(self) -> None:
        materialized = self.store.materialize(
            self.project_id, self.repository, self.result, self.blobs
        )
        manifest = self.manifest(materialized)
        for changed in (
            replace(manifest, base_revision="f" * 40),
            replace(manifest, result_content_sha256="e" * 64),
        ):
            with (
                self.subTest(changed=changed),
                self.assertRaisesRegex(CandidateError, "recovery identity"),
            ):
                self.store.recover(changed, self.repository, self.result, self.blobs)

    def test_materializes_directory_leaf_transitions(self) -> None:
        transition_repo = self.root / "transition-source"
        transition_repo.mkdir()
        git(self.git_binary, "init", "--quiet", cwd=transition_repo)
        (transition_repo / "folder").mkdir()
        (transition_repo / "folder" / "old.txt").write_text("old\n")
        (transition_repo / "leaf").write_text("leaf\n")
        git(self.git_binary, "add", ".", cwd=transition_repo)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "base",
            cwd=transition_repo,
        )
        base = git(self.git_binary, "rev-parse", "HEAD", cwd=transition_repo)
        workspace = self.root / "transition-workspace"
        workspace.mkdir()
        GitSnapshotter(binary=self.git_binary).snapshot(
            transition_repo, base, workspace
        )
        attempt = "att_" + "7" * 32
        results = WorkspaceResultStore(
            self.root / "transition-results", git_binary=self.git_binary
        )
        results.stage(attempt, workspace, base)
        shutil.rmtree(workspace / "folder")
        (workspace / "folder").write_text("now a file\n")
        (workspace / "leaf").unlink()
        (workspace / "leaf").mkdir()
        (workspace / "leaf" / "child.txt").write_text("child\n")
        captured = results.capture(attempt, workspace)
        result, blobs = results.load_bundle(
            attempt, expected_digest=captured.manifest.digest
        )

        materialized = self.store.materialize(
            self.project_id, transition_repo, result, blobs
        )

        self.assertEqual(
            self.candidate_git("show", f"{materialized.head_revision}:folder"),
            "now a file",
        )
        self.assertEqual(
            self.candidate_git("show", f"{materialized.head_revision}:leaf/child.txt"),
            "child",
        )

    def test_rejects_changes_beneath_or_over_a_gitlink(self) -> None:
        gitlink_repo = self.root / "gitlink-source"
        shutil.copytree(
            self.repository,
            gitlink_repo,
            ignore=shutil.ignore_patterns("operator-only.txt"),
        )
        git(
            self.git_binary,
            "update-index",
            "--add",
            "--cacheinfo",
            f"160000,{self.base},module",
            cwd=gitlink_repo,
        )
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "gitlink",
            cwd=gitlink_repo,
        )
        base = git(self.git_binary, "rev-parse", "HEAD", cwd=gitlink_repo)
        body = b"not allowed\n"
        digest = hashlib.sha256(body).hexdigest()
        state = FileState(kind="file", mode=0o644, size=len(body), sha256=digest)
        for path in ("module/child.txt", "module"):
            result = ResultManifest(
                1,
                "att_" + "8" * 32,
                base,
                (ResultChange(path, "added", None, state),),
            )
            with (
                self.subTest(path=path),
                self.assertRaisesRegex(CandidateError, "Git link"),
            ):
                self.store.materialize(
                    self.project_id, gitlink_repo, result, {digest: body}
                )

    def test_rejects_unsafe_base_symlink_graph(self) -> None:
        unsafe = self.root / "unsafe-source"
        unsafe.mkdir()
        git(self.git_binary, "init", "--quiet", cwd=unsafe)
        (unsafe / "file.txt").write_text("before\n")
        (unsafe / "escape").symlink_to("../outside")
        git(self.git_binary, "add", ".", cwd=unsafe)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "unsafe",
            cwd=unsafe,
        )
        base = git(self.git_binary, "rev-parse", "HEAD", cwd=unsafe)
        before = b"before\n"
        after = b"after\n"
        result = ResultManifest(
            1,
            "att_" + "9" * 32,
            base,
            (
                ResultChange(
                    "file.txt",
                    "modified",
                    FileState(
                        kind="file",
                        mode=0o644,
                        size=len(before),
                        sha256=hashlib.sha256(before).hexdigest(),
                    ),
                    FileState(
                        kind="file",
                        mode=0o644,
                        size=len(after),
                        sha256=hashlib.sha256(after).hexdigest(),
                    ),
                ),
            ),
        )
        with self.assertRaisesRegex(CandidateError, "symlink"):
            self.store.materialize(
                self.project_id,
                unsafe,
                result,
                {hashlib.sha256(after).hexdigest(): after},
            )

    def test_base_symlink_identity_survives_same_blob_as_changed_regular_file(
        self,
    ) -> None:
        repository = self.root / "blob-identity-source"
        repository.mkdir()
        git(self.git_binary, "init", "--quiet", cwd=repository)
        (repository / "target.txt").write_text("target\n", encoding="utf-8")
        (repository / "a-link").symlink_to("target.txt")
        (repository / "z-collision.txt").write_text("target.txt", encoding="utf-8")
        git(self.git_binary, "add", ".", cwd=repository)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "shared blob identity",
            cwd=repository,
        )
        base = git(self.git_binary, "rev-parse", "HEAD", cwd=repository)
        workspace = self.root / "blob-identity-workspace"
        workspace.mkdir()
        GitSnapshotter(binary=self.git_binary).snapshot(repository, base, workspace)
        results = WorkspaceResultStore(
            self.root / "blob-identity-results", git_binary=self.git_binary
        )
        attempt_id = "att_" + "d" * 32
        results.stage(attempt_id, workspace, base)
        (workspace / "z-collision.txt").write_text("changed\n", encoding="utf-8")
        captured = results.capture(attempt_id, workspace)
        result, blobs = results.load_bundle(
            attempt_id, expected_digest=captured.manifest.digest
        )

        materialized = self.store.materialize(
            "boxp_" + "e" * 32, repository, result, blobs
        )
        candidate_repository = (
            self.store.root
            / "projects"
            / hashlib.sha256(("boxp_" + "e" * 32).encode()).hexdigest()
            / materialized.object_format
            / "repository.git"
        )

        self.assertEqual(
            git(
                self.git_binary,
                f"--git-dir={candidate_repository}",
                "show",
                f"{materialized.head_revision}:a-link",
            ),
            "target.txt",
        )
        self.assertEqual(
            git(
                self.git_binary,
                f"--git-dir={candidate_repository}",
                "show",
                f"{materialized.head_revision}:z-collision.txt",
            ),
            "changed",
        )

    def test_concurrent_materialization_is_idempotent(self) -> None:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(
                    self.store.materialize,
                    self.project_id,
                    self.repository,
                    self.result,
                    self.blobs,
                )
                for _ in range(4)
            ]
        materialized = [future.result() for future in futures]
        self.assertEqual(len(set(materialized)), 1)

    def test_sha256_repository_is_supported_when_git_supports_it(self) -> None:
        repository = self.root / "sha256-source"
        repository.mkdir()
        try:
            git(
                self.git_binary,
                "init",
                "--quiet",
                "--object-format=sha256",
                cwd=repository,
            )
        except subprocess.CalledProcessError:
            self.skipTest("installed Git does not support SHA-256 repositories")
        before, after = b"before\n", b"after\n"
        (repository / "file.txt").write_bytes(before)
        git(self.git_binary, "add", ".", cwd=repository)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "base",
            cwd=repository,
        )
        base = git(self.git_binary, "rev-parse", "HEAD", cwd=repository)
        result = ResultManifest(
            1,
            "att_" + "a" * 32,
            base,
            (
                ResultChange(
                    "file.txt",
                    "modified",
                    FileState(
                        kind="file",
                        mode=0o644,
                        size=len(before),
                        sha256=hashlib.sha256(before).hexdigest(),
                    ),
                    FileState(
                        kind="file",
                        mode=0o644,
                        size=len(after),
                        sha256=hashlib.sha256(after).hexdigest(),
                    ),
                ),
            ),
        )
        materialized = self.store.materialize(
            self.project_id,
            repository,
            result,
            {hashlib.sha256(after).hexdigest(): after},
        )
        self.assertEqual(materialized.object_format, "sha256")
        self.assertEqual(len(materialized.head_revision), 64)


class CandidateServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        discovered = shutil.which("git")
        if discovered is None:
            self.skipTest("Git CLI is required")
        self.git_binary = Path(discovered)
        self.repository = self.root / "source"
        self.repository.mkdir()
        git(self.git_binary, "init", "--quiet", cwd=self.repository)
        (self.repository / "file.txt").write_text("before\n")
        git(self.git_binary, "add", ".", cwd=self.repository)
        git(
            self.git_binary,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "base",
            cwd=self.repository,
        )
        self.base = git(self.git_binary, "rev-parse", "HEAD", cwd=self.repository)
        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Operator")
        instance = self.database.create_krab_instance("Local", "http://127.0.0.1:8765")
        identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "1" * 32, "worker", 1),
            "file:/private/token",
            user_id=self.user["id"],
        )
        self.project = self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="project",
            display_name="Project",
            context=ProjectContext.from_mapping(
                {"purpose": "Test candidates", "repository_path": str(self.repository)}
            ),
            created_by=self.user["id"],
        )
        work_item = self.database.import_work_item(
            project_key="project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "candidate",
                    "source_updated_at": "2026-10-14T00:00:00Z",
                    "title": "Candidate",
                    "brief": "Build it",
                }
            ),
            imported_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.from_mapping(
                {"image": "worker:local", "command": ["worker", "run"]}
            ),
            krab_identity_id=identity["id"],
            created_by=self.user["id"],
        )
        run = self.database.create_run(
            project_key="project",
            profile_name="Worker",
            work_item_id=work_item["id"],
            requested_by=self.user["id"],
            source_revision=self.base,
        )
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id="candidate-test",
            expected_run_version=run["version"],
        )
        self.attempt = self.database.start_run_attempt(
            preparing["id"],
            owner_id="candidate-test",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision=self.base,
            container_id="a" * 64,
            container_name="candidate-test",
            image_id="sha256:" + "b" * 64,
        )
        workspace = self.root / "workspace"
        workspace.mkdir()
        GitSnapshotter(binary=self.git_binary).snapshot(
            self.repository, self.base, workspace
        )
        self.results = WorkspaceResultStore(
            self.root / "results", git_binary=self.git_binary
        )
        self.results.stage(self.attempt["id"], workspace, self.base)
        (workspace / "file.txt").write_text("after\n")
        captured = self.results.capture(self.attempt["id"], workspace)
        self.database.record_attempt_result(self.attempt["id"], captured.manifest)
        self.database.complete_run_attempt(
            self.attempt["id"],
            owner_id="candidate-test",
            expected_attempt_version=self.attempt["version"],
            exit_code=0,
            image_id="sha256:" + "b" * 64,
        )
        self.store = GitCandidateStore(self.root / "candidates", binary=self.git_binary)
        self.control = ControlPlane(self.database)

    def create(self) -> dict[str, object]:
        return self.control.create_candidate(
            self.attempt["id"],
            created_by_handle="operator",
            result_store=self.results,
            candidate_store=self.store,
        )

    def test_create_show_list_and_idempotent_retry(self) -> None:
        created = self.create()
        replayed = self.create()

        self.assertEqual(replayed, created)
        self.assertEqual(created["attempt_id"], self.attempt["id"])
        self.assertEqual(
            self.control.get_candidate(
                created["id"],
                result_store=self.results,
                candidate_store=self.store,
            ),
            created,
        )
        self.assertEqual(
            self.database.list_candidates(project_id=self.project["id"]),
            [created],
        )
        self.assertEqual(
            self.database.list_candidates(producer_run_id=self.attempt["run_id"]),
            [created],
        )
        self.assertEqual(
            self.database.list_candidates(work_thread_id=created["work_thread_id"]),
            [created],
        )
        self.assertEqual(self.database.list_candidates(project_id="missing"), [])

    def test_show_and_idempotent_create_inspect_a_healthy_cache_first(self) -> None:
        created = self.create()
        self.repository.rename(self.root / "source-moved")
        self.results.root.rename(self.root / "results-moved")

        shown = self.control.get_candidate(
            created["id"],
            result_store=self.results,
            candidate_store=self.store,
        )
        replayed = self.create()

        self.assertEqual(shown, created)
        self.assertEqual(replayed, created)

    def test_show_reconstructs_the_cache_from_ledger_and_result_store(self) -> None:
        created = self.create()
        shutil.rmtree(self.store.root)

        shown = self.control.get_candidate(
            created["id"],
            result_store=self.results,
            candidate_store=self.store,
        )

        self.assertEqual(shown, created)
        manifest = CandidateManifest.from_mapping(created["manifest"])
        self.assertEqual(
            self.store.inspect(manifest).head_revision,
            manifest.head_revision,
        )

    def test_candidate_reads_trust_the_immutable_ledger(self) -> None:
        created = self.create()
        with closing(self.database.connect()) as connection, connection:
            connection.execute(
                "UPDATE run_attempts SET state = 'failed', exit_code = 1 WHERE id = ?",
                (self.attempt["id"],),
            )

        self.assertEqual(self.database.get_candidate(created["id"]), created)
        self.assertEqual(self.database.list_candidates(), [created])
        self.assertEqual(
            self.control.get_candidate(
                created["id"],
                result_store=self.results,
                candidate_store=self.store,
            ),
            created,
        )

    def test_creation_authority_and_provenance_are_checked_once(self) -> None:
        source = self.database.get_candidate_source(self.attempt["id"])
        result, blobs = self.results.load_bundle(
            self.attempt["id"], expected_digest=source["result_content_sha256"]
        )
        materialized = self.store.materialize(
            source["project_id"], self.repository, result, blobs
        )
        manifest = CandidateManifest(
            CANDIDATE_SCHEMA_VERSION,
            self.attempt["id"],
            source["attempt_result_id"],
            source["producer_run_id"],
            source["project_id"],
            source["context_version_id"],
            source["work_thread_id"],
            materialized.base_revision,
            result.digest,
            materialized.head_revision,
            materialized.tree_revision,
            materialized.object_format,
        )
        with self.assertRaisesRegex(ConflictError, "provenance"):
            self.database.create_candidate(
                replace(manifest, producer_run_id="run_" + "f" * 32),
                created_by=self.user["id"],
            )
        with self.assertRaisesRegex(ConflictError, "provenance"):
            self.database.create_candidate(
                replace(manifest, base_revision="f" * 40),
                created_by=self.user["id"],
            )
        with closing(self.database.connect()) as connection, connection:
            connection.execute(
                "UPDATE run_attempts SET state = 'failed', exit_code = 1 WHERE id = ?",
                (self.attempt["id"],),
            )
        with self.assertRaisesRegex(ConflictError, "successful attempt"):
            self.database.create_candidate(manifest, created_by=self.user["id"])

    def test_candidate_rows_are_immutable_and_queries_are_bounded(self) -> None:
        created = self.create()
        with closing(self.database.connect()) as connection:
            with self.assertRaises(sqlite3.IntegrityError), connection:
                connection.execute(
                    "UPDATE integration_candidates SET head_revision = ? WHERE id = ?",
                    ("f" * 40, created["id"]),
                )
            with self.assertRaises(sqlite3.IntegrityError), connection:
                connection.execute(
                    "DELETE FROM integration_candidates WHERE id = ?",
                    (created["id"],),
                )
        for limit in (0, True, 501):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                self.database.list_candidates(limit=limit)
        with self.assertRaisesRegex(ValueError, "one scope"):
            self.database.list_candidates(
                project_id=self.project["id"],
                producer_run_id=self.attempt["run_id"],
            )
        with self.assertRaises(NotFoundError):
            self.database.get_candidate("cnd_" + "f" * 32)


class CandidateMigrationTests(unittest.TestCase):
    def test_schema_ten_migrates_existing_candidates_through_current_schema(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "control-plane.db"
            connection = sqlite3.connect(path)
            connection.executescript(SCHEMA[: SCHEMA.index(CANDIDATE_SCHEMA)])
            connection.execute("PRAGMA user_version = 10")
            connection.commit()
            connection.close()
            os.chmod(path, 0o600)

            database = Database(path)
            first = database.initialize()
            second = database.initialize()

            self.assertEqual(first["schema_version"], SCHEMA_VERSION)
            self.assertEqual(second, first)
            with closing(database.connect()) as connection:
                table = connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name = 'integration_candidates'"
                ).fetchone()
            self.assertIsNotNone(table)

    def test_fresh_schema_has_only_query_backed_candidate_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Database(Path(temporary) / "control-plane.db")
            database.initialize()
            with closing(database.connect()) as connection:
                names = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE type = 'index' AND tbl_name = 'integration_candidates' "
                        "AND sql IS NOT NULL"
                    )
                }
        self.assertEqual(
            names,
            {
                "integration_candidate_created",
                "integration_candidate_project_created",
                "integration_candidate_run_created",
                "integration_candidate_thread_created",
            },
        )
