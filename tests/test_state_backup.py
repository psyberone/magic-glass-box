from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from jsonschema import Draft202012Validator, FormatChecker

from magic_glass_box import state_backup as state_backup_module
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    WorkItemEnvelope,
    canonical_json,
)
from magic_glass_box.krab import KrabIdentity
from magic_glass_box.results import (
    FileState,
    ResultChange,
    ResultManifest,
    WorkspaceResultStore,
)
from magic_glass_box.state_backup import BackupError, StateBackup


class StateBackupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.state = self.root / "state"
        self.state.mkdir(mode=0o700)
        self.database = Database(self.state / "control-plane.db")
        self.database.initialize()
        self.user = self.database.create_user("operator", "Local Operator")
        instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        identity = self.database.store_krab_identity(
            instance["id"],
            KrabIdentity("usr_" + "1" * 32, "agent_one", 1),
            "file:/private/krab-agent-one",
            user_id=self.user["id"],
        )
        self.database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "2" * 32,
            krab_project_key="backup-project",
            display_name="Backup Project",
            context=ProjectContext.from_mapping(
                {"purpose": "Prove coherent backup and restore."}
            ),
            created_by=self.user["id"],
        )
        work_item = self.database.import_work_item(
            project_key="backup-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "backup-proof",
                    "source_updated_at": "2026-11-03T12:00:00Z",
                    "title": "Prove coherent state backup",
                    "brief": "Retain the database and authoritative result bytes.",
                }
            ),
            imported_by=self.user["id"],
        )
        self.database.create_agent_profile(
            name="Worker",
            spec=AgentProfileSpec.create(image="agent:local", command=["agent"]),
            krab_identity_id=identity["id"],
            created_by=self.user["id"],
        )
        run = self.database.create_run(
            project_key="backup-project",
            profile_name="Worker",
            work_item_id=work_item["id"],
            requested_by=self.user["id"],
            source_revision="a" * 40,
        )
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id="backup-proof",
            expected_run_version=run["version"],
        )
        running = self.database.start_run_attempt(
            preparing["id"],
            owner_id="backup-proof",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision="a" * 40,
            container_id="container-proof",
            container_name="backup-proof",
            image_id="agent@sha256:" + "b" * 64,
        )
        self.attempt_id = running["id"]
        self.manifest = ResultManifest(1, self.attempt_id, "a" * 40, ())
        self.results = WorkspaceResultStore(self.state / "results")
        result_root = self.results.ensure_root()
        result_directory = result_root / self.attempt_id
        result_directory.mkdir(mode=0o700)
        (result_directory / "blobs").mkdir(mode=0o700)
        manifest_path = result_directory / "manifest.json"
        manifest_path.write_text(self.manifest.canonical, encoding="utf-8")
        os.chmod(manifest_path, 0o600)
        self.database.record_attempt_result(self.attempt_id, self.manifest)
        self.second_attempt_id, self.second_manifest, self.second_content = (
            self._record_changed_result(work_item["id"])
        )
        candidates = self.state / "candidates"
        candidates.mkdir(mode=0o700)
        (candidates / "reconstructable.txt").write_text("not authoritative")

    def _record_changed_result(
        self, work_item_id: str
    ) -> tuple[str, ResultManifest, bytes]:
        run = self.database.create_run(
            project_key="backup-project",
            profile_name="Worker",
            work_item_id=work_item_id,
            requested_by=self.user["id"],
            source_revision="c" * 40,
        )
        preparing = self.database.begin_run_attempt(
            run["id"],
            runtime_adapter="command",
            owner_id="backup-proof-second",
            expected_run_version=run["version"],
        )
        running = self.database.start_run_attempt(
            preparing["id"],
            owner_id="backup-proof-second",
            expected_attempt_version=preparing["version"],
            expected_run_version=preparing["run_version"],
            source_revision="c" * 40,
            container_id="container-proof-second",
            container_name="backup-proof-second",
            image_id="agent@sha256:" + "d" * 64,
        )
        content = b"second retained result\n"
        digest = hashlib.sha256(content).hexdigest()
        state = FileState("file", mode=0o644, size=len(content), sha256=digest)
        manifest = ResultManifest(
            1,
            running["id"],
            "c" * 40,
            (ResultChange("proof.txt", "added", None, state),),
        )
        result_directory = self.results.root / running["id"]
        result_directory.mkdir(mode=0o700)
        blob_root = result_directory / "blobs"
        blob_root.mkdir(mode=0o700)
        manifest_path = result_directory / "manifest.json"
        manifest_path.write_text(manifest.canonical, encoding="utf-8")
        os.chmod(manifest_path, 0o600)
        blob_path = blob_root / digest
        blob_path.write_bytes(content)
        os.chmod(blob_path, 0o600)
        self.database.record_attempt_result(running["id"], manifest)
        return running["id"], manifest, content

    @staticmethod
    def _inventory_digest(*manifests: ResultManifest) -> str:
        digest = hashlib.sha256()
        for manifest in sorted(manifests, key=lambda item: item.attempt_id):
            digest.update(
                (
                    canonical_json(
                        {
                            "attempt_id": manifest.attempt_id,
                            "changed_bytes": manifest.changed_bytes,
                            "changed_paths": manifest.changed_paths,
                            "content_sha256": manifest.digest,
                        }
                    )
                    + "\n"
                ).encode("utf-8")
            )
        return digest.hexdigest()

    @staticmethod
    def _write_manifest(path: Path, value: object) -> None:
        path.write_text(canonical_json(value), encoding="utf-8")
        os.chmod(path, 0o600)

    def test_live_wal_backup_verifies_and_restores_all_authoritative_state(
        self,
    ) -> None:
        reader = self.database.connect()
        self.addCleanup(reader.close)
        reader.execute("BEGIN")
        reader.execute("SELECT COUNT(*) FROM users").fetchone()
        self.database.create_user("second_user", "Second User")
        self.assertTrue(Path(str(self.database.path) + "-wal").exists())

        backup = self.root / "backup"
        created = StateBackup(self.database).create(backup)

        schema = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "schemas/state-backup-manifest-v1.schema.json"
            ).read_text(encoding="utf-8")
        )
        Draft202012Validator.check_schema(schema)
        manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(manifest)
        self.assertEqual(created, StateBackup.verify(backup))
        self.assertEqual(created["retained_results"]["count"], 2)
        self.assertEqual(created["retained_results"]["changed_paths"], 1)
        self.assertEqual(
            created["retained_results"]["changed_bytes"], len(self.second_content)
        )
        self.assertEqual(
            created["retained_results"]["inventory_sha256"],
            self._inventory_digest(self.manifest, self.second_manifest),
        )
        self.assertFalse((backup / "candidates").exists())
        self.assertFalse((backup / "control-plane.db-wal").exists())

        target = self.root / "restored-state"
        restored = StateBackup.restore(backup, target)

        restored_database = Database(target / "control-plane.db")
        restored_status = restored_database.status()
        source_status = self.database.status()
        restored_status.pop("database")
        source_status.pop("database")
        self.assertEqual(restored_status, source_status)
        restored_result = WorkspaceResultStore(target / "results").inspect(
            self.attempt_id, expected_digest=self.manifest.digest
        )
        self.assertEqual(restored_result, self.manifest)
        self.assertFalse((target / "candidates").exists())
        self.assertEqual(restored["state_directory"], str(target))
        self.assertEqual(target.stat().st_mode & 0o777, 0o700)
        self.assertEqual((target / "control-plane.db").stat().st_mode & 0o777, 0o600)

    def test_manifest_parser_rejects_invalid_types_bounds_and_fixed_fields(
        self,
    ) -> None:
        backup = self.root / "manifest-validation-backup"
        StateBackup(self.database).create(backup)
        path = backup / "manifest.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        cases: tuple[tuple[str, tuple[str, ...], object], ...] = (
            ("schema boolean", ("schema_version",), True),
            ("application name", ("application", "name"), "other"),
            ("blank application version", ("application", "version"), " "),
            ("long application version", ("application", "version"), "v" * 65),
            ("blank build revision", ("application", "build_revision"), " "),
            ("database file", ("database", "file"), "other.db"),
            ("database schema boolean", ("database", "schema_version"), True),
            ("database schema lower bound", ("database", "schema_version"), 0),
            ("database bytes boolean", ("database", "bytes"), True),
            ("database bytes lower bound", ("database", "bytes"), 0),
            ("database digest type", ("database", "sha256"), 7),
            ("database digest format", ("database", "sha256"), "A" * 64),
            ("result directory", ("retained_results", "directory"), "other"),
            ("result count boolean", ("retained_results", "count"), True),
            ("result count lower bound", ("retained_results", "count"), -1),
            ("changed paths lower bound", ("retained_results", "changed_paths"), -1),
            ("changed bytes lower bound", ("retained_results", "changed_bytes"), -1),
            (
                "inventory digest format",
                ("retained_results", "inventory_sha256"),
                "0" * 63,
            ),
            (
                "exclusion value",
                ("excluded_state", "credentials"),
                "included",
            ),
        )
        for name, keys, replacement in cases:
            with self.subTest(name=name):
                value = json.loads(json.dumps(original))
                cursor = value
                for key in keys[:-1]:
                    cursor = cursor[key]
                cursor[keys[-1]] = replacement
                self._write_manifest(path, value)
                with self.assertRaises(BackupError):
                    StateBackup.verify(backup)

        for name, value in (
            ("not an object", []),
            (
                "missing field",
                {key: value for key, value in original.items() if key != "database"},
            ),
            ("unknown field", original | {"unexpected": True}),
        ):
            with self.subTest(name=name):
                self._write_manifest(path, value)
                with self.assertRaises(BackupError):
                    StateBackup.verify(backup)

    def test_backup_preserves_build_identity_and_uses_sibling_staging(self) -> None:
        backup = self.root / "build-identity-backup"
        real_mkdtemp = tempfile.mkdtemp
        with (
            mock.patch.dict(
                os.environ,
                {"MAGIC_GLASS_BOX_BUILD_REVISION": "revision-proof"},
                clear=False,
            ),
            mock.patch(
                "magic_glass_box.state_backup.tempfile.mkdtemp",
                wraps=real_mkdtemp,
            ) as mkdtemp,
            mock.patch(
                "magic_glass_box.state_backup.shutil.rmtree",
                wraps=shutil.rmtree,
            ) as remove_tree,
        ):
            created = StateBackup(self.database).create(backup)

        self.assertEqual(created["application"]["build_revision"], "revision-proof")
        mkdtemp.assert_called_once_with(prefix=".mgb-backup-", dir=backup.parent)
        remove_tree.assert_not_called()

    def test_restore_uses_sibling_staging_and_cleans_interrupted_copy(self) -> None:
        backup = self.root / "restore-staging-backup"
        StateBackup(self.database).create(backup)
        target = self.root / "restored-with-sibling-staging"
        real_mkdtemp = tempfile.mkdtemp
        with mock.patch(
            "magic_glass_box.state_backup.tempfile.mkdtemp", wraps=real_mkdtemp
        ) as mkdtemp:
            StateBackup.restore(backup, target)
        mkdtemp.assert_called_once_with(prefix=".mgb-restore-", dir=target.parent)

        interrupted = self.root / "interrupted-restore"
        with (
            mock.patch(
                "magic_glass_box.state_backup._copy_file",
                side_effect=BackupError("injected copy failure"),
            ),
            self.assertRaises(BackupError),
        ):
            StateBackup.restore(backup, interrupted)
        self.assertFalse(interrupted.exists())
        self.assertEqual(
            [
                path
                for path in self.root.iterdir()
                if path.name.startswith(".mgb-restore-")
            ],
            [],
        )

    def test_verify_rejects_extra_entries_nonempty_staging_and_linked_files(
        self,
    ) -> None:
        extra_root = self.root / "extra-root-backup"
        StateBackup(self.database).create(extra_root)
        (extra_root / "unexpected").write_text("unexpected", encoding="utf-8")
        with self.assertRaises(BackupError):
            StateBackup.verify(extra_root)

        extra_result = self.root / "extra-result-backup"
        StateBackup(self.database).create(extra_result)
        (extra_result / "results" / "unexpected").mkdir(mode=0o700)
        with self.assertRaises(BackupError):
            StateBackup.verify(extra_result)

        staged = self.root / "nonempty-staging-backup"
        StateBackup(self.database).create(staged)
        staged_file = staged / "results" / ".staging" / "partial"
        staged_file.write_text("partial", encoding="utf-8")
        os.chmod(staged_file, 0o600)
        with self.assertRaises(BackupError):
            StateBackup.verify(staged)

        linked_manifest = self.root / "linked-manifest-backup"
        StateBackup(self.database).create(linked_manifest)
        os.link(linked_manifest / "manifest.json", self.root / "manifest-hardlink")
        with self.assertRaises(BackupError):
            StateBackup.verify(linked_manifest)

        linked_blob = self.root / "linked-blob-backup"
        StateBackup(self.database).create(linked_blob)
        blob = (
            linked_blob
            / "results"
            / self.second_attempt_id
            / "blobs"
            / self.second_manifest.changes[0].after.sha256
        )
        os.link(blob, self.root / "blob-hardlink")
        with self.assertRaises(BackupError):
            StateBackup.verify(linked_blob)

    def test_restore_rejects_a_target_nested_inside_the_backup(self) -> None:
        backup = self.root / "nested-restore-backup"
        StateBackup(self.database).create(backup)

        with self.assertRaisesRegex(BackupError, "outside the backup"):
            StateBackup.restore(backup, backup / "nested-state")

    def test_low_level_writes_are_no_follow_complete_and_private(self) -> None:
        destination = self.root / "bounded-write"
        content = b"partial-write-proof"
        real_open = os.open
        real_write = os.write
        open_calls: list[tuple[int, int]] = []

        def capture_open(path: Path, flags: int, mode: int = 0o777) -> int:
            open_calls.append((flags, mode))
            return real_open(path, flags, mode)

        def partial_write(descriptor: int, value: memoryview) -> int:
            chunk = bytes(value[:1])
            return real_write(descriptor, chunk)

        with (
            mock.patch(
                "magic_glass_box.state_backup.os.open", side_effect=capture_open
            ),
            mock.patch(
                "magic_glass_box.state_backup.os.write", side_effect=partial_write
            ),
        ):
            state_backup_module._write_file(destination, content)

        self.assertEqual(destination.read_bytes(), content)
        self.assertEqual(destination.stat().st_mode & 0o777, 0o600)
        self.assertEqual(open_calls[0][1], 0o600)
        if hasattr(os, "O_NOFOLLOW"):
            self.assertTrue(open_calls[0][0] & os.O_NOFOLLOW)

        zero_write = self.root / "zero-write"
        with (
            mock.patch("magic_glass_box.state_backup.os.write", return_value=0),
            self.assertRaisesRegex(BackupError, "could not be written"),
        ):
            state_backup_module._write_file(zero_write, b"content")

    def test_copy_and_hash_helpers_use_bounded_reads(self) -> None:
        source = self.root / "bounded-source"
        destination = self.root / "bounded-copy"
        content = b"a" * (64 * 1024 + 17)
        source.write_bytes(content)
        os.chmod(source, 0o600)
        real_path_open = Path.open
        read_sizes: list[int | None] = []

        class Reader:
            def __init__(self, wrapped: object):
                self.wrapped = wrapped

            def read(self, size: int | None = -1) -> bytes:
                read_sizes.append(size)
                return self.wrapped.read(size)

            def close(self) -> None:
                self.wrapped.close()

            def __enter__(self) -> Reader:
                return self

            def __exit__(self, *_: object) -> None:
                self.close()

        def controlled_open(
            path: Path,
            mode: str = "r",
            buffering: int = -1,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
        ) -> object:
            opened = real_path_open(
                path,
                mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
            )
            return Reader(opened) if path == source and mode == "rb" else opened

        with mock.patch.object(
            Path, "open", autospec=True, side_effect=controlled_open
        ):
            state_backup_module._copy_file(source, destination, label="bounded source")
        self.assertEqual(destination.read_bytes(), content)
        self.assertEqual(read_sizes, [64 * 1024, 64 * 1024, 64 * 1024])

        read_sizes.clear()
        with mock.patch.object(
            Path, "open", autospec=True, side_effect=controlled_open
        ):
            size, digest = state_backup_module._hash_file(
                source, label="bounded source"
            )
        self.assertEqual(size, len(content))
        self.assertEqual(digest, hashlib.sha256(content).hexdigest())
        self.assertEqual(read_sizes, [64 * 1024, 64 * 1024, 64 * 1024])

    def test_manifest_read_is_no_follow_and_accepts_its_exact_size_limit(self) -> None:
        backup = self.root / "manifest-read-backup"
        StateBackup(self.database).create(backup)
        path = backup / "manifest.json"
        raw = path.read_bytes()
        real_open = os.open
        flags_seen: list[int] = []

        def capture_open(target: Path, flags: int, mode: int = 0o777) -> int:
            flags_seen.append(flags)
            return real_open(target, flags, mode)

        with (
            mock.patch(
                "magic_glass_box.state_backup.MAX_BACKUP_MANIFEST_BYTES", len(raw)
            ),
            mock.patch(
                "magic_glass_box.state_backup.os.open", side_effect=capture_open
            ),
        ):
            parsed = state_backup_module._read_manifest(path)
        self.assertEqual(parsed.to_dict(), json.loads(raw))
        if hasattr(os, "O_NOFOLLOW"):
            self.assertTrue(flags_seen[0] & os.O_NOFOLLOW)

        with (
            mock.patch(
                "magic_glass_box.state_backup.MAX_BACKUP_MANIFEST_BYTES",
                len(raw) - 1,
            ),
            self.assertRaisesRegex(BackupError, "exceeds the size limit"),
        ):
            state_backup_module._read_manifest(path)

    def test_directory_sync_and_owner_checks_enforce_local_safety(self) -> None:
        real_open = os.open
        flags_seen: list[int] = []

        def capture_open(path: Path, flags: int, mode: int = 0o777) -> int:
            flags_seen.append(flags)
            return real_open(path, flags, mode)

        with mock.patch(
            "magic_glass_box.state_backup.os.open", side_effect=capture_open
        ):
            state_backup_module._fsync_directory(self.root)
        if hasattr(os, "O_DIRECTORY"):
            self.assertEqual(flags_seen[0], os.O_RDONLY | os.O_DIRECTORY)
        else:
            self.assertEqual(flags_seen[0], os.O_RDONLY)

        regular = self.root / "not-a-directory"
        regular.write_text("file", encoding="utf-8")
        os.chmod(regular, 0o600)
        with self.assertRaisesRegex(BackupError, "real directory"):
            state_backup_module._private_directory(regular, label="test directory")
        with self.assertRaisesRegex(BackupError, "regular file"):
            state_backup_module._regular_file(self.root, label="test file")

        if hasattr(os, "getuid"):
            foreign_file = mock.Mock(
                st_mode=stat.S_IFREG | 0o600,
                st_nlink=1,
                st_uid=os.getuid() + 1,
            )
            with (
                mock.patch.object(Path, "lstat", return_value=foreign_file),
                self.assertRaisesRegex(BackupError, "current user"),
            ):
                state_backup_module._regular_file(regular, label="foreign file")
            foreign_directory = mock.Mock(
                st_mode=stat.S_IFDIR | 0o700,
                st_uid=os.getuid() + 1,
            )
            with (
                mock.patch.object(Path, "lstat", return_value=foreign_directory),
                self.assertRaisesRegex(BackupError, "current user"),
            ):
                state_backup_module._private_directory(
                    self.root, label="foreign directory"
                )

    def test_verify_and_restore_reject_missing_or_corrupt_result_content(self) -> None:
        backup = self.root / "backup"
        StateBackup(self.database).create(backup)
        manifest_path = backup / "results" / self.attempt_id / "manifest.json"
        manifest_path.write_text("{}", encoding="utf-8")

        with self.assertRaises(BackupError):
            StateBackup.verify(backup)
        target = self.root / "rejected-restore"
        with self.assertRaises(BackupError):
            StateBackup.restore(backup, target)
        self.assertFalse(target.exists())

    def test_verify_rejects_non_private_result_content(self) -> None:
        backup = self.root / "backup"
        StateBackup(self.database).create(backup)
        manifest_path = backup / "results" / self.attempt_id / "manifest.json"
        os.chmod(manifest_path, 0o644)

        with self.assertRaises(BackupError):
            StateBackup.verify(backup)

    def test_publication_and_restore_never_replace_existing_directories(self) -> None:
        existing_backup = self.root / "existing-backup"
        existing_backup.mkdir()
        with self.assertRaises(BackupError):
            StateBackup(self.database).create(existing_backup)

        backup = self.root / "backup"
        StateBackup(self.database).create(backup)
        existing_target = self.root / "existing-target"
        existing_target.mkdir()
        sentinel = existing_target / "keep"
        sentinel.write_text("unchanged", encoding="utf-8")
        with self.assertRaises(BackupError):
            StateBackup.restore(backup, existing_target)
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "unchanged")

    def test_relative_database_path_cannot_hide_a_destination_inside_live_state(
        self,
    ) -> None:
        relative_database = Database(
            Path(os.path.relpath(self.database.path, Path.cwd()))
        )
        nested_backup = self.state / "nested-backup"

        with self.assertRaisesRegex(BackupError, "outside the live state root"):
            StateBackup(relative_database).create(nested_backup)
        self.assertFalse(nested_backup.exists())

    def test_interrupted_backup_publication_leaves_no_visible_partial_backup(
        self,
    ) -> None:
        backup = self.root / "interrupted-backup"
        with (
            mock.patch(
                "magic_glass_box.state_backup.os.rename",
                side_effect=OSError("injected publication failure"),
            ),
            self.assertRaises(BackupError),
        ):
            StateBackup(self.database).create(backup)

        self.assertFalse(backup.exists())
        self.assertEqual(
            [path for path in self.root.iterdir() if path.name.startswith(".mgb-")],
            [],
        )


if __name__ == "__main__":
    unittest.main()
