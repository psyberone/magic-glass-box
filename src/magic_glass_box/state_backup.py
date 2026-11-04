from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import __version__
from .db import SCHEMA_VERSION, Database
from .domain import (
    ValidationError,
    canonical_json,
    parse_rfc3339_timestamp,
    require_string,
)
from .path_safety import read_bounded_descriptor
from .results import ResultError, ResultManifest, WorkspaceResultStore

BACKUP_SCHEMA_VERSION = 1
BACKUP_MANIFEST = "manifest.json"
BACKUP_DATABASE = "control-plane.db"
BACKUP_RESULTS = "results"
MAX_BACKUP_MANIFEST_BYTES = 64 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ATTEMPT_ID_RE = re.compile(r"^att_[0-9a-f]{32}$")
_EXCLUDED_STATE = {
    "candidate_store": "reconstructable_from_database_and_results",
    "credentials": "external_operator_managed",
    "gateways": "ephemeral",
    "workspaces": "ephemeral",
}


class BackupError(ValidationError):
    """A coherent state backup could not be created, verified, or restored."""


def _exact_mapping(value: object, *, fields: set[str], subject: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise BackupError(f"{subject} has missing or unknown fields")
    return value


def _integer(value: object, *, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise BackupError(f"{field} must be an integer of at least {minimum}")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise BackupError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _normalized_timestamp(value: object) -> str:
    try:
        parsed = parse_rfc3339_timestamp(value, field="backup created_at")
    except (TypeError, ValueError, ValidationError) as exc:
        raise BackupError("backup created_at must be an RFC 3339 timestamp") from exc
    return (
        parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


@dataclass(frozen=True)
class StateBackupManifest:
    created_at: str
    application_version: str
    build_revision: str | None
    database_bytes: int
    database_sha256: str
    database_schema_version: int
    result_count: int
    result_changed_paths: int
    result_changed_bytes: int
    result_inventory_sha256: str

    @classmethod
    def from_mapping(cls, value: object) -> StateBackupManifest:
        item = _exact_mapping(
            value,
            fields={
                "schema_version",
                "created_at",
                "application",
                "database",
                "retained_results",
                "excluded_state",
            },
            subject="state backup manifest",
        )
        if (
            isinstance(item["schema_version"], bool)
            or item["schema_version"] != BACKUP_SCHEMA_VERSION
        ):
            raise BackupError("state backup schema_version must be 1")
        application = _exact_mapping(
            item["application"],
            fields={"name", "version", "build_revision"},
            subject="state backup application",
        )
        if application["name"] != "magic-glass-box":
            raise BackupError("state backup application name is invalid")
        try:
            application_version = require_string(
                application["version"], field="application version", max_bytes=64
            )
        except ValueError as exc:
            raise BackupError(str(exc)) from exc
        if not application_version.strip():
            raise BackupError("application version must not be blank")
        build_revision = application["build_revision"]
        if build_revision is not None:
            try:
                build_revision = require_string(
                    build_revision, field="build revision", max_bytes=128
                )
            except ValueError as exc:
                raise BackupError(str(exc)) from exc
            if not build_revision.strip():
                raise BackupError("build revision must not be blank")

        database = _exact_mapping(
            item["database"],
            fields={"file", "schema_version", "bytes", "sha256"},
            subject="state backup database",
        )
        if database["file"] != BACKUP_DATABASE:
            raise BackupError("state backup database file is invalid")
        database_schema_version = _integer(
            database["schema_version"],
            field="database schema_version",
            minimum=1,
        )
        results = _exact_mapping(
            item["retained_results"],
            fields={
                "directory",
                "count",
                "changed_paths",
                "changed_bytes",
                "inventory_sha256",
            },
            subject="state backup retained_results",
        )
        if results["directory"] != BACKUP_RESULTS:
            raise BackupError("state backup result directory is invalid")
        excluded = _exact_mapping(
            item["excluded_state"],
            fields=set(_EXCLUDED_STATE),
            subject="state backup excluded_state",
        )
        if excluded != _EXCLUDED_STATE:
            raise BackupError("state backup exclusions are invalid")
        return cls(
            created_at=_normalized_timestamp(item["created_at"]),
            application_version=application_version,
            build_revision=build_revision,
            database_bytes=_integer(
                database["bytes"], field="database bytes", minimum=1
            ),
            database_sha256=_sha256(database["sha256"], field="database sha256"),
            database_schema_version=database_schema_version,
            result_count=_integer(results["count"], field="result count"),
            result_changed_paths=_integer(
                results["changed_paths"], field="result changed_paths"
            ),
            result_changed_bytes=_integer(
                results["changed_bytes"], field="result changed_bytes"
            ),
            result_inventory_sha256=_sha256(
                results["inventory_sha256"], field="result inventory_sha256"
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": BACKUP_SCHEMA_VERSION,
            "created_at": self.created_at,
            "application": {
                "name": "magic-glass-box",
                "version": self.application_version,
                "build_revision": self.build_revision,
            },
            "database": {
                "file": BACKUP_DATABASE,
                "schema_version": self.database_schema_version,
                "bytes": self.database_bytes,
                "sha256": self.database_sha256,
            },
            "retained_results": {
                "directory": BACKUP_RESULTS,
                "count": self.result_count,
                "changed_paths": self.result_changed_paths,
                "changed_bytes": self.result_changed_bytes,
                "inventory_sha256": self.result_inventory_sha256,
            },
            "excluded_state": dict(_EXCLUDED_STATE),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())


def _private_directory(path: Path, *, label: str) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise BackupError(f"{label} is unavailable") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise BackupError(f"{label} must be a real directory")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise BackupError(f"{label} is not owned by the current user")
    if info.st_mode & 0o077:
        raise BackupError(f"{label} must use mode 0700")


def _regular_file(path: Path, *, label: str) -> os.stat_result:
    try:
        info = path.lstat()
    except OSError as exc:
        raise BackupError(f"{label} is unavailable") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise BackupError(f"{label} must be a regular file")
    if info.st_nlink != 1:
        raise BackupError(f"{label} must not be hard-linked")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise BackupError(f"{label} is not owned by the current user")
    if info.st_mode & 0o077:
        raise BackupError(f"{label} must use mode 0600")
    return info


def _read_manifest(path: Path) -> StateBackupManifest:
    info = _regular_file(path, label="backup manifest")
    if info.st_size > MAX_BACKUP_MANIFEST_BYTES:
        raise BackupError("backup manifest exceeds the size limit")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        try:
            raw = read_bounded_descriptor(descriptor, maximum=MAX_BACKUP_MANIFEST_BYTES)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise BackupError("backup manifest could not be read") from exc
    if len(raw) > MAX_BACKUP_MANIFEST_BYTES:
        raise BackupError("backup manifest exceeds the size limit")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise BackupError("backup manifest is not valid UTF-8 JSON") from exc
    manifest = StateBackupManifest.from_mapping(value)
    if raw != manifest.canonical.encode("utf-8"):
        raise BackupError("backup manifest is not canonical")
    return manifest


def _write_file(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags, 0o600)
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short file write")
            view = view[written:]
        os.fsync(descriptor)
    except OSError as exc:
        raise BackupError("backup content could not be written") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _copy_file(source: Path, destination: Path, *, label: str) -> None:
    _regular_file(source, label=label)
    source_file = None
    destination_file = None
    try:
        source_file = source.open("rb")
        destination_file = destination.open("xb")
        os.chmod(destination, 0o600)
        while True:
            chunk = source_file.read(64 * 1024)
            if not chunk:
                break
            destination_file.write(chunk)
        destination_file.flush()
        os.fsync(destination_file.fileno())
    except OSError as exc:
        raise BackupError(f"{label} could not be copied") from exc
    finally:
        if source_file is not None:
            source_file.close()
        if destination_file is not None:
            destination_file.close()


def _hash_file(path: Path, *, label: str) -> tuple[int, str]:
    info = _regular_file(path, label=label)
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(64 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise BackupError(f"{label} could not be hashed") from exc
    return info.st_size, digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        descriptor = os.open(path, flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise BackupError("backup directory could not be synchronized") from exc


def _open_backup_database(path: Path) -> sqlite3.Connection:
    _regular_file(path, label="backup database")
    try:
        connection = sqlite3.connect(
            path.as_uri() + "?mode=ro&immutable=1",
            uri=True,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA trusted_schema = OFF")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
    except sqlite3.Error as exc:
        raise BackupError("backup database could not be opened") from exc


def _verify_database(connection: sqlite3.Connection) -> int:
    try:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version != SCHEMA_VERSION:
            raise BackupError(
                f"backup database schema {version} is not supported; "
                f"expected {SCHEMA_VERSION}"
            )
        integrity = connection.execute("PRAGMA quick_check").fetchone()[0]
        if integrity != "ok":
            raise BackupError(f"backup database integrity check failed: {integrity}")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise BackupError("backup database foreign key check failed")
        return version
    except sqlite3.Error as exc:
        raise BackupError("backup database verification failed") from exc


def _result_inventory_entry(row: sqlite3.Row) -> bytes:
    return (
        canonical_json(
            {
                "attempt_id": row["attempt_id"],
                "changed_bytes": row["changed_bytes"],
                "changed_paths": row["changed_paths"],
                "content_sha256": row["content_sha256"],
            }
        ).encode("utf-8")
        + b"\n"
    )


def _result_rows(connection: sqlite3.Connection) -> sqlite3.Cursor:
    try:
        return connection.execute(
            "SELECT attempt_id, manifest_json, content_sha256, changed_paths, "
            "changed_bytes FROM attempt_results ORDER BY attempt_id"
        )
    except sqlite3.Error as exc:
        raise BackupError("backup result inventory could not be read") from exc


def _write_result_bundle(
    result_root: Path,
    manifest: ResultManifest,
    blobs: dict[str, bytes],
) -> None:
    directory = result_root / manifest.attempt_id
    try:
        directory.mkdir(mode=0o700)
        blob_root = directory / "blobs"
        blob_root.mkdir(mode=0o700)
    except OSError as exc:
        raise BackupError("backup result directory could not be created") from exc
    _write_file(directory / "manifest.json", manifest.canonical.encode("utf-8"))
    for digest, content in sorted(blobs.items()):
        _write_file(blob_root / digest, content)
    _fsync_directory(blob_root)
    _fsync_directory(directory)


def _copy_results_from_database(
    connection: sqlite3.Connection,
    source_root: Path,
    destination_root: Path,
) -> tuple[int, int, int, str]:
    try:
        destination_root.mkdir(mode=0o700)
        (destination_root / ".staging").mkdir(mode=0o700)
    except OSError as exc:
        raise BackupError("backup result root could not be created") from exc
    source = WorkspaceResultStore(source_root)
    inventory = hashlib.sha256()
    count = 0
    changed_paths = 0
    changed_bytes = 0
    for row in _result_rows(connection):
        attempt_id = row["attempt_id"]
        content_sha256 = row["content_sha256"]
        if (
            not isinstance(attempt_id, str)
            or _ATTEMPT_ID_RE.fullmatch(attempt_id) is None
            or not isinstance(content_sha256, str)
            or _SHA256_RE.fullmatch(content_sha256) is None
        ):
            raise BackupError("backup database contains an invalid result identity")
        try:
            manifest, blobs = source.load_bundle(
                attempt_id, expected_digest=content_sha256
            )
        except ResultError as exc:
            raise BackupError("authoritative result content is unavailable") from exc
        if (
            row["manifest_json"] != manifest.canonical
            or row["changed_paths"] != manifest.changed_paths
            or row["changed_bytes"] != manifest.changed_bytes
        ):
            raise BackupError("database and retained result content disagree")
        _write_result_bundle(destination_root, manifest, blobs)
        inventory.update(_result_inventory_entry(row))
        count += 1
        changed_paths += manifest.changed_paths
        changed_bytes += manifest.changed_bytes
    _fsync_directory(destination_root / ".staging")
    _fsync_directory(destination_root)
    return count, changed_paths, changed_bytes, inventory.hexdigest()


def _verify_results(
    connection: sqlite3.Connection,
    result_root: Path,
) -> tuple[int, int, int, str]:
    _private_directory(result_root, label="backup result root")
    store = WorkspaceResultStore(result_root)
    inventory = hashlib.sha256()
    expected_entries = {".staging"}
    count = 0
    changed_paths = 0
    changed_bytes = 0
    for row in _result_rows(connection):
        attempt_id = row["attempt_id"]
        content_sha256 = row["content_sha256"]
        if (
            not isinstance(attempt_id, str)
            or _ATTEMPT_ID_RE.fullmatch(attempt_id) is None
            or not isinstance(content_sha256, str)
            or _SHA256_RE.fullmatch(content_sha256) is None
        ):
            raise BackupError("backup database contains an invalid result identity")
        directory = result_root / attempt_id
        _private_directory(directory, label="backup result directory")
        _private_directory(directory / "blobs", label="backup result blob directory")
        _regular_file(directory / "manifest.json", label="backup result manifest")
        try:
            manifest = store.inspect(attempt_id, expected_digest=content_sha256)
        except ResultError as exc:
            raise BackupError("backup retained result is invalid") from exc
        if (
            row["manifest_json"] != manifest.canonical
            or row["changed_paths"] != manifest.changed_paths
            or row["changed_bytes"] != manifest.changed_bytes
        ):
            raise BackupError("backup database and retained result disagree")
        expected_entries.add(attempt_id)
        try:
            blob_entries = tuple((directory / "blobs").iterdir())
        except OSError as exc:
            raise BackupError(
                "backup result blob directory could not be listed"
            ) from exc
        for blob in blob_entries:
            _regular_file(blob, label="backup result blob")
        inventory.update(_result_inventory_entry(row))
        count += 1
        changed_paths += manifest.changed_paths
        changed_bytes += manifest.changed_bytes
    try:
        actual_entries = {path.name for path in result_root.iterdir()}
    except OSError as exc:
        raise BackupError("backup result root could not be listed") from exc
    if actual_entries != expected_entries:
        raise BackupError("backup result set does not match the database")
    _private_directory(result_root / ".staging", label="backup result staging")
    try:
        if any((result_root / ".staging").iterdir()):
            raise BackupError("backup result staging must be empty")
    except OSError as exc:
        raise BackupError("backup result staging could not be listed") from exc
    return count, changed_paths, changed_bytes, inventory.hexdigest()


def _verify_payload(root: Path, manifest: StateBackupManifest) -> None:
    database_path = root / BACKUP_DATABASE
    database_bytes, database_sha256 = _hash_file(database_path, label="backup database")
    if (
        database_bytes != manifest.database_bytes
        or database_sha256 != manifest.database_sha256
    ):
        raise BackupError("backup database digest does not match the manifest")
    with closing(_open_backup_database(database_path)) as connection:
        version = _verify_database(connection)
        results = _verify_results(connection, root / BACKUP_RESULTS)
    if version != manifest.database_schema_version:
        raise BackupError("backup database schema does not match the manifest")
    expected_results = (
        manifest.result_count,
        manifest.result_changed_paths,
        manifest.result_changed_bytes,
        manifest.result_inventory_sha256,
    )
    if results != expected_results:
        raise BackupError("backup result inventory does not match the manifest")


class StateBackup:
    """Create and restore coherent SQLite-plus-result state snapshots."""

    def __init__(self, database: Database):
        if not isinstance(database, Database):
            raise BackupError("state backup requires a Database")
        self.database = database

    @staticmethod
    def _prepare_parent(destination: Path) -> Path:
        parent = destination.parent
        existed = parent.exists()
        try:
            parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not existed:
                os.chmod(parent, 0o700)
        except OSError as exc:
            raise BackupError("backup parent directory is unavailable") from exc
        try:
            info = parent.lstat()
        except OSError as exc:
            raise BackupError("backup parent directory is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise BackupError("backup parent must be a real directory")
        return parent

    @staticmethod
    def _publish(staging: Path, destination: Path) -> None:
        if destination.exists() or destination.is_symlink():
            raise BackupError("backup destination already exists")
        try:
            os.rename(staging, destination)
        except OSError as exc:
            raise BackupError("backup could not be published atomically") from exc
        _fsync_directory(destination.parent)

    def _snapshot_database(self, destination: Path) -> None:
        try:
            with (
                closing(self.database.connect()) as source,
                closing(sqlite3.connect(destination)) as target,
            ):
                self.database._check_schema(source)
                source.backup(target)
                target.execute("PRAGMA journal_mode = DELETE")
                target.commit()
        except (sqlite3.Error, OSError) as exc:
            raise BackupError("database snapshot could not be created") from exc
        try:
            os.chmod(destination, 0o600)
            with destination.open("rb") as snapshot:
                os.fsync(snapshot.fileno())
        except OSError as exc:
            raise BackupError("database snapshot could not be synchronized") from exc

    def create(self, destination: Path) -> dict[str, Any]:
        destination = Path(destination).expanduser().absolute()
        state_root = self.database.path.expanduser().resolve().parent
        compared_destination = destination.resolve(strict=False)
        if compared_destination == state_root or compared_destination.is_relative_to(
            state_root
        ):
            raise BackupError("backup destination must be outside the live state root")
        if destination.exists() or destination.is_symlink():
            raise BackupError("backup destination already exists")
        parent = self._prepare_parent(destination)
        staging = Path(tempfile.mkdtemp(prefix=".mgb-backup-", dir=parent))
        os.chmod(staging, 0o700)
        published = False
        try:
            database_path = staging / BACKUP_DATABASE
            self._snapshot_database(database_path)
            with closing(_open_backup_database(database_path)) as connection:
                schema_version = _verify_database(connection)
                result_summary = _copy_results_from_database(
                    connection,
                    state_root / BACKUP_RESULTS,
                    staging / BACKUP_RESULTS,
                )
            database_bytes, database_sha256 = _hash_file(
                database_path, label="backup database"
            )
            build_revision = os.environ.get("MAGIC_GLASS_BOX_BUILD_REVISION")
            manifest = StateBackupManifest(
                created_at=datetime.now(UTC)
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z"),
                application_version=__version__,
                build_revision=build_revision,
                database_bytes=database_bytes,
                database_sha256=database_sha256,
                database_schema_version=schema_version,
                result_count=result_summary[0],
                result_changed_paths=result_summary[1],
                result_changed_bytes=result_summary[2],
                result_inventory_sha256=result_summary[3],
            )
            _write_file(staging / BACKUP_MANIFEST, manifest.canonical.encode("utf-8"))
            _fsync_directory(staging)
            verified = self.verify(staging)
            self._publish(staging, destination)
            published = True
            return verified
        finally:
            if not published and staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

    @classmethod
    def verify(cls, backup: Path) -> dict[str, Any]:
        backup = Path(backup).expanduser().absolute()
        _private_directory(backup, label="backup root")
        try:
            entries = {path.name for path in backup.iterdir()}
        except OSError as exc:
            raise BackupError("backup root could not be listed") from exc
        if entries != {BACKUP_MANIFEST, BACKUP_DATABASE, BACKUP_RESULTS}:
            raise BackupError("backup root contains missing or unexpected entries")
        manifest = _read_manifest(backup / BACKUP_MANIFEST)
        _verify_payload(backup, manifest)
        return manifest.to_dict()

    @classmethod
    def restore(cls, backup: Path, target_state_directory: Path) -> dict[str, Any]:
        backup = Path(backup).expanduser().absolute()
        target = Path(target_state_directory).expanduser().absolute()
        if target.exists() or target.is_symlink():
            raise BackupError("restore target already exists")
        resolved_backup = backup.resolve(strict=False)
        resolved_target = target.resolve(strict=False)
        if resolved_target == resolved_backup or resolved_target.is_relative_to(
            resolved_backup
        ):
            raise BackupError("restore target must be outside the backup")
        manifest_value = cls.verify(backup)
        manifest = StateBackupManifest.from_mapping(manifest_value)
        parent = cls._prepare_parent(target)
        staging = Path(tempfile.mkdtemp(prefix=".mgb-restore-", dir=parent))
        os.chmod(staging, 0o700)
        published = False
        try:
            _copy_file(
                backup / BACKUP_DATABASE,
                staging / BACKUP_DATABASE,
                label="backup database",
            )
            with closing(_open_backup_database(backup / BACKUP_DATABASE)) as source:
                _verify_database(source)
                _copy_results_from_database(
                    source,
                    backup / BACKUP_RESULTS,
                    staging / BACKUP_RESULTS,
                )
            _verify_payload(staging, manifest)
            _fsync_directory(staging)
            cls._publish(staging, target)
            published = True
            return manifest.to_dict() | {"state_directory": str(target)}
        finally:
            if not published and staging.exists():
                shutil.rmtree(staging, ignore_errors=True)
