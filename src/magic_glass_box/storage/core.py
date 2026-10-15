from __future__ import annotations

import os
import secrets
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .errors import StorageError
from .schema import (
    DATABASE_FILENAME,
    LEGACY_DATABASE_FILENAME,
    LEGACY_STATE_DIRECTORY_NAME,
    MIGRATIONS,
    SCHEMA,
    SCHEMA_VERSION,
    STATE_DIRECTORY_NAME,
)


class _StrictRow:
    """SQLite row with exact-name lookup and sqlite3.Row-compatible access."""

    __slots__ = ("_indexes", "_keys", "_values")

    def __init__(self, cursor: sqlite3.Cursor, values: tuple[Any, ...]):
        self._keys = tuple(column[0] for column in cursor.description or ())
        self._values = values
        self._indexes: dict[str, int] = {}
        for index, key in enumerate(self._keys):
            self._indexes.setdefault(key, index)

    def __getitem__(self, key: int | slice | str) -> Any:
        if isinstance(key, int | slice):
            try:
                return self._values[key]
            except IndexError:
                raise IndexError("tuple index out of range") from None
        if isinstance(key, str):
            try:
                return self._values[self._indexes[key]]
            except KeyError:
                raise IndexError("No item with that key") from None
        raise IndexError("Index must be int or string")

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> list[str]:
        return list(self._keys)


def _strict_row_factory(cursor: sqlite3.Cursor, values: tuple[Any, ...]) -> _StrictRow:
    return _StrictRow(cursor, values)


def default_db_path() -> Path:
    override = os.environ.get("MAGIC_GLASS_BOX_DB")
    if override:
        return Path(override).expanduser()
    override = os.environ.get("HEX_DB")
    if override:
        return Path(override).expanduser()
    state_override = os.environ.get("MAGIC_GLASS_BOX_STATE_DIR")
    if state_override:
        return Path(state_override).expanduser() / DATABASE_FILENAME
    legacy_state_override = os.environ.get("HEX_STATE_DIR")
    if legacy_state_override:
        return Path(legacy_state_override).expanduser() / LEGACY_DATABASE_FILENAME
    xdg = os.environ.get("XDG_STATE_HOME")
    root = Path(xdg).expanduser() if xdg else Path.home() / ".local" / "state"
    current = root / STATE_DIRECTORY_NAME / DATABASE_FILENAME
    legacy = root / LEGACY_STATE_DIRECTORY_NAME / LEGACY_DATABASE_FILENAME
    if current.exists() or not legacy.exists():
        return current
    return legacy


class DatabaseCore:
    def __init__(self, path: Path):
        self.path = Path(path).expanduser().absolute()

    @staticmethod
    def now() -> str:
        return datetime.now(UTC).isoformat(timespec="microseconds")

    @staticmethod
    def new_id(prefix: str) -> str:
        return f"{prefix}_{secrets.token_hex(16)}"

    def initialize(self) -> dict[str, Any]:
        self._prepare_file()
        with self._connection() as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise StorageError(
                    f"database schema {version} is newer than supported "
                    f"{SCHEMA_VERSION}"
                )
            if version == 0:
                existing = connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table' "
                    "AND name NOT LIKE 'sqlite_%' LIMIT 1"
                ).fetchone()
                if existing is not None:
                    raise StorageError("unversioned database is not empty")
                try:
                    connection.executescript("BEGIN EXCLUSIVE;\n" + SCHEMA)
                    connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                    connection.commit()
                except sqlite3.Error as exc:
                    connection.rollback()
                    raise StorageError(f"cannot initialize database: {exc}") from exc
            elif version < SCHEMA_VERSION:
                self._migrate(connection, version)
            self._check_schema(connection)
        os.chmod(self.path, 0o600)
        return self.status()

    @staticmethod
    def _migrate(connection: sqlite3.Connection, version: int) -> None:
        while version < SCHEMA_VERSION:
            migration = MIGRATIONS.get(version)
            if migration is None:
                raise StorageError(f"no migration from database schema {version}")
            target = version + 1
            try:
                connection.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + migration
                    + f"\nPRAGMA user_version = {target};\nCOMMIT;"
                )
            except sqlite3.Error as exc:
                connection.rollback()
                raise StorageError(
                    f"cannot migrate database from schema {version} to {target}: {exc}"
                ) from exc
            version = target

    def _prepare_file(self) -> None:
        parent_exists = self.path.parent.exists()
        try:
            self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        except OSError as exc:
            raise StorageError(f"cannot create state directory: {exc}") from exc
        if not parent_exists:
            os.chmod(self.path.parent, 0o700)
        try:
            info = self.path.lstat()
        except FileNotFoundError:
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                descriptor = os.open(self.path, flags, 0o600)
            except OSError as exc:
                raise StorageError(f"cannot create database: {exc}") from exc
            os.close(descriptor)
            return
        self._validate_file_info(info)

    def _validate_file_info(self, info: os.stat_result) -> None:
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise StorageError("database must be a regular file, not a symlink")
        if info.st_nlink != 1:
            raise StorageError("database must not be hard-linked")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise StorageError("database is not owned by the current user")
        if info.st_mode & 0o077:
            raise StorageError(
                "database must not be accessible by group or other users"
            )

    def connect(self) -> sqlite3.Connection:
        try:
            info = self.path.lstat()
        except FileNotFoundError as exc:
            raise StorageError("database is not initialized") from exc
        self._validate_file_info(info)
        try:
            connection = sqlite3.connect(
                self.path,
                timeout=5.0,
                isolation_level=None,
                check_same_thread=False,
            )
            connection.row_factory = _strict_row_factory
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA trusted_schema = OFF")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            return connection
        except sqlite3.Error as exc:
            raise StorageError(f"cannot open database: {exc}") from exc

    @contextmanager
    def _connection(
        self, connection: sqlite3.Connection | None = None
    ) -> Iterator[sqlite3.Connection]:
        if connection is not None:
            try:
                yield connection
            except sqlite3.OperationalError as exc:
                raise StorageError("database operation failed") from exc
            return
        with closing(self.connect()) as connection:
            try:
                yield connection
            except sqlite3.OperationalError as exc:
                raise StorageError("database operation failed") from exc

    @contextmanager
    def _read_snapshot(self) -> Iterator[sqlite3.Connection]:
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN")
                try:
                    yield connection
                except Exception:
                    connection.rollback()
                    raise
                else:
                    connection.commit()
            except sqlite3.OperationalError as exc:
                raise StorageError("database operation failed") from exc

    @staticmethod
    def _check_schema(connection: sqlite3.Connection) -> None:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version != SCHEMA_VERSION:
            raise StorageError(
                f"database schema {version} is not supported; expected {SCHEMA_VERSION}"
            )
        result = connection.execute("PRAGMA quick_check").fetchone()[0]
        if result != "ok":
            raise StorageError(f"database integrity check failed: {result}")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise StorageError("database foreign key check failed")

    def status(self) -> dict[str, Any]:
        with self._connection() as connection:
            self._check_schema(connection)
            count_queries = (
                ("users", "SELECT COUNT(*) FROM users"),
                ("krab_instances", "SELECT COUNT(*) FROM krab_instances"),
                ("krab_identities", "SELECT COUNT(*) FROM krab_identities"),
                ("projects", "SELECT COUNT(*) FROM projects"),
                ("agent_profiles", "SELECT COUNT(*) FROM agent_profiles"),
                ("provider_grants", "SELECT COUNT(*) FROM provider_grants"),
                ("runs", "SELECT COUNT(*) FROM runs"),
                ("run_attempts", "SELECT COUNT(*) FROM run_attempts"),
                ("attempt_handoffs", "SELECT COUNT(*) FROM attempt_handoffs"),
                ("attempt_results", "SELECT COUNT(*) FROM attempt_results"),
                (
                    "integration_candidates",
                    "SELECT COUNT(*) FROM integration_candidates",
                ),
                (
                    "integration_gate_policies",
                    "SELECT COUNT(*) FROM integration_gate_policies",
                ),
                (
                    "integration_gate_review_runs",
                    "SELECT COUNT(*) FROM integration_gate_review_runs",
                ),
                (
                    "integration_gate_evidence",
                    "SELECT COUNT(*) FROM integration_gate_evidence",
                ),
                (
                    "work_item_landing_dependencies",
                    "SELECT COUNT(*) FROM work_item_landing_dependencies",
                ),
                (
                    "work_item_landing_dependency_events",
                    "SELECT COUNT(*) FROM work_item_landing_dependency_events",
                ),
                ("integration_refs", "SELECT COUNT(*) FROM integration_refs"),
                (
                    "integration_queue_items",
                    "SELECT COUNT(*) FROM integration_queue_items",
                ),
                (
                    "integration_queue_events",
                    "SELECT COUNT(*) FROM integration_queue_events",
                ),
                ("integration_folds", "SELECT COUNT(*) FROM integration_folds"),
                (
                    "integration_landing_plans",
                    "SELECT COUNT(*) FROM integration_landing_plans",
                ),
                (
                    "integration_landing_receipts",
                    "SELECT COUNT(*) FROM integration_landing_receipts",
                ),
                ("interactions", "SELECT COUNT(*) FROM interactions"),
                ("work_items", "SELECT COUNT(*) FROM work_items"),
                ("work_threads", "SELECT COUNT(*) FROM work_threads"),
                (
                    "work_thread_events",
                    "SELECT COUNT(*) FROM work_thread_events",
                ),
                ("work_log_entries", "SELECT COUNT(*) FROM work_log_entries"),
                (
                    "work_memory_checkpoints",
                    "SELECT COUNT(*) FROM work_memory_checkpoints",
                ),
                ("work_assignments", "SELECT COUNT(*) FROM work_assignments"),
                ("supervisor_actions", "SELECT COUNT(*) FROM supervisor_actions"),
                (
                    "supervisor_escalations",
                    "SELECT COUNT(*) FROM supervisor_escalations",
                ),
            )
            counts = {
                name: connection.execute(query).fetchone()[0]
                for name, query in count_queries
            }
        return {
            "database": str(self.path),
            "schema_version": SCHEMA_VERSION,
            "counts": counts,
        }
