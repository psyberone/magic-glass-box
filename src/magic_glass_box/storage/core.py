from __future__ import annotations

import os
import secrets
import sqlite3
import stat
import time
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .errors import ConflictError, NotFoundError, StorageError
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

    @staticmethod
    def _active_user(
        connection: sqlite3.Connection, user_id: str, *, role: str
    ) -> None:
        row = connection.execute(
            "SELECT active FROM users WHERE id = ?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"user not found: {user_id}")
        if row["active"] != 1:
            raise ConflictError(f"{role} is inactive")

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
                self._initialize_schema(connection)
            elif version < SCHEMA_VERSION:
                self._migrate(connection, version)
            self._check_schema(connection)
        os.chmod(self.path, 0o600)
        return self.status()

    @staticmethod
    def _execute_statements(connection: sqlite3.Connection, script: str) -> None:
        pending = ""
        for line in script.splitlines(keepends=True):
            pending += line
            if not sqlite3.complete_statement(pending):
                continue
            statement = pending.strip()
            pending = ""
            if statement:
                connection.execute(statement)
        if pending.strip():
            raise sqlite3.OperationalError("incomplete database schema statement")

    @classmethod
    def _initialize_schema(cls, connection: sqlite3.Connection) -> None:
        foreign_keys_enabled = bool(
            connection.execute("PRAGMA foreign_keys").fetchone()[0]
        )
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("BEGIN EXCLUSIVE")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise StorageError(
                    f"database schema {version} is newer than supported "
                    f"{SCHEMA_VERSION}"
                )
            if version != 0:
                connection.commit()
                if version < SCHEMA_VERSION:
                    cls._migrate(connection, version)
                return
            existing = connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' LIMIT 1"
            ).fetchone()
            if existing is not None:
                raise StorageError("unversioned database is not empty")
            cls._execute_statements(connection, SCHEMA)
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise StorageError(
                    "database foreign key check failed after initialization"
                )
            connection.commit()
        except StorageError:
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise StorageError(f"cannot initialize database: {exc}") from exc
        finally:
            connection.execute(
                "PRAGMA foreign_keys = " + ("ON" if foreign_keys_enabled else "OFF")
            )

    @classmethod
    def _migrate(cls, connection: sqlite3.Connection, version: int) -> None:
        target = version + 1
        foreign_keys_enabled = bool(
            connection.execute("PRAGMA foreign_keys").fetchone()[0]
        )
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("BEGIN EXCLUSIVE")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise StorageError(
                    f"database schema {version} is newer than supported "
                    f"{SCHEMA_VERSION}"
                )
            while version < SCHEMA_VERSION:
                migration = MIGRATIONS.get(version)
                if migration is None:
                    raise StorageError(f"no migration from database schema {version}")
                target = version + 1
                cls._execute_statements(connection, migration)
                connection.execute(f"PRAGMA user_version = {target}")
                version = target
            if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise StorageError("database foreign key check failed after migration")
            connection.commit()
        except StorageError:
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise StorageError(
                f"cannot migrate database from schema {version} to {target}: {exc}"
            ) from exc
        finally:
            connection.execute(
                "PRAGMA foreign_keys = " + ("ON" if foreign_keys_enabled else "OFF")
            )

    def _prepare_file(self) -> None:
        parent_exists = self.path.parent.exists()
        try:
            self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        except OSError as exc:
            raise StorageError(f"cannot create state directory: {exc}") from exc
        if not parent_exists:
            os.chmod(self.path.parent, 0o700)
        try:
            parent_info = self.path.parent.lstat()
        except OSError as exc:
            raise StorageError(f"cannot inspect state directory: {exc}") from exc
        if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
            raise StorageError("state directory must be a real directory")
        if hasattr(os, "getuid") and parent_info.st_uid != os.getuid():
            raise StorageError("state directory must be owned by the current user")
        if parent_info.st_mode & 0o077:
            raise StorageError("state directory must be private")
        try:
            info = self.path.lstat()
        except FileNotFoundError:
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                descriptor = os.open(self.path, flags, 0o600)
            except FileExistsError:
                try:
                    self._validate_file_info(self.path.lstat())
                except OSError as exc:
                    raise StorageError(
                        f"cannot inspect concurrently created database: {exc}"
                    ) from exc
                return
            except OSError as exc:
                raise StorageError(f"cannot create database: {exc}") from exc
            os.close(descriptor)
            return
        self._validate_file_info(info)

    def _validate_file_info(self, info: os.stat_result) -> None:
        if stat.S_ISLNK(info.st_mode):
            raise StorageError("database must not be a symlink")
        if not stat.S_ISREG(info.st_mode):
            raise StorageError("database must be a regular file")
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
        connection: sqlite3.Connection | None = None
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
            deadline = time.monotonic() + 5.0
            for statement in (
                "PRAGMA journal_mode = WAL",
                "PRAGMA synchronous = FULL",
            ):
                while True:
                    try:
                        connection.execute(statement)
                        break
                    except sqlite3.OperationalError as exc:
                        if (
                            "locked" not in str(exc).lower()
                            or time.monotonic() >= deadline
                        ):
                            raise
                        time.sleep(0.01)
            return connection
        except sqlite3.Error as exc:
            if connection is not None:
                connection.close()
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
                (
                    "inference_connectors",
                    "SELECT COUNT(*) FROM inference_connectors",
                ),
                (
                    "service_sidecars",
                    "SELECT COUNT(*) FROM service_sidecars",
                ),
                (
                    "service_operations",
                    "SELECT COUNT(*) FROM service_operations",
                ),
                (
                    "service_operation_attempts",
                    "SELECT COUNT(*) FROM service_operation_attempts",
                ),
                (
                    "project_capability_bindings",
                    "SELECT COUNT(*) FROM project_capability_bindings",
                ),
                (
                    "project_capability_binding_events",
                    "SELECT COUNT(*) FROM project_capability_binding_events",
                ),
                (
                    "service_operation_capability_bindings",
                    "SELECT COUNT(*) FROM service_operation_capability_bindings",
                ),
                ("provider_grants", "SELECT COUNT(*) FROM provider_grants"),
                (
                    "provider_connector_bindings",
                    "SELECT COUNT(*) FROM provider_connector_bindings",
                ),
                ("runs", "SELECT COUNT(*) FROM runs"),
                ("run_attempts", "SELECT COUNT(*) FROM run_attempts"),
                (
                    "provider_recoveries",
                    "SELECT COUNT(*) FROM provider_recoveries",
                ),
                ("attempt_handoffs", "SELECT COUNT(*) FROM attempt_handoffs"),
                ("attempt_results", "SELECT COUNT(*) FROM attempt_results"),
                (
                    "attempt_recovery_checkpoints",
                    "SELECT COUNT(*) FROM attempt_recovery_checkpoints",
                ),
                (
                    "attempt_recovery_applications",
                    "SELECT COUNT(*) FROM attempt_recovery_applications",
                ),
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
                    "integration_gate_change_runs",
                    "SELECT COUNT(*) FROM integration_gate_change_runs",
                ),
                (
                    "integration_maintainer_reviews",
                    "SELECT COUNT(*) FROM integration_maintainer_reviews",
                ),
                (
                    "integration_gate_maintainer_change_runs",
                    "SELECT COUNT(*) FROM integration_gate_maintainer_change_runs",
                ),
                (
                    "integration_gate_evidence",
                    "SELECT COUNT(*) FROM integration_gate_evidence",
                ),
                (
                    "integration_review_verdicts",
                    "SELECT COUNT(*) FROM integration_review_verdicts",
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
                    "integration_landing_plan_nonces",
                    "SELECT COUNT(*) FROM integration_landing_plan_nonces",
                ),
                (
                    "integration_provider_landings",
                    "SELECT COUNT(*) FROM integration_provider_landings",
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
                (
                    "workflow_template_definitions",
                    "SELECT COUNT(*) FROM workflow_template_definitions",
                ),
                (
                    "workflow_configurations",
                    "SELECT COUNT(*) FROM workflow_configurations",
                ),
                (
                    "workflow_configuration_versions",
                    "SELECT COUNT(*) FROM workflow_configuration_versions",
                ),
                (
                    "workflow_executions",
                    "SELECT COUNT(*) FROM workflow_executions",
                ),
                (
                    "workflow_execution_events",
                    "SELECT COUNT(*) FROM workflow_execution_events",
                ),
                (
                    "workflow_run_bindings",
                    "SELECT COUNT(*) FROM workflow_run_bindings",
                ),
                (
                    "workflow_gate_bindings",
                    "SELECT COUNT(*) FROM workflow_gate_bindings",
                ),
                ("workflow_rulings", "SELECT COUNT(*) FROM workflow_rulings"),
                (
                    "workflow_landing_bindings",
                    "SELECT COUNT(*) FROM workflow_landing_bindings",
                ),
                (
                    "workflow_landing_chains",
                    "SELECT COUNT(*) FROM workflow_landing_chains",
                ),
                (
                    "workflow_provider_targets",
                    "SELECT COUNT(*) FROM workflow_provider_targets",
                ),
                (
                    "workflow_completions",
                    "SELECT COUNT(*) FROM workflow_completions",
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
