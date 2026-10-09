from __future__ import annotations

import json
import os
import secrets
import sqlite3
import stat
from collections.abc import Iterator, Mapping
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .domain import (
    AgentProfileSpec,
    ProjectContext,
    RunState,
    ValidationError,
    canonical_json,
    require_string,
    validate_handle,
    validate_krab_id,
    validate_name,
    validate_project_key,
    validate_transition,
)
from .krab import KrabIdentity, validate_base_url

SCHEMA_VERSION = 1
STATE_DIRECTORY_NAME = "magic-glass-box"
DATABASE_FILENAME = "control-plane.db"
LEGACY_STATE_DIRECTORY_NAME = "hexadecapus"
LEGACY_DATABASE_FILENAME = "hex.db"


SCHEMA = """
CREATE TABLE users (
    id TEXT PRIMARY KEY,
    handle TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE krab_instances (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    base_url TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE krab_identities (
    id TEXT PRIMARY KEY,
    krab_instance_id TEXT NOT NULL,
    krab_user_id TEXT NOT NULL,
    handle TEXT NOT NULL,
    token_version INTEGER NOT NULL CHECK (token_version >= 1),
    credential_ref TEXT NOT NULL,
    verified_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (krab_instance_id, krab_user_id),
    UNIQUE (krab_instance_id, handle),
    FOREIGN KEY (krab_instance_id) REFERENCES krab_instances(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE user_krab_bindings (
    user_id TEXT NOT NULL,
    krab_identity_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    PRIMARY KEY (user_id, krab_identity_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (krab_identity_id) REFERENCES krab_identities(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    krab_instance_id TEXT NOT NULL,
    krab_project_id TEXT NOT NULL,
    krab_project_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (krab_instance_id, krab_project_id),
    UNIQUE (krab_instance_id, krab_project_key),
    UNIQUE (krab_project_key),
    FOREIGN KEY (krab_instance_id) REFERENCES krab_instances(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE project_context_versions (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    context_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (project_id, version),
    UNIQUE (project_id, content_sha256),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE agent_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE agent_profile_versions (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    spec_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    krab_identity_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (profile_id, version),
    UNIQUE (profile_id, content_sha256, krab_identity_id),
    FOREIGN KEY (profile_id) REFERENCES agent_profiles(id) ON DELETE RESTRICT,
    FOREIGN KEY (krab_identity_id) REFERENCES krab_identities(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    context_version_id TEXT NOT NULL,
    profile_version_id TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'queued', 'preparing', 'running', 'waiting_for_input',
            'succeeded', 'failed', 'cancelled'
        )
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    source_revision TEXT,
    krab_session_id TEXT,
    krab_claim_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (context_version_id)
        REFERENCES project_context_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (profile_version_id)
        REFERENCES agent_profile_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (requested_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE run_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (run_id, sequence),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX project_context_project_version
    ON project_context_versions(project_id, version DESC);
CREATE INDEX profile_version_profile_version
    ON agent_profile_versions(profile_id, version DESC);
CREATE INDEX run_project_created ON runs(project_id, created_at DESC);
CREATE INDEX run_state_created ON runs(state, created_at);
CREATE INDEX run_event_stream ON run_events(run_id, sequence);

CREATE TRIGGER project_context_immutable_update
BEFORE UPDATE ON project_context_versions
BEGIN
    SELECT RAISE(ABORT, 'project context versions are immutable');
END;
CREATE TRIGGER project_context_immutable_delete
BEFORE DELETE ON project_context_versions
BEGIN
    SELECT RAISE(ABORT, 'project context versions are immutable');
END;
CREATE TRIGGER agent_profile_version_immutable_update
BEFORE UPDATE ON agent_profile_versions
BEGIN
    SELECT RAISE(ABORT, 'agent profile versions are immutable');
END;
CREATE TRIGGER agent_profile_version_immutable_delete
BEFORE DELETE ON agent_profile_versions
BEGIN
    SELECT RAISE(ABORT, 'agent profile versions are immutable');
END;
CREATE TRIGGER run_event_immutable_update
BEFORE UPDATE ON run_events
BEGIN
    SELECT RAISE(ABORT, 'run events are immutable');
END;
CREATE TRIGGER run_event_immutable_delete
BEFORE DELETE ON run_events
BEGIN
    SELECT RAISE(ABORT, 'run events are immutable');
END;
"""


class StorageError(RuntimeError):
    """Control-plane persistence failed."""


class NotFoundError(StorageError):
    pass


class ConflictError(StorageError):
    pass


class ConcurrentUpdateError(ConflictError):
    pass


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


class Database:
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
        with closing(self.connect()) as connection:
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
            self._check_schema(connection)
        os.chmod(self.path, 0o600)
        return self.status()

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
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA trusted_schema = OFF")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            return connection
        except sqlite3.Error as exc:
            raise StorageError(f"cannot open database: {exc}") from exc

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

    def status(self) -> dict[str, Any]:
        with closing(self.connect()) as connection:
            self._check_schema(connection)
            count_queries = (
                ("users", "SELECT COUNT(*) FROM users"),
                ("krab_instances", "SELECT COUNT(*) FROM krab_instances"),
                ("krab_identities", "SELECT COUNT(*) FROM krab_identities"),
                ("projects", "SELECT COUNT(*) FROM projects"),
                ("agent_profiles", "SELECT COUNT(*) FROM agent_profiles"),
                ("runs", "SELECT COUNT(*) FROM runs"),
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

    def create_user(self, handle: str, display_name: str) -> dict[str, Any]:
        handle = validate_handle(handle)
        display_name = require_string(display_name, field="display_name", max_bytes=256)
        user_id = self.new_id("boxu")
        now = self.now()
        try:
            with closing(self.connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO users "
                    "(id, handle, display_name, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (user_id, handle, display_name, now, now),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError(f"local user already exists: {handle}") from exc
        return self.get_user(handle)

    def get_user(self, handle: str) -> dict[str, Any]:
        handle = validate_handle(handle)
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT id, handle, display_name, active, created_at, updated_at "
                "FROM users WHERE handle = ?",
                (handle,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"local user not found: {handle}")
        return dict(row)

    def create_krab_instance(self, name: str, base_url: str) -> dict[str, Any]:
        name = validate_name(name, field="instance name")
        _, _, normalized_url = validate_base_url(base_url)
        instance_id = self.new_id("ksi")
        now = self.now()
        try:
            with closing(self.connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO krab_instances "
                    "(id, name, base_url, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (instance_id, name, normalized_url, now, now),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError("Krab instance name or URL already exists") from exc
        return self.get_krab_instance(name)

    def get_krab_instance(self, name: str) -> dict[str, Any]:
        name = validate_name(name, field="instance name")
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT id, name, base_url, created_at, updated_at "
                "FROM krab_instances WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"Krab instance not found: {name}")
        return dict(row)

    def store_krab_identity(
        self,
        instance_id: str,
        identity: KrabIdentity,
        credential_ref: str,
        *,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        validate_krab_id(identity.user_id, "usr")
        validate_handle(identity.handle)
        credential_ref = require_string(
            credential_ref, field="credential_ref", max_bytes=4096
        )
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT id FROM krab_identities "
                    "WHERE krab_instance_id = ? AND krab_user_id = ?",
                    (instance_id, identity.user_id),
                ).fetchone()
                identity_id = existing["id"] if existing else self.new_id("kid")
                if existing:
                    connection.execute(
                        "UPDATE krab_identities SET handle = ?, token_version = ?, "
                        "credential_ref = ?, verified_at = ?, updated_at = ? "
                        "WHERE id = ?",
                        (
                            identity.handle,
                            identity.token_version,
                            credential_ref,
                            now,
                            now,
                            identity_id,
                        ),
                    )
                else:
                    connection.execute(
                        "INSERT INTO krab_identities "
                        "(id, krab_instance_id, krab_user_id, handle, token_version, "
                        "credential_ref, verified_at, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            identity_id,
                            instance_id,
                            identity.user_id,
                            identity.handle,
                            identity.token_version,
                            credential_ref,
                            now,
                            now,
                            now,
                        ),
                    )
                if user_id is not None:
                    connection.execute(
                        "INSERT OR IGNORE INTO user_krab_bindings "
                        "(user_id, krab_identity_id, created_at) VALUES (?, ?, ?)",
                        (user_id, identity_id, now),
                    )
                    binding = connection.execute(
                        "SELECT user_id FROM user_krab_bindings "
                        "WHERE krab_identity_id = ?",
                        (identity_id,),
                    ).fetchone()
                    if binding is None or binding["user_id"] != user_id:
                        raise ConflictError(
                            "Krab identity is already linked to another local user"
                        )
                connection.commit()
            except ConflictError:
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "Krab identity conflicts with an existing handle or binding"
                ) from exc
        return self.get_krab_identity(instance_id, identity.handle)

    def get_krab_identity(self, instance_id: str, handle: str) -> dict[str, Any]:
        handle = validate_handle(handle)
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT ki.id, ki.krab_instance_id, ki.krab_user_id, ki.handle, "
                "ki.token_version, ki.credential_ref, ki.verified_at, "
                "ki.created_at, ki.updated_at, ub.user_id AS local_user_id "
                "FROM krab_identities ki "
                "LEFT JOIN user_krab_bindings ub ON ub.krab_identity_id = ki.id "
                "WHERE ki.krab_instance_id = ? AND ki.handle = ?",
                (instance_id, handle),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"Krab identity not found: {handle}")
        return dict(row)

    def create_project(
        self,
        *,
        instance_id: str,
        krab_project_id: str,
        krab_project_key: str,
        display_name: str,
        context: ProjectContext,
        created_by: str,
    ) -> dict[str, Any]:
        krab_project_id = validate_krab_id(krab_project_id, "prj")
        krab_project_key = validate_project_key(krab_project_key)
        display_name = require_string(display_name, field="display_name", max_bytes=256)
        project_id = self.new_id("boxp")
        context_id = self.new_id("ctx")
        now = self.now()
        try:
            with closing(self.connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO projects "
                    "(id, krab_instance_id, krab_project_id, krab_project_key, "
                    "display_name, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        project_id,
                        instance_id,
                        krab_project_id,
                        krab_project_key,
                        display_name,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO project_context_versions "
                    "(id, project_id, version, context_json, content_sha256, "
                    "created_by, created_at) VALUES (?, ?, 1, ?, ?, ?, ?)",
                    (
                        context_id,
                        project_id,
                        context.canonical,
                        context.digest,
                        created_by,
                        now,
                    ),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError("project mapping already exists or is invalid") from exc
        return self.get_project(krab_project_key)

    def get_project(self, project_key: str) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT p.id, p.krab_instance_id, p.krab_project_id, "
                "p.krab_project_key, p.display_name, p.active, p.created_at, "
                "p.updated_at, c.id AS context_version_id, "
                "c.version AS context_version, c.context_json, c.content_sha256 "
                "FROM projects p JOIN project_context_versions c "
                "ON c.project_id = p.id "
                "WHERE p.krab_project_key = ? "
                "ORDER BY c.version DESC LIMIT 1",
                (project_key,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"project not found: {project_key}")
        result = dict(row)
        result["context"] = json.loads(result.pop("context_json"))
        return result

    def add_project_context(
        self, project_key: str, context: ProjectContext, created_by: str
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        now = self.now()
        context_id = self.new_id("ctx")
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                project = connection.execute(
                    "SELECT id FROM projects WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None:
                    raise NotFoundError(f"project not found: {project_key}")
                latest = connection.execute(
                    "SELECT COALESCE(MAX(version), 0) AS version "
                    "FROM project_context_versions WHERE project_id = ?",
                    (project["id"],),
                ).fetchone()["version"]
                connection.execute(
                    "INSERT INTO project_context_versions "
                    "(id, project_id, version, context_json, content_sha256, "
                    "created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        context_id,
                        project["id"],
                        latest + 1,
                        context.canonical,
                        context.digest,
                        created_by,
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE projects SET updated_at = ? WHERE id = ?",
                    (now, project["id"]),
                )
                connection.commit()
            except NotFoundError:
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("identical project context already exists") from exc
        return self.get_project(project_key)

    def create_agent_profile(
        self,
        *,
        name: str,
        spec: AgentProfileSpec,
        krab_identity_id: str,
        created_by: str,
    ) -> dict[str, Any]:
        name = validate_name(name, field="profile name")
        profile_id = self.new_id("agp")
        version_id = self.new_id("apv")
        now = self.now()
        try:
            with closing(self.connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO agent_profiles "
                    "(id, name, created_by, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (profile_id, name, created_by, now, now),
                )
                connection.execute(
                    "INSERT INTO agent_profile_versions "
                    "(id, profile_id, version, spec_json, content_sha256, "
                    "krab_identity_id, created_by, created_at) "
                    "VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
                    (
                        version_id,
                        profile_id,
                        spec.canonical,
                        spec.digest,
                        krab_identity_id,
                        created_by,
                        now,
                    ),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError("agent profile already exists or is invalid") from exc
        return self.get_agent_profile(name)

    def get_agent_profile(self, name: str) -> dict[str, Any]:
        name = validate_name(name, field="profile name")
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT p.id, p.name, p.active, p.created_at, p.updated_at, "
                "v.id AS profile_version_id, v.version AS profile_version, "
                "v.spec_json, v.content_sha256, v.krab_identity_id, "
                "ki.handle AS krab_handle, ki.krab_user_id "
                "FROM agent_profiles p JOIN agent_profile_versions v "
                "ON v.profile_id = p.id JOIN krab_identities ki "
                "ON ki.id = v.krab_identity_id "
                "WHERE p.name = ? ORDER BY v.version DESC LIMIT 1",
                (name,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"agent profile not found: {name}")
        result = dict(row)
        result["spec"] = json.loads(result.pop("spec_json"))
        return result

    def add_agent_profile_version(
        self,
        *,
        name: str,
        spec: AgentProfileSpec,
        krab_identity_id: str,
        created_by: str,
    ) -> dict[str, Any]:
        name = validate_name(name, field="profile name")
        now = self.now()
        version_id = self.new_id("apv")
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                profile = connection.execute(
                    "SELECT id FROM agent_profiles WHERE name = ?", (name,)
                ).fetchone()
                if profile is None:
                    raise NotFoundError(f"agent profile not found: {name}")
                latest = connection.execute(
                    "SELECT COALESCE(MAX(version), 0) AS version "
                    "FROM agent_profile_versions WHERE profile_id = ?",
                    (profile["id"],),
                ).fetchone()["version"]
                connection.execute(
                    "INSERT INTO agent_profile_versions "
                    "(id, profile_id, version, spec_json, content_sha256, "
                    "krab_identity_id, created_by, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        version_id,
                        profile["id"],
                        latest + 1,
                        spec.canonical,
                        spec.digest,
                        krab_identity_id,
                        created_by,
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE agent_profiles SET updated_at = ? WHERE id = ?",
                    (now, profile["id"]),
                )
                connection.commit()
            except NotFoundError:
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "identical agent profile version already exists"
                ) from exc
        return self.get_agent_profile(name)

    def create_run(
        self,
        *,
        project_key: str,
        profile_name: str,
        requested_by: str,
        source_revision: str | None = None,
        krab_session_id: str | None = None,
        krab_claim_id: str | None = None,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        profile_name = validate_name(profile_name, field="profile name")
        if source_revision is not None:
            source_revision = require_string(
                source_revision, field="source_revision", max_bytes=512
            )
        if krab_session_id is not None:
            krab_session_id = validate_krab_id(krab_session_id, "ses")
        if krab_claim_id is not None:
            krab_claim_id = validate_krab_id(krab_claim_id, "clm")
        run_id = self.new_id("run")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                project = connection.execute(
                    "SELECT id, active, krab_instance_id FROM projects "
                    "WHERE krab_project_key = ?",
                    (project_key,),
                ).fetchone()
                if project is None or not project["active"]:
                    raise NotFoundError(f"active project not found: {project_key}")
                context = connection.execute(
                    "SELECT id FROM project_context_versions WHERE project_id = ? "
                    "ORDER BY version DESC LIMIT 1",
                    (project["id"],),
                ).fetchone()
                profile = connection.execute(
                    "SELECT id, active FROM agent_profiles WHERE name = ?",
                    (profile_name,),
                ).fetchone()
                if profile is None or not profile["active"]:
                    raise NotFoundError(
                        f"active agent profile not found: {profile_name}"
                    )
                profile_version = connection.execute(
                    "SELECT av.id, ki.krab_instance_id "
                    "FROM agent_profile_versions av "
                    "JOIN krab_identities ki ON ki.id = av.krab_identity_id "
                    "WHERE av.profile_id = ? ORDER BY av.version DESC LIMIT 1",
                    (profile["id"],),
                ).fetchone()
                if profile_version["krab_instance_id"] != project["krab_instance_id"]:
                    raise ConflictError(
                        "agent profile and project belong to different Krab instances"
                    )
                connection.execute(
                    "INSERT INTO runs "
                    "(id, project_id, context_version_id, profile_version_id, "
                    "requested_by, state, version, source_revision, krab_session_id, "
                    "krab_claim_id, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        project["id"],
                        context["id"],
                        profile_version["id"],
                        requested_by,
                        source_revision,
                        krab_session_id,
                        krab_claim_id,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, metadata_json, "
                    "created_at) VALUES (?, 1, 'run.created', NULL, 'queued', '{}', ?)",
                    (run_id, now),
                )
                connection.commit()
            except (NotFoundError, ConflictError):
                connection.rollback()
                raise
            except (sqlite3.IntegrityError, TypeError) as exc:
                connection.rollback()
                raise ConflictError("run could not be created") from exc
        return self.get_run(run_id)

    def transition_run(
        self,
        run_id: str,
        *,
        target: RunState,
        expected_version: int,
        reason: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        source_revision: str | None = None,
    ) -> dict[str, Any]:
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 1
        ):
            raise ValidationError("expected_version must be a positive integer")
        if reason is not None:
            reason = require_string(reason, field="reason", max_bytes=4096)
        if metadata is None:
            event_metadata: dict[str, Any] = {}
        elif not isinstance(metadata, Mapping) or not all(
            isinstance(key, str) for key in metadata
        ):
            raise ValidationError("transition metadata must be an object")
        else:
            event_metadata = dict(metadata)
        if "reason" in event_metadata:
            raise ValidationError("transition metadata uses reserved field: reason")
        if reason is not None:
            event_metadata["reason"] = reason
        metadata_json = canonical_json(event_metadata)
        if len(metadata_json.encode("utf-8")) > 16 * 1024:
            raise ValidationError("transition metadata exceeds 16384 UTF-8 bytes")
        if source_revision is not None:
            if target is not RunState.RUNNING:
                raise ValidationError(
                    "source revision can only be pinned when entering running"
                )
            source_revision = require_string(
                source_revision, field="source_revision", max_bytes=512
            )
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT state, version, source_revision, started_at "
                    "FROM runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if row["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"run version is {row['version']}, expected {expected_version}"
                    )
                source = RunState(row["state"])
                validate_transition(source, target)
                terminal = target in {
                    RunState.SUCCEEDED,
                    RunState.FAILED,
                    RunState.CANCELLED,
                }
                started_at = row["started_at"]
                if target is RunState.RUNNING and started_at is None:
                    started_at = now
                finished_at = now if terminal else None
                pinned_revision = (
                    source_revision
                    if source_revision is not None
                    else row["source_revision"]
                )
                changed = connection.execute(
                    "UPDATE runs SET state = ?, version = version + 1, "
                    "updated_at = ?, started_at = ?, finished_at = ?, "
                    "source_revision = ? "
                    "WHERE id = ? AND version = ?",
                    (
                        target.value,
                        now,
                        started_at,
                        finished_at,
                        pinned_revision,
                        run_id,
                        expected_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, metadata_json, "
                    "created_at) VALUES (?, ?, 'run.transitioned', ?, ?, ?, ?)",
                    (
                        run_id,
                        expected_version + 1,
                        source.value,
                        target.value,
                        metadata_json,
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConcurrentUpdateError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("run transition could not be recorded") from exc
        return self.get_run(run_id)

    def get_run_execution(self, run_id: str) -> dict[str, Any]:
        """Load the immutable context, profile, and identity pinned by a run."""

        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT r.id, r.state, r.version, r.source_revision, "
                "p.krab_project_key AS project_key, c.context_json, "
                "av.spec_json, ki.credential_ref "
                "FROM runs r JOIN projects p ON p.id = r.project_id "
                "JOIN project_context_versions c ON c.id = r.context_version_id "
                "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
                "JOIN krab_identities ki ON ki.id = av.krab_identity_id "
                "WHERE r.id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"run not found: {run_id}")
        result = dict(row)
        try:
            result["context"] = json.loads(result.pop("context_json"))
            result["profile"] = json.loads(result.pop("spec_json"))
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("pinned run data is invalid") from exc
        result["credential_references"] = {"krab": result.pop("credential_ref")}
        return result

    def get_run(self, run_id: str) -> dict[str, Any]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT r.id, r.state, r.version, r.source_revision, "
                "r.krab_session_id, r.krab_claim_id, r.created_at, r.updated_at, "
                "r.started_at, r.finished_at, p.krab_project_key AS project_key, "
                "c.id AS context_version_id, c.version AS context_version, "
                "ap.name AS profile_name, av.id AS profile_version_id, "
                "av.version AS profile_version, u.handle AS requested_by "
                "FROM runs r JOIN projects p ON p.id = r.project_id "
                "JOIN project_context_versions c ON c.id = r.context_version_id "
                "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
                "JOIN agent_profiles ap ON ap.id = av.profile_id "
                "JOIN users u ON u.id = r.requested_by WHERE r.id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"run not found: {run_id}")
            events = connection.execute(
                "SELECT sequence, kind, from_state, to_state, metadata_json, "
                "created_at FROM run_events WHERE run_id = ? ORDER BY sequence",
                (run_id,),
            ).fetchall()
        result = dict(row)
        result["events"] = [
            {**dict(event), "metadata": json.loads(event["metadata_json"])}
            for event in events
        ]
        for event in result["events"]:
            event.pop("metadata_json")
        return result

    def iter_active_runs(self) -> Iterator[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT id FROM runs WHERE state IN "
                "('queued', 'preparing', 'running', 'waiting_for_input') "
                "ORDER BY created_at"
            ).fetchall()
        for row in rows:
            yield self.get_run(row["id"])
