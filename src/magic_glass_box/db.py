from __future__ import annotations

import json
import os
import secrets
import sqlite3
import stat
from collections.abc import Iterator, Mapping
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .domain import (
    AgentProfileSpec,
    AttemptState,
    GatewayCapability,
    HandoffEnvelope,
    HandoffWorkState,
    InteractionKind,
    InteractionState,
    ProjectContext,
    ProviderGrantSpec,
    RunState,
    ValidationError,
    WorkAssignmentSpec,
    WorkItemEnvelope,
    WorkLogEntry,
    WorkLogKind,
    WorkMemoryCheckpoint,
    canonical_json,
    require_string,
    validate_handle,
    validate_krab_id,
    validate_name,
    validate_project_key,
    validate_provider_name,
    validate_transition,
)
from .krab import IDEMPOTENCY_KEY_RE, KrabIdentity, validate_base_url

SCHEMA_VERSION = 6
STATE_DIRECTORY_NAME = "magic-glass-box"
DATABASE_FILENAME = "control-plane.db"
LEGACY_STATE_DIRECTORY_NAME = "hexadecapus"
LEGACY_DATABASE_FILENAME = "hex.db"
MAX_WORK_ASSIGNMENTS_PER_THREAD = 16
MAX_VISIBLE_ASSIGNMENT_EVENTS = 16


SCHEMA_V1 = """
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

INTERACTION_SCHEMA = """
CREATE TABLE interactions (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('question', 'approval', 'choice')),
    state TEXT NOT NULL CHECK (state IN ('open', 'answered', 'cancelled')),
    version INTEGER NOT NULL CHECK (version >= 1),
    recipient_krab_identity_id TEXT NOT NULL,
    request_item_id TEXT NOT NULL UNIQUE,
    response_item_id TEXT UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (recipient_krab_identity_id)
        REFERENCES krab_identities(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'open' AND response_item_id IS NULL AND resolved_at IS NULL)
        OR (
            state = 'answered'
            AND response_item_id IS NOT NULL
            AND resolved_at IS NOT NULL
        )
        OR (
            state = 'cancelled'
            AND response_item_id IS NULL
            AND resolved_at IS NOT NULL
        )
    )
) STRICT;

CREATE TABLE interaction_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    interaction_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'interaction.opened', 'interaction.answered',
            'interaction.cancelled'
        )
    ),
    from_state TEXT CHECK (
        from_state IS NULL OR from_state IN ('open', 'answered', 'cancelled')
    ),
    to_state TEXT NOT NULL CHECK (to_state IN ('open', 'answered', 'cancelled')),
    krab_item_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (interaction_id, sequence),
    FOREIGN KEY (interaction_id) REFERENCES interactions(id) ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX interaction_open_run
    ON interactions(run_id) WHERE state = 'open';
CREATE INDEX interaction_run_created ON interactions(run_id, created_at);
CREATE INDEX interaction_state_created ON interactions(state, created_at);
CREATE INDEX interaction_event_stream
    ON interaction_events(interaction_id, sequence);

CREATE TRIGGER interaction_event_immutable_update
BEFORE UPDATE ON interaction_events
BEGIN
    SELECT RAISE(ABORT, 'interaction events are immutable');
END;
CREATE TRIGGER interaction_event_immutable_delete
BEFORE DELETE ON interaction_events
BEGIN
    SELECT RAISE(ABORT, 'interaction events are immutable');
END;
"""

WORK_ITEM_SCHEMA = """
CREATE TABLE work_items (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    source_updated_at TEXT NOT NULL,
    last_imported_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (project_id, source, external_id),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (last_imported_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE work_item_versions (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    content_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    source_updated_at TEXT NOT NULL,
    imported_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (work_item_id, version),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (imported_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

ALTER TABLE runs ADD COLUMN work_item_version_id TEXT
    REFERENCES work_item_versions(id) ON DELETE RESTRICT;

CREATE INDEX work_item_project_updated
    ON work_items(project_id, updated_at DESC);
CREATE INDEX work_item_version_item_version
    ON work_item_versions(work_item_id, version DESC);
CREATE INDEX run_work_item_version ON runs(work_item_version_id);

CREATE TRIGGER work_item_identity_immutable
BEFORE UPDATE OF project_id, source, external_id ON work_items
WHEN OLD.project_id != NEW.project_id
    OR OLD.source != NEW.source
    OR OLD.external_id != NEW.external_id
BEGIN
    SELECT RAISE(ABORT, 'work item identity is immutable');
END;
CREATE TRIGGER work_item_version_immutable_update
BEFORE UPDATE ON work_item_versions
BEGIN
    SELECT RAISE(ABORT, 'work item versions are immutable');
END;
CREATE TRIGGER work_item_version_immutable_delete
BEFORE DELETE ON work_item_versions
BEGIN
    SELECT RAISE(ABORT, 'work item versions are immutable');
END;
"""

PROVIDER_SCHEMA = """
CREATE TABLE provider_grants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('active', 'revoked')),
    version INTEGER NOT NULL CHECK (version >= 1),
    policy_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    credential_ref TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT,
    revoked_by TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (revoked_by) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'active' AND revoked_at IS NULL AND revoked_by IS NULL)
        OR (state = 'revoked' AND revoked_at IS NOT NULL AND revoked_by IS NOT NULL)
    )
) STRICT;

CREATE TABLE provider_grant_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_grant_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN ('provider_grant.created', 'provider_grant.revoked')
    ),
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (provider_grant_id, sequence),
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE run_provider_grants (
    run_id TEXT NOT NULL,
    authority_name TEXT NOT NULL,
    provider_grant_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, authority_name),
    UNIQUE (run_id, provider_grant_id),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE provider_invocations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    provider_grant_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (
        outcome IN ('succeeded', 'failed', 'denied', 'busy')
    ),
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 0),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 0),
    http_status INTEGER CHECK (http_status BETWEEN 100 AND 599),
    error_code TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX provider_grant_state ON provider_grants(state, expires_at);
CREATE INDEX provider_grant_event_stream
    ON provider_grant_events(provider_grant_id, sequence);
CREATE INDEX run_provider_grant_grant
    ON run_provider_grants(provider_grant_id, run_id);
CREATE INDEX provider_invocation_run
    ON provider_invocations(run_id, started_at, id);

CREATE TRIGGER provider_grant_policy_immutable
BEFORE UPDATE OF
    name, policy_json, content_sha256, credential_ref, expires_at,
    created_by, created_at
ON provider_grants
BEGIN
    SELECT RAISE(ABORT, 'provider grant policy is immutable');
END;
CREATE TRIGGER provider_grant_immutable_delete
BEFORE DELETE ON provider_grants
BEGIN
    SELECT RAISE(ABORT, 'provider grants cannot be deleted');
END;
CREATE TRIGGER provider_grant_event_immutable_update
BEFORE UPDATE ON provider_grant_events
BEGIN
    SELECT RAISE(ABORT, 'provider grant events are immutable');
END;
CREATE TRIGGER provider_grant_event_immutable_delete
BEFORE DELETE ON provider_grant_events
BEGIN
    SELECT RAISE(ABORT, 'provider grant events are immutable');
END;
CREATE TRIGGER run_provider_grant_immutable_update
BEFORE UPDATE ON run_provider_grants
BEGIN
    SELECT RAISE(ABORT, 'run provider grants are immutable');
END;
CREATE TRIGGER run_provider_grant_immutable_delete
BEFORE DELETE ON run_provider_grants
BEGIN
    SELECT RAISE(ABORT, 'run provider grants are immutable');
END;
CREATE TRIGGER provider_invocation_immutable_update
BEFORE UPDATE ON provider_invocations
BEGIN
    SELECT RAISE(ABORT, 'provider invocations are immutable');
END;
CREATE TRIGGER provider_invocation_immutable_delete
BEFORE DELETE ON provider_invocations
BEGIN
    SELECT RAISE(ABORT, 'provider invocations are immutable');
END;
"""

ATTEMPT_SCHEMA = """
CREATE TABLE run_attempts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    profile_version_id TEXT NOT NULL,
    runtime_adapter TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'preparing', 'running', 'handed_off', 'succeeded', 'failed',
            'cancelled', 'interrupted'
        )
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    source_revision TEXT,
    container_id TEXT,
    container_name TEXT,
    image_id TEXT,
    exit_code INTEGER,
    reason TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    UNIQUE (run_id, sequence),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (profile_version_id)
        REFERENCES agent_profile_versions(id) ON DELETE RESTRICT,
    CHECK (
        (state IN ('preparing', 'running') AND finished_at IS NULL)
        OR (state NOT IN ('preparing', 'running') AND finished_at IS NOT NULL)
    )
) STRICT;

ALTER TABLE interactions ADD COLUMN attempt_id TEXT
    REFERENCES run_attempts(id) ON DELETE RESTRICT;

CREATE TABLE run_attempt_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'attempt.created', 'attempt.started', 'attempt.handoff_submitted',
            'attempt.completed', 'attempt.failed', 'attempt.interrupted'
        )
    ),
    from_state TEXT CHECK (
        from_state IS NULL OR from_state IN (
            'preparing', 'running', 'handed_off', 'succeeded', 'failed',
            'cancelled', 'interrupted'
        )
    ),
    to_state TEXT NOT NULL CHECK (
        to_state IN (
            'preparing', 'running', 'handed_off', 'succeeded', 'failed',
            'cancelled', 'interrupted'
        )
    ),
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (attempt_id, sequence),
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE attempt_handoffs (
    id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL UNIQUE,
    content_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX run_attempt_stream ON run_attempts(run_id, sequence);
CREATE INDEX interaction_attempt ON interactions(attempt_id);
CREATE UNIQUE INDEX run_attempt_active ON run_attempts(run_id)
    WHERE state IN ('preparing', 'running');
CREATE INDEX run_attempt_event_stream
    ON run_attempt_events(attempt_id, sequence);

CREATE TRIGGER run_attempt_identity_immutable
BEFORE UPDATE OF
    id, run_id, sequence, profile_version_id, runtime_adapter, created_at
ON run_attempts
BEGIN
    SELECT RAISE(ABORT, 'run attempt identity is immutable');
END;
CREATE TRIGGER run_attempt_event_immutable_update
BEFORE UPDATE ON run_attempt_events
BEGIN
    SELECT RAISE(ABORT, 'run attempt events are immutable');
END;
CREATE TRIGGER run_attempt_event_immutable_delete
BEFORE DELETE ON run_attempt_events
BEGIN
    SELECT RAISE(ABORT, 'run attempt events are immutable');
END;
CREATE TRIGGER attempt_handoff_immutable_update
BEFORE UPDATE ON attempt_handoffs
BEGIN
    SELECT RAISE(ABORT, 'attempt handoffs are immutable');
END;
CREATE TRIGGER attempt_handoff_immutable_delete
BEFORE DELETE ON attempt_handoffs
BEGIN
    SELECT RAISE(ABORT, 'attempt handoffs are immutable');
END;
"""

WORK_MEMORY_SCHEMA = """
CREATE TABLE work_threads (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    state TEXT NOT NULL CHECK (state IN ('active', 'completed')),
    version INTEGER NOT NULL CHECK (version >= 1),
    log_sequence INTEGER NOT NULL CHECK (log_sequence >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (work_item_id, generation),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'active' AND completed_at IS NULL)
        OR (state = 'completed' AND completed_at IS NOT NULL)
    )
) STRICT;

INSERT INTO work_threads (
    id, work_item_id, generation, state, version, log_sequence,
    created_at, updated_at
)
SELECT
    'wth_' || lower(hex(randomblob(16))), id, 1, 'active', 1, 0,
    created_at, updated_at
FROM work_items;

ALTER TABLE runs ADD COLUMN work_thread_id TEXT
    REFERENCES work_threads(id) ON DELETE RESTRICT;

UPDATE runs
SET work_thread_id = (
    SELECT wt.id
    FROM work_item_versions wiv
    JOIN work_threads wt ON wt.work_item_id = wiv.work_item_id
    WHERE wiv.id = runs.work_item_version_id AND wt.state = 'active'
);

CREATE TABLE work_assignments (
    id TEXT PRIMARY KEY,
    work_thread_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    target_profile_version_id TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open', 'claimed', 'completed', 'cancelled')),
    version INTEGER NOT NULL CHECK (version >= 1),
    created_by_run_id TEXT NOT NULL,
    created_by_attempt_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    claimed_by_run_id TEXT,
    claimed_by_attempt_id TEXT,
    lease_expires_at TEXT,
    result_entry_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (work_thread_id, sequence),
    UNIQUE (work_thread_id, created_by_run_id, idempotency_key),
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (target_profile_version_id)
        REFERENCES agent_profile_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by_attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (claimed_by_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (claimed_by_attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (result_entry_id) REFERENCES work_log_entries(id) ON DELETE RESTRICT,
    CHECK (
        (
            state = 'open' AND claimed_by_run_id IS NULL
            AND claimed_by_attempt_id IS NULL AND lease_expires_at IS NULL
            AND result_entry_id IS NULL AND completed_at IS NULL
        ) OR (
            state = 'claimed' AND claimed_by_run_id IS NOT NULL
            AND claimed_by_attempt_id IS NOT NULL AND lease_expires_at IS NOT NULL
            AND result_entry_id IS NULL AND completed_at IS NULL
        ) OR (
            state = 'completed' AND claimed_by_run_id IS NOT NULL
            AND claimed_by_attempt_id IS NOT NULL AND lease_expires_at IS NOT NULL
            AND result_entry_id IS NOT NULL AND completed_at IS NOT NULL
        ) OR (
            state = 'cancelled' AND result_entry_id IS NULL
            AND completed_at IS NOT NULL
        )
    )
) STRICT;

CREATE TABLE work_assignment_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    assignment_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'assignment.created', 'assignment.claimed',
            'assignment.reclaimed', 'assignment.completed',
            'assignment.cancelled'
        )
    ),
    from_state TEXT CHECK (
        from_state IS NULL OR from_state IN (
            'open', 'claimed', 'completed', 'cancelled'
        )
    ),
    to_state TEXT NOT NULL CHECK (
        to_state IN ('open', 'claimed', 'completed', 'cancelled')
    ),
    actor_run_id TEXT NOT NULL,
    actor_attempt_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (assignment_id, sequence),
    FOREIGN KEY (assignment_id) REFERENCES work_assignments(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE work_log_entries (
    id TEXT PRIMARY KEY,
    work_thread_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'plan', 'progress', 'finding', 'decision', 'blocker',
            'verification', 'review', 'assignment_result', 'outcome'
        )
    ),
    content_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    author_run_id TEXT NOT NULL,
    author_attempt_id TEXT NOT NULL,
    assignment_id TEXT,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (work_thread_id, sequence),
    UNIQUE (work_thread_id, author_run_id, idempotency_key),
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (author_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (author_attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (assignment_id) REFERENCES work_assignments(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE work_memory_checkpoints (
    id TEXT PRIMARY KEY,
    work_thread_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    through_sequence INTEGER NOT NULL CHECK (through_sequence >= 0),
    content_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    producer_run_id TEXT NOT NULL,
    producer_attempt_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (work_thread_id, version),
    UNIQUE (work_thread_id, producer_run_id, idempotency_key),
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (producer_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (producer_attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX work_thread_active
    ON work_threads(work_item_id) WHERE state = 'active';
CREATE INDEX run_work_thread ON runs(work_thread_id);
CREATE INDEX work_assignment_thread_state
    ON work_assignments(work_thread_id, state, sequence);
CREATE INDEX work_assignment_claimed_run
    ON work_assignments(claimed_by_run_id, state);
CREATE INDEX work_assignment_event_stream
    ON work_assignment_events(assignment_id, sequence);
CREATE INDEX work_log_thread_sequence
    ON work_log_entries(work_thread_id, sequence);
CREATE INDEX work_checkpoint_thread_version
    ON work_memory_checkpoints(work_thread_id, version DESC);

CREATE TRIGGER work_thread_identity_immutable
BEFORE UPDATE OF id, work_item_id, generation, created_at ON work_threads
BEGIN
    SELECT RAISE(ABORT, 'work thread identity is immutable');
END;
CREATE TRIGGER work_assignment_identity_immutable
BEFORE UPDATE OF
    id, work_thread_id, sequence, target_profile_version_id, spec_json,
    content_sha256, created_by_run_id, created_by_attempt_id,
    idempotency_key, created_at
ON work_assignments
BEGIN
    SELECT RAISE(ABORT, 'work assignment identity is immutable');
END;
CREATE TRIGGER work_assignment_event_immutable_update
BEFORE UPDATE ON work_assignment_events
BEGIN
    SELECT RAISE(ABORT, 'work assignment events are immutable');
END;
CREATE TRIGGER work_assignment_event_immutable_delete
BEFORE DELETE ON work_assignment_events
BEGIN
    SELECT RAISE(ABORT, 'work assignment events are immutable');
END;
CREATE TRIGGER work_log_entry_immutable_update
BEFORE UPDATE ON work_log_entries
BEGIN
    SELECT RAISE(ABORT, 'work-log entries are immutable');
END;
CREATE TRIGGER work_log_entry_immutable_delete
BEFORE DELETE ON work_log_entries
BEGIN
    SELECT RAISE(ABORT, 'work-log entries are immutable');
END;
CREATE TRIGGER work_checkpoint_immutable_update
BEFORE UPDATE ON work_memory_checkpoints
BEGIN
    SELECT RAISE(ABORT, 'work-memory checkpoints are immutable');
END;
CREATE TRIGGER work_checkpoint_immutable_delete
BEFORE DELETE ON work_memory_checkpoints
BEGIN
    SELECT RAISE(ABORT, 'work-memory checkpoints are immutable');
END;
"""

SCHEMA = (
    SCHEMA_V1
    + INTERACTION_SCHEMA
    + WORK_ITEM_SCHEMA
    + PROVIDER_SCHEMA
    + ATTEMPT_SCHEMA
    + WORK_MEMORY_SCHEMA
)
MIGRATIONS = {
    1: INTERACTION_SCHEMA,
    2: WORK_ITEM_SCHEMA,
    3: PROVIDER_SCHEMA,
    4: ATTEMPT_SCHEMA,
    5: WORK_MEMORY_SCHEMA,
}


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
                ("provider_grants", "SELECT COUNT(*) FROM provider_grants"),
                ("runs", "SELECT COUNT(*) FROM runs"),
                ("run_attempts", "SELECT COUNT(*) FROM run_attempts"),
                ("attempt_handoffs", "SELECT COUNT(*) FROM attempt_handoffs"),
                ("interactions", "SELECT COUNT(*) FROM interactions"),
                ("work_items", "SELECT COUNT(*) FROM work_items"),
                ("work_threads", "SELECT COUNT(*) FROM work_threads"),
                ("work_log_entries", "SELECT COUNT(*) FROM work_log_entries"),
                (
                    "work_memory_checkpoints",
                    "SELECT COUNT(*) FROM work_memory_checkpoints",
                ),
                ("work_assignments", "SELECT COUNT(*) FROM work_assignments"),
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

    @staticmethod
    def _timestamp(value: object, *, field: str) -> str:
        text = require_string(value, field=field, max_bytes=64)
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValidationError(
                f"{field} must be a valid RFC 3339 timestamp"
            ) from exc
        if parsed.tzinfo is None:
            raise ValidationError(f"{field} must include a timezone")
        return parsed.astimezone(UTC).isoformat(timespec="microseconds")

    @staticmethod
    def _provider_grant_record(
        connection: sqlite3.Connection,
        *,
        name: str | None = None,
        grant_id: str | None = None,
        include_reference: bool = False,
    ) -> dict[str, Any]:
        if (name is None) == (grant_id is None):
            raise TypeError("select a provider grant by exactly one identifier")
        row = connection.execute(
            "SELECT pg.id, pg.name, pg.state, pg.version, pg.policy_json, "
            "pg.content_sha256, pg.credential_ref, pg.expires_at, "
            "pg.revoked_at, creator.handle AS created_by, "
            "revoker.handle AS revoked_by, pg.created_at, pg.updated_at "
            "FROM provider_grants pg "
            "JOIN users creator ON creator.id = pg.created_by "
            "LEFT JOIN users revoker ON revoker.id = pg.revoked_by "
            "WHERE (? IS NOT NULL AND pg.name = ?) "
            "OR (? IS NOT NULL AND pg.id = ?)",
            (name, name, grant_id, grant_id),
        ).fetchone()
        if row is None:
            identifier = name if name is not None else grant_id
            raise NotFoundError(f"provider grant not found: {identifier}")
        result = dict(row)
        try:
            spec = ProviderGrantSpec.from_mapping(json.loads(result.pop("policy_json")))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored provider grant policy is invalid") from exc
        if spec.digest != result["content_sha256"]:
            raise StorageError("stored provider grant policy digest does not match")
        result["policy"] = spec.to_dict()
        if not include_reference:
            result.pop("credential_ref")
        events = connection.execute(
            "SELECT e.sequence, e.kind, u.handle AS actor, e.created_at "
            "FROM provider_grant_events e "
            "JOIN users u ON u.id = e.actor_id "
            "WHERE e.provider_grant_id = ? ORDER BY e.sequence",
            (result["id"],),
        ).fetchall()
        result["events"] = [dict(event) for event in events]
        return result

    def create_provider_grant(
        self,
        *,
        name: str,
        spec: ProviderGrantSpec,
        credential_ref: str,
        expires_at: str | None,
        created_by: str,
    ) -> dict[str, Any]:
        name = validate_provider_name(name, field="provider grant name")
        if not isinstance(spec, ProviderGrantSpec):
            raise ValidationError("spec must be a ProviderGrantSpec")
        credential_ref = require_string(
            credential_ref, field="credential_ref", max_bytes=4096
        )
        if expires_at is not None:
            expires_at = self._timestamp(expires_at, field="expires_at")
        grant_id = self.new_id("pgr")
        now = self.now()
        try:
            with closing(self.connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO provider_grants "
                    "(id, name, state, version, policy_json, content_sha256, "
                    "credential_ref, expires_at, created_by, created_at, "
                    "updated_at) VALUES (?, ?, 'active', 1, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        grant_id,
                        name,
                        spec.canonical,
                        spec.digest,
                        credential_ref,
                        expires_at,
                        created_by,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO provider_grant_events "
                    "(provider_grant_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, 1, 'provider_grant.created', ?, ?)",
                    (grant_id, created_by, now),
                )
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise ConflictError("provider grant already exists or is invalid") from exc
        return self.get_provider_grant(name)

    def get_provider_grant(self, name: str) -> dict[str, Any]:
        name = validate_provider_name(name, field="provider grant name")
        with closing(self.connect()) as connection:
            return self._provider_grant_record(connection, name=name)

    def revoke_provider_grant(
        self,
        name: str,
        *,
        expected_version: int,
        revoked_by: str,
    ) -> dict[str, Any]:
        name = validate_provider_name(name, field="provider grant name")
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 1
        ):
            raise ValidationError("expected_version must be a positive integer")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                current = connection.execute(
                    "SELECT id, state, version FROM provider_grants WHERE name = ?",
                    (name,),
                ).fetchone()
                if current is None:
                    raise NotFoundError(f"provider grant not found: {name}")
                if current["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"provider grant version is {current['version']}, "
                        f"expected {expected_version}"
                    )
                if current["state"] != "active":
                    raise ConflictError("provider grant is already revoked")
                cursor = connection.execute(
                    "UPDATE provider_grants SET state = 'revoked', "
                    "version = version + 1, revoked_at = ?, revoked_by = ?, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (now, revoked_by, now, current["id"], expected_version),
                )
                if cursor.rowcount != 1:
                    raise ConcurrentUpdateError("provider grant changed concurrently")
                connection.execute(
                    "INSERT INTO provider_grant_events "
                    "(provider_grant_id, sequence, kind, actor_id, created_at) "
                    "VALUES (?, ?, 'provider_grant.revoked', ?, ?)",
                    (current["id"], expected_version + 1, revoked_by, now),
                )
                connection.commit()
            except (NotFoundError, ConflictError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("provider grant could not be revoked") from exc
        return self.get_provider_grant(name)

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
                "ki.created_at, ki.updated_at, ub.user_id AS local_user_id, "
                "u.active AS local_user_active "
                "FROM krab_identities ki "
                "LEFT JOIN user_krab_bindings ub ON ub.krab_identity_id = ki.id "
                "LEFT JOIN users u ON u.id = ub.user_id "
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

    @staticmethod
    def _work_item_record(
        connection: sqlite3.Connection, work_item_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT wi.id, p.krab_project_key AS project_key, wi.source, "
            "wi.external_id, wi.source_updated_at, "
            "wiv.id AS work_item_version_id, wiv.version, wiv.content_json, "
            "wiv.content_sha256, "
            "wiv.source_updated_at AS version_source_updated_at, "
            "u.handle AS imported_by, wi.created_at, wi.updated_at "
            "FROM work_items wi JOIN projects p ON p.id = wi.project_id "
            "JOIN work_item_versions wiv ON wiv.work_item_id = wi.id "
            "JOIN users u ON u.id = wi.last_imported_by "
            "WHERE wi.id = ? ORDER BY wiv.version DESC LIMIT 1",
            (work_item_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work item not found: {work_item_id}")
        result = dict(row)
        try:
            content = json.loads(result.pop("content_json"))
            envelope = WorkItemEnvelope.from_mapping(
                {
                    **content,
                    "source": result["source"],
                    "external_id": result["external_id"],
                    "source_updated_at": result.pop("version_source_updated_at"),
                }
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored work item is invalid") from exc
        if envelope.digest != result["content_sha256"]:
            raise StorageError("stored work item content digest does not match")
        result["content"] = envelope.content
        return result

    def get_work_item(self, work_item_id: str) -> dict[str, Any]:
        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
        with closing(self.connect()) as connection:
            return self._work_item_record(connection, work_item_id)

    def import_work_item(
        self,
        *,
        project_key: str,
        envelope: WorkItemEnvelope,
        imported_by: str,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        if not isinstance(envelope, WorkItemEnvelope):
            raise ValidationError("envelope must be a WorkItemEnvelope")
        imported_by = require_string(imported_by, field="imported_by", max_bytes=64)
        work_item_id = self.new_id("wki")
        version_id = self.new_id("wiv")
        thread_id = self.new_id("wth")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                project = connection.execute(
                    "SELECT id FROM projects WHERE krab_project_key = ? AND active = 1",
                    (project_key,),
                ).fetchone()
                if project is None:
                    raise NotFoundError(f"active project not found: {project_key}")
                existing = connection.execute(
                    "SELECT id, source_updated_at FROM work_items "
                    "WHERE project_id = ? AND source = ? AND external_id = ?",
                    (project["id"], envelope.source, envelope.external_id),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO work_items "
                        "(id, project_id, source, external_id, source_updated_at, "
                        "last_imported_by, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            work_item_id,
                            project["id"],
                            envelope.source,
                            envelope.external_id,
                            envelope.source_updated_at,
                            imported_by,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO work_item_versions "
                        "(id, work_item_id, version, content_json, content_sha256, "
                        "source_updated_at, imported_by, created_at) "
                        "VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
                        (
                            version_id,
                            work_item_id,
                            envelope.canonical,
                            envelope.digest,
                            envelope.source_updated_at,
                            imported_by,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO work_threads "
                        "(id, work_item_id, generation, state, version, "
                        "log_sequence, created_at, updated_at) "
                        "VALUES (?, ?, 1, 'active', 1, 0, ?, ?)",
                        (thread_id, work_item_id, now, now),
                    )
                    import_status = "created"
                else:
                    work_item_id = existing["id"]
                    current = connection.execute(
                        "SELECT version, content_sha256 FROM work_item_versions "
                        "WHERE work_item_id = ? ORDER BY version DESC LIMIT 1",
                        (work_item_id,),
                    ).fetchone()
                    if current is None:
                        raise StorageError("work item has no content version")
                    if envelope.source_updated_at < existing["source_updated_at"]:
                        raise ConflictError("work item import is stale")
                    if (
                        envelope.source_updated_at == existing["source_updated_at"]
                        and envelope.digest != current["content_sha256"]
                    ):
                        raise ConflictError(
                            "work item import changed at the same source revision"
                        )
                    if envelope.digest == current["content_sha256"]:
                        import_status = "unchanged"
                    else:
                        connection.execute(
                            "INSERT INTO work_item_versions "
                            "(id, work_item_id, version, content_json, "
                            "content_sha256, source_updated_at, imported_by, "
                            "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                version_id,
                                work_item_id,
                                current["version"] + 1,
                                envelope.canonical,
                                envelope.digest,
                                envelope.source_updated_at,
                                imported_by,
                                now,
                            ),
                        )
                        import_status = "updated"
                    connection.execute(
                        "UPDATE work_items SET source_updated_at = ?, "
                        "last_imported_by = ?, updated_at = ? WHERE id = ?",
                        (
                            envelope.source_updated_at,
                            imported_by,
                            now,
                            work_item_id,
                        ),
                    )
                result = self._work_item_record(connection, work_item_id)
                connection.commit()
            except (ConflictError, NotFoundError, StorageError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work item could not be imported") from exc
        result["import_status"] = import_status
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

    def _available_provider_grants(
        self,
        connection: sqlite3.Connection,
        spec: AgentProfileSpec,
    ) -> list[sqlite3.Row]:
        if GatewayCapability.PROVIDER_INVOKE not in spec.gateway_capabilities:
            return []
        result: list[sqlite3.Row] = []
        now = self.now()
        for name in spec.credential_names:
            if name == "krab":
                continue
            row = connection.execute(
                "SELECT id, name, state, expires_at FROM provider_grants "
                "WHERE name = ?",
                (name,),
            ).fetchone()
            if (
                row is None
                or row["state"] != "active"
                or (row["expires_at"] is not None and row["expires_at"] <= now)
            ):
                raise ConflictError(f"provider grant is unavailable: {name}")
            result.append(row)
        return result

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
                self._available_provider_grants(connection, spec)
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
                self._available_provider_grants(connection, spec)
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
        work_item_id: str,
        requested_by: str,
        source_revision: str | None = None,
        krab_session_id: str | None = None,
        krab_claim_id: str | None = None,
    ) -> dict[str, Any]:
        project_key = validate_project_key(project_key)
        profile_name = validate_name(profile_name, field="profile name")
        work_item_id = require_string(work_item_id, field="work_item_id", max_bytes=64)
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
                work_item_version = connection.execute(
                    "SELECT wiv.id, wt.id AS work_thread_id FROM work_items wi "
                    "JOIN work_item_versions wiv ON wiv.work_item_id = wi.id "
                    "JOIN work_threads wt ON wt.work_item_id = wi.id "
                    "WHERE wi.id = ? AND wi.project_id = ? "
                    "AND wt.state = 'active' "
                    "ORDER BY wiv.version DESC LIMIT 1",
                    (work_item_id, project["id"]),
                ).fetchone()
                if work_item_version is None:
                    raise NotFoundError(
                        "work item not found for project: " + work_item_id
                    )
                work_item_version_id = work_item_version["id"]
                work_thread_id = work_item_version["work_thread_id"]
                profile = connection.execute(
                    "SELECT id, active FROM agent_profiles WHERE name = ?",
                    (profile_name,),
                ).fetchone()
                if profile is None or not profile["active"]:
                    raise NotFoundError(
                        f"active agent profile not found: {profile_name}"
                    )
                profile_version = connection.execute(
                    "SELECT av.id, av.spec_json, ki.krab_instance_id "
                    "FROM agent_profile_versions av "
                    "JOIN krab_identities ki ON ki.id = av.krab_identity_id "
                    "WHERE av.profile_id = ? ORDER BY av.version DESC LIMIT 1",
                    (profile["id"],),
                ).fetchone()
                if profile_version["krab_instance_id"] != project["krab_instance_id"]:
                    raise ConflictError(
                        "agent profile and project belong to different Krab instances"
                    )
                try:
                    profile_spec = AgentProfileSpec.from_mapping(
                        json.loads(profile_version["spec_json"])
                    )
                except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                    raise StorageError("stored agent profile is invalid") from exc
                provider_grants = self._available_provider_grants(
                    connection, profile_spec
                )
                connection.execute(
                    "INSERT INTO runs "
                    "(id, project_id, context_version_id, profile_version_id, "
                    "work_item_version_id, work_thread_id, requested_by, "
                    "state, version, "
                    "source_revision, krab_session_id, krab_claim_id, created_at, "
                    "updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        project["id"],
                        context["id"],
                        profile_version["id"],
                        work_item_version_id,
                        work_thread_id,
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
                for grant in provider_grants:
                    connection.execute(
                        "INSERT INTO run_provider_grants "
                        "(run_id, authority_name, provider_grant_id, created_at) "
                        "VALUES (?, ?, ?, ?)",
                        (run_id, grant["name"], grant["id"], now),
                    )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError):
                connection.rollback()
                raise
            except (sqlite3.IntegrityError, TypeError) as exc:
                connection.rollback()
                raise ConflictError("run could not be created") from exc
        return self.get_run(run_id)

    def begin_run_attempt(
        self,
        run_id: str,
        *,
        runtime_adapter: str,
        expected_run_version: int,
    ) -> dict[str, Any]:
        """Atomically claim a queued durable run for one disposable worker."""

        runtime_adapter = validate_provider_name(
            runtime_adapter, field="runtime_adapter"
        )
        if (
            isinstance(expected_run_version, bool)
            or not isinstance(expected_run_version, int)
            or expected_run_version < 1
        ):
            raise ValidationError("expected_run_version must be a positive integer")
        attempt_id = self.new_id("att")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                run = connection.execute(
                    "SELECT state, version, profile_version_id FROM runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
                if run is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if run["version"] != expected_run_version:
                    raise ConcurrentUpdateError(
                        f"run version is {run['version']}, "
                        f"expected {expected_run_version}"
                    )
                if run["state"] != RunState.QUEUED.value:
                    raise ConflictError(f"run is not queued: {run['state']}")
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 FROM run_attempts "
                    "WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO run_attempts "
                    "(id, run_id, sequence, profile_version_id, runtime_adapter, "
                    "state, version, created_at) "
                    "VALUES (?, ?, ?, ?, ?, 'preparing', 1, ?)",
                    (
                        attempt_id,
                        run_id,
                        sequence,
                        run["profile_version_id"],
                        runtime_adapter,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO run_attempt_events "
                    "(attempt_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) "
                    "VALUES (?, 1, 'attempt.created', NULL, 'preparing', '{}', ?)",
                    (attempt_id, now),
                )
                changed = connection.execute(
                    "UPDATE runs SET state = 'preparing', version = version + 1, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (now, run_id, expected_run_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                metadata = canonical_json(
                    {
                        "attempt_id": attempt_id,
                        "attempt_sequence": sequence,
                        "runtime_adapter": runtime_adapter,
                    }
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, metadata_json, "
                    "created_at) VALUES (?, ?, 'run.transitioned', 'queued', "
                    "'preparing', ?, ?)",
                    (run_id, expected_run_version + 1, metadata, now),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("run attempt could not be created") from exc
        return self.get_run_attempt(attempt_id)

    def start_run_attempt(
        self,
        attempt_id: str,
        *,
        expected_attempt_version: int,
        expected_run_version: int,
        source_revision: str,
        container_id: str,
        container_name: str,
        image_id: str,
    ) -> dict[str, Any]:
        """Record a worker start and the resolved source revision atomically."""

        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        for value, field, maximum in (
            (source_revision, "source_revision", 512),
            (container_id, "container_id", 512),
            (container_name, "container_name", 512),
            (image_id, "image_id", 512),
        ):
            require_string(value, field=field, max_bytes=maximum)
        for value, field in (
            (expected_attempt_version, "expected_attempt_version"),
            (expected_run_version, "expected_run_version"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValidationError(f"{field} must be a positive integer")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt = connection.execute(
                    "SELECT ra.run_id, ra.state, ra.version, r.state AS run_state, "
                    "r.version AS run_version, r.started_at "
                    "FROM run_attempts ra JOIN runs r ON r.id = ra.run_id "
                    "WHERE ra.id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise NotFoundError(f"run attempt not found: {attempt_id}")
                if attempt["version"] != expected_attempt_version:
                    raise ConcurrentUpdateError(
                        f"attempt version is {attempt['version']}, "
                        f"expected {expected_attempt_version}"
                    )
                if attempt["state"] != AttemptState.PREPARING.value:
                    raise ConflictError(
                        f"run attempt is not preparing: {attempt['state']}"
                    )
                if attempt["run_version"] != expected_run_version:
                    raise ConcurrentUpdateError(
                        f"run version is {attempt['run_version']}, "
                        f"expected {expected_run_version}"
                    )
                if attempt["run_state"] != RunState.PREPARING.value:
                    raise ConflictError(f"run is not preparing: {attempt['run_state']}")
                attempt_changed = connection.execute(
                    "UPDATE run_attempts SET state = 'running', version = version + 1, "
                    "source_revision = ?, container_id = ?, container_name = ?, "
                    "image_id = ?, started_at = ? WHERE id = ? AND version = ?",
                    (
                        source_revision,
                        container_id,
                        container_name,
                        image_id,
                        now,
                        attempt_id,
                        expected_attempt_version,
                    ),
                ).rowcount
                if attempt_changed != 1:
                    raise ConcurrentUpdateError("run attempt changed concurrently")
                started_at = attempt["started_at"] or now
                run_changed = connection.execute(
                    "UPDATE runs SET state = 'running', version = version + 1, "
                    "source_revision = ?, updated_at = ?, started_at = ? "
                    "WHERE id = ? AND version = ?",
                    (
                        source_revision,
                        now,
                        started_at,
                        attempt["run_id"],
                        expected_run_version,
                    ),
                ).rowcount
                if run_changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                metadata = canonical_json(
                    {
                        "container_id": container_id,
                        "container_name": container_name,
                        "image_id": image_id,
                        "source_revision": source_revision,
                    }
                )
                connection.execute(
                    "INSERT INTO run_attempt_events "
                    "(attempt_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, 2, 'attempt.started', "
                    "'preparing', 'running', ?, ?)",
                    (attempt_id, metadata, now),
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, metadata_json, "
                    "created_at) VALUES (?, ?, 'run.transitioned', 'preparing', "
                    "'running', ?, ?)",
                    (
                        attempt["run_id"],
                        expected_run_version + 1,
                        metadata,
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("run attempt start could not be recorded") from exc
        return self.get_run_attempt(attempt_id)

    def complete_run_attempt(
        self,
        attempt_id: str,
        *,
        expected_attempt_version: int,
        exit_code: int,
        image_id: str,
    ) -> dict[str, Any]:
        """Close one worker attempt and, when appropriate, its durable run."""

        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        image_id = require_string(image_id, field="image_id", max_bytes=512)
        if (
            isinstance(expected_attempt_version, bool)
            or not isinstance(expected_attempt_version, int)
            or expected_attempt_version < 1
        ):
            raise ValidationError("expected_attempt_version must be a positive integer")
        if (
            isinstance(exit_code, bool)
            or not isinstance(exit_code, int)
            or not -(1 << 31) <= exit_code < (1 << 31)
        ):
            raise ValidationError("exit_code must be a signed 32-bit integer")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt = connection.execute(
                    "SELECT ra.run_id, ra.state, ra.version, ra.image_id, "
                    "r.state AS run_state, r.version AS run_version "
                    "FROM run_attempts ra JOIN runs r ON r.id = ra.run_id "
                    "WHERE ra.id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise NotFoundError(f"run attempt not found: {attempt_id}")
                if attempt["version"] != expected_attempt_version:
                    raise ConcurrentUpdateError(
                        f"attempt version is {attempt['version']}, "
                        f"expected {expected_attempt_version}"
                    )
                if attempt["state"] != AttemptState.RUNNING.value:
                    raise ConflictError(
                        f"run attempt is not running: {attempt['state']}"
                    )
                if attempt["image_id"] != image_id:
                    raise ConflictError("run attempt image identity changed")

                run_state = RunState(attempt["run_state"])
                handoff = connection.execute(
                    "SELECT id FROM attempt_handoffs WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                reason: str | None = None
                if run_state is RunState.RUNNING:
                    outcome = (
                        AttemptState.SUCCEEDED
                        if exit_code == 0
                        else AttemptState.FAILED
                    )
                    target = RunState.SUCCEEDED if exit_code == 0 else RunState.FAILED
                    if exit_code != 0:
                        reason = f"agent exited with code {exit_code}"
                    run_metadata: dict[str, Any] = {
                        "exit_code": exit_code,
                        "image_id": image_id,
                    }
                    if reason is not None:
                        run_metadata["reason"] = reason
                    run_changed = connection.execute(
                        "UPDATE runs SET state = ?, version = version + 1, "
                        "updated_at = ?, finished_at = ? "
                        "WHERE id = ? AND version = ?",
                        (
                            target.value,
                            now,
                            now,
                            attempt["run_id"],
                            attempt["run_version"],
                        ),
                    ).rowcount
                    if run_changed != 1:
                        raise ConcurrentUpdateError("run changed concurrently")
                    connection.execute(
                        "INSERT INTO run_events "
                        "(run_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) VALUES (?, ?, "
                        "'run.transitioned', 'running', ?, ?, ?)",
                        (
                            attempt["run_id"],
                            attempt["run_version"] + 1,
                            target.value,
                            canonical_json(run_metadata),
                            now,
                        ),
                    )
                elif run_state in {
                    RunState.WAITING_FOR_INPUT,
                    RunState.QUEUED,
                }:
                    if exit_code != 0:
                        outcome = AttemptState.FAILED
                        reason = f"agent exited with code {exit_code}"
                    elif handoff is None:
                        outcome = AttemptState.INTERRUPTED
                        reason = "worker exited while paused without a handoff"
                    else:
                        outcome = AttemptState.HANDED_OFF
                elif run_state is RunState.CANCELLED:
                    outcome = AttemptState.CANCELLED
                    reason = "run was cancelled"
                else:
                    raise ConflictError(
                        f"run finished before its attempt: {run_state.value}"
                    )

                changed = connection.execute(
                    "UPDATE run_attempts SET state = ?, version = version + 1, "
                    "exit_code = ?, reason = ?, finished_at = ? "
                    "WHERE id = ? AND version = ?",
                    (
                        outcome.value,
                        exit_code,
                        reason,
                        now,
                        attempt_id,
                        expected_attempt_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run attempt changed concurrently")
                event_metadata: dict[str, Any] = {
                    "exit_code": exit_code,
                    "image_id": image_id,
                }
                if handoff is not None:
                    event_metadata["handoff_id"] = handoff["id"]
                if reason is not None:
                    event_metadata["reason"] = reason
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM run_attempt_events WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO run_attempt_events "
                    "(attempt_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, ?, "
                    "'attempt.completed', 'running', ?, ?, ?)",
                    (
                        attempt_id,
                        event_sequence,
                        outcome.value,
                        canonical_json(event_metadata),
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "run attempt outcome could not be recorded"
                ) from exc
        return self.get_run_attempt(attempt_id)

    def fail_run_attempt(
        self,
        attempt_id: str,
        *,
        expected_attempt_version: int,
        reason: str,
        interrupted: bool = False,
    ) -> dict[str, Any]:
        """Close an active attempt after cleanup, preserving durable waits."""

        attempt_id = require_string(attempt_id, field="attempt_id", max_bytes=64)
        reason = require_string(reason, field="reason", max_bytes=4096)
        if (
            isinstance(expected_attempt_version, bool)
            or not isinstance(expected_attempt_version, int)
            or expected_attempt_version < 1
        ):
            raise ValidationError("expected_attempt_version must be a positive integer")
        if not isinstance(interrupted, bool):
            raise ValidationError("interrupted must be boolean")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt = connection.execute(
                    "SELECT ra.run_id, ra.state, ra.version, r.state AS run_state, "
                    "r.version AS run_version FROM run_attempts ra "
                    "JOIN runs r ON r.id = ra.run_id WHERE ra.id = ?",
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise NotFoundError(f"run attempt not found: {attempt_id}")
                if attempt["version"] != expected_attempt_version:
                    raise ConcurrentUpdateError(
                        f"attempt version is {attempt['version']}, "
                        f"expected {expected_attempt_version}"
                    )
                if attempt["state"] not in {
                    AttemptState.PREPARING.value,
                    AttemptState.RUNNING.value,
                }:
                    raise ConflictError(
                        f"run attempt is not active: {attempt['state']}"
                    )
                source_state = attempt["state"]
                run_state = RunState(attempt["run_state"])
                if run_state is RunState.CANCELLED:
                    outcome = AttemptState.CANCELLED
                else:
                    outcome = (
                        AttemptState.INTERRUPTED if interrupted else AttemptState.FAILED
                    )
                if run_state in {RunState.PREPARING, RunState.RUNNING}:
                    run_metadata: dict[str, Any] = {"reason": reason}
                    if interrupted:
                        run_metadata["reconciled"] = True
                    changed = connection.execute(
                        "UPDATE runs SET state = 'failed', version = version + 1, "
                        "updated_at = ?, finished_at = ? "
                        "WHERE id = ? AND version = ?",
                        (
                            now,
                            now,
                            attempt["run_id"],
                            attempt["run_version"],
                        ),
                    ).rowcount
                    if changed != 1:
                        raise ConcurrentUpdateError("run changed concurrently")
                    connection.execute(
                        "INSERT INTO run_events "
                        "(run_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) VALUES (?, ?, "
                        "'run.transitioned', ?, 'failed', ?, ?)",
                        (
                            attempt["run_id"],
                            attempt["run_version"] + 1,
                            run_state.value,
                            canonical_json(run_metadata),
                            now,
                        ),
                    )
                elif run_state not in {
                    RunState.QUEUED,
                    RunState.WAITING_FOR_INPUT,
                    RunState.CANCELLED,
                }:
                    raise ConflictError(
                        f"run finished before its attempt: {run_state.value}"
                    )
                changed = connection.execute(
                    "UPDATE run_attempts SET state = ?, version = version + 1, "
                    "reason = ?, finished_at = ? WHERE id = ? AND version = ?",
                    (
                        outcome.value,
                        reason,
                        now,
                        attempt_id,
                        expected_attempt_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run attempt changed concurrently")
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM run_attempt_events WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()[0]
                kind = "attempt.interrupted" if interrupted else "attempt.failed"
                connection.execute(
                    "INSERT INTO run_attempt_events "
                    "(attempt_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        attempt_id,
                        event_sequence,
                        kind,
                        source_state,
                        outcome.value,
                        canonical_json({"reason": reason}),
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "run attempt failure could not be recorded"
                ) from exc
        return self.get_run_attempt(attempt_id)

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
                open_interactions = connection.execute(
                    "SELECT id, version FROM interactions "
                    "WHERE run_id = ? AND state = 'open'",
                    (run_id,),
                ).fetchall()
                if open_interactions and target in {
                    RunState.QUEUED,
                    RunState.RUNNING,
                }:
                    raise ConflictError(
                        "run cannot resume while it has an open interaction"
                    )
                if open_interactions and target in {
                    RunState.FAILED,
                    RunState.CANCELLED,
                }:
                    for interaction in open_interactions:
                        interaction_version = interaction["version"]
                        connection.execute(
                            "UPDATE interactions SET state = 'cancelled', "
                            "version = version + 1, updated_at = ?, resolved_at = ? "
                            "WHERE id = ? AND version = ?",
                            (
                                now,
                                now,
                                interaction["id"],
                                interaction_version,
                            ),
                        )
                        connection.execute(
                            "INSERT INTO interaction_events "
                            "(interaction_id, sequence, kind, from_state, to_state, "
                            "krab_item_id, created_at) "
                            "VALUES (?, ?, 'interaction.cancelled', 'open', "
                            "'cancelled', NULL, ?)",
                            (
                                interaction["id"],
                                interaction_version + 1,
                                now,
                            ),
                        )
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
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("run transition could not be recorded") from exc
        return self.get_run(run_id)

    def open_interaction(
        self,
        run_id: str,
        *,
        kind: InteractionKind,
        recipient_krab_identity_id: str,
        request_item_id: str,
        expected_run_version: int,
    ) -> dict[str, Any]:
        if not isinstance(kind, InteractionKind):
            raise ValidationError("interaction kind is invalid")
        recipient_krab_identity_id = require_string(
            recipient_krab_identity_id,
            field="recipient_krab_identity_id",
            max_bytes=64,
        )
        request_item_id = validate_krab_id(request_item_id, "itm")
        if (
            isinstance(expected_run_version, bool)
            or not isinstance(expected_run_version, int)
            or expected_run_version < 1
        ):
            raise ValidationError("expected_run_version must be a positive integer")

        interaction_id = self.new_id("int")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                run = connection.execute(
                    "SELECT r.state, r.version, r.krab_session_id, "
                    "p.krab_instance_id FROM runs r "
                    "JOIN projects p ON p.id = r.project_id WHERE r.id = ?",
                    (run_id,),
                ).fetchone()
                if run is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if run["version"] != expected_run_version:
                    raise ConcurrentUpdateError(
                        f"run version is {run['version']}, "
                        f"expected {expected_run_version}"
                    )
                if run["state"] != RunState.RUNNING.value:
                    raise ConflictError(f"run is not running: {run['state']}")
                if run["krab_session_id"] is None:
                    raise ConflictError("run has no Krab session")
                active_attempt = connection.execute(
                    "SELECT id FROM run_attempts WHERE run_id = ? "
                    "AND state = 'running'",
                    (run_id,),
                ).fetchone()

                recipient = connection.execute(
                    "SELECT ki.krab_instance_id, ki.krab_user_id, "
                    "ub.user_id, u.handle AS local_user_handle, u.active "
                    "FROM krab_identities ki "
                    "LEFT JOIN user_krab_bindings ub "
                    "ON ub.krab_identity_id = ki.id "
                    "LEFT JOIN users u ON u.id = ub.user_id WHERE ki.id = ?",
                    (recipient_krab_identity_id,),
                ).fetchone()
                if recipient is None:
                    raise NotFoundError("recipient Krab identity not found")
                if recipient["krab_instance_id"] != run["krab_instance_id"]:
                    raise ConflictError(
                        "recipient belongs to a different Krab instance"
                    )
                if recipient["user_id"] is None or not recipient["active"]:
                    raise NotFoundError(
                        "recipient Krab identity has no active bound local user"
                    )

                connection.execute(
                    "INSERT INTO interactions "
                    "(id, run_id, kind, state, version, "
                    "recipient_krab_identity_id, request_item_id, response_item_id, "
                    "created_at, updated_at, resolved_at, attempt_id) "
                    "VALUES (?, ?, ?, 'open', 1, ?, ?, NULL, ?, ?, NULL, ?)",
                    (
                        interaction_id,
                        run_id,
                        kind.value,
                        recipient_krab_identity_id,
                        request_item_id,
                        now,
                        now,
                        None if active_attempt is None else active_attempt["id"],
                    ),
                )
                changed = connection.execute(
                    "UPDATE runs SET state = 'waiting_for_input', "
                    "version = version + 1, updated_at = ? "
                    "WHERE id = ? AND version = ?",
                    (now, run_id, expected_run_version),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                connection.execute(
                    "INSERT INTO interaction_events "
                    "(interaction_id, sequence, kind, from_state, to_state, "
                    "krab_item_id, created_at) "
                    "VALUES (?, 1, 'interaction.opened', NULL, 'open', ?, ?)",
                    (interaction_id, request_item_id, now),
                )
                metadata = canonical_json(
                    {
                        "interaction_id": interaction_id,
                        "interaction_kind": kind.value,
                        "recipient_krab_user_id": recipient["krab_user_id"],
                        "request_item_id": request_item_id,
                    }
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) "
                    "VALUES (?, ?, 'run.transitioned', 'running', "
                    "'waiting_for_input', ?, ?)",
                    (run_id, expected_run_version + 1, metadata, now),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("interaction could not be opened") from exc
        return self.get_interaction(interaction_id)

    def answer_interaction(
        self,
        interaction_id: str,
        *,
        response_item_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        response_item_id = validate_krab_id(response_item_id, "itm")
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 1
        ):
            raise ValidationError("expected_version must be a positive integer")

        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                interaction = connection.execute(
                    "SELECT i.state, i.version, i.request_item_id, i.run_id, "
                    "r.state AS run_state, r.version AS run_version "
                    "FROM interactions i JOIN runs r ON r.id = i.run_id "
                    "WHERE i.id = ?",
                    (interaction_id,),
                ).fetchone()
                if interaction is None:
                    raise NotFoundError(f"interaction not found: {interaction_id}")
                if interaction["version"] != expected_version:
                    raise ConcurrentUpdateError(
                        f"interaction version is {interaction['version']}, "
                        f"expected {expected_version}"
                    )
                if interaction["state"] != InteractionState.OPEN.value:
                    raise ConflictError(
                        f"interaction is not open: {interaction['state']}"
                    )
                if response_item_id == interaction["request_item_id"]:
                    raise ValidationError(
                        "response item must differ from the request item"
                    )
                if interaction["run_state"] != RunState.WAITING_FOR_INPUT.value:
                    raise ConflictError(
                        "interaction run is not waiting for input: "
                        + interaction["run_state"]
                    )

                changed = connection.execute(
                    "UPDATE interactions SET state = 'answered', "
                    "version = version + 1, response_item_id = ?, updated_at = ?, "
                    "resolved_at = ? WHERE id = ? AND version = ?",
                    (
                        response_item_id,
                        now,
                        now,
                        interaction_id,
                        expected_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("interaction changed concurrently")
                run_changed = connection.execute(
                    "UPDATE runs SET state = 'queued', version = version + 1, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (
                        now,
                        interaction["run_id"],
                        interaction["run_version"],
                    ),
                ).rowcount
                if run_changed != 1:
                    raise ConcurrentUpdateError("run changed concurrently")
                connection.execute(
                    "INSERT INTO interaction_events "
                    "(interaction_id, sequence, kind, from_state, to_state, "
                    "krab_item_id, created_at) "
                    "VALUES (?, ?, 'interaction.answered', 'open', 'answered', ?, ?)",
                    (
                        interaction_id,
                        expected_version + 1,
                        response_item_id,
                        now,
                    ),
                )
                metadata = canonical_json(
                    {
                        "interaction_id": interaction_id,
                        "response_item_id": response_item_id,
                    }
                )
                connection.execute(
                    "INSERT INTO run_events "
                    "(run_id, sequence, kind, from_state, to_state, "
                    "metadata_json, created_at) "
                    "VALUES (?, ?, 'run.transitioned', 'waiting_for_input', "
                    "'queued', ?, ?)",
                    (
                        interaction["run_id"],
                        interaction["run_version"] + 1,
                        metadata,
                        now,
                    ),
                )
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("interaction could not be answered") from exc
        return self.get_interaction(interaction_id)

    def get_interaction(self, interaction_id: str) -> dict[str, Any]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT i.id, i.run_id, i.kind, i.state, i.version, "
                "i.attempt_id, i.request_item_id, i.response_item_id, i.created_at, "
                "i.updated_at, i.resolved_at, r.state AS run_state, "
                "r.version AS run_version, r.krab_session_id, "
                "p.krab_project_key AS project_key, "
                "ki.id AS recipient_krab_identity_id, "
                "ki.krab_user_id AS recipient_krab_user_id, "
                "ki.handle AS recipient_krab_handle, "
                "u.id AS recipient_local_user_id, "
                "u.handle AS recipient_local_user_handle "
                "FROM interactions i JOIN runs r ON r.id = i.run_id "
                "JOIN projects p ON p.id = r.project_id "
                "JOIN krab_identities ki "
                "ON ki.id = i.recipient_krab_identity_id "
                "JOIN user_krab_bindings ub "
                "ON ub.krab_identity_id = ki.id "
                "JOIN users u ON u.id = ub.user_id WHERE i.id = ?",
                (interaction_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"interaction not found: {interaction_id}")
            events = connection.execute(
                "SELECT sequence, kind, from_state, to_state, krab_item_id, "
                "created_at FROM interaction_events "
                "WHERE interaction_id = ? ORDER BY sequence",
                (interaction_id,),
            ).fetchall()
        result = dict(row)
        result["recipient"] = {
            "local_user_id": result.pop("recipient_local_user_id"),
            "local_user_handle": result.pop("recipient_local_user_handle"),
            "krab_identity_id": result.pop("recipient_krab_identity_id"),
            "krab_user_id": result.pop("recipient_krab_user_id"),
            "krab_handle": result.pop("recipient_krab_handle"),
        }
        result["events"] = [dict(event) for event in events]
        return result

    def list_run_interactions(self, run_id: str) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            exists = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                raise NotFoundError(f"run not found: {run_id}")
            rows = connection.execute(
                "SELECT id FROM interactions WHERE run_id = ? ORDER BY created_at, id",
                (run_id,),
            ).fetchall()
        return [self.get_interaction(row["id"]) for row in rows]

    def get_run_provider_grant(
        self, run_id: str, authority_name: str
    ) -> dict[str, Any]:
        authority_name = validate_provider_name(
            authority_name, field="provider authority name"
        )
        with closing(self.connect()) as connection:
            mapping = connection.execute(
                "SELECT provider_grant_id FROM run_provider_grants "
                "WHERE run_id = ? AND authority_name = ?",
                (run_id, authority_name),
            ).fetchone()
            if mapping is None:
                raise NotFoundError(
                    f"provider grant is not authorized for run: {authority_name}"
                )
            result = self._provider_grant_record(
                connection,
                grant_id=mapping["provider_grant_id"],
                include_reference=True,
            )
        if result["state"] != "active" or (
            result["expires_at"] is not None and result["expires_at"] <= self.now()
        ):
            raise ConflictError(f"provider grant is unavailable: {authority_name}")
        result["authority_name"] = authority_name
        return result

    def list_run_provider_grants(self, run_id: str) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            exists = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                raise NotFoundError(f"run not found: {run_id}")
            rows = connection.execute(
                "SELECT rpg.authority_name, pg.id AS grant_id, pg.name, "
                "pg.state, pg.expires_at, pg.policy_json, pg.content_sha256 "
                "FROM run_provider_grants rpg JOIN provider_grants pg "
                "ON pg.id = rpg.provider_grant_id WHERE rpg.run_id = ? "
                "ORDER BY rpg.authority_name",
                (run_id,),
            ).fetchall()
        now = self.now()
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                spec = ProviderGrantSpec.from_mapping(json.loads(row["policy_json"]))
            except (json.JSONDecodeError, TypeError, ValidationError) as exc:
                raise StorageError("stored provider grant policy is invalid") from exc
            if spec.digest != row["content_sha256"]:
                raise StorageError("stored provider grant policy digest does not match")
            state = row["state"]
            if (
                state == "active"
                and row["expires_at"] is not None
                and row["expires_at"] <= now
            ):
                state = "expired"
            result.append(
                {
                    "authority_name": row["authority_name"],
                    "grant_id": row["grant_id"],
                    "name": row["name"],
                    "provider": spec.provider,
                    "model": spec.model,
                    "state": state,
                    "expires_at": row["expires_at"],
                }
            )
        return result

    def record_provider_invocation(
        self,
        *,
        run_id: str,
        provider_grant_id: str,
        outcome: str,
        request_bytes: int,
        response_bytes: int,
        http_status: int | None,
        error_code: str | None,
        started_at: str,
    ) -> dict[str, Any]:
        if outcome not in {"succeeded", "failed", "denied", "busy"}:
            raise ValidationError("provider invocation outcome is invalid")
        for field, value in (
            ("request_bytes", request_bytes),
            ("response_bytes", response_bytes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValidationError(f"{field} must be a non-negative integer")
        if http_status is not None and (
            isinstance(http_status, bool)
            or not isinstance(http_status, int)
            or not 100 <= http_status <= 599
        ):
            raise ValidationError("http_status must be between 100 and 599")
        if error_code is not None:
            error_code = require_string(error_code, field="error_code", max_bytes=64)
        if outcome == "succeeded":
            if http_status is None or not 200 <= http_status < 300 or error_code:
                raise ValidationError("successful provider invocation is inconsistent")
        elif error_code is None:
            raise ValidationError("failed provider invocation requires an error_code")
        started_at = self._timestamp(started_at, field="started_at")
        invocation_id = self.new_id("pinv")
        finished_at = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                authority = connection.execute(
                    "SELECT 1 FROM run_provider_grants "
                    "WHERE run_id = ? AND provider_grant_id = ?",
                    (run_id, provider_grant_id),
                ).fetchone()
                if authority is None:
                    raise ConflictError(
                        "provider grant is not pinned to the requested run"
                    )
                connection.execute(
                    "INSERT INTO provider_invocations "
                    "(id, run_id, provider_grant_id, outcome, request_bytes, "
                    "response_bytes, http_status, error_code, started_at, "
                    "finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        invocation_id,
                        run_id,
                        provider_grant_id,
                        outcome,
                        request_bytes,
                        response_bytes,
                        http_status,
                        error_code,
                        started_at,
                        finished_at,
                    ),
                )
                connection.commit()
            except ConflictError:
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "provider invocation could not be recorded"
                ) from exc
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT id, run_id, provider_grant_id, outcome, request_bytes, "
                "response_bytes, http_status, error_code, started_at, finished_at "
                "FROM provider_invocations WHERE id = ?",
                (invocation_id,),
            ).fetchone()
        if row is None:
            raise StorageError("provider invocation was not recorded")
        return dict(row)

    def list_provider_invocations(self, run_id: str) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            exists = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                raise NotFoundError(f"run not found: {run_id}")
            rows = connection.execute(
                "SELECT id, run_id, provider_grant_id, outcome, request_bytes, "
                "response_bytes, http_status, error_code, started_at, finished_at "
                "FROM provider_invocations WHERE run_id = ? "
                "ORDER BY started_at, id",
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _work_idempotency_key(value: object) -> str:
        if not isinstance(value, str) or IDEMPOTENCY_KEY_RE.fullmatch(value) is None:
            raise ValidationError("idempotency_key is invalid")
        return value

    @staticmethod
    def _positive_integer(value: object, *, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError(f"{field} must be a positive integer")
        return value

    @staticmethod
    def _work_author(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT r.work_thread_id, r.profile_version_id, r.state AS run_state, "
            "wt.state AS thread_state, ra.id AS attempt_id, "
            "ra.runtime_adapter, ap.name AS profile_name, "
            "av.version AS profile_version "
            "FROM runs r LEFT JOIN work_threads wt ON wt.id = r.work_thread_id "
            "LEFT JOIN run_attempts ra ON ra.run_id = r.id AND ra.state = 'running' "
            "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
            "JOIN agent_profiles ap ON ap.id = av.profile_id WHERE r.id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"run not found: {run_id}")
        if row["work_thread_id"] is None:
            raise ConflictError("run has no durable work thread")
        if row["thread_state"] != "active":
            raise ConflictError(f"work thread is not active: {row['thread_state']}")
        if row["attempt_id"] is None:
            raise ConflictError("run has no active running attempt")
        return row

    @staticmethod
    def _work_log_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            entry = WorkLogEntry.from_mapping(json.loads(row["content_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored work-log entry is invalid") from exc
        if entry.digest != row["content_sha256"] or entry.kind.value != row["kind"]:
            raise StorageError("stored work-log entry digest does not match")
        return {
            "id": row["id"],
            "thread_id": row["work_thread_id"],
            "sequence": row["sequence"],
            "content_sha256": row["content_sha256"],
            "assignment_id": row["assignment_id"],
            "author": {
                "run_id": row["author_run_id"],
                "attempt_id": row["author_attempt_id"],
                "profile_name": row["profile_name"],
                "profile_version_id": row["profile_version_id"],
                "profile_version": row["profile_version"],
                "runtime_adapter": row["runtime_adapter"],
            },
            "created_at": row["created_at"],
            **entry.to_dict(),
        }

    @staticmethod
    def _work_log_row(connection: sqlite3.Connection, entry_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT e.id, e.work_thread_id, e.sequence, e.kind, e.content_json, "
            "e.content_sha256, e.author_run_id, e.author_attempt_id, "
            "e.assignment_id, e.created_at, ap.name AS profile_name, "
            "av.id AS profile_version_id, av.version AS profile_version, "
            "ra.runtime_adapter FROM work_log_entries e "
            "JOIN run_attempts ra ON ra.id = e.author_attempt_id "
            "JOIN agent_profile_versions av ON av.id = ra.profile_version_id "
            "JOIN agent_profiles ap ON ap.id = av.profile_id WHERE e.id = ?",
            (entry_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work-log entry not found: {entry_id}")
        return row

    @staticmethod
    def _checkpoint_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            checkpoint = WorkMemoryCheckpoint.from_mapping(
                json.loads(row["content_json"])
            )
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored work-memory checkpoint is invalid") from exc
        if checkpoint.digest != row["content_sha256"]:
            raise StorageError("stored work-memory checkpoint digest does not match")
        return {
            "id": row["id"],
            "thread_id": row["work_thread_id"],
            "version": row["version"],
            "through_sequence": row["through_sequence"],
            "content_sha256": row["content_sha256"],
            "producer": {
                "run_id": row["producer_run_id"],
                "attempt_id": row["producer_attempt_id"],
                "profile_name": row["profile_name"],
                "profile_version_id": row["profile_version_id"],
                "profile_version": row["profile_version"],
                "runtime_adapter": row["runtime_adapter"],
            },
            "created_at": row["created_at"],
            **checkpoint.to_dict(),
        }

    @staticmethod
    def _checkpoint_row(
        connection: sqlite3.Connection, checkpoint_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT c.id, c.work_thread_id, c.version, c.through_sequence, "
            "c.content_json, c.content_sha256, c.producer_run_id, "
            "c.producer_attempt_id, c.created_at, ap.name AS profile_name, "
            "av.id AS profile_version_id, av.version AS profile_version, "
            "ra.runtime_adapter FROM work_memory_checkpoints c "
            "JOIN run_attempts ra ON ra.id = c.producer_attempt_id "
            "JOIN agent_profile_versions av ON av.id = ra.profile_version_id "
            "JOIN agent_profiles ap ON ap.id = av.profile_id WHERE c.id = ?",
            (checkpoint_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work-memory checkpoint not found: {checkpoint_id}")
        return row

    @staticmethod
    def _assignment_record(
        connection: sqlite3.Connection, assignment_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT a.id, a.work_thread_id, a.sequence, a.spec_json, "
            "a.content_sha256, a.state, a.version, a.target_profile_version_id, "
            "target.name AS target_profile_name, "
            "target_version.version AS target_profile_version, "
            "a.created_by_run_id, a.created_by_attempt_id, "
            "creator.name AS creator_profile_name, a.claimed_by_run_id, "
            "a.claimed_by_attempt_id, claimant.name AS claimant_profile_name, "
            "a.lease_expires_at, a.result_entry_id, a.created_at, a.updated_at, "
            "a.completed_at FROM work_assignments a "
            "JOIN agent_profile_versions target_version "
            "ON target_version.id = a.target_profile_version_id "
            "JOIN agent_profiles target ON target.id = target_version.profile_id "
            "JOIN runs creator_run ON creator_run.id = a.created_by_run_id "
            "JOIN agent_profile_versions creator_version "
            "ON creator_version.id = creator_run.profile_version_id "
            "JOIN agent_profiles creator ON creator.id = creator_version.profile_id "
            "LEFT JOIN runs claimant_run "
            "ON claimant_run.id = a.claimed_by_run_id "
            "LEFT JOIN agent_profile_versions claimant_version "
            "ON claimant_version.id = claimant_run.profile_version_id "
            "LEFT JOIN agent_profiles claimant "
            "ON claimant.id = claimant_version.profile_id WHERE a.id = ?",
            (assignment_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work assignment not found: {assignment_id}")
        try:
            spec = WorkAssignmentSpec.from_mapping(json.loads(row["spec_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored work assignment is invalid") from exc
        if spec.digest != row["content_sha256"]:
            raise StorageError("stored work assignment digest does not match")
        event_count = connection.execute(
            "SELECT COUNT(*) FROM work_assignment_events WHERE assignment_id = ?",
            (assignment_id,),
        ).fetchone()[0]
        events = connection.execute(
            "SELECT sequence, kind, from_state, to_state, actor_run_id, "
            "actor_attempt_id, metadata_json, created_at "
            "FROM work_assignment_events WHERE assignment_id = ? "
            "ORDER BY sequence DESC LIMIT ?",
            (assignment_id, MAX_VISIBLE_ASSIGNMENT_EVENTS),
        ).fetchall()
        try:
            parsed_events = [
                {**dict(event), "metadata": json.loads(event["metadata_json"])}
                for event in reversed(events)
            ]
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored work assignment event is invalid") from exc
        for event in parsed_events:
            event.pop("metadata_json")
        return {
            "id": row["id"],
            "thread_id": row["work_thread_id"],
            "sequence": row["sequence"],
            "state": row["state"],
            "version": row["version"],
            "content_sha256": row["content_sha256"],
            "spec": spec.to_dict(),
            "target": {
                "profile_name": row["target_profile_name"],
                "profile_version_id": row["target_profile_version_id"],
                "profile_version": row["target_profile_version"],
            },
            "created_by": {
                "run_id": row["created_by_run_id"],
                "attempt_id": row["created_by_attempt_id"],
                "profile_name": row["creator_profile_name"],
            },
            "claimed_by_run_id": row["claimed_by_run_id"],
            "claimed_by_attempt_id": row["claimed_by_attempt_id"],
            "claimant_profile_name": row["claimant_profile_name"],
            "lease_expires_at": row["lease_expires_at"],
            "result_entry_id": row["result_entry_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
            "event_count": event_count,
            "events_truncated": event_count > len(parsed_events),
            "events": parsed_events,
        }

    @staticmethod
    def _validate_related_interactions(
        connection: sqlite3.Connection,
        *,
        thread_id: str,
        entry: WorkLogEntry,
    ) -> None:
        if not entry.related_interaction_ids:
            return
        for interaction_id in entry.related_interaction_ids:
            exists = connection.execute(
                "SELECT 1 FROM interactions i JOIN runs r ON r.id = i.run_id "
                "WHERE r.work_thread_id = ? AND i.id = ?",
                (thread_id, interaction_id),
            ).fetchone()
            if exists is None:
                raise ConflictError(
                    "related interaction does not belong to work thread"
                )

    def get_work_memory_for_run(
        self,
        run_id: str,
        *,
        after_sequence: int | None = None,
        limit: int = 16,
    ) -> dict[str, Any]:
        if after_sequence is not None and (
            isinstance(after_sequence, bool)
            or not isinstance(after_sequence, int)
            or after_sequence < 0
        ):
            raise ValidationError("after_sequence must be a non-negative integer")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 64
        ):
            raise ValidationError("limit must be between 1 and 64")
        with closing(self.connect()) as connection:
            thread = connection.execute(
                "SELECT wt.id, wt.work_item_id, wt.generation, wt.state, "
                "wt.version, wt.log_sequence, wt.created_at, wt.updated_at, "
                "wt.completed_at FROM runs r JOIN work_threads wt "
                "ON wt.id = r.work_thread_id WHERE r.id = ?",
                (run_id,),
            ).fetchone()
            if thread is None:
                run_exists = connection.execute(
                    "SELECT 1 FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                if run_exists is None:
                    raise NotFoundError(f"run not found: {run_id}")
                raise ConflictError("run has no durable work thread")
            checkpoint_row = connection.execute(
                "SELECT c.id, c.work_thread_id, c.version, c.through_sequence, "
                "c.content_json, c.content_sha256, c.producer_run_id, "
                "c.producer_attempt_id, c.created_at, ap.name AS profile_name, "
                "av.id AS profile_version_id, av.version AS profile_version, "
                "ra.runtime_adapter FROM work_memory_checkpoints c "
                "JOIN run_attempts ra ON ra.id = c.producer_attempt_id "
                "JOIN agent_profile_versions av ON av.id = ra.profile_version_id "
                "JOIN agent_profiles ap ON ap.id = av.profile_id "
                "WHERE c.work_thread_id = ? ORDER BY c.version DESC LIMIT 1",
                (thread["id"],),
            ).fetchone()
            checkpoint_value = (
                None
                if checkpoint_row is None
                else self._checkpoint_record(checkpoint_row)
            )
            effective_after = (
                after_sequence
                if after_sequence is not None
                else (
                    0
                    if checkpoint_value is None
                    else checkpoint_value["through_sequence"]
                )
            )
            rows = connection.execute(
                "SELECT e.id FROM work_log_entries e WHERE e.work_thread_id = ? "
                "AND e.sequence > ? ORDER BY e.sequence LIMIT ?",
                (thread["id"], effective_after, limit + 1),
            ).fetchall()
            page_rows = rows[:limit]
            entries = [
                self._work_log_record(self._work_log_row(connection, row["id"]))
                for row in page_rows
            ]
            assignment_rows = connection.execute(
                "SELECT id FROM work_assignments WHERE work_thread_id = ? "
                "ORDER BY sequence",
                (thread["id"],),
            ).fetchall()
            assignments = [
                self._assignment_record(connection, row["id"])
                for row in assignment_rows
            ]
        result = dict(thread)
        result["thread_id"] = result.pop("id")
        result["checkpoint"] = checkpoint_value
        result["entries"] = entries
        result["entries_truncated"] = len(rows) > limit
        result["next_after_sequence"] = (
            entries[-1]["sequence"] if len(rows) > limit and entries else None
        )
        result["assignments"] = assignments
        return result

    def append_work_log(
        self,
        run_id: str,
        entry: WorkLogEntry,
        *,
        idempotency_key: str,
        assignment_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(entry, WorkLogEntry):
            raise ValidationError("entry must be a validated WorkLogEntry")
        idempotency_key = self._work_idempotency_key(idempotency_key)
        if assignment_id is not None:
            assignment_id = require_string(
                assignment_id, field="assignment_id", max_bytes=64
            )
        entry_id = self.new_id("wle")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                existing = connection.execute(
                    "SELECT id, content_sha256, assignment_id FROM work_log_entries "
                    "WHERE work_thread_id = ? AND author_run_id = ? "
                    "AND idempotency_key = ?",
                    (author["work_thread_id"], run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["content_sha256"] != entry.digest
                        or existing["assignment_id"] != assignment_id
                    ):
                        raise ConflictError(
                            "idempotency key already names a different work-log entry"
                        )
                    stored_id = existing["id"]
                else:
                    if assignment_id is not None:
                        assignment_row = connection.execute(
                            "SELECT state, claimed_by_run_id, claimed_by_attempt_id, "
                            "lease_expires_at, work_thread_id FROM work_assignments "
                            "WHERE id = ?",
                            (assignment_id,),
                        ).fetchone()
                        if assignment_row is None:
                            raise NotFoundError(
                                f"work assignment not found: {assignment_id}"
                            )
                        if assignment_row["work_thread_id"] != author["work_thread_id"]:
                            raise ConflictError(
                                "work assignment belongs to another thread"
                            )
                        if (
                            assignment_row["state"] != "claimed"
                            or assignment_row["claimed_by_run_id"] != run_id
                            or assignment_row["claimed_by_attempt_id"]
                            != author["attempt_id"]
                        ):
                            raise ConflictError(
                                "work assignment is claimed by another run"
                            )
                        if assignment_row["lease_expires_at"] <= now:
                            raise ConflictError("work assignment lease has expired")
                    self._validate_related_interactions(
                        connection,
                        thread_id=author["work_thread_id"],
                        entry=entry,
                    )
                    thread = connection.execute(
                        "SELECT log_sequence FROM work_threads WHERE id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()
                    sequence = thread["log_sequence"] + 1
                    connection.execute(
                        "INSERT INTO work_log_entries "
                        "(id, work_thread_id, sequence, kind, content_json, "
                        "content_sha256, author_run_id, author_attempt_id, "
                        "assignment_id, idempotency_key, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            entry_id,
                            author["work_thread_id"],
                            sequence,
                            entry.kind.value,
                            entry.canonical,
                            entry.digest,
                            run_id,
                            author["attempt_id"],
                            assignment_id,
                            idempotency_key,
                            now,
                        ),
                    )
                    connection.execute(
                        "UPDATE work_threads SET log_sequence = ?, "
                        "version = version + 1, updated_at = ? WHERE id = ?",
                        (sequence, now, author["work_thread_id"]),
                    )
                    stored_id = entry_id
                result = self._work_log_record(
                    self._work_log_row(connection, stored_id)
                )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work-log entry could not be recorded") from exc
        return result

    def create_work_memory_checkpoint(
        self,
        run_id: str,
        checkpoint: WorkMemoryCheckpoint,
        *,
        through_sequence: int,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if not isinstance(checkpoint, WorkMemoryCheckpoint):
            raise ValidationError("checkpoint must be a validated WorkMemoryCheckpoint")
        if (
            isinstance(through_sequence, bool)
            or not isinstance(through_sequence, int)
            or through_sequence < 0
        ):
            raise ValidationError("through_sequence must be a non-negative integer")
        self._positive_integer(expected_thread_version, field="expected_thread_version")
        idempotency_key = self._work_idempotency_key(idempotency_key)
        checkpoint_id = self.new_id("wmc")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                existing = connection.execute(
                    "SELECT id, content_sha256, through_sequence "
                    "FROM work_memory_checkpoints WHERE work_thread_id = ? "
                    "AND producer_run_id = ? AND idempotency_key = ?",
                    (author["work_thread_id"], run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["content_sha256"] != checkpoint.digest
                        or existing["through_sequence"] != through_sequence
                    ):
                        raise ConflictError(
                            "idempotency key already names a different checkpoint"
                        )
                    stored_id = existing["id"]
                else:
                    thread = connection.execute(
                        "SELECT version, log_sequence FROM work_threads WHERE id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()
                    if thread["version"] != expected_thread_version:
                        raise ConcurrentUpdateError(
                            f"work thread version is {thread['version']}, "
                            f"expected {expected_thread_version}"
                        )
                    if through_sequence != thread["log_sequence"]:
                        raise ConflictError(
                            "checkpoint must cover the latest log sequence"
                        )
                    version = connection.execute(
                        "SELECT COALESCE(MAX(version), 0) + 1 "
                        "FROM work_memory_checkpoints WHERE work_thread_id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()[0]
                    connection.execute(
                        "INSERT INTO work_memory_checkpoints "
                        "(id, work_thread_id, version, through_sequence, "
                        "content_json, content_sha256, producer_run_id, "
                        "producer_attempt_id, idempotency_key, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            checkpoint_id,
                            author["work_thread_id"],
                            version,
                            through_sequence,
                            checkpoint.canonical,
                            checkpoint.digest,
                            run_id,
                            author["attempt_id"],
                            idempotency_key,
                            now,
                        ),
                    )
                    changed = connection.execute(
                        "UPDATE work_threads SET version = version + 1, "
                        "updated_at = ? WHERE id = ? AND version = ?",
                        (
                            now,
                            author["work_thread_id"],
                            expected_thread_version,
                        ),
                    ).rowcount
                    if changed != 1:
                        raise ConcurrentUpdateError("work thread changed concurrently")
                    stored_id = checkpoint_id
                result = self._checkpoint_record(
                    self._checkpoint_row(connection, stored_id)
                )
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError(
                    "work-memory checkpoint could not be recorded"
                ) from exc
        return result

    def create_work_assignment(
        self,
        run_id: str,
        *,
        target_profile_name: str,
        spec: WorkAssignmentSpec,
        expected_thread_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        target_profile_name = validate_name(
            target_profile_name, field="target profile name"
        )
        if not isinstance(spec, WorkAssignmentSpec):
            raise ValidationError("spec must be a validated WorkAssignmentSpec")
        self._positive_integer(expected_thread_version, field="expected_thread_version")
        idempotency_key = self._work_idempotency_key(idempotency_key)
        assignment_id = self.new_id("was")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                existing = connection.execute(
                    "SELECT a.id, a.target_profile_version_id, a.content_sha256, "
                    "ap.name AS target_profile_name FROM work_assignments a "
                    "JOIN agent_profile_versions av "
                    "ON av.id = a.target_profile_version_id "
                    "JOIN agent_profiles ap ON ap.id = av.profile_id "
                    "WHERE a.work_thread_id = ? "
                    "AND created_by_run_id = ? AND idempotency_key = ?",
                    (author["work_thread_id"], run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["target_profile_name"] != target_profile_name
                        or existing["content_sha256"] != spec.digest
                    ):
                        raise ConflictError(
                            "idempotency key already names a different work assignment"
                        )
                    stored_id = existing["id"]
                else:
                    target = connection.execute(
                        "SELECT av.id, av.version, ki.krab_instance_id "
                        "FROM agent_profiles ap JOIN agent_profile_versions av "
                        "ON av.profile_id = ap.id JOIN krab_identities ki "
                        "ON ki.id = av.krab_identity_id WHERE ap.name = ? "
                        "AND ap.active = 1 ORDER BY av.version DESC LIMIT 1",
                        (target_profile_name,),
                    ).fetchone()
                    if target is None:
                        raise NotFoundError(
                            f"active agent profile not found: {target_profile_name}"
                        )
                    project_instance = connection.execute(
                        "SELECT p.krab_instance_id FROM work_threads wt "
                        "JOIN work_items wi ON wi.id = wt.work_item_id "
                        "JOIN projects p ON p.id = wi.project_id WHERE wt.id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()["krab_instance_id"]
                    if target["krab_instance_id"] != project_instance:
                        raise ConflictError(
                            "target profile and work item belong to different "
                            "Krab instances"
                        )
                    thread = connection.execute(
                        "SELECT version FROM work_threads WHERE id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()
                    if thread["version"] != expected_thread_version:
                        raise ConcurrentUpdateError(
                            f"work thread version is {thread['version']}, "
                            f"expected {expected_thread_version}"
                        )
                    assignment_count = connection.execute(
                        "SELECT COUNT(*) FROM work_assignments "
                        "WHERE work_thread_id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()[0]
                    if assignment_count >= MAX_WORK_ASSIGNMENTS_PER_THREAD:
                        raise ConflictError(
                            "work thread has reached its assignment limit"
                        )
                    sequence = connection.execute(
                        "SELECT COALESCE(MAX(sequence), 0) + 1 "
                        "FROM work_assignments WHERE work_thread_id = ?",
                        (author["work_thread_id"],),
                    ).fetchone()[0]
                    connection.execute(
                        "INSERT INTO work_assignments "
                        "(id, work_thread_id, sequence, target_profile_version_id, "
                        "spec_json, content_sha256, state, version, "
                        "created_by_run_id, created_by_attempt_id, idempotency_key, "
                        "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, "
                        "'open', 1, ?, ?, ?, ?, ?)",
                        (
                            assignment_id,
                            author["work_thread_id"],
                            sequence,
                            target["id"],
                            spec.canonical,
                            spec.digest,
                            run_id,
                            author["attempt_id"],
                            idempotency_key,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO work_assignment_events "
                        "(assignment_id, sequence, kind, from_state, to_state, "
                        "actor_run_id, actor_attempt_id, metadata_json, created_at) "
                        "VALUES (?, 1, 'assignment.created', NULL, 'open', ?, ?, ?, ?)",
                        (
                            assignment_id,
                            run_id,
                            author["attempt_id"],
                            canonical_json({"target_profile_version_id": target["id"]}),
                            now,
                        ),
                    )
                    changed = connection.execute(
                        "UPDATE work_threads SET version = version + 1, "
                        "updated_at = ? WHERE id = ? AND version = ?",
                        (
                            now,
                            author["work_thread_id"],
                            expected_thread_version,
                        ),
                    ).rowcount
                    if changed != 1:
                        raise ConcurrentUpdateError("work thread changed concurrently")
                    stored_id = assignment_id
                result = self._assignment_record(connection, stored_id)
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work assignment could not be created") from exc
        return result

    def claim_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        lease_seconds: int,
    ) -> dict[str, Any]:
        assignment_id = require_string(
            assignment_id, field="assignment_id", max_bytes=64
        )
        self._positive_integer(
            expected_assignment_version, field="expected_assignment_version"
        )
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or not 30 <= lease_seconds <= 86_400
        ):
            raise ValidationError("lease_seconds must be between 30 and 86400")
        now = self.now()
        lease_expires_at = (
            datetime.now(UTC) + timedelta(seconds=lease_seconds)
        ).isoformat(timespec="microseconds")
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                current = connection.execute(
                    "SELECT work_thread_id, target_profile_version_id, state, "
                    "version, claimed_by_run_id, claimed_by_attempt_id, "
                    "lease_expires_at FROM work_assignments WHERE id = ?",
                    (assignment_id,),
                ).fetchone()
                if current is None:
                    raise NotFoundError(f"work assignment not found: {assignment_id}")
                if current["work_thread_id"] != author["work_thread_id"]:
                    raise ConflictError("work assignment belongs to another thread")
                if current["target_profile_version_id"] != author["profile_version_id"]:
                    raise ConflictError(
                        "run does not use the assignment target profile version"
                    )
                if (
                    current["state"] == "claimed"
                    and current["claimed_by_run_id"] == run_id
                    and current["claimed_by_attempt_id"] == author["attempt_id"]
                    and current["lease_expires_at"] > now
                ):
                    result = self._assignment_record(connection, assignment_id)
                    connection.commit()
                    return result
                if current["version"] != expected_assignment_version:
                    raise ConcurrentUpdateError(
                        f"work assignment version is {current['version']}, "
                        f"expected {expected_assignment_version}"
                    )
                if current["state"] == "claimed" and current["lease_expires_at"] > now:
                    raise ConflictError("work assignment is claimed by another run")
                if current["state"] not in {"open", "claimed"}:
                    raise ConflictError(
                        f"work assignment cannot be claimed: {current['state']}"
                    )
                reclaimed = current["state"] == "claimed"
                changed = connection.execute(
                    "UPDATE work_assignments SET state = 'claimed', "
                    "version = version + 1, claimed_by_run_id = ?, "
                    "claimed_by_attempt_id = ?, lease_expires_at = ?, "
                    "updated_at = ? WHERE id = ? AND version = ?",
                    (
                        run_id,
                        author["attempt_id"],
                        lease_expires_at,
                        now,
                        assignment_id,
                        expected_assignment_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("work assignment changed concurrently")
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM work_assignment_events WHERE assignment_id = ?",
                    (assignment_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO work_assignment_events "
                    "(assignment_id, sequence, kind, from_state, to_state, "
                    "actor_run_id, actor_attempt_id, metadata_json, created_at) "
                    "VALUES (?, ?, ?, ?, 'claimed', ?, ?, ?, ?)",
                    (
                        assignment_id,
                        event_sequence,
                        "assignment.reclaimed" if reclaimed else "assignment.claimed",
                        current["state"],
                        run_id,
                        author["attempt_id"],
                        canonical_json({"lease_expires_at": lease_expires_at}),
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE work_threads SET version = version + 1, "
                    "updated_at = ? WHERE id = ?",
                    (now, author["work_thread_id"]),
                )
                result = self._assignment_record(connection, assignment_id)
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work assignment could not be claimed") from exc
        return result

    def complete_work_assignment(
        self,
        run_id: str,
        assignment_id: str,
        *,
        expected_assignment_version: int,
        result: WorkLogEntry,
        idempotency_key: str,
    ) -> dict[str, Any]:
        assignment_id = require_string(
            assignment_id, field="assignment_id", max_bytes=64
        )
        self._positive_integer(
            expected_assignment_version, field="expected_assignment_version"
        )
        if not isinstance(result, WorkLogEntry):
            raise ValidationError("result must be a validated WorkLogEntry")
        if result.kind is not WorkLogKind.ASSIGNMENT_RESULT:
            raise ValidationError("result kind must be assignment_result")
        idempotency_key = self._work_idempotency_key(idempotency_key)
        entry_id = self.new_id("wle")
        now = self.now()
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                author = self._work_author(connection, run_id)
                current = connection.execute(
                    "SELECT work_thread_id, state, version, claimed_by_run_id, "
                    "claimed_by_attempt_id, lease_expires_at, result_entry_id "
                    "FROM work_assignments WHERE id = ?",
                    (assignment_id,),
                ).fetchone()
                if current is None:
                    raise NotFoundError(f"work assignment not found: {assignment_id}")
                if current["work_thread_id"] != author["work_thread_id"]:
                    raise ConflictError("work assignment belongs to another thread")
                existing = connection.execute(
                    "SELECT id, content_sha256, assignment_id FROM work_log_entries "
                    "WHERE work_thread_id = ? AND author_run_id = ? "
                    "AND idempotency_key = ?",
                    (author["work_thread_id"], run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["content_sha256"] != result.digest
                        or existing["assignment_id"] != assignment_id
                        or current["result_entry_id"] != existing["id"]
                    ):
                        raise ConflictError(
                            "idempotency key already names a different "
                            "assignment result"
                        )
                    stored = self._assignment_record(connection, assignment_id)
                    connection.commit()
                    return stored
                if current["version"] != expected_assignment_version:
                    raise ConcurrentUpdateError(
                        f"work assignment version is {current['version']}, "
                        f"expected {expected_assignment_version}"
                    )
                if (
                    current["state"] != "claimed"
                    or current["claimed_by_run_id"] != run_id
                    or current["claimed_by_attempt_id"] != author["attempt_id"]
                ):
                    raise ConflictError("work assignment is claimed by another run")
                if current["lease_expires_at"] <= now:
                    raise ConflictError("work assignment lease has expired")
                self._validate_related_interactions(
                    connection,
                    thread_id=author["work_thread_id"],
                    entry=result,
                )
                thread = connection.execute(
                    "SELECT log_sequence FROM work_threads WHERE id = ?",
                    (author["work_thread_id"],),
                ).fetchone()
                log_sequence = thread["log_sequence"] + 1
                connection.execute(
                    "INSERT INTO work_log_entries "
                    "(id, work_thread_id, sequence, kind, content_json, "
                    "content_sha256, author_run_id, author_attempt_id, "
                    "assignment_id, idempotency_key, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        entry_id,
                        author["work_thread_id"],
                        log_sequence,
                        result.kind.value,
                        result.canonical,
                        result.digest,
                        run_id,
                        author["attempt_id"],
                        assignment_id,
                        idempotency_key,
                        now,
                    ),
                )
                changed = connection.execute(
                    "UPDATE work_assignments SET state = 'completed', "
                    "version = version + 1, result_entry_id = ?, "
                    "updated_at = ?, completed_at = ? WHERE id = ? AND version = ?",
                    (
                        entry_id,
                        now,
                        now,
                        assignment_id,
                        expected_assignment_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConcurrentUpdateError("work assignment changed concurrently")
                event_sequence = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM work_assignment_events WHERE assignment_id = ?",
                    (assignment_id,),
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO work_assignment_events "
                    "(assignment_id, sequence, kind, from_state, to_state, "
                    "actor_run_id, actor_attempt_id, metadata_json, created_at) "
                    "VALUES (?, ?, 'assignment.completed', 'claimed', "
                    "'completed', ?, ?, ?, ?)",
                    (
                        assignment_id,
                        event_sequence,
                        run_id,
                        author["attempt_id"],
                        canonical_json({"result_entry_id": entry_id}),
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE work_threads SET log_sequence = ?, "
                    "version = version + 1, updated_at = ? WHERE id = ?",
                    (log_sequence, now, author["work_thread_id"]),
                )
                stored = self._assignment_record(connection, assignment_id)
                connection.commit()
            except (NotFoundError, ConflictError, StorageError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("work assignment could not be completed") from exc
        return stored

    @staticmethod
    def _handoff_record(row: sqlite3.Row) -> dict[str, Any]:
        try:
            envelope = HandoffEnvelope.from_mapping(json.loads(row["content_json"]))
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("stored attempt handoff is invalid") from exc
        if envelope.digest != row["content_sha256"]:
            raise StorageError("stored attempt handoff digest does not match")
        return {
            "id": row["id"],
            "attempt_id": row["attempt_id"],
            "content_sha256": row["content_sha256"],
            "created_at": row["created_at"],
            **envelope.to_dict(),
        }

    def get_run_attempt(self, attempt_id: str) -> dict[str, Any]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT ra.id, ra.run_id, ra.sequence, ra.runtime_adapter, "
                "ra.state, ra.version, ra.source_revision, ra.container_id, "
                "ra.container_name, ra.image_id, ra.exit_code, ra.reason, "
                "ra.created_at, ra.started_at, ra.finished_at, "
                "r.state AS run_state, r.version AS run_version, "
                "ap.name AS profile_name, av.id AS profile_version_id, "
                "av.version AS profile_version "
                "FROM run_attempts ra JOIN runs r ON r.id = ra.run_id "
                "JOIN agent_profile_versions av ON av.id = ra.profile_version_id "
                "JOIN agent_profiles ap ON ap.id = av.profile_id WHERE ra.id = ?",
                (attempt_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"run attempt not found: {attempt_id}")
            events = connection.execute(
                "SELECT sequence, kind, from_state, to_state, metadata_json, "
                "created_at FROM run_attempt_events WHERE attempt_id = ? "
                "ORDER BY sequence",
                (attempt_id,),
            ).fetchall()
            handoff = connection.execute(
                "SELECT id, attempt_id, content_json, content_sha256, created_at "
                "FROM attempt_handoffs WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
        result = dict(row)
        try:
            result["events"] = [
                {
                    **dict(event),
                    "metadata": json.loads(event["metadata_json"]),
                }
                for event in events
            ]
        except (json.JSONDecodeError, TypeError) as exc:
            raise StorageError("stored run attempt event is invalid") from exc
        for event in result["events"]:
            event.pop("metadata_json")
        result["handoff"] = None if handoff is None else self._handoff_record(handoff)
        return result

    def list_run_attempts(self, run_id: str) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            exists = connection.execute(
                "SELECT 1 FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                raise NotFoundError(f"run not found: {run_id}")
            rows = connection.execute(
                "SELECT id FROM run_attempts WHERE run_id = ? ORDER BY sequence",
                (run_id,),
            ).fetchall()
        return [self.get_run_attempt(row["id"]) for row in rows]

    def create_attempt_handoff(
        self, run_id: str, handoff: HandoffEnvelope
    ) -> dict[str, Any]:
        if not isinstance(handoff, HandoffEnvelope):
            raise ValidationError("handoff must be a validated envelope")
        if handoff.work_state is not HandoffWorkState.WAITING_FOR_INPUT:
            raise ValidationError("handoff work_state must be waiting_for_input")
        handoff_id = self.new_id("hof")
        now = self.now()
        stored_attempt_id: str | None = None
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                run = connection.execute(
                    "SELECT state FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                if run is None:
                    raise NotFoundError(f"run not found: {run_id}")
                if run["state"] not in {
                    RunState.WAITING_FOR_INPUT.value,
                    RunState.QUEUED.value,
                }:
                    raise ConflictError(f"run is not waiting for input: {run['state']}")
                attempt = connection.execute(
                    "SELECT id, state FROM run_attempts WHERE run_id = ? "
                    "AND state IN ('preparing', 'running')",
                    (run_id,),
                ).fetchone()
                if attempt is None or attempt["state"] != AttemptState.RUNNING.value:
                    raise ConflictError("run has no active running attempt")
                attempt_interactions = connection.execute(
                    "SELECT id FROM interactions WHERE run_id = ? AND attempt_id = ? "
                    "AND state IN ('open', 'answered') ORDER BY id",
                    (run_id, attempt["id"]),
                ).fetchall()
                expected_interactions = tuple(row["id"] for row in attempt_interactions)
                if (
                    not expected_interactions
                    or not handoff.pending_interaction_ids
                    or len(expected_interactions)
                    != len(handoff.pending_interaction_ids)
                    or set(expected_interactions)
                    != set(handoff.pending_interaction_ids)
                ):
                    raise ConflictError(
                        "handoff pending interactions do not match the run"
                    )
                existing = connection.execute(
                    "SELECT id, attempt_id, content_json, content_sha256, created_at "
                    "FROM attempt_handoffs WHERE attempt_id = ?",
                    (attempt["id"],),
                ).fetchone()
                if existing is not None:
                    if existing["content_sha256"] != handoff.digest:
                        raise ConflictError(
                            "run attempt already has a different handoff"
                        )
                    stored_attempt_id = attempt["id"]
                else:
                    connection.execute(
                        "INSERT INTO attempt_handoffs "
                        "(id, attempt_id, content_json, content_sha256, created_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            handoff_id,
                            attempt["id"],
                            handoff.canonical,
                            handoff.digest,
                            now,
                        ),
                    )
                    sequence = connection.execute(
                        "SELECT COALESCE(MAX(sequence), 0) + 1 "
                        "FROM run_attempt_events WHERE attempt_id = ?",
                        (attempt["id"],),
                    ).fetchone()[0]
                    connection.execute(
                        "INSERT INTO run_attempt_events "
                        "(attempt_id, sequence, kind, from_state, to_state, "
                        "metadata_json, created_at) VALUES (?, ?, "
                        "'attempt.handoff_submitted', 'running', 'running', ?, ?)",
                        (
                            attempt["id"],
                            sequence,
                            canonical_json(
                                {
                                    "handoff_id": handoff_id,
                                    "content_sha256": handoff.digest,
                                }
                            ),
                            now,
                        ),
                    )
                    stored_attempt_id = attempt["id"]
                connection.commit()
            except (NotFoundError, ConflictError, ValidationError):
                connection.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ConflictError("attempt handoff could not be recorded") from exc
        if stored_attempt_id is None:
            raise StorageError("attempt handoff was not recorded")
        result = self.get_run_attempt(stored_attempt_id)["handoff"]
        if result is None:
            raise StorageError("attempt handoff was not recorded")
        return result

    def get_run_execution(self, run_id: str) -> dict[str, Any]:
        """Load every immutable input and identity pinned by a run."""

        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT r.id, r.state, r.version, r.source_revision, "
                "r.work_thread_id, "
                "p.krab_project_key AS project_key, c.context_json, "
                "av.spec_json, ki.credential_ref, wi.id AS work_item_id, "
                "wi.source AS work_item_source, "
                "wi.external_id AS work_item_external_id, "
                "wiv.id AS work_item_version_id, "
                "wiv.version AS work_item_version, "
                "wiv.source_updated_at AS work_item_source_updated_at, "
                "wiv.content_json AS work_item_json, "
                "wiv.content_sha256 AS work_item_sha256 "
                "FROM runs r JOIN projects p ON p.id = r.project_id "
                "JOIN project_context_versions c ON c.id = r.context_version_id "
                "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
                "JOIN krab_identities ki ON ki.id = av.krab_identity_id "
                "LEFT JOIN work_item_versions wiv "
                "ON wiv.id = r.work_item_version_id "
                "LEFT JOIN work_items wi ON wi.id = wiv.work_item_id "
                "WHERE r.id = ?",
                (run_id,),
            ).fetchone()
            provider_references = connection.execute(
                "SELECT rpg.authority_name, pg.credential_ref "
                "FROM run_provider_grants rpg JOIN provider_grants pg "
                "ON pg.id = rpg.provider_grant_id WHERE rpg.run_id = ? "
                "ORDER BY rpg.authority_name",
                (run_id,),
            ).fetchall()
            handoff_row = connection.execute(
                "SELECT h.id, h.attempt_id, h.content_json, h.content_sha256, "
                "h.created_at FROM attempt_handoffs h JOIN run_attempts a "
                "ON a.id = h.attempt_id WHERE a.run_id = ? "
                "ORDER BY a.sequence DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"run not found: {run_id}")
        result = dict(row)
        try:
            result["context"] = json.loads(result.pop("context_json"))
            result["profile"] = json.loads(result.pop("spec_json"))
            work_item_json = result.pop("work_item_json")
            if work_item_json is None:
                result["work_item"] = None
            else:
                content = json.loads(work_item_json)
                work_item = WorkItemEnvelope.from_mapping(
                    {
                        **content,
                        "source": result.pop("work_item_source"),
                        "external_id": result.pop("work_item_external_id"),
                        "source_updated_at": result.pop("work_item_source_updated_at"),
                    }
                )
                if work_item.digest != result.pop("work_item_sha256"):
                    raise ValidationError("work item digest does not match")
                result["work_item"] = {
                    "id": result["work_item_id"],
                    "version": result["work_item_version"],
                    "source": work_item.source,
                    "external_id": work_item.external_id,
                    "source_updated_at": work_item.source_updated_at,
                    **work_item.content,
                }
            if work_item_json is None:
                result.pop("work_item_source")
                result.pop("work_item_external_id")
                result.pop("work_item_source_updated_at")
                result.pop("work_item_sha256")
        except (json.JSONDecodeError, TypeError, ValidationError) as exc:
            raise StorageError("pinned run data is invalid") from exc
        result["credential_references"] = {
            "krab": result.pop("credential_ref"),
            **{
                provider["authority_name"]: provider["credential_ref"]
                for provider in provider_references
            },
        }
        result["handoff"] = (
            None if handoff_row is None else self._handoff_record(handoff_row)
        )
        result["work_memory"] = self.get_work_memory_for_run(run_id)
        return result

    def get_run_transport(self, run_id: str) -> dict[str, Any]:
        """Load trusted Krab routing metadata pinned by a run."""

        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT r.id, r.state, r.version, r.krab_session_id, "
                "r.krab_claim_id, p.krab_project_key AS project_key, "
                "p.krab_instance_id, ksi.name AS krab_instance_name, "
                "ki.id AS agent_krab_identity_id, "
                "ki.krab_user_id AS agent_krab_user_id, "
                "ki.handle AS agent_krab_handle "
                "FROM runs r JOIN projects p ON p.id = r.project_id "
                "JOIN krab_instances ksi ON ksi.id = p.krab_instance_id "
                "JOIN agent_profile_versions av "
                "ON av.id = r.profile_version_id "
                "JOIN krab_identities ki ON ki.id = av.krab_identity_id "
                "WHERE r.id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"run not found: {run_id}")
        return dict(row)

    def get_run(self, run_id: str) -> dict[str, Any]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT r.id, r.state, r.version, r.source_revision, "
                "r.work_thread_id, "
                "r.krab_session_id, r.krab_claim_id, r.created_at, r.updated_at, "
                "r.started_at, r.finished_at, p.krab_project_key AS project_key, "
                "c.id AS context_version_id, c.version AS context_version, "
                "ap.name AS profile_name, av.id AS profile_version_id, "
                "av.version AS profile_version, u.handle AS requested_by, "
                "wi.id AS work_item_id, wiv.id AS work_item_version_id, "
                "wiv.version AS work_item_version, "
                "wiv.content_json AS work_item_json "
                "FROM runs r JOIN projects p ON p.id = r.project_id "
                "JOIN project_context_versions c ON c.id = r.context_version_id "
                "JOIN agent_profile_versions av ON av.id = r.profile_version_id "
                "JOIN agent_profiles ap ON ap.id = av.profile_id "
                "JOIN users u ON u.id = r.requested_by "
                "LEFT JOIN work_item_versions wiv "
                "ON wiv.id = r.work_item_version_id "
                "LEFT JOIN work_items wi ON wi.id = wiv.work_item_id "
                "WHERE r.id = ?",
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
        try:
            work_item_json = result.pop("work_item_json")
            result["work_item_title"] = (
                None if work_item_json is None else json.loads(work_item_json)["title"]
            )
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise StorageError("pinned work item data is invalid") from exc
        result["events"] = [
            {**dict(event), "metadata": json.loads(event["metadata_json"])}
            for event in events
        ]
        for event in result["events"]:
            event.pop("metadata_json")
        result["interactions"] = self.list_run_interactions(run_id)
        result["attempts"] = self.list_run_attempts(run_id)
        result["provider_grants"] = self.list_run_provider_grants(run_id)
        result["provider_invocations"] = self.list_provider_invocations(run_id)
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
