from __future__ import annotations

SCHEMA_VERSION = 9
STATE_DIRECTORY_NAME = "magic-glass-box"
DATABASE_FILENAME = "control-plane.db"
LEGACY_STATE_DIRECTORY_NAME = "hexadecapus"
LEGACY_DATABASE_FILENAME = "hex.db"
MAX_WORK_ASSIGNMENTS_PER_THREAD = 16
MAX_VISIBLE_ASSIGNMENT_EVENTS = 16
MIN_ATTEMPT_LEASE_SECONDS = 10
MAX_ATTEMPT_LEASE_SECONDS = 3600


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

WORK_THREAD_LIFECYCLE_SCHEMA = """
CREATE TABLE work_thread_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_thread_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN ('work_thread.created', 'work_thread.completed')
    ),
    from_state TEXT CHECK (from_state IS NULL OR from_state = 'active'),
    to_state TEXT NOT NULL CHECK (to_state IN ('active', 'completed')),
    actor_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (work_thread_id, sequence),
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (kind = 'work_thread.created' AND from_state IS NULL AND to_state = 'active')
        OR (
            kind = 'work_thread.completed'
            AND from_state = 'active'
            AND to_state = 'completed'
        )
    )
) STRICT;

INSERT INTO work_thread_events (
    work_thread_id, sequence, kind, from_state, to_state, actor_id,
    metadata_json, created_at
)
SELECT
    wt.id, 1, 'work_thread.created', NULL, 'active', wi.last_imported_by,
    '{}', wt.created_at
FROM work_threads wt
JOIN work_items wi ON wi.id = wt.work_item_id;

INSERT INTO work_thread_events (
    work_thread_id, sequence, kind, from_state, to_state, actor_id,
    metadata_json, created_at
)
SELECT
    wt.id, 2, 'work_thread.completed', 'active', 'completed',
    wi.last_imported_by, '{"migration_backfill":true}', wt.completed_at
FROM work_threads wt
JOIN work_items wi ON wi.id = wt.work_item_id
WHERE wt.state = 'completed';

CREATE INDEX work_thread_event_stream
    ON work_thread_events(work_thread_id, sequence);

CREATE TRIGGER work_thread_event_immutable_update
BEFORE UPDATE ON work_thread_events
BEGIN
    SELECT RAISE(ABORT, 'work-thread events are immutable');
END;
CREATE TRIGGER work_thread_event_immutable_delete
BEFORE DELETE ON work_thread_events
BEGIN
    SELECT RAISE(ABORT, 'work-thread events are immutable');
END;
CREATE TRIGGER work_thread_completed_immutable
BEFORE UPDATE ON work_threads
WHEN OLD.state = 'completed'
BEGIN
    SELECT RAISE(ABORT, 'completed work threads are immutable');
END;
CREATE TRIGGER work_thread_immutable_delete
BEFORE DELETE ON work_threads
BEGIN
    SELECT RAISE(ABORT, 'work threads cannot be deleted');
END;
"""

EXECUTION_INTEGRITY_SCHEMA = """
ALTER TABLE run_attempts ADD COLUMN owner_id TEXT;
ALTER TABLE run_attempts ADD COLUMN heartbeat_at TEXT;
ALTER TABLE run_attempts ADD COLUMN lease_expires_at TEXT;
ALTER TABLE run_attempts ADD COLUMN cancel_requested_at TEXT;
ALTER TABLE run_attempts ADD COLUMN cancel_requested_by TEXT
    REFERENCES users(id) ON DELETE RESTRICT;
ALTER TABLE run_attempts ADD COLUMN cancel_reason TEXT;

UPDATE run_attempts
SET owner_id = 'orchestrator-legacy-recovery',
    heartbeat_at = COALESCE(started_at, created_at),
    lease_expires_at = COALESCE(started_at, created_at)
WHERE state IN ('preparing', 'running');

CREATE TABLE attempt_results (
    id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    manifest_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    changed_paths INTEGER NOT NULL CHECK (changed_paths >= 0),
    changed_bytes INTEGER NOT NULL CHECK (changed_bytes >= 0),
    created_at TEXT NOT NULL,
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX run_attempt_expired_lease
    ON run_attempts(state, lease_expires_at)
    WHERE state IN ('preparing', 'running');

CREATE TRIGGER run_attempt_active_control_insert
BEFORE INSERT ON run_attempts
WHEN NEW.state IN ('preparing', 'running') AND (
    NEW.owner_id IS NULL OR NEW.heartbeat_at IS NULL
    OR NEW.lease_expires_at IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'active run attempts require an owner lease');
END;
CREATE TRIGGER run_attempt_active_control_update
BEFORE UPDATE ON run_attempts
WHEN NEW.state IN ('preparing', 'running') AND (
    NEW.owner_id IS NULL OR NEW.heartbeat_at IS NULL
    OR NEW.lease_expires_at IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'active run attempts require an owner lease');
END;
CREATE TRIGGER run_attempt_cancel_consistent_insert
BEFORE INSERT ON run_attempts
WHEN NOT (
    (
        NEW.cancel_requested_at IS NULL
        AND NEW.cancel_requested_by IS NULL
        AND NEW.cancel_reason IS NULL
    )
    OR (
        NEW.cancel_requested_at IS NOT NULL
        AND NEW.cancel_requested_by IS NOT NULL
        AND NEW.cancel_reason IS NOT NULL
    )
)
BEGIN
    SELECT RAISE(ABORT, 'run attempt cancellation request is incomplete');
END;
CREATE TRIGGER run_attempt_cancel_consistent_update
BEFORE UPDATE ON run_attempts
WHEN NOT (
    (
        NEW.cancel_requested_at IS NULL
        AND NEW.cancel_requested_by IS NULL
        AND NEW.cancel_reason IS NULL
    )
    OR (
        NEW.cancel_requested_at IS NOT NULL
        AND NEW.cancel_requested_by IS NOT NULL
        AND NEW.cancel_reason IS NOT NULL
    )
)
BEGIN
    SELECT RAISE(ABORT, 'run attempt cancellation request is incomplete');
END;
CREATE TRIGGER attempt_result_immutable_update
BEFORE UPDATE ON attempt_results
BEGIN
    SELECT RAISE(ABORT, 'attempt results are immutable');
END;
CREATE TRIGGER attempt_result_immutable_delete
BEFORE DELETE ON attempt_results
BEGIN
    SELECT RAISE(ABORT, 'attempt results are immutable');
END;
"""

RUN_REPLACEMENT_SCHEMA = """
ALTER TABLE runs ADD COLUMN predecessor_run_id TEXT
    REFERENCES runs(id) ON DELETE RESTRICT;
ALTER TABLE runs ADD COLUMN replacement_kind TEXT
    CHECK (replacement_kind IN ('retry', 'reassign'));

CREATE INDEX run_predecessor_created
    ON runs(predecessor_run_id, created_at DESC)
    WHERE predecessor_run_id IS NOT NULL;
CREATE UNIQUE INDEX run_one_active_successor
    ON runs(predecessor_run_id)
    WHERE predecessor_run_id IS NOT NULL
      AND state IN ('queued', 'preparing', 'running', 'waiting_for_input');

CREATE TRIGGER run_replacement_consistent_insert
BEFORE INSERT ON runs
WHEN (NEW.predecessor_run_id IS NULL) <> (NEW.replacement_kind IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'run replacement metadata is incomplete');
END;
CREATE TRIGGER run_replacement_consistent_update
BEFORE UPDATE ON runs
WHEN (NEW.predecessor_run_id IS NULL) <> (NEW.replacement_kind IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'run replacement metadata is incomplete');
END;
CREATE TRIGGER run_replacement_immutable_update
BEFORE UPDATE OF predecessor_run_id, replacement_kind ON runs
WHEN NEW.predecessor_run_id IS NOT OLD.predecessor_run_id
  OR NEW.replacement_kind IS NOT OLD.replacement_kind
BEGIN
    SELECT RAISE(ABORT, 'run replacement metadata is immutable');
END;
"""

SCHEMA = (
    SCHEMA_V1
    + INTERACTION_SCHEMA
    + WORK_ITEM_SCHEMA
    + PROVIDER_SCHEMA
    + ATTEMPT_SCHEMA
    + WORK_MEMORY_SCHEMA
    + WORK_THREAD_LIFECYCLE_SCHEMA
    + EXECUTION_INTEGRITY_SCHEMA
    + RUN_REPLACEMENT_SCHEMA
)
MIGRATIONS = {
    1: INTERACTION_SCHEMA,
    2: WORK_ITEM_SCHEMA,
    3: PROVIDER_SCHEMA,
    4: ATTEMPT_SCHEMA,
    5: WORK_MEMORY_SCHEMA,
    6: WORK_THREAD_LIFECYCLE_SCHEMA,
    7: EXECUTION_INTEGRITY_SCHEMA,
    8: RUN_REPLACEMENT_SCHEMA,
}
